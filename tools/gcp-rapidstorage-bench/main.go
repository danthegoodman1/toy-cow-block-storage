package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"cloud.google.com/go/storage/experimental"
)

const mib = 1024 * 1024

type config struct {
	bucket           string
	prefix           string
	modes            []string
	workers          []int
	opMiB            []int
	totalMiB         int
	publishMiB       int
	chunkMiB         int
	finalizeOnClose  bool
	cleanup          bool
	timeout          time.Duration
	csvPath          string
	jsonOutput       bool
	latencySamples   int
	latencyObjectKiB int
	probeTarget      string
}

type workerResult struct {
	err            error
	bytes          int64
	publishedBytes int64
	publishDone    time.Time
	writeLatencies []time.Duration
	flushLatencies []time.Duration
	closeLatencies []time.Duration
	probeLatencies []time.Duration
	objectNames    []string
}

type resultRow struct {
	Bucket          string  `json:"bucket"`
	Prefix          string  `json:"prefix"`
	Mode            string  `json:"mode"`
	Workers         int     `json:"workers"`
	TotalMiB        int     `json:"total_mib_per_worker"`
	OpMiB           int     `json:"op_mib"`
	PublishMiB      int     `json:"publish_mib"`
	ChunkMiB        int     `json:"chunk_mib"`
	FinalizeOnClose bool    `json:"finalize_on_close"`
	Bytes           int64   `json:"bytes"`
	PublishedBytes  int64   `json:"published_bytes"`
	Seconds         float64 `json:"seconds"`
	WriteMiBps      float64 `json:"write_mibps"`
	PublishedMiBps  float64 `json:"published_mibps"`
	Writes          int     `json:"writes"`
	Flushes         int     `json:"flushes"`
	Closes          int     `json:"closes"`
	WriteP50Ms      float64 `json:"write_p50_ms"`
	WriteP99Ms      float64 `json:"write_p99_ms"`
	FlushP50Ms      float64 `json:"flush_p50_ms"`
	FlushP99Ms      float64 `json:"flush_p99_ms"`
	CloseP50Ms      float64 `json:"close_p50_ms"`
	CloseP99Ms      float64 `json:"close_p99_ms"`
	ProbeSamples    int     `json:"probe_samples"`
	ProbeP50Ms      float64 `json:"probe_p50_ms"`
	ProbeP99Ms      float64 `json:"probe_p99_ms"`
	Errors          string  `json:"errors,omitempty"`
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	var client *storage.Client
	if requiresStorageClient(cfg.modes) {
		var err error
		client, err = storage.NewGRPCClient(ctx, experimental.WithZonalBucketAPIs())
		if err != nil {
			fmt.Fprintf(os.Stderr, "storage.NewGRPCClient: %v\n", err)
			os.Exit(1)
		}
		defer client.Close()
	}

	rows := make([]resultRow, 0, len(cfg.modes)*len(cfg.workers)*len(cfg.opMiB))
	exitCode := 0
	for _, mode := range cfg.modes {
		for _, workers := range cfg.workers {
			for _, opMiB := range cfg.opMiB {
				row := runOnce(ctx, client, cfg, mode, workers, opMiB)
				rows = append(rows, row)
				if cfg.jsonOutput {
					encoded, _ := json.Marshal(row)
					fmt.Println(string(encoded))
				} else {
					printTextRow(row)
				}
				if row.Errors != "" {
					exitCode = 1
				}
			}
		}
	}

	if cfg.csvPath != "" {
		if err := writeCSV(cfg.csvPath, rows); err != nil {
			fmt.Fprintf(os.Stderr, "write csv: %v\n", err)
			os.Exit(1)
		}
	}
	os.Exit(exitCode)
}

func parseConfig() (config, error) {
	var cfg config
	var modes, workers, opMiB string

	flag.StringVar(&cfg.bucket, "bucket", "", "Rapid Storage bucket name")
	flag.StringVar(&cfg.prefix, "prefix", "rapidbench", "object prefix for benchmark data")
	flag.StringVar(&modes, "mode", "at-end", "comma-separated modes: at-end,interval,close-at-end,metadata-probe,tcp-probe,tiny-flush-probe")
	flag.StringVar(&workers, "workers", "16", "comma-separated worker counts")
	flag.StringVar(&opMiB, "op-mib", "4", "comma-separated append sizes in MiB")
	flag.IntVar(&cfg.totalMiB, "total-mib", 1024, "total MiB written per worker")
	flag.IntVar(&cfg.publishMiB, "publish-mib", 128, "flush/publish interval in MiB for interval mode")
	flag.IntVar(&cfg.chunkMiB, "chunk-mib", 0, "Cloud Storage writer chunk size in MiB; defaults to op-mib for each run")
	flag.BoolVar(&cfg.finalizeOnClose, "finalize-on-close", false, "finalize appendable objects when closing writers")
	flag.BoolVar(&cfg.cleanup, "cleanup", true, "delete benchmark objects after each run")
	flag.DurationVar(&cfg.timeout, "timeout", 30*time.Minute, "overall benchmark timeout")
	flag.StringVar(&cfg.csvPath, "csv", "", "optional CSV output path")
	flag.BoolVar(&cfg.jsonOutput, "json", false, "emit one JSON result object per run")
	flag.IntVar(&cfg.latencySamples, "latency-samples", 128, "latency probe samples per worker")
	flag.IntVar(&cfg.latencyObjectKiB, "latency-object-kib", 4, "object size in KiB for latency probe objects")
	flag.StringVar(&cfg.probeTarget, "probe-target", "storage.googleapis.com:443", "TCP target for tcp-probe mode")
	flag.Parse()

	var err error
	cfg.modes, err = parseModes(modes)
	if err != nil {
		return cfg, err
	}
	cfg.bucket = strings.TrimSpace(cfg.bucket)
	if cfg.bucket == "" && requiresStorageClient(cfg.modes) {
		return cfg, errors.New("--bucket is required")
	}
	cfg.prefix = strings.Trim(strings.TrimSpace(cfg.prefix), "/")
	if cfg.prefix == "" {
		return cfg, errors.New("--prefix must not be empty")
	}
	cfg.workers, err = parsePositiveIntList(workers, "--workers")
	if err != nil {
		return cfg, err
	}
	cfg.opMiB, err = parsePositiveIntList(opMiB, "--op-mib")
	if err != nil {
		return cfg, err
	}
	if cfg.totalMiB <= 0 {
		return cfg, errors.New("--total-mib must be positive")
	}
	if cfg.publishMiB <= 0 {
		return cfg, errors.New("--publish-mib must be positive")
	}
	if cfg.chunkMiB < 0 {
		return cfg, errors.New("--chunk-mib must be non-negative")
	}
	if cfg.timeout <= 0 {
		return cfg, errors.New("--timeout must be positive")
	}
	if cfg.latencySamples <= 0 {
		return cfg, errors.New("--latency-samples must be positive")
	}
	if cfg.latencyObjectKiB <= 0 {
		return cfg, errors.New("--latency-object-kib must be positive")
	}
	if strings.TrimSpace(cfg.probeTarget) == "" {
		return cfg, errors.New("--probe-target must not be empty")
	}
	return cfg, nil
}

func requiresStorageClient(modes []string) bool {
	for _, mode := range modes {
		if mode != "tcp-probe" {
			return true
		}
	}
	return false
}

func parseModes(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	modes := make([]string, 0, len(parts))
	for _, part := range parts {
		mode := strings.TrimSpace(part)
		switch mode {
		case "at-end", "interval", "close-at-end", "metadata-probe", "tcp-probe", "tiny-flush-probe":
			modes = append(modes, mode)
		case "":
		default:
			return nil, fmt.Errorf("unsupported --mode %q", mode)
		}
	}
	if len(modes) == 0 {
		return nil, errors.New("--mode must include at least one mode")
	}
	return modes, nil
}

func parsePositiveIntList(raw, name string) ([]int, error) {
	parts := strings.Split(raw, ",")
	values := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("%s contains non-positive integer %q", name, part)
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must include at least one value", name)
	}
	return values, nil
}

func runOnce(ctx context.Context, client *storage.Client, cfg config, mode string, workers int, opMiB int) resultRow {
	row := resultRow{
		Bucket:          cfg.bucket,
		Prefix:          cfg.prefix,
		Mode:            mode,
		Workers:         workers,
		TotalMiB:        cfg.totalMiB,
		OpMiB:           opMiB,
		PublishMiB:      cfg.publishMiB,
		ChunkMiB:        cfg.chunkMiB,
		FinalizeOnClose: cfg.finalizeOnClose,
	}
	if row.ChunkMiB == 0 {
		row.ChunkMiB = opMiB
	}
	switch mode {
	case "metadata-probe":
		return runMetadataProbe(ctx, client, cfg, row)
	case "tcp-probe":
		return runTCPProbe(ctx, cfg, row)
	case "tiny-flush-probe":
		return runTinyFlushProbe(ctx, client, cfg, row)
	}
	if cfg.totalMiB%opMiB != 0 {
		row.Errors = fmt.Sprintf("--total-mib=%d must be a multiple of --op-mib=%d", cfg.totalMiB, opMiB)
		return row
	}
	if mode == "interval" && cfg.publishMiB%opMiB != 0 {
		row.Errors = fmt.Sprintf("--publish-mib=%d must be a multiple of --op-mib=%d for interval mode", cfg.publishMiB, opMiB)
		return row
	}

	opBytes := opMiB * mib
	totalBytes := int64(cfg.totalMiB) * mib
	publishBytes := int64(cfg.publishMiB) * mib
	chunkBytes := row.ChunkMiB * mib
	runPrefix := fmt.Sprintf("%s/%s/%s-w%d-op%d", cfg.prefix, time.Now().UTC().Format("20060102T150405.000000000Z"), mode, workers, opMiB)
	payload := makePayload(opBytes)

	results := make(chan workerResult, workers)
	start := make(chan struct{})
	ready := make(chan struct{}, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		objectName := fmt.Sprintf("%s/worker-%04d", runPrefix, worker)
		go func(worker int, objectName string) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results <- runWorker(ctx, client, cfg.bucket, objectName, mode, payload, totalBytes, publishBytes, chunkBytes, cfg.finalizeOnClose)
		}(worker, objectName)
	}
	for i := 0; i < workers; i++ {
		<-ready
	}
	startedAt := time.Now()
	close(start)
	wg.Wait()
	close(results)

	var errs []string
	var writeLatencies, flushLatencies, closeLatencies, probeLatencies []time.Duration
	var maxPublishDone time.Time
	var objectNames []string
	for result := range results {
		row.Bytes += result.bytes
		row.PublishedBytes += result.publishedBytes
		objectNames = append(objectNames, result.objectNames...)
		if result.publishDone.After(maxPublishDone) {
			maxPublishDone = result.publishDone
		}
		if result.err != nil {
			errs = append(errs, result.err.Error())
		}
		writeLatencies = append(writeLatencies, result.writeLatencies...)
		flushLatencies = append(flushLatencies, result.flushLatencies...)
		closeLatencies = append(closeLatencies, result.closeLatencies...)
		probeLatencies = append(probeLatencies, result.probeLatencies...)
	}

	if maxPublishDone.IsZero() {
		maxPublishDone = time.Now()
	}
	row.Seconds = maxPublishDone.Sub(startedAt).Seconds()
	if row.Seconds > 0 {
		row.WriteMiBps = (float64(row.Bytes) / mib) / row.Seconds
		row.PublishedMiBps = (float64(row.PublishedBytes) / mib) / row.Seconds
	}
	row.Writes = len(writeLatencies)
	row.Flushes = len(flushLatencies)
	row.Closes = len(closeLatencies)
	row.WriteP50Ms = percentileMillis(writeLatencies, 50)
	row.WriteP99Ms = percentileMillis(writeLatencies, 99)
	row.FlushP50Ms = percentileMillis(flushLatencies, 50)
	row.FlushP99Ms = percentileMillis(flushLatencies, 99)
	row.CloseP50Ms = percentileMillis(closeLatencies, 50)
	row.CloseP99Ms = percentileMillis(closeLatencies, 99)
	row.ProbeSamples = len(probeLatencies)
	row.ProbeP50Ms = percentileMillis(probeLatencies, 50)
	row.ProbeP99Ms = percentileMillis(probeLatencies, 99)
	if row.PublishedBytes != row.Bytes && len(errs) == 0 {
		errs = append(errs, fmt.Sprintf("published_bytes=%d does not equal bytes=%d", row.PublishedBytes, row.Bytes))
	}
	if len(errs) > 0 {
		row.Errors = strings.Join(errs, "; ")
	}

	if cfg.cleanup {
		cleanupObjects(ctx, client, cfg.bucket, objectNames)
	}
	return row
}

func runWorker(ctx context.Context, client *storage.Client, bucket string, objectName string, mode string, payload []byte, totalBytes int64, publishBytes int64, chunkBytes int, finalizeOnClose bool) workerResult {
	result := workerResult{objectNames: []string{objectName}}
	writer := client.Bucket(bucket).Object(objectName).NewWriter(ctx)
	writer.Append = true
	writer.FinalizeOnClose = finalizeOnClose
	writer.ChunkSize = chunkBytes

	var nextPublish = publishBytes
	for result.bytes < totalBytes {
		started := time.Now()
		n, err := writer.Write(payload)
		result.writeLatencies = append(result.writeLatencies, time.Since(started))
		result.bytes += int64(n)
		if err != nil {
			result.err = fmt.Errorf("%s write at %d: %w", objectName, result.bytes, err)
			return result
		}
		if n != len(payload) {
			result.err = fmt.Errorf("%s short write: wrote %d want %d", objectName, n, len(payload))
			return result
		}
		if mode == "interval" && result.bytes >= nextPublish {
			size, err := flushWriter(writer, &result)
			if err != nil {
				result.err = fmt.Errorf("%s interval flush through %d: %w", objectName, nextPublish, err)
				return result
			}
			result.publishedBytes = size
			nextPublish += publishBytes
		}
	}

	switch mode {
	case "at-end":
		size, err := flushWriter(writer, &result)
		if err != nil {
			result.err = fmt.Errorf("%s final flush: %w", objectName, err)
			return result
		}
		result.publishedBytes = size
		result.publishDone = time.Now()
		err = closeWriter(writer, &result)
		if err != nil {
			result.err = fmt.Errorf("%s cleanup close after final flush: %w", objectName, err)
		}
	case "interval":
		if result.publishedBytes < result.bytes {
			size, err := flushWriter(writer, &result)
			if err != nil {
				result.err = fmt.Errorf("%s final tail flush: %w", objectName, err)
				return result
			}
			result.publishedBytes = size
		}
		result.publishDone = time.Now()
		err := closeWriter(writer, &result)
		if err != nil {
			result.err = fmt.Errorf("%s cleanup close after interval flushes: %w", objectName, err)
		}
	case "close-at-end":
		err := closeWriter(writer, &result)
		result.publishDone = time.Now()
		if err != nil {
			result.err = fmt.Errorf("%s close: %w", objectName, err)
			return result
		}
		result.publishedBytes = result.bytes
	default:
		result.err = fmt.Errorf("unsupported mode %q", mode)
	}
	return result
}

func runMetadataProbe(ctx context.Context, client *storage.Client, cfg config, row resultRow) resultRow {
	runPrefix := fmt.Sprintf("%s/%s/metadata-probe-w%d", cfg.prefix, time.Now().UTC().Format("20060102T150405.000000000Z"), row.Workers)
	payload := makePayload(cfg.latencyObjectKiB * 1024)
	results := make(chan workerResult, row.Workers)
	start := make(chan struct{})
	ready := make(chan struct{}, row.Workers)
	var wg sync.WaitGroup
	wg.Add(row.Workers)
	for worker := 0; worker < row.Workers; worker++ {
		objectName := fmt.Sprintf("%s/worker-%04d-probe", runPrefix, worker)
		go func(objectName string) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results <- runMetadataProbeWorker(ctx, client, cfg.bucket, objectName, payload, cfg.latencySamples)
		}(objectName)
	}
	for i := 0; i < row.Workers; i++ {
		<-ready
	}
	startedAt := time.Now()
	close(start)
	wg.Wait()
	close(results)
	return finishProbeRow(ctx, client, cfg, row, startedAt, results)
}

func runMetadataProbeWorker(ctx context.Context, client *storage.Client, bucket string, objectName string, payload []byte, samples int) workerResult {
	result := workerResult{objectNames: []string{objectName}}
	if err := createProbeObject(ctx, client, bucket, objectName, payload); err != nil {
		result.err = err
		return result
	}
	for i := 0; i < samples; i++ {
		started := time.Now()
		_, err := client.Bucket(bucket).Object(objectName).Attrs(ctx)
		result.probeLatencies = append(result.probeLatencies, time.Since(started))
		if err != nil {
			result.err = fmt.Errorf("%s attrs sample %d: %w", objectName, i, err)
			return result
		}
	}
	result.publishDone = time.Now()
	return result
}

func runTCPProbe(ctx context.Context, cfg config, row resultRow) resultRow {
	results := make(chan workerResult, row.Workers)
	start := make(chan struct{})
	ready := make(chan struct{}, row.Workers)
	var wg sync.WaitGroup
	wg.Add(row.Workers)
	for worker := 0; worker < row.Workers; worker++ {
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results <- runTCPProbeWorker(ctx, cfg.probeTarget, cfg.latencySamples)
		}()
	}
	for i := 0; i < row.Workers; i++ {
		<-ready
	}
	startedAt := time.Now()
	close(start)
	wg.Wait()
	close(results)
	return finishProbeRow(ctx, nil, cfg, row, startedAt, results)
}

func runTCPProbeWorker(ctx context.Context, target string, samples int) workerResult {
	var dialer net.Dialer
	result := workerResult{}
	addr, err := net.ResolveTCPAddr("tcp", target)
	if err != nil {
		result.err = fmt.Errorf("%s resolve: %w", target, err)
		return result
	}
	for i := 0; i < samples; i++ {
		started := time.Now()
		conn, err := dialer.DialContext(ctx, "tcp", addr.String())
		result.probeLatencies = append(result.probeLatencies, time.Since(started))
		if err != nil {
			result.err = fmt.Errorf("%s tcp sample %d: %w", target, i, err)
			return result
		}
		_ = conn.Close()
	}
	result.publishDone = time.Now()
	return result
}

func runTinyFlushProbe(ctx context.Context, client *storage.Client, cfg config, row resultRow) resultRow {
	runPrefix := fmt.Sprintf("%s/%s/tiny-flush-probe-w%d", cfg.prefix, time.Now().UTC().Format("20060102T150405.000000000Z"), row.Workers)
	payload := makePayload(cfg.latencyObjectKiB * 1024)
	results := make(chan workerResult, row.Workers)
	start := make(chan struct{})
	ready := make(chan struct{}, row.Workers)
	var wg sync.WaitGroup
	wg.Add(row.Workers)
	for worker := 0; worker < row.Workers; worker++ {
		workerPrefix := fmt.Sprintf("%s/worker-%04d", runPrefix, worker)
		go func(workerPrefix string) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results <- runTinyFlushProbeWorker(ctx, client, cfg.bucket, workerPrefix, payload, cfg.latencySamples)
		}(workerPrefix)
	}
	for i := 0; i < row.Workers; i++ {
		<-ready
	}
	startedAt := time.Now()
	close(start)
	wg.Wait()
	close(results)
	return finishProbeRow(ctx, client, cfg, row, startedAt, results)
}

func runTinyFlushProbeWorker(ctx context.Context, client *storage.Client, bucket string, workerPrefix string, payload []byte, samples int) workerResult {
	result := workerResult{}
	chunkBytes := max(256*1024, len(payload))
	for i := 0; i < samples; i++ {
		objectName := fmt.Sprintf("%s/sample-%04d", workerPrefix, i)
		result.objectNames = append(result.objectNames, objectName)
		writer := client.Bucket(bucket).Object(objectName).NewWriter(ctx)
		writer.Append = true
		writer.ChunkSize = chunkBytes
		started := time.Now()
		n, err := writer.Write(payload)
		result.writeLatencies = append(result.writeLatencies, time.Since(started))
		result.bytes += int64(n)
		if err != nil {
			result.err = fmt.Errorf("%s write: %w", objectName, err)
			return result
		}
		if n != len(payload) {
			result.err = fmt.Errorf("%s short write: wrote %d want %d", objectName, n, len(payload))
			return result
		}
		size, err := flushWriter(writer, &result)
		if err != nil {
			result.err = fmt.Errorf("%s flush: %w", objectName, err)
			return result
		}
		result.publishedBytes += size
		if err := closeWriter(writer, &result); err != nil {
			result.err = fmt.Errorf("%s close: %w", objectName, err)
			return result
		}
		result.publishDone = time.Now()
	}
	return result
}

func finishProbeRow(ctx context.Context, client *storage.Client, cfg config, row resultRow, startedAt time.Time, results <-chan workerResult) resultRow {
	var errs []string
	var writeLatencies, flushLatencies, closeLatencies, probeLatencies []time.Duration
	var maxPublishDone time.Time
	var objectNames []string
	for result := range results {
		row.Bytes += result.bytes
		row.PublishedBytes += result.publishedBytes
		objectNames = append(objectNames, result.objectNames...)
		if result.publishDone.After(maxPublishDone) {
			maxPublishDone = result.publishDone
		}
		if result.err != nil {
			errs = append(errs, result.err.Error())
		}
		writeLatencies = append(writeLatencies, result.writeLatencies...)
		flushLatencies = append(flushLatencies, result.flushLatencies...)
		closeLatencies = append(closeLatencies, result.closeLatencies...)
		probeLatencies = append(probeLatencies, result.probeLatencies...)
	}
	if maxPublishDone.IsZero() {
		maxPublishDone = time.Now()
	}
	row.Seconds = maxPublishDone.Sub(startedAt).Seconds()
	if row.Seconds > 0 {
		row.WriteMiBps = (float64(row.Bytes) / mib) / row.Seconds
		row.PublishedMiBps = (float64(row.PublishedBytes) / mib) / row.Seconds
	}
	row.Writes = len(writeLatencies)
	row.Flushes = len(flushLatencies)
	row.Closes = len(closeLatencies)
	row.WriteP50Ms = percentileMillis(writeLatencies, 50)
	row.WriteP99Ms = percentileMillis(writeLatencies, 99)
	row.FlushP50Ms = percentileMillis(flushLatencies, 50)
	row.FlushP99Ms = percentileMillis(flushLatencies, 99)
	row.CloseP50Ms = percentileMillis(closeLatencies, 50)
	row.CloseP99Ms = percentileMillis(closeLatencies, 99)
	row.ProbeSamples = len(probeLatencies)
	row.ProbeP50Ms = percentileMillis(probeLatencies, 50)
	row.ProbeP99Ms = percentileMillis(probeLatencies, 99)
	if len(errs) > 0 {
		row.Errors = strings.Join(errs, "; ")
	}
	if cfg.cleanup && client != nil {
		cleanupObjects(ctx, client, cfg.bucket, objectNames)
	}
	return row
}

func createProbeObject(ctx context.Context, client *storage.Client, bucket string, objectName string, payload []byte) error {
	writer := client.Bucket(bucket).Object(objectName).NewWriter(ctx)
	writer.Append = true
	writer.ChunkSize = max(256*1024, len(payload))
	if _, err := writer.Write(payload); err != nil {
		return fmt.Errorf("%s setup write: %w", objectName, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("%s setup close: %w", objectName, err)
	}
	return nil
}

func flushWriter(writer *storage.Writer, result *workerResult) (int64, error) {
	started := time.Now()
	size, err := writer.Flush()
	result.flushLatencies = append(result.flushLatencies, time.Since(started))
	return size, err
}

func closeWriter(writer *storage.Writer, result *workerResult) error {
	started := time.Now()
	err := writer.Close()
	result.closeLatencies = append(result.closeLatencies, time.Since(started))
	return err
}

func makePayload(bytes int) []byte {
	payload := make([]byte, bytes)
	for i := range payload {
		payload[i] = byte((i * 131) ^ (i >> 7))
	}
	return payload
}

func percentileMillis(values []time.Duration, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int((percentile / 100) * float64(len(sorted)-1))
	return float64(sorted[index].Microseconds()) / 1000.0
}

func cleanupObjects(ctx context.Context, client *storage.Client, bucket string, objectNames []string) {
	cleanupCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	for _, objectName := range objectNames {
		wg.Add(1)
		go func(objectName string) {
			defer wg.Done()
			_ = client.Bucket(bucket).Object(objectName).Delete(cleanupCtx)
		}(objectName)
	}
	wg.Wait()
}

func printTextRow(row resultRow) {
	fmt.Printf(
		"mode=%s workers=%d op_mib=%d total_mib=%d seconds=%.3f published_mibps=%.1f flush_p50_ms=%.2f flush_p99_ms=%.2f close_p50_ms=%.2f close_p99_ms=%.2f probe_samples=%d probe_p50_ms=%.2f probe_p99_ms=%.2f errors=%q\n",
		row.Mode,
		row.Workers,
		row.OpMiB,
		row.TotalMiB,
		row.Seconds,
		row.PublishedMiBps,
		row.FlushP50Ms,
		row.FlushP99Ms,
		row.CloseP50Ms,
		row.CloseP99Ms,
		row.ProbeSamples,
		row.ProbeP50Ms,
		row.ProbeP99Ms,
		row.Errors,
	)
}

func writeCSV(path string, rows []resultRow) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(csvHeader()); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(csvFields(row)); err != nil {
			return err
		}
	}
	return writer.Error()
}

func csvHeader() []string {
	return []string{
		"bucket",
		"prefix",
		"mode",
		"workers",
		"total_mib_per_worker",
		"op_mib",
		"publish_mib",
		"chunk_mib",
		"finalize_on_close",
		"bytes",
		"published_bytes",
		"seconds",
		"write_mibps",
		"published_mibps",
		"writes",
		"flushes",
		"closes",
		"write_p50_ms",
		"write_p99_ms",
		"flush_p50_ms",
		"flush_p99_ms",
		"close_p50_ms",
		"close_p99_ms",
		"probe_samples",
		"probe_p50_ms",
		"probe_p99_ms",
		"errors",
	}
}

func csvFields(row resultRow) []string {
	return []string{
		row.Bucket,
		row.Prefix,
		row.Mode,
		strconv.Itoa(row.Workers),
		strconv.Itoa(row.TotalMiB),
		strconv.Itoa(row.OpMiB),
		strconv.Itoa(row.PublishMiB),
		strconv.Itoa(row.ChunkMiB),
		strconv.FormatBool(row.FinalizeOnClose),
		strconv.FormatInt(row.Bytes, 10),
		strconv.FormatInt(row.PublishedBytes, 10),
		fmt.Sprintf("%.6f", row.Seconds),
		fmt.Sprintf("%.3f", row.WriteMiBps),
		fmt.Sprintf("%.3f", row.PublishedMiBps),
		strconv.Itoa(row.Writes),
		strconv.Itoa(row.Flushes),
		strconv.Itoa(row.Closes),
		fmt.Sprintf("%.3f", row.WriteP50Ms),
		fmt.Sprintf("%.3f", row.WriteP99Ms),
		fmt.Sprintf("%.3f", row.FlushP50Ms),
		fmt.Sprintf("%.3f", row.FlushP99Ms),
		fmt.Sprintf("%.3f", row.CloseP50Ms),
		fmt.Sprintf("%.3f", row.CloseP99Ms),
		strconv.Itoa(row.ProbeSamples),
		fmt.Sprintf("%.3f", row.ProbeP50Ms),
		fmt.Sprintf("%.3f", row.ProbeP99Ms),
		row.Errors,
	}
}
