fn partitioned_file_index(
    worker: u64,
    op_index: u64,
    concurrency: usize,
    files_len: usize,
) -> usize {
    if files_len == 0 {
        return 0;
    }
    if concurrency == 0 {
        return op_index as usize % files_len;
    }
    if concurrency > files_len {
        return worker as usize % files_len;
    }

    let worker = worker as usize % concurrency;
    let base = files_len * worker / concurrency;
    let next_base = files_len * (worker + 1) / concurrency;
    let span = next_base.saturating_sub(base).max(1);
    base + (op_index as usize % span)
}

fn make_payload(bytes: usize) -> Vec<u8> {
    (0..bytes)
        .map(|index| (index as u8).wrapping_mul(31))
        .collect()
}

#[derive(Debug, Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn below(&mut self, upper: u64) -> u64 {
        if upper == 0 { 0 } else { self.next() % upper }
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockBatchOpProfile {
    total_nanos: u64,
    batch_operation_count: u64,
    collapsed_range_count: u64,
    requested_bytes: u64,
    committed_bytes: u64,
}

#[derive(Debug)]
struct WorkerReport {
    attempts: u64,
    successes: u64,
    errors: u64,
    bytes: u64,
    durable_bytes: u64,
    published_bytes: u64,
    max_latency_nanos: u64,
    latency_seen: u64,
    latencies: Vec<u64>,
    sample_limit: usize,
    block_batch_profiles: Vec<BlockBatchOpProfile>,
}

impl WorkerReport {
    fn new(sample_limit: usize) -> Self {
        Self {
            attempts: 0,
            successes: 0,
            errors: 0,
            bytes: 0,
            durable_bytes: 0,
            published_bytes: 0,
            max_latency_nanos: 0,
            latency_seen: 0,
            latencies: Vec::with_capacity(sample_limit.min(1024)),
            sample_limit,
            block_batch_profiles: Vec::new(),
        }
    }

    fn record(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        progress: OpProgress,
        success: bool,
        rng: &mut Lcg,
    ) {
        self.attempts = self.attempts.saturating_add(1);
        self.latency_seen = self.latency_seen.saturating_add(1);
        self.max_latency_nanos = self.max_latency_nanos.max(latency_nanos);
        if success {
            self.successes = self.successes.saturating_add(1);
            self.bytes = self.bytes.saturating_add(bytes);
            self.durable_bytes = self
                .durable_bytes
                .saturating_add(progress.durable_bytes);
            self.published_bytes = self
                .published_bytes
                .saturating_add(progress.published_bytes);
            if let Some(profile) = progress.block_batch_profile {
                self.block_batch_profiles.push(profile);
            }
        } else {
            self.errors = self.errors.saturating_add(1);
        }

        if self.latencies.len() < self.sample_limit {
            self.latencies.push(latency_nanos);
        } else {
            let replacement = rng.below(self.latency_seen) as usize;
            if replacement < self.sample_limit {
                self.latencies[replacement] = latency_nanos;
            }
        }
    }
}

#[derive(Debug)]
struct BenchReport {
    workload: Workload,
    provider: ProviderKind,
    durability: DurabilityMode,
    rtt_us: u128,
    serial_rtts: u32,
    concurrency: usize,
    op_size: usize,
    elapsed: Duration,
    attempts: u64,
    successes: u64,
    errors: u64,
    bytes: u64,
    durable_bytes: u64,
    published_bytes: u64,
    block_batch_profiles: Vec<BlockBatchOpProfile>,
    p50_nanos: u64,
    p90_nanos: u64,
    p99_nanos: u64,
    p999_nanos: u64,
    max_nanos: u64,
    samples: usize,
}

impl BenchReport {
    fn from_workers(elapsed: Duration, workers: Vec<WorkerReport>) -> Self {
        let mut attempts = 0_u64;
        let mut successes = 0_u64;
        let mut errors = 0_u64;
        let mut bytes = 0_u64;
        let mut durable_bytes = 0_u64;
        let mut published_bytes = 0_u64;
        let mut max_nanos = 0_u64;
        let mut samples = Vec::new();
        let mut block_batch_profiles = Vec::new();

        for worker in workers {
            attempts = attempts.saturating_add(worker.attempts);
            successes = successes.saturating_add(worker.successes);
            errors = errors.saturating_add(worker.errors);
            bytes = bytes.saturating_add(worker.bytes);
            durable_bytes = durable_bytes.saturating_add(worker.durable_bytes);
            published_bytes = published_bytes.saturating_add(worker.published_bytes);
            max_nanos = max_nanos.max(worker.max_latency_nanos);
            samples.extend(worker.latencies);
            block_batch_profiles.extend(worker.block_batch_profiles);
        }
        samples.sort_unstable();

        Self {
            workload: Workload::BlockWrite4k,
            provider: ProviderKind::Local,
            durability: DurabilityMode::Acknowledged,
            rtt_us: 0,
            serial_rtts: 0,
            concurrency: 0,
            op_size: 0,
            elapsed,
            attempts,
            successes,
            errors,
            bytes,
            durable_bytes,
            published_bytes,
            block_batch_profiles,
            p50_nanos: percentile(&samples, 0.50),
            p90_nanos: percentile(&samples, 0.90),
            p99_nanos: percentile(&samples, 0.99),
            p999_nanos: percentile(&samples, 0.999),
            max_nanos,
            samples: samples.len(),
        }
    }

    fn print_csv(&self) {
        let seconds = self.elapsed.as_secs_f64();
        let success_iops = self.successes as f64 / seconds;
        let attempt_iops = self.attempts as f64 / seconds;
        let mbps = self.bytes as f64 / seconds / 1_000_000.0;
        let durable_mbps = self.durable_bytes as f64 / seconds / 1_000_000.0;
        let published_mbps = self.published_bytes as f64 / seconds / 1_000_000.0;
        println!(
            "{},{},{},{},{},{},{},{:.6},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{}",
            self.workload.name(),
            self.provider,
            self.durability,
            self.rtt_us,
            self.serial_rtts,
            self.concurrency,
            self.op_size,
            seconds,
            self.attempts,
            self.successes,
            self.errors,
            success_iops,
            attempt_iops,
            mbps,
            durable_mbps,
            published_mbps,
            self.durable_bytes,
            self.published_bytes,
            nanos_to_micros(self.p50_nanos),
            nanos_to_micros(self.p90_nanos),
            nanos_to_micros(self.p99_nanos),
            nanos_to_micros(self.p999_nanos),
            nanos_to_micros(self.max_nanos),
            self.samples
        );
    }
}

fn percentile(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn nanos_to_micros(nanos: u64) -> f64 {
    nanos as f64 / 1000.0
}
