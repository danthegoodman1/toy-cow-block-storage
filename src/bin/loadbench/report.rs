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
    commit_nanos: u64,
    flush_device_nanos: u64,
    batch_operation_count: u64,
    collapsed_range_count: u64,
    requested_bytes: u64,
    committed_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct NativeFileBatchOpProfile {
    total_nanos: u64,
    commit_nanos: u64,
    batch_operation_count: u64,
    requested_bytes: u64,
    committed_range_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum LatencyClass {
    StreamAppend,
    StreamPublish,
}

/// Upper bound on distinct error groups a report retains per variant family.
///
/// Raw error strings carry offsets, paths and lengths, so grouping on them
/// would grow without limit under a run that produces hundreds of thousands
/// of errors. The key is bounded instead, and anything past the cap folds
/// into a per-variant overflow bucket, so the tally can hold at most
/// `ERROR_GROUP_LIMIT + ErrorKindLabel::VARIANTS.len()` groups.
const ERROR_GROUP_LIMIT: usize = 64;

/// Upper bound on a retained first-error sample. The errno is captured
/// separately in `ErrorGroupKey::os_error`, so truncation here never costs
/// the diagnosis.
const ERROR_SAMPLE_MAX_BYTES: usize = 512;

/// Upper bound on the normalized message prefix used to separate errno-less
/// errors. Long enough to tell the direct-I/O write loop's three distinct
/// errno-less failures apart, short enough to stay a bounded key.
const ERROR_PREFIX_MAX_BYTES: usize = 96;

const ERROR_OVERFLOW_SAMPLE: &str = "(overflow bucket: distinct error-group cap reached)";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ErrorKindLabel {
    InvalidArgument,
    NotFound,
    Conflict,
    Unavailable,
    Corrupt,
    Unsupported,
}

impl ErrorKindLabel {
    /// The `StorageError` variants, in the order their fixed `matrix.csv`
    /// count columns appear.
    const VARIANTS: [Self; 6] = [
        Self::InvalidArgument,
        Self::NotFound,
        Self::Conflict,
        Self::Unavailable,
        Self::Corrupt,
        Self::Unsupported,
    ];

    fn of(error: &StorageError) -> Self {
        match error {
            StorageError::InvalidArgument { .. } => Self::InvalidArgument,
            StorageError::NotFound { .. } => Self::NotFound,
            StorageError::Conflict { .. } => Self::Conflict,
            StorageError::Unavailable { .. } => Self::Unavailable,
            StorageError::Corrupt { .. } => Self::Corrupt,
            StorageError::Unsupported { .. } => Self::Unsupported,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid-argument",
            Self::NotFound => "not-found",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
            Self::Corrupt => "corrupt",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Bounded grouping key.
///
/// `overflow` sorts after the real groups within a variant and, crucially,
/// keeps the variant: the six `errors_*` columns therefore always sum to the
/// `errors` column, whether or not the cap was hit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ErrorGroupKey {
    kind: ErrorKindLabel,
    overflow: bool,
    os_error: Option<i32>,
    prefix: String,
}

#[derive(Debug, Clone)]
struct ErrorGroup {
    count: u64,
    /// The sample from the first group occurrence in merge order, i.e. the
    /// lowest-indexed worker holding this group. Deterministic, but not
    /// necessarily the chronologically first error of the run.
    first_sample: String,
}

#[derive(Debug, Default)]
struct ErrorTally {
    groups: BTreeMap<ErrorGroupKey, ErrorGroup>,
}

impl ErrorTally {
    /// Error path only. Allocation and formatting here are fine; the success
    /// path never reaches this function.
    fn record(&mut self, error: &StorageError) {
        let message = error.to_string();
        let os_error = parse_os_error_code(&message);
        let key = ErrorGroupKey {
            kind: ErrorKindLabel::of(error),
            overflow: false,
            // With an errno, the errno is the discriminator; splitting
            // further by call site would fragment the count the GCP trip
            // exists to read. Without one, the normalized message prefix is
            // the only thing separating distinct failures — the direct-I/O
            // write loop alone returns three errno-less `Unavailable`s
            // ("made no progress", "reported too many bytes", "repeatedly
            // returned unaligned short writes") and telling them apart is
            // the point of the exercise.
            prefix: if os_error.is_some() {
                String::new()
            } else {
                normalize_error_prefix(&message)
            },
            os_error,
        };
        self.add(key, 1, &message);
    }

    fn add(&mut self, key: ErrorGroupKey, count: u64, sample: &str) {
        if let Some(group) = self.groups.get_mut(&key) {
            group.count = group.count.saturating_add(count);
            return;
        }
        let key = if self.groups.len() >= ERROR_GROUP_LIMIT {
            ErrorGroupKey {
                kind: key.kind,
                overflow: true,
                os_error: None,
                prefix: String::new(),
            }
        } else {
            key
        };
        let overflow = key.overflow;
        self.groups
            .entry(key)
            .and_modify(|group| group.count = group.count.saturating_add(count))
            .or_insert_with(|| ErrorGroup {
                count,
                first_sample: if overflow {
                    ERROR_OVERFLOW_SAMPLE.to_string()
                } else {
                    truncate_error_sample(sample)
                },
            });
    }

    fn merge(&mut self, other: Self) {
        for (key, group) in other.groups {
            self.add(key, group.count, &group.first_sample);
        }
    }

    /// Total for one variant, overflow included. Summing this over
    /// `VARIANTS` always reproduces the `errors` column.
    fn count_for_kind(&self, kind: ErrorKindLabel) -> u64 {
        self.groups
            .iter()
            .filter(|(key, _)| key.kind == kind)
            .fold(0_u64, |total, (_, group)| total.saturating_add(group.count))
    }

    /// Errno of the highest-count group carrying one, with its count.
    /// `(0, 0)` when no group has an errno. Ties resolve to the
    /// lowest-ordered key, so the answer is deterministic.
    ///
    /// This is duplicated out of the sidecar into `matrix.csv` on purpose:
    /// the errno is the one fact the GCP trip exists to obtain, and
    /// `matrix.csv` is the artifact with positional consumers and a
    /// historical series behind it.
    fn top_os_error(&self) -> (i32, u64) {
        self.groups
            .iter()
            .filter_map(|(key, group)| key.os_error.map(|code| (code, group.count)))
            .fold((0_i32, 0_u64), |best, (code, count)| {
                if count > best.1 { (code, count) } else { best }
            })
    }
}

/// Recover the errno from an `io::Error` that `fs_error` stringified.
/// `io::Error`'s `Display` for an OS error ends in `(os error N)`.
fn parse_os_error_code(message: &str) -> Option<i32> {
    const MARKER: &str = "(os error ";
    let start = message.rfind(MARKER)? + MARKER.len();
    let rest = message.get(start..)?;
    let end = rest.find(')')?;
    rest.get(..end)?.trim().parse::<i32>().ok()
}

/// Collapse the variable parts of an error message into a bounded key.
///
/// Digit runs become a single `#`, so offsets, lengths and block numbers do
/// not each mint a group; the result is then truncated. Distinct static
/// messages that share a 96-byte prefix would still merge, which the group
/// cap makes safe rather than correct — the sidecar's sample shows what
/// landed in the group.
///
/// Digit collapse also merges messages that differ only in a type width:
/// `"invalid u16"` and `"invalid u128"` both normalize to `"invalid u#"`,
/// as do the `overflows u32`/`u64` guards. That is confined to the
/// `InvalidArgument` and `Corrupt` width checks and cannot obscure the
/// direct-I/O question this instrumentation exists to answer — those
/// failures are `Unavailable` and either carry an errno (in which case the
/// prefix is unused) or are the three digit-free write-loop messages.
fn normalize_error_prefix(message: &str) -> String {
    let mut prefix = String::with_capacity(ERROR_PREFIX_MAX_BYTES);
    let mut previous_was_digit = false;
    for character in message.chars() {
        let is_digit = character.is_ascii_digit();
        if is_digit && previous_was_digit {
            continue;
        }
        previous_was_digit = is_digit;
        let character = if is_digit {
            '#'
        } else if character.is_control() {
            ' '
        } else {
            character
        };
        if prefix.len() + character.len_utf8() > ERROR_PREFIX_MAX_BYTES {
            break;
        }
        prefix.push(character);
    }
    prefix
}

/// Bound the retained sample and flatten control characters so one group is
/// always one CSV line.
fn truncate_error_sample(message: &str) -> String {
    let mut sample = String::with_capacity(message.len().min(ERROR_SAMPLE_MAX_BYTES));
    for character in message.chars() {
        let character = if character.is_control() {
            ' '
        } else {
            character
        };
        if sample.len() + character.len_utf8() > ERROR_SAMPLE_MAX_BYTES {
            break;
        }
        sample.push(character);
    }
    sample
}

/// RFC4180 quoting: wrap in double quotes, double any embedded quote. The
/// sidecar carries free-text error strings, so every sample field goes
/// through this — otherwise one comma turns into a shifted column.
fn csv_quote(field: &str) -> String {
    let mut quoted = String::with_capacity(field.len() + 2);
    quoted.push('"');
    for character in field.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

#[derive(Debug)]
struct WorkerReport {
    attempts: u64,
    successes: u64,
    errors: u64,
    errors_by_kind: ErrorTally,
    bytes: u64,
    durable_bytes: u64,
    published_bytes: u64,
    max_latency_nanos: u64,
    latency_seen: u64,
    latencies: Vec<u64>,
    stream_append_max_latency_nanos: u64,
    stream_append_latency_seen: u64,
    stream_append_latencies: Vec<u64>,
    stream_publish_max_latency_nanos: u64,
    stream_publish_latency_seen: u64,
    stream_publish_latencies: Vec<u64>,
    stream_final_drain_max_latency_nanos: u64,
    stream_final_drain_latency_seen: u64,
    stream_final_drain_latencies: Vec<u64>,
    stream_barrier_wait_max_latency_nanos: u64,
    stream_barrier_wait_latency_seen: u64,
    stream_barrier_wait_latencies: Vec<u64>,
    stream_append_phase_max_nanos: u64,
    stream_boundary_phase_max_nanos: u64,
    sample_limit: usize,
    block_batch_profiles: Vec<BlockBatchOpProfile>,
    native_file_batch_profiles: Vec<NativeFileBatchOpProfile>,
}

impl WorkerReport {
    fn new(sample_limit: usize) -> Self {
        Self {
            attempts: 0,
            successes: 0,
            errors: 0,
            errors_by_kind: ErrorTally::default(),
            bytes: 0,
            durable_bytes: 0,
            published_bytes: 0,
            max_latency_nanos: 0,
            latency_seen: 0,
            latencies: Vec::with_capacity(sample_limit.min(1024)),
            stream_append_max_latency_nanos: 0,
            stream_append_latency_seen: 0,
            stream_append_latencies: Vec::with_capacity(sample_limit.min(1024)),
            stream_publish_max_latency_nanos: 0,
            stream_publish_latency_seen: 0,
            stream_publish_latencies: Vec::with_capacity(sample_limit.min(1024)),
            stream_final_drain_max_latency_nanos: 0,
            stream_final_drain_latency_seen: 0,
            stream_final_drain_latencies: Vec::with_capacity(sample_limit.min(1024)),
            stream_barrier_wait_max_latency_nanos: 0,
            stream_barrier_wait_latency_seen: 0,
            stream_barrier_wait_latencies: Vec::with_capacity(sample_limit.min(1024)),
            stream_append_phase_max_nanos: 0,
            stream_boundary_phase_max_nanos: 0,
            sample_limit,
            block_batch_profiles: Vec::new(),
            native_file_batch_profiles: Vec::new(),
        }
    }

    fn record_stream_append(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        progress: OpProgress,
        success: bool,
        rng: &mut Lcg,
    ) {
        self.record_classified(
            latency_nanos,
            bytes,
            progress,
            success,
            Some(LatencyClass::StreamAppend),
            rng,
        );
    }

    fn record_stream_publish(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        progress: OpProgress,
        success: bool,
        rng: &mut Lcg,
    ) {
        self.record_classified(
            latency_nanos,
            bytes,
            progress,
            success,
            Some(LatencyClass::StreamPublish),
            rng,
        );
    }

    fn record_stream_final_drain(&mut self, latency_nanos: u64, rng: &mut Lcg) {
        self.stream_final_drain_latency_seen =
            self.stream_final_drain_latency_seen.saturating_add(1);
        self.stream_final_drain_max_latency_nanos =
            self.stream_final_drain_max_latency_nanos.max(latency_nanos);
        sample_latency(
            &mut self.stream_final_drain_latencies,
            self.sample_limit,
            self.stream_final_drain_latency_seen,
            latency_nanos,
            rng,
        );
    }

    fn record_stream_barrier_wait(&mut self, latency_nanos: u64, rng: &mut Lcg) {
        self.stream_barrier_wait_latency_seen =
            self.stream_barrier_wait_latency_seen.saturating_add(1);
        self.stream_barrier_wait_max_latency_nanos =
            self.stream_barrier_wait_max_latency_nanos.max(latency_nanos);
        sample_latency(
            &mut self.stream_barrier_wait_latencies,
            self.sample_limit,
            self.stream_barrier_wait_latency_seen,
            latency_nanos,
            rng,
        );
    }

    fn record_stream_phases(&mut self, append_phase_nanos: u64, boundary_phase_nanos: u64) {
        self.stream_append_phase_max_nanos =
            self.stream_append_phase_max_nanos.max(append_phase_nanos);
        self.stream_boundary_phase_max_nanos = self
            .stream_boundary_phase_max_nanos
            .max(boundary_phase_nanos);
    }

    fn record(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        progress: OpProgress,
        success: bool,
        rng: &mut Lcg,
    ) {
        self.record_classified(latency_nanos, bytes, progress, success, None, rng);
    }

    /// Classify a failed operation. Kept separate from `record` so the
    /// success path pays only the branch that decides not to call this —
    /// no allocation, formatting or map lookup per successful op.
    fn record_error(&mut self, error: &StorageError) {
        self.errors_by_kind.record(error);
    }

    fn record_classified(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        progress: OpProgress,
        success: bool,
        class: Option<LatencyClass>,
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
            if let Some(profile) = progress.native_file_batch_profile {
                self.native_file_batch_profiles.push(profile);
            }
        } else {
            self.errors = self.errors.saturating_add(1);
        }

        sample_latency(
            &mut self.latencies,
            self.sample_limit,
            self.latency_seen,
            latency_nanos,
            rng,
        );

        match class {
            Some(LatencyClass::StreamAppend) => {
                self.stream_append_latency_seen =
                    self.stream_append_latency_seen.saturating_add(1);
                self.stream_append_max_latency_nanos =
                    self.stream_append_max_latency_nanos.max(latency_nanos);
                sample_latency(
                    &mut self.stream_append_latencies,
                    self.sample_limit,
                    self.stream_append_latency_seen,
                    latency_nanos,
                    rng,
                );
            }
            Some(LatencyClass::StreamPublish) => {
                self.stream_publish_latency_seen =
                    self.stream_publish_latency_seen.saturating_add(1);
                self.stream_publish_max_latency_nanos =
                    self.stream_publish_max_latency_nanos.max(latency_nanos);
                sample_latency(
                    &mut self.stream_publish_latencies,
                    self.sample_limit,
                    self.stream_publish_latency_seen,
                    latency_nanos,
                    rng,
                );
            }
            None => {}
        }
    }
}

fn sample_latency(
    samples: &mut Vec<u64>,
    sample_limit: usize,
    latency_seen: u64,
    latency_nanos: u64,
    rng: &mut Lcg,
) {
    if samples.len() < sample_limit {
        samples.push(latency_nanos);
    } else {
        let replacement = rng.below(latency_seen) as usize;
        if replacement < sample_limit {
            samples[replacement] = latency_nanos;
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
    errors_by_kind: ErrorTally,
    bytes: u64,
    durable_bytes: u64,
    published_bytes: u64,
    block_batch_profiles: Vec<BlockBatchOpProfile>,
    native_file_batch_profiles: Vec<NativeFileBatchOpProfile>,
    append_log_profiles: Vec<AppendLogMicrobenchProfile>,
    p50_nanos: u64,
    p90_nanos: u64,
    p99_nanos: u64,
    p999_nanos: u64,
    max_nanos: u64,
    samples: usize,
    stream_append_p50_nanos: u64,
    stream_append_p90_nanos: u64,
    stream_append_p99_nanos: u64,
    stream_append_p999_nanos: u64,
    stream_append_max_nanos: u64,
    stream_append_samples: usize,
    stream_publish_p50_nanos: u64,
    stream_publish_p90_nanos: u64,
    stream_publish_p99_nanos: u64,
    stream_publish_p999_nanos: u64,
    stream_publish_max_nanos: u64,
    stream_publish_samples: usize,
    stream_final_drain_p50_nanos: u64,
    stream_final_drain_p99_nanos: u64,
    stream_final_drain_max_nanos: u64,
    stream_final_drain_samples: usize,
    stream_append_phase_nanos: u64,
    stream_boundary_phase_nanos: u64,
    stream_barrier_wait_p50_nanos: u64,
    stream_barrier_wait_p99_nanos: u64,
    stream_barrier_wait_max_nanos: u64,
    stream_barrier_wait_samples: usize,
}

impl BenchReport {
    fn csv_header() -> &'static str {
        // APPEND ONLY. These columns are consumed positionally by
        // `score_trip.sh` (`$14` mbps, `$21` p99) and by the GCP harness
        // scripts; inserting anywhere but the end silently corrupts every
        // historical comparison. The trailing `errors_*` columns are fixed
        // shape and numeric on purpose — free-text error samples go to the
        // `*.errors.csv` sidecar so this row never needs quoting.
        "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,seconds,attempts,successes,errors,success_iops,attempt_iops,mbps,durable_mbps,published_mbps,durable_bytes,published_bytes,p50_us,p90_us,p99_us,p999_us,max_us,samples,stream_append_p50_us,stream_append_p90_us,stream_append_p99_us,stream_append_p999_us,stream_append_max_us,stream_append_samples,stream_publish_p50_us,stream_publish_p90_us,stream_publish_p99_us,stream_publish_p999_us,stream_publish_max_us,stream_publish_samples,stream_final_drain_p50_us,stream_final_drain_p99_us,stream_final_drain_max_us,stream_final_drain_samples,stream_append_phase_seconds,stream_boundary_phase_seconds,stream_barrier_wait_p50_us,stream_barrier_wait_p99_us,stream_barrier_wait_max_us,stream_barrier_wait_samples,errors_invalid_argument,errors_not_found,errors_conflict,errors_unavailable,errors_corrupt,errors_unsupported,errors_top_os_error,errors_top_os_error_count"
    }

    /// Sidecar written next to `matrix.csv`. Carries the errno and the
    /// verbatim first-error sample, which cannot live in `matrix.csv`
    /// without making that file's column count variable.
    fn error_csv_header() -> &'static str {
        "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,error_kind,os_error,count,first_sample"
    }

    fn from_workers(elapsed: Duration, workers: Vec<WorkerReport>) -> Self {
        let mut attempts = 0_u64;
        let mut successes = 0_u64;
        let mut errors = 0_u64;
        let mut errors_by_kind = ErrorTally::default();
        let mut bytes = 0_u64;
        let mut durable_bytes = 0_u64;
        let mut published_bytes = 0_u64;
        let mut max_nanos = 0_u64;
        let mut samples = Vec::new();
        let mut stream_append_max_nanos = 0_u64;
        let mut stream_append_samples = Vec::new();
        let mut stream_publish_max_nanos = 0_u64;
        let mut stream_publish_samples = Vec::new();
        let mut stream_final_drain_max_nanos = 0_u64;
        let mut stream_final_drain_samples = Vec::new();
        let mut stream_barrier_wait_max_nanos = 0_u64;
        let mut stream_barrier_wait_samples = Vec::new();
        let mut stream_append_phase_nanos = 0_u64;
        let mut stream_boundary_phase_nanos = 0_u64;
        let mut block_batch_profiles = Vec::new();
        let mut native_file_batch_profiles = Vec::new();

        for worker in workers {
            attempts = attempts.saturating_add(worker.attempts);
            successes = successes.saturating_add(worker.successes);
            errors = errors.saturating_add(worker.errors);
            errors_by_kind.merge(worker.errors_by_kind);
            bytes = bytes.saturating_add(worker.bytes);
            durable_bytes = durable_bytes.saturating_add(worker.durable_bytes);
            published_bytes = published_bytes.saturating_add(worker.published_bytes);
            max_nanos = max_nanos.max(worker.max_latency_nanos);
            samples.extend(worker.latencies);
            stream_append_max_nanos =
                stream_append_max_nanos.max(worker.stream_append_max_latency_nanos);
            stream_append_samples.extend(worker.stream_append_latencies);
            stream_publish_max_nanos =
                stream_publish_max_nanos.max(worker.stream_publish_max_latency_nanos);
            stream_publish_samples.extend(worker.stream_publish_latencies);
            stream_final_drain_max_nanos = stream_final_drain_max_nanos
                .max(worker.stream_final_drain_max_latency_nanos);
            stream_final_drain_samples.extend(worker.stream_final_drain_latencies);
            stream_barrier_wait_max_nanos = stream_barrier_wait_max_nanos
                .max(worker.stream_barrier_wait_max_latency_nanos);
            stream_barrier_wait_samples.extend(worker.stream_barrier_wait_latencies);
            stream_append_phase_nanos =
                stream_append_phase_nanos.max(worker.stream_append_phase_max_nanos);
            stream_boundary_phase_nanos =
                stream_boundary_phase_nanos.max(worker.stream_boundary_phase_max_nanos);
            block_batch_profiles.extend(worker.block_batch_profiles);
            native_file_batch_profiles.extend(worker.native_file_batch_profiles);
        }
        samples.sort_unstable();
        stream_append_samples.sort_unstable();
        stream_publish_samples.sort_unstable();
        stream_final_drain_samples.sort_unstable();
        stream_barrier_wait_samples.sort_unstable();

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
            errors_by_kind,
            bytes,
            durable_bytes,
            published_bytes,
            block_batch_profiles,
            native_file_batch_profiles,
            append_log_profiles: Vec::new(),
            p50_nanos: percentile(&samples, 0.50),
            p90_nanos: percentile(&samples, 0.90),
            p99_nanos: percentile(&samples, 0.99),
            p999_nanos: percentile(&samples, 0.999),
            max_nanos,
            samples: samples.len(),
            stream_append_p50_nanos: percentile(&stream_append_samples, 0.50),
            stream_append_p90_nanos: percentile(&stream_append_samples, 0.90),
            stream_append_p99_nanos: percentile(&stream_append_samples, 0.99),
            stream_append_p999_nanos: percentile(&stream_append_samples, 0.999),
            stream_append_max_nanos,
            stream_append_samples: stream_append_samples.len(),
            stream_publish_p50_nanos: percentile(&stream_publish_samples, 0.50),
            stream_publish_p90_nanos: percentile(&stream_publish_samples, 0.90),
            stream_publish_p99_nanos: percentile(&stream_publish_samples, 0.99),
            stream_publish_p999_nanos: percentile(&stream_publish_samples, 0.999),
            stream_publish_max_nanos,
            stream_publish_samples: stream_publish_samples.len(),
            stream_final_drain_p50_nanos: percentile(&stream_final_drain_samples, 0.50),
            stream_final_drain_p99_nanos: percentile(&stream_final_drain_samples, 0.99),
            stream_final_drain_max_nanos,
            stream_final_drain_samples: stream_final_drain_samples.len(),
            stream_append_phase_nanos,
            stream_boundary_phase_nanos,
            stream_barrier_wait_p50_nanos: percentile(&stream_barrier_wait_samples, 0.50),
            stream_barrier_wait_p99_nanos: percentile(&stream_barrier_wait_samples, 0.99),
            stream_barrier_wait_max_nanos,
            stream_barrier_wait_samples: stream_barrier_wait_samples.len(),
        }
    }

    fn csv_row(&self) -> String {
        let seconds = self.elapsed.as_secs_f64();
        let success_iops = self.successes as f64 / seconds;
        let attempt_iops = self.attempts as f64 / seconds;
        let mbps = self.bytes as f64 / seconds / 1_000_000.0;
        let durable_mbps = self.durable_bytes as f64 / seconds / 1_000_000.0;
        let published_mbps = self.published_bytes as f64 / seconds / 1_000_000.0;
        // Fixed-shape numeric tail, in `ErrorKindLabel::VARIANTS` order so
        // the row cannot drift from the header's `errors_*` columns. Every
        // value here is a `u64`/`i32`, so this tail can never introduce a
        // comma or a quote into a row that positional consumers parse with
        // a quote-blind `awk -F,`.
        let mut error_kind_counts = String::new();
        for kind in ErrorKindLabel::VARIANTS {
            error_kind_counts.push(',');
            error_kind_counts.push_str(&self.errors_by_kind.count_for_kind(kind).to_string());
        }
        let (top_os_error, top_os_error_count) = self.errors_by_kind.top_os_error();
        error_kind_counts.push(',');
        error_kind_counts.push_str(&top_os_error.to_string());
        error_kind_counts.push(',');
        error_kind_counts.push_str(&top_os_error_count.to_string());
        format!(
            "{},{},{},{},{},{},{},{:.6},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{},{:.6},{:.6},{:.3},{:.3},{:.3},{}{}",
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
            self.samples,
            nanos_to_micros(self.stream_append_p50_nanos),
            nanos_to_micros(self.stream_append_p90_nanos),
            nanos_to_micros(self.stream_append_p99_nanos),
            nanos_to_micros(self.stream_append_p999_nanos),
            nanos_to_micros(self.stream_append_max_nanos),
            self.stream_append_samples,
            nanos_to_micros(self.stream_publish_p50_nanos),
            nanos_to_micros(self.stream_publish_p90_nanos),
            nanos_to_micros(self.stream_publish_p99_nanos),
            nanos_to_micros(self.stream_publish_p999_nanos),
            nanos_to_micros(self.stream_publish_max_nanos),
            self.stream_publish_samples,
            nanos_to_micros(self.stream_final_drain_p50_nanos),
            nanos_to_micros(self.stream_final_drain_p99_nanos),
            nanos_to_micros(self.stream_final_drain_max_nanos),
            self.stream_final_drain_samples,
            nanos_to_seconds(self.stream_append_phase_nanos),
            nanos_to_seconds(self.stream_boundary_phase_nanos),
            nanos_to_micros(self.stream_barrier_wait_p50_nanos),
            nanos_to_micros(self.stream_barrier_wait_p99_nanos),
            nanos_to_micros(self.stream_barrier_wait_max_nanos),
            self.stream_barrier_wait_samples,
            error_kind_counts
        )
    }

    /// One sidecar row per error group; empty on a clean run. The sidecar
    /// file itself is written either way, so an absent file means the
    /// instrumentation never ran rather than that the run was clean.
    fn error_csv_rows(&self) -> Vec<String> {
        self.errors_by_kind
            .groups
            .iter()
            .map(|(key, group)| {
                let kind = if key.overflow {
                    format!("{}-overflow", key.kind.name())
                } else {
                    key.kind.name().to_string()
                };
                format!(
                    "{},{},{},{},{},{},{},{},{},{},{}",
                    self.workload.name(),
                    self.provider,
                    self.durability,
                    self.rtt_us,
                    self.serial_rtts,
                    self.concurrency,
                    self.op_size,
                    kind,
                    key.os_error
                        .map(|code| code.to_string())
                        .unwrap_or_default(),
                    group.count,
                    csv_quote(&group.first_sample)
                )
            })
            .collect()
    }

    fn print_csv(&self) {
        println!("{}", self.csv_row());
    }

    /// Human-readable tally. Goes to stderr because stdout is captured as
    /// CSV by the GCP harness (`remote_block_vs_rbd.sh` pipes it through
    /// `tee .../stdout.csv`).
    fn print_error_summary(&self) {
        if self.errors_by_kind.groups.is_empty() {
            return;
        }
        eprintln!(
            "errors {} {} c{} op_size={}: {} total across {} kind(s)",
            self.workload.name(),
            self.provider,
            self.concurrency,
            self.op_size,
            self.errors,
            self.errors_by_kind.groups.len()
        );
        for (key, group) in &self.errors_by_kind.groups {
            let errno = key
                .os_error
                .map(|code| format!(" os_error={code}"))
                .unwrap_or_default();
            let overflow = if key.overflow { "-overflow" } else { "" };
            eprintln!(
                "  {}{}{} count={} first={}",
                key.kind.name(),
                overflow,
                errno,
                group.count,
                group.first_sample
            );
        }
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

fn nanos_to_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}
