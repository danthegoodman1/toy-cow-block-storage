use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataCreateFileRequest, MetadataCreateKeyspaceRequest,
    MetadataPlane,
};
use toy_cow_block_storage::{
    AppendReservation, AppendSession, ByteRange, CreateDeviceRequest, CreateFileRequest,
    CreateKeyspaceRequest, DeviceId, DeviceSpec, DurableCoordinator, DurableDataLogPolicy, FileId,
    FileSpec, FlushResult, KeyspaceId, LocalCoordinator, LocalStoreConfig, Result, StorageError,
    StorageNodeId, WriteDurability,
};

static NEXT_ROOT_ID: AtomicU64 = AtomicU64::new(1);

const BLOCK_SIZE: u32 = 4096;
const DEFAULT_DEVICE_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_ROOT_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_CAPACITY_BYTES: u64 = DEFAULT_FILE_ROOT_BLOCKS * BLOCK_SIZE as u64;
const SESSION_APPEND_FILE_STRIDE: usize = 64;

fn main() {
    if let Err(error) = run() {
        eprintln!("loadbench failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse()?;
    println!(
        "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,seconds,attempts,successes,errors,success_iops,attempt_iops,mbps,p50_us,p90_us,p99_us,p999_us,max_us,samples"
    );

    for workload in &args.workloads {
        for &concurrency in &args.concurrency {
            let report = run_case(&args, *workload, concurrency)?;
            report.print_csv();
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Args {
    provider: ProviderKind,
    durability: DurabilityMode,
    workloads: Vec<Workload>,
    concurrency: Vec<usize>,
    duration: Duration,
    warmup: Duration,
    rtt: Duration,
    serial_rtts: u32,
    delay_mode: DelayMode,
    root: PathBuf,
    files: usize,
    shards: usize,
    storage_nodes: usize,
    device_blocks: u64,
    samples_per_worker: usize,
}

impl Args {
    fn parse() -> Result<Self> {
        let mut args = Self {
            provider: ProviderKind::Local,
            durability: DurabilityMode::Acknowledged,
            workloads: Workload::north_star_suite(),
            concurrency: vec![1, 4, 16],
            duration: Duration::from_secs(1),
            warmup: Duration::from_millis(200),
            rtt: Duration::ZERO,
            serial_rtts: 1,
            delay_mode: DelayMode::Spin,
            root: env::temp_dir().join("toy-cow-block-storage-loadbench"),
            files: 1024,
            shards: 64,
            storage_nodes: 1,
            device_blocks: DEFAULT_DEVICE_BLOCKS,
            samples_per_worker: 200_000,
        };

        let mut raw = env::args().skip(1);
        while let Some(flag) = raw.next() {
            match flag.as_str() {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--provider" => args.provider = parse_next(&mut raw, "--provider")?,
                "--durability" => args.durability = parse_next(&mut raw, "--durability")?,
                "--workloads" => {
                    let value: String = parse_next(&mut raw, "--workloads")?;
                    args.workloads = parse_workloads(&value)?;
                }
                "--concurrency" => {
                    let value: String = parse_next(&mut raw, "--concurrency")?;
                    args.concurrency = parse_usize_list(&value, "--concurrency")?;
                }
                "--duration-ms" => {
                    let value: u64 = parse_next(&mut raw, "--duration-ms")?;
                    args.duration = Duration::from_millis(value);
                }
                "--warmup-ms" => {
                    let value: u64 = parse_next(&mut raw, "--warmup-ms")?;
                    args.warmup = Duration::from_millis(value);
                }
                "--rtt-us" => {
                    let value: u64 = parse_next(&mut raw, "--rtt-us")?;
                    args.rtt = Duration::from_micros(value);
                }
                "--serial-rtts" => args.serial_rtts = parse_next(&mut raw, "--serial-rtts")?,
                "--delay-mode" => args.delay_mode = parse_next(&mut raw, "--delay-mode")?,
                "--root" => args.root = PathBuf::from(parse_next::<String>(&mut raw, "--root")?),
                "--files" => args.files = parse_next(&mut raw, "--files")?,
                "--shards" => args.shards = parse_next(&mut raw, "--shards")?,
                "--storage-nodes" => args.storage_nodes = parse_next(&mut raw, "--storage-nodes")?,
                "--device-blocks" => args.device_blocks = parse_next(&mut raw, "--device-blocks")?,
                "--samples-per-worker" => {
                    args.samples_per_worker = parse_next(&mut raw, "--samples-per-worker")?;
                }
                other => {
                    return Err(StorageError::invalid_argument(format!(
                        "unknown loadbench flag {other}"
                    )));
                }
            }
        }

        if args.workloads.is_empty() {
            return Err(StorageError::invalid_argument(
                "at least one workload is required",
            ));
        }
        if args.concurrency.is_empty() || args.concurrency.contains(&0) {
            return Err(StorageError::invalid_argument(
                "concurrency list must contain positive values",
            ));
        }
        if args.files == 0 {
            return Err(StorageError::invalid_argument(
                "files must be greater than zero",
            ));
        }
        if args.shards == 0 {
            return Err(StorageError::invalid_argument(
                "shards must be greater than zero",
            ));
        }
        if args.storage_nodes == 0 {
            return Err(StorageError::invalid_argument(
                "storage-nodes must be greater than zero",
            ));
        }
        if args.device_blocks < args.shards as u64 {
            return Err(StorageError::invalid_argument(
                "device-blocks must be at least shards",
            ));
        }
        if args.samples_per_worker == 0 {
            return Err(StorageError::invalid_argument(
                "samples-per-worker must be greater than zero",
            ));
        }

        Ok(args)
    }

    fn config(&self) -> LocalStoreConfig {
        LocalStoreConfig {
            shard_count: self.shards,
            block_size: BLOCK_SIZE,
            file_root_blocks: DEFAULT_FILE_ROOT_BLOCKS,
            metadata_fanout: 4,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(1),
            observability_event_capacity: 16_384,
        }
    }

    fn storage_node_ids(&self) -> Vec<StorageNodeId> {
        (1..=self.storage_nodes as u128)
            .map(StorageNodeId::from_raw)
            .collect()
    }

    fn modeled_delay(&self) -> Duration {
        self.rtt
            .checked_mul(self.serial_rtts)
            .unwrap_or(Duration::MAX)
    }
}

fn print_help() {
    println!(
        "usage: cargo run --release --bin loadbench -- [options]\n\
\n\
options:\n\
  --provider local|durable                 default: local\n\
  --durability ack|flushed|ack-flush:N     default: ack\n\
  --workloads LIST                         default: north-star\n\
                                           aliases: north-star, append-batch, append-session\n\
                                           names: block-write-4k,\n\
                                           block-write-4k-shard-lanes, block-read-4k,\n\
                                           block-write-1m, native-read-4k,\n\
                                           native-write-4k, native-write-1m,\n\
                                           native-write-4m, native-write-32m,\n\
                                           native-append-4k, native-append-1m,\n\
                                           native-append-4m, native-append-32m,\n\
                                           native-session-append-4k,\n\
                                           native-session-append-1m,\n\
                                           native-session-append-4m,\n\
                                           native-session-append-32m,\n\
                                           native-hot-append-4k\n\
  --concurrency LIST                       default: 1,4,16\n\
  --duration-ms N                          default: 1000\n\
  --warmup-ms N                            default: 200\n\
  --rtt-us N                               modeled per-operation RTT, default: 0\n\
  --serial-rtts N                          modeled serial RTTs per op, default: 1\n\
  --delay-mode spin|sleep                  modeled RTT delay mode, default: spin\n\
  --files N                                native file count, default: 1024\n\
  --shards N                               block shard count, default: 64\n\
  --storage-nodes N                        local storage node count, default: 1\n\
  --device-blocks N                        logical device blocks, default: 1048576\n\
  --samples-per-worker N                   latency reservoir size, default: 200000\n\
  --root PATH                              durable scratch root"
    );
}

fn parse_next<T: FromStr>(raw: &mut impl Iterator<Item = String>, flag: &str) -> Result<T>
where
    T::Err: fmt::Display,
{
    let value = raw
        .next()
        .ok_or_else(|| StorageError::invalid_argument(format!("{flag} requires a value")))?;
    value
        .parse()
        .map_err(|error| StorageError::invalid_argument(format!("invalid {flag}: {error}")))
}

fn parse_usize_list(value: &str, flag: &str) -> Result<Vec<usize>> {
    value
        .split(',')
        .map(|part| {
            part.parse::<usize>().map_err(|error| {
                StorageError::invalid_argument(format!("invalid {flag} entry {part}: {error}"))
            })
        })
        .collect()
}

fn parse_workloads(value: &str) -> Result<Vec<Workload>> {
    let mut workloads = Vec::new();
    for part in value.split(',') {
        match part {
            "north-star" | "all" => workloads.extend(Workload::north_star_suite()),
            "append-batch" => workloads.extend(Workload::append_batch_suite()),
            "append-session" => workloads.extend(Workload::append_session_suite()),
            _ => workloads.push(Workload::from_str(part)?),
        }
    }
    Ok(workloads)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Local,
    Durable,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::Durable => f.write_str("durable"),
        }
    }
}

impl FromStr for ProviderKind {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "durable" => Ok(Self::Durable),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown provider {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurabilityMode {
    Acknowledged,
    Flushed,
    AckFlushEvery(u64),
}

impl DurabilityMode {
    fn write_durability(self) -> WriteDurability {
        match self {
            Self::Acknowledged | Self::AckFlushEvery(_) => WriteDurability::Acknowledged,
            Self::Flushed => WriteDurability::Flushed,
        }
    }
}

impl fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged => f.write_str("ack"),
            Self::Flushed => f.write_str("flushed"),
            Self::AckFlushEvery(every) => write!(f, "ack-flush:{every}"),
        }
    }
}

impl FromStr for DurabilityMode {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "ack" => Ok(Self::Acknowledged),
            "flushed" => Ok(Self::Flushed),
            _ if value.starts_with("ack-flush:") => {
                let every = value["ack-flush:".len()..]
                    .parse::<u64>()
                    .map_err(|error| {
                        StorageError::invalid_argument(format!(
                            "invalid ack-flush interval: {error}"
                        ))
                    })?;
                if every == 0 {
                    return Err(StorageError::invalid_argument(
                        "ack-flush interval must be greater than zero",
                    ));
                }
                Ok(Self::AckFlushEvery(every))
            }
            _ => Err(StorageError::invalid_argument(format!(
                "unknown durability mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DelayMode {
    Spin,
    Sleep,
}

impl FromStr for DelayMode {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "spin" => Ok(Self::Spin),
            "sleep" => Ok(Self::Sleep),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown delay mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    BlockWrite4k,
    BlockWrite4kShardLanes,
    BlockRead4k,
    BlockWrite1m,
    NativeRead4k,
    NativeWrite4k,
    NativeWrite1m,
    NativeWrite4m,
    NativeWrite32m,
    NativeAppend4k,
    NativeAppend1m,
    NativeAppend4m,
    NativeAppend32m,
    NativeSessionAppend4k,
    NativeSessionAppend1m,
    NativeSessionAppend4m,
    NativeSessionAppend32m,
    NativeHotAppend4k,
}

impl Workload {
    fn north_star_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4k,
            Self::BlockWrite4kShardLanes,
            Self::BlockRead4k,
            Self::BlockWrite1m,
            Self::NativeRead4k,
            Self::NativeWrite4k,
            Self::NativeAppend4k,
            Self::NativeHotAppend4k,
        ]
    }

    fn append_batch_suite() -> Vec<Self> {
        vec![
            Self::NativeAppend4k,
            Self::NativeAppend1m,
            Self::NativeAppend4m,
            Self::NativeAppend32m,
            Self::NativeWrite4k,
            Self::NativeWrite1m,
            Self::NativeWrite4m,
            Self::NativeWrite32m,
        ]
    }

    fn append_session_suite() -> Vec<Self> {
        vec![
            Self::NativeSessionAppend4k,
            Self::NativeSessionAppend1m,
            Self::NativeSessionAppend4m,
            Self::NativeSessionAppend32m,
            Self::NativeWrite4k,
            Self::NativeWrite1m,
            Self::NativeWrite4m,
            Self::NativeWrite32m,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::BlockWrite4k => "block-write-4k",
            Self::BlockWrite4kShardLanes => "block-write-4k-shard-lanes",
            Self::BlockRead4k => "block-read-4k",
            Self::BlockWrite1m => "block-write-1m",
            Self::NativeRead4k => "native-read-4k",
            Self::NativeWrite4k => "native-write-4k",
            Self::NativeWrite1m => "native-write-1m",
            Self::NativeWrite4m => "native-write-4m",
            Self::NativeWrite32m => "native-write-32m",
            Self::NativeAppend4k => "native-append-4k",
            Self::NativeAppend1m => "native-append-1m",
            Self::NativeAppend4m => "native-append-4m",
            Self::NativeAppend32m => "native-append-32m",
            Self::NativeSessionAppend4k => "native-session-append-4k",
            Self::NativeSessionAppend1m => "native-session-append-1m",
            Self::NativeSessionAppend4m => "native-session-append-4m",
            Self::NativeSessionAppend32m => "native-session-append-32m",
            Self::NativeHotAppend4k => "native-hot-append-4k",
        }
    }

    fn op_size(self) -> usize {
        match self {
            Self::BlockWrite1m
            | Self::NativeWrite1m
            | Self::NativeAppend1m
            | Self::NativeSessionAppend1m => 1024 * 1024,
            Self::NativeWrite4m | Self::NativeAppend4m | Self::NativeSessionAppend4m => {
                4 * 1024 * 1024
            }
            Self::NativeWrite32m | Self::NativeAppend32m | Self::NativeSessionAppend32m => {
                32 * 1024 * 1024
            }
            Self::BlockWrite4k
            | Self::BlockWrite4kShardLanes
            | Self::BlockRead4k
            | Self::NativeWrite4k
            | Self::NativeRead4k
            | Self::NativeAppend4k
            | Self::NativeSessionAppend4k
            | Self::NativeHotAppend4k => 4096,
        }
    }

    fn is_read(self) -> bool {
        matches!(self, Self::BlockRead4k | Self::NativeRead4k)
    }

    fn is_native_write(self) -> bool {
        matches!(
            self,
            Self::NativeWrite4k | Self::NativeWrite1m | Self::NativeWrite4m | Self::NativeWrite32m
        )
    }

    fn is_native_append(self) -> bool {
        matches!(
            self,
            Self::NativeAppend4k
                | Self::NativeAppend1m
                | Self::NativeAppend4m
                | Self::NativeAppend32m
        )
    }

    fn is_native_session_append(self) -> bool {
        matches!(
            self,
            Self::NativeSessionAppend4k
                | Self::NativeSessionAppend1m
                | Self::NativeSessionAppend4m
                | Self::NativeSessionAppend32m
        )
    }

    fn is_block(self) -> bool {
        matches!(
            self,
            Self::BlockWrite4k
                | Self::BlockWrite4kShardLanes
                | Self::BlockRead4k
                | Self::BlockWrite1m
        )
    }
}

impl FromStr for Workload {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "block-write-4k" => Ok(Self::BlockWrite4k),
            "block-write-4k-shard-lanes" => Ok(Self::BlockWrite4kShardLanes),
            "block-read-4k" => Ok(Self::BlockRead4k),
            "block-write-1m" => Ok(Self::BlockWrite1m),
            "native-read-4k" => Ok(Self::NativeRead4k),
            "native-write-4k" => Ok(Self::NativeWrite4k),
            "native-write-1m" => Ok(Self::NativeWrite1m),
            "native-write-4m" => Ok(Self::NativeWrite4m),
            "native-write-32m" => Ok(Self::NativeWrite32m),
            "native-append-4k" => Ok(Self::NativeAppend4k),
            "native-append-1m" => Ok(Self::NativeAppend1m),
            "native-append-4m" => Ok(Self::NativeAppend4m),
            "native-append-32m" => Ok(Self::NativeAppend32m),
            "native-session-append-4k" => Ok(Self::NativeSessionAppend4k),
            "native-session-append-1m" => Ok(Self::NativeSessionAppend1m),
            "native-session-append-4m" => Ok(Self::NativeSessionAppend4m),
            "native-session-append-32m" => Ok(Self::NativeSessionAppend32m),
            "native-hot-append-4k" => Ok(Self::NativeHotAppend4k),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown workload {value}"
            ))),
        }
    }
}

#[derive(Clone)]
enum BenchStore {
    Local(Arc<LocalCoordinator>),
    Durable(Arc<DurableCoordinator>),
}

impl BenchStore {
    fn open(args: &Args, root: &Path) -> Result<Self> {
        match args.provider {
            ProviderKind::Local => Ok(Self::Local(Arc::new(LocalCoordinator::with_storage_nodes(
                args.config(),
                args.storage_node_ids(),
            )?))),
            ProviderKind::Durable => Ok(Self::Durable(Arc::new(
                DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
                    root,
                    args.config(),
                    args.storage_node_ids(),
                    DurableDataLogPolicy::default(),
                )?,
            ))),
        }
    }

    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_device(MetadataCreateDeviceRequest::from(request))
                .map(|head| head.device_id),
            Self::Durable(store) => store.create_device(request),
        }
    }

    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id),
            Self::Durable(store) => store.create_keyspace(request),
        }
    }

    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_file(MetadataCreateFileRequest {
                    keyspace_id,
                    request,
                })
                .map(|head| head.file_id),
            Self::Durable(store) => store.create_file(keyspace_id, request),
        }
    }

    fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.write_device(device_id, offset, data, durability),
            Self::Durable(store) => store.write_device(device_id, offset, data, durability),
        }
        .map(|_| ())
    }

    fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        match self {
            Self::Local(store) => store.read_device(device_id, range, buf),
            Self::Durable(store) => store.read_device(device_id, range, buf),
        }
    }

    fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        match self {
            Self::Local(store) => {
                let info = store.metadata().device_info(device_id)?;
                Ok(FlushResult {
                    device_id,
                    durable_through: info.latest_commit,
                })
            }
            Self::Durable(store) => store.flush_device(device_id),
        }
    }

    fn write_file_at(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.write_file_at(keyspace_id, file_id, offset, data, durability)
            }
            Self::Durable(store) => {
                store.write_file_at(keyspace_id, file_id, offset, data, durability)
            }
        }
        .map(|_| ())
    }

    fn open_append_session(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendSession> {
        match self {
            Self::Local(store) => store.open_append_session(keyspace_id, file_id),
            Self::Durable(store) => store.open_append_session(keyspace_id, file_id),
        }
    }

    fn append_file_once(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<()> {
        let session = self.open_append_session(keyspace_id, file_id)?;
        let reservation = match self {
            Self::Local(store) => store.reserve_append(&session, data.len() as u64),
            Self::Durable(store) => store.reserve_append(&session, data.len() as u64),
        }?;
        self.append_reserved(reservation, data, durability)
    }

    fn reserve_append(&self, session: &AppendSession, len: u64) -> Result<AppendReservation> {
        match self {
            Self::Local(store) => store.reserve_append(session, len),
            Self::Durable(store) => store.reserve_append(session, len),
        }
    }

    fn append_reserved(
        &self,
        reservation: AppendReservation,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.append_reserved(reservation, data, durability),
            Self::Durable(store) => store.append_reserved(reservation, data, durability),
        }
        .map(|_| ())
    }

    fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.read_file(keyspace_id, file_id, range, buf),
            Self::Durable(store) => store.read_file(keyspace_id, file_id, range, buf),
        }
    }

    fn flush_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FlushResult> {
        match self {
            Self::Local(store) => {
                let head = store.metadata().get_file_head(keyspace_id, file_id)?;
                Ok(FlushResult {
                    device_id: DeviceId::from_raw(file_id.raw()),
                    durable_through: head.latest_commit,
                })
            }
            Self::Durable(store) => store.flush_file(keyspace_id, file_id),
        }
    }
}

#[derive(Clone)]
struct BenchContext {
    store: BenchStore,
    target: Target,
    payload: Arc<Vec<u8>>,
    op_size: usize,
}

#[derive(Clone)]
enum Target {
    Block {
        device_id: DeviceId,
        logical_blocks: u64,
        hot_blocks: u64,
        shard_count: usize,
    },
    Native {
        keyspace_id: KeyspaceId,
        files: Arc<Vec<FileId>>,
    },
}

fn run_case(args: &Args, workload: Workload, concurrency: usize) -> Result<BenchReport> {
    let root = args.root.join(format!(
        "{}-{}-c{}-{}",
        std::process::id(),
        workload.name(),
        concurrency,
        NEXT_ROOT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    if matches!(args.provider, ProviderKind::Durable) {
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).map_err(fs_error)?;
    }

    let store = BenchStore::open(args, &root)?;
    let context = setup_context(args, workload, store)?;
    if !args.warmup.is_zero() {
        let _ = execute_load(args, workload, concurrency, context.clone(), args.warmup)?;
    }
    let mut report = execute_load(args, workload, concurrency, context, args.duration)?;
    report.provider = args.provider;
    report.durability = args.durability;
    report.workload = workload;
    report.concurrency = concurrency;
    report.rtt_us = args.rtt.as_micros();
    report.serial_rtts = args.serial_rtts;
    report.op_size = workload.op_size();

    if matches!(args.provider, ProviderKind::Durable) {
        let _ = fs::remove_dir_all(&root);
    }

    Ok(report)
}

fn setup_context(args: &Args, workload: Workload, store: BenchStore) -> Result<BenchContext> {
    let op_size = workload.op_size();
    let payload = Arc::new(make_payload(op_size));
    let target = if workload.is_block() {
        let device_id = store.create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: args.device_blocks,
                block_size: BLOCK_SIZE,
            },
            name: None,
        })?;
        let hot_blocks = if workload.is_read() {
            seed_block_read_workload(&store, device_id, args, &payload)?
        } else {
            args.device_blocks
        };
        Target::Block {
            device_id,
            logical_blocks: args.device_blocks,
            hot_blocks,
            shard_count: args.shards,
        }
    } else {
        let keyspace_id = store.create_keyspace(CreateKeyspaceRequest { name: None })?;
        let file_count = if matches!(workload, Workload::NativeHotAppend4k) {
            1
        } else {
            args.files
        };
        let mut files = Vec::with_capacity(file_count);
        for _ in 0..file_count {
            let file_id = store.create_file(
                keyspace_id,
                CreateFileRequest {
                    spec: FileSpec { name: None },
                },
            )?;
            if matches!(workload, Workload::NativeRead4k) {
                store.write_file_at(
                    keyspace_id,
                    file_id,
                    0,
                    &payload,
                    WriteDurability::Acknowledged,
                )?;
            }
            files.push(file_id);
        }
        Target::Native {
            keyspace_id,
            files: Arc::new(files),
        }
    };

    Ok(BenchContext {
        store,
        target,
        payload,
        op_size,
    })
}

fn seed_block_read_workload(
    store: &BenchStore,
    device_id: DeviceId,
    args: &Args,
    payload: &[u8],
) -> Result<u64> {
    let hot_blocks = args.device_blocks.min(4096);
    for block in 0..hot_blocks {
        store.write_device(
            device_id,
            block * u64::from(BLOCK_SIZE),
            payload,
            WriteDurability::Acknowledged,
        )?;
    }
    store.flush_device(device_id)?;
    Ok(hot_blocks)
}

fn execute_load(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    context: BenchContext,
    duration: Duration,
) -> Result<BenchReport> {
    let started = Instant::now();
    let deadline = started + duration;
    let modeled_delay = args.modeled_delay();
    let worker_config = WorkerConfig {
        workload,
        concurrency,
        deadline,
        modeled_delay,
        delay_mode: args.delay_mode,
        durability: args.durability,
        samples_per_worker: args.samples_per_worker,
    };

    let reports = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let context = context.clone();
            let config = worker_config;
            handles.push(scope.spawn(move || run_worker(context, worker as u64, config)));
        }

        let mut reports = Vec::with_capacity(concurrency);
        for handle in handles {
            reports.push(
                handle
                    .join()
                    .map_err(|_| StorageError::unavailable("loadbench worker panicked"))??,
            );
        }
        Ok::<_, StorageError>(reports)
    })?;

    let elapsed = started.elapsed();
    Ok(BenchReport::from_workers(elapsed, reports))
}

#[derive(Debug, Clone, Copy)]
struct WorkerConfig {
    workload: Workload,
    concurrency: usize,
    deadline: Instant,
    modeled_delay: Duration,
    delay_mode: DelayMode,
    durability: DurabilityMode,
    samples_per_worker: usize,
}

fn run_worker(context: BenchContext, worker: u64, config: WorkerConfig) -> Result<WorkerReport> {
    let mut rng = Lcg::new(0x9e37_79b9_7f4a_7c15_u64 ^ worker.wrapping_mul(0xd1b5_4a32_d192_ed03));
    let mut report = WorkerReport::new(config.samples_per_worker);
    let mut state = WorkerState::default();
    let mut read_buf = if config.workload.is_read() {
        vec![0; context.op_size]
    } else {
        Vec::new()
    };

    while Instant::now() < config.deadline {
        let started = Instant::now();
        if !config.modeled_delay.is_zero() {
            apply_modeled_delay(config.modeled_delay, config.delay_mode);
        }
        let result = run_one_op(
            &context,
            worker,
            &mut state,
            &mut rng,
            &config,
            &mut read_buf,
        )
        .and_then(|_| {
            maybe_flush(
                &context,
                config.workload,
                config.durability,
                report.attempts + 1,
                worker,
                state.last_native_file_index,
            )
        });
        let elapsed = started.elapsed();
        let latency_nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        report.record(
            latency_nanos,
            context.op_size as u64,
            result.is_ok(),
            &mut rng,
        );
    }

    Ok(report)
}

#[derive(Default)]
struct WorkerState {
    session_append: Option<SessionAppendState>,
    next_session_file_index: Option<usize>,
    native_file_op: u64,
    last_native_file_index: Option<usize>,
}

impl WorkerState {
    fn next_partitioned_file_index(
        &mut self,
        worker: u64,
        concurrency: usize,
        files_len: usize,
    ) -> usize {
        let file_index =
            partitioned_file_index(worker, self.native_file_op, concurrency, files_len);
        self.native_file_op = self.native_file_op.saturating_add(1);
        self.last_native_file_index = Some(file_index);
        file_index
    }
}

struct SessionAppendState {
    file_index: usize,
    session: AppendSession,
    next_offset: u64,
}

fn apply_modeled_delay(delay: Duration, mode: DelayMode) {
    match mode {
        DelayMode::Sleep => thread::sleep(delay),
        DelayMode::Spin => {
            let deadline = Instant::now() + delay;
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
        }
    }
}

fn run_one_op(
    context: &BenchContext,
    worker: u64,
    state: &mut WorkerState,
    rng: &mut Lcg,
    config: &WorkerConfig,
    read_buf: &mut [u8],
) -> Result<()> {
    let workload = config.workload;
    let concurrency = config.concurrency;
    let durability = config.durability.write_durability();

    match (&context.target, workload) {
        (
            Target::Block {
                device_id,
                logical_blocks,
                ..
            },
            Workload::BlockWrite4k,
        ) => {
            let block = rng.below(*logical_blocks);
            context.store.write_device(
                *device_id,
                block * u64::from(BLOCK_SIZE),
                &context.payload,
                durability,
            )
        }
        (
            Target::Block {
                device_id,
                logical_blocks,
                shard_count,
                ..
            },
            Workload::BlockWrite4kShardLanes,
        ) => {
            let shard_count = *shard_count as u64;
            let shard = worker % shard_count;
            let start = logical_blocks
                .checked_mul(shard)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count;
            let end = logical_blocks
                .checked_mul(shard + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count;
            let block = start + rng.below(end - start);
            context.store.write_device(
                *device_id,
                block * u64::from(BLOCK_SIZE),
                &context.payload,
                durability,
            )
        }
        (
            Target::Block {
                device_id,
                logical_blocks,
                ..
            },
            Workload::BlockWrite1m,
        ) => {
            let blocks = (context.op_size as u64) / u64::from(BLOCK_SIZE);
            let start = rng.below(logical_blocks.saturating_sub(blocks).saturating_add(1));
            context.store.write_device(
                *device_id,
                start * u64::from(BLOCK_SIZE),
                &context.payload,
                durability,
            )
        }
        (
            Target::Block {
                device_id,
                hot_blocks,
                ..
            },
            Workload::BlockRead4k,
        ) => {
            let block = rng.below(*hot_blocks);
            context.store.read_device(
                *device_id,
                ByteRange::new(block * u64::from(BLOCK_SIZE), context.op_size as u64),
                read_buf,
            )
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_write() => {
            let file_index = state.next_partitioned_file_index(worker, concurrency, files.len());
            let file_id = files[file_index];
            context
                .store
                .write_file_at(*keyspace_id, file_id, 0, &context.payload, durability)
        }
        (Target::Native { keyspace_id, files }, Workload::NativeRead4k) => {
            let file_id = files[rng.below(files.len() as u64) as usize];
            context.store.read_file(
                *keyspace_id,
                file_id,
                ByteRange::new(0, context.op_size as u64),
                read_buf,
            )
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_append() => {
            let file_index = state.next_partitioned_file_index(worker, concurrency, files.len());
            let file_id = files[file_index];
            context
                .store
                .append_file_once(*keyspace_id, file_id, &context.payload, durability)
        }
        (Target::Native { keyspace_id, files }, workload)
            if workload.is_native_session_append() =>
        {
            let payload_len = context.payload.len() as u64;
            for _ in 0..files.len() {
                if let Some(session) = state.session_append.as_ref() {
                    let would_exceed = session
                        .next_offset
                        .checked_add(payload_len)
                        .is_none_or(|end| end > DEFAULT_FILE_CAPACITY_BYTES);
                    if would_exceed {
                        state.next_session_file_index =
                            Some((session.file_index + SESSION_APPEND_FILE_STRIDE) % files.len());
                        state.session_append = None;
                    }
                }
                if state.session_append.is_none() {
                    let file_index = state
                        .next_session_file_index
                        .get_or_insert_with(|| worker as usize % files.len());
                    let file_id = files[*file_index];
                    let session = context.store.open_append_session(*keyspace_id, file_id)?;
                    state.session_append = Some(SessionAppendState {
                        file_index: *file_index,
                        session,
                        next_offset: 0,
                    });
                }
                let session = state
                    .session_append
                    .as_ref()
                    .map(|session| &session.session)
                    .ok_or_else(|| StorageError::conflict("append session state missing"))?;
                let reservation = match context.store.reserve_append(session, payload_len) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        if let Some(session) = state.session_append.as_ref() {
                            state.next_session_file_index = Some(
                                (session.file_index + SESSION_APPEND_FILE_STRIDE) % files.len(),
                            );
                        }
                        state.session_append = None;
                        return Err(error);
                    }
                };
                let next_offset = reservation.offset.saturating_add(reservation.len);
                if next_offset > DEFAULT_FILE_CAPACITY_BYTES {
                    if let Some(session) = state.session_append.as_ref() {
                        state.next_session_file_index =
                            Some((session.file_index + SESSION_APPEND_FILE_STRIDE) % files.len());
                    }
                    state.session_append = None;
                    continue;
                }
                let result =
                    context
                        .store
                        .append_reserved(reservation, &context.payload, durability);
                if result.is_err() {
                    if let Some(session) = state.session_append.as_ref() {
                        state.next_session_file_index =
                            Some((session.file_index + SESSION_APPEND_FILE_STRIDE) % files.len());
                    }
                    state.session_append = None;
                } else if let Some(session) = state.session_append.as_mut() {
                    session.next_offset = next_offset;
                    state.last_native_file_index = Some(session.file_index);
                }
                return result;
            }
            Err(StorageError::conflict(
                "append-session benchmark exhausted every file lane",
            ))
        }
        (Target::Native { keyspace_id, files }, Workload::NativeHotAppend4k) => {
            let file_id = files[0];
            state.last_native_file_index = Some(0);
            context
                .store
                .append_file_once(*keyspace_id, file_id, &context.payload, durability)
        }
        _ => Err(StorageError::invalid_argument(
            "workload does not match benchmark target",
        )),
    }
}

fn maybe_flush(
    context: &BenchContext,
    workload: Workload,
    durability: DurabilityMode,
    attempts_after_op: u64,
    worker: u64,
    last_native_file_index: Option<usize>,
) -> Result<()> {
    let DurabilityMode::AckFlushEvery(every) = durability else {
        return Ok(());
    };
    if !attempts_after_op.is_multiple_of(every) {
        return Ok(());
    }

    match &context.target {
        Target::Block { device_id, .. } => context.store.flush_device(*device_id).map(|_| ()),
        Target::Native { keyspace_id, files } => {
            let file_id = if matches!(workload, Workload::NativeHotAppend4k) {
                files[0]
            } else {
                files[last_native_file_index.unwrap_or(worker as usize % files.len())]
            };
            context.store.flush_file(*keyspace_id, file_id).map(|_| ())
        }
    }
}

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

#[derive(Debug)]
struct WorkerReport {
    attempts: u64,
    successes: u64,
    errors: u64,
    bytes: u64,
    max_latency_nanos: u64,
    latency_seen: u64,
    latencies: Vec<u64>,
    sample_limit: usize,
}

impl WorkerReport {
    fn new(sample_limit: usize) -> Self {
        Self {
            attempts: 0,
            successes: 0,
            errors: 0,
            bytes: 0,
            max_latency_nanos: 0,
            latency_seen: 0,
            latencies: Vec::with_capacity(sample_limit.min(1024)),
            sample_limit,
        }
    }

    fn record(&mut self, latency_nanos: u64, bytes: u64, success: bool, rng: &mut Lcg) {
        self.attempts = self.attempts.saturating_add(1);
        self.latency_seen = self.latency_seen.saturating_add(1);
        self.max_latency_nanos = self.max_latency_nanos.max(latency_nanos);
        if success {
            self.successes = self.successes.saturating_add(1);
            self.bytes = self.bytes.saturating_add(bytes);
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
        let mut max_nanos = 0_u64;
        let mut samples = Vec::new();

        for worker in workers {
            attempts = attempts.saturating_add(worker.attempts);
            successes = successes.saturating_add(worker.successes);
            errors = errors.saturating_add(worker.errors);
            bytes = bytes.saturating_add(worker.bytes);
            max_nanos = max_nanos.max(worker.max_latency_nanos);
            samples.extend(worker.latencies);
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
        println!(
            "{},{},{},{},{},{},{},{:.6},{},{},{},{:.2},{:.2},{:.2},{:.3},{:.3},{:.3},{:.3},{:.3},{}",
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

fn fs_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("filesystem operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn partitioned_file_index_gives_unique_lanes_across_workers() {
        let concurrency = 16;
        let files_len = 128;

        for op_index in 0..32 {
            let mut seen = BTreeSet::new();
            for worker in 0..concurrency {
                let file_index =
                    partitioned_file_index(worker as u64, op_index, concurrency, files_len);
                assert!(
                    seen.insert(file_index),
                    "worker {worker} collided on file {file_index} at op {op_index}"
                );
            }
        }
    }

    #[test]
    fn partitioned_file_index_stays_inside_worker_partition() {
        let concurrency = 4;
        let files_len = 10;

        for worker in 0..concurrency {
            let base = files_len * worker / concurrency;
            let next_base = files_len * (worker + 1) / concurrency;
            for op_index in 0..32 {
                let file_index =
                    partitioned_file_index(worker as u64, op_index, concurrency, files_len);
                assert!(
                    (base..next_base).contains(&file_index),
                    "file {file_index} escaped worker {worker} partition {base}..{next_base}"
                );
            }
        }
    }

    #[test]
    fn partitioned_file_index_handles_more_workers_than_files() {
        let files_len = 3;
        let concurrency = 8;

        for worker in 0..concurrency {
            let file_index = partitioned_file_index(worker as u64, 0, concurrency, files_len);
            assert_eq!(file_index, worker % files_len);
        }
    }

    #[test]
    fn worker_state_remembers_last_partitioned_file_index() {
        let mut state = WorkerState::default();
        let file_index = state.next_partitioned_file_index(2, 4, 16);

        assert_eq!(file_index, 8);
        assert_eq!(state.last_native_file_index, Some(8));
        assert_eq!(state.native_file_op, 1);
    }
}
