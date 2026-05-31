use std::env;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataCreateFileRequest, MetadataCreateKeyspaceRequest,
    MetadataPlane,
};
use toy_cow_block_storage::{
    AppendStream, AppendTicket, ByteRange, CreateDeviceRequest, CreateFileRequest,
    CreateKeyspaceRequest, DeviceId, DeviceSpec, DurableAppendMark, DurableCoordinator,
    DurableDataLogPolicy, DurablePersistProfile, FileId, FileSpec, FlushResult, KeyspaceId,
    LocalCoordinator, LocalStoreConfig, MetadataTxnMode, MetadataTxnProfile, PayloadIntegrity,
    ReadVerification, Result, StorageError, StorageNodeId, TxnBlockCoordinator,
    TxnBlockWriteProfile, WriteDurability,
};

static NEXT_ROOT_ID: AtomicU64 = AtomicU64::new(1);

const BLOCK_SIZE: u32 = 4096;
const DEFAULT_DEVICE_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_ROOT_BLOCKS: u64 = 1_048_576;
const DEFAULT_FILE_CAPACITY_BYTES: u64 = DEFAULT_FILE_ROOT_BLOCKS * BLOCK_SIZE as u64;
const STREAM_APPEND_FILE_STRIDE: usize = 64;
const DEFAULT_PROFILE_CAPACITY: usize = 1_000_000;

fn main() {
    if let Err(error) = run() {
        eprintln!("loadbench failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse()?;
    println!(
        "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,seconds,attempts,successes,errors,success_iops,attempt_iops,mbps,durable_mbps,published_mbps,durable_bytes,published_bytes,p50_us,p90_us,p99_us,p999_us,max_us,samples"
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
    durable_profile_csv: Option<PathBuf>,
    metadata_profile_csv: Option<PathBuf>,
    block_write_profile_csv: Option<PathBuf>,
    stream_flush_bytes: Option<u64>,
    stream_publish_bytes: Option<u64>,
    payload_integrity: PayloadIntegrity,
    read_verification: ReadVerification,
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
            durable_profile_csv: None,
            metadata_profile_csv: None,
            block_write_profile_csv: None,
            stream_flush_bytes: None,
            stream_publish_bytes: None,
            payload_integrity: PayloadIntegrity::Verified,
            read_verification: ReadVerification::Default,
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
                "--durable-profile-csv" => {
                    args.durable_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--durable-profile-csv",
                    )?));
                }
                "--metadata-profile-csv" => {
                    args.metadata_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--metadata-profile-csv",
                    )?));
                }
                "--block-write-profile-csv" => {
                    args.block_write_profile_csv = Some(PathBuf::from(parse_next::<String>(
                        &mut raw,
                        "--block-write-profile-csv",
                    )?));
                }
                "--stream-flush-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--stream-flush-mib")?;
                    args.stream_flush_bytes = Some(mib_to_bytes(mib, "--stream-flush-mib")?);
                }
                "--stream-publish-mib" => {
                    let mib: u64 = parse_next(&mut raw, "--stream-publish-mib")?;
                    args.stream_publish_bytes = Some(mib_to_bytes(mib, "--stream-publish-mib")?);
                }
                "--payload-integrity" => {
                    let value: String = parse_next(&mut raw, "--payload-integrity")?;
                    args.payload_integrity = parse_payload_integrity(&value)?;
                }
                "--read-verification" => {
                    let value: String = parse_next(&mut raw, "--read-verification")?;
                    args.read_verification = parse_read_verification(&value)?;
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
  --provider local|durable|txn-serial|txn-sharded\n\
                                           default: local\n\
  --durability ack|flushed|ack-flush:N     default: ack\n\
  --workloads LIST                         default: north-star\n\
                                           aliases: north-star, append-batch, append-stream, block-metadata\n\
                                           names: block-write-4k,\n\
                                           block-write-4k-same-shard-contended,\n\
                                           block-write-4k-same-shard-serialized,\n\
                                           block-write-4k-shard-lanes,\n\
                                           block-write-4k-device-lanes, block-read-4k,\n\
                                           block-write-1m, block-write-1m-shard-lanes,\n\
                                           block-write-1m-device-lanes,\n\
                                           native-read-4k,\n\
                                           native-write-4k, native-write-1m,\n\
                                           native-write-4m, native-write-32m,\n\
                                           native-append-4k, native-append-1m,\n\
                                           native-append-4m, native-append-32m,\n\
                                           native-stream-ingest-1m,\n\
                                           native-stream-ingest-4m,\n\
                                           native-stream-ingest-32m,\n\
                                           native-stream-append-flush-1m,\n\
                                           native-stream-append-flush-4m,\n\
                                           native-stream-append-flush-32m,\n\
                                           native-stream-publish-preflushed-1m,\n\
                                           native-stream-flush-publish-1m,\n\
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
  --durable-profile-csv PATH               append durable persist profiles to CSV\n\
  --metadata-profile-csv PATH              append txn metadata profiles to CSV\n\
  --block-write-profile-csv PATH           append txn block write pipeline profiles to CSV\n\
  --stream-flush-mib N                     flush append streams after N MiB per stream; 2-4 is latency-first\n\
  --stream-publish-mib N                   publish append streams after N MiB per stream\n\
  --payload-integrity verified|unchecked   write payload integrity, default: verified\n\
  --read-verification default|require-verified|skip\n\
                                           read verification policy, default: default\n\
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

fn mib_to_bytes(mib: u64, flag: &str) -> Result<u64> {
    mib.checked_mul(1024 * 1024)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| StorageError::invalid_argument(format!("{flag} must be greater than zero")))
}

fn parse_payload_integrity(value: &str) -> Result<PayloadIntegrity> {
    match value {
        "verified" | "crc32c" => Ok(PayloadIntegrity::Verified),
        "unchecked" | "none" | "no-verify" => Ok(PayloadIntegrity::Unchecked),
        _ => Err(StorageError::invalid_argument(format!(
            "unknown payload integrity {value}"
        ))),
    }
}

fn payload_integrity_name(value: PayloadIntegrity) -> &'static str {
    match value {
        PayloadIntegrity::Verified => "verified",
        PayloadIntegrity::Unchecked => "unchecked",
    }
}

fn parse_read_verification(value: &str) -> Result<ReadVerification> {
    match value {
        "default" => Ok(ReadVerification::Default),
        "require-verified" | "required" => Ok(ReadVerification::RequireVerified),
        "skip" | "none" | "no-verify" => Ok(ReadVerification::Skip),
        _ => Err(StorageError::invalid_argument(format!(
            "unknown read verification {value}"
        ))),
    }
}

fn parse_workloads(value: &str) -> Result<Vec<Workload>> {
    let mut workloads = Vec::new();
    for part in value.split(',') {
        match part {
            "north-star" | "all" => workloads.extend(Workload::north_star_suite()),
            "append-batch" => workloads.extend(Workload::append_batch_suite()),
            "append-stream" => workloads.extend(Workload::append_stream_suite()),
            "block-metadata" => workloads.extend(Workload::block_metadata_suite()),
            _ => workloads.push(Workload::from_str(part)?),
        }
    }
    Ok(workloads)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Local,
    Durable,
    TxnSerial,
    TxnSharded,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::Durable => f.write_str("durable"),
            Self::TxnSerial => f.write_str("txn-serial"),
            Self::TxnSharded => f.write_str("txn-sharded"),
        }
    }
}

impl FromStr for ProviderKind {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "durable" => Ok(Self::Durable),
            "txn-serial" => Ok(Self::TxnSerial),
            "txn-sharded" => Ok(Self::TxnSharded),
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
    BlockWrite4kSameShardContended,
    BlockWrite4kSameShardSerialized,
    BlockWrite4kShardLanes,
    BlockWrite4kDeviceLanes,
    BlockRead4k,
    BlockWrite1m,
    BlockWrite1mShardLanes,
    BlockWrite1mDeviceLanes,
    NativeRead4k,
    NativeWrite4k,
    NativeWrite1m,
    NativeWrite4m,
    NativeWrite32m,
    NativeAppend4k,
    NativeAppend1m,
    NativeAppend4m,
    NativeAppend32m,
    NativeStreamIngest1m,
    NativeStreamIngest4m,
    NativeStreamIngest32m,
    NativeStreamAppendFlush1m,
    NativeStreamAppendFlush4m,
    NativeStreamAppendFlush32m,
    NativeStreamPublishPreflushed1m,
    NativeStreamFlushPublish1m,
    NativeHotAppend4k,
}

impl Workload {
    fn north_star_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4k,
            Self::BlockWrite4kShardLanes,
            Self::BlockRead4k,
            Self::BlockWrite1m,
            Self::BlockWrite1mShardLanes,
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

    fn append_stream_suite() -> Vec<Self> {
        vec![
            Self::NativeWrite1m,
            Self::NativeWrite4m,
            Self::NativeWrite32m,
            Self::NativeStreamIngest1m,
            Self::NativeStreamIngest4m,
            Self::NativeStreamIngest32m,
            Self::NativeStreamAppendFlush1m,
            Self::NativeStreamAppendFlush4m,
            Self::NativeStreamAppendFlush32m,
            Self::NativeStreamPublishPreflushed1m,
            Self::NativeStreamFlushPublish1m,
        ]
    }

    fn block_metadata_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4kSameShardContended,
            Self::BlockWrite4kSameShardSerialized,
            Self::BlockWrite4kShardLanes,
            Self::BlockWrite4kDeviceLanes,
            Self::BlockWrite1mShardLanes,
            Self::BlockWrite1mDeviceLanes,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::BlockWrite4k => "block-write-4k",
            Self::BlockWrite4kSameShardContended => "block-write-4k-same-shard-contended",
            Self::BlockWrite4kSameShardSerialized => "block-write-4k-same-shard-serialized",
            Self::BlockWrite4kShardLanes => "block-write-4k-shard-lanes",
            Self::BlockWrite4kDeviceLanes => "block-write-4k-device-lanes",
            Self::BlockRead4k => "block-read-4k",
            Self::BlockWrite1m => "block-write-1m",
            Self::BlockWrite1mShardLanes => "block-write-1m-shard-lanes",
            Self::BlockWrite1mDeviceLanes => "block-write-1m-device-lanes",
            Self::NativeRead4k => "native-read-4k",
            Self::NativeWrite4k => "native-write-4k",
            Self::NativeWrite1m => "native-write-1m",
            Self::NativeWrite4m => "native-write-4m",
            Self::NativeWrite32m => "native-write-32m",
            Self::NativeAppend4k => "native-append-4k",
            Self::NativeAppend1m => "native-append-1m",
            Self::NativeAppend4m => "native-append-4m",
            Self::NativeAppend32m => "native-append-32m",
            Self::NativeStreamIngest1m => "native-stream-ingest-1m",
            Self::NativeStreamIngest4m => "native-stream-ingest-4m",
            Self::NativeStreamIngest32m => "native-stream-ingest-32m",
            Self::NativeStreamAppendFlush1m => "native-stream-append-flush-1m",
            Self::NativeStreamAppendFlush4m => "native-stream-append-flush-4m",
            Self::NativeStreamAppendFlush32m => "native-stream-append-flush-32m",
            Self::NativeStreamPublishPreflushed1m => "native-stream-publish-preflushed-1m",
            Self::NativeStreamFlushPublish1m => "native-stream-flush-publish-1m",
            Self::NativeHotAppend4k => "native-hot-append-4k",
        }
    }

    fn op_size(self) -> usize {
        match self {
            Self::BlockWrite1m
            | Self::BlockWrite1mShardLanes
            | Self::BlockWrite1mDeviceLanes
            | Self::NativeWrite1m
            | Self::NativeAppend1m
            | Self::NativeStreamIngest1m
            | Self::NativeStreamAppendFlush1m
            | Self::NativeStreamPublishPreflushed1m
            | Self::NativeStreamFlushPublish1m => 1024 * 1024,
            Self::NativeWrite4m
            | Self::NativeAppend4m
            | Self::NativeStreamIngest4m
            | Self::NativeStreamAppendFlush4m => 4 * 1024 * 1024,
            Self::NativeWrite32m
            | Self::NativeAppend32m
            | Self::NativeStreamIngest32m
            | Self::NativeStreamAppendFlush32m => 32 * 1024 * 1024,
            Self::BlockWrite4k
            | Self::BlockWrite4kSameShardContended
            | Self::BlockWrite4kSameShardSerialized
            | Self::BlockWrite4kShardLanes
            | Self::BlockWrite4kDeviceLanes
            | Self::BlockRead4k
            | Self::NativeWrite4k
            | Self::NativeRead4k
            | Self::NativeAppend4k
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

    fn is_native_stream(self) -> bool {
        matches!(
            self,
            Self::NativeStreamIngest1m
                | Self::NativeStreamIngest4m
                | Self::NativeStreamIngest32m
                | Self::NativeStreamAppendFlush1m
                | Self::NativeStreamAppendFlush4m
                | Self::NativeStreamAppendFlush32m
                | Self::NativeStreamPublishPreflushed1m
                | Self::NativeStreamFlushPublish1m
        )
    }

    fn is_native_stream_ingest(self) -> bool {
        matches!(
            self,
            Self::NativeStreamIngest1m | Self::NativeStreamIngest4m | Self::NativeStreamIngest32m
        )
    }

    fn is_native_stream_append_flush(self) -> bool {
        matches!(
            self,
            Self::NativeStreamAppendFlush1m
                | Self::NativeStreamAppendFlush4m
                | Self::NativeStreamAppendFlush32m
        )
    }

    fn is_native_stream_publish_preflushed(self) -> bool {
        matches!(self, Self::NativeStreamPublishPreflushed1m)
    }

    fn is_native_stream_flush_publish(self) -> bool {
        matches!(self, Self::NativeStreamFlushPublish1m)
    }

    fn is_block(self) -> bool {
        matches!(
            self,
            Self::BlockWrite4k
                | Self::BlockWrite4kSameShardContended
                | Self::BlockWrite4kSameShardSerialized
                | Self::BlockWrite4kShardLanes
                | Self::BlockWrite4kDeviceLanes
                | Self::BlockRead4k
                | Self::BlockWrite1m
                | Self::BlockWrite1mShardLanes
                | Self::BlockWrite1mDeviceLanes
        )
    }

    fn is_block_device_lanes(self) -> bool {
        matches!(
            self,
            Self::BlockWrite4kDeviceLanes | Self::BlockWrite1mDeviceLanes
        )
    }
}

impl FromStr for Workload {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "block-write-4k" => Ok(Self::BlockWrite4k),
            "block-write-4k-same-shard-contended" => Ok(Self::BlockWrite4kSameShardContended),
            "block-write-4k-same-shard-serialized" => Ok(Self::BlockWrite4kSameShardSerialized),
            "block-write-4k-shard-lanes" => Ok(Self::BlockWrite4kShardLanes),
            "block-write-4k-device-lanes" => Ok(Self::BlockWrite4kDeviceLanes),
            "block-read-4k" => Ok(Self::BlockRead4k),
            "block-write-1m" => Ok(Self::BlockWrite1m),
            "block-write-1m-shard-lanes" => Ok(Self::BlockWrite1mShardLanes),
            "block-write-1m-device-lanes" => Ok(Self::BlockWrite1mDeviceLanes),
            "native-read-4k" => Ok(Self::NativeRead4k),
            "native-write-4k" => Ok(Self::NativeWrite4k),
            "native-write-1m" => Ok(Self::NativeWrite1m),
            "native-write-4m" => Ok(Self::NativeWrite4m),
            "native-write-32m" => Ok(Self::NativeWrite32m),
            "native-append-4k" => Ok(Self::NativeAppend4k),
            "native-append-1m" => Ok(Self::NativeAppend1m),
            "native-append-4m" => Ok(Self::NativeAppend4m),
            "native-append-32m" => Ok(Self::NativeAppend32m),
            "native-stream-ingest-1m" => Ok(Self::NativeStreamIngest1m),
            "native-stream-ingest-4m" => Ok(Self::NativeStreamIngest4m),
            "native-stream-ingest-32m" => Ok(Self::NativeStreamIngest32m),
            "native-stream-append-flush-1m" => Ok(Self::NativeStreamAppendFlush1m),
            "native-stream-append-flush-4m" => Ok(Self::NativeStreamAppendFlush4m),
            "native-stream-append-flush-32m" => Ok(Self::NativeStreamAppendFlush32m),
            "native-stream-publish-preflushed-1m" => Ok(Self::NativeStreamPublishPreflushed1m),
            "native-stream-flush-publish-1m" => Ok(Self::NativeStreamFlushPublish1m),
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
    Txn(Arc<TxnBlockCoordinator>),
}

impl BenchStore {
    fn open(args: &Args, root: &Path) -> Result<Self> {
        match args.provider {
            ProviderKind::Local => Ok(Self::Local(Arc::new(LocalCoordinator::with_storage_nodes(
                args.config(),
                args.storage_node_ids(),
            )?))),
            ProviderKind::TxnSerial => {
                let store = Arc::new(TxnBlockCoordinator::with_storage_nodes(
                    args.config(),
                    args.storage_node_ids(),
                    MetadataTxnMode::Serial,
                )?);
                if args.metadata_profile_csv.is_some() {
                    store.enable_metadata_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.block_write_profile_csv.is_some() {
                    store.enable_block_write_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Txn(store))
            }
            ProviderKind::TxnSharded => {
                let store = Arc::new(TxnBlockCoordinator::with_storage_nodes(
                    args.config(),
                    args.storage_node_ids(),
                    MetadataTxnMode::Sharded {
                        shard_count: args.shards,
                    },
                )?);
                if args.metadata_profile_csv.is_some() {
                    store.enable_metadata_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.block_write_profile_csv.is_some() {
                    store.enable_block_write_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Txn(store))
            }
            ProviderKind::Durable => {
                let store = Arc::new(
                    DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
                        root,
                        args.config(),
                        args.storage_node_ids(),
                        DurableDataLogPolicy::default(),
                    )?,
                );
                if args.durable_profile_csv.is_some() {
                    store.enable_persist_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Durable(store))
            }
        }
    }

    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_device(MetadataCreateDeviceRequest::from(request))
                .map(|head| head.device_id),
            Self::Durable(store) => store.create_device(request),
            Self::Txn(store) => store.create_device(request),
        }
    }

    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id),
            Self::Durable(store) => store.create_keyspace(request),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
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
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
            Self::Durable(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
            Self::Txn(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
        }
        .map(|_| ())
    }

    fn read_device(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Durable(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Txn(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
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
            Self::Txn(store) => store.flush_device(device_id),
        }
    }

    fn write_file_at(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.write_file_at_with_integrity(
                keyspace_id,
                file_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
            Self::Durable(store) => store.write_file_at_with_integrity(
                keyspace_id,
                file_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
        .map(|_| ())
    }

    fn open_append_stream(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendStream> {
        match self {
            Self::Local(store) => store.open_append_stream(keyspace_id, file_id),
            Self::Durable(store) => store.open_append_stream(keyspace_id, file_id),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn append_file_once(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        let stream = self.open_append_stream(keyspace_id, file_id)?;
        self.append_stream(&stream, data, durability, payload_integrity)?;
        let mark = self.flush_append_stream(&stream)?;
        self.publish_append_stream(&stream, &mark)
    }

    fn append_stream(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        match self {
            Self::Local(store) => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Durable(store) => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn flush_append_stream(&self, stream: &AppendStream) -> Result<DurableAppendMark> {
        match self {
            Self::Local(store) => store.flush_append_stream(stream),
            Self::Durable(store) => store.flush_append_stream(stream),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn publish_append_stream(&self, stream: &AppendStream, mark: &DurableAppendMark) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.publish_append_stream(stream, mark, WriteDurability::Acknowledged)
            }
            Self::Durable(store) => store.publish_append_stream(stream, mark),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
        .map(|_| ())
    }

    fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Durable(store) => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
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
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn drain_persist_profiles(&self, max: usize) -> Result<Vec<DurablePersistProfile>> {
        match self {
            Self::Local(_) => Ok(Vec::new()),
            Self::Durable(store) => store.drain_persist_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_metadata_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        match self {
            Self::Txn(store) => store.drain_metadata_profiles(max),
            Self::Local(_) | Self::Durable(_) => Ok(Vec::new()),
        }
    }

    fn drain_block_write_profiles(&self, max: usize) -> Result<Vec<TxnBlockWriteProfile>> {
        match self {
            Self::Txn(store) => store.drain_block_write_profiles(max),
            Self::Local(_) | Self::Durable(_) => Ok(Vec::new()),
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
        devices: Arc<Vec<DeviceId>>,
        logical_blocks: u64,
        hot_blocks: u64,
        shard_count: usize,
        serialized_lock: Arc<Mutex<()>>,
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
    let context = setup_context(args, workload, concurrency, store)?;
    let profile_store = context.store.clone();
    let _ = profile_store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if !args.warmup.is_zero() {
        let _ = execute_load(args, workload, concurrency, context.clone(), args.warmup)?;
        let _ = profile_store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
    }
    let mut report = execute_load(args, workload, concurrency, context, args.duration)?;
    append_profile_csv(args, workload, concurrency, &profile_store)?;
    append_metadata_profile_csv(args, workload, concurrency, &profile_store)?;
    append_block_write_profile_csv(args, workload, concurrency, &profile_store)?;
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

fn append_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.durable_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(fs_error)?;
    }
    let write_header = !path.exists();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(fs_error)?;
    if write_header {
        writeln!(
            file,
            "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,total_nanos,persist_lock_wait_nanos,sqlite_lock_wait_nanos,local_snapshot_nanos,metadata_publish_lock_wait_nanos,commit_sequence_alloc_nanos,data_log_append_sync_nanos,data_log_encode_nanos,data_log_write_nanos,data_log_file_sync_nanos,data_log_dir_sync_nanos,node_catalog_publish_nanos,root_sqlite_row_sync_nanos,root_sqlite_commit_nanos,new_segment_count,new_segment_bytes,touched_node_count,logical_conflict_count,touched_shard_head_rows,touched_manifest_rows,commit_rows_written,durable_commit_high_water"
        )
        .map_err(fs_error)?;
    }
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(),
            profile.sequence,
            profile.total_nanos,
            profile.lock_wait_nanos,
            profile.sqlite_lock_wait_nanos,
            profile.local_snapshot_nanos,
            profile.metadata_publish_lock_wait_nanos,
            profile.commit_sequence_alloc_nanos,
            profile.data_log_append_sync_nanos,
            profile.data_log_encode_nanos,
            profile.data_log_write_nanos,
            profile.data_log_file_sync_nanos,
            profile.data_log_dir_sync_nanos,
            profile.node_catalog_publish_nanos,
            profile.root_sqlite_row_sync_nanos,
            profile.root_sqlite_commit_nanos,
            profile.new_segment_count,
            profile.new_segment_bytes,
            profile.touched_node_count,
            profile.logical_conflict_count,
            profile.touched_shard_head_rows,
            profile.touched_manifest_rows,
            profile.commit_rows_written,
            profile.durable_commit_high_water,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_metadata_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.metadata_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(fs_error)?;
    }
    let write_header = !path.exists();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(fs_error)?;
    if write_header {
        writeln!(
            file,
            "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,sequence,phase,total_nanos,tx_lock_wait_nanos,read_validation_nanos,apply_write_nanos,commit_version_alloc_nanos,touched_key_shards,read_key_count,write_key_count,conflict_count"
        )
        .map_err(fs_error)?;
    }
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(),
            profile.sequence,
            profile.phase,
            profile.total_nanos,
            profile.tx_lock_wait_nanos,
            profile.read_validation_nanos,
            profile.apply_write_nanos,
            profile.commit_version_alloc_nanos,
            profile.touched_key_shards,
            profile.read_key_count,
            profile.write_key_count,
            profile.conflict_count,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn append_block_write_profile_csv(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: &BenchStore,
) -> Result<()> {
    let Some(path) = &args.block_write_profile_csv else {
        return Ok(());
    };
    let profiles = store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if profiles.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(fs_error)?;
    }
    let write_header = !path.exists();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(fs_error)?;
    if write_header {
        writeln!(
            file,
            "workload,provider,durability,rtt_us,serial_rtts,concurrency,op_size,storage_nodes,payload_integrity,sequence,total_nanos,device_spec_lookup_nanos,range_split_shard_head_read_nanos,write_intent_alloc_nanos,payload_copy_nanos,segment_write_nanos,storage_node_ids_nanos,placement_select_nanos,segment_id_alloc_nanos,grant_issue_nanos,storage_node_transport_dispatch_nanos,grant_verify_nanos,catalog_duplicate_probe_nanos,catalog_duplicate_probe_lock_wait_nanos,catalog_reserve_nanos,catalog_reserve_lock_wait_nanos,catalog_begin_nanos,catalog_begin_lock_wait_nanos,segment_store_write_nanos,segment_store_lock_wait_nanos,checksum_integrity_nanos,segment_store_insert_nanos,segment_sync_nanos,segment_sync_lock_wait_nanos,receipt_create_nanos,receipt_verify_nanos,catalog_commit_nanos,catalog_commit_lock_wait_nanos,tree_path_copy_nanos,metadata_publish_call_nanos,mark_referenced_nanos,mark_reference_evidence_nanos,mark_reference_transport_dispatch_nanos,mark_reference_verify_nanos,mark_reference_catalog_nanos,mark_reference_catalog_lock_wait_nanos,touched_shard_count,segment_count,profile_storage_node_count"
        )
        .map_err(fs_error)?;
    }
    for profile in profiles {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            workload.name(),
            args.provider,
            args.durability,
            args.rtt.as_micros(),
            args.serial_rtts,
            concurrency,
            workload.op_size(),
            args.storage_nodes,
            payload_integrity_name(args.payload_integrity),
            profile.sequence,
            profile.total_nanos,
            profile.device_spec_lookup_nanos,
            profile.range_split_shard_head_read_nanos,
            profile.write_intent_alloc_nanos,
            profile.payload_copy_nanos,
            profile.segment_write_nanos,
            profile.storage_node_ids_nanos,
            profile.placement_select_nanos,
            profile.segment_id_alloc_nanos,
            profile.grant_issue_nanos,
            profile.storage_node_transport_dispatch_nanos,
            profile.grant_verify_nanos,
            profile.catalog_duplicate_probe_nanos,
            profile.catalog_duplicate_probe_lock_wait_nanos,
            profile.catalog_reserve_nanos,
            profile.catalog_reserve_lock_wait_nanos,
            profile.catalog_begin_nanos,
            profile.catalog_begin_lock_wait_nanos,
            profile.segment_store_write_nanos,
            profile.segment_store_lock_wait_nanos,
            profile.checksum_integrity_nanos,
            profile.segment_store_insert_nanos,
            profile.segment_sync_nanos,
            profile.segment_sync_lock_wait_nanos,
            profile.receipt_create_nanos,
            profile.receipt_verify_nanos,
            profile.catalog_commit_nanos,
            profile.catalog_commit_lock_wait_nanos,
            profile.tree_path_copy_nanos,
            profile.metadata_publish_call_nanos,
            profile.mark_referenced_nanos,
            profile.mark_reference_evidence_nanos,
            profile.mark_reference_transport_dispatch_nanos,
            profile.mark_reference_verify_nanos,
            profile.mark_reference_catalog_nanos,
            profile.mark_reference_catalog_lock_wait_nanos,
            profile.touched_shard_count,
            profile.segment_count,
            profile.storage_node_count,
        )
        .map_err(fs_error)?;
    }
    Ok(())
}

fn setup_context(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: BenchStore,
) -> Result<BenchContext> {
    let op_size = workload.op_size();
    let payload = Arc::new(make_payload(op_size));
    let target = if workload.is_block() {
        let device_count = if workload.is_block_device_lanes() {
            concurrency.max(1)
        } else {
            1
        };
        let mut devices = Vec::with_capacity(device_count);
        for _ in 0..device_count {
            devices.push(store.create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: args.device_blocks,
                    block_size: BLOCK_SIZE,
                },
                name: None,
            })?);
        }
        let device_id = devices[0];
        let hot_blocks = if workload.is_read() {
            seed_block_read_workload(&store, device_id, args, &payload)?
        } else {
            args.device_blocks
        };
        Target::Block {
            device_id,
            devices: Arc::new(devices),
            logical_blocks: args.device_blocks,
            hot_blocks,
            shard_count: args.shards,
            serialized_lock: Arc::new(Mutex::new(())),
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
                    WriteDurability::Flushed,
                    args.payload_integrity,
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
            args.payload_integrity,
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
        stream_flush_bytes: args.stream_flush_bytes,
        stream_publish_bytes: args.stream_publish_bytes,
        payload_integrity: args.payload_integrity,
        read_verification: args.read_verification,
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
    stream_flush_bytes: Option<u64>,
    stream_publish_bytes: Option<u64>,
    payload_integrity: PayloadIntegrity,
    read_verification: ReadVerification,
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
        prepare_for_timed_op(&context, worker, &mut state, &config)?;
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
        .and_then(|mut progress| {
            progress.merge(maybe_flush(
                &context,
                config.workload,
                config.durability,
                report.attempts + 1,
                worker,
                &mut state,
            )?);
            Ok(progress)
        });
        let elapsed = started.elapsed();
        let latency_nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        let progress = result.as_ref().copied().unwrap_or_default();
        report.record(
            latency_nanos,
            context.op_size as u64,
            progress.durable_bytes,
            progress.published_bytes,
            result.is_ok(),
            &mut rng,
        );
    }

    Ok(report)
}

#[derive(Default)]
struct WorkerState {
    stream_append: Option<StreamAppendState>,
    next_stream_file_index: Option<usize>,
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

struct StreamAppendState {
    file_index: usize,
    stream: AppendStream,
    next_offset: u64,
    durable_offset: u64,
    published_offset: u64,
    durable_mark: Option<DurableAppendMark>,
}

fn advance_stream_lane(state: &mut WorkerState, files_len: usize) {
    if let Some(stream) = state.stream_append.as_ref() {
        state.next_stream_file_index =
            Some((stream.file_index + STREAM_APPEND_FILE_STRIDE) % files_len);
    }
    state.stream_append = None;
}

fn ensure_stream_append_state(
    context: &BenchContext,
    keyspace_id: KeyspaceId,
    files: &[FileId],
    worker: u64,
    state: &mut WorkerState,
    payload_len: u64,
) -> Result<()> {
    if let Some(stream) = state.stream_append.as_ref() {
        let would_exceed = stream
            .next_offset
            .checked_add(payload_len)
            .is_none_or(|end| end > DEFAULT_FILE_CAPACITY_BYTES);
        if would_exceed {
            advance_stream_lane(state, files.len());
        }
    }
    if state.stream_append.is_none() {
        let file_index = *state
            .next_stream_file_index
            .get_or_insert_with(|| worker as usize % files.len());
        let file_id = files[file_index];
        let stream = context.store.open_append_stream(keyspace_id, file_id)?;
        let visible_base_size = stream.visible_base_size;
        state.stream_append = Some(StreamAppendState {
            file_index,
            stream,
            next_offset: visible_base_size,
            durable_offset: visible_base_size,
            published_offset: visible_base_size,
            durable_mark: None,
        });
    }
    Ok(())
}

fn prepare_for_timed_op(
    context: &BenchContext,
    worker: u64,
    state: &mut WorkerState,
    config: &WorkerConfig,
) -> Result<()> {
    if !config.workload.is_native_stream_publish_preflushed() {
        return Ok(());
    }
    let Target::Native { keyspace_id, files } = &context.target else {
        return Ok(());
    };
    let payload_len = context.payload.len() as u64;
    ensure_stream_append_state(context, *keyspace_id, files, worker, state, payload_len)?;
    let needs_preflush = state
        .stream_append
        .as_ref()
        .is_none_or(|stream| stream.durable_mark.is_none());
    if !needs_preflush {
        return Ok(());
    }
    let stream = state
        .stream_append
        .as_ref()
        .map(|stream| stream.stream.clone())
        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
    let ticket = context.store.append_stream(
        &stream,
        &context.payload,
        WriteDurability::Acknowledged,
        config.payload_integrity,
    )?;
    let mark = context.store.flush_append_stream(&stream)?;
    if let Some(stream_state) = state.stream_append.as_mut() {
        stream_state.next_offset = ticket.range.offset.saturating_add(ticket.range.len);
        stream_state.durable_offset = mark.durable_through;
        stream_state.durable_mark = Some(mark);
        state.last_native_file_index = Some(stream_state.file_index);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct OpProgress {
    durable_bytes: u64,
    published_bytes: u64,
}

impl OpProgress {
    fn merge(&mut self, other: Self) {
        self.durable_bytes = self.durable_bytes.saturating_add(other.durable_bytes);
        self.published_bytes = self.published_bytes.saturating_add(other.published_bytes);
    }
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
) -> Result<OpProgress> {
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
            context
                .store
                .write_device(
                    *device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (
            Target::Block {
                device_id,
                logical_blocks,
                shard_count,
                ..
            },
            Workload::BlockWrite4kSameShardContended,
        ) => {
            let end = logical_blocks
                .checked_div(*shard_count as u64)
                .ok_or_else(|| StorageError::invalid_argument("shard count is zero"))?
                .max(1);
            let block = rng.below(end);
            context
                .store
                .write_device(
                    *device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (
            Target::Block {
                device_id,
                logical_blocks,
                shard_count,
                serialized_lock,
                ..
            },
            Workload::BlockWrite4kSameShardSerialized,
        ) => {
            let _guard = serialized_lock
                .lock()
                .map_err(|_| StorageError::unavailable("serialized block lane lock poisoned"))?;
            let end = logical_blocks
                .checked_div(*shard_count as u64)
                .ok_or_else(|| StorageError::invalid_argument("shard count is zero"))?
                .max(1);
            let block = rng.below(end);
            context
                .store
                .write_device(
                    *device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
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
            context
                .store
                .write_device(
                    *device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (
            Target::Block {
                devices,
                logical_blocks,
                ..
            },
            Workload::BlockWrite4kDeviceLanes,
        ) => {
            let device_id = devices[worker as usize % devices.len()];
            let block = rng.below(*logical_blocks);
            context
                .store
                .write_device(
                    device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
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
            context
                .store
                .write_device(
                    *device_id,
                    start * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (
            Target::Block {
                device_id,
                logical_blocks,
                shard_count,
                ..
            },
            Workload::BlockWrite1mShardLanes,
        ) => {
            let blocks = (context.op_size as u64) / u64::from(BLOCK_SIZE);
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
            let span = end.saturating_sub(start);
            let block = start + rng.below(span.saturating_sub(blocks).saturating_add(1));
            context
                .store
                .write_device(
                    *device_id,
                    block * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (
            Target::Block {
                devices,
                logical_blocks,
                ..
            },
            Workload::BlockWrite1mDeviceLanes,
        ) => {
            let device_id = devices[worker as usize % devices.len()];
            let blocks = (context.op_size as u64) / u64::from(BLOCK_SIZE);
            let start = rng.below(logical_blocks.saturating_sub(blocks).saturating_add(1));
            context
                .store
                .write_device(
                    device_id,
                    start * u64::from(BLOCK_SIZE),
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
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
            context
                .store
                .read_device(
                    *device_id,
                    ByteRange::new(block * u64::from(BLOCK_SIZE), context.op_size as u64),
                    read_buf,
                    config.read_verification,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_write() => {
            let file_index = state.next_partitioned_file_index(worker, concurrency, files.len());
            let file_id = files[file_index];
            context
                .store
                .write_file_at(
                    *keyspace_id,
                    file_id,
                    0,
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, Workload::NativeRead4k) => {
            let file_id = files[rng.below(files.len() as u64) as usize];
            context
                .store
                .read_file(
                    *keyspace_id,
                    file_id,
                    ByteRange::new(0, context.op_size as u64),
                    read_buf,
                    config.read_verification,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_append() => {
            let file_index = state.next_partitioned_file_index(worker, concurrency, files.len());
            let file_id = files[file_index];
            context
                .store
                .append_file_once(
                    *keyspace_id,
                    file_id,
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_stream() => {
            let payload_len = context.payload.len() as u64;
            for _ in 0..files.len() {
                let mut progress = OpProgress::default();
                ensure_stream_append_state(
                    context,
                    *keyspace_id,
                    files,
                    worker,
                    state,
                    payload_len,
                )?;
                if workload.is_native_stream_publish_preflushed() {
                    let stream = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.stream.clone())
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    let mark = state
                        .stream_append
                        .as_ref()
                        .and_then(|stream| stream.durable_mark.clone())
                        .ok_or_else(|| {
                            StorageError::conflict("append stream has no durable mark")
                        })?;
                    let previous_published = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.published_offset)
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    context.store.publish_append_stream(&stream, &mark)?;
                    progress.published_bytes = progress
                        .published_bytes
                        .saturating_add(mark.durable_through.saturating_sub(previous_published));
                    advance_stream_lane(state, files.len());
                    return Ok(progress);
                }
                let stream = state
                    .stream_append
                    .as_ref()
                    .map(|stream| &stream.stream)
                    .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                let ticket = match context.store.append_stream(
                    stream,
                    &context.payload,
                    WriteDurability::Acknowledged,
                    config.payload_integrity,
                ) {
                    Ok(ticket) => ticket,
                    Err(error) => {
                        advance_stream_lane(state, files.len());
                        return Err(error);
                    }
                };
                let next_offset = ticket.range.offset.saturating_add(ticket.range.len);
                if next_offset > DEFAULT_FILE_CAPACITY_BYTES {
                    advance_stream_lane(state, files.len());
                    continue;
                }
                if let Some(stream) = state.stream_append.as_mut() {
                    stream.next_offset = next_offset;
                    stream.durable_mark = None;
                    state.last_native_file_index = Some(stream.file_index);
                }
                let threshold_flush = state
                    .stream_append
                    .as_ref()
                    .zip(config.stream_flush_bytes)
                    .is_some_and(|(stream, threshold)| {
                        stream.next_offset.saturating_sub(stream.durable_offset) >= threshold
                    });
                if workload.is_native_stream_append_flush()
                    || workload.is_native_stream_flush_publish()
                    || threshold_flush
                {
                    let stream = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.stream.clone())
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    let previous_durable = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.durable_offset)
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    let mark = context.store.flush_append_stream(&stream)?;
                    if let Some(stream_state) = state.stream_append.as_mut() {
                        progress.durable_bytes = progress
                            .durable_bytes
                            .saturating_add(mark.durable_through.saturating_sub(previous_durable));
                        stream_state.durable_offset = mark.durable_through;
                        stream_state.durable_mark = Some(mark);
                    }
                }
                let threshold_publish = state
                    .stream_append
                    .as_ref()
                    .zip(config.stream_publish_bytes)
                    .is_some_and(|(stream, threshold)| {
                        stream
                            .durable_offset
                            .saturating_sub(stream.published_offset)
                            >= threshold
                    });
                if workload.is_native_stream_flush_publish() || threshold_publish {
                    let stream = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.stream.clone())
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    let mark = state
                        .stream_append
                        .as_ref()
                        .and_then(|stream| stream.durable_mark.clone())
                        .ok_or_else(|| {
                            StorageError::conflict("append stream has no durable mark")
                        })?;
                    let previous_published = state
                        .stream_append
                        .as_ref()
                        .map(|stream| stream.published_offset)
                        .ok_or_else(|| StorageError::conflict("append stream state missing"))?;
                    context.store.publish_append_stream(&stream, &mark)?;
                    if let Some(stream_state) = state.stream_append.as_mut() {
                        progress.published_bytes = progress.published_bytes.saturating_add(
                            mark.durable_through.saturating_sub(previous_published),
                        );
                        stream_state.published_offset = mark.durable_through;
                    }
                    if workload.is_native_stream_flush_publish() {
                        advance_stream_lane(state, files.len());
                    }
                }
                return Ok(progress);
            }
            Err(StorageError::conflict(
                "append-stream benchmark exhausted every file lane",
            ))
        }
        (Target::Native { keyspace_id, files }, Workload::NativeHotAppend4k) => {
            let file_id = files[0];
            state.last_native_file_index = Some(0);
            context
                .store
                .append_file_once(
                    *keyspace_id,
                    file_id,
                    &context.payload,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
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
    state: &mut WorkerState,
) -> Result<OpProgress> {
    let DurabilityMode::AckFlushEvery(every) = durability else {
        return Ok(OpProgress::default());
    };
    if !attempts_after_op.is_multiple_of(every) {
        return Ok(OpProgress::default());
    }

    match &context.target {
        Target::Block {
            device_id, devices, ..
        } => {
            let flush_device = if workload.is_block_device_lanes() {
                devices[worker as usize % devices.len()]
            } else {
                *device_id
            };
            context
                .store
                .flush_device(flush_device)
                .map(|_| OpProgress::default())
        }
        Target::Native { .. } if workload.is_native_stream_ingest() => {
            if let Some(stream_state) = state.stream_append.as_mut() {
                let previous_durable = stream_state.durable_offset;
                let mark = context.store.flush_append_stream(&stream_state.stream)?;
                let durable_through = mark.durable_through;
                stream_state.durable_offset = durable_through;
                stream_state.durable_mark = Some(mark);
                return Ok(OpProgress {
                    durable_bytes: durable_through.saturating_sub(previous_durable),
                    published_bytes: 0,
                });
            }
            Ok(OpProgress::default())
        }
        Target::Native { keyspace_id, files } => {
            let file_id = if matches!(workload, Workload::NativeHotAppend4k) {
                files[0]
            } else {
                files[state
                    .last_native_file_index
                    .unwrap_or(worker as usize % files.len())]
            };
            context
                .store
                .flush_file(*keyspace_id, file_id)
                .map(|_| OpProgress::default())
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
    durable_bytes: u64,
    published_bytes: u64,
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
            durable_bytes: 0,
            published_bytes: 0,
            max_latency_nanos: 0,
            latency_seen: 0,
            latencies: Vec::with_capacity(sample_limit.min(1024)),
            sample_limit,
        }
    }

    fn record(
        &mut self,
        latency_nanos: u64,
        bytes: u64,
        durable_bytes: u64,
        published_bytes: u64,
        success: bool,
        rng: &mut Lcg,
    ) {
        self.attempts = self.attempts.saturating_add(1);
        self.latency_seen = self.latency_seen.saturating_add(1);
        self.max_latency_nanos = self.max_latency_nanos.max(latency_nanos);
        if success {
            self.successes = self.successes.saturating_add(1);
            self.bytes = self.bytes.saturating_add(bytes);
            self.durable_bytes = self.durable_bytes.saturating_add(durable_bytes);
            self.published_bytes = self.published_bytes.saturating_add(published_bytes);
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

        for worker in workers {
            attempts = attempts.saturating_add(worker.attempts);
            successes = successes.saturating_add(worker.successes);
            errors = errors.saturating_add(worker.errors);
            bytes = bytes.saturating_add(worker.bytes);
            durable_bytes = durable_bytes.saturating_add(worker.durable_bytes);
            published_bytes = published_bytes.saturating_add(worker.published_bytes);
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
            durable_bytes,
            published_bytes,
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

    #[test]
    fn append_stream_suite_uses_explicit_ingest_flush_and_publish_names() {
        let suite = Workload::append_stream_suite();
        assert!(suite.contains(&Workload::NativeStreamIngest1m));
        assert!(suite.contains(&Workload::NativeStreamAppendFlush1m));
        assert!(suite.contains(&Workload::NativeStreamPublishPreflushed1m));
        assert!(suite.contains(&Workload::NativeStreamFlushPublish1m));
        assert!(Workload::from_str("native-stream-append-1m").is_err());
        assert!(Workload::from_str("native-stream-publish-1m").is_err());
    }

    #[test]
    fn integrity_flags_parse_explicit_modes() {
        assert_eq!(
            parse_payload_integrity("unchecked").unwrap(),
            PayloadIntegrity::Unchecked
        );
        assert_eq!(
            parse_read_verification("require-verified").unwrap(),
            ReadVerification::RequireVerified
        );
        assert!(parse_payload_integrity("maybe").is_err());
        assert!(parse_read_verification("maybe").is_err());
    }

    #[test]
    fn bench_report_aggregates_durable_and_published_bytes() {
        let mut first = WorkerReport::new(8);
        let mut second = WorkerReport::new(8);
        let mut rng = Lcg::new(1);

        first.record(10, 100, 64, 32, true, &mut rng);
        second.record(20, 200, 128, 96, true, &mut rng);

        let report = BenchReport::from_workers(Duration::from_secs(1), vec![first, second]);
        assert_eq!(report.bytes, 300);
        assert_eq!(report.durable_bytes, 192);
        assert_eq!(report.published_bytes, 128);
    }
}
