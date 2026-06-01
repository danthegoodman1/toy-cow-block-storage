use std::env;
use std::fs::{self, OpenOptions};
use std::hint::black_box;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const MIB: usize = 1024 * 1024;
const RECORD_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + 8;
const LOG_WRITER_PAYLOAD_MIB: usize = 1;
const LOG_WRITER_SYNC_GROUP_MIB: usize = 32;

#[derive(Debug, Clone, Copy)]
enum Mode {
    Raw,
    StreamCrc32c,
    FramedCopy,
    FramedCrc32c,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::StreamCrc32c => "stream-crc32c",
            Self::FramedCopy => "framed-copy",
            Self::FramedCrc32c => "framed-crc32c",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LogWriterShape {
    CurrentStreamFiles,
    HotStorageNodeFd,
}

impl LogWriterShape {
    fn name(self) -> &'static str {
        match self {
            Self::CurrentStreamFiles => "current-stream-files",
            Self::HotStorageNodeFd => "hot-storage-node-fd",
        }
    }
}

#[derive(Debug)]
struct ProbeResult {
    path: PathBuf,
    mode: Mode,
    concurrency: usize,
    group_mib: usize,
    total_mib: usize,
    seconds: f64,
    mbps: f64,
    write_p50_ms: f64,
    write_p99_ms: f64,
    write_max_ms: f64,
    sync_p50_ms: f64,
    sync_p99_ms: f64,
    sync_max_ms: f64,
    group_p50_ms: f64,
    group_p99_ms: f64,
    group_max_ms: f64,
}

#[derive(Debug, Default)]
struct WorkerResult {
    open_ns: Vec<u128>,
    write_ns: Vec<u128>,
    sync_ns: Vec<u128>,
    group_ns: Vec<u128>,
    checksum_xor: u64,
}

#[derive(Debug)]
struct LogWriterProbeResult {
    path: PathBuf,
    shape: LogWriterShape,
    mode: Mode,
    concurrency: usize,
    storage_nodes: usize,
    payload_mib: usize,
    sync_group_mib: usize,
    rounds: usize,
    total_mib: usize,
    seconds: f64,
    mbps: f64,
    open_p50_ms: f64,
    open_p99_ms: f64,
    open_max_ms: f64,
    write_p50_ms: f64,
    write_p99_ms: f64,
    write_max_ms: f64,
    sync_p50_ms: f64,
    sync_p99_ms: f64,
    sync_max_ms: f64,
    op_p50_ms: f64,
    op_p99_ms: f64,
    op_max_ms: f64,
    checksum_xor: u64,
}

fn main() -> io::Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--log-writer") {
        let root = args
            .get(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/toy-cow-block-storage-log-writer-probe"));
        let rounds = args
            .get(2)
            .map(|value| {
                value.parse::<usize>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid rounds value {value:?}: {error}"),
                    )
                })
            })
            .transpose()?
            .unwrap_or(64);
        let storage_nodes = args
            .get(3)
            .map(|value| {
                value.parse::<usize>().map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid storage node count {value:?}: {error}"),
                    )
                })
            })
            .transpose()?
            .unwrap_or(4);
        return run_log_writer_suite(&root, rounds, storage_nodes);
    }

    let root = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/toy-cow-block-storage-diskprobe"));
    fs::create_dir_all(&root)?;

    println!(
        "path,mode,concurrency,group_mib,total_mib,seconds,mbps,write_p50_ms,write_p99_ms,write_max_ms,sync_p50_ms,sync_p99_ms,sync_max_ms,group_p50_ms,group_p99_ms,group_max_ms"
    );

    for mode in [Mode::Raw, Mode::FramedCopy, Mode::FramedCrc32c] {
        for group_mib in [1, 4, 16, 32] {
            print_result(run_probe(&root, mode, 1, group_mib, 512)?);
        }
    }

    for mode in [Mode::Raw, Mode::FramedCopy, Mode::FramedCrc32c] {
        for concurrency in [4, 16] {
            print_result(run_probe(&root, mode, concurrency, 16, 128)?);
            print_result(run_probe(&root, mode, concurrency, 32, 128)?);
        }
    }

    Ok(())
}

fn print_result(result: ProbeResult) {
    println!(
        "{},{},{},{},{},{:.6},{:.2},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
        result.path.display(),
        result.mode.name(),
        result.concurrency,
        result.group_mib,
        result.total_mib,
        result.seconds,
        result.mbps,
        result.write_p50_ms,
        result.write_p99_ms,
        result.write_max_ms,
        result.sync_p50_ms,
        result.sync_p99_ms,
        result.sync_max_ms,
        result.group_p50_ms,
        result.group_p99_ms,
        result.group_max_ms
    );
}

fn run_probe(
    root: &Path,
    mode: Mode,
    concurrency: usize,
    group_mib: usize,
    total_mib_per_worker: usize,
) -> io::Result<ProbeResult> {
    let dir = root.join(format!("{}-c{}-g{}", mode.name(), concurrency, group_mib));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;

    let started = Instant::now();
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let path = dir.join(format!("worker-{worker}.log"));
        workers.push(thread::spawn(move || {
            run_worker(&path, mode, group_mib, total_mib_per_worker)
        }));
    }

    let mut write_ns = Vec::new();
    let mut sync_ns = Vec::new();
    let mut group_ns = Vec::new();
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| io::Error::other("diskprobe worker panicked"))??;
        write_ns.extend(result.write_ns);
        sync_ns.extend(result.sync_ns);
        group_ns.extend(result.group_ns);
    }
    let seconds = started.elapsed().as_secs_f64();
    let total_mib = total_mib_per_worker
        .checked_mul(concurrency)
        .ok_or_else(|| io::Error::other("total MiB overflow"))?;
    let mbps = total_mib as f64 / seconds;

    Ok(ProbeResult {
        path: dir,
        mode,
        concurrency,
        group_mib,
        total_mib,
        seconds,
        mbps,
        write_p50_ms: percentile_ms(&write_ns, 0.50),
        write_p99_ms: percentile_ms(&write_ns, 0.99),
        write_max_ms: percentile_ms(&write_ns, 1.00),
        sync_p50_ms: percentile_ms(&sync_ns, 0.50),
        sync_p99_ms: percentile_ms(&sync_ns, 0.99),
        sync_max_ms: percentile_ms(&sync_ns, 1.00),
        group_p50_ms: percentile_ms(&group_ns, 0.50),
        group_p99_ms: percentile_ms(&group_ns, 0.99),
        group_max_ms: percentile_ms(&group_ns, 1.00),
    })
}

fn run_worker(
    path: &Path,
    mode: Mode,
    group_mib: usize,
    total_mib: usize,
) -> io::Result<WorkerResult> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let payload = make_payload(MIB);
    let mut write_ns = Vec::new();
    let mut sync_ns = Vec::new();
    let mut group_ns = Vec::new();
    let open_ns = Vec::new();
    let groups = total_mib / group_mib;
    let mut checksum_xor = 0_u64;
    for group in 0..groups {
        let group_started = Instant::now();
        let write_started = Instant::now();
        for record in 0..group_mib {
            checksum_xor ^=
                write_probe_record(&mut file, mode, &payload, group, group_mib, record)?;
        }
        write_ns.push(write_started.elapsed().as_nanos());
        let sync_started = Instant::now();
        file.sync_data()?;
        sync_ns.push(sync_started.elapsed().as_nanos());
        group_ns.push(group_started.elapsed().as_nanos());
    }
    Ok(WorkerResult {
        open_ns,
        write_ns,
        sync_ns,
        group_ns,
        checksum_xor,
    })
}

fn run_log_writer_suite(root: &Path, rounds: usize, storage_nodes: usize) -> io::Result<()> {
    fs::create_dir_all(root)?;
    println!(
        "path,shape,mode,concurrency,storage_nodes,payload_mib,sync_group_mib,rounds,total_mib,seconds,mbps,open_p50_ms,open_p99_ms,open_max_ms,write_p50_ms,write_p99_ms,write_max_ms,sync_p50_ms,sync_p99_ms,sync_max_ms,op_p50_ms,op_p99_ms,op_max_ms,checksum_xor"
    );

    for mode in [Mode::Raw, Mode::StreamCrc32c] {
        for concurrency in [1, 4, 16] {
            print_log_writer_result(run_log_writer_probe(
                root,
                LogWriterShape::CurrentStreamFiles,
                mode,
                concurrency,
                rounds,
                storage_nodes,
            )?);
            print_log_writer_result(run_log_writer_probe(
                root,
                LogWriterShape::HotStorageNodeFd,
                mode,
                concurrency,
                rounds,
                storage_nodes,
            )?);
        }
    }

    Ok(())
}

fn print_log_writer_result(result: LogWriterProbeResult) {
    println!(
        "{},{},{},{},{},{},{},{},{},{:.6},{:.2},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{}",
        result.path.display(),
        result.shape.name(),
        result.mode.name(),
        result.concurrency,
        result.storage_nodes,
        result.payload_mib,
        result.sync_group_mib,
        result.rounds,
        result.total_mib,
        result.seconds,
        result.mbps,
        result.open_p50_ms,
        result.open_p99_ms,
        result.open_max_ms,
        result.write_p50_ms,
        result.write_p99_ms,
        result.write_max_ms,
        result.sync_p50_ms,
        result.sync_p99_ms,
        result.sync_max_ms,
        result.op_p50_ms,
        result.op_p99_ms,
        result.op_max_ms,
        result.checksum_xor
    );
}

fn run_log_writer_probe(
    root: &Path,
    shape: LogWriterShape,
    mode: Mode,
    concurrency: usize,
    rounds: usize,
    storage_nodes: usize,
) -> io::Result<LogWriterProbeResult> {
    let dir = root.join(format!(
        "{}-{}-c{}-n{}-r{}",
        shape.name(),
        mode.name(),
        concurrency,
        storage_nodes,
        rounds
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir)?;

    let started = Instant::now();
    let result = match shape {
        LogWriterShape::CurrentStreamFiles => {
            run_current_stream_files_probe(&dir, mode, concurrency, rounds, storage_nodes)?
        }
        LogWriterShape::HotStorageNodeFd => {
            run_hot_storage_node_fd_probe(&dir, mode, concurrency, rounds, storage_nodes)?
        }
    };
    let seconds = started.elapsed().as_secs_f64();
    let total_mib = LOG_WRITER_PAYLOAD_MIB
        .checked_mul(rounds)
        .and_then(|value| value.checked_mul(concurrency))
        .ok_or_else(|| io::Error::other("total MiB overflow"))?;
    let mbps = total_mib as f64 / seconds;

    Ok(LogWriterProbeResult {
        path: dir,
        shape,
        mode,
        concurrency,
        storage_nodes,
        payload_mib: LOG_WRITER_PAYLOAD_MIB,
        sync_group_mib: LOG_WRITER_SYNC_GROUP_MIB,
        rounds,
        total_mib,
        seconds,
        mbps,
        open_p50_ms: percentile_ms(&result.open_ns, 0.50),
        open_p99_ms: percentile_ms(&result.open_ns, 0.99),
        open_max_ms: percentile_ms(&result.open_ns, 1.00),
        write_p50_ms: percentile_ms(&result.write_ns, 0.50),
        write_p99_ms: percentile_ms(&result.write_ns, 0.99),
        write_max_ms: percentile_ms(&result.write_ns, 1.00),
        sync_p50_ms: percentile_ms(&result.sync_ns, 0.50),
        sync_p99_ms: percentile_ms(&result.sync_ns, 0.99),
        sync_max_ms: percentile_ms(&result.sync_ns, 1.00),
        op_p50_ms: percentile_ms(&result.group_ns, 0.50),
        op_p99_ms: percentile_ms(&result.group_ns, 0.99),
        op_max_ms: percentile_ms(&result.group_ns, 1.00),
        checksum_xor: result.checksum_xor,
    })
}

fn run_current_stream_files_probe(
    dir: &Path,
    mode: Mode,
    concurrency: usize,
    rounds: usize,
    storage_nodes: usize,
) -> io::Result<WorkerResult> {
    if storage_nodes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "storage node count must be greater than zero",
        ));
    }
    let payload = Arc::new(make_payload(LOG_WRITER_PAYLOAD_MIB * MIB));
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let storage_node = worker % storage_nodes;
        let node_dir = dir.join(format!("node-{storage_node}"));
        fs::create_dir_all(&node_dir)?;
        let path = node_dir.join(format!("stream-state-{worker}.log"));
        let payload = Arc::clone(&payload);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            run_current_stream_file_worker(&path, mode, payload, barrier, worker, rounds)
        }));
    }

    let mut merged = WorkerResult::default();
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| io::Error::other("log-writer probe worker panicked"))??;
        merged.merge(result);
    }
    Ok(merged)
}

fn run_current_stream_file_worker(
    path: &Path,
    mode: Mode,
    payload: Arc<Vec<u8>>,
    barrier: Arc<Barrier>,
    worker: usize,
    rounds: usize,
) -> io::Result<WorkerResult> {
    let mut result = WorkerResult::default();
    let mut offset = 0_u64;
    for round in 0..rounds {
        barrier.wait();
        let op_started = Instant::now();

        let open_started = Instant::now();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        result.open_ns.push(open_started.elapsed().as_nanos());

        let write_started = Instant::now();
        file.seek(SeekFrom::Start(offset))?;
        result.checksum_xor ^=
            write_probe_record(&mut file, mode, &payload, round, rounds, worker)?;
        offset = file.stream_position()?;
        result.write_ns.push(write_started.elapsed().as_nanos());
        drop(file);

        let sync_started = Instant::now();
        OpenOptions::new().read(true).open(path)?.sync_data()?;
        result.sync_ns.push(sync_started.elapsed().as_nanos());
        result.group_ns.push(op_started.elapsed().as_nanos());
    }
    Ok(result)
}

fn run_hot_storage_node_fd_probe(
    dir: &Path,
    mode: Mode,
    concurrency: usize,
    rounds: usize,
    storage_nodes: usize,
) -> io::Result<WorkerResult> {
    if storage_nodes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "storage node count must be greater than zero",
        ));
    }
    let payload = make_payload(LOG_WRITER_PAYLOAD_MIB * MIB);
    let mut files = Vec::with_capacity(storage_nodes);
    for storage_node in 0..storage_nodes {
        let node_dir = dir.join(format!("node-{storage_node}"));
        fs::create_dir_all(&node_dir)?;
        let path = node_dir.join("storage-node.log");
        files.push(
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(path)?,
        );
    }
    let mut result = WorkerResult::default();

    let max_records_per_sync = LOG_WRITER_SYNC_GROUP_MIB / LOG_WRITER_PAYLOAD_MIB;
    for round in 0..rounds {
        let mut worker_start = 0;
        while worker_start < concurrency {
            let worker_end = concurrency.min(worker_start + max_records_per_sync);
            let mut batches = vec![Vec::new(); storage_nodes];
            for worker in worker_start..worker_end {
                batches[worker % storage_nodes].push(worker);
            }
            let op_started = Instant::now();

            thread::scope(|scope| -> io::Result<()> {
                let mut handles = Vec::new();
                for (file, batch) in files.iter_mut().zip(batches.iter()) {
                    if batch.is_empty() {
                        continue;
                    }
                    let payload = &payload;
                    handles.push(scope.spawn(move || -> io::Result<(u128, u128, u64)> {
                        let mut checksum_xor = 0_u64;
                        let write_started = Instant::now();
                        for worker in batch.iter().copied() {
                            checksum_xor ^=
                                write_probe_record(file, mode, payload, round, rounds, worker)?;
                        }
                        let write_ns = write_started.elapsed().as_nanos();

                        let sync_started = Instant::now();
                        file.sync_data()?;
                        let sync_ns = sync_started.elapsed().as_nanos();
                        Ok((write_ns, sync_ns, checksum_xor))
                    }));
                }
                for handle in handles {
                    let (write_ns, sync_ns, checksum_xor) = handle
                        .join()
                        .map_err(|_| io::Error::other("hot log writer worker panicked"))??;
                    result.write_ns.push(write_ns);
                    result.sync_ns.push(sync_ns);
                    result.checksum_xor ^= checksum_xor;
                }
                Ok(())
            })?;

            let op_nanos = op_started.elapsed().as_nanos();
            result
                .group_ns
                .extend(std::iter::repeat_n(op_nanos, worker_end - worker_start));
            worker_start = worker_end;
        }
    }
    Ok(result)
}

impl WorkerResult {
    fn merge(&mut self, other: Self) {
        self.open_ns.extend(other.open_ns);
        self.write_ns.extend(other.write_ns);
        self.sync_ns.extend(other.sync_ns);
        self.group_ns.extend(other.group_ns);
        self.checksum_xor ^= other.checksum_xor;
    }
}

fn write_probe_record(
    file: &mut impl Write,
    mode: Mode,
    payload: &[u8],
    group: usize,
    group_mib: usize,
    record: usize,
) -> io::Result<u64> {
    match mode {
        Mode::Raw => {
            file.write_all(payload)?;
            Ok(0)
        }
        Mode::StreamCrc32c => {
            let checksum = u64::from(crc32c::crc32c(payload));
            black_box(checksum);
            file.write_all(payload)?;
            Ok(checksum)
        }
        Mode::FramedCopy | Mode::FramedCrc32c => {
            let checksum = match mode {
                Mode::Raw | Mode::StreamCrc32c | Mode::FramedCopy => 0,
                Mode::FramedCrc32c => u64::from(crc32c::crc32c(payload)),
            };
            let mut framed = Vec::with_capacity(RECORD_HEADER_BYTES + payload.len());
            framed.extend_from_slice(b"TCOWDAT!");
            framed.extend_from_slice(&1_u16.to_be_bytes());
            let segment_id = (group
                .checked_mul(group_mib)
                .and_then(|base| base.checked_add(record))
                .ok_or_else(|| io::Error::other("segment id overflow"))?)
                as u128;
            framed.extend_from_slice(&segment_id.to_be_bytes());
            framed.extend_from_slice(&(payload.len() as u64).to_be_bytes());
            framed.extend_from_slice(&checksum.to_be_bytes());
            framed.extend_from_slice(payload);
            black_box(checksum);
            file.write_all(&framed)?;
            Ok(checksum)
        }
    }
}

fn percentile_ms(samples: &[u128], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = if percentile >= 1.0 {
        sorted.len() - 1
    } else {
        ((sorted.len() - 1) as f64 * percentile).floor() as usize
    };
    sorted[index] as f64 / 1_000_000.0
}

fn make_payload(bytes: usize) -> Vec<u8> {
    (0..bytes)
        .map(|index| (index as u8).wrapping_mul(31))
        .collect()
}
