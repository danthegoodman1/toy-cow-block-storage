use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

const MIB: usize = 1024 * 1024;
const RECORD_HEADER_BYTES: usize = 8 + 2 + 16 + 8 + 8;

#[derive(Debug, Clone, Copy)]
enum Mode {
    Raw,
    FramedCopy,
    FramedCrc32c,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::FramedCopy => "framed-copy",
            Self::FramedCrc32c => "framed-crc32c",
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

#[derive(Debug)]
struct WorkerResult {
    write_ns: Vec<u128>,
    sync_ns: Vec<u128>,
    group_ns: Vec<u128>,
}

fn main() -> io::Result<()> {
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
    let groups = total_mib / group_mib;
    for group in 0..groups {
        let group_started = Instant::now();
        let write_started = Instant::now();
        for record in 0..group_mib {
            match mode {
                Mode::Raw => file.write_all(&payload)?,
                Mode::FramedCopy | Mode::FramedCrc32c => {
                    let checksum = match mode {
                        Mode::Raw | Mode::FramedCopy => 0,
                        Mode::FramedCrc32c => u64::from(crc32c::crc32c(&payload)),
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
                    framed.extend_from_slice(&payload);
                    file.write_all(&framed)?;
                }
            }
        }
        write_ns.push(write_started.elapsed().as_nanos());
        let sync_started = Instant::now();
        file.sync_data()?;
        sync_ns.push(sync_started.elapsed().as_nanos());
        group_ns.push(group_started.elapsed().as_nanos());
    }
    Ok(WorkerResult {
        write_ns,
        sync_ns,
        group_ns,
    })
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
