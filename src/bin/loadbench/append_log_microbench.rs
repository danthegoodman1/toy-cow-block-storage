#[derive(Debug, Clone)]
struct AppendLogMicrobenchProfile {
    strategy: &'static str,
    total_nanos: u64,
    append_nanos: u64,
    file_sync_nanos: u64,
    file_sync_sum_nanos: u64,
    file_sync_max_nanos: u64,
    dir_sync_nanos: u64,
    bytes_written: u64,
    sync_bytes: u64,
    append_record_count: u64,
    estimated_run_count: u64,
    files_synced: u64,
    dirs_synced: u64,
    storage_nodes: u64,
    stream_count: u64,
    max_file_bytes: u64,
    target_data_log_bytes: u64,
}

#[derive(Debug)]
struct AppendLogMicrobenchWorkerOutput {
    worker: u64,
    report: WorkerReport,
    files: Vec<PathBuf>,
    dirs: Vec<PathBuf>,
    bytes_written: u64,
    append_record_count: u64,
    estimated_run_count: u64,
    max_file_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct AppendLogAllocation {
    log_id: u64,
    offset: u64,
    end: u64,
}

#[derive(Debug)]
struct AppendLogNodeLane {
    dir: PathBuf,
    target_data_log_bytes: u64,
    state: Mutex<AppendLogNodeLaneState>,
}

#[derive(Debug)]
struct AppendLogNodeLaneState {
    log_id: u64,
    total_bytes: u64,
    files: Vec<PathBuf>,
    max_file_bytes: u64,
}

impl AppendLogNodeLane {
    fn new(root: &Path, storage_node: usize, target_data_log_bytes: u64) -> Self {
        Self {
            dir: root.join(format!("node-{storage_node}")),
            target_data_log_bytes,
            state: Mutex::new(AppendLogNodeLaneState {
                log_id: 1,
                total_bytes: 0,
                files: Vec::new(),
                max_file_bytes: 0,
            }),
        }
    }

    fn append(&self, payload: &[u8]) -> Result<AppendLogAllocation> {
        let record_len = u64::try_from(payload.len())
            .map_err(|_| StorageError::invalid_argument("append-log payload overflows u64"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| StorageError::unavailable("append-log node lane lock poisoned"))?;
        if state.total_bytes != 0
            && state
                .total_bytes
                .checked_add(record_len)
                .ok_or_else(|| StorageError::invalid_argument("append-log size overflows"))?
                > self.target_data_log_bytes
        {
            state.log_id = state
                .log_id
                .checked_add(1)
                .ok_or_else(|| StorageError::invalid_argument("append-log id overflows"))?;
            state.total_bytes = 0;
        }

        fs::create_dir_all(&self.dir).map_err(fs_error)?;
        let path = self.dir.join(format!("log-{}.dat", state.log_id));
        let existed = path.exists();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(fs_error)?;
        let offset = state.total_bytes;
        file.write_all(payload).map_err(fs_error)?;
        let end = offset
            .checked_add(record_len)
            .ok_or_else(|| StorageError::invalid_argument("append-log offset overflows"))?;
        state.total_bytes = end;
        state.max_file_bytes = state.max_file_bytes.max(end);
        if !existed {
            state.files.push(path);
        }
        Ok(AppendLogAllocation {
            log_id: state.log_id,
            offset,
            end,
        })
    }
}

fn execute_append_log_microbench_load(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    context: BenchContext,
) -> Result<BenchReport> {
    let total_bytes = validate_append_log_microbench_workload(workload, args)?;
    let Target::AppendLogMicrobench { root } = &context.target else {
        return Err(StorageError::invalid_argument(
            "append-log microbench requires append-log target",
        ));
    };
    let _ = fs::remove_dir_all(root.as_ref());
    fs::create_dir_all(root.as_ref()).map_err(fs_error)?;

    let started = Instant::now();
    let append_started = Instant::now();
    let node_lanes = if workload.is_append_log_microbench_node_shared() {
        Some(Arc::new(
            (0..args.storage_nodes)
                .map(|node| AppendLogNodeLane::new(root.as_ref(), node, args.target_data_log_bytes))
                .map(Arc::new)
                .collect::<Vec<_>>(),
        ))
    } else {
        None
    };

    let outputs = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let payload = Arc::clone(&context.payload);
            let root = Arc::clone(root);
            let node_lanes = node_lanes.clone();
            handles.push(scope.spawn(move || {
                run_append_log_microbench_worker(
                    workload,
                    worker as u64,
                    concurrency,
                    args.storage_nodes,
                    args.samples_per_worker,
                    args.target_data_log_bytes,
                    total_bytes,
                    payload,
                    root,
                    node_lanes,
                )
            }));
        }

        let mut outputs = Vec::with_capacity(concurrency);
        for handle in handles {
            outputs.push(
                handle
                    .join()
                    .map_err(|_| StorageError::unavailable("append-log worker panicked"))??,
            );
        }
        Ok::<_, StorageError>(outputs)
    })?;
    let append_nanos = elapsed_nanos_u64(append_started);

    let mut files = Vec::new();
    let mut dirs = Vec::new();
    let mut bytes_written = 0_u64;
    let mut append_record_count = 0_u64;
    let mut estimated_run_count = 0_u64;
    let mut max_file_bytes = 0_u64;
    let mut reports = Vec::with_capacity(outputs.len());
    for output in outputs {
        files.extend(output.files);
        dirs.extend(output.dirs);
        bytes_written = bytes_written.saturating_add(output.bytes_written);
        append_record_count = append_record_count.saturating_add(output.append_record_count);
        estimated_run_count = estimated_run_count.saturating_add(output.estimated_run_count);
        max_file_bytes = max_file_bytes.max(output.max_file_bytes);
        reports.push((output.worker, output.report, output.bytes_written));
    }
    dedup_paths(&mut files);
    dedup_paths(&mut dirs);

    let sync_started = Instant::now();
    let file_sync = sync_append_log_files(files.clone())?;
    let file_sync_nanos = elapsed_nanos_u64(sync_started);
    let dir_sync_started = Instant::now();
    for dir in &dirs {
        sync_dir_path(dir)?;
    }
    let dir_sync_nanos = elapsed_nanos_u64(dir_sync_started);
    let sync_nanos = elapsed_nanos_u64(sync_started);

    let mut worker_reports = Vec::with_capacity(reports.len());
    for (worker, mut report, worker_bytes) in reports {
        let mut rng = Lcg::new(0x94d0_49bb_1331_11eb_u64 ^ worker);
        report.record_stream_publish(
            sync_nanos,
            0,
            OpProgress {
                durable_bytes: worker_bytes,
                published_bytes: 0,
                block_batch_profile: None,
                native_file_batch_profile: None,
            },
            true,
            &mut rng,
        );
        worker_reports.push(report);
    }

    let mut report = BenchReport::from_workers(started.elapsed(), worker_reports);
    report.append_log_profiles.push(AppendLogMicrobenchProfile {
        strategy: if workload.is_append_log_microbench_node_shared() {
            "node-shared"
        } else {
            "stream-private"
        },
        total_nanos: elapsed_nanos_u64(started),
        append_nanos,
        file_sync_nanos,
        file_sync_sum_nanos: file_sync.sync_sum_nanos,
        file_sync_max_nanos: file_sync.sync_max_nanos,
        dir_sync_nanos,
        bytes_written,
        sync_bytes: file_sync.sync_bytes,
        append_record_count,
        estimated_run_count,
        files_synced: file_sync.files_synced,
        dirs_synced: dirs.len() as u64,
        storage_nodes: args.storage_nodes as u64,
        stream_count: concurrency as u64,
        max_file_bytes,
        target_data_log_bytes: args.target_data_log_bytes,
    });
    Ok(report)
}

fn validate_append_log_microbench_workload(workload: Workload, args: &Args) -> Result<u64> {
    if !workload.is_append_log_microbench() {
        return Ok(args.stream_total_bytes);
    }
    let op_size = workload.op_size(args)? as u64;
    validate_fixed_stream_total_bytes(args.stream_total_bytes, op_size)?;
    Ok(args.stream_total_bytes)
}

#[allow(clippy::too_many_arguments)]
fn run_append_log_microbench_worker(
    workload: Workload,
    worker: u64,
    _concurrency: usize,
    storage_nodes: usize,
    samples_per_worker: usize,
    target_data_log_bytes: u64,
    total_bytes: u64,
    payload: Arc<Vec<u8>>,
    root: Arc<PathBuf>,
    node_lanes: Option<Arc<Vec<Arc<AppendLogNodeLane>>>>,
) -> Result<AppendLogMicrobenchWorkerOutput> {
    let mut rng = Lcg::new(0xf135_7aea_2e62_a9c5_u64 ^ worker);
    let mut report = WorkerReport::new(samples_per_worker);
    let node = if storage_nodes == 0 {
        return Err(StorageError::invalid_argument(
            "append-log microbench storage-nodes must be greater than zero",
        ));
    } else {
        worker as usize % storage_nodes
    };
    let dir = root.join(format!("node-{node}"));
    let mut files = Vec::<PathBuf>::new();
    let dirs = vec![dir.clone()];
    let mut bytes_written = 0_u64;
    let mut append_record_count = 0_u64;
    let mut estimated_run_count = 0_u64;
    let mut max_file_bytes = 0_u64;
    let mut last_log_id = None::<u64>;
    let mut last_end = 0_u64;

    let mut private_log_id = worker
        .checked_mul(1_000_000)
        .and_then(|base| base.checked_add(1))
        .ok_or_else(|| StorageError::invalid_argument("append-log id overflows"))?;
    let mut private_log_bytes = 0_u64;

    while bytes_written < total_bytes {
        let append_started = Instant::now();
        let allocation = if workload.is_append_log_microbench_node_shared() {
            let lanes = node_lanes
                .as_ref()
                .ok_or_else(|| StorageError::corrupt("missing append-log node lanes"))?;
            lanes
                .get(node)
                .ok_or_else(|| StorageError::corrupt("append-log node lane missing"))?
                .append(&payload)?
        } else {
            append_stream_private_log_record(
                &dir,
                &mut files,
                &mut private_log_id,
                &mut private_log_bytes,
                target_data_log_bytes,
                &payload,
            )?
        };
        let append_nanos = elapsed_nanos_u64(append_started);
        report.record_stream_append(
            append_nanos,
            payload.len() as u64,
            OpProgress::default(),
            true,
            &mut rng,
        );

        append_record_count = append_record_count.saturating_add(1);
        bytes_written = bytes_written.saturating_add(payload.len() as u64);
        if last_log_id != Some(allocation.log_id) || allocation.offset != last_end {
            estimated_run_count = estimated_run_count.saturating_add(1);
        }
        last_log_id = Some(allocation.log_id);
        last_end = allocation.end;
        max_file_bytes = max_file_bytes.max(allocation.end);
    }

    if let Some(lanes) = node_lanes {
        let state = lanes
            .get(node)
            .ok_or_else(|| StorageError::corrupt("append-log node lane missing"))?
            .state
            .lock()
            .map_err(|_| StorageError::unavailable("append-log node lane lock poisoned"))?;
        files = state.files.clone();
        max_file_bytes = max_file_bytes.max(state.max_file_bytes);
    }

    Ok(AppendLogMicrobenchWorkerOutput {
        worker,
        report,
        files,
        dirs,
        bytes_written,
        append_record_count,
        estimated_run_count,
        max_file_bytes,
    })
}

fn append_stream_private_log_record(
    dir: &Path,
    files: &mut Vec<PathBuf>,
    log_id: &mut u64,
    log_bytes: &mut u64,
    target_data_log_bytes: u64,
    payload: &[u8],
) -> Result<AppendLogAllocation> {
    let record_len = u64::try_from(payload.len())
        .map_err(|_| StorageError::invalid_argument("append-log payload overflows u64"))?;
    if *log_bytes != 0
        && (*log_bytes)
            .checked_add(record_len)
            .ok_or_else(|| StorageError::invalid_argument("append-log size overflows"))?
            > target_data_log_bytes
    {
        *log_id = (*log_id)
            .checked_add(1)
            .ok_or_else(|| StorageError::invalid_argument("append-log id overflows"))?;
        *log_bytes = 0;
    }

    fs::create_dir_all(dir).map_err(fs_error)?;
    let path = dir.join(format!("log-{}.dat", *log_id));
    let existed = path.exists();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(fs_error)?;
    let offset = *log_bytes;
    file.write_all(payload).map_err(fs_error)?;
    let end = offset
        .checked_add(record_len)
        .ok_or_else(|| StorageError::invalid_argument("append-log offset overflows"))?;
    *log_bytes = end;
    if !existed {
        files.push(path);
    }
    Ok(AppendLogAllocation {
        log_id: *log_id,
        offset,
        end,
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct AppendLogFileSyncProfile {
    files_synced: u64,
    sync_bytes: u64,
    sync_sum_nanos: u64,
    sync_max_nanos: u64,
}

fn sync_append_log_files(paths: Vec<PathBuf>) -> Result<AppendLogFileSyncProfile> {
    if paths.is_empty() {
        return Ok(AppendLogFileSyncProfile::default());
    }
    let mut handles = Vec::with_capacity(paths.len());
    for path in paths {
        handles.push(thread::spawn(move || {
            let file = fs::File::open(&path).map_err(fs_error)?;
            let bytes = file.metadata().map_err(fs_error)?.len();
            let started = Instant::now();
            file.sync_data().map_err(fs_error)?;
            Ok::<_, StorageError>((bytes, elapsed_nanos_u64(started)))
        }));
    }
    let mut profile = AppendLogFileSyncProfile::default();
    for handle in handles {
        let (bytes, nanos) = handle
            .join()
            .map_err(|_| StorageError::unavailable("append-log sync worker panicked"))??;
        profile.files_synced = profile.files_synced.saturating_add(1);
        profile.sync_bytes = profile.sync_bytes.saturating_add(bytes);
        profile.sync_sum_nanos = profile.sync_sum_nanos.saturating_add(nanos);
        profile.sync_max_nanos = profile.sync_max_nanos.max(nanos);
    }
    Ok(profile)
}

fn sync_dir_path(path: &Path) -> Result<()> {
    fs::File::open(path)
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)
}

fn dedup_paths(paths: &mut Vec<PathBuf>) {
    paths.sort();
    paths.dedup();
}
