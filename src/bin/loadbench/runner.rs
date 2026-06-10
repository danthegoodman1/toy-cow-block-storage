fn run_case(args: &Args, workload: Workload, concurrency: usize) -> Result<BenchReport> {
    let case_id = format!(
        "{}-{}-c{}-{}",
        std::process::id(),
        workload.name(),
        concurrency,
        NEXT_ROOT_ID.fetch_add(1, Ordering::Relaxed)
    );
    let root = args.root.join(&case_id);
    let append_visible_journal = args
        .append_visible_journal_dir
        .as_ref()
        .map(|dir| dir.join(format!("{case_id}-append-visible-publish.journal")));
    if matches!(args.provider, ProviderKind::Durable) {
        let _ = fs::remove_dir_all(&root);
        if let Some(path) = &append_visible_journal {
            let _ = fs::remove_file(path);
        }
        fs::create_dir_all(&root).map_err(fs_error)?;
    }
    let storage_node_data_targets = prepare_storage_node_data_dirs(args, &root, &case_id)?;

    let store = BenchStore::open(args, &root, append_visible_journal.clone())?;
    let context = setup_context(args, workload, concurrency, store, &root)?;
    let profile_store = context.store.clone();
    let _ = profile_store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_append_publish_wait_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_append_ingest_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_read_profiles(DEFAULT_PROFILE_CAPACITY)?;
    let _ = profile_store.drain_native_file_batch_commit_profiles(DEFAULT_PROFILE_CAPACITY)?;
    if !workload.is_native_stream_publish_fixed()
        && !workload.is_append_log_microbench()
        && !args.warmup.is_zero()
    {
        if workload.is_native_mixed() {
            let _ =
                execute_mixed_native_load(args, workload, concurrency, context.clone(), args.warmup)?;
        } else {
            let _ = execute_load(args, workload, concurrency, context.clone(), args.warmup)?;
        }
        let _ = profile_store.drain_persist_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_append_publish_wait_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_append_ingest_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_metadata_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_block_write_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_read_profiles(DEFAULT_PROFILE_CAPACITY)?;
        let _ = profile_store.drain_native_file_batch_commit_profiles(DEFAULT_PROFILE_CAPACITY)?;
    }
    let mut report = if workload.is_append_log_microbench() {
        execute_append_log_microbench_load(args, workload, concurrency, context)?
    } else if workload.is_native_mixed() {
        execute_mixed_native_load(args, workload, concurrency, context, args.duration)?
    } else if workload.is_native_stream_publish_fixed() {
        execute_fixed_stream_publish_load(args, workload, concurrency, context)?
    } else {
        execute_load(args, workload, concurrency, context, args.duration)?
    };
    report.provider = args.provider;
    report.durability = args.durability;
    report.workload = workload;
    report.concurrency = concurrency;
    report.rtt_us = args.rtt.as_micros();
    report.serial_rtts = args.serial_rtts;
    report.op_size = workload.op_size(args)?;
    append_profile_csv(args, workload, concurrency, &profile_store)?;
    append_append_publish_profile_csv(args, workload, concurrency, &profile_store)?;
    append_append_ingest_profile_csv(args, workload, concurrency, &profile_store)?;
    append_metadata_profile_csv(args, workload, concurrency, &profile_store)?;
    append_block_write_profile_csv(args, workload, concurrency, &profile_store)?;
    append_read_profile_csv(args, workload, concurrency, &profile_store)?;
    append_native_file_batch_commit_profile_csv(args, workload, concurrency, &profile_store)?;
    append_block_batch_profile_csv(args, &report)?;
    append_native_file_batch_profile_csv(args, &report)?;
    append_append_log_profile_csv(args, &report)?;

    if matches!(args.provider, ProviderKind::Durable) {
        let _ = fs::remove_dir_all(&root);
        for target in &storage_node_data_targets {
            let _ = fs::remove_dir_all(target);
        }
        if let Some(path) = &append_visible_journal {
            let _ = fs::remove_file(path);
        }
    }

    Ok(report)
}

fn prepare_storage_node_data_dirs(
    args: &Args,
    root: &Path,
    case_id: &str,
) -> Result<Vec<PathBuf>> {
    if !matches!(args.provider, ProviderKind::Durable) || args.storage_node_data_dirs.is_empty() {
        return Ok(Vec::new());
    }

    let data_dir = root.join("data");
    fs::create_dir_all(&data_dir).map_err(fs_error)?;
    let mut targets = Vec::with_capacity(args.storage_nodes);
    for (index, storage_node) in args.storage_node_ids().into_iter().enumerate() {
        let target = args.storage_node_data_dirs[index]
            .join(case_id)
            .join(format!("node-{}", storage_node.raw()));
        fs::create_dir_all(&target).map_err(fs_error)?;
        let target = fs::canonicalize(&target).map_err(fs_error)?;
        let link = data_dir.join(format!("node-{}", storage_node.raw()));
        let _ = fs::remove_file(&link);
        std::os::unix::fs::symlink(&target, &link).map_err(fs_error)?;
        targets.push(target);
    }
    Ok(targets)
}

fn setup_context(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: BenchStore,
    root: &Path,
) -> Result<BenchContext> {
    let op_size = workload.op_size(args)?;
    let payload = Arc::new(make_payload(op_size));
    let target = if workload.is_append_log_microbench() {
        Target::AppendLogMicrobench {
            root: Arc::new(root.join("append-log-microbench")),
        }
    } else if workload.is_block() {
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
        let hot_blocks = if matches!(workload, Workload::BlockRead4k | Workload::BlockRead1m) {
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
            if matches!(workload, Workload::NativeRead4k | Workload::NativeRead1m) {
                store.commit_file_batch(
                    keyspace_id,
                    file_id,
                    &[FileBatchWrite::new(0, payload.as_ref().clone())],
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
    let read_blocks = u64::try_from(payload.len())
        .map_err(|_| StorageError::invalid_argument("read payload length overflows u64"))?
        .checked_div(u64::from(BLOCK_SIZE))
        .ok_or_else(|| StorageError::invalid_argument("block size must not be zero"))?;
    if read_blocks == 0 || read_blocks > args.device_blocks {
        return Err(StorageError::invalid_argument(
            "block read workload exceeds device size",
        ));
    }
    let hot_blocks = args.device_blocks.min(4096_u64.max(read_blocks));
    let block_payload = payload
        .get(..usize::try_from(BLOCK_SIZE).expect("block size fits usize"))
        .ok_or_else(|| StorageError::invalid_argument("read payload is smaller than one block"))?;
    for block in 0..hot_blocks {
        store.write_device(
            device_id,
            block * u64::from(BLOCK_SIZE),
            block_payload,
            WriteDurability::Acknowledged,
            args.payload_integrity,
        )?;
    }
    store.flush_device(device_id)?;
    Ok(hot_blocks)
}
