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
    report.op_size = workload.op_size(args)?;

    if matches!(args.provider, ProviderKind::Durable) {
        let _ = fs::remove_dir_all(&root);
    }

    Ok(report)
}
fn setup_context(
    args: &Args,
    workload: Workload,
    concurrency: usize,
    store: BenchStore,
) -> Result<BenchContext> {
    let op_size = workload.op_size(args)?;
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
