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
        native_file_batch: if workload.is_native_file_batch() {
            Some(workload.native_file_batch_spec(args)?)
        } else {
            None
        },
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
    native_file_batch: Option<NativeFileBatchSpec>,
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

fn build_native_file_batch_writes(
    spec: NativeFileBatchSpec,
    op_index: u64,
    payload: &[u8],
    rng: &mut Lcg,
) -> Result<Vec<FileBatchWrite>> {
    let batch_bytes = spec.ops.checked_mul(spec.write_bytes).ok_or_else(|| {
        StorageError::invalid_argument("native file batch payload size overflows usize")
    })?;
    if batch_bytes > payload.len() {
        return Err(StorageError::invalid_argument(
            "native file batch payload is smaller than requested writes",
        ));
    }
    let batch_bytes_u64 = u64::try_from(batch_bytes).map_err(|_| {
        StorageError::invalid_argument("native file batch payload size overflows u64")
    })?;
    if batch_bytes_u64 > DEFAULT_FILE_CAPACITY_BYTES {
        return Err(StorageError::invalid_argument(
            "native file batch exceeds file root capacity",
        ));
    }
    let base = op_index
        .checked_mul(batch_bytes_u64)
        .ok_or_else(|| StorageError::invalid_argument("native file batch offset overflows"))?
        % DEFAULT_FILE_CAPACITY_BYTES;
    let base = if base
        .checked_add(batch_bytes_u64)
        .is_none_or(|end| end > DEFAULT_FILE_CAPACITY_BYTES)
    {
        0
    } else {
        base
    };
    let mut writes = Vec::with_capacity(spec.ops);
    for index in 0..spec.ops {
        let payload_start = index.checked_mul(spec.write_bytes).ok_or_else(|| {
            StorageError::invalid_argument("native file batch payload offset overflows")
        })?;
        let payload_end = payload_start
            .checked_add(spec.write_bytes)
            .ok_or_else(|| StorageError::invalid_argument("native file batch payload overflows"))?;
        let offset = match spec.overlap {
            NativeFileBatchOverlap::Sequential => {
                base + u64::try_from(payload_start).map_err(|_| {
                    StorageError::invalid_argument("native file batch offset overflows u64")
                })?
            }
            NativeFileBatchOverlap::OverwriteHotset => 0,
            NativeFileBatchOverlap::Random if op_index == 0 => {
                base + u64::try_from(payload_start).map_err(|_| {
                    StorageError::invalid_argument("native file batch offset overflows u64")
                })?
            }
            NativeFileBatchOverlap::Random => {
                let slots = (batch_bytes / spec.write_bytes).max(1);
                let slot = rng.below(slots as u64) as usize;
                base + u64::try_from(slot.checked_mul(spec.write_bytes).ok_or_else(|| {
                    StorageError::invalid_argument("native file batch random offset overflows")
                })?)
                .map_err(|_| StorageError::invalid_argument("native file batch offset overflow"))?
            }
        };
        writes.push(FileBatchWrite::new(
            offset,
            payload[payload_start..payload_end].to_vec(),
        ));
    }
    Ok(writes)
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
        (Target::Native { keyspace_id, files }, Workload::NativeWrite4kSameFile) => {
            let file_id = files[0];
            context
                .store
                .commit_file_batch(
                    *keyspace_id,
                    file_id,
                    &[FileBatchWrite::new(0, context.payload.as_ref().clone())],
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_file_batch() => {
            let spec = config
                .native_file_batch
                .ok_or_else(|| StorageError::corrupt("missing native file batch spec"))?;
            let file_index = worker as usize % files.len();
            let file_id = files[file_index];
            let op_index = state.native_file_op;
            state.native_file_op = state.native_file_op.saturating_add(1);
            state.last_native_file_index = Some(file_index);
            let writes = build_native_file_batch_writes(spec, op_index, &context.payload, rng)?;
            context
                .store
                .commit_file_batch(
                    *keyspace_id,
                    file_id,
                    &writes,
                    durability,
                    config.payload_integrity,
                )
                .map(|_| OpProgress::default())
        }
        (Target::Native { keyspace_id, files }, workload) if workload.is_native_write() => {
            let file_index = state.next_partitioned_file_index(worker, concurrency, files.len());
            let file_id = files[file_index];
            context
                .store
                .commit_file_batch(
                    *keyspace_id,
                    file_id,
                    &[FileBatchWrite::new(0, context.payload.as_ref().clone())],
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
        (Target::Native { keyspace_id, files }, Workload::NativeAppend4kSameFile) => {
            let file_id = files[0];
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
