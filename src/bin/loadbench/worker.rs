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
        block_batch: if workload.is_block_batch() {
            Some(workload.block_batch_spec(args)?)
        } else {
            None
        },
        block_batch_profiles_enabled: args.block_batch_profile_csv.is_some(),
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
    block_batch: Option<BlockBatchSpec>,
    block_batch_profiles_enabled: bool,
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
            progress,
            result.is_ok(),
            &mut rng,
        );
    }

    Ok(report)
}

#[derive(Default)]
struct WorkerState {
    stream_append: Option<StreamAppendState>,
    block_writeback: BlockWritebackState,
    block_batch_op: u64,
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

#[derive(Default)]
struct BlockWritebackState {
    writes: Vec<BlockBatchWrite>,
    dirty_bytes: u64,
    staged_commit: Option<BlockBatchCommit>,
}

impl BlockWritebackState {
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    fn dirty_bytes(&self) -> u64 {
        self.dirty_bytes
    }

    fn clear(&mut self) {
        self.writes.clear();
        self.dirty_bytes = 0;
        self.staged_commit = None;
    }

    fn push_write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.dirty_bytes = self.dirty_bytes.saturating_add(bytes.len() as u64);
        self.writes.push(BlockBatchWrite {
            offset,
            bytes: bytes.to_vec(),
            payload_integrity,
        });
        Ok(())
    }
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
    if let Target::Block {
        device_id,
        logical_blocks,
        ..
    } = &context.target
        && let Some(dirty_bytes) = config.workload.block_writeback_fsync_bytes()
    {
        fill_block_writeback_dirty(context, worker, state, config, *logical_blocks, dirty_bytes)?;
        if config.workload.is_block_writeback_prestaged()
            && state.block_writeback.staged_commit.is_none()
        {
            let commit = context.store.commit_block_batch(
                *device_id,
                &state.block_writeback.writes,
                WriteDurability::Acknowledged,
            )?;
            state.block_writeback.staged_commit = Some(commit);
        }
        return Ok(());
    }
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
    block_batch_profile: Option<BlockBatchOpProfile>,
}

impl OpProgress {
    fn merge(&mut self, other: Self) {
        self.durable_bytes = self.durable_bytes.saturating_add(other.durable_bytes);
        self.published_bytes = self.published_bytes.saturating_add(other.published_bytes);
        if self.block_batch_profile.is_none() {
            self.block_batch_profile = other.block_batch_profile;
        }
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

fn elapsed_nanos_u64(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
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

fn block_batch_lane(
    worker: u64,
    concurrency: usize,
    logical_blocks: u64,
    batch_blocks: u64,
) -> (u64, u64) {
    let max_lanes = logical_blocks.checked_div(batch_blocks.max(1)).unwrap_or(0);
    let lane_count = (concurrency as u64).min(max_lanes).max(1);
    let lane = worker % lane_count;
    let start = logical_blocks.saturating_mul(lane) / lane_count;
    let end = logical_blocks.saturating_mul(lane + 1) / lane_count;
    (start, end.max(start.saturating_add(batch_blocks)))
}

struct BlockBatchBuild<'a> {
    spec: BlockBatchSpec,
    worker: u64,
    concurrency: usize,
    logical_blocks: u64,
    op_index: u64,
    payload: &'a [u8],
    payload_integrity: PayloadIntegrity,
}

fn build_block_batch_writes(
    input: BlockBatchBuild<'_>,
    rng: &mut Lcg,
) -> Result<Vec<BlockBatchWrite>> {
    let BlockBatchBuild {
        spec,
        worker,
        concurrency,
        logical_blocks,
        op_index,
        payload,
        payload_integrity,
    } = input;
    let batch_bytes = spec.ops.checked_mul(spec.write_bytes).ok_or_else(|| {
        StorageError::invalid_argument("block batch payload size overflows usize")
    })?;
    if batch_bytes > payload.len() {
        return Err(StorageError::invalid_argument(
            "block batch payload is smaller than requested writes",
        ));
    }
    let write_blocks = (u64::try_from(spec.write_bytes)
        .map_err(|_| StorageError::invalid_argument("block batch write bytes overflow u64"))?
        / u64::from(BLOCK_SIZE))
    .max(1);
    let batch_blocks = u64::try_from(spec.ops)
        .map_err(|_| StorageError::invalid_argument("block batch op count overflow u64"))?
        .checked_mul(write_blocks)
        .ok_or_else(|| StorageError::invalid_argument("block batch block count overflows"))?;
    if batch_blocks > logical_blocks {
        return Err(StorageError::invalid_argument(
            "block batch exceeds logical device size",
        ));
    }
    let (lane_start, lane_end) = block_batch_lane(worker, concurrency, logical_blocks, batch_blocks);
    let lane_span = lane_end.saturating_sub(lane_start).min(logical_blocks - lane_start);
    let usable_blocks = lane_span.saturating_sub(batch_blocks);
    let step_blocks = write_blocks.max(1);
    let slots = usable_blocks
        .checked_div(step_blocks)
        .unwrap_or(0)
        .saturating_add(1);
    let start_block = lane_start
        + op_index
            .wrapping_mul(batch_blocks)
            .checked_div(step_blocks)
            .unwrap_or(0)
            .wrapping_rem(slots.max(1))
            .saturating_mul(step_blocks);

    let mut writes = Vec::with_capacity(spec.ops);
    for index in 0..spec.ops {
        let payload_start = index
            .checked_mul(spec.write_bytes)
            .ok_or_else(|| StorageError::invalid_argument("block batch payload offset overflows"))?;
        let payload_end = payload_start
            .checked_add(spec.write_bytes)
            .ok_or_else(|| StorageError::invalid_argument("block batch payload end overflows"))?;
        let write_block = match spec.overlap {
            BlockBatchOverlap::Sequential => start_block
                .checked_add(
                    u64::try_from(index)
                        .map_err(|_| StorageError::invalid_argument("block batch index overflow"))?
                        .saturating_mul(write_blocks),
                )
                .ok_or_else(|| StorageError::invalid_argument("block batch offset overflows"))?,
            BlockBatchOverlap::OverwriteHotset => start_block,
            BlockBatchOverlap::Random if op_index == 0 => start_block
                .checked_add(
                    u64::try_from(index)
                        .map_err(|_| StorageError::invalid_argument("block batch index overflow"))?
                        .saturating_mul(write_blocks),
                )
                .ok_or_else(|| StorageError::invalid_argument("block batch offset overflows"))?,
            BlockBatchOverlap::Random => {
                let slot = rng.below(spec.ops as u64);
                start_block
                    .checked_add(slot.saturating_mul(write_blocks))
                    .ok_or_else(|| StorageError::invalid_argument("block batch offset overflows"))?
            }
        };
        writes.push(BlockBatchWrite {
            offset: write_block
                .checked_mul(u64::from(BLOCK_SIZE))
                .ok_or_else(|| StorageError::invalid_argument("block batch byte offset overflows"))?,
            bytes: payload[payload_start..payload_end].to_vec(),
            payload_integrity,
        });
    }
    Ok(writes)
}

fn block_writeback_start_block(
    worker: u64,
    concurrency: usize,
    logical_blocks: u64,
    dirty_bytes: u64,
) -> Result<u64> {
    if dirty_bytes == 0 {
        return Err(StorageError::invalid_argument(
            "writeback dirty window must be greater than zero",
        ));
    }
    if !dirty_bytes.is_multiple_of(u64::from(BLOCK_SIZE)) {
        return Err(StorageError::invalid_argument(
            "writeback dirty window must be block aligned",
        ));
    }
    let dirty_blocks = dirty_bytes / u64::from(BLOCK_SIZE);
    if dirty_blocks > logical_blocks {
        return Err(StorageError::invalid_argument(
            "writeback dirty window exceeds logical device size",
        ));
    }
    let (start, _) = block_batch_lane(worker, concurrency, logical_blocks, dirty_blocks.max(1));
    Ok(start.min(logical_blocks - dirty_blocks))
}

fn fill_block_writeback_dirty(
    context: &BenchContext,
    worker: u64,
    state: &mut WorkerState,
    config: &WorkerConfig,
    logical_blocks: u64,
    dirty_bytes: u64,
) -> Result<()> {
    if state.block_writeback.dirty_bytes() == dirty_bytes {
        return Ok(());
    }
    let dirty_bytes_usize = usize::try_from(dirty_bytes)
        .map_err(|_| StorageError::invalid_argument("writeback dirty bytes overflow usize"))?;
    if context.payload.len() < dirty_bytes_usize {
        return Err(StorageError::invalid_argument(
            "writeback payload is smaller than dirty window",
        ));
    }
    let start_block =
        block_writeback_start_block(worker, config.concurrency, logical_blocks, dirty_bytes)?;
    state.block_writeback.clear();
    let mut payload_offset = 0usize;
    while payload_offset < dirty_bytes_usize {
        let offset = start_block
            .checked_mul(u64::from(BLOCK_SIZE))
            .and_then(|base| base.checked_add(payload_offset as u64))
            .ok_or_else(|| StorageError::invalid_argument("writeback byte offset overflows"))?;
        let next = payload_offset + BLOCK_SIZE as usize;
        state
            .block_writeback
            .push_write(
                offset,
                &context.payload[payload_offset..next],
                config.payload_integrity,
            )?;
        payload_offset = next;
    }
    Ok(())
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
                ..
            },
            workload,
        ) if workload.block_writeback_fsync_bytes().is_some() => {
            if state.block_writeback.is_empty() {
                return Err(StorageError::corrupt(
                    "writeback fsync workload has no dirty ranges",
                ));
            }
            let dirty_bytes = state.block_writeback.dirty_bytes();
            let dirty_range_count = state.block_writeback.writes.len();
            let started = config.block_batch_profiles_enabled.then(Instant::now);
            let commit_started = Instant::now();
            let commit = if workload.is_block_writeback_prestaged() {
                state
                    .block_writeback
                    .staged_commit
                    .take()
                    .ok_or_else(|| StorageError::corrupt("writeback fsync was not prestaged"))?
            } else {
                context.store.commit_block_batch(
                    *device_id,
                    &state.block_writeback.writes,
                    WriteDurability::Acknowledged,
                )?
            };
            let commit_nanos = elapsed_nanos_u64(commit_started);
            let flush_started = Instant::now();
            context.store.flush_device(*device_id)?;
            let flush_device_nanos = elapsed_nanos_u64(flush_started);
            state.block_writeback.clear();
            let block_batch_profile = started.map(|started| BlockBatchOpProfile {
                total_nanos: elapsed_nanos_u64(started),
                commit_nanos,
                flush_device_nanos,
                batch_operation_count: dirty_range_count as u64,
                collapsed_range_count: commit.collapsed_range_count,
                requested_bytes: dirty_bytes,
                committed_bytes: commit.committed_bytes,
            });
            Ok(OpProgress {
                durable_bytes: commit.committed_bytes,
                published_bytes: commit.committed_bytes,
                block_batch_profile,
            })
        }
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
            Workload::BlockRead4k | Workload::BlockRead1m,
        ) => {
            let blocks = (context.op_size as u64) / u64::from(BLOCK_SIZE);
            let max_start = hot_blocks.saturating_sub(blocks);
            let block = rng.below(max_start.saturating_add(1));
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
        (
            Target::Block {
                device_id,
                logical_blocks,
                ..
            },
            workload,
        ) if workload.is_block_batch() => {
            let spec = config
                .block_batch
                .ok_or_else(|| StorageError::corrupt("missing block batch spec"))?;
            let op_index = state.block_batch_op;
            state.block_batch_op = state.block_batch_op.saturating_add(1);
            let writes = build_block_batch_writes(
                BlockBatchBuild {
                    spec,
                    worker,
                    concurrency,
                    logical_blocks: *logical_blocks,
                    op_index,
                    payload: context.payload.as_ref(),
                    payload_integrity: config.payload_integrity,
                },
                rng,
            )?;
            let started = config.block_batch_profiles_enabled.then(Instant::now);
            let commit_started = Instant::now();
            let commit = context
                .store
                .commit_block_batch(*device_id, &writes, durability)?;
            let commit_nanos = elapsed_nanos_u64(commit_started);
            let block_batch_profile = started.map(|started| BlockBatchOpProfile {
                total_nanos: elapsed_nanos_u64(started),
                commit_nanos,
                flush_device_nanos: 0,
                batch_operation_count: commit.write_count,
                collapsed_range_count: commit.collapsed_range_count,
                requested_bytes: context.op_size as u64,
                committed_bytes: commit.committed_bytes,
            });
            Ok(OpProgress {
                durable_bytes: 0,
                published_bytes: commit.committed_bytes,
                block_batch_profile,
            })
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
        (Target::Native { keyspace_id, files }, Workload::NativeRead4k | Workload::NativeRead1m) => {
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
    if workload.is_block_writeback() {
        return Ok(OpProgress::default());
    }
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
                    block_batch_profile: None,
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
