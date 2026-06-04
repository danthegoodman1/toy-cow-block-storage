/// Durable in-process coordinator using SQLite metadata and node-scoped rolled
/// data logs.
#[derive(Debug, Clone)]
pub struct DurableCoordinator {
    local: LocalCoordinator,
    durable: DurableSqliteStore,
    persisted_segments: Arc<Mutex<BTreeSet<SegmentId>>>,
    pending_block_deltas: Arc<Mutex<Vec<BlockDeltaCommit>>>,
    pending_data_log_append: Arc<Mutex<PendingDataLogAppend>>,
    block_delta_staging_lock: Arc<Mutex<()>>,
    block_delta_prestage: Arc<BlockDeltaPrestageTracker>,
    stream_append_lanes: Arc<Mutex<BTreeMap<AppendStreamId, Arc<StreamAppendLane>>>>,
    persist_lock: Arc<Mutex<()>>,
    persist_coordinator: Arc<PersistCoordinator>,
    stream_prefix_persist_coordinator: Arc<StreamPrefixPersistCoordinator>,
    persist_profiler: Arc<Mutex<Option<PersistProfiler>>>,
    read_profiler: Arc<Mutex<Option<ReadProfiler>>>,
    maintenance_policy: MaintenancePolicy,
    maintenance_cursor: Arc<Mutex<Option<DurableDataLogRef>>>,
    maintenance_worker: Option<Arc<MaintenanceWorker>>,
}

#[derive(Debug)]
struct StreamAppendLane {
    append: Mutex<()>,
    pending: Mutex<PendingDataLogAppend>,
}

impl StreamAppendLane {
    fn new() -> Self {
        Self {
            append: Mutex::new(()),
            pending: Mutex::new(PendingDataLogAppend::default()),
        }
    }
}

impl DurableCoordinator {
    pub fn open(root: impl AsRef<Path>, config: LocalStoreConfig) -> Result<Self> {
        Self::open_with_data_log_policy(root, config, DurableDataLogPolicy::default())
    }

    pub fn open_with_data_log_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        policy: DurableDataLogPolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_data_log_policy(
            root,
            config,
            vec![config.storage_node],
            policy,
        )
    }

    pub fn open_with_storage_nodes_and_data_log_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        policy: DurableDataLogPolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_maintenance_policy(
            root,
            config,
            storage_nodes,
            MaintenancePolicy::manual(policy),
        )
    }

    /// Open a one-node durable store with an explicit maintenance policy.
    ///
    /// Manual mode starts no background worker. Opportunistic and always-on
    /// modes remain implementation details below the block/native APIs; callers
    /// still observe the same read/write/fork/snapshot/restore semantics.
    pub fn open_with_maintenance_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        policy: MaintenancePolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_maintenance_policy(
            root,
            config,
            vec![config.storage_node],
            policy,
        )
    }

    /// Open a durable store with provider-private storage-node placement.
    ///
    /// The supplied node list seeds a new store. Reopen reconstructs the
    /// registry from SQLite and verifies row-native metadata plus data-log
    /// placements before returning.
    pub fn open_with_storage_nodes_and_maintenance_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        maintenance_policy: MaintenancePolicy,
    ) -> Result<Self> {
        config.validate()?;
        maintenance_policy.validate()?;
        let storage_nodes = normalize_storage_nodes(config.storage_node, storage_nodes);
        let paths = DurableStorePaths::new(root, config.storage_node)?;
        let durable = DurableSqliteStore::open(
            paths,
            maintenance_policy.data_log_policy,
            storage_nodes.clone(),
        )?;

        let local = durable
            .load(config)?
            .unwrap_or(LocalCoordinator::with_storage_nodes(config, storage_nodes)?);
        let append_stream_incarnation = durable.append_stream_incarnation()?;
        local
            .metadata
            .use_append_stream_incarnation(append_stream_incarnation)?;
        let persisted_segments = local.segment_ids()?;
        let durable_through = durable_commit_high_water_from_local(&local)?;
        let maintenance_cursor = Arc::new(Mutex::new(durable.load_maintenance_cursor()?));

        let mut store = Self {
            local,
            durable,
            persisted_segments: Arc::new(Mutex::new(persisted_segments)),
            pending_block_deltas: Arc::new(Mutex::new(Vec::new())),
            pending_data_log_append: Arc::new(Mutex::new(PendingDataLogAppend::default())),
            block_delta_staging_lock: Arc::new(Mutex::new(())),
            block_delta_prestage: Arc::new(BlockDeltaPrestageTracker::new()),
            stream_append_lanes: Arc::new(Mutex::new(BTreeMap::new())),
            persist_lock: Arc::new(Mutex::new(())),
            persist_coordinator: Arc::new(PersistCoordinator::new(durable_through)),
            stream_prefix_persist_coordinator: Arc::new(StreamPrefixPersistCoordinator::new()),
            persist_profiler: Arc::new(Mutex::new(None)),
            read_profiler: Arc::new(Mutex::new(None)),
            maintenance_policy,
            maintenance_cursor,
            maintenance_worker: None,
        };
        store.start_maintenance_worker_if_needed()?;
        Ok(store)
    }

    fn start_maintenance_worker_if_needed(&mut self) -> Result<()> {
        if matches!(self.maintenance_policy.mode, MaintenanceMode::AlwaysOn) {
            let worker = MaintenanceWorker::start(DurableMaintenanceParts {
                local: self.local.clone(),
                durable: self.durable.clone(),
                persist_lock: Arc::clone(&self.persist_lock),
                maintenance_cursor: Arc::clone(&self.maintenance_cursor),
                maintenance_policy: self.maintenance_policy,
            })?;
            if self.startup_maintenance_has_work()? {
                worker.notify();
            }
            self.maintenance_worker = Some(worker);
        }
        Ok(())
    }

    fn startup_maintenance_has_work(&self) -> Result<bool> {
        self.maintenance_plan_has_commands(self.maintenance_policy)
    }

    fn maintenance_plan_has_commands(&self, policy: MaintenancePolicy) -> Result<bool> {
        let scheduler = MaintenanceScheduler::new(policy)?;
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            0,
            0,
            policy_uses_sqlite_wal_pressure(policy),
        )?;
        Ok(!scheduler.step(&observation).commands.is_empty())
    }

    fn maintenance_parts(&self) -> DurableMaintenanceParts {
        DurableMaintenanceParts {
            local: self.local.clone(),
            durable: self.durable.clone(),
            persist_lock: Arc::clone(&self.persist_lock),
            maintenance_cursor: Arc::clone(&self.maintenance_cursor),
            maintenance_policy: self.maintenance_policy,
        }
    }

    pub fn enable_persist_profiling(&self, capacity: usize) -> Result<()> {
        *lock(&self.persist_profiler)? = Some(PersistProfiler::new(capacity)?);
        self.local.metadata.enable_publish_profiling(capacity)?;
        Ok(())
    }

    pub fn drain_persist_profiles(&self, max: usize) -> Result<Vec<DurablePersistProfile>> {
        let mut profiler = lock(&self.persist_profiler)?;
        Ok(profiler
            .as_mut()
            .map(|profiler| profiler.drain(max))
            .unwrap_or_default())
    }

    fn record_persist_profile(&self, profile: DurablePersistProfile) -> Result<()> {
        if let Some(profiler) = lock(&self.persist_profiler)?.as_mut() {
            profiler.record(profile);
        }
        Ok(())
    }

    pub fn enable_read_profiling(&self, capacity: usize) -> Result<()> {
        *lock(&self.read_profiler)? = Some(ReadProfiler::new(capacity)?);
        Ok(())
    }

    pub fn drain_read_profiles(&self, max: usize) -> Result<Vec<ReadProfile>> {
        let mut profiler = lock(&self.read_profiler)?;
        Ok(profiler
            .as_mut()
            .map(|profiler| profiler.drain(max))
            .unwrap_or_default())
    }

    fn record_read_profile(&self, profile: ReadProfile) -> Result<()> {
        if let Some(profiler) = lock(&self.read_profiler)?.as_mut() {
            profiler.record(profile);
        }
        Ok(())
    }

    fn attach_metadata_publish_profile(&self, profile: &mut DurablePersistProfile) -> Result<()> {
        let summary = summarize_metadata_publish_profiles(
            self.local.metadata.drain_publish_profiles(usize::MAX)?,
        );
        profile.metadata_publish_lock_wait_nanos = summary.lock_wait_nanos;
        profile.commit_sequence_alloc_nanos = summary.commit_sequence_alloc_nanos;
        profile.logical_conflict_count = summary.logical_conflict_count;
        profile.touched_shard_head_rows = summary.touched_shard_head_rows;
        profile.touched_manifest_rows = summary.touched_manifest_rows;
        profile.commit_rows_written = summary.commit_rows_written;
        Ok(())
    }

    fn record_pending_block_delta(&self, delta: Option<BlockDeltaCommit>) -> Result<()> {
        if let Some(delta) = delta {
            lock(&self.pending_block_deltas)?.push(delta);
        }
        Ok(())
    }

    fn prune_pending_block_deltas_through(&self, durable_through: CommitSeq) -> Result<()> {
        lock(&self.pending_block_deltas)?
            .retain(|delta| delta.commit_seq.raw() > durable_through.raw());
        Ok(())
    }

    fn begin_block_delta_prestage(&self, commit_seq: CommitSeq) -> Result<()> {
        let mut state = lock(&self.block_delta_prestage.inner)?;
        state.in_flight.insert(commit_seq);
        state.failed.remove(&commit_seq);
        Ok(())
    }

    fn finish_block_delta_prestage(&self, commit_seq: CommitSeq, result: Result<()>) -> Result<()> {
        let mut state = lock(&self.block_delta_prestage.inner)?;
        state.in_flight.remove(&commit_seq);
        match result {
            Ok(()) => {
                state.failed.remove(&commit_seq);
            }
            Err(_) => {
                state.failed.insert(commit_seq);
            }
        }
        self.block_delta_prestage.cvar.notify_all();
        Ok(())
    }

    fn wait_for_block_delta_prestage(&self, deltas: &[BlockDeltaCommit]) -> Result<u64> {
        let selected: BTreeSet<_> = deltas.iter().map(|delta| delta.commit_seq).collect();
        if selected.is_empty() {
            return Ok(0);
        }
        let started = Instant::now();
        let mut state = lock(&self.block_delta_prestage.inner)?;
        while selected
            .iter()
            .any(|commit_seq| state.in_flight.contains(commit_seq))
        {
            state = wait_on_cvar(&self.block_delta_prestage.cvar, state)?;
        }
        Ok(duration_nanos_u64(started.elapsed()))
    }

    fn wait_for_all_block_delta_prestage(&self) -> Result<u64> {
        let started = Instant::now();
        let mut state = lock(&self.block_delta_prestage.inner)?;
        while !state.in_flight.is_empty() {
            state = wait_on_cvar(&self.block_delta_prestage.cvar, state)?;
        }
        Ok(duration_nanos_u64(started.elapsed()))
    }

    fn prune_block_delta_prestage_through(&self, durable_through: CommitSeq) -> Result<()> {
        lock(&self.block_delta_prestage.inner)?
            .failed
            .retain(|commit_seq| commit_seq.raw() > durable_through.raw());
        Ok(())
    }

    fn prestage_block_delta_segments(&self, delta: &BlockDeltaCommit) -> Result<()> {
        let segment_ids = delta.segment_ids();
        let previous_segments = lock(&self.persisted_segments)?.clone();
        let pending_base = lock(&self.pending_data_log_append)?.clone();
        let pending_segments = pending_base.segment_ids();
        let missing_segments: BTreeSet<_> = segment_ids
            .iter()
            .copied()
            .filter(|segment_id| {
                !previous_segments.contains(segment_id) && !pending_segments.contains(segment_id)
            })
            .collect();
        if missing_segments.is_empty() {
            return Ok(());
        }
        let (_, payloads) = self.local.state_for_segment_ids(&missing_segments)?;
        let appended = self.durable.prestage_segments(payloads, &pending_base)?;
        lock(&self.pending_data_log_append)?.merge(appended);
        Ok(())
    }

    fn has_pending_block_delta_in_range(
        &self,
        first_commit: u64,
        target_commit: CommitSeq,
    ) -> Result<bool> {
        Ok(lock(&self.pending_block_deltas)?.iter().any(|delta| {
            delta.commit_seq.raw() >= first_commit
                && delta.commit_seq.raw() <= target_commit.raw()
        }))
    }

    fn contiguous_pending_block_deltas(
        &self,
        durable_through: CommitSeq,
        target: CommitSeq,
    ) -> Result<Option<Vec<BlockDeltaCommit>>> {
        if durable_through.raw() >= target.raw() {
            return Ok(Some(Vec::new()));
        }
        let mut pending = lock(&self.pending_block_deltas)?.clone();
        pending.sort_by_key(|delta| delta.commit_seq.raw());
        let mut next = durable_through
            .raw()
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("durable block delta sequence overflows"))?;
        let mut selected = Vec::new();
        for delta in pending {
            let seq = delta.commit_seq.raw();
            if seq < next {
                continue;
            }
            if seq > target.raw() {
                break;
            }
            if seq != next {
                return Ok(None);
            }
            selected.push(delta);
            next = next
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("durable block delta sequence overflows"))?;
        }
        if next == target.raw().saturating_add(1) {
            Ok(Some(selected))
        } else {
            Ok(None)
        }
    }

    fn ready_contiguous_pending_block_deltas(
        &self,
        durable_through: CommitSeq,
        target: CommitSeq,
    ) -> Result<Option<Vec<BlockDeltaCommit>>> {
        if durable_through.raw() >= target.raw() {
            return Ok(Some(Vec::new()));
        }
        let in_flight = lock(&self.block_delta_prestage.inner)?.in_flight.clone();
        let mut pending = lock(&self.pending_block_deltas)?.clone();
        pending.sort_by_key(|delta| delta.commit_seq.raw());
        let mut next = durable_through
            .raw()
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("durable block delta sequence overflows"))?;
        let mut selected = Vec::new();
        for delta in pending {
            let seq = delta.commit_seq.raw();
            if seq < next {
                continue;
            }
            if seq > target.raw() {
                break;
            }
            if seq != next {
                return Ok(None);
            }
            if in_flight.contains(&delta.commit_seq) {
                break;
            }
            selected.push(delta);
            next = next
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("durable block delta sequence overflows"))?;
        }
        if !selected.is_empty() || next == target.raw().saturating_add(1) {
            Ok(Some(selected))
        } else {
            Ok(None)
        }
    }

    fn persist_block_deltas_until(&self, required: CommitSeq) -> Result<()> {
        loop {
            let mut state = lock(&self.persist_coordinator.inner)?;
            state.requested_through = state.requested_through.max(required);
            if state.durable_through >= required {
                return Ok(());
            }
            if !state.in_flight {
                let target_commit = state.requested_through;
                state.in_flight = true;
                drop(state);
                let result = self.persist_block_deltas_physical(Instant::now(), target_commit);
                let mut state = lock(&self.persist_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                let generation = state.generation;
                match result {
                    Ok(durable_through) => {
                        state.durable_through = state.durable_through.max(durable_through);
                        state.requested_through =
                            state.requested_through.max(state.durable_through);
                        state.last_error = None;
                        self.persist_coordinator.cvar.notify_all();
                        if state.durable_through >= required {
                            return Ok(());
                        }
                        return Err(StorageError::conflict(
                            "durable block delta persist did not reach required commit sequence",
                        ));
                    }
                    Err(error) => {
                        state.last_error = Some((generation, error.clone()));
                        self.persist_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
            }

            let generation = state.generation;
            while state.in_flight && state.generation == generation {
                state = wait_on_cvar(&self.persist_coordinator.cvar, state)?;
            }
            if state.durable_through >= required {
                return Ok(());
            }
            if state.generation != generation
                && let Some((error_generation, error)) = &state.last_error
                && *error_generation == state.generation
            {
                return Err(error.clone());
            }
        }
    }

    fn persist_block_deltas_physical(
        &self,
        total_started: Instant,
        minimum_target: CommitSeq,
    ) -> Result<CommitSeq> {
        let (durable_through, target_commit) = {
            let state = lock(&self.persist_coordinator.inner)?;
            (
                state.durable_through,
                state.requested_through.max(minimum_target),
            )
        };
        let Some(deltas) = self.contiguous_pending_block_deltas(durable_through, target_commit)?
        else {
            return self.persist_physical(total_started, None, Some(target_commit));
        };
        if deltas.is_empty() {
            return Ok(durable_through);
        }
        self.persist_block_delta_batch(total_started, deltas)
    }

    fn persist_block_delta_batch(
        &self,
        total_started: Instant,
        mut deltas: Vec<BlockDeltaCommit>,
    ) -> Result<CommitSeq> {
        let mut block_delta_prestage_wait_nanos = self.wait_for_block_delta_prestage(&deltas)?;
        let durable_before = deltas
            .first()
            .and_then(|delta| delta.commit_seq.raw().checked_sub(1))
            .map(CommitSeq::from_raw)
            .ok_or_else(|| StorageError::conflict("block delta batch has no first sequence"))?;
        let requested_through = lock(&self.persist_coordinator.inner)?.requested_through;
        if let Some(expanded) =
            self.ready_contiguous_pending_block_deltas(durable_before, requested_through)?
            && expanded.len() > deltas.len()
        {
            deltas = expanded;
            block_delta_prestage_wait_nanos =
                block_delta_prestage_wait_nanos.saturating_add(
                    self.wait_for_block_delta_prestage(&deltas)?,
                );
        }
        let block_delta_selected_count = usize_to_u64(deltas.len());
        let block_delta_selected_bytes = deltas
            .iter()
            .map(|delta| delta.committed_bytes)
            .fold(0_u64, u64::saturating_add);
        let lock_started = Instant::now();
        let _persist_guard = lock(&self.persist_lock)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let snapshot_started = Instant::now();
        let previous_segments = lock(&self.persisted_segments)?.clone();
        let mut segment_ids = BTreeSet::new();
        for delta in &deltas {
            segment_ids.extend(delta.segment_ids());
        }
        let mut pending_append = lock(&self.pending_data_log_append)?.clone();
        pending_append.retain_current_placements(&segment_ids);
        let pending_segments = pending_append.segment_ids();
        let (nodes, payloads) = self.local.state_for_segment_ids(&segment_ids)?;
        let new_segments: Vec<_> = payloads
            .into_iter()
            .filter(|payload| {
                !previous_segments.contains(&payload.segment_id)
                    && !pending_segments.contains(&payload.segment_id)
            })
            .collect();
        let local_snapshot_nanos = duration_nanos_u64(snapshot_started.elapsed());

        let mut profile =
            self.durable
                .persist_block_delta_commits(
                    &deltas,
                    &nodes,
                    &segment_ids,
                    new_segments,
                    pending_append,
                )?;
        let durable_through = CommitSeq::from_raw(profile.durable_commit_high_water);
        lock(&self.persisted_segments)?.extend(segment_ids.iter().copied());
        lock(&self.pending_data_log_append)?.remove_segments(&segment_ids);
        self.prune_pending_block_deltas_through(durable_through)?;
        self.prune_block_delta_prestage_through(durable_through)?;
        profile.lock_wait_nanos = lock_wait_nanos;
        profile.block_delta_prestage_wait_nanos = block_delta_prestage_wait_nanos;
        profile.block_delta_selected_count = block_delta_selected_count;
        profile.block_delta_selected_bytes = block_delta_selected_bytes;
        profile.local_snapshot_nanos = local_snapshot_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        self.attach_metadata_publish_profile(&mut profile)?;
        self.record_persist_profile(profile)?;
        Ok(durable_through)
    }

    fn persist_until(&self, required: CommitSeq) -> Result<()> {
        loop {
            let mut state = lock(&self.persist_coordinator.inner)?;
            state.requested_through = state.requested_through.max(required);
            if state.durable_through >= required {
                return Ok(());
            }
            if !state.in_flight {
                let target_commit = state.requested_through;
                state.in_flight = true;
                drop(state);
                let result = self.persist_physical(Instant::now(), None, Some(target_commit));
                let mut state = lock(&self.persist_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                let generation = state.generation;
                match result {
                    Ok(durable_through) => {
                        state.durable_through = state.durable_through.max(durable_through);
                        state.requested_through =
                            state.requested_through.max(state.durable_through);
                        state.last_error = None;
                        self.persist_coordinator.cvar.notify_all();
                        if state.durable_through >= required {
                            return Ok(());
                        }
                        return Err(StorageError::conflict(
                            "durable persist did not reach required commit sequence",
                        ));
                    }
                    Err(error) => {
                        state.last_error = Some((generation, error.clone()));
                        self.persist_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
            }

            let generation = state.generation;
            while state.in_flight && state.generation == generation {
                state = wait_on_cvar(&self.persist_coordinator.cvar, state)?;
            }
            if state.durable_through >= required {
                return Ok(());
            }
            if state.generation != generation
                && let Some((error_generation, error)) = &state.last_error
                && *error_generation == state.generation
            {
                return Err(error.clone());
            }
        }
    }

    fn persist_now_with_catalog_changes(
        &self,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<()> {
        let total_started = Instant::now();
        loop {
            let mut state = lock(&self.persist_coordinator.inner)?;
            if !state.in_flight {
                state.in_flight = true;
                drop(state);
                let result = self.persist_physical(total_started, changed_catalog_segments, None);
                let mut state = lock(&self.persist_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                let generation = state.generation;
                match result {
                    Ok(durable_through) => {
                        state.durable_through = state.durable_through.max(durable_through);
                        state.requested_through =
                            state.requested_through.max(state.durable_through);
                        state.last_error = None;
                        self.persist_coordinator.cvar.notify_all();
                        return Ok(());
                    }
                    Err(error) => {
                        state.last_error = Some((generation, error.clone()));
                        self.persist_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
            }
            state = wait_on_cvar(&self.persist_coordinator.cvar, state)?;
            drop(state);
        }
    }

    fn persist_now(&self) -> Result<()> {
        self.persist_now_with_catalog_changes(None)
    }

    fn has_unfolded_block_deltas(&self) -> Result<bool> {
        Ok(!lock(&self.pending_block_deltas)?.is_empty() || self.durable.has_block_delta_commits()?)
    }

    fn fold_block_deltas_before_gc(&self) -> Result<()> {
        if self.has_unfolded_block_deltas()? {
            self.persist_now()?;
        }
        Ok(())
    }

    fn persist_physical(
        &self,
        total_started: Instant,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
        target_commit: Option<CommitSeq>,
    ) -> Result<CommitSeq> {
        let block_delta_prestage_wait_nanos = self.wait_for_all_block_delta_prestage()?;
        let lock_started = Instant::now();
        let _persist_guard = lock(&self.persist_lock)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let snapshot_started = Instant::now();
        let previous_segments = lock(&self.persisted_segments)?.clone();
        let pending_append = lock(&self.pending_data_log_append)?.clone();
        let mut exported_segments = previous_segments.clone();
        exported_segments.extend(pending_append.segment_ids());
        let previous_cursor = if target_commit.is_some() {
            self.durable.export_cursor()?
        } else {
            None
        };
        if pending_append.is_empty()
            && let (Some(target_commit), Some(previous_cursor)) =
                (target_commit, previous_cursor.as_ref())
            && !self.has_pending_block_delta_in_range(
                previous_cursor.next_commit_seq,
                target_commit,
            )?
            && let Some(delta) = self
                .local
                .native_metadata_delta_through(target_commit, previous_cursor)?
        {
            let segment_ids = delta.referenced_segment_ids.clone();
            let (nodes, payloads) = self.local.state_for_segment_ids(&segment_ids)?;
            let new_segments: Vec<_> = payloads
                .into_iter()
                .filter(|payload| !previous_segments.contains(&payload.segment_id))
                .collect();
            let mut profile = self.durable.persist_native_metadata_delta(
                &delta,
                &nodes,
                &segment_ids,
                new_segments,
            )?;
            lock(&self.persisted_segments)?.extend(segment_ids);
            *lock(&self.pending_data_log_append)? = PendingDataLogAppend::default();
            profile.lock_wait_nanos = lock_wait_nanos;
            profile.block_delta_prestage_wait_nanos = block_delta_prestage_wait_nanos;
            profile.local_snapshot_nanos = duration_nanos_u64(snapshot_started.elapsed());
            profile.total_nanos = duration_nanos_u64(total_started.elapsed());
            let durable_through = CommitSeq::from_raw(profile.durable_commit_high_water);
            self.attach_metadata_publish_profile(&mut profile)?;
            self.record_persist_profile(profile)?;
            return Ok(durable_through);
        }
        let (image, current_segments, new_segments) = if let Some(target_commit) = target_commit {
            self.local.state_for_durable_persist_through(
                &exported_segments,
                target_commit,
                previous_cursor.as_ref(),
            )?
        } else {
            self.local.state_for_durable_persist(&exported_segments)?
        };
        let local_snapshot_nanos = duration_nanos_u64(snapshot_started.elapsed());
        let outcome = self.durable.persist(
            &image,
            &previous_segments,
            &current_segments,
            new_segments,
            pending_append,
            changed_catalog_segments,
        )?;
        *lock(&self.persisted_segments)? = outcome.kept_segments;
        *lock(&self.pending_data_log_append)? = PendingDataLogAppend::default();
        let mut profile = outcome.profile;
        profile.lock_wait_nanos = lock_wait_nanos;
        profile.block_delta_prestage_wait_nanos = block_delta_prestage_wait_nanos;
        profile.local_snapshot_nanos = local_snapshot_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        let durable_through = CommitSeq::from_raw(profile.durable_commit_high_water);
        self.prune_pending_block_deltas_through(durable_through)?;
        self.attach_metadata_publish_profile(&mut profile)?;
        self.record_persist_profile(profile)?;
        Ok(durable_through)
    }

    fn persist_append_stream(&self, stream: &AppendStream) -> Result<u64> {
        let target = self.local.metadata.append_stream_prefix_persist_target(stream)?;
        self.persist_append_stream_prefix(stream, target)
    }

    fn persist_append_stream_prefix(&self, stream: &AppendStream, target: u64) -> Result<u64> {
        let mut observed_generation = 0_u64;
        {
            let mut state = lock(&self.stream_prefix_persist_coordinator.inner)?;
            state.add_request(stream, target);
        }
        loop {
            if let Some(durable_through) = self
                .local
                .metadata
                .append_stream_durable_high_water_if_reached(stream, target)?
            {
                lock(&self.stream_prefix_persist_coordinator.inner)?.release_request(stream.stream_id);
                return Ok(durable_through);
            }

            let mut state = lock(&self.stream_prefix_persist_coordinator.inner)?;
            if let Some((error_generation, error)) = state.last_error.clone()
                && error_generation > observed_generation
            {
                state.release_request(stream.stream_id);
                return Err(error);
            }
            if !state.in_flight {
                state.in_flight = true;
                let requests = state.snapshot_requests();
                drop(state);

                let result = self.persist_append_stream_batches_until(&requests);

                let mut state = lock(&self.stream_prefix_persist_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                observed_generation = state.generation;
                match result {
                    Ok(()) => {
                        state.last_error = None;
                        self.stream_prefix_persist_coordinator.cvar.notify_all();
                    }
                    Err(error) => {
                        state.release_request(stream.stream_id);
                        state.last_error = Some((state.generation, error.clone()));
                        self.stream_prefix_persist_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
                continue;
            }

            let generation = state.generation;
            while state.in_flight && state.generation == generation {
                state = wait_on_cvar(&self.stream_prefix_persist_coordinator.cvar, state)?;
            }
            observed_generation = state.generation;
        }
    }

    fn persist_append_stream_batches_until(&self, requests: &[(AppendStream, u64)]) -> Result<()> {
        let mut made_progress = false;
        for _ in 0..MAX_STREAM_PREFIX_PERSIST_GROUPS_PER_RUN {
            if self.append_stream_requests_reached(requests)? {
                return Ok(());
            }
            if !self.persist_one_append_stream_request_batch(requests)? {
                return Err(StorageError::conflict(
                    "append stream prefix persist target has no persistable records",
                ));
            }
            made_progress = true;
        }
        if made_progress {
            Ok(())
        } else {
            Err(StorageError::conflict(
                "append stream prefix persist target has no persistable records",
            ))
        }
    }

    fn append_stream_requests_reached(&self, requests: &[(AppendStream, u64)]) -> Result<bool> {
        for (stream, durable_through) in requests {
            if self
                .local
                .metadata
                .append_stream_durable_high_water_if_reached(stream, *durable_through)?
                .is_none()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn persist_one_append_stream_request_batch(
        &self,
        requests: &[(AppendStream, u64)],
    ) -> Result<bool> {
        let total_started = Instant::now();
        let _persist_guard = lock(&self.persist_lock)?;
        let lock_wait_nanos = duration_nanos_u64(total_started.elapsed());

        let snapshot_started = Instant::now();
        let plans = self
            .local
            .metadata
            .append_stream_prefix_persist_plans_for(requests, MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES)?;
        if plans.is_empty() {
            return Ok(false);
        }
        let segment_ids: BTreeSet<SegmentId> = BTreeSet::new();
        let run_log_refs: BTreeSet<_> = plans
            .iter()
            .flat_map(|plan| {
                plan.batch
                    .records
                    .iter()
                    .map(AppendStreamRunRecord::log_ref)
            })
            .collect();
        let stream_prefix_storage_node_count = usize_to_u64(
            plans
                .iter()
                .flat_map(|plan| plan.batch.records.iter())
                .map(AppendStreamRunRecord::storage_node)
                .collect::<BTreeSet<_>>()
                .len(),
        );
        let new_run_count = usize_to_u64(
            plans
                .iter()
                .flat_map(|plan| plan.batch.records.iter())
                .count(),
        );
        let new_run_bytes = plans
            .iter()
            .flat_map(|plan| plan.batch.records.iter())
            .map(|record| record.len)
            .fold(0_u64, u64::saturating_add);
        let mut stream_prefix_pending_lock_wait_nanos = 0_u64;
        let mut pending_stream_append = PendingDataLogAppend::default();
        for plan in &plans {
            let Some(lane) = self.existing_stream_append_lane(plan.stream.stream_id)? else {
                continue;
            };
            let plan_log_refs: BTreeSet<_> = plan
                .batch
                .records
                .iter()
                .map(AppendStreamRunRecord::log_ref)
                .collect();
            let pending_lock_started = Instant::now();
            let pending_stream = lock(&lane.pending)?;
            stream_prefix_pending_lock_wait_nanos = stream_prefix_pending_lock_wait_nanos
                .saturating_add(duration_nanos_u64(pending_lock_started.elapsed()));
            pending_stream_append.merge(pending_stream.selected_log_refs(&plan_log_refs));
        }
        let mut pending_log_refs = pending_stream_append.log_refs();
        let missing_log_refs: Vec<_> = run_log_refs
            .difference(&pending_log_refs)
            .copied()
            .collect();
        for log_ref in missing_log_refs {
            let path = data_log_path(
                &self.durable.paths.data_dir,
                log_ref.storage_node,
                log_ref.log_id,
            );
            let total_bytes = path.metadata().map_err(fs_error)?.len();
            pending_stream_append.logs.insert(
                log_ref,
                PendingDataLogManifest {
                    storage_node: log_ref.storage_node,
                    log_id: log_ref.log_id,
                    state: STREAM_DATA_LOG_STATE_ACTIVE.to_string(),
                    total_bytes,
                    needs_dir_sync: false,
                },
            );
            pending_log_refs.insert(log_ref);
        }
        let nodes = self.local.selected_state_for_segment_ids(&segment_ids)?;
        let cursor = self.local.durable_export_cursor()?;
        let exported_streams: Vec<_> = plans
            .iter()
            .map(|plan| {
                self.local
                    .metadata
                    .append_stream_durable_export_at(&plan.stream, plan.batch.durable_through)
            })
            .collect::<Result<_>>()?;
        let local_snapshot_nanos = duration_nanos_u64(snapshot_started.elapsed());

        let mut profile = self.durable.persist_preingested_append_stream_prefix(
            &cursor,
            &exported_streams,
            &nodes,
            &segment_ids,
            pending_stream_append,
        )?;
        for plan in &plans {
            if let Some(lane) = self.existing_stream_append_lane(plan.stream.stream_id)? {
                let plan_log_refs: BTreeSet<_> = plan
                    .batch
                    .records
                    .iter()
                    .map(AppendStreamRunRecord::log_ref)
                    .collect();
                let pending_lock_started = Instant::now();
                let mut pending_stream = lock(&lane.pending)?;
                stream_prefix_pending_lock_wait_nanos = stream_prefix_pending_lock_wait_nanos
                    .saturating_add(duration_nanos_u64(pending_lock_started.elapsed()));
                pending_stream.remove_log_refs(&plan_log_refs);
            }
            self.local
                .metadata
                .mark_append_stream_durable_through(&plan.stream, plan.batch.durable_through)?;
        }

        profile.lock_wait_nanos = lock_wait_nanos;
        profile.local_snapshot_nanos = local_snapshot_nanos;
        profile.stream_prefix_request_count = usize_to_u64(requests.len());
        profile.stream_prefix_plan_count = usize_to_u64(plans.len());
        profile.stream_prefix_record_count = new_run_count;
        profile.stream_prefix_payload_bytes = new_run_bytes;
        profile.stream_prefix_storage_node_count = stream_prefix_storage_node_count;
        profile.stream_prefix_pending_lock_wait_nanos = stream_prefix_pending_lock_wait_nanos;
        profile.new_segment_count = profile.new_segment_count.saturating_add(new_run_count);
        profile.new_segment_bytes = profile.new_segment_bytes.saturating_add(new_run_bytes);
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        self.attach_metadata_publish_profile(&mut profile)?;
        self.record_persist_profile(profile)?;
        Ok(true)
    }

    fn persist_append_stream_publish_delta(
        &self,
        stream: &AppendStream,
        commit_seq: CommitSeq,
        changed_segments: &BTreeSet<SegmentId>,
    ) -> Result<()> {
        let total_started = Instant::now();
        loop {
            let mut state = lock(&self.persist_coordinator.inner)?;
            state.requested_through = state.requested_through.max(commit_seq);
            if state.durable_through >= commit_seq {
                return Ok(());
            }
            if !state.in_flight {
                let target_commit = state.requested_through;
                state.in_flight = true;
                drop(state);
                let result = self.persist_append_stream_publish_delta_physical(
                    stream,
                    target_commit,
                    changed_segments,
                    total_started,
                );
                let mut state = lock(&self.persist_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                let generation = state.generation;
                match result {
                    Ok(durable_high_water) => {
                        state.durable_through = state.durable_through.max(durable_high_water);
                        state.requested_through =
                            state.requested_through.max(state.durable_through);
                        state.last_error = None;
                        self.persist_coordinator.cvar.notify_all();
                        if state.durable_through >= commit_seq {
                            return Ok(());
                        }
                        return Err(StorageError::conflict(
                            "append stream publish persist did not reach required commit sequence",
                        ));
                    }
                    Err(error) => {
                        state.last_error = Some((generation, error.clone()));
                        self.persist_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
            }

            let generation = state.generation;
            while state.in_flight && state.generation == generation {
                state = wait_on_cvar(&self.persist_coordinator.cvar, state)?;
            }
            if state.durable_through >= commit_seq {
                return Ok(());
            }
            if state.generation != generation
                && let Some((error_generation, error)) = &state.last_error
                && *error_generation == state.generation
            {
                return Err(error.clone());
            }
        }
    }

    fn persist_append_stream_publish_delta_physical(
        &self,
        stream: &AppendStream,
        minimum_target: CommitSeq,
        changed_segments: &BTreeSet<SegmentId>,
        total_started: Instant,
    ) -> Result<CommitSeq> {
        let target_commit = lock(&self.persist_coordinator.inner)?
            .requested_through
            .max(minimum_target);

        let snapshot_started = Instant::now();
        let Some(previous_cursor) = self.durable.export_cursor()? else {
            return self.persist_physical(total_started, None, Some(target_commit));
        };
        let pending_append = lock(&self.pending_data_log_append)?.clone();
        if !pending_append.is_empty() {
            return self.persist_physical(total_started, None, Some(target_commit));
        }
        let Some(delta) = self
            .local
            .native_append_publish_delta_through(stream, target_commit, &previous_cursor)?
        else {
            return self.persist_physical(total_started, None, Some(target_commit));
        };
        let previous_segments = lock(&self.persisted_segments)?.clone();
        if delta
            .referenced_segment_ids
            .iter()
            .any(|segment_id| !previous_segments.contains(segment_id))
        {
            return self.persist_physical(total_started, None, Some(target_commit));
        }
        let mut changed_segments = changed_segments.clone();
        changed_segments.extend(delta.referenced_segment_ids.iter().copied());
        let nodes = self
            .local
            .selected_state_for_segment_ids(&changed_segments)?;
        let local_snapshot_nanos = duration_nanos_u64(snapshot_started.elapsed());

        let mut profile = self.durable.persist_native_metadata_delta(
            &delta,
            &nodes,
            &changed_segments,
            Vec::new(),
        )?;
        let durable_high_water = CommitSeq::from_raw(profile.durable_commit_high_water);
        if durable_high_water < target_commit {
            return Err(StorageError::conflict(
                "append stream publish persist did not reach required commit sequence",
            ));
        }

        profile.lock_wait_nanos = 0;
        profile.local_snapshot_nanos = local_snapshot_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        self.attach_metadata_publish_profile(&mut profile)?;
        self.record_persist_profile(profile)?;
        Ok(durable_high_water)
    }

    /// Return the maintenance policy configured for this store.
    pub fn maintenance_policy(&self) -> MaintenancePolicy {
        self.maintenance_policy
    }

    /// Observe current durable maintenance pressure without mutating state.
    ///
    /// The observation is suitable for diagnostics or deterministic planning.
    /// It is not a lock or lease: executors must tolerate the state changing
    /// before a plan is run.
    pub fn observe_maintenance(&self) -> Result<MaintenanceObservation> {
        let cursor = *lock(&self.maintenance_cursor)?;
        self.durable.maintenance_observation(cursor, 0, 0, true)
    }

    pub fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        let observation = self.observe_maintenance()?;
        self.local
            .diagnostics_snapshot_with_maintenance(Some(&observation))
    }

    pub fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        self.local.drain_events(max)
    }

    /// Plan one maintenance tick from the current observation.
    ///
    /// This performs SQLite reads only. It does not compact logs, update the
    /// fairness cursor, throttle a write, or start background work.
    pub fn plan_maintenance(&self) -> Result<MaintenanceTickPlan> {
        let scheduler = MaintenanceScheduler::new(self.maintenance_policy)?;
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            0,
            0,
            policy_uses_sqlite_wal_pressure(self.maintenance_policy),
        )?;
        let plan = scheduler.step(&observation);
        self.local.observability.increment(|counters| {
            counters.maintenance_plans = counters.maintenance_plans.saturating_add(1);
            counters.maintenance_logs_selected = counters
                .maintenance_logs_selected
                .saturating_add(usize_to_u64(plan.diagnostics.selected_logs.len()));
            counters.maintenance_logs_skipped = counters
                .maintenance_logs_skipped
                .saturating_add(usize_to_u64(plan.diagnostics.skipped_logs.len()));
        });
        self.local
            .observability
            .record(StorageEventKind::MaintenancePlanned);
        Ok(plan)
    }

    /// Run one bounded maintenance tick synchronously.
    ///
    /// Success means completed compaction work and the fairness cursor were
    /// durably published. If the tick fails, already committed maintenance work
    /// remains valid, and acknowledged user data must remain readable.
    pub fn run_maintenance_tick(&self) -> Result<MaintenanceTickReport> {
        run_maintenance_tick_parts(&self.maintenance_parts(), 0, 0)
    }

    /// Stop the optional always-on maintenance worker.
    ///
    /// Manual and opportunistic stores have no worker, so this is a no-op. For
    /// always-on stores, the call waits for an in-flight bounded tick to finish
    /// before returning.
    pub fn shutdown_maintenance(&self) {
        if let Some(worker) = &self.maintenance_worker {
            worker.shutdown();
        }
    }

    fn admit_write(&self, bytes: u64, flushed: bool) -> Result<WriteAdmission> {
        let should_observe = self.maintenance_policy.write_backpressure_enabled
            || matches!(self.maintenance_policy.mode, MaintenanceMode::Opportunistic);
        if !should_observe {
            return Ok(WriteAdmission::Accept);
        }
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            bytes,
            if flushed { bytes } else { 0 },
            policy_uses_sqlite_wal_pressure(self.maintenance_policy),
        )?;
        let plan = MaintenanceScheduler::new(self.maintenance_policy)?.step(&observation);
        if self.maintenance_policy.write_backpressure_enabled
            && let Some(reason) = plan.admission.unavailable_reason()
        {
            self.local.observability.record_with_update(
                StorageEventKind::CoordinatorWriteUnavailable,
                None,
                None,
                None,
                Some(reason),
                |counters| {
                    counters.coordinator_write_attempts =
                        counters.coordinator_write_attempts.saturating_add(1);
                    counters.coordinator_write_unavailable =
                        counters.coordinator_write_unavailable.saturating_add(1);
                },
            );
            return Err(StorageError::unavailable(reason));
        }
        if matches!(self.maintenance_policy.mode, MaintenanceMode::Opportunistic)
            && !plan.commands.is_empty()
        {
            self.run_maintenance_tick()?;
            return Ok(WriteAdmission::AcceptAndSchedule);
        }
        if plan.commands.is_empty() {
            Ok(WriteAdmission::Accept)
        } else {
            Ok(WriteAdmission::AcceptAndSchedule)
        }
    }

    fn after_successful_write(&self, _admission: WriteAdmission) {
        match self.maintenance_policy.mode {
            MaintenanceMode::Manual | MaintenanceMode::Opportunistic => {}
            MaintenanceMode::AlwaysOn => self.notify_background_maintenance(),
        }
    }

    pub fn metadata(&self) -> Arc<InMemoryMetadataPlane> {
        self.local.metadata()
    }

    pub fn segment_catalog(&self) -> Arc<InMemoryLocalSegmentCatalog> {
        self.local.segment_catalog()
    }

    pub fn segment_store(&self) -> Arc<InMemorySegmentStore> {
        self.local.segment_store()
    }

    #[cfg(test)]
    fn storage_node_ids_for_test(&self) -> Vec<StorageNodeId> {
        self.local.storage_node_ids_for_test()
    }

    #[cfg(test)]
    fn set_persist_delay_for_test(&self, delay: Option<Duration>) -> Result<()> {
        *lock(&self.durable.persist_delay)? = delay;
        Ok(())
    }

    #[cfg(test)]
    fn fail_next_persist_for_test(&self) {
        self.durable.fail_next_persist.store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn fail_next_prestage_for_test(&self) {
        self.durable.fail_next_prestage.store(true, Ordering::SeqCst);
    }

    pub fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_device(MetadataCreateDeviceRequest::from(request))
                .map(|head| head.device_id)
        })
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        self.local.metadata.device_info(device_id)
    }

    pub fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id)
        })
    }

    pub fn create_file(
        &self,
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    ) -> Result<FileId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_file(MetadataCreateFileRequest {
                    keyspace_id,
                    request,
                })
                .map(|head| head.file_id)
        })
    }

    pub fn open_append_stream(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendStream> {
        self.local.open_append_stream(keyspace_id, file_id)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.run_and_persist(|local| local.metadata.checkpoint(device_id))
    }

    pub fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        self.run_and_persist(|local| local.metadata.checkpoint_keyspace(keyspace_id))
    }

    pub fn snapshot_keyspace(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                    source,
                    target: request.target,
                    name: request.name,
                })
                .map(|head| head.keyspace_id)
        })
    }

    pub fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId> {
        self.run_and_persist(|local| local.restore_keyspace(source, point))
    }

    pub fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<WriteCommit> {
        self.write_device_with_integrity(
            device_id,
            offset,
            data,
            durability,
            PayloadIntegrity::Verified,
        )
    }

    pub fn write_device_with_integrity(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<WriteCommit> {
        let len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, len);
        if data.is_empty() {
            let commit = self
                .local
                .write_device_with_integrity(device_id, offset, data, durability, payload_integrity)?;
            return Ok(commit);
        }
        let commit = self.commit_block_batch(
            device_id,
            &[BlockBatchWrite {
                offset,
                bytes: data.to_vec(),
                payload_integrity,
            }],
            durability,
        )?;
        Ok(WriteCommit {
            device_id: commit.device_id,
            commit_seq: commit.commit_seq,
            range,
            durability: commit.durability,
        })
    }

    pub fn commit_block_batch(
        &self,
        device_id: DeviceId,
        writes: &[BlockBatchWrite],
        durability: crate::api::WriteDurability,
    ) -> Result<BlockBatchCommit> {
        let flushed = matches!(durability, crate::api::WriteDurability::Flushed);
        let total_bytes = writes.iter().try_fold(0u64, |total, write| {
            total
                .checked_add(u64::try_from(write.bytes.len()).map_err(|_| {
                    StorageError::invalid_argument("block batch byte length overflows u64")
                })?)
                .ok_or_else(|| StorageError::invalid_argument("block batch byte length overflows"))
        })?;
        let admission = self.admit_write(total_bytes, flushed)?;
        let result = if flushed {
            self.local
                .commit_block_batch_with_delta(device_id, writes, durability)
                .and_then(|committed| {
                    let delta = committed.delta.clone();
                    let commit = committed.commit;
                    self.record_pending_block_delta(delta)?;
                    self.persist_block_deltas_until(commit.commit_seq)?;
                    Ok(commit)
                })
        } else {
            let committed = {
                let _staging_guard = lock(&self.block_delta_staging_lock)?;
                let committed =
                    self.local
                        .commit_block_batch_with_delta(device_id, writes, durability)?;
                if let Some(delta) = committed.delta.clone() {
                    self.record_pending_block_delta(Some(delta.clone()))?;
                    self.begin_block_delta_prestage(delta.commit_seq)?;
                }
                committed
            };
            if let Some(delta) = committed.delta.clone() {
                let result = self.prestage_block_delta_segments(&delta);
                self.finish_block_delta_prestage(delta.commit_seq, result)?;
            }
            Ok(committed.commit)
        };
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        self.read_device_with_verification(device_id, range, buf, ReadVerification::Default)
    }

    pub fn read_device_with_verification(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let total_started = Instant::now();
        let resolve_started = Instant::now();
        let (plan, resolve_profile) =
            MetadataReadService::resolve_block_read(&self.local, device_id, range)?;
        let metadata_resolve_nanos = duration_nanos_u64(resolve_started.elapsed());
        let mut profile = assemble_read_plan_profiled(self, plan, verification, buf)?;
        profile.metadata_resolve_nanos = metadata_resolve_nanos;
        profile.metadata_lock_wait_nanos = resolve_profile.metadata_lock_wait_nanos;
        profile.metadata_tree_walk_nanos = resolve_profile.metadata_tree_walk_nanos;
        profile.metadata_placement_lookup_nanos =
            resolve_profile.metadata_placement_lookup_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        self.record_read_profile(profile)
    }

    pub fn write_zeroes(&self, device_id: DeviceId, offset: u64, len: u64) -> Result<WriteCommit> {
        let admission = self.admit_write(len, true)?;
        let result = self
            .local
            .write_zeroes(device_id, offset, len)
            .and_then(|commit| {
                self.persist_until(commit.commit_seq)?;
                Ok(commit)
            });
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn discard_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        len: u64,
    ) -> Result<WriteCommit> {
        let admission = self.admit_write(len, true)?;
        let result = self
            .local
            .discard_device(device_id, offset, len)
            .and_then(|commit| {
                self.persist_until(commit.commit_seq)?;
                Ok(commit)
            });
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn commit_file_batch(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: &[FileBatchWrite],
        durability: crate::api::WriteDurability,
    ) -> Result<FileWriteCommit> {
        self.commit_file_batch_with_integrity(
            keyspace_id,
            file_id,
            writes,
            durability,
            PayloadIntegrity::Verified,
        )
    }

    pub fn commit_file_batch_with_integrity(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: &[FileBatchWrite],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit> {
        let flushed = matches!(durability, crate::api::WriteDurability::Flushed);
        let admission = self.admit_write(
            writes.iter().try_fold(0u64, |total, write| {
                total
                    .checked_add(u64::try_from(write.bytes.len()).map_err(|_| {
                        StorageError::invalid_argument("write byte length overflows u64")
                    })?)
                    .ok_or_else(|| StorageError::invalid_argument("write byte length overflows"))
            })?,
            flushed,
        )?;
        let result = self
            .local
            .commit_file_batch_with_integrity(
                keyspace_id,
                file_id,
                writes,
                durability,
                payload_integrity,
            )
            .and_then(|commit| {
                if flushed {
                    self.persist_until(commit.commit_seq)?;
                }
                Ok(commit)
            });
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn append_stream(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<AppendTicket> {
        self.append_stream_with_integrity(stream, data, durability, PayloadIntegrity::Verified)
    }

    fn stream_append_lane(&self, stream_id: AppendStreamId) -> Result<Arc<StreamAppendLane>> {
        let mut lanes = lock(&self.stream_append_lanes)?;
        Ok(Arc::clone(
            lanes
                .entry(stream_id)
                .or_insert_with(|| Arc::new(StreamAppendLane::new())),
        ))
    }

    fn existing_stream_append_lane(
        &self,
        stream_id: AppendStreamId,
    ) -> Result<Option<Arc<StreamAppendLane>>> {
        Ok(lock(&self.stream_append_lanes)?.get(&stream_id).cloned())
    }

    fn remove_stream_append_lane(&self, stream_id: AppendStreamId) -> Result<()> {
        lock(&self.stream_append_lanes)?.remove(&stream_id);
        Ok(())
    }

    pub fn append_stream_with_integrity(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        let flushed = matches!(durability, crate::api::WriteDurability::Flushed);
        let admission = self.admit_write(
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?,
            flushed,
        )?;
        let lane = self.stream_append_lane(stream.stream_id)?;
        let _append_guard = lock(&lane.append)?;
        let result = self
            .local
            .prepare_append_stream_run(
                stream,
                data.len(),
                crate::api::WriteDurability::Acknowledged,
            )
            .and_then(|prepared| {
                let payload = DurableAppendRunChunkPayload {
                    run_id: prepared.run_id,
                    storage_node: prepared.storage_node,
                    stream_id: prepared.stream.stream_id,
                    writer_epoch: prepared.stream.writer_epoch,
                    keyspace_id: prepared.stream.keyspace_id,
                    file_id: prepared.stream.file_id,
                    file_offset_start: prepared.range.offset,
                    payload_integrity,
                    chunks: vec![Arc::from(data)],
                };
                let pending_base = lock(&lane.pending)?.clone();
                let (run, append, _) = self
                    .durable
                    .append_run_payload_chunks_unsynced(payload, Some(&pending_base))?;
                let appended_log_refs = append.log_refs();
                lock(&lane.pending)?.merge(append);
                let commit = self.local.commit_prepared_append_stream_run(prepared, run);
                let ticket = match commit {
                    Ok(ticket) => ticket,
                    Err(error) => {
                        lock(&lane.pending)?.remove_log_refs(&appended_log_refs);
                        return Err(error);
                    }
                };
                if flushed {
                    self.persist_append_stream(stream)?;
                }
                Ok(ticket)
            });
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn submit_append_publish(
        &self,
        stream: &AppendStream,
        publish_through: u64,
    ) -> Result<AppendPublishTicket> {
        self.local.submit_append_publish(stream, publish_through)
    }

    pub fn wait_append_publish(
        &self,
        ticket: &AppendPublishTicket,
    ) -> Result<AppendPublishCommit> {
        let stream = match self.local.metadata.append_publish_ticket_status(ticket)? {
            AppendPublishTicketStatus::Completed(commit) => return Ok(commit),
            AppendPublishTicketStatus::Pending(stream) => stream,
        };
        self.persist_append_stream_prefix(&stream, ticket.publish_through)?;
        let commit = self
            .local
            .wait_append_publish(ticket, WriteDurability::Flushed)?;
        self.persist_append_stream_publish_delta(&stream, commit.commit_seq, &BTreeSet::new())?;
        Ok(commit)
    }

    pub fn publish_append_stream(
        &self,
        stream: &AppendStream,
        publish_through: u64,
    ) -> Result<AppendPublishCommit> {
        let ticket = self.submit_append_publish(stream, publish_through)?;
        self.wait_append_publish(&ticket)
    }

    pub fn release_append_stream(&self, stream: &AppendStream) -> Result<()> {
        self.local.release_append_stream(stream)?;
        self.remove_stream_append_lane(stream.stream_id)?;
        self.persist_now()
    }

    pub fn abort_append_stream(&self, stream: &AppendStream) -> Result<()> {
        self.local.abort_append_stream(stream)?;
        self.remove_stream_append_lane(stream.stream_id)?;
        self.persist_now()
    }

    pub fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.read_file_with_verification(
            keyspace_id,
            file_id,
            range,
            buf,
            ReadVerification::Default,
        )
    }

    pub fn read_file_with_verification(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let total_started = Instant::now();
        let resolve_started = Instant::now();
        let (plan, resolve_profile) =
            MetadataReadService::resolve_file_read(&self.local, keyspace_id, file_id, range)?;
        let metadata_resolve_nanos = duration_nanos_u64(resolve_started.elapsed());
        let mut profile = assemble_read_plan_profiled(self, plan, verification, buf)?;
        profile.metadata_resolve_nanos = metadata_resolve_nanos;
        profile.metadata_lock_wait_nanos = resolve_profile.metadata_lock_wait_nanos;
        profile.metadata_tree_walk_nanos = resolve_profile.metadata_tree_walk_nanos;
        profile.metadata_placement_lookup_nanos =
            resolve_profile.metadata_placement_lookup_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        self.record_read_profile(profile)
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        self.run_and_persist(|local| local.fork_device(source, request))
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        self.run_and_persist(|local| local.restore_device(source, point))
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.run_and_persist(|local| local.delete_device(device_id))
    }

    pub fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        let info = {
            let _staging_guard = lock(&self.block_delta_staging_lock)?;
            self.local.metadata.device_info(device_id)?
        };
        self.persist_block_deltas_until(info.latest_commit)?;
        Ok(FlushResult {
            device_id,
            durable_through: info.latest_commit,
        })
    }

    pub fn flush_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FlushResult> {
        let head = self.local.metadata.get_file_head(keyspace_id, file_id)?;
        self.persist_until(head.latest_commit)?;
        Ok(FlushResult {
            device_id: DeviceId::from_raw(file_id.raw()),
            durable_through: head.latest_commit,
        })
    }

    pub fn compact_data_logs(
        &self,
        policy: DurableDataLogPolicy,
    ) -> Result<DurableCompactionReport> {
        let _persist_guard = lock(&self.persist_lock)?;
        self.durable.compact_data_logs(policy)
    }

    pub fn run_metadata_custodian(
        &self,
        policy: RetentionPolicy,
    ) -> Result<MetadataCustodianReport> {
        self.fold_block_deltas_before_gc()?;
        let result = self.local.run_metadata_custodian(policy);
        let changed = result.as_ref().ok().map(|report| {
            report
                .catalog_released_segments
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
        });
        let persist = self.persist_with_catalog_changes(changed.as_ref());
        let report = match (result, persist) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }?;
        self.notify_background_maintenance();
        Ok(report)
    }

    pub fn run_storage_node_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let result = self.local.run_storage_node_custodian(expired_write_intents);
        let changed = result.as_ref().ok().map(|report| {
            report
                .expired_reservations
                .iter()
                .chain(&report.failed_writes)
                .chain(&report.orphan_segments)
                .chain(&report.deleted_released_segments)
                .copied()
                .collect::<BTreeSet<_>>()
        });
        let persist = self.persist_with_catalog_changes(changed.as_ref());
        let report = match (result, persist) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }?;
        self.notify_background_maintenance();
        Ok(report)
    }

    fn notify_background_maintenance(&self) {
        if matches!(self.maintenance_policy.mode, MaintenanceMode::AlwaysOn)
            && let Some(worker) = &self.maintenance_worker
        {
            worker.notify();
        }
    }

    fn run_and_persist<T>(&self, op: impl FnOnce(&LocalCoordinator) -> Result<T>) -> Result<T> {
        self.run_and_maybe_persist(true, op)
    }

    fn run_and_maybe_persist<T>(
        &self,
        persist: bool,
        op: impl FnOnce(&LocalCoordinator) -> Result<T>,
    ) -> Result<T> {
        let result = op(&self.local);
        if !persist {
            return result;
        }
        let persist = self.persist();
        match (result, persist) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }
    }

    fn persist(&self) -> Result<()> {
        self.persist_with_catalog_changes(None)
    }

    fn persist_with_catalog_changes(
        &self,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<()> {
        self.persist_now_with_catalog_changes(changed_catalog_segments)
    }
}

impl StorageNodeReadService for DurableCoordinator {
    fn read_segment_source(
        &self,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        range: ByteRange,
        integrity: SegmentPayloadIntegrity,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<ReadSourceProfile> {
        self.local.read_segment_source(
            storage_node,
            segment_id,
            range,
            integrity,
            verification,
            buf,
        )
    }

    fn read_append_run_source(
        &self,
        storage_node: StorageNodeId,
        log_id: u64,
        range: ByteRange,
        integrity: SegmentPayloadIntegrity,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<ReadSourceProfile> {
        self.durable.read_append_run_source_payload(
            storage_node,
            log_id,
            range,
            integrity,
            verification,
            buf,
        )
    }
}

impl ObservableProvider for LocalCoordinator {
    fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        LocalCoordinator::diagnostics_snapshot(self)
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        LocalCoordinator::drain_events(self, max)
    }
}

impl ObservableProvider for DurableCoordinator {
    fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        DurableCoordinator::diagnostics_snapshot(self)
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        DurableCoordinator::drain_events(self, max)
    }
}
