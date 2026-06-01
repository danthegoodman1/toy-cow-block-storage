/// Durable in-process coordinator using SQLite metadata and node-scoped rolled
/// data logs.
#[derive(Debug, Clone)]
pub struct DurableCoordinator {
    local: LocalCoordinator,
    durable: DurableSqliteStore,
    persisted_segments: Arc<Mutex<BTreeSet<SegmentId>>>,
    pending_data_log_append: Arc<Mutex<PendingDataLogAppend>>,
    pending_stream_data_log_append: Arc<Mutex<PendingDataLogAppend>>,
    stream_append_lanes: Arc<Mutex<BTreeMap<AppendStreamId, Arc<Mutex<()>>>>>,
    persist_lock: Arc<Mutex<()>>,
    persist_coordinator: Arc<PersistCoordinator>,
    stream_flush_coordinator: Arc<StreamFlushCoordinator>,
    persist_profiler: Arc<Mutex<Option<PersistProfiler>>>,
    maintenance_policy: MaintenancePolicy,
    maintenance_cursor: Arc<Mutex<Option<DurableDataLogRef>>>,
    maintenance_worker: Option<Arc<MaintenanceWorker>>,
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
            pending_data_log_append: Arc::new(Mutex::new(PendingDataLogAppend::default())),
            pending_stream_data_log_append: Arc::new(Mutex::new(PendingDataLogAppend::default())),
            stream_append_lanes: Arc::new(Mutex::new(BTreeMap::new())),
            persist_lock: Arc::new(Mutex::new(())),
            persist_coordinator: Arc::new(PersistCoordinator::new(durable_through)),
            stream_flush_coordinator: Arc::new(StreamFlushCoordinator::new()),
            persist_profiler: Arc::new(Mutex::new(None)),
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

    fn persist_until(&self, required: CommitSeq) -> Result<()> {
        let total_started = Instant::now();
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
                let result = self.persist_physical(total_started, None, Some(target_commit));
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

    fn persist_physical(
        &self,
        total_started: Instant,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
        target_commit: Option<CommitSeq>,
    ) -> Result<CommitSeq> {
        let _persist_guard = lock(&self.persist_lock)?;
        let lock_wait_nanos = duration_nanos_u64(total_started.elapsed());
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
        profile.local_snapshot_nanos = local_snapshot_nanos;
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        let durable_through = CommitSeq::from_raw(profile.durable_commit_high_water);
        self.attach_metadata_publish_profile(&mut profile)?;
        self.record_persist_profile(profile)?;
        Ok(durable_through)
    }

    fn persist_append_stream(&self, stream: &AppendStream) -> Result<DurableAppendMark> {
        let target = self.local.metadata.append_stream_flush_target(stream)?;
        let mut observed_generation = 0_u64;
        {
            let mut state = lock(&self.stream_flush_coordinator.inner)?;
            state.add_request(stream, target.durable_through);
        }
        loop {
            if let Some(mark) = self
                .local
                .metadata
                .append_stream_durable_mark_if_reached(stream, target.durable_through)?
            {
                lock(&self.stream_flush_coordinator.inner)?.release_request(stream.stream_id);
                return Ok(mark);
            }

            let mut state = lock(&self.stream_flush_coordinator.inner)?;
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

                let mut state = lock(&self.stream_flush_coordinator.inner)?;
                state.in_flight = false;
                state.generation = state.generation.saturating_add(1);
                observed_generation = state.generation;
                match result {
                    Ok(()) => {
                        state.last_error = None;
                        self.stream_flush_coordinator.cvar.notify_all();
                    }
                    Err(error) => {
                        state.release_request(stream.stream_id);
                        state.last_error = Some((state.generation, error.clone()));
                        self.stream_flush_coordinator.cvar.notify_all();
                        return Err(error);
                    }
                }
                continue;
            }

            let generation = state.generation;
            while state.in_flight && state.generation == generation {
                state = wait_on_cvar(&self.stream_flush_coordinator.cvar, state)?;
            }
            observed_generation = state.generation;
        }
    }

    fn persist_append_stream_batches_until(&self, requests: &[(AppendStream, u64)]) -> Result<()> {
        for _ in 0..MAX_STREAM_FLUSH_GROUPS_PER_RUN {
            if self.append_stream_requests_reached(requests)? {
                return Ok(());
            }
            if !self.persist_one_append_stream_request_batch(requests)? {
                return Err(StorageError::conflict(
                    "append stream flush target has no persistable records",
                ));
            }
        }
        Err(StorageError::conflict(
            "append stream flush target exceeded bounded persist groups",
        ))
    }

    fn append_stream_requests_reached(&self, requests: &[(AppendStream, u64)]) -> Result<bool> {
        for (stream, durable_through) in requests {
            if self
                .local
                .metadata
                .append_stream_durable_mark_if_reached(stream, *durable_through)?
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
            .append_stream_flush_plans_for(requests, MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES)?;
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
        let pending_stream = lock(&self.pending_stream_data_log_append)?;
        let mut pending_stream_append = pending_stream.selected_log_refs(&run_log_refs);
        drop(pending_stream);
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

        let mut profile = self.durable.persist_preingested_append_stream_flush(
            &cursor,
            &exported_streams,
            &nodes,
            &segment_ids,
            pending_stream_append,
        )?;
        let mut pending_stream = lock(&self.pending_stream_data_log_append)?;
        pending_stream.remove_log_refs(&run_log_refs);
        for plan in &plans {
            self.local
                .metadata
                .mark_append_stream_durable_through(&plan.stream, plan.batch.durable_through)?;
        }

        profile.lock_wait_nanos = lock_wait_nanos;
        profile.local_snapshot_nanos = local_snapshot_nanos;
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
        let flushed = matches!(durability, crate::api::WriteDurability::Flushed);
        let admission = self.admit_write(
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?,
            flushed,
        )?;
        let result = self
            .local
            .write_device_with_integrity(device_id, offset, data, durability, payload_integrity)
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
        let segment_ids = self.local.segment_ids_for_device_read(device_id, range)?;
        self.local.read_device_unverified(device_id, range, buf)?;
        self.local
            .verify_segment_payloads_for_read(segment_ids, verification)
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

    fn stream_append_lane(&self, stream_id: AppendStreamId) -> Result<Arc<Mutex<()>>> {
        let mut lanes = lock(&self.stream_append_lanes)?;
        Ok(Arc::clone(
            lanes
                .entry(stream_id)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
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
        let _lane_guard = lock(&lane)?;
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
                let pending_base = lock(&self.pending_stream_data_log_append)?.clone();
                let (run, append, _) = self
                    .durable
                    .append_run_payload_chunks_unsynced(payload, Some(&pending_base))?;
                let appended_log_refs = append.log_refs();
                lock(&self.pending_stream_data_log_append)?.merge(append);
                let commit = self.local.commit_prepared_append_stream_run(prepared, run);
                let ticket = match commit {
                    Ok(ticket) => ticket,
                    Err(error) => {
                        lock(&self.pending_stream_data_log_append)?
                            .remove_log_refs(&appended_log_refs);
                        return Err(error);
                    }
                };
                if flushed {
                    self.flush_append_stream(stream)?;
                }
                Ok(ticket)
            });
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn flush_append_stream(&self, stream: &AppendStream) -> Result<DurableAppendMark> {
        self.persist_append_stream(stream)
    }

    pub fn publish_append_stream(
        &self,
        stream: &AppendStream,
        mark: &DurableAppendMark,
    ) -> Result<AppendPublishCommit> {
        let commit = self
            .local
            .publish_append_stream(stream, mark, WriteDurability::Flushed)?;
        self.persist_append_stream_publish_delta(stream, commit.commit_seq, &BTreeSet::new())?;
        Ok(commit)
    }

    pub fn abort_append_stream(&self, stream: &AppendStream) -> Result<()> {
        self.local.abort_append_stream(stream)?;
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
        let segment_ids = self
            .local
            .segment_ids_for_file_read(keyspace_id, file_id, range)?;
        let run_extents = self
            .local
            .run_extents_for_file_read(keyspace_id, file_id, range)?;
        self.local
            .read_file_unverified(keyspace_id, file_id, range, buf)?;
        for extent in run_extents {
            let bytes = self
                .durable
                .read_append_run_range_payload(&extent.run, verification)?;
            let output_offset = usize::try_from(
                extent
                    .file_offset_start
                    .checked_sub(range.offset)
                    .ok_or_else(|| {
                        StorageError::corrupt("run-backed extent precedes read range")
                    })?,
            )
            .map_err(|_| StorageError::invalid_argument("run read offset overflows usize"))?;
            let output_len = bytes.len();
            let output_end = output_offset
                .checked_add(output_len)
                .ok_or_else(|| StorageError::invalid_argument("run read end overflows"))?;
            let output = buf
                .get_mut(output_offset..output_end)
                .ok_or_else(|| StorageError::corrupt("run-backed read output exceeds buffer"))?;
            output.copy_from_slice(&bytes);
        }
        self.local
            .verify_segment_payloads_for_read(segment_ids, verification)
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
        let info = self.local.metadata.device_info(device_id)?;
        self.persist_until(info.latest_commit)?;
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
