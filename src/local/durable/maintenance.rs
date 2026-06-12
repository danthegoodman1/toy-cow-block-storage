#[derive(Clone)]
pub(super) struct DurableMaintenanceParts {
    local: LocalCoordinator,
    durable: DurableSqliteStore,
    persist_lock: Arc<Mutex<()>>,
    pending_data_log_append: Arc<Mutex<PendingDataLogAppend>>,
    maintenance_cursor: Arc<Mutex<Option<DurableDataLogRef>>>,
    maintenance_policy: MaintenancePolicy,
}

#[derive(Debug)]
pub(super) struct MaintenanceWorkerState {
    shutdown: bool,
    notified: bool,
}

pub(super) struct MaintenanceWorker {
    state: Arc<(Mutex<MaintenanceWorkerState>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MaintenanceWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceWorker").finish_non_exhaustive()
    }
}

impl MaintenanceWorker {
    fn start(parts: DurableMaintenanceParts) -> Result<Arc<Self>> {
        let state = Arc::new((
            Mutex::new(MaintenanceWorkerState {
                shutdown: false,
                notified: false,
            }),
            Condvar::new(),
        ));
        let worker_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .name("toy-cow-maintenance".to_string())
            .spawn(move || maintenance_worker_loop(parts, worker_state))
            .map_err(|error| {
                StorageError::unavailable(format!("failed to start maintenance worker: {error}"))
            })?;
        Ok(Arc::new(Self {
            state,
            handle: Mutex::new(Some(handle)),
        }))
    }

    fn notify(&self) {
        let (lock_state, cvar) = &*self.state;
        if let Ok(mut state) = lock_state.lock() {
            state.notified = true;
            cvar.notify_one();
        }
    }

    fn shutdown(&self) {
        let (lock_state, cvar) = &*self.state;
        if let Ok(mut state) = lock_state.lock() {
            state.shutdown = true;
            state.notified = true;
            cvar.notify_one();
        }
        if let Ok(mut handle) = self.handle.lock()
            && let Some(handle) = handle.take()
        {
            let _ = handle.join();
        }
    }
}

impl Drop for MaintenanceWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(super) fn maintenance_worker_loop(
    parts: DurableMaintenanceParts,
    state: Arc<(Mutex<MaintenanceWorkerState>, Condvar)>,
) {
    loop {
        let (lock_state, cvar) = &*state;
        let mut guard = match lock_state.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        while !guard.shutdown && !guard.notified {
            guard = match cvar.wait(guard) {
                Ok(guard) => guard,
                Err(_) => return,
            };
        }
        if guard.shutdown {
            return;
        }
        guard.notified = false;
        drop(guard);

        while let Ok(report) = run_maintenance_tick_parts(&parts, 0, 0) {
            if report.plan.commands.is_empty() {
                break;
            }
        }
    }
}

pub(super) fn empty_compaction_report() -> DurableCompactionReport {
    DurableCompactionReport {
        deleted_logs: Vec::new(),
        relocated_logs: Vec::new(),
        relocated_segments: Vec::new(),
        bytes_copied: 0,
        bytes_deleted: 0,
    }
}

pub(super) fn maintenance_tick_data_log_policy(policy: MaintenancePolicy) -> DurableDataLogPolicy {
    let mut data_log_policy = policy.data_log_policy;
    data_log_policy.max_compaction_copy_bytes = data_log_policy
        .max_compaction_copy_bytes
        .min(policy.compaction_copy_budget_per_tick);
    data_log_policy
}

pub(super) fn run_maintenance_tick_parts(
    parts: &DurableMaintenanceParts,
    recent_write_bytes: u64,
    recent_flushed_write_bytes: u64,
) -> Result<MaintenanceTickReport> {
    let scheduler = MaintenanceScheduler::new(parts.maintenance_policy)?;
    let cursor = *lock(&parts.maintenance_cursor)?;
    let observation = parts.durable.maintenance_observation(
        cursor,
        recent_write_bytes,
        recent_flushed_write_bytes,
        policy_uses_sqlite_wal_pressure(parts.maintenance_policy),
    )?;
    let plan = scheduler.step(&observation);
    parts.local.observability.increment(|counters| {
        counters.maintenance_plans = counters.maintenance_plans.saturating_add(1);
        counters.maintenance_logs_selected = counters
            .maintenance_logs_selected
            .saturating_add(usize_to_u64(plan.diagnostics.selected_logs.len()));
        counters.maintenance_logs_skipped = counters
            .maintenance_logs_skipped
            .saturating_add(usize_to_u64(plan.diagnostics.skipped_logs.len()));
    });
    parts
        .local
        .observability
        .record(StorageEventKind::MaintenancePlanned);
    let mut compaction = empty_compaction_report();
    if !plan.commands.is_empty() {
        let _persist_guard = lock(&parts.persist_lock)?;
        // Logs that still carry unpublished placements (catalog rows queued
        // behind a flushed ack or an acknowledged write) hold durable payload
        // bytes the catalog cannot see yet; compacting them would delete
        // those bytes. Their placements publish shortly, so skip them this
        // tick. Reading the pending set after taking the persist lock is
        // race-free: placements leave the set only after their rows commit.
        let pending_log_refs: BTreeSet<DurableDataLogRef> =
            lock(&parts.pending_data_log_append)?.log_refs();
        for command in &plan.commands {
            match command {
                MaintenanceCommand::CompactDataLogs { logs } => {
                    let logs: Vec<DurableDataLogRef> = logs
                        .iter()
                        .copied()
                        .filter(|log_ref| !pending_log_refs.contains(log_ref))
                        .collect();
                    let report = parts.durable.compact_data_log_refs(
                        maintenance_tick_data_log_policy(parts.maintenance_policy),
                        &logs,
                    )?;
                    compaction.deleted_logs.extend(report.deleted_logs);
                    compaction.relocated_logs.extend(report.relocated_logs);
                    compaction
                        .relocated_segments
                        .extend(report.relocated_segments);
                    compaction.bytes_copied = compaction
                        .bytes_copied
                        .checked_add(report.bytes_copied)
                        .ok_or_else(|| {
                            StorageError::conflict("maintenance bytes_copied overflow")
                        })?;
                    compaction.bytes_deleted = compaction
                        .bytes_deleted
                        .checked_add(report.bytes_deleted)
                        .ok_or_else(|| {
                            StorageError::conflict("maintenance bytes_deleted overflow")
                        })?;
                }
            }
        }
    }
    if plan.next_cursor != cursor {
        parts.durable.persist_maintenance_cursor(plan.next_cursor)?;
        *lock(&parts.maintenance_cursor)? = plan.next_cursor;
    }
    parts.local.observability.increment(|counters| {
        counters.maintenance_ticks = counters.maintenance_ticks.saturating_add(1);
        counters.maintenance_bytes_copied = counters
            .maintenance_bytes_copied
            .saturating_add(compaction.bytes_copied);
        counters.maintenance_bytes_deleted = counters
            .maintenance_bytes_deleted
            .saturating_add(compaction.bytes_deleted);
    });
    parts
        .local
        .observability
        .record(StorageEventKind::MaintenanceTicked);
    Ok(MaintenanceTickReport { plan, compaction })
}

pub(super) fn policy_uses_sqlite_wal_pressure(policy: MaintenancePolicy) -> bool {
    policy.max_sqlite_wal_bytes != u64::MAX
}
