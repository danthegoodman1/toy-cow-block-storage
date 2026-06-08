#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableDataLogPolicy {
    pub target_data_log_bytes: u64,
    pub file_sync_fanout: usize,
    pub min_reclaimable_ratio_ppm: u32,
    pub min_reclaimable_bytes: u64,
    pub max_compaction_copy_bytes: u64,
}

impl Default for DurableDataLogPolicy {
    fn default() -> Self {
        Self {
            target_data_log_bytes: 64 * 1024 * 1024,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 500_000,
            min_reclaimable_bytes: 4 * 1024 * 1024,
            max_compaction_copy_bytes: 64 * 1024 * 1024,
        }
    }
}

impl DurableDataLogPolicy {
    fn validate(self) -> Result<()> {
        if self.target_data_log_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "target_data_log_bytes must be greater than zero",
            ));
        }
        if self.file_sync_fanout == 0 {
            return Err(StorageError::invalid_argument(
                "file_sync_fanout must be greater than zero",
            ));
        }
        if self.min_reclaimable_ratio_ppm > 1_000_000 {
            return Err(StorageError::invalid_argument(
                "min_reclaimable_ratio_ppm must be <= 1_000_000",
            ));
        }
        if self.max_compaction_copy_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_compaction_copy_bytes must be greater than zero",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn compact_everything_for_test() -> Self {
        Self {
            target_data_log_bytes: 8 * 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        }
    }
}

/// Provider-private batching policy for durable append-visible publishes.
///
/// This is a latency/throughput scheduling knob below the public native file
/// API. It does not change what a successful publish makes durable or visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendPublishBatchPolicy {
    /// Pending tickets needed to start a batch without an extra coalesce wait.
    pub target_tickets: usize,
    /// Short wait used to let nearby publish waiters join a batch.
    pub idle_coalesce_delay: Duration,
    /// Maximum time one batch owner may spend collecting peer tickets.
    pub max_coalesce_delay: Duration,
}

impl Default for AppendPublishBatchPolicy {
    fn default() -> Self {
        Self {
            target_tickets: 4,
            idle_coalesce_delay: Duration::from_micros(250),
            max_coalesce_delay: Duration::from_millis(5),
        }
    }
}

impl AppendPublishBatchPolicy {
    /// Validate that append publish batching can make deterministic progress.
    pub fn validate(self) -> Result<()> {
        if self.target_tickets == 0 {
            return Err(StorageError::invalid_argument(
                "append publish batch target_tickets must be greater than zero",
            ));
        }
        if self.idle_coalesce_delay > self.max_coalesce_delay {
            return Err(StorageError::invalid_argument(
                "append publish idle_coalesce_delay must be <= max_coalesce_delay",
            ));
        }
        Ok(())
    }
}

/// Durable data-log identity within a storage node.
///
/// The pair is provider-owned diagnostic state. Public block and native callers
/// can observe it in maintenance reports, but they must not choose log IDs or
/// infer physical offsets from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurableDataLogRef {
    /// Storage node that owns the log.
    pub storage_node: StorageNodeId,
    /// Node-local monotonically increasing log identifier.
    pub log_id: u64,
}

/// Summary of data-log compaction work completed by a maintenance tick.
///
/// A successful report means all listed relocations and deletions were
/// published durably in SQLite before any old log file was removed. Failure may
/// leave already-completed maintenance work in place, but must not make
/// acknowledged segment data unreadable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCompactionReport {
    /// Sealed logs that contained no live placements and were removed.
    pub deleted_logs: Vec<DurableDataLogRef>,
    /// Sealed logs whose live placements were copied elsewhere before removal.
    pub relocated_logs: Vec<DurableDataLogRef>,
    /// Segment IDs whose current placement moved during compaction.
    pub relocated_segments: Vec<SegmentId>,
    /// Live payload bytes copied into replacement logs.
    pub bytes_copied: u64,
    /// Total old log bytes removed from disk.
    pub bytes_deleted: u64,
}

/// Runtime mode for durable maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaintenanceMode {
    /// No background work. Callers explicitly observe, plan, and run ticks.
    #[default]
    Manual,
    /// A write may run one bounded maintenance tick before it is admitted.
    Opportunistic,
    /// A local worker runs bounded ticks after writes or custodian work notify it.
    AlwaysOn,
}

/// Policy knobs for deterministic durable maintenance and write admission.
///
/// The policy lives below the public block/native APIs. It may throttle or
/// reject writes with `StorageError::Unavailable`, but it must not change read,
/// fork, snapshot, restore, or flush semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenancePolicy {
    /// How maintenance is driven at runtime.
    pub mode: MaintenanceMode,
    /// Data-log rolling and compaction thresholds.
    pub data_log_policy: DurableDataLogPolicy,
    /// Whether writes consult admission thresholds before they run.
    pub write_backpressure_enabled: bool,
    /// Dirty bytes at or above this value schedule maintenance.
    pub dirty_low_watermark_bytes: u64,
    /// Dirty bytes at or above this value throttle admitted writes.
    pub dirty_high_watermark_bytes: u64,
    /// Sealed-log count above this value throttles admitted writes.
    pub max_sealed_logs: usize,
    /// Reclaimable debt above this value rejects admitted writes.
    pub max_reclaimable_debt_bytes: u64,
    /// Maximum live bytes a maintenance tick may copy.
    pub compaction_copy_budget_per_tick: u64,
    /// SQLite WAL size above this value throttles admitted writes.
    pub max_sqlite_wal_bytes: u64,
    /// Maximum logs considered by one scheduler tick.
    pub max_logs_scanned_per_tick: usize,
    /// Local v1 supports exactly one executor.
    pub max_concurrent_compaction_jobs: usize,
}

impl Default for MaintenancePolicy {
    fn default() -> Self {
        Self {
            mode: MaintenanceMode::Manual,
            data_log_policy: DurableDataLogPolicy::default(),
            write_backpressure_enabled: false,
            dirty_low_watermark_bytes: 16 * 1024 * 1024,
            dirty_high_watermark_bytes: 128 * 1024 * 1024,
            max_sealed_logs: 128,
            max_reclaimable_debt_bytes: 512 * 1024 * 1024,
            compaction_copy_budget_per_tick: 32 * 1024 * 1024,
            max_sqlite_wal_bytes: 128 * 1024 * 1024,
            max_logs_scanned_per_tick: 16,
            max_concurrent_compaction_jobs: 1,
        }
    }
}

impl MaintenancePolicy {
    /// Build the default manual policy with a specific data-log policy.
    pub fn manual(data_log_policy: DurableDataLogPolicy) -> Self {
        Self {
            data_log_policy,
            ..Self::default()
        }
    }

    /// Validate that the policy is supported by the local durable provider.
    ///
    /// Success means the scheduler can evaluate this policy deterministically.
    /// It does not reserve disk space or start any background worker.
    pub fn validate(self) -> Result<()> {
        self.data_log_policy.validate()?;
        if self.dirty_low_watermark_bytes > self.dirty_high_watermark_bytes {
            return Err(StorageError::invalid_argument(
                "dirty_low_watermark_bytes must be <= dirty_high_watermark_bytes",
            ));
        }
        if self.max_sealed_logs == 0 {
            return Err(StorageError::invalid_argument(
                "max_sealed_logs must be greater than zero",
            ));
        }
        if self.max_reclaimable_debt_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_reclaimable_debt_bytes must be greater than zero",
            ));
        }
        if self.compaction_copy_budget_per_tick == 0 {
            return Err(StorageError::invalid_argument(
                "compaction_copy_budget_per_tick must be greater than zero",
            ));
        }
        if self.max_sqlite_wal_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_sqlite_wal_bytes must be greater than zero",
            ));
        }
        if self.max_logs_scanned_per_tick == 0 {
            return Err(StorageError::invalid_argument(
                "max_logs_scanned_per_tick must be greater than zero",
            ));
        }
        if self.max_concurrent_compaction_jobs != 1 {
            return Err(StorageError::unsupported(
                "local maintenance supports exactly one compaction executor",
            ));
        }
        Ok(())
    }
}

/// Deterministic admission decision for a write.
///
/// `Throttle` and `Reject` are both surfaced as `StorageError::Unavailable` by
/// the local durable provider. Adapters above this layer may retry, sleep, or
/// fail their own request, but the core never hides a wait in this decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAdmission {
    /// Run the write without maintenance pressure.
    Accept,
    /// Run the write and schedule or perform bounded maintenance according to mode.
    AcceptAndSchedule,
    /// Temporarily refuse the write with a stable reason.
    Throttle { reason: &'static str },
    /// Refuse the write until maintenance or capacity state changes.
    Reject { reason: &'static str },
}

impl WriteAdmission {
    fn unavailable_reason(self) -> Option<&'static str> {
        match self {
            Self::Throttle { reason } | Self::Reject { reason } => Some(reason),
            Self::Accept | Self::AcceptAndSchedule => None,
        }
    }
}

/// Point-in-time scheduler input derived from durable provider state.
///
/// Observations are snapshots for planning only. They do not lock data logs or
/// reserve future writes; stale observations must lead to idempotent maintenance
/// commands or skipped work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceObservation {
    /// Per-storage-node log pressure.
    pub nodes: Vec<MaintenanceNodeObservation>,
    /// Current SQLite WAL bytes, or zero when unavailable.
    pub sqlite_wal_bytes: u64,
    /// Count of queued release records not yet reflected in log debt.
    pub pending_custodian_releases: usize,
    /// Oldest commit still protected by PITR retention, if known.
    pub pitr_retention_floor: Option<CommitSeq>,
    /// Bytes in the write being admitted, if any.
    pub recent_write_bytes: u64,
    /// Flushed bytes in the write being admitted, if any.
    pub recent_flushed_write_bytes: u64,
    /// Last persisted fairness cursor for log selection.
    pub compaction_cursor: Option<DurableDataLogRef>,
}

/// Maintenance pressure for one storage node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceNodeObservation {
    /// Node this observation describes.
    pub storage_node: StorageNodeId,
    /// Bytes in active logs that are not compaction candidates.
    pub active_log_bytes: u64,
    /// Count of sealed logs on this node.
    pub sealed_log_count: usize,
    /// Bytes that make sealed logs dirty.
    pub dirty_bytes: u64,
    /// Bytes currently eligible for reclamation.
    pub reclaimable_bytes: u64,
    /// Sealed log details available for bounded scheduling.
    pub logs: Vec<MaintenanceDataLogObservation>,
}

/// Scheduler-visible state for one sealed data log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceDataLogObservation {
    /// Node-local log identity.
    pub log_ref: DurableDataLogRef,
    /// Total durable bytes in the log.
    pub total_bytes: u64,
    /// Bytes that must be copied before the log can be deleted.
    pub live_bytes: u64,
    /// Bytes no longer referenced by published metadata.
    pub dead_bytes: u64,
    /// Dead bytes past retention and eligible for reclamation.
    pub reclaimable_bytes: u64,
}

/// Bounded maintenance command emitted by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceCommand {
    /// Compact these logs if they are still sealed and eligible.
    CompactDataLogs { logs: Vec<DurableDataLogRef> },
}

/// Deterministic output of one scheduler step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickPlan {
    /// Write admission decision for the associated observation.
    pub admission: WriteAdmission,
    /// Bounded commands to run, if any.
    pub commands: Vec<MaintenanceCommand>,
    /// Human-readable counters and skip reasons for tests and operators.
    pub diagnostics: MaintenanceDiagnostics,
    /// Cursor to persist after the tick finishes or is skipped.
    pub next_cursor: Option<DurableDataLogRef>,
}

/// Scheduler counters and explanations.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceDiagnostics {
    /// Total dirty sealed-log bytes observed.
    pub dirty_bytes: u64,
    /// Total reclaimable sealed-log bytes observed.
    pub reclaimable_bytes: u64,
    /// Total sealed logs observed.
    pub sealed_log_count: usize,
    /// SQLite WAL bytes observed.
    pub sqlite_wal_bytes: u64,
    /// Logs selected for compaction.
    pub selected_logs: Vec<DurableDataLogRef>,
    /// Logs considered but not selected.
    pub skipped_logs: Vec<MaintenanceSkippedLog>,
    /// Stable throttle/reject reason, when admission refused the write.
    pub throttle_reason: Option<&'static str>,
}

/// Explanation for a log skipped by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceSkippedLog {
    /// Skipped log.
    pub log_ref: DurableDataLogRef,
    /// Stable diagnostic reason.
    pub reason: &'static str,
}

/// Result of running one bounded maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickReport {
    /// The plan that was executed.
    pub plan: MaintenanceTickPlan,
    /// Durable compaction work completed by this tick.
    pub compaction: DurableCompactionReport,
}

/// Pure deterministic scheduler for durable maintenance.
///
/// The scheduler performs no I/O and owns no background state. Identical policy
/// plus identical observation must produce identical plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceScheduler {
    policy: MaintenancePolicy,
}

impl MaintenanceScheduler {
    /// Create a scheduler after validating the policy.
    pub fn new(policy: MaintenancePolicy) -> Result<Self> {
        policy.validate()?;
        Ok(Self { policy })
    }

    /// Return the validated policy used by this scheduler.
    pub fn policy(&self) -> MaintenancePolicy {
        self.policy
    }

    /// Plan one deterministic maintenance/admission step.
    ///
    /// Success never mutates provider state. The returned commands are
    /// provider-private and must be revalidated by the executor because the
    /// observation may already be stale.
    pub fn step(&self, observation: &MaintenanceObservation) -> MaintenanceTickPlan {
        let mut diagnostics = MaintenanceDiagnostics {
            sqlite_wal_bytes: observation.sqlite_wal_bytes,
            ..MaintenanceDiagnostics::default()
        };
        let mut candidate_logs = Vec::new();
        for node in &observation.nodes {
            diagnostics.dirty_bytes = diagnostics.dirty_bytes.saturating_add(node.dirty_bytes);
            diagnostics.reclaimable_bytes = diagnostics
                .reclaimable_bytes
                .saturating_add(node.reclaimable_bytes);
            diagnostics.sealed_log_count = diagnostics
                .sealed_log_count
                .saturating_add(node.sealed_log_count);
            for log in &node.logs {
                if !self.log_is_compaction_candidate(log) {
                    diagnostics.skipped_logs.push(MaintenanceSkippedLog {
                        log_ref: log.log_ref,
                        reason: "below_reclaim_threshold",
                    });
                    continue;
                }
                candidate_logs.push(*log);
            }
        }
        candidate_logs.sort_by_key(|log| log.log_ref);
        if let Some(cursor) = observation.compaction_cursor
            && let Some(index) = candidate_logs.iter().position(|log| log.log_ref > cursor)
        {
            candidate_logs.rotate_left(index);
        }

        let admission = self.admission(&diagnostics);
        diagnostics.throttle_reason = admission.unavailable_reason();

        let mut copy_budget = self.policy.compaction_copy_budget_per_tick;
        let mut selected = Vec::new();
        for log in candidate_logs
            .into_iter()
            .take(self.policy.max_logs_scanned_per_tick)
        {
            if log.live_bytes > copy_budget {
                diagnostics.skipped_logs.push(MaintenanceSkippedLog {
                    log_ref: log.log_ref,
                    reason: "copy_budget_exhausted",
                });
                continue;
            }
            selected.push(log.log_ref);
            copy_budget = copy_budget.saturating_sub(log.live_bytes);
        }
        diagnostics.selected_logs = selected.clone();
        let next_cursor = selected.last().copied().or(observation.compaction_cursor);
        let commands = if selected.is_empty() {
            Vec::new()
        } else {
            vec![MaintenanceCommand::CompactDataLogs { logs: selected }]
        };

        MaintenanceTickPlan {
            admission: if matches!(admission, WriteAdmission::Accept) && !commands.is_empty() {
                WriteAdmission::AcceptAndSchedule
            } else {
                admission
            },
            commands,
            diagnostics,
            next_cursor,
        }
    }

    fn admission(&self, diagnostics: &MaintenanceDiagnostics) -> WriteAdmission {
        if diagnostics.reclaimable_bytes > self.policy.max_reclaimable_debt_bytes {
            return WriteAdmission::Reject {
                reason: "maintenance reclaimable debt exceeds hard limit",
            };
        }
        if diagnostics.dirty_bytes >= self.policy.dirty_high_watermark_bytes {
            return WriteAdmission::Throttle {
                reason: "maintenance dirty bytes above high watermark",
            };
        }
        if diagnostics.sealed_log_count > self.policy.max_sealed_logs {
            return WriteAdmission::Throttle {
                reason: "maintenance sealed log count above limit",
            };
        }
        if diagnostics.sqlite_wal_bytes > self.policy.max_sqlite_wal_bytes {
            return WriteAdmission::Throttle {
                reason: "maintenance SQLite WAL above limit",
            };
        }
        if diagnostics.dirty_bytes >= self.policy.dirty_low_watermark_bytes {
            return WriteAdmission::AcceptAndSchedule;
        }
        WriteAdmission::Accept
    }

    fn log_is_compaction_candidate(&self, log: &MaintenanceDataLogObservation) -> bool {
        if log.total_bytes == 0 {
            return false;
        }
        if log.live_bytes == 0 && log.dead_bytes != 0 {
            return true;
        }
        let reclaimable_ratio = log
            .dead_bytes
            .saturating_mul(1_000_000)
            .checked_div(log.total_bytes)
            .unwrap_or(0);
        log.dead_bytes >= self.policy.data_log_policy.min_reclaimable_bytes
            && reclaimable_ratio >= u64::from(self.policy.data_log_policy.min_reclaimable_ratio_ppm)
    }
}
