#[derive(Debug)]
pub(super) struct ObservabilityInner {
    counters: DiagnosticsCounters,
    events: VecDeque<StorageEvent>,
    next_event_sequence: u64,
    capacity: usize,
}

#[derive(Debug)]
pub(super) struct Observability {
    inner: Mutex<ObservabilityInner>,
}

impl Observability {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "observability_event_capacity must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Mutex::new(ObservabilityInner {
                counters: DiagnosticsCounters::default(),
                events: VecDeque::with_capacity(capacity),
                next_event_sequence: 1,
                capacity,
            }),
        })
    }

    fn increment(&self, update: impl FnOnce(&mut DiagnosticsCounters)) {
        if let Ok(mut inner) = self.inner.lock() {
            update(&mut inner.counters);
        }
    }

    fn record(&self, kind: StorageEventKind) {
        self.record_with(kind, None, None, None, None);
    }

    fn record_with(
        &self,
        kind: StorageEventKind,
        storage_node: Option<StorageNodeId>,
        segment_id: Option<SegmentId>,
        commit_seq: Option<CommitSeq>,
        reason: Option<&'static str>,
    ) {
        self.record_with_update(kind, storage_node, segment_id, commit_seq, reason, |_| {});
    }

    fn record_with_update(
        &self,
        kind: StorageEventKind,
        storage_node: Option<StorageNodeId>,
        segment_id: Option<SegmentId>,
        commit_seq: Option<CommitSeq>,
        reason: Option<&'static str>,
        update: impl FnOnce(&mut DiagnosticsCounters),
    ) {
        if let Ok(mut inner) = self.inner.lock() {
            update(&mut inner.counters);
            let sequence = inner.next_event_sequence;
            inner.next_event_sequence = inner.next_event_sequence.saturating_add(1);
            if inner.events.len() == inner.capacity {
                inner.events.pop_front();
                inner.counters.observability_events_dropped = inner
                    .counters
                    .observability_events_dropped
                    .saturating_add(1);
            }
            inner.counters.observability_events_recorded = inner
                .counters
                .observability_events_recorded
                .saturating_add(1);
            inner.events.push_back(StorageEvent {
                sequence,
                kind,
                storage_node,
                segment_id,
                commit_seq,
                reason,
            });
        }
    }

    fn snapshot_parts(&self) -> Result<(DiagnosticsCounters, Vec<StorageEvent>, u64, u64, u64)> {
        let inner = lock(&self.inner)?;
        let last_sequence = inner.next_event_sequence.saturating_sub(1);
        Ok((
            inner.counters,
            inner.events.iter().cloned().collect(),
            usize_to_u64(inner.events.len()),
            usize_to_u64(inner.capacity),
            last_sequence,
        ))
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        let mut inner = lock(&self.inner)?;
        let count = max.min(inner.events.len());
        Ok(inner.events.drain(..count).collect())
    }
}

pub(super) fn receipt_rejection_reason(error: &StorageError) -> &'static str {
    match error {
        StorageError::Corrupt { .. } => "bad_proof",
        StorageError::Conflict { reason } if reason.contains("proof") => "bad_proof",
        StorageError::Conflict { reason } if reason.contains("stale") => "stale_epoch",
        StorageError::Conflict { reason } if reason.contains("duplicate") => "replay",
        StorageError::Conflict { .. } | StorageError::InvalidArgument { .. } => "scope",
        StorageError::NotFound { .. } | StorageError::Unavailable { .. } => "scope",
        StorageError::Unsupported { .. } => "unsupported",
    }
}

pub(super) fn count_receipt_rejection(counters: &mut DiagnosticsCounters, reason: &'static str) {
    counters.receipt_rejections = counters.receipt_rejections.saturating_add(1);
    match reason {
        "bad_proof" => {
            counters.receipt_rejected_bad_proof =
                counters.receipt_rejected_bad_proof.saturating_add(1);
        }
        "stale_epoch" => {
            counters.receipt_rejected_epoch = counters.receipt_rejected_epoch.saturating_add(1);
        }
        "replay" => {
            counters.receipt_rejected_replay = counters.receipt_rejected_replay.saturating_add(1);
        }
        _ => {
            counters.receipt_rejected_scope = counters.receipt_rejected_scope.saturating_add(1);
        }
    }
}
