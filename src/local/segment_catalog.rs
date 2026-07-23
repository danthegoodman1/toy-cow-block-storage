/// Local segment lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentLifecycleState {
    Reserved,
    Writing,
    DurablePendingMetadata,
    Referenced,
    Released,
    Freed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct CatalogEntry {
    intent: SegmentReservationIntent,
    reservation: SegmentReservation,
    state: SegmentLifecycleState,
    receipt: Option<SegmentWriteReceipt>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct CatalogInner {
    next_segment_id: u128,
    entries: BTreeMap<SegmentId, CatalogEntry>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CatalogLifecycleCounts {
    reserved: usize,
    writing: usize,
    durable_pending: usize,
    referenced: usize,
    released: usize,
    freed: usize,
}

/// Compile-time identity of a catalog-mutex acquirer.
///
/// Every acquisition of the catalog mutex names its call site with one of
/// these tags: within the catalog API the lock is reachable only through
/// `TaggedCatalogMutex::lock`, which requires a tag. The tags are enumerated
/// from the real call sites. Per-acquirer wait/hold accounting keyed by them
/// answers who HOLDS the catalog mutex, which the per-caller
/// `*_lock_wait_nanos` profile columns (who waits) cannot.
///
/// Like `ReadProfile`, this is process-local opt-in diagnostics: not durable
/// state and not part of the public storage contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CatalogAcquirer {
    /// Segment-write staging: duplicate probe (and duplicate receipt check).
    StagingProbe,
    /// Segment-write staging: reservation insert.
    StagingReserve,
    /// Segment-write staging: Reserved -> Writing transition.
    StagingBegin,
    /// Segment-write staging: receipt commit into DurablePendingMetadata.
    StagingCommit,
    /// Batched mark-referenced from publish, replay, and registry callers.
    Mark,
    /// Read-path receipt lookup before payload verification.
    ReadVerify,
    /// Async segment-row publisher's chunked live snapshot.
    RowPublisherSnapshot,
    /// Row publisher's payload refetch for segments that missed prestaging.
    MissingSegmentsSnapshot,
    /// Block metadata-delta prestage payload fetch: delta segments commit
    /// through the local block-delta lane, so the durable layer does not
    /// hold their payload windows and fetches them from the catalogs. The
    /// block-journal lane never acquires here: it carries prestage payloads
    /// from staging.
    PrestageDeltaSnapshot,
    /// Block metadata-delta persist snapshot.
    PersistBlockDelta,
    /// Native-file metadata-delta persist snapshot.
    PersistNativeFileDelta,
    /// Physical (whole-store) persist payload snapshot.
    PersistPhysical,
    /// Full-catalog clone for full persists and durable exports.
    PersistFull,
    /// Append-stream request-batch persist snapshot.
    PersistAppendStreamBatch,
    /// Append-stream publish-delta persist snapshot.
    PersistPublishDelta,
    /// Prepared append-publish-plan persist snapshot.
    PersistPreparedPlans,
    /// Owner-node discovery scans (`owner_node_for_segment`).
    OwnerScan,
    /// Cold point lookups: receipts, states, intents, placements.
    ReceiptLookup,
    /// Referenced -> Released transition from metadata GC evidence.
    Release,
    /// Reopen/recovery scans and adopted-segment snapshots.
    Replay,
    /// Maintenance-tick lifecycle scan and released-segment deletion.
    MaintenanceTick,
    /// Custodian scan and expired-intent reconciliation.
    Custodian,
    /// Unique-catalog-ownership sweep.
    Sweep,
    /// Lifecycle counts for maintenance observation and diagnostics.
    MaintenanceObserve,
}

impl CatalogAcquirer {
    pub(super) const ALL: [CatalogAcquirer; 24] = [
        CatalogAcquirer::StagingProbe,
        CatalogAcquirer::StagingReserve,
        CatalogAcquirer::StagingBegin,
        CatalogAcquirer::StagingCommit,
        CatalogAcquirer::Mark,
        CatalogAcquirer::ReadVerify,
        CatalogAcquirer::RowPublisherSnapshot,
        CatalogAcquirer::MissingSegmentsSnapshot,
        CatalogAcquirer::PrestageDeltaSnapshot,
        CatalogAcquirer::PersistBlockDelta,
        CatalogAcquirer::PersistNativeFileDelta,
        CatalogAcquirer::PersistPhysical,
        CatalogAcquirer::PersistFull,
        CatalogAcquirer::PersistAppendStreamBatch,
        CatalogAcquirer::PersistPublishDelta,
        CatalogAcquirer::PersistPreparedPlans,
        CatalogAcquirer::OwnerScan,
        CatalogAcquirer::ReceiptLookup,
        CatalogAcquirer::Release,
        CatalogAcquirer::Replay,
        CatalogAcquirer::MaintenanceTick,
        CatalogAcquirer::Custodian,
        CatalogAcquirer::Sweep,
        CatalogAcquirer::MaintenanceObserve,
    ];

    /// Stable snake_case name for CSV and report output.
    pub fn name(self) -> &'static str {
        match self {
            CatalogAcquirer::StagingProbe => "staging_probe",
            CatalogAcquirer::StagingReserve => "staging_reserve",
            CatalogAcquirer::StagingBegin => "staging_begin",
            CatalogAcquirer::StagingCommit => "staging_commit",
            CatalogAcquirer::Mark => "mark",
            CatalogAcquirer::ReadVerify => "read_verify",
            CatalogAcquirer::RowPublisherSnapshot => "row_publisher_snapshot",
            CatalogAcquirer::MissingSegmentsSnapshot => "missing_segments_snapshot",
            CatalogAcquirer::PrestageDeltaSnapshot => "prestage_delta_snapshot",
            CatalogAcquirer::PersistBlockDelta => "persist_block_delta",
            CatalogAcquirer::PersistNativeFileDelta => "persist_native_file_delta",
            CatalogAcquirer::PersistPhysical => "persist_physical",
            CatalogAcquirer::PersistFull => "persist_full",
            CatalogAcquirer::PersistAppendStreamBatch => "persist_append_stream_batch",
            CatalogAcquirer::PersistPublishDelta => "persist_publish_delta",
            CatalogAcquirer::PersistPreparedPlans => "persist_prepared_plans",
            CatalogAcquirer::OwnerScan => "owner_scan",
            CatalogAcquirer::ReceiptLookup => "receipt_lookup",
            CatalogAcquirer::Release => "release",
            CatalogAcquirer::Replay => "replay",
            CatalogAcquirer::MaintenanceTick => "maintenance_tick",
            CatalogAcquirer::Custodian => "custodian",
            CatalogAcquirer::Sweep => "sweep",
            CatalogAcquirer::MaintenanceObserve => "maintenance_observe",
        }
    }

    fn slot_index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Default)]
struct CatalogHoldSlot {
    acquisitions: AtomicU64,
    wait_nanos: AtomicU64,
    hold_nanos: AtomicU64,
    max_hold_nanos: AtomicU64,
}

/// Snapshot of one acquirer's catalog-mutex accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct CatalogHoldCounters {
    pub(super) acquisitions: u64,
    pub(super) wait_nanos: u64,
    pub(super) hold_nanos: u64,
    pub(super) max_hold_nanos: u64,
}

/// Per-acquirer catalog-mutex accounting for one storage node.
///
/// Process-local opt-in diagnostics (like `ReadProfile`): not durable state
/// and not part of the public storage contracts. Draining resets the
/// counters, so consecutive drains cover disjoint windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogHoldProfile {
    pub storage_node: StorageNodeId,
    pub acquirer: CatalogAcquirer,
    /// Lock acquisitions recorded for this acquirer.
    pub acquisitions: u64,
    /// Total time spent waiting to acquire, in nanoseconds.
    pub wait_nanos: u64,
    /// Total time the lock was held, in nanoseconds.
    pub hold_nanos: u64,
    /// Longest single hold, in nanoseconds.
    pub max_hold_nanos: u64,
}

/// The catalog mutex plus per-acquirer wait/hold accounting.
///
/// `lock` is the only path to the mutex within the catalog API and it
/// requires a `CatalogAcquirer` tag; the returned guard records real
/// measured wait and hold durations into the tag's slot when it drops. The
/// storage-core files are `include!`d into one module, so the raw `mutex`
/// field is technically reachable throughout `crate::local` — production
/// call sites go through the tagged lock by convention, and nothing outside
/// this file names `inner.mutex` (grep-checkable). The accounting is a
/// clock read plus relaxed atomics per acquisition — a few tens of
/// nanoseconds beyond the lock itself — so it stays on in production paths.
#[derive(Debug)]
struct TaggedCatalogMutex {
    mutex: Mutex<CatalogInner>,
    holds: [CatalogHoldSlot; CatalogAcquirer::ALL.len()],
}

impl TaggedCatalogMutex {
    fn new(inner: CatalogInner) -> Self {
        Self {
            mutex: Mutex::new(inner),
            holds: std::array::from_fn(|_| CatalogHoldSlot::default()),
        }
    }

    fn lock(&self, acquirer: CatalogAcquirer) -> Result<CatalogHoldGuard<'_>> {
        let wait_started = Instant::now();
        let inner = lock(&self.mutex)?;
        let acquired_at = Instant::now();
        Ok(CatalogHoldGuard {
            inner,
            slot: &self.holds[acquirer.slot_index()],
            acquired_at,
            wait_nanos: duration_nanos_u64(acquired_at.duration_since(wait_started)),
        })
    }

    /// Snapshot and reset every acquirer's counters.
    ///
    /// The drain is non-atomic with respect to an in-flight hold: an
    /// acquisition straddling the drain can land its count in the old
    /// window and its nanos in the new one, and a hold still in flight at a
    /// final drain is lost entirely — at most one acquisition per catalog
    /// per drain, noise at the aggregate scale this measures.
    fn drain_hold_counters(&self) -> Vec<(CatalogAcquirer, CatalogHoldCounters)> {
        CatalogAcquirer::ALL
            .iter()
            .map(|acquirer| {
                let slot = &self.holds[acquirer.slot_index()];
                (
                    *acquirer,
                    CatalogHoldCounters {
                        acquisitions: slot.acquisitions.swap(0, Ordering::Relaxed),
                        wait_nanos: slot.wait_nanos.swap(0, Ordering::Relaxed),
                        hold_nanos: slot.hold_nanos.swap(0, Ordering::Relaxed),
                        max_hold_nanos: slot.max_hold_nanos.swap(0, Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }
}

struct CatalogHoldGuard<'a> {
    inner: MutexGuard<'a, CatalogInner>,
    slot: &'a CatalogHoldSlot,
    acquired_at: Instant,
    wait_nanos: u64,
}

impl CatalogHoldGuard<'_> {
    fn lock_wait_nanos(&self) -> u64 {
        self.wait_nanos
    }
}

impl std::ops::Deref for CatalogHoldGuard<'_> {
    type Target = CatalogInner;

    fn deref(&self) -> &CatalogInner {
        &self.inner
    }
}

impl std::ops::DerefMut for CatalogHoldGuard<'_> {
    fn deref_mut(&mut self) -> &mut CatalogInner {
        &mut self.inner
    }
}

impl Drop for CatalogHoldGuard<'_> {
    fn drop(&mut self) {
        // Runs just before the mutex guard releases, so the measured hold
        // covers the whole critical section. Relaxed ordering: the counters
        // are statistics, not synchronization.
        let hold_nanos = duration_nanos_u64(self.acquired_at.elapsed());
        self.slot.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.slot.wait_nanos.fetch_add(self.wait_nanos, Ordering::Relaxed);
        self.slot.hold_nanos.fetch_add(hold_nanos, Ordering::Relaxed);
        self.slot.max_hold_nanos.fetch_max(hold_nanos, Ordering::Relaxed);
    }
}

/// In-memory implementation of `LocalSegmentCatalog`.
#[derive(Debug)]
pub struct InMemoryLocalSegmentCatalog {
    config: LocalStoreConfig,
    inner: TaggedCatalogMutex,
}

impl InMemoryLocalSegmentCatalog {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: TaggedCatalogMutex::new(CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            }),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: CatalogInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: TaggedCatalogMutex::new(inner),
        })
    }

    /// Snapshot and reset this catalog's per-acquirer hold accounting.
    pub(super) fn drain_hold_counters(&self) -> Vec<(CatalogAcquirer, CatalogHoldCounters)> {
        self.inner.drain_hold_counters()
    }

    /// Test-only corruption injection: remove an entry out from under the
    /// catalog, bypassing lifecycle transitions and hold attribution. This
    /// simulates external corruption, not a real acquirer, and keeps the raw
    /// mutex unnamed outside this file.
    #[cfg(test)]
    pub(super) fn corrupt_remove_entry(&self, segment_id: SegmentId) -> Result<()> {
        lock(&self.inner.mutex)?.entries.remove(&segment_id);
        Ok(())
    }

    fn state_inner(&self) -> Result<CatalogInner> {
        Ok(self.inner.lock(CatalogAcquirer::PersistFull)?.clone())
    }

    /// Copy of the catalog restricted to the requested segment ids.
    ///
    /// Hot per-write paths use this instead of `state_inner` so snapshot cost
    /// scales with the request, not with every segment the node has ever
    /// cataloged. Returns `None` when none of the ids live on this node.
    fn selected_state_inner(
        &self,
        acquirer: CatalogAcquirer,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<Option<CatalogInner>> {
        let inner = self.inner.lock(acquirer)?;
        let mut entries = BTreeMap::new();
        for segment_id in segment_ids {
            if let Some(entry) = inner.entries.get(segment_id) {
                entries.insert(*segment_id, entry.clone());
            }
        }
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(CatalogInner {
            next_segment_id: inner.next_segment_id,
            entries,
        }))
    }

    /// `selected_state_inner` with the lock held for at most `chunk_len` ids
    /// per acquisition and a scheduler yield between acquisitions.
    ///
    /// The async segment-row publisher snapshots row batches while foreground
    /// reserve/commit/mark operations contend for the same mutex. Short holds
    /// and yields alone do not guarantee fairness (a tight release-reacquire
    /// loop can win the unfair mutex back before a parked waiter wakes), so
    /// the publisher also bounds each drain cycle and spends the gap between
    /// cycles in lock-free SQLite writes; this method keeps the per-cycle
    /// lock work short. Entries may change between chunks, which the
    /// row-publish contract already tolerates: rows reflect a catalog state
    /// no older than enqueue time, and lifecycle states only advance.
    fn selected_state_inner_chunked(
        &self,
        segment_ids: &[SegmentId],
        chunk_len: usize,
    ) -> Result<Option<CatalogInner>> {
        let mut entries = BTreeMap::new();
        let mut next_segment_id = 0_u128;
        let mut chunks = segment_ids.chunks(chunk_len.max(1)).peekable();
        while let Some(chunk) = chunks.next() {
            {
                let inner = self.inner.lock(CatalogAcquirer::RowPublisherSnapshot)?;
                next_segment_id = inner.next_segment_id;
                for segment_id in chunk {
                    if let Some(entry) = inner.entries.get(segment_id) {
                        entries.insert(*segment_id, entry.clone());
                    }
                }
            }
            if chunks.peek().is_some() {
                std::thread::yield_now();
            }
        }
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(CatalogInner {
            next_segment_id,
            entries,
        }))
    }

    fn reserve_segment_with_id(
        &self,
        segment_id: SegmentId,
        intent: SegmentReservationIntent,
    ) -> Result<SegmentReservation> {
        self.reserve_segment_with_id_profiled(segment_id, intent)
            .map(|(reservation, _)| reservation)
    }

    fn reserve_segment_with_id_profiled(
        &self,
        segment_id: SegmentId,
        intent: SegmentReservationIntent,
    ) -> Result<(SegmentReservation, LocalCatalogOpProfile)> {
        let total_started = Instant::now();
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let mut inner = self.inner.lock(CatalogAcquirer::StagingReserve)?;
        let lock_wait_nanos = inner.lock_wait_nanos();
        if inner.entries.contains_key(&segment_id) {
            return Err(StorageError::conflict("segment ID already exists"));
        }
        if segment_id.raw() >= inner.next_segment_id {
            inner.next_segment_id = segment_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("segment id overflow"))?;
        }
        let reservation = SegmentReservation {
            segment_id,
            bytes: intent.bytes,
        };
        inner.entries.insert(
            segment_id,
            CatalogEntry {
                intent,
                reservation: reservation.clone(),
                state: SegmentLifecycleState::Reserved,
                receipt: None,
            },
        );
        Ok((
            reservation,
            LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            },
        ))
    }

    pub fn contains_segment(
        &self,
        acquirer: CatalogAcquirer,
        segment_id: SegmentId,
    ) -> Result<bool> {
        self.contains_segment_profiled(acquirer, segment_id)
            .map(|(contains, _)| contains)
    }

    fn contains_segment_profiled(
        &self,
        acquirer: CatalogAcquirer,
        segment_id: SegmentId,
    ) -> Result<(bool, LocalCatalogOpProfile)> {
        let total_started = Instant::now();
        let inner = self.inner.lock(acquirer)?;
        let lock_wait_nanos = inner.lock_wait_nanos();
        Ok((
            inner.entries.contains_key(&segment_id),
            LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            },
        ))
    }

    pub fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        let inner = self.inner.lock(CatalogAcquirer::ReceiptLookup)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.state)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        let inner = self.inner.lock(CatalogAcquirer::ReceiptLookup)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .as_ref()
            .map(SegmentWriteReceipt::replica_commit)
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn receipt_for_segment(
        &self,
        acquirer: CatalogAcquirer,
        segment_id: SegmentId,
    ) -> Result<SegmentWriteReceipt> {
        let inner = self.inner.lock(acquirer)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .clone()
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn intent_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReservationIntent> {
        let inner = self.inner.lock(CatalogAcquirer::ReceiptLookup)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.intent.clone())
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn entries(
        &self,
        acquirer: CatalogAcquirer,
    ) -> Result<Vec<(SegmentId, SegmentLifecycleState, WriteIntentId)>> {
        let inner = self.inner.lock(acquirer)?;
        Ok(inner
            .entries
            .iter()
            .map(|(segment_id, entry)| (*segment_id, entry.state, entry.intent.write_intent))
            .collect())
    }

    fn lifecycle_counts(&self) -> Result<CatalogLifecycleCounts> {
        let inner = self.inner.lock(CatalogAcquirer::MaintenanceObserve)?;
        let mut counts = CatalogLifecycleCounts::default();
        for entry in inner.entries.values() {
            match entry.state {
                SegmentLifecycleState::Reserved => counts.reserved += 1,
                SegmentLifecycleState::Writing => counts.writing += 1,
                SegmentLifecycleState::DurablePendingMetadata => counts.durable_pending += 1,
                SegmentLifecycleState::Referenced => counts.referenced += 1,
                SegmentLifecycleState::Released => counts.released += 1,
                SegmentLifecycleState::Freed => counts.freed += 1,
            }
        }
        Ok(counts)
    }

    fn begin_write_profiled(
        &self,
        reservation: &SegmentReservation,
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let mut inner = self.inner.lock(CatalogAcquirer::StagingBegin)?;
        let lock_wait_nanos = inner.lock_wait_nanos();
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != *reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Writing;
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            SegmentLifecycleState::Writing => Ok(LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            }),
            _ => Err(StorageError::conflict(
                "segment write can only begin from Reserved state",
            )),
        }
    }

    fn commit_segment_profiled(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let mut inner = self.inner.lock(CatalogAcquirer::StagingCommit)?;
        let lock_wait_nanos = inner.lock_wait_nanos();
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        if receipt.segment_id != reservation.segment_id
            || receipt.descriptor.segment_id != reservation.segment_id
            || receipt.placement.segment_id != reservation.segment_id
        {
            return Err(StorageError::invalid_argument(
                "segment receipt IDs must match reservation",
            ));
        }
        if receipt.storage_node != self.config.storage_node
            || receipt.placement.storage_node != self.config.storage_node
        {
            return Err(StorageError::invalid_argument(
                "segment receipt storage node does not match local catalog",
            ));
        }
        if receipt.bytes != reservation.bytes
            || receipt.descriptor.bytes != reservation.bytes
            || receipt.placement.bytes != reservation.bytes
        {
            return Err(StorageError::invalid_argument(
                "segment receipt bytes must match reservation",
            ));
        }

        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.receipt = Some(receipt);
                entry.state = SegmentLifecycleState::DurablePendingMetadata;
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            SegmentLifecycleState::DurablePendingMetadata
                if entry.receipt.as_ref() == Some(&receipt) =>
            {
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            _ => Err(StorageError::conflict(
                "segment receipt requires Writing state",
            )),
        }
    }

    /// Mark a batch of segments referenced under one lock acquisition.
    ///
    /// Validates every segment before applying any transition, so a bad id
    /// mid-batch leaves the catalog untouched. Marking is idempotent for
    /// segments already referenced.
    fn mark_segments_referenced_profiled(
        &self,
        segment_ids: &[SegmentId],
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let mut inner = self.inner.lock(CatalogAcquirer::Mark)?;
        let lock_wait_nanos = inner.lock_wait_nanos();
        for segment_id in segment_ids {
            let entry = inner
                .entries
                .get(segment_id)
                .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
            if !matches!(
                entry.state,
                SegmentLifecycleState::DurablePendingMetadata | SegmentLifecycleState::Referenced
            ) {
                return Err(StorageError::conflict(
                    "segment can be referenced only from DurablePendingMetadata state",
                ));
            }
        }
        for segment_id in segment_ids {
            let entry = Self::get_entry_mut(&mut inner, *segment_id)?;
            entry.state = SegmentLifecycleState::Referenced;
        }
        Ok(LocalCatalogOpProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            lock_wait_nanos,
        })
    }

    /// `LocalSegmentCatalog::delete_segment` with an explicit acquirer tag:
    /// the maintenance tick and the custodian both delete released segments,
    /// so the shared transition names its caller here.
    fn delete_segment_as(&self, acquirer: CatalogAcquirer, segment_id: SegmentId) -> Result<()> {
        let mut inner = self.inner.lock(acquirer)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Released => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Released segments are safe to delete",
            )),
        }
    }

    fn get_entry_mut(inner: &mut CatalogInner, segment_id: SegmentId) -> Result<&mut CatalogEntry> {
        inner
            .entries
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }
}

impl LocalSegmentCatalog for InMemoryLocalSegmentCatalog {
    fn reserve_segment(&self, intent: SegmentReservationIntent) -> Result<SegmentReservation> {
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let segment_id = {
            let inner = self.inner.lock(CatalogAcquirer::StagingReserve)?;
            SegmentId::from_raw(inner.next_segment_id)
        };
        self.reserve_segment_with_id(segment_id, intent)
    }

    fn begin_write(&self, reservation: &SegmentReservation) -> Result<()> {
        self.begin_write_profiled(reservation).map(|_| ())
    }

    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
    ) -> Result<()> {
        self.commit_segment_profiled(reservation, receipt)
            .map(|_| ())
    }

    fn mark_segment_referenced(&self, segment_id: SegmentId) -> Result<()> {
        self.mark_segments_referenced_profiled(std::slice::from_ref(&segment_id))
            .map(|_| ())
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = self.inner.lock(CatalogAcquirer::Release)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Referenced => {
                entry.state = SegmentLifecycleState::Released;
                Ok(())
            }
            SegmentLifecycleState::Released => Ok(()),
            _ => Err(StorageError::conflict(
                "segment can be released only from Referenced state",
            )),
        }
    }

    fn expire_reservation(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = self.inner.lock(CatalogAcquirer::Custodian)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Reserved segments can expire as reservations",
            )),
        }
    }

    fn fail_write(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = self.inner.lock(CatalogAcquirer::Custodian)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Writing segments can fail as writes",
            )),
        }
    }

    fn free_orphan_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = self.inner.lock(CatalogAcquirer::Custodian)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only DurablePendingMetadata orphan segments can be freed",
            )),
        }
    }

    fn locate_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaPlacement> {
        let inner = self.inner.lock(CatalogAcquirer::ReceiptLookup)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata
            | SegmentLifecycleState::Referenced
            | SegmentLifecycleState::Released => entry
                .receipt
                .as_ref()
                .map(|receipt| receipt.placement.clone())
                .ok_or_else(|| StorageError::corrupt("committed segment missing placement")),
            SegmentLifecycleState::Freed => {
                Err(StorageError::not_found("segment", segment_id.to_string()))
            }
            SegmentLifecycleState::Reserved | SegmentLifecycleState::Writing => Err(
                StorageError::unavailable("segment placement is not committed yet"),
            ),
        }
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        self.delete_segment_as(CatalogAcquirer::Custodian, segment_id)
    }
}
