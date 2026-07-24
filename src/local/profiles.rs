#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalCatalogOpProfile {
    total_nanos: u64,
    lock_wait_nanos: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalSegmentStoreWriteProfile {
    total_nanos: u64,
    lock_wait_nanos: u64,
    checksum_integrity_nanos: u64,
    insert_nanos: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalSegmentStoreSyncProfile {
    total_nanos: u64,
    lock_wait_nanos: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalSegmentStoreReadProfile {
    total_nanos: u64,
    lock_wait_nanos: u64,
    copy_nanos: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalSegmentStoreVerifyProfile {
    total_nanos: u64,
    lock_wait_nanos: u64,
    checksum_nanos: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReadSourceProfile {
    total_nanos: u64,
    storage_node_catalog_lookup_nanos: u64,
    storage_node_payload_read_nanos: u64,
    storage_node_lock_wait_nanos: u64,
    verification_nanos: u64,
    copy_nanos: u64,
}

/// Timing for the metadata-resolve half of one read.
///
/// `metadata_tree_walk_nanos` is the wall time of the head fetch plus tree
/// traversal minus the placement-lookup section, so tree walk and placement
/// lookup partition the walk. `metadata_lock_wait_nanos` sums the CONTENDED
/// wait for the metadata-plane mutex across every acquisition the resolve
/// makes (device info, head, and each tree node); uncontended acquisitions
/// report zero (see `lock_timed`), unlike the pair-per-acquisition
/// "acquisition elapsed" metric the pre-existing lock-wait columns and the
/// M5 catalog-hold CSV keep. It is a carve-out that overlaps the other
/// buckets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReadResolveProfile {
    pub metadata_lock_wait_nanos: u64,
    pub metadata_tree_walk_nanos: u64,
    pub metadata_placement_lookup_nanos: u64,
}

/// Process-local timing for one block or native read.
///
/// Profiles are opt-in diagnostics for integration benchmarks. They are not
/// durable state and are not part of the public block/native contracts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadProfile {
    pub sequence: u64,
    pub total_nanos: u64,
    pub metadata_resolve_nanos: u64,
    pub metadata_lock_wait_nanos: u64,
    pub metadata_tree_walk_nanos: u64,
    pub metadata_placement_lookup_nanos: u64,
    pub assemble_nanos: u64,
    pub zero_fill_nanos: u64,
    pub storage_node_read_nanos: u64,
    pub storage_node_catalog_lookup_nanos: u64,
    pub storage_node_payload_read_nanos: u64,
    pub storage_node_lock_wait_nanos: u64,
    pub verification_nanos: u64,
    pub copy_nanos: u64,
    pub block_journal_overlay_read_nanos: u64,
    /// Contended wait for the block-journal overlay mutex during the
    /// overlay read; uncontended acquisitions report zero (see
    /// `lock_timed`). A carve-out subset of
    /// `block_journal_overlay_read_nanos`.
    pub block_journal_overlay_lock_wait_nanos: u64,
    pub logical_bytes: u64,
    pub extent_count: u64,
    pub zero_extent_count: u64,
    pub segment_extent_count: u64,
    pub append_run_extent_count: u64,
    pub storage_node_count: u64,
}

impl ReadProfile {
    fn absorb_source(&mut self, source: ReadSourceProfile) {
        self.storage_node_read_nanos = self
            .storage_node_read_nanos
            .saturating_add(source.total_nanos);
        self.storage_node_catalog_lookup_nanos = self
            .storage_node_catalog_lookup_nanos
            .saturating_add(source.storage_node_catalog_lookup_nanos);
        self.storage_node_payload_read_nanos = self
            .storage_node_payload_read_nanos
            .saturating_add(source.storage_node_payload_read_nanos);
        self.storage_node_lock_wait_nanos = self
            .storage_node_lock_wait_nanos
            .saturating_add(source.storage_node_lock_wait_nanos);
        self.verification_nanos = self
            .verification_nanos
            .saturating_add(source.verification_nanos);
        self.copy_nanos = self.copy_nanos.saturating_add(source.copy_nanos);
    }
}

/// Number of shards in every profile sink.
///
/// Sized so concurrent bench workers (typically up to 32) rarely share a
/// shard mutex; two threads per shard at c32 keeps each shard lock
/// effectively uncontended.
const PROFILE_SINK_SHARD_COUNT: usize = 16;

/// Stable per-thread shard slot for profile sinks.
///
/// Threads take round-robin slots at first use, so bench worker threads
/// spread evenly across shards regardless of thread-id values. The slot is
/// shared by every sink in the process, which is fine: it only picks which
/// shard mutex a thread uses.
fn profile_sink_thread_shard() -> usize {
    static NEXT_PROFILE_SINK_THREAD_SLOT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static PROFILE_SINK_THREAD_SLOT: usize = NEXT_PROFILE_SINK_THREAD_SLOT
            .fetch_add(1, Ordering::Relaxed)
            % PROFILE_SINK_SHARD_COUNT;
    }
    PROFILE_SINK_THREAD_SLOT.with(|slot| *slot)
}

/// Profile records a `ShardedProfileSink` can store.
///
/// The sink assigns a process-wide drain-order sequence at record time;
/// record types that expose a `sequence` field store it, others ignore it.
pub(super) trait ShardedProfileRecord {
    fn set_profile_sequence(&mut self, _sequence: u64) {}
}

impl ShardedProfileRecord for ReadProfile {
    fn set_profile_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

impl ShardedProfileRecord for NativeFileBatchCommitProfile {
    fn set_profile_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

impl ShardedProfileRecord for DurablePersistProfile {
    fn set_profile_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

impl ShardedProfileRecord for AppendPublishWaitProfile {
    fn set_profile_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

impl ShardedProfileRecord for AppendIngestProfile {
    fn set_profile_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

impl ShardedProfileRecord for MetadataPublishProfile {}

/// One sink shard, aligned to two cache lines so neighboring shard mutexes
/// never share a line.
///
/// `len` mirrors `queue.len()` (updated under the queue lock, read without
/// it) so drains skip empty shards without touching their mutexes.
#[repr(align(128))]
#[derive(Debug)]
struct ProfileSinkShard<T> {
    len: AtomicUsize,
    queue: Mutex<VecDeque<(u64, T)>>,
}

impl<T> ProfileSinkShard<T> {
    fn new() -> Self {
        Self {
            len: AtomicUsize::new(0),
            queue: Mutex::new(VecDeque::new()),
        }
    }
}

/// Low-contention, opt-in profile sink.
///
/// Replaces the former single-mutex profilers: recording a sample locks only
/// the recording thread's shard (round-robin thread-to-shard slots), so
/// concurrent workers do not serialize on one global sink mutex, and a
/// disabled sink costs one relaxed atomic load instead of a lock. Retention
/// approximates the old ring buffer: a process-wide length counter bounds
/// the sink at roughly `capacity` samples, evicting the recording shard's
/// oldest sample once the bound is reached (single-threaded recording
/// matches the old exact ring). `drain` merges shards by the record-time
/// sequence, so drained order equals record order; draining removes the
/// returned samples and later drains cover later windows. `enable` clears
/// the sink and restarts sequences at 1.
#[derive(Debug)]
pub(super) struct ShardedProfileSink<T> {
    enabled: AtomicBool,
    capacity: AtomicUsize,
    len: AtomicUsize,
    next_sequence: AtomicU64,
    shards: Vec<ProfileSinkShard<T>>,
}

impl<T: ShardedProfileRecord> ShardedProfileSink<T> {
    pub(super) fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            capacity: AtomicUsize::new(0),
            len: AtomicUsize::new(0),
            next_sequence: AtomicU64::new(1),
            shards: (0..PROFILE_SINK_SHARD_COUNT)
                .map(|_| ProfileSinkShard::new())
                .collect(),
        }
    }

    pub(super) fn enable(&self, capacity: usize) -> Result<()> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "profile capacity must be greater than zero",
            ));
        }
        // Hold every shard lock so the reset is atomic against concurrent
        // records.
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            guards.push(lock(&shard.queue)?);
        }
        for (shard, guard) in self.shards.iter().zip(guards.iter_mut()) {
            guard.clear();
            shard.len.store(0, Ordering::Relaxed);
        }
        self.capacity.store(capacity, Ordering::Relaxed);
        self.len.store(0, Ordering::Relaxed);
        self.next_sequence.store(1, Ordering::Relaxed);
        self.enabled.store(true, Ordering::Release);
        Ok(())
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(super) fn record(&self, mut profile: T) -> Result<()> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let shard = &self.shards[profile_sink_thread_shard()];
        let mut queue = lock(&shard.queue)?;
        // Sequences are assigned under the shard lock so each shard queue
        // stays sequence-sorted, which drain's merge relies on.
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        profile.set_profile_sequence(sequence);
        if self.len.load(Ordering::Relaxed) >= self.capacity.load(Ordering::Relaxed)
            && queue.pop_front().is_some()
        {
            // Evicted this shard's oldest sample; the process-wide length is
            // unchanged. The length counter is approximate under races and
            // may overshoot by at most the shard count.
        } else {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        queue.push_back((sequence, profile));
        shard.len.store(queue.len(), Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn drain(&self, max: usize) -> Result<Vec<T>> {
        if !self.enabled.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        // Fast path for the per-op drains (`attach_metadata_publish_profile`
        // runs on every persist): an empty sink returns without locking or
        // allocating, and non-empty drains lock only shards that hold
        // samples. A record racing a drain lands in the next drain window,
        // which the diagnostics contract tolerates.
        if self.len.load(Ordering::Relaxed) == 0 || max == 0 {
            return Ok(Vec::new());
        }
        let mut guards = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            if shard.len.load(Ordering::Relaxed) == 0 {
                continue;
            }
            guards.push((&shard.len, lock(&shard.queue)?));
        }
        let mut out = Vec::new();
        while out.len() < max {
            let mut best: Option<usize> = None;
            let mut best_sequence = u64::MAX;
            for (index, (_, guard)) in guards.iter().enumerate() {
                if let Some((sequence, _)) = guard.front()
                    && *sequence < best_sequence
                {
                    best_sequence = *sequence;
                    best = Some(index);
                }
            }
            let Some(index) = best else {
                break;
            };
            let Some((_, profile)) = guards[index].1.pop_front() else {
                break;
            };
            out.push(profile);
        }
        for (len, guard) in &guards {
            len.store(guard.len(), Ordering::Relaxed);
        }
        if !out.is_empty() {
            // Still under the drained shard locks, so no record on those
            // shards races the length update.
            let _ = self
                .len
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |len| {
                    Some(len.saturating_sub(out.len()))
                });
        }
        Ok(out)
    }
}

/// Process-local timing for one native file batch commit.
///
/// Profiles are opt-in diagnostics for integration benchmarks. They are not
/// durable state and are not part of the public native-file contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeFileBatchCommitProfile {
    pub sequence: u64,
    pub total_nanos: u64,
    pub metadata_head_nanos: u64,
    pub collapse_nanos: u64,
    pub root_load_nanos: u64,
    pub segment_group_nanos: u64,
    pub preservation_check_nanos: u64,
    pub preservation_read_nanos: u64,
    pub overlay_nanos: u64,
    pub segment_write_nanos: u64,
    pub storage_node_ids_nanos: u64,
    pub placement_select_nanos: u64,
    pub segment_id_alloc_nanos: u64,
    pub grant_issue_nanos: u64,
    pub storage_node_transport_dispatch_nanos: u64,
    pub grant_verify_nanos: u64,
    pub catalog_duplicate_probe_nanos: u64,
    pub catalog_duplicate_probe_lock_wait_nanos: u64,
    pub catalog_reserve_nanos: u64,
    pub catalog_reserve_lock_wait_nanos: u64,
    pub catalog_begin_nanos: u64,
    pub catalog_begin_lock_wait_nanos: u64,
    pub segment_store_write_nanos: u64,
    pub segment_store_lock_wait_nanos: u64,
    pub checksum_integrity_nanos: u64,
    pub segment_store_insert_nanos: u64,
    pub segment_sync_nanos: u64,
    pub segment_sync_lock_wait_nanos: u64,
    pub receipt_create_nanos: u64,
    pub receipt_verify_nanos: u64,
    pub catalog_commit_nanos: u64,
    pub catalog_commit_lock_wait_nanos: u64,
    pub tree_path_copy_nanos: u64,
    pub metadata_publish_nanos: u64,
    pub mark_referenced_nanos: u64,
    /// Mark dispatch outside the catalog transition: carried-node routing,
    /// the node call residual, and node-side event recording.
    pub mark_reference_dispatch_nanos: u64,
    pub mark_reference_catalog_nanos: u64,
    pub mark_reference_catalog_lock_wait_nanos: u64,
    pub append_stream_invalidate_nanos: u64,
    pub write_count: u64,
    pub collapsed_range_count: u64,
    pub segment_group_count: u64,
    pub segment_count: u64,
    pub requested_bytes: u64,
    pub committed_bytes: u64,
    pub committed_range_bytes: u64,
    pub preserved_read_bytes: u64,
}

impl NativeFileBatchCommitProfile {
    fn absorb_segment_write(&mut self, profile: LocalSegmentWriteProfile) {
        self.storage_node_ids_nanos = self
            .storage_node_ids_nanos
            .saturating_add(profile.storage_node_ids_nanos);
        self.placement_select_nanos = self
            .placement_select_nanos
            .saturating_add(profile.placement_select_nanos);
        self.segment_id_alloc_nanos = self
            .segment_id_alloc_nanos
            .saturating_add(profile.segment_id_alloc_nanos);
        self.grant_issue_nanos = self.grant_issue_nanos.saturating_add(profile.grant_issue_nanos);
        self.storage_node_transport_dispatch_nanos = self
            .storage_node_transport_dispatch_nanos
            .saturating_add(profile.storage_node_transport_dispatch_nanos);
        self.grant_verify_nanos = self
            .grant_verify_nanos
            .saturating_add(profile.grant_verify_nanos);
        self.catalog_duplicate_probe_nanos = self
            .catalog_duplicate_probe_nanos
            .saturating_add(profile.catalog_duplicate_probe_nanos);
        self.catalog_duplicate_probe_lock_wait_nanos = self
            .catalog_duplicate_probe_lock_wait_nanos
            .saturating_add(profile.catalog_duplicate_probe_lock_wait_nanos);
        self.catalog_reserve_nanos = self
            .catalog_reserve_nanos
            .saturating_add(profile.catalog_reserve_nanos);
        self.catalog_reserve_lock_wait_nanos = self
            .catalog_reserve_lock_wait_nanos
            .saturating_add(profile.catalog_reserve_lock_wait_nanos);
        self.catalog_begin_nanos = self
            .catalog_begin_nanos
            .saturating_add(profile.catalog_begin_nanos);
        self.catalog_begin_lock_wait_nanos = self
            .catalog_begin_lock_wait_nanos
            .saturating_add(profile.catalog_begin_lock_wait_nanos);
        self.segment_store_write_nanos = self
            .segment_store_write_nanos
            .saturating_add(profile.segment_store_write_nanos);
        self.segment_store_lock_wait_nanos = self
            .segment_store_lock_wait_nanos
            .saturating_add(profile.segment_store_lock_wait_nanos);
        self.checksum_integrity_nanos = self
            .checksum_integrity_nanos
            .saturating_add(profile.checksum_integrity_nanos);
        self.segment_store_insert_nanos = self
            .segment_store_insert_nanos
            .saturating_add(profile.segment_store_insert_nanos);
        self.segment_sync_nanos = self
            .segment_sync_nanos
            .saturating_add(profile.segment_sync_nanos);
        self.segment_sync_lock_wait_nanos = self
            .segment_sync_lock_wait_nanos
            .saturating_add(profile.segment_sync_lock_wait_nanos);
        self.receipt_create_nanos = self
            .receipt_create_nanos
            .saturating_add(profile.receipt_create_nanos);
        self.receipt_verify_nanos = self
            .receipt_verify_nanos
            .saturating_add(profile.receipt_verify_nanos);
        self.catalog_commit_nanos = self
            .catalog_commit_nanos
            .saturating_add(profile.catalog_commit_nanos);
        self.catalog_commit_lock_wait_nanos = self
            .catalog_commit_lock_wait_nanos
            .saturating_add(profile.catalog_commit_lock_wait_nanos);
    }

    fn absorb_mark_referenced(&mut self, profile: LocalMarkReferencedProfile) {
        // This profile keeps one dispatch bucket, so carried-node routing,
        // the call overhead, and node-side event recording all land in it;
        // the block journal lane profile reports them as separate columns
        // instead.
        self.mark_reference_dispatch_nanos = self
            .mark_reference_dispatch_nanos
            .saturating_add(profile.routing_nanos)
            .saturating_add(profile.mark_call_residual_nanos)
            .saturating_add(profile.observability_record_nanos);
        self.mark_reference_catalog_nanos = self
            .mark_reference_catalog_nanos
            .saturating_add(profile.catalog_mark_nanos);
        self.mark_reference_catalog_lock_wait_nanos = self
            .mark_reference_catalog_lock_wait_nanos
            .saturating_add(profile.catalog_mark_lock_wait_nanos);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalSegmentWriteProfile {
    storage_node_ids_nanos: u64,
    placement_select_nanos: u64,
    segment_id_alloc_nanos: u64,
    grant_issue_nanos: u64,
    storage_node_transport_dispatch_nanos: u64,
    grant_verify_nanos: u64,
    catalog_duplicate_probe_nanos: u64,
    catalog_duplicate_probe_lock_wait_nanos: u64,
    catalog_reserve_nanos: u64,
    catalog_reserve_lock_wait_nanos: u64,
    catalog_begin_nanos: u64,
    catalog_begin_lock_wait_nanos: u64,
    segment_store_write_nanos: u64,
    segment_store_lock_wait_nanos: u64,
    checksum_integrity_nanos: u64,
    segment_store_insert_nanos: u64,
    segment_sync_nanos: u64,
    segment_sync_lock_wait_nanos: u64,
    receipt_create_nanos: u64,
    receipt_verify_nanos: u64,
    catalog_commit_nanos: u64,
    catalog_commit_lock_wait_nanos: u64,
}

impl LocalSegmentWriteProfile {
    fn absorb(&mut self, other: Self) {
        self.storage_node_ids_nanos = self
            .storage_node_ids_nanos
            .saturating_add(other.storage_node_ids_nanos);
        self.placement_select_nanos = self
            .placement_select_nanos
            .saturating_add(other.placement_select_nanos);
        self.segment_id_alloc_nanos = self
            .segment_id_alloc_nanos
            .saturating_add(other.segment_id_alloc_nanos);
        self.grant_issue_nanos = self
            .grant_issue_nanos
            .saturating_add(other.grant_issue_nanos);
        self.storage_node_transport_dispatch_nanos = self
            .storage_node_transport_dispatch_nanos
            .saturating_add(other.storage_node_transport_dispatch_nanos);
        self.grant_verify_nanos = self
            .grant_verify_nanos
            .saturating_add(other.grant_verify_nanos);
        self.catalog_duplicate_probe_nanos = self
            .catalog_duplicate_probe_nanos
            .saturating_add(other.catalog_duplicate_probe_nanos);
        self.catalog_duplicate_probe_lock_wait_nanos = self
            .catalog_duplicate_probe_lock_wait_nanos
            .saturating_add(other.catalog_duplicate_probe_lock_wait_nanos);
        self.catalog_reserve_nanos = self
            .catalog_reserve_nanos
            .saturating_add(other.catalog_reserve_nanos);
        self.catalog_reserve_lock_wait_nanos = self
            .catalog_reserve_lock_wait_nanos
            .saturating_add(other.catalog_reserve_lock_wait_nanos);
        self.catalog_begin_nanos = self
            .catalog_begin_nanos
            .saturating_add(other.catalog_begin_nanos);
        self.catalog_begin_lock_wait_nanos = self
            .catalog_begin_lock_wait_nanos
            .saturating_add(other.catalog_begin_lock_wait_nanos);
        self.segment_store_write_nanos = self
            .segment_store_write_nanos
            .saturating_add(other.segment_store_write_nanos);
        self.segment_store_lock_wait_nanos = self
            .segment_store_lock_wait_nanos
            .saturating_add(other.segment_store_lock_wait_nanos);
        self.checksum_integrity_nanos = self
            .checksum_integrity_nanos
            .saturating_add(other.checksum_integrity_nanos);
        self.segment_store_insert_nanos = self
            .segment_store_insert_nanos
            .saturating_add(other.segment_store_insert_nanos);
        self.segment_sync_nanos = self
            .segment_sync_nanos
            .saturating_add(other.segment_sync_nanos);
        self.segment_sync_lock_wait_nanos = self
            .segment_sync_lock_wait_nanos
            .saturating_add(other.segment_sync_lock_wait_nanos);
        self.receipt_create_nanos = self
            .receipt_create_nanos
            .saturating_add(other.receipt_create_nanos);
        self.receipt_verify_nanos = self
            .receipt_verify_nanos
            .saturating_add(other.receipt_verify_nanos);
        self.catalog_commit_nanos = self
            .catalog_commit_nanos
            .saturating_add(other.catalog_commit_nanos);
        self.catalog_commit_lock_wait_nanos = self
            .catalog_commit_lock_wait_nanos
            .saturating_add(other.catalog_commit_lock_wait_nanos);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LocalMarkReferencedProfile {
    /// Point resolution of the storage node carried by receipts or segment
    /// refs; marks never scan catalogs to find a segment's owner.
    routing_nanos: u64,
    /// Node call time not attributed to the catalog mark or event recording.
    /// This is the in-process stand-in for request serialization/transport,
    /// measured as the call wall time minus the node-side measured buckets.
    mark_call_residual_nanos: u64,
    catalog_mark_nanos: u64,
    catalog_mark_lock_wait_nanos: u64,
    /// Node-side observability event/counter recording during the mark.
    observability_record_nanos: u64,
}
