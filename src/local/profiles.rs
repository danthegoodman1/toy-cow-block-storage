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

#[derive(Debug)]
pub(super) struct ReadProfiler {
    capacity: usize,
    next_sequence: u64,
    profiles: VecDeque<ReadProfile>,
}

impl ReadProfiler {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "read profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            profiles: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: ReadProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.profiles.len() == self.capacity {
            self.profiles.pop_front();
        }
        self.profiles.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<ReadProfile> {
        let count = max.min(self.profiles.len());
        self.profiles.drain(..count).collect()
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
    evidence_create_nanos: u64,
    transport_dispatch_nanos: u64,
    verify_nanos: u64,
    catalog_mark_nanos: u64,
    catalog_mark_lock_wait_nanos: u64,
}
