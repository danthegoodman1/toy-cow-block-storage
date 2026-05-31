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

