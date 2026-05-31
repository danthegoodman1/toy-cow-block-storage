/// Execution mode for the in-memory metadata transaction backend used to
/// diagnose block metadata convergence without SQLite in the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTxnMode {
    Serial,
    Sharded { shard_count: usize },
}

/// Profile phase for a metadata transaction. Publish rows are the hot path;
/// node rows are included so hidden metadata-node write costs do not disappear
/// from the profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTxnProfilePhase {
    CreateDevice,
    PersistNode,
    PublishBlockCommit,
    Checkpoint,
    Fork,
    Restore,
    Delete,
}

impl fmt::Display for MetadataTxnProfilePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateDevice => f.write_str("create-device"),
            Self::PersistNode => f.write_str("persist-node"),
            Self::PublishBlockCommit => f.write_str("publish-block-commit"),
            Self::Checkpoint => f.write_str("checkpoint"),
            Self::Fork => f.write_str("fork"),
            Self::Restore => f.write_str("restore"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

/// Per-transaction profile row for the in-memory metadata-service simulator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTxnProfile {
    pub sequence: u64,
    pub phase: MetadataTxnProfilePhase,
    pub total_nanos: u64,
    pub tx_lock_wait_nanos: u64,
    pub read_validation_nanos: u64,
    pub apply_write_nanos: u64,
    pub commit_version_alloc_nanos: u64,
    pub touched_key_shards: u64,
    pub read_key_count: u64,
    pub write_key_count: u64,
    pub conflict_count: u64,
}

impl MetadataTxnProfile {
    fn new(phase: MetadataTxnProfilePhase) -> Self {
        Self {
            sequence: 0,
            phase,
            total_nanos: 0,
            tx_lock_wait_nanos: 0,
            read_validation_nanos: 0,
            apply_write_nanos: 0,
            commit_version_alloc_nanos: 0,
            touched_key_shards: 0,
            read_key_count: 0,
            write_key_count: 0,
            conflict_count: 0,
        }
    }
}

/// Per-successful-write profile row for the transaction block coordinator.
///
/// This profile intentionally spans the whole block write pipeline around the
/// metadata transaction so loadbench can attribute same-device shard-lane
/// costs without introducing a fake data or metadata path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnBlockWriteProfile {
    pub sequence: u64,
    pub total_nanos: u64,
    pub device_spec_lookup_nanos: u64,
    pub range_split_shard_head_read_nanos: u64,
    pub write_intent_alloc_nanos: u64,
    pub payload_copy_nanos: u64,
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
    pub metadata_publish_call_nanos: u64,
    pub mark_referenced_nanos: u64,
    pub mark_reference_evidence_nanos: u64,
    pub mark_reference_transport_dispatch_nanos: u64,
    pub mark_reference_verify_nanos: u64,
    pub mark_reference_catalog_nanos: u64,
    pub mark_reference_catalog_lock_wait_nanos: u64,
    pub touched_shard_count: u64,
    pub segment_count: u64,
    pub storage_node_count: u64,
}

impl TxnBlockWriteProfile {
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
        self.grant_issue_nanos = self
            .grant_issue_nanos
            .saturating_add(profile.grant_issue_nanos);
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
        self.mark_reference_evidence_nanos = self
            .mark_reference_evidence_nanos
            .saturating_add(profile.evidence_create_nanos);
        self.mark_reference_transport_dispatch_nanos = self
            .mark_reference_transport_dispatch_nanos
            .saturating_add(profile.transport_dispatch_nanos);
        self.mark_reference_verify_nanos = self
            .mark_reference_verify_nanos
            .saturating_add(profile.verify_nanos);
        self.mark_reference_catalog_nanos = self
            .mark_reference_catalog_nanos
            .saturating_add(profile.catalog_mark_nanos);
        self.mark_reference_catalog_lock_wait_nanos = self
            .mark_reference_catalog_lock_wait_nanos
            .saturating_add(profile.catalog_mark_lock_wait_nanos);
    }
}

#[derive(Debug)]
pub(super) struct TxnBlockWriteProfileBuffer {
    capacity: usize,
    next_sequence: u64,
    rows: VecDeque<TxnBlockWriteProfile>,
}

impl TxnBlockWriteProfileBuffer {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "block write profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            rows: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: TxnBlockWriteProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.rows.len() == self.capacity {
            self.rows.pop_front();
        }
        self.rows.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<TxnBlockWriteProfile> {
        let mut out = Vec::new();
        for _ in 0..max {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            out.push(row);
        }
        out
    }
}

#[derive(Debug)]
pub(super) struct MetadataTxnProfileBuffer {
    capacity: usize,
    next_sequence: u64,
    rows: VecDeque<MetadataTxnProfile>,
}

impl MetadataTxnProfileBuffer {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "metadata transaction profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            rows: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: MetadataTxnProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.rows.len() == self.capacity {
            self.rows.pop_front();
        }
        self.rows.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<MetadataTxnProfile> {
        let mut out = Vec::new();
        for _ in 0..max {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            out.push(row);
        }
        out
    }
}
