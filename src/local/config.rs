#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LocalStoreConfig {
    pub shard_count: usize,
    pub block_size: u32,
    pub file_root_blocks: u64,
    pub metadata_fanout: usize,
    pub metadata_leaf_blocks: u64,
    pub storage_node: StorageNodeId,
    pub observability_event_capacity: usize,
    pub stream_auto_persist_bytes: Option<u64>,
}

impl Default for LocalStoreConfig {
    fn default() -> Self {
        Self {
            shard_count: 1,
            block_size: 4096,
            file_root_blocks: 1,
            metadata_fanout: 4,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(1),
            observability_event_capacity: DEFAULT_OBSERVABILITY_EVENT_CAPACITY,
            stream_auto_persist_bytes: None,
        }
    }
}

impl LocalStoreConfig {
    fn storage_shape_matches(self, other: Self) -> bool {
        self.shard_count == other.shard_count
            && self.block_size == other.block_size
            && self.file_root_blocks == other.file_root_blocks
            && self.metadata_fanout == other.metadata_fanout
            && self.metadata_leaf_blocks == other.metadata_leaf_blocks
            && self.storage_node == other.storage_node
    }

    fn with_runtime_policy(self, expected: Self) -> Self {
        Self {
            observability_event_capacity: expected.observability_event_capacity,
            stream_auto_persist_bytes: expected.stream_auto_persist_bytes,
            ..self
        }
    }

    pub fn validate(self) -> Result<()> {
        if self.shard_count == 0 {
            return Err(StorageError::invalid_argument(
                "shard_count must be greater than zero",
            ));
        }

        if self.block_size == 0 {
            return Err(StorageError::invalid_argument(
                "block_size must be greater than zero",
            ));
        }

        if !self.block_size.is_power_of_two() {
            return Err(StorageError::invalid_argument(
                "block_size must be a power of two",
            ));
        }

        if self.file_root_blocks == 0 {
            return Err(StorageError::invalid_argument(
                "file_root_blocks must be greater than zero",
            ));
        }

        if self.metadata_fanout < 2 {
            return Err(StorageError::invalid_argument(
                "metadata_fanout must be at least two",
            ));
        }

        if self.metadata_leaf_blocks == 0 {
            return Err(StorageError::invalid_argument(
                "metadata_leaf_blocks must be greater than zero",
            ));
        }

        if self.observability_event_capacity == 0 {
            return Err(StorageError::invalid_argument(
                "observability_event_capacity must be greater than zero",
            ));
        }

        if self.stream_auto_persist_bytes == Some(0) {
            return Err(StorageError::invalid_argument(
                "stream_auto_persist_bytes must be greater than zero when configured",
            ));
        }

        Ok(())
    }

    fn for_storage_node(self, storage_node: StorageNodeId) -> Self {
        Self {
            storage_node,
            ..self
        }
    }
}

pub(super) fn normalize_storage_nodes(
    primary: StorageNodeId,
    storage_nodes: Vec<StorageNodeId>,
) -> Vec<StorageNodeId> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for node_id in std::iter::once(primary).chain(storage_nodes) {
        if seen.insert(node_id) {
            out.push(node_id);
        }
    }
    out
}

pub(super) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

pub(super) fn durable_commit_high_water_from_local(local: &LocalCoordinator) -> Result<CommitSeq> {
    let metadata = local.metadata.state_inner()?;
    Ok(CommitSeq::from_raw(
        metadata.next_commit_seq.saturating_sub(1),
    ))
}
