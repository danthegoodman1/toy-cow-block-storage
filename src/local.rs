use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::api::{
    BlockClient, BlockDevice, BlockRequest, BlockRequestEnvelope, BlockResponse,
    BlockResponseEnvelope, BlockServer, BlockTransport, ByteRange, CreateDeviceRequest,
    DeleteResult, DeviceInfo, FlushResult, ForkRequest, ReadResponse, RestorePoint, WriteCommit,
};
use crate::error::{Result, StorageError};
use crate::extent::{
    AppendCommit, AppendLease, CreateFileRequest, FileInfo, NativeFile, NativeFileClient,
    NativeRequest, NativeRequestEnvelope, NativeResponse, NativeResponseEnvelope, NativeServer,
    NativeTransport,
};
use crate::id::{
    AppendLeaseId, BlockCount, BlockIndex, CheckpointId, CommitGroupId, CommitSeq,
    DeviceGeneration, DeviceId, ExtentId, FileId, FileVersion, LogicalTime, MetadataNodeId,
    RequestId, SegmentId, StorageNodeId, WriteIntentId, WriterEpoch,
};
use crate::object::{
    Checkpoint, CommitGroup, DeleteRecord, DeviceHead, FileHead, ForkRecord, LeafEntry,
    MappingOwner, MetadataChild, MetadataNode, MetadataNodeKind, RootUpdate, SegmentDescriptor,
    ShardCommit, ShardRootUpdate,
};
use crate::provider::{
    CommitGroupIntent, LocalSegmentCatalog, MetadataCreateDeviceRequest, MetadataCreateFileRequest,
    MetadataFence, MetadataForkRequest, MetadataPlane, RetentionPolicy, SegmentReplicaCommit,
    SegmentReplicaPlacement, SegmentReservation, SegmentReservationIntent, SegmentStore,
};

/// Local provider configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalStoreConfig {
    pub shard_count: usize,
    pub block_size: u32,
    pub file_root_blocks: u64,
    pub metadata_fanout: usize,
    pub metadata_leaf_blocks: u64,
    pub storage_node: StorageNodeId,
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
        }
    }
}

impl LocalStoreConfig {
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

        Ok(())
    }
}

/// Shared local in-process provider bundle.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    metadata: Arc<InMemoryMetadataPlane>,
    segment_store: Arc<InMemorySegmentStore>,
    segment_catalog: Arc<InMemoryLocalSegmentCatalog>,
    next_write_intent: Arc<Mutex<u128>>,
    next_extent_id: Arc<Mutex<u128>>,
}

impl LocalObjectStore {
    pub fn new() -> Self {
        Self::with_config(LocalStoreConfig::default()).expect("default local store config is valid")
    }

    pub fn with_config(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            metadata: Arc::new(InMemoryMetadataPlane::new(config)?),
            segment_store: Arc::new(InMemorySegmentStore::new(config)?),
            segment_catalog: Arc::new(InMemoryLocalSegmentCatalog::new(config)?),
            next_write_intent: Arc::new(Mutex::new(1)),
            next_extent_id: Arc::new(Mutex::new(1)),
        })
    }

    pub fn metadata(&self) -> Arc<InMemoryMetadataPlane> {
        Arc::clone(&self.metadata)
    }

    pub fn segment_store(&self) -> Arc<InMemorySegmentStore> {
        Arc::clone(&self.segment_store)
    }

    pub fn segment_catalog(&self) -> Arc<InMemoryLocalSegmentCatalog> {
        Arc::clone(&self.segment_catalog)
    }

    pub fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<WriteCommit> {
        let info = self.metadata.device_info(device_id)?;
        let len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&info.spec)?;

        if len == 0 {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability,
            });
        }

        let block_size = u64::from(info.spec.block_size);
        let chunks = self.split_device_range(&info, range)?;
        let owner = MappingOwner::BlockDevice(device_id);
        let write_intent = self.next_write_intent()?;
        let mut updates = Vec::with_capacity(chunks.len());
        let mut segment_ids = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let chunk_offset = chunk
                .range
                .start
                .raw()
                .checked_mul(block_size)
                .and_then(|start| start.checked_sub(offset))
                .ok_or_else(|| StorageError::invalid_argument("write chunk offset overflows"))?;
            let byte_start = usize::try_from(chunk_offset).map_err(|_| {
                StorageError::invalid_argument("write chunk offset overflows usize")
            })?;
            let chunk_len = chunk
                .range
                .blocks
                .raw()
                .checked_mul(block_size)
                .ok_or_else(|| StorageError::invalid_argument("write chunk length overflows"))?;
            let byte_len = usize::try_from(chunk_len).map_err(|_| {
                StorageError::invalid_argument("write chunk length overflows usize")
            })?;
            let byte_end = byte_start
                .checked_add(byte_len)
                .ok_or_else(|| StorageError::invalid_argument("write chunk end overflows"))?;
            let chunk_bytes = data
                .get(byte_start..byte_end)
                .ok_or_else(|| StorageError::corrupt("write chunk is outside request bytes"))?;
            let reservation =
                self.write_segment_for_owner_with_intent(owner, write_intent, chunk_bytes)?;
            segment_ids.push(reservation.segment_id);

            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: Some(SegmentReplacement {
                    segment_id: reservation.segment_id,
                    segment_base: chunk.range.start,
                }),
            };
            let new_root = self.replace_tree_range(chunk.old_root, edit)?.root;
            updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
                new_root,
            }));
        }

        let current = self.metadata.get_head(device_id)?;
        let commit_group = self.metadata.publish_commit_group(CommitGroupIntent {
            owner,
            fence: MetadataFence::DeviceGeneration(current.generation),
            updates,
        })?;

        for segment_id in segment_ids {
            self.segment_catalog.mark_segment_referenced(segment_id)?;
        }

        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability,
        })
    }

    pub fn write_zeroes(&self, device_id: DeviceId, offset: u64, len: u64) -> Result<WriteCommit> {
        let zeroes = usize::try_from(len)
            .map_err(|_| StorageError::invalid_argument("zero range length overflows usize"))?;
        self.write_device(
            device_id,
            offset,
            &vec![0; zeroes],
            crate::api::WriteDurability::Acknowledged,
        )
    }

    pub fn discard_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        len: u64,
    ) -> Result<WriteCommit> {
        let info = self.metadata.device_info(device_id)?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&info.spec)?;

        if len == 0 {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability: crate::api::WriteDurability::Acknowledged,
            });
        }

        let chunks = self.split_device_range(&info, range)?;
        let owner = MappingOwner::BlockDevice(device_id);
        let mut updates = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: None,
            };
            let edit_result = self.replace_tree_range(chunk.old_root, edit)?;
            if edit_result.changed {
                updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: chunk.shard_id,
                    old_root: chunk.old_root,
                    new_root: edit_result.root,
                }));
            }
        }

        if updates.is_empty() {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability: crate::api::WriteDurability::Acknowledged,
            });
        }

        let current = self.metadata.get_head(device_id)?;
        let commit_group = self.metadata.publish_commit_group(CommitGroupIntent {
            owner,
            fence: MetadataFence::DeviceGeneration(current.generation),
            updates,
        })?;

        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability: crate::api::WriteDurability::Acknowledged,
        })
    }

    pub fn acquire_append_lease(&self, file_id: FileId) -> Result<AppendLease> {
        self.metadata.acquire_append_lease(file_id)
    }

    pub fn append_file(
        &self,
        lease: AppendLease,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<AppendCommit> {
        if data.is_empty() {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        let data_len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?;
        if data_len % u64::from(self.metadata.config.block_size) != 0 {
            return Err(StorageError::invalid_argument(
                "append payload must be block aligned",
            ));
        }

        let head = self.metadata.get_file_head(lease.file_id)?;
        if head.version != lease.base_version {
            return Err(StorageError::conflict("stale append lease"));
        }
        self.metadata
            .validate_writer_epoch(lease.file_id, lease.writer_epoch)?;

        let owner = MappingOwner::NativeFile(lease.file_id);
        let reservation = self.write_segment_for_owner_with_intent(
            owner,
            WriteIntentId::from_raw(lease.lease_id.raw()),
            data,
        )?;
        let block_size = u64::from(self.metadata.config.block_size);
        let append_range = crate::api::BlockRange::new(
            BlockIndex::from_raw(head.size / block_size),
            BlockCount::from_raw(data_len / block_size),
        );
        let new_size = head
            .size
            .checked_add(data_len)
            .ok_or_else(|| StorageError::invalid_argument("file size overflows u64"))?;
        let edit = TreeRangeEdit {
            range: append_range,
            replacement: Some(SegmentReplacement {
                segment_id: reservation.segment_id,
                segment_base: append_range.start,
            }),
        };
        if self.tree_has_mappings(head.root, append_range)? {
            return Err(StorageError::conflict(
                "append range overlaps existing file metadata",
            ));
        }
        let new_root = self.replace_tree_range(head.root, edit)?.root;

        self.metadata.publish_commit_group(CommitGroupIntent {
            owner,
            fence: MetadataFence::WriterEpoch {
                base_version: lease.base_version,
                writer_epoch: lease.writer_epoch,
            },
            updates: vec![RootUpdate::FileRoot {
                old_root: head.root,
                new_root,
                new_size,
            }],
        })?;
        self.segment_catalog
            .mark_segment_referenced(reservation.segment_id)?;
        let committed = self.metadata.get_file_head(lease.file_id)?;

        Ok(AppendCommit {
            file_id: lease.file_id,
            extent_id: self.next_extent_id()?,
            range: ByteRange::new(head.size, data_len),
            version: committed.version,
            durability,
        })
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        let head = self.metadata.fork_device(MetadataForkRequest {
            source,
            target: request.target,
            name: request.name,
        })?;
        Ok(head.device_id)
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        let head = self.metadata.restore_device(source, point)?;
        Ok(head.device_id)
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.metadata.delete_device(device_id)
    }

    pub fn mark_reachable_for_gc(&self, policy: RetentionPolicy) -> Result<MetadataMarkReport> {
        self.metadata.mark_reachable_for_gc(policy)
    }

    pub fn sweep_metadata_after_mark(
        &self,
        policy: RetentionPolicy,
        epoch: u64,
    ) -> Result<MetadataSweepReport> {
        let sweep = self.metadata.sweep_unmarked_after_mark(policy, epoch)?;
        for segment_id in &sweep.released_segments {
            if self.segment_catalog.state(*segment_id)? == SegmentLifecycleState::Referenced {
                self.segment_catalog.release_segment(*segment_id)?;
            }
        }
        Ok(sweep)
    }

    pub fn run_metadata_custodian(
        &self,
        policy: RetentionPolicy,
    ) -> Result<MetadataCustodianReport> {
        let mark = self.mark_reachable_for_gc(policy.clone())?;
        let sweep = self.sweep_metadata_after_mark(policy, mark.epoch)?;
        let mut catalog_released_segments = Vec::new();
        for segment_id in &sweep.released_segments {
            if self.segment_catalog.state(*segment_id)? == SegmentLifecycleState::Released {
                catalog_released_segments.push(*segment_id);
            }
        }
        Ok(MetadataCustodianReport {
            mark,
            sweep,
            catalog_released_segments,
        })
    }

    pub fn run_storage_node_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let mut report = StorageNodeCustodianReport {
            expired_reservations: Vec::new(),
            failed_writes: Vec::new(),
            orphan_segments: Vec::new(),
            deleted_released_segments: Vec::new(),
        };

        for (segment_id, state, write_intent) in self.segment_catalog.entries()? {
            match state {
                SegmentLifecycleState::Reserved
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.expire_reservation(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.expired_reservations.push(segment_id);
                }
                SegmentLifecycleState::Writing if expired_write_intents.contains(&write_intent) => {
                    self.segment_catalog.fail_write(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.failed_writes.push(segment_id);
                }
                SegmentLifecycleState::DurablePendingMetadata
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.free_orphan_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.orphan_segments.push(segment_id);
                }
                SegmentLifecycleState::Released => {
                    self.segment_catalog.delete_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.deleted_released_segments.push(segment_id);
                }
                _ => {}
            }
        }

        Ok(report)
    }

    fn split_device_range(
        &self,
        info: &DeviceInfo,
        range: ByteRange,
    ) -> Result<Vec<DeviceWriteChunk>> {
        let block_size = u64::from(info.spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let head = self.metadata.get_head(info.device_id)?;
        let mut chunks = Vec::new();

        for (shard, root) in head.shard_roots.iter().enumerate() {
            let node = self.metadata.get_metadata_node(*root)?;
            let Some(overlap) = node.covered_range.intersection(requested)? else {
                continue;
            };
            let shard_id = u32::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?;
            chunks.push(DeviceWriteChunk {
                shard_id: crate::id::ShardId::from_raw(shard_id),
                old_root: *root,
                range: overlap,
            });
        }

        if chunks.is_empty() && range.len != 0 {
            return Err(StorageError::corrupt(
                "device range did not overlap any shard roots",
            ));
        }

        Ok(chunks)
    }

    #[cfg(test)]
    fn write_segment_for_owner(
        &self,
        owner: MappingOwner,
        data: &[u8],
    ) -> Result<SegmentReservation> {
        let write_intent = self.next_write_intent()?;
        self.write_segment_for_owner_with_intent(owner, write_intent, data)
    }

    fn write_segment_for_owner_with_intent(
        &self,
        owner: MappingOwner,
        write_intent: WriteIntentId,
        data: &[u8],
    ) -> Result<SegmentReservation> {
        let intent = SegmentReservationIntent {
            write_intent,
            owner,
            bytes: u64::try_from(data.len()).map_err(|_| {
                StorageError::invalid_argument("segment reservation byte length overflows u64")
            })?,
        };
        let reservation = self.segment_catalog.reserve_segment(intent)?;
        self.segment_catalog.begin_write(&reservation)?;
        let commit = self.segment_store.write_segment(&reservation, data)?;
        self.segment_store.sync_segment(reservation.segment_id)?;
        self.segment_catalog
            .commit_segment(reservation.clone(), commit)?;
        Ok(reservation)
    }

    fn descriptors_for_entries(&self, entries: &[LeafEntry]) -> Result<Vec<SegmentDescriptor>> {
        let mut descriptors: BTreeMap<SegmentId, SegmentDescriptor> = BTreeMap::new();
        for entry in entries {
            if let std::collections::btree_map::Entry::Vacant(vacant) =
                descriptors.entry(entry.segment_id)
            {
                vacant.insert(
                    self.segment_catalog
                        .commit_for_segment(entry.segment_id)?
                        .descriptor,
                );
            }
        }
        Ok(descriptors.into_values().collect())
    }

    fn next_write_intent(&self) -> Result<WriteIntentId> {
        let mut next = lock(&self.next_write_intent)?;
        let id = WriteIntentId::from_raw(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("write intent id overflow"))?;
        Ok(id)
    }

    fn next_extent_id(&self) -> Result<ExtentId> {
        let mut next = lock(&self.next_extent_id)?;
        let id = ExtentId::from_raw(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("extent id overflow"))?;
        Ok(id)
    }

    fn replace_tree_range(
        &self,
        root_id: MetadataNodeId,
        edit: TreeRangeEdit,
    ) -> Result<TreeEditResult> {
        edit.range.validate_non_empty()?;
        let root = self.metadata.get_metadata_node(root_id)?;
        if !root.covered_range.contains_range(edit.range)? {
            return Err(StorageError::invalid_argument(
                "edit range is outside metadata tree coverage",
            ));
        }
        self.replace_tree_range_at(&root, edit)
    }

    fn replace_tree_range_at(
        &self,
        node: &MetadataNode,
        edit: TreeRangeEdit,
    ) -> Result<TreeEditResult> {
        if !node.covered_range.overlaps(edit.range)? {
            return Ok(TreeEditResult {
                root: node.node_id,
                changed: false,
            });
        }

        match &node.kind {
            MetadataNodeKind::Leaf { entries } => {
                let Some(overlap) = node.covered_range.intersection(edit.range)? else {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                };
                let replacement = edit.replacement.map(|replacement| {
                    let offset = overlap.start.raw() - replacement.segment_base.raw();
                    LeafEntry {
                        logical_start: overlap.start,
                        blocks: overlap.blocks,
                        segment_id: replacement.segment_id,
                        segment_offset: BlockIndex::from_raw(offset),
                    }
                });
                let new_entries =
                    replace_leaf_entries(entries, node.covered_range, overlap, replacement)?;
                if new_entries == *entries {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }
                let segment_descriptors = self.descriptors_for_entries(&new_entries)?;
                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Leaf {
                        entries: new_entries,
                    },
                )?;
                new_node.validate(&segment_descriptors)?;
                self.metadata.persist_metadata_node(new_node.clone())?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
            MetadataNodeKind::Internal { children } => {
                let mut changed = false;
                let mut new_children = Vec::with_capacity(children.len());
                for child in children {
                    if child.range.overlaps(edit.range)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        let child_result = self.replace_tree_range_at(&child_node, edit)?;
                        changed |= child_result.changed;
                        new_children.push(MetadataChild {
                            range: child.range,
                            node_id: child_result.root,
                        });
                    } else {
                        new_children.push(child.clone());
                    }
                }

                if !changed {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }

                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Internal {
                        children: new_children,
                    },
                )?;
                new_node.validate(&[])?;
                self.metadata.persist_metadata_node(new_node.clone())?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
        }
    }

    fn tree_has_mappings(
        &self,
        root_id: MetadataNodeId,
        range: crate::api::BlockRange,
    ) -> Result<bool> {
        range.validate_non_empty()?;
        let node = self.metadata.get_metadata_node(root_id)?;
        self.node_has_mappings(&node, range)
    }

    fn node_has_mappings(
        &self,
        node: &MetadataNode,
        range: crate::api::BlockRange,
    ) -> Result<bool> {
        if !node.covered_range.overlaps(range)? {
            return Ok(false);
        }

        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(range)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        if self.node_has_mappings(&child_node, range)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            MetadataNodeKind::Leaf { entries } => {
                for entry in entries {
                    if entry.logical_range().overlaps(range)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub fn validate_metadata_tree(&self, root_id: MetadataNodeId) -> Result<MetadataTreeStats> {
        let mut visited = BTreeSet::new();
        self.validate_metadata_tree_at(root_id, 1, &mut visited)
    }

    fn validate_metadata_tree_at(
        &self,
        node_id: MetadataNodeId,
        depth: usize,
        visited: &mut BTreeSet<MetadataNodeId>,
    ) -> Result<MetadataTreeStats> {
        if !visited.insert(node_id) {
            return Err(StorageError::corrupt(
                "metadata tree contains a repeated node ID",
            ));
        }

        let node = self.metadata.get_metadata_node(node_id)?;
        match &node.kind {
            MetadataNodeKind::Leaf { entries } => {
                if node.covered_range.blocks.raw() > self.metadata.config.metadata_leaf_blocks {
                    return Err(StorageError::corrupt(
                        "metadata leaf exceeds configured leaf block span",
                    ));
                }
                let descriptors = self.descriptors_for_entries(entries)?;
                node.validate(&descriptors)?;
                Ok(MetadataTreeStats {
                    nodes: 1,
                    leaves: 1,
                    max_depth: depth,
                })
            }
            MetadataNodeKind::Internal { children } => {
                if children.len() > self.metadata.config.metadata_fanout {
                    return Err(StorageError::corrupt(
                        "metadata internal node exceeds configured fanout",
                    ));
                }
                node.validate(&[])?;
                let mut stats = MetadataTreeStats {
                    nodes: 1,
                    leaves: 0,
                    max_depth: depth,
                };
                for child in children {
                    let child_node = self.metadata.get_metadata_node(child.node_id)?;
                    if child_node.covered_range != child.range {
                        return Err(StorageError::corrupt(
                            "metadata child range does not match child node coverage",
                        ));
                    }
                    let child_stats =
                        self.validate_metadata_tree_at(child.node_id, depth + 1, visited)?;
                    stats.nodes += child_stats.nodes;
                    stats.leaves += child_stats.leaves;
                    stats.max_depth = stats.max_depth.max(child_stats.max_depth);
                }
                Ok(stats)
            }
        }
    }

    pub fn metadata_tree_node_ids(&self, root_id: MetadataNodeId) -> Result<Vec<MetadataNodeId>> {
        let mut out = Vec::new();
        self.collect_metadata_tree_node_ids(root_id, &mut out)?;
        Ok(out)
    }

    fn collect_metadata_tree_node_ids(
        &self,
        node_id: MetadataNodeId,
        out: &mut Vec<MetadataNodeId>,
    ) -> Result<()> {
        out.push(node_id);
        let node = self.metadata.get_metadata_node(node_id)?;
        if let MetadataNodeKind::Internal { children } = node.kind {
            for child in children {
                self.collect_metadata_tree_node_ids(child.node_id, out)?;
            }
        }
        Ok(())
    }

    pub fn render_metadata_tree(&self, root_id: MetadataNodeId) -> Result<String> {
        let mut out = String::new();
        self.render_metadata_tree_at(root_id, 0, &mut out)?;
        Ok(out)
    }

    fn render_metadata_tree_at(
        &self,
        node_id: MetadataNodeId,
        depth: usize,
        out: &mut String,
    ) -> Result<()> {
        let node = self.metadata.get_metadata_node(node_id)?;
        let indent = "  ".repeat(depth);
        match node.kind {
            MetadataNodeKind::Internal { children } => {
                out.push_str(&format!(
                    "{indent}node {} internal [{}..{}) children={}\n",
                    node.node_id,
                    node.covered_range.start.raw(),
                    node.covered_range.end_exclusive()?.raw(),
                    children.len()
                ));
                for child in children {
                    self.render_metadata_tree_at(child.node_id, depth + 1, out)?;
                }
            }
            MetadataNodeKind::Leaf { entries } => {
                out.push_str(&format!(
                    "{indent}node {} leaf [{}..{}) entries={}\n",
                    node.node_id,
                    node.covered_range.start.raw(),
                    node.covered_range.end_exclusive()?.raw(),
                    entries.len()
                ));
                for entry in entries {
                    out.push_str(&format!(
                        "{indent}  [{}..{}) -> segment {}@{}\n",
                        entry.logical_start.raw(),
                        entry.logical_range().end_exclusive()?.raw(),
                        entry.segment_id,
                        entry.segment_offset.raw()
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let info = self.metadata.device_info(device_id)?;
        range.validate_for_device(&info.spec)?;
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        buf.fill(0);
        if range.len == 0 {
            return Ok(());
        }

        let block_size = u64::from(info.spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let head = self.metadata.get_head(device_id)?;

        for root in head.shard_roots {
            let node = self.metadata.get_metadata_node(root)?;
            if node.covered_range.overlaps(requested)? {
                self.read_metadata_node(&node, requested, block_size, buf)?;
            }
        }

        Ok(())
    }

    pub fn read_file(&self, file_id: FileId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let head = self.metadata.get_file_head(file_id)?;
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }
        let end = range.end_exclusive()?;
        if end > head.size {
            return Err(StorageError::invalid_argument(
                "native file read extends past end of file",
            ));
        }

        buf.fill(0);
        if range.len == 0 {
            let _ = self.metadata.get_metadata_node(head.root)?;
            return Ok(());
        }

        if !range.is_aligned_to(self.metadata.config.block_size) {
            return Err(StorageError::unsupported(
                "non-empty native file reads require block alignment in this phase",
            ));
        }

        let block_size = u64::from(self.metadata.config.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let root = self.metadata.get_metadata_node(head.root)?;
        self.read_metadata_node(&root, requested, block_size, buf)
    }

    fn read_metadata_node(
        &self,
        node: &MetadataNode,
        requested: crate::api::BlockRange,
        block_size: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.read_metadata_node(&child_node, requested, block_size, buf)?;
                    }
                }
                Ok(())
            }
            MetadataNodeKind::Leaf { entries } => {
                for entry in entries {
                    let Some(overlap) = entry.logical_range().intersection(requested)? else {
                        continue;
                    };
                    let segment_offset_blocks = entry
                        .segment_offset
                        .raw()
                        .checked_add(overlap.start.raw() - entry.logical_start.raw())
                        .ok_or_else(|| {
                            StorageError::invalid_argument("segment read offset overflows")
                        })?;
                    let segment_range = ByteRange::new(
                        segment_offset_blocks
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte offset overflows")
                            })?,
                        overlap
                            .blocks
                            .raw()
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte length overflows")
                            })?,
                    );
                    let output_offset = usize::try_from(
                        (overlap.start.raw() - requested.start.raw())
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("read output offset overflows")
                            })?,
                    )
                    .map_err(|_| {
                        StorageError::invalid_argument("read output offset overflows usize")
                    })?;
                    let output_len = usize::try_from(segment_range.len).map_err(|_| {
                        StorageError::invalid_argument("read output length overflows usize")
                    })?;
                    let output_end = output_offset.checked_add(output_len).ok_or_else(|| {
                        StorageError::invalid_argument("read output end overflows")
                    })?;
                    let output = buf.get_mut(output_offset..output_end).ok_or_else(|| {
                        StorageError::corrupt("metadata read output range exceeds buffer")
                    })?;
                    self.segment_store
                        .read_segment(entry.segment_id, segment_range, output)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for LocalObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeviceWriteChunk {
    shard_id: crate::id::ShardId,
    old_root: MetadataNodeId,
    range: crate::api::BlockRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentReplacement {
    segment_id: SegmentId,
    segment_base: BlockIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeRangeEdit {
    range: crate::api::BlockRange,
    replacement: Option<SegmentReplacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeEditResult {
    root: MetadataNodeId,
    changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTreeStats {
    pub nodes: usize,
    pub leaves: usize,
    pub max_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMarkReport {
    pub epoch: u64,
    pub roots: Vec<MetadataNodeId>,
    pub metadata_nodes: Vec<MetadataNodeId>,
    pub segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSweepReport {
    pub epoch: u64,
    pub deleted_metadata_nodes: Vec<MetadataNodeId>,
    pub released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCustodianReport {
    pub mark: MetadataMarkReport,
    pub sweep: MetadataSweepReport,
    pub catalog_released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodeCustodianReport {
    pub expired_reservations: Vec<SegmentId>,
    pub failed_writes: Vec<SegmentId>,
    pub orphan_segments: Vec<SegmentId>,
    pub deleted_released_segments: Vec<SegmentId>,
}

#[derive(Debug)]
struct MetadataInner {
    next_device_id: u128,
    next_file_id: u128,
    next_metadata_node_id: u128,
    next_commit_group_id: u128,
    next_commit_seq: u64,
    next_checkpoint_id: u128,
    next_gc_epoch: u64,
    device_heads: BTreeMap<DeviceId, DeviceHead>,
    deleted_device_heads: BTreeMap<DeviceId, DeviceHead>,
    device_specs: BTreeMap<DeviceId, crate::api::DeviceSpec>,
    file_heads: BTreeMap<FileId, FileHead>,
    file_specs: BTreeMap<FileId, crate::extent::FileSpec>,
    file_writer_epochs: BTreeMap<FileId, WriterEpoch>,
    metadata_nodes: BTreeMap<MetadataNodeId, MetadataNode>,
    commit_groups: BTreeMap<CommitGroupId, CommitGroup>,
    shard_commits: Vec<ShardCommit>,
    fork_records: BTreeMap<CommitSeq, ForkRecord>,
    delete_records: BTreeMap<CommitSeq, DeleteRecord>,
    checkpoints: BTreeMap<CheckpointId, Checkpoint>,
    metadata_last_mark_epoch: BTreeMap<MetadataNodeId, u64>,
    segment_last_mark_epoch: BTreeMap<SegmentId, u64>,
}

impl MetadataInner {
    fn new() -> Self {
        Self {
            next_device_id: 1,
            next_file_id: 1,
            next_metadata_node_id: 1,
            next_commit_group_id: 1,
            next_commit_seq: 1,
            next_checkpoint_id: 1,
            next_gc_epoch: 1,
            device_heads: BTreeMap::new(),
            deleted_device_heads: BTreeMap::new(),
            device_specs: BTreeMap::new(),
            file_heads: BTreeMap::new(),
            file_specs: BTreeMap::new(),
            file_writer_epochs: BTreeMap::new(),
            metadata_nodes: BTreeMap::new(),
            commit_groups: BTreeMap::new(),
            shard_commits: Vec::new(),
            fork_records: BTreeMap::new(),
            delete_records: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            metadata_last_mark_epoch: BTreeMap::new(),
            segment_last_mark_epoch: BTreeMap::new(),
        }
    }

    fn alloc_device_id(&mut self) -> DeviceId {
        let id = DeviceId::from_raw(self.next_device_id);
        self.next_device_id += 1;
        id
    }

    fn reserve_device_id_at_least_after(&mut self, device_id: DeviceId) -> Result<()> {
        if device_id.raw() >= self.next_device_id {
            self.next_device_id = device_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("device id overflow"))?;
        }
        Ok(())
    }

    fn alloc_file_id(&mut self) -> FileId {
        let id = FileId::from_raw(self.next_file_id);
        self.next_file_id += 1;
        id
    }

    fn alloc_metadata_node_id(&mut self) -> MetadataNodeId {
        let id = MetadataNodeId::from_raw(self.next_metadata_node_id);
        self.next_metadata_node_id += 1;
        id
    }

    fn alloc_commit_group_id(&mut self) -> CommitGroupId {
        let id = CommitGroupId::from_raw(self.next_commit_group_id);
        self.next_commit_group_id += 1;
        id
    }

    fn alloc_commit_seq(&mut self) -> Result<CommitSeq> {
        let seq = CommitSeq::from_raw(self.next_commit_seq);
        self.next_commit_seq = self
            .next_commit_seq
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("commit sequence overflow"))?;
        Ok(seq)
    }

    fn alloc_checkpoint_id(&mut self) -> CheckpointId {
        let id = CheckpointId::from_raw(self.next_checkpoint_id);
        self.next_checkpoint_id += 1;
        id
    }

    fn alloc_gc_epoch(&mut self) -> Result<u64> {
        let epoch = self.next_gc_epoch;
        self.next_gc_epoch = self
            .next_gc_epoch
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("GC epoch overflow"))?;
        Ok(epoch)
    }

    fn insert_checkpoint(
        &mut self,
        owner: MappingOwner,
        commit_seq: CommitSeq,
        shard_roots: Vec<MetadataNodeId>,
    ) -> CheckpointId {
        let checkpoint_id = self.alloc_checkpoint_id();
        let checkpoint = Checkpoint {
            checkpoint_id,
            commit_seq,
            time: LogicalTime::from_raw(commit_seq.raw()),
            owner,
            shard_roots,
        };
        self.checkpoints.insert(checkpoint_id, checkpoint);
        checkpoint_id
    }
}

/// In-memory implementation of `MetadataPlane`.
#[derive(Debug)]
pub struct InMemoryMetadataPlane {
    config: LocalStoreConfig,
    inner: Mutex<MetadataInner>,
}

impl InMemoryMetadataPlane {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(MetadataInner::new()),
        })
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let inner = lock(&self.inner)?;
        let head = inner
            .device_heads
            .get(&device_id)
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        let spec = inner
            .device_specs
            .get(&device_id)
            .ok_or_else(|| StorageError::corrupt("device head exists without spec"))?;

        Ok(DeviceInfo {
            device_id,
            generation: head.generation,
            spec: spec.clone(),
            latest_commit: head.latest_commit,
        })
    }

    pub fn commit_group(&self, commit_group: CommitGroupId) -> Result<CommitGroup> {
        let inner = lock(&self.inner)?;
        inner
            .commit_groups
            .get(&commit_group)
            .cloned()
            .ok_or_else(|| StorageError::not_found("commit_group", commit_group.to_string()))
    }

    pub fn commit_groups_for_seq(&self, commit_seq: CommitSeq) -> Result<Vec<CommitGroup>> {
        let inner = lock(&self.inner)?;
        let mut groups: Vec<_> = inner
            .commit_groups
            .values()
            .filter(|group| group.commit_seq == commit_seq)
            .cloned()
            .collect();
        groups.sort_by_key(|group| group.commit_group.raw());
        Ok(groups)
    }

    pub fn fork_records_for_source(&self, source: DeviceId) -> Result<Vec<ForkRecord>> {
        let inner = lock(&self.inner)?;
        let mut records: Vec<_> = inner
            .fork_records
            .values()
            .filter(|record| record.source == source)
            .cloned()
            .collect();
        records.sort_by_key(|record| record.commit_seq.raw());
        Ok(records)
    }

    pub fn fork_record(&self, commit_seq: CommitSeq) -> Result<ForkRecord> {
        let inner = lock(&self.inner)?;
        inner
            .fork_records
            .get(&commit_seq)
            .cloned()
            .ok_or_else(|| StorageError::not_found("fork_record", commit_seq.to_string()))
    }

    pub fn delete_record(&self, commit_seq: CommitSeq) -> Result<DeleteRecord> {
        let inner = lock(&self.inner)?;
        inner
            .delete_records
            .get(&commit_seq)
            .cloned()
            .ok_or_else(|| StorageError::not_found("delete_record", commit_seq.to_string()))
    }

    pub fn live_device_ids(&self) -> Result<Vec<DeviceId>> {
        let inner = lock(&self.inner)?;
        Ok(inner.device_heads.keys().copied().collect())
    }

    pub fn deleted_device_ids(&self) -> Result<Vec<DeviceId>> {
        let inner = lock(&self.inner)?;
        Ok(inner.deleted_device_heads.keys().copied().collect())
    }

    pub fn shard_commits_for_device(&self, device_id: DeviceId) -> Result<Vec<ShardCommit>> {
        let inner = lock(&self.inner)?;
        Ok(Self::shard_commits_for_device_locked(&inner, device_id))
    }

    pub fn replay_device_roots(
        &self,
        device_id: DeviceId,
        commit_seq: CommitSeq,
    ) -> Result<Vec<MetadataNodeId>> {
        let inner = lock(&self.inner)?;
        Self::replay_device_roots_locked(&inner, device_id, commit_seq, None)
    }

    pub fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        let inner = lock(&self.inner)?;
        let MappingOwner::BlockDevice(device_id) = checkpoint.owner else {
            return Err(StorageError::unsupported(
                "phase 9 checkpoint validation supports block-device checkpoints",
            ));
        };
        let replayed = match Self::replay_device_roots_locked(
            &inner,
            device_id,
            checkpoint.commit_seq,
            Some(checkpoint.checkpoint_id),
        ) {
            Ok(replayed) => replayed,
            Err(_) if checkpoint.commit_seq.raw() == 0 => {
                Self::validate_checkpoint_root_shape_locked(
                    &inner,
                    device_id,
                    &checkpoint.shard_roots,
                    self.config.shard_count,
                )?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if replayed != checkpoint.shard_roots {
            return Err(StorageError::corrupt(
                "checkpoint roots do not match replayed timeline",
            ));
        }
        Ok(())
    }

    pub fn metadata_node_count(&self) -> Result<usize> {
        Ok(lock(&self.inner)?.metadata_nodes.len())
    }

    #[cfg(test)]
    fn set_next_commit_seq_for_test(&self, next_commit_seq: u64) -> Result<()> {
        lock(&self.inner)?.next_commit_seq = next_commit_seq;
        Ok(())
    }

    pub fn allocate_metadata_node(
        &self,
        covered_range: crate::api::BlockRange,
        kind: MetadataNodeKind,
    ) -> Result<MetadataNode> {
        let mut inner = lock(&self.inner)?;
        Ok(MetadataNode {
            node_id: inner.alloc_metadata_node_id(),
            covered_range,
            kind,
        })
    }

    pub fn acquire_append_lease(&self, file_id: FileId) -> Result<AppendLease> {
        let mut inner = lock(&self.inner)?;
        let head = inner
            .file_heads
            .get(&file_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("file", file_id.to_string()))?;
        let current_epoch = inner
            .file_writer_epochs
            .get(&file_id)
            .copied()
            .unwrap_or_else(|| WriterEpoch::from_raw(0));
        let writer_epoch = current_epoch
            .raw()
            .checked_add(1)
            .map(WriterEpoch::from_raw)
            .ok_or_else(|| StorageError::conflict("writer epoch overflow"))?;
        inner.file_writer_epochs.insert(file_id, writer_epoch);
        Ok(AppendLease {
            file_id,
            lease_id: AppendLeaseId::from_raw(writer_epoch.raw() as u128),
            writer_epoch,
            base_version: head.version,
        })
    }

    pub fn validate_writer_epoch(&self, file_id: FileId, writer_epoch: WriterEpoch) -> Result<()> {
        let inner = lock(&self.inner)?;
        match inner.file_writer_epochs.get(&file_id) {
            Some(current) if *current == writer_epoch => Ok(()),
            Some(_) => Err(StorageError::conflict("stale writer epoch")),
            None => Err(StorageError::not_found("file", file_id.to_string())),
        }
    }

    fn create_empty_tree(
        inner: &mut MetadataInner,
        config: LocalStoreConfig,
        range: crate::api::BlockRange,
    ) -> Result<MetadataNode> {
        range.validate_non_empty()?;
        if range.blocks.raw() <= config.metadata_leaf_blocks {
            let node = MetadataNode {
                node_id: inner.alloc_metadata_node_id(),
                covered_range: range,
                kind: MetadataNodeKind::Leaf {
                    entries: Vec::new(),
                },
            };
            node.validate(&[])?;
            inner.metadata_nodes.insert(node.node_id, node.clone());
            return Ok(node);
        }

        let child_count = config
            .metadata_fanout
            .min(usize::try_from(range.blocks.raw()).map_err(|_| {
                StorageError::invalid_argument("metadata range block count overflows usize")
            })?);
        let range_start = range.start.raw();
        let range_blocks = range.blocks.raw();
        let child_count_u64 = u64::try_from(child_count)
            .map_err(|_| StorageError::invalid_argument("metadata fanout overflows u64"))?;
        let mut children = Vec::with_capacity(child_count);

        for child_index in 0..child_count {
            let child_index_u64 = u64::try_from(child_index)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let next_child_index_u64 = u64::try_from(child_index + 1)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let child_start_offset = range_blocks
                .checked_mul(child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?
                / child_count_u64;
            let child_end_offset = range_blocks
                .checked_mul(next_child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range end overflows"))?
                / child_count_u64;
            let child_start = range_start
                .checked_add(child_start_offset)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?;
            let child_blocks = child_end_offset - child_start_offset;
            let child_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(child_start),
                BlockCount::from_raw(child_blocks),
            );
            let child = Self::create_empty_tree(inner, config, child_range)?;
            children.push(MetadataChild {
                range: child_range,
                node_id: child.node_id,
            });
        }

        let node = MetadataNode {
            node_id: inner.alloc_metadata_node_id(),
            covered_range: range,
            kind: MetadataNodeKind::Internal { children },
        };
        node.validate(&[])?;
        inner.metadata_nodes.insert(node.node_id, node.clone());
        Ok(node)
    }

    fn next_generation(generation: DeviceGeneration) -> Result<DeviceGeneration> {
        generation
            .raw()
            .checked_add(1)
            .map(DeviceGeneration::from_raw)
            .ok_or_else(|| StorageError::conflict("device generation overflow"))
    }

    fn next_file_version(version: FileVersion) -> Result<FileVersion> {
        version
            .raw()
            .checked_add(1)
            .map(FileVersion::from_raw)
            .ok_or_else(|| StorageError::conflict("file version overflow"))
    }

    fn shard_commits_for_device_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
    ) -> Vec<ShardCommit> {
        let mut commits: Vec<_> = inner
            .shard_commits
            .iter()
            .filter(|commit| commit.device_id == device_id)
            .cloned()
            .collect();
        commits.sort_by_key(|commit| (commit.commit_seq.raw(), commit.shard_id.raw()));
        commits
    }

    fn latest_device_checkpoint_at_or_before_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Option<Checkpoint> {
        inner
            .checkpoints
            .values()
            .filter(|checkpoint| {
                checkpoint.owner == MappingOwner::BlockDevice(device_id)
                    && checkpoint.commit_seq.raw() <= commit_seq.raw()
                    && Some(checkpoint.checkpoint_id) != excluded_checkpoint
            })
            .max_by_key(|checkpoint| checkpoint.commit_seq.raw())
            .cloned()
    }

    fn replay_device_roots_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Result<Vec<MetadataNodeId>> {
        let checkpoint = Self::latest_device_checkpoint_at_or_before_locked(
            inner,
            device_id,
            commit_seq,
            excluded_checkpoint,
        )
        .ok_or_else(|| StorageError::not_found("checkpoint", device_id.to_string()))?;
        let mut roots = checkpoint.shard_roots;

        for commit in Self::shard_commits_for_device_locked(inner, device_id)
            .into_iter()
            .filter(|commit| {
                commit.commit_seq.raw() > checkpoint.commit_seq.raw()
                    && commit.commit_seq.raw() <= commit_seq.raw()
            })
        {
            let shard = usize::try_from(commit.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            if shard >= roots.len() {
                return Err(StorageError::corrupt(
                    "shard commit references shard outside root set",
                ));
            }
            if roots[shard] != commit.old_root {
                return Err(StorageError::corrupt(
                    "shard commit old_root does not match replay state",
                ));
            }
            roots[shard] = commit.new_root;
        }

        Ok(roots)
    }

    fn validate_checkpoint_root_shape_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        shard_roots: &[MetadataNodeId],
        expected_shard_count: usize,
    ) -> Result<()> {
        if !inner.device_specs.contains_key(&device_id) {
            return Err(StorageError::not_found("device", device_id.to_string()));
        }
        if shard_roots.len() != expected_shard_count {
            return Err(StorageError::corrupt(
                "checkpoint shard root count does not match device layout",
            ));
        }
        for root in shard_roots {
            if !inner.metadata_nodes.contains_key(root) {
                return Err(StorageError::not_found("metadata_node", root.to_string()));
            }
        }
        Ok(())
    }

    fn target_commit_for_restore_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        point: RestorePoint,
    ) -> Result<CommitSeq> {
        match point {
            RestorePoint::Commit(commit_seq) => {
                if Self::device_timeline_contains_commit_locked(inner, device_id, commit_seq) {
                    Ok(commit_seq)
                } else {
                    Err(StorageError::not_found("commit", commit_seq.to_string()))
                }
            }
            RestorePoint::Checkpoint(checkpoint_id) => {
                let checkpoint = inner.checkpoints.get(&checkpoint_id).ok_or_else(|| {
                    StorageError::not_found("checkpoint", checkpoint_id.to_string())
                })?;
                if checkpoint.owner != MappingOwner::BlockDevice(device_id) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source device",
                    ));
                }
                Ok(checkpoint.commit_seq)
            }
            RestorePoint::Time(time) => {
                let mut candidates: Vec<(CommitSeq, bool)> = inner
                    .checkpoints
                    .values()
                    .filter_map(|checkpoint| {
                        (checkpoint.owner == MappingOwner::BlockDevice(device_id)
                            && checkpoint.time.raw() <= time.raw())
                        .then_some((checkpoint.commit_seq, false))
                    })
                    .collect();
                candidates.extend(inner.shard_commits.iter().filter_map(|commit| {
                    (commit.device_id == device_id && commit.time.raw() <= time.raw())
                        .then_some((commit.commit_seq, false))
                }));
                candidates.extend(inner.delete_records.values().filter_map(|record| {
                    (record.device_id == device_id && record.time.raw() <= time.raw())
                        .then_some((record.commit_seq, true))
                }));
                let (commit_seq, is_delete) = candidates
                    .into_iter()
                    .max_by_key(|(seq, is_delete)| (seq.raw(), *is_delete))
                    .ok_or_else(|| StorageError::not_found("restore_time", time.to_string()))?;
                if is_delete {
                    return Err(StorageError::not_found(
                        "restore_time",
                        format!("{time} is after device deletion"),
                    ));
                }
                Ok(commit_seq)
            }
        }
    }

    fn device_timeline_contains_commit_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
    ) -> bool {
        inner.checkpoints.values().any(|checkpoint| {
            checkpoint.owner == MappingOwner::BlockDevice(device_id)
                && checkpoint.commit_seq == commit_seq
        }) || inner
            .shard_commits
            .iter()
            .any(|commit| commit.device_id == device_id && commit.commit_seq == commit_seq)
    }

    fn roots_for_gc_locked(inner: &MetadataInner, policy: RetentionPolicy) -> Vec<MetadataNodeId> {
        let mut roots = Vec::new();
        for head in inner.device_heads.values() {
            roots.extend(head.shard_roots.iter().copied());
        }
        for head in inner.file_heads.values() {
            roots.push(head.root);
        }
        for checkpoint in inner.checkpoints.values() {
            match checkpoint.owner {
                MappingOwner::BlockDevice(device_id) => {
                    if inner.device_heads.contains_key(&device_id)
                        || policy.retain_deleted_devices
                            && inner.deleted_device_heads.contains_key(&device_id)
                    {
                        roots.extend(checkpoint.shard_roots.iter().copied());
                    }
                }
                MappingOwner::NativeFile(_) => {
                    roots.extend(checkpoint.shard_roots.iter().copied());
                }
            }
        }
        if policy.retain_deleted_devices {
            for head in inner.deleted_device_heads.values() {
                roots.extend(head.shard_roots.iter().copied());
            }
            for record in inner.delete_records.values() {
                roots.extend(record.shard_roots.iter().copied());
            }
        }
        roots.sort();
        roots.dedup();
        roots
    }

    fn collect_node_segments(node: &MetadataNode, out: &mut BTreeSet<SegmentId>) {
        if let MetadataNodeKind::Leaf { entries } = &node.kind {
            for entry in entries {
                out.insert(entry.segment_id);
            }
        }
    }

    fn collect_all_segments_locked(inner: &MetadataInner) -> BTreeSet<SegmentId> {
        let mut segments = BTreeSet::new();
        for node in inner.metadata_nodes.values() {
            Self::collect_node_segments(node, &mut segments);
        }
        segments
    }

    fn collect_reachable_locked(
        inner: &MetadataInner,
        roots: &[MetadataNodeId],
    ) -> Result<(BTreeSet<MetadataNodeId>, BTreeSet<SegmentId>)> {
        let mut nodes = BTreeSet::new();
        let mut segments = BTreeSet::new();
        let mut stack: Vec<_> = roots.iter().copied().rev().collect();

        while let Some(node_id) = stack.pop() {
            if !nodes.insert(node_id) {
                continue;
            }
            let node = inner
                .metadata_nodes
                .get(&node_id)
                .ok_or_else(|| StorageError::not_found("metadata_node", node_id.to_string()))?;
            match &node.kind {
                MetadataNodeKind::Internal { children } => {
                    for child in children.iter().rev() {
                        stack.push(child.node_id);
                    }
                }
                MetadataNodeKind::Leaf { entries } => {
                    for entry in entries {
                        segments.insert(entry.segment_id);
                    }
                }
            }
        }

        Ok((nodes, segments))
    }

    pub fn mark_reachable_for_gc(&self, policy: RetentionPolicy) -> Result<MetadataMarkReport> {
        let mut inner = lock(&self.inner)?;
        let epoch = inner.alloc_gc_epoch()?;
        let roots = Self::roots_for_gc_locked(&inner, policy.clone());
        let (nodes, segments) = Self::collect_reachable_locked(&inner, &roots)?;

        for node_id in &nodes {
            inner.metadata_last_mark_epoch.insert(*node_id, epoch);
        }
        for segment_id in &segments {
            inner.segment_last_mark_epoch.insert(*segment_id, epoch);
        }

        Ok(MetadataMarkReport {
            epoch,
            roots,
            metadata_nodes: nodes.into_iter().collect(),
            segments: segments.into_iter().collect(),
        })
    }

    pub fn sweep_unmarked_after_mark(
        &self,
        policy: RetentionPolicy,
        epoch: u64,
    ) -> Result<MetadataSweepReport> {
        if epoch == 0 {
            return Err(StorageError::invalid_argument(
                "GC epoch must be greater than zero",
            ));
        }

        let mut inner = lock(&self.inner)?;
        if epoch >= inner.next_gc_epoch {
            return Err(StorageError::invalid_argument("unknown GC epoch"));
        }

        let roots = Self::roots_for_gc_locked(&inner, policy.clone());
        let (currently_reachable_nodes, currently_reachable_segments) =
            Self::collect_reachable_locked(&inner, &roots)?;
        let all_segments = Self::collect_all_segments_locked(&inner);
        let mut deleted_metadata_nodes = Vec::new();

        let candidate_nodes: Vec<_> = inner
            .metadata_nodes
            .keys()
            .copied()
            .filter(|node_id| {
                inner.metadata_last_mark_epoch.get(node_id).copied() != Some(epoch)
                    && !currently_reachable_nodes.contains(node_id)
            })
            .collect();
        for node_id in candidate_nodes {
            inner.metadata_nodes.remove(&node_id);
            inner.metadata_last_mark_epoch.remove(&node_id);
            deleted_metadata_nodes.push(node_id);
        }

        let mut released_segments: Vec<_> = all_segments
            .into_iter()
            .filter(|segment_id| {
                inner.segment_last_mark_epoch.get(segment_id).copied() != Some(epoch)
                    && !currently_reachable_segments.contains(segment_id)
            })
            .collect();
        released_segments.sort();
        released_segments.dedup();
        deleted_metadata_nodes.sort();

        if !policy.retain_deleted_devices {
            let expired_devices: BTreeSet<_> = inner.deleted_device_heads.keys().copied().collect();
            for device_id in &expired_devices {
                inner.deleted_device_heads.remove(device_id);
                inner.device_specs.remove(device_id);
            }
            inner
                .delete_records
                .retain(|_, record| !expired_devices.contains(&record.device_id));
            inner
                .checkpoints
                .retain(|_, checkpoint| match checkpoint.owner {
                    MappingOwner::BlockDevice(device_id) => !expired_devices.contains(&device_id),
                    MappingOwner::NativeFile(_) => true,
                });
            inner
                .shard_commits
                .retain(|commit| !expired_devices.contains(&commit.device_id));
            inner.fork_records.retain(|_, record| {
                !expired_devices.contains(&record.source)
                    && !expired_devices.contains(&record.target)
            });
        }

        Ok(MetadataSweepReport {
            epoch,
            deleted_metadata_nodes,
            released_segments,
        })
    }

    pub fn last_mark_epoch_for_node(&self, node_id: MetadataNodeId) -> Result<Option<u64>> {
        let inner = lock(&self.inner)?;
        Ok(inner.metadata_last_mark_epoch.get(&node_id).copied())
    }

    pub fn last_mark_epoch_for_segment(&self, segment_id: SegmentId) -> Result<Option<u64>> {
        let inner = lock(&self.inner)?;
        Ok(inner.segment_last_mark_epoch.get(&segment_id).copied())
    }
}

impl MetadataPlane for InMemoryMetadataPlane {
    fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead> {
        self.config.validate()?;
        request.spec.validate()?;

        let shard_count = u64::try_from(self.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard_count overflows u64"))?;
        if request.spec.logical_blocks < shard_count {
            return Err(StorageError::invalid_argument(
                "logical_blocks must be at least shard_count",
            ));
        }

        let mut inner = lock(&self.inner)?;
        let device_id = inner.alloc_device_id();
        let mut shard_roots = Vec::with_capacity(self.config.shard_count);

        for shard in 0..self.config.shard_count {
            let shard = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = request
                .spec
                .logical_blocks
                .checked_mul(shard)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count;
            let end = request
                .spec
                .logical_blocks
                .checked_mul(shard + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count;
            let node = Self::create_empty_tree(
                &mut inner,
                self.config,
                crate::api::BlockRange::new(
                    BlockIndex::from_raw(start),
                    BlockCount::from_raw(end - start),
                ),
            )?;
            shard_roots.push(node.node_id);
        }

        let head = DeviceHead {
            device_id,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit: CommitSeq::from_raw(0),
        };
        head.validate(self.config.shard_count)?;

        inner.device_specs.insert(device_id, request.spec);
        inner.device_heads.insert(device_id, head.clone());
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(device_id),
            head.latest_commit,
            head.shard_roots.clone(),
        );
        Ok(head)
    }

    fn create_file(&self, request: MetadataCreateFileRequest) -> Result<FileHead> {
        self.config.validate()?;
        let mut inner = lock(&self.inner)?;
        let file_id = inner.alloc_file_id();
        let root = Self::create_empty_tree(
            &mut inner,
            self.config,
            crate::api::BlockRange::new(
                BlockIndex::from_raw(0),
                BlockCount::from_raw(self.config.file_root_blocks),
            ),
        )?;
        let head = FileHead {
            file_id,
            version: FileVersion::from_raw(0),
            root: root.node_id,
            size: 0,
            latest_commit: CommitSeq::from_raw(0),
        };
        head.validate_current(root.covered_range, self.config.block_size)?;

        inner.file_specs.insert(file_id, request.request.spec);
        inner.file_heads.insert(file_id, head.clone());
        inner
            .file_writer_epochs
            .insert(file_id, WriterEpoch::from_raw(0));
        Ok(head)
    }

    fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead> {
        let inner = lock(&self.inner)?;
        inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))
    }

    fn list_live_devices(&self) -> Result<Vec<DeviceId>> {
        self.live_device_ids()
    }

    fn list_deleted_devices(&self) -> Result<Vec<DeviceId>> {
        self.deleted_device_ids()
    }

    fn get_file_head(&self, file_id: FileId) -> Result<FileHead> {
        let inner = lock(&self.inner)?;
        inner
            .file_heads
            .get(&file_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("file", file_id.to_string()))
    }

    fn get_file_info(&self, file_id: FileId) -> Result<FileInfo> {
        let head = self.get_file_head(file_id)?;
        Ok(FileInfo {
            file_id,
            size: head.size,
            version: head.version,
        })
    }

    fn persist_metadata_node(&self, node: MetadataNode) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        match inner.metadata_nodes.get(&node.node_id) {
            Some(existing) if existing == &node => Ok(()),
            Some(_) => Err(StorageError::conflict(
                "metadata node ID already exists with different content",
            )),
            None => {
                inner.metadata_nodes.insert(node.node_id, node);
                Ok(())
            }
        }
    }

    fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode> {
        let inner = lock(&self.inner)?;
        inner
            .metadata_nodes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("metadata_node", node_id.to_string()))
    }

    fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup> {
        let mut inner = lock(&self.inner)?;

        match intent.owner {
            MappingOwner::BlockDevice(device_id) => {
                let current = inner
                    .device_heads
                    .get(&device_id)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
                match intent.fence {
                    MetadataFence::DeviceGeneration(generation)
                        if generation == current.generation => {}
                    MetadataFence::DeviceGeneration(_) => {
                        return Err(StorageError::conflict("stale device generation fence"));
                    }
                    _ => {
                        return Err(StorageError::invalid_argument(
                            "block device commit requires device-generation fence",
                        ));
                    }
                }

                if intent.updates.is_empty() {
                    return Err(StorageError::invalid_argument(
                        "commit group must include at least one root update",
                    ));
                }

                let mut next_roots = current.shard_roots.clone();
                let mut shard_commits = Vec::with_capacity(intent.updates.len());
                for update in &intent.updates {
                    let RootUpdate::BlockShard(update) = update else {
                        return Err(StorageError::invalid_argument(
                            "block device commit cannot include file-root updates",
                        ));
                    };
                    let shard = usize::try_from(update.shard_id.raw())
                        .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
                    if shard >= next_roots.len() {
                        return Err(StorageError::invalid_argument(
                            "shard update is outside device root set",
                        ));
                    }
                    if next_roots[shard] != update.old_root {
                        return Err(StorageError::conflict("stale shard root"));
                    }
                    if !inner.metadata_nodes.contains_key(&update.new_root) {
                        return Err(StorageError::not_found(
                            "metadata_node",
                            update.new_root.to_string(),
                        ));
                    }
                    shard_commits.push((update.shard_id, update.old_root, update.new_root));
                    next_roots[shard] = update.new_root;
                }

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group_id = inner.alloc_commit_group_id();
                let commit_group = CommitGroup {
                    commit_group: commit_group_id,
                    commit_seq,
                    owner: intent.owner,
                    updates: intent.updates,
                };
                for (shard_id, old_root, new_root) in shard_commits {
                    inner.shard_commits.push(ShardCommit {
                        commit_seq,
                        commit_group: commit_group_id,
                        time: LogicalTime::from_raw(commit_seq.raw()),
                        device_id,
                        shard_id,
                        old_root,
                        new_root,
                    });
                }
                let mut next_head = current.clone();
                next_head.generation = Self::next_generation(next_head.generation)?;
                next_head.latest_commit = commit_seq;
                next_head.shard_roots = next_roots;
                inner.device_heads.insert(device_id, next_head);
                inner
                    .commit_groups
                    .insert(commit_group.commit_group, commit_group.clone());
                Ok(commit_group)
            }
            MappingOwner::NativeFile(file_id) => {
                let current = inner
                    .file_heads
                    .get(&file_id)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("file", file_id.to_string()))?;
                match intent.fence {
                    MetadataFence::FileVersion(version) if version == current.version => {}
                    MetadataFence::FileVersion(_) => {
                        return Err(StorageError::conflict("stale file version fence"));
                    }
                    MetadataFence::WriterEpoch {
                        base_version,
                        writer_epoch,
                    } if base_version == current.version
                        && Some(&writer_epoch) == inner.file_writer_epochs.get(&file_id) => {}
                    MetadataFence::WriterEpoch { .. } => {
                        return Err(StorageError::conflict("stale writer epoch fence"));
                    }
                    _ => {
                        return Err(StorageError::invalid_argument(
                            "native file commit requires file-version or writer-epoch fence",
                        ));
                    }
                }

                if intent.updates.len() != 1 {
                    return Err(StorageError::invalid_argument(
                        "native file commit must include exactly one file-root update",
                    ));
                }

                let (old_root, new_root, new_size) = match intent.updates.as_slice() {
                    [
                        RootUpdate::FileRoot {
                            old_root,
                            new_root,
                            new_size,
                        },
                    ] => (*old_root, *new_root, *new_size),
                    [_] => {
                        return Err(StorageError::invalid_argument(
                            "native file commit cannot include shard-root updates",
                        ));
                    }
                    _ => unreachable!("length checked above"),
                };
                if current.root != old_root {
                    return Err(StorageError::conflict("stale file root"));
                }
                if !inner.metadata_nodes.contains_key(&new_root) {
                    return Err(StorageError::not_found(
                        "metadata_node",
                        new_root.to_string(),
                    ));
                }
                let new_root_node =
                    inner
                        .metadata_nodes
                        .get(&new_root)
                        .cloned()
                        .ok_or_else(|| {
                            StorageError::not_found("metadata_node", new_root.to_string())
                        })?;

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group = CommitGroup {
                    commit_group: inner.alloc_commit_group_id(),
                    commit_seq,
                    owner: intent.owner,
                    updates: vec![RootUpdate::FileRoot {
                        old_root,
                        new_root,
                        new_size,
                    }],
                };
                let mut next_head = current.clone();
                next_head.version = Self::next_file_version(next_head.version)?;
                next_head.latest_commit = commit_seq;
                next_head.root = new_root;
                next_head.size = new_size;
                next_head.validate_transition_from(
                    &current,
                    new_root_node.covered_range,
                    self.config.block_size,
                )?;
                inner.file_heads.insert(file_id, next_head);
                inner
                    .commit_groups
                    .insert(commit_group.commit_group, commit_group.clone());
                Ok(commit_group)
            }
        }
    }

    fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead> {
        let mut inner = lock(&self.inner)?;
        let source_head = inner
            .device_heads
            .get(&request.source)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", request.source.to_string()))?;
        let source_spec = inner
            .device_specs
            .get(&request.source)
            .cloned()
            .ok_or_else(|| StorageError::corrupt("source device head exists without spec"))?;
        let target = match request.target {
            Some(target) => {
                if inner.device_heads.contains_key(&target)
                    || inner.deleted_device_heads.contains_key(&target)
                {
                    return Err(StorageError::conflict("target device already exists"));
                }
                inner.reserve_device_id_at_least_after(target)?;
                target
            }
            None => inner.alloc_device_id(),
        };
        let latest_commit = inner.alloc_commit_seq()?;
        let shard_roots = source_head.shard_roots.clone();
        let head = DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: shard_roots.clone(),
            latest_commit,
        };
        head.validate(self.config.shard_count)?;
        let record = ForkRecord {
            commit_seq: latest_commit,
            source: request.source,
            target,
            shard_roots,
        };
        inner.device_specs.insert(target, source_spec);
        inner.device_heads.insert(target, head.clone());
        inner.fork_records.insert(latest_commit, record);
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(target),
            latest_commit,
            head.shard_roots.clone(),
        );
        Ok(head)
    }

    fn restore_device(
        &self,
        source: DeviceId,
        point: crate::api::RestorePoint,
    ) -> Result<DeviceHead> {
        let mut inner = lock(&self.inner)?;
        let source_spec = inner
            .device_specs
            .get(&source)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", source.to_string()))?;
        let target_commit = Self::target_commit_for_restore_locked(&inner, source, point)?;
        let shard_roots = Self::replay_device_roots_locked(&inner, source, target_commit, None)?;
        let target = inner.alloc_device_id();
        let latest_commit = inner.alloc_commit_seq()?;
        let head = DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit,
        };
        head.validate(self.config.shard_count)?;
        inner.device_specs.insert(target, source_spec);
        inner.device_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(target),
            latest_commit,
            head.shard_roots.clone(),
        );
        Ok(head)
    }

    fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        let mut inner = lock(&self.inner)?;
        let mut head = inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        let commit_seq = inner.alloc_commit_seq()?;
        inner.device_heads.remove(&device_id);
        head.latest_commit = commit_seq;
        let record = DeleteRecord {
            commit_seq,
            time: LogicalTime::from_raw(commit_seq.raw()),
            device_id,
            shard_roots: head.shard_roots.clone(),
        };
        inner.deleted_device_heads.insert(device_id, head);
        inner.delete_records.insert(commit_seq, record);
        Ok(DeleteResult {
            device_id,
            commit_seq,
        })
    }

    fn get_delete_record(&self, commit_seq: CommitSeq) -> Result<DeleteRecord> {
        self.delete_record(commit_seq)
    }

    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        let mut inner = lock(&self.inner)?;
        let head = inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        Ok(inner.insert_checkpoint(
            MappingOwner::BlockDevice(device_id),
            head.latest_commit,
            head.shard_roots,
        ))
    }

    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint> {
        let inner = lock(&self.inner)?;
        inner
            .checkpoints
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("checkpoint", checkpoint_id.to_string()))
    }

    fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        let inner = lock(&self.inner)?;
        Ok(Self::roots_for_gc_locked(&inner, policy))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentRecord {
    bytes: Vec<u8>,
    synced: bool,
    commit: SegmentReplicaCommit,
}

#[derive(Debug)]
struct SegmentStoreInner {
    next_offset: u64,
    segments: BTreeMap<SegmentId, SegmentRecord>,
}

/// In-memory implementation of `SegmentStore`.
#[derive(Debug)]
pub struct InMemorySegmentStore {
    config: LocalStoreConfig,
    inner: Mutex<SegmentStoreInner>,
}

impl InMemorySegmentStore {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(SegmentStoreInner {
                next_offset: 0,
                segments: BTreeMap::new(),
            }),
        })
    }

    pub fn is_synced(&self, segment_id: SegmentId) -> Result<bool> {
        let inner = lock(&self.inner)?;
        inner
            .segments
            .get(&segment_id)
            .map(|record| record.synced)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn contains_segment(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(lock(&self.inner)?.segments.contains_key(&segment_id))
    }
}

impl SegmentStore for InMemorySegmentStore {
    fn write_segment(
        &self,
        reservation: &SegmentReservation,
        bytes: &[u8],
    ) -> Result<SegmentReplicaCommit> {
        self.config.validate()?;

        if bytes.is_empty() {
            return Err(StorageError::invalid_argument(
                "segment write must contain bytes",
            ));
        }

        let bytes_len = u64::try_from(bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment write length overflows u64"))?;
        if reservation.bytes != bytes_len {
            return Err(StorageError::invalid_argument(
                "reservation byte count does not match write length",
            ));
        }

        if bytes_len % u64::from(self.config.block_size) != 0 {
            return Err(StorageError::invalid_argument(
                "segment write length must be block aligned",
            ));
        }

        let mut inner = lock(&self.inner)?;
        if let Some(existing) = inner.segments.get(&reservation.segment_id) {
            if existing.bytes == bytes {
                return Ok(existing.commit.clone());
            }
            return Err(StorageError::conflict(
                "segment ID already exists with different bytes",
            ));
        }

        let offset = inner.next_offset;
        inner.next_offset = inner
            .next_offset
            .checked_add(reservation.bytes)
            .ok_or_else(|| StorageError::conflict("local segment offset overflow"))?;
        let blocks = reservation.bytes / u64::from(self.config.block_size);
        let commit = SegmentReplicaCommit {
            descriptor: SegmentDescriptor {
                segment_id: reservation.segment_id,
                blocks: BlockCount::from_raw(blocks),
                bytes: reservation.bytes,
                checksum: Some(checksum64(bytes)),
            },
            placement: SegmentReplicaPlacement {
                segment_id: reservation.segment_id,
                storage_node: self.config.storage_node,
                offset,
                bytes: reservation.bytes,
            },
        };
        inner.segments.insert(
            reservation.segment_id,
            SegmentRecord {
                bytes: bytes.to_vec(),
                synced: false,
                commit: commit.clone(),
            },
        );
        Ok(commit)
    }

    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let inner = lock(&self.inner)?;
        let record = inner
            .segments
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        if !record.synced {
            return Err(StorageError::unavailable("segment is not synced"));
        }
        let end = range.end_exclusive()?;
        let record_len = u64::try_from(record.bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment byte length overflows u64"))?;
        if end > record_len {
            return Err(StorageError::invalid_argument(
                "segment read extends past end of segment",
            ));
        }
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        let start = usize::try_from(range.offset)
            .map_err(|_| StorageError::invalid_argument("segment read offset overflows usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| StorageError::invalid_argument("segment read end overflows usize"))?;
        let source = record
            .bytes
            .get(start..end)
            .ok_or_else(|| StorageError::corrupt("segment read range exceeds segment bytes"))?;
        buf.copy_from_slice(source);
        Ok(())
    }

    fn sync_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let record = inner
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        record.synced = true;
        Ok(())
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        lock(&self.inner)?.segments.remove(&segment_id);
        Ok(())
    }
}

/// Local segment lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentLifecycleState {
    Reserved,
    Writing,
    DurablePendingMetadata,
    Referenced,
    Released,
    Freed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CatalogEntry {
    intent: SegmentReservationIntent,
    reservation: SegmentReservation,
    state: SegmentLifecycleState,
    commit: Option<SegmentReplicaCommit>,
}

#[derive(Debug)]
struct CatalogInner {
    next_segment_id: u128,
    entries: BTreeMap<SegmentId, CatalogEntry>,
}

/// In-memory implementation of `LocalSegmentCatalog`.
#[derive(Debug)]
pub struct InMemoryLocalSegmentCatalog {
    config: LocalStoreConfig,
    inner: Mutex<CatalogInner>,
}

impl InMemoryLocalSegmentCatalog {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            }),
        })
    }

    pub fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.state)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .commit
            .clone()
            .ok_or_else(|| StorageError::unavailable("segment has no durable commit"))
    }

    pub fn intent_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReservationIntent> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.intent.clone())
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn entries(&self) -> Result<Vec<(SegmentId, SegmentLifecycleState, WriteIntentId)>> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .entries
            .iter()
            .map(|(segment_id, entry)| (*segment_id, entry.state, entry.intent.write_intent))
            .collect())
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

        let mut inner = lock(&self.inner)?;
        let segment_id = SegmentId::from_raw(inner.next_segment_id);
        inner.next_segment_id += 1;
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
                commit: None,
            },
        );
        Ok(reservation)
    }

    fn begin_write(&self, reservation: &SegmentReservation) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != *reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Writing;
                Ok(())
            }
            SegmentLifecycleState::Writing => Ok(()),
            _ => Err(StorageError::conflict(
                "segment write can only begin from Reserved state",
            )),
        }
    }

    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        commit: SegmentReplicaCommit,
    ) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        if commit.descriptor.segment_id != reservation.segment_id
            || commit.placement.segment_id != reservation.segment_id
        {
            return Err(StorageError::invalid_argument(
                "segment commit IDs must match reservation",
            ));
        }
        if commit.placement.storage_node != self.config.storage_node {
            return Err(StorageError::invalid_argument(
                "segment commit storage node does not match local catalog",
            ));
        }
        if commit.descriptor.bytes != reservation.bytes
            || commit.placement.bytes != reservation.bytes
        {
            return Err(StorageError::invalid_argument(
                "segment commit bytes must match reservation",
            ));
        }

        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.commit = Some(commit);
                entry.state = SegmentLifecycleState::DurablePendingMetadata;
                Ok(())
            }
            SegmentLifecycleState::DurablePendingMetadata
                if entry.commit.as_ref() == Some(&commit) =>
            {
                Ok(())
            }
            _ => Err(StorageError::conflict(
                "segment commit requires Writing state",
            )),
        }
    }

    fn mark_segment_referenced(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Referenced;
                Ok(())
            }
            SegmentLifecycleState::Referenced => Ok(()),
            _ => Err(StorageError::conflict(
                "segment can be referenced only from DurablePendingMetadata state",
            )),
        }
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
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
        let mut inner = lock(&self.inner)?;
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
        let mut inner = lock(&self.inner)?;
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
        let mut inner = lock(&self.inner)?;
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
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata
            | SegmentLifecycleState::Referenced
            | SegmentLifecycleState::Released => entry
                .commit
                .as_ref()
                .map(|commit| commit.placement.clone())
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
        let mut inner = lock(&self.inner)?;
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
}

/// Local block request coordinator.
#[derive(Debug, Clone)]
pub struct LocalBlockServer {
    store: LocalObjectStore,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    serial: Arc<Mutex<()>>,
}

impl LocalBlockServer {
    pub fn new(store: LocalObjectStore) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            serial: Arc::new(Mutex::new(())),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }
}

impl BlockServer for LocalBlockServer {
    fn handle(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let _serial_guard = lock(&self.serial)?;
        lock(&self.request_log)?.push(request.request_id);
        let response = match request.request {
            BlockRequest::Create { request } => {
                let head = self
                    .store
                    .metadata
                    .create_device(MetadataCreateDeviceRequest::from(request))?;
                BlockResponse::Created(head.device_id)
            }
            BlockRequest::Info { device_id } => {
                BlockResponse::Info(self.store.metadata.device_info(device_id)?)
            }
            BlockRequest::Read { device_id, range } => {
                let len = usize::try_from(range.len).map_err(|_| {
                    StorageError::invalid_argument("read byte length overflows usize")
                })?;
                let mut bytes = vec![0; len];
                self.store.read_device(device_id, range, &mut bytes)?;
                BlockResponse::Read(ReadResponse { bytes })
            }
            BlockRequest::Write {
                device_id,
                offset,
                bytes,
                durability,
            } => BlockResponse::Write(
                self.store
                    .write_device(device_id, offset, &bytes, durability)?,
            ),
            BlockRequest::WriteZeroes { device_id, range } => BlockResponse::Write(
                self.store
                    .write_zeroes(device_id, range.offset, range.len)?,
            ),
            BlockRequest::Discard { device_id, range } => BlockResponse::Write(
                self.store
                    .discard_device(device_id, range.offset, range.len)?,
            ),
            BlockRequest::Flush { device_id, .. } => {
                let info = self.store.metadata.device_info(device_id)?;
                BlockResponse::Flush(FlushResult {
                    device_id,
                    durable_through: info.latest_commit,
                })
            }
            BlockRequest::Fork { source, request } => {
                BlockResponse::Forked(self.store.fork_device(source, request)?)
            }
            BlockRequest::Restore { source, point } => {
                BlockResponse::Restored(self.store.restore_device(source, point)?)
            }
            BlockRequest::Delete { device_id } => {
                BlockResponse::Deleted(self.store.delete_device(device_id)?)
            }
        };
        Ok(BlockResponseEnvelope {
            request_id: request.request_id,
            response,
        })
    }
}

/// Local native-file request coordinator.
#[derive(Debug, Clone)]
pub struct LocalNativeServer {
    store: LocalObjectStore,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    serial: Arc<Mutex<()>>,
}

impl LocalNativeServer {
    pub fn new(store: LocalObjectStore) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            serial: Arc::new(Mutex::new(())),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }
}

impl NativeServer for LocalNativeServer {
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let _serial_guard = lock(&self.serial)?;
        lock(&self.request_log)?.push(request.request_id);
        let response = match request.request {
            NativeRequest::CreateFile { request } => {
                let head = self
                    .store
                    .metadata
                    .create_file(MetadataCreateFileRequest::from(request))?;
                NativeResponse::FileCreated(head.file_id)
            }
            NativeRequest::FileInfo { file_id } => {
                NativeResponse::FileInfo(self.store.metadata.get_file_info(file_id)?)
            }
            NativeRequest::Read { file_id, range } => {
                let len = usize::try_from(range.len).map_err(|_| {
                    StorageError::invalid_argument("read byte length overflows usize")
                })?;
                let mut bytes = vec![0; len];
                self.store.read_file(file_id, range, &mut bytes)?;
                NativeResponse::Read(ReadResponse { bytes })
            }
            NativeRequest::AcquireAppend { file_id } => {
                NativeResponse::AppendLease(self.store.acquire_append_lease(file_id)?)
            }
            NativeRequest::Append {
                file_id,
                lease,
                bytes,
                durability,
            } => {
                if file_id != lease.file_id {
                    return Err(StorageError::invalid_argument(
                        "append lease file_id does not match request file_id",
                    ));
                }
                NativeResponse::Append(self.store.append_file(lease, &bytes, durability)?)
            }
            NativeRequest::Flush { file_id } => {
                let info = self.store.metadata.get_file_info(file_id)?;
                NativeResponse::Flush(FlushResult {
                    device_id: DeviceId::from_raw(info.file_id.raw()),
                    durable_through: CommitSeq::from_raw(info.version.raw()),
                })
            }
        };
        Ok(NativeResponseEnvelope {
            request_id: request.request_id,
            response,
        })
    }
}

/// In-process block transport.
#[derive(Clone)]
pub struct InProcessBlockTransport {
    server: Arc<dyn BlockServer>,
}

impl InProcessBlockTransport {
    pub fn new(server: Arc<dyn BlockServer>) -> Self {
        Self { server }
    }
}

impl BlockTransport for InProcessBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        self.server.handle(request)
    }
}

/// In-process native-file transport.
#[derive(Clone)]
pub struct InProcessNativeTransport {
    server: Arc<dyn NativeServer>,
}

impl InProcessNativeTransport {
    pub fn new(server: Arc<dyn NativeServer>) -> Self {
        Self { server }
    }
}

impl NativeTransport for InProcessNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        self.server.handle(request)
    }
}

/// Local `BlockClient` backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockClient {
    transport: InProcessBlockTransport,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockClient {
    pub fn new(transport: InProcessBlockTransport) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_device(&self, device_id: DeviceId) -> Result<LocalBlockDevice> {
        self.device_info(device_id)?;
        Ok(LocalBlockDevice {
            device_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockClient for LocalBlockClient {
    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Create { request },
        ))?;
        match response.response {
            BlockResponse::Created(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected create-device response")),
        }
    }

    fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info { device_id },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }
}

/// Local `BlockDevice` handle backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockDevice {
    device_id: DeviceId,
    transport: InProcessBlockTransport,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockDevice {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockDevice for LocalBlockDevice {
    fn device_id(&self) -> DeviceId {
        self.device_id
    }

    fn info(&self) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Read {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected block-read response")),
        }
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Write {
                device_id: self.device_id,
                offset,
                bytes: data.to_vec(),
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected block-write response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Flush {
                device_id: self.device_id,
                scope: crate::api::FlushScope::Device,
            },
        ))?;
        match response.response {
            BlockResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected block-flush response")),
        }
    }

    fn write_zeroes(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::WriteZeroes {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected write-zeroes response")),
        }
    }

    fn discard(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Discard {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected discard response")),
        }
    }

    fn fork(&self, request: ForkRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Fork {
                source: self.device_id,
                request,
            },
        ))?;
        match response.response {
            BlockResponse::Forked(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected fork response")),
        }
    }

    fn restore(&self, point: RestorePoint) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Restore {
                source: self.device_id,
                point,
            },
        ))?;
        match response.response {
            BlockResponse::Restored(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected restore response")),
        }
    }

    fn delete(&self) -> Result<DeleteResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Delete {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Deleted(delete) => Ok(delete),
            _ => Err(StorageError::corrupt("unexpected block-delete response")),
        }
    }
}

/// Local `NativeFileClient` backed by a native-file transport.
#[derive(Clone)]
pub struct LocalNativeFileClient {
    transport: InProcessNativeTransport,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeFileClient {
    pub fn new(transport: InProcessNativeTransport) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_file(&self, file_id: FileId) -> Result<LocalNativeFile> {
        self.file_info(file_id)?;
        Ok(LocalNativeFile {
            file_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeFileClient for LocalNativeFileClient {
    fn create_file(&self, request: CreateFileRequest) -> Result<FileId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CreateFile { request },
        ))?;
        match response.response {
            NativeResponse::FileCreated(file_id) => Ok(file_id),
            _ => Err(StorageError::corrupt("unexpected create-file response")),
        }
    }

    fn file_info(&self, file_id: FileId) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo { file_id },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn acquire_append(&self, file_id: FileId) -> Result<AppendLease> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::AcquireAppend { file_id },
        ))?;
        match response.response {
            NativeResponse::AppendLease(lease) => Ok(lease),
            _ => Err(StorageError::corrupt("unexpected append-lease response")),
        }
    }
}

/// Local `NativeFile` handle backed by a native-file transport.
#[derive(Clone)]
pub struct LocalNativeFile {
    file_id: FileId,
    transport: InProcessNativeTransport,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeFile {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeFile for LocalNativeFile {
    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn info(&self) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo {
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Read {
                file_id: self.file_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            NativeResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected native-read response")),
        }
    }

    fn acquire_append(&self) -> Result<AppendLease> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::AcquireAppend {
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::AppendLease(lease) => Ok(lease),
            _ => Err(StorageError::corrupt("unexpected append-lease response")),
        }
    }

    fn append_with_lease(&self, lease: AppendLease, data: &[u8]) -> Result<AppendCommit> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Append {
                file_id: self.file_id,
                lease,
                bytes: data.to_vec(),
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            NativeResponse::Append(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected native-append response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Flush {
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected native-flush response")),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))
}

fn next_request_id(next: &Mutex<u128>) -> Result<RequestId> {
    let mut next = lock(next)?;
    let request_id = RequestId::from_raw(*next);
    *next = next
        .checked_add(1)
        .ok_or_else(|| StorageError::conflict("request id overflow"))?;
    Ok(request_id)
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn replace_leaf_entries(
    entries: &[LeafEntry],
    covered_range: crate::api::BlockRange,
    replacement_range: crate::api::BlockRange,
    replacement: Option<LeafEntry>,
) -> Result<Vec<LeafEntry>> {
    replacement_range.validate_non_empty()?;
    if !covered_range.contains_range(replacement_range)? {
        return Err(StorageError::invalid_argument(
            "replacement range is outside leaf coverage",
        ));
    }

    let mut out = Vec::with_capacity(entries.len() + usize::from(replacement.is_some()));
    let replacement_end = replacement_range.end_exclusive()?.raw();

    for entry in entries {
        let entry_range = entry.logical_range();
        let entry_end = entry_range.end_exclusive()?.raw();
        if !entry_range.overlaps(replacement_range)? {
            out.push(entry.clone());
            continue;
        }

        if entry.logical_start.raw() < replacement_range.start.raw() {
            out.push(LeafEntry {
                logical_start: entry.logical_start,
                blocks: BlockCount::from_raw(
                    replacement_range.start.raw() - entry.logical_start.raw(),
                ),
                segment_id: entry.segment_id,
                segment_offset: entry.segment_offset,
            });
        }

        if entry_end > replacement_end {
            let skipped_blocks = replacement_end - entry.logical_start.raw();
            let segment_offset = entry
                .segment_offset
                .raw()
                .checked_add(skipped_blocks)
                .ok_or_else(|| StorageError::invalid_argument("leaf segment offset overflows"))?;
            out.push(LeafEntry {
                logical_start: BlockIndex::from_raw(replacement_end),
                blocks: BlockCount::from_raw(entry_end - replacement_end),
                segment_id: entry.segment_id,
                segment_offset: BlockIndex::from_raw(segment_offset),
            });
        }
    }

    if let Some(replacement) = replacement {
        out.push(replacement);
    }
    out.sort_by_key(|entry| entry.logical_start.raw());
    coalesce_leaf_entries(out)
}

fn coalesce_leaf_entries(entries: Vec<LeafEntry>) -> Result<Vec<LeafEntry>> {
    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(previous) = out.last_mut()
            && previous.segment_id == entry.segment_id
            && previous.logical_range().end_exclusive()? == entry.logical_start
            && previous.segment_end_exclusive()? == entry.segment_offset
        {
            previous.blocks = BlockCount::from_raw(
                previous
                    .blocks
                    .raw()
                    .checked_add(entry.blocks.raw())
                    .ok_or_else(|| StorageError::invalid_argument("leaf entry size overflows"))?,
            );
            continue;
        }
        out.push(entry);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{BlockRequest, CreateDeviceRequest, DeviceSpec, FlushScope, WriteDurability};
    use crate::extent::{CreateFileRequest, FileSpec};
    use crate::id::{ClientEpoch, LogicalDeadline, ShardId, WriteIntentId};
    use crate::object::{LeafEntry, ShardRootUpdate};

    fn config() -> LocalStoreConfig {
        LocalStoreConfig {
            shard_count: 2,
            block_size: 4096,
            file_root_blocks: 8,
            metadata_fanout: 2,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(77),
        }
    }

    fn tree_config() -> LocalStoreConfig {
        LocalStoreConfig {
            metadata_fanout: 2,
            metadata_leaf_blocks: 2,
            file_root_blocks: 32,
            ..config()
        }
    }

    fn device_request() -> MetadataCreateDeviceRequest {
        MetadataCreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: Some("root".to_string()),
        }
    }

    fn metadata_leaf(node_id: u128, start: u64, blocks: u64) -> MetadataNode {
        MetadataNode {
            node_id: MetadataNodeId::from_raw(node_id),
            covered_range: crate::api::BlockRange::new(
                BlockIndex::from_raw(start),
                BlockCount::from_raw(blocks),
            ),
            kind: MetadataNodeKind::Leaf {
                entries: Vec::new(),
            },
        }
    }

    fn reservation_intent() -> SegmentReservationIntent {
        SegmentReservationIntent {
            write_intent: WriteIntentId::from_raw(1),
            owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
            bytes: 4096,
        }
    }

    #[test]
    fn metadata_nodes_are_immutable_and_missing_lookup_errors() {
        let metadata = InMemoryMetadataPlane::new(config()).unwrap();
        let node = metadata_leaf(999, 0, 4);

        metadata.persist_metadata_node(node.clone()).unwrap();
        assert_eq!(metadata.get_metadata_node(node.node_id).unwrap(), node);
        metadata.persist_metadata_node(node.clone()).unwrap();

        let changed = MetadataNode {
            covered_range: crate::api::BlockRange::new(
                BlockIndex::from_raw(4),
                BlockCount::from_raw(4),
            ),
            ..node.clone()
        };
        assert!(metadata.persist_metadata_node(changed).is_err());
        assert!(
            metadata
                .get_metadata_node(MetadataNodeId::from_raw(1000))
                .is_err()
        );
    }

    #[test]
    fn metadata_publish_is_fenced_atomic_and_checks_missing_roots() {
        let metadata = InMemoryMetadataPlane::new(config()).unwrap();
        let head = metadata.create_device(device_request()).unwrap();
        let new_node = metadata_leaf(999, 0, 8);
        metadata.persist_metadata_node(new_node.clone()).unwrap();

        let stale_missing = CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: MetadataNodeId::from_raw(404),
            })],
        };
        assert!(metadata.publish_commit_group(stale_missing).is_err());
        assert_eq!(metadata.get_head(head.device_id).unwrap(), head);

        let commit = metadata
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: new_node.node_id,
                })],
            })
            .unwrap();
        assert_eq!(commit.commit_seq, CommitSeq::from_raw(1));

        let updated = metadata.get_head(head.device_id).unwrap();
        assert_eq!(updated.shard_roots[0], new_node.node_id);
        assert_eq!(updated.generation, DeviceGeneration::from_raw(1));

        let stale = CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(1),
                old_root: head.shard_roots[1],
                new_root: new_node.node_id,
            })],
        };
        assert!(metadata.publish_commit_group(stale).is_err());
        assert_eq!(metadata.get_head(head.device_id).unwrap(), updated);
    }

    #[test]
    fn file_commit_uses_version_fence_and_roots_for_gc_include_live_owners() {
        let metadata = InMemoryMetadataPlane::new(config()).unwrap();
        let file = metadata
            .create_file(MetadataCreateFileRequest {
                request: CreateFileRequest {
                    spec: FileSpec {
                        name: Some("log".to_string()),
                    },
                },
            })
            .unwrap();
        let new_root = metadata_leaf(1001, 0, 8);
        metadata.persist_metadata_node(new_root.clone()).unwrap();

        metadata
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::NativeFile(file.file_id),
                fence: MetadataFence::FileVersion(file.version),
                updates: vec![RootUpdate::FileRoot {
                    old_root: file.root,
                    new_root: new_root.node_id,
                    new_size: 0,
                }],
            })
            .unwrap();

        let updated = metadata.get_file_head(file.file_id).unwrap();
        assert_eq!(updated.root, new_root.node_id);
        assert_eq!(updated.version, FileVersion::from_raw(1));

        let roots = metadata
            .roots_for_gc(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();
        assert!(roots.contains(&new_root.node_id));
    }

    #[test]
    fn delete_moves_device_out_of_live_catalog_without_deleting_objects() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 16);
        let device_id = device.device_id();
        device.write_at(0, &[7; 4096]).unwrap();
        let head_before_delete = store.metadata().get_head(device_id).unwrap();
        let node_count_before_delete = store.metadata().metadata_node_count().unwrap();
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Referenced
        );

        let delete = device.delete().unwrap();

        assert_eq!(delete.device_id, device_id);
        assert!(delete.commit_seq.raw() > head_before_delete.latest_commit.raw());
        assert_eq!(store.metadata().list_live_devices().unwrap(), Vec::new());
        assert_eq!(
            store.metadata().list_deleted_devices().unwrap(),
            vec![device_id]
        );
        assert!(store.metadata().get_head(device_id).is_err());
        assert!(device.info().is_err());
        assert!(device.read_at(0, &mut [0; 4096]).is_err());
        assert!(device.write_at(0, &[8; 4096]).is_err());
        assert!(device.delete().is_err());
        assert_eq!(
            store
                .metadata()
                .delete_record(delete.commit_seq)
                .unwrap()
                .shard_roots,
            head_before_delete.shard_roots
        );
        assert_eq!(
            store.metadata().metadata_node_count().unwrap(),
            node_count_before_delete
        );
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Referenced
        );
    }

    #[test]
    fn failed_delete_publish_preserves_live_head() {
        let metadata = InMemoryMetadataPlane::new(config()).unwrap();
        let head = metadata.create_device(device_request()).unwrap();
        metadata.set_next_commit_seq_for_test(u64::MAX).unwrap();

        assert!(metadata.delete_device(head.device_id).is_err());
        assert_eq!(metadata.get_head(head.device_id).unwrap(), head);
        assert_eq!(metadata.list_live_devices().unwrap(), vec![head.device_id]);
        assert_eq!(metadata.list_deleted_devices().unwrap(), Vec::new());
    }

    #[test]
    fn roots_for_gc_respects_deleted_device_retention_policy() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 16);
        let device_id = device.device_id();
        device.write_at(0, &[7; 4096]).unwrap();
        let checkpoint_id = store.metadata().checkpoint(device_id).unwrap();
        device.write_at(4096, &[8; 4096]).unwrap();
        let delete = device.delete().unwrap();
        let checkpoint = store.metadata().get_checkpoint(checkpoint_id).unwrap();
        let delete_record = store.metadata().delete_record(delete.commit_seq).unwrap();

        let without_retention = store
            .metadata()
            .roots_for_gc(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();
        assert!(without_retention.is_empty());

        let with_retention = store
            .metadata()
            .roots_for_gc(RetentionPolicy {
                retain_deleted_devices: true,
            })
            .unwrap();
        let mut sorted = with_retention.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(with_retention, sorted);
        for root in checkpoint.shard_roots {
            assert!(with_retention.contains(&root));
        }
        for root in delete_record.shard_roots {
            assert!(with_retention.contains(&root));
        }
    }

    #[test]
    fn generated_delete_retention_roots_match_reference_model() {
        fn expected_roots(
            live_roots: &BTreeMap<DeviceId, Vec<MetadataNodeId>>,
            deleted_roots: &BTreeMap<DeviceId, Vec<MetadataNodeId>>,
            checkpoint_roots: &[(DeviceId, Vec<MetadataNodeId>)],
            retain_deleted: bool,
        ) -> Vec<MetadataNodeId> {
            let mut roots = Vec::new();
            for roots_for_device in live_roots.values() {
                roots.extend(roots_for_device.iter().copied());
            }
            for (device_id, roots_for_checkpoint) in checkpoint_roots {
                if live_roots.contains_key(device_id)
                    || retain_deleted && deleted_roots.contains_key(device_id)
                {
                    roots.extend(roots_for_checkpoint.iter().copied());
                }
            }
            if retain_deleted {
                for roots_for_device in deleted_roots.values() {
                    roots.extend(roots_for_device.iter().copied());
                }
            }
            roots.sort();
            roots.dedup();
            roots
        }

        for seed in 0..10 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(config()).unwrap();
            let server = Arc::new(LocalBlockServer::new(store.clone()));
            let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
            let mut live_roots: BTreeMap<DeviceId, Vec<MetadataNodeId>> = BTreeMap::new();
            let mut deleted_roots: BTreeMap<DeviceId, Vec<MetadataNodeId>> = BTreeMap::new();
            let mut checkpoint_roots: Vec<(DeviceId, Vec<MetadataNodeId>)> = Vec::new();

            for create_index in 0..3 {
                let device_id = client
                    .create_device(CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: Some(format!("seed-{seed}-{create_index}")),
                    })
                    .unwrap();
                let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
                live_roots.insert(device_id, roots.clone());
                checkpoint_roots.push((device_id, roots));
            }

            for step in 0..30 {
                if live_roots.is_empty() {
                    let device_id = client
                        .create_device(CreateDeviceRequest {
                            spec: DeviceSpec {
                                logical_blocks: 16,
                                block_size: 4096,
                            },
                            name: Some(format!("seed-{seed}-recreate-{step}")),
                        })
                        .unwrap();
                    let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
                    harness
                        .trace
                        .record(format!("create step={step} device={device_id}"));
                    live_roots.insert(device_id, roots.clone());
                    checkpoint_roots.push((device_id, roots));
                }

                let live_ids: Vec<_> = live_roots.keys().copied().collect();
                let device_id = live_ids[harness.rng.choose_index(live_ids.len()).unwrap()];
                match harness.rng.next_u64() % 4 {
                    0 => {
                        let block = harness.rng.next_u64() % 16;
                        let byte = (1 + harness.rng.next_u64() % 254) as u8;
                        harness.trace.record(format!(
                            "write step={step} device={device_id} block={block} byte={byte}"
                        ));
                        client
                            .open_device(device_id)
                            .unwrap()
                            .write_at(block * 4096, &[byte; 4096])
                            .unwrap();
                        let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
                        live_roots.insert(device_id, roots);
                    }
                    1 => {
                        harness
                            .trace
                            .record(format!("checkpoint step={step} device={device_id}"));
                        let checkpoint = store.metadata().checkpoint(device_id).unwrap();
                        checkpoint_roots.push((
                            device_id,
                            store
                                .metadata()
                                .get_checkpoint(checkpoint)
                                .unwrap()
                                .shard_roots,
                        ));
                    }
                    2 if live_roots.len() + deleted_roots.len() < 8 => {
                        harness
                            .trace
                            .record(format!("fork step={step} source={device_id}"));
                        let child = client
                            .open_device(device_id)
                            .unwrap()
                            .fork(ForkRequest {
                                target: None,
                                name: Some(format!("fork-{seed}-{step}")),
                            })
                            .unwrap();
                        let roots = store.metadata().get_head(child).unwrap().shard_roots;
                        live_roots.insert(child, roots.clone());
                        checkpoint_roots.push((child, roots));
                    }
                    _ => {
                        harness
                            .trace
                            .record(format!("delete step={step} device={device_id}"));
                        let roots = live_roots.remove(&device_id).unwrap();
                        let delete = client.open_device(device_id).unwrap().delete().unwrap();
                        assert_eq!(
                            store
                                .metadata()
                                .delete_record(delete.commit_seq)
                                .unwrap()
                                .shard_roots,
                            roots
                        );
                        deleted_roots.insert(device_id, roots);
                    }
                }

                assert_eq!(
                    store.metadata().list_live_devices().unwrap(),
                    live_roots.keys().copied().collect::<Vec<_>>(),
                    "seed={seed} trace={:?}",
                    harness.trace.events()
                );
                assert_eq!(
                    store.metadata().list_deleted_devices().unwrap(),
                    deleted_roots.keys().copied().collect::<Vec<_>>(),
                    "seed={seed} trace={:?}",
                    harness.trace.events()
                );
                assert_eq!(
                    store
                        .metadata()
                        .roots_for_gc(RetentionPolicy {
                            retain_deleted_devices: false,
                        })
                        .unwrap(),
                    expected_roots(&live_roots, &deleted_roots, &checkpoint_roots, false),
                    "seed={seed} trace={:?}",
                    harness.trace.events()
                );
                assert_eq!(
                    store
                        .metadata()
                        .roots_for_gc(RetentionPolicy {
                            retain_deleted_devices: true,
                        })
                        .unwrap(),
                    expected_roots(&live_roots, &deleted_roots, &checkpoint_roots, true),
                    "seed={seed} trace={:?}",
                    harness.trace.events()
                );
            }
        }
    }

    #[test]
    fn deleted_device_can_restore_from_retained_checkpoint_but_not_after_delete_time() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();
        device.write_at(0, &[3; 4096]).unwrap();
        let checkpoint = store.metadata().checkpoint(device_id).unwrap();
        device.write_at(0, &[4; 4096]).unwrap();
        let delete = device.delete().unwrap();

        let restored_id = device
            .restore(RestorePoint::Checkpoint(checkpoint))
            .expect("checkpoint roots are retained before GC");
        let restored = client.open_device(restored_id).unwrap();
        let mut bytes = [0; 4096];
        restored.read_at(0, &mut bytes).unwrap();
        assert_eq!(bytes, [3; 4096]);

        assert!(
            store
                .metadata()
                .restore_device(
                    device_id,
                    RestorePoint::Time(LogicalTime::from_raw(delete.commit_seq.raw()))
                )
                .is_err()
        );
    }

    #[test]
    fn metadata_gc_releases_deleted_device_segments_after_retention_expires() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 16);
        let device_id = device.device_id();
        device.write_at(0, &[7; 4096]).unwrap();
        device.delete().unwrap();

        let report = store
            .run_metadata_custodian(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();

        assert!(!report.sweep.deleted_metadata_nodes.is_empty());
        assert_eq!(report.sweep.released_segments, vec![SegmentId::from_raw(1)]);
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Released
        );
        assert!(
            store
                .segment_store()
                .contains_segment(SegmentId::from_raw(1))
                .unwrap()
        );

        let storage_report = store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
        assert_eq!(
            storage_report.deleted_released_segments,
            vec![SegmentId::from_raw(1)]
        );
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Freed
        );
        assert!(
            !store
                .segment_store()
                .contains_segment(SegmentId::from_raw(1))
                .unwrap()
        );
        assert!(store.metadata().get_head(device_id).is_err());
    }

    #[test]
    fn retention_expiring_gc_prunes_deleted_pitr_catalog() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 16);
        let device_id = device.device_id();
        device.write_at(0, &[7; 4096]).unwrap();
        let checkpoint = store.metadata().checkpoint(device_id).unwrap();
        device.delete().unwrap();

        store
            .run_metadata_custodian(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();

        assert_eq!(store.metadata().list_deleted_devices().unwrap(), Vec::new());
        assert!(
            store
                .metadata()
                .roots_for_gc(RetentionPolicy {
                    retain_deleted_devices: true,
                })
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
                .is_err()
        );
    }

    #[test]
    fn metadata_gc_retains_deleted_pitr_roots_when_policy_requires_it() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();
        device.write_at(0, &[9; 4096]).unwrap();
        let checkpoint = store.metadata().checkpoint(device_id).unwrap();
        device.delete().unwrap();

        let report = store
            .run_metadata_custodian(RetentionPolicy {
                retain_deleted_devices: true,
            })
            .unwrap();

        assert!(report.sweep.released_segments.is_empty());
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Referenced
        );
        let restored_id = store
            .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
            .unwrap();
        let restored = client.open_device(restored_id).unwrap();
        let mut bytes = [0; 4096];
        restored.read_at(0, &mut bytes).unwrap();
        assert_eq!(bytes, [9; 4096]);
    }

    #[test]
    fn paused_gc_sweep_preserves_nodes_marked_in_epoch() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 16);
        device.write_at(0, &[5; 4096]).unwrap();
        let mark = store
            .mark_reachable_for_gc(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();
        assert!(mark.metadata_nodes.iter().all(|node| {
            store.metadata().last_mark_epoch_for_node(*node).unwrap() == Some(mark.epoch)
        }));
        assert_eq!(
            store
                .metadata()
                .last_mark_epoch_for_segment(SegmentId::from_raw(1))
                .unwrap(),
            Some(mark.epoch)
        );

        device.delete().unwrap();
        let first_sweep = store
            .sweep_metadata_after_mark(
                RetentionPolicy {
                    retain_deleted_devices: false,
                },
                mark.epoch,
            )
            .unwrap();
        assert!(first_sweep.deleted_metadata_nodes.is_empty());
        assert!(first_sweep.released_segments.is_empty());
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Referenced
        );

        let second = store
            .run_metadata_custodian(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();
        assert!(!second.sweep.deleted_metadata_nodes.is_empty());
        assert_eq!(second.sweep.released_segments, vec![SegmentId::from_raw(1)]);
    }

    #[test]
    fn generated_gc_interleavings_preserve_live_device_models() {
        fn assert_live_models(
            store: &LocalObjectStore,
            client: &LocalBlockClient,
            models: &BTreeMap<DeviceId, Vec<u8>>,
            seed: u64,
            trace: &[String],
        ) {
            for (device_id, model) in models {
                let device = client.open_device(*device_id).unwrap();
                let mut actual = vec![0; model.len() * 4096];
                device.read_at(0, &mut actual).unwrap();
                assert_model_blocks(
                    &actual,
                    model,
                    seed,
                    trace,
                    &render_device_roots(store, *device_id),
                );
                validate_device_roots(store, *device_id);
            }
        }

        for seed in 0..8 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..tree_config()
            })
            .unwrap();
            let server = Arc::new(LocalBlockServer::new(store.clone()));
            let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
            let root = client
                .create_device(CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 16,
                        block_size: 4096,
                    },
                    name: None,
                })
                .unwrap();
            let mut models = BTreeMap::from([(root, vec![0u8; 16])]);
            let mut deleted = BTreeSet::new();

            for step in 0..36 {
                let paused_gc = harness.rng.next_u64() % 3 == 0;
                let policy = RetentionPolicy {
                    retain_deleted_devices: harness.rng.next_u64() % 2 == 0,
                };
                let paused_mark = if paused_gc {
                    let mark = store.mark_reachable_for_gc(policy.clone()).unwrap();
                    harness.trace.record(format!(
                        "mark step={step} epoch={} retain_deleted={}",
                        mark.epoch, policy.retain_deleted_devices
                    ));
                    Some(mark)
                } else {
                    None
                };

                if models.is_empty() {
                    let device_id = client
                        .create_device(CreateDeviceRequest {
                            spec: DeviceSpec {
                                logical_blocks: 16,
                                block_size: 4096,
                            },
                            name: Some(format!("recreated-{seed}-{step}")),
                        })
                        .unwrap();
                    harness
                        .trace
                        .record(format!("create step={step} device={device_id}"));
                    models.insert(device_id, vec![0u8; 16]);
                }

                let device_ids: Vec<_> = models.keys().copied().collect();
                let device_id = device_ids[harness.rng.choose_index(device_ids.len()).unwrap()];
                match harness.rng.next_u64() % 4 {
                    0 => {
                        let block = harness.rng.next_u64() % 16;
                        let byte = (1 + harness.rng.next_u64() % 254) as u8;
                        harness.trace.record(format!(
                            "write step={step} device={device_id} block={block} byte={byte}"
                        ));
                        client
                            .open_device(device_id)
                            .unwrap()
                            .write_at(block * 4096, &[byte; 4096])
                            .unwrap();
                        models.get_mut(&device_id).unwrap()[block as usize] = byte;
                    }
                    1 if models.len() < 6 => {
                        let child = client
                            .open_device(device_id)
                            .unwrap()
                            .fork(ForkRequest {
                                target: None,
                                name: Some(format!("gc-child-{seed}-{step}")),
                            })
                            .unwrap();
                        harness
                            .trace
                            .record(format!("fork step={step} source={device_id} child={child}"));
                        models.insert(child, models.get(&device_id).unwrap().clone());
                    }
                    2 => {
                        harness
                            .trace
                            .record(format!("checkpoint step={step} device={device_id}"));
                        store.metadata().checkpoint(device_id).unwrap();
                    }
                    _ => {
                        harness
                            .trace
                            .record(format!("delete step={step} device={device_id}"));
                        client.open_device(device_id).unwrap().delete().unwrap();
                        models.remove(&device_id);
                        deleted.insert(device_id);
                    }
                }

                if let Some(mark) = paused_mark {
                    let sweep = store.sweep_metadata_after_mark(policy, mark.epoch).unwrap();
                    harness.trace.record(format!(
                        "sweep step={step} epoch={} deleted_nodes={} released_segments={}",
                        sweep.epoch,
                        sweep.deleted_metadata_nodes.len(),
                        sweep.released_segments.len()
                    ));
                } else if harness.rng.next_u64() % 2 == 0 {
                    let report = store.run_metadata_custodian(policy).unwrap();
                    harness.trace.record(format!(
                        "gc step={step} epoch={} deleted_nodes={} released_segments={}",
                        report.mark.epoch,
                        report.sweep.deleted_metadata_nodes.len(),
                        report.sweep.released_segments.len()
                    ));
                }
                store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
                assert_live_models(&store, &client, &models, seed, harness.trace.events());
                for device_id in &deleted {
                    assert!(store.metadata().get_head(*device_id).is_err());
                }
            }
        }
    }

    #[test]
    fn generated_end_to_end_simulator_is_replayable_across_operations_and_faults() {
        fn graph_summary(
            store: &LocalObjectStore,
            native_file_count: usize,
        ) -> crate::sim::ObjectGraphSummary {
            let entries = store.segment_catalog().entries().unwrap();
            crate::sim::ObjectGraphSummary {
                live_devices: store.metadata().list_live_devices().unwrap().len(),
                deleted_devices: store.metadata().list_deleted_devices().unwrap().len(),
                native_files: native_file_count,
                metadata_nodes: store.metadata().metadata_node_count().unwrap(),
                gc_roots: store
                    .metadata()
                    .roots_for_gc(RetentionPolicy {
                        retain_deleted_devices: true,
                    })
                    .unwrap()
                    .len(),
                referenced_segments: entries
                    .iter()
                    .filter(|(_, state, _)| *state == SegmentLifecycleState::Referenced)
                    .count(),
                released_segments: entries
                    .iter()
                    .filter(|(_, state, _)| *state == SegmentLifecycleState::Released)
                    .count(),
                freed_segments: entries
                    .iter()
                    .filter(|(_, state, _)| *state == SegmentLifecycleState::Freed)
                    .count(),
            }
        }

        fn validate_live_devices(
            store: &LocalObjectStore,
            client: &LocalBlockClient,
            seed: u64,
            trace: &[String],
            models: &BTreeMap<DeviceId, Vec<u8>>,
        ) {
            for (device_id, model) in models {
                let device = client.open_device(*device_id).unwrap();
                let mut actual = vec![0; model.len() * 4096];
                device.read_at(0, &mut actual).unwrap();
                assert_model_blocks(
                    &actual,
                    model,
                    seed,
                    trace,
                    &render_device_roots(store, *device_id),
                );
            }
        }

        fn run(seed: u64) -> crate::sim::FailureArtifact {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let faults = crate::sim::FaultInjector::new(seed ^ 0x0051_ab1e);
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..tree_config()
            })
            .unwrap();
            let block_server = Arc::new(LocalBlockServer::new(store.clone()));
            let block_client = LocalBlockClient::new(InProcessBlockTransport::new(block_server));
            let native_server = Arc::new(LocalNativeServer::new(store.clone()));
            let native_client =
                LocalNativeFileClient::new(InProcessNativeTransport::new(native_server));
            let mut device_models: BTreeMap<DeviceId, Vec<u8>> = BTreeMap::new();
            let mut deleted_devices = BTreeSet::new();
            let mut checkpoints: Vec<(DeviceId, CheckpointId, Vec<u8>)> = Vec::new();
            let mut file_models: BTreeMap<FileId, Vec<u8>> = BTreeMap::new();
            let mut expired_intents = BTreeSet::new();

            for step in 0..48 {
                let fault_kind = match step % 8 {
                    0 => crate::sim::FaultKind::PublishConflict,
                    1 => crate::sim::FaultKind::DuplicateEffect,
                    2 => crate::sim::FaultKind::DelayedEffect,
                    3 => crate::sim::FaultKind::MissingObject,
                    4 => crate::sim::FaultKind::WriteIntentExpiry,
                    5 => crate::sim::FaultKind::OrphanSegment,
                    6 => crate::sim::FaultKind::MissedAsyncFree,
                    _ => crate::sim::FaultKind::CrashReplayBoundary,
                };
                if step < 8 || faults.should_inject(step, fault_kind) {
                    match fault_kind {
                        crate::sim::FaultKind::PublishConflict => {
                            let file_id = if let Some(file_id) = file_models.keys().next().copied()
                            {
                                file_id
                            } else {
                                let file_id = native_client
                                    .create_file(CreateFileRequest {
                                        spec: FileSpec { name: None },
                                    })
                                    .unwrap();
                                file_models.insert(file_id, Vec::new());
                                file_id
                            };
                            let file = native_client.open_file(file_id).unwrap();
                            let stale = file.acquire_append().unwrap();
                            let fresh = file.acquire_append().unwrap();
                            assert!(
                                file.append_with_lease(stale, &repeated_blocks(1, 1))
                                    .is_err()
                            );
                            file.append_with_lease(fresh, &repeated_blocks(1, 2))
                                .unwrap();
                            file_models.get_mut(&file_id).unwrap().push(2);
                            harness
                                .trace
                                .record(format!("fault publish_conflict step={step}"));
                        }
                        crate::sim::FaultKind::DuplicateEffect => {
                            let reservation = SegmentReservation {
                                segment_id: SegmentId::from_raw(90_000 + u128::from(step)),
                                bytes: 4096,
                            };
                            let first = store
                                .segment_store()
                                .write_segment(&reservation, &[8; 4096])
                                .unwrap();
                            let second = store
                                .segment_store()
                                .write_segment(&reservation, &[8; 4096])
                                .unwrap();
                            assert_eq!(first, second);
                            harness
                                .trace
                                .record(format!("fault duplicate_effect step={step}"));
                        }
                        crate::sim::FaultKind::DelayedEffect => {
                            let policy = RetentionPolicy {
                                retain_deleted_devices: harness.rng.next_u64() % 2 == 0,
                            };
                            let mark = store.mark_reachable_for_gc(policy.clone()).unwrap();
                            harness.trace.record(format!(
                                "fault delayed_mark step={step} epoch={}",
                                mark.epoch
                            ));
                            store.sweep_metadata_after_mark(policy, mark.epoch).unwrap();
                        }
                        crate::sim::FaultKind::MissingObject => {
                            assert!(
                                store
                                    .metadata()
                                    .get_metadata_node(MetadataNodeId::from_raw(999_999))
                                    .is_err()
                            );
                            harness
                                .trace
                                .record(format!("fault missing_object step={step}"));
                        }
                        crate::sim::FaultKind::WriteIntentExpiry => {
                            store.run_storage_node_custodian(&expired_intents).unwrap();
                            harness
                                .trace
                                .record(format!("fault write_intent_expiry step={step}"));
                        }
                        crate::sim::FaultKind::OrphanSegment => {
                            let owner = device_models
                                .keys()
                                .next()
                                .copied()
                                .map(MappingOwner::BlockDevice)
                                .unwrap_or_else(|| {
                                    MappingOwner::BlockDevice(DeviceId::from_raw(1))
                                });
                            let reservation =
                                store.write_segment_for_owner(owner, &[6; 4096]).unwrap();
                            let intent = store
                                .segment_catalog()
                                .intent_for_segment(reservation.segment_id)
                                .unwrap()
                                .write_intent;
                            expired_intents.insert(intent);
                            harness.trace.record(format!(
                                "fault orphan_segment step={step} segment={}",
                                reservation.segment_id
                            ));
                        }
                        crate::sim::FaultKind::MissedAsyncFree => {
                            store
                                .run_metadata_custodian(RetentionPolicy {
                                    retain_deleted_devices: false,
                                })
                                .unwrap();
                            store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
                            harness
                                .trace
                                .record(format!("fault missed_async_free step={step}"));
                        }
                        crate::sim::FaultKind::CrashReplayBoundary => {
                            validate_live_devices(
                                &store,
                                &block_client,
                                seed,
                                harness.trace.events(),
                                &device_models,
                            );
                            harness
                                .trace
                                .record(format!("fault crash_replay_boundary step={step}"));
                        }
                    }
                }

                match harness.rng.next_u64() % 8 {
                    0 | 1 if device_models.is_empty() => {
                        let device_id = block_client
                            .create_device(CreateDeviceRequest {
                                spec: DeviceSpec {
                                    logical_blocks: 16,
                                    block_size: 4096,
                                },
                                name: Some(format!("sim-{seed}-{step}")),
                            })
                            .unwrap();
                        device_models.insert(device_id, vec![0; 16]);
                        harness
                            .trace
                            .record(format!("create step={step} device={device_id}"));
                    }
                    0 => {
                        let device_id = *device_models.keys().next().unwrap();
                        let block = harness.rng.next_u64() % 16;
                        let byte = (1 + harness.rng.next_u64() % 254) as u8;
                        block_client
                            .open_device(device_id)
                            .unwrap()
                            .write_at(block * 4096, &[byte; 4096])
                            .unwrap();
                        device_models.get_mut(&device_id).unwrap()[block as usize] = byte;
                        harness.trace.record(format!(
                            "write step={step} device={device_id} block={block} byte={byte}"
                        ));
                    }
                    1 if device_models.len() < 6 => {
                        let source = *device_models.keys().next().unwrap();
                        let child = block_client
                            .open_device(source)
                            .unwrap()
                            .fork(ForkRequest {
                                target: None,
                                name: Some(format!("sim-child-{seed}-{step}")),
                            })
                            .unwrap();
                        device_models.insert(child, device_models.get(&source).unwrap().clone());
                        harness
                            .trace
                            .record(format!("fork step={step} source={source} child={child}"));
                    }
                    2 if !device_models.is_empty() => {
                        let device_id = *device_models.keys().next().unwrap();
                        let checkpoint = store.metadata().checkpoint(device_id).unwrap();
                        checkpoints.push((
                            device_id,
                            checkpoint,
                            device_models.get(&device_id).unwrap().clone(),
                        ));
                        harness
                            .trace
                            .record(format!("checkpoint step={step} device={device_id}"));
                    }
                    3 if !checkpoints.is_empty() => {
                        let index = harness.rng.choose_index(checkpoints.len()).unwrap();
                        let (source, checkpoint, model) = checkpoints[index].clone();
                        if let Ok(restored) =
                            store.restore_device(source, RestorePoint::Checkpoint(checkpoint))
                        {
                            device_models.insert(restored, model);
                            harness.trace.record(format!(
                                "restore step={step} source={source} restored={restored}"
                            ));
                        } else {
                            harness
                                .trace
                                .record(format!("restore_expired step={step} source={source}"));
                        }
                    }
                    4 if !device_models.is_empty() => {
                        let device_id = *device_models.keys().next().unwrap();
                        block_client
                            .open_device(device_id)
                            .unwrap()
                            .delete()
                            .unwrap();
                        device_models.remove(&device_id);
                        deleted_devices.insert(device_id);
                        harness
                            .trace
                            .record(format!("delete step={step} device={device_id}"));
                    }
                    5 => {
                        let file_id = native_client
                            .create_file(CreateFileRequest {
                                spec: FileSpec { name: None },
                            })
                            .unwrap();
                        file_models.insert(file_id, Vec::new());
                        harness
                            .trace
                            .record(format!("create_file step={step} file={file_id}"));
                    }
                    6 if !file_models.is_empty() => {
                        let file_id = *file_models.keys().next().unwrap();
                        let file = native_client.open_file(file_id).unwrap();
                        let lease = file.acquire_append().unwrap();
                        let byte = (1 + harness.rng.next_u64() % 254) as u8;
                        file.append_with_lease(lease, &[byte; 4096]).unwrap();
                        file_models.get_mut(&file_id).unwrap().push(byte);
                        harness
                            .trace
                            .record(format!("append step={step} file={file_id} byte={byte}"));
                    }
                    _ => {
                        store
                            .run_metadata_custodian(RetentionPolicy {
                                retain_deleted_devices: harness.rng.next_u64() % 2 == 0,
                            })
                            .unwrap();
                        store.run_storage_node_custodian(&expired_intents).unwrap();
                        harness.trace.record(format!("gc step={step}"));
                    }
                }

                validate_live_devices(
                    &store,
                    &block_client,
                    seed,
                    harness.trace.events(),
                    &device_models,
                );
                for (file_id, model) in &file_models {
                    let file = native_client.open_file(*file_id).unwrap();
                    let mut actual = vec![0; model.len() * 4096];
                    file.read_at(0, &mut actual).unwrap();
                    assert_model_blocks(
                        &actual,
                        model,
                        seed,
                        harness.trace.events(),
                        "native file",
                    );
                }
                for device_id in &deleted_devices {
                    assert!(store.metadata().get_head(*device_id).is_err());
                }
            }

            crate::sim::FailureArtifact::new(
                seed,
                harness.trace.events(),
                graph_summary(&store, file_models.len()),
            )
        }

        for seed in 0..10 {
            assert_eq!(run(seed), run(seed));
        }
    }

    #[test]
    fn storage_node_custodian_reclaims_expired_failed_orphan_and_released_segments() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let reserved = store
            .segment_catalog()
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(10),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
            .unwrap();
        let writing = store
            .segment_catalog()
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(11),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
            .unwrap();
        store.segment_catalog().begin_write(&writing).unwrap();
        let orphan = store
            .segment_catalog()
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(12),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
            .unwrap();
        store.segment_catalog().begin_write(&orphan).unwrap();
        let orphan_commit = store
            .segment_store()
            .write_segment(&orphan, &[3; 4096])
            .unwrap();
        store
            .segment_store()
            .sync_segment(orphan.segment_id)
            .unwrap();
        store
            .segment_catalog()
            .commit_segment(orphan.clone(), orphan_commit)
            .unwrap();
        let referenced = store
            .write_segment_for_owner(MappingOwner::BlockDevice(DeviceId::from_raw(1)), &[4; 4096])
            .unwrap();
        store
            .segment_catalog()
            .mark_segment_referenced(referenced.segment_id)
            .unwrap();
        store
            .segment_catalog()
            .release_segment(referenced.segment_id)
            .unwrap();

        let untouched = store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
        assert!(untouched.expired_reservations.is_empty());
        assert!(untouched.failed_writes.is_empty());
        assert!(untouched.orphan_segments.is_empty());
        assert_eq!(
            untouched.deleted_released_segments,
            vec![referenced.segment_id]
        );
        assert_eq!(
            store.segment_catalog().state(orphan.segment_id).unwrap(),
            SegmentLifecycleState::DurablePendingMetadata
        );

        let expired = BTreeSet::from([
            WriteIntentId::from_raw(10),
            WriteIntentId::from_raw(11),
            WriteIntentId::from_raw(12),
        ]);
        let report = store.run_storage_node_custodian(&expired).unwrap();
        assert_eq!(report.expired_reservations, vec![reserved.segment_id]);
        assert_eq!(report.failed_writes, vec![writing.segment_id]);
        assert_eq!(report.orphan_segments, vec![orphan.segment_id]);
        assert_eq!(
            store.segment_catalog().state(reserved.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );
        assert_eq!(
            store.segment_catalog().state(writing.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );
        assert_eq!(
            store.segment_catalog().state(orphan.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );
        assert!(
            !store
                .segment_store()
                .contains_segment(orphan.segment_id)
                .unwrap()
        );
    }

    #[test]
    fn segment_store_is_immutable_idempotent_and_reports_missing_objects() {
        let store = InMemorySegmentStore::new(config()).unwrap();
        let reservation = SegmentReservation {
            segment_id: SegmentId::from_raw(7),
            bytes: 4096,
        };
        let bytes = vec![11; 4096];
        let commit = store.write_segment(&reservation, &bytes).unwrap();
        assert_eq!(commit.descriptor.segment_id, reservation.segment_id);
        assert!(!store.is_synced(reservation.segment_id).unwrap());

        assert_eq!(store.write_segment(&reservation, &bytes).unwrap(), commit);
        assert!(store.write_segment(&reservation, &[12; 4096]).is_err());
        assert!(
            store
                .read_segment(reservation.segment_id, ByteRange::new(0, 1), &mut [0])
                .is_err()
        );

        store.sync_segment(reservation.segment_id).unwrap();
        assert!(store.is_synced(reservation.segment_id).unwrap());

        let mut out = vec![0; 16];
        store
            .read_segment(reservation.segment_id, ByteRange::new(8, 16), &mut out)
            .unwrap();
        assert_eq!(out, vec![11; 16]);
        assert!(
            store
                .read_segment(SegmentId::from_raw(404), ByteRange::new(0, 1), &mut [0])
                .is_err()
        );
    }

    #[test]
    fn local_catalog_lifecycle_rejects_invalid_state_jumps() {
        let catalog = InMemoryLocalSegmentCatalog::new(config()).unwrap();
        let store = InMemorySegmentStore::new(config()).unwrap();
        let reservation = catalog.reserve_segment(reservation_intent()).unwrap();

        assert_eq!(
            catalog.state(reservation.segment_id).unwrap(),
            SegmentLifecycleState::Reserved
        );
        assert!(
            catalog
                .commit_segment(
                    reservation.clone(),
                    SegmentReplicaCommit {
                        descriptor: SegmentDescriptor {
                            segment_id: reservation.segment_id,
                            blocks: BlockCount::from_raw(1),
                            bytes: 4096,
                            checksum: None,
                        },
                        placement: SegmentReplicaPlacement {
                            segment_id: reservation.segment_id,
                            storage_node: config().storage_node,
                            offset: 0,
                            bytes: 4096,
                        },
                    },
                )
                .is_err()
        );

        catalog.begin_write(&reservation).unwrap();
        let commit = store.write_segment(&reservation, &[1; 4096]).unwrap();
        store.sync_segment(reservation.segment_id).unwrap();
        catalog
            .commit_segment(reservation.clone(), commit.clone())
            .unwrap();
        catalog
            .commit_segment(reservation.clone(), commit.clone())
            .unwrap();
        assert_eq!(
            catalog.state(reservation.segment_id).unwrap(),
            SegmentLifecycleState::DurablePendingMetadata
        );
        assert_eq!(
            catalog.locate_segment(reservation.segment_id).unwrap(),
            commit.placement
        );

        catalog
            .mark_segment_referenced(reservation.segment_id)
            .unwrap();
        catalog.release_segment(reservation.segment_id).unwrap();
        catalog.delete_segment(reservation.segment_id).unwrap();
        assert_eq!(
            catalog.state(reservation.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );
        assert!(catalog.locate_segment(reservation.segment_id).is_err());
    }

    #[test]
    fn local_catalog_reconciles_expired_reservations_and_failed_writes() {
        let catalog = InMemoryLocalSegmentCatalog::new(config()).unwrap();

        let expired = catalog.reserve_segment(reservation_intent()).unwrap();
        catalog.expire_reservation(expired.segment_id).unwrap();
        assert_eq!(
            catalog.state(expired.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );

        let failed = catalog.reserve_segment(reservation_intent()).unwrap();
        catalog.begin_write(&failed).unwrap();
        catalog.fail_write(failed.segment_id).unwrap();
        assert_eq!(
            catalog.state(failed.segment_id).unwrap(),
            SegmentLifecycleState::Freed
        );

        let invalid = catalog.reserve_segment(reservation_intent()).unwrap();
        assert!(catalog.release_segment(invalid.segment_id).is_err());
        assert!(catalog.delete_segment(invalid.segment_id).is_err());
    }

    #[test]
    fn local_transports_preserve_request_identity_and_order() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let block_server = Arc::new(LocalBlockServer::new(store.clone()));
        let block_transport = InProcessBlockTransport::new(block_server.clone());
        let create = BlockRequestEnvelope::new(
            RequestId::from_raw(1),
            ClientEpoch::from_raw(1),
            Some(LogicalDeadline::from_raw(100)),
            BlockRequest::Create {
                request: CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 16,
                        block_size: 4096,
                    },
                    name: None,
                },
            },
        );
        let created = block_transport.call(create).unwrap();
        assert_eq!(created.request_id, RequestId::from_raw(1));
        let device_id = match created.response {
            BlockResponse::Created(device_id) => device_id,
            _ => panic!("unexpected block response"),
        };
        let info = block_transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(2),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .unwrap();
        assert_eq!(info.request_id, RequestId::from_raw(2));
        assert_eq!(
            block_server.request_log().unwrap(),
            vec![RequestId::from_raw(1), RequestId::from_raw(2)]
        );

        let native_server = Arc::new(LocalNativeServer::new(store));
        let native_transport = InProcessNativeTransport::new(native_server.clone());
        let created = native_transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(3),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::CreateFile {
                    request: CreateFileRequest {
                        spec: FileSpec { name: None },
                    },
                },
            ))
            .unwrap();
        assert_eq!(created.request_id, RequestId::from_raw(3));
        assert_eq!(
            native_server.request_log().unwrap(),
            vec![RequestId::from_raw(3)]
        );
    }

    #[test]
    fn local_block_client_creates_opens_and_reads_empty_device_across_shards() {
        let cfg = LocalStoreConfig {
            shard_count: 4,
            ..config()
        };
        let store = LocalObjectStore::with_config(cfg).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: Some("empty".to_string()),
            })
            .unwrap();

        let device = client.open_device(device_id).unwrap();
        assert_eq!(device.device_id(), device_id);
        assert_eq!(device.info().unwrap().spec.logical_blocks, 16);

        let head = store.metadata().get_head(device_id).unwrap();
        assert_eq!(head.shard_roots.len(), cfg.shard_count);
        for root in &head.shard_roots {
            store.metadata().get_metadata_node(*root).unwrap();
        }

        let mut buf = vec![99; 6 * 4096];
        device.read_at(3 * 4096, &mut buf).unwrap();
        assert_eq!(buf, vec![0; 6 * 4096]);

        let mut empty = Vec::new();
        device.read_at(16 * 4096, &mut empty).unwrap();
        assert!(device.read_at(1, &mut [0; 4096]).is_err());
    }

    #[test]
    fn sparse_block_reads_overlay_segment_entries_on_zeroes() {
        let cfg = LocalStoreConfig {
            shard_count: 1,
            ..config()
        };
        let store = LocalObjectStore::with_config(cfg).unwrap();
        let head = store.metadata().create_device(device_request()).unwrap();
        let reservation = SegmentReservation {
            segment_id: SegmentId::from_raw(500),
            bytes: 4096,
        };
        store
            .segment_store()
            .write_segment(&reservation, &[7; 4096])
            .unwrap();
        store
            .segment_store()
            .sync_segment(reservation.segment_id)
            .unwrap();

        let node = MetadataNode {
            node_id: MetadataNodeId::from_raw(500),
            covered_range: crate::api::BlockRange::new(
                BlockIndex::from_raw(0),
                BlockCount::from_raw(16),
            ),
            kind: MetadataNodeKind::Leaf {
                entries: vec![LeafEntry {
                    logical_start: BlockIndex::from_raw(2),
                    blocks: BlockCount::from_raw(1),
                    segment_id: reservation.segment_id,
                    segment_offset: BlockIndex::from_raw(0),
                }],
            },
        };
        store
            .metadata()
            .persist_metadata_node(node.clone())
            .unwrap();
        store
            .metadata()
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: node.node_id,
                })],
            })
            .unwrap();

        let mut buf = vec![0; 4 * 4096];
        store
            .read_device(head.device_id, ByteRange::new(0, 4 * 4096), &mut buf)
            .unwrap();

        assert_eq!(&buf[0..4096], vec![0; 4096].as_slice());
        assert_eq!(&buf[4096..8192], vec![0; 4096].as_slice());
        assert_eq!(&buf[8192..12288], vec![7; 4096].as_slice());
        assert_eq!(&buf[12288..16384], vec![0; 4096].as_slice());
    }

    #[test]
    fn local_native_file_client_creates_opens_and_reads_empty_file() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalNativeServer::new(store));
        let client = LocalNativeFileClient::new(InProcessNativeTransport::new(server));
        let file_id = client
            .create_file(CreateFileRequest {
                spec: FileSpec {
                    name: Some("empty".to_string()),
                },
            })
            .unwrap();

        let file = client.open_file(file_id).unwrap();
        assert_eq!(file.file_id(), file_id);
        let info = file.info().unwrap();
        assert_eq!(info.size, 0);
        assert_eq!(info.version, FileVersion::from_raw(0));

        let mut empty = Vec::new();
        file.read_at(0, &mut empty).unwrap();
        assert!(file.read_at(0, &mut [0]).is_err());
    }

    #[test]
    fn block_writes_and_overwrites_preserve_expected_ranges() {
        struct Case {
            name: &'static str,
            start_block: u64,
            blocks: u64,
            byte: u8,
        }

        let cases = [
            Case {
                name: "beginning",
                start_block: 0,
                blocks: 2,
                byte: 2,
            },
            Case {
                name: "middle",
                start_block: 3,
                blocks: 2,
                byte: 3,
            },
            Case {
                name: "end",
                start_block: 6,
                blocks: 2,
                byte: 4,
            },
            Case {
                name: "full-range",
                start_block: 0,
                blocks: 8,
                byte: 5,
            },
            Case {
                name: "same-range",
                start_block: 2,
                blocks: 3,
                byte: 6,
            },
            Case {
                name: "cross-shard",
                start_block: 3,
                blocks: 3,
                byte: 7,
            },
        ];

        for case in cases {
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..config()
            })
            .unwrap();
            let device = create_local_device(&store, 8);
            let initial = repeated_blocks(8, 1);
            device.write_at(0, &initial).unwrap();

            let overwrite = repeated_blocks(case.blocks, case.byte);
            device
                .write_at(case.start_block * 4096, &overwrite)
                .unwrap();

            let mut actual = vec![0; 8 * 4096];
            device.read_at(0, &mut actual).unwrap();

            let mut expected = initial;
            for block in case.start_block..case.start_block + case.blocks {
                let start = block as usize * 4096;
                expected[start..start + 4096].fill(case.byte);
            }
            assert_eq!(actual, expected, "case {}", case.name);
        }
    }

    #[test]
    fn cross_shard_write_publishes_one_commit_group_and_references_segments_after_sync() {
        let store = LocalObjectStore::with_config(LocalStoreConfig {
            shard_count: 2,
            ..config()
        })
        .unwrap();
        let device = create_local_device(&store, 8);
        let commit = device.write_at(3 * 4096, &repeated_blocks(3, 9)).unwrap();

        let groups = store
            .metadata()
            .commit_groups_for_seq(commit.commit_seq)
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].updates.len(), 2);

        let roots = store
            .metadata()
            .get_head(device.device_id())
            .unwrap()
            .shard_roots;
        let mut referenced_segments = Vec::new();
        for root in roots {
            let node = store.metadata().get_metadata_node(root).unwrap();
            let MetadataNodeKind::Leaf { entries } = node.kind else {
                panic!("default test roots should be leaves");
            };
            for entry in entries {
                referenced_segments.push(entry.segment_id);
                assert!(store.segment_store().is_synced(entry.segment_id).unwrap());
                assert_eq!(
                    store.segment_catalog().state(entry.segment_id).unwrap(),
                    SegmentLifecycleState::Referenced
                );
            }
        }
        assert_eq!(referenced_segments.len(), 2);
        let first_intent = store
            .segment_catalog()
            .intent_for_segment(referenced_segments[0])
            .unwrap()
            .write_intent;
        let second_intent = store
            .segment_catalog()
            .intent_for_segment(referenced_segments[1])
            .unwrap()
            .write_intent;
        assert_eq!(first_intent, second_intent);
    }

    #[test]
    fn metadata_tree_shape_is_deterministic_for_a_write_trace() {
        fn run_trace() -> String {
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 1,
                ..tree_config()
            })
            .unwrap();
            let device = create_local_device(&store, 16);
            for (start, blocks, byte) in [(0, 1, 1), (7, 2, 2), (14, 2, 3), (4, 4, 4)] {
                device
                    .write_at(start * 4096, &repeated_blocks(blocks, byte))
                    .unwrap();
            }
            let root = store
                .metadata()
                .get_head(device.device_id())
                .unwrap()
                .shard_roots[0];
            let stats = store.validate_metadata_tree(root).unwrap();
            assert!(stats.max_depth > 1);
            store.render_metadata_tree(root).unwrap()
        }

        assert_eq!(run_trace(), run_trace());
    }

    #[test]
    fn root_to_leaf_path_copy_changes_only_touched_nodes() {
        let store = LocalObjectStore::with_config(LocalStoreConfig {
            shard_count: 1,
            ..tree_config()
        })
        .unwrap();
        let device = create_local_device(&store, 16);
        let old_root = store
            .metadata()
            .get_head(device.device_id())
            .unwrap()
            .shard_roots[0];
        let old_stats = store.validate_metadata_tree(old_root).unwrap();
        let old_ids: BTreeSet<_> = store
            .metadata_tree_node_ids(old_root)
            .unwrap()
            .into_iter()
            .collect();

        device.write_at(0, &repeated_blocks(1, 9)).unwrap();

        let new_root = store
            .metadata()
            .get_head(device.device_id())
            .unwrap()
            .shard_roots[0];
        let new_stats = store.validate_metadata_tree(new_root).unwrap();
        assert_eq!(old_stats.nodes, new_stats.nodes);
        assert_eq!(old_stats.max_depth, new_stats.max_depth);
        let new_ids: BTreeSet<_> = store
            .metadata_tree_node_ids(new_root)
            .unwrap()
            .into_iter()
            .collect();
        let new_only = new_ids.difference(&old_ids).count();
        let shared = old_ids.intersection(&new_ids).count();

        assert_eq!(new_only, old_stats.max_depth);
        assert_eq!(shared, old_stats.nodes - old_stats.max_depth);
    }

    #[test]
    fn generated_block_tree_reads_match_reference_model() {
        for seed in 0..16 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..tree_config()
            })
            .unwrap();
            let device = create_local_device(&store, 32);
            let mut model = vec![0u8; 32];

            for step in 0..32 {
                let start = harness.rng.next_u64() % 32;
                let max_blocks = (32 - start).min(5);
                let blocks = 1 + harness.rng.next_u64() % max_blocks;
                let byte = (1 + harness.rng.next_u64() % 254) as u8;
                harness.trace.record(format!(
                    "write step={step} start={start} blocks={blocks} byte={byte}"
                ));
                device
                    .write_at(start * 4096, &repeated_blocks(blocks, byte))
                    .unwrap();
                for block in start..start + blocks {
                    model[block as usize] = byte;
                }

                let mut actual = vec![0; 32 * 4096];
                device.read_at(0, &mut actual).unwrap();
                assert_model_blocks(
                    &actual,
                    &model,
                    seed,
                    harness.trace.events(),
                    &render_device_roots(&store, device.device_id()),
                );
                validate_device_roots(&store, device.device_id());
            }
        }
    }

    #[test]
    fn generated_native_tree_reads_match_reference_model() {
        for seed in 0..16 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(tree_config()).unwrap();
            let server = Arc::new(LocalNativeServer::new(store.clone()));
            let client = LocalNativeFileClient::new(InProcessNativeTransport::new(server));
            let file_id = client
                .create_file(CreateFileRequest {
                    spec: FileSpec { name: None },
                })
                .unwrap();
            let file = client.open_file(file_id).unwrap();
            let mut model = Vec::new();

            for step in 0..16 {
                let remaining = 32 - model.len() as u64;
                if remaining == 0 {
                    break;
                }
                let blocks = 1 + harness.rng.next_u64() % remaining.min(4);
                let byte = (1 + harness.rng.next_u64() % 254) as u8;
                harness
                    .trace
                    .record(format!("append step={step} blocks={blocks} byte={byte}"));
                let lease = file.acquire_append().unwrap();
                file.append_with_lease(lease, &repeated_blocks(blocks, byte))
                    .unwrap();
                model.extend(std::iter::repeat_n(byte, blocks as usize));

                let mut actual = vec![0; model.len() * 4096];
                file.read_at(0, &mut actual).unwrap();
                let root = store.metadata().get_file_head(file_id).unwrap().root;
                assert_model_blocks(
                    &actual,
                    &model,
                    seed,
                    harness.trace.events(),
                    &store.render_metadata_tree(root).unwrap(),
                );
                store.validate_metadata_tree(root).unwrap();
            }
        }
    }

    #[test]
    fn fork_copies_roots_without_allocating_metadata_and_records_catalog() {
        let store = LocalObjectStore::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let device = create_local_device(&store, 32);
        device.write_at(0, &repeated_blocks(8, 1)).unwrap();
        device.write_at(20 * 4096, &repeated_blocks(4, 2)).unwrap();
        let parent_head = store.metadata().get_head(device.device_id()).unwrap();
        let metadata_nodes_before = store.metadata().metadata_node_count().unwrap();

        let child_id = device
            .fork(ForkRequest {
                target: Some(DeviceId::from_raw(99)),
                name: Some("child".to_string()),
            })
            .unwrap();

        let child_head = store.metadata().get_head(child_id).unwrap();
        assert_eq!(child_id, DeviceId::from_raw(99));
        assert_eq!(child_head.shard_roots, parent_head.shard_roots);
        assert_eq!(
            store.metadata().get_head(device.device_id()).unwrap(),
            parent_head
        );
        assert_eq!(
            store.metadata().metadata_node_count().unwrap(),
            metadata_nodes_before
        );

        let record = store
            .metadata()
            .fork_record(child_head.latest_commit)
            .unwrap();
        assert_eq!(record.source, device.device_id());
        assert_eq!(record.target, child_id);
        assert_eq!(record.shard_roots, parent_head.shard_roots);
        assert_eq!(
            store
                .metadata()
                .fork_records_for_source(device.device_id())
                .unwrap(),
            vec![record]
        );
    }

    #[test]
    fn forked_devices_initially_match_and_then_diverge() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let parent_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 8,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let parent = client.open_device(parent_id).unwrap();
        parent.write_at(0, &repeated_blocks(8, 1)).unwrap();

        let child_id = parent
            .fork(ForkRequest {
                target: None,
                name: Some("child".to_string()),
            })
            .unwrap();
        let child = client.open_device(child_id).unwrap();
        assert_eq!(read_device_bytes(&parent, 8), repeated_blocks(8, 1));
        assert_eq!(read_device_bytes(&child, 8), repeated_blocks(8, 1));

        parent.write_at(0, &repeated_blocks(1, 2)).unwrap();
        assert_eq!(&read_device_bytes(&parent, 8)[0..4096], vec![2; 4096]);
        assert_eq!(&read_device_bytes(&child, 8)[0..4096], vec![1; 4096]);

        child.write_at(7 * 4096, &repeated_blocks(1, 3)).unwrap();
        assert_eq!(
            &read_device_bytes(&child, 8)[7 * 4096..8 * 4096],
            vec![3; 4096]
        );
        assert_eq!(
            &read_device_bytes(&parent, 8)[7 * 4096..8 * 4096],
            vec![1; 4096]
        );
    }

    #[test]
    fn generated_repeated_forks_and_divergent_writes_match_reference_model() {
        for seed in 0..12 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..tree_config()
            })
            .unwrap();
            let server = Arc::new(LocalBlockServer::new(store.clone()));
            let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
            let root_id = client
                .create_device(CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 32,
                        block_size: 4096,
                    },
                    name: Some("root".to_string()),
                })
                .unwrap();
            let mut device_ids = vec![root_id];
            let mut models = BTreeMap::from([(root_id, vec![0u8; 32])]);

            for step in 0..32 {
                let fork = harness.rng.next_u64() % 3 == 0 && device_ids.len() < 8;
                if fork {
                    let source_index = harness.rng.choose_index(device_ids.len()).unwrap();
                    let source_id = device_ids[source_index];
                    let source = client.open_device(source_id).unwrap();
                    let child_id = source
                        .fork(ForkRequest {
                            target: None,
                            name: Some(format!("child-{seed}-{step}")),
                        })
                        .unwrap();
                    harness.trace.record(format!(
                        "fork step={step} source={source_id} child={child_id}"
                    ));
                    device_ids.push(child_id);
                    models.insert(child_id, models.get(&source_id).unwrap().clone());
                } else {
                    let target_index = harness.rng.choose_index(device_ids.len()).unwrap();
                    let target_id = device_ids[target_index];
                    let start = harness.rng.next_u64() % 32;
                    let max_blocks = (32 - start).min(4);
                    let blocks = 1 + harness.rng.next_u64() % max_blocks;
                    let byte = (1 + harness.rng.next_u64() % 254) as u8;
                    harness.trace.record(format!(
                        "write step={step} device={target_id} start={start} blocks={blocks} byte={byte}"
                    ));
                    let device = client.open_device(target_id).unwrap();
                    device
                        .write_at(start * 4096, &repeated_blocks(blocks, byte))
                        .unwrap();
                    let model = models.get_mut(&target_id).unwrap();
                    for block in start..start + blocks {
                        model[block as usize] = byte;
                    }
                }

                for device_id in &device_ids {
                    let device = client.open_device(*device_id).unwrap();
                    let mut actual = vec![0; 32 * 4096];
                    device.read_at(0, &mut actual).unwrap();
                    assert_model_blocks(
                        &actual,
                        models.get(device_id).unwrap(),
                        seed,
                        harness.trace.events(),
                        &render_device_roots(&store, *device_id),
                    );
                    validate_device_roots(&store, *device_id);
                }
            }
        }
    }

    #[test]
    fn pitr_replays_roots_and_restores_to_commit_checkpoint_and_time() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 8,
                    block_size: 4096,
                },
                name: Some("pitr".to_string()),
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();

        let commit1 = device.write_at(0, &repeated_blocks(8, 1)).unwrap();
        let checkpoint1 = store.metadata().checkpoint(device_id).unwrap();
        let commit2 = device.write_at(3 * 4096, &repeated_blocks(3, 2)).unwrap();
        let checkpoint2 = store.metadata().checkpoint(device_id).unwrap();

        let head = store.metadata().get_head(device_id).unwrap();
        assert_eq!(
            store
                .metadata()
                .replay_device_roots(device_id, commit2.commit_seq)
                .unwrap(),
            head.shard_roots
        );
        assert_eq!(
            store
                .metadata()
                .replay_device_roots(device_id, commit1.commit_seq)
                .unwrap(),
            store
                .metadata()
                .get_checkpoint(checkpoint1)
                .unwrap()
                .shard_roots
        );

        let shard_commits = store
            .metadata()
            .shard_commits_for_device(device_id)
            .unwrap();
        let commit2_group_ids: BTreeSet<_> = shard_commits
            .iter()
            .filter(|commit| commit.commit_seq == commit2.commit_seq)
            .map(|commit| commit.commit_group)
            .collect();
        assert_eq!(commit2_group_ids.len(), 1);

        let restored_from_commit = device
            .restore(RestorePoint::Commit(commit1.commit_seq))
            .unwrap();
        let restored_from_checkpoint = device
            .restore(RestorePoint::Checkpoint(checkpoint1))
            .unwrap();
        let restored_from_time = device
            .restore(RestorePoint::Time(LogicalTime::from_raw(
                commit2.commit_seq.raw(),
            )))
            .unwrap();

        assert_eq!(
            read_device_bytes(&client.open_device(restored_from_commit).unwrap(), 8),
            repeated_blocks(8, 1)
        );
        assert_eq!(
            read_device_bytes(&client.open_device(restored_from_checkpoint).unwrap(), 8),
            repeated_blocks(8, 1)
        );

        let mut expected2 = repeated_blocks(8, 1);
        expected2[3 * 4096..6 * 4096].fill(2);
        assert_eq!(
            read_device_bytes(&client.open_device(restored_from_time).unwrap(), 8),
            expected2
        );

        assert!(
            store
                .metadata()
                .validate_checkpoint(&store.metadata().get_checkpoint(checkpoint2).unwrap())
                .is_ok()
        );
        assert!(
            device
                .restore(RestorePoint::Commit(CommitSeq::from_raw(999)))
                .is_err()
        );
    }

    #[test]
    fn checkpoint_validation_detects_mismatched_roots() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 8);
        let initial_roots = store
            .metadata()
            .get_head(device.device_id())
            .unwrap()
            .shard_roots;
        device.write_at(0, &repeated_blocks(8, 1)).unwrap();
        let checkpoint_id = store.metadata().checkpoint(device.device_id()).unwrap();
        let checkpoint = store.metadata().get_checkpoint(checkpoint_id).unwrap();
        assert!(store.metadata().validate_checkpoint(&checkpoint).is_ok());

        let mut corrupted = checkpoint;
        corrupted.shard_roots[0] = initial_roots[0];
        assert!(store.metadata().validate_checkpoint(&corrupted).is_err());
    }

    #[test]
    fn pitr_restore_interacts_with_forks_without_mutating_sources() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let parent_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 8,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let parent = client.open_device(parent_id).unwrap();
        let parent_commit = parent.write_at(0, &repeated_blocks(8, 4)).unwrap();
        let child_id = parent
            .fork(ForkRequest {
                target: None,
                name: Some("child".to_string()),
            })
            .unwrap();
        let child = client.open_device(child_id).unwrap();
        let child_base = store.metadata().get_head(child_id).unwrap().latest_commit;
        child.write_at(7 * 4096, &repeated_blocks(1, 9)).unwrap();

        let parent_restore = parent
            .restore(RestorePoint::Commit(parent_commit.commit_seq))
            .unwrap();
        let child_restore = child.restore(RestorePoint::Commit(child_base)).unwrap();

        assert_eq!(
            read_device_bytes(&client.open_device(parent_restore).unwrap(), 8),
            repeated_blocks(8, 4)
        );
        assert_eq!(
            read_device_bytes(&client.open_device(child_restore).unwrap(), 8),
            repeated_blocks(8, 4)
        );
        assert_eq!(
            &read_device_bytes(&child, 8)[7 * 4096..8 * 4096],
            vec![9; 4096]
        );
        assert_eq!(read_device_bytes(&parent, 8), repeated_blocks(8, 4));
    }

    #[test]
    fn generated_pitr_restores_match_historical_model() {
        for seed in 0..12 {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let store = LocalObjectStore::with_config(LocalStoreConfig {
                shard_count: 2,
                ..tree_config()
            })
            .unwrap();
            let server = Arc::new(LocalBlockServer::new(store.clone()));
            let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
            let device_id = client
                .create_device(CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 32,
                        block_size: 4096,
                    },
                    name: None,
                })
                .unwrap();
            let device = client.open_device(device_id).unwrap();
            let mut model = vec![0u8; 32];
            let mut history = vec![(CommitSeq::from_raw(0), model.clone())];

            for step in 0..24 {
                let start = harness.rng.next_u64() % 32;
                let max_blocks = (32 - start).min(5);
                let blocks = 1 + harness.rng.next_u64() % max_blocks;
                let byte = (1 + harness.rng.next_u64() % 254) as u8;
                harness.trace.record(format!(
                    "write step={step} start={start} blocks={blocks} byte={byte}"
                ));
                let commit = device
                    .write_at(start * 4096, &repeated_blocks(blocks, byte))
                    .unwrap();
                for block in start..start + blocks {
                    model[block as usize] = byte;
                }
                history.push((commit.commit_seq, model.clone()));
                if harness.rng.next_u64() % 4 == 0 {
                    store.metadata().checkpoint(device_id).unwrap();
                }
            }

            for _ in 0..8 {
                let index = harness.rng.choose_index(history.len()).unwrap();
                let (commit_seq, expected) = &history[index];
                let restored = device.restore(RestorePoint::Commit(*commit_seq)).unwrap();
                let restored_device = client.open_device(restored).unwrap();
                let mut actual = vec![0; 32 * 4096];
                restored_device.read_at(0, &mut actual).unwrap();
                assert_model_blocks(
                    &actual,
                    expected,
                    seed,
                    harness.trace.events(),
                    &render_device_roots(&store, restored),
                );
            }
        }
    }

    #[test]
    fn discard_removes_mapping_and_write_zeroes_reads_as_zeroes() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let device = create_local_device(&store, 8);
        device.write_at(0, &repeated_blocks(8, 8)).unwrap();
        device.discard(2 * 4096, 2 * 4096).unwrap();
        device.write_zeroes(5 * 4096, 4096).unwrap();

        let mut actual = vec![0; 8 * 4096];
        device.read_at(0, &mut actual).unwrap();
        assert_eq!(&actual[0..2 * 4096], repeated_blocks(2, 8).as_slice());
        assert_eq!(&actual[2 * 4096..4 * 4096], vec![0; 2 * 4096].as_slice());
        assert_eq!(&actual[4 * 4096..5 * 4096], vec![8; 4096].as_slice());
        assert_eq!(&actual[5 * 4096..6 * 4096], vec![0; 4096].as_slice());
        assert_eq!(
            &actual[6 * 4096..8 * 4096],
            repeated_blocks(2, 8).as_slice()
        );
    }

    #[test]
    fn failed_publish_after_durable_segment_write_leaves_old_roots_and_orphan() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let head = store.metadata().create_device(device_request()).unwrap();
        let reservation = store
            .write_segment_for_owner(
                MappingOwner::BlockDevice(head.device_id),
                &repeated_blocks(1, 9),
            )
            .unwrap();
        let old_root = store
            .metadata()
            .get_metadata_node(head.shard_roots[0])
            .unwrap();
        let node = store
            .metadata()
            .allocate_metadata_node(
                old_root.covered_range,
                MetadataNodeKind::Leaf {
                    entries: vec![LeafEntry {
                        logical_start: old_root.covered_range.start,
                        blocks: BlockCount::from_raw(1),
                        segment_id: reservation.segment_id,
                        segment_offset: BlockIndex::from_raw(0),
                    }],
                },
            )
            .unwrap();
        store
            .metadata()
            .persist_metadata_node(node.clone())
            .unwrap();

        let failed = store.metadata().publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(DeviceGeneration::from_raw(99)),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: node.node_id,
            })],
        });

        assert!(failed.is_err());
        assert_eq!(store.metadata().get_head(head.device_id).unwrap(), head);
        assert_eq!(
            store
                .segment_catalog()
                .state(reservation.segment_id)
                .unwrap(),
            SegmentLifecycleState::DurablePendingMetadata
        );
        let mut buf = vec![1; 4096];
        store
            .read_device(head.device_id, ByteRange::new(0, 4096), &mut buf)
            .unwrap();
        assert_eq!(buf, vec![0; 4096]);
    }

    #[test]
    fn native_append_valid_stale_and_stolen_leases_are_deterministic() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = Arc::new(LocalNativeServer::new(store.clone()));
        let client = LocalNativeFileClient::new(InProcessNativeTransport::new(server));
        let file_id = client
            .create_file(CreateFileRequest {
                spec: FileSpec { name: None },
            })
            .unwrap();
        let file = client.open_file(file_id).unwrap();

        let first = file.acquire_append().unwrap();
        let stolen = file.acquire_append().unwrap();
        let stolen_intent = WriteIntentId::from_raw(stolen.lease_id.raw());
        assert!(
            file.append_with_lease(first, &repeated_blocks(1, 1))
                .is_err()
        );

        let commit = file
            .append_with_lease(stolen.clone(), &repeated_blocks(2, 2))
            .unwrap();
        assert_eq!(commit.version, FileVersion::from_raw(1));
        assert_eq!(commit.range, ByteRange::new(0, 2 * 4096));
        assert!(
            file.append_with_lease(stolen, &repeated_blocks(1, 3))
                .is_err()
        );

        let mut actual = vec![0; 2 * 4096];
        file.read_at(0, &mut actual).unwrap();
        assert_eq!(actual, repeated_blocks(2, 2));

        let head = store.metadata().get_file_head(file_id).unwrap();
        let root = store.metadata().get_metadata_node(head.root).unwrap();
        let MetadataNodeKind::Leaf { entries } = root.kind else {
            panic!("default test native file root should remain a leaf");
        };
        assert_eq!(entries.len(), 1);
        let intent = store
            .segment_catalog()
            .intent_for_segment(entries[0].segment_id)
            .unwrap();
        assert_eq!(intent.write_intent, stolen_intent);
    }

    #[test]
    fn native_append_publish_failure_leaves_file_version_and_orphan_unchanged() {
        let store = LocalObjectStore::with_config(LocalStoreConfig {
            file_root_blocks: 1,
            ..config()
        })
        .unwrap();
        let server = Arc::new(LocalNativeServer::new(store.clone()));
        let client = LocalNativeFileClient::new(InProcessNativeTransport::new(server));
        let file_id = client
            .create_file(CreateFileRequest {
                spec: FileSpec { name: None },
            })
            .unwrap();
        let file = client.open_file(file_id).unwrap();
        let lease = file.acquire_append().unwrap();

        let failed = file.append_with_lease(lease, &repeated_blocks(2, 4));
        assert!(failed.is_err());
        let info = file.info().unwrap();
        assert_eq!(info.version, FileVersion::from_raw(0));
        assert_eq!(info.size, 0);

        let reservation = SegmentId::from_raw(1);
        assert_eq!(
            store.segment_catalog().state(reservation).unwrap(),
            SegmentLifecycleState::DurablePendingMetadata
        );
    }

    #[test]
    fn deterministic_simulation_checks_roots_after_create_and_read() {
        fn run(seed: u64) -> (Vec<String>, Vec<u8>) {
            let mut harness = crate::sim::DeterministicHarness::new(seed);
            let cfg = LocalStoreConfig {
                shard_count: 4,
                ..config()
            };
            let store = LocalObjectStore::with_config(cfg).unwrap();
            let server = Arc::new(LocalBlockServer::new(store.clone()));
            let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
            let device_id = client
                .create_device(CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 16,
                        block_size: 4096,
                    },
                    name: None,
                })
                .unwrap();
            harness.trace.record(format!("created={device_id}"));
            let head = store.metadata().get_head(device_id).unwrap();
            for root in &head.shard_roots {
                store.metadata().get_metadata_node(*root).unwrap();
                harness.trace.record(format!("root={root}"));
            }

            let device = client.open_device(device_id).unwrap();
            let mut buf = vec![1; 4096 * 2];
            device.read_at(4 * 4096, &mut buf).unwrap();
            for root in &store.metadata().get_head(device_id).unwrap().shard_roots {
                store.metadata().get_metadata_node(*root).unwrap();
            }
            harness.trace.record("read=ok");
            (harness.trace.into_events(), buf)
        }

        assert_eq!(run(99), run(99));
    }

    #[test]
    fn block_and_native_services_share_segment_lifecycle_machinery() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let block_server = LocalBlockServer::new(store.clone());
        let native_server = LocalNativeServer::new(store.clone());
        let reservation = store
            .segment_catalog()
            .reserve_segment(reservation_intent())
            .unwrap();

        assert_eq!(
            block_server
                .store
                .segment_catalog()
                .state(reservation.segment_id)
                .unwrap(),
            SegmentLifecycleState::Reserved
        );
        assert_eq!(
            native_server
                .store
                .segment_catalog()
                .state(reservation.segment_id)
                .unwrap(),
            SegmentLifecycleState::Reserved
        );
    }

    #[test]
    fn local_providers_replay_ordered_commands_deterministically() {
        assert_eq!(deterministic_provider_run(), deterministic_provider_run());
    }

    fn deterministic_provider_run() -> (
        DeviceHead,
        CommitGroup,
        SegmentReplicaCommit,
        SegmentLifecycleState,
        Vec<MetadataNodeId>,
    ) {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let head = store.metadata().create_device(device_request()).unwrap();
        let new_node = metadata_leaf(2000, 0, 8);
        store
            .metadata()
            .persist_metadata_node(new_node.clone())
            .unwrap();
        let commit_group = store
            .metadata()
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: new_node.node_id,
                })],
            })
            .unwrap();

        let reservation = store
            .segment_catalog()
            .reserve_segment(reservation_intent())
            .unwrap();
        store.segment_catalog().begin_write(&reservation).unwrap();
        let replica_commit = store
            .segment_store()
            .write_segment(&reservation, &[5; 4096])
            .unwrap();
        store
            .segment_store()
            .sync_segment(reservation.segment_id)
            .unwrap();
        store
            .segment_catalog()
            .commit_segment(reservation.clone(), replica_commit.clone())
            .unwrap();
        store
            .segment_catalog()
            .mark_segment_referenced(reservation.segment_id)
            .unwrap();
        let state = store
            .segment_catalog()
            .state(reservation.segment_id)
            .unwrap();
        let roots = store
            .metadata()
            .roots_for_gc(RetentionPolicy {
                retain_deleted_devices: false,
            })
            .unwrap();

        (
            store.metadata().get_head(head.device_id).unwrap(),
            commit_group,
            replica_commit,
            state,
            roots,
        )
    }

    #[test]
    fn unsupported_local_service_operations_preserve_no_partial_state() {
        let store = LocalObjectStore::with_config(config()).unwrap();
        let server = LocalBlockServer::new(store.clone());
        let response = server.handle(BlockRequestEnvelope::new(
            RequestId::from_raw(10),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Flush {
                device_id: DeviceId::from_raw(404),
                scope: FlushScope::Device,
            },
        ));

        assert!(response.is_err());
        assert!(store.metadata().get_head(DeviceId::from_raw(404)).is_err());

        let native = LocalNativeServer::new(store);
        let response = native.handle(NativeRequestEnvelope::new(
            RequestId::from_raw(11),
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::Append {
                file_id: FileId::from_raw(1),
                lease: crate::extent::AppendLease {
                    file_id: FileId::from_raw(1),
                    lease_id: crate::id::AppendLeaseId::from_raw(1),
                    writer_epoch: WriterEpoch::from_raw(0),
                    base_version: FileVersion::from_raw(0),
                },
                bytes: vec![1],
                durability: WriteDurability::Acknowledged,
            },
        ));

        assert!(response.is_err());
    }

    #[test]
    fn leaf_entries_can_reference_local_segment_descriptors_for_validation() {
        let store = InMemorySegmentStore::new(config()).unwrap();
        let reservation = SegmentReservation {
            segment_id: SegmentId::from_raw(77),
            bytes: 4096,
        };
        let commit = store.write_segment(&reservation, &[3; 4096]).unwrap();
        let node = MetadataNode {
            node_id: MetadataNodeId::from_raw(77),
            covered_range: crate::api::BlockRange::new(
                BlockIndex::from_raw(0),
                BlockCount::from_raw(1),
            ),
            kind: MetadataNodeKind::Leaf {
                entries: vec![LeafEntry {
                    logical_start: BlockIndex::from_raw(0),
                    blocks: BlockCount::from_raw(1),
                    segment_id: commit.descriptor.segment_id,
                    segment_offset: BlockIndex::from_raw(0),
                }],
            },
        };

        assert!(node.validate(&[commit.descriptor]).is_ok());
    }

    fn create_local_device(store: &LocalObjectStore, logical_blocks: u64) -> LocalBlockDevice {
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        client.open_device(device_id).unwrap()
    }

    fn repeated_blocks(blocks: u64, byte: u8) -> Vec<u8> {
        vec![byte; blocks as usize * 4096]
    }

    fn read_device_bytes(device: &LocalBlockDevice, blocks: u64) -> Vec<u8> {
        let mut out = vec![0; blocks as usize * 4096];
        device.read_at(0, &mut out).unwrap();
        out
    }

    fn validate_device_roots(store: &LocalObjectStore, device_id: DeviceId) {
        let head = store.metadata().get_head(device_id).unwrap();
        for root in head.shard_roots {
            store.validate_metadata_tree(root).unwrap();
        }
    }

    fn render_device_roots(store: &LocalObjectStore, device_id: DeviceId) -> String {
        let head = store.metadata().get_head(device_id).unwrap();
        let mut out = String::new();
        for (shard, root) in head.shard_roots.iter().enumerate() {
            out.push_str(&format!("shard {shard}\n"));
            out.push_str(&store.render_metadata_tree(*root).unwrap());
        }
        out
    }

    fn assert_model_blocks(actual: &[u8], model: &[u8], seed: u64, trace: &[String], tree: &str) {
        assert_eq!(actual.len(), model.len() * 4096);
        for (block, expected) in model.iter().copied().enumerate() {
            let start = block * 4096;
            let end = start + 4096;
            if actual[start..end].iter().any(|byte| *byte != expected) {
                panic!(
                    "seed {seed} block {block} expected byte {expected}\ntrace:\n{}\ntree:\n{tree}",
                    trace.join("\n")
                );
            }
        }
    }
}
