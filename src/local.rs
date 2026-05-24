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
    Checkpoint, CommitGroup, DeviceHead, FileHead, ForkRecord, LeafEntry, MappingOwner,
    MetadataChild, MetadataNode, MetadataNodeKind, RootUpdate, SegmentDescriptor, ShardRootUpdate,
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

#[derive(Debug)]
struct MetadataInner {
    next_device_id: u128,
    next_file_id: u128,
    next_metadata_node_id: u128,
    next_commit_group_id: u128,
    next_commit_seq: u64,
    next_checkpoint_id: u128,
    device_heads: BTreeMap<DeviceId, DeviceHead>,
    device_specs: BTreeMap<DeviceId, crate::api::DeviceSpec>,
    file_heads: BTreeMap<FileId, FileHead>,
    file_specs: BTreeMap<FileId, crate::extent::FileSpec>,
    file_writer_epochs: BTreeMap<FileId, WriterEpoch>,
    metadata_nodes: BTreeMap<MetadataNodeId, MetadataNode>,
    commit_groups: BTreeMap<CommitGroupId, CommitGroup>,
    fork_records: BTreeMap<CommitSeq, ForkRecord>,
    checkpoints: BTreeMap<CheckpointId, Checkpoint>,
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
            device_heads: BTreeMap::new(),
            device_specs: BTreeMap::new(),
            file_heads: BTreeMap::new(),
            file_specs: BTreeMap::new(),
            file_writer_epochs: BTreeMap::new(),
            metadata_nodes: BTreeMap::new(),
            commit_groups: BTreeMap::new(),
            fork_records: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
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

    pub fn metadata_node_count(&self) -> Result<usize> {
        Ok(lock(&self.inner)?.metadata_nodes.len())
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
                    next_roots[shard] = update.new_root;
                }

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group = CommitGroup {
                    commit_group: inner.alloc_commit_group_id(),
                    commit_seq,
                    owner: intent.owner,
                    updates: intent.updates,
                };
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
                if inner.device_heads.contains_key(&target) {
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
        Ok(head)
    }

    fn restore_device(
        &self,
        _source: DeviceId,
        _point: crate::api::RestorePoint,
    ) -> Result<DeviceHead> {
        Err(StorageError::unsupported(
            "point-in-time restore is implemented in a later phase",
        ))
    }

    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        let mut inner = lock(&self.inner)?;
        let head = inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        let checkpoint_id = inner.alloc_checkpoint_id();
        let checkpoint = Checkpoint {
            checkpoint_id,
            commit_seq: head.latest_commit,
            time: LogicalTime::from_raw(head.latest_commit.raw()),
            owner: MappingOwner::BlockDevice(device_id),
            shard_roots: head.shard_roots,
        };
        inner.checkpoints.insert(checkpoint_id, checkpoint);
        Ok(checkpoint_id)
    }

    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint> {
        let inner = lock(&self.inner)?;
        inner
            .checkpoints
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("checkpoint", checkpoint_id.to_string()))
    }

    fn roots_for_gc(&self, _policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        let inner = lock(&self.inner)?;
        let mut roots = Vec::new();
        for head in inner.device_heads.values() {
            roots.extend(head.shard_roots.iter().copied());
        }
        for head in inner.file_heads.values() {
            roots.push(head.root);
        }
        for checkpoint in inner.checkpoints.values() {
            roots.extend(checkpoint.shard_roots.iter().copied());
        }
        roots.sort();
        roots.dedup();
        Ok(roots)
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
            BlockRequest::Restore { .. } | BlockRequest::Delete { .. } => {
                return Err(StorageError::unsupported(
                    "restore and delete are implemented in later phases",
                ));
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

    fn restore(&self, _point: RestorePoint) -> Result<DeviceId> {
        Err(StorageError::unsupported(
            "block restore is implemented in a later phase",
        ))
    }

    fn delete(&self) -> Result<DeleteResult> {
        Err(StorageError::unsupported(
            "block delete is implemented in a later phase",
        ))
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
