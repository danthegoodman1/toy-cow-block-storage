use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::api::{
    BlockRequest, BlockRequestEnvelope, BlockResponse, BlockResponseEnvelope, BlockServer,
    BlockTransport, ByteRange, DeviceInfo,
};
use crate::error::{Result, StorageError};
use crate::extent::{
    FileInfo, NativeRequest, NativeRequestEnvelope, NativeResponse, NativeResponseEnvelope,
    NativeServer, NativeTransport,
};
use crate::id::{
    BlockCount, BlockIndex, CheckpointId, CommitGroupId, CommitSeq, DeviceGeneration, DeviceId,
    FileId, FileVersion, LogicalTime, MetadataNodeId, RequestId, SegmentId, StorageNodeId,
    WriterEpoch,
};
use crate::object::{
    Checkpoint, CommitGroup, DeviceHead, FileHead, MappingOwner, MetadataNode, MetadataNodeKind,
    RootUpdate, SegmentDescriptor,
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
    pub storage_node: StorageNodeId,
}

impl Default for LocalStoreConfig {
    fn default() -> Self {
        Self {
            shard_count: 1,
            block_size: 4096,
            file_root_blocks: 1,
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

        Ok(())
    }
}

/// Shared local in-process provider bundle.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    metadata: Arc<InMemoryMetadataPlane>,
    segment_store: Arc<InMemorySegmentStore>,
    segment_catalog: Arc<InMemoryLocalSegmentCatalog>,
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
}

impl Default for LocalObjectStore {
    fn default() -> Self {
        Self::new()
    }
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
            checkpoints: BTreeMap::new(),
        }
    }

    fn alloc_device_id(&mut self) -> DeviceId {
        let id = DeviceId::from_raw(self.next_device_id);
        self.next_device_id += 1;
        id
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

    fn create_empty_leaf(inner: &mut MetadataInner, range: crate::api::BlockRange) -> MetadataNode {
        let node = MetadataNode {
            node_id: inner.alloc_metadata_node_id(),
            covered_range: range,
            kind: MetadataNodeKind::Leaf {
                entries: Vec::new(),
            },
        };
        inner.metadata_nodes.insert(node.node_id, node.clone());
        node
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

        if request.spec.logical_blocks < self.config.shard_count as u64 {
            return Err(StorageError::invalid_argument(
                "logical_blocks must be at least shard_count",
            ));
        }

        let mut inner = lock(&self.inner)?;
        let device_id = inner.alloc_device_id();
        let mut shard_roots = Vec::with_capacity(self.config.shard_count);

        for shard in 0..self.config.shard_count {
            let start = request.spec.logical_blocks * shard as u64 / self.config.shard_count as u64;
            let end =
                request.spec.logical_blocks * (shard as u64 + 1) / self.config.shard_count as u64;
            let node = Self::create_empty_leaf(
                &mut inner,
                crate::api::BlockRange::new(
                    BlockIndex::from_raw(start),
                    BlockCount::from_raw(end - start),
                ),
            );
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
        let root = Self::create_empty_leaf(
            &mut inner,
            crate::api::BlockRange::new(
                BlockIndex::from_raw(0),
                BlockCount::from_raw(self.config.file_root_blocks),
            ),
        );
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
                    let shard = update.shard_id.raw() as usize;
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
                let mut next_head = current;
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

                let (old_root, new_root) = match intent.updates.as_slice() {
                    [RootUpdate::FileRoot { old_root, new_root }] => (*old_root, *new_root),
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

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group = CommitGroup {
                    commit_group: inner.alloc_commit_group_id(),
                    commit_seq,
                    owner: intent.owner,
                    updates: vec![RootUpdate::FileRoot { old_root, new_root }],
                };
                let mut next_head = current;
                next_head.version = Self::next_file_version(next_head.version)?;
                next_head.latest_commit = commit_seq;
                next_head.root = new_root;
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
                target
            }
            None => inner.alloc_device_id(),
        };
        let latest_commit = inner.alloc_commit_seq()?;
        let head = DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: source_head.shard_roots,
            latest_commit,
        };
        head.validate(self.config.shard_count)?;
        inner.device_specs.insert(target, source_spec);
        inner.device_heads.insert(target, head.clone());
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

        if reservation.bytes != bytes.len() as u64 {
            return Err(StorageError::invalid_argument(
                "reservation byte count does not match write length",
            ));
        }

        if bytes.len() as u64 % u64::from(self.config.block_size) != 0 {
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
        if end > record.bytes.len() as u64 {
            return Err(StorageError::invalid_argument(
                "segment read extends past end of segment",
            ));
        }
        if buf.len() as u64 != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        let start = range.offset as usize;
        let end = end as usize;
        buf.copy_from_slice(&record.bytes[start..end]);
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
}

impl LocalBlockServer {
    pub fn new(store: LocalObjectStore) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }
}

impl BlockServer for LocalBlockServer {
    fn handle(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
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
            BlockRequest::Read { .. } => {
                return Err(StorageError::unsupported(
                    "block reads are implemented in a later phase",
                ));
            }
            BlockRequest::Write { .. }
            | BlockRequest::Flush { .. }
            | BlockRequest::WriteZeroes { .. }
            | BlockRequest::Discard { .. }
            | BlockRequest::Fork { .. }
            | BlockRequest::Restore { .. }
            | BlockRequest::Delete { .. } => {
                return Err(StorageError::unsupported(
                    "mutating block operations are implemented in later phases",
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
}

impl LocalNativeServer {
    pub fn new(store: LocalObjectStore) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }
}

impl NativeServer for LocalNativeServer {
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
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
            NativeRequest::Read { .. } => {
                return Err(StorageError::unsupported(
                    "native file reads are implemented in a later phase",
                ));
            }
            NativeRequest::AcquireAppend { .. }
            | NativeRequest::Append { .. }
            | NativeRequest::Flush { .. } => {
                return Err(StorageError::unsupported(
                    "native append and flush operations are implemented in later phases",
                ));
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

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
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
            storage_node: StorageNodeId::from_raw(77),
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
}
