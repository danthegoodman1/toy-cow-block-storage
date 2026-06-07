use std::collections::BTreeMap;
use std::sync::Arc;

use crate::api::BlockRange;
use crate::error::{Result, StorageError};
use crate::id::{
    AppendPublishTicketId, AppendRunId, AppendStreamId, BlockCount, BlockIndex, CheckpointId,
    CommitGroupId, CommitSeq, DeviceGeneration, DeviceId, FileId, FileVersion,
    KeyspaceCatalogShardId, KeyspaceGeneration, KeyspaceId, KeyspaceRootId, LogicalTime,
    MetadataNodeId, SegmentId, ShardId, StorageNodeId, WriterEpoch,
};

/// Stored payload integrity for one immutable data segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentPayloadIntegrity {
    Crc32c(u64),
    Unchecked,
}

/// Owner namespace for shared metadata roots.
///
/// Commit groups, checkpoints, and GC roots should be keyed by mapping owner so
/// block and native file mapping layers can share substrate machinery without
/// being implemented on top of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MappingOwner {
    BlockDevice(DeviceId),
    NativeKeyspace(KeyspaceId),
}

/// Current committed block-device root set.
///
/// A `DeviceHead` is the durable read view for a block device. Providers must
/// treat `generation`, `latest_commit`, and all `shard_roots` as one committed
/// view: readers should never observe only part of a newer root set. Block
/// writes are fenced by the touched shard roots' expected old IDs, so
/// independent shard writes can publish without contending on the whole-device
/// generation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeviceHead {
    pub device_id: DeviceId,
    pub generation: DeviceGeneration,
    pub shard_roots: Vec<MetadataNodeId>,
    pub latest_commit: CommitSeq,
}

/// Current committed native keyspace catalog shard set.
///
/// A `KeyspaceHead` is the reconstructed read view for the native
/// filesystem-like API. File creates/writes/appends are fenced by the owning
/// file head and publish by replacing exactly one catalog shard root, so
/// independent files in different catalog shards do not converge on one live
/// keyspace-root object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceHead {
    pub keyspace_id: KeyspaceId,
    pub generation: KeyspaceGeneration,
    pub shard_roots: Vec<KeyspaceCatalogShardId>,
    pub file_count: usize,
    pub latest_commit: CommitSeq,
}

impl DeviceHead {
    /// Validate the committed head shape for a fixed-shard device lineage.
    pub fn validate(&self, expected_shard_count: usize) -> Result<()> {
        if expected_shard_count == 0 {
            return Err(StorageError::invalid_argument(
                "expected shard count must be greater than zero",
            ));
        }

        if self.shard_roots.len() != expected_shard_count {
            return Err(StorageError::invalid_argument(format!(
                "device head has {} shard roots, expected {expected_shard_count}",
                self.shard_roots.len()
            )));
        }

        Ok(())
    }
}

/// Current committed native file root inside a keyspace catalog.
///
/// A `FileHead` is published by replacing the containing immutable
/// `KeyspaceRoot`. Providers must advance `version`, `root`, `size`, and
/// `latest_commit` together so stale append writers can be rejected by
/// version/epoch fencing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileHead {
    pub file_id: FileId,
    pub version: FileVersion,
    pub root: MetadataNodeId,
    pub size: u64,
    pub latest_commit: CommitSeq,
}

impl FileHead {
    /// Validate the current file head against its root coverage.
    ///
    /// The caller supplies root coverage because the head stores only the root
    /// ID. Provider implementations must fetch the root node and pass its
    /// covered range here before accepting a committed head.
    pub fn validate_current(&self, root_coverage: BlockRange, block_size: u32) -> Result<()> {
        let capacity = byte_capacity(root_coverage, block_size)?;

        if self.size > capacity {
            return Err(StorageError::invalid_argument(
                "file size exceeds current root coverage",
            ));
        }

        Ok(())
    }

    /// Validate a file-head transition against the previous committed head.
    pub fn validate_transition_from(
        &self,
        previous: &Self,
        root_coverage: BlockRange,
        block_size: u32,
    ) -> Result<()> {
        self.validate_current(root_coverage, block_size)?;

        if self.file_id != previous.file_id {
            return Err(StorageError::invalid_argument(
                "file-head transition changed file_id",
            ));
        }

        if self.version.raw() < previous.version.raw() {
            return Err(StorageError::invalid_argument(
                "file version must not regress",
            ));
        }

        if self.latest_commit.raw() < previous.latest_commit.raw() {
            return Err(StorageError::invalid_argument(
                "file latest_commit must not regress",
            ));
        }

        if (self.root != previous.root || self.size != previous.size)
            && self.version.raw() == previous.version.raw()
        {
            return Err(StorageError::invalid_argument(
                "file root or size changes must advance file version",
            ));
        }

        Ok(())
    }
}

/// Immutable native keyspace catalog entry.
///
/// File creation metadata lives with the committed file head in the immutable
/// keyspace catalog. Snapshots and restores therefore copy the entire namespace
/// view by root pointer, not by consulting side tables.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceFile {
    pub name: Option<String>,
    pub head: FileHead,
}

/// Immutable native keyspace catalog shard.
///
/// A shard owns a deterministic subset of the files in one immutable keyspace
/// root. Updating one file creates one fresh shard body and one fresh
/// `KeyspaceRoot`; untouched shard bodies remain shared by root ID.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceCatalogShard {
    pub shard_id: KeyspaceCatalogShardId,
    pub files: BTreeMap<FileId, KeyspaceFile>,
}

impl KeyspaceCatalogShard {
    pub fn validate(&self) -> Result<()> {
        for (file_id, entry) in &self.files {
            if *file_id != entry.head.file_id {
                return Err(StorageError::invalid_argument(
                    "keyspace catalog key does not match file head",
                ));
            }
        }
        Ok(())
    }
}

/// Immutable native keyspace catalog checkpoint root.
///
/// Live native file publishes replace one shard in `KeyspaceHead`. A
/// `KeyspaceRoot` materializes a point-in-time shard vector for checkpoints,
/// snapshots, and restore replay anchors; ordinary file writes do not allocate
/// one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceRoot {
    pub root_id: KeyspaceRootId,
    pub shard_roots: Arc<[KeyspaceCatalogShardId]>,
    pub file_count: usize,
}

impl KeyspaceRoot {
    pub fn validate(&self) -> Result<()> {
        if self.shard_roots.is_empty() {
            return Err(StorageError::invalid_argument(
                "keyspace catalog root must include at least one shard",
            ));
        }

        Ok(())
    }
}

/// Immutable metadata tree node.
///
/// A node ID names exactly one node body. Providers may accept an identical
/// duplicate persist, but different content for an existing ID must fail.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataNode {
    pub node_id: MetadataNodeId,
    pub covered_range: BlockRange,
    pub kind: MetadataNodeKind,
}

impl MetadataNode {
    /// Validate metadata node shape and local references.
    ///
    /// Segment descriptors are supplied explicitly so validation stays pure and
    /// provider-free. Empty leaf entries are valid for sparse regions; empty
    /// covered node ranges are not.
    pub fn validate(&self, segments: &[SegmentDescriptor]) -> Result<()> {
        self.covered_range.validate_non_empty()?;

        match &self.kind {
            MetadataNodeKind::Internal { children } => {
                validate_child_ranges(self.covered_range, children)
            }
            MetadataNodeKind::Leaf {
                entries,
                run_extents,
            } => {
                validate_leaf_entries(self.covered_range, entries, segments)?;
                validate_run_backed_file_extents(run_extents)
            }
        }
    }
}

/// Metadata node payload.
///
/// Internal children and leaf entries are immutable after publication. Phase 2
/// validation will enforce ordering, coverage, and overlap invariants.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetadataNodeKind {
    Internal {
        children: Vec<MetadataChild>,
    },
    Leaf {
        entries: Vec<LeafEntry>,
        run_extents: Vec<RunBackedFileExtent>,
    },
}

/// Child pointer in an internal metadata node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataChild {
    pub range: BlockRange,
    pub node_id: MetadataNodeId,
}

/// Logical range to immutable segment slice mapping.
///
/// Leaf entries must be sorted, non-overlapping, non-empty, and bounded by the
/// referenced segment descriptor once validation is implemented.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeafEntry {
    pub logical_start: BlockIndex,
    pub blocks: BlockCount,
    pub segment_id: SegmentId,
    pub segment_offset: BlockIndex,
}

impl LeafEntry {
    pub const fn logical_range(&self) -> BlockRange {
        BlockRange::new(self.logical_start, self.blocks)
    }

    pub fn segment_end_exclusive(&self) -> Result<BlockIndex> {
        self.segment_offset
            .raw()
            .checked_add(self.blocks.raw())
            .map(BlockIndex::from_raw)
            .ok_or_else(|| StorageError::invalid_argument("segment slice overflows u64"))
    }

    pub fn validate_against(
        &self,
        covered_range: BlockRange,
        descriptor: &SegmentDescriptor,
    ) -> Result<()> {
        self.logical_range().validate_non_empty()?;

        if !covered_range.contains_range(self.logical_range())? {
            return Err(StorageError::invalid_argument(
                "leaf entry is outside node covered range",
            ));
        }

        if self.segment_id != descriptor.segment_id {
            return Err(StorageError::invalid_argument(
                "leaf entry segment_id does not match descriptor",
            ));
        }

        if self.segment_end_exclusive()?.raw() > descriptor.blocks.raw() {
            return Err(StorageError::invalid_argument(
                "leaf entry segment slice exceeds segment bounds",
            ));
        }

        Ok(())
    }
}

/// Durable append-run manifest owned by a storage-node append lane.
///
/// A run is private append-stream data until a stream publish attaches a covered
/// range to visible native file metadata. Unlike an ordinary immutable segment,
/// the run identity describes bytes already present in a storage-node append
/// log. A publish may coalesce adjacent compatible runs into one visible extent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendLogRun {
    pub run_id: AppendRunId,
    pub storage_node: StorageNodeId,
    pub stream_id: AppendStreamId,
    pub writer_epoch: WriterEpoch,
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub file_offset_start: u64,
    pub payload_len: u64,
    pub log_id: u64,
    pub log_payload_offset: u64,
    pub log_record_bytes: u64,
    pub integrity: SegmentPayloadIntegrity,
}

impl AppendLogRun {
    pub fn validate(&self) -> Result<()> {
        if self.payload_len == 0 {
            return Err(StorageError::invalid_argument(
                "append log run must contain bytes",
            ));
        }
        self.file_offset_end()?;
        self.log_payload_end()?;
        if self.log_record_bytes < self.payload_len {
            return Err(StorageError::invalid_argument(
                "append log record must cover payload bytes",
            ));
        }
        Ok(())
    }

    pub fn file_offset_end(&self) -> Result<u64> {
        self.file_offset_start
            .checked_add(self.payload_len)
            .ok_or_else(|| StorageError::invalid_argument("append run file range overflows"))
    }

    pub fn log_payload_end(&self) -> Result<u64> {
        self.log_payload_offset
            .checked_add(self.payload_len)
            .ok_or_else(|| StorageError::invalid_argument("append run log range overflows"))
    }

    pub fn full_range(&self) -> AppendLogRunRange {
        AppendLogRunRange {
            run_id: self.run_id,
            storage_node: self.storage_node,
            stream_id: self.stream_id,
            writer_epoch: self.writer_epoch,
            keyspace_id: self.keyspace_id,
            file_id: self.file_id,
            file_offset_start: self.file_offset_start,
            payload_len: self.payload_len,
            log_id: self.log_id,
            log_payload_offset: self.log_payload_offset,
            integrity: self.integrity,
        }
    }
}

/// Visible or private byte range inside an append log run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendLogRunRange {
    pub run_id: AppendRunId,
    pub storage_node: StorageNodeId,
    pub stream_id: AppendStreamId,
    pub writer_epoch: WriterEpoch,
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub file_offset_start: u64,
    pub payload_len: u64,
    pub log_id: u64,
    pub log_payload_offset: u64,
    pub integrity: SegmentPayloadIntegrity,
}

impl AppendLogRunRange {
    pub fn validate(&self) -> Result<()> {
        if self.payload_len == 0 {
            return Err(StorageError::invalid_argument(
                "append log run range must contain bytes",
            ));
        }
        self.file_offset_end()?;
        self.log_payload_end()?;
        Ok(())
    }

    pub fn file_offset_end(&self) -> Result<u64> {
        self.file_offset_start
            .checked_add(self.payload_len)
            .ok_or_else(|| StorageError::invalid_argument("append run file range overflows"))
    }

    pub fn log_payload_end(&self) -> Result<u64> {
        self.log_payload_offset
            .checked_add(self.payload_len)
            .ok_or_else(|| StorageError::invalid_argument("append run log range overflows"))
    }

    fn can_coalesce_with(&self, next: &Self) -> Result<bool> {
        Ok(self.run_id == next.run_id
            && self.storage_node == next.storage_node
            && self.stream_id == next.stream_id
            && self.writer_epoch == next.writer_epoch
            && self.keyspace_id == next.keyspace_id
            && self.file_id == next.file_id
            && self.log_id == next.log_id
            && self.integrity == next.integrity
            && self.file_offset_end()? == next.file_offset_start
            && self.log_payload_end()? == next.log_payload_offset)
    }
}

/// Coalesce adjacent compatible append-run ranges in deterministic file order.
///
/// Gaps are preserved as separate ranges. Overlaps are invalid because a
/// visible file extent set must not contain ambiguous bytes.
pub fn coalesce_append_log_run_ranges(
    mut ranges: Vec<AppendLogRunRange>,
) -> Result<Vec<AppendLogRunRange>> {
    ranges.sort_by_key(|range| {
        (
            range.file_offset_start,
            range.storage_node.raw(),
            range.log_id,
            range.log_payload_offset,
            range.run_id.raw(),
        )
    });

    let mut coalesced: Vec<AppendLogRunRange> = Vec::new();
    for range in ranges {
        range.validate()?;
        if let Some(previous) = coalesced.last_mut() {
            if range.file_offset_start < previous.file_offset_end()? {
                return Err(StorageError::invalid_argument(
                    "append run ranges must not overlap",
                ));
            }
            if previous.can_coalesce_with(&range)? {
                previous.payload_len = previous
                    .payload_len
                    .checked_add(range.payload_len)
                    .ok_or_else(|| {
                        StorageError::invalid_argument("append run coalesced length overflows")
                    })?;
                continue;
            }
        }
        coalesced.push(range);
    }
    Ok(coalesced)
}

pub fn validate_run_backed_file_extents(extents: &[RunBackedFileExtent]) -> Result<()> {
    let mut previous_end = None;
    for extent in extents {
        extent.validate()?;
        if let Some(previous_end) = previous_end
            && extent.file_offset_start < previous_end
        {
            return Err(StorageError::invalid_argument(
                "run-backed file extents must not overlap",
            ));
        }
        previous_end = Some(
            extent
                .file_offset_start
                .checked_add(extent.payload_len)
                .ok_or_else(|| {
                    StorageError::invalid_argument("run-backed file extent range overflows")
                })?,
        );
    }
    Ok(())
}

/// Native file extent backed directly by an append-log run range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunBackedFileExtent {
    pub file_offset_start: u64,
    pub payload_len: u64,
    pub run: AppendLogRunRange,
}

impl RunBackedFileExtent {
    pub fn validate(&self) -> Result<()> {
        self.run.validate()?;
        if self.payload_len == 0 {
            return Err(StorageError::invalid_argument(
                "run-backed file extent must contain bytes",
            ));
        }
        if self.file_offset_start != self.run.file_offset_start
            || self.payload_len != self.run.payload_len
        {
            return Err(StorageError::invalid_argument(
                "run-backed file extent range must match run range",
            ));
        }
        Ok(())
    }
}

/// File-scoped durable record for one visible native append publish.
///
/// This is the replay primitive for decoupling append publish from the global
/// native metadata delta journal. The record is scoped to a single file lineage:
/// replay may apply it only when the reconstructed file head still matches the
/// base version and size, and all extents cover the appended suffix exactly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendVisiblePublish {
    pub record_id: AppendPublishTicketId,
    pub commit_seq: CommitSeq,
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub base_writer_epoch: WriterEpoch,
    pub writer_epoch: WriterEpoch,
    pub base_file_version: FileVersion,
    pub new_file_version: FileVersion,
    pub old_size: u64,
    pub new_size: u64,
    pub publish_through: u64,
    pub run_extents: Vec<RunBackedFileExtent>,
}

impl AppendVisiblePublish {
    pub fn validate(&self) -> Result<()> {
        if self.commit_seq.raw() == 0 {
            return Err(StorageError::invalid_argument(
                "append visible publish commit sequence must be nonzero",
            ));
        }
        if self.new_size <= self.old_size {
            return Err(StorageError::invalid_argument(
                "append visible publish must advance file size",
            ));
        }
        if self.writer_epoch.raw() < self.base_writer_epoch.raw() {
            return Err(StorageError::invalid_argument(
                "append visible publish writer epoch must not precede base epoch",
            ));
        }
        if self.publish_through != self.new_size {
            return Err(StorageError::invalid_argument(
                "append visible publish target must match new file size",
            ));
        }
        let expected_version = self.base_file_version.raw().checked_add(1).ok_or_else(|| {
            StorageError::invalid_argument("append visible publish file version overflows")
        })?;
        if self.new_file_version.raw() != expected_version {
            return Err(StorageError::invalid_argument(
                "append visible publish must advance file version once",
            ));
        }
        if self.run_extents.is_empty() {
            return Err(StorageError::invalid_argument(
                "append visible publish must include run extents",
            ));
        }

        let mut next_offset = self.old_size;
        for extent in &self.run_extents {
            extent.validate()?;
            if extent.file_offset_start != next_offset {
                return Err(StorageError::invalid_argument(
                    "append visible publish extents must be contiguous",
                ));
            }
            if extent.run.keyspace_id != self.keyspace_id
                || extent.run.file_id != self.file_id
                || extent.run.writer_epoch != self.writer_epoch
            {
                return Err(StorageError::invalid_argument(
                    "append visible publish extent belongs to a different file lineage",
                ));
            }
            next_offset = next_offset.checked_add(extent.payload_len).ok_or_else(|| {
                StorageError::invalid_argument("append visible publish extent range overflows")
            })?;
            if next_offset > self.new_size {
                return Err(StorageError::invalid_argument(
                    "append visible publish extents exceed new file size",
                ));
            }
        }
        if next_offset != self.new_size {
            return Err(StorageError::invalid_argument(
                "append visible publish extents do not cover published suffix",
            ));
        }
        Ok(())
    }

    pub fn validate_for_reconstructed_head(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        _file_writer_epoch: WriterEpoch,
        file_version: FileVersion,
        size: u64,
        latest_commit: CommitSeq,
    ) -> Result<()> {
        self.validate()?;
        if self.keyspace_id != keyspace_id
            || self.file_id != file_id
            || self.base_file_version != file_version
            || self.old_size != size
        {
            return Err(StorageError::invalid_argument(
                "append visible publish record does not match reconstructed file head",
            ));
        }
        if self.commit_seq.raw() <= latest_commit.raw() {
            return Err(StorageError::invalid_argument(
                "append visible publish commit sequence does not advance reconstructed file head",
            ));
        }
        Ok(())
    }
}

/// Result of replaying file-scoped visible append publish records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendVisiblePublishReplay {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub base_writer_epoch: WriterEpoch,
    pub base_file_version: FileVersion,
    pub base_size: u64,
    pub base_commit_seq: CommitSeq,
    pub latest_writer_epoch: WriterEpoch,
    pub latest_file_version: FileVersion,
    pub latest_size: u64,
    pub latest_commit_seq: CommitSeq,
    pub run_extents: Vec<RunBackedFileExtent>,
}

/// Replay one file lineage's append-visible records without a global cursor.
///
/// Records may arrive in any deterministic storage order. Replay applies the
/// record whose base version and old size match the current file state, then
/// advances to that record's new version and size. Any leftover record is a gap,
/// duplicate, stale publish, or foreign-file record and is rejected.
pub fn replay_append_visible_publishes_for_file(
    keyspace_id: KeyspaceId,
    file_id: FileId,
    base_writer_epoch: WriterEpoch,
    base_file_version: FileVersion,
    base_size: u64,
    base_commit_seq: CommitSeq,
    records: &[AppendVisiblePublish],
) -> Result<AppendVisiblePublishReplay> {
    let mut remaining = records.to_vec();
    remaining.sort_by_key(|record| {
        (
            record.base_file_version.raw(),
            record.old_size,
            record.new_file_version.raw(),
            record.record_id.raw(),
        )
    });

    let mut latest_writer_epoch = base_writer_epoch;
    let mut latest_file_version = base_file_version;
    let mut latest_size = base_size;
    let mut latest_commit_seq = base_commit_seq;
    let mut run_extents = Vec::new();
    while let Some(index) = remaining.iter().position(|record| {
        record.base_file_version == latest_file_version && record.old_size == latest_size
    }) {
        let record = remaining.remove(index);
        record.validate_for_reconstructed_head(
            keyspace_id,
            file_id,
            latest_writer_epoch,
            latest_file_version,
            latest_size,
            latest_commit_seq,
        )?;
        latest_writer_epoch = latest_writer_epoch.max(record.writer_epoch);
        latest_file_version = record.new_file_version;
        latest_size = record.new_size;
        latest_commit_seq = record.commit_seq;
        run_extents.extend(record.run_extents);
    }

    if !remaining.is_empty() {
        return Err(StorageError::invalid_argument(
            "append visible publish replay is not contiguous",
        ));
    }

    Ok(AppendVisiblePublishReplay {
        keyspace_id,
        file_id,
        base_writer_epoch,
        base_file_version,
        base_size,
        base_commit_seq,
        latest_writer_epoch,
        latest_file_version,
        latest_size,
        latest_commit_seq,
        run_extents,
    })
}

/// Immutable data segment descriptor.
///
/// A descriptor describes committed bytes. Segment stores must not mutate the
/// bytes behind an existing segment ID. This is logical segment identity, not
/// physical placement; one segment may have one local replica in v1 and many
/// replica placements in a later replicated implementation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub blocks: BlockCount,
    pub bytes: u64,
    pub integrity: SegmentPayloadIntegrity,
}

impl SegmentDescriptor {
    pub fn validate(&self, block_size: u32) -> Result<()> {
        if block_size == 0 {
            return Err(StorageError::invalid_argument(
                "block_size must be greater than zero",
            ));
        }

        if !block_size.is_power_of_two() {
            return Err(StorageError::invalid_argument(
                "block_size must be a power of two",
            ));
        }

        if self.blocks.raw() == 0 {
            return Err(StorageError::invalid_argument(
                "segment must contain at least one block",
            ));
        }

        let expected_bytes = self
            .blocks
            .raw()
            .checked_mul(u64::from(block_size))
            .ok_or_else(|| StorageError::invalid_argument("segment byte size overflows u64"))?;

        if self.bytes != expected_bytes {
            return Err(StorageError::invalid_argument(
                "segment byte size must equal blocks * block_size",
            ));
        }

        Ok(())
    }
}

/// Single shard-root replacement in a block-device commit group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardRootUpdate {
    pub shard_id: ShardId,
    pub old_root: MetadataNodeId,
    pub new_root: MetadataNodeId,
}

/// Metadata root update inside a commit group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RootUpdate {
    BlockShard(ShardRootUpdate),
    FileCreated {
        file_id: FileId,
        new_root: MetadataNodeId,
        new_size: u64,
    },
    FileRoot {
        file_id: FileId,
        old_root: MetadataNodeId,
        new_root: MetadataNodeId,
        new_size: u64,
    },
}

/// Durable metadata publication record.
///
/// A commit group records the root updates that became visible atomically for a
/// single mapping owner. It is the replay/PITR unit for committed root changes.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommitGroup {
    pub commit_group: CommitGroupId,
    pub commit_seq: CommitSeq,
    pub owner: MappingOwner,
    pub updates: Vec<RootUpdate>,
}

/// Durable fork catalog record.
///
/// Fork records capture the O(1) root-pointer copy that created a child device.
/// They are intentionally separate from shard-root commit groups because a fork
/// creates a new owner without changing the source device's roots.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForkRecord {
    pub commit_seq: CommitSeq,
    pub source: DeviceId,
    pub target: DeviceId,
    pub shard_roots: Vec<MetadataNodeId>,
}

/// Append-only block-device shard-root timeline record.
///
/// A multi-shard public write creates one `ShardCommit` per changed shard, all
/// sharing the same commit sequence and commit-group identity. PITR replay
/// starts from a checkpoint and applies these records in commit order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardCommit {
    pub commit_seq: CommitSeq,
    pub commit_group: CommitGroupId,
    pub time: LogicalTime,
    pub device_id: DeviceId,
    pub shard_id: ShardId,
    pub old_root: MetadataNodeId,
    pub new_root: MetadataNodeId,
}

/// Append-only native keyspace catalog-shard timeline record.
///
/// A native file create/write/append publishes one new catalog shard root.
/// PITR replay starts from a native keyspace checkpoint and applies these
/// records in commit order, replacing only the touched shard.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceCommit {
    pub commit_seq: CommitSeq,
    pub commit_group: CommitGroupId,
    pub time: LogicalTime,
    pub keyspace_id: KeyspaceId,
    pub shard_index: u32,
    pub old_shard: KeyspaceCatalogShardId,
    pub new_shard: KeyspaceCatalogShardId,
    pub old_file_count: usize,
    pub new_file_count: usize,
}

/// Append-only native file-root audit record inside a keyspace commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileCommit {
    pub commit_seq: CommitSeq,
    pub commit_group: CommitGroupId,
    pub time: LogicalTime,
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub old_root: Option<MetadataNodeId>,
    pub new_root: MetadataNodeId,
    pub old_version: Option<FileVersion>,
    pub new_version: FileVersion,
    pub old_size: u64,
    pub new_size: u64,
}

/// Append-only device deletion timeline record.
///
/// Deleting a device removes it from the live catalog but records the roots
/// that were live at the deletion point. GC policy decides whether those roots
/// remain retained.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteRecord {
    pub commit_seq: CommitSeq,
    pub time: LogicalTime,
    pub device_id: DeviceId,
    pub shard_roots: Vec<MetadataNodeId>,
}

/// Root payload for a durable PITR checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CheckpointRoots {
    BlockShard(Vec<MetadataNodeId>),
    NativeKeyspace(KeyspaceRootId),
}

/// Durable PITR checkpoint.
///
/// Checkpoints summarize owner roots at a commit sequence so restore can replay
/// only later append-only commit records.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub checkpoint_id: CheckpointId,
    pub commit_seq: CommitSeq,
    pub time: LogicalTime,
    pub owner: MappingOwner,
    pub roots: CheckpointRoots,
}

fn byte_capacity(range: BlockRange, block_size: u32) -> Result<u64> {
    range.validate_non_empty()?;

    if block_size == 0 {
        return Err(StorageError::invalid_argument(
            "block_size must be greater than zero",
        ));
    }

    if !block_size.is_power_of_two() {
        return Err(StorageError::invalid_argument(
            "block_size must be a power of two",
        ));
    }

    range
        .blocks
        .raw()
        .checked_mul(u64::from(block_size))
        .ok_or_else(|| StorageError::invalid_argument("range byte capacity overflows u64"))
}

fn validate_child_ranges(covered_range: BlockRange, children: &[MetadataChild]) -> Result<()> {
    if children.is_empty() {
        return Err(StorageError::invalid_argument(
            "internal metadata node must have children",
        ));
    }

    let mut previous: Option<BlockRange> = None;
    let mut next_expected_start = covered_range.start.raw();

    for child in children {
        child.range.validate_non_empty()?;

        if !covered_range.contains_range(child.range)? {
            return Err(StorageError::invalid_argument(
                "metadata child range is outside node covered range",
            ));
        }

        if let Some(previous) = previous {
            if child.range.start.raw() < previous.start.raw() {
                return Err(StorageError::invalid_argument(
                    "metadata child ranges must be sorted",
                ));
            }

            let previous_end = previous.end_exclusive()?.raw();
            if child.range.start.raw() < previous_end {
                return Err(StorageError::invalid_argument(
                    "metadata child ranges must not overlap",
                ));
            }

            if child.range.start.raw() > previous_end {
                return Err(StorageError::invalid_argument(
                    "metadata child ranges must cover the parent without gaps",
                ));
            }
        } else if child.range.start.raw() != next_expected_start {
            return Err(StorageError::invalid_argument(
                "first metadata child must start at the parent start",
            ));
        }

        next_expected_start = child.range.end_exclusive()?.raw();
        previous = Some(child.range);
    }

    if next_expected_start != covered_range.end_exclusive()?.raw() {
        return Err(StorageError::invalid_argument(
            "metadata child ranges must end at the parent end",
        ));
    }

    Ok(())
}

fn validate_leaf_entries(
    covered_range: BlockRange,
    entries: &[LeafEntry],
    segments: &[SegmentDescriptor],
) -> Result<()> {
    let mut previous: Option<BlockRange> = None;

    for entry in entries {
        let descriptor = segments
            .iter()
            .find(|segment| segment.segment_id == entry.segment_id)
            .ok_or_else(|| {
                StorageError::invalid_argument("leaf entry references missing segment")
            })?;

        entry.validate_against(covered_range, descriptor)?;

        let range = entry.logical_range();
        if let Some(previous) = previous {
            if range.start.raw() < previous.start.raw() {
                return Err(StorageError::invalid_argument(
                    "leaf entries must be sorted",
                ));
            }

            if range.start.raw() < previous.end_exclusive()?.raw() {
                return Err(StorageError::invalid_argument(
                    "leaf entries must not overlap",
                ));
            }
        }

        previous = Some(range);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK_SIZE: u32 = 4096;

    fn range(start: u64, blocks: u64) -> BlockRange {
        BlockRange::new(BlockIndex::from_raw(start), BlockCount::from_raw(blocks))
    }

    fn segment(segment_id: u128, blocks: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: SegmentId::from_raw(segment_id),
            blocks: BlockCount::from_raw(blocks),
            bytes: blocks * u64::from(BLOCK_SIZE),
            integrity: SegmentPayloadIntegrity::Unchecked,
        }
    }

    fn entry(logical_start: u64, blocks: u64, segment_id: u128, segment_offset: u64) -> LeafEntry {
        LeafEntry {
            logical_start: BlockIndex::from_raw(logical_start),
            blocks: BlockCount::from_raw(blocks),
            segment_id: SegmentId::from_raw(segment_id),
            segment_offset: BlockIndex::from_raw(segment_offset),
        }
    }

    fn append_run_range(file_offset_start: u64, payload_len: u64) -> AppendLogRunRange {
        AppendLogRunRange {
            run_id: AppendRunId::from_raw(1),
            storage_node: StorageNodeId::from_raw(7),
            stream_id: AppendStreamId::from_raw(9),
            writer_epoch: WriterEpoch::from_raw(11),
            keyspace_id: KeyspaceId::from_raw(13),
            file_id: FileId::from_raw(15),
            file_offset_start,
            payload_len,
            log_id: 17,
            log_payload_offset: 4096 + file_offset_start,
            integrity: SegmentPayloadIntegrity::Unchecked,
        }
    }

    #[test]
    fn device_head_validation_requires_fixed_nonzero_shard_count() {
        let head = DeviceHead {
            device_id: DeviceId::from_raw(1),
            generation: DeviceGeneration::from_raw(1),
            shard_roots: vec![MetadataNodeId::from_raw(10), MetadataNodeId::from_raw(11)],
            latest_commit: CommitSeq::from_raw(0),
        };

        assert!(head.validate(2).is_ok());
        assert!(head.validate(0).is_err());
        assert!(head.validate(1).is_err());
        assert!(head.validate(3).is_err());
    }

    #[test]
    fn file_head_validation_rejects_out_of_bounds_size_and_regressing_transition() {
        let previous = FileHead {
            file_id: FileId::from_raw(7),
            version: FileVersion::from_raw(4),
            root: MetadataNodeId::from_raw(1),
            size: 8 * u64::from(BLOCK_SIZE),
            latest_commit: CommitSeq::from_raw(9),
        };
        let current = FileHead {
            version: FileVersion::from_raw(5),
            root: MetadataNodeId::from_raw(2),
            size: 9 * u64::from(BLOCK_SIZE),
            latest_commit: CommitSeq::from_raw(10),
            ..previous.clone()
        };

        assert!(
            current
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_ok()
        );

        let too_large = FileHead {
            size: 17 * u64::from(BLOCK_SIZE),
            ..current.clone()
        };
        assert!(
            too_large
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_err()
        );

        let regressed_version = FileHead {
            version: FileVersion::from_raw(3),
            ..current.clone()
        };
        assert!(
            regressed_version
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_err()
        );

        let regressed_commit = FileHead {
            latest_commit: CommitSeq::from_raw(8),
            ..current.clone()
        };
        assert!(
            regressed_commit
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_err()
        );

        let changed_without_version = FileHead {
            version: previous.version,
            root: MetadataNodeId::from_raw(3),
            size: 9 * u64::from(BLOCK_SIZE),
            latest_commit: CommitSeq::from_raw(10),
            ..previous.clone()
        };
        assert!(
            changed_without_version
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_err()
        );

        let wrong_file = FileHead {
            file_id: FileId::from_raw(8),
            ..current
        };
        assert!(
            wrong_file
                .validate_transition_from(&previous, range(0, 16), BLOCK_SIZE)
                .is_err()
        );
    }

    #[test]
    fn segment_descriptor_validation_checks_shape_and_byte_size() {
        assert!(segment(1, 4).validate(BLOCK_SIZE).is_ok());

        let zero_blocks = SegmentDescriptor {
            blocks: BlockCount::from_raw(0),
            bytes: 0,
            ..segment(1, 4)
        };
        assert!(zero_blocks.validate(BLOCK_SIZE).is_err());

        let byte_mismatch = SegmentDescriptor {
            bytes: 1,
            ..segment(1, 4)
        };
        assert!(byte_mismatch.validate(BLOCK_SIZE).is_err());

        let overflow = SegmentDescriptor {
            blocks: BlockCount::from_raw(u64::MAX),
            bytes: u64::MAX,
            ..segment(1, 4)
        };
        assert!(overflow.validate(BLOCK_SIZE).is_err());

        assert!(segment(1, 4).validate(3000).is_err());
    }

    #[test]
    fn append_log_run_validation_rejects_empty_and_overflowing_ranges() {
        let run = AppendLogRun {
            run_id: AppendRunId::from_raw(1),
            storage_node: StorageNodeId::from_raw(2),
            stream_id: AppendStreamId::from_raw(3),
            writer_epoch: WriterEpoch::from_raw(4),
            keyspace_id: KeyspaceId::from_raw(5),
            file_id: FileId::from_raw(6),
            file_offset_start: 1024,
            payload_len: 4096,
            log_id: 7,
            log_payload_offset: 8192,
            log_record_bytes: 4096 + 64,
            integrity: SegmentPayloadIntegrity::Unchecked,
        };
        assert!(run.validate().is_ok());
        assert_eq!(run.full_range().payload_len, 4096);

        assert!(
            AppendLogRun {
                payload_len: 0,
                ..run.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AppendLogRun {
                file_offset_start: u64::MAX,
                ..run.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AppendLogRun {
                log_record_bytes: 4095,
                ..run
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn append_log_run_ranges_coalesce_only_adjacent_compatible_runs() {
        let one_mib = 1024 * 1024;
        let ranges = vec![
            append_run_range(one_mib, one_mib),
            append_run_range(0, one_mib),
            append_run_range(2 * one_mib, one_mib),
        ];
        let coalesced = coalesce_append_log_run_ranges(ranges).unwrap();
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].file_offset_start, 0);
        assert_eq!(coalesced[0].payload_len, 3 * one_mib);

        let mut different_log = append_run_range(3 * one_mib, one_mib);
        different_log.log_id += 1;
        let coalesced =
            coalesce_append_log_run_ranges(vec![append_run_range(0, one_mib), different_log])
                .unwrap();
        assert_eq!(coalesced.len(), 2);

        let overlap = coalesce_append_log_run_ranges(vec![
            append_run_range(0, one_mib),
            append_run_range(one_mib / 2, one_mib),
        ]);
        assert!(overlap.is_err());
    }

    #[test]
    fn run_backed_file_extent_range_must_match_its_run() {
        let run = append_run_range(4096, 8192);
        assert!(
            RunBackedFileExtent {
                file_offset_start: 4096,
                payload_len: 8192,
                run: run.clone(),
            }
            .validate()
            .is_ok()
        );

        assert!(
            RunBackedFileExtent {
                file_offset_start: 0,
                payload_len: 8192,
                run,
            }
            .validate()
            .is_err()
        );
    }

    fn run_extent(file_offset_start: u64, payload_len: u64) -> RunBackedFileExtent {
        RunBackedFileExtent {
            file_offset_start,
            payload_len,
            run: append_run_range(file_offset_start, payload_len),
        }
    }

    fn append_visible_publish(extents: Vec<RunBackedFileExtent>) -> AppendVisiblePublish {
        AppendVisiblePublish {
            record_id: AppendPublishTicketId::from_raw(101),
            commit_seq: CommitSeq::from_raw(101),
            keyspace_id: KeyspaceId::from_raw(13),
            file_id: FileId::from_raw(15),
            base_writer_epoch: WriterEpoch::from_raw(10),
            writer_epoch: WriterEpoch::from_raw(11),
            base_file_version: FileVersion::from_raw(4),
            new_file_version: FileVersion::from_raw(5),
            old_size: 4096,
            new_size: 4096 + 8192,
            publish_through: 4096 + 8192,
            run_extents: extents,
        }
    }

    fn append_visible_publish_record(
        record_id: u128,
        base_file_version: u64,
        old_size: u64,
        payload_len: u64,
    ) -> AppendVisiblePublish {
        AppendVisiblePublish {
            record_id: AppendPublishTicketId::from_raw(record_id),
            commit_seq: CommitSeq::from_raw(
                100 + u64::try_from(record_id).expect("test record id fits u64"),
            ),
            keyspace_id: KeyspaceId::from_raw(13),
            file_id: FileId::from_raw(15),
            base_writer_epoch: WriterEpoch::from_raw(11),
            writer_epoch: WriterEpoch::from_raw(11),
            base_file_version: FileVersion::from_raw(base_file_version),
            new_file_version: FileVersion::from_raw(base_file_version + 1),
            old_size,
            new_size: old_size + payload_len,
            publish_through: old_size + payload_len,
            run_extents: vec![run_extent(old_size, payload_len)],
        }
    }

    #[test]
    fn append_visible_publish_validation_requires_exact_file_suffix() {
        let publish = append_visible_publish(vec![run_extent(4096, 4096), run_extent(8192, 4096)]);
        assert!(publish.validate().is_ok());
        assert!(
            publish
                .validate_for_reconstructed_head(
                    KeyspaceId::from_raw(13),
                    FileId::from_raw(15),
                    WriterEpoch::from_raw(10),
                    FileVersion::from_raw(4),
                    4096,
                    CommitSeq::from_raw(100),
                )
                .is_ok()
        );

        assert!(
            AppendVisiblePublish {
                publish_through: publish.new_size + 1,
                ..publish.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AppendVisiblePublish {
                new_file_version: FileVersion::from_raw(6),
                ..publish.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AppendVisiblePublish {
                writer_epoch: WriterEpoch::from_raw(12),
                ..publish.clone()
            }
            .validate()
            .is_err()
        );
        let mut skipped_epoch_extents = vec![run_extent(4096, 4096), run_extent(8192, 4096)];
        for extent in &mut skipped_epoch_extents {
            extent.run.writer_epoch = WriterEpoch::from_raw(12);
        }
        let skipped_epoch = AppendVisiblePublish {
            writer_epoch: WriterEpoch::from_raw(12),
            run_extents: skipped_epoch_extents,
            ..publish.clone()
        };
        assert!(skipped_epoch.validate().is_ok());
        assert!(
            skipped_epoch
                .validate_for_reconstructed_head(
                    KeyspaceId::from_raw(13),
                    FileId::from_raw(15),
                    WriterEpoch::from_raw(12),
                    FileVersion::from_raw(4),
                    4096,
                    CommitSeq::from_raw(100),
                )
                .is_ok()
        );
        assert!(
            AppendVisiblePublish {
                run_extents: vec![run_extent(8192, 4096)],
                ..publish.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            AppendVisiblePublish {
                run_extents: vec![run_extent(4096, 4096), run_extent(12_288, 4096)],
                ..publish.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            publish
                .validate_for_reconstructed_head(
                    KeyspaceId::from_raw(13),
                    FileId::from_raw(15),
                    WriterEpoch::from_raw(9),
                    FileVersion::from_raw(4),
                    4096,
                    CommitSeq::from_raw(100),
                )
                .is_ok()
        );
        assert!(
            publish
                .validate_for_reconstructed_head(
                    KeyspaceId::from_raw(13),
                    FileId::from_raw(15),
                    WriterEpoch::from_raw(10),
                    FileVersion::from_raw(4),
                    4096,
                    CommitSeq::from_raw(101),
                )
                .is_err()
        );

        let mut wrong_file = run_extent(4096, 8192);
        wrong_file.run.file_id = FileId::from_raw(99);
        assert!(
            AppendVisiblePublish {
                run_extents: vec![wrong_file],
                ..publish
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn append_visible_publish_replay_applies_file_local_chain_without_global_order() {
        let second = append_visible_publish_record(2, 5, 8192, 4096);
        let first = append_visible_publish_record(1, 4, 4096, 4096);

        let replay = replay_append_visible_publishes_for_file(
            KeyspaceId::from_raw(13),
            FileId::from_raw(15),
            WriterEpoch::from_raw(11),
            FileVersion::from_raw(4),
            4096,
            CommitSeq::from_raw(100),
            &[second, first],
        )
        .unwrap();

        assert_eq!(replay.base_file_version, FileVersion::from_raw(4));
        assert_eq!(replay.base_size, 4096);
        assert_eq!(replay.base_commit_seq, CommitSeq::from_raw(100));
        assert_eq!(replay.base_writer_epoch, WriterEpoch::from_raw(11));
        assert_eq!(replay.latest_writer_epoch, WriterEpoch::from_raw(11));
        assert_eq!(replay.latest_file_version, FileVersion::from_raw(6));
        assert_eq!(replay.latest_size, 12_288);
        assert_eq!(replay.latest_commit_seq, CommitSeq::from_raw(102));
        assert_eq!(
            replay
                .run_extents
                .iter()
                .map(|extent| extent.file_offset_start)
                .collect::<Vec<_>>(),
            vec![4096, 8192]
        );
    }

    #[test]
    fn append_visible_publish_replay_rejects_gaps_duplicates_and_foreign_records() {
        let first = append_visible_publish_record(1, 4, 4096, 4096);
        let duplicate = append_visible_publish_record(2, 4, 4096, 4096);
        assert!(
            replay_append_visible_publishes_for_file(
                KeyspaceId::from_raw(13),
                FileId::from_raw(15),
                WriterEpoch::from_raw(11),
                FileVersion::from_raw(4),
                4096,
                CommitSeq::from_raw(100),
                &[first.clone(), duplicate],
            )
            .is_err()
        );

        let gap = append_visible_publish_record(3, 6, 12_288, 4096);
        assert!(
            replay_append_visible_publishes_for_file(
                KeyspaceId::from_raw(13),
                FileId::from_raw(15),
                WriterEpoch::from_raw(11),
                FileVersion::from_raw(4),
                4096,
                CommitSeq::from_raw(100),
                &[first.clone(), gap],
            )
            .is_err()
        );

        let mut foreign = first;
        foreign.file_id = FileId::from_raw(99);
        assert!(
            replay_append_visible_publishes_for_file(
                KeyspaceId::from_raw(13),
                FileId::from_raw(15),
                WriterEpoch::from_raw(11),
                FileVersion::from_raw(4),
                4096,
                CommitSeq::from_raw(100),
                &[foreign],
            )
            .is_err()
        );

        let first = append_visible_publish_record(4, 4, 4096, 4096);
        let replay = replay_append_visible_publishes_for_file(
            KeyspaceId::from_raw(13),
            FileId::from_raw(15),
            WriterEpoch::from_raw(12),
            FileVersion::from_raw(4),
            4096,
            CommitSeq::from_raw(100),
            &[first],
        )
        .unwrap();
        assert_eq!(replay.latest_writer_epoch, WriterEpoch::from_raw(12));
    }

    #[test]
    fn metadata_leaf_validation_accepts_sorted_non_overlapping_entries() {
        let node = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(10, 20),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(10, 2, 1, 0), entry(15, 3, 2, 4)],
                run_extents: Vec::new(),
            },
        };
        let segments = [segment(1, 10), segment(2, 10)];

        assert!(node.validate(&segments).is_ok());
    }

    #[test]
    fn metadata_leaf_validation_rejects_bad_entry_shapes() {
        let segments = [segment(1, 10)];

        let overlapping = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 4, 1, 0), entry(3, 2, 1, 4)],
                run_extents: Vec::new(),
            },
        };
        assert!(overlapping.validate(&segments).is_err());

        let unsorted = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 30),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(20, 2, 1, 0), entry(10, 2, 1, 2)],
                run_extents: Vec::new(),
            },
        };
        assert!(unsorted.validate(&segments).is_err());

        let zero_length = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 0, 1, 0)],
                run_extents: Vec::new(),
            },
        };
        assert!(zero_length.validate(&segments).is_err());

        let out_of_range = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(9, 2, 1, 0)],
                run_extents: Vec::new(),
            },
        };
        assert!(out_of_range.validate(&segments).is_err());

        let missing_segment = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 2, 99, 0)],
                run_extents: Vec::new(),
            },
        };
        assert!(missing_segment.validate(&segments).is_err());

        let segment_bounds = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 2, 1, 9)],
                run_extents: Vec::new(),
            },
        };
        assert!(segment_bounds.validate(&segments).is_err());

        let segment_slice_overflow = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 1, 1, u64::MAX)],
                run_extents: Vec::new(),
            },
        };
        assert!(segment_slice_overflow.validate(&segments).is_err());
    }

    #[test]
    fn metadata_internal_validation_checks_children() {
        let valid = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 100),
            kind: MetadataNodeKind::Internal {
                children: vec![
                    MetadataChild {
                        range: range(0, 50),
                        node_id: MetadataNodeId::from_raw(2),
                    },
                    MetadataChild {
                        range: range(50, 50),
                        node_id: MetadataNodeId::from_raw(3),
                    },
                ],
            },
        };
        assert!(valid.validate(&[]).is_ok());

        let empty = MetadataNode {
            kind: MetadataNodeKind::Internal {
                children: Vec::new(),
            },
            ..valid.clone()
        };
        assert!(empty.validate(&[]).is_err());

        let overlapping = MetadataNode {
            kind: MetadataNodeKind::Internal {
                children: vec![
                    MetadataChild {
                        range: range(0, 60),
                        node_id: MetadataNodeId::from_raw(2),
                    },
                    MetadataChild {
                        range: range(50, 50),
                        node_id: MetadataNodeId::from_raw(3),
                    },
                ],
            },
            ..valid.clone()
        };
        assert!(overlapping.validate(&[]).is_err());

        let gap = MetadataNode {
            kind: MetadataNodeKind::Internal {
                children: vec![
                    MetadataChild {
                        range: range(0, 10),
                        node_id: MetadataNodeId::from_raw(2),
                    },
                    MetadataChild {
                        range: range(20, 80),
                        node_id: MetadataNodeId::from_raw(3),
                    },
                ],
            },
            ..valid.clone()
        };
        assert!(gap.validate(&[]).is_err());

        let out_of_bounds = MetadataNode {
            kind: MetadataNodeKind::Internal {
                children: vec![MetadataChild {
                    range: range(99, 2),
                    node_id: MetadataNodeId::from_raw(2),
                }],
            },
            ..valid
        };
        assert!(out_of_bounds.validate(&[]).is_err());
    }
}
