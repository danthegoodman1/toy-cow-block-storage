use crate::api::BlockRange;
use crate::id::{
    BlockCount, BlockIndex, CheckpointId, CommitGroupId, CommitSeq, DeviceGeneration, DeviceId,
    FileId, FileVersion, LogicalTime, MetadataNodeId, SegmentId, ShardId,
};

/// Owner namespace for shared metadata roots.
///
/// Commit groups, checkpoints, and GC roots should be keyed by mapping owner so
/// block and native file mapping layers can share substrate machinery without
/// being implemented on top of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MappingOwner {
    BlockDevice(DeviceId),
    NativeFile(FileId),
}

/// Current committed block-device root set.
///
/// A `DeviceHead` is the durable publication unit for a block device. Providers
/// must treat `generation`, `latest_commit`, and all `shard_roots` as one
/// committed view: readers should never observe only part of a newer root set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceHead {
    pub device_id: DeviceId,
    pub generation: DeviceGeneration,
    pub shard_roots: Vec<MetadataNodeId>,
    pub latest_commit: CommitSeq,
}

/// Current committed native file root.
///
/// A `FileHead` is the durable publication unit for a native file. Providers
/// must advance `version`, `root`, `size`, and `latest_commit` together so
/// stale append writers can be rejected by version/epoch fencing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHead {
    pub file_id: FileId,
    pub version: FileVersion,
    pub root: MetadataNodeId,
    pub size: u64,
    pub latest_commit: CommitSeq,
}

/// Immutable metadata tree node.
///
/// A node ID names exactly one node body. Providers may accept an identical
/// duplicate persist, but different content for an existing ID must fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataNode {
    pub node_id: MetadataNodeId,
    pub covered_range: BlockRange,
    pub kind: MetadataNodeKind,
}

/// Metadata node payload.
///
/// Internal children and leaf entries are immutable after publication. Phase 2
/// validation will enforce ordering, coverage, and overlap invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNodeKind {
    Internal { children: Vec<MetadataChild> },
    Leaf { entries: Vec<LeafEntry> },
}

/// Child pointer in an internal metadata node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataChild {
    pub range: BlockRange,
    pub node_id: MetadataNodeId,
}

/// Logical range to immutable segment slice mapping.
///
/// Leaf entries must be sorted, non-overlapping, non-empty, and bounded by the
/// referenced segment descriptor once validation is implemented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEntry {
    pub logical_start: BlockIndex,
    pub blocks: BlockCount,
    pub segment_id: SegmentId,
    pub segment_offset: BlockIndex,
}

/// Immutable data segment descriptor.
///
/// A descriptor describes committed bytes. Segment stores must not mutate the
/// bytes behind an existing segment ID. This is logical segment identity, not
/// physical placement; one segment may have one local replica in v1 and many
/// replica placements in a later replicated implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub blocks: BlockCount,
    pub bytes: u64,
    pub checksum: Option<u64>,
}

/// Single shard-root replacement in a block-device commit group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRootUpdate {
    pub shard_id: ShardId,
    pub old_root: MetadataNodeId,
    pub new_root: MetadataNodeId,
}

/// Metadata root update inside a commit group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootUpdate {
    BlockShard(ShardRootUpdate),
    FileRoot {
        old_root: MetadataNodeId,
        new_root: MetadataNodeId,
    },
}

/// Durable metadata publication record.
///
/// A commit group records the root updates that became visible atomically for a
/// single mapping owner. It is the replay/PITR unit for committed root changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGroup {
    pub commit_group: CommitGroupId,
    pub commit_seq: CommitSeq,
    pub owner: MappingOwner,
    pub updates: Vec<RootUpdate>,
}

/// Durable PITR checkpoint.
///
/// Checkpoints summarize owner roots at a commit sequence so restore can replay
/// only later append-only commit records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub checkpoint_id: CheckpointId,
    pub commit_seq: CommitSeq,
    pub time: LogicalTime,
    pub owner: MappingOwner,
    pub shard_roots: Vec<MetadataNodeId>,
}
