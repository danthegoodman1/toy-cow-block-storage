use crate::api::BlockRange;
use crate::id::{
    BlockCount, BlockIndex, CheckpointId, CommitGroupId, CommitSeq, DeviceGeneration, DeviceId,
    LogicalTime, MetadataNodeId, SegmentId, ShardId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceHead {
    pub device_id: DeviceId,
    pub generation: DeviceGeneration,
    pub shard_roots: Vec<MetadataNodeId>,
    pub latest_commit: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataNode {
    pub node_id: MetadataNodeId,
    pub covered_range: BlockRange,
    pub kind: MetadataNodeKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataNodeKind {
    Internal { children: Vec<MetadataChild> },
    Leaf { entries: Vec<LeafEntry> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataChild {
    pub range: BlockRange,
    pub node_id: MetadataNodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafEntry {
    pub logical_start: BlockIndex,
    pub blocks: BlockCount,
    pub segment_id: SegmentId,
    pub segment_offset: BlockIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub blocks: BlockCount,
    pub bytes: u64,
    pub checksum: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRootUpdate {
    pub shard_id: ShardId,
    pub old_root: MetadataNodeId,
    pub new_root: MetadataNodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGroup {
    pub commit_group: CommitGroupId,
    pub commit_seq: CommitSeq,
    pub device_id: DeviceId,
    pub updates: Vec<ShardRootUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub checkpoint_id: CheckpointId,
    pub commit_seq: CommitSeq,
    pub time: LogicalTime,
    pub device_id: DeviceId,
    pub shard_roots: Vec<MetadataNodeId>,
}
