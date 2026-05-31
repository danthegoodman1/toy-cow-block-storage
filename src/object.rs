use std::collections::BTreeMap;
use std::sync::Arc;

use crate::api::BlockRange;
use crate::error::{Result, StorageError};
use crate::id::{
    BlockCount, BlockIndex, CheckpointId, CommitGroupId, CommitSeq, DeviceGeneration, DeviceId,
    FileId, FileVersion, KeyspaceCatalogShardId, KeyspaceGeneration, KeyspaceId, KeyspaceRootId,
    LogicalTime, MetadataNodeId, SegmentId, ShardId,
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

/// Current committed native keyspace catalog root.
///
/// A `KeyspaceHead` is the durable publication unit for the native
/// filesystem-like API. Snapshots and restores copy its immutable catalog root
/// pointer, not individual file metadata roots.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceHead {
    pub keyspace_id: KeyspaceId,
    pub generation: KeyspaceGeneration,
    pub root: KeyspaceRootId,
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

/// Immutable native keyspace catalog root.
///
/// The local catalog is sharded so native file publish cost is bounded by one
/// deterministic catalog shard instead of the whole keyspace. The public API
/// still depends only on the immutable catalog root boundary: snapshots and
/// restores copy one `KeyspaceRootId`, while file creates/writes/appends publish
/// one new root plus one changed shard.
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
            MetadataNodeKind::Leaf { entries } => {
                validate_leaf_entries(self.covered_range, entries, segments)
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
    Internal { children: Vec<MetadataChild> },
    Leaf { entries: Vec<LeafEntry> },
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

/// Append-only native keyspace catalog-root timeline record.
///
/// A native file create or append publishes a new immutable keyspace catalog
/// root. PITR replay starts from a native keyspace checkpoint and applies these
/// records in commit order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceCommit {
    pub commit_seq: CommitSeq,
    pub commit_group: CommitGroupId,
    pub time: LogicalTime,
    pub keyspace_id: KeyspaceId,
    pub old_root: KeyspaceRootId,
    pub new_root: KeyspaceRootId,
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
    fn metadata_leaf_validation_accepts_sorted_non_overlapping_entries() {
        let node = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(10, 20),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(10, 2, 1, 0), entry(15, 3, 2, 4)],
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
            },
        };
        assert!(overlapping.validate(&segments).is_err());

        let unsorted = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 30),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(20, 2, 1, 0), entry(10, 2, 1, 2)],
            },
        };
        assert!(unsorted.validate(&segments).is_err());

        let zero_length = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 0, 1, 0)],
            },
        };
        assert!(zero_length.validate(&segments).is_err());

        let out_of_range = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(9, 2, 1, 0)],
            },
        };
        assert!(out_of_range.validate(&segments).is_err());

        let missing_segment = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 2, 99, 0)],
            },
        };
        assert!(missing_segment.validate(&segments).is_err());

        let segment_bounds = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 2, 1, 9)],
            },
        };
        assert!(segment_bounds.validate(&segments).is_err());

        let segment_slice_overflow = MetadataNode {
            node_id: MetadataNodeId::from_raw(1),
            covered_range: range(0, 10),
            kind: MetadataNodeKind::Leaf {
                entries: vec![entry(1, 1, 1, u64::MAX)],
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
