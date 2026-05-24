use crate::api::{ByteRange, CreateDeviceRequest, DeleteResult, DeviceSpec, RestorePoint};
use crate::error::Result;
use crate::extent::{CreateFileRequest, CreateKeyspaceRequest, FileInfo, KeyspaceInfo};
use crate::id::{
    CheckpointId, CommitSeq, DeviceGeneration, DeviceId, FileId, FileVersion, KeyspaceId,
    MetadataNodeId, SegmentId, StorageNodeId, WriteIntentId, WriterEpoch,
};
use crate::object::{
    Checkpoint, CommitGroup, DeleteRecord, DeviceHead, FileHead, KeyspaceHead, MappingOwner,
    MetadataNode, RootUpdate, SegmentDescriptor,
};

/// A metadata-root publish request.
///
/// Implementors must treat this as the atomic metadata transition for one owner.
/// For block devices, all shard-root updates in the intent become visible
/// together or none do. For native keyspaces, the catalog-root transition and
/// enclosed file root/version transition become visible together or none does.
/// Successful publishes must be durably replayable before success is returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitGroupIntent {
    pub owner: MappingOwner,
    pub fence: MetadataFence,
    pub updates: Vec<RootUpdate>,
}

/// Fencing token for metadata publishes.
///
/// Implementors must reject a publish when the current owner state no longer
/// matches the supplied fence. Rejection must not partially apply any root
/// update. A stale native writer should be rejected through `WriterEpoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFence {
    DeviceGeneration(DeviceGeneration),
    FileVersion(FileVersion),
    WriterEpoch {
        base_version: FileVersion,
        writer_epoch: WriterEpoch,
    },
}

/// Internal create-device request accepted by the metadata plane.
///
/// The metadata plane may choose internal shard layout. The public `DeviceSpec`
/// intentionally carries only user-visible shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCreateDeviceRequest {
    pub spec: DeviceSpec,
    pub name: Option<String>,
}

/// Internal create-file request accepted by the metadata plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCreateFileRequest {
    pub keyspace_id: KeyspaceId,
    pub request: CreateFileRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCreateKeyspaceRequest {
    pub request: CreateKeyspaceRequest,
}

impl From<CreateDeviceRequest> for MetadataCreateDeviceRequest {
    fn from(request: CreateDeviceRequest) -> Self {
        Self {
            spec: request.spec,
            name: request.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataForkRequest {
    pub source: DeviceId,
    pub target: Option<DeviceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshotKeyspaceRequest {
    pub source: KeyspaceId,
    pub target: Option<KeyspaceId>,
    pub name: Option<String>,
}

/// Retention settings used when enumerating GC roots.
///
/// The policy is deliberately expressed in commit-age terms instead of wall
/// clock time so retention can be simulated and replayed deterministically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Retain every deleted device root indefinitely.
    pub retain_deleted_devices: bool,
    /// When deleted devices are not retained indefinitely, keep their roots
    /// until at least this many commits have elapsed since the delete commit.
    ///
    /// A value of zero makes deleted-device roots eligible immediately. A value
    /// of `N` keeps roots while `current_commit - delete_commit < N`.
    pub deleted_device_grace_commits: u64,
    /// Retain point-in-time restore roots for this many commit-sequence
    /// advancements.
    ///
    /// A value of zero retains no historical PITR roots beyond current live
    /// heads and separately retained deleted roots. A value of `N` keeps restore
    /// points while `current_commit - restore_commit < N`.
    ///
    /// Implementations may materialize a checkpoint at the PITR window floor as
    /// a replay anchor for commits inside the window.
    pub pitr_grace_commits: u64,
}

impl RetentionPolicy {
    /// Retain deleted devices indefinitely.
    pub const fn retain_deleted_devices() -> Self {
        Self {
            retain_deleted_devices: true,
            deleted_device_grace_commits: 0,
            pitr_grace_commits: 0,
        }
    }

    /// Retain deleted devices and PITR history indefinitely.
    pub const fn retain_everything() -> Self {
        Self {
            retain_deleted_devices: true,
            deleted_device_grace_commits: 0,
            pitr_grace_commits: u64::MAX,
        }
    }

    /// Make deleted-device roots eligible as soon as the next safe GC epoch
    /// proves them unreachable.
    pub const fn expire_deleted_immediately() -> Self {
        Self {
            retain_deleted_devices: false,
            deleted_device_grace_commits: 0,
            pitr_grace_commits: 0,
        }
    }

    /// Retain deleted-device roots until the supplied commit age has elapsed.
    pub const fn expire_deleted_after_commits(commits: u64) -> Self {
        Self {
            retain_deleted_devices: false,
            deleted_device_grace_commits: commits,
            pitr_grace_commits: 0,
        }
    }

    /// Return this policy with a deterministic PITR commit-age window.
    pub const fn with_pitr_grace_commits(mut self, commits: u64) -> Self {
        self.pitr_grace_commits = commits;
        self
    }
}

/// Global metadata contract for block and native file mapping layers.
///
/// Minimal implementor guarantees:
///
/// - Metadata objects and published roots are durable before success is returned.
/// - Object IDs are immutable: writing or persisting an existing ID with
///   different content must fail rather than mutate the object.
/// - Root publishes are atomic per `CommitGroupIntent`.
/// - Root publishes are fenced by `MetadataFence`; stale fences fail without
///   partial visibility.
/// - Reads observe the latest successful publish for the requested owner.
/// - GC root enumeration includes every live owner root and every retained PITR
///   root required by the supplied retention policy.
/// - The metadata plane never deletes local segment bytes directly; it only
///   records metadata reachability and release evidence for custodians.
pub trait MetadataPlane: Send + Sync {
    /// Create a block device and publish its initial empty roots.
    ///
    /// Success means the returned `DeviceHead` is durable and visible to
    /// subsequent `get_head` calls. Implementors may choose internal shard
    /// layout, but every live device must have a complete root set.
    fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead>;

    /// Create a native keyspace and publish its initial empty catalog root.
    ///
    /// Success means the returned `KeyspaceHead` is durable and visible to
    /// subsequent native keyspace/file operations.
    fn create_keyspace(&self, request: MetadataCreateKeyspaceRequest) -> Result<KeyspaceHead>;

    /// Return the latest committed native keyspace head.
    fn get_keyspace_head(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceHead>;

    /// Return user-facing native keyspace information derived from committed
    /// state.
    fn get_keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo>;

    /// Create a native file inside a keyspace and publish a new keyspace
    /// catalog root containing its initial empty file root.
    ///
    /// Success means the returned `FileHead` is durable, versioned, and visible
    /// to subsequent keyspace-scoped `get_file_head` calls.
    fn create_file(&self, request: MetadataCreateFileRequest) -> Result<FileHead>;

    /// Return the latest committed block device head.
    ///
    /// Must not synthesize uncommitted or partially published state. Deleted
    /// devices are absent from this live-head lookup.
    fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead>;

    /// List live block devices in deterministic ID order.
    ///
    /// Success returns only devices with currently visible live heads. Deleted
    /// devices must not appear, even if their retained PITR roots remain GC
    /// roots under the active retention policy.
    fn list_live_devices(&self) -> Result<Vec<DeviceId>>;

    /// List deleted block devices in deterministic ID order.
    ///
    /// Success returns devices removed from the live catalog whose metadata
    /// history may still be retained by PITR policy. This is catalog evidence
    /// for custodians; it must not make the device readable through live block
    /// APIs.
    fn list_deleted_devices(&self) -> Result<Vec<DeviceId>>;

    /// Return the latest committed native file head.
    ///
    /// Must not return a head from a stale append lease or failed append commit.
    fn get_file_head(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileHead>;

    /// Return user-facing native file information derived from committed state.
    fn get_file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo>;

    /// Persist an immutable metadata node.
    ///
    /// Implementors may make identical writes idempotent. A write of different
    /// content to an existing `node_id` must fail.
    fn persist_metadata_node(&self, node: MetadataNode) -> Result<()>;

    /// Fetch an immutable metadata node by ID.
    fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode>;

    /// Atomically publish a fenced metadata commit group.
    ///
    /// Implementors must verify all referenced metadata nodes exist before
    /// publishing. A successful return means the commit group is durable,
    /// ordered, and visible to reads/replay. A failed return must leave the
    /// previous committed roots visible.
    fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup>;

    /// Fork a block device by copying current shard-root pointers.
    ///
    /// Success must be O(1) with respect to logical device size and metadata
    /// tree size. It must not walk leaves or bump deep segment references.
    fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead>;

    /// Restore a block device to a retained point in time.
    ///
    /// Restore creates a new committed head rather than mutating historical
    /// state. Missing or expired restore points must fail cleanly.
    fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceHead>;

    /// Snapshot the current native keyspace catalog into a new keyspace.
    ///
    /// Success copies the source keyspace's immutable catalog root pointer and
    /// does not walk file metadata leaves or bump deep segment references.
    fn snapshot_keyspace(&self, request: MetadataSnapshotKeyspaceRequest) -> Result<KeyspaceHead>;

    /// Restore a native keyspace to a retained point in time as a new keyspace.
    ///
    /// Restore creates a new committed keyspace head rather than mutating
    /// historical state. Missing or expired restore points must fail cleanly.
    fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceHead>;

    /// Remove a block device from the live catalog and append deletion evidence.
    ///
    /// Success means `get_head`, live listings, and public block operations no
    /// longer observe the device as live. It must not synchronously delete
    /// metadata nodes or segment bytes; retained PITR roots and later GC decide
    /// reachability and physical release.
    fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult>;

    /// Fetch a durable device deletion record by commit sequence.
    ///
    /// The record is append-only evidence for retention and GC. Missing records
    /// must fail cleanly rather than inventing deletion state.
    fn get_delete_record(&self, commit_seq: CommitSeq) -> Result<DeleteRecord>;

    /// Write a durable checkpoint for a block device.
    ///
    /// The checkpoint must be replay-consistent with the commit sequence it
    /// reports.
    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId>;

    /// Write a durable checkpoint for a native keyspace.
    ///
    /// The checkpoint captures one immutable keyspace catalog root, making
    /// restore filesystem-level instead of per-file.
    fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId>;

    /// Fetch a durable checkpoint by ID.
    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint>;

    /// Enumerate metadata roots that must be treated as live for GC.
    ///
    /// Must include live block roots, live native keyspace file roots, and
    /// retained PITR roots required by `policy`. Returning too few roots is
    /// data loss.
    ///
    /// Implementors may create deterministic replay-anchor checkpoints required
    /// by the retention policy before returning roots, but must not make user
    /// data visible or free any object from this call alone.
    fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>>;
}

/// Immutable segment byte storage.
///
/// A `SegmentStore` represents one storage endpoint or placement domain. Future
/// replicated writes should compose multiple `SegmentStore` calls at the
/// server/placement-coordinator layer, then publish metadata only after the
/// requested replica durability is satisfied. The public block/native clients
/// should not fan out replica writes themselves.
///
/// Minimal implementor guarantees:
///
/// - Segment bytes are immutable once successfully written.
/// - `write_segment` must write only into the supplied reservation.
/// - `sync_segment` must make the segment durable enough for metadata to safely
///   reference it according to the selected durability policy.
/// - Reads must never expose bytes from failed or uncommitted writes.
pub trait SegmentStore: Send + Sync {
    /// Write segment bytes for a local reservation and return descriptor plus
    /// replica placement information.
    ///
    /// Success means the bytes were accepted by the store, but callers still
    /// must call `sync_segment` before publishing metadata that references the
    /// segment unless the implementation documents stronger durability.
    fn write_segment(
        &self,
        reservation: &SegmentReservation,
        bytes: &[u8],
    ) -> Result<SegmentReplicaCommit>;

    /// Read a byte range from an immutable segment.
    ///
    /// Implementors must validate range bounds and buffer length.
    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()>;

    /// Flush/sync a segment to the durability level needed before metadata
    /// publish.
    fn sync_segment(&self, segment_id: SegmentId) -> Result<()>;

    /// Delete local immutable segment bytes after catalog evidence says they
    /// are safe to free.
    ///
    /// Implementors should make this idempotent for already-missing local bytes
    /// so storage-node custodians can reconcile missed frees. Deleting bytes
    /// must not mutate metadata reachability; callers must obtain release
    /// evidence through metadata GC and local catalog state first.
    fn delete_segment(&self, segment_id: SegmentId) -> Result<()>;
}

/// Segment reservation request for a specific write intent and mapping owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReservationIntent {
    pub write_intent: WriteIntentId,
    pub owner: MappingOwner,
    pub bytes: u64,
}

/// Reserved local segment space on one storage endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReservation {
    pub segment_id: SegmentId,
    pub bytes: u64,
}

/// Physical placement for one segment replica on one storage endpoint.
///
/// A logical segment may have one placement in local v1 and many placements in
/// a later replicated implementation. Metadata leaf entries reference the
/// logical `SegmentId`; replica selection remains below the metadata tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReplicaPlacement {
    pub segment_id: SegmentId,
    pub storage_node: StorageNodeId,
    pub offset: u64,
    pub bytes: u64,
}

/// Durable write result for one segment replica.
///
/// A future replicated coordinator can collect multiple replica commits for the
/// same logical segment before publishing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReplicaCommit {
    pub descriptor: SegmentDescriptor,
    pub placement: SegmentReplicaPlacement,
}

/// Per-storage-node catalog of local segment replica placement and lifecycle
/// state.
///
/// Minimal implementor guarantees:
///
/// - Reservations are scoped to write intents and may expire.
/// - State transitions must reject invalid jumps, especially freeing
///   `DurablePendingMetadata` while its write intent can still publish.
/// - A committed replica is not considered metadata-referenced until the
///   metadata commit succeeds for its logical segment.
/// - `delete_segment` frees only local physical state that is safe to free
///   according to custodian/release evidence.
pub trait LocalSegmentCatalog: Send + Sync {
    /// Reserve local segment space for a write intent.
    fn reserve_segment(&self, intent: SegmentReservationIntent) -> Result<SegmentReservation>;

    /// Mark a reserved segment as actively being written.
    ///
    /// This transition is local catalog state only. It must not make bytes
    /// readable or metadata-reachable.
    fn begin_write(&self, reservation: &SegmentReservation) -> Result<()>;

    /// Commit a written segment replica into durable-pending-metadata state.
    ///
    /// This must not by itself make the segment reachable from reads; metadata
    /// publish does that.
    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        commit: SegmentReplicaCommit,
    ) -> Result<()>;

    /// Mark a durable-pending segment as referenced by committed metadata.
    fn mark_segment_referenced(&self, segment_id: SegmentId) -> Result<()>;

    /// Mark a referenced segment as released by metadata reachability evidence.
    ///
    /// Release does not necessarily delete bytes immediately; it makes the
    /// replica eligible for `delete_segment`.
    fn release_segment(&self, segment_id: SegmentId) -> Result<()>;

    /// Reconcile a reservation that expired before any durable write.
    fn expire_reservation(&self, segment_id: SegmentId) -> Result<()>;

    /// Reconcile a write that failed before durable-pending-metadata state.
    fn fail_write(&self, segment_id: SegmentId) -> Result<()>;

    /// Reconcile a durable-pending segment whose write intent can no longer
    /// publish metadata.
    ///
    /// This is the orphan cleanup path for bytes that became durable before a
    /// metadata publish failed, expired, or was fenced off.
    fn free_orphan_segment(&self, segment_id: SegmentId) -> Result<()>;

    /// Locate this storage endpoint's local replica placement.
    fn locate_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaPlacement>;

    /// Delete local segment bytes/state that are safe to free.
    fn delete_segment(&self, segment_id: SegmentId) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_create_request_preserves_public_create_fields() {
        let public = CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 128,
                block_size: 4096,
            },
            name: Some("root".to_string()),
        };

        let metadata = MetadataCreateDeviceRequest::from(public.clone());

        assert_eq!(metadata.spec, public.spec);
        assert_eq!(metadata.name, public.name);
    }

    #[test]
    fn metadata_create_file_request_preserves_public_request() {
        let public = CreateFileRequest {
            spec: crate::extent::FileSpec {
                name: Some("journal".to_string()),
            },
        };

        let metadata = MetadataCreateFileRequest {
            keyspace_id: KeyspaceId::from_raw(7),
            request: public.clone(),
        };

        assert_eq!(metadata.keyspace_id, KeyspaceId::from_raw(7));
        assert_eq!(metadata.request, public);
    }
}
