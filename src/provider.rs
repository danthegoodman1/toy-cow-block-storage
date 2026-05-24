use crate::api::{ByteRange, CreateDeviceRequest, DeviceSpec, RestorePoint};
use crate::error::Result;
use crate::extent::{CreateFileRequest, FileInfo};
use crate::id::{
    CheckpointId, DeviceGeneration, DeviceId, FileId, FileVersion, MetadataNodeId, SegmentId,
    StorageNodeId, WriteIntentId, WriterEpoch,
};
use crate::object::{
    Checkpoint, CommitGroup, DeviceHead, FileHead, MappingOwner, MetadataNode, RootUpdate,
    SegmentDescriptor,
};

/// A metadata-root publish request.
///
/// Implementors must treat this as the atomic metadata transition for one owner.
/// For block devices, all shard-root updates in the intent become visible
/// together or none do. For native files, the file root/version transition
/// becomes visible together or none does. Successful publishes must be
/// durably replayable before success is returned.
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
    pub request: CreateFileRequest,
}

impl From<CreateFileRequest> for MetadataCreateFileRequest {
    fn from(request: CreateFileRequest) -> Self {
        Self { request }
    }
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

/// Retention settings used when enumerating GC roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub retain_deleted_devices: bool,
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

    /// Create a native file and publish its initial empty file root.
    ///
    /// Success means the returned `FileHead` is durable, versioned, and visible
    /// to subsequent `get_file_head` calls.
    fn create_file(&self, request: MetadataCreateFileRequest) -> Result<FileHead>;

    /// Return the latest committed block device head.
    ///
    /// Must not synthesize uncommitted or partially published state.
    fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead>;

    /// Return the latest committed native file head.
    ///
    /// Must not return a head from a stale append lease or failed append commit.
    fn get_file_head(&self, file_id: FileId) -> Result<FileHead>;

    /// Return user-facing native file information derived from committed state.
    fn get_file_info(&self, file_id: FileId) -> Result<FileInfo>;

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

    /// Write a durable checkpoint for a block device.
    ///
    /// The checkpoint must be replay-consistent with the commit sequence it
    /// reports.
    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId>;

    /// Fetch a durable checkpoint by ID.
    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint>;

    /// Enumerate metadata roots that must be treated as live for GC.
    ///
    /// Must include live block roots, live native file roots, and retained PITR
    /// roots required by `policy`. Returning too few roots is data loss.
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

    /// Commit a written segment replica into durable-pending-metadata state.
    ///
    /// This must not by itself make the segment reachable from reads; metadata
    /// publish does that.
    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        commit: SegmentReplicaCommit,
    ) -> Result<()>;

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

        let metadata = MetadataCreateFileRequest::from(public.clone());

        assert_eq!(metadata.request, public);
    }
}
