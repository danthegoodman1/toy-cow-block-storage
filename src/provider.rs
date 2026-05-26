use std::collections::BTreeSet;
use std::sync::Arc;

use crate::api::{
    BlockRange, ByteRange, CreateDeviceRequest, DeleteResult, DeviceSpec, RestorePoint,
    WriteDurability,
};
use crate::error::Result;
use crate::extent::{CreateFileRequest, CreateKeyspaceRequest, FileInfo, KeyspaceInfo};
use crate::id::{
    CheckpointId, CommitSeq, DeviceGeneration, DeviceId, FileId, FileVersion, GrantEpoch, GrantId,
    GrantNonce, KeyspaceId, LogicalDeadline, MetadataNodeId, PrincipalId, SegmentId,
    ServerIncarnation, StorageNodeId, StorageNodeKeyId, TenantId, WriteIntentId, WriterEpoch,
};
use crate::object::{
    Checkpoint, CommitGroup, DeleteRecord, DeviceHead, FileHead, KeyspaceHead, MappingOwner,
    MetadataNode, RootUpdate, SegmentDescriptor,
};

/// Physical storage-node maintenance report.
///
/// This is storage-node-local lifecycle evidence. It does not imply metadata
/// reachability changed; metadata release evidence and coordinator routing own
/// that boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodeCustodianReport {
    pub expired_reservations: Vec<SegmentId>,
    pub failed_writes: Vec<SegmentId>,
    pub orphan_segments: Vec<SegmentId>,
    pub deleted_released_segments: Vec<SegmentId>,
}

/// Proof algorithm advertised by grants and receipts.
///
/// The local provider uses `DeterministicTestMacV1` so tests can replay exactly
/// without pulling in a crypto dependency. Remote production storage nodes
/// should use `NodeSignatureV1`, with the verifier resolving `key_id` through a
/// node-key registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ProofScheme {
    DeterministicTestMacV1,
    NodeSignatureV1,
}

/// Opaque proof bytes for grants, receipts, and reference evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProofTag(pub [u8; 32]);

impl ProofTag {
    pub const ZERO: Self = Self([0; 32]);

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical hash of a write grant body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GrantHash(pub [u8; 32]);

/// Logical operation a grant authorizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WriteGrantIntent {
    BlockWrite {
        device_id: DeviceId,
        range: BlockRange,
        fence: DeviceGeneration,
    },
    NativeWrite {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        base_version: FileVersion,
    },
    NativeAppend {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        append_offset: u64,
        bytes: u64,
        base_version: FileVersion,
        writer_epoch: WriterEpoch,
    },
    NativeReservedAppend {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        append_offset: u64,
        bytes: u64,
        base_version: FileVersion,
        writer_epoch: WriterEpoch,
    },
    Internal {
        owner: MappingOwner,
    },
}

impl WriteGrantIntent {
    pub const fn owner(self) -> MappingOwner {
        match self {
            Self::BlockWrite { device_id, .. } => MappingOwner::BlockDevice(device_id),
            Self::NativeWrite { keyspace_id, .. }
            | Self::NativeAppend { keyspace_id, .. }
            | Self::NativeReservedAppend { keyspace_id, .. } => {
                MappingOwner::NativeKeyspace(keyspace_id)
            }
            Self::Internal { owner } => owner,
        }
    }
}

/// Request to issue a scoped write grant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteGrantRequest {
    pub tenant: TenantId,
    pub principal: PrincipalId,
    pub intent: WriteGrantIntent,
    pub write_intent: WriteIntentId,
    pub segment_id: SegmentId,
    pub storage_node: StorageNodeId,
    pub max_bytes: u64,
    pub durability: WriteDurability,
    pub expires_at: LogicalDeadline,
}

/// Metadata-issued capability authorizing one scoped storage-node write.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteGrant {
    pub tenant: TenantId,
    pub principal: PrincipalId,
    pub grant_id: GrantId,
    pub nonce: GrantNonce,
    pub grant_epoch: GrantEpoch,
    pub expires_at: LogicalDeadline,
    pub owner: MappingOwner,
    pub intent: WriteGrantIntent,
    pub write_intent: WriteIntentId,
    pub segment_id: SegmentId,
    pub storage_node: StorageNodeId,
    pub max_bytes: u64,
    pub durability: WriteDurability,
    pub key_id: StorageNodeKeyId,
    pub proof_scheme: ProofScheme,
    pub proof: ProofTag,
}

impl WriteGrant {
    pub fn canonical_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(192);
        self.write_canonical(&mut out);
        out
    }

    pub fn hash(&self) -> GrantHash {
        let mut out = StableHash32::new();
        self.write_canonical(&mut out);
        GrantHash(out.finish().0)
    }

    fn write_canonical(&self, out: &mut impl CanonicalSink) {
        put_bytes(out, b"TCOW_WRITE_GRANT_V1");
        put_u128(out, self.tenant.raw());
        put_u128(out, self.principal.raw());
        put_u128(out, self.grant_id.raw());
        put_u128(out, self.nonce.raw());
        put_u64(out, self.grant_epoch.raw());
        put_u64(out, self.expires_at.raw());
        put_mapping_owner(out, self.owner);
        put_write_grant_intent(out, self.intent);
        put_u128(out, self.write_intent.raw());
        put_u128(out, self.segment_id.raw());
        put_u128(out, self.storage_node.raw());
        put_u64(out, self.max_bytes);
        put_write_durability(out, self.durability);
        put_u128(out, self.key_id.raw());
        put_proof_scheme(out, self.proof_scheme);
    }
}

/// Segment lifecycle state promised by a storage-node receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentReceiptLifecycle {
    DurablePendingMetadata,
}

/// Verifiable proof that a storage node durably wrote one pending segment.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentWriteReceipt {
    pub tenant: TenantId,
    pub grant_id: GrantId,
    pub grant_hash: GrantHash,
    pub principal: PrincipalId,
    pub owner: MappingOwner,
    pub storage_node: StorageNodeId,
    pub storage_node_incarnation: ServerIncarnation,
    pub segment_id: SegmentId,
    pub write_intent: WriteIntentId,
    pub intent: WriteGrantIntent,
    pub bytes: u64,
    pub checksum: Option<u64>,
    pub durability: WriteDurability,
    pub lifecycle: SegmentReceiptLifecycle,
    pub receipt_epoch: GrantEpoch,
    pub expires_at: LogicalDeadline,
    pub node_key_id: StorageNodeKeyId,
    pub proof_scheme: ProofScheme,
    pub proof: ProofTag,
    pub descriptor: SegmentDescriptor,
    pub placement: SegmentReplicaPlacement,
}

impl SegmentWriteReceipt {
    pub fn canonical_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        self.write_canonical(&mut out);
        out
    }

    fn write_canonical(&self, out: &mut impl CanonicalSink) {
        put_bytes(out, b"TCOW_SEGMENT_RECEIPT_V1");
        put_u128(out, self.tenant.raw());
        put_u128(out, self.grant_id.raw());
        put_bytes(out, &self.grant_hash.0);
        put_u128(out, self.principal.raw());
        put_mapping_owner(out, self.owner);
        put_u128(out, self.storage_node.raw());
        put_u64(out, self.storage_node_incarnation.raw());
        put_u128(out, self.segment_id.raw());
        put_u128(out, self.write_intent.raw());
        put_write_grant_intent(out, self.intent);
        put_u64(out, self.bytes);
        put_optional_u64(out, self.checksum);
        put_write_durability(out, self.durability);
        put_segment_receipt_lifecycle(out, self.lifecycle);
        put_u64(out, self.receipt_epoch.raw());
        put_u64(out, self.expires_at.raw());
        put_u128(out, self.node_key_id.raw());
        put_proof_scheme(out, self.proof_scheme);
        put_segment_descriptor(out, &self.descriptor);
        put_segment_replica_placement(out, &self.placement);
    }

    pub fn replica_commit(&self) -> SegmentReplicaCommit {
        SegmentReplicaCommit {
            descriptor: self.descriptor.clone(),
            placement: self.placement.clone(),
        }
    }
}

/// Receipt verified by a `GrantReceiptAuthority`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSegmentReceipt {
    pub(crate) receipt: SegmentWriteReceipt,
    pub(crate) descriptor: SegmentDescriptor,
}

impl VerifiedSegmentReceipt {
    pub fn receipt(&self) -> &SegmentWriteReceipt {
        &self.receipt
    }

    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }
}

/// Metadata-produced evidence that a pending storage segment is now referenced.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReferenceEvidence {
    pub tenant: TenantId,
    pub principal: PrincipalId,
    pub owner: MappingOwner,
    pub grant_id: GrantId,
    pub segment_id: SegmentId,
    pub storage_node: StorageNodeId,
    pub metadata_commit: CommitSeq,
    pub receipt_epoch: GrantEpoch,
    pub node_key_id: StorageNodeKeyId,
    pub proof_scheme: ProofScheme,
    pub proof: ProofTag,
}

impl ReferenceEvidence {
    pub fn canonical_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128);
        self.write_canonical(&mut out);
        out
    }

    fn write_canonical(&self, out: &mut impl CanonicalSink) {
        put_bytes(out, b"TCOW_REFERENCE_EVIDENCE_V1");
        put_u128(out, self.tenant.raw());
        put_u128(out, self.principal.raw());
        put_mapping_owner(out, self.owner);
        put_u128(out, self.grant_id.raw());
        put_u128(out, self.segment_id.raw());
        put_u128(out, self.storage_node.raw());
        put_u64(out, self.metadata_commit.raw());
        put_u64(out, self.receipt_epoch.raw());
        put_u128(out, self.node_key_id.raw());
        put_proof_scheme(out, self.proof_scheme);
    }
}

/// Node-local maintenance observation returned through storage-node transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodeMaintenanceObservation {
    pub storage_node: StorageNodeId,
    pub reserved_segments: usize,
    pub writing_segments: usize,
    pub durable_pending_segments: usize,
    pub referenced_segments: usize,
    pub released_segments: usize,
    pub freed_segments: usize,
}

/// Node-local maintenance report returned through storage-node transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodeMaintenanceReport {
    pub storage_node: StorageNodeId,
    pub deleted_released_segments: Vec<SegmentId>,
    pub skipped_segments: Vec<SegmentId>,
}

/// Boundary responsible for grant and receipt proof verification.
pub trait GrantReceiptAuthority: Send + Sync {
    /// Issue a scoped storage write grant.
    ///
    /// Success means the returned grant is authenticated by this authority and
    /// may be presented to the selected storage node before its deterministic
    /// expiration boundary. It does not write bytes or make data visible.
    fn issue_write_grant(&self, request: WriteGrantRequest) -> Result<WriteGrant>;

    /// Verify a grant at the storage-node boundary.
    ///
    /// Success means the grant proof is valid for the requested node, segment,
    /// and byte count. Failure must leave storage-node bytes and catalog state
    /// unchanged.
    fn verify_write_grant(
        &self,
        grant: &WriteGrant,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        bytes: u64,
    ) -> Result<()>;

    /// Create a durable-pending segment receipt after bytes are synced.
    ///
    /// Success means the receipt body and proof bind the supplied grant,
    /// storage-node identity, descriptor, placement, checksum, lifecycle, and
    /// durability. It still does not make metadata reference the segment.
    fn create_segment_receipt(
        &self,
        grant: &WriteGrant,
        commit: SegmentReplicaCommit,
        storage_node_incarnation: ServerIncarnation,
    ) -> Result<SegmentWriteReceipt>;

    /// Verify a storage-node receipt for metadata publish evidence.
    ///
    /// Success returns an unforgeable-in-crate `VerifiedSegmentReceipt` whose
    /// descriptor may be used to validate metadata leaves. Implementations must
    /// reject stale, corrupt, wrong-node, wrong-lifecycle, wrong-durability, or
    /// wrong-checksum receipts without consulting storage-node bytes.
    fn verify_segment_receipt(
        &self,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt>;

    /// Create evidence that metadata publish made a segment referenced.
    ///
    /// Success means storage nodes may transition the matching
    /// durable-pending segment to referenced for this metadata commit. It must
    /// be produced only after metadata publish succeeds.
    fn create_reference_evidence(
        &self,
        receipt: &SegmentWriteReceipt,
        metadata_commit: CommitSeq,
    ) -> Result<ReferenceEvidence>;

    /// Verify reference evidence at the storage-node boundary.
    ///
    /// Success authorizes only the matching segment on the matching storage
    /// node to move from durable-pending to referenced. Failure must not expose
    /// or free data.
    fn verify_reference_evidence(
        &self,
        evidence: &ReferenceEvidence,
        segment_id: SegmentId,
        storage_node: StorageNodeId,
    ) -> Result<()>;
}

/// A metadata-root publish request.
///
/// Implementors must treat this as the atomic metadata transition for one owner.
/// For block devices, all shard-root updates in the intent become visible
/// together or none do. For native keyspaces, the catalog-root transition and
/// enclosed file root/version transition become visible together or none does.
/// Successful publishes must be durably replayable before success is returned.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataCreateDeviceRequest {
    pub spec: DeviceSpec,
    pub name: Option<String>,
}

/// Internal create-file request accepted by the metadata plane.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataCreateFileRequest {
    pub keyspace_id: KeyspaceId,
    pub request: CreateFileRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataForkRequest {
    pub source: DeviceId,
    pub target: Option<DeviceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataSnapshotKeyspaceRequest {
    pub source: KeyspaceId,
    pub target: Option<KeyspaceId>,
    pub name: Option<String>,
}

/// Immutable metadata-node persist request with verified storage evidence.
///
/// Leaf nodes carry verified storage-node receipts. Metadata extracts segment
/// descriptors from those receipts to validate logical ranges without opening a
/// storage-node catalog or reading segment bytes. Internal nodes usually have
/// no receipt evidence because they reference metadata children, not segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataNodeWrite {
    pub node: MetadataNode,
    pub segment_receipts: Vec<VerifiedSegmentReceipt>,
}

impl MetadataNodeWrite {
    pub fn new(node: MetadataNode, segment_receipts: Vec<VerifiedSegmentReceipt>) -> Self {
        Self {
            node,
            segment_receipts,
        }
    }

    pub fn segment_descriptors(&self) -> Vec<SegmentDescriptor> {
        self.segment_receipts
            .iter()
            .map(|receipt| receipt.descriptor.clone())
            .collect()
    }
}

/// Retention settings used when enumerating GC roots.
///
/// The policy is deliberately expressed in commit-age terms instead of wall
/// clock time so retention can be simulated and replayed deterministically.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
/// - The metadata plane must not open storage-node catalogs, data logs, or
///   segment bytes. Verified receipt evidence supplied with metadata-node
///   writes is the full physical evidence it may inspect.
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
    /// content to an existing `node_id` must fail. Leaf nodes must be validated
    /// against the supplied segment descriptors; missing or insufficient
    /// verified receipts must fail instead of consulting storage-node state.
    fn persist_metadata_node(&self, write: MetadataNodeWrite) -> Result<()>;

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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentReservationIntent {
    /// Provider-unique write attempt ID used by custodians to expire failed or
    /// abandoned segment writes. This is separate from file writer epochs or
    /// append lease IDs so expiring one storage write cannot accidentally free
    /// another owner that reused a higher-level fencing token.
    pub write_intent: WriteIntentId,
    pub owner: MappingOwner,
    pub bytes: u64,
}

/// Reserved local segment space on one storage endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentReservation {
    pub segment_id: SegmentId,
    pub bytes: u64,
}

/// Physical placement for one segment replica on one storage endpoint.
///
/// A logical segment may have one placement in local v1 and many placements in
/// a later replicated implementation. Metadata leaf entries reference the
/// logical `SegmentId`; replica selection remains below the metadata tree.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentReplicaCommit {
    pub descriptor: SegmentDescriptor,
    pub placement: SegmentReplicaPlacement,
}

/// Coordinator-to-storage-node request.
///
/// These messages are physical segment-node operations. They must not carry
/// device roots, file versions, keyspace catalogs, PITR timelines, or metadata
/// commit-group decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageNodeRequest {
    WriteSegment {
        grant: WriteGrant,
        bytes: Vec<u8>,
    },
    ReadSegment {
        segment_id: SegmentId,
        range: ByteRange,
    },
    MarkReferenced {
        evidence: ReferenceEvidence,
    },
    Release {
        segment_id: SegmentId,
    },
    RunCustodian {
        expired_write_intents: BTreeSet<WriteIntentId>,
    },
    ObserveMaintenance,
    RunMaintenanceTick,
}

/// Coordinator-to-storage-node response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageNodeResponse {
    WriteSegment { receipt: Box<SegmentWriteReceipt> },
    ReadSegment { bytes: Vec<u8> },
    MarkReferenced,
    Released,
    Custodian(StorageNodeCustodianReport),
    MaintenanceObserved(StorageNodeMaintenanceObservation),
    MaintenanceTicked(StorageNodeMaintenanceReport),
}

/// Transport boundary from a coordinator to one physical storage node.
///
/// Minimal implementor guarantees:
///
/// - Storage-node transports own bytes, local segment lifecycle, and physical
///   placement for exactly one storage node.
/// - `WriteSegment` reserves the supplied logical segment ID, writes and syncs
///   bytes, and returns a durable-pending receipt. It must not make metadata
///   roots visible.
/// - `MarkReferenced` may only mark a durable-pending segment after the
///   coordinator has a successful metadata publish.
/// - Storage nodes never decide logical visibility, writer fencing, PITR
///   retention, or commit-group ordering.
pub trait StorageNodeTransport: Send + Sync {
    fn storage_node_id(&self) -> StorageNodeId;
    fn send(&self, request: StorageNodeRequest) -> Result<StorageNodeResponse>;
}

/// Provider-private directory for locating storage nodes.
///
/// The coordinator uses this to choose nodes, allocate logical segment IDs, and
/// resolve reads. Metadata implementations must not depend on this directory.
pub trait StorageNodeDirectory: Send + Sync {
    fn storage_node_ids(&self) -> Result<Vec<StorageNodeId>>;
    fn allocate_segment_id(&self) -> Result<SegmentId>;
    fn transport_for_node(
        &self,
        storage_node: StorageNodeId,
    ) -> Result<Arc<dyn StorageNodeTransport>>;
    fn transport_for_segment(&self, segment_id: SegmentId)
    -> Result<Arc<dyn StorageNodeTransport>>;
}

/// Deterministic provider-private placement policy.
///
/// Clients never choose storage nodes. Embedded or service coordinators call
/// this policy below the public block/native APIs.
pub trait PlacementPolicy: Send + Sync {
    fn choose_storage_node(&self, candidates: &[StorageNodeId]) -> Result<StorageNodeId>;
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
    /// The receipt must already have been created by the storage node after
    /// syncing bytes. This must not by itself make the segment reachable from
    /// reads; metadata publish does that.
    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
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

/// Deterministic proof function for local tests and simulations.
///
/// This is intentionally not cryptographic. It gives the local provider stable
/// proof bytes with the same verification shape as a production signature path.
#[cfg(test)]
fn deterministic_test_proof(key_id: StorageNodeKeyId, canonical_body: &[u8]) -> ProofTag {
    let mut hash = StableHash32::new();
    hash.update(&key_id.raw().to_be_bytes());
    hash.update(canonical_body);
    hash.finish()
}

pub(crate) fn deterministic_test_proof_for_grant(
    key_id: StorageNodeKeyId,
    grant: &WriteGrant,
) -> ProofTag {
    deterministic_test_proof_for_canonical(key_id, |hash| grant.write_canonical(hash))
}

pub(crate) fn deterministic_test_grant_hash_and_proof(
    key_id: StorageNodeKeyId,
    grant: &WriteGrant,
) -> (GrantHash, ProofTag) {
    let mut hash = StableHash32::new();
    let mut proof = StableHash32::new();
    proof.update(&key_id.raw().to_be_bytes());
    {
        let mut sink = BodyAndProofHash32 {
            body: &mut hash,
            proof: &mut proof,
        };
        grant.write_canonical(&mut sink);
    }
    (GrantHash(hash.finish().0), proof.finish())
}

pub(crate) fn deterministic_test_proof_for_receipt(
    key_id: StorageNodeKeyId,
    receipt: &SegmentWriteReceipt,
) -> ProofTag {
    deterministic_test_proof_for_canonical(key_id, |hash| receipt.write_canonical(hash))
}

pub(crate) fn deterministic_test_proof_for_reference(
    key_id: StorageNodeKeyId,
    evidence: &ReferenceEvidence,
) -> ProofTag {
    deterministic_test_proof_for_canonical(key_id, |hash| evidence.write_canonical(hash))
}

fn deterministic_test_proof_for_canonical(
    key_id: StorageNodeKeyId,
    write_body: impl FnOnce(&mut StableHash32),
) -> ProofTag {
    let mut hash = StableHash32::new();
    hash.update(&key_id.raw().to_be_bytes());
    write_body(&mut hash);
    hash.finish()
}

trait CanonicalSink {
    fn append(&mut self, bytes: &[u8]);
}

impl CanonicalSink for Vec<u8> {
    fn append(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

#[derive(Debug, Clone)]
struct StableHash32 {
    lanes: [u64; 4],
    index: usize,
}

impl StableHash32 {
    fn new() -> Self {
        Self {
            lanes: [
                0x243f_6a88_85a3_08d3u64,
                0x1319_8a2e_0370_7344u64,
                0xa409_3822_299f_31d0u64,
                0x082e_fa98_ec4e_6c89u64,
            ],
            index: 0,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            let lane = self.index % self.lanes.len();
            self.lanes[lane] ^= u64::from(*byte) << ((self.index % 8) * 8);
            self.lanes[lane] = self.lanes[lane]
                .rotate_left((7 + lane * 5) as u32)
                .wrapping_mul(0x9e37_79b1_85eb_ca87);
            self.lanes[(lane + 1) % 4] ^= self.lanes[lane].rotate_right(11);
            self.index = self.index.saturating_add(1);
        }
    }

    fn finish(self) -> ProofTag {
        let mut out = [0; 32];
        for (chunk, lane) in out.chunks_exact_mut(8).zip(self.lanes) {
            chunk.copy_from_slice(&lane.to_be_bytes());
        }
        ProofTag(out)
    }
}

impl CanonicalSink for StableHash32 {
    fn append(&mut self, bytes: &[u8]) {
        self.update(bytes);
    }
}

struct BodyAndProofHash32<'a> {
    body: &'a mut StableHash32,
    proof: &'a mut StableHash32,
}

impl CanonicalSink for BodyAndProofHash32<'_> {
    fn append(&mut self, bytes: &[u8]) {
        self.body.update(bytes);
        self.proof.update(bytes);
    }
}

fn put_bytes(out: &mut impl CanonicalSink, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.append(bytes);
}

fn put_u8(out: &mut impl CanonicalSink, value: u8) {
    out.append(&[value]);
}

fn put_u64(out: &mut impl CanonicalSink, value: u64) {
    out.append(&value.to_be_bytes());
}

fn put_u128(out: &mut impl CanonicalSink, value: u128) {
    out.append(&value.to_be_bytes());
}

fn put_optional_u64(out: &mut impl CanonicalSink, value: Option<u64>) {
    match value {
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
        None => put_u8(out, 0),
    }
}

fn put_mapping_owner(out: &mut impl CanonicalSink, owner: MappingOwner) {
    match owner {
        MappingOwner::BlockDevice(device_id) => {
            put_u8(out, 1);
            put_u128(out, device_id.raw());
        }
        MappingOwner::NativeKeyspace(keyspace_id) => {
            put_u8(out, 2);
            put_u128(out, keyspace_id.raw());
        }
    }
}

fn put_write_grant_intent(out: &mut impl CanonicalSink, intent: WriteGrantIntent) {
    match intent {
        WriteGrantIntent::BlockWrite {
            device_id,
            range,
            fence,
        } => {
            put_u8(out, 1);
            put_u128(out, device_id.raw());
            put_u64(out, range.start.raw());
            put_u64(out, range.blocks.raw());
            put_u64(out, fence.raw());
        }
        WriteGrantIntent::NativeWrite {
            keyspace_id,
            file_id,
            range,
            base_version,
        } => {
            put_u8(out, 2);
            put_u128(out, keyspace_id.raw());
            put_u128(out, file_id.raw());
            put_u64(out, range.offset);
            put_u64(out, range.len);
            put_u64(out, base_version.raw());
        }
        WriteGrantIntent::NativeAppend {
            keyspace_id,
            file_id,
            append_offset,
            bytes,
            base_version,
            writer_epoch,
        } => {
            put_u8(out, 3);
            put_u128(out, keyspace_id.raw());
            put_u128(out, file_id.raw());
            put_u64(out, append_offset);
            put_u64(out, bytes);
            put_u64(out, base_version.raw());
            put_u64(out, writer_epoch.raw());
        }
        WriteGrantIntent::NativeReservedAppend {
            keyspace_id,
            file_id,
            append_offset,
            bytes,
            base_version,
            writer_epoch,
        } => {
            put_u8(out, 4);
            put_u128(out, keyspace_id.raw());
            put_u128(out, file_id.raw());
            put_u64(out, append_offset);
            put_u64(out, bytes);
            put_u64(out, base_version.raw());
            put_u64(out, writer_epoch.raw());
        }
        WriteGrantIntent::Internal { owner } => {
            put_u8(out, 5);
            put_mapping_owner(out, owner);
        }
    }
}

fn put_write_durability(out: &mut impl CanonicalSink, durability: WriteDurability) {
    match durability {
        WriteDurability::Acknowledged => put_u8(out, 1),
        WriteDurability::Flushed => put_u8(out, 2),
    }
}

fn put_proof_scheme(out: &mut impl CanonicalSink, scheme: ProofScheme) {
    match scheme {
        ProofScheme::DeterministicTestMacV1 => put_u8(out, 1),
        ProofScheme::NodeSignatureV1 => put_u8(out, 2),
    }
}

fn put_segment_receipt_lifecycle(out: &mut impl CanonicalSink, lifecycle: SegmentReceiptLifecycle) {
    match lifecycle {
        SegmentReceiptLifecycle::DurablePendingMetadata => put_u8(out, 1),
    }
}

fn put_segment_descriptor(out: &mut impl CanonicalSink, descriptor: &SegmentDescriptor) {
    put_u128(out, descriptor.segment_id.raw());
    put_u64(out, descriptor.blocks.raw());
    put_u64(out, descriptor.bytes);
    put_optional_u64(out, descriptor.checksum);
}

fn put_segment_replica_placement(
    out: &mut impl CanonicalSink,
    placement: &SegmentReplicaPlacement,
) {
    put_u128(out, placement.segment_id.raw());
    put_u128(out, placement.storage_node.raw());
    put_u64(out, placement.offset);
    put_u64(out, placement.bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{BlockCount, BlockIndex};

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

    #[test]
    fn optimized_grant_hash_and_proof_match_canonical_body_path() {
        let key_id = StorageNodeKeyId::from_raw(11);
        let mut grant = WriteGrant {
            tenant: TenantId::from_raw(1),
            principal: PrincipalId::from_raw(2),
            grant_id: GrantId::from_raw(3),
            nonce: GrantNonce::from_raw(4),
            grant_epoch: GrantEpoch::from_raw(5),
            expires_at: LogicalDeadline::from_raw(6),
            owner: MappingOwner::BlockDevice(DeviceId::from_raw(7)),
            intent: WriteGrantIntent::BlockWrite {
                device_id: DeviceId::from_raw(7),
                range: BlockRange::new(BlockIndex::from_raw(8), BlockCount::from_raw(2)),
                fence: DeviceGeneration::from_raw(9),
            },
            write_intent: WriteIntentId::from_raw(10),
            segment_id: SegmentId::from_raw(12),
            storage_node: StorageNodeId::from_raw(13),
            max_bytes: 8192,
            durability: WriteDurability::Flushed,
            key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: ProofTag::ZERO,
        };
        grant.proof = deterministic_test_proof(key_id, &grant.canonical_body());

        let mut body_hash = StableHash32::new();
        body_hash.update(&grant.canonical_body());
        let expected_hash = GrantHash(body_hash.finish().0);
        let expected_proof = deterministic_test_proof(key_id, &grant.canonical_body());

        let (optimized_hash, optimized_proof) =
            deterministic_test_grant_hash_and_proof(key_id, &grant);
        assert_eq!(optimized_hash, expected_hash);
        assert_eq!(grant.hash(), expected_hash);
        assert_eq!(optimized_proof, expected_proof);
        assert_eq!(
            deterministic_test_proof_for_grant(key_id, &grant),
            expected_proof
        );
    }

    #[test]
    fn optimized_receipt_and_reference_proofs_match_canonical_body_path() {
        let key_id = StorageNodeKeyId::from_raw(21);
        let descriptor = SegmentDescriptor {
            segment_id: SegmentId::from_raw(22),
            blocks: BlockCount::from_raw(1),
            bytes: 4096,
            checksum: Some(0xabc),
        };
        let placement = SegmentReplicaPlacement {
            segment_id: descriptor.segment_id,
            storage_node: StorageNodeId::from_raw(23),
            offset: 24,
            bytes: descriptor.bytes,
        };
        let mut receipt = SegmentWriteReceipt {
            tenant: TenantId::from_raw(25),
            grant_id: GrantId::from_raw(26),
            grant_hash: GrantHash([3; 32]),
            principal: PrincipalId::from_raw(27),
            owner: MappingOwner::NativeKeyspace(KeyspaceId::from_raw(28)),
            storage_node: placement.storage_node,
            storage_node_incarnation: ServerIncarnation::from_raw(29),
            segment_id: descriptor.segment_id,
            write_intent: WriteIntentId::from_raw(30),
            intent: WriteGrantIntent::NativeWrite {
                keyspace_id: KeyspaceId::from_raw(28),
                file_id: FileId::from_raw(31),
                range: ByteRange::new(0, 4096),
                base_version: FileVersion::from_raw(32),
            },
            bytes: descriptor.bytes,
            checksum: descriptor.checksum,
            durability: WriteDurability::Acknowledged,
            lifecycle: SegmentReceiptLifecycle::DurablePendingMetadata,
            receipt_epoch: GrantEpoch::from_raw(33),
            expires_at: LogicalDeadline::from_raw(34),
            node_key_id: key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: ProofTag::ZERO,
            descriptor,
            placement,
        };
        receipt.proof = deterministic_test_proof(key_id, &receipt.canonical_body());
        assert_eq!(
            deterministic_test_proof_for_receipt(key_id, &receipt),
            deterministic_test_proof(key_id, &receipt.canonical_body())
        );

        let mut evidence = ReferenceEvidence {
            tenant: receipt.tenant,
            principal: receipt.principal,
            owner: receipt.owner,
            grant_id: receipt.grant_id,
            segment_id: receipt.segment_id,
            storage_node: receipt.storage_node,
            metadata_commit: CommitSeq::from_raw(35),
            receipt_epoch: receipt.receipt_epoch,
            node_key_id: key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: ProofTag::ZERO,
        };
        evidence.proof = deterministic_test_proof(key_id, &evidence.canonical_body());
        assert_eq!(
            deterministic_test_proof_for_reference(key_id, &evidence),
            deterministic_test_proof(key_id, &evidence.canonical_body())
        );
    }
}
