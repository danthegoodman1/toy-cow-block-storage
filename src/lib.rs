//! Toy copy-on-write block storage.
//!
//! The crate is intentionally split into contracts, deterministic core
//! scaffolding, provider boundaries, and simulation utilities. The first local
//! implementation should use these boundaries directly so remote or durable
//! implementations can replace adapters later.

#![forbid(unsafe_code)]

pub mod api;
pub mod core;
pub mod error;
pub mod extent;
pub mod id;
pub mod local;
pub mod object;
pub mod provider;
pub mod sim;

pub use api::{
    BlockClient, BlockDevice, BlockOperation, BlockRange, BlockRequest, BlockRequestEnvelope,
    BlockResponse, BlockResponseEnvelope, BlockServer, BlockTransport, ByteRange,
    CreateDeviceRequest, DeleteResult, DeviceInfo, DeviceSpec, FlushResult, FlushScope,
    ForkRequest, ReadResponse, RestorePoint, WriteCommit, WriteDurability,
};
pub use error::{Result, StorageError};
pub use extent::{
    AppendCommit, AppendLease, CreateFileRequest, CreateKeyspaceRequest, FileInfo, FileSpec,
    FileWriteCommit, KeyspaceInfo, NativeFile, NativeKeyspaceClient, NativeOperation,
    NativeRequest, NativeRequestEnvelope, NativeResponse, NativeResponseEnvelope, NativeServer,
    NativeTransport, SnapshotKeyspaceRequest,
};
pub use id::{
    AppendLeaseId, BlockCount, BlockIndex, CheckpointId, ClientEpoch, CommitSeq, DeviceGeneration,
    DeviceId, ExtentId, FileId, FileVersion, GrantEpoch, GrantId, GrantNonce,
    KeyspaceCatalogShardId, KeyspaceGeneration, KeyspaceId, KeyspaceRootId, LogicalDeadline,
    LogicalTime, PrincipalId, RequestId, ServerIncarnation, StorageNodeId, StorageNodeKeyId,
    TenantId, WriteIntentId, WriterEpoch,
};
pub use local::{
    ChaosRemoteWireTransport, ChaosStorageNodeTransport, ChaosTransportMetrics,
    DurableCompactionReport, DurableCoordinator, DurableDataLogPolicy, DurableDataLogRef,
    InMemoryLocalSegmentCatalog, InMemoryMetadataPlane, InMemorySegmentStore,
    InProcessBlockTransport, InProcessNativeTransport, LocalBlockClient, LocalBlockDevice,
    LocalBlockServer, LocalCoordinator, LocalNativeClient, LocalNativeFile, LocalNativeServer,
    LocalStoreConfig, MaintenanceCommand, MaintenanceDataLogObservation, MaintenanceDiagnostics,
    MaintenanceMode, MaintenanceNodeObservation, MaintenanceObservation, MaintenancePolicy,
    MaintenanceScheduler, MaintenanceSkippedLog, MaintenanceTickPlan, MaintenanceTickReport,
    MetadataCustodianReport, MetadataMarkReport, MetadataSweepReport, RemoteBlockEndpoint,
    RemoteBlockTransport, RemoteNativeEndpoint, RemoteNativeTransport, RemoteWireTransport,
    SegmentLifecycleState, WriteAdmission,
};
pub use provider::{
    GrantHash, GrantReceiptAuthority, MetadataNodeWrite, PlacementPolicy, ProofScheme, ProofTag,
    ReferenceEvidence, SegmentReceiptLifecycle, SegmentWriteReceipt, StorageNodeCustodianReport,
    StorageNodeDirectory, StorageNodeMaintenanceObservation, StorageNodeMaintenanceReport,
    StorageNodeRequest, StorageNodeResponse, StorageNodeTransport, VerifiedSegmentReceipt,
    WriteGrant, WriteGrantIntent, WriteGrantRequest,
};
pub use sim::{
    FailureArtifact, FaultInjector, FaultKind, ObjectGraphSummary, minimize_trace_by_deletion,
};
