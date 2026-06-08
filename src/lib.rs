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
    BlockBatchCommit, BlockBatchWrite, BlockClient, BlockDevice, BlockOperation, BlockRange,
    BlockRequest, BlockRequestEnvelope, BlockResponse, BlockResponseEnvelope, BlockServer,
    BlockTransport, ByteRange, CreateDeviceRequest, DeleteResult, DeviceInfo, DeviceSpec,
    FlushResult, FlushScope, ForkRequest, PayloadIntegrity, ReadResponse, ReadVerification,
    RestorePoint, WriteCommit, WriteDurability,
};
pub use error::{Result, StorageError};
pub use extent::{
    AppendPublishCommit, AppendPublishTicket, AppendStream, AppendTicket, CreateFileRequest,
    CreateKeyspaceRequest, FileBatchWrite, FileInfo, FileSpec, FileWriteCommit, KeyspaceInfo,
    NativeFile, NativeKeyspaceClient, NativeOperation, NativeRequest, NativeRequestEnvelope,
    NativeResponse, NativeResponseEnvelope, NativeServer, NativeTransport, SnapshotKeyspaceRequest,
};
pub use id::{
    AppendPublishTicketId, AppendStreamId, AppendTicketId, BlockCount, BlockIndex, CheckpointId,
    ClientEpoch, CommitSeq, DeviceGeneration, DeviceId, ExtentId, FileId, FileVersion, GrantEpoch,
    GrantId, GrantNonce, KeyspaceCatalogShardId, KeyspaceGeneration, KeyspaceId, KeyspaceRootId,
    LogicalDeadline, LogicalTime, PrincipalId, RequestId, ServerIncarnation, StorageNodeId,
    StorageNodeKeyId, TenantId, WriteIntentId, WriterEpoch,
};
pub use local::{
    AppendIngestAdmissionPolicy, AppendIngestDataLogPolicy, AppendIngestPolicy,
    AppendIngestProfile, AppendPublishBatchPolicy, AppendPublishWaitProfile,
    ChaosRemoteWireTransport, ChaosStorageNodeTransport, ChaosTransportMetrics,
    DurableCompactionReport, DurableCoordinator, DurableDataLogPolicy, DurableDataLogRef,
    DurablePersistProfile, InMemoryLocalSegmentCatalog, InMemoryMetadataPlane,
    InMemorySegmentStore, InProcessBlockTransport, InProcessNativeTransport, LocalBlockClient,
    LocalBlockDevice, LocalBlockServer, LocalCoordinator, LocalNativeClient, LocalNativeFile,
    LocalNativeServer, LocalStoreConfig, MaintenanceCommand, MaintenanceDataLogObservation,
    MaintenanceDiagnostics, MaintenanceMode, MaintenanceNodeObservation, MaintenanceObservation,
    MaintenancePolicy, MaintenanceScheduler, MaintenanceSkippedLog, MaintenanceTickPlan,
    MaintenanceTickReport, MetadataCustodianReport, MetadataMarkReport, MetadataSweepReport,
    MetadataTxnMode, MetadataTxnProfile, MetadataTxnProfilePhase, ReadProfile, RemoteBlockEndpoint,
    RemoteBlockTransport, RemoteNativeEndpoint, RemoteNativeTransport, RemoteWireTransport,
    SegmentLifecycleState, TxnBlockCoordinator, TxnBlockMetadataPlane, TxnBlockWriteProfile,
    WriteAdmission,
};
pub use object::SegmentPayloadIntegrity;
pub use provider::{
    DIAGNOSTICS_COUNTER_NAMES, DIAGNOSTICS_GAUGE_NAMES, DiagnosticsCounters, DiagnosticsGauges,
    DiagnosticsNodeSnapshot, DiagnosticsSnapshot, GrantHash, GrantReceiptAuthority,
    MetadataNodeWrite, ObservableProvider, PlacementPolicy, ProofScheme, ProofTag,
    ReferenceEvidence, STORAGE_EVENT_KIND_NAMES, SegmentReceiptLifecycle, SegmentWriteReceipt,
    StorageEvent, StorageEventKind, StorageNodeCustodianReport, StorageNodeDirectory,
    StorageNodeMaintenanceObservation, StorageNodeMaintenanceReport, StorageNodeRequest,
    StorageNodeResponse, StorageNodeTransport, VerifiedSegmentReceipt, WriteGrant,
    WriteGrantIntent, WriteGrantRequest,
};
pub use sim::{
    FailureArtifact, FaultInjector, FaultKind, ObjectGraphSummary, minimize_trace_by_deletion,
};
