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
    DeviceId, ExtentId, FileId, FileVersion, KeyspaceGeneration, KeyspaceId, KeyspaceRootId,
    LogicalDeadline, LogicalTime, RequestId, StorageNodeId, WriteIntentId, WriterEpoch,
};
pub use local::{
    InMemoryLocalSegmentCatalog, InMemoryMetadataPlane, InMemorySegmentStore,
    InProcessBlockTransport, InProcessNativeTransport, LocalBlockClient, LocalBlockDevice,
    LocalBlockServer, LocalNativeClient, LocalNativeFile, LocalNativeServer, LocalObjectStore,
    LocalStoreConfig, MetadataCustodianReport, MetadataMarkReport, MetadataSweepReport,
    SegmentLifecycleState, StorageNodeCustodianReport,
};
pub use sim::{
    FailureArtifact, FaultInjector, FaultKind, ObjectGraphSummary, minimize_trace_by_deletion,
};
