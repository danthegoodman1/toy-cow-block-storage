use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::time::Instant;

use rusqlite::{Connection, OptionalExtension, params};

use crate::api::{
    BlockBatchCommit, BlockBatchWrite, BlockClient, BlockDevice, BlockRange, BlockRequest,
    BlockRequestEnvelope, BlockResponse, BlockResponseEnvelope, BlockServer, BlockTransport,
    ByteRange, CreateDeviceRequest, DeleteResult, DeviceInfo, DeviceSpec, FlushResult, FlushScope,
    ForkRequest, PayloadIntegrity, ReadResponse, ReadVerification, RestorePoint, WriteCommit,
    WriteDurability,
};
use crate::error::{Result, StorageError};
use crate::extent::{
    AppendPublishCommit, AppendPublishTicket, AppendStream, AppendTicket, CreateFileRequest,
    CreateKeyspaceRequest, FileBatchWrite, FileInfo, FileSpec, FileWriteCommit, KeyspaceInfo,
    NativeFile, NativeKeyspaceClient, NativeRequest, NativeRequestEnvelope, NativeResponse,
    NativeResponseEnvelope, NativeServer, NativeTransport, SnapshotKeyspaceRequest,
};
use crate::id::{
    AppendPublishTicketId, AppendRunId, AppendStreamId, AppendTicketId, BlockCount, BlockIndex,
    CheckpointId, ClientEpoch, CommitGroupId, CommitSeq, DeviceGeneration, DeviceId, ExtentId,
    FileId, FileVersion, GrantEpoch, GrantId, GrantNonce, KeyspaceCatalogShardId,
    KeyspaceGeneration, KeyspaceId, KeyspaceRootId, LogicalDeadline, LogicalTime, MetadataNodeId,
    PrincipalId, RequestId, SegmentId, ServerIncarnation, ShardId, StorageNodeId, StorageNodeKeyId,
    TenantId, WriteIntentId, WriterEpoch,
};
use crate::object::{
    AppendLogRun, AppendLogRunRange, AppendVisiblePublish, Checkpoint, CheckpointRoots,
    CommitGroup, DeleteRecord, DeviceHead, FileCommit, FileHead, ForkRecord, KeyspaceCatalogShard,
    KeyspaceCommit, KeyspaceFile, KeyspaceHead, KeyspaceRoot, LeafEntry, MappingOwner,
    MetadataChild, MetadataNode, MetadataNodeKind, RootUpdate, RunBackedFileExtent,
    SegmentDescriptor, SegmentPayloadIntegrity, ShardCommit, ShardRootUpdate,
    coalesce_append_log_run_ranges,
};
use crate::provider::{
    CommitGroupIntent, DiagnosticsCounters, DiagnosticsGauges, DiagnosticsNodeSnapshot,
    DiagnosticsSnapshot, GrantReceiptAuthority, LocalSegmentCatalog, MetadataCreateDeviceRequest,
    MetadataCreateFileRequest, MetadataCreateKeyspaceRequest, MetadataFence, MetadataForkRequest,
    MetadataNodeWrite, MetadataPlane, MetadataSnapshotKeyspaceRequest, ObservableProvider,
    PlacementPolicy, ProofScheme, ReferenceEvidence, RetentionPolicy, SegmentReceiptLifecycle,
    SegmentReplicaCommit, SegmentReplicaPlacement, SegmentReservation, SegmentReservationIntent,
    SegmentStore, SegmentWriteReceipt, StorageEvent, StorageEventKind, StorageNodeCustodianReport,
    StorageNodeDirectory, StorageNodeMaintenanceObservation, StorageNodeMaintenanceReport,
    StorageNodeRequest, StorageNodeResponse, StorageNodeTransport, VerifiedSegmentReceipt,
    WriteGrant, WriteGrantIntent, WriteGrantRequest, deterministic_test_grant_hash_and_proof,
    deterministic_test_proof_for_grant, deterministic_test_proof_for_receipt,
    deterministic_test_proof_for_reference,
};

include!("profiles.rs");
include!("config.rs");
include!("observability.rs");
include!("proof.rs");
include!("storage_node.rs");
include!("read_path.rs");
include!("coordinator.rs");

mod txn_metadata;
pub use txn_metadata::{
    MetadataTxnMode, MetadataTxnProfile, MetadataTxnProfilePhase, TxnBlockCoordinator,
    TxnBlockMetadataPlane, TxnBlockWriteProfile,
};

include!("durable/paths.rs");
include!("durable/policy.rs");
include!("durable/block_delta.rs");
include!("durable/sqlite.rs");
include!("durable/data_log.rs");
include!("durable/persist.rs");
include!("durable/reopen.rs");
include!("durable/maintenance.rs");
include!("durable/coordinator.rs");
include!("metadata_tree.rs");
include!("metadata_plane.rs");
include!("segment_store.rs");
include!("segment_catalog.rs");
include!("server.rs");
include!("transport/in_process.rs");
include!("transport/chaos.rs");
include!("transport/remote.rs");
include!("transport/network.rs");
include!("transport/tcp.rs");
include!("client.rs");
include!("util.rs");
include!("codec.rs");

#[cfg(test)]
mod tests;

const KEYSPACE_CATALOG_SHARD_COUNT: usize = 256;
const LOCAL_TENANT_ID: TenantId = TenantId::from_raw(1);
const LOCAL_PRINCIPAL_ID: PrincipalId = PrincipalId::from_raw(1);
const LOCAL_GRANT_EPOCH: GrantEpoch = GrantEpoch::from_raw(1);
const LOCAL_GRANT_EXPIRATION: LogicalDeadline = LogicalDeadline::from_raw(u64::MAX);
const LOCAL_STORAGE_NODE_INCARNATION: ServerIncarnation = ServerIncarnation::from_raw(1);
const DEFAULT_OBSERVABILITY_EVENT_CAPACITY: usize = 1024;
const DEFAULT_BLOCK_BATCH_MAX_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_NATIVE_FILE_BATCH_MAX_BYTES: u64 = 32 * 1024 * 1024;
