use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rusqlite::Connection;
use std::{
    fs,
    hint::black_box,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use toy_cow_block_storage::api::BlockRange;
use toy_cow_block_storage::id::{BlockCount, BlockIndex, MetadataNodeId, SegmentId, StorageNodeId};
use toy_cow_block_storage::local::{
    ChaosStorageNodeTransport, DurableCoordinator, DurableDataLogPolicy, InMemoryMetadataPlane,
    InMemorySegmentStore, LocalCoordinator, LocalStoreConfig, MaintenanceMode, MaintenancePolicy,
};
use toy_cow_block_storage::object::{
    LeafEntry, MetadataNode, MetadataNodeKind, SegmentDescriptor, SegmentPayloadIntegrity,
};
use toy_cow_block_storage::provider::{
    MetadataCreateDeviceRequest, MetadataCreateFileRequest, MetadataCreateKeyspaceRequest,
    MetadataNodeWrite, MetadataPlane, MetadataSnapshotKeyspaceRequest, RetentionPolicy,
    SegmentReservation, SegmentStore, StorageNodeRequest, StorageNodeTransport,
};
use toy_cow_block_storage::sim::SeededRng;
use toy_cow_block_storage::{
    AppendStream, AppendStreamId, BlockClient, BlockDevice, BlockRequest, ByteRange, DeviceId,
    DeviceSpec, FileBatchWrite, FileId, ForkRequest, KeyspaceId, NativeFile, NativeKeyspaceClient,
    NativeRequest, PayloadIntegrity, RestorePoint, WriteDurability, WriterEpoch,
};

include!("local.rs");
include!("native.rs");
include!("durable.rs");
include!("group.rs");
