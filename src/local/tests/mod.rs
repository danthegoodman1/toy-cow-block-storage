use super::*;
use crate::api::{
    BlockBatchWrite, BlockRequest, CreateDeviceRequest, DeviceSpec, FlushScope, PayloadIntegrity,
    WriteDurability,
};
use crate::extent::{CreateFileRequest, CreateKeyspaceRequest, FileSpec};
use crate::id::{ClientEpoch, LogicalDeadline, ShardId, WriteIntentId};
use crate::object::{LeafEntry, ShardRootUpdate, replay_append_visible_publishes_for_file};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(1);

fn config() -> LocalStoreConfig {
    LocalStoreConfig {
        shard_count: 2,
        block_size: 4096,
        file_root_blocks: 8,
        metadata_fanout: 2,
        metadata_leaf_blocks: 1024,
        storage_node: StorageNodeId::from_raw(77),
        observability_event_capacity: DEFAULT_OBSERVABILITY_EVENT_CAPACITY,
        stream_auto_persist_bytes: None,
    }
}

fn tree_config() -> LocalStoreConfig {
    LocalStoreConfig {
        metadata_fanout: 2,
        metadata_leaf_blocks: 2,
        file_root_blocks: 32,
        ..config()
    }
}

fn durable_temp_dir(name: &str) -> PathBuf {
    let id = NEXT_TEMP_DIR.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "toy-cow-block-storage-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn device_request() -> MetadataCreateDeviceRequest {
    MetadataCreateDeviceRequest {
        spec: DeviceSpec {
            logical_blocks: 16,
            block_size: 4096,
        },
        name: Some("root".to_string()),
    }
}

#[test]
fn observability_ring_buffer_is_bounded_ordered_and_drainable() {
    let observability = Observability::new(2).unwrap();
    observability.record(StorageEventKind::GrantIssued);
    observability.record(StorageEventKind::MaintenancePlanned);
    observability.record(StorageEventKind::MaintenanceTicked);

    let (counters, events, len, capacity, last_sequence) = observability.snapshot_parts().unwrap();
    assert_eq!(counters.observability_events_recorded, 3);
    assert_eq!(counters.observability_events_dropped, 1);
    assert_eq!(len, 2);
    assert_eq!(capacity, 2);
    assert_eq!(last_sequence, 3);
    assert_eq!(events[0].sequence, 2);
    assert_eq!(events[1].sequence, 3);

    let drained = observability.drain_events(1).unwrap();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].sequence, 2);
    let (_, events, len, _, _) = observability.snapshot_parts().unwrap();
    assert_eq!(len, 1);
    assert_eq!(events[0].sequence, 3);
}

#[test]
fn observability_names_and_event_kinds_are_stable() {
    assert_eq!(
        crate::provider::DIAGNOSTICS_COUNTER_NAMES,
        &[
            "observability_events_recorded",
            "observability_events_dropped",
            "coordinator_write_attempts",
            "coordinator_write_publish_successes",
            "coordinator_write_publish_failures",
            "coordinator_write_unavailable",
            "coordinator_write_idempotency_hits",
            "metadata_stale_fences",
            "metadata_custodian_runs",
            "storage_node_custodian_runs",
            "storage_segment_writes",
            "storage_segment_duplicate_writes",
            "storage_segment_references",
            "storage_segment_releases",
            "maintenance_plans",
            "maintenance_ticks",
            "maintenance_logs_selected",
            "maintenance_logs_skipped",
            "maintenance_bytes_copied",
            "maintenance_bytes_deleted",
            "grants_issued",
            "grant_rejections",
            "receipts_verified",
            "receipt_rejections",
            "receipt_rejected_bad_proof",
            "receipt_rejected_scope",
            "receipt_rejected_epoch",
            "receipt_rejected_replay",
        ]
    );
    assert_eq!(
        crate::provider::DIAGNOSTICS_GAUGE_NAMES,
        &[
            "live_device_heads",
            "deleted_device_heads",
            "live_keyspace_heads",
            "metadata_nodes",
            "commit_seq",
            "checkpoint_count",
            "gc_epoch",
            "pending_release_evidence",
            "sqlite_wal_bytes",
            "maintenance_dirty_bytes",
            "maintenance_reclaimable_bytes",
            "maintenance_sealed_logs",
            "event_buffer_len",
            "event_buffer_capacity",
            "last_event_sequence",
        ]
    );
    assert_eq!(
        crate::provider::STORAGE_EVENT_KIND_NAMES,
        &[
            "CoordinatorWriteStarted",
            "CoordinatorWriteUnavailable",
            "StorageSegmentWritten",
            "StorageSegmentWriteRetried",
            "StorageSegmentReferenced",
            "StorageSegmentReleased",
            "MetadataPublishSucceeded",
            "MetadataPublishFailed",
            "DeviceForked",
            "DeviceRestored",
            "KeyspaceRestored",
            "MetadataCustodianRan",
            "StorageNodeCustodianRan",
            "MaintenancePlanned",
            "MaintenanceTicked",
            "GrantIssued",
            "GrantRejected",
            "ReceiptVerified",
            "ReceiptRejected",
        ]
    );
}

#[test]
fn observability_tracks_local_write_restore_gc_and_custodians() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device_id = store
        .metadata()
        .create_device(device_request())
        .unwrap()
        .device_id;
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 7),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
    store
        .fork_device(
            device_id,
            ForkRequest {
                target: None,
                name: None,
            },
        )
        .unwrap();
    store
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    store.delete_device(device_id).unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();

    let snapshot = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.counters.coordinator_write_attempts, 1);
    assert_eq!(snapshot.counters.coordinator_write_publish_successes, 1);
    assert_eq!(snapshot.counters.storage_segment_writes, 1);
    assert_eq!(snapshot.counters.storage_segment_references, 1);
    assert_eq!(snapshot.counters.metadata_custodian_runs, 1);
    assert_eq!(snapshot.counters.storage_node_custodian_runs, 1);
    assert_eq!(snapshot.gauges.live_device_heads, 2);
    assert_eq!(snapshot.gauges.deleted_device_heads, 0);
    assert_eq!(snapshot.nodes[0].referenced_segments, 1);

    let kinds: Vec<_> = snapshot
        .recent_events
        .iter()
        .map(|event| event.kind)
        .collect();
    assert!(kinds.contains(&StorageEventKind::CoordinatorWriteStarted));
    assert!(kinds.contains(&StorageEventKind::StorageSegmentWritten));
    assert!(kinds.contains(&StorageEventKind::MetadataPublishSucceeded));
    assert!(kinds.contains(&StorageEventKind::StorageSegmentReferenced));
    assert!(kinds.contains(&StorageEventKind::DeviceForked));
    assert!(kinds.contains(&StorageEventKind::DeviceRestored));
    assert!(kinds.contains(&StorageEventKind::MetadataCustodianRan));
    assert!(kinds.contains(&StorageEventKind::StorageNodeCustodianRan));

    let second = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot, second);
    let drained = store.drain_events(usize::MAX).unwrap();
    assert_eq!(drained.len(), snapshot.recent_events.len());
    assert!(
        store
            .diagnostics_snapshot()
            .unwrap()
            .recent_events
            .is_empty()
    );
}

#[test]
fn observability_tracks_native_keyspace_restore_through_public_server() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);
    file.write_at(0, b"before").unwrap();
    let checkpoint = client.checkpoint_keyspace(keyspace_id).unwrap();
    file.write_at(0, b"after!").unwrap();

    let restored = client
        .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    assert_ne!(restored, keyspace_id);
    let snapshot = store.diagnostics_snapshot().unwrap();
    assert!(snapshot.recent_events.iter().any(|event| event.kind
        == StorageEventKind::KeyspaceRestored
        && event.commit_seq.is_some()));
}

#[test]
fn observability_event_overflow_is_deterministic() {
    let cfg = LocalStoreConfig {
        observability_event_capacity: 2,
        ..config()
    };
    let store = LocalCoordinator::with_config(cfg).unwrap();
    let device_id = store
        .metadata()
        .create_device(device_request())
        .unwrap()
        .device_id;
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 3),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let snapshot = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.recent_events.len(), 2);
    assert!(snapshot.counters.observability_events_dropped > 0);
    assert_eq!(
        snapshot.gauges.last_event_sequence,
        snapshot.counters.observability_events_recorded
    );
    assert!(
        snapshot.recent_events[0].sequence < snapshot.recent_events[1].sequence,
        "bounded event buffer must preserve oldest-to-newest order"
    );
}

#[test]
fn observability_tracks_receipt_rejection_reasons() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device_id = store
        .metadata()
        .create_device(device_request())
        .unwrap()
        .device_id;
    let range = crate::api::BlockRange::new(BlockIndex::from_raw(0), BlockCount::from_raw(1));
    let grant = store
        .issue_block_write_grant(device_id, range, WriteDurability::Acknowledged)
        .unwrap();
    let mut receipt = store
        .write_granted_segment(&grant, repeated_blocks(1, 9))
        .unwrap();
    receipt.proof = crate::provider::ProofTag([0xff; 32]);

    assert!(store.submit_block_write_receipt(&grant, receipt).is_err());
    let snapshot = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.counters.receipt_rejections, 1);
    assert_eq!(snapshot.counters.receipt_rejected_bad_proof, 1);
    assert!(
        snapshot
            .recent_events
            .iter()
            .any(|event| event.kind == StorageEventKind::ReceiptRejected
                && event.reason == Some("bad_proof"))
    );
}

#[test]
fn durable_reopen_diagnostics_match_persisted_state_without_replaying_events() {
    let root = durable_temp_dir("observability-reopen");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    let before = store.diagnostics_snapshot().unwrap();
    assert_eq!(before.counters.coordinator_write_attempts, 1);
    assert_eq!(before.nodes[0].referenced_segments, 1);
    assert!(before.nodes[0].active_log_bytes > 0);
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let after = reopened.diagnostics_snapshot().unwrap();
    assert_eq!(after.counters.coordinator_write_attempts, 0);
    assert_eq!(after.gauges.live_device_heads, 1);
    assert_eq!(after.nodes[0].referenced_segments, 1);
    assert!(after.nodes[0].active_log_bytes > 0);
    assert!(after.recent_events.is_empty());
    assert_eq!(after, reopened.diagnostics_snapshot().unwrap());
    drop(reopened);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_reopen_allows_observability_capacity_change_without_state_migration() {
    let root = durable_temp_dir("observability-capacity-reopen");
    let initial_cfg = LocalStoreConfig {
        observability_event_capacity: 2,
        ..config()
    };
    let store = DurableCoordinator::open(&root, initial_cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 6),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let larger_buffer_cfg = LocalStoreConfig {
        observability_event_capacity: 8,
        ..config()
    };
    let reopened = DurableCoordinator::open(&root, larger_buffer_cfg).unwrap();
    assert_eq!(
        reopened
            .diagnostics_snapshot()
            .unwrap()
            .gauges
            .event_buffer_capacity,
        8
    );
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 6));
    drop(reopened);

    let bad_shape = LocalStoreConfig {
        block_size: 8192,
        observability_event_capacity: 8,
        ..config()
    };
    let error = DurableCoordinator::open(&root, bad_shape).unwrap_err();
    assert_eq!(
        error,
        StorageError::corrupt("durable SQLite state disagrees with open config")
    );
    let _ = fs::remove_dir_all(root);
}

fn metadata_leaf(node_id: u128, start: u64, blocks: u64) -> MetadataNode {
    MetadataNode {
        node_id: MetadataNodeId::from_raw(node_id),
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(start),
            BlockCount::from_raw(blocks),
        ),
        kind: MetadataNodeKind::Leaf {
            entries: Vec::new(),
            run_extents: Vec::new(),
        },
    }
}

fn reservation_intent() -> SegmentReservationIntent {
    SegmentReservationIntent {
        write_intent: WriteIntentId::from_raw(1),
        owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
        bytes: 4096,
    }
}

fn receipt_for_commit(
    intent: SegmentReservationIntent,
    commit: SegmentReplicaCommit,
) -> SegmentWriteReceipt {
    let authority = LocalGrantReceiptAuthority;
    let grant = authority
        .issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::Internal {
                owner: intent.owner,
            },
            write_intent: intent.write_intent,
            segment_id: commit.descriptor.segment_id,
            storage_node: commit.placement.storage_node,
            max_bytes: commit.descriptor.bytes,
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
        .unwrap();
    authority
        .create_segment_receipt(&grant, commit, LOCAL_STORAGE_NODE_INCARNATION)
        .unwrap()
}

fn verified_receipt_for_commit(
    intent: SegmentReservationIntent,
    commit: SegmentReplicaCommit,
) -> VerifiedSegmentReceipt {
    let authority = LocalGrantReceiptAuthority;
    let receipt = receipt_for_commit(intent, commit);
    authority.verify_segment_receipt(&receipt).unwrap()
}

fn grant_for_segment(
    storage_node: StorageNodeId,
    segment_id: SegmentId,
    write_intent: WriteIntentId,
    owner: MappingOwner,
    bytes: u64,
) -> WriteGrant {
    LocalGrantReceiptAuthority
        .issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::Internal { owner },
            write_intent,
            segment_id,
            storage_node,
            max_bytes: bytes,
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
        .unwrap()
}

fn resign_grant(grant: &mut WriteGrant) {
    grant.proof = deterministic_test_proof_for_grant(grant.key_id, grant);
}

fn resign_receipt(receipt: &mut SegmentWriteReceipt) {
    receipt.proof = deterministic_test_proof_for_receipt(receipt.node_key_id, receipt);
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

trait ProviderConformanceStore {
    fn create_device_for_conformance(&self, request: CreateDeviceRequest) -> Result<DeviceId>;
    fn checkpoint_device_for_conformance(&self, device_id: DeviceId) -> Result<CheckpointId>;
    fn write_device_for_conformance(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
    ) -> Result<WriteCommit>;
    fn read_device_for_conformance(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()>;
    fn create_keyspace_for_conformance(&self, request: CreateKeyspaceRequest)
    -> Result<KeyspaceId>;
    fn create_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    ) -> Result<FileId>;
    fn checkpoint_keyspace_for_conformance(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId>;
    fn snapshot_keyspace_for_conformance(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId>;
    fn write_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
    ) -> Result<FileWriteCommit>;
    fn append_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
    ) -> Result<AppendPublishCommit>;
    fn read_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()>;
}

impl ProviderConformanceStore for LocalCoordinator {
    fn create_device_for_conformance(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.metadata()
            .create_device(MetadataCreateDeviceRequest::from(request))
            .map(|head| head.device_id)
    }

    fn checkpoint_device_for_conformance(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.metadata().checkpoint(device_id)
    }

    fn write_device_for_conformance(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
    ) -> Result<WriteCommit> {
        self.write_device(device_id, offset, data, WriteDurability::Acknowledged)
    }

    fn read_device_for_conformance(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.read_device(device_id, range, buf)
    }

    fn create_keyspace_for_conformance(
        &self,
        request: CreateKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.metadata()
            .create_keyspace(MetadataCreateKeyspaceRequest { request })
            .map(|head| head.keyspace_id)
    }

    fn create_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    ) -> Result<FileId> {
        self.metadata()
            .create_file(MetadataCreateFileRequest {
                keyspace_id,
                request,
            })
            .map(|head| head.file_id)
    }

    fn checkpoint_keyspace_for_conformance(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        self.metadata().checkpoint_keyspace(keyspace_id)
    }

    fn snapshot_keyspace_for_conformance(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.metadata()
            .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                source,
                target: request.target,
                name: request.name,
            })
            .map(|head| head.keyspace_id)
    }

    fn write_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
    ) -> Result<FileWriteCommit> {
        self.commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(offset, data.to_vec())],
            WriteDurability::Acknowledged,
        )
    }

    fn append_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
    ) -> Result<AppendPublishCommit> {
        let stream = self.open_append_stream(keyspace_id, file_id)?;
        let ticket = self.append_stream(&stream, data, WriteDurability::Acknowledged)?;
        self.publish_append_stream(
            &stream,
            ticket.range.end_exclusive()?,
            WriteDurability::Acknowledged,
        )
    }

    fn read_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.read_file(keyspace_id, file_id, range, buf)
    }
}

impl ProviderConformanceStore for DurableCoordinator {
    fn create_device_for_conformance(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.create_device(request)
    }

    fn checkpoint_device_for_conformance(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.checkpoint(device_id)
    }

    fn write_device_for_conformance(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
    ) -> Result<WriteCommit> {
        self.write_device(device_id, offset, data, WriteDurability::Flushed)
    }

    fn read_device_for_conformance(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.read_device(device_id, range, buf)
    }

    fn create_keyspace_for_conformance(
        &self,
        request: CreateKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.create_keyspace(request)
    }

    fn create_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    ) -> Result<FileId> {
        self.create_file(keyspace_id, request)
    }

    fn checkpoint_keyspace_for_conformance(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        self.checkpoint_keyspace(keyspace_id)
    }

    fn snapshot_keyspace_for_conformance(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.snapshot_keyspace(source, request)
    }

    fn write_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
    ) -> Result<FileWriteCommit> {
        self.commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(offset, data.to_vec())],
            WriteDurability::Flushed,
        )
    }

    fn append_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
    ) -> Result<AppendPublishCommit> {
        let stream = self.open_append_stream(keyspace_id, file_id)?;
        let ticket = self.append_stream(&stream, data, WriteDurability::Acknowledged)?;
        self.publish_append_stream(&stream, ticket.range.end_exclusive()?)
    }

    fn read_file_for_conformance(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.read_file(keyspace_id, file_id, range, buf)
    }
}

#[derive(Debug, Clone, Copy)]
struct ProviderConformanceOutcome {
    device_id: DeviceId,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    snapshot_keyspace: KeyspaceId,
}

fn run_provider_conformance(store: &dyn ProviderConformanceStore) -> ProviderConformanceOutcome {
    let device_id = store
        .create_device_for_conformance(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 32,
                block_size: 4096,
            },
            name: Some("conformance-device".to_string()),
        })
        .unwrap();
    let first = store
        .write_device_for_conformance(device_id, 0, &repeated_blocks(2, 3))
        .unwrap();
    let checkpoint = store.checkpoint_device_for_conformance(device_id).unwrap();
    let second = store
        .write_device_for_conformance(device_id, 4096, &repeated_blocks(2, 7))
        .unwrap();
    assert!(second.commit_seq.raw() > first.commit_seq.raw());

    let mut device_bytes = vec![0; 3 * 4096];
    store
        .read_device_for_conformance(device_id, ByteRange::new(0, 3 * 4096), &mut device_bytes)
        .unwrap();
    assert_eq!(&device_bytes[0..4096], vec![3; 4096].as_slice());
    assert_eq!(&device_bytes[4096..12288], repeated_blocks(2, 7).as_slice());
    assert!(checkpoint.raw() > 0);

    let keyspace_id = store
        .create_keyspace_for_conformance(CreateKeyspaceRequest {
            name: Some("conformance-keyspace".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file_for_conformance(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store
        .write_file_for_conformance(keyspace_id, file_id, 0, b"alpha")
        .unwrap();
    let keyspace_checkpoint = store
        .checkpoint_keyspace_for_conformance(keyspace_id)
        .unwrap();
    let snapshot_keyspace = store
        .snapshot_keyspace_for_conformance(
            keyspace_id,
            SnapshotKeyspaceRequest {
                target: None,
                name: Some("snapshot".to_string()),
            },
        )
        .unwrap();
    store
        .append_file_for_conformance(keyspace_id, file_id, b"-beta")
        .unwrap();

    let mut source = vec![0; b"alpha-beta".len()];
    store
        .read_file_for_conformance(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"alpha-beta".len() as u64),
            &mut source,
        )
        .unwrap();
    assert_eq!(source, b"alpha-beta");

    let mut snapshot = vec![0; b"alpha".len()];
    store
        .read_file_for_conformance(
            snapshot_keyspace,
            file_id,
            ByteRange::new(0, b"alpha".len() as u64),
            &mut snapshot,
        )
        .unwrap();
    assert_eq!(snapshot, b"alpha");
    assert!(keyspace_checkpoint.raw() > 0);
    ProviderConformanceOutcome {
        device_id,
        keyspace_id,
        file_id,
        snapshot_keyspace,
    }
}

fn start_tcp_wire_server(endpoint: Arc<dyn RemoteWireTransport>) -> TcpRemoteWireServer {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    TcpRemoteWireServer::start(listener, endpoint, DEFAULT_NETWORK_MAX_FRAME_BYTES).unwrap()
}

#[test]
fn metadata_nodes_are_immutable_and_missing_lookup_errors() {
    let metadata = InMemoryMetadataPlane::new(config()).unwrap();
    let node = metadata_leaf(999, 0, 4);

    metadata
        .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
        .unwrap();
    assert_eq!(metadata.get_metadata_node(node.node_id).unwrap(), node);
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
        .unwrap();

    let changed = MetadataNode {
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(4),
            BlockCount::from_raw(4),
        ),
        ..node.clone()
    };
    assert!(
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(changed, Vec::new()))
            .is_err()
    );
    assert!(
        metadata
            .get_metadata_node(MetadataNodeId::from_raw(1000))
            .is_err()
    );
}

#[test]
fn metadata_publish_merges_independent_shards_and_checks_missing_roots() {
    let metadata = InMemoryMetadataPlane::new(config()).unwrap();
    let head = metadata.create_device(device_request()).unwrap();
    let new_node = metadata_leaf(999, 0, 8);
    let shard_one_node = metadata_leaf(1000, 8, 8);
    let stale_same_shard_node = metadata_leaf(1001, 0, 8);
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(new_node.clone(), Vec::new()))
        .unwrap();
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(shard_one_node.clone(), Vec::new()))
        .unwrap();
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(
            stale_same_shard_node.clone(),
            Vec::new(),
        ))
        .unwrap();

    let stale_missing = CommitGroupIntent {
        owner: MappingOwner::BlockDevice(head.device_id),
        fence: MetadataFence::DeviceGeneration(head.generation),
        updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
            shard_id: ShardId::from_raw(0),
            old_root: head.shard_roots[0],
            new_root: MetadataNodeId::from_raw(404),
        })],
    };
    assert!(metadata.publish_commit_group(stale_missing).is_err());
    assert_eq!(metadata.get_head(head.device_id).unwrap(), head);

    let commit = metadata
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: new_node.node_id,
            })],
        })
        .unwrap();
    assert_eq!(commit.commit_seq, CommitSeq::from_raw(1));

    let updated = metadata.get_head(head.device_id).unwrap();
    assert_eq!(updated.shard_roots[0], new_node.node_id);
    assert_eq!(updated.generation, DeviceGeneration::from_raw(1));

    let independent = metadata
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(1),
                old_root: head.shard_roots[1],
                new_root: shard_one_node.node_id,
            })],
        })
        .unwrap();
    assert_eq!(independent.commit_seq, CommitSeq::from_raw(2));

    let merged = metadata.get_head(head.device_id).unwrap();
    assert_eq!(merged.shard_roots[0], new_node.node_id);
    assert_eq!(merged.shard_roots[1], shard_one_node.node_id);
    assert_eq!(merged.generation, DeviceGeneration::from_raw(2));

    let stale_same_shard = CommitGroupIntent {
        owner: MappingOwner::BlockDevice(head.device_id),
        fence: MetadataFence::DeviceGeneration(head.generation),
        updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
            shard_id: ShardId::from_raw(0),
            old_root: head.shard_roots[0],
            new_root: stale_same_shard_node.node_id,
        })],
    };
    assert!(metadata.publish_commit_group(stale_same_shard).is_err());
    assert_eq!(metadata.get_head(head.device_id).unwrap(), merged);
}

#[test]
fn metadata_node_persist_requires_verified_receipt_evidence() {
    let metadata = InMemoryMetadataPlane::new(config()).unwrap();
    let descriptor = SegmentDescriptor {
        segment_id: SegmentId::from_raw(700),
        blocks: BlockCount::from_raw(1),
        bytes: 4096,
        integrity: SegmentPayloadIntegrity::Unchecked,
    };
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(700),
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(0),
            BlockCount::from_raw(1),
        ),
        kind: MetadataNodeKind::Leaf {
            entries: vec![LeafEntry {
                logical_start: BlockIndex::from_raw(0),
                blocks: BlockCount::from_raw(1),
                segment_id: descriptor.segment_id,
                segment_offset: BlockIndex::from_raw(0),
            }],
            run_extents: Vec::new(),
        },
    };

    assert!(
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
            .is_err()
    );
    let receipt = verified_receipt_for_commit(
        reservation_intent(),
        SegmentReplicaCommit {
            descriptor,
            placement: SegmentReplicaPlacement {
                segment_id: SegmentId::from_raw(700),
                storage_node: config().storage_node,
                offset: 0,
                bytes: 4096,
            },
        },
    );
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(node.clone(), vec![receipt]))
        .unwrap();
    assert_eq!(metadata.get_metadata_node(node.node_id).unwrap(), node);
}

#[test]
fn file_commit_uses_version_fence_and_roots_for_gc_include_live_owners() {
    let metadata = InMemoryMetadataPlane::new(config()).unwrap();
    let keyspace = metadata
        .create_keyspace(MetadataCreateKeyspaceRequest {
            request: CreateKeyspaceRequest { name: None },
        })
        .unwrap();
    let file = metadata
        .create_file(MetadataCreateFileRequest {
            keyspace_id: keyspace.keyspace_id,
            request: CreateFileRequest {
                spec: FileSpec {
                    name: Some("log".to_string()),
                },
            },
        })
        .unwrap();
    let new_root = metadata_leaf(1001, 0, 8);
    metadata
        .persist_metadata_node(MetadataNodeWrite::new(new_root.clone(), Vec::new()))
        .unwrap();

    metadata
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::NativeKeyspace(keyspace.keyspace_id),
            fence: MetadataFence::FileVersion(file.version),
            updates: vec![RootUpdate::FileRoot {
                file_id: file.file_id,
                old_root: file.root,
                new_root: new_root.node_id,
                new_size: 0,
            }],
        })
        .unwrap();

    let updated = metadata
        .get_file_head(keyspace.keyspace_id, file.file_id)
        .unwrap();
    assert_eq!(updated.root, new_root.node_id);
    assert_eq!(updated.version, FileVersion::from_raw(1));

    let roots = metadata
        .roots_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    assert!(roots.contains(&new_root.node_id));
}

#[test]
fn delete_moves_device_out_of_live_catalog_without_deleting_objects() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    let device_id = device.device_id();
    device.write_at(0, &[7; 4096]).unwrap();
    let head_before_delete = store.metadata().get_head(device_id).unwrap();
    let node_count_before_delete = store.metadata().metadata_node_count().unwrap();
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );

    let delete = device.delete().unwrap();

    assert_eq!(delete.device_id, device_id);
    assert!(delete.commit_seq.raw() > head_before_delete.latest_commit.raw());
    assert_eq!(store.metadata().list_live_devices().unwrap(), Vec::new());
    assert_eq!(
        store.metadata().list_deleted_devices().unwrap(),
        vec![device_id]
    );
    assert!(store.metadata().get_head(device_id).is_err());
    assert!(device.info().is_err());
    assert!(device.read_at(0, &mut [0; 4096]).is_err());
    assert!(device.write_at(0, &[8; 4096]).is_err());
    assert!(device.delete().is_err());
    assert_eq!(
        store
            .metadata()
            .delete_record(delete.commit_seq)
            .unwrap()
            .shard_roots,
        head_before_delete.shard_roots
    );
    assert_eq!(
        store.metadata().metadata_node_count().unwrap(),
        node_count_before_delete
    );
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );
}

#[test]
fn failed_delete_publish_preserves_live_head() {
    let metadata = InMemoryMetadataPlane::new(config()).unwrap();
    let head = metadata.create_device(device_request()).unwrap();
    metadata.set_next_commit_seq_for_test(u64::MAX).unwrap();

    assert!(metadata.delete_device(head.device_id).is_err());
    assert_eq!(metadata.get_head(head.device_id).unwrap(), head);
    assert_eq!(metadata.list_live_devices().unwrap(), vec![head.device_id]);
    assert_eq!(metadata.list_deleted_devices().unwrap(), Vec::new());
}

#[test]
fn roots_for_gc_respects_deleted_device_retention_policy() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    let device_id = device.device_id();
    device.write_at(0, &[7; 4096]).unwrap();
    let checkpoint_id = store.metadata().checkpoint(device_id).unwrap();
    device.write_at(4096, &[8; 4096]).unwrap();
    let delete = device.delete().unwrap();
    let checkpoint = store.metadata().get_checkpoint(checkpoint_id).unwrap();
    let delete_record = store.metadata().delete_record(delete.commit_seq).unwrap();

    let without_retention = store
        .metadata()
        .roots_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    assert!(without_retention.is_empty());

    let with_retention = store
        .metadata()
        .roots_for_gc(RetentionPolicy::retain_deleted_devices())
        .unwrap();
    let mut sorted = with_retention.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(with_retention, sorted);
    for root in &delete_record.shard_roots {
        assert!(with_retention.contains(root));
    }

    let with_pitr_retention = store
        .metadata()
        .roots_for_gc(RetentionPolicy::retain_everything())
        .unwrap();
    for root in &InMemoryMetadataPlane::checkpoint_block_roots(&checkpoint).unwrap() {
        assert!(with_pitr_retention.contains(root));
    }
    for root in &delete_record.shard_roots {
        assert!(with_pitr_retention.contains(root));
    }
}

#[test]
fn generated_delete_retention_roots_match_reference_model() {
    fn expected_roots(
        live_roots: &BTreeMap<DeviceId, Vec<MetadataNodeId>>,
        deleted_roots: &BTreeMap<DeviceId, Vec<MetadataNodeId>>,
        checkpoint_roots: &[(DeviceId, Vec<MetadataNodeId>)],
        retain_deleted: bool,
        retain_pitr: bool,
    ) -> Vec<MetadataNodeId> {
        let mut roots = Vec::new();
        for roots_for_device in live_roots.values() {
            roots.extend(roots_for_device.iter().copied());
        }
        if retain_pitr {
            for (device_id, roots_for_checkpoint) in checkpoint_roots {
                if live_roots.contains_key(device_id)
                    || retain_deleted && deleted_roots.contains_key(device_id)
                {
                    roots.extend(roots_for_checkpoint.iter().copied());
                }
            }
        }
        if retain_deleted {
            for roots_for_device in deleted_roots.values() {
                roots.extend(roots_for_device.iter().copied());
            }
        }
        roots.sort();
        roots.dedup();
        roots
    }

    for seed in 0..10 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(config()).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let mut live_roots: BTreeMap<DeviceId, Vec<MetadataNodeId>> = BTreeMap::new();
        let mut deleted_roots: BTreeMap<DeviceId, Vec<MetadataNodeId>> = BTreeMap::new();
        let mut checkpoint_roots: Vec<(DeviceId, Vec<MetadataNodeId>)> = Vec::new();

        for create_index in 0..3 {
            let device_id = client
                .create_device(CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 16,
                        block_size: 4096,
                    },
                    name: Some(format!("seed-{seed}-{create_index}")),
                })
                .unwrap();
            let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
            live_roots.insert(device_id, roots.clone());
            checkpoint_roots.push((device_id, roots));
        }

        for step in 0..30 {
            if live_roots.is_empty() {
                let device_id = client
                    .create_device(CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: Some(format!("seed-{seed}-recreate-{step}")),
                    })
                    .unwrap();
                let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
                harness
                    .trace
                    .record(format!("create step={step} device={device_id}"));
                live_roots.insert(device_id, roots.clone());
                checkpoint_roots.push((device_id, roots));
            }

            let live_ids: Vec<_> = live_roots.keys().copied().collect();
            let device_id = live_ids[harness.rng.choose_index(live_ids.len()).unwrap()];
            match harness.rng.next_u64() % 4 {
                0 => {
                    let block = harness.rng.next_u64() % 16;
                    let byte = (1 + harness.rng.next_u64() % 254) as u8;
                    harness.trace.record(format!(
                        "write step={step} device={device_id} block={block} byte={byte}"
                    ));
                    client
                        .open_device(device_id)
                        .unwrap()
                        .write_at(block * 4096, &[byte; 4096])
                        .unwrap();
                    let roots = store.metadata().get_head(device_id).unwrap().shard_roots;
                    live_roots.insert(device_id, roots);
                }
                1 => {
                    harness
                        .trace
                        .record(format!("checkpoint step={step} device={device_id}"));
                    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
                    let checkpoint = store.metadata().get_checkpoint(checkpoint).unwrap();
                    checkpoint_roots.push((
                        device_id,
                        InMemoryMetadataPlane::checkpoint_block_roots(&checkpoint).unwrap(),
                    ));
                }
                2 if live_roots.len() + deleted_roots.len() < 8 => {
                    harness
                        .trace
                        .record(format!("fork step={step} source={device_id}"));
                    let child = client
                        .open_device(device_id)
                        .unwrap()
                        .fork(ForkRequest {
                            target: None,
                            name: Some(format!("fork-{seed}-{step}")),
                        })
                        .unwrap();
                    let roots = store.metadata().get_head(child).unwrap().shard_roots;
                    live_roots.insert(child, roots.clone());
                    checkpoint_roots.push((child, roots));
                }
                _ => {
                    harness
                        .trace
                        .record(format!("delete step={step} device={device_id}"));
                    let roots = live_roots.remove(&device_id).unwrap();
                    let delete = client.open_device(device_id).unwrap().delete().unwrap();
                    assert_eq!(
                        store
                            .metadata()
                            .delete_record(delete.commit_seq)
                            .unwrap()
                            .shard_roots,
                        roots
                    );
                    deleted_roots.insert(device_id, roots);
                }
            }

            assert_eq!(
                store.metadata().list_live_devices().unwrap(),
                live_roots.keys().copied().collect::<Vec<_>>(),
                "seed={seed} trace={:?}",
                harness.trace.events()
            );
            assert_eq!(
                store.metadata().list_deleted_devices().unwrap(),
                deleted_roots.keys().copied().collect::<Vec<_>>(),
                "seed={seed} trace={:?}",
                harness.trace.events()
            );
            assert_eq!(
                store
                    .metadata()
                    .roots_for_gc(RetentionPolicy::expire_deleted_immediately())
                    .unwrap(),
                expected_roots(&live_roots, &deleted_roots, &checkpoint_roots, false, false),
                "seed={seed} trace={:?}",
                harness.trace.events()
            );
            assert_eq!(
                store
                    .metadata()
                    .roots_for_gc(RetentionPolicy::retain_deleted_devices())
                    .unwrap(),
                expected_roots(&live_roots, &deleted_roots, &checkpoint_roots, true, false),
                "seed={seed} trace={:?}",
                harness.trace.events()
            );
        }
    }
}

#[test]
fn deleted_device_can_restore_from_retained_checkpoint_but_not_after_delete_time() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let device = client.open_device(device_id).unwrap();
    device.write_at(0, &[3; 4096]).unwrap();
    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
    device.write_at(0, &[4; 4096]).unwrap();
    let delete = device.delete().unwrap();

    let restored_id = device
        .restore(RestorePoint::Checkpoint(checkpoint))
        .expect("checkpoint roots are retained before GC");
    let restored = client.open_device(restored_id).unwrap();
    let mut bytes = [0; 4096];
    restored.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes, [3; 4096]);

    assert!(
        store
            .metadata()
            .restore_device(
                device_id,
                RestorePoint::Time(LogicalTime::from_raw(delete.commit_seq.raw()))
            )
            .is_err()
    );
}

#[test]
fn metadata_gc_releases_deleted_device_segments_after_retention_expires() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    let device_id = device.device_id();
    device.write_at(0, &[7; 4096]).unwrap();
    device.delete().unwrap();

    let report = store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();

    assert!(!report.sweep.deleted_metadata_nodes.is_empty());
    assert_eq!(report.sweep.released_segments, vec![SegmentId::from_raw(1)]);
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Released
    );
    assert!(
        store
            .segment_store()
            .contains_segment(SegmentId::from_raw(1))
            .unwrap()
    );

    let storage_report = store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert_eq!(
        storage_report.deleted_released_segments,
        vec![SegmentId::from_raw(1)]
    );
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Freed
    );
    assert!(
        !store
            .segment_store()
            .contains_segment(SegmentId::from_raw(1))
            .unwrap()
    );
    assert!(store.metadata().get_head(device_id).is_err());
}

#[test]
fn deleted_device_retention_can_expire_by_commit_age() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    let device_id = device.device_id();
    device.write_at(0, &[7; 4096]).unwrap();
    device.delete().unwrap();

    let retained = store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_after_commits(2))
        .unwrap();
    assert!(retained.sweep.released_segments.is_empty());
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );
    assert_eq!(
        store.metadata().list_deleted_devices().unwrap(),
        vec![device_id]
    );

    let other = create_local_device(&store, 16);
    other.write_at(0, &[8; 4096]).unwrap();
    let still_retained = store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_after_commits(2))
        .unwrap();
    assert!(still_retained.sweep.released_segments.is_empty());

    other.write_at(4096, &[9; 4096]).unwrap();
    let expired = store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_after_commits(2))
        .unwrap();
    assert_eq!(
        expired.sweep.released_segments,
        vec![SegmentId::from_raw(1)]
    );
    assert_eq!(store.metadata().list_deleted_devices().unwrap(), Vec::new());
}

#[test]
fn retention_expiring_gc_prunes_deleted_pitr_catalog() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    let device_id = device.device_id();
    device.write_at(0, &[7; 4096]).unwrap();
    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
    device.delete().unwrap();

    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();

    assert_eq!(store.metadata().list_deleted_devices().unwrap(), Vec::new());
    assert!(
        store
            .metadata()
            .roots_for_gc(RetentionPolicy::retain_deleted_devices())
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
            .is_err()
    );
}

#[test]
fn metadata_gc_retains_deleted_pitr_roots_when_policy_requires_it() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let device = client.open_device(device_id).unwrap();
    device.write_at(0, &[9; 4096]).unwrap();
    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
    device.delete().unwrap();

    let report = store
        .run_metadata_custodian(RetentionPolicy::retain_everything())
        .unwrap();

    assert!(report.sweep.released_segments.is_empty());
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );
    let restored_id = store
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    let restored = client.open_device(restored_id).unwrap();
    let mut bytes = [0; 4096];
    restored.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes, [9; 4096]);
}

#[test]
fn paused_gc_sweep_preserves_nodes_marked_in_epoch() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);
    device.write_at(0, &[5; 4096]).unwrap();
    let mark = store
        .mark_reachable_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    assert!(mark.metadata_nodes.iter().all(|node| {
        store.metadata().last_mark_epoch_for_node(*node).unwrap() == Some(mark.epoch)
    }));
    assert_eq!(
        store
            .metadata()
            .last_mark_epoch_for_segment(SegmentId::from_raw(1))
            .unwrap(),
        Some(mark.epoch)
    );

    device.delete().unwrap();
    let first_sweep = store
        .sweep_metadata_after_mark(RetentionPolicy::expire_deleted_immediately(), mark.epoch)
        .unwrap();
    for node in &mark.metadata_nodes {
        assert!(store.metadata().get_metadata_node(*node).is_ok());
    }
    assert!(first_sweep.released_segments.is_empty());
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );

    let second = store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    assert!(!second.sweep.deleted_metadata_nodes.is_empty());
    assert_eq!(second.sweep.released_segments, vec![SegmentId::from_raw(1)]);
}

#[test]
fn generated_gc_interleavings_preserve_live_device_models() {
    fn assert_live_models(
        store: &LocalCoordinator,
        client: &LocalBlockClient,
        models: &BTreeMap<DeviceId, Vec<u8>>,
        seed: u64,
        trace: &[String],
    ) {
        for (device_id, model) in models {
            let device = client.open_device(*device_id).unwrap();
            let mut actual = vec![0; model.len() * 4096];
            device.read_at(0, &mut actual).unwrap();
            assert_model_blocks(
                &actual,
                model,
                seed,
                trace,
                &render_device_roots(store, *device_id),
            );
            validate_device_roots(store, *device_id);
        }
    }

    for seed in 0..8 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let root = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let mut models = BTreeMap::from([(root, vec![0u8; 16])]);
        let mut deleted = BTreeSet::new();

        for step in 0..36 {
            let paused_gc = harness.rng.next_u64().is_multiple_of(3);
            let policy = if harness.rng.next_u64().is_multiple_of(2) {
                RetentionPolicy::retain_deleted_devices()
            } else {
                RetentionPolicy::expire_deleted_immediately()
            };
            let paused_mark = if paused_gc {
                let mark = store.mark_reachable_for_gc(policy.clone()).unwrap();
                harness.trace.record(format!(
                    "mark step={step} epoch={} retain_deleted={}",
                    mark.epoch, policy.retain_deleted_devices
                ));
                Some(mark)
            } else {
                None
            };

            if models.is_empty() {
                let device_id = client
                    .create_device(CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: Some(format!("recreated-{seed}-{step}")),
                    })
                    .unwrap();
                harness
                    .trace
                    .record(format!("create step={step} device={device_id}"));
                models.insert(device_id, vec![0u8; 16]);
            }

            let device_ids: Vec<_> = models.keys().copied().collect();
            let device_id = device_ids[harness.rng.choose_index(device_ids.len()).unwrap()];
            match harness.rng.next_u64() % 4 {
                0 => {
                    let block = harness.rng.next_u64() % 16;
                    let byte = (1 + harness.rng.next_u64() % 254) as u8;
                    harness.trace.record(format!(
                        "write step={step} device={device_id} block={block} byte={byte}"
                    ));
                    client
                        .open_device(device_id)
                        .unwrap()
                        .write_at(block * 4096, &[byte; 4096])
                        .unwrap();
                    models.get_mut(&device_id).unwrap()[block as usize] = byte;
                }
                1 if models.len() < 6 => {
                    let child = client
                        .open_device(device_id)
                        .unwrap()
                        .fork(ForkRequest {
                            target: None,
                            name: Some(format!("gc-child-{seed}-{step}")),
                        })
                        .unwrap();
                    harness
                        .trace
                        .record(format!("fork step={step} source={device_id} child={child}"));
                    models.insert(child, models.get(&device_id).unwrap().clone());
                }
                2 => {
                    harness
                        .trace
                        .record(format!("checkpoint step={step} device={device_id}"));
                    store.metadata().checkpoint(device_id).unwrap();
                }
                _ => {
                    harness
                        .trace
                        .record(format!("delete step={step} device={device_id}"));
                    client.open_device(device_id).unwrap().delete().unwrap();
                    models.remove(&device_id);
                    deleted.insert(device_id);
                }
            }

            if let Some(mark) = paused_mark {
                let sweep = store.sweep_metadata_after_mark(policy, mark.epoch).unwrap();
                harness.trace.record(format!(
                    "sweep step={step} epoch={} deleted_nodes={} released_segments={}",
                    sweep.epoch,
                    sweep.deleted_metadata_nodes.len(),
                    sweep.released_segments.len()
                ));
            } else if harness.rng.next_u64().is_multiple_of(2) {
                let report = store.run_metadata_custodian(policy).unwrap();
                harness.trace.record(format!(
                    "gc step={step} epoch={} deleted_nodes={} released_segments={}",
                    report.mark.epoch,
                    report.sweep.deleted_metadata_nodes.len(),
                    report.sweep.released_segments.len()
                ));
            }
            store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
            assert_live_models(&store, &client, &models, seed, harness.trace.events());
            for device_id in &deleted {
                assert!(store.metadata().get_head(*device_id).is_err());
            }
        }
    }
}

#[test]
fn generated_end_to_end_simulator_is_replayable_across_operations_and_faults() {
    fn graph_summary(
        store: &LocalCoordinator,
        native_file_count: usize,
    ) -> crate::sim::ObjectGraphSummary {
        let entries = store.segment_catalog().entries().unwrap();
        crate::sim::ObjectGraphSummary {
            live_devices: store.metadata().list_live_devices().unwrap().len(),
            deleted_devices: store.metadata().list_deleted_devices().unwrap().len(),
            native_files: native_file_count,
            metadata_nodes: store.metadata().metadata_node_count().unwrap(),
            gc_roots: store
                .metadata()
                .roots_for_gc(RetentionPolicy::retain_deleted_devices())
                .unwrap()
                .len(),
            referenced_segments: entries
                .iter()
                .filter(|(_, state, _)| *state == SegmentLifecycleState::Referenced)
                .count(),
            released_segments: entries
                .iter()
                .filter(|(_, state, _)| *state == SegmentLifecycleState::Released)
                .count(),
            freed_segments: entries
                .iter()
                .filter(|(_, state, _)| *state == SegmentLifecycleState::Freed)
                .count(),
        }
    }

    fn validate_live_devices(
        store: &LocalCoordinator,
        client: &LocalBlockClient,
        seed: u64,
        trace: &[String],
        models: &BTreeMap<DeviceId, Vec<u8>>,
    ) {
        for (device_id, model) in models {
            let device = client.open_device(*device_id).unwrap();
            let mut actual = vec![0; model.len() * 4096];
            device.read_at(0, &mut actual).unwrap();
            assert_model_blocks(
                &actual,
                model,
                seed,
                trace,
                &render_device_roots(store, *device_id),
            );
        }
    }

    fn run(seed: u64) -> crate::sim::FailureArtifact {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let faults = crate::sim::FaultInjector::new(seed ^ 0x0051_ab1e);
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let block_server = Arc::new(LocalBlockServer::new(store.clone()));
        let block_client = LocalBlockClient::new(InProcessBlockTransport::new(block_server));
        let native_client = create_native_client(&store);
        let native_keyspace = create_local_keyspace(&native_client);
        let mut device_models: BTreeMap<DeviceId, Vec<u8>> = BTreeMap::new();
        let mut deleted_devices = BTreeSet::new();
        let mut checkpoints: Vec<(DeviceId, CheckpointId, Vec<u8>)> = Vec::new();
        let mut file_models: BTreeMap<FileId, Vec<u8>> = BTreeMap::new();
        let mut expired_intents = BTreeSet::new();

        for step in 0..48 {
            let fault_kind = match step % 8 {
                0 => crate::sim::FaultKind::PublishConflict,
                1 => crate::sim::FaultKind::DuplicateEffect,
                2 => crate::sim::FaultKind::DelayedEffect,
                3 => crate::sim::FaultKind::MissingObject,
                4 => crate::sim::FaultKind::WriteIntentExpiry,
                5 => crate::sim::FaultKind::OrphanSegment,
                6 => crate::sim::FaultKind::MissedAsyncFree,
                _ => crate::sim::FaultKind::CrashReplayBoundary,
            };
            if step < 8 || faults.should_inject(step, fault_kind) {
                match fault_kind {
                    crate::sim::FaultKind::PublishConflict => {
                        let file_id = if let Some(file_id) = file_models.keys().next().copied() {
                            file_id
                        } else {
                            let file_id = native_client
                                .create_file(
                                    native_keyspace,
                                    CreateFileRequest {
                                        spec: FileSpec { name: None },
                                    },
                                )
                                .unwrap();
                            file_models.insert(file_id, Vec::new());
                            file_id
                        };
                        let file = native_client.open_file(native_keyspace, file_id).unwrap();
                        let stale = file.open_append_stream().unwrap();
                        let fresh = file.open_append_stream().unwrap();
                        assert!(
                            append_native_file_with_stream(&file, &stale, &repeated_blocks(1, 1))
                                .is_err()
                        );
                        append_native_file_with_stream(&file, &fresh, &repeated_blocks(1, 2))
                            .unwrap();
                        file_models.get_mut(&file_id).unwrap().push(2);
                        harness
                            .trace
                            .record(format!("fault publish_conflict step={step}"));
                    }
                    crate::sim::FaultKind::DuplicateEffect => {
                        let reservation = SegmentReservation {
                            segment_id: SegmentId::from_raw(90_000 + u128::from(step)),
                            bytes: 4096,
                        };
                        let first = store
                            .segment_store()
                            .write_segment(&reservation, &[8; 4096])
                            .unwrap();
                        let second = store
                            .segment_store()
                            .write_segment(&reservation, &[8; 4096])
                            .unwrap();
                        assert_eq!(first, second);
                        harness
                            .trace
                            .record(format!("fault duplicate_effect step={step}"));
                    }
                    crate::sim::FaultKind::DelayedEffect => {
                        let policy = if harness.rng.next_u64().is_multiple_of(2) {
                            RetentionPolicy::retain_deleted_devices()
                        } else {
                            RetentionPolicy::expire_deleted_immediately()
                        };
                        let mark = store.mark_reachable_for_gc(policy.clone()).unwrap();
                        harness.trace.record(format!(
                            "fault delayed_mark step={step} epoch={}",
                            mark.epoch
                        ));
                        store.sweep_metadata_after_mark(policy, mark.epoch).unwrap();
                    }
                    crate::sim::FaultKind::MissingObject => {
                        assert!(
                            store
                                .metadata()
                                .get_metadata_node(MetadataNodeId::from_raw(999_999))
                                .is_err()
                        );
                        harness
                            .trace
                            .record(format!("fault missing_object step={step}"));
                    }
                    crate::sim::FaultKind::WriteIntentExpiry => {
                        store.run_storage_node_custodian(&expired_intents).unwrap();
                        harness
                            .trace
                            .record(format!("fault write_intent_expiry step={step}"));
                    }
                    crate::sim::FaultKind::OrphanSegment => {
                        let owner = device_models
                            .keys()
                            .next()
                            .copied()
                            .map(MappingOwner::BlockDevice)
                            .unwrap_or_else(|| MappingOwner::BlockDevice(DeviceId::from_raw(1)));
                        let reservation = store.write_segment_for_owner(owner, &[6; 4096]).unwrap();
                        let intent = store
                            .segment_catalog()
                            .intent_for_segment(reservation.segment_id)
                            .unwrap()
                            .write_intent;
                        expired_intents.insert(intent);
                        harness.trace.record(format!(
                            "fault orphan_segment step={step} segment={}",
                            reservation.segment_id
                        ));
                    }
                    crate::sim::FaultKind::MissedAsyncFree => {
                        store
                            .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
                            .unwrap();
                        store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
                        harness
                            .trace
                            .record(format!("fault missed_async_free step={step}"));
                    }
                    crate::sim::FaultKind::CrashReplayBoundary => {
                        validate_live_devices(
                            &store,
                            &block_client,
                            seed,
                            harness.trace.events(),
                            &device_models,
                        );
                        harness
                            .trace
                            .record(format!("fault crash_replay_boundary step={step}"));
                    }
                }
            }

            match harness.rng.next_u64() % 8 {
                0 | 1 if device_models.is_empty() => {
                    let device_id = block_client
                        .create_device(CreateDeviceRequest {
                            spec: DeviceSpec {
                                logical_blocks: 16,
                                block_size: 4096,
                            },
                            name: Some(format!("sim-{seed}-{step}")),
                        })
                        .unwrap();
                    device_models.insert(device_id, vec![0; 16]);
                    harness
                        .trace
                        .record(format!("create step={step} device={device_id}"));
                }
                0 => {
                    let device_id = *device_models.keys().next().unwrap();
                    let block = harness.rng.next_u64() % 16;
                    let byte = (1 + harness.rng.next_u64() % 254) as u8;
                    block_client
                        .open_device(device_id)
                        .unwrap()
                        .write_at(block * 4096, &[byte; 4096])
                        .unwrap();
                    device_models.get_mut(&device_id).unwrap()[block as usize] = byte;
                    harness.trace.record(format!(
                        "write step={step} device={device_id} block={block} byte={byte}"
                    ));
                }
                1 if device_models.len() < 6 => {
                    let source = *device_models.keys().next().unwrap();
                    let child = block_client
                        .open_device(source)
                        .unwrap()
                        .fork(ForkRequest {
                            target: None,
                            name: Some(format!("sim-child-{seed}-{step}")),
                        })
                        .unwrap();
                    device_models.insert(child, device_models.get(&source).unwrap().clone());
                    harness
                        .trace
                        .record(format!("fork step={step} source={source} child={child}"));
                }
                2 if !device_models.is_empty() => {
                    let device_id = *device_models.keys().next().unwrap();
                    let checkpoint = store.metadata().checkpoint(device_id).unwrap();
                    checkpoints.push((
                        device_id,
                        checkpoint,
                        device_models.get(&device_id).unwrap().clone(),
                    ));
                    harness
                        .trace
                        .record(format!("checkpoint step={step} device={device_id}"));
                }
                3 if !checkpoints.is_empty() => {
                    let index = harness.rng.choose_index(checkpoints.len()).unwrap();
                    let (source, checkpoint, model) = checkpoints[index].clone();
                    if let Ok(restored) =
                        store.restore_device(source, RestorePoint::Checkpoint(checkpoint))
                    {
                        device_models.insert(restored, model);
                        harness.trace.record(format!(
                            "restore step={step} source={source} restored={restored}"
                        ));
                    } else {
                        harness
                            .trace
                            .record(format!("restore_expired step={step} source={source}"));
                    }
                }
                4 if !device_models.is_empty() => {
                    let device_id = *device_models.keys().next().unwrap();
                    block_client
                        .open_device(device_id)
                        .unwrap()
                        .delete()
                        .unwrap();
                    device_models.remove(&device_id);
                    deleted_devices.insert(device_id);
                    harness
                        .trace
                        .record(format!("delete step={step} device={device_id}"));
                }
                5 => {
                    let file_id = native_client
                        .create_file(
                            native_keyspace,
                            CreateFileRequest {
                                spec: FileSpec { name: None },
                            },
                        )
                        .unwrap();
                    file_models.insert(file_id, Vec::new());
                    harness
                        .trace
                        .record(format!("create_file step={step} file={file_id}"));
                }
                6 if !file_models.is_empty() => {
                    let file_id = *file_models.keys().next().unwrap();
                    let file = native_client.open_file(native_keyspace, file_id).unwrap();
                    let byte = (1 + harness.rng.next_u64() % 254) as u8;
                    append_native_file_once(&file, &[byte; 4096]).unwrap();
                    file_models.get_mut(&file_id).unwrap().push(byte);
                    harness
                        .trace
                        .record(format!("append step={step} file={file_id} byte={byte}"));
                }
                _ => {
                    let policy = if harness.rng.next_u64().is_multiple_of(2) {
                        RetentionPolicy::retain_deleted_devices()
                    } else {
                        RetentionPolicy::expire_deleted_immediately()
                    };
                    store.run_metadata_custodian(policy).unwrap();
                    store.run_storage_node_custodian(&expired_intents).unwrap();
                    harness.trace.record(format!("gc step={step}"));
                }
            }

            validate_live_devices(
                &store,
                &block_client,
                seed,
                harness.trace.events(),
                &device_models,
            );
            for (file_id, model) in &file_models {
                let file = native_client.open_file(native_keyspace, *file_id).unwrap();
                let mut actual = vec![0; model.len() * 4096];
                file.read_at(0, &mut actual).unwrap();
                assert_model_blocks(&actual, model, seed, harness.trace.events(), "native file");
            }
            for device_id in &deleted_devices {
                assert!(store.metadata().get_head(*device_id).is_err());
            }
        }

        crate::sim::FailureArtifact::new(
            seed,
            harness.trace.events(),
            graph_summary(&store, file_models.len()),
        )
    }

    for seed in 0..10 {
        assert_eq!(run(seed), run(seed));
    }
}

#[test]
fn storage_node_custodian_reclaims_expired_failed_orphan_and_released_segments() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let reserved = store
        .segment_catalog()
        .reserve_segment_with_id(
            SegmentId::from_raw(101),
            SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(10),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            },
        )
        .unwrap();
    let writing = store
        .segment_catalog()
        .reserve_segment_with_id(
            SegmentId::from_raw(102),
            SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(11),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            },
        )
        .unwrap();
    store.segment_catalog().begin_write(&writing).unwrap();
    let orphan = store
        .segment_catalog()
        .reserve_segment_with_id(
            SegmentId::from_raw(103),
            SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(12),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            },
        )
        .unwrap();
    store.segment_catalog().begin_write(&orphan).unwrap();
    let orphan_commit = store
        .segment_store()
        .write_segment(&orphan, &[3; 4096])
        .unwrap();
    store
        .segment_store()
        .sync_segment(orphan.segment_id)
        .unwrap();
    let orphan_receipt = receipt_for_commit(
        SegmentReservationIntent {
            write_intent: WriteIntentId::from_raw(12),
            owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
            bytes: 4096,
        },
        orphan_commit,
    );
    store
        .segment_catalog()
        .commit_segment(orphan.clone(), orphan_receipt)
        .unwrap();
    let referenced = store
        .write_segment_for_owner(MappingOwner::BlockDevice(DeviceId::from_raw(1)), &[4; 4096])
        .unwrap();
    store
        .segment_catalog()
        .mark_segment_referenced(referenced.segment_id)
        .unwrap();
    store
        .segment_catalog()
        .release_segment(referenced.segment_id)
        .unwrap();

    let untouched = store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert!(untouched.expired_reservations.is_empty());
    assert!(untouched.failed_writes.is_empty());
    assert!(untouched.orphan_segments.is_empty());
    assert_eq!(
        untouched.deleted_released_segments,
        vec![referenced.segment_id]
    );
    assert_eq!(
        store.segment_catalog().state(orphan.segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );

    let expired = BTreeSet::from([
        WriteIntentId::from_raw(10),
        WriteIntentId::from_raw(11),
        WriteIntentId::from_raw(12),
    ]);
    let report = store.run_storage_node_custodian(&expired).unwrap();
    assert_eq!(report.expired_reservations, vec![reserved.segment_id]);
    assert_eq!(report.failed_writes, vec![writing.segment_id]);
    assert_eq!(report.orphan_segments, vec![orphan.segment_id]);
    assert_eq!(
        store.segment_catalog().state(reserved.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );
    assert_eq!(
        store.segment_catalog().state(writing.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );
    assert_eq!(
        store.segment_catalog().state(orphan.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );
    assert!(
        !store
            .segment_store()
            .contains_segment(orphan.segment_id)
            .unwrap()
    );
}

#[test]
fn segment_store_is_immutable_idempotent_and_reports_missing_objects() {
    let store = InMemorySegmentStore::new(config()).unwrap();
    let reservation = SegmentReservation {
        segment_id: SegmentId::from_raw(7),
        bytes: 4096,
    };
    let bytes = vec![11; 4096];
    let commit = store.write_segment(&reservation, &bytes).unwrap();
    assert_eq!(commit.descriptor.segment_id, reservation.segment_id);
    assert!(!store.is_synced(reservation.segment_id).unwrap());

    assert_eq!(store.write_segment(&reservation, &bytes).unwrap(), commit);
    assert!(store.write_segment(&reservation, &[12; 4096]).is_err());
    assert!(
        store
            .read_segment(reservation.segment_id, ByteRange::new(0, 1), &mut [0])
            .is_err()
    );

    store.sync_segment(reservation.segment_id).unwrap();
    assert!(store.is_synced(reservation.segment_id).unwrap());

    let mut out = vec![0; 16];
    store
        .read_segment(reservation.segment_id, ByteRange::new(8, 16), &mut out)
        .unwrap();
    assert_eq!(out, vec![11; 16]);
    assert!(
        store
            .read_segment(SegmentId::from_raw(404), ByteRange::new(0, 1), &mut [0])
            .is_err()
    );
}

#[test]
fn provider_conformance_harness_runs_against_memory_and_durable_stores() {
    let memory = LocalCoordinator::with_config(tree_config()).unwrap();
    run_provider_conformance(&memory);

    let root = durable_temp_dir("provider-conformance");
    let cfg = tree_config();
    let durable = DurableCoordinator::open(&root, cfg).unwrap();
    let durable_outcome = run_provider_conformance(&durable);
    drop(durable);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut device_bytes = vec![0; 3 * 4096];
    reopened
        .read_device(
            durable_outcome.device_id,
            ByteRange::new(0, 3 * 4096),
            &mut device_bytes,
        )
        .unwrap();
    assert_eq!(&device_bytes[0..4096], vec![3; 4096].as_slice());
    assert_eq!(&device_bytes[4096..12288], repeated_blocks(2, 7).as_slice());

    let mut source = vec![0; b"alpha-beta".len()];
    reopened
        .read_file(
            durable_outcome.keyspace_id,
            durable_outcome.file_id,
            ByteRange::new(0, b"alpha-beta".len() as u64),
            &mut source,
        )
        .unwrap();
    assert_eq!(source, b"alpha-beta");

    let mut snapshot = vec![0; b"alpha".len()];
    reopened
        .read_file(
            durable_outcome.snapshot_keyspace,
            durable_outcome.file_id,
            ByteRange::new(0, b"alpha".len() as u64),
            &mut snapshot,
        )
        .unwrap();
    assert_eq!(snapshot, b"alpha");
    let _ = fs::remove_dir_all(root);
}

fn assert_durable_row_round_trip<T>(value: &T)
where
    T: DurableCodec + Clone + PartialEq,
{
    let bytes = encode_row(value).unwrap();
    let decoded = decode_row::<T>(&bytes).unwrap();
    assert!(decoded == value.clone());
}

#[test]
fn durable_row_payload_codecs_round_trip_real_block_and_native_rows() {
    let store = LocalCoordinator::with_config(tree_config()).unwrap();
    let device = create_local_device(&store, 32);
    device.write_at(7 * 4096, &repeated_blocks(3, 8)).unwrap();
    store.metadata().checkpoint(device.device_id()).unwrap();
    let forked = device
        .fork(ForkRequest {
            target: None,
            name: Some("codec-fork".to_string()),
        })
        .unwrap();
    store
        .write_device(
            forked,
            8 * 4096,
            &repeated_blocks(1, 9),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let native = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&native);
    let (_file_id, file) = create_local_file(&native, keyspace_id);
    file.write_at(0, b"codec").unwrap();
    append_native_file_once(&file, b"-state").unwrap();
    native.checkpoint_keyspace(keyspace_id).unwrap();

    let metadata = store.metadata.state_inner().unwrap();
    assert_durable_row_round_trip(metadata.device_specs.get(&device.device_id()).unwrap());
    let device_head = metadata.device_heads.get(&device.device_id()).unwrap();
    assert_durable_row_round_trip(&DurableDeviceManifest::from_head(device_head).unwrap());
    assert_durable_row_round_trip(
        &DurableDeviceShardHead::from_head(
            device_head,
            0,
            device_head.shard_roots[0],
            device_head.latest_commit,
        )
        .unwrap(),
    );
    let keyspace_head = metadata.keyspace_heads.get(&keyspace_id).unwrap();
    assert_durable_row_round_trip(&DurableKeyspaceManifest::from_head(keyspace_head).unwrap());
    assert_durable_row_round_trip(
        &DurableKeyspaceShardHead::from_head(
            keyspace_head,
            0,
            keyspace_head.shard_roots[0],
            keyspace_head.latest_commit,
        )
        .unwrap(),
    );
    assert_durable_row_round_trip(metadata.keyspace_roots.values().next().unwrap());
    assert_durable_row_round_trip(metadata.keyspace_catalog_shards.values().next().unwrap());
    assert_durable_row_round_trip(metadata.file_writer_epochs.values().next().unwrap());
    assert_durable_row_round_trip(metadata.metadata_nodes.values().next().unwrap());
    assert_durable_row_round_trip(metadata.commit_groups.values().next().unwrap());
    assert_durable_row_round_trip(metadata.shard_commits.last().unwrap());
    assert_durable_row_round_trip(metadata.keyspace_commits.last().unwrap());
    assert_durable_row_round_trip(metadata.file_commits.last().unwrap());
    assert_durable_row_round_trip(metadata.checkpoints.values().next().unwrap());

    let catalog = store.segment_catalog().state_inner().unwrap();
    assert_durable_row_round_trip(catalog.entries.values().next().unwrap());

    let run = AppendLogRun {
        run_id: AppendRunId::from_raw(1),
        storage_node: StorageNodeId::from_raw(1),
        stream_id: AppendStreamId::from_raw(2),
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id,
        file_id: _file_id,
        file_offset_start: 0,
        payload_len: 4096,
        log_id: 4,
        log_payload_offset: 512,
        log_record_bytes: 4096 + 64,
        integrity: SegmentPayloadIntegrity::Unchecked,
    };
    let run_range = run.full_range();
    assert_durable_row_round_trip(&run);
    assert_durable_row_round_trip(&run_range);
    assert_durable_row_round_trip(&RunBackedFileExtent {
        file_offset_start: run_range.file_offset_start,
        payload_len: run_range.payload_len,
        run: run_range.clone(),
    });
    assert_durable_row_round_trip(&AppendVisiblePublish {
        record_id: AppendPublishTicketId::from_raw(9),
        commit_seq: CommitSeq::from_raw(10),
        keyspace_id,
        file_id: _file_id,
        base_writer_epoch: WriterEpoch::from_raw(2),
        writer_epoch: WriterEpoch::from_raw(3),
        base_file_version: FileVersion::from_raw(1),
        new_file_version: FileVersion::from_raw(2),
        old_size: 0,
        new_size: run_range.payload_len,
        publish_through: run_range.payload_len,
        run_extents: vec![RunBackedFileExtent {
            file_offset_start: run_range.file_offset_start,
            payload_len: run_range.payload_len,
            run: run_range,
        }],
    });
}

#[test]
fn durable_row_payload_codec_rejects_malformed_inputs() {
    let store = LocalCoordinator::with_config(tree_config()).unwrap();
    let device = create_local_device(&store, 32);
    device.write_at(7 * 4096, &repeated_blocks(1, 8)).unwrap();
    let catalog = store.segment_catalog().state_inner().unwrap();
    let entry = catalog.entries.values().next().unwrap();
    let bytes = encode_row(entry).unwrap();

    let mut truncated = bytes.clone();
    truncated.pop();
    assert!(decode_row::<CatalogEntry>(&truncated).is_err());

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_row::<CatalogEntry>(&trailing).is_err());

    let mut invalid_tag = DurableEncoder { bytes: Vec::new() };
    99u8.encode(&mut invalid_tag).unwrap();
    let mut invalid_tag = DurableDecoder {
        bytes: &invalid_tag.bytes,
        offset: 0,
    };
    assert!(SegmentLifecycleState::decode(&mut invalid_tag).is_err());

    let mut oversized_vector = DurableEncoder { bytes: Vec::new() };
    (MAX_DURABLE_COLLECTION_LEN + 1)
        .encode(&mut oversized_vector)
        .unwrap();
    let mut oversized_vector = DurableDecoder {
        bytes: &oversized_vector.bytes,
        offset: 0,
    };
    assert!(Vec::<u8>::decode(&mut oversized_vector).is_err());

    let mut oversized_string = DurableEncoder { bytes: Vec::new() };
    (MAX_DURABLE_STRING_LEN + 1)
        .encode(&mut oversized_string)
        .unwrap();
    let mut oversized_string = DurableDecoder {
        bytes: &oversized_string.bytes,
        offset: 0,
    };
    assert!(String::decode(&mut oversized_string).is_err());

    let mut offset_overflow = DurableDecoder {
        bytes: &[0],
        offset: usize::MAX,
    };
    assert!(offset_overflow.take(1).is_err());
}

#[test]
fn durable_provider_reopens_committed_block_contents_and_restore_points() {
    let root = durable_temp_dir("block-restart");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: Some("durable".to_string()),
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(2, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    let checkpoint = store.checkpoint(device_id).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(2, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert!(root.join("metadata.sqlite").exists());
    assert!(data_log_path(&root.join("data"), cfg.storage_node, 1).exists());

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut current = vec![0; 2 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 2 * 4096), &mut current)
        .unwrap();
    assert_eq!(current, repeated_blocks(2, 4));

    let restored = reopened
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    let mut restored_bytes = vec![0; 2 * 4096];
    reopened
        .read_device(restored, ByteRange::new(0, 2 * 4096), &mut restored_bytes)
        .unwrap();
    assert_eq!(restored_bytes, repeated_blocks(2, 3));

    drop(reopened);
    let reopened_again = DurableCoordinator::open(&root, cfg).unwrap();
    let mut restored_after_restart = vec![0; 2 * 4096];
    reopened_again
        .read_device(
            restored,
            ByteRange::new(0, 2 * 4096),
            &mut restored_after_restart,
        )
        .unwrap();
    assert_eq!(restored_after_restart, repeated_blocks(2, 3));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_batch_commit_survives_reopen_and_pitr() {
    let root = durable_temp_dir("block-batch-restart");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: Some("durable-batch".to_string()),
        })
        .unwrap();
    store
        .commit_block_batch(
            device_id,
            &[
                BlockBatchWrite {
                    offset: 0,
                    bytes: repeated_blocks(1, 21),
                    payload_integrity: PayloadIntegrity::Verified,
                },
                BlockBatchWrite {
                    offset: 8 * 4096,
                    bytes: repeated_blocks(1, 22),
                    payload_integrity: PayloadIntegrity::Verified,
                },
            ],
            WriteDurability::Flushed,
        )
        .unwrap();
    let checkpoint = store.checkpoint(device_id).unwrap();
    store
        .commit_block_batch(
            device_id,
            &[BlockBatchWrite {
                offset: 0,
                bytes: repeated_blocks(1, 23),
                payload_integrity: PayloadIntegrity::Verified,
            }],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut current = vec![0; 9 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 9 * 4096), &mut current)
        .unwrap();
    assert_eq!(&current[0..4096], repeated_blocks(1, 23).as_slice());
    assert_eq!(
        &current[8 * 4096..9 * 4096],
        repeated_blocks(1, 22).as_slice()
    );

    let restored = reopened
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    let mut restored_bytes = vec![0; 9 * 4096];
    reopened
        .read_device(restored, ByteRange::new(0, 9 * 4096), &mut restored_bytes)
        .unwrap();
    assert_eq!(&restored_bytes[0..4096], repeated_blocks(1, 21).as_slice());
    assert_eq!(
        &restored_bytes[8 * 4096..9 * 4096],
        repeated_blocks(1, 22).as_slice()
    );
    drop(reopened);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_multi_node_reopens_block_and_native_placements() {
    let root = durable_temp_dir("multi-node-restart");
    let cfg = config();
    let node_ids = vec![
        cfg.storage_node,
        StorageNodeId::from_raw(78),
        StorageNodeId::from_raw(79),
    ];
    let store = DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
        &root,
        cfg,
        node_ids.clone(),
        DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    for block in 0..3 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Flushed,
            )
            .unwrap();
    }
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    for byte in [4, 5, 6] {
        append_durable_store_once(
            &store,
            keyspace_id,
            file_id,
            &repeated_blocks(1, byte),
            WriteDurability::Flushed,
        )
        .unwrap();
    }
    assert_eq!(store.storage_node_ids_for_test(), node_ids);
    assert_eq!(
        segment_storage_nodes(
            &store.local,
            &device_segment_ids(&store.metadata(), device_id)
        )
        .len(),
        3
    );
    assert_eq!(
        segment_storage_nodes(
            &store.local,
            &file_segment_ids(&store.metadata(), keyspace_id, file_id),
        )
        .len(),
        0
    );
    assert_eq!(
        run_storage_nodes(&file_run_extents(&store.metadata(), keyspace_id, file_id)).len(),
        3
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert_eq!(reopened.storage_node_ids_for_test(), node_ids);
    let mut device_bytes = vec![0; 3 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 3 * 4096), &mut device_bytes)
        .unwrap();
    assert_eq!(&device_bytes[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&device_bytes[4096..8192], repeated_blocks(1, 2).as_slice());
    assert_eq!(&device_bytes[8192..12288], repeated_blocks(1, 3).as_slice());
    let mut file_bytes = vec![0; 3 * 4096];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, 3 * 4096),
            &mut file_bytes,
        )
        .unwrap();
    assert_eq!(&file_bytes[0..4096], repeated_blocks(1, 4).as_slice());
    assert_eq!(&file_bytes[4096..8192], repeated_blocks(1, 5).as_slice());
    assert_eq!(&file_bytes[8192..12288], repeated_blocks(1, 6).as_slice());
    assert_eq!(
        segment_storage_nodes(
            &reopened.local,
            &device_segment_ids(&reopened.metadata(), device_id),
        )
        .len(),
        3
    );
    assert_eq!(
        segment_storage_nodes(
            &reopened.local,
            &file_segment_ids(&reopened.metadata(), keyspace_id, file_id),
        )
        .len(),
        0
    );
    assert_eq!(
        run_storage_nodes(&file_run_extents(
            &reopened.metadata(),
            keyspace_id,
            file_id
        ))
        .len(),
        3
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_acknowledged_write_requires_flush_for_restart_visibility() {
    let root = durable_temp_dir("ack-flush-restart");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 6),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    drop(store);
    let reopened_before_flush = DurableCoordinator::open(&root, cfg).unwrap();
    let mut before_flush = vec![99; 4096];
    reopened_before_flush
        .read_device(device_id, ByteRange::new(0, 4096), &mut before_flush)
        .unwrap();
    assert_eq!(before_flush, vec![0; 4096]);

    reopened_before_flush
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 7),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let flush = reopened_before_flush.flush_device(device_id).unwrap();
    assert!(flush.durable_through.raw() > 0);

    drop(reopened_before_flush);
    let reopened_after_flush = DurableCoordinator::open(&root, cfg).unwrap();
    let mut after_flush = vec![0; 4096];
    reopened_after_flush
        .read_device(device_id, ByteRange::new(0, 4096), &mut after_flush)
        .unwrap();
    assert_eq!(after_flush, repeated_blocks(1, 7));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_persist_until_does_not_export_later_block_commits() {
    let root = durable_temp_dir("persist-target-block");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    let first = store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 11),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let second = store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 12),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    store.persist_until(first.commit_seq).unwrap();
    let profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(
        profiles[0].durable_commit_high_water,
        first.commit_seq.raw()
    );
    assert!(second.commit_seq.raw() > first.commit_seq.raw());

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 11));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_persist_until_does_not_export_later_native_commits() {
    let root = durable_temp_dir("persist-target-native");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    let first = store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, b"first".to_vec())],
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let second = store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, b"later".to_vec())],
            WriteDurability::Acknowledged,
        )
        .unwrap();

    store.persist_until(first.commit_seq).unwrap();
    let profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(
        profiles[0].durable_commit_high_water,
        first.commit_seq.raw()
    );
    assert_eq!(profiles[0].new_segment_count, 1);
    assert!(second.commit_seq.raw() > first.commit_seq.raw());

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; b"first".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"first".len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"first");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_target_persist_can_advance_to_later_native_commit_without_restart() {
    let root = durable_temp_dir("persist-target-native-advance");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    let first = store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, b"first".to_vec())],
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let second = store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, b"later".to_vec())],
            WriteDurability::Acknowledged,
        )
        .unwrap();

    store.persist_until(first.commit_seq).unwrap();
    store.persist_until(second.commit_seq).unwrap();
    let profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(profiles.len(), 2);
    assert_eq!(
        profiles[0].durable_commit_high_water,
        first.commit_seq.raw()
    );
    assert_eq!(
        profiles[1].durable_commit_high_water,
        second.commit_seq.raw()
    );
    assert_eq!(profiles[0].new_segment_count, 1);
    assert_eq!(profiles[1].new_segment_count, 1);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; b"later".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"later".len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"later");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_acknowledged_write_prestages_data_log_without_cataloging_before_flush() {
    let root = durable_temp_dir("ack-prestages-data-log-before-flush");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 21),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let data_log = data_log_path(&root.join("data"), cfg.storage_node, 1);
    assert!(data_log.exists());
    assert!(data_log.metadata().unwrap().len() > 4096);
    assert!(store.durable.data_log_rows_for_test().unwrap().is_empty());
    assert_eq!(block_delta_commit_count(&root), 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut hidden = vec![99; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut hidden)
        .unwrap();
    assert_eq!(hidden, vec![0; 4096]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_acknowledged_block_prestage_does_not_wait_for_persist_lock() {
    let root = durable_temp_dir("ack-block-prestage-bypasses-persist-lock");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();

    let persist_guard = lock(&store.persist_lock).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            let result = store.write_device(
                device_id,
                0,
                &repeated_blocks(1, 25),
                WriteDurability::Acknowledged,
            );
            tx.send(result.map(|_| ())).unwrap();
        })
    };

    rx.recv_timeout(Duration::from_secs(1))
        .expect("acknowledged block prestage should not wait for physical persist lock")
        .unwrap();
    drop(persist_guard);
    worker.join().unwrap();

    let data_log = data_log_path(&root.join("data"), cfg.storage_node, 1);
    assert!(data_log.exists());
    assert!(store.durable.data_log_rows_for_test().unwrap().is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_acknowledged_append_stream_ingests_private_data_without_cataloging_before_publish() {
    let root = durable_temp_dir("stream-ack-private-memory-before-publish");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 31),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    assert!(
        data_log_path(&root.join("data"), cfg.storage_node, 1).exists(),
        "acknowledged stream ingest should append raw bytes to a private storage-node log"
    );
    assert!(store.durable.data_log_rows_for_test().unwrap().is_empty());

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        reopened
            .publish_append_stream(&stream, stream.visible_base_size + 4096)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_persist_keeps_private_bytes_invisible_after_reopen() {
    let root = durable_temp_dir("stream-auto-persist-private-invisible");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 37);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();

    assert_eq!(
        store
            .local
            .metadata
            .append_stream_durable_high_water_if_reached(&stream, 4096)
            .unwrap(),
        Some(4096),
        "auto-persist should advance only the private durable high-water"
    );
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        reopened
            .publish_append_stream(&stream, payload.len() as u64)
            .is_err(),
        "auto-persisted private bytes are not public stream recovery"
    );
    assert!(
        !reopened
            .metadata()
            .state_inner()
            .unwrap()
            .append_streams
            .contains_key(&stream.stream_id)
    );
    assert_eq!(
        reopened
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );
    let fresh = reopened.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(fresh.visible_base_size, 0);
    let ticket = reopened
        .append_stream(&fresh, b"new", WriteDurability::Acknowledged)
        .unwrap();
    reopened
        .publish_append_stream(&fresh, ticket.range.end_exclusive().unwrap())
        .unwrap();
    let mut bytes = vec![0; b"new".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"new");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_persist_publish_does_not_reappend_payload() {
    let root = durable_temp_dir("stream-auto-persist-publish-no-reappend");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 39);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(
        store
            .local
            .metadata
            .append_stream_durable_high_water_if_reached(&stream, 4096)
            .unwrap(),
        Some(4096),
        "dirty-tail threshold should sync private payload before publish"
    );

    store.publish_append_stream(&stream, 4096).unwrap();
    let publish_profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(publish_profiles.len(), 1);
    assert_eq!(publish_profiles[0].new_segment_count, 0);
    assert_eq!(publish_profiles[0].new_segment_bytes, 0);
    assert_eq!(publish_profiles[0].data_log_append_sync_nanos, 0);
    assert_eq!(publish_profiles[0].data_log_encode_nanos, 0);
    assert_eq!(publish_profiles[0].data_log_write_nanos, 0);
    assert_eq!(publish_profiles[0].data_log_file_sync_nanos, 0);
    assert_eq!(publish_profiles[0].data_log_dir_sync_nanos, 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_persist_syncs_payload_at_threshold_before_publish() {
    let root = durable_temp_dir("stream-auto-persist-syncs-at-threshold");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 93);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(
        store
            .local
            .metadata
            .append_stream_durable_high_water_if_reached(&stream, 4096)
            .unwrap(),
        Some(4096),
        "dirty-tail threshold should make append payload durable before publish"
    );

    store.enable_persist_profiling(8).unwrap();
    store.publish_append_stream(&stream, 4096).unwrap();
    let publish_profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(publish_profiles.len(), 1);
    assert_eq!(
        publish_profiles[0].data_log_files_synced, 0,
        "publish should not sync payload already forced durable by dirty-tail threshold"
    );
    assert_eq!(publish_profiles[0].data_log_sync_bytes, 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_payload_sync_skips_already_synced_log_bytes() {
    let root = durable_temp_dir("append-payload-sync-skips-already-synced");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let payload = repeated_blocks(1, 84);
    let append_payload = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(77),
        storage_node: cfg.storage_node,
        stream_id: AppendStreamId::from_raw(88),
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 0,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![payload.as_slice()],
        background_sync_step_bytes: None,
    };
    let (_run, pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(append_payload, None)
        .unwrap();

    let (first, _) = store.durable.sync_pending_append_payload(&pending).unwrap();
    assert!(first.files_synced > 0);
    assert!(first.sync_bytes > 0);

    let (second, _) = store.durable.sync_pending_append_payload(&pending).unwrap();
    assert_eq!(
        second.files_synced, 0,
        "payload sync high-water should avoid re-syncing already durable append-log bytes"
    );
    assert_eq!(second.sync_bytes, 0);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_payload_chunks_start_background_sync_progress() {
    let root = durable_temp_dir("append-payload-chunks-background-sync");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let first = repeated_blocks(1, 86);
    let second = repeated_blocks(1, 87);
    let append_payload = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(78),
        storage_node: cfg.storage_node,
        stream_id: AppendStreamId::from_raw(89),
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 0,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![first.as_slice(), second.as_slice()],
        background_sync_step_bytes: Some(4096),
    };
    let (_run, pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(append_payload, None)
        .unwrap();
    let (log_ref, manifest) = pending.logs.iter().next().unwrap();
    assert!(
        store
            .durable
            .wait_for_synced_append_log_for_test(
                *log_ref,
                manifest.total_bytes,
                Duration::from_secs(2),
            )
            .unwrap(),
        "multi-chunk append writes should pipeline private append-log sync"
    );

    let (sync, _) = store.durable.sync_pending_append_payload(&pending).unwrap();
    assert_eq!(sync.files_synced, 0);
    assert_eq!(sync.sync_bytes, 0);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_skips_presynced_payload_without_private_persist() {
    let root = durable_temp_dir("append-publish-skips-presynced-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 85);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();

    let records = store
        .local
        .metadata
        .append_stream_publish_records(&stream, 0, payload.len() as u64)
        .unwrap();
    let log_refs = records
        .iter()
        .map(AppendStreamRunRecord::log_ref)
        .collect::<BTreeSet<_>>();
    let pending = store
        .durable
        .pending_append_run_manifests_for_log_refs(&log_refs, None)
        .unwrap();
    let (presync, _) = store.durable.sync_pending_append_payload(&pending).unwrap();
    assert!(presync.files_synced > 0);

    store.enable_persist_profiling(8).unwrap();
    store
        .publish_append_stream(&stream, payload.len() as u64)
        .unwrap();
    let publish_profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(publish_profiles.len(), 1);
    assert_eq!(
        publish_profiles[0].data_log_files_synced, 0,
        "publish should trust process-local pre-synced append payload high-water"
    );
    assert_eq!(publish_profiles[0].data_log_sync_bytes, 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_skips_background_synced_sealed_log() {
    let root = durable_temp_dir("append-publish-skips-background-synced-sealed-log");
    let cfg = config();
    let store = DurableCoordinator::open_with_data_log_policy(
        &root,
        cfg,
        DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            ..DurableDataLogPolicy::default()
        },
    )
    .unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload_a = repeated_blocks(1, 86);
    let payload_b = repeated_blocks(1, 87);
    store
        .append_stream(&stream, &payload_a, WriteDurability::Acknowledged)
        .unwrap();
    store
        .append_stream(&stream, &payload_b, WriteDurability::Acknowledged)
        .unwrap();

    let records = store
        .local
        .metadata
        .append_stream_publish_records(&stream, 0, payload_a.len() as u64)
        .unwrap();
    let first_log_ref = records[0].log_ref();
    let log_refs = BTreeSet::from([first_log_ref]);
    let pending = store
        .durable
        .pending_append_run_manifests_for_log_refs(&log_refs, None)
        .unwrap();
    let first_log_bytes = pending.logs[&first_log_ref].total_bytes;
    assert!(
        store
            .durable
            .wait_for_synced_append_log_for_test(
                first_log_ref,
                first_log_bytes,
                Duration::from_secs(2),
            )
            .unwrap(),
        "sealed append log should be pre-synced by the background worker"
    );

    store.enable_persist_profiling(8).unwrap();
    store
        .publish_append_stream(&stream, payload_a.len() as u64)
        .unwrap();
    let publish_profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(publish_profiles.len(), 1);
    assert_eq!(
        publish_profiles[0].data_log_files_synced, 0,
        "publish should skip payload already synced by the background worker"
    );
    assert_eq!(publish_profiles[0].data_log_sync_bytes, 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload_a.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload_a.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload_a);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_persist_does_not_block_append_ack() {
    let root = durable_temp_dir("stream-auto-persist-nonblocking-append");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store
        .set_persist_delay_for_test(Some(Duration::from_millis(250)))
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 41);
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = {
        let store = Arc::clone(&store);
        let stream = stream.clone();
        thread::spawn(move || {
            let result = store.append_stream(&stream, &payload, WriteDurability::Acknowledged);
            tx.send(result.map(|_| ())).unwrap();
        })
    };

    rx.recv_timeout(Duration::from_millis(100))
        .expect("append ack should not wait for background auto-persist delay")
        .unwrap();
    worker.join().unwrap();
    store.set_persist_delay_for_test(None).unwrap();
    store.persist_append_stream_prefix(&stream, 4096).unwrap();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_persist_dirty_tail_sync_bypasses_root_persist_delay() {
    let root = durable_temp_dir("stream-auto-persist-payload-sync-backpressure");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store
        .set_persist_delay_for_test(Some(Duration::from_millis(150)))
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    for byte in [44, 45, 46] {
        store
            .append_stream(
                &stream,
                &repeated_blocks(1, byte),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = {
        let store = Arc::clone(&store);
        let stream = stream.clone();
        thread::spawn(move || {
            let result = store.append_stream(
                &stream,
                &repeated_blocks(1, 47),
                WriteDurability::Acknowledged,
            );
            tx.send(result.map(|_| ())).unwrap();
        })
    };

    rx.recv_timeout(Duration::from_millis(100))
        .expect("dirty-tail payload sync should not wait for root persist delay")
        .unwrap();
    worker.join().unwrap();
    store.set_persist_delay_for_test(None).unwrap();
    assert_eq!(
        store
            .local
            .metadata
            .append_stream_durable_high_water_if_reached(&stream, 16 * 1024)
            .unwrap(),
        Some(16 * 1024),
        "dirty-tail payload sync should advance the in-memory durable high-water"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_stream_auto_payload_sync_failure_does_not_poison_publish_retry() {
    let root = durable_temp_dir("stream-auto-persist-payload-sync-failure-retry");
    let cfg = LocalStoreConfig {
        stream_auto_persist_bytes: Some(4096),
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    for byte in [43, 44, 45] {
        store
            .append_stream(
                &stream,
                &repeated_blocks(1, byte),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }
    store.fail_next_append_payload_sync_for_test();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 46),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    assert!(
        !store
            .durable
            .fail_next_append_payload_sync
            .load(Ordering::SeqCst),
        "dirty-tail sync should consume the injected failure without rejecting append"
    );
    assert_eq!(
        store
            .local
            .metadata
            .append_stream_durable_high_water_if_reached(&stream, 16 * 1024)
            .unwrap(),
        None,
        "failed dirty-tail sync must not mark the private prefix durable"
    );

    store.publish_append_stream(&stream, 16 * 1024).unwrap();
    let expected = [
        repeated_blocks(1, 43),
        repeated_blocks(1, 44),
        repeated_blocks(1, 45),
        repeated_blocks(1, 46),
    ]
    .concat();
    let mut bytes = vec![0; expected.len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, 16 * 1024),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, expected);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_metadata_failure_keeps_head_invisible_and_ticket_retryable() {
    let root = durable_temp_dir("stream-publish-metadata-failure-retry");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 82);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    let ticket = store.submit_append_publish(&stream, 4096).unwrap();

    store.fail_next_persist_for_test();
    let failed = store.wait_append_publish(&ticket);
    assert!(matches!(failed, Err(StorageError::Unavailable { .. })));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );
    assert!(
        store
            .local
            .metadata
            .state_inner()
            .unwrap()
            .append_publish_in_flight
            .is_empty(),
        "failed durable publish should clear the transient in-flight marker"
    );

    let commit = store.wait_append_publish(&ticket).unwrap();
    assert_eq!(commit.range, ByteRange::new(0, 4096));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        4096
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_payload_sync_failure_keeps_head_invisible_and_ticket_retryable() {
    let root = durable_temp_dir("stream-publish-payload-sync-failure-retry");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 83);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    let ticket = store.submit_append_publish(&stream, 4096).unwrap();

    store.fail_next_append_payload_sync_for_test();
    let failed = store.wait_append_publish(&ticket);
    assert!(matches!(failed, Err(StorageError::Unavailable { .. })));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );
    assert!(
        store
            .local
            .metadata
            .state_inner()
            .unwrap()
            .append_publish_in_flight
            .is_empty(),
        "failed payload sync should clear the transient in-flight marker"
    );

    let commit = store.wait_append_publish(&ticket).unwrap();
    assert_eq!(commit.range, ByteRange::new(0, 4096));
    let mut bytes = vec![0; payload.len()];
    store
        .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, payload);
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut reopened_bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, 4096),
            &mut reopened_bytes,
        )
        .unwrap();
    assert_eq!(reopened_bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn prepared_append_publish_plan_is_invisible_and_fences_same_file_mutations() {
    let root = durable_temp_dir("stream-publish-plan-fences-file");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 83),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store.persist_append_stream_prefix(&stream, 4096).unwrap();
    let ticket = store.submit_append_publish(&stream, 4096).unwrap();
    let previous = store.durable.export_cursor().unwrap().unwrap();

    let plan = store
        .local
        .prepare_append_publish_plan(&ticket, WriteDurability::Flushed, &previous)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0,
        "prepared plan must not update the visible file head"
    );

    let tail = store
        .append_stream(&stream, b"tail", WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(tail.range.offset, 4096);
    assert!(
        store
            .local
            .commit_file_batch(
                keyspace_id,
                file_id,
                &[FileBatchWrite::new(0, b"write-at".to_vec())],
                WriteDurability::Flushed,
            )
            .is_err()
    );
    assert!(store.open_append_stream(keyspace_id, file_id).is_err());
    assert!(store.release_append_stream(&stream).is_err());
    assert!(store.abort_append_stream(&stream).is_err());
    assert!(
        store
            .submit_append_publish(&stream, tail.range.end_exclusive().unwrap())
            .is_err()
    );

    store.local.cancel_append_publish_plan(&plan).unwrap();
    let commit = store.wait_append_publish(&ticket).unwrap();
    assert_eq!(commit.range, ByteRange::new(0, 4096));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        4096
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_publish_prefix_profiles_are_bounded_sync_groups() {
    let root = durable_temp_dir("stream-publish-prefix-bounded-groups");
    let cfg = LocalStoreConfig {
        file_root_blocks: 48 * 1024,
        metadata_leaf_blocks: 48 * 1024,
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(16).unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let one_mib = vec![41; 1024 * 1024];
    let total_bytes = MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES + 8 * 1024 * 1024;
    let chunks = total_bytes / (1024 * 1024);
    for _ in 0..chunks {
        store
            .append_stream(&stream, &one_mib, WriteDurability::Acknowledged)
            .unwrap();
    }
    let prepublish_state = store.local.metadata.state_inner().unwrap();
    let prepublish_runs = &prepublish_state
        .append_streams
        .get(&stream.stream_id)
        .unwrap()
        .records;
    assert_eq!(
        prepublish_runs.len(),
        2,
        "stream rows should store bounded append runs, not one record per client append"
    );
    assert_eq!(prepublish_runs[0].len, MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES);
    assert_eq!(prepublish_runs[1].len, 8 * 1024 * 1024);

    store.publish_append_stream(&stream, total_bytes).unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(
        profiles.len(),
        1,
        "foreground publish should persist payload refs and visible metadata in one durable operation"
    );
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.new_segment_bytes)
            .sum::<u64>(),
        total_bytes
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_prefix_persist_collapses_many_appends_into_one_run_extent() {
    let root = durable_temp_dir("stream-prefix-persist-collapses-appends");
    let cfg = LocalStoreConfig {
        file_root_blocks: 16 * 1024,
        metadata_leaf_blocks: 16 * 1024,
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let one_mib = vec![73; 1024 * 1024];
    for _ in 0..16 {
        store
            .append_stream(&stream, &one_mib, WriteDurability::Acknowledged)
            .unwrap();
    }
    let prepublish_state = store.local.metadata.state_inner().unwrap();
    assert_eq!(
        prepublish_state
            .append_streams
            .get(&stream.stream_id)
            .unwrap()
            .records
            .len(),
        1,
        "stream rows should coalesce contiguous ingest before publish-prefix persistence"
    );

    store
        .publish_append_stream(&stream, 16 * 1024 * 1024)
        .unwrap();

    let run_extents = file_run_extents(&store.metadata(), keyspace_id, file_id);
    assert_eq!(
        run_extents.len(),
        1,
        "one bounded prefix-persist group should publish one run extent, not one extent per append"
    );
    assert_eq!(run_extents[0].payload_len, 16 * 1024 * 1024);

    let mut bytes = vec![0; one_mib.len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(15 * 1024 * 1024, one_mib.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, one_mib);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_node_shared_logs_keep_interleaved_files_visible() {
    let root = durable_temp_dir("stream-node-shared-logs-keep-files-visible");
    let cfg = LocalStoreConfig {
        file_root_blocks: 16 * 1024,
        metadata_leaf_blocks: 16 * 1024,
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_a = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();
    let stream_a = store.open_append_stream(keyspace_id, file_a).unwrap();
    let stream_b = store.open_append_stream(keyspace_id, file_b).unwrap();
    let one_mib_a = vec![11; 1024 * 1024];
    let one_mib_b = vec![29; 1024 * 1024];
    for _ in 0..8 {
        store
            .append_stream(&stream_a, &one_mib_a, WriteDurability::Acknowledged)
            .unwrap();
        store
            .append_stream(&stream_b, &one_mib_b, WriteDurability::Acknowledged)
            .unwrap();
    }

    store
        .publish_append_stream(&stream_a, 8 * 1024 * 1024)
        .unwrap();
    store
        .publish_append_stream(&stream_b, 8 * 1024 * 1024)
        .unwrap();

    let extents_a = file_run_extents(&store.metadata(), keyspace_id, file_a);
    let extents_b = file_run_extents(&store.metadata(), keyspace_id, file_b);
    assert_eq!(extents_a.len(), 8);
    assert_eq!(extents_b.len(), 8);
    assert_eq!(
        extents_a
            .iter()
            .map(|extent| extent.payload_len)
            .sum::<u64>(),
        8 * 1024 * 1024
    );
    assert_eq!(
        extents_b
            .iter()
            .map(|extent| extent.payload_len)
            .sum::<u64>(),
        8 * 1024 * 1024
    );
    assert!(
        extents_a
            .iter()
            .all(|extent| extent.run.log_id == extents_a[0].run.log_id)
    );
    assert!(
        extents_b
            .iter()
            .all(|extent| extent.run.log_id == extents_b[0].run.log_id)
    );
    let mut bytes_a = vec![0; 8 * 1024 * 1024];
    let mut bytes_b = vec![0; 8 * 1024 * 1024];
    store
        .read_file(
            keyspace_id,
            file_a,
            ByteRange::new(0, bytes_a.len() as u64),
            &mut bytes_a,
        )
        .unwrap();
    store
        .read_file(
            keyspace_id,
            file_b,
            ByteRange::new(0, bytes_b.len() as u64),
            &mut bytes_b,
        )
        .unwrap();
    assert_eq!(bytes_a, one_mib_a.repeat(8));
    assert_eq!(bytes_b, one_mib_b.repeat(8));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_concurrent_append_stream_publishes_do_not_overdrain_other_streams() {
    let root = durable_temp_dir("stream-publish-no-overdrain");
    let cfg = LocalStoreConfig {
        file_root_blocks: 16 * 1024,
        metadata_leaf_blocks: 16 * 1024,
        ..config()
    };
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let one_mib = vec![51; 1024 * 1024];
    let mut streams = Vec::new();
    for index in 0..4 {
        let file_id = store
            .create_file(
                keyspace_id,
                CreateFileRequest {
                    spec: FileSpec {
                        name: Some(format!("file-{index}")),
                    },
                },
            )
            .unwrap();
        let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
        for _ in 0..16 {
            store
                .append_stream(&stream, &one_mib, WriteDurability::Acknowledged)
                .unwrap();
        }
        streams.push(stream);
    }
    store.enable_persist_profiling(16).unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(streams.len() + 1));
    let mut handles = Vec::new();
    for stream in streams {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            store
                .publish_append_stream(&stream, 16 * 1024 * 1024)
                .unwrap()
        }));
    }
    barrier.wait();

    let commits: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert!(
        commits
            .iter()
            .all(|commit| commit.range.end_exclusive().unwrap() == 16 * 1024 * 1024)
    );
    let profiles = store.drain_persist_profiles(16).unwrap();
    let data_profiles: Vec<_> = profiles
        .iter()
        .filter(|profile| profile.new_segment_bytes > 0)
        .collect();
    assert!(
        (1..=4).contains(&data_profiles.len()),
        "publish waiters may collapse into one foreground publish, but must not overdrain non-waiters"
    );
    assert_eq!(
        data_profiles
            .iter()
            .map(|profile| profile.new_segment_bytes)
            .sum::<u64>(),
        4 * 16 * 1024 * 1024
    );
    assert!(
        profiles
            .iter()
            .all(|profile| profile.lock_wait_nanos < 1_000_000)
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn stream_prefix_persist_request_table_keeps_same_stream_waiters() {
    let stream = AppendStream {
        keyspace_id: KeyspaceId::from_raw(1),
        file_id: FileId::from_raw(2),
        stream_id: AppendStreamId::from_raw(3),
        writer_epoch: WriterEpoch::from_raw(4),
        base_version: FileVersion::from_raw(5),
        visible_base_size: 0,
    };
    let mut state = StreamPrefixPersistCoordinatorState {
        in_flight: false,
        generation: 0,
        requests: BTreeMap::new(),
        last_error: None,
    };

    state.add_request(&stream, 16 * 1024 * 1024);
    state.add_request(&stream, 32 * 1024 * 1024);

    let requests = state.snapshot_requests();
    assert_eq!(requests, vec![(stream.clone(), 32 * 1024 * 1024)]);
    let request = state.requests.get(&stream.stream_id).unwrap();
    assert_eq!(request.waiters, 2);

    state.release_request(stream.stream_id);
    let request = state.requests.get(&stream.stream_id).unwrap();
    assert_eq!(request.waiters, 1);
    assert_eq!(request.durable_through, 32 * 1024 * 1024);

    state.release_request(stream.stream_id);
    assert!(state.requests.is_empty());
}

#[test]
fn durable_stream_publish_does_not_persist_unrelated_private_stream_data() {
    let root = durable_temp_dir("stream-publish-leaves-private-data-unpublished");
    let cfg = LocalStoreConfig {
        file_root_blocks: 16 * 1024,
        ..config()
    };
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let target_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("target".to_string()),
                },
            },
        )
        .unwrap();
    let private_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("private".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(16).unwrap();

    let one_mib = vec![17; 1024 * 1024];
    let target_stream = store.open_append_stream(keyspace_id, target_file).unwrap();
    store
        .append_stream(&target_stream, &one_mib, WriteDurability::Acknowledged)
        .unwrap();

    let private_stream = store.open_append_stream(keyspace_id, private_file).unwrap();
    for _ in 0..40 {
        store
            .append_stream(&private_stream, &one_mib, WriteDurability::Acknowledged)
            .unwrap();
    }

    store
        .publish_append_stream(&target_stream, 1024 * 1024)
        .unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();
    assert!(
        !profiles.is_empty(),
        "publish should still record a physical metadata persist"
    );
    assert!(
        profiles
            .iter()
            .all(|profile| profile.new_segment_bytes <= MAX_DATA_LOG_SYNC_GROUP_BYTES)
    );
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.new_segment_bytes)
            .sum::<u64>(),
        1024 * 1024,
        "publishing one stream must persist only that stream's requested prefix"
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        matches!(
            reopened.publish_append_stream(&private_stream, 40 * 1024 * 1024),
            Err(StorageError::Conflict { reason }) if reason == "stale append stream"
        ),
        "unpublished private stream tokens must not become publishable through unrelated publish"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_publish_delta_does_not_reappend_persisted_payloads() {
    let root = durable_temp_dir("stream-publish-no-reappend");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 51);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    store.publish_append_stream(&stream, 4096).unwrap();
    let publish_profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(publish_profiles.len(), 1);
    assert_eq!(publish_profiles[0].new_segment_bytes, 4096);
    assert!(publish_profiles[0].data_log_files_synced > 0);
    assert!(publish_profiles[0].data_log_file_sync_max_nanos > 0);
    assert!(publish_profiles[0].node_catalog_manifest_rows > 0);
    assert_eq!(publish_profiles[0].node_catalog_placement_rows, 0);
    assert_eq!(publish_profiles[0].node_catalog_segment_rows, 0);
    let head = store
        .local
        .metadata
        .get_file_head(keyspace_id, file_id)
        .unwrap();
    let root_node = store.local.metadata.get_metadata_node(head.root).unwrap();
    let MetadataNodeKind::Leaf {
        entries,
        run_extents,
    } = root_node.kind
    else {
        panic!("small native file root should remain a leaf");
    };
    assert!(
        entries.is_empty(),
        "append-run publish should not create ordinary segment entries"
    );
    assert_eq!(run_extents.len(), 1);
    assert_eq!(run_extents[0].file_offset_start, 0);
    assert_eq!(run_extents[0].payload_len, 4096);
    assert_eq!(run_extents[0].run.keyspace_id, keyspace_id);
    assert_eq!(run_extents[0].run.file_id, file_id);
    assert_eq!(run_extents[0].run.stream_id, stream.stream_id);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_publish_delta_is_target_bounded() {
    let root = durable_temp_dir("stream-publish-target-bounded");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_a = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();
    store.enable_persist_profiling(16).unwrap();

    let stream_a = store.open_append_stream(keyspace_id, file_a).unwrap();
    let payload_a = repeated_blocks(1, 61);
    store
        .append_stream(&stream_a, &payload_a, WriteDurability::Acknowledged)
        .unwrap();
    store.persist_append_stream_prefix(&stream_a, 4096).unwrap();

    let stream_b = store.open_append_stream(keyspace_id, file_b).unwrap();
    let payload_b = repeated_blocks(1, 62);
    store
        .append_stream(&stream_b, &payload_b, WriteDurability::Acknowledged)
        .unwrap();
    store.persist_append_stream_prefix(&stream_b, 4096).unwrap();
    let _ = store.drain_persist_profiles(16).unwrap();

    let changed_a = BTreeSet::new();
    let changed_b = BTreeSet::new();

    let commit_a = store
        .local
        .publish_append_stream(&stream_a, 4096, WriteDurability::Flushed)
        .unwrap();
    let commit_b = store
        .local
        .publish_append_stream(&stream_b, 4096, WriteDurability::Flushed)
        .unwrap();
    assert!(commit_b.commit_seq > commit_a.commit_seq);

    store
        .persist_append_stream_publish_delta(&stream_a, commit_a.commit_seq, &changed_a)
        .unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(
        profiles.len(),
        1,
        "the first publish persist should export the requested commit"
    );
    assert_eq!(profiles[0].new_segment_bytes, 0);
    assert!(
        profiles[0].durable_commit_high_water >= commit_a.commit_seq.raw(),
        "publish delta must advance the durable high-water through the requested commit"
    );
    assert!(
        profiles[0].durable_commit_high_water < commit_b.commit_seq.raw(),
        "publish delta should not scan forward and export unrelated later commits"
    );

    store
        .persist_append_stream_publish_delta(&stream_b, commit_b.commit_seq, &changed_b)
        .unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].new_segment_bytes, 0);
    assert!(profiles[0].durable_commit_high_water >= commit_b.commit_seq.raw());

    store.flush_file(keyspace_id, file_b).unwrap();
    assert!(
        store.drain_persist_profiles(16).unwrap().is_empty(),
        "flush_file should also observe the coalesced durable high-water"
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut read_a = vec![0; payload_a.len()];
    reopened
        .read_file(
            keyspace_id,
            file_a,
            ByteRange::new(0, payload_a.len() as u64),
            &mut read_a,
        )
        .unwrap();
    assert_eq!(read_a, payload_a);
    let mut read_b = vec![0; payload_b.len()];
    reopened
        .read_file(
            keyspace_id,
            file_b,
            ByteRange::new(0, payload_b.len() as u64),
            &mut read_b,
        )
        .unwrap();
    assert_eq!(read_b, payload_b);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_stream_publish_delta_does_not_wait_for_data_log_persist_lock() {
    let root = durable_temp_dir("stream-publish-bypasses-data-log-lock");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 65);
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();
    store.persist_append_stream_prefix(&stream, 4096).unwrap();
    store.enable_persist_profiling(16).unwrap();

    let persist_guard = lock(&store.persist_lock).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = {
        let store = Arc::clone(&store);
        let stream = stream.clone();
        thread::spawn(move || {
            let result = store.publish_append_stream(&stream, 4096);
            tx.send(result.map(|_| ())).unwrap();
        })
    };

    rx.recv_timeout(Duration::from_secs(1))
        .expect("append-stream publish should not wait for the data-log persist lock")
        .unwrap();
    drop(persist_guard);
    worker.join().unwrap();

    let profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].new_segment_bytes, 0);
    assert_eq!(profiles[0].data_log_files_synced, 0);
    assert_eq!(profiles[0].data_log_file_sync_nanos, 0);
    assert_eq!(profiles[0].data_log_dir_sync_nanos, 0);
    assert!(profiles[0].lock_wait_nanos < 1_000_000);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_concurrent_append_stream_publish_persists_to_requested_high_water() {
    let root = durable_temp_dir("stream-publish-requested-high-water");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_a = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();

    let stream_a = store.open_append_stream(keyspace_id, file_a).unwrap();
    let payload_a = repeated_blocks(1, 63);
    store
        .append_stream(&stream_a, &payload_a, WriteDurability::Acknowledged)
        .unwrap();
    store.persist_append_stream_prefix(&stream_a, 4096).unwrap();

    let stream_b = store.open_append_stream(keyspace_id, file_b).unwrap();
    let payload_b = repeated_blocks(1, 64);
    store
        .append_stream(&stream_b, &payload_b, WriteDurability::Acknowledged)
        .unwrap();
    store.persist_append_stream_prefix(&stream_b, 4096).unwrap();

    let commit_a = store
        .local
        .publish_append_stream(&stream_a, 4096, WriteDurability::Flushed)
        .unwrap();
    let commit_b = store
        .local
        .publish_append_stream(&stream_b, 4096, WriteDurability::Flushed)
        .unwrap();
    assert!(commit_b.commit_seq > commit_a.commit_seq);

    store.enable_persist_profiling(16).unwrap();
    store
        .set_persist_delay_for_test(Some(Duration::from_millis(20)))
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let stream = stream_a.clone();
        thread::spawn(move || {
            barrier.wait();
            store.persist_append_stream_publish_delta(
                &stream,
                commit_a.commit_seq,
                &BTreeSet::new(),
            )
        })
    };
    let second = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let stream = stream_b.clone();
        thread::spawn(move || {
            barrier.wait();
            store.persist_append_stream_publish_delta(
                &stream,
                commit_b.commit_seq,
                &BTreeSet::new(),
            )
        })
    };
    barrier.wait();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
    store.set_persist_delay_for_test(None).unwrap();

    let profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.new_segment_bytes)
            .sum::<u64>(),
        0,
        "metadata publish persistence should not reappend stream payload bytes"
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.durable_commit_high_water >= commit_b.commit_seq.raw())
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut read_a = vec![0; payload_a.len()];
    reopened
        .read_file(
            keyspace_id,
            file_a,
            ByteRange::new(0, payload_a.len() as u64),
            &mut read_a,
        )
        .unwrap();
    assert_eq!(read_a, payload_a);
    let mut read_b = vec![0; payload_b.len()];
    reopened
        .read_file(
            keyspace_id,
            file_b,
            ByteRange::new(0, payload_b.len() as u64),
            &mut read_b,
        )
        .unwrap();
    assert_eq!(read_b, payload_b);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_flush_persists_acknowledged_segment_once() {
    let root = durable_temp_dir("ack-flush-persists-segment-once");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 22),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let data_log = data_log_path(&root.join("data"), cfg.storage_node, 1);
    let preflush_len = data_log.metadata().unwrap().len();
    assert!(preflush_len > 4096);

    store.flush_device(device_id).unwrap();
    let postflush_len = data_log.metadata().unwrap().len();
    assert_eq!(postflush_len, preflush_len);
    let profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].new_segment_count, 0);
    assert_eq!(profiles[0].new_segment_bytes, 0);
    assert_eq!(profiles[0].data_log_prestaged_segment_count, 1);
    assert_eq!(profiles[0].data_log_prestaged_segment_bytes, 4096);
    assert_eq!(profiles[0].data_log_sync_only_bytes, 4096);
    assert_eq!(profiles[0].data_log_flush_write_bytes, 0);
    assert_eq!(profiles[0].data_log_records_written, 0);
    assert_eq!(profiles[0].block_delta_selected_count, 1);
    assert_eq!(profiles[0].block_delta_selected_bytes, 4096);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 22));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_prestaged_flush_skips_directory_sync_for_existing_log() {
    let root = durable_temp_dir("block-prestage-existing-log-no-dir-sync");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 26),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store.flush_device(device_id).unwrap();
    let first = store.drain_persist_profiles(8).unwrap();
    assert_eq!(first.len(), 1);
    assert!(
        first[0].data_log_dir_sync_nanos > 0,
        "new unsynced data-log files require directory sync before metadata publish"
    );

    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 27),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store.flush_device(device_id).unwrap();
    let second = store.drain_persist_profiles(8).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0].data_log_dir_sync_nanos, 0,
        "existing active data-log files should not force a directory sync on every fsync"
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 2 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 2 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[0..4096], repeated_blocks(1, 26));
    assert_eq!(&bytes[4096..2 * 4096], repeated_blocks(1, 27));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_prestage_unavailable_falls_back_at_flush() {
    let root = durable_temp_dir("block-prestage-fallback");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_persist_profiling(8).unwrap();
    store.fail_next_prestage_for_test();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 23),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    assert!(!data_log_path(&root.join("data"), cfg.storage_node, 1).exists());

    store.flush_device(device_id).unwrap();
    let profiles = store.drain_persist_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].new_segment_count, 1);
    assert_eq!(profiles[0].new_segment_bytes, 4096);
    assert_eq!(profiles[0].data_log_prestaged_segment_count, 0);
    assert_eq!(profiles[0].data_log_flush_write_bytes, 4096);
    assert_eq!(profiles[0].data_log_records_written, 1);
    assert!(profiles[0].data_log_write_nanos > 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 23));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn failed_flush_after_block_prestage_does_not_expose_tail_after_reopen() {
    let root = durable_temp_dir("block-prestage-failed-flush-hidden");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 24),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    assert!(data_log_path(&root.join("data"), cfg.storage_node, 1).exists());

    store.fail_next_persist_for_test();
    assert!(store.flush_device(device_id).is_err());
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![99; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, vec![0; 4096]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_persist_profiling_is_opt_in_and_records_physical_persists() {
    let root = durable_temp_dir("persist-profile");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    assert!(store.drain_persist_profiles(16).unwrap().is_empty());

    store.enable_persist_profiling(8).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 8),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let flush = store.flush_device(device_id).unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();

    assert_eq!(profiles.len(), 1);
    let profile = profiles[0];
    assert_eq!(profile.sequence, 1);
    assert!(profile.total_nanos >= profile.local_snapshot_nanos);
    assert!(profile.data_log_append_sync_nanos >= profile.data_log_file_sync_nanos);
    assert_eq!(profile.data_log_encode_nanos, 0);
    assert_eq!(profile.data_log_write_nanos, 0);
    assert!(profile.data_log_file_sync_nanos > 0);
    assert!(profile.data_log_file_sync_sum_nanos > 0);
    assert!(profile.data_log_file_sync_max_nanos > 0);
    assert!(profile.data_log_file_sync_sum_nanos >= profile.data_log_file_sync_max_nanos);
    assert!(profile.data_log_files_synced > 0);
    assert!(profile.data_log_sync_bytes > 0);
    assert_eq!(profile.new_segment_count, 0);
    assert_eq!(profile.new_segment_bytes, 0);
    assert_eq!(profile.data_log_prestaged_segment_count, 1);
    assert_eq!(profile.data_log_prestaged_segment_bytes, 4096);
    assert_eq!(profile.data_log_sync_only_bytes, 4096);
    assert_eq!(profile.data_log_flush_write_bytes, 0);
    assert_eq!(profile.data_log_records_written, 0);
    assert!(profile.touched_node_count > 0);
    assert!(profile.durable_commit_high_water >= flush.durable_through.raw());
    assert!(store.drain_persist_profiles(16).unwrap().is_empty());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_concurrent_flushes_coalesce_to_one_physical_persist() {
    let root = durable_temp_dir("persist-coalesce");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store.enable_persist_profiling(64).unwrap();
    store
        .set_persist_delay_for_test(Some(Duration::from_millis(50)))
        .unwrap();
    for block in 0..4 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, block as u8 + 1),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }

    let barrier = Arc::new(std::sync::Barrier::new(5));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            store.flush_device(device_id).unwrap().durable_through
        }));
    }
    barrier.wait();

    let mut durable_through = Vec::new();
    for handle in handles {
        durable_through.push(handle.join().unwrap());
    }
    assert!(durable_through.iter().all(|seq| seq.raw() >= 4));
    let profiles = store.drain_persist_profiles(64).unwrap();
    assert_eq!(
        profiles.len(),
        1,
        "concurrent flush waiters should share one physical persist"
    );
    assert_eq!(profiles[0].new_segment_count, 0);
    assert_eq!(profiles[0].new_segment_bytes, 0);
    assert_eq!(profiles[0].data_log_prestaged_segment_count, 4);
    assert_eq!(profiles[0].data_log_prestaged_segment_bytes, 4 * 4096);
    assert_eq!(profiles[0].data_log_sync_only_bytes, 4 * 4096);
    assert_eq!(profiles[0].data_log_flush_write_bytes, 0);
    assert_eq!(profiles[0].data_log_records_written, 0);
    assert_eq!(profiles[0].block_delta_selected_count, 4);
    assert_eq!(profiles[0].block_delta_selected_bytes, 4 * 4096);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    for block in 0..4 {
        let mut buf = vec![0; 4096];
        reopened
            .read_device(device_id, ByteRange::new(block * 4096, 4096), &mut buf)
            .unwrap();
        assert_eq!(buf, repeated_blocks(1, block as u8 + 1));
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_persist_failure_wakes_waiters_and_later_flush_retries() {
    let root = durable_temp_dir("persist-failure-wakes");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .set_persist_delay_for_test(Some(Duration::from_millis(50)))
        .unwrap();
    store.fail_next_persist_for_test();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 9),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            store.flush_device(device_id)
        }));
    }
    barrier.wait();
    for handle in handles {
        assert!(handle.join().unwrap().is_err());
    }

    store.set_persist_delay_for_test(None).unwrap();
    let flush = store.flush_device(device_id).unwrap();
    assert!(flush.durable_through.raw() > 0);
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut buf = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut buf)
        .unwrap();
    assert_eq!(buf, repeated_blocks(1, 9));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_uses_row_native_metadata_without_current_state_blob() {
    let root = durable_temp_dir("row-native-no-current-state");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 11),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let current_state_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'current_state'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(current_state_tables, 0);
    for table in [
        "store_meta",
        "device_manifests",
        "device_shard_heads",
        "metadata_nodes",
    ] {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(count > 0, "{table} should have row-native rows");
    }
    for table in [
        "data_logs",
        "segment_placements",
        "storage_nodes",
        "segment_records",
        "segment_catalog_entries",
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                     WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "{table} should not live in metadata.sqlite");
    }
    drop(conn);

    let node_conn = node_catalog_conn(&root, cfg.storage_node);
    for table in [
        "node_meta",
        "data_logs",
        "segment_placements",
        "segment_catalog_entries",
    ] {
        let count: i64 = node_conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(count > 0, "{table} should have node-local catalog rows");
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_stores_block_flushes_as_delta_rows_until_checkpoint() {
    let root = durable_temp_dir("per-shard-device-heads");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let old_head_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name IN ('device_heads', 'deleted_device_heads')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_head_tables, 0);
    let manifest_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM device_manifests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(manifest_count, 1);
    let initial_manifest: Vec<u8> = conn
        .query_row(
            "SELECT payload FROM device_manifests WHERE device_id = ?1",
            params![device_id.raw().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let initial_shards = device_shard_payloads_for_test(&conn);
    assert_eq!(initial_shards.len(), cfg.shard_count);
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let after_first_manifest: Vec<u8> = conn
        .query_row(
            "SELECT payload FROM device_manifests WHERE device_id = ?1",
            params![device_id.raw().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_first_manifest, initial_manifest);
    let after_first = device_shard_payloads_for_test(&conn);
    assert_eq!(
        changed_payload_count(&initial_shards, &after_first),
        0,
        "a flushed block delta should not rewrite checkpoint shard-head rows"
    );
    let first_delta_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM block_delta_commits", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(first_delta_count, 1);
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .write_device(
            device_id,
            8 * 4096,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let after_second = device_shard_payloads_for_test(&conn);
    assert_eq!(
        changed_payload_count(&initial_shards, &after_second),
        0,
        "uncheckpointed block deltas should leave shard-head rows stable"
    );
    let second_delta_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM block_delta_commits", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(second_delta_count, 2);
    drop(conn);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut buf = vec![0; 16 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 16 * 4096), &mut buf)
        .unwrap();
    assert_eq!(&buf[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&buf[8 * 4096..9 * 4096], repeated_blocks(1, 2).as_slice());
    reopened.checkpoint(device_id).unwrap();
    drop(reopened);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let after_checkpoint = device_shard_payloads_for_test(&conn);
    assert_eq!(
        changed_payload_count(&initial_shards, &after_checkpoint),
        2,
        "checkpoint should fold both dirty shard roots"
    );
    let delta_count_after_checkpoint: i64 = conn
        .query_row("SELECT COUNT(*) FROM block_delta_commits", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(delta_count_after_checkpoint, 0);
    drop(conn);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_corrupt_block_delta_payload() {
    let root = durable_temp_dir("block-delta-corrupt-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 44),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute("UPDATE block_delta_commits SET payload = x'ff'", [])
        .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_zero_and_discard_use_metadata_only_block_deltas() {
    let root = durable_temp_dir("metadata-only-zero-discard");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store.enable_persist_profiling(32).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();

    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(8, 9),
            WriteDurability::Flushed,
        )
        .unwrap();
    let _ = store.drain_persist_profiles(32).unwrap();

    store.write_zeroes(device_id, 2 * 4096, 4096).unwrap();
    let zero_profiles = store.drain_persist_profiles(32).unwrap();
    assert_metadata_only_block_delta_profiles(&zero_profiles, 4096);

    store.discard_device(device_id, 5 * 4096, 2 * 4096).unwrap();
    let discard_profiles = store.drain_persist_profiles(32).unwrap();
    assert_metadata_only_block_delta_profiles(&discard_profiles, 2 * 4096);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut actual = vec![0; 8 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 8 * 4096), &mut actual)
        .unwrap();
    assert_eq!(&actual[0..2 * 4096], repeated_blocks(2, 9).as_slice());
    assert_eq!(&actual[2 * 4096..3 * 4096], vec![0; 4096].as_slice());
    assert_eq!(
        &actual[3 * 4096..5 * 4096],
        repeated_blocks(2, 9).as_slice()
    );
    assert_eq!(&actual[5 * 4096..7 * 4096], vec![0; 2 * 4096].as_slice());
    assert_eq!(
        &actual[7 * 4096..8 * 4096],
        repeated_blocks(1, 9).as_slice()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_native_flush_after_block_ack_falls_back_to_full_persist() {
    let root = durable_temp_dir("block-native-gap-full-persist");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();

    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 45),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, b"native".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut block = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut block)
        .unwrap();
    assert_eq!(block, repeated_blocks(1, 45));
    let mut file = vec![0; b"native".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"native".len() as u64),
            &mut file,
        )
        .unwrap();
    assert_eq!(file, b"native");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_stores_native_keyspace_heads_as_per_shard_rows() {
    let root = durable_temp_dir("per-shard-keyspace-heads");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_a_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let old_head_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'keyspace_heads'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(old_head_tables, 0);
    let manifest_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM keyspace_manifests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(manifest_count, 1);
    let initial_manifest: Vec<u8> = conn
        .query_row(
            "SELECT payload FROM keyspace_manifests WHERE keyspace_id = ?1",
            params![keyspace_id.raw().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let initial_shards = keyspace_shard_payloads_for_test(&conn);
    assert_eq!(initial_shards.len(), KEYSPACE_CATALOG_SHARD_COUNT);
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_a_id,
            &[FileBatchWrite::new(0, b"aaaa".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let after_first_manifest: Vec<u8> = conn
        .query_row(
            "SELECT payload FROM keyspace_manifests WHERE keyspace_id = ?1",
            params![keyspace_id.raw().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_first_manifest, initial_manifest);
    let after_first = keyspace_shard_payloads_for_test(&conn);
    assert_eq!(
        changed_payload_count(&initial_shards, &after_first),
        1,
        "a single-file write should update one keyspace shard-head row"
    );
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_b_id,
            &[FileBatchWrite::new(0, b"bbbb".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let after_second = keyspace_shard_payloads_for_test(&conn);
    assert_eq!(
        changed_payload_count(&after_first, &after_second),
        1,
        "an independent file write should update only its keyspace shard-head row"
    );
    drop(conn);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut a = vec![0; 4];
    reopened
        .read_file(keyspace_id, file_a_id, ByteRange::new(0, 4), &mut a)
        .unwrap();
    let mut b = vec![0; 4];
    reopened
        .read_file(keyspace_id, file_b_id, ByteRange::new(0, 4), &mut b)
        .unwrap();
    assert_eq!(a, b"aaaa");
    assert_eq!(b, b"bbbb");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_persist_profile_reports_block_metadata_contention_fields() {
    let root = durable_temp_dir("block-metadata-profile-fields");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store.enable_persist_profiling(16).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let _ = store.drain_persist_profiles(16).unwrap();

    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    let profiles = store.drain_persist_profiles(16).unwrap();
    let profile = profiles.last().expect("flushed write should persist");
    assert_eq!(profile.logical_conflict_count, 0);
    assert_eq!(profile.touched_shard_head_rows, 1);
    assert_eq!(profile.commit_rows_written, 1);
    assert!(profile.total_nanos >= profile.lock_wait_nanos);
    assert!(profile.root_sqlite_row_sync_nanos > 0);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_legacy_current_state_blob_store() {
    let root = durable_temp_dir("legacy-current-state");
    fs::create_dir_all(&root).unwrap();
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute_batch(
        "CREATE TABLE current_state(
               id INTEGER PRIMARY KEY CHECK (id = 1),
               state_blob BLOB NOT NULL
             );
             INSERT INTO current_state(id, state_blob) VALUES (1, x'00');",
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, config()).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_root_storage_node_catalog_tables() {
    let root = durable_temp_dir("root-storage-catalog-table");
    fs::create_dir_all(&root).unwrap();
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute_batch("CREATE TABLE data_logs(log_id INTEGER PRIMARY KEY);")
        .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, config()).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_row_native_rows_without_cursor() {
    let root = durable_temp_dir("row-native-without-cursor");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "INSERT INTO device_specs(device_id, payload) VALUES ('1', x'00')",
        [],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_recovers_node_catalog_rows_without_cursor_as_storage_orphans() {
    let root = durable_temp_dir("node-catalog-without-cursor");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    drop(store);

    let conn = node_catalog_conn(&root, cfg.storage_node);
    conn.execute(
        "INSERT INTO node_meta(
	               id, storage_node, ordinal, next_catalog_segment_id, segment_store_next_offset
	             ) VALUES (1, ?1, 0, '3', 0)",
        params![cfg.storage_node.raw().to_string()],
    )
    .unwrap();
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 77),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert_eq!(
        first_device_segment(&store, device_id),
        SegmentId::from_raw(3)
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_repairs_root_referenced_pending_catalog_rows_on_reopen() {
    let root = durable_temp_dir("pending-catalog-reference-repair");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 11),
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = first_device_segment(&store, device_id);
    drop(store);

    assert_eq!(
        node_catalog_entry(&root, cfg.storage_node, segment_id).state,
        SegmentLifecycleState::DurablePendingMetadata
    );

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 11));
    assert_eq!(
        reopened.segment_catalog().state(segment_id).unwrap(),
        SegmentLifecycleState::Referenced
    );
    drop(reopened);

    assert_eq!(
        node_catalog_entry(&root, cfg.storage_node, segment_id).state,
        SegmentLifecycleState::Referenced
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_persists_native_stream_run_extents_on_publish() {
    let root = durable_temp_dir("native-stream-reference-persist");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &repeated_blocks(1, 12),
        WriteDurability::Flushed,
    )
    .unwrap();
    assert!(file_segment_ids(&store.metadata(), keyspace_id, file_id).is_empty());
    let run_extents = file_run_extents(&store.metadata(), keyspace_id, file_id);
    assert_eq!(run_extents.len(), 1);
    assert_eq!(run_extents[0].run.storage_node, cfg.storage_node);
    let data_log_rows = store.durable.data_log_rows_for_test().unwrap();
    assert_eq!(data_log_rows.len(), 1);
    assert!(
        data_log_rows[0].live_bytes > 0,
        "published append-run log bytes must remain protected from compaction"
    );
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let delta_tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'native_metadata_delta_commits'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        delta_tables, 0,
        "append publish deltas should not live in root SQLite"
    );
    drop(conn);
    assert_eq!(
        native_publish_journal_commit_count(&root, 0),
        0,
        "append publish should not persist a native metadata delta"
    );
    assert_eq!(
        append_visible_publish_journal_count(&root, keyspace_id, file_id),
        1,
        "append publish should persist one compact file-scoped visible record"
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(file_segment_ids(&reopened.metadata(), keyspace_id, file_id).is_empty());
    let reopened_run_extents = file_run_extents(&reopened.metadata(), keyspace_id, file_id);
    assert_eq!(reopened_run_extents, run_extents);
    assert_eq!(
        native_publish_journal_commit_count(&root, 0),
        0,
        "reopen should not synthesize native metadata deltas"
    );
    assert_eq!(
        append_visible_publish_journal_count(&root, keyspace_id, file_id),
        1,
        "reopen should keep the append-visible journal available for idempotent replay"
    );
    reopened.persist_now().unwrap();
    assert_eq!(
        native_publish_journal_commit_count(&root, 0),
        0,
        "full native persist should leave the native metadata delta journal empty"
    );
    assert_eq!(
        append_visible_publish_journal_count(&root, keyspace_id, file_id),
        1,
        "append-visible pruning is separate from full row-native persistence"
    );
    reopened
        .durable
        .compact_data_logs(DurableDataLogPolicy::compact_everything_for_test())
        .unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 12));
    drop(reopened);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_hot_path_rejects_committed_record_corruption() {
    let root = durable_temp_dir("append-visible-publish-hot-path-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &repeated_blocks(1, 13),
        WriteDurability::Flushed,
    )
    .unwrap();
    drop(store);

    let journal = append_visible_publish_journal_path_for_test(&root, keyspace_id, file_id);
    let mut bytes = fs::read(&journal).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0xff;
    fs::write(&journal, bytes).unwrap();

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_hot_path_ignores_incomplete_tail() {
    let root = durable_temp_dir("append-visible-publish-hot-path-incomplete-tail");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let payload = repeated_blocks(1, 14);
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &payload,
        WriteDurability::Flushed,
    )
    .unwrap();
    drop(store);

    let mut journal = OpenOptions::new()
        .append(true)
        .open(append_visible_publish_journal_path_for_test(
            &root,
            keyspace_id,
            file_id,
        ))
        .unwrap();
    journal.write_all(b"incomplete").unwrap();
    journal.sync_data().unwrap();
    drop(journal);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

fn append_visible_publish_for_journal(
    record_id: u128,
    base_file_version: u64,
    base_writer_epoch: u64,
    writer_epoch: u64,
    old_size: u64,
    payload_len: u64,
) -> AppendVisiblePublish {
    let keyspace_id = KeyspaceId::from_raw(41);
    let file_id = FileId::from_raw(42);
    let base_writer_epoch = WriterEpoch::from_raw(base_writer_epoch);
    let writer_epoch = WriterEpoch::from_raw(writer_epoch);
    let run = AppendLogRunRange {
        run_id: AppendRunId::from_raw(record_id),
        storage_node: StorageNodeId::from_raw(44),
        stream_id: AppendStreamId::from_raw(45),
        writer_epoch,
        keyspace_id,
        file_id,
        file_offset_start: old_size,
        payload_len,
        log_id: 46,
        log_payload_offset: old_size,
        integrity: SegmentPayloadIntegrity::Unchecked,
    };
    AppendVisiblePublish {
        record_id: AppendPublishTicketId::from_raw(record_id),
        commit_seq: CommitSeq::from_raw(
            100 + u64::try_from(record_id).expect("test record id fits u64"),
        ),
        keyspace_id,
        file_id,
        base_writer_epoch,
        writer_epoch,
        base_file_version: FileVersion::from_raw(base_file_version),
        new_file_version: FileVersion::from_raw(base_file_version + 1),
        old_size,
        new_size: old_size + payload_len,
        publish_through: old_size + payload_len,
        run_extents: vec![RunBackedFileExtent {
            file_offset_start: old_size,
            payload_len,
            run,
        }],
    }
}

#[test]
fn durable_append_visible_publish_journal_round_trips_and_ignores_incomplete_tail() {
    let root = durable_temp_dir("append-visible-publish-journal-tail");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let first = append_visible_publish_for_journal(1, 4, 42, 43, 4096, 4096);
    let second = append_visible_publish_for_journal(2, 5, 43, 43, 8192, 4096);
    store
        .durable
        .append_append_visible_publish_journal_record(&first)
        .unwrap();
    store
        .durable
        .append_append_visible_publish_journal_record(&second)
        .unwrap();
    let journal = store.durable.append_visible_publish_journal_path();
    let mut file = OpenOptions::new().append(true).open(&journal).unwrap();
    file.write_all(b"incomplete").unwrap();
    file.sync_data().unwrap();
    drop(file);

    let records = store
        .durable
        .append_visible_publish_journal_records()
        .unwrap();
    assert_eq!(records, vec![first, second]);
    let replay = replay_append_visible_publishes_for_file(
        KeyspaceId::from_raw(41),
        FileId::from_raw(42),
        WriterEpoch::from_raw(42),
        FileVersion::from_raw(4),
        4096,
        CommitSeq::from_raw(100),
        &records,
    )
    .unwrap();
    assert_eq!(replay.latest_file_version, FileVersion::from_raw(6));
    assert_eq!(replay.latest_size, 12_288);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_journal_rejects_committed_record_corruption() {
    let root = durable_temp_dir("append-visible-publish-journal-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let record = append_visible_publish_for_journal(1, 4, 42, 43, 4096, 4096);
    store
        .durable
        .append_append_visible_publish_journal_record(&record)
        .unwrap();
    let journal = store.durable.append_visible_publish_journal_path();

    let mut bytes = fs::read(&journal).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 0xff;
    fs::write(&journal, bytes).unwrap();

    assert!(
        store
            .durable
            .append_visible_publish_journal_records()
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_record_materializes_on_reopen() {
    let root = durable_temp_dir("append-visible-publish-materialize-reopen");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let base_head = store
        .metadata()
        .get_file_head(keyspace_id, file_id)
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let payload = repeated_blocks(1, 91);
    let ticket = store
        .append_stream(&stream, &payload, WriteDurability::Flushed)
        .unwrap();
    assert!(file_run_extents(&store.metadata(), keyspace_id, file_id).is_empty());

    let publish_records = store
        .local
        .metadata
        .append_stream_publish_records(
            &stream,
            base_head.size,
            ticket.range.end_exclusive().unwrap(),
        )
        .unwrap();
    let run_extents = publish_records
        .into_iter()
        .map(|record| RunBackedFileExtent {
            file_offset_start: record.offset,
            payload_len: record.len,
            run: record.run.full_range(),
        })
        .collect::<Vec<_>>();
    let metadata = store.metadata().state_inner().unwrap();
    let base_writer_epoch = metadata
        .file_writer_epochs
        .get(&(keyspace_id, file_id))
        .copied()
        .unwrap();
    let commit_seq = CommitSeq::from_raw(metadata.next_commit_seq);
    drop(metadata);
    let visible = AppendVisiblePublish {
        record_id: AppendPublishTicketId::from_raw(900),
        commit_seq,
        keyspace_id,
        file_id,
        base_writer_epoch,
        writer_epoch: stream.writer_epoch,
        base_file_version: base_head.version,
        new_file_version: FileVersion::from_raw(base_head.version.raw() + 1),
        old_size: base_head.size,
        new_size: ticket.range.end_exclusive().unwrap(),
        publish_through: ticket.range.end_exclusive().unwrap(),
        run_extents,
    };
    visible.validate().unwrap();
    store
        .durable
        .append_append_visible_publish_journal_record(&visible)
        .unwrap();
    assert_eq!(native_publish_journal_commit_count(&root, 0), 0);
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert_eq!(native_publish_journal_commit_count(&root, 0), 0);
    let reopened_head = reopened
        .metadata()
        .get_file_head(keyspace_id, file_id)
        .unwrap();
    assert_eq!(reopened_head.version, visible.new_file_version);
    assert_eq!(reopened_head.size, visible.new_size);
    assert_eq!(reopened_head.latest_commit, visible.commit_seq);
    assert_eq!(
        reopened
            .metadata()
            .state_inner()
            .unwrap()
            .file_writer_epochs
            .get(&(keyspace_id, file_id)),
        Some(&visible.writer_epoch)
    );
    assert_eq!(
        file_run_extents(&reopened.metadata(), keyspace_id, file_id),
        visible.run_extents
    );
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    reopened.persist_now().unwrap();
    drop(reopened);

    let reopened_again = DurableCoordinator::open(&root, cfg).unwrap();
    let reopened_again_head = reopened_again
        .metadata()
        .get_file_head(keyspace_id, file_id)
        .unwrap();
    assert_eq!(reopened_again_head.version, visible.new_file_version);
    assert_eq!(reopened_again_head.size, visible.new_size);
    assert_eq!(reopened_again_head.latest_commit, visible.commit_seq);
    let mut bytes_after_fold = vec![0; payload.len()];
    reopened_again
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes_after_fold,
        )
        .unwrap();
    assert_eq!(bytes_after_fold, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_journal_can_live_outside_store_root() {
    let root = durable_temp_dir("append-visible-publish-split-root");
    let journal_root = durable_temp_dir("append-visible-publish-split-journal");
    let journal = journal_root.join("append-visible-publish.journal");
    let cfg = config();
    let store =
        DurableCoordinator::open_with_append_visible_publish_journal(&root, cfg, &journal).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let payload = repeated_blocks(1, 73);
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &payload,
        WriteDurability::Flushed,
    )
    .unwrap();
    let run_extents = file_run_extents(&store.metadata(), keyspace_id, file_id);
    assert_eq!(run_extents.len(), 1);
    let lane_journal =
        append_visible_publish_journal_path_for_test(&journal_root, keyspace_id, file_id);
    assert!(lane_journal.exists());
    assert!(!journal.exists());
    assert!(!root.join("append-visible-publish.journal").exists());
    assert_eq!(
        load_append_visible_publish_journal_records(&lane_journal)
            .unwrap()
            .len(),
        1
    );
    drop(store);

    let reopened =
        DurableCoordinator::open_with_append_visible_publish_journal(&root, cfg, &journal).unwrap();
    assert_eq!(
        file_run_extents(&reopened.metadata(), keyspace_id, file_id),
        run_extents
    );
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(journal_root);
}

#[test]
fn durable_append_visible_publish_replays_unpersisted_writer_epoch_skip() {
    let root = durable_temp_dir("append-visible-publish-writer-epoch-skip");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let stale = store.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(stale.writer_epoch, WriterEpoch::from_raw(1));
    let fresh = store.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(fresh.writer_epoch, WriterEpoch::from_raw(2));
    let payload = repeated_blocks(1, 55);
    store
        .append_stream(&fresh, &payload, WriteDurability::Acknowledged)
        .unwrap();
    store
        .publish_append_stream(&fresh, payload.len() as u64)
        .unwrap();
    assert_eq!(native_publish_journal_commit_count(&root, 0), 0);
    let records = load_append_visible_publish_journal_records(
        &append_visible_publish_journal_path_for_test(&root, keyspace_id, file_id),
    )
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].base_writer_epoch, WriterEpoch::from_raw(0));
    assert_eq!(records[0].writer_epoch, WriterEpoch::from_raw(2));
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; payload.len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, payload.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, payload);
    assert_eq!(
        reopened
            .metadata()
            .state_inner()
            .unwrap()
            .file_writer_epochs
            .get(&(keyspace_id, file_id)),
        Some(&WriterEpoch::from_raw(2))
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_visible_publish_replays_independent_catalog_lane_journals() {
    let root = durable_temp_dir("append-visible-publish-independent-lanes");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let first_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("first".to_string()),
                },
            },
        )
        .unwrap();
    let second_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("second".to_string()),
                },
            },
        )
        .unwrap();
    assert_ne!(
        append_publish_lane_index_for_test(first_file),
        append_publish_lane_index_for_test(second_file)
    );

    let first_payload = repeated_blocks(1, 61);
    let second_payload = repeated_blocks(1, 62);
    append_durable_store_once(
        &store,
        keyspace_id,
        first_file,
        &first_payload,
        WriteDurability::Flushed,
    )
    .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        second_file,
        &second_payload,
        WriteDurability::Flushed,
    )
    .unwrap();
    let first_journal =
        append_visible_publish_journal_path_for_test(&root, keyspace_id, first_file);
    let second_journal =
        append_visible_publish_journal_path_for_test(&root, keyspace_id, second_file);
    assert_ne!(first_journal, second_journal);
    assert_eq!(
        load_append_visible_publish_journal_records(&first_journal)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        load_append_visible_publish_journal_records(&second_journal)
            .unwrap()
            .len(),
        1
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut first = vec![0; first_payload.len()];
    reopened
        .read_file(
            keyspace_id,
            first_file,
            ByteRange::new(0, first_payload.len() as u64),
            &mut first,
        )
        .unwrap();
    assert_eq!(first, first_payload);
    let mut second = vec![0; second_payload.len()];
    reopened
        .read_file(
            keyspace_id,
            second_file,
            ByteRange::new(0, second_payload.len() as u64),
            &mut second,
        )
        .unwrap();
    assert_eq!(second, second_payload);
    reopened
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_failed_root_publish_leaves_pending_orphan_and_does_not_reuse_id() {
    let root = durable_temp_dir("failed-root-publish-pending-orphan");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let root_after_first_publish = metadata_file_snapshot(&root);
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    let committed_segments = device_segment_ids(&store.metadata(), device_id);
    assert_eq!(
        committed_segments,
        vec![SegmentId::from_raw(1), SegmentId::from_raw(2)]
    );
    let orphan_intent = store
        .segment_catalog()
        .intent_for_segment(SegmentId::from_raw(2))
        .unwrap()
        .write_intent;
    drop(store);

    restore_metadata_file_snapshot(&root, &root_after_first_publish);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert_eq!(
        device_segment_ids(&reopened.metadata(), device_id),
        vec![SegmentId::from_raw(1)]
    );
    let mut bytes = vec![99; 2 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 2 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&bytes[4096..8192], vec![0; 4096].as_slice());
    assert_eq!(
        reopened
            .segment_catalog()
            .state(SegmentId::from_raw(2))
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );

    let report = reopened
        .run_storage_node_custodian(&BTreeSet::from([orphan_intent]))
        .unwrap();
    assert_eq!(report.orphan_segments, vec![SegmentId::from_raw(2)]);
    assert!(
        !reopened
            .segment_store()
            .contains_segment(SegmentId::from_raw(2))
            .unwrap()
    );
    reopened
        .write_device(
            device_id,
            8192,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert_eq!(
        device_segment_ids(&reopened.metadata(), device_id),
        vec![SegmentId::from_raw(1), SegmentId::from_raw(3)]
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_missing_row_native_head_root() {
    let root = durable_temp_dir("row-native-missing-root");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 12),
            WriteDurability::Flushed,
        )
        .unwrap();
    store.checkpoint(device_id).unwrap();
    let head = store.metadata().get_head(device_id).unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "DELETE FROM metadata_nodes WHERE node_id = ?1",
        params![head.shard_roots[0].raw().to_string()],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_missing_row_native_catalog_entry() {
    let root = durable_temp_dir("row-native-missing-catalog-entry");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 14),
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    drop(store);

    let conn = node_catalog_conn(&root, placement.storage_node);
    conn.execute(
        "DELETE FROM segment_catalog_entries WHERE segment_id = ?1",
        params![segment_id.raw().to_string()],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_corrupt_row_native_payload() {
    let root = durable_temp_dir("row-native-corrupt-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 15),
            WriteDurability::Flushed,
        )
        .unwrap();
    store.checkpoint(device_id).unwrap();
    let head = store.metadata().get_head(device_id).unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "UPDATE metadata_nodes SET payload = x'ff' WHERE node_id = ?1",
        params![head.shard_roots[0].raw().to_string()],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_missing_row_native_timeline_root() {
    let root = durable_temp_dir("row-native-missing-timeline-root");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 16),
            WriteDurability::Flushed,
        )
        .unwrap();
    let second = store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 17),
            WriteDurability::Flushed,
        )
        .unwrap();
    store.checkpoint(device_id).unwrap();
    let current = store.metadata().get_head(device_id).unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    let payload: Vec<u8> = conn
        .query_row(
            "SELECT payload FROM shard_commits WHERE commit_seq = ?1 LIMIT 1",
            params![u64_to_i64(second.commit_seq.raw()).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    let commit: ShardCommit = decode_row(&payload).unwrap();
    assert_ne!(commit.old_root, current.shard_roots[0]);
    conn.execute(
        "DELETE FROM metadata_nodes WHERE node_id = ?1",
        params![commit.old_root.raw().to_string()],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_row_native_cursor_behind_rows() {
    let root = durable_temp_dir("row-native-stale-cursor");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "UPDATE store_meta SET next_device_id = '1' WHERE id = 1",
        [],
    )
    .unwrap();
    drop(conn);

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_advances_write_intent_cursor_from_node_catalog_rows() {
    let root = durable_temp_dir("row-native-stale-write-intent-cursor");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 18),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "UPDATE store_meta
             SET next_write_intent = '1'
             WHERE id = 1",
        [],
    )
    .unwrap();
    drop(conn);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 20),
            WriteDurability::Flushed,
        )
        .unwrap();
    let intent = store
        .segment_catalog()
        .intent_for_segment(SegmentId::from_raw(2))
        .unwrap();
    assert_eq!(intent.write_intent, WriteIntentId::from_raw(2));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_advances_placement_cursor_from_node_catalog_rows() {
    let root = durable_temp_dir("row-native-stale-placement-cursor");
    let cfg = config();
    let second_node = StorageNodeId::from_raw(2);
    let store = DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
        &root,
        cfg,
        vec![cfg.storage_node, second_node],
        DurableDataLogPolicy::default(),
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 19),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.execute(
        "UPDATE store_meta
             SET next_placement_index = 0
             WHERE id = 1",
        [],
    )
    .unwrap();
    drop(conn);

    let store = DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
        &root,
        cfg,
        vec![cfg.storage_node, second_node],
        DurableDataLogPolicy::default(),
    )
    .unwrap();
    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 20),
            WriteDurability::Flushed,
        )
        .unwrap();
    let placement = store
        .durable
        .placement_for_test(SegmentId::from_raw(2))
        .unwrap();
    assert_eq!(placement.storage_node, second_node);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn data_log_checksum_uses_crc32c_golden_value() {
    assert_eq!(data_log_checksum(b"123456789"), 0xe306_9283);
}

#[test]
fn data_log_records_distinguish_segments_from_append_runs() {
    let segment_bytes = repeated_blocks(1, 31);
    let segment_record = encode_data_log_record(
        SegmentId::from_raw(10),
        segment_payload_integrity(PayloadIntegrity::Verified, &segment_bytes),
        &segment_bytes,
    )
    .unwrap();
    let segment = decode_segment_data_log_record(&segment_record).unwrap();
    assert_eq!(segment.segment_id, SegmentId::from_raw(10));
    assert_eq!(segment.bytes, segment_bytes);
    assert!(decode_append_run_data_log_record(&segment_record).is_err());

    let run_bytes = repeated_blocks(1, 32);
    let run_record = encode_append_run_data_log_record(
        AppendRunId::from_raw(11),
        segment_payload_integrity(PayloadIntegrity::Verified, &run_bytes),
        &run_bytes,
    )
    .unwrap();
    let run = decode_append_run_data_log_record(&run_record).unwrap();
    assert_eq!(run.run_id, AppendRunId::from_raw(11));
    assert_eq!(
        run.integrity,
        segment_payload_integrity(PayloadIntegrity::Verified, &run_bytes)
    );
    assert_eq!(run.bytes, run_bytes);
    assert!(decode_segment_data_log_record(&run_record).is_err());
}

#[test]
fn durable_append_run_payload_writes_without_segment_placement() {
    let root = durable_temp_dir("append-run-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let bytes = repeated_blocks(1, 33);
    let payload = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(77),
        storage_node: cfg.storage_node,
        stream_id: AppendStreamId::from_raw(88),
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 4096,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![bytes.as_slice()],
        background_sync_step_bytes: None,
    };

    let (run, pending, profile) = store
        .durable
        .write_append_run_payload_chunks_unsynced(payload, None)
        .unwrap();
    assert_eq!(run.run_id, AppendRunId::from_raw(77));
    assert_eq!(run.file_offset_start, 4096);
    assert_eq!(run.payload_len, 4096);
    assert_eq!(pending.placements.len(), 0);
    assert_eq!(pending.logs.len(), 1);
    assert!(profile.write_nanos > 0);
    let mut read = vec![0; bytes.len()];
    store
        .durable
        .read_append_run_source_payload(
            run.storage_node,
            run.log_id,
            ByteRange::new(run.log_payload_offset, run.payload_len),
            run.integrity,
            ReadVerification::Default,
            &mut read,
        )
        .unwrap();
    assert_eq!(read, bytes);
    assert!(store.local.segment_ids().unwrap().is_empty());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_run_payloads_share_node_owned_active_log_across_streams() {
    let root = durable_temp_dir("append-run-node-owned-active-log");
    let cfg = config();
    let store = DurableCoordinator::open_with_data_log_policy(
        &root,
        cfg,
        DurableDataLogPolicy {
            target_data_log_bytes: 1024 * 1024,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();

    let first_bytes = repeated_blocks(1, 11);
    let second_bytes = repeated_blocks(1, 12);
    let first = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(77),
        storage_node: cfg.storage_node,
        stream_id: AppendStreamId::from_raw(88),
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 0,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![first_bytes.as_slice()],
        background_sync_step_bytes: None,
    };
    let second = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(78),
        storage_node: cfg.storage_node,
        stream_id: AppendStreamId::from_raw(89),
        writer_epoch: WriterEpoch::from_raw(4),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(6),
        file_offset_start: 0,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![second_bytes.as_slice()],
        background_sync_step_bytes: None,
    };

    let (first_run, first_pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(first, None)
        .unwrap();
    let (second_run, second_pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(second, None)
        .unwrap();

    assert_eq!(first_run.storage_node, second_run.storage_node);
    assert_eq!(first_run.log_id, second_run.log_id);
    assert_eq!(first_run.log_payload_offset, 0);
    assert_eq!(second_run.log_payload_offset, first_run.payload_len);
    assert_eq!(first_pending.logs.len(), 1);
    assert_eq!(second_pending.logs.len(), 1);
    assert_eq!(
        first_pending.logs.keys().next(),
        second_pending.logs.keys().next()
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_store_selects_append_run_manifests_and_sealed_refs() {
    let root = durable_temp_dir("append-run-manifest-selection");
    let cfg = config();
    let store = DurableCoordinator::open_with_data_log_policy(
        &root,
        cfg,
        DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();

    let stream_id = AppendStreamId::from_raw(88);
    let first_bytes = repeated_blocks(1, 11);
    let second_bytes = repeated_blocks(1, 12);
    let first_payload = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(77),
        storage_node: cfg.storage_node,
        stream_id,
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 0,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![first_bytes.as_slice()],
        background_sync_step_bytes: None,
    };
    let (first_run, first_pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(first_payload, None)
        .unwrap();
    let second_payload = DurableAppendRunChunkPayload {
        run_id: AppendRunId::from_raw(78),
        storage_node: cfg.storage_node,
        stream_id,
        writer_epoch: WriterEpoch::from_raw(3),
        keyspace_id: KeyspaceId::from_raw(4),
        file_id: FileId::from_raw(5),
        file_offset_start: 4096,
        payload_integrity: PayloadIntegrity::Verified,
        chunks: vec![second_bytes.as_slice()],
        background_sync_step_bytes: None,
    };
    let (second_run, second_pending, _) = store
        .durable
        .write_append_run_payload_chunks_unsynced(second_payload, Some(&first_pending))
        .unwrap();
    assert_ne!(first_run.log_id, second_run.log_id);

    let mut lane_pending = first_pending;
    lane_pending.merge(second_pending);
    let selected = BTreeSet::from([
        DurableDataLogRef {
            storage_node: first_run.storage_node,
            log_id: first_run.log_id,
        },
        DurableDataLogRef {
            storage_node: second_run.storage_node,
            log_id: second_run.log_id,
        },
    ]);
    let manifests = store
        .durable
        .pending_append_run_manifests_for_log_refs(&selected, Some(&lane_pending))
        .unwrap();
    assert_eq!(manifests.logs.len(), 2);
    assert_eq!(manifests.sealed_logs.len(), 1);
    assert!(manifests.sealed_logs.contains(&DurableDataLogRef {
        storage_node: first_run.storage_node,
        log_id: first_run.log_id,
    }));
    assert!(manifests.placements.is_empty());

    let fallback = store
        .durable
        .pending_append_run_manifests_for_log_refs(&selected, None)
        .unwrap();
    assert_eq!(fallback.logs.len(), 2);
    assert!(fallback.sealed_logs.is_empty());
    assert!(fallback.placements.is_empty());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_batched_flush_persists_many_segments_in_one_data_log() {
    let root = durable_temp_dir("batched-flush-one-log");
    let cfg = config();
    let store = DurableCoordinator::open_with_data_log_policy(
        &root,
        cfg,
        DurableDataLogPolicy {
            target_data_log_bytes: 1024 * 1024,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 32,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    for block in 0..32 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, block as u8),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }
    store.flush_device(device_id).unwrap();

    let rows = store.durable.data_log_rows_for_test().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        store.durable.data_log_states_for_test().unwrap()[0].1,
        "active"
    );

    drop(store);
    let reopened = DurableCoordinator::open_with_data_log_policy(
        &root,
        cfg,
        DurableDataLogPolicy {
            target_data_log_bytes: 1024 * 1024,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();
    let mut bytes = vec![0; 32 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 32 * 4096), &mut bytes)
        .unwrap();
    for block in 0..32 {
        assert_eq!(
            &bytes[block * 4096..(block + 1) * 4096],
            repeated_blocks(1, block as u8).as_slice()
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_batched_flush_rolls_logs_and_reopens_every_segment() {
    let root = durable_temp_dir("batched-flush-rolls");
    let cfg = config();
    let policy = DurableDataLogPolicy {
        target_data_log_bytes: 4096,
        file_sync_fanout: 4,
        min_reclaimable_ratio_ppm: 1,
        min_reclaimable_bytes: 1,
        max_compaction_copy_bytes: u64::MAX,
    };
    let store = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    for block in 0..4 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }
    store.flush_device(device_id).unwrap();

    let states = store.durable.data_log_states_for_test().unwrap();
    assert_eq!(states.len(), 4);
    assert_eq!(
        states
            .iter()
            .filter(|(_, state)| state.as_str() == "sealed")
            .count(),
        3
    );
    assert_eq!(
        states
            .iter()
            .filter(|(_, state)| state.as_str() == "active")
            .count(),
        1
    );

    drop(store);
    let reopened = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let mut bytes = vec![0; 4 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4 * 4096), &mut bytes)
        .unwrap();
    for block in 0..4 {
        assert_eq!(
            &bytes[block * 4096..(block + 1) * 4096],
            repeated_blocks(1, (block + 1) as u8).as_slice()
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_data_log_ignores_unplaced_tail_records() {
    let root = durable_temp_dir("data-log-unplaced-tail");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let data_log = data_log_path(&root.join("data"), cfg.storage_node, 1);
    let unplaced = repeated_blocks(1, 9);
    OpenOptions::new()
        .append(true)
        .open(&data_log)
        .unwrap()
        .write_all(
            &encode_data_log_record(
                SegmentId::from_raw(999),
                segment_payload_integrity(PayloadIntegrity::Verified, &unplaced),
                &unplaced,
            )
            .unwrap(),
        )
        .unwrap();
    let torn_payload = repeated_blocks(1, 10);
    let torn = encode_data_log_record(
        SegmentId::from_raw(1000),
        segment_payload_integrity(PayloadIntegrity::Verified, &torn_payload),
        &torn_payload,
    )
    .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&data_log)
        .unwrap()
        .write_all(&torn[..torn.len() / 2])
        .unwrap();

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 3));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_data_log_rejects_current_payload_corruption() {
    let root = durable_temp_dir("data-log-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    drop(store);

    let path = data_log_path(
        &root.join("data"),
        placement.storage_node,
        placement.data_log_id,
    );
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(placement.payload_offset))
        .unwrap();
    file.write_all(&[0xff]).unwrap();
    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_data_log_rejects_current_checksum_corruption() {
    let root = durable_temp_dir("data-log-checksum-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    drop(store);

    let path = data_log_path(
        &root.join("data"),
        placement.storage_node,
        placement.data_log_id,
    );
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let checksum_offset =
        placement.record_offset + u64::try_from(DATA_LOG_CHECKSUM_OFFSET).unwrap();
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    let mut byte = [0; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(checksum_offset)).unwrap();
    file.write_all(&[!byte[0]]).unwrap();
    file.sync_data().unwrap();

    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_read_device_rejects_in_memory_payload_corruption_by_default() {
    let root = durable_temp_dir("read-device-memory-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    corrupt_in_memory_segment_payload(&store, &placement, segment_id);

    let mut bytes = vec![0; 4096];
    assert!(
        store
            .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_read_file_rejects_in_memory_payload_corruption_by_default() {
    let root = durable_temp_dir("read-file-memory-corruption");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, repeated_blocks(1, 5))],
            WriteDurability::Flushed,
        )
        .unwrap();
    let segment_id = file_segment_ids(&store.metadata(), keyspace_id, file_id)
        .into_iter()
        .next()
        .unwrap();
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    corrupt_in_memory_segment_payload(&store, &placement, segment_id);

    let mut bytes = vec![0; 4096];
    assert!(
        store
            .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_unchecked_block_payload_allows_default_read_but_not_required_verification() {
    let root = durable_temp_dir("unchecked-block-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let payload = repeated_blocks(1, 44);
    store
        .write_device_with_integrity(
            device_id,
            0,
            &payload,
            WriteDurability::Flushed,
            PayloadIntegrity::Unchecked,
        )
        .unwrap();

    let mut bytes = vec![0; 4096];
    store
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, payload);
    assert!(
        store
            .read_device_with_verification(
                device_id,
                ByteRange::new(0, 4096),
                &mut bytes,
                ReadVerification::RequireVerified,
            )
            .is_err()
    );

    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    corrupt_in_memory_segment_payload(&store, &placement, segment_id);
    store
        .read_device_with_verification(
            device_id,
            ByteRange::new(0, 4096),
            &mut bytes,
            ReadVerification::Skip,
        )
        .unwrap();
    assert_ne!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_unchecked_native_payload_allows_default_read_but_not_required_verification() {
    let root = durable_temp_dir("unchecked-native-payload");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let payload = repeated_blocks(1, 45);
    store
        .commit_file_batch_with_integrity(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, payload.clone())],
            WriteDurability::Flushed,
            PayloadIntegrity::Unchecked,
        )
        .unwrap();

    let mut bytes = vec![0; 4096];
    store
        .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, payload);
    assert!(
        store
            .read_file_with_verification(
                keyspace_id,
                file_id,
                ByteRange::new(0, 4096),
                &mut bytes,
                ReadVerification::RequireVerified,
            )
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_read_plan_zero_fills_sparse_ranges() {
    let root = durable_temp_dir("read-plan-sparse-block");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let payload = repeated_blocks(1, 17);
    store
        .write_device(device_id, 4096, &payload, WriteDurability::Flushed)
        .unwrap();

    let (plan, _) = store
        .local
        .resolve_block_read(device_id, ByteRange::new(0, 3 * 4096))
        .unwrap();
    assert_eq!(plan.logical_len, 3 * 4096);
    assert_eq!(plan.extents.len(), 3);
    assert!(matches!(plan.extents[0].source, ReadSource::Zero));
    assert!(matches!(plan.extents[1].source, ReadSource::Segment { .. }));
    assert!(matches!(plan.extents[2].source, ReadSource::Zero));

    let mut bytes = vec![0xff; 3 * 4096];
    store
        .read_device(device_id, ByteRange::new(0, 3 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[..4096], &[0; 4096]);
    assert_eq!(&bytes[4096..8192], payload.as_slice());
    assert_eq!(&bytes[8192..], &[0; 4096]);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_read_plan_assembles_multiple_segment_extents() {
    let root = durable_temp_dir("read-plan-multi-block");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let first = repeated_blocks(1, 21);
    let second = repeated_blocks(1, 22);
    store
        .write_device(device_id, 0, &first, WriteDurability::Flushed)
        .unwrap();
    store
        .write_device(device_id, 4096, &second, WriteDurability::Flushed)
        .unwrap();

    let (plan, _) = store
        .local
        .resolve_block_read(device_id, ByteRange::new(0, 2 * 4096))
        .unwrap();
    assert_eq!(
        plan.extents
            .iter()
            .filter(|extent| matches!(extent.source, ReadSource::Segment { .. }))
            .count(),
        2
    );

    let mut bytes = vec![0; 2 * 4096];
    store
        .read_device(device_id, ByteRange::new(0, 2 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[..4096], first.as_slice());
    assert_eq!(&bytes[4096..], second.as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_native_read_plan_assembles_segments_and_published_append_runs() {
    let root = durable_temp_dir("read-plan-native-append-run");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let segment_bytes = repeated_blocks(1, 31);
    let run_bytes = repeated_blocks(1, 32);
    store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, segment_bytes.clone())],
            WriteDurability::Flushed,
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &run_bytes,
        WriteDurability::Flushed,
    )
    .unwrap();

    let (plan, _) = store
        .local
        .resolve_file_read(keyspace_id, file_id, ByteRange::new(0, 2 * 4096))
        .unwrap();
    assert!(
        plan.extents
            .iter()
            .any(|extent| matches!(extent.source, ReadSource::Segment { .. }))
    );
    assert!(
        plan.extents
            .iter()
            .any(|extent| matches!(extent.source, ReadSource::AppendRun { .. }))
    );

    let mut bytes = vec![0; 2 * 4096];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, 2 * 4096),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(&bytes[..4096], segment_bytes.as_slice());
    assert_eq!(&bytes[4096..], run_bytes.as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_native_read_ignores_unpublished_append_stream_bytes() {
    let root = durable_temp_dir("read-plan-unpublished-stream");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let payload = repeated_blocks(1, 41);
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, &payload, WriteDurability::Acknowledged)
        .unwrap();

    let mut bytes = vec![0; 4096];
    assert!(
        store
            .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
            .is_err()
    );

    store.publish_append_stream(&stream, 4096).unwrap();
    store
        .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_block_read_fails_when_storage_node_catalog_placement_is_missing() {
    let root = durable_temp_dir("read-plan-missing-placement");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let payload = repeated_blocks(1, 51);
    store
        .write_device(device_id, 0, &payload, WriteDurability::Flushed)
        .unwrap();

    let segment_id = first_device_segment(&store, device_id);
    let placement = store.durable.placement_for_test(segment_id).unwrap();
    let catalog = store
        .local
        .segment_catalog_for_node(placement.storage_node)
        .unwrap();
    lock(&catalog.inner).unwrap().entries.remove(&segment_id);

    let mut bytes = vec![0; 4096];
    assert!(
        store
            .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_read_profiling_is_opt_in_and_records_block_phase_shape() {
    let root = durable_temp_dir("read-profile-block");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let payload = repeated_blocks(1, 61);
    store
        .write_device(device_id, 0, &payload, WriteDurability::Flushed)
        .unwrap();

    let mut bytes = vec![0; 4096];
    store
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert!(store.drain_read_profiles(8).unwrap().is_empty());

    store.enable_read_profiling(8).unwrap();
    store
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    let profiles = store.drain_read_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    let profile = profiles[0];
    assert_eq!(profile.sequence, 1);
    assert_eq!(profile.logical_bytes, 4096);
    assert_eq!(profile.extent_count, 1);
    assert_eq!(profile.segment_extent_count, 1);
    assert_eq!(profile.zero_extent_count, 0);
    assert_eq!(profile.append_run_extent_count, 0);
    assert_eq!(profile.storage_node_count, 1);
    assert!(profile.total_nanos >= profile.metadata_resolve_nanos);
    assert!(profile.metadata_resolve_nanos >= profile.metadata_lock_wait_nanos);
    assert!(profile.metadata_resolve_nanos >= profile.metadata_tree_walk_nanos);
    assert!(profile.metadata_resolve_nanos >= profile.metadata_placement_lookup_nanos);
    assert!(profile.assemble_nanos >= profile.storage_node_read_nanos);
    assert!(profile.storage_node_read_nanos > 0);
    assert!(profile.storage_node_payload_read_nanos > 0);
    assert!(profile.verification_nanos > 0);
    assert!(profile.copy_nanos > 0);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_read_profiling_counts_append_run_sources() {
    let root = durable_temp_dir("read-profile-append-run");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let segment_bytes = repeated_blocks(1, 62);
    let run_bytes = repeated_blocks(1, 63);
    store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[FileBatchWrite::new(0, segment_bytes)],
            WriteDurability::Flushed,
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        &run_bytes,
        WriteDurability::Flushed,
    )
    .unwrap();

    store.enable_read_profiling(8).unwrap();
    let mut bytes = vec![0; 2 * 4096];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, 2 * 4096),
            &mut bytes,
        )
        .unwrap();
    let profiles = store.drain_read_profiles(8).unwrap();
    assert_eq!(profiles.len(), 1);
    let profile = profiles[0];
    assert_eq!(profile.logical_bytes, 2 * 4096);
    assert_eq!(profile.segment_extent_count, 1);
    assert_eq!(profile.append_run_extent_count, 1);
    assert_eq!(profile.storage_node_count, 1);
    assert!(profile.metadata_resolve_nanos >= profile.metadata_tree_walk_nanos);
    assert!(profile.metadata_resolve_nanos >= profile.metadata_placement_lookup_nanos);
    assert!(profile.storage_node_read_nanos > 0);
    assert!(profile.storage_node_payload_read_nanos > 0);
    assert!(profile.verification_nanos > 0);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_missing_data_log_for_current_placement() {
    let root = durable_temp_dir("missing-data-log");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    fs::remove_file(data_log_path(&root.join("data"), cfg.storage_node, 1)).unwrap();
    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_sqlite_rejects_missing_node_catalog_for_current_metadata() {
    let root = durable_temp_dir("missing-node-catalog");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);
    fs::remove_file(node_catalog_path(&root.join("data"), cfg.storage_node)).unwrap();
    assert!(DurableCoordinator::open(&root, cfg).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_data_log_compaction_relocates_partial_logs_and_deletes_dead_logs() {
    let root = durable_temp_dir("data-log-compaction");
    let cfg = config();
    let policy = DurableDataLogPolicy {
        target_data_log_bytes: 9 * 1024,
        file_sync_fanout: 4,
        min_reclaimable_ratio_ppm: 1,
        min_reclaimable_bytes: 1,
        max_compaction_copy_bytes: u64::MAX,
    };
    let store = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            8192,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    let before = store.durable.data_log_rows_for_test().unwrap();
    assert!(
        before
            .iter()
            .any(|log| log.log_id == 1 && log.dead_bytes > 0)
    );

    let report = store
        .compact_data_logs(DurableDataLogPolicy::compact_everything_for_test())
        .unwrap();
    let first_log = DurableDataLogRef {
        storage_node: cfg.storage_node,
        log_id: 1,
    };
    assert!(report.relocated_logs.contains(&first_log));
    assert!(!data_log_path(&root.join("data"), cfg.storage_node, first_log.log_id).exists());

    drop(store);
    let reopened = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let mut bytes = vec![0; 3 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 3 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[0..4096], repeated_blocks(1, 4).as_slice());
    assert_eq!(&bytes[4096..8192], repeated_blocks(1, 2).as_slice());
    assert_eq!(&bytes[8192..12288], repeated_blocks(1, 3).as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_data_log_compaction_honors_pitr_retention_until_gc_releases_segment() {
    let root = durable_temp_dir("data-log-pitr-retention");
    let cfg = config();
    let policy = DurableDataLogPolicy {
        target_data_log_bytes: 4096,
        file_sync_fanout: 4,
        min_reclaimable_ratio_ppm: 1,
        min_reclaimable_bytes: 1,
        max_compaction_copy_bytes: u64::MAX,
    };
    let store = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    let old_segment_id = first_device_segment(&store, device_id);
    let old_placement = store.durable.placement_for_test(old_segment_id).unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert_eq!(block_delta_commit_count(&root), 2);

    let retained = RetentionPolicy::expire_deleted_immediately().with_pitr_grace_commits(10);
    store.run_metadata_custodian(retained).unwrap();
    assert_eq!(
        block_delta_commit_count(&root),
        0,
        "durable GC should fold block delta rows before sweeping metadata"
    );
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    let retained_report = store
        .compact_data_logs(DurableDataLogPolicy::compact_everything_for_test())
        .unwrap();
    let old_log = DurableDataLogRef {
        storage_node: old_placement.storage_node,
        log_id: old_placement.data_log_id,
    };
    assert!(!retained_report.deleted_logs.contains(&old_log));
    assert!(data_log_path(&root.join("data"), old_log.storage_node, old_log.log_id).exists());
    assert!(store.durable.placement_for_test(old_segment_id).is_ok());

    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    let expired_report = store
        .compact_data_logs(DurableDataLogPolicy::compact_everything_for_test())
        .unwrap();
    assert!(expired_report.deleted_logs.contains(&old_log));
    assert!(!data_log_path(&root.join("data"), old_log.storage_node, old_log.log_id).exists());
    assert!(store.durable.placement_for_test(old_segment_id).is_err());
    drop(store);

    let reopened = DurableCoordinator::open_with_data_log_policy(&root, cfg, policy).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 2));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn maintenance_scheduler_is_deterministic_and_bounded() {
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy::compact_everything_for_test(),
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: 1024 * 1024,
        max_sealed_logs: 16,
        max_reclaimable_debt_bytes: 1024 * 1024,
        compaction_copy_budget_per_tick: 4096,
        max_sqlite_wal_bytes: 1024 * 1024,
        max_logs_scanned_per_tick: 2,
        max_concurrent_compaction_jobs: 1,
    };
    let scheduler = MaintenanceScheduler::new(policy).unwrap();
    let node = StorageNodeId::from_raw(7);
    let observation = MaintenanceObservation {
        nodes: vec![MaintenanceNodeObservation {
            storage_node: node,
            active_log_bytes: 4096,
            sealed_log_count: 3,
            dirty_bytes: 12_288,
            reclaimable_bytes: 12_288,
            logs: (1..=3)
                .map(|log_id| MaintenanceDataLogObservation {
                    log_ref: DurableDataLogRef {
                        storage_node: node,
                        log_id,
                    },
                    total_bytes: 8192,
                    live_bytes: 2048,
                    dead_bytes: 6144,
                    reclaimable_bytes: 6144,
                })
                .collect(),
        }],
        sqlite_wal_bytes: 0,
        pending_custodian_releases: 0,
        pitr_retention_floor: None,
        recent_write_bytes: 4096,
        recent_flushed_write_bytes: 4096,
        compaction_cursor: Some(DurableDataLogRef {
            storage_node: node,
            log_id: 1,
        }),
    };

    let first = scheduler.step(&observation);
    let second = scheduler.step(&observation);
    assert_eq!(first, second);
    assert!(matches!(first.admission, WriteAdmission::AcceptAndSchedule));
    assert_eq!(first.diagnostics.selected_logs.len(), 2);
    assert_eq!(
        first.diagnostics.selected_logs[0],
        DurableDataLogRef {
            storage_node: node,
            log_id: 2
        }
    );
}

#[test]
fn idle_maintenance_tick_does_not_persist_cursor_or_compact() {
    let root = durable_temp_dir("idle-maintenance-no-persist");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy::compact_everything_for_test(),
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    assert_eq!(store.observe_maintenance().unwrap().compaction_cursor, None);

    let report = store.run_maintenance_tick().unwrap();
    assert!(report.plan.commands.is_empty());
    assert_eq!(report.plan.next_cursor, None);
    assert!(report.compaction.deleted_logs.is_empty());
    assert!(report.compaction.relocated_logs.is_empty());
    assert_eq!(report.compaction.bytes_copied, 0);
    assert_eq!(report.compaction.bytes_deleted, 0);
    assert_eq!(store.observe_maintenance().unwrap().compaction_cursor, None);
    drop(store);

    let reopened = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    assert_eq!(
        reopened.observe_maintenance().unwrap().compaction_cursor,
        None
    );
    drop(reopened);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn maintenance_planning_skips_wal_stats_only_when_policy_cannot_use_them() {
    let root = durable_temp_dir("maintenance-wal-skip");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy::compact_everything_for_test(),
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let exact_wal_bytes = store.durable.sqlite_wal_bytes().unwrap();
    let observation = store.observe_maintenance().unwrap();
    assert_eq!(observation.sqlite_wal_bytes, exact_wal_bytes);
    let plan = store.plan_maintenance().unwrap();
    assert_eq!(plan.diagnostics.sqlite_wal_bytes, 0);
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_data_logs_are_scoped_to_storage_nodes_and_reopen() {
    let root = durable_temp_dir("node-scoped-data-logs");
    let mut cfg = config();
    cfg.storage_node = StorageNodeId::from_raw(1);
    let nodes = vec![
        StorageNodeId::from_raw(1),
        StorageNodeId::from_raw(2),
        StorageNodeId::from_raw(3),
    ];
    let store = DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
        &root,
        cfg,
        nodes.clone(),
        DurableDataLogPolicy {
            target_data_log_bytes: 1024 * 1024,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
    )
    .unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 3,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    for block in 0..3 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Flushed,
            )
            .unwrap();
    }

    let rows = store.durable.data_log_rows_for_test().unwrap();
    let row_nodes: BTreeSet<_> = rows.iter().map(|row| row.storage_node).collect();
    assert_eq!(row_nodes.len(), 3);
    for node in &nodes {
        assert!(node_data_log_dir(&root.join("data"), *node).exists());
        assert!(data_log_path(&root.join("data"), *node, 1).exists());
    }

    drop(store);
    let reopened = DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
        &root,
        cfg,
        nodes,
        DurableDataLogPolicy::default(),
    )
    .unwrap();
    let mut bytes = vec![0; 3 * 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 3 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&bytes[4096..8192], repeated_blocks(1, 2).as_slice());
    assert_eq!(&bytes[8192..12288], repeated_blocks(1, 3).as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_data_log_policy_rejects_zero_file_sync_fanout() {
    let root = durable_temp_dir("data-log-zero-sync-fanout");
    let policy = DurableDataLogPolicy {
        file_sync_fanout: 0,
        ..DurableDataLogPolicy::default()
    };
    assert!(DurableCoordinator::open_with_data_log_policy(&root, config(), policy).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn maintenance_throttles_writes_until_manual_tick_reclaims_debt() {
    let root = durable_temp_dir("maintenance-throttle");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: 1,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();

    let throttled = store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap_err();
    assert_eq!(
        throttled,
        StorageError::unavailable("maintenance dirty bytes above high watermark")
    );
    let snapshot = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.counters.coordinator_write_attempts, 3);
    assert_eq!(snapshot.counters.coordinator_write_unavailable, 1);
    assert!(snapshot.recent_events.iter().any(|event| {
        event.kind == StorageEventKind::CoordinatorWriteUnavailable
            && event.reason == Some("maintenance dirty bytes above high watermark")
    }));

    let report = store.run_maintenance_tick().unwrap();
    assert!(!report.plan.commands.is_empty());
    assert!(report.compaction.bytes_deleted > 0);
    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn scheduled_compaction_matches_manual_compaction() {
    fn prepare(root: &Path) -> (DurableCoordinator, DeviceId) {
        let cfg = config();
        let store = DurableCoordinator::open_with_maintenance_policy(
            root,
            cfg,
            MaintenancePolicy {
                mode: MaintenanceMode::Manual,
                data_log_policy: DurableDataLogPolicy {
                    target_data_log_bytes: 4096,
                    file_sync_fanout: 4,
                    min_reclaimable_ratio_ppm: 1,
                    min_reclaimable_bytes: 1,
                    max_compaction_copy_bytes: u64::MAX,
                },
                write_backpressure_enabled: true,
                dirty_low_watermark_bytes: 1,
                dirty_high_watermark_bytes: u64::MAX,
                max_sealed_logs: 64,
                max_reclaimable_debt_bytes: u64::MAX,
                compaction_copy_budget_per_tick: u64::MAX,
                max_sqlite_wal_bytes: u64::MAX,
                max_logs_scanned_per_tick: 64,
                max_concurrent_compaction_jobs: 1,
            },
        )
        .unwrap();
        let device_id = store
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 4,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        for block in 0..4 {
            store
                .write_device(
                    device_id,
                    block * 4096,
                    &repeated_blocks(1, (block + 1) as u8),
                    WriteDurability::Flushed,
                )
                .unwrap();
        }
        store
            .write_device(
                device_id,
                0,
                &repeated_blocks(1, 9),
                WriteDurability::Flushed,
            )
            .unwrap();
        store
            .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
            .unwrap();
        store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
        (store, device_id)
    }

    let manual_root = durable_temp_dir("manual-compaction-equivalence");
    let scheduled_root = durable_temp_dir("scheduled-compaction-equivalence");
    let (manual, manual_device) = prepare(&manual_root);
    let (scheduled, scheduled_device) = prepare(&scheduled_root);

    manual
        .compact_data_logs(DurableDataLogPolicy::compact_everything_for_test())
        .unwrap();
    scheduled.run_maintenance_tick().unwrap();

    let mut manual_bytes = vec![0; 4 * 4096];
    let mut scheduled_bytes = vec![0; 4 * 4096];
    manual
        .read_device(
            manual_device,
            ByteRange::new(0, 4 * 4096),
            &mut manual_bytes,
        )
        .unwrap();
    scheduled
        .read_device(
            scheduled_device,
            ByteRange::new(0, 4 * 4096),
            &mut scheduled_bytes,
        )
        .unwrap();
    assert_eq!(manual_bytes, scheduled_bytes);

    let manual_dead: u64 = manual
        .durable
        .data_log_rows_for_test()
        .unwrap()
        .iter()
        .map(|row| row.dead_bytes)
        .sum();
    let scheduled_dead: u64 = scheduled
        .durable
        .data_log_rows_for_test()
        .unwrap()
        .iter()
        .map(|row| row.dead_bytes)
        .sum();
    assert_eq!(manual_dead, scheduled_dead);
    assert_eq!(scheduled_dead, 0);
    let _ = fs::remove_dir_all(manual_root);
    let _ = fs::remove_dir_all(scheduled_root);
}

#[test]
fn repeated_maintenance_ticks_are_idempotent_and_restart_safe() {
    let root = durable_temp_dir("maintenance-idempotent-restart");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();

    let first = store.run_maintenance_tick().unwrap();
    let second = store.run_maintenance_tick().unwrap();
    assert!(first.compaction.bytes_deleted > 0);
    assert!(second.compaction.bytes_deleted <= first.compaction.bytes_deleted);
    store.shutdown_maintenance();
    drop(store);

    let reopened = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 2));
    reopened.shutdown_maintenance();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn maintenance_cursor_persists_across_reopen() {
    let root = durable_temp_dir("maintenance-cursor-restart");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Manual,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: true,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: 1,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 1,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    for block in 0..3 {
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Flushed,
            )
            .unwrap();
        store
            .write_device(
                device_id,
                block * 4096,
                &repeated_blocks(1, (block + 5) as u8),
                WriteDurability::Flushed,
            )
            .unwrap();
    }
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();

    let first = store.run_maintenance_tick().unwrap();
    let cursor = first.plan.next_cursor.unwrap();
    assert_eq!(first.plan.diagnostics.selected_logs.len(), 1);
    drop(store);

    let reopened = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    assert_eq!(
        reopened.observe_maintenance().unwrap().compaction_cursor,
        Some(cursor)
    );
    let next = reopened.plan_maintenance().unwrap();
    assert_eq!(next.diagnostics.selected_logs.len(), 1);
    assert!(next.diagnostics.selected_logs[0] > cursor);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn opportunistic_maintenance_runs_before_the_admitted_write() {
    let root = durable_temp_dir("opportunistic-maintenance");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::Opportunistic,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: false,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert!(store.observe_maintenance().unwrap().nodes[0].dirty_bytes > 0);

    store
        .write_device(
            device_id,
            4096,
            &repeated_blocks(1, 3),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert_eq!(store.observe_maintenance().unwrap().nodes[0].dirty_bytes, 0);
    let mut bytes = vec![0; 2 * 4096];
    store
        .read_device(device_id, ByteRange::new(0, 2 * 4096), &mut bytes)
        .unwrap();
    assert_eq!(&bytes[0..4096], repeated_blocks(1, 2).as_slice());
    assert_eq!(&bytes[4096..8192], repeated_blocks(1, 3).as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn always_on_maintenance_worker_shuts_down_and_reopens_cleanly() {
    let root = durable_temp_dir("always-on-maintenance-shutdown");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::AlwaysOn,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: false,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };
    let store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 7),
            WriteDurability::Flushed,
        )
        .unwrap();
    store.shutdown_maintenance();
    drop(store);

    let reopened = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
    let mut bytes = vec![0; 4096];
    reopened
        .read_device(device_id, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 7));
    reopened.shutdown_maintenance();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn always_on_startup_plan_detects_clean_and_dirty_state_without_hidden_work() {
    let clean_root = durable_temp_dir("always-on-startup-clean");
    let dirty_root = durable_temp_dir("always-on-startup-dirty");
    let cfg = config();
    let policy = MaintenancePolicy {
        mode: MaintenanceMode::AlwaysOn,
        data_log_policy: DurableDataLogPolicy {
            target_data_log_bytes: 4096,
            file_sync_fanout: 4,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        },
        write_backpressure_enabled: false,
        dirty_low_watermark_bytes: 1,
        dirty_high_watermark_bytes: u64::MAX,
        max_sealed_logs: 64,
        max_reclaimable_debt_bytes: u64::MAX,
        compaction_copy_budget_per_tick: u64::MAX,
        max_sqlite_wal_bytes: u64::MAX,
        max_logs_scanned_per_tick: 64,
        max_concurrent_compaction_jobs: 1,
    };

    let clean = DurableCoordinator::open_with_maintenance_policy(&clean_root, cfg, policy).unwrap();
    assert!(!clean.startup_maintenance_has_work().unwrap());
    clean.shutdown_maintenance();

    let mut manual_policy = policy;
    manual_policy.mode = MaintenanceMode::Manual;
    let dirty =
        DurableCoordinator::open_with_maintenance_policy(&dirty_root, cfg, manual_policy).unwrap();
    let device_id = dirty
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 4,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    dirty
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 1),
            WriteDurability::Flushed,
        )
        .unwrap();
    dirty
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 2),
            WriteDurability::Flushed,
        )
        .unwrap();
    dirty
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    dirty.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert!(dirty.maintenance_plan_has_commands(policy).unwrap());
    drop(clean);
    drop(dirty);
    let _ = fs::remove_dir_all(clean_root);
    let _ = fs::remove_dir_all(dirty_root);
}

#[test]
fn generated_maintenance_interleavings_preserve_durable_contents() {
    for seed in 0..4 {
        let root = durable_temp_dir(&format!("maintenance-generated-{seed}"));
        let cfg = config();
        let policy = MaintenancePolicy {
            mode: MaintenanceMode::Manual,
            data_log_policy: DurableDataLogPolicy {
                target_data_log_bytes: 4096,
                file_sync_fanout: 4,
                min_reclaimable_ratio_ppm: 1,
                min_reclaimable_bytes: 1,
                max_compaction_copy_bytes: u64::MAX,
            },
            write_backpressure_enabled: true,
            dirty_low_watermark_bytes: 1,
            dirty_high_watermark_bytes: u64::MAX,
            max_sealed_logs: 64,
            max_reclaimable_debt_bytes: u64::MAX,
            compaction_copy_budget_per_tick: 16 * 4096,
            max_sqlite_wal_bytes: u64::MAX,
            max_logs_scanned_per_tick: 4,
            max_concurrent_compaction_jobs: 1,
        };
        let mut rng = crate::sim::SeededRng::new(seed);
        let mut store =
            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
        let device_id = store
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 8,
                    block_size: 4096,
                },
                name: Some(format!("device-{seed}")),
            })
            .unwrap();
        let mut model = [0u8; 8];
        for step in 0..24 {
            match rng.next_u64() % 5 {
                0 | 1 => {
                    let block = rng.next_u64() as usize % model.len();
                    let byte = (1 + rng.next_u64() % 254) as u8;
                    store
                        .write_device(
                            device_id,
                            (block * 4096) as u64,
                            &[byte; 4096],
                            WriteDurability::Flushed,
                        )
                        .unwrap_or_else(|error| {
                            panic!("seed={seed} step={step} write failed: {error}")
                        });
                    model[block] = byte;
                }
                2 => {
                    store
                        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
                        .unwrap();
                    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
                }
                3 => {
                    store.run_maintenance_tick().unwrap();
                }
                _ => {
                    drop(store);
                    store = DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy)
                        .unwrap();
                }
            }
            let mut bytes = vec![0; model.len() * 4096];
            store
                .read_device(device_id, ByteRange::new(0, bytes.len() as u64), &mut bytes)
                .unwrap_or_else(|error| panic!("seed={seed} step={step} read failed: {error}"));
            for (block, expected) in model.iter().enumerate() {
                assert_eq!(
                    &bytes[block * 4096..(block + 1) * 4096],
                    &[*expected; 4096],
                    "seed={seed} step={step} block={block}"
                );
            }
        }
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn durable_sqlite_data_log_generated_replay_matches_reference_model() {
    #[derive(Clone, Copy)]
    struct ModelIds {
        device_id: DeviceId,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    }

    fn assert_models(
        store: &DurableCoordinator,
        ids: ModelIds,
        blocks: &[u8],
        file_blocks: &[u8],
        seed: u64,
        trace: &[String],
    ) {
        let mut actual_blocks = vec![0; blocks.len() * 4096];
        store
            .read_device(
                ids.device_id,
                ByteRange::new(0, actual_blocks.len() as u64),
                &mut actual_blocks,
            )
            .unwrap();
        assert_model_blocks(&actual_blocks, blocks, seed, trace, "durable block replay");

        let mut actual_file = vec![0; file_blocks.len() * 4096];
        store
            .read_file(
                ids.keyspace_id,
                ids.file_id,
                ByteRange::new(0, actual_file.len() as u64),
                &mut actual_file,
            )
            .unwrap();
        assert_model_blocks(
            &actual_file,
            file_blocks,
            seed,
            trace,
            "durable file replay",
        );
    }

    for seed in 0..4 {
        let root = durable_temp_dir(&format!("journal-generated-replay-{seed}"));
        let cfg = tree_config();
        let mut rng = crate::sim::SeededRng::new(seed);
        let mut trace = Vec::new();
        let mut store = DurableCoordinator::open(&root, cfg).unwrap();
        let device_id = store
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: Some(format!("device-{seed}")),
            })
            .unwrap();
        let keyspace_id = store
            .create_keyspace(CreateKeyspaceRequest {
                name: Some(format!("ks-{seed}")),
            })
            .unwrap();
        let file_id = store
            .create_file(
                keyspace_id,
                CreateFileRequest {
                    spec: FileSpec {
                        name: Some(format!("file-{seed}")),
                    },
                },
            )
            .unwrap();
        let ids = ModelIds {
            device_id,
            keyspace_id,
            file_id,
        };

        let mut live_blocks = vec![0u8; 16];
        let mut durable_blocks = live_blocks.clone();
        let mut live_file = Vec::new();
        let mut durable_file = live_file.clone();
        for step in 0..18 {
            match rng.next_u64() % 6 {
                0 => {
                    let block = rng.next_u64() as usize % live_blocks.len();
                    let byte = (1 + rng.next_u64() % 254) as u8;
                    store
                        .write_device(
                            device_id,
                            (block * 4096) as u64,
                            &[byte; 4096],
                            WriteDurability::Acknowledged,
                        )
                        .unwrap();
                    live_blocks[block] = byte;
                    trace.push(format!("step={step} block_ack block={block} byte={byte}"));
                }
                1 => {
                    let block = rng.next_u64() as usize % live_blocks.len();
                    let byte = (1 + rng.next_u64() % 254) as u8;
                    store
                        .write_device(
                            device_id,
                            (block * 4096) as u64,
                            &[byte; 4096],
                            WriteDurability::Flushed,
                        )
                        .unwrap();
                    live_blocks[block] = byte;
                    durable_blocks = live_blocks.clone();
                    durable_file = live_file.clone();
                    trace.push(format!(
                        "step={step} block_flushed block={block} byte={byte}"
                    ));
                }
                2 => {
                    let byte = (1 + rng.next_u64() % 254) as u8;
                    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
                    store
                        .append_stream(&stream, &[byte; 4096], WriteDurability::Acknowledged)
                        .unwrap();
                    trace.push(format!("step={step} append_private_ack byte={byte}"));
                }
                3 => {
                    let byte = (1 + rng.next_u64() % 254) as u8;
                    append_durable_store_once(
                        &store,
                        keyspace_id,
                        file_id,
                        &[byte; 4096],
                        WriteDurability::Flushed,
                    )
                    .unwrap();
                    live_file.push(byte);
                    durable_blocks = live_blocks.clone();
                    durable_file = live_file.clone();
                    trace.push(format!("step={step} append_flushed byte={byte}"));
                }
                4 => {
                    store.flush_device(device_id).unwrap();
                    store.flush_file(keyspace_id, file_id).unwrap();
                    durable_blocks = live_blocks.clone();
                    durable_file = live_file.clone();
                    trace.push(format!("step={step} flush"));
                }
                _ => {
                    drop(store);
                    store = DurableCoordinator::open(&root, cfg).unwrap();
                    live_blocks = durable_blocks.clone();
                    live_file = durable_file.clone();
                    trace.push(format!("step={step} crash_reopen"));
                }
            }
            assert_models(&store, ids, &live_blocks, &live_file, seed, &trace);
        }

        store.flush_device(device_id).unwrap();
        store.flush_file(keyspace_id, file_id).unwrap();
        durable_blocks = live_blocks;
        durable_file = live_file;
        drop(store);
        let reopened = DurableCoordinator::open(&root, cfg).unwrap();
        trace.push("final_reopen".to_string());
        assert_models(&reopened, ids, &durable_blocks, &durable_file, seed, &trace);
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn durable_append_stream_open_does_not_persist_writer_epoch() {
    let root = durable_temp_dir("native-restart");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    drop(store);

    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let before = store.metadata().state_inner().unwrap();
    assert_eq!(
        before.file_writer_epochs.get(&(keyspace_id, file_id)),
        Some(&WriterEpoch::from_raw(0))
    );
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(stream.writer_epoch, WriterEpoch::from_raw(1));
    let after_acquire = store.metadata().state_inner().unwrap();
    assert_eq!(
        after_acquire
            .file_writer_epochs
            .get(&(keyspace_id, file_id)),
        Some(&WriterEpoch::from_raw(1))
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let after_reopen = reopened.metadata().state_inner().unwrap();
    assert_eq!(
        after_reopen.file_writer_epochs.get(&(keyspace_id, file_id)),
        Some(&WriterEpoch::from_raw(0))
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_unpublished_append_streams_fail_after_reopen_even_when_epoch_repeats() {
    let root = durable_temp_dir("native-restart-stale-stream");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stale = store.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(stale.writer_epoch, WriterEpoch::from_raw(1));
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let fresh = reopened.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(fresh.writer_epoch, stale.writer_epoch);
    assert_ne!(fresh.stream_id, stale.stream_id);
    assert!(
        append_durable_store_with_stream(
            &reopened,
            &stale,
            b"stale",
            WriteDurability::Acknowledged
        )
        .is_err()
    );
    append_durable_store_with_stream(&reopened, &fresh, b"durable", WriteDurability::Flushed)
        .unwrap();
    let mut bytes = vec![0; b"durable".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"durable".len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"durable");

    drop(reopened);
    let reopened_again = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes_after_restart = vec![0; b"durable".len()];
    reopened_again
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"durable".len() as u64),
            &mut bytes_after_restart,
        )
        .unwrap();
    assert_eq!(bytes_after_restart, b"durable");
    assert_eq!(
        reopened_again
            .metadata()
            .state_inner()
            .unwrap()
            .file_writer_epochs
            .get(&(keyspace_id, file_id)),
        Some(&WriterEpoch::from_raw(1))
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn append_stream_stealing_is_scoped_to_one_file() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_a, _) = create_local_file(&client, keyspace_id);
    let (file_b, _) = create_local_file(&client, keyspace_id);

    let file_b_stream = store.open_append_stream(keyspace_id, file_b).unwrap();
    let stale_file_a = store.open_append_stream(keyspace_id, file_a).unwrap();
    let fresh_file_a = store.open_append_stream(keyspace_id, file_a).unwrap();

    assert!(
        append_local_store_with_stream(
            &store,
            &stale_file_a,
            b"stale",
            WriteDurability::Acknowledged
        )
        .is_err()
    );
    append_local_store_with_stream(&store, &file_b_stream, b"b", WriteDurability::Acknowledged)
        .unwrap();
    append_local_store_with_stream(&store, &fresh_file_a, b"a", WriteDurability::Acknowledged)
        .unwrap();

    let mut file_a_bytes = vec![0; 1];
    store
        .read_file(keyspace_id, file_a, ByteRange::new(0, 1), &mut file_a_bytes)
        .unwrap();
    assert_eq!(file_a_bytes, b"a");
    let mut file_b_bytes = vec![0; 1];
    store
        .read_file(keyspace_id, file_b, ByteRange::new(0, 1), &mut file_b_bytes)
        .unwrap();
    assert_eq!(file_b_bytes, b"b");
}

#[test]
fn same_file_write_at_invalidates_append_stream_without_touching_other_files() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_a_id, file_a) = create_local_file(&client, keyspace_id);
    let (_file_b_id, file_b) = create_local_file(&client, keyspace_id);

    let stale_a = file_a.open_append_stream().unwrap();
    let live_b = file_b.open_append_stream().unwrap();
    file_a.write_at(0, b"base").unwrap();

    assert!(append_native_file_with_stream(&file_a, &stale_a, b"x").is_err());
    append_native_file_with_stream(&file_b, &live_b, b"b").unwrap();
    append_native_file_once(&file_a, b"x").unwrap();

    let mut file_a_bytes = vec![0; b"basex".len()];
    store
        .read_file(
            keyspace_id,
            file_a_id,
            ByteRange::new(0, b"basex".len() as u64),
            &mut file_a_bytes,
        )
        .unwrap();
    assert_eq!(file_a_bytes, b"basex");
    let mut file_b_bytes = vec![0; 1];
    file_b.read_at(0, &mut file_b_bytes).unwrap();
    assert_eq!(file_b_bytes, b"b");
}

#[test]
fn durable_publish_does_not_revoke_append_stream() {
    let root = durable_temp_dir("native-publish-keeps-stream-active");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let first = store
        .append_stream(&stream, b"one", WriteDurability::Acknowledged)
        .unwrap();
    store
        .publish_append_stream(&stream, first.range.end_exclusive().unwrap())
        .unwrap();

    let second = store
        .append_stream(&stream, b"two", WriteDurability::Acknowledged)
        .unwrap();
    let commit = store
        .publish_append_stream(&stream, second.range.end_exclusive().unwrap())
        .unwrap();
    assert_eq!(commit.range, ByteRange::new(3, 3));

    let mut bytes = vec![0; b"onetwo".len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"onetwo");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_release_append_stream_revokes_token_and_discards_private_tail() {
    let root = durable_temp_dir("native-release-revokes-stream");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let published = store
        .append_stream(&stream, b"published", WriteDurability::Acknowledged)
        .unwrap();
    store
        .publish_append_stream(&stream, published.range.end_exclusive().unwrap())
        .unwrap();
    let private = store
        .append_stream(&stream, b"private", WriteDurability::Acknowledged)
        .unwrap();
    store.release_append_stream(&stream).unwrap();

    assert!(
        store
            .append_stream(&stream, b"after", WriteDurability::Acknowledged)
            .is_err()
    );
    assert!(
        store
            .submit_append_publish(&stream, private.range.end_exclusive().unwrap())
            .is_err()
    );
    assert!(store.release_append_stream(&stream).is_err());
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        b"published".len() as u64
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; b"published".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"published");
    assert_eq!(
        reopened
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        b"published".len() as u64
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_submitted_publish_captures_prefix_while_later_appends_continue() {
    let root = durable_temp_dir("native-ticket-captures-prefix");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let first = repeated_blocks(1, 7);
    let second = repeated_blocks(1, 8);
    store
        .append_stream(&stream, &first, WriteDurability::Acknowledged)
        .unwrap();
    let ticket = store
        .submit_append_publish(&stream, first.len() as u64)
        .unwrap();
    let mut forged_ticket = ticket.clone();
    forged_ticket.ticket_id = AppendPublishTicketId::from_raw(ticket.ticket_id.raw() + 1);
    assert!(store.wait_append_publish(&forged_ticket).is_err());
    store
        .append_stream(&stream, &second, WriteDurability::Acknowledged)
        .unwrap();

    let first_commit = store.wait_append_publish(&ticket).unwrap();
    assert_eq!(store.wait_append_publish(&ticket).unwrap(), first_commit);
    assert_eq!(first_commit.range, ByteRange::new(0, first.len() as u64));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        first.len() as u64
    );
    let mut first_bytes = vec![0; first.len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, first.len() as u64),
            &mut first_bytes,
        )
        .unwrap();
    assert_eq!(first_bytes, first);

    let full_len = (first.len() + second.len()) as u64;
    let second_commit = store.publish_append_stream(&stream, full_len).unwrap();
    assert_eq!(
        second_commit.range,
        ByteRange::new(first.len() as u64, second.len() as u64)
    );
    let mut all_bytes = vec![0; first.len() + second.len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, full_len),
            &mut all_bytes,
        )
        .unwrap();
    assert_eq!(&all_bytes[..first.len()], first.as_slice());
    assert_eq!(&all_bytes[first.len()..], second.as_slice());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_same_file_publish_tickets_serialize_without_inflight_conflict() {
    let root = durable_temp_dir("native-ticket-same-file-serializes");
    let cfg = config();
    let store = Arc::new(DurableCoordinator::open(&root, cfg).unwrap());
    store.enable_append_publish_wait_profiling(16).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let first = repeated_blocks(1, 71);
    let second = repeated_blocks(1, 72);
    store
        .append_stream(&stream, &first, WriteDurability::Acknowledged)
        .unwrap();
    let first_ticket = store
        .submit_append_publish(&stream, first.len() as u64)
        .unwrap();
    store
        .append_stream(&stream, &second, WriteDurability::Acknowledged)
        .unwrap();
    let total_len = (first.len() + second.len()) as u64;
    let second_ticket = store.submit_append_publish(&stream, total_len).unwrap();

    let persist_guard = lock(&store.persist_lock).unwrap();
    let first_wait = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.wait_append_publish(&first_ticket))
    };
    thread::sleep(Duration::from_millis(20));
    let second_wait = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.wait_append_publish(&second_ticket))
    };
    thread::sleep(Duration::from_millis(20));
    drop(persist_guard);
    let first_commit = first_wait.join().unwrap().unwrap();
    let second_commit = second_wait.join().unwrap().unwrap();

    assert_eq!(first_commit.range, ByteRange::new(0, first.len() as u64));
    assert_eq!(
        second_commit.range,
        ByteRange::new(first.len() as u64, second.len() as u64)
    );
    let mut all_bytes = vec![0; first.len() + second.len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, total_len),
            &mut all_bytes,
        )
        .unwrap();
    assert_eq!(&all_bytes[..first.len()], first.as_slice());
    assert_eq!(&all_bytes[first.len()..], second.as_slice());
    let profiles = store.drain_append_publish_wait_profiles(16).unwrap();
    assert_eq!(profiles.len(), 2);
    assert!(profiles.iter().all(|profile| profile.success));
    assert!(
        profiles
            .iter()
            .any(|profile| profile.persist_batches_started > 0
                && profile.persist_batch_nanos > 0
                && profile.max_batch_ticket_count >= 1)
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.batch_same_file_skip_count >= 1),
        "same-file tickets must be observed but skipped from the first physical batch"
    );
    assert!(
        profiles
            .iter()
            .any(|profile| profile.cvar_waits > 0 && profile.coordinator_wait_nanos > 0)
    );
    drop(store);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_wait_batches_submitted_pending_tickets() {
    let root = durable_temp_dir("native-ticket-batches-submitted-pending");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let first_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("first".to_string()),
                },
            },
        )
        .unwrap();
    let second_file =
        create_file_in_same_append_publish_lane_for_test(&store, keyspace_id, first_file, "second");
    let first_stream = store.open_append_stream(keyspace_id, first_file).unwrap();
    let second_stream = store.open_append_stream(keyspace_id, second_file).unwrap();
    let first_payload = repeated_blocks(1, 91);
    let second_payload = repeated_blocks(1, 92);
    store
        .append_stream(&first_stream, &first_payload, WriteDurability::Acknowledged)
        .unwrap();
    store
        .append_stream(
            &second_stream,
            &second_payload,
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let first_ticket = store.submit_append_publish(&first_stream, 4096).unwrap();
    let second_ticket = store.submit_append_publish(&second_stream, 4096).unwrap();
    store.enable_persist_profiling(16).unwrap();
    store.enable_append_publish_wait_profiling(16).unwrap();

    let first_commit = store.wait_append_publish(&first_ticket).unwrap();
    assert_eq!(first_commit.range, ByteRange::new(0, 4096));
    let second_commit = store.wait_append_publish(&second_ticket).unwrap();
    assert_eq!(second_commit.range, ByteRange::new(0, 4096));

    let mut bytes = vec![0; 4096];
    store
        .read_file(keyspace_id, first_file, ByteRange::new(0, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, first_payload);
    store
        .read_file(
            keyspace_id,
            second_file,
            ByteRange::new(0, 4096),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, second_payload);

    let persist_profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(
        persist_profiles.len(),
        1,
        "one waiter should drive one physical publish for all submitted pending tickets"
    );
    assert_eq!(persist_profiles[0].stream_prefix_plan_count, 2);

    let wait_profiles = store.drain_append_publish_wait_profiles(16).unwrap();
    assert_eq!(wait_profiles.len(), 2);
    assert!(
        wait_profiles
            .iter()
            .any(|profile| profile.persist_batches_started == 1
                && profile.max_batch_ticket_count >= 2)
    );
    assert!(
        wait_profiles
            .iter()
            .any(|profile| profile.batch_metadata_pending_ticket_count >= 2
                && profile.batch_planned_ticket_count >= 2),
        "batch profile should expose pending demand and admitted tickets"
    );
    assert!(
        wait_profiles
            .iter()
            .any(|profile| profile.completed_without_register)
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_wait_batches_cross_lane_pending_tickets() {
    let root = durable_temp_dir("native-ticket-batches-cross-lane-pending");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let first_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("first".to_string()),
                },
            },
        )
        .unwrap();
    let second_file = create_file_in_different_append_publish_lane_for_test(
        &store,
        keyspace_id,
        first_file,
        "second",
    );
    let first_stream = store.open_append_stream(keyspace_id, first_file).unwrap();
    let second_stream = store.open_append_stream(keyspace_id, second_file).unwrap();
    let first_payload = repeated_blocks(1, 93);
    let second_payload = repeated_blocks(1, 94);
    store
        .append_stream(&first_stream, &first_payload, WriteDurability::Acknowledged)
        .unwrap();
    store
        .append_stream(
            &second_stream,
            &second_payload,
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let first_ticket = store.submit_append_publish(&first_stream, 4096).unwrap();
    let second_ticket = store.submit_append_publish(&second_stream, 4096).unwrap();
    store.enable_persist_profiling(16).unwrap();
    store.enable_append_publish_wait_profiling(16).unwrap();

    let first_commit = store.wait_append_publish(&first_ticket).unwrap();
    assert_eq!(first_commit.range, ByteRange::new(0, 4096));
    let second_commit = store.wait_append_publish(&second_ticket).unwrap();
    assert_eq!(second_commit.range, ByteRange::new(0, 4096));

    let persist_profiles = store.drain_persist_profiles(16).unwrap();
    assert_eq!(
        persist_profiles.len(),
        1,
        "one waiter should drive one shared physical publish for cross-lane tickets"
    );
    assert_eq!(persist_profiles[0].stream_prefix_plan_count, 2);
    assert_eq!(
        load_append_visible_publish_journal_records(&root.join("append-visible-publish.journal"))
            .unwrap()
            .len(),
        2,
        "cross-lane group commit should use the shared append-visible batch journal"
    );
    assert_eq!(
        append_visible_publish_journal_count(&root, keyspace_id, first_file),
        0
    );
    assert_eq!(
        append_visible_publish_journal_count(&root, keyspace_id, second_file),
        0
    );

    let wait_profiles = store.drain_append_publish_wait_profiles(16).unwrap();
    assert_eq!(wait_profiles.len(), 2);
    let driver = wait_profiles
        .iter()
        .find(|profile| profile.persist_batches_started == 1)
        .expect("one waiter should drive the cross-lane physical batch");
    assert!(driver.max_batch_ticket_count >= 2);
    assert!(driver.batch_metadata_pending_ticket_count >= 2);
    assert!(driver.batch_planned_ticket_count >= 2);
    assert!(driver.batch_journal_lane_count >= 2);
    assert!(driver.batch_shared_journal);
    assert!(driver.persist_batch_plan_nanos > 0);
    assert!(driver.persist_batch_durable_nanos > 0);
    assert!(driver.persist_batch_apply_nanos > 0);

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut first = vec![0; first_payload.len()];
    reopened
        .read_file(
            keyspace_id,
            first_file,
            ByteRange::new(0, first_payload.len() as u64),
            &mut first,
        )
        .unwrap();
    assert_eq!(first, first_payload);
    let mut second = vec![0; second_payload.len()];
    reopened
        .read_file(
            keyspace_id,
            second_file,
            ByteRange::new(0, second_payload.len() as u64),
            &mut second,
        )
        .unwrap();
    assert_eq!(second, second_payload);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_wait_starts_immediately_at_target_batch_demand() {
    let root = durable_temp_dir("native-ticket-target-batch-start");
    let store = DurableCoordinator::open(&root, config()).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let mut tickets = Vec::new();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let mut publish_through = 0_u64;
    for index in 0..5 {
        let payload = vec![100 + index as u8; 128];
        store
            .append_stream(&stream, &payload, WriteDurability::Acknowledged)
            .unwrap();
        publish_through += payload.len() as u64;
        tickets.push(
            store
                .submit_append_publish(&stream, publish_through)
                .unwrap(),
        );
    }
    store.enable_append_publish_wait_profiling(16).unwrap();

    let mut expected_offset = 0_u64;
    for ticket in &tickets {
        let commit = store.wait_append_publish(ticket).unwrap();
        assert_eq!(commit.range, ByteRange::new(expected_offset, 128));
        expected_offset += 128;
    }

    let profiles = store.drain_append_publish_wait_profiles(16).unwrap();
    let driver = profiles
        .iter()
        .find(|profile| profile.persist_batches_started == 1)
        .expect("one waiter should drive the target-sized physical batch");
    assert!(driver.batch_coalesce_hit_target);
    assert_eq!(
        driver.coalesce_waits, 0,
        "already-target-sized demand should not spend an extra coalesce wait"
    );
    assert!(driver.batch_metadata_pending_ticket_count >= 4);
    assert!(driver.max_batch_ticket_count >= 4);
    assert!(
        driver.batch_same_file_skip_count >= 4,
        "same-file tickets should count toward demand but only one can plan per physical batch"
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn append_publish_batch_policy_defaults_match_gcp_latency_run() {
    let policy = AppendPublishBatchPolicy::default();
    assert_eq!(policy.target_tickets, 4);
    assert_eq!(policy.idle_coalesce_delay, Duration::from_micros(250));
    assert_eq!(policy.max_coalesce_delay, Duration::from_millis(5));
    policy.validate().unwrap();
}

#[test]
fn durable_append_publish_batch_policy_rejects_invalid_values() {
    let root = durable_temp_dir("append-publish-batch-policy-invalid");
    let cfg = config();
    let invalid_target = AppendPublishBatchPolicy {
        target_tickets: 0,
        ..AppendPublishBatchPolicy::default()
    };
    assert!(
        DurableCoordinator::open_with_storage_nodes_maintenance_policy_and_append_publish_batch_policy(
            &root,
            cfg,
            vec![cfg.storage_node],
            MaintenancePolicy::default(),
            invalid_target,
        )
        .is_err()
    );

    let invalid_delay = AppendPublishBatchPolicy {
        idle_coalesce_delay: Duration::from_millis(2),
        max_coalesce_delay: Duration::from_millis(1),
        ..AppendPublishBatchPolicy::default()
    };
    assert!(
        DurableCoordinator::open_with_storage_nodes_maintenance_policy_and_append_publish_batch_policy(
            &root,
            cfg,
            vec![cfg.storage_node],
            MaintenancePolicy::default(),
            invalid_delay,
        )
        .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_append_publish_batch_policy_controls_target_demand() {
    let root = durable_temp_dir("append-publish-batch-policy-target");
    let cfg = config();
    let policy = AppendPublishBatchPolicy {
        target_tickets: 2,
        idle_coalesce_delay: Duration::ZERO,
        max_coalesce_delay: Duration::ZERO,
    };
    let store =
        DurableCoordinator::open_with_storage_nodes_maintenance_policy_and_append_publish_batch_policy(
            &root,
            cfg,
            vec![cfg.storage_node],
            MaintenancePolicy::default(),
            policy,
        )
        .unwrap();
    assert_eq!(store.append_publish_batch_policy(), policy);
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    let first_payload = repeated_blocks(1, 101);
    let second_payload = repeated_blocks(1, 102);
    store
        .append_stream(&stream, &first_payload, WriteDurability::Acknowledged)
        .unwrap();
    let first = store.submit_append_publish(&stream, 4096).unwrap();
    store
        .append_stream(&stream, &second_payload, WriteDurability::Acknowledged)
        .unwrap();
    let second = store.submit_append_publish(&stream, 8192).unwrap();
    store.enable_append_publish_wait_profiling(16).unwrap();

    let first_commit = store.wait_append_publish(&first).unwrap();
    assert_eq!(first_commit.range, ByteRange::new(0, 4096));
    let second_commit = store.wait_append_publish(&second).unwrap();
    assert_eq!(second_commit.range, ByteRange::new(4096, 4096));
    let profiles = store.drain_append_publish_wait_profiles(16).unwrap();
    let driver = profiles
        .iter()
        .find(|profile| profile.persist_batches_started == 1)
        .expect("one waiter should drive the custom target-sized batch");
    assert!(driver.batch_coalesce_hit_target);
    assert_eq!(driver.coalesce_waits, 0);
    assert!(driver.batch_metadata_pending_ticket_count >= 2);
    assert!(driver.max_batch_ticket_count >= 2);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn append_stream_prepare_without_record_leaves_no_accepted_tail_hole() {
    let root = durable_temp_dir("append-prepare-no-tail-hole");
    let store = DurableCoordinator::open(&root, config()).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    {
        let prepared = store
            .local
            .prepare_append_stream_run(&stream, 4096, WriteDurability::Acknowledged)
            .unwrap();
        assert_eq!(prepared.range, ByteRange::new(0, 4096));
    }

    let ticket = store
        .local
        .append_stream(&stream, b"ok", WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(ticket.range, ByteRange::new(0, 2));
    let commit = store
        .local
        .publish_append_stream(&stream, 2, WriteDurability::Acknowledged);
    assert!(commit.is_ok());
    let mut bytes = [0_u8; 2];
    store
        .local
        .read_file(keyspace_id, file_id, ByteRange::new(0, 2), &mut bytes)
        .unwrap();
    assert_eq!(&bytes, b"ok");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_reopen_invalidates_unpublished_append_streams() {
    let root = durable_temp_dir("native-restart-stale-stream");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, b"stale", WriteDurability::Acknowledged)
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        reopened
            .publish_append_stream(&stream, b"stale".len() as u64)
            .is_err()
    );
    append_durable_store_once(
        &reopened,
        keyspace_id,
        file_id,
        b"durable",
        WriteDurability::Flushed,
    )
    .unwrap();

    let mut bytes = vec![0; b"durable".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"durable".len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"durable");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn unrelated_persist_does_not_make_unpublished_append_bytes_resumable() {
    let root = durable_temp_dir("native-stream-unpublished-private-pruned");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("target".to_string()),
                },
            },
        )
        .unwrap();
    let other_file = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("other".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, b"private", WriteDurability::Acknowledged)
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            other_file,
            &[FileBatchWrite::new(0, b"other".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        reopened
            .publish_append_stream(&stream, b"private".len() as u64)
            .is_err()
    );
    assert_eq!(
        reopened
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );

    let fresh = reopened.open_append_stream(keyspace_id, file_id).unwrap();
    append_durable_store_with_stream(&reopened, &fresh, b"new", WriteDurability::Acknowledged)
        .unwrap();
    let mut bytes = vec![0; b"new".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, b"new".len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"new");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_unpublished_stream_is_invisible_and_not_resumable_after_reopen() {
    let root = durable_temp_dir("native-stream-unpublished-invisible");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, b"private", WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );
    let mut hidden = vec![0; b"private".len()];
    assert!(
        store
            .read_file(
                keyspace_id,
                file_id,
                ByteRange::new(0, hidden.len() as u64),
                &mut hidden
            )
            .is_err()
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert_eq!(
        reopened
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        0
    );
    assert!(
        reopened
            .publish_append_stream(&stream, b"private".len() as u64)
            .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_unpublished_stream_cannot_be_resumed_by_token_after_reopen() {
    let root = durable_temp_dir("native-stream-token-authority");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        b"base",
        WriteDurability::Flushed,
    )
    .unwrap();

    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, b"private", WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file_id)
            .unwrap()
            .size,
        b"base".len() as u64
    );
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut forged_stream = stream.clone();
    forged_stream.stream_id = AppendStreamId::from_raw(stream.stream_id.raw() + 1);
    assert!(
        reopened
            .publish_append_stream(&forged_stream, b"baseprivate".len() as u64)
            .is_err(),
        "logical file identity alone must not resume private bytes"
    );
    assert!(
        reopened
            .publish_append_stream(&stream, b"baseprivate".len() as u64)
            .is_err(),
        "unpublished private bytes are not a public restart-resume contract"
    );

    let mut bytes = vec![0; b"base".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"base");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_takeover_without_stream_token_starts_from_visible_head() {
    let root = durable_temp_dir("native-stream-takeover-visible-head");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_id,
        b"base",
        WriteDurability::Flushed,
    )
    .unwrap();
    let old = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&old, b"old-private", WriteDurability::Acknowledged)
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let fresh = reopened.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(fresh.visible_base_size, b"base".len() as u64);
    assert_eq!(fresh.writer_epoch, old.writer_epoch);
    assert_ne!(fresh.stream_id, old.stream_id);
    assert!(
        !reopened
            .metadata()
            .state_inner()
            .unwrap()
            .append_streams
            .contains_key(&old.stream_id)
    );
    assert!(
        reopened
            .publish_append_stream(&old, b"baseold-private".len() as u64)
            .is_err(),
        "unpublished private stream state is not recovered after reopen"
    );
    append_durable_store_with_stream(&reopened, &fresh, b"new", WriteDurability::Acknowledged)
        .unwrap();

    let mut bytes = vec![0; b"basenew".len()];
    reopened
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"basenew");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn new_append_stream_fences_old_private_data_and_starts_at_visible_head() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, _) = create_local_file(&client, keyspace_id);
    append_local_store_once(
        &store,
        keyspace_id,
        file_id,
        b"base",
        WriteDurability::Acknowledged,
    )
    .unwrap();

    let old = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&old, b"old", WriteDurability::Acknowledged)
        .unwrap();
    let fresh = store.open_append_stream(keyspace_id, file_id).unwrap();
    assert_eq!(fresh.visible_base_size, b"base".len() as u64);
    assert!(
        store
            .publish_append_stream(&old, b"baseold".len() as u64, WriteDurability::Acknowledged,)
            .is_err()
    );

    let ticket = store
        .append_stream(&fresh, b"new", WriteDurability::Acknowledged)
        .unwrap();
    store
        .publish_append_stream(
            &fresh,
            ticket.range.end_exclusive().unwrap(),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let mut bytes = vec![0; b"basenew".len()];
    store
        .read_file(
            keyspace_id,
            file_id,
            ByteRange::new(0, bytes.len() as u64),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, b"basenew");
}

#[test]
fn gc_roots_active_private_stream_data_and_reclaims_fenced_private_data() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, _) = create_local_file(&client, keyspace_id);
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(&stream, b"private", WriteDurability::Acknowledged)
        .unwrap();
    let private_log = {
        let state = store.metadata().state_inner().unwrap();
        let private_run = &state
            .append_streams
            .get(&stream.stream_id)
            .unwrap()
            .records
            .first()
            .unwrap()
            .run;
        (private_run.storage_node, private_run.log_id)
    };

    let active_mark = store
        .mark_reachable_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    let active_sweep = store
        .sweep_metadata_after_mark(
            RetentionPolicy::expire_deleted_immediately(),
            active_mark.epoch,
        )
        .unwrap();
    assert!(active_sweep.released_segments.is_empty());
    assert!(
        lock(&store.append_run_logs)
            .unwrap()
            .contains_key(&private_log)
    );

    let _fresh = store.open_append_stream(keyspace_id, file_id).unwrap();
    let fenced_mark = store
        .mark_reachable_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    let sweep = store
        .sweep_metadata_after_mark(
            RetentionPolicy::expire_deleted_immediately(),
            fenced_mark.epoch,
        )
        .unwrap();
    assert!(sweep.released_segments.is_empty());
    assert!(
        !lock(&store.append_run_logs)
            .unwrap()
            .contains_key(&private_log)
    );
}

#[test]
fn durable_provider_reopen_matrix_covers_block_and_native_commit_shapes() {
    let root = durable_temp_dir("durable-matrix");
    let cfg = tree_config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();

    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 32,
                block_size: 4096,
            },
            name: Some("source".to_string()),
        })
        .unwrap();
    store
        .write_device(
            device_id,
            7 * 4096,
            &repeated_blocks(3, 4),
            WriteDurability::Flushed,
        )
        .unwrap();
    let checkpoint = store.checkpoint(device_id).unwrap();
    let forked = store
        .fork_device(
            device_id,
            ForkRequest {
                target: None,
                name: Some("forked".to_string()),
            },
        )
        .unwrap();
    store
        .write_device(
            forked,
            8 * 4096,
            &repeated_blocks(1, 9),
            WriteDurability::Flushed,
        )
        .unwrap();
    let restored = store
        .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    store.delete_device(device_id).unwrap();

    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_a = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_a,
            &[FileBatchWrite::new(0, b"before".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    let keyspace_checkpoint = store.checkpoint_keyspace(keyspace_id).unwrap();
    let snapshot_keyspace = store
        .snapshot_keyspace(
            keyspace_id,
            SnapshotKeyspaceRequest {
                target: None,
                name: Some("snap".to_string()),
            },
        )
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_a,
            &[FileBatchWrite::new(0, b"after!".to_vec())],
            WriteDurability::Flushed,
        )
        .unwrap();
    append_durable_store_once(
        &store,
        keyspace_id,
        file_b,
        b"tail",
        WriteDurability::Flushed,
    )
    .unwrap();
    let restored_keyspace = store
        .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(keyspace_checkpoint))
        .unwrap();

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(reopened.device_info(device_id).is_err());

    let mut forked_bytes = vec![0; 3 * 4096];
    reopened
        .read_device(
            forked,
            ByteRange::new(7 * 4096, 3 * 4096),
            &mut forked_bytes,
        )
        .unwrap();
    assert_eq!(&forked_bytes[0..4096], vec![4; 4096].as_slice());
    assert_eq!(&forked_bytes[4096..8192], vec![9; 4096].as_slice());
    assert_eq!(&forked_bytes[8192..12288], vec![4; 4096].as_slice());

    let mut restored_bytes = vec![0; 3 * 4096];
    reopened
        .read_device(
            restored,
            ByteRange::new(7 * 4096, 3 * 4096),
            &mut restored_bytes,
        )
        .unwrap();
    assert_eq!(restored_bytes, repeated_blocks(3, 4));

    let mut source_file = vec![0; b"after!".len()];
    reopened
        .read_file(
            keyspace_id,
            file_a,
            ByteRange::new(0, b"after!".len() as u64),
            &mut source_file,
        )
        .unwrap();
    assert_eq!(source_file, b"after!");

    let mut snapshot_file = vec![0; b"before".len()];
    reopened
        .read_file(
            snapshot_keyspace,
            file_a,
            ByteRange::new(0, b"before".len() as u64),
            &mut snapshot_file,
        )
        .unwrap();
    assert_eq!(snapshot_file, b"before");

    let mut restored_file = vec![0; b"before".len()];
    reopened
        .read_file(
            restored_keyspace,
            file_a,
            ByteRange::new(0, b"before".len() as u64),
            &mut restored_file,
        )
        .unwrap();
    assert_eq!(restored_file, b"before");

    let mut appended = vec![0; b"tail".len()];
    reopened
        .read_file(
            keyspace_id,
            file_b,
            ByteRange::new(0, b"tail".len() as u64),
            &mut appended,
        )
        .unwrap();
    assert_eq!(appended, b"tail");

    reopened
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();
    reopened
        .metadata()
        .validate_keyspace_catalog_for_test(snapshot_keyspace)
        .unwrap();
    reopened
        .metadata()
        .validate_keyspace_catalog_for_test(restored_keyspace)
        .unwrap();
    let _ = fs::remove_dir_all(root);
}

#[test]
fn durable_provider_persists_storage_node_custodian_deletions() {
    let root = durable_temp_dir("custodian-restart");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let device_id = store
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    store
        .write_device(
            device_id,
            0,
            &repeated_blocks(1, 8),
            WriteDurability::Flushed,
        )
        .unwrap();
    assert!(
        store
            .segment_store()
            .contains_segment(SegmentId::from_raw(1))
            .unwrap()
    );

    store.delete_device(device_id).unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert!(
        !store
            .segment_store()
            .contains_segment(SegmentId::from_raw(1))
            .unwrap()
    );

    drop(store);
    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    assert!(
        !reopened
            .segment_store()
            .contains_segment(SegmentId::from_raw(1))
            .unwrap()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn local_catalog_lifecycle_rejects_invalid_state_jumps() {
    let catalog = InMemoryLocalSegmentCatalog::new(config()).unwrap();
    let store = InMemorySegmentStore::new(config()).unwrap();
    let reservation = catalog.reserve_segment(reservation_intent()).unwrap();

    assert_eq!(
        catalog.state(reservation.segment_id).unwrap(),
        SegmentLifecycleState::Reserved
    );
    assert!(
        catalog
            .commit_segment(
                reservation.clone(),
                receipt_for_commit(
                    reservation_intent(),
                    SegmentReplicaCommit {
                        descriptor: SegmentDescriptor {
                            segment_id: reservation.segment_id,
                            blocks: BlockCount::from_raw(1),
                            bytes: 4096,
                            integrity: SegmentPayloadIntegrity::Unchecked,
                        },
                        placement: SegmentReplicaPlacement {
                            segment_id: reservation.segment_id,
                            storage_node: config().storage_node,
                            offset: 0,
                            bytes: 4096,
                        },
                    },
                ),
            )
            .is_err()
    );

    catalog.begin_write(&reservation).unwrap();
    let commit = store.write_segment(&reservation, &[1; 4096]).unwrap();
    store.sync_segment(reservation.segment_id).unwrap();
    let receipt = receipt_for_commit(reservation_intent(), commit.clone());
    catalog
        .commit_segment(reservation.clone(), receipt.clone())
        .unwrap();
    catalog
        .commit_segment(reservation.clone(), receipt.clone())
        .unwrap();
    assert_eq!(
        catalog.state(reservation.segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    assert_eq!(
        catalog.locate_segment(reservation.segment_id).unwrap(),
        commit.placement
    );

    catalog
        .mark_segment_referenced(reservation.segment_id)
        .unwrap();
    catalog.release_segment(reservation.segment_id).unwrap();
    catalog.delete_segment(reservation.segment_id).unwrap();
    assert_eq!(
        catalog.state(reservation.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );
    assert!(catalog.locate_segment(reservation.segment_id).is_err());
}

#[test]
fn local_catalog_reconciles_expired_reservations_and_failed_writes() {
    let catalog = InMemoryLocalSegmentCatalog::new(config()).unwrap();

    let expired = catalog.reserve_segment(reservation_intent()).unwrap();
    catalog.expire_reservation(expired.segment_id).unwrap();
    assert_eq!(
        catalog.state(expired.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );

    let failed = catalog.reserve_segment(reservation_intent()).unwrap();
    catalog.begin_write(&failed).unwrap();
    catalog.fail_write(failed.segment_id).unwrap();
    assert_eq!(
        catalog.state(failed.segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );

    let invalid = catalog.reserve_segment(reservation_intent()).unwrap();
    assert!(catalog.release_segment(invalid.segment_id).is_err());
    assert!(catalog.delete_segment(invalid.segment_id).is_err());
}

#[test]
fn local_transports_preserve_request_identity_and_order() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store.clone()));
    let block_transport = InProcessBlockTransport::new(block_server.clone());
    let create = BlockRequestEnvelope::new(
        RequestId::from_raw(1),
        ClientEpoch::from_raw(1),
        Some(LogicalDeadline::from_raw(100)),
        BlockRequest::Create {
            request: CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            },
        },
    );
    let created = block_transport.call(create.clone()).unwrap();
    let duplicate_created = block_transport.call(create.clone()).unwrap();
    assert_eq!(duplicate_created, created);
    assert_eq!(created.request_id, RequestId::from_raw(1));
    let device_id = match created.response.clone() {
        BlockResponse::Created(device_id) => device_id,
        _ => panic!("unexpected block response"),
    };
    assert!(
        block_transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(1),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );
    let info = block_transport
        .call(BlockRequestEnvelope::new(
            RequestId::from_raw(2),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Info { device_id },
        ))
        .unwrap();
    assert_eq!(info.request_id, RequestId::from_raw(2));
    let missing = BlockRequestEnvelope::new(
        RequestId::from_raw(3),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Info {
            device_id: DeviceId::from_raw(404),
        },
    );
    assert!(block_transport.call(missing.clone()).is_err());
    assert!(block_transport.call(missing).is_err());
    assert_eq!(
        block_server.request_log().unwrap(),
        vec![
            RequestId::from_raw(1),
            RequestId::from_raw(2),
            RequestId::from_raw(3),
        ]
    );

    let native_server = Arc::new(LocalNativeServer::new(store));
    let native_transport = InProcessNativeTransport::new(native_server.clone());
    let create_keyspace = NativeRequestEnvelope::new(
        RequestId::from_raw(3),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::CreateKeyspace {
            request: CreateKeyspaceRequest { name: None },
        },
    );
    let keyspace_id = match native_transport.call(create_keyspace).unwrap().response {
        NativeResponse::KeyspaceCreated(keyspace_id) => keyspace_id,
        _ => panic!("unexpected native response"),
    };
    let create_file = NativeRequestEnvelope::new(
        RequestId::from_raw(4),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::CreateFile {
            keyspace_id,
            request: CreateFileRequest {
                spec: FileSpec { name: None },
            },
        },
    );
    let created = native_transport.call(create_file.clone()).unwrap();
    let duplicate_created = native_transport.call(create_file).unwrap();
    assert_eq!(duplicate_created, created);
    assert_eq!(created.request_id, RequestId::from_raw(4));
    let file_id = match created.response {
        NativeResponse::FileCreated(file_id) => file_id,
        _ => panic!("unexpected native response"),
    };
    assert!(
        native_transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(3),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                },
            ))
            .is_err()
    );
    let invalid_read = NativeRequestEnvelope::new(
        RequestId::from_raw(5),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::Read {
            keyspace_id,
            file_id,
            range: ByteRange::new(0, 1),
            verification: ReadVerification::Default,
        },
    );
    assert!(native_transport.call(invalid_read.clone()).is_err());
    assert!(native_transport.call(invalid_read).is_err());
    assert_eq!(
        native_server.request_log().unwrap(),
        vec![
            RequestId::from_raw(3),
            RequestId::from_raw(4),
            RequestId::from_raw(5)
        ]
    );
}

#[test]
fn remote_block_transport_serializes_dedupes_and_rejects_stale_faults() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(RemoteBlockEndpoint::new(
        block_server.clone(),
        ServerIncarnation::from_raw(1),
        8,
        4,
    ));
    let transport = RemoteBlockTransport::new(endpoint.clone());
    let create = BlockRequestEnvelope::new(
        RequestId::from_raw(1),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Create {
            request: CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            },
        },
    );
    let created = transport.call(create.clone()).unwrap();
    let duplicate = transport.call(create).unwrap();
    assert_eq!(duplicate, created);
    assert_eq!(
        block_server.request_log().unwrap(),
        vec![RequestId::from_raw(1)]
    );
    let device_id = match created.response {
        BlockResponse::Created(device_id) => device_id,
        _ => panic!("unexpected block response"),
    };

    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(1),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );

    endpoint
        .set_logical_time(LogicalTime::from_raw(10))
        .unwrap();
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(2),
                ClientEpoch::from_raw(1),
                Some(LogicalDeadline::from_raw(9)),
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );
    endpoint.set_shutdown(true).unwrap();
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(3),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );
    endpoint.set_shutdown(false).unwrap();

    let stale_wire = bincode::serialize(&RemoteWireRequest {
        incarnation: ServerIncarnation::from_raw(99),
        envelope: BlockRequestEnvelope::new(
            RequestId::from_raw(4),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Info { device_id },
        ),
    })
    .unwrap();
    let stale_response = endpoint.handle_wire(&stale_wire).unwrap();
    assert!(
        transport
            .decode_response(RequestId::from_raw(4), &stale_response)
            .is_err()
    );

    let mismatched = bincode::serialize(&RemoteWireReply::Ok {
        incarnation: ServerIncarnation::from_raw(1),
        envelope: BlockResponseEnvelope {
            request_id: RequestId::from_raw(44),
            response: BlockResponse::Info(
                block_server.store.metadata.device_info(device_id).unwrap(),
            ),
        },
    })
    .unwrap();
    assert!(
        transport
            .decode_response(RequestId::from_raw(4), &mismatched)
            .is_err()
    );
    assert!(
        transport
            .decode_response(RequestId::from_raw(4), &[])
            .is_err()
    );
}

#[test]
fn remote_block_endpoint_enforces_backpressure() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(RemoteBlockEndpoint::new(
        block_server,
        ServerIncarnation::from_raw(1),
        8,
        0,
    ));
    let transport = RemoteBlockTransport::new(endpoint);
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(1),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Create {
                    request: CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: None,
                    },
                },
            ))
            .is_err()
    );
}

#[test]
fn chaos_block_wire_transport_covers_drop_delay_duplicate_and_reorder() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(RemoteBlockEndpoint::new(
        block_server.clone(),
        ServerIncarnation::from_raw(11),
        32,
        4,
    ));
    let chaos = Arc::new(ChaosRemoteWireTransport::new(endpoint.clone()));
    let transport = RemoteBlockTransport::with_wire(chaos.clone(), endpoint.incarnation());
    let create = BlockRequestEnvelope::new(
        RequestId::from_raw(1),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Create {
            request: CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            },
        },
    );
    let created = transport.call(create).unwrap();
    let device_id = match created.response {
        BlockResponse::Created(device_id) => device_id,
        _ => panic!("unexpected block response"),
    };

    chaos.duplicate_next_request().unwrap();
    let info = BlockRequestEnvelope::new(
        RequestId::from_raw(2),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Info { device_id },
    );
    transport.call(info).unwrap();
    assert_eq!(
        block_server
            .request_log()
            .unwrap()
            .iter()
            .filter(|request_id| **request_id == RequestId::from_raw(2))
            .count(),
        1
    );

    chaos.drop_next_request().unwrap();
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(3),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );
    assert!(
        !block_server
            .request_log()
            .unwrap()
            .contains(&RequestId::from_raw(3))
    );

    let write = BlockRequestEnvelope::new(
        RequestId::from_raw(4),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Write {
            device_id,
            offset: 0,
            bytes: vec![7; 4096],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        },
    );
    chaos.drop_next_response().unwrap();
    assert!(transport.call(write.clone()).is_err());
    let retry = transport.call(write).unwrap();
    assert_eq!(retry.request_id, RequestId::from_raw(4));
    assert_eq!(
        block_server
            .request_log()
            .unwrap()
            .iter()
            .filter(|request_id| **request_id == RequestId::from_raw(4))
            .count(),
        1
    );

    let read = BlockRequestEnvelope::new(
        RequestId::from_raw(5),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Read {
            device_id,
            range: ByteRange::new(0, 4096),
            verification: ReadVerification::Default,
        },
    );
    chaos.delay_next_response().unwrap();
    assert!(transport.call(read.clone()).is_err());
    assert_eq!(chaos.delayed_len().unwrap(), 1);
    chaos.return_delayed_response_next_call().unwrap();
    let delayed = transport.call(read).unwrap();
    match delayed.response {
        BlockResponse::Read(response) => assert_eq!(response.bytes, vec![7; 4096]),
        _ => panic!("unexpected block response"),
    }

    let stale_read = BlockRequestEnvelope::new(
        RequestId::from_raw(6),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Read {
            device_id,
            range: ByteRange::new(0, 4096),
            verification: ReadVerification::Default,
        },
    );
    chaos.delay_next_response().unwrap();
    assert!(transport.call(stale_read).is_err());
    let current_info = BlockRequestEnvelope::new(
        RequestId::from_raw(7),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Info { device_id },
    );
    chaos.reorder_next_response_with_delayed().unwrap();
    assert!(matches!(
        transport.call(current_info.clone()),
        Err(StorageError::Corrupt { .. })
    ));
    chaos.return_delayed_response_next_call().unwrap();
    let recovered = transport.call(current_info).unwrap();
    assert_eq!(recovered.request_id, RequestId::from_raw(7));

    chaos.fail_next_call().unwrap();
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(8),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );
    assert!(
        !block_server
            .request_log()
            .unwrap()
            .contains(&RequestId::from_raw(8))
    );

    let metrics = chaos.metrics().unwrap();
    assert_eq!(metrics.request_drops, 1);
    assert_eq!(metrics.response_drops, 1);
    assert_eq!(metrics.duplicated_requests, 1);
    assert_eq!(metrics.delayed_responses, 2);
    assert_eq!(metrics.reordered_responses, 1);
    assert_eq!(metrics.injected_failures, 1);
}

#[test]
fn server_lock_striping_does_not_force_unrelated_targets_through_one_lock() {
    let device_a = BlockRequest::Info {
        device_id: DeviceId::from_raw(1),
    };
    let device_b = BlockRequest::Info {
        device_id: DeviceId::from_raw(2),
    };
    assert_ne!(
        block_request_stripe(&device_a),
        block_request_stripe(&device_b)
    );

    let file_a = NativeRequest::FileInfo {
        keyspace_id: KeyspaceId::from_raw(1),
        file_id: FileId::from_raw(1),
    };
    let file_b = NativeRequest::FileInfo {
        keyspace_id: KeyspaceId::from_raw(1),
        file_id: FileId::from_raw(2),
    };
    assert_ne!(
        native_request_stripe(&file_a),
        native_request_stripe(&file_b)
    );
}

#[test]
fn chaos_native_wire_transport_covers_drop_delay_duplicate_and_reorder() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let native_server = Arc::new(LocalNativeServer::new(store));
    let endpoint = Arc::new(RemoteNativeEndpoint::new(
        native_server.clone(),
        ServerIncarnation::from_raw(12),
        32,
        4,
    ));
    let chaos = Arc::new(ChaosRemoteWireTransport::new(endpoint.clone()));
    let transport = RemoteNativeTransport::with_wire(chaos.clone(), endpoint.incarnation());
    let keyspace_id = match transport
        .call(NativeRequestEnvelope::new(
            RequestId::from_raw(1),
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::CreateKeyspace {
                request: CreateKeyspaceRequest { name: None },
            },
        ))
        .unwrap()
        .response
    {
        NativeResponse::KeyspaceCreated(keyspace_id) => keyspace_id,
        _ => panic!("unexpected native response"),
    };
    let file_id = match transport
        .call(NativeRequestEnvelope::new(
            RequestId::from_raw(2),
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::CreateFile {
                keyspace_id,
                request: CreateFileRequest {
                    spec: FileSpec { name: None },
                },
            },
        ))
        .unwrap()
        .response
    {
        NativeResponse::FileCreated(file_id) => file_id,
        _ => panic!("unexpected native response"),
    };

    chaos.duplicate_next_request().unwrap();
    transport
        .call(NativeRequestEnvelope::new(
            RequestId::from_raw(3),
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::FileInfo {
                keyspace_id,
                file_id,
            },
        ))
        .unwrap();
    assert_eq!(
        native_server
            .request_log()
            .unwrap()
            .iter()
            .filter(|request_id| **request_id == RequestId::from_raw(3))
            .count(),
        1
    );

    chaos.drop_next_request().unwrap();
    assert!(
        transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(4),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                },
            ))
            .is_err()
    );
    assert!(
        !native_server
            .request_log()
            .unwrap()
            .contains(&RequestId::from_raw(4))
    );

    let write = NativeRequestEnvelope::new(
        RequestId::from_raw(5),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::CommitFileBatch {
            keyspace_id,
            file_id,
            writes: vec![FileBatchWrite::new(0, b"native".to_vec())],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        },
    );
    chaos.drop_next_response().unwrap();
    assert!(transport.call(write.clone()).is_err());
    transport.call(write).unwrap();
    assert_eq!(
        native_server
            .request_log()
            .unwrap()
            .iter()
            .filter(|request_id| **request_id == RequestId::from_raw(5))
            .count(),
        1
    );

    let read = NativeRequestEnvelope::new(
        RequestId::from_raw(6),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::Read {
            keyspace_id,
            file_id,
            range: ByteRange::new(0, b"native".len() as u64),
            verification: ReadVerification::Default,
        },
    );
    chaos.delay_next_response().unwrap();
    assert!(transport.call(read.clone()).is_err());
    chaos.return_delayed_response_next_call().unwrap();
    let delayed = transport.call(read).unwrap();
    match delayed.response {
        NativeResponse::Read(response) => assert_eq!(response.bytes, b"native"),
        _ => panic!("unexpected native response"),
    }

    let stale_info = NativeRequestEnvelope::new(
        RequestId::from_raw(7),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::FileInfo {
            keyspace_id,
            file_id,
        },
    );
    chaos.delay_next_response().unwrap();
    assert!(transport.call(stale_info).is_err());
    let current_keyspace = NativeRequestEnvelope::new(
        RequestId::from_raw(8),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::KeyspaceInfo { keyspace_id },
    );
    chaos.reorder_next_response_with_delayed().unwrap();
    assert!(matches!(
        transport.call(current_keyspace.clone()),
        Err(StorageError::Corrupt { .. })
    ));
    chaos.return_delayed_response_next_call().unwrap();
    let recovered = transport.call(current_keyspace).unwrap();
    assert_eq!(recovered.request_id, RequestId::from_raw(8));

    chaos.fail_next_call().unwrap();
    assert!(
        transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(9),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                },
            ))
            .is_err()
    );
    assert!(
        !native_server
            .request_log()
            .unwrap()
            .contains(&RequestId::from_raw(9))
    );

    let metrics = chaos.metrics().unwrap();
    assert_eq!(metrics.request_drops, 1);
    assert_eq!(metrics.response_drops, 1);
    assert_eq!(metrics.duplicated_requests, 1);
    assert_eq!(metrics.delayed_responses, 2);
    assert_eq!(metrics.reordered_responses, 1);
    assert_eq!(metrics.injected_failures, 1);
}

#[test]
fn remote_native_transport_serializes_retries_and_preserves_file_semantics() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let native_server = Arc::new(LocalNativeServer::new(store));
    let endpoint = Arc::new(RemoteNativeEndpoint::new(
        native_server.clone(),
        ServerIncarnation::from_raw(5),
        8,
        4,
    ));
    let transport = RemoteNativeTransport::new(endpoint.clone());
    let client = LocalNativeClient::with_transport(Arc::new(transport.clone()));
    let keyspace_id = client
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("remote".to_string()),
        })
        .unwrap();
    let file_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let file = client.open_file(keyspace_id, file_id).unwrap();
    append_native_file_once(&file, b"remote").unwrap();
    let mut bytes = vec![0; b"remote".len()];
    file.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes, b"remote");

    let info = NativeRequestEnvelope::new(
        RequestId::from_raw(50),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::FileInfo {
            keyspace_id,
            file_id,
        },
    );
    let first = transport.call(info.clone()).unwrap();
    let duplicate = transport.call(info).unwrap();
    assert_eq!(duplicate, first);

    assert!(
        transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(50),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::KeyspaceInfo { keyspace_id },
            ))
            .is_err()
    );

    endpoint
        .set_logical_time(LogicalTime::from_raw(10))
        .unwrap();
    assert!(
        transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(51),
                ClientEpoch::from_raw(1),
                Some(LogicalDeadline::from_raw(9)),
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                },
            ))
            .is_err()
    );
    endpoint.set_shutdown(true).unwrap();
    assert!(
        transport
            .call(NativeRequestEnvelope::new(
                RequestId::from_raw(52),
                ClientEpoch::from_raw(1),
                None,
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                },
            ))
            .is_err()
    );
    endpoint.set_shutdown(false).unwrap();

    let stale_wire = bincode::serialize(&RemoteWireRequest {
        incarnation: ServerIncarnation::from_raw(99),
        envelope: NativeRequestEnvelope::new(
            RequestId::from_raw(53),
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::FileInfo {
                keyspace_id,
                file_id,
            },
        ),
    })
    .unwrap();
    let stale_response = endpoint.handle_wire(&stale_wire).unwrap();
    assert!(
        transport
            .decode_response(RequestId::from_raw(53), &stale_response)
            .is_err()
    );

    let mismatched = bincode::serialize(&RemoteWireReply::Ok {
        incarnation: ServerIncarnation::from_raw(5),
        envelope: NativeResponseEnvelope {
            request_id: RequestId::from_raw(99),
            response: NativeResponse::FileInfo(
                native_server
                    .store
                    .metadata
                    .get_file_info(keyspace_id, file_id)
                    .unwrap(),
            ),
        },
    })
    .unwrap();
    assert!(
        transport
            .decode_response(RequestId::from_raw(54), &mismatched)
            .is_err()
    );
    assert!(
        transport
            .decode_response(RequestId::from_raw(54), &[])
            .is_err()
    );

    assert!(
        native_server
            .request_log()
            .unwrap()
            .contains(&RequestId::from_raw(50))
    );
}

#[test]
fn network_wire_codec_round_trips_rejects_malformed_frames_and_has_golden_bytes() {
    let request = RemoteWireRequest {
        incarnation: ServerIncarnation::from_raw(3),
        envelope: BlockRequestEnvelope::new(
            RequestId::from_raw(1),
            ClientEpoch::from_raw(2),
            None,
            BlockRequest::Info {
                device_id: DeviceId::from_raw(4),
            },
        ),
    };
    let frame = encode_network_frame(NETWORK_BLOCK_REQUEST, &request).unwrap();
    assert_eq!(
        bytes_to_hex(&frame),
        concat!(
            "54434f5757495245",
            "0001",
            "01",
            "0000000000000003",
            "00000000000000000000000000000001",
            "0000000000000002",
            "00",
            "02",
            "00000000000000000000000000000004",
        )
    );
    let decoded: RemoteWireRequest<BlockRequestEnvelope> =
        decode_network_frame(NETWORK_BLOCK_REQUEST, &frame).unwrap();
    assert_eq!(decoded.incarnation, request.incarnation);
    assert_eq!(decoded.envelope, request.envelope);

    let mut bad_magic = frame.clone();
    bad_magic[0] ^= 0xff;
    assert!(
        decode_network_frame::<RemoteWireRequest<BlockRequestEnvelope>>(
            NETWORK_BLOCK_REQUEST,
            &bad_magic,
        )
        .is_err()
    );

    let mut bad_version = frame.clone();
    bad_version[9] = 2;
    assert!(
        decode_network_frame::<RemoteWireRequest<BlockRequestEnvelope>>(
            NETWORK_BLOCK_REQUEST,
            &bad_version,
        )
        .is_err()
    );

    let mut bad_kind = frame.clone();
    bad_kind[10] = NETWORK_NATIVE_REQUEST;
    assert!(
        decode_network_frame::<RemoteWireRequest<BlockRequestEnvelope>>(
            NETWORK_BLOCK_REQUEST,
            &bad_kind,
        )
        .is_err()
    );

    let mut truncated = frame.clone();
    truncated.pop();
    assert!(
        decode_network_frame::<RemoteWireRequest<BlockRequestEnvelope>>(
            NETWORK_BLOCK_REQUEST,
            &truncated,
        )
        .is_err()
    );

    let mut trailing = frame.clone();
    trailing.push(0);
    assert!(
        decode_network_frame::<RemoteWireRequest<BlockRequestEnvelope>>(
            NETWORK_BLOCK_REQUEST,
            &trailing,
        )
        .is_err()
    );

    let mismatched = encode_network_frame(
        NETWORK_BLOCK_RESPONSE,
        &RemoteWireReply::Ok {
            incarnation: ServerIncarnation::from_raw(3),
            envelope: BlockResponseEnvelope {
                request_id: RequestId::from_raw(99),
                response: BlockResponse::Created(DeviceId::from_raw(4)),
            },
        },
    )
    .unwrap();
    let transport = NetworkBlockTransport::new(
        Arc::new(NetworkBlockEndpoint::new(
            Arc::new(LocalBlockServer::new(LocalCoordinator::new())),
            ServerIncarnation::from_raw(3),
            8,
            4,
        )),
        ServerIncarnation::from_raw(3),
    );
    assert!(
        transport
            .decode_response(RequestId::from_raw(1), &mismatched)
            .is_err()
    );
}

#[test]
fn network_block_transport_loopback_retries_and_rejects_corrupt_frames() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(NetworkBlockEndpoint::new(
        block_server.clone(),
        ServerIncarnation::from_raw(21),
        32,
        4,
    ));
    let tcp_server = start_tcp_wire_server(endpoint);
    let tcp = Arc::new(TcpRemoteWireTransport::new(
        tcp_server.local_addr(),
        DEFAULT_NETWORK_MAX_FRAME_BYTES,
    ));
    let chaos = Arc::new(ChaosRemoteWireTransport::new(tcp));
    let transport = NetworkBlockTransport::new(chaos.clone(), ServerIncarnation::from_raw(21));

    let create = BlockRequestEnvelope::new(
        RequestId::from_raw(1),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Create {
            request: CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            },
        },
    );
    let created = transport.call(create).unwrap();
    let device_id = match created.response {
        BlockResponse::Created(device_id) => device_id,
        _ => panic!("unexpected block response"),
    };

    let write = BlockRequestEnvelope::new(
        RequestId::from_raw(2),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Write {
            device_id,
            offset: 0,
            bytes: vec![8; 4096],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        },
    );
    chaos.drop_next_response().unwrap();
    assert!(transport.call(write.clone()).is_err());
    transport.call(write).unwrap();
    assert_eq!(
        block_server
            .request_log()
            .unwrap()
            .iter()
            .filter(|request_id| **request_id == RequestId::from_raw(2))
            .count(),
        1
    );

    chaos.corrupt_next_response().unwrap();
    assert!(matches!(
        transport.call(BlockRequestEnvelope::new(
            RequestId::from_raw(3),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Info { device_id },
        )),
        Err(StorageError::Corrupt { .. })
    ));

    let read = transport
        .call(BlockRequestEnvelope::new(
            RequestId::from_raw(4),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Read {
                device_id,
                range: ByteRange::new(0, 4096),
                verification: ReadVerification::Default,
            },
        ))
        .unwrap();
    match read.response {
        BlockResponse::Read(read) => assert_eq!(read.bytes, vec![8; 4096]),
        _ => panic!("unexpected block response"),
    }

    let stale = NetworkBlockTransport::new(
        Arc::new(TcpRemoteWireTransport::new(
            tcp_server.local_addr(),
            DEFAULT_NETWORK_MAX_FRAME_BYTES,
        )),
        ServerIncarnation::from_raw(99),
    );
    assert!(
        stale
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(5),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Info { device_id },
            ))
            .is_err()
    );

    let tiny = TcpRemoteWireTransport::new(tcp_server.local_addr(), 4);
    assert!(tiny.call_wire(vec![0; 8]).is_err());
    tcp_server.shutdown().unwrap();
}

#[test]
fn tcp_wire_server_accepts_split_client_frames() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(NetworkBlockEndpoint::new(
        block_server,
        ServerIncarnation::from_raw(25),
        8,
        4,
    ));
    let tcp_server = start_tcp_wire_server(endpoint);
    let request = RemoteWireRequest {
        incarnation: ServerIncarnation::from_raw(25),
        envelope: BlockRequestEnvelope::new(
            RequestId::from_raw(1),
            ClientEpoch::from_raw(1),
            None,
            BlockRequest::Create {
                request: CreateDeviceRequest {
                    spec: DeviceSpec {
                        logical_blocks: 16,
                        block_size: 4096,
                    },
                    name: None,
                },
            },
        ),
    };
    let frame = encode_network_frame(NETWORK_BLOCK_REQUEST, &request).unwrap();
    let frame_len = u32::try_from(frame.len()).unwrap().to_be_bytes();
    let mut stream = TcpStream::connect(tcp_server.local_addr()).unwrap();
    stream.write_all(&frame_len).unwrap();
    thread::sleep(Duration::from_millis(10));
    stream.write_all(&frame).unwrap();

    let response = read_tcp_frame(&mut stream, DEFAULT_NETWORK_MAX_FRAME_BYTES).unwrap();
    let reply: RemoteWireReply<BlockResponseEnvelope> =
        decode_network_frame(NETWORK_BLOCK_RESPONSE, &response).unwrap();
    assert!(matches!(
        reply,
        RemoteWireReply::Ok {
            envelope: BlockResponseEnvelope {
                response: BlockResponse::Created(_),
                ..
            },
            ..
        }
    ));
    tcp_server.shutdown().unwrap();
}

#[test]
fn network_native_transport_loopback_preserves_file_semantics() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let native_server = Arc::new(LocalNativeServer::new(store));
    let endpoint = Arc::new(NetworkNativeEndpoint::new(
        native_server,
        ServerIncarnation::from_raw(22),
        32,
        4,
    ));
    let tcp_server = start_tcp_wire_server(endpoint);
    let transport =
        NetworkNativeTransport::tcp(tcp_server.local_addr(), ServerIncarnation::from_raw(22));
    let client = LocalNativeClient::with_transport(Arc::new(transport));
    let keyspace_id = client
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("net".to_string()),
        })
        .unwrap();
    let file_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    let file = client.open_file(keyspace_id, file_id).unwrap();
    file.write_at(0, b"alpha").unwrap();
    append_native_file_once(&file, b"-beta").unwrap();
    let mut bytes = vec![0; b"alpha-beta".len()];
    file.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes, b"alpha-beta");
    tcp_server.shutdown().unwrap();
}

#[test]
fn network_endpoints_enforce_backpressure_and_deadlines() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(NetworkBlockEndpoint::new(
        block_server,
        ServerIncarnation::from_raw(23),
        8,
        0,
    ));
    let tcp_server = start_tcp_wire_server(endpoint.clone());
    let transport =
        NetworkBlockTransport::tcp(tcp_server.local_addr(), ServerIncarnation::from_raw(23));
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(1),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Create {
                    request: CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: None,
                    },
                },
            ))
            .is_err()
    );
    tcp_server.shutdown().unwrap();

    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = Arc::new(LocalBlockServer::new(store));
    let endpoint = Arc::new(NetworkBlockEndpoint::new(
        block_server,
        ServerIncarnation::from_raw(24),
        8,
        4,
    ));
    endpoint
        .set_logical_time(LogicalTime::from_raw(10))
        .unwrap();
    let tcp_server = start_tcp_wire_server(endpoint.clone());
    let transport =
        NetworkBlockTransport::tcp(tcp_server.local_addr(), ServerIncarnation::from_raw(24));
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(2),
                ClientEpoch::from_raw(1),
                Some(LogicalDeadline::from_raw(9)),
                BlockRequest::Create {
                    request: CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: None,
                    },
                },
            ))
            .is_err()
    );
    endpoint.set_shutdown(true).unwrap();
    assert!(
        transport
            .call(BlockRequestEnvelope::new(
                RequestId::from_raw(3),
                ClientEpoch::from_raw(1),
                None,
                BlockRequest::Create {
                    request: CreateDeviceRequest {
                        spec: DeviceSpec {
                            logical_blocks: 16,
                            block_size: 4096,
                        },
                        name: None,
                    },
                },
            ))
            .is_err()
    );
    tcp_server.shutdown().unwrap();
}

#[test]
fn local_block_client_creates_opens_and_reads_empty_device_across_shards() {
    let cfg = LocalStoreConfig {
        shard_count: 4,
        ..config()
    };
    let store = LocalCoordinator::with_config(cfg).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 16,
                block_size: 4096,
            },
            name: Some("empty".to_string()),
        })
        .unwrap();

    let device = client.open_device(device_id).unwrap();
    assert_eq!(device.device_id(), device_id);
    assert_eq!(device.info().unwrap().spec.logical_blocks, 16);

    let head = store.metadata().get_head(device_id).unwrap();
    assert_eq!(head.shard_roots.len(), cfg.shard_count);
    for root in &head.shard_roots {
        store.metadata().get_metadata_node(*root).unwrap();
    }

    let mut buf = vec![99; 6 * 4096];
    device.read_at(3 * 4096, &mut buf).unwrap();
    assert_eq!(buf, vec![0; 6 * 4096]);

    let mut empty = Vec::new();
    device.read_at(16 * 4096, &mut empty).unwrap();
    assert!(device.read_at(1, &mut [0; 4096]).is_err());
}

#[test]
fn sparse_block_reads_overlay_segment_entries_on_zeroes() {
    let cfg = LocalStoreConfig {
        shard_count: 1,
        ..config()
    };
    let store = LocalCoordinator::with_config(cfg).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let reservation = store
        .segment_catalog()
        .reserve_segment(reservation_intent())
        .unwrap();
    store.segment_catalog().begin_write(&reservation).unwrap();
    let commit = store
        .segment_store()
        .write_segment(&reservation, &[7; 4096])
        .unwrap();
    store
        .segment_store()
        .sync_segment(reservation.segment_id)
        .unwrap();
    let receipt = receipt_for_commit(reservation_intent(), commit.clone());
    store
        .segment_catalog()
        .commit_segment(reservation.clone(), receipt.clone())
        .unwrap();
    store
        .segment_catalog()
        .mark_segment_referenced(reservation.segment_id)
        .unwrap();

    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(500),
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(0),
            BlockCount::from_raw(16),
        ),
        kind: MetadataNodeKind::Leaf {
            entries: vec![LeafEntry {
                logical_start: BlockIndex::from_raw(2),
                blocks: BlockCount::from_raw(1),
                segment_id: reservation.segment_id,
                segment_offset: BlockIndex::from_raw(0),
            }],
            run_extents: Vec::new(),
        },
    };
    store
        .metadata()
        .persist_metadata_node(MetadataNodeWrite::new(
            node.clone(),
            vec![
                LocalGrantReceiptAuthority
                    .verify_segment_receipt(&receipt)
                    .unwrap(),
            ],
        ))
        .unwrap();
    store
        .metadata()
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: node.node_id,
            })],
        })
        .unwrap();

    let mut buf = vec![0; 4 * 4096];
    store
        .read_device(head.device_id, ByteRange::new(0, 4 * 4096), &mut buf)
        .unwrap();

    assert_eq!(&buf[0..4096], vec![0; 4096].as_slice());
    assert_eq!(&buf[4096..8192], vec![0; 4096].as_slice());
    assert_eq!(&buf[8192..12288], vec![7; 4096].as_slice());
    assert_eq!(&buf[12288..16384], vec![0; 4096].as_slice());
}

#[test]
fn local_native_file_client_creates_opens_and_reads_empty_file() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let file_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("empty".to_string()),
                },
            },
        )
        .unwrap();

    let file = client.open_file(keyspace_id, file_id).unwrap();
    assert_eq!(file.keyspace_id(), keyspace_id);
    assert_eq!(file.file_id(), file_id);
    let info = file.info().unwrap();
    assert_eq!(info.size, 0);
    assert_eq!(info.version, FileVersion::from_raw(0));

    let mut empty = Vec::new();
    file.read_at(0, &mut empty).unwrap();
    assert!(file.read_at(0, &mut [0]).is_err());
}

#[test]
fn block_writes_and_overwrites_preserve_expected_ranges() {
    struct Case {
        name: &'static str,
        start_block: u64,
        blocks: u64,
        byte: u8,
    }

    let cases = [
        Case {
            name: "beginning",
            start_block: 0,
            blocks: 2,
            byte: 2,
        },
        Case {
            name: "middle",
            start_block: 3,
            blocks: 2,
            byte: 3,
        },
        Case {
            name: "end",
            start_block: 6,
            blocks: 2,
            byte: 4,
        },
        Case {
            name: "full-range",
            start_block: 0,
            blocks: 8,
            byte: 5,
        },
        Case {
            name: "same-range",
            start_block: 2,
            blocks: 3,
            byte: 6,
        },
        Case {
            name: "cross-shard",
            start_block: 3,
            blocks: 3,
            byte: 7,
        },
    ];

    for case in cases {
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..config()
        })
        .unwrap();
        let device = create_local_device(&store, 8);
        let initial = repeated_blocks(8, 1);
        device.write_at(0, &initial).unwrap();

        let overwrite = repeated_blocks(case.blocks, case.byte);
        device
            .write_at(case.start_block * 4096, &overwrite)
            .unwrap();

        let mut actual = vec![0; 8 * 4096];
        device.read_at(0, &mut actual).unwrap();

        let mut expected = initial;
        for block in case.start_block..case.start_block + case.blocks {
            let start = block as usize * 4096;
            expected[start..start + 4096].fill(case.byte);
        }
        assert_eq!(actual, expected, "case {}", case.name);
    }
}

#[test]
fn cross_shard_write_publishes_one_commit_group_and_references_segments_after_sync() {
    let store = LocalCoordinator::with_config(LocalStoreConfig {
        shard_count: 2,
        ..config()
    })
    .unwrap();
    let device = create_local_device(&store, 8);
    let commit = device.write_at(3 * 4096, &repeated_blocks(3, 9)).unwrap();

    let groups = store
        .metadata()
        .commit_groups_for_seq(commit.commit_seq)
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].updates.len(), 2);

    let roots = store
        .metadata()
        .get_head(device.device_id())
        .unwrap()
        .shard_roots;
    let mut referenced_segments = Vec::new();
    for root in roots {
        let node = store.metadata().get_metadata_node(root).unwrap();
        let MetadataNodeKind::Leaf { entries, .. } = node.kind else {
            panic!("default test roots should be leaves");
        };
        for entry in entries {
            referenced_segments.push(entry.segment_id);
            assert!(store.segment_store().is_synced(entry.segment_id).unwrap());
            assert_eq!(
                store.segment_catalog().state(entry.segment_id).unwrap(),
                SegmentLifecycleState::Referenced
            );
        }
    }
    assert_eq!(referenced_segments.len(), 2);
    let first_intent = store
        .segment_catalog()
        .intent_for_segment(referenced_segments[0])
        .unwrap()
        .write_intent;
    let second_intent = store
        .segment_catalog()
        .intent_for_segment(referenced_segments[1])
        .unwrap()
        .write_intent;
    assert_eq!(first_intent, second_intent);
}

#[test]
fn metadata_tree_shape_is_deterministic_for_a_write_trace() {
    fn run_trace() -> String {
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 1,
            ..tree_config()
        })
        .unwrap();
        let device = create_local_device(&store, 16);
        for (start, blocks, byte) in [(0, 1, 1), (7, 2, 2), (14, 2, 3), (4, 4, 4)] {
            device
                .write_at(start * 4096, &repeated_blocks(blocks, byte))
                .unwrap();
        }
        let root = store
            .metadata()
            .get_head(device.device_id())
            .unwrap()
            .shard_roots[0];
        let stats = store.validate_metadata_tree(root).unwrap();
        assert!(stats.max_depth > 1);
        store.render_metadata_tree(root).unwrap()
    }

    assert_eq!(run_trace(), run_trace());
}

#[test]
fn root_to_leaf_path_copy_changes_only_touched_nodes() {
    let store = LocalCoordinator::with_config(LocalStoreConfig {
        shard_count: 1,
        ..tree_config()
    })
    .unwrap();
    let device = create_local_device(&store, 16);
    let old_root = store
        .metadata()
        .get_head(device.device_id())
        .unwrap()
        .shard_roots[0];
    let old_stats = store.validate_metadata_tree(old_root).unwrap();
    let old_ids: BTreeSet<_> = store
        .metadata_tree_node_ids(old_root)
        .unwrap()
        .into_iter()
        .collect();

    device.write_at(0, &repeated_blocks(1, 9)).unwrap();

    let new_root = store
        .metadata()
        .get_head(device.device_id())
        .unwrap()
        .shard_roots[0];
    let new_stats = store.validate_metadata_tree(new_root).unwrap();
    assert_eq!(old_stats.nodes, new_stats.nodes);
    assert_eq!(old_stats.max_depth, new_stats.max_depth);
    let new_ids: BTreeSet<_> = store
        .metadata_tree_node_ids(new_root)
        .unwrap()
        .into_iter()
        .collect();
    let new_only = new_ids.difference(&old_ids).count();
    let shared = old_ids.intersection(&new_ids).count();

    assert_eq!(new_only, old_stats.max_depth);
    assert_eq!(shared, old_stats.nodes - old_stats.max_depth);
}

#[test]
fn generated_block_tree_reads_match_reference_model() {
    for seed in 0..16 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let device = create_local_device(&store, 32);
        let mut model = vec![0u8; 32];

        for step in 0..32 {
            let start = harness.rng.next_u64() % 32;
            let max_blocks = (32 - start).min(5);
            let blocks = 1 + harness.rng.next_u64() % max_blocks;
            let byte = (1 + harness.rng.next_u64() % 254) as u8;
            harness.trace.record(format!(
                "write step={step} start={start} blocks={blocks} byte={byte}"
            ));
            device
                .write_at(start * 4096, &repeated_blocks(blocks, byte))
                .unwrap();
            for block in start..start + blocks {
                model[block as usize] = byte;
            }

            let mut actual = vec![0; 32 * 4096];
            device.read_at(0, &mut actual).unwrap();
            assert_model_blocks(
                &actual,
                &model,
                seed,
                harness.trace.events(),
                &render_device_roots(&store, device.device_id()),
            );
            validate_device_roots(&store, device.device_id());
        }
    }
}

#[test]
fn generated_native_tree_reads_match_reference_model() {
    for seed in 0..16 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(tree_config()).unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (file_id, file) = create_local_file(&client, keyspace_id);
        let mut model = Vec::new();
        let capacity = 32 * 4096;

        for step in 0..16 {
            if model.len() == capacity {
                break;
            }
            let byte = (1 + harness.rng.next_u64() % 254) as u8;
            let expected_version = if model.is_empty() || harness.rng.next_u64().is_multiple_of(2) {
                let remaining = capacity - model.len();
                let len = 1 + harness.rng.next_u64() as usize % remaining.min(5000);
                harness
                    .trace
                    .record(format!("append step={step} len={len} byte={byte}"));
                let payload = vec![byte; len];
                let commit = append_native_file_once(&file, &payload).unwrap();
                model.extend_from_slice(&payload);
                commit.version
            } else {
                let offset = harness.rng.next_u64() as usize % (model.len() + 1);
                let remaining = capacity - offset;
                let len = 1 + harness.rng.next_u64() as usize % remaining.min(5000);
                let payload = vec![byte; len];
                harness.trace.record(format!(
                    "write step={step} offset={offset} len={len} byte={byte}"
                ));
                let commit = file.write_at(offset as u64, &payload).unwrap();
                apply_model_write(&mut model, offset, &payload);
                commit.version
            };

            let info = file.info().unwrap();
            assert_eq!(info.size, model.len() as u64);
            assert_eq!(info.version, expected_version);
            let mut actual = vec![0; model.len()];
            file.read_at(0, &mut actual).unwrap();
            let root = store
                .metadata()
                .get_file_head(keyspace_id, file_id)
                .unwrap()
                .root;
            assert_model_bytes(
                &actual,
                &model,
                seed,
                harness.trace.events(),
                &store.render_metadata_tree(root).unwrap(),
            );
            store.validate_metadata_tree(root).unwrap();
        }
    }
}

#[test]
fn fork_copies_roots_without_allocating_metadata_and_records_catalog() {
    let store = LocalCoordinator::with_config(LocalStoreConfig {
        shard_count: 2,
        ..tree_config()
    })
    .unwrap();
    let device = create_local_device(&store, 32);
    device.write_at(0, &repeated_blocks(8, 1)).unwrap();
    device.write_at(20 * 4096, &repeated_blocks(4, 2)).unwrap();
    let parent_head = store.metadata().get_head(device.device_id()).unwrap();
    let metadata_nodes_before = store.metadata().metadata_node_count().unwrap();

    let child_id = device
        .fork(ForkRequest {
            target: Some(DeviceId::from_raw(99)),
            name: Some("child".to_string()),
        })
        .unwrap();

    let child_head = store.metadata().get_head(child_id).unwrap();
    assert_eq!(child_id, DeviceId::from_raw(99));
    assert_eq!(child_head.shard_roots, parent_head.shard_roots);
    assert_eq!(
        store.metadata().get_head(device.device_id()).unwrap(),
        parent_head
    );
    assert_eq!(
        store.metadata().metadata_node_count().unwrap(),
        metadata_nodes_before
    );

    let record = store
        .metadata()
        .fork_record(child_head.latest_commit)
        .unwrap();
    assert_eq!(record.source, device.device_id());
    assert_eq!(record.target, child_id);
    assert_eq!(record.shard_roots, parent_head.shard_roots);
    assert_eq!(
        store
            .metadata()
            .fork_records_for_source(device.device_id())
            .unwrap(),
        vec![record]
    );
}

#[test]
fn forked_devices_initially_match_and_then_diverge() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let parent_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let parent = client.open_device(parent_id).unwrap();
    parent.write_at(0, &repeated_blocks(8, 1)).unwrap();

    let child_id = parent
        .fork(ForkRequest {
            target: None,
            name: Some("child".to_string()),
        })
        .unwrap();
    let child = client.open_device(child_id).unwrap();
    assert_eq!(read_device_bytes(&parent, 8), repeated_blocks(8, 1));
    assert_eq!(read_device_bytes(&child, 8), repeated_blocks(8, 1));

    parent.write_at(0, &repeated_blocks(1, 2)).unwrap();
    assert_eq!(&read_device_bytes(&parent, 8)[0..4096], vec![2; 4096]);
    assert_eq!(&read_device_bytes(&child, 8)[0..4096], vec![1; 4096]);

    child.write_at(7 * 4096, &repeated_blocks(1, 3)).unwrap();
    assert_eq!(
        &read_device_bytes(&child, 8)[7 * 4096..8 * 4096],
        vec![3; 4096]
    );
    assert_eq!(
        &read_device_bytes(&parent, 8)[7 * 4096..8 * 4096],
        vec![1; 4096]
    );
}

#[test]
fn generated_repeated_forks_and_divergent_writes_match_reference_model() {
    for seed in 0..12 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let root_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 32,
                    block_size: 4096,
                },
                name: Some("root".to_string()),
            })
            .unwrap();
        let mut device_ids = vec![root_id];
        let mut models = BTreeMap::from([(root_id, vec![0u8; 32])]);

        for step in 0..32 {
            let fork = harness.rng.next_u64().is_multiple_of(3) && device_ids.len() < 8;
            if fork {
                let source_index = harness.rng.choose_index(device_ids.len()).unwrap();
                let source_id = device_ids[source_index];
                let source = client.open_device(source_id).unwrap();
                let child_id = source
                    .fork(ForkRequest {
                        target: None,
                        name: Some(format!("child-{seed}-{step}")),
                    })
                    .unwrap();
                harness.trace.record(format!(
                    "fork step={step} source={source_id} child={child_id}"
                ));
                device_ids.push(child_id);
                models.insert(child_id, models.get(&source_id).unwrap().clone());
            } else {
                let target_index = harness.rng.choose_index(device_ids.len()).unwrap();
                let target_id = device_ids[target_index];
                let start = harness.rng.next_u64() % 32;
                let max_blocks = (32 - start).min(4);
                let blocks = 1 + harness.rng.next_u64() % max_blocks;
                let byte = (1 + harness.rng.next_u64() % 254) as u8;
                harness.trace.record(format!(
                    "write step={step} device={target_id} start={start} blocks={blocks} byte={byte}"
                ));
                let device = client.open_device(target_id).unwrap();
                device
                    .write_at(start * 4096, &repeated_blocks(blocks, byte))
                    .unwrap();
                let model = models.get_mut(&target_id).unwrap();
                for block in start..start + blocks {
                    model[block as usize] = byte;
                }
            }

            for device_id in &device_ids {
                let device = client.open_device(*device_id).unwrap();
                let mut actual = vec![0; 32 * 4096];
                device.read_at(0, &mut actual).unwrap();
                assert_model_blocks(
                    &actual,
                    models.get(device_id).unwrap(),
                    seed,
                    harness.trace.events(),
                    &render_device_roots(&store, *device_id),
                );
                validate_device_roots(&store, *device_id);
            }
        }
    }
}

#[test]
fn pitr_replays_roots_and_restores_to_commit_checkpoint_and_time() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: Some("pitr".to_string()),
        })
        .unwrap();
    let device = client.open_device(device_id).unwrap();

    let commit1 = device.write_at(0, &repeated_blocks(8, 1)).unwrap();
    let checkpoint1 = store.metadata().checkpoint(device_id).unwrap();
    let commit2 = device.write_at(3 * 4096, &repeated_blocks(3, 2)).unwrap();
    let checkpoint2 = store.metadata().checkpoint(device_id).unwrap();

    let head = store.metadata().get_head(device_id).unwrap();
    assert_eq!(
        store
            .metadata()
            .replay_device_roots(device_id, commit2.commit_seq)
            .unwrap(),
        head.shard_roots
    );
    assert_eq!(
        store
            .metadata()
            .replay_device_roots(device_id, commit1.commit_seq)
            .unwrap(),
        InMemoryMetadataPlane::checkpoint_block_roots(
            &store.metadata().get_checkpoint(checkpoint1).unwrap()
        )
        .unwrap()
    );

    let shard_commits = store
        .metadata()
        .shard_commits_for_device(device_id)
        .unwrap();
    let commit2_group_ids: BTreeSet<_> = shard_commits
        .iter()
        .filter(|commit| commit.commit_seq == commit2.commit_seq)
        .map(|commit| commit.commit_group)
        .collect();
    assert_eq!(commit2_group_ids.len(), 1);

    let restored_from_commit = device
        .restore(RestorePoint::Commit(commit1.commit_seq))
        .unwrap();
    let restored_from_checkpoint = device
        .restore(RestorePoint::Checkpoint(checkpoint1))
        .unwrap();
    let restored_from_time = device
        .restore(RestorePoint::Time(LogicalTime::from_raw(
            commit2.commit_seq.raw(),
        )))
        .unwrap();

    assert_eq!(
        read_device_bytes(&client.open_device(restored_from_commit).unwrap(), 8),
        repeated_blocks(8, 1)
    );
    assert_eq!(
        read_device_bytes(&client.open_device(restored_from_checkpoint).unwrap(), 8),
        repeated_blocks(8, 1)
    );

    let mut expected2 = repeated_blocks(8, 1);
    expected2[3 * 4096..6 * 4096].fill(2);
    assert_eq!(
        read_device_bytes(&client.open_device(restored_from_time).unwrap(), 8),
        expected2
    );

    assert!(
        store
            .metadata()
            .validate_checkpoint(&store.metadata().get_checkpoint(checkpoint2).unwrap())
            .is_ok()
    );
    assert!(
        device
            .restore(RestorePoint::Commit(CommitSeq::from_raw(999)))
            .is_err()
    );
}

#[test]
fn pitr_gc_releases_history_older_than_commit_window() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: Some("pitr-window".to_string()),
        })
        .unwrap();
    let device = client.open_device(device_id).unwrap();

    let commit1 = device.write_at(0, &[1; 4096]).unwrap();
    let checkpoint1 = store.metadata().checkpoint(device_id).unwrap();
    let commit2 = device.write_at(0, &[2; 4096]).unwrap();
    let commit3 = device.write_at(0, &[3; 4096]).unwrap();

    let report = store
        .run_metadata_custodian(
            RetentionPolicy::expire_deleted_immediately().with_pitr_grace_commits(2),
        )
        .unwrap();

    assert_eq!(report.sweep.released_segments, vec![SegmentId::from_raw(1)]);
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::Released
    );
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(2))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(3))
            .unwrap(),
        SegmentLifecycleState::Referenced
    );

    let retained_commits = store
        .metadata()
        .shard_commits_for_device(device_id)
        .unwrap();
    assert!(
        !retained_commits
            .iter()
            .any(|commit| commit.commit_seq == commit1.commit_seq)
    );
    assert!(
        !retained_commits
            .iter()
            .any(|commit| commit.commit_seq == commit2.commit_seq)
    );
    assert!(
        retained_commits
            .iter()
            .any(|commit| commit.commit_seq == commit3.commit_seq)
    );

    let restored = device
        .restore(RestorePoint::Commit(commit2.commit_seq))
        .unwrap();
    assert_eq!(
        read_device_bytes(&client.open_device(restored).unwrap(), 8),
        repeated_blocks(1, 2)
            .into_iter()
            .chain(vec![0; 7 * 4096])
            .collect::<Vec<_>>()
    );
    assert!(
        device
            .restore(RestorePoint::Commit(commit1.commit_seq))
            .is_err()
    );
    assert!(store.metadata().get_checkpoint(checkpoint1).is_err());
    assert!(
        device
            .restore(RestorePoint::Checkpoint(checkpoint1))
            .is_err()
    );
}

#[test]
fn checkpoint_validation_detects_mismatched_roots() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 8);
    let initial_roots = store
        .metadata()
        .get_head(device.device_id())
        .unwrap()
        .shard_roots;
    device.write_at(0, &repeated_blocks(8, 1)).unwrap();
    let checkpoint_id = store.metadata().checkpoint(device.device_id()).unwrap();
    let checkpoint = store.metadata().get_checkpoint(checkpoint_id).unwrap();
    assert!(store.metadata().validate_checkpoint(&checkpoint).is_ok());

    let mut corrupted = checkpoint;
    if let CheckpointRoots::BlockShard(roots) = &mut corrupted.roots {
        roots[0] = initial_roots[0];
    } else {
        panic!("expected block checkpoint roots");
    }
    assert!(store.metadata().validate_checkpoint(&corrupted).is_err());
}

#[test]
fn pitr_restore_interacts_with_forks_without_mutating_sources() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let parent_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks: 8,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    let parent = client.open_device(parent_id).unwrap();
    let parent_commit = parent.write_at(0, &repeated_blocks(8, 4)).unwrap();
    let child_id = parent
        .fork(ForkRequest {
            target: None,
            name: Some("child".to_string()),
        })
        .unwrap();
    let child = client.open_device(child_id).unwrap();
    let child_base = store.metadata().get_head(child_id).unwrap().latest_commit;
    child.write_at(7 * 4096, &repeated_blocks(1, 9)).unwrap();

    let parent_restore = parent
        .restore(RestorePoint::Commit(parent_commit.commit_seq))
        .unwrap();
    let child_restore = child.restore(RestorePoint::Commit(child_base)).unwrap();

    assert_eq!(
        read_device_bytes(&client.open_device(parent_restore).unwrap(), 8),
        repeated_blocks(8, 4)
    );
    assert_eq!(
        read_device_bytes(&client.open_device(child_restore).unwrap(), 8),
        repeated_blocks(8, 4)
    );
    assert_eq!(
        &read_device_bytes(&child, 8)[7 * 4096..8 * 4096],
        vec![9; 4096]
    );
    assert_eq!(read_device_bytes(&parent, 8), repeated_blocks(8, 4));
}

#[test]
fn generated_pitr_restores_match_historical_model() {
    for seed in 0..12 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            shard_count: 2,
            ..tree_config()
        })
        .unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 32,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        let device = client.open_device(device_id).unwrap();
        let mut model = vec![0u8; 32];
        let mut history = vec![(CommitSeq::from_raw(0), model.clone())];

        for step in 0..24 {
            let start = harness.rng.next_u64() % 32;
            let max_blocks = (32 - start).min(5);
            let blocks = 1 + harness.rng.next_u64() % max_blocks;
            let byte = (1 + harness.rng.next_u64() % 254) as u8;
            harness.trace.record(format!(
                "write step={step} start={start} blocks={blocks} byte={byte}"
            ));
            let commit = device
                .write_at(start * 4096, &repeated_blocks(blocks, byte))
                .unwrap();
            for block in start..start + blocks {
                model[block as usize] = byte;
            }
            history.push((commit.commit_seq, model.clone()));
            if harness.rng.next_u64().is_multiple_of(4) {
                store.metadata().checkpoint(device_id).unwrap();
            }
        }

        for _ in 0..8 {
            let index = harness.rng.choose_index(history.len()).unwrap();
            let (commit_seq, expected) = &history[index];
            let restored = device.restore(RestorePoint::Commit(*commit_seq)).unwrap();
            let restored_device = client.open_device(restored).unwrap();
            let mut actual = vec![0; 32 * 4096];
            restored_device.read_at(0, &mut actual).unwrap();
            assert_model_blocks(
                &actual,
                expected,
                seed,
                harness.trace.events(),
                &render_device_roots(&store, restored),
            );
        }
    }
}

#[test]
fn discard_removes_mapping_and_write_zeroes_reads_as_zeroes() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 8);
    device.write_at(0, &repeated_blocks(8, 8)).unwrap();
    device.discard(2 * 4096, 2 * 4096).unwrap();
    device.write_zeroes(5 * 4096, 4096).unwrap();

    let mut actual = vec![0; 8 * 4096];
    device.read_at(0, &mut actual).unwrap();
    assert_eq!(&actual[0..2 * 4096], repeated_blocks(2, 8).as_slice());
    assert_eq!(&actual[2 * 4096..4 * 4096], vec![0; 2 * 4096].as_slice());
    assert_eq!(&actual[4 * 4096..5 * 4096], vec![8; 4096].as_slice());
    assert_eq!(&actual[5 * 4096..6 * 4096], vec![0; 4096].as_slice());
    assert_eq!(
        &actual[6 * 4096..8 * 4096],
        repeated_blocks(2, 8).as_slice()
    );
}

#[test]
fn block_batch_commit_publishes_multiple_writes_atomically() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 16);

    let commit = device
        .commit_batch(&[
            BlockBatchWrite {
                offset: 0,
                bytes: repeated_blocks(1, 3),
                payload_integrity: PayloadIntegrity::Verified,
            },
            BlockBatchWrite {
                offset: 8 * 4096,
                bytes: repeated_blocks(1, 9),
                payload_integrity: PayloadIntegrity::Verified,
            },
        ])
        .unwrap();

    assert_eq!(commit.write_count, 2);
    assert_eq!(commit.collapsed_range_count, 2);
    assert_eq!(commit.committed_bytes, 2 * 4096);
    let info = device.info().unwrap();
    assert_eq!(info.latest_commit, commit.commit_seq);

    let mut actual = vec![0; 16 * 4096];
    device.read_at(0, &mut actual).unwrap();
    assert_eq!(&actual[0..4096], repeated_blocks(1, 3).as_slice());
    assert_eq!(
        &actual[8 * 4096..9 * 4096],
        repeated_blocks(1, 9).as_slice()
    );
    assert_eq!(&actual[4096..8 * 4096], vec![0; 7 * 4096].as_slice());
}

#[test]
fn block_batch_commit_collapses_overlaps_by_request_order() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let device = create_local_device(&store, 8);

    let commit = device
        .commit_batch(&[
            BlockBatchWrite {
                offset: 0,
                bytes: repeated_blocks(2, 1),
                payload_integrity: PayloadIntegrity::Verified,
            },
            BlockBatchWrite {
                offset: 4096,
                bytes: repeated_blocks(1, 7),
                payload_integrity: PayloadIntegrity::Verified,
            },
        ])
        .unwrap();

    assert_eq!(commit.write_count, 2);
    assert_eq!(commit.collapsed_range_count, 1);
    assert_eq!(commit.committed_bytes, 2 * 4096);
    let mut actual = vec![0; 3 * 4096];
    device.read_at(0, &mut actual).unwrap();
    assert_eq!(&actual[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&actual[4096..2 * 4096], repeated_blocks(1, 7).as_slice());
    assert_eq!(
        &actual[2 * 4096..3 * 4096],
        repeated_blocks(1, 0).as_slice()
    );
}

#[test]
fn block_batch_commit_publishes_multi_shard_update_in_one_commit_group() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();

    let commit = store
        .commit_block_batch(
            head.device_id,
            &[
                BlockBatchWrite {
                    offset: 0,
                    bytes: repeated_blocks(1, 4),
                    payload_integrity: PayloadIntegrity::Verified,
                },
                BlockBatchWrite {
                    offset: 8 * 4096,
                    bytes: repeated_blocks(1, 5),
                    payload_integrity: PayloadIntegrity::Verified,
                },
            ],
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let inner = store.metadata().state_inner().unwrap();
    let group = inner.commit_groups.values().last().unwrap();
    assert_eq!(group.commit_seq, commit.commit_seq);
    assert_eq!(group.updates.len(), 2);
    let shard_commits: Vec<_> = inner
        .shard_commits
        .iter()
        .filter(|shard| shard.commit_seq == commit.commit_seq)
        .collect();
    assert_eq!(shard_commits.len(), 2);
}

#[test]
fn block_batch_publish_failure_keeps_old_roots_and_pending_segments() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let original = store.metadata().get_head(head.device_id).unwrap();
    store
        .metadata()
        .set_next_commit_seq_for_test(u64::MAX)
        .unwrap();

    let failed = store.commit_block_batch(
        head.device_id,
        &[
            BlockBatchWrite {
                offset: 0,
                bytes: repeated_blocks(1, 11),
                payload_integrity: PayloadIntegrity::Verified,
            },
            BlockBatchWrite {
                offset: 8 * 4096,
                bytes: repeated_blocks(1, 12),
                payload_integrity: PayloadIntegrity::Verified,
            },
        ],
        WriteDurability::Acknowledged,
    );

    assert!(failed.is_err());
    assert_eq!(store.metadata().get_head(head.device_id).unwrap(), original);
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(2))
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    let mut buf = vec![1; 2 * 4096];
    store
        .read_device(head.device_id, ByteRange::new(0, 2 * 4096), &mut buf)
        .unwrap();
    assert_eq!(buf, vec![0; 2 * 4096]);
}

#[test]
fn failed_publish_after_durable_segment_write_leaves_old_roots_and_orphan() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let reservation = store
        .write_segment_for_owner(
            MappingOwner::BlockDevice(head.device_id),
            &repeated_blocks(1, 9),
        )
        .unwrap();
    let old_root = store
        .metadata()
        .get_metadata_node(head.shard_roots[0])
        .unwrap();
    let node = store
        .metadata()
        .allocate_metadata_node(
            old_root.covered_range,
            MetadataNodeKind::Leaf {
                entries: vec![LeafEntry {
                    logical_start: old_root.covered_range.start,
                    blocks: BlockCount::from_raw(1),
                    segment_id: reservation.segment_id,
                    segment_offset: BlockIndex::from_raw(0),
                }],
                run_extents: Vec::new(),
            },
        )
        .unwrap();
    store
        .metadata()
        .persist_metadata_node(MetadataNodeWrite::new(
            node.clone(),
            vec![
                store
                    .verify_segment_receipt(
                        &store
                            .storage_nodes
                            .receipt_for_segment(reservation.segment_id)
                            .unwrap(),
                    )
                    .unwrap(),
            ],
        ))
        .unwrap();

    let failed = store.metadata().publish_commit_group(CommitGroupIntent {
        owner: MappingOwner::BlockDevice(head.device_id),
        fence: MetadataFence::DeviceGeneration(head.generation),
        updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
            shard_id: ShardId::from_raw(0),
            old_root: MetadataNodeId::from_raw(404),
            new_root: node.node_id,
        })],
    });

    assert!(failed.is_err());
    assert_eq!(store.metadata().get_head(head.device_id).unwrap(), head);
    assert_eq!(
        store
            .segment_catalog()
            .state(reservation.segment_id)
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    let mut buf = vec![1; 4096];
    store
        .read_device(head.device_id, ByteRange::new(0, 4096), &mut buf)
        .unwrap();
    assert_eq!(buf, vec![0; 4096]);
}

#[test]
fn block_write_publish_failure_does_not_mark_storage_receipt_referenced() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let original = store.metadata().get_head(head.device_id).unwrap();
    store
        .metadata()
        .set_next_commit_seq_for_test(u64::MAX)
        .unwrap();

    let failed = store.write_device(
        head.device_id,
        0,
        &repeated_blocks(1, 11),
        WriteDurability::Acknowledged,
    );

    assert!(failed.is_err());
    assert_eq!(store.metadata().get_head(head.device_id).unwrap(), original);
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    let snapshot = store.diagnostics_snapshot().unwrap();
    assert_eq!(snapshot.counters.coordinator_write_attempts, 1);
    assert_eq!(snapshot.counters.coordinator_write_publish_successes, 0);
    assert_eq!(snapshot.counters.coordinator_write_publish_failures, 1);
    assert_eq!(snapshot.counters.storage_segment_writes, 1);
    assert_eq!(snapshot.counters.storage_segment_references, 0);
    assert!(snapshot.recent_events.iter().any(|event| {
        event.kind == StorageEventKind::MetadataPublishFailed
            && event.reason == Some("publish_failed")
    }));
    let mut buf = vec![1; 4096];
    store
        .read_device(head.device_id, ByteRange::new(0, 4096), &mut buf)
        .unwrap();
    assert_eq!(buf, vec![0; 4096]);
}

#[test]
fn native_append_streams_reuse_and_stealing_are_deterministic() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);

    let first = file.open_append_stream().unwrap();
    let stolen = file.open_append_stream().unwrap();
    let stolen_stream_id = stolen.stream_id;
    assert!(append_native_file_with_stream(&file, &first, &repeated_blocks(1, 1)).is_err());

    let commit = append_native_file_with_stream(&file, &stolen, &repeated_blocks(2, 2)).unwrap();
    assert_eq!(commit.version, FileVersion::from_raw(1));
    assert_eq!(commit.range, ByteRange::new(0, 2 * 4096));
    let second = append_native_file_with_stream(&file, &stolen, &repeated_blocks(1, 3)).unwrap();
    assert_eq!(second.version, FileVersion::from_raw(2));
    assert_eq!(second.range, ByteRange::new(2 * 4096, 4096));

    let mut actual = vec![0; 3 * 4096];
    file.read_at(0, &mut actual).unwrap();
    assert_eq!(
        actual,
        repeated_blocks(2, 2)
            .into_iter()
            .chain(repeated_blocks(1, 3))
            .collect::<Vec<_>>()
    );

    let head = store
        .metadata()
        .get_file_head(keyspace_id, file_id)
        .unwrap();
    let root = store.metadata().get_metadata_node(head.root).unwrap();
    let MetadataNodeKind::Leaf {
        entries,
        run_extents,
    } = root.kind
    else {
        panic!("default test native file root should remain a leaf");
    };
    assert!(entries.is_empty());
    assert_eq!(run_extents.len(), 2);
    assert_ne!(run_extents[0].run.run_id.raw(), stolen_stream_id.raw());
}

#[test]
fn native_append_publish_failure_leaves_file_version_and_private_run_unpublished() {
    let store = LocalCoordinator::with_config(LocalStoreConfig {
        file_root_blocks: 1,
        ..config()
    })
    .unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);
    let failed = append_native_file_once(&file, &repeated_blocks(2, 4));
    assert!(failed.is_err());
    let info = file.info().unwrap();
    assert_eq!(info.version, FileVersion::from_raw(0));
    assert_eq!(info.size, 0);
    assert!(file_run_extents(&store.metadata(), keyspace_id, file_id).is_empty());
    assert!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .is_err(),
        "append-stream ingest must not create a normal segment reservation"
    );
    assert_eq!(lock(&store.append_run_logs).unwrap().len(), 1);
}

#[test]
fn native_write_publish_failure_does_not_mark_storage_receipt_referenced() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);
    let original = file.info().unwrap();
    store
        .metadata()
        .set_next_commit_seq_for_test(u64::MAX)
        .unwrap();

    let failed = file.write_at(0, &repeated_blocks(1, 13));

    assert!(failed.is_err());
    assert_eq!(file.info().unwrap(), original);
    assert_eq!(
        store
            .segment_catalog()
            .state(SegmentId::from_raw(1))
            .unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
    let mut buf = vec![1; 4096];
    assert!(
        store
            .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut buf)
            .is_err()
    );
    assert_eq!(buf, vec![1; 4096]);
}

#[test]
fn native_file_accepts_unaligned_appends_and_reads() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);

    let first = append_native_file_once(&file, b"abc").unwrap();
    assert_eq!(first.range, ByteRange::new(0, 3));
    assert_eq!(file.info().unwrap().size, 3);

    let middle = vec![4; 4090];
    let second = append_native_file_once(&file, &middle).unwrap();
    assert_eq!(second.range, ByteRange::new(3, 4090));

    let suffix = vec![5; 8];
    let third = append_native_file_once(&file, &suffix).unwrap();
    assert_eq!(third.range, ByteRange::new(4093, 8));
    assert_eq!(file.info().unwrap().size, 4101);

    let mut expected = b"abc".to_vec();
    expected.extend_from_slice(&middle);
    expected.extend_from_slice(&suffix);

    let mut full = vec![0; expected.len()];
    file.read_at(0, &mut full).unwrap();
    assert_eq!(full, expected);

    let mut crossing = vec![0; 11];
    file.read_at(4090, &mut crossing).unwrap();
    assert_eq!(crossing, expected[4090..4101]);

    let mut single = vec![0; 1];
    file.read_at(2, &mut single).unwrap();
    assert_eq!(single, b"c");
    assert!(file.read_at(4098, &mut [0; 4]).is_err());
}

#[test]
fn native_file_write_at_is_first_class_and_snapshot_isolated() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);

    let first = file.write_at(0, b"hello world").unwrap();
    assert_eq!(first.range, ByteRange::new(0, 11));
    assert_eq!(first.version, FileVersion::from_raw(1));
    assert_eq!(file.info().unwrap().size, 11);

    let snapshot_keyspace = client
        .snapshot_keyspace(
            keyspace_id,
            SnapshotKeyspaceRequest {
                target: None,
                name: Some("before-overwrite".to_string()),
            },
        )
        .unwrap();

    let overwrite = file.write_at(0, b"goodbye!!!!").unwrap();
    assert_eq!(overwrite.range, ByteRange::new(0, 11));
    assert_eq!(overwrite.version, FileVersion::from_raw(2));

    let zero = file.write_at(11, &[]).unwrap();
    assert_eq!(zero.version, overwrite.version);
    assert_eq!(zero.commit_seq, overwrite.commit_seq);
    assert!(file.write_at(12, b"x").is_err());

    let mut source = vec![0; 11];
    file.read_at(0, &mut source).unwrap();
    assert_eq!(source.as_slice(), b"goodbye!!!!");

    let snapshot_file = client.open_file(snapshot_keyspace, file_id).unwrap();
    let mut snapshot = vec![0; 11];
    snapshot_file.read_at(0, &mut snapshot).unwrap();
    assert_eq!(snapshot.as_slice(), b"hello world");
}

#[test]
fn native_file_batch_commit_collapses_overlaps_and_advances_once() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);

    let commit = file
        .commit_batch(&[
            FileBatchWrite::new(0, b"abcd".to_vec()),
            FileBatchWrite::new(4, b"efgh".to_vec()),
            FileBatchWrite::new(2, b"ZZ".to_vec()),
        ])
        .unwrap();

    assert_eq!(commit.range, ByteRange::new(0, 8));
    assert_eq!(commit.version, FileVersion::from_raw(1));
    assert_eq!(file.info().unwrap().version, FileVersion::from_raw(1));
    let mut bytes = vec![0; 8];
    file.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes.as_slice(), b"abZZefgh");
}

#[test]
fn native_file_batch_commit_is_the_single_write_helper_path() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);

    let helper = file.write_at(0, b"hello").unwrap();
    let batch = file
        .commit_batch(&[FileBatchWrite::new(5, b" world".to_vec())])
        .unwrap();

    assert_eq!(helper.version, FileVersion::from_raw(1));
    assert_eq!(batch.version, FileVersion::from_raw(2));
    let mut bytes = vec![0; 11];
    file.read_at(0, &mut bytes).unwrap();
    assert_eq!(bytes.as_slice(), b"hello world");
}

#[test]
fn native_file_batch_commit_invalidates_only_same_file_append_streams() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_a_id, file_a) = create_local_file(&client, keyspace_id);
    let (_file_b_id, file_b) = create_local_file(&client, keyspace_id);

    let stale_a = file_a.open_append_stream().unwrap();
    let live_b = file_b.open_append_stream().unwrap();
    file_a
        .commit_batch(&[FileBatchWrite::new(0, b"base".to_vec())])
        .unwrap();

    assert!(append_native_file_with_stream(&file_a, &stale_a, b"x").is_err());
    append_native_file_with_stream(&file_b, &live_b, b"b").unwrap();

    let mut file_b_bytes = vec![0; 1];
    file_b.read_at(0, &mut file_b_bytes).unwrap();
    assert_eq!(file_b_bytes, b"b");
}

#[test]
fn durable_native_file_batch_commit_survives_reopen() {
    let root = durable_temp_dir("native-file-batch-reopen");
    let cfg = config();
    let store = DurableCoordinator::open(&root, cfg).unwrap();
    let keyspace_id = store
        .create_keyspace(CreateKeyspaceRequest {
            name: Some("ks".to_string()),
        })
        .unwrap();
    let file_id = store
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("file".to_string()),
                },
            },
        )
        .unwrap();
    store
        .commit_file_batch(
            keyspace_id,
            file_id,
            &[
                FileBatchWrite::new(0, b"left".to_vec()),
                FileBatchWrite::new(4, b"right".to_vec()),
            ],
            WriteDurability::Flushed,
        )
        .unwrap();
    drop(store);

    let reopened = DurableCoordinator::open(&root, cfg).unwrap();
    let mut bytes = vec![0; 9];
    reopened
        .read_file(keyspace_id, file_id, ByteRange::new(0, 9), &mut bytes)
        .unwrap();
    assert_eq!(bytes.as_slice(), b"leftright");
    let _ = fs::remove_dir_all(root);
}

#[test]
fn native_file_write_at_preserves_unmodified_bytes_and_rejects_sparse_gaps() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);

    let mut expected = vec![1; 4093];
    file.write_at(0, &expected).unwrap();

    file.write_at(4093, &[2u8; 8]).unwrap();
    expected.extend_from_slice(&[2u8; 8]);

    file.write_at(4090, &[3u8; 8]).unwrap();
    expected[4090..4098].fill(3);

    let info = file.info().unwrap();
    assert_eq!(info.size, expected.len() as u64);
    let mut actual = vec![0; expected.len()];
    file.read_at(0, &mut actual).unwrap();
    assert_eq!(actual, expected);

    let segment_entries = store.segment_catalog().entries().unwrap().len();
    let metadata_nodes = store.metadata().metadata_node_count().unwrap();
    let latest_commit = store
        .metadata()
        .get_file_head(keyspace_id, file.file_id())
        .unwrap()
        .latest_commit;
    let zero = file.write_at(info.size, &[]).unwrap();
    assert_eq!(zero.version, info.version);
    assert_eq!(zero.commit_seq, latest_commit);
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace_id, file.file_id())
            .unwrap()
            .latest_commit,
        latest_commit
    );
    assert_eq!(
        store.segment_catalog().entries().unwrap().len(),
        segment_entries
    );
    assert_eq!(
        store.metadata().metadata_node_count().unwrap(),
        metadata_nodes
    );

    assert!(file.write_at(info.size + 1, b"x").is_err());
    let mut after_failed_sparse_write = vec![0; expected.len()];
    file.read_at(0, &mut after_failed_sparse_write).unwrap();
    assert_eq!(after_failed_sparse_write, expected);
}

#[test]
fn native_keyspace_catalog_publish_updates_one_shard_without_allocating_keyspace_root() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let mut file_ids = Vec::new();
    for index in 0..(KEYSPACE_CATALOG_SHARD_COUNT + 4) {
        file_ids.push(
            client
                .create_file(
                    keyspace_id,
                    CreateFileRequest {
                        spec: FileSpec {
                            name: Some(format!("file-{index}")),
                        },
                    },
                )
                .unwrap(),
        );
    }
    store
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();

    let before_write = store
        .metadata()
        .keyspace_root_for_test(keyspace_id)
        .unwrap();
    let first_shard =
        InMemoryMetadataPlane::keyspace_catalog_shard_index(file_ids[0], &before_write).unwrap();
    let crowded_shard = InMemoryMetadataPlane::keyspace_catalog_shard_index(
        file_ids[KEYSPACE_CATALOG_SHARD_COUNT],
        &before_write,
    )
    .unwrap();
    assert_eq!(crowded_shard, first_shard);
    let shard_count_before_write = store
        .metadata()
        .keyspace_catalog_shard_count_for_test()
        .unwrap();
    let root_count_before_write = store.metadata().keyspace_root_count_for_test().unwrap();
    let file = client
        .open_file(keyspace_id, file_ids[KEYSPACE_CATALOG_SHARD_COUNT])
        .unwrap();
    file.write_at(0, &[7; 4096]).unwrap();
    let after_write = store
        .metadata()
        .keyspace_root_for_test(keyspace_id)
        .unwrap();

    assert_eq!(before_write.file_count, KEYSPACE_CATALOG_SHARD_COUNT + 4);
    assert_eq!(after_write.file_count, before_write.file_count);
    assert_eq!(after_write.shard_roots.len(), KEYSPACE_CATALOG_SHARD_COUNT);
    assert_eq!(changed_catalog_shards(&before_write, &after_write), 1);
    store
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap(),
        shard_count_before_write + 1
    );
    assert_eq!(
        store.metadata().keyspace_root_count_for_test().unwrap(),
        root_count_before_write
    );

    let before_create = after_write;
    let shard_count_before_create = store
        .metadata()
        .keyspace_catalog_shard_count_for_test()
        .unwrap();
    let root_count_before_create = store.metadata().keyspace_root_count_for_test().unwrap();
    client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("new-file".to_string()),
                },
            },
        )
        .unwrap();
    let after_create = store
        .metadata()
        .keyspace_root_for_test(keyspace_id)
        .unwrap();
    assert_eq!(after_create.file_count, before_create.file_count + 1);
    assert_eq!(changed_catalog_shards(&before_create, &after_create), 1);
    store
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap(),
        shard_count_before_create + 1
    );
    assert_eq!(
        store.metadata().keyspace_root_count_for_test().unwrap(),
        root_count_before_create
    );
}

#[test]
fn native_keyspace_snapshot_and_restore_are_filesystem_level() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let file_a_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("a".to_string()),
                },
            },
        )
        .unwrap();
    let file_b_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec {
                    name: Some("b".to_string()),
                },
            },
        )
        .unwrap();
    let file_a = client.open_file(keyspace_id, file_a_id).unwrap();
    let file_b = client.open_file(keyspace_id, file_b_id).unwrap();

    append_native_file_once(&file_a, &repeated_blocks(1, 1)).unwrap();
    append_native_file_once(&file_b, &repeated_blocks(1, 2)).unwrap();
    let checkpoint = client.checkpoint_keyspace(keyspace_id).unwrap();
    let checkpoint_head = store.metadata().get_keyspace_head(keyspace_id).unwrap();
    let stale_source_stream = file_a.open_append_stream().unwrap();

    append_native_file_with_stream(&file_a, &stale_source_stream, &repeated_blocks(1, 3)).unwrap();

    let nodes_before_restore = store.metadata().metadata_node_count().unwrap();
    let catalog_shards_before_restore = store
        .metadata()
        .keyspace_catalog_shard_count_for_test()
        .unwrap();
    let restored_keyspace = client
        .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    let restored_head = store
        .metadata()
        .get_keyspace_head(restored_keyspace)
        .unwrap();
    assert_eq!(restored_head.shard_roots, checkpoint_head.shard_roots);
    assert_eq!(restored_head.file_count, checkpoint_head.file_count);
    assert_eq!(
        store.metadata().metadata_node_count().unwrap(),
        nodes_before_restore
    );
    assert_eq!(
        store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap(),
        catalog_shards_before_restore
    );
    assert_eq!(
        store
            .metadata()
            .file_name_for_test(restored_keyspace, file_a_id)
            .unwrap(),
        Some("a".to_string())
    );
    assert_eq!(
        store
            .metadata()
            .file_name_for_test(restored_keyspace, file_b_id)
            .unwrap(),
        Some("b".to_string())
    );
    let restored_by_time = client
        .restore_keyspace(
            keyspace_id,
            RestorePoint::Time(store.metadata().get_checkpoint(checkpoint).unwrap().time),
        )
        .unwrap();
    let restored_a = client.open_file(restored_keyspace, file_a_id).unwrap();
    let restored_b = client.open_file(restored_keyspace, file_b_id).unwrap();
    let restored_time_a = client.open_file(restored_by_time, file_a_id).unwrap();
    assert_eq!(read_file_bytes(&restored_a, 1), repeated_blocks(1, 1));
    assert_eq!(read_file_bytes(&restored_b, 1), repeated_blocks(1, 2));
    assert_eq!(read_file_bytes(&restored_time_a, 1), repeated_blocks(1, 1));
    assert_eq!(
        read_file_bytes(&file_a, 2),
        repeated_blocks(1, 1)
            .into_iter()
            .chain(repeated_blocks(1, 3))
            .collect::<Vec<_>>()
    );

    assert!(
        append_native_file_with_stream(&restored_a, &stale_source_stream, &repeated_blocks(1, 4))
            .is_err()
    );
    append_native_file_once(&restored_a, &repeated_blocks(1, 5)).unwrap();
    assert_eq!(
        read_file_bytes(&restored_a, 2),
        repeated_blocks(1, 1)
            .into_iter()
            .chain(repeated_blocks(1, 5))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        read_file_bytes(&file_a, 2),
        repeated_blocks(1, 1)
            .into_iter()
            .chain(repeated_blocks(1, 3))
            .collect::<Vec<_>>()
    );

    let snapshot_source_head = store.metadata().get_keyspace_head(keyspace_id).unwrap();
    let nodes_before_snapshot = store.metadata().metadata_node_count().unwrap();
    let catalog_shards_before_snapshot = store
        .metadata()
        .keyspace_catalog_shard_count_for_test()
        .unwrap();
    let snapshot_keyspace = client
        .snapshot_keyspace(
            keyspace_id,
            SnapshotKeyspaceRequest {
                target: None,
                name: Some("current".to_string()),
            },
        )
        .unwrap();
    let snapshot_head = store
        .metadata()
        .get_keyspace_head(snapshot_keyspace)
        .unwrap();
    assert_eq!(snapshot_head.shard_roots, snapshot_source_head.shard_roots);
    assert_eq!(snapshot_head.file_count, snapshot_source_head.file_count);
    assert_eq!(
        store.metadata().metadata_node_count().unwrap(),
        nodes_before_snapshot
    );
    assert_eq!(
        store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap(),
        catalog_shards_before_snapshot
    );
    assert_eq!(
        store
            .metadata()
            .file_name_for_test(snapshot_keyspace, file_a_id)
            .unwrap(),
        Some("a".to_string())
    );
    assert!(
        client
            .snapshot_keyspace(
                keyspace_id,
                SnapshotKeyspaceRequest {
                    target: Some(snapshot_keyspace),
                    name: Some("duplicate".to_string()),
                },
            )
            .is_err()
    );
    let snapshot_a = client.open_file(snapshot_keyspace, file_a_id).unwrap();
    let snapshot_b = client.open_file(snapshot_keyspace, file_b_id).unwrap();
    assert_eq!(
        read_file_bytes(&snapshot_a, 2),
        repeated_blocks(1, 1)
            .into_iter()
            .chain(repeated_blocks(1, 3))
            .collect::<Vec<_>>()
    );
    assert_eq!(read_file_bytes(&snapshot_b, 1), repeated_blocks(1, 2));

    append_native_file_once(&snapshot_b, &repeated_blocks(1, 6)).unwrap();
    assert_eq!(read_file_bytes(&file_b, 1), repeated_blocks(1, 2));
    assert_eq!(
        read_file_bytes(&snapshot_b, 2),
        repeated_blocks(1, 2)
            .into_iter()
            .chain(repeated_blocks(1, 6))
            .collect::<Vec<_>>()
    );
    store
        .metadata()
        .validate_keyspace_catalog_for_test(keyspace_id)
        .unwrap();
    store
        .metadata()
        .validate_keyspace_catalog_for_test(restored_keyspace)
        .unwrap();
    store
        .metadata()
        .validate_keyspace_catalog_for_test(restored_by_time)
        .unwrap();
    store
        .metadata()
        .validate_keyspace_catalog_for_test(snapshot_keyspace)
        .unwrap();
}

#[test]
fn native_keyspace_checkpoint_validation_rejects_mismatched_root() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (_file_id, file) = create_local_file(&client, keyspace_id);
    let checkpoint1 = client.checkpoint_keyspace(keyspace_id).unwrap();

    append_native_file_once(&file, &repeated_blocks(1, 9)).unwrap();
    let checkpoint2 = client.checkpoint_keyspace(keyspace_id).unwrap();
    let first = store.metadata().get_checkpoint(checkpoint1).unwrap();
    let mut corrupted = store.metadata().get_checkpoint(checkpoint2).unwrap();
    assert!(store.metadata().validate_checkpoint(&corrupted).is_ok());

    corrupted.roots = first.roots;
    assert!(store.metadata().validate_checkpoint(&corrupted).is_err());
}

#[test]
fn native_keyspace_checkpoint_restore_uses_checkpoint_root_without_timeline_replay() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);

    file.write_at(0, b"stable").unwrap();
    let checkpoint = client.checkpoint_keyspace(keyspace_id).unwrap();
    let checkpoint_head = store.metadata().get_keyspace_head(keyspace_id).unwrap();
    let changed = file.write_at(0, b"change").unwrap();

    store
        .metadata()
        .clear_keyspace_commits_for_test(keyspace_id)
        .unwrap();

    let restored = client
        .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(checkpoint))
        .unwrap();
    let restored_head = store.metadata().get_keyspace_head(restored).unwrap();
    assert_eq!(restored_head.shard_roots, checkpoint_head.shard_roots);
    assert_eq!(restored_head.file_count, checkpoint_head.file_count);
    let restored_file = client.open_file(restored, file_id).unwrap();
    let mut actual = vec![0; b"stable".len()];
    restored_file.read_at(0, &mut actual).unwrap();
    assert_eq!(actual, b"stable");
    assert!(
        client
            .restore_keyspace(keyspace_id, RestorePoint::Commit(changed.commit_seq))
            .is_err()
    );
    store
        .metadata()
        .validate_keyspace_catalog_for_test(restored)
        .unwrap();
}

#[test]
fn native_keyspace_pitr_gc_respects_commit_window() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, file) = create_local_file(&client, keyspace_id);

    let commit1 = append_native_file_once(&file, &repeated_blocks(1, 1)).unwrap();
    let checkpoint1 = client.checkpoint_keyspace(keyspace_id).unwrap();
    let commit2 = append_native_file_once(&file, &repeated_blocks(1, 2)).unwrap();
    let commit3 = append_native_file_once(&file, &repeated_blocks(1, 3)).unwrap();

    let report = store
        .run_metadata_custodian(
            RetentionPolicy::expire_deleted_immediately().with_pitr_grace_commits(2),
        )
        .unwrap();

    assert!(report.sweep.released_segments.is_empty());
    assert!(file_segment_ids(&store.metadata(), keyspace_id, file_id).is_empty());
    assert_eq!(
        file_run_extents(&store.metadata(), keyspace_id, file_id).len(),
        3
    );

    let retained = store
        .metadata()
        .keyspace_commits_for_keyspace(keyspace_id)
        .unwrap();
    assert!(
        !retained
            .iter()
            .any(|commit| commit.commit_seq == commit1.commit_seq)
    );
    assert!(
        !retained
            .iter()
            .any(|commit| commit.commit_seq == commit2.commit_seq)
    );
    assert!(
        retained
            .iter()
            .any(|commit| commit.commit_seq == commit3.commit_seq)
    );

    let restored = client
        .restore_keyspace(keyspace_id, RestorePoint::Commit(commit2.commit_seq))
        .unwrap();
    let restored_file = client.open_file(restored, file_id).unwrap();
    assert_eq!(
        read_file_bytes(&restored_file, 2),
        repeated_blocks(1, 1)
            .into_iter()
            .chain(repeated_blocks(1, 2))
            .collect::<Vec<_>>()
    );
    assert!(
        client
            .restore_keyspace(keyspace_id, RestorePoint::Commit(commit1.commit_seq))
            .is_err()
    );
    assert!(store.metadata().get_checkpoint(checkpoint1).is_err());
}

#[test]
fn generated_native_keyspace_restores_match_historical_model() {
    for seed in 0..8 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let store = LocalCoordinator::with_config(tree_config()).unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (file_a, handle_a) = create_local_file(&client, keyspace_id);
        let (file_b, handle_b) = create_local_file(&client, keyspace_id);
        let mut model: BTreeMap<FileId, NativeFileReference> = BTreeMap::from([
            (file_a, NativeFileReference::empty()),
            (file_b, NativeFileReference::empty()),
        ]);
        let mut history = vec![(
            client.keyspace_info(keyspace_id).unwrap().latest_commit,
            model.clone(),
        )];
        let capacity = 32 * 4096;

        for step in 0..18 {
            let (file_id, handle) = if harness.rng.next_u64().is_multiple_of(2) {
                (file_a, &handle_a)
            } else {
                (file_b, &handle_b)
            };
            let byte = (1 + harness.rng.next_u64() % 254) as u8;
            let file_model = model.get_mut(&file_id).unwrap();
            let commit_seq = if file_model.bytes.is_empty()
                || (file_model.bytes.len() < capacity && harness.rng.next_u64().is_multiple_of(2))
            {
                let remaining = capacity - file_model.bytes.len();
                let len = 1 + harness.rng.next_u64() as usize % remaining.min(5000);
                let payload = vec![byte; len];
                harness.trace.record(format!(
                    "append step={step} file={file_id} len={len} byte={byte}"
                ));
                let commit = append_native_file_once(handle, &payload).unwrap();
                file_model.bytes.extend_from_slice(&payload);
                file_model.version = commit.version;
                commit.commit_seq
            } else {
                let max_offset = if file_model.bytes.len() == capacity {
                    capacity - 1
                } else {
                    file_model.bytes.len()
                };
                let offset = harness.rng.next_u64() as usize % (max_offset + 1);
                let remaining = capacity - offset;
                let len = 1 + harness.rng.next_u64() as usize % remaining.min(5000);
                let payload = vec![byte; len];
                harness.trace.record(format!(
                    "write step={step} file={file_id} offset={offset} len={len} byte={byte}"
                ));
                let commit = handle.write_at(offset as u64, &payload).unwrap();
                apply_model_write(&mut file_model.bytes, offset, &payload);
                file_model.version = commit.version;
                commit.commit_seq
            };
            store
                .metadata()
                .validate_keyspace_catalog_for_test(keyspace_id)
                .unwrap();
            history.push((commit_seq, model.clone()));
            if harness.rng.next_u64().is_multiple_of(4) {
                client.checkpoint_keyspace(keyspace_id).unwrap();
            }
        }

        for _ in 0..6 {
            let index = harness.rng.choose_index(history.len()).unwrap();
            let (commit_seq, expected) = &history[index];
            let restored = client
                .restore_keyspace(keyspace_id, RestorePoint::Commit(*commit_seq))
                .unwrap();
            store
                .metadata()
                .validate_keyspace_catalog_for_test(restored)
                .unwrap();
            for (file_id, expected_file) in expected {
                let restored_file = client.open_file(restored, *file_id).unwrap();
                let info = restored_file.info().unwrap();
                assert_eq!(info.size, expected_file.bytes.len() as u64);
                assert_eq!(info.version, expected_file.version);
                let mut actual = vec![0; expected_file.bytes.len()];
                restored_file.read_at(0, &mut actual).unwrap();
                assert_model_bytes(
                    &actual,
                    &expected_file.bytes,
                    seed,
                    harness.trace.events(),
                    "native keyspace restore",
                );
            }
        }
    }
}

#[test]
fn deterministic_simulation_checks_roots_after_create_and_read() {
    fn run(seed: u64) -> (Vec<String>, Vec<u8>) {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let cfg = LocalStoreConfig {
            shard_count: 4,
            ..config()
        };
        let store = LocalCoordinator::with_config(cfg).unwrap();
        let server = Arc::new(LocalBlockServer::new(store.clone()));
        let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
        let device_id = client
            .create_device(CreateDeviceRequest {
                spec: DeviceSpec {
                    logical_blocks: 16,
                    block_size: 4096,
                },
                name: None,
            })
            .unwrap();
        harness.trace.record(format!("created={device_id}"));
        let head = store.metadata().get_head(device_id).unwrap();
        for root in &head.shard_roots {
            store.metadata().get_metadata_node(*root).unwrap();
            harness.trace.record(format!("root={root}"));
        }

        let device = client.open_device(device_id).unwrap();
        let mut buf = vec![1; 4096 * 2];
        device.read_at(4 * 4096, &mut buf).unwrap();
        for root in &store.metadata().get_head(device_id).unwrap().shard_roots {
            store.metadata().get_metadata_node(*root).unwrap();
        }
        harness.trace.record("read=ok");
        (harness.trace.into_events(), buf)
    }

    assert_eq!(run(99), run(99));
}

#[test]
fn block_and_native_services_share_segment_lifecycle_machinery() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let block_server = LocalBlockServer::new(store.clone());
    let native_server = LocalNativeServer::new(store.clone());
    let reservation = store
        .segment_catalog()
        .reserve_segment(reservation_intent())
        .unwrap();

    assert_eq!(
        block_server
            .store
            .segment_catalog()
            .state(reservation.segment_id)
            .unwrap(),
        SegmentLifecycleState::Reserved
    );
    assert_eq!(
        native_server
            .store
            .segment_catalog()
            .state(reservation.segment_id)
            .unwrap(),
        SegmentLifecycleState::Reserved
    );
}

#[test]
fn local_providers_replay_ordered_commands_deterministically() {
    assert_eq!(deterministic_provider_run(), deterministic_provider_run());
}

fn deterministic_provider_run() -> (
    DeviceHead,
    CommitGroup,
    SegmentReplicaCommit,
    SegmentLifecycleState,
    Vec<MetadataNodeId>,
) {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let new_node = metadata_leaf(2000, 0, 8);
    store
        .metadata()
        .persist_metadata_node(MetadataNodeWrite::new(new_node.clone(), Vec::new()))
        .unwrap();
    let commit_group = store
        .metadata()
        .publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: ShardId::from_raw(0),
                old_root: head.shard_roots[0],
                new_root: new_node.node_id,
            })],
        })
        .unwrap();

    let reservation = store
        .segment_catalog()
        .reserve_segment(reservation_intent())
        .unwrap();
    store.segment_catalog().begin_write(&reservation).unwrap();
    let replica_commit = store
        .segment_store()
        .write_segment(&reservation, &[5; 4096])
        .unwrap();
    store
        .segment_store()
        .sync_segment(reservation.segment_id)
        .unwrap();
    let receipt = receipt_for_commit(reservation_intent(), replica_commit.clone());
    store
        .segment_catalog()
        .commit_segment(reservation.clone(), receipt.clone())
        .unwrap();
    store
        .segment_catalog()
        .mark_segment_referenced(reservation.segment_id)
        .unwrap();
    let state = store
        .segment_catalog()
        .state(reservation.segment_id)
        .unwrap();
    let roots = store
        .metadata()
        .roots_for_gc(RetentionPolicy::expire_deleted_immediately())
        .unwrap();

    (
        store.metadata().get_head(head.device_id).unwrap(),
        commit_group,
        replica_commit,
        state,
        roots,
    )
}

#[test]
fn unsupported_local_service_operations_preserve_no_partial_state() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let server = LocalBlockServer::new(store.clone());
    let response = server.handle(BlockRequestEnvelope::new(
        RequestId::from_raw(10),
        ClientEpoch::from_raw(1),
        None,
        BlockRequest::Flush {
            device_id: DeviceId::from_raw(404),
            scope: FlushScope::Device,
        },
    ));

    assert!(response.is_err());
    assert!(store.metadata().get_head(DeviceId::from_raw(404)).is_err());

    let native = LocalNativeServer::new(store);
    let keyspace_id = KeyspaceId::from_raw(1);
    let response = native.handle(NativeRequestEnvelope::new(
        RequestId::from_raw(11),
        ClientEpoch::from_raw(1),
        None,
        NativeRequest::AppendStream {
            keyspace_id,
            file_id: FileId::from_raw(1),
            stream: crate::extent::AppendStream {
                keyspace_id,
                file_id: FileId::from_raw(1),
                stream_id: crate::id::AppendStreamId::from_raw(1),
                writer_epoch: WriterEpoch::from_raw(0),
                base_version: FileVersion::from_raw(0),
                visible_base_size: 0,
            },
            bytes: vec![1],
            payload_integrity: PayloadIntegrity::Verified,
        },
    ));

    assert!(response.is_err());
}

#[test]
fn local_multi_node_placement_spreads_block_and_native_segments_without_api_leaks() {
    let cfg = config();
    let node_ids = vec![
        cfg.storage_node,
        StorageNodeId::from_raw(78),
        StorageNodeId::from_raw(79),
    ];
    let store = LocalCoordinator::with_storage_nodes(cfg, node_ids.clone()).unwrap();
    assert_eq!(store.storage_node_ids_for_test(), node_ids);

    let device = store.metadata().create_device(device_request()).unwrap();
    for block in 0..3 {
        store
            .write_device(
                device.device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }
    let mut device_bytes = vec![0; 3 * 4096];
    store
        .read_device(
            device.device_id,
            ByteRange::new(0, 3 * 4096),
            &mut device_bytes,
        )
        .unwrap();
    assert_eq!(&device_bytes[0..4096], repeated_blocks(1, 1).as_slice());
    assert_eq!(&device_bytes[4096..8192], repeated_blocks(1, 2).as_slice());
    assert_eq!(&device_bytes[8192..12288], repeated_blocks(1, 3).as_slice());
    let device_segments = device_segment_ids(&store.metadata(), device.device_id);
    assert_eq!(device_segments.len(), 3);
    assert_eq!(segment_storage_nodes(&store, &device_segments).len(), 3);

    let keyspace = store
        .metadata()
        .create_keyspace(MetadataCreateKeyspaceRequest {
            request: CreateKeyspaceRequest { name: None },
        })
        .unwrap();
    let file = store
        .metadata()
        .create_file(MetadataCreateFileRequest {
            keyspace_id: keyspace.keyspace_id,
            request: CreateFileRequest {
                spec: FileSpec { name: None },
            },
        })
        .unwrap();
    for byte in [4, 5, 6] {
        append_local_store_once(
            &store,
            keyspace.keyspace_id,
            file.file_id,
            &repeated_blocks(1, byte),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    }
    let mut file_bytes = vec![0; 3 * 4096];
    store
        .read_file(
            keyspace.keyspace_id,
            file.file_id,
            ByteRange::new(0, 3 * 4096),
            &mut file_bytes,
        )
        .unwrap();
    assert_eq!(&file_bytes[0..4096], repeated_blocks(1, 4).as_slice());
    assert_eq!(&file_bytes[4096..8192], repeated_blocks(1, 5).as_slice());
    assert_eq!(&file_bytes[8192..12288], repeated_blocks(1, 6).as_slice());
    let file_segments = file_segment_ids(&store.metadata(), keyspace.keyspace_id, file.file_id);
    assert!(file_segments.is_empty());
    assert_eq!(
        run_storage_nodes(&file_run_extents(
            &store.metadata(),
            keyspace.keyspace_id,
            file.file_id
        ))
        .len(),
        3
    );
}

#[test]
fn append_stream_storage_lanes_are_stable_and_spread_across_streams() {
    let cfg = config();
    let node_ids = vec![
        cfg.storage_node,
        StorageNodeId::from_raw(78),
        StorageNodeId::from_raw(79),
    ];
    let store = LocalCoordinator::with_storage_nodes(cfg, node_ids.clone()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_a, _) = create_local_file(&client, keyspace_id);
    let (file_b, _) = create_local_file(&client, keyspace_id);
    let (file_c, _) = create_local_file(&client, keyspace_id);

    let stream_a = store.open_append_stream(keyspace_id, file_a).unwrap();
    for byte in [1, 2, 3] {
        store
            .append_stream(
                &stream_a,
                &repeated_blocks(1, byte),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }

    let stream_b = store.open_append_stream(keyspace_id, file_b).unwrap();
    store
        .append_stream(
            &stream_b,
            &repeated_blocks(1, 4),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let stream_c = store.open_append_stream(keyspace_id, file_c).unwrap();
    store
        .append_stream(
            &stream_c,
            &repeated_blocks(1, 5),
            WriteDurability::Acknowledged,
        )
        .unwrap();

    let state = store.metadata().state_inner().unwrap();
    let stream_a_records = &state
        .append_streams
        .get(&stream_a.stream_id)
        .unwrap()
        .records;
    assert_eq!(
        stream_a_records.len(),
        1,
        "one stream should keep a stable append lane and coalesce adjacent runs"
    );
    assert_eq!(stream_a_records[0].len, 3 * 4096);

    let assigned_nodes: BTreeSet<_> = [stream_a, stream_b, stream_c]
        .iter()
        .map(|stream| {
            state
                .append_streams
                .get(&stream.stream_id)
                .unwrap()
                .records
                .first()
                .unwrap()
                .run
                .storage_node
        })
        .collect();
    assert_eq!(
        assigned_nodes,
        node_ids.iter().copied().collect::<BTreeSet<_>>()
    );
}

#[test]
fn append_stream_prefix_persist_batches_are_bounded_per_storage_node_lane() {
    let cfg = config();
    let node_ids = vec![
        cfg.storage_node,
        StorageNodeId::from_raw(78),
        StorageNodeId::from_raw(79),
    ];
    let store = LocalCoordinator::with_storage_nodes(cfg, node_ids.clone()).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let mut streams = Vec::new();
    for byte in [11, 12, 13] {
        let (file_id, _) = create_local_file(&client, keyspace_id);
        let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
        store
            .append_stream(
                &stream,
                &repeated_blocks(1, byte),
                WriteDurability::Acknowledged,
            )
            .unwrap();
        streams.push(stream);
    }

    let requests: Vec<_> = streams
        .iter()
        .map(|stream| (stream.clone(), stream.visible_base_size + 4096))
        .collect();
    let plans = store
        .metadata()
        .append_stream_prefix_persist_plans_for(&requests, 4096)
        .unwrap();
    assert_eq!(
        plans.len(),
        3,
        "one 4KiB prefix from each storage-node lane should share one physical batch"
    );
    assert_eq!(
        plans
            .iter()
            .map(|plan| plan.batch.payload_bytes)
            .sum::<u64>(),
        3 * 4096
    );
    let planned_nodes: BTreeSet<_> = plans
        .iter()
        .flat_map(|plan| {
            plan.batch
                .records
                .iter()
                .map(|record| record.run.storage_node)
        })
        .collect();
    assert_eq!(
        planned_nodes,
        node_ids.iter().copied().collect::<BTreeSet<_>>()
    );
}

#[test]
fn append_stream_prefix_persist_slices_coalesced_record_after_partial_durable_mark() {
    let cfg = config();
    let store = LocalCoordinator::with_config(cfg).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, _) = create_local_file(&client, keyspace_id);
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 14),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 15),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store
        .metadata()
        .mark_append_stream_durable_through(&stream, 4096)
        .unwrap();

    let plans = store
        .metadata()
        .append_stream_prefix_persist_plans_for(&[(stream, 8192)], 4096)
        .unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].batch.durable_through, 8192);
    assert_eq!(plans[0].batch.payload_bytes, 4096);
    assert_eq!(plans[0].batch.records.len(), 1);
    assert_eq!(plans[0].batch.records[0].offset, 4096);
    assert_eq!(plans[0].batch.records[0].len, 4096);
}

#[test]
fn append_stream_durable_mark_ignores_stale_lower_high_water() {
    let cfg = config();
    let store = LocalCoordinator::with_config(cfg).unwrap();
    let client = create_native_client(&store);
    let keyspace_id = create_local_keyspace(&client);
    let (file_id, _) = create_local_file(&client, keyspace_id);
    let stream = store.open_append_stream(keyspace_id, file_id).unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(2, 16),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    store
        .metadata()
        .mark_append_stream_durable_through(&stream, 8192)
        .unwrap();
    assert_eq!(
        store
            .metadata()
            .mark_append_stream_durable_through(&stream, 4096)
            .unwrap(),
        8192
    );
    assert_eq!(
        store
            .metadata()
            .append_stream_durable_high_water_if_reached(&stream, 8192)
            .unwrap(),
        Some(8192)
    );
}

#[test]
fn storage_node_transport_write_receipt_stays_pending_until_reference_message() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let intent = SegmentReservationIntent {
        write_intent: WriteIntentId::from_raw(88),
        owner: MappingOwner::BlockDevice(DeviceId::from_raw(99)),
        bytes: 4096,
    };
    let authority = LocalGrantReceiptAuthority;
    let grant = authority
        .issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::Internal {
                owner: intent.owner,
            },
            write_intent: intent.write_intent,
            segment_id,
            storage_node: cfg.storage_node,
            max_bytes: intent.bytes,
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
        .unwrap();

    let response = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::WriteSegment {
            grant,
            bytes: repeated_blocks(1, 12),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected write-segment receipt");
    };

    assert_eq!(receipt.segment_id, segment_id);
    assert_eq!(receipt.placement.storage_node, cfg.storage_node);
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );

    let evidence = authority
        .create_reference_evidence(&receipt, CommitSeq::from_raw(1))
        .unwrap();
    let response = registry
        .transport_for_segment(segment_id)
        .unwrap()
        .send(StorageNodeRequest::MarkReferenced { evidence })
        .unwrap();
    assert_eq!(response, StorageNodeResponse::MarkReferenced);
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::Referenced
    );
}

#[test]
fn grants_and_receipts_reject_scope_and_proof_corruption() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let owner = MappingOwner::BlockDevice(DeviceId::from_raw(99));
    let mut grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(99),
        owner,
        4096,
    );
    grant.tenant = TenantId::from_raw(404);
    assert!(
        registry
            .transport_for_node(cfg.storage_node)
            .unwrap()
            .send(StorageNodeRequest::WriteSegment {
                grant,
                bytes: repeated_blocks(1, 1),
            })
            .is_err()
    );

    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(99),
        owner,
        4096,
    );
    let response = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::WriteSegment {
            grant,
            bytes: repeated_blocks(1, 2),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { mut receipt } = response else {
        panic!("expected receipt");
    };
    LocalGrantReceiptAuthority
        .verify_segment_receipt(&receipt)
        .unwrap();
    receipt.proof.0[0] ^= 0xff;
    assert!(
        LocalGrantReceiptAuthority
            .verify_segment_receipt(&receipt)
            .is_err()
    );
}

#[test]
fn grants_and_receipts_reject_signed_semantic_mismatches() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let owner = MappingOwner::BlockDevice(DeviceId::from_raw(99));
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(991),
        owner,
        4096,
    );

    let mut wrong_owner_grant = grant.clone();
    wrong_owner_grant.owner = MappingOwner::BlockDevice(DeviceId::from_raw(100));
    resign_grant(&mut wrong_owner_grant);
    assert!(
        registry
            .transport_for_node(cfg.storage_node)
            .unwrap()
            .send(StorageNodeRequest::WriteSegment {
                grant: wrong_owner_grant,
                bytes: repeated_blocks(1, 3),
            })
            .is_err()
    );

    let mut stale_epoch_grant = grant.clone();
    stale_epoch_grant.grant_epoch = GrantEpoch::from_raw(0);
    resign_grant(&mut stale_epoch_grant);
    assert!(
        registry
            .transport_for_node(cfg.storage_node)
            .unwrap()
            .send(StorageNodeRequest::WriteSegment {
                grant: stale_epoch_grant,
                bytes: repeated_blocks(1, 3),
            })
            .is_err()
    );

    assert!(
        registry
            .transport_for_node(cfg.storage_node)
            .unwrap()
            .send(StorageNodeRequest::WriteSegment {
                grant: grant.clone(),
                bytes: repeated_blocks(1, 3)[..2048].to_vec(),
            })
            .is_err()
    );
    assert!(registry.state(segment_id).is_err());

    let response = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::WriteSegment {
            grant: grant.clone(),
            bytes: repeated_blocks(1, 3),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected receipt");
    };

    let mut wrong_owner_receipt = (*receipt).clone();
    wrong_owner_receipt.owner = MappingOwner::BlockDevice(DeviceId::from_raw(100));
    resign_receipt(&mut wrong_owner_receipt);
    assert!(
        LocalGrantReceiptAuthority
            .verify_segment_receipt(&wrong_owner_receipt)
            .is_err()
    );

    let mut stale_epoch_receipt = (*receipt).clone();
    stale_epoch_receipt.receipt_epoch = GrantEpoch::from_raw(0);
    resign_receipt(&mut stale_epoch_receipt);
    assert!(
        LocalGrantReceiptAuthority
            .verify_segment_receipt(&stale_epoch_receipt)
            .is_err()
    );

    let mut mismatched_grant_hash = (*receipt).clone();
    mismatched_grant_hash.grant_hash.0[0] ^= 0xff;
    resign_receipt(&mut mismatched_grant_hash);
    assert!(
        LocalGrantReceiptAuthority
            .verify_segment_receipt(&mismatched_grant_hash)
            .is_ok()
    );
    assert!(
        LocalGrantReceiptAuthority
            .verify_receipt_matches_grant(&grant, &mismatched_grant_hash)
            .is_err()
    );
}

#[test]
fn storage_node_retries_same_grant_idempotently_but_rejects_conflicting_bytes() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(992),
        MappingOwner::BlockDevice(DeviceId::from_raw(99)),
        4096,
    );
    let transport = registry.transport_for_node(cfg.storage_node).unwrap();
    let first = transport
        .send(StorageNodeRequest::WriteSegment {
            grant: grant.clone(),
            bytes: repeated_blocks(1, 8),
        })
        .unwrap();
    let retry = transport
        .send(StorageNodeRequest::WriteSegment {
            grant: grant.clone(),
            bytes: repeated_blocks(1, 8),
        })
        .unwrap();
    assert_eq!(retry, first);
    assert!(
        transport
            .send(StorageNodeRequest::WriteSegment {
                grant,
                bytes: repeated_blocks(1, 9),
            })
            .is_err()
    );
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
}

#[test]
fn storage_node_duplicate_retry_compares_stored_bytes_not_only_receipt_checksum() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(993),
        MappingOwner::BlockDevice(DeviceId::from_raw(99)),
        4096,
    );
    let transport = registry.transport_for_node(cfg.storage_node).unwrap();
    let original = repeated_blocks(1, 7);
    transport
        .send(StorageNodeRequest::WriteSegment {
            grant: grant.clone(),
            bytes: original.clone(),
        })
        .unwrap();

    let node = registry.node(cfg.storage_node).unwrap();
    {
        let mut inner = lock(&node.segment_store.inner).unwrap();
        let record = inner.segments.get_mut(&segment_id).unwrap();
        record.bytes = Arc::from(repeated_blocks(1, 8));
    }

    assert!(
        transport
            .send(StorageNodeRequest::WriteSegment {
                grant,
                bytes: original,
            })
            .is_err()
    );
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
}

#[test]
fn trusted_block_grant_receipt_flow_publishes_and_marks_reference() {
    let store = LocalCoordinator::with_config(LocalStoreConfig {
        shard_count: 1,
        ..config()
    })
    .unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let grant = store
        .issue_block_write_grant(
            head.device_id,
            crate::api::BlockRange::new(BlockIndex::from_raw(2), BlockCount::from_raw(1)),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let receipt = store
        .write_granted_segment(&grant, repeated_blocks(1, 55))
        .unwrap();
    let commit = store
        .submit_block_write_receipt(&grant, receipt.clone())
        .unwrap();
    assert_eq!(commit.range, ByteRange::new(2 * 4096, 4096));
    assert_eq!(
        store.storage_nodes.state(receipt.segment_id).unwrap(),
        SegmentLifecycleState::Referenced
    );

    let mut bytes = vec![0; 4096];
    store
        .read_device(head.device_id, ByteRange::new(2 * 4096, 4096), &mut bytes)
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 55));

    assert!(store.submit_block_write_receipt(&grant, receipt).is_err());
    let mut bytes_after_duplicate = vec![0; 4096];
    store
        .read_device(
            head.device_id,
            ByteRange::new(2 * 4096, 4096),
            &mut bytes_after_duplicate,
        )
        .unwrap();
    assert_eq!(bytes_after_duplicate, repeated_blocks(1, 55));
}

#[test]
fn trusted_block_grants_merge_independent_shards_from_same_generation() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let head = store.metadata().create_device(device_request()).unwrap();
    let left_range = crate::api::BlockRange::new(BlockIndex::from_raw(2), BlockCount::from_raw(1));
    let right_range =
        crate::api::BlockRange::new(BlockIndex::from_raw(10), BlockCount::from_raw(1));
    let left_grant = store
        .issue_block_write_grant(head.device_id, left_range, WriteDurability::Acknowledged)
        .unwrap();
    let right_grant = store
        .issue_block_write_grant(head.device_id, right_range, WriteDurability::Acknowledged)
        .unwrap();
    let left_receipt = store
        .write_granted_segment(&left_grant, repeated_blocks(1, 11))
        .unwrap();
    let right_receipt = store
        .write_granted_segment(&right_grant, repeated_blocks(1, 22))
        .unwrap();

    store
        .submit_block_write_receipt(&left_grant, left_receipt)
        .unwrap();
    store
        .submit_block_write_receipt(&right_grant, right_receipt)
        .unwrap();

    let mut left = vec![0; 4096];
    let mut right = vec![0; 4096];
    store
        .read_device(head.device_id, ByteRange::new(2 * 4096, 4096), &mut left)
        .unwrap();
    store
        .read_device(head.device_id, ByteRange::new(10 * 4096, 4096), &mut right)
        .unwrap();
    assert_eq!(left, repeated_blocks(1, 11));
    assert_eq!(right, repeated_blocks(1, 22));
    assert_eq!(
        store
            .metadata()
            .get_head(head.device_id)
            .unwrap()
            .generation,
        DeviceGeneration::from_raw(2)
    );
}

#[test]
fn trusted_native_grant_receipt_flow_publishes_write_and_stream_append() {
    let store = LocalCoordinator::with_config(config()).unwrap();
    let keyspace = store
        .metadata()
        .create_keyspace(MetadataCreateKeyspaceRequest {
            request: CreateKeyspaceRequest { name: None },
        })
        .unwrap();
    let file = store
        .metadata()
        .create_file(MetadataCreateFileRequest {
            keyspace_id: keyspace.keyspace_id,
            request: CreateFileRequest {
                spec: FileSpec { name: None },
            },
        })
        .unwrap();
    let stream = store
        .open_append_stream(keyspace.keyspace_id, file.file_id)
        .unwrap();
    store
        .append_stream(
            &stream,
            &repeated_blocks(1, 21),
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let append = store
        .publish_append_stream(&stream, 4096, WriteDurability::Acknowledged)
        .unwrap();
    assert_eq!(append.range, ByteRange::new(0, 4096));
    assert_eq!(
        store
            .metadata()
            .get_file_head(keyspace.keyspace_id, file.file_id)
            .unwrap()
            .size,
        4096
    );

    let write_grant = store
        .issue_native_write_grant(
            keyspace.keyspace_id,
            file.file_id,
            ByteRange::new(0, 4096),
            4096,
            WriteDurability::Acknowledged,
        )
        .unwrap();
    let write_receipt = store
        .write_granted_segment(&write_grant, repeated_blocks(1, 66))
        .unwrap();
    let write = store
        .submit_native_write_receipt(&write_grant, write_receipt.clone())
        .unwrap();
    assert_eq!(write.range, ByteRange::new(0, 4096));
    assert_eq!(
        store.storage_nodes.state(write_receipt.segment_id).unwrap(),
        SegmentLifecycleState::Referenced
    );
    assert!(
        store
            .submit_native_write_receipt(&write_grant, write_receipt.clone())
            .is_err()
    );

    let mut bytes = vec![0; 4096];
    store
        .read_file(
            keyspace.keyspace_id,
            file.file_id,
            ByteRange::new(0, 4096),
            &mut bytes,
        )
        .unwrap();
    assert_eq!(bytes, repeated_blocks(1, 66));
}

#[test]
fn generated_trusted_block_receipt_flow_matches_normal_writes() {
    for seed in 0..8 {
        let mut harness = crate::sim::DeterministicHarness::new(seed);
        let cfg = LocalStoreConfig {
            shard_count: 1,
            ..config()
        };
        let normal = LocalCoordinator::with_config(cfg).unwrap();
        let trusted = LocalCoordinator::with_config(cfg).unwrap();
        let normal_head = normal.metadata().create_device(device_request()).unwrap();
        let trusted_head = trusted.metadata().create_device(device_request()).unwrap();
        for step in 0..24 {
            let block = harness.rng.next_u64() % 16;
            let byte = (step as u8).wrapping_add((seed as u8) << 1);
            let payload = repeated_blocks(1, byte);
            normal
                .write_device(
                    normal_head.device_id,
                    block * 4096,
                    &payload,
                    WriteDurability::Acknowledged,
                )
                .unwrap();
            let grant = trusted
                .issue_block_write_grant(
                    trusted_head.device_id,
                    crate::api::BlockRange::new(
                        BlockIndex::from_raw(block),
                        BlockCount::from_raw(1),
                    ),
                    WriteDurability::Acknowledged,
                )
                .unwrap();
            let receipt = trusted
                .write_granted_segment(&grant, payload.clone())
                .unwrap();
            trusted.submit_block_write_receipt(&grant, receipt).unwrap();
            harness
                .trace
                .record(format!("write block {block} byte {byte}"));
        }

        let mut normal_bytes = vec![0; 16 * 4096];
        let mut trusted_bytes = vec![0; 16 * 4096];
        normal
            .read_device(
                normal_head.device_id,
                ByteRange::new(0, 16 * 4096),
                &mut normal_bytes,
            )
            .unwrap();
        trusted
            .read_device(
                trusted_head.device_id,
                ByteRange::new(0, 16 * 4096),
                &mut trusted_bytes,
            )
            .unwrap();
        assert_eq!(
            trusted_bytes,
            normal_bytes,
            "seed {seed} trace {:?}",
            harness.trace.events()
        );
    }
}

#[test]
fn storage_node_rejects_reference_without_metadata_evidence() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(123),
        MappingOwner::BlockDevice(DeviceId::from_raw(8)),
        4096,
    );
    let response = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::WriteSegment {
            grant,
            bytes: repeated_blocks(1, 9),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected receipt");
    };
    let mut evidence = LocalGrantReceiptAuthority
        .create_reference_evidence(&receipt, CommitSeq::from_raw(1))
        .unwrap();
    evidence.proof.0[0] ^= 0xff;
    assert!(
        registry
            .transport_for_segment(segment_id)
            .unwrap()
            .send(StorageNodeRequest::MarkReferenced { evidence })
            .is_err()
    );
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::DurablePendingMetadata
    );
}

#[test]
fn chaos_storage_node_transport_exercises_duplicate_delay_and_corruption() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let inner = registry.transport_for_node(cfg.storage_node).unwrap();
    let chaos = ChaosStorageNodeTransport::new(inner);
    let segment_id = registry.allocate_segment_id().unwrap();
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(321),
        MappingOwner::BlockDevice(DeviceId::from_raw(8)),
        4096,
    );

    chaos.duplicate_next_request().unwrap();
    let response = chaos
        .send(StorageNodeRequest::WriteSegment {
            grant: grant.clone(),
            bytes: repeated_blocks(1, 10),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected receipt");
    };
    LocalGrantReceiptAuthority
        .verify_segment_receipt(&receipt)
        .unwrap();
    assert_eq!(chaos.metrics().unwrap().duplicated_requests, 1);

    let delayed_segment = registry.allocate_segment_id().unwrap();
    let delayed_grant = grant_for_segment(
        cfg.storage_node,
        delayed_segment,
        WriteIntentId::from_raw(322),
        MappingOwner::BlockDevice(DeviceId::from_raw(8)),
        4096,
    );
    chaos.delay_next_response().unwrap();
    assert!(
        chaos
            .send(StorageNodeRequest::WriteSegment {
                grant: delayed_grant.clone(),
                bytes: repeated_blocks(1, 11),
            })
            .is_err()
    );
    assert_eq!(chaos.delayed_len().unwrap(), 1);
    chaos.return_delayed_response_next_call().unwrap();
    let delayed = chaos.send(StorageNodeRequest::ObserveMaintenance).unwrap();
    assert!(matches!(delayed, StorageNodeResponse::WriteSegment { .. }));

    let corrupt_segment = registry.allocate_segment_id().unwrap();
    let corrupt_grant = grant_for_segment(
        cfg.storage_node,
        corrupt_segment,
        WriteIntentId::from_raw(323),
        MappingOwner::BlockDevice(DeviceId::from_raw(8)),
        4096,
    );
    chaos.corrupt_next_receipt().unwrap();
    let response = chaos
        .send(StorageNodeRequest::WriteSegment {
            grant: corrupt_grant,
            bytes: repeated_blocks(1, 12),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected corrupted receipt");
    };
    assert!(
        LocalGrantReceiptAuthority
            .verify_segment_receipt(&receipt)
            .is_err()
    );
}

#[test]
fn storage_node_maintenance_messages_return_typed_reports() {
    let cfg = config();
    let registry = StorageNodeRegistry::new(cfg, vec![cfg.storage_node]).unwrap();
    let segment_id = registry.allocate_segment_id().unwrap();
    let grant = grant_for_segment(
        cfg.storage_node,
        segment_id,
        WriteIntentId::from_raw(444),
        MappingOwner::BlockDevice(DeviceId::from_raw(8)),
        4096,
    );
    let response = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::WriteSegment {
            grant,
            bytes: repeated_blocks(1, 13),
        })
        .unwrap();
    let StorageNodeResponse::WriteSegment { receipt } = response else {
        panic!("expected receipt");
    };
    let evidence = LocalGrantReceiptAuthority
        .create_reference_evidence(&receipt, CommitSeq::from_raw(1))
        .unwrap();
    registry
        .transport_for_segment(segment_id)
        .unwrap()
        .send(StorageNodeRequest::MarkReferenced { evidence })
        .unwrap();
    registry.release_segment(segment_id).unwrap();

    let observed = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::ObserveMaintenance)
        .unwrap();
    let StorageNodeResponse::MaintenanceObserved(observed) = observed else {
        panic!("expected maintenance observation");
    };
    assert_eq!(observed.released_segments, 1);

    let tick = registry
        .transport_for_node(cfg.storage_node)
        .unwrap()
        .send(StorageNodeRequest::RunMaintenanceTick)
        .unwrap();
    let StorageNodeResponse::MaintenanceTicked(report) = tick else {
        panic!("expected maintenance report");
    };
    assert_eq!(report.deleted_released_segments, vec![segment_id]);
    assert_eq!(
        registry.state(segment_id).unwrap(),
        SegmentLifecycleState::Freed
    );
}

#[test]
fn local_multi_node_custodian_reclaims_released_segments_on_owning_node_only() {
    let cfg = config();
    let store = LocalCoordinator::with_storage_nodes(
        cfg,
        vec![
            cfg.storage_node,
            StorageNodeId::from_raw(78),
            StorageNodeId::from_raw(79),
        ],
    )
    .unwrap();
    let device = store.metadata().create_device(device_request()).unwrap();
    for block in 0..3 {
        store
            .write_device(
                device.device_id,
                block * 4096,
                &repeated_blocks(1, (block + 1) as u8),
                WriteDurability::Acknowledged,
            )
            .unwrap();
    }
    let segments = device_segment_ids(&store.metadata(), device.device_id);
    let released = segments[1];
    let owner = store
        .storage_nodes
        .commit_for_segment(released)
        .unwrap()
        .placement
        .storage_node;
    let other_nodes: Vec<_> = store
        .storage_node_ids_for_test()
        .into_iter()
        .filter(|node_id| *node_id != owner)
        .collect();

    store.delete_device(device.device_id).unwrap();
    store
        .run_metadata_custodian(RetentionPolicy::expire_deleted_immediately())
        .unwrap();
    assert_eq!(
        store
            .segment_catalog_for_node(owner)
            .unwrap()
            .state(released)
            .unwrap(),
        SegmentLifecycleState::Released
    );
    let report = store.run_storage_node_custodian(&BTreeSet::new()).unwrap();
    assert!(report.deleted_released_segments.contains(&released));
    assert!(
        !store
            .segment_store_for_node(owner)
            .unwrap()
            .contains_segment(released)
            .unwrap()
    );
    for node_id in other_nodes {
        assert!(
            !store
                .segment_catalog_for_node(node_id)
                .unwrap()
                .contains_segment(released)
                .unwrap()
        );
    }
}

#[test]
fn local_multi_node_registry_rejects_duplicate_segment_ownership() {
    let cfg = config();
    let store = LocalCoordinator::with_storage_nodes(
        cfg,
        vec![cfg.storage_node, StorageNodeId::from_raw(78)],
    )
    .unwrap();
    let segment_id = SegmentId::from_raw(900);
    for node_id in [cfg.storage_node, StorageNodeId::from_raw(78)] {
        store
            .segment_catalog_for_node(node_id)
            .unwrap()
            .reserve_segment_with_id(segment_id, reservation_intent())
            .unwrap();
    }

    let error = store
        .storage_nodes
        .commit_for_segment(segment_id)
        .unwrap_err();
    assert!(matches!(error, StorageError::Corrupt { .. }));
}

#[test]
fn leaf_entries_can_reference_local_segment_descriptors_for_validation() {
    let store = InMemorySegmentStore::new(config()).unwrap();
    let reservation = SegmentReservation {
        segment_id: SegmentId::from_raw(77),
        bytes: 4096,
    };
    let commit = store.write_segment(&reservation, &[3; 4096]).unwrap();
    let node = MetadataNode {
        node_id: MetadataNodeId::from_raw(77),
        covered_range: crate::api::BlockRange::new(
            BlockIndex::from_raw(0),
            BlockCount::from_raw(1),
        ),
        kind: MetadataNodeKind::Leaf {
            entries: vec![LeafEntry {
                logical_start: BlockIndex::from_raw(0),
                blocks: BlockCount::from_raw(1),
                segment_id: commit.descriptor.segment_id,
                segment_offset: BlockIndex::from_raw(0),
            }],
            run_extents: Vec::new(),
        },
    };

    assert!(node.validate(&[commit.descriptor]).is_ok());
}

fn create_local_device(store: &LocalCoordinator, logical_blocks: u64) -> LocalBlockDevice {
    let server = Arc::new(LocalBlockServer::new(store.clone()));
    let client = LocalBlockClient::new(InProcessBlockTransport::new(server));
    let device_id = client
        .create_device(CreateDeviceRequest {
            spec: DeviceSpec {
                logical_blocks,
                block_size: 4096,
            },
            name: None,
        })
        .unwrap();
    client.open_device(device_id).unwrap()
}

fn create_native_client(store: &LocalCoordinator) -> LocalNativeClient {
    let server = Arc::new(LocalNativeServer::new(store.clone()));
    LocalNativeClient::new(InProcessNativeTransport::new(server))
}

fn create_local_keyspace(client: &LocalNativeClient) -> KeyspaceId {
    client
        .create_keyspace(CreateKeyspaceRequest { name: None })
        .unwrap()
}

fn create_local_file(
    client: &LocalNativeClient,
    keyspace_id: KeyspaceId,
) -> (FileId, LocalNativeFile) {
    let file_id = client
        .create_file(
            keyspace_id,
            CreateFileRequest {
                spec: FileSpec { name: None },
            },
        )
        .unwrap();
    let file = client.open_file(keyspace_id, file_id).unwrap();
    (file_id, file)
}

fn append_native_file_once(file: &LocalNativeFile, data: &[u8]) -> Result<AppendPublishCommit> {
    let stream = file.open_append_stream()?;
    append_native_file_with_stream(file, &stream, data)
}

fn append_native_file_with_stream(
    file: &LocalNativeFile,
    stream: &AppendStream,
    data: &[u8],
) -> Result<AppendPublishCommit> {
    let ticket = file.append_stream(stream, data)?;
    file.publish_append_stream(stream, ticket.range.end_exclusive()?)
}

fn append_local_store_once(
    store: &LocalCoordinator,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    data: &[u8],
    durability: WriteDurability,
) -> Result<AppendPublishCommit> {
    let stream = store.open_append_stream(keyspace_id, file_id)?;
    append_local_store_with_stream(store, &stream, data, durability)
}

fn append_local_store_with_stream(
    store: &LocalCoordinator,
    stream: &AppendStream,
    data: &[u8],
    durability: WriteDurability,
) -> Result<AppendPublishCommit> {
    let ticket = store.append_stream(stream, data, durability)?;
    store.publish_append_stream(stream, ticket.range.end_exclusive()?, durability)
}

fn append_durable_store_once(
    store: &DurableCoordinator,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    data: &[u8],
    durability: WriteDurability,
) -> Result<AppendPublishCommit> {
    let stream = store.open_append_stream(keyspace_id, file_id)?;
    append_durable_store_with_stream(store, &stream, data, durability)
}

fn append_durable_store_with_stream(
    store: &DurableCoordinator,
    stream: &AppendStream,
    data: &[u8],
    durability: WriteDurability,
) -> Result<AppendPublishCommit> {
    let ticket = store.append_stream(stream, data, durability)?;
    store.publish_append_stream(stream, ticket.range.end_exclusive()?)
}

fn repeated_blocks(blocks: u64, byte: u8) -> Vec<u8> {
    vec![byte; blocks as usize * 4096]
}

fn device_shard_payloads_for_test(conn: &Connection) -> BTreeMap<String, Vec<u8>> {
    let mut stmt = conn
        .prepare("SELECT row_key, payload FROM device_shard_heads ORDER BY row_key")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next().unwrap() {
        out.insert(row.get(0).unwrap(), row.get(1).unwrap());
    }
    out
}

fn block_delta_commit_count(root: &Path) -> i64 {
    let conn = Connection::open(root.join("metadata.sqlite")).unwrap();
    conn.query_row("SELECT COUNT(*) FROM block_delta_commits", [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn assert_metadata_only_block_delta_profiles(profiles: &[DurablePersistProfile], bytes: u64) {
    assert!(!profiles.is_empty());
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.block_delta_selected_bytes)
            .sum::<u64>(),
        bytes
    );
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.data_log_write_bytes)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.data_log_sync_bytes)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.new_segment_bytes)
            .sum::<u64>(),
        0
    );
}

fn keyspace_shard_payloads_for_test(conn: &Connection) -> BTreeMap<String, Vec<u8>> {
    let mut stmt = conn
        .prepare("SELECT row_key, payload FROM keyspace_shard_heads ORDER BY row_key")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next().unwrap() {
        out.insert(row.get(0).unwrap(), row.get(1).unwrap());
    }
    out
}

fn changed_payload_count(
    before: &BTreeMap<String, Vec<u8>>,
    after: &BTreeMap<String, Vec<u8>>,
) -> usize {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    before
        .iter()
        .filter(|(key, payload)| after.get(*key) != Some(*payload))
        .count()
}

fn first_device_segment(store: &DurableCoordinator, device_id: DeviceId) -> SegmentId {
    device_segment_ids(&store.metadata(), device_id)
        .into_iter()
        .next()
        .unwrap()
}

fn corrupt_in_memory_segment_payload(
    store: &DurableCoordinator,
    placement: &SegmentPlacementRow,
    segment_id: SegmentId,
) {
    let segment_store = store
        .local
        .segment_store_for_node(placement.storage_node)
        .unwrap();
    let mut inner = lock(&segment_store.inner).unwrap();
    let record = inner.segments.get_mut(&segment_id).unwrap();
    Arc::make_mut(&mut record.bytes)[0] ^= 0xff;
}

fn node_catalog_conn(root: &Path, storage_node: StorageNodeId) -> Connection {
    Connection::open(node_catalog_path(&root.join("data"), storage_node)).unwrap()
}

fn node_catalog_entry(
    root: &Path,
    storage_node: StorageNodeId,
    segment_id: SegmentId,
) -> CatalogEntry {
    let conn = node_catalog_conn(root, storage_node);
    let table = node_catalog_table(storage_node, "segment_catalog_entries").unwrap();
    let payload: Vec<u8> = conn
        .query_row(
            &format!("SELECT payload FROM {table} WHERE segment_id = ?1"),
            params![segment_id.raw().to_string()],
            |row| row.get(0),
        )
        .unwrap();
    decode_row(&payload).unwrap()
}

fn metadata_file_snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "native-publish.journal",
        "append-visible-publish.journal",
    ]
    .into_iter()
    .filter_map(|file_name| {
        fs::read(root.join(file_name))
            .ok()
            .map(|bytes| (file_name.to_string(), bytes))
    })
    .collect()
}

fn restore_metadata_file_snapshot(root: &Path, snapshot: &[(String, Vec<u8>)]) {
    for file_name in [
        "metadata.sqlite",
        "metadata.sqlite-wal",
        "metadata.sqlite-shm",
        "native-publish.journal",
        "append-visible-publish.journal",
    ] {
        let _ = fs::remove_file(root.join(file_name));
    }
    for (file_name, bytes) in snapshot {
        fs::write(root.join(file_name), bytes).unwrap();
    }
}

fn native_publish_journal_commit_count(root: &Path, next_commit_seq: u64) -> usize {
    load_native_publish_journal_commits_since(&root.join("native-publish.journal"), next_commit_seq)
        .unwrap()
        .len()
}

fn append_publish_lane_index_for_test(file_id: FileId) -> usize {
    InMemoryMetadataPlane::keyspace_catalog_shard_index_for_len(
        file_id,
        KEYSPACE_CATALOG_SHARD_COUNT,
    )
    .unwrap()
}

fn create_file_in_same_append_publish_lane_for_test(
    store: &DurableCoordinator,
    keyspace_id: KeyspaceId,
    anchor: FileId,
    name: &str,
) -> FileId {
    let target_lane = append_publish_lane_index_for_test(anchor);
    let mut attempt = 0_u64;
    loop {
        attempt += 1;
        let file_id = store
            .create_file(
                keyspace_id,
                CreateFileRequest {
                    spec: FileSpec {
                        name: Some(format!("{name}-{target_lane}-{attempt}")),
                    },
                },
            )
            .unwrap();
        if file_id != anchor && append_publish_lane_index_for_test(file_id) == target_lane {
            return file_id;
        }
    }
}

fn create_file_in_different_append_publish_lane_for_test(
    store: &DurableCoordinator,
    keyspace_id: KeyspaceId,
    anchor: FileId,
    name: &str,
) -> FileId {
    let anchor_lane = append_publish_lane_index_for_test(anchor);
    let mut attempt = 0_u64;
    loop {
        attempt += 1;
        let file_id = store
            .create_file(
                keyspace_id,
                CreateFileRequest {
                    spec: FileSpec {
                        name: Some(format!("{name}-{anchor_lane}-{attempt}")),
                    },
                },
            )
            .unwrap();
        if append_publish_lane_index_for_test(file_id) != anchor_lane {
            return file_id;
        }
    }
}

fn append_visible_publish_journal_path_for_test(
    root: &Path,
    _keyspace_id: KeyspaceId,
    file_id: FileId,
) -> PathBuf {
    append_visible_publish_journal_path_for_lane(
        &root.join("append-visible-publish.journal"),
        append_publish_lane_index_for_test(file_id),
    )
    .unwrap()
}

fn append_visible_publish_journal_count(
    root: &Path,
    keyspace_id: KeyspaceId,
    file_id: FileId,
) -> usize {
    load_append_visible_publish_journal_records(&append_visible_publish_journal_path_for_test(
        root,
        keyspace_id,
        file_id,
    ))
    .unwrap()
    .len()
}

fn device_segment_ids(
    metadata: &Arc<InMemoryMetadataPlane>,
    device_id: DeviceId,
) -> Vec<SegmentId> {
    let mut out = Vec::new();
    for root in metadata.get_head(device_id).unwrap().shard_roots {
        collect_tree_segments(metadata, root, &mut out);
    }
    out.sort();
    out.dedup();
    out
}

fn file_segment_ids(
    metadata: &Arc<InMemoryMetadataPlane>,
    keyspace_id: KeyspaceId,
    file_id: FileId,
) -> Vec<SegmentId> {
    let mut out = Vec::new();
    let head = metadata.get_file_head(keyspace_id, file_id).unwrap();
    collect_tree_segments(metadata, head.root, &mut out);
    out.sort();
    out.dedup();
    out
}

fn file_run_extents(
    metadata: &Arc<InMemoryMetadataPlane>,
    keyspace_id: KeyspaceId,
    file_id: FileId,
) -> Vec<RunBackedFileExtent> {
    let mut out = Vec::new();
    let head = metadata.get_file_head(keyspace_id, file_id).unwrap();
    collect_tree_run_extents(metadata, head.root, &mut out);
    out.sort_by_key(|extent| extent.file_offset_start);
    out
}

fn collect_tree_segments(
    metadata: &Arc<InMemoryMetadataPlane>,
    node_id: MetadataNodeId,
    out: &mut Vec<SegmentId>,
) {
    let node = metadata.get_metadata_node(node_id).unwrap();
    match node.kind {
        MetadataNodeKind::Leaf { entries, .. } => {
            out.extend(entries.into_iter().map(|entry| entry.segment_id));
        }
        MetadataNodeKind::Internal { children } => {
            for child in children {
                collect_tree_segments(metadata, child.node_id, out);
            }
        }
    }
}

fn collect_tree_run_extents(
    metadata: &Arc<InMemoryMetadataPlane>,
    node_id: MetadataNodeId,
    out: &mut Vec<RunBackedFileExtent>,
) {
    let node = metadata.get_metadata_node(node_id).unwrap();
    match node.kind {
        MetadataNodeKind::Leaf { run_extents, .. } => {
            out.extend(run_extents);
        }
        MetadataNodeKind::Internal { children } => {
            for child in children {
                collect_tree_run_extents(metadata, child.node_id, out);
            }
        }
    }
}

fn run_storage_nodes(extents: &[RunBackedFileExtent]) -> BTreeSet<StorageNodeId> {
    extents
        .iter()
        .map(|extent| extent.run.storage_node)
        .collect()
}

fn segment_storage_nodes(
    store: &LocalCoordinator,
    segment_ids: &[SegmentId],
) -> BTreeSet<StorageNodeId> {
    segment_ids
        .iter()
        .map(|segment_id| {
            store
                .storage_nodes
                .commit_for_segment(*segment_id)
                .unwrap()
                .placement
                .storage_node
        })
        .collect()
}

fn read_device_bytes(device: &LocalBlockDevice, blocks: u64) -> Vec<u8> {
    let mut out = vec![0; blocks as usize * 4096];
    device.read_at(0, &mut out).unwrap();
    out
}

fn read_file_bytes(file: &LocalNativeFile, blocks: u64) -> Vec<u8> {
    let mut out = vec![0; blocks as usize * 4096];
    file.read_at(0, &mut out).unwrap();
    out
}

fn changed_catalog_shards(before: &KeyspaceRoot, after: &KeyspaceRoot) -> usize {
    assert_eq!(before.shard_roots.len(), after.shard_roots.len());
    before
        .shard_roots
        .iter()
        .zip(after.shard_roots.iter())
        .filter(|(before, after)| before != after)
        .count()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeFileReference {
    bytes: Vec<u8>,
    version: FileVersion,
}

impl NativeFileReference {
    fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            version: FileVersion::from_raw(0),
        }
    }
}

fn apply_model_write(model: &mut Vec<u8>, offset: usize, payload: &[u8]) {
    let end = offset + payload.len();
    if end > model.len() {
        model.resize(end, 0);
    }
    model[offset..end].copy_from_slice(payload);
}

fn validate_device_roots(store: &LocalCoordinator, device_id: DeviceId) {
    let head = store.metadata().get_head(device_id).unwrap();
    for root in head.shard_roots {
        store.validate_metadata_tree(root).unwrap();
    }
}

fn render_device_roots(store: &LocalCoordinator, device_id: DeviceId) -> String {
    let head = store.metadata().get_head(device_id).unwrap();
    let mut out = String::new();
    for (shard, root) in head.shard_roots.iter().enumerate() {
        out.push_str(&format!("shard {shard}\n"));
        out.push_str(&store.render_metadata_tree(*root).unwrap());
    }
    out
}

fn assert_model_blocks(actual: &[u8], model: &[u8], seed: u64, trace: &[String], tree: &str) {
    assert_eq!(actual.len(), model.len() * 4096);
    for (block, expected) in model.iter().copied().enumerate() {
        let start = block * 4096;
        let end = start + 4096;
        if actual[start..end].iter().any(|byte| *byte != expected) {
            panic!(
                "seed {seed} block {block} expected byte {expected}\ntrace:\n{}\ntree:\n{tree}",
                trace.join("\n")
            );
        }
    }
}

fn assert_model_bytes(actual: &[u8], model: &[u8], seed: u64, trace: &[String], tree: &str) {
    if actual == model {
        return;
    }
    let mismatch = actual
        .iter()
        .zip(model.iter())
        .position(|(actual, expected)| actual != expected)
        .unwrap_or_else(|| actual.len().min(model.len()));
    let actual_byte = actual.get(mismatch).copied();
    let expected_byte = model.get(mismatch).copied();
    panic!(
        "seed {seed} byte {mismatch} expected {expected_byte:?} actual {actual_byte:?} expected_len={} actual_len={}\ntrace:\n{}\ntree:\n{tree}",
        model.len(),
        actual.len(),
        trace.join("\n")
    );
}
