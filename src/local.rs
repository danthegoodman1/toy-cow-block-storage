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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

use crate::api::{
    BlockClient, BlockDevice, BlockRequest, BlockRequestEnvelope, BlockResponse,
    BlockResponseEnvelope, BlockServer, BlockTransport, ByteRange, CreateDeviceRequest,
    DeleteResult, DeviceInfo, FlushResult, FlushScope, ForkRequest, ReadResponse, RestorePoint,
    WriteCommit, WriteDurability,
};
use crate::error::{Result, StorageError};
use crate::extent::{
    AppendCommit, AppendReservation, AppendSession, CreateFileRequest, CreateKeyspaceRequest,
    FileInfo, FileSpec, FileWriteCommit, KeyspaceInfo, NativeFile, NativeKeyspaceClient,
    NativeRequest, NativeRequestEnvelope, NativeResponse, NativeResponseEnvelope, NativeServer,
    NativeTransport, SnapshotKeyspaceRequest,
};
use crate::id::{
    AppendReservationId, AppendSessionId, BlockCount, BlockIndex, CheckpointId, ClientEpoch,
    CommitGroupId, CommitSeq, DeviceGeneration, DeviceId, ExtentId, FileId, FileVersion,
    GrantEpoch, GrantId, GrantNonce, KeyspaceCatalogShardId, KeyspaceGeneration, KeyspaceId,
    KeyspaceRootId, LogicalDeadline, LogicalTime, MetadataNodeId, PrincipalId, RequestId,
    SegmentId, ServerIncarnation, ShardId, StorageNodeId, StorageNodeKeyId, TenantId,
    WriteIntentId, WriterEpoch,
};
use crate::object::{
    Checkpoint, CheckpointRoots, CommitGroup, DeleteRecord, DeviceHead, FileCommit, FileHead,
    ForkRecord, KeyspaceCatalogShard, KeyspaceCommit, KeyspaceFile, KeyspaceHead, KeyspaceRoot,
    LeafEntry, MappingOwner, MetadataChild, MetadataNode, MetadataNodeKind, RootUpdate,
    SegmentDescriptor, ShardCommit, ShardRootUpdate,
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

const KEYSPACE_CATALOG_SHARD_COUNT: usize = 256;
const LOCAL_TENANT_ID: TenantId = TenantId::from_raw(1);
const LOCAL_PRINCIPAL_ID: PrincipalId = PrincipalId::from_raw(1);
const LOCAL_GRANT_EPOCH: GrantEpoch = GrantEpoch::from_raw(1);
const LOCAL_GRANT_EXPIRATION: LogicalDeadline = LogicalDeadline::from_raw(u64::MAX);
const LOCAL_STORAGE_NODE_INCARNATION: ServerIncarnation = ServerIncarnation::from_raw(1);
const DEFAULT_OBSERVABILITY_EVENT_CAPACITY: usize = 1024;

/// Local provider configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LocalStoreConfig {
    pub shard_count: usize,
    pub block_size: u32,
    pub file_root_blocks: u64,
    pub metadata_fanout: usize,
    pub metadata_leaf_blocks: u64,
    pub storage_node: StorageNodeId,
    pub observability_event_capacity: usize,
}

impl Default for LocalStoreConfig {
    fn default() -> Self {
        Self {
            shard_count: 1,
            block_size: 4096,
            file_root_blocks: 1,
            metadata_fanout: 4,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(1),
            observability_event_capacity: DEFAULT_OBSERVABILITY_EVENT_CAPACITY,
        }
    }
}

impl LocalStoreConfig {
    fn storage_shape_matches(self, other: Self) -> bool {
        self.shard_count == other.shard_count
            && self.block_size == other.block_size
            && self.file_root_blocks == other.file_root_blocks
            && self.metadata_fanout == other.metadata_fanout
            && self.metadata_leaf_blocks == other.metadata_leaf_blocks
            && self.storage_node == other.storage_node
    }

    fn with_observability_event_capacity(self, observability_event_capacity: usize) -> Self {
        Self {
            observability_event_capacity,
            ..self
        }
    }

    pub fn validate(self) -> Result<()> {
        if self.shard_count == 0 {
            return Err(StorageError::invalid_argument(
                "shard_count must be greater than zero",
            ));
        }

        if self.block_size == 0 {
            return Err(StorageError::invalid_argument(
                "block_size must be greater than zero",
            ));
        }

        if !self.block_size.is_power_of_two() {
            return Err(StorageError::invalid_argument(
                "block_size must be a power of two",
            ));
        }

        if self.file_root_blocks == 0 {
            return Err(StorageError::invalid_argument(
                "file_root_blocks must be greater than zero",
            ));
        }

        if self.metadata_fanout < 2 {
            return Err(StorageError::invalid_argument(
                "metadata_fanout must be at least two",
            ));
        }

        if self.metadata_leaf_blocks == 0 {
            return Err(StorageError::invalid_argument(
                "metadata_leaf_blocks must be greater than zero",
            ));
        }

        if self.observability_event_capacity == 0 {
            return Err(StorageError::invalid_argument(
                "observability_event_capacity must be greater than zero",
            ));
        }

        Ok(())
    }

    fn for_storage_node(self, storage_node: StorageNodeId) -> Self {
        Self {
            storage_node,
            ..self
        }
    }
}

fn normalize_storage_nodes(
    primary: StorageNodeId,
    storage_nodes: Vec<StorageNodeId>,
) -> Vec<StorageNodeId> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for node_id in std::iter::once(primary).chain(storage_nodes) {
        if seen.insert(node_id) {
            out.push(node_id);
        }
    }
    out
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Debug)]
struct ObservabilityInner {
    counters: DiagnosticsCounters,
    events: VecDeque<StorageEvent>,
    next_event_sequence: u64,
    capacity: usize,
}

#[derive(Debug)]
struct Observability {
    inner: Mutex<ObservabilityInner>,
}

impl Observability {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "observability_event_capacity must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Mutex::new(ObservabilityInner {
                counters: DiagnosticsCounters::default(),
                events: VecDeque::with_capacity(capacity),
                next_event_sequence: 1,
                capacity,
            }),
        })
    }

    fn increment(&self, update: impl FnOnce(&mut DiagnosticsCounters)) {
        if let Ok(mut inner) = self.inner.lock() {
            update(&mut inner.counters);
        }
    }

    fn record(&self, kind: StorageEventKind) {
        self.record_with(kind, None, None, None, None);
    }

    fn record_with(
        &self,
        kind: StorageEventKind,
        storage_node: Option<StorageNodeId>,
        segment_id: Option<SegmentId>,
        commit_seq: Option<CommitSeq>,
        reason: Option<&'static str>,
    ) {
        self.record_with_update(kind, storage_node, segment_id, commit_seq, reason, |_| {});
    }

    fn record_with_update(
        &self,
        kind: StorageEventKind,
        storage_node: Option<StorageNodeId>,
        segment_id: Option<SegmentId>,
        commit_seq: Option<CommitSeq>,
        reason: Option<&'static str>,
        update: impl FnOnce(&mut DiagnosticsCounters),
    ) {
        if let Ok(mut inner) = self.inner.lock() {
            update(&mut inner.counters);
            let sequence = inner.next_event_sequence;
            inner.next_event_sequence = inner.next_event_sequence.saturating_add(1);
            if inner.events.len() == inner.capacity {
                inner.events.pop_front();
                inner.counters.observability_events_dropped = inner
                    .counters
                    .observability_events_dropped
                    .saturating_add(1);
            }
            inner.counters.observability_events_recorded = inner
                .counters
                .observability_events_recorded
                .saturating_add(1);
            inner.events.push_back(StorageEvent {
                sequence,
                kind,
                storage_node,
                segment_id,
                commit_seq,
                reason,
            });
        }
    }

    fn snapshot_parts(&self) -> Result<(DiagnosticsCounters, Vec<StorageEvent>, u64, u64, u64)> {
        let inner = lock(&self.inner)?;
        let last_sequence = inner.next_event_sequence.saturating_sub(1);
        Ok((
            inner.counters,
            inner.events.iter().cloned().collect(),
            usize_to_u64(inner.events.len()),
            usize_to_u64(inner.capacity),
            last_sequence,
        ))
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        let mut inner = lock(&self.inner)?;
        let count = max.min(inner.events.len());
        Ok(inner.events.drain(..count).collect())
    }
}

fn receipt_rejection_reason(error: &StorageError) -> &'static str {
    match error {
        StorageError::Corrupt { .. } => "bad_proof",
        StorageError::Conflict { reason } if reason.contains("proof") => "bad_proof",
        StorageError::Conflict { reason } if reason.contains("stale") => "stale_epoch",
        StorageError::Conflict { reason } if reason.contains("duplicate") => "replay",
        StorageError::Conflict { .. } | StorageError::InvalidArgument { .. } => "scope",
        StorageError::NotFound { .. } | StorageError::Unavailable { .. } => "scope",
        StorageError::Unsupported { .. } => "unsupported",
    }
}

fn count_receipt_rejection(counters: &mut DiagnosticsCounters, reason: &'static str) {
    counters.receipt_rejections = counters.receipt_rejections.saturating_add(1);
    match reason {
        "bad_proof" => {
            counters.receipt_rejected_bad_proof =
                counters.receipt_rejected_bad_proof.saturating_add(1);
        }
        "stale_epoch" => {
            counters.receipt_rejected_epoch = counters.receipt_rejected_epoch.saturating_add(1);
        }
        "replay" => {
            counters.receipt_rejected_replay = counters.receipt_rejected_replay.saturating_add(1);
        }
        _ => {
            counters.receipt_rejected_scope = counters.receipt_rejected_scope.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Default)]
struct LocalGrantReceiptAuthority;

impl LocalGrantReceiptAuthority {
    fn node_key_id(storage_node: StorageNodeId) -> StorageNodeKeyId {
        StorageNodeKeyId::from_raw(storage_node.raw())
    }

    fn grant_id(segment_id: SegmentId) -> GrantId {
        GrantId::from_raw(segment_id.raw())
    }

    fn nonce(segment_id: SegmentId, write_intent: WriteIntentId) -> GrantNonce {
        GrantNonce::from_raw(segment_id.raw() ^ write_intent.raw().rotate_left(17))
    }

    fn verify_expected_proof(
        expected: crate::provider::ProofTag,
        proof: crate::provider::ProofTag,
    ) -> Result<()> {
        if proof != expected {
            return Err(StorageError::conflict("proof verification failed"));
        }
        Ok(())
    }

    fn verify_grant_proof_and_hash(grant: &WriteGrant) -> Result<crate::provider::GrantHash> {
        let (hash, expected_proof) = deterministic_test_grant_hash_and_proof(grant.key_id, grant);
        Self::verify_expected_proof(expected_proof, grant.proof)?;
        Ok(hash)
    }

    fn verify_receipt_proof(receipt: &SegmentWriteReceipt) -> Result<()> {
        Self::verify_expected_proof(
            deterministic_test_proof_for_receipt(receipt.node_key_id, receipt),
            receipt.proof,
        )
    }

    fn verify_reference_proof(evidence: &ReferenceEvidence) -> Result<()> {
        Self::verify_expected_proof(
            deterministic_test_proof_for_reference(evidence.node_key_id, evidence),
            evidence.proof,
        )
    }

    fn verify_not_expired(expires_at: LogicalDeadline) -> Result<()> {
        if expires_at.raw() < LOCAL_GRANT_EPOCH.raw() {
            return Err(StorageError::unavailable("write grant expired"));
        }
        Ok(())
    }

    fn verify_receipt_matches_grant(
        &self,
        grant: &WriteGrant,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        let grant_hash = self.verify_write_grant_and_hash(
            grant,
            receipt.storage_node,
            receipt.segment_id,
            receipt.bytes,
        )?;
        let verified = self.verify_segment_receipt(receipt)?;
        if receipt.grant_id != grant.grant_id
            || receipt.grant_hash != grant_hash
            || receipt.tenant != grant.tenant
            || receipt.principal != grant.principal
            || receipt.owner != grant.owner
            || receipt.intent != grant.intent
            || receipt.write_intent != grant.write_intent
            || receipt.segment_id != grant.segment_id
            || receipt.storage_node != grant.storage_node
            || receipt.bytes != grant.max_bytes
            || receipt.durability != grant.durability
            || receipt.receipt_epoch != grant.grant_epoch
            || receipt.expires_at != grant.expires_at
            || receipt.node_key_id != grant.key_id
        {
            return Err(StorageError::conflict(
                "segment receipt does not match write grant",
            ));
        }
        Ok(verified)
    }

    fn create_segment_receipt_after_verified_write(
        &self,
        grant: &WriteGrant,
        grant_hash: crate::provider::GrantHash,
        commit: SegmentReplicaCommit,
        storage_node_incarnation: ServerIncarnation,
    ) -> Result<SegmentWriteReceipt> {
        if commit.descriptor.segment_id != grant.segment_id
            || commit.placement.segment_id != grant.segment_id
        {
            return Err(StorageError::conflict(
                "receipt commit does not match granted segment",
            ));
        }
        if commit.descriptor.bytes != grant.max_bytes || commit.placement.bytes != grant.max_bytes {
            return Err(StorageError::conflict(
                "receipt byte count does not match granted byte count",
            ));
        }
        let mut receipt = SegmentWriteReceipt {
            tenant: grant.tenant,
            grant_id: grant.grant_id,
            grant_hash,
            principal: grant.principal,
            owner: grant.owner,
            storage_node: grant.storage_node,
            storage_node_incarnation,
            segment_id: grant.segment_id,
            write_intent: grant.write_intent,
            intent: grant.intent,
            bytes: grant.max_bytes,
            checksum: commit.descriptor.checksum,
            durability: grant.durability,
            lifecycle: SegmentReceiptLifecycle::DurablePendingMetadata,
            receipt_epoch: grant.grant_epoch,
            expires_at: grant.expires_at,
            node_key_id: Self::node_key_id(grant.storage_node),
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
            descriptor: commit.descriptor,
            placement: commit.placement,
        };
        receipt.proof = deterministic_test_proof_for_receipt(receipt.node_key_id, &receipt);
        Ok(receipt)
    }

    fn verify_write_grant_and_hash(
        &self,
        grant: &WriteGrant,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        bytes: u64,
    ) -> Result<crate::provider::GrantHash> {
        Self::verify_not_expired(grant.expires_at)?;
        if grant.grant_epoch != LOCAL_GRANT_EPOCH {
            return Err(StorageError::conflict(
                "write grant epoch does not match verifier epoch",
            ));
        }
        if grant.owner != grant.intent.owner() {
            return Err(StorageError::conflict(
                "write grant owner does not match intent",
            ));
        }
        if grant.max_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "write grant must authorize at least one byte",
            ));
        }
        if grant.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local grant verifier supports only deterministic test proofs",
            ));
        }
        if grant.key_id != Self::node_key_id(grant.storage_node) {
            return Err(StorageError::conflict(
                "grant key does not match storage node",
            ));
        }
        let grant_hash = Self::verify_grant_proof_and_hash(grant)?;
        if grant.storage_node != storage_node {
            return Err(StorageError::conflict(
                "write grant storage node does not match request",
            ));
        }
        if grant.segment_id != segment_id {
            return Err(StorageError::conflict(
                "write grant segment ID does not match request",
            ));
        }
        if bytes != grant.max_bytes {
            return Err(StorageError::invalid_argument(
                "write grant byte count does not match granted segment length",
            ));
        }
        Ok(grant_hash)
    }
}

impl GrantReceiptAuthority for LocalGrantReceiptAuthority {
    fn issue_write_grant(&self, request: WriteGrantRequest) -> Result<WriteGrant> {
        if request.max_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "write grant must authorize at least one byte",
            ));
        }
        Self::verify_not_expired(request.expires_at)?;
        let owner = request.intent.owner();
        let key_id = Self::node_key_id(request.storage_node);
        let mut grant = WriteGrant {
            tenant: request.tenant,
            principal: request.principal,
            grant_id: Self::grant_id(request.segment_id),
            nonce: Self::nonce(request.segment_id, request.write_intent),
            grant_epoch: LOCAL_GRANT_EPOCH,
            expires_at: request.expires_at,
            owner,
            intent: request.intent,
            write_intent: request.write_intent,
            segment_id: request.segment_id,
            storage_node: request.storage_node,
            max_bytes: request.max_bytes,
            durability: request.durability,
            key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
        };
        grant.proof = deterministic_test_proof_for_grant(key_id, &grant);
        Ok(grant)
    }

    fn verify_write_grant(
        &self,
        grant: &WriteGrant,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        bytes: u64,
    ) -> Result<()> {
        self.verify_write_grant_and_hash(grant, storage_node, segment_id, bytes)
            .map(|_| ())
    }

    fn create_segment_receipt(
        &self,
        grant: &WriteGrant,
        commit: SegmentReplicaCommit,
        storage_node_incarnation: ServerIncarnation,
    ) -> Result<SegmentWriteReceipt> {
        let grant_hash = self.verify_write_grant_and_hash(
            grant,
            commit.placement.storage_node,
            commit.descriptor.segment_id,
            commit.descriptor.bytes,
        )?;
        self.create_segment_receipt_after_verified_write(
            grant,
            grant_hash,
            commit,
            storage_node_incarnation,
        )
    }

    fn verify_segment_receipt(
        &self,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        Self::verify_not_expired(receipt.expires_at)?;
        if receipt.receipt_epoch != LOCAL_GRANT_EPOCH {
            return Err(StorageError::conflict(
                "receipt epoch does not match verifier epoch",
            ));
        }
        if receipt.owner != receipt.intent.owner() {
            return Err(StorageError::conflict(
                "receipt owner does not match intent",
            ));
        }
        if receipt.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "receipt must describe at least one byte",
            ));
        }
        if receipt.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local receipt verifier supports only deterministic test proofs",
            ));
        }
        if receipt.node_key_id != Self::node_key_id(receipt.storage_node) {
            return Err(StorageError::conflict(
                "receipt key does not match storage node",
            ));
        }
        Self::verify_receipt_proof(receipt)?;
        if receipt.lifecycle != SegmentReceiptLifecycle::DurablePendingMetadata {
            return Err(StorageError::conflict(
                "receipt is not durable-pending-metadata",
            ));
        }
        if receipt.segment_id != receipt.descriptor.segment_id
            || receipt.segment_id != receipt.placement.segment_id
        {
            return Err(StorageError::conflict(
                "receipt segment IDs do not match descriptor and placement",
            ));
        }
        if receipt.storage_node != receipt.placement.storage_node {
            return Err(StorageError::conflict(
                "receipt storage node does not match placement",
            ));
        }
        if receipt.bytes != receipt.descriptor.bytes || receipt.bytes != receipt.placement.bytes {
            return Err(StorageError::conflict(
                "receipt byte count does not match descriptor and placement",
            ));
        }
        if receipt.checksum != receipt.descriptor.checksum {
            return Err(StorageError::conflict(
                "receipt checksum does not match descriptor",
            ));
        }
        Ok(VerifiedSegmentReceipt {
            receipt: receipt.clone(),
            descriptor: receipt.descriptor.clone(),
        })
    }

    fn create_reference_evidence(
        &self,
        receipt: &SegmentWriteReceipt,
        metadata_commit: CommitSeq,
    ) -> Result<ReferenceEvidence> {
        self.verify_segment_receipt(receipt)?;
        let mut evidence = ReferenceEvidence {
            tenant: receipt.tenant,
            principal: receipt.principal,
            owner: receipt.owner,
            grant_id: receipt.grant_id,
            segment_id: receipt.segment_id,
            storage_node: receipt.storage_node,
            metadata_commit,
            receipt_epoch: receipt.receipt_epoch,
            node_key_id: receipt.node_key_id,
            proof_scheme: ProofScheme::DeterministicTestMacV1,
            proof: crate::provider::ProofTag::ZERO,
        };
        evidence.proof = deterministic_test_proof_for_reference(evidence.node_key_id, &evidence);
        Ok(evidence)
    }

    fn verify_reference_evidence(
        &self,
        evidence: &ReferenceEvidence,
        segment_id: SegmentId,
        storage_node: StorageNodeId,
    ) -> Result<()> {
        if evidence.proof_scheme != ProofScheme::DeterministicTestMacV1 {
            return Err(StorageError::unsupported(
                "local reference verifier supports only deterministic test proofs",
            ));
        }
        if evidence.segment_id != segment_id {
            return Err(StorageError::conflict(
                "reference evidence segment ID does not match request",
            ));
        }
        if evidence.storage_node != storage_node {
            return Err(StorageError::conflict(
                "reference evidence storage node does not match request",
            ));
        }
        if evidence.node_key_id != Self::node_key_id(storage_node) {
            return Err(StorageError::conflict(
                "reference evidence key does not match storage node",
            ));
        }
        Self::verify_reference_proof(evidence)
    }
}

/// In-process storage-node role.
#[derive(Debug, Clone)]
struct LocalStorageNode {
    storage_node: StorageNodeId,
    segment_store: Arc<InMemorySegmentStore>,
    segment_catalog: Arc<InMemoryLocalSegmentCatalog>,
    authority: Arc<LocalGrantReceiptAuthority>,
    observability: Arc<Observability>,
}

impl LocalStorageNode {
    fn observe_maintenance(&self) -> Result<StorageNodeMaintenanceObservation> {
        let counts = self.segment_catalog.lifecycle_counts()?;
        Ok(StorageNodeMaintenanceObservation {
            storage_node: self.storage_node,
            reserved_segments: counts.reserved,
            writing_segments: counts.writing,
            durable_pending_segments: counts.durable_pending,
            referenced_segments: counts.referenced,
            released_segments: counts.released,
            freed_segments: counts.freed,
        })
    }

    fn run_maintenance_tick(&self) -> Result<StorageNodeMaintenanceReport> {
        let mut deleted_released_segments = Vec::new();
        let mut skipped_segments = Vec::new();
        for (segment_id, state, _) in self.segment_catalog.entries()? {
            if state == SegmentLifecycleState::Released {
                self.segment_catalog.delete_segment(segment_id)?;
                self.segment_store.delete_segment(segment_id)?;
                deleted_released_segments.push(segment_id);
            } else if matches!(
                state,
                SegmentLifecycleState::Reserved
                    | SegmentLifecycleState::Writing
                    | SegmentLifecycleState::DurablePendingMetadata
            ) {
                skipped_segments.push(segment_id);
            }
        }
        Ok(StorageNodeMaintenanceReport {
            storage_node: self.storage_node,
            deleted_released_segments,
            skipped_segments,
        })
    }

    fn run_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let mut report = StorageNodeCustodianReport {
            expired_reservations: Vec::new(),
            failed_writes: Vec::new(),
            orphan_segments: Vec::new(),
            deleted_released_segments: Vec::new(),
        };

        for (segment_id, state, write_intent) in self.segment_catalog.entries()? {
            match state {
                SegmentLifecycleState::Reserved
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.expire_reservation(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.expired_reservations.push(segment_id);
                }
                SegmentLifecycleState::Writing if expired_write_intents.contains(&write_intent) => {
                    self.segment_catalog.fail_write(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.failed_writes.push(segment_id);
                }
                SegmentLifecycleState::DurablePendingMetadata
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.free_orphan_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.orphan_segments.push(segment_id);
                }
                SegmentLifecycleState::Released => {
                    self.segment_catalog.delete_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.deleted_released_segments.push(segment_id);
                }
                _ => {}
            }
        }

        Ok(report)
    }
}

impl StorageNodeTransport for LocalStorageNode {
    fn storage_node_id(&self) -> StorageNodeId {
        self.storage_node
    }

    fn send(&self, request: StorageNodeRequest) -> Result<StorageNodeResponse> {
        match request {
            StorageNodeRequest::WriteSegment { grant, bytes } => {
                let bytes_len = u64::try_from(bytes.len()).map_err(|_| {
                    StorageError::invalid_argument("segment write length overflows u64")
                })?;
                let grant_hash = match self.authority.verify_write_grant_and_hash(
                    &grant,
                    self.storage_node,
                    grant.segment_id,
                    bytes_len,
                ) {
                    Ok(hash) => hash,
                    Err(error) => {
                        self.observability.record_with_update(
                            StorageEventKind::GrantRejected,
                            Some(self.storage_node),
                            Some(grant.segment_id),
                            None,
                            Some("scope"),
                            |counters| {
                                counters.grant_rejections =
                                    counters.grant_rejections.saturating_add(1);
                            },
                        );
                        return Err(error);
                    }
                };
                if self.segment_catalog.contains_segment(grant.segment_id)? {
                    let receipt = self.segment_catalog.receipt_for_segment(grant.segment_id)?;
                    self.authority.verify_segment_receipt(&receipt)?;
                    if receipt.grant_id == grant.grant_id
                        && receipt.grant_hash == grant_hash
                        && receipt.bytes == bytes_len
                        && receipt.checksum == Some(checksum64(&bytes))
                    {
                        let existing_len = usize::try_from(bytes_len).map_err(|_| {
                            StorageError::invalid_argument(
                                "duplicate segment length overflows usize",
                            )
                        })?;
                        let mut existing = vec![0; existing_len];
                        self.segment_store.read_segment(
                            grant.segment_id,
                            ByteRange::new(0, bytes_len),
                            &mut existing,
                        )?;
                        if existing == bytes {
                            self.observability.record_with_update(
                                StorageEventKind::StorageSegmentWriteRetried,
                                Some(self.storage_node),
                                Some(grant.segment_id),
                                None,
                                None,
                                |counters| {
                                    counters.storage_segment_duplicate_writes =
                                        counters.storage_segment_duplicate_writes.saturating_add(1);
                                    counters.coordinator_write_idempotency_hits = counters
                                        .coordinator_write_idempotency_hits
                                        .saturating_add(1);
                                },
                            );
                            return Ok(StorageNodeResponse::WriteSegment {
                                receipt: Box::new(receipt),
                            });
                        }
                    }
                    return Err(StorageError::conflict(
                        "duplicate segment write conflicts with existing receipt",
                    ));
                }
                let intent = SegmentReservationIntent {
                    write_intent: grant.write_intent,
                    owner: grant.owner,
                    bytes: grant.max_bytes,
                };
                let reservation = self
                    .segment_catalog
                    .reserve_segment_with_id(grant.segment_id, intent)?;
                self.segment_catalog.begin_write(&reservation)?;
                let commit = self
                    .segment_store
                    .write_segment_owned(&reservation, bytes)?;
                self.segment_store.sync_segment(reservation.segment_id)?;
                let receipt = self.authority.create_segment_receipt_after_verified_write(
                    &grant,
                    grant_hash,
                    commit,
                    LOCAL_STORAGE_NODE_INCARNATION,
                )?;
                self.segment_catalog
                    .commit_segment(reservation, receipt.clone())?;
                self.observability.record_with_update(
                    StorageEventKind::StorageSegmentWritten,
                    Some(self.storage_node),
                    Some(receipt.segment_id),
                    None,
                    None,
                    |counters| {
                        counters.storage_segment_writes =
                            counters.storage_segment_writes.saturating_add(1);
                    },
                );
                Ok(StorageNodeResponse::WriteSegment {
                    receipt: Box::new(receipt),
                })
            }
            StorageNodeRequest::ReadSegment { segment_id, range } => {
                let len = usize::try_from(range.len).map_err(|_| {
                    StorageError::invalid_argument("segment read byte length overflows usize")
                })?;
                let mut bytes = vec![0; len];
                self.segment_store
                    .read_segment(segment_id, range, &mut bytes)?;
                Ok(StorageNodeResponse::ReadSegment { bytes })
            }
            StorageNodeRequest::MarkReferenced { evidence } => {
                let segment_id = evidence.segment_id;
                self.authority.verify_reference_evidence(
                    &evidence,
                    segment_id,
                    self.storage_node,
                )?;
                self.segment_catalog.mark_segment_referenced(segment_id)?;
                self.observability.record_with_update(
                    StorageEventKind::StorageSegmentReferenced,
                    Some(self.storage_node),
                    Some(segment_id),
                    Some(evidence.metadata_commit),
                    None,
                    |counters| {
                        counters.storage_segment_references =
                            counters.storage_segment_references.saturating_add(1);
                    },
                );
                Ok(StorageNodeResponse::MarkReferenced)
            }
            StorageNodeRequest::Release { segment_id } => {
                self.segment_catalog.release_segment(segment_id)?;
                self.observability.record_with_update(
                    StorageEventKind::StorageSegmentReleased,
                    Some(self.storage_node),
                    Some(segment_id),
                    None,
                    None,
                    |counters| {
                        counters.storage_segment_releases =
                            counters.storage_segment_releases.saturating_add(1);
                    },
                );
                Ok(StorageNodeResponse::Released)
            }
            StorageNodeRequest::RunCustodian {
                expired_write_intents,
            } => Ok(StorageNodeResponse::Custodian(
                self.run_custodian(&expired_write_intents)?,
            )),
            StorageNodeRequest::ObserveMaintenance => Ok(StorageNodeResponse::MaintenanceObserved(
                self.observe_maintenance()?,
            )),
            StorageNodeRequest::RunMaintenanceTick => Ok(StorageNodeResponse::MaintenanceTicked(
                self.run_maintenance_tick()?,
            )),
        }
    }
}

#[derive(Debug, Clone)]
struct StorageNodeRegistry {
    config: LocalStoreConfig,
    node_order: Arc<Vec<StorageNodeId>>,
    nodes: Arc<BTreeMap<StorageNodeId, LocalStorageNode>>,
    next_placement_index: Arc<Mutex<u64>>,
    next_segment_id: Arc<Mutex<u128>>,
}

impl StorageNodeRegistry {
    #[cfg(test)]
    fn new(config: LocalStoreConfig, storage_nodes: Vec<StorageNodeId>) -> Result<Self> {
        let observability = Arc::new(Observability::new(config.observability_event_capacity)?);
        Self::new_with_observability(config, storage_nodes, observability)
    }

    fn new_with_observability(
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        observability: Arc<Observability>,
    ) -> Result<Self> {
        config.validate()?;
        let node_order = normalize_storage_nodes(config.storage_node, storage_nodes);
        let mut nodes = BTreeMap::new();
        let authority = Arc::new(LocalGrantReceiptAuthority);
        for node_id in &node_order {
            let node_config = config.for_storage_node(*node_id);
            nodes.insert(
                *node_id,
                LocalStorageNode {
                    storage_node: *node_id,
                    segment_store: Arc::new(InMemorySegmentStore::new(node_config)?),
                    segment_catalog: Arc::new(InMemoryLocalSegmentCatalog::new(node_config)?),
                    authority: Arc::clone(&authority),
                    observability: Arc::clone(&observability),
                },
            );
        }
        Ok(Self {
            config,
            node_order: Arc::new(node_order),
            nodes: Arc::new(nodes),
            next_placement_index: Arc::new(Mutex::new(0)),
            next_segment_id: Arc::new(Mutex::new(1)),
        })
    }

    fn from_inner_with_observability(
        config: LocalStoreConfig,
        inner: StorageNodeRegistryInner,
        observability: Arc<Observability>,
    ) -> Result<Self> {
        config.validate()?;
        if inner.node_order.is_empty() {
            return Err(StorageError::corrupt("storage node registry has no nodes"));
        }
        let mut seen = BTreeSet::new();
        for node_id in &inner.node_order {
            if !seen.insert(*node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry has duplicate node IDs",
                ));
            }
            if !inner.nodes.contains_key(node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry order references missing node",
                ));
            }
        }
        for node_id in inner.nodes.keys() {
            if !seen.contains(node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry contains unordered node",
                ));
            }
        }

        let mut nodes = BTreeMap::new();
        let authority = Arc::new(LocalGrantReceiptAuthority);
        for node_id in &inner.node_order {
            let image = inner
                .nodes
                .get(node_id)
                .ok_or_else(|| StorageError::corrupt("storage node image missing"))?;
            let node_config = config.for_storage_node(*node_id);
            nodes.insert(
                *node_id,
                LocalStorageNode {
                    storage_node: *node_id,
                    segment_store: Arc::new(InMemorySegmentStore::from_inner(
                        node_config,
                        image.segment_store.clone(),
                    )?),
                    segment_catalog: Arc::new(InMemoryLocalSegmentCatalog::from_inner(
                        node_config,
                        image.segment_catalog.clone(),
                    )?),
                    authority: Arc::clone(&authority),
                    observability: Arc::clone(&observability),
                },
            );
        }

        Ok(Self {
            config,
            node_order: Arc::new(inner.node_order),
            nodes: Arc::new(nodes),
            next_placement_index: Arc::new(Mutex::new(inner.next_placement_index)),
            next_segment_id: Arc::new(Mutex::new(inner.next_segment_id)),
        })
    }

    #[cfg(test)]
    fn state_inner(&self) -> Result<StorageNodeRegistryInner> {
        let mut nodes = BTreeMap::new();
        for (node_id, node) in self.nodes.iter() {
            nodes.insert(
                *node_id,
                StorageNodeInner {
                    segment_store: node.segment_store.state_inner()?,
                    segment_catalog: node.segment_catalog.state_inner()?,
                },
            );
        }
        Ok(StorageNodeRegistryInner {
            next_segment_id: *lock(&self.next_segment_id)?,
            next_placement_index: *lock(&self.next_placement_index)?,
            node_order: self.node_order.as_ref().clone(),
            nodes,
        })
    }

    fn state_inner_for_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(
        StorageNodeRegistryInner,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let mut nodes = BTreeMap::new();
        let mut image_segments = BTreeSet::new();
        let mut new_segments = Vec::new();
        for (node_id, node) in self.nodes.iter() {
            let (segment_store, node_segments, mut node_new_segments) = node
                .segment_store
                .state_inner_for_persist(previous_segments, *node_id)?;
            image_segments.extend(node_segments);
            new_segments.append(&mut node_new_segments);
            nodes.insert(
                *node_id,
                StorageNodeInner {
                    segment_store,
                    segment_catalog: node.segment_catalog.state_inner()?,
                },
            );
        }
        Ok((
            StorageNodeRegistryInner {
                next_segment_id: *lock(&self.next_segment_id)?,
                next_placement_index: *lock(&self.next_placement_index)?,
                node_order: self.node_order.as_ref().clone(),
                nodes,
            },
            image_segments,
            new_segments,
        ))
    }

    fn segment_ids(&self) -> Result<BTreeSet<SegmentId>> {
        let mut out = BTreeSet::new();
        for node in self.nodes.values() {
            out.extend(node.segment_store.segment_ids()?);
        }
        Ok(out)
    }

    fn primary_node(&self) -> Result<&LocalStorageNode> {
        self.node(self.config.storage_node)
    }

    fn node(&self, storage_node: StorageNodeId) -> Result<&LocalStorageNode> {
        self.nodes
            .get(&storage_node)
            .ok_or_else(|| StorageError::not_found("storage_node", storage_node.to_string()))
    }

    #[cfg(test)]
    fn node_ids(&self) -> Vec<StorageNodeId> {
        self.node_order.as_ref().clone()
    }

    fn owner_node_for_segment(&self, segment_id: SegmentId) -> Result<&LocalStorageNode> {
        let mut found = None;
        for (node_id, node) in self.nodes.iter() {
            if node.segment_catalog.contains_segment(segment_id)? {
                if found.is_some() {
                    return Err(StorageError::corrupt(
                        "segment appears in multiple storage-node catalogs",
                    ));
                }
                found = Some(*node_id);
            }
        }
        let node_id =
            found.ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        self.node(node_id)
    }

    #[cfg(test)]
    fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .commit_for_segment(segment_id)
    }

    fn receipt_for_segment(&self, segment_id: SegmentId) -> Result<SegmentWriteReceipt> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .receipt_for_segment(segment_id)
    }

    fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .state(segment_id)
    }

    fn mark_segment_referenced(
        &self,
        receipt: &SegmentWriteReceipt,
        commit_seq: CommitSeq,
        authority: &dyn GrantReceiptAuthority,
    ) -> Result<()> {
        let evidence = authority.create_reference_evidence(receipt, commit_seq)?;
        let response = self
            .transport_for_segment(receipt.segment_id)?
            .send(StorageNodeRequest::MarkReferenced { evidence })?;
        if response != StorageNodeResponse::MarkReferenced {
            return Err(StorageError::corrupt(
                "storage node returned unexpected mark-referenced response",
            ));
        }
        Ok(())
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let response = self
            .transport_for_segment(segment_id)?
            .send(StorageNodeRequest::Release { segment_id })?;
        if response != StorageNodeResponse::Released {
            return Err(StorageError::corrupt(
                "storage node returned unexpected release response",
            ));
        }
        Ok(())
    }

    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let response = self
            .transport_for_segment(segment_id)?
            .send(StorageNodeRequest::ReadSegment { segment_id, range })?;
        let StorageNodeResponse::ReadSegment { bytes } = response else {
            return Err(StorageError::corrupt(
                "storage node returned unexpected read response",
            ));
        };
        if bytes.len() != buf.len() {
            return Err(StorageError::corrupt(
                "storage node read response length disagrees with buffer",
            ));
        }
        buf.copy_from_slice(&bytes);
        Ok(())
    }

    fn diagnostics_nodes(
        &self,
        maintenance: Option<&MaintenanceObservation>,
    ) -> Result<Vec<DiagnosticsNodeSnapshot>> {
        let mut nodes = BTreeMap::new();
        for node_id in self.node_order.iter() {
            let counts = self.node(*node_id)?.segment_catalog.lifecycle_counts()?;
            nodes.insert(
                *node_id,
                DiagnosticsNodeSnapshot {
                    storage_node: *node_id,
                    reserved_segments: usize_to_u64(counts.reserved),
                    writing_segments: usize_to_u64(counts.writing),
                    durable_pending_segments: usize_to_u64(counts.durable_pending),
                    pending_orphan_segments: usize_to_u64(counts.durable_pending),
                    referenced_segments: usize_to_u64(counts.referenced),
                    released_segments: usize_to_u64(counts.released),
                    freed_segments: usize_to_u64(counts.freed),
                    active_log_bytes: 0,
                    sealed_log_count: 0,
                    sealed_log_bytes: 0,
                    dirty_bytes: 0,
                    reclaimable_bytes: 0,
                },
            );
        }
        if let Some(maintenance) = maintenance {
            for observed in &maintenance.nodes {
                if let Some(node) = nodes.get_mut(&observed.storage_node) {
                    node.active_log_bytes = observed.active_log_bytes;
                    node.sealed_log_count = usize_to_u64(observed.sealed_log_count);
                    node.sealed_log_bytes = observed
                        .logs
                        .iter()
                        .map(|log| log.total_bytes)
                        .fold(0_u64, u64::saturating_add);
                    node.dirty_bytes = observed.dirty_bytes;
                    node.reclaimable_bytes = observed.reclaimable_bytes;
                }
            }
        }
        Ok(nodes.into_values().collect())
    }
}

impl PlacementPolicy for StorageNodeRegistry {
    fn choose_storage_node(&self, candidates: &[StorageNodeId]) -> Result<StorageNodeId> {
        if candidates.is_empty() {
            return Err(StorageError::invalid_argument(
                "placement policy requires at least one storage node",
            ));
        }
        let mut next = lock(&self.next_placement_index)?;
        let index = usize::try_from(*next % candidates.len() as u64)
            .map_err(|_| StorageError::invalid_argument("placement index overflows usize"))?;
        let node_id = candidates[index];
        self.node(node_id)?;
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("placement index overflow"))?;
        Ok(node_id)
    }
}

impl StorageNodeDirectory for StorageNodeRegistry {
    fn storage_node_ids(&self) -> Result<Vec<StorageNodeId>> {
        Ok(self.node_order.as_ref().clone())
    }

    fn allocate_segment_id(&self) -> Result<SegmentId> {
        let mut next = lock(&self.next_segment_id)?;
        loop {
            let candidate = SegmentId::from_raw(*next);
            *next = next
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("segment id overflow"))?;
            let mut exists = false;
            for node in self.nodes.values() {
                if node.segment_catalog.contains_segment(candidate)? {
                    exists = true;
                    break;
                }
            }
            if !exists {
                return Ok(candidate);
            }
        }
    }

    fn transport_for_node(
        &self,
        storage_node: StorageNodeId,
    ) -> Result<Arc<dyn StorageNodeTransport>> {
        Ok(Arc::new(self.node(storage_node)?.clone()))
    }

    fn transport_for_segment(
        &self,
        segment_id: SegmentId,
    ) -> Result<Arc<dyn StorageNodeTransport>> {
        Ok(Arc::new(self.owner_node_for_segment(segment_id)?.clone()))
    }
}

/// In-process coordinator that owns request orchestration across metadata and
/// storage-node roles.
#[derive(Debug, Clone)]
pub struct LocalCoordinator {
    metadata: Arc<InMemoryMetadataPlane>,
    storage_nodes: StorageNodeRegistry,
    authority: Arc<LocalGrantReceiptAuthority>,
    next_write_intent: Arc<Mutex<u128>>,
    next_extent_id: Arc<Mutex<u128>>,
    observability: Arc<Observability>,
}

impl LocalCoordinator {
    pub fn new() -> Self {
        Self::with_config(LocalStoreConfig::default()).expect("default local store config is valid")
    }

    pub fn with_config(config: LocalStoreConfig) -> Result<Self> {
        Self::with_storage_nodes(config, vec![config.storage_node])
    }

    pub fn with_storage_nodes(
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
    ) -> Result<Self> {
        config.validate()?;
        let observability = Arc::new(Observability::new(config.observability_event_capacity)?);
        Ok(Self {
            metadata: Arc::new(InMemoryMetadataPlane::new(config)?),
            storage_nodes: StorageNodeRegistry::new_with_observability(
                config,
                storage_nodes,
                Arc::clone(&observability),
            )?,
            authority: Arc::new(LocalGrantReceiptAuthority),
            next_write_intent: Arc::new(Mutex::new(1)),
            next_extent_id: Arc::new(Mutex::new(1)),
            observability,
        })
    }

    fn from_state_image(image: DurableStoreImage) -> Result<Self> {
        image.config.validate()?;
        let observability = Arc::new(Observability::new(
            image.config.observability_event_capacity,
        )?);
        Ok(Self {
            metadata: Arc::new(InMemoryMetadataPlane::from_inner(
                image.config,
                image.metadata,
            )?),
            storage_nodes: StorageNodeRegistry::from_inner_with_observability(
                image.config,
                image.storage_nodes,
                Arc::clone(&observability),
            )?,
            authority: Arc::new(LocalGrantReceiptAuthority),
            next_write_intent: Arc::new(Mutex::new(image.next_write_intent)),
            next_extent_id: Arc::new(Mutex::new(image.next_extent_id)),
            observability,
        })
    }

    #[cfg(test)]
    fn state_image(&self) -> Result<DurableStoreImage> {
        Ok(DurableStoreImage {
            config: self.metadata.config,
            metadata: self.metadata.state_inner()?,
            storage_nodes: self.storage_nodes.state_inner()?,
            next_write_intent: *lock(&self.next_write_intent)?,
            next_extent_id: *lock(&self.next_extent_id)?,
        })
    }

    fn state_image_for_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(
        DurableStoreImage,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let (storage_nodes, image_segments, new_segments) = self
            .storage_nodes
            .state_inner_for_persist(previous_segments)?;
        Ok((
            DurableStoreImage {
                config: self.metadata.config,
                metadata: self.metadata.state_inner()?,
                storage_nodes,
                next_write_intent: *lock(&self.next_write_intent)?,
                next_extent_id: *lock(&self.next_extent_id)?,
            },
            image_segments,
            new_segments,
        ))
    }

    fn segment_ids(&self) -> Result<BTreeSet<SegmentId>> {
        self.storage_nodes.segment_ids()
    }

    pub fn metadata(&self) -> Arc<InMemoryMetadataPlane> {
        Arc::clone(&self.metadata)
    }

    pub fn segment_store(&self) -> Arc<InMemorySegmentStore> {
        Arc::clone(
            &self
                .storage_nodes
                .primary_node()
                .expect("primary storage node exists")
                .segment_store,
        )
    }

    pub fn segment_catalog(&self) -> Arc<InMemoryLocalSegmentCatalog> {
        Arc::clone(
            &self
                .storage_nodes
                .primary_node()
                .expect("primary storage node exists")
                .segment_catalog,
        )
    }

    fn diagnostics_snapshot_with_maintenance(
        &self,
        maintenance: Option<&MaintenanceObservation>,
    ) -> Result<DiagnosticsSnapshot> {
        let (counters, events, event_buffer_len, event_buffer_capacity, last_event_sequence) =
            self.observability.snapshot_parts()?;
        let metadata = self.metadata.state_inner()?;
        let nodes = self.storage_nodes.diagnostics_nodes(maintenance)?;
        let mut gauges = DiagnosticsGauges {
            live_device_heads: usize_to_u64(metadata.device_heads.len()),
            deleted_device_heads: usize_to_u64(metadata.deleted_device_heads.len()),
            live_keyspace_heads: usize_to_u64(metadata.keyspace_heads.len()),
            metadata_nodes: usize_to_u64(metadata.metadata_nodes.len()),
            commit_seq: metadata.next_commit_seq.saturating_sub(1),
            checkpoint_count: usize_to_u64(metadata.checkpoints.len()),
            gc_epoch: metadata.next_gc_epoch.saturating_sub(1),
            pending_release_evidence: nodes
                .iter()
                .map(|node| node.released_segments)
                .fold(0_u64, u64::saturating_add),
            event_buffer_len,
            event_buffer_capacity,
            last_event_sequence,
            ..DiagnosticsGauges::default()
        };
        if let Some(maintenance) = maintenance {
            gauges.sqlite_wal_bytes = maintenance.sqlite_wal_bytes;
            gauges.maintenance_dirty_bytes = maintenance
                .nodes
                .iter()
                .map(|node| node.dirty_bytes)
                .fold(0_u64, u64::saturating_add);
            gauges.maintenance_reclaimable_bytes = maintenance
                .nodes
                .iter()
                .map(|node| node.reclaimable_bytes)
                .fold(0_u64, u64::saturating_add);
            gauges.maintenance_sealed_logs = maintenance
                .nodes
                .iter()
                .map(|node| usize_to_u64(node.sealed_log_count))
                .fold(0_u64, u64::saturating_add);
        }
        Ok(DiagnosticsSnapshot {
            counters,
            gauges,
            nodes,
            recent_events: events,
        })
    }

    pub fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        self.diagnostics_snapshot_with_maintenance(None)
    }

    pub fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        self.observability.drain_events(max)
    }

    #[cfg(test)]
    fn storage_node_ids_for_test(&self) -> Vec<StorageNodeId> {
        self.storage_nodes.node_ids()
    }

    #[cfg(test)]
    fn segment_catalog_for_node(
        &self,
        storage_node: StorageNodeId,
    ) -> Result<Arc<InMemoryLocalSegmentCatalog>> {
        Ok(Arc::clone(
            &self.storage_nodes.node(storage_node)?.segment_catalog,
        ))
    }

    #[cfg(test)]
    fn segment_store_for_node(
        &self,
        storage_node: StorageNodeId,
    ) -> Result<Arc<InMemorySegmentStore>> {
        Ok(Arc::clone(
            &self.storage_nodes.node(storage_node)?.segment_store,
        ))
    }

    fn publish_commit_group_observed(&self, intent: CommitGroupIntent) -> Result<CommitGroup> {
        match self.metadata.publish_commit_group(intent) {
            Ok(commit_group) => {
                self.observability.record_with_update(
                    StorageEventKind::MetadataPublishSucceeded,
                    None,
                    None,
                    Some(commit_group.commit_seq),
                    None,
                    |counters| {
                        counters.coordinator_write_publish_successes = counters
                            .coordinator_write_publish_successes
                            .saturating_add(1);
                    },
                );
                Ok(commit_group)
            }
            Err(error) => {
                self.observability.record_with_update(
                    StorageEventKind::MetadataPublishFailed,
                    None,
                    None,
                    None,
                    Some("publish_failed"),
                    |counters| {
                        counters.coordinator_write_publish_failures = counters
                            .coordinator_write_publish_failures
                            .saturating_add(1);
                        if matches!(error, StorageError::Conflict { .. }) {
                            counters.metadata_stale_fences =
                                counters.metadata_stale_fences.saturating_add(1);
                        }
                    },
                );
                Err(error)
            }
        }
    }

    pub fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<WriteCommit> {
        let info = self.metadata.device_info(device_id)?;
        let len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&info.spec)?;

        if len == 0 {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability,
            });
        }
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );

        let block_size = u64::from(info.spec.block_size);
        let chunks = self.split_device_range(&info, range)?;
        let owner = MappingOwner::BlockDevice(device_id);
        let write_intent = self.next_write_intent()?;
        let mut updates = Vec::with_capacity(chunks.len());
        let mut segment_receipts = Vec::with_capacity(chunks.len());
        let current = self.metadata.get_head(device_id)?;

        for chunk in chunks {
            let chunk_offset = chunk
                .range
                .start
                .raw()
                .checked_mul(block_size)
                .and_then(|start| start.checked_sub(offset))
                .ok_or_else(|| StorageError::invalid_argument("write chunk offset overflows"))?;
            let byte_start = usize::try_from(chunk_offset).map_err(|_| {
                StorageError::invalid_argument("write chunk offset overflows usize")
            })?;
            let chunk_len = chunk
                .range
                .blocks
                .raw()
                .checked_mul(block_size)
                .ok_or_else(|| StorageError::invalid_argument("write chunk length overflows"))?;
            let byte_len = usize::try_from(chunk_len).map_err(|_| {
                StorageError::invalid_argument("write chunk length overflows usize")
            })?;
            let byte_end = byte_start
                .checked_add(byte_len)
                .ok_or_else(|| StorageError::invalid_argument("write chunk end overflows"))?;
            let chunk_bytes = data
                .get(byte_start..byte_end)
                .ok_or_else(|| StorageError::corrupt("write chunk is outside request bytes"))?;
            let verified_receipt = self.write_segment_for_intent_with_id_owned_verified(
                WriteGrantIntent::BlockWrite {
                    device_id,
                    range: chunk.range,
                    fence: current.generation,
                    shard_id: chunk.shard_id,
                    old_root: chunk.old_root,
                },
                write_intent,
                chunk_bytes.to_vec(),
                durability,
            )?;
            let segment_id = verified_receipt.descriptor.segment_id;

            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: Some(SegmentReplacement {
                    segment_id,
                    segment_base: chunk.range.start,
                }),
            };
            let new_root = self
                .replace_tree_range_with_receipts(
                    chunk.old_root,
                    edit,
                    std::slice::from_ref(&verified_receipt),
                )?
                .root;
            segment_receipts.push(verified_receipt);
            updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
                new_root,
            }));
        }

        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner,
            fence: MetadataFence::DeviceGeneration(current.generation),
            updates,
        })?;

        for receipt in &segment_receipts {
            self.storage_nodes.mark_segment_referenced(
                receipt.receipt(),
                commit_group.commit_seq,
                self.authority.as_ref(),
            )?;
        }

        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability,
        })
    }

    pub fn write_zeroes(&self, device_id: DeviceId, offset: u64, len: u64) -> Result<WriteCommit> {
        let zeroes = usize::try_from(len)
            .map_err(|_| StorageError::invalid_argument("zero range length overflows usize"))?;
        self.write_device(
            device_id,
            offset,
            &vec![0; zeroes],
            crate::api::WriteDurability::Acknowledged,
        )
    }

    pub fn discard_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        len: u64,
    ) -> Result<WriteCommit> {
        let info = self.metadata.device_info(device_id)?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&info.spec)?;

        if len == 0 {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability: crate::api::WriteDurability::Acknowledged,
            });
        }

        let chunks = self.split_device_range(&info, range)?;
        let owner = MappingOwner::BlockDevice(device_id);
        let mut updates = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: None,
            };
            let edit_result = self.replace_tree_range(chunk.old_root, edit)?;
            if edit_result.changed {
                updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: chunk.shard_id,
                    old_root: chunk.old_root,
                    new_root: edit_result.root,
                }));
            }
        }

        if updates.is_empty() {
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability: crate::api::WriteDurability::Acknowledged,
            });
        }
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );

        let current = self.metadata.get_head(device_id)?;
        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner,
            fence: MetadataFence::DeviceGeneration(current.generation),
            updates,
        })?;

        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability: crate::api::WriteDurability::Acknowledged,
        })
    }

    pub fn open_append_session(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendSession> {
        self.metadata.open_append_session(keyspace_id, file_id)
    }

    pub fn reserve_append(&self, session: &AppendSession, len: u64) -> Result<AppendReservation> {
        self.metadata.reserve_append(session, len)
    }

    pub fn append_reserved(
        &self,
        reservation: AppendReservation,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<AppendCommit> {
        if data.is_empty() {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );
        let data_len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?;
        if data_len != reservation.len {
            return Err(StorageError::invalid_argument(
                "append payload length does not match reservation",
            ));
        }

        let head = self
            .metadata
            .get_file_head(reservation.keyspace_id, reservation.file_id)?;
        if head.size != reservation.offset {
            return Err(StorageError::conflict(
                "append reservation commits out of order",
            ));
        }
        self.metadata.validate_append_reservation(&reservation)?;

        let owner = MappingOwner::NativeKeyspace(reservation.keyspace_id);
        let block_size = u64::from(self.metadata.config.block_size);
        let tail_bytes = head.size % block_size;
        let segment_start_block = head.size / block_size;
        let segment_payload_len = tail_bytes
            .checked_add(data_len)
            .ok_or_else(|| StorageError::invalid_argument("append segment length overflows"))?;
        let segment_blocks = blocks_for_bytes(segment_payload_len, block_size)?;
        let segment_len = segment_blocks.checked_mul(block_size).ok_or_else(|| {
            StorageError::invalid_argument("append segment byte length overflows")
        })?;
        let segment_len_usize = usize::try_from(segment_len).map_err(|_| {
            StorageError::invalid_argument("append segment byte length overflows usize")
        })?;
        let segment_bytes = if tail_bytes == 0 && data_len == segment_len {
            data.to_vec()
        } else {
            let mut segment_bytes = Vec::with_capacity(segment_len_usize);

            if tail_bytes != 0 {
                let tail_range = crate::api::BlockRange::new(
                    BlockIndex::from_raw(segment_start_block),
                    BlockCount::from_raw(1),
                );
                if !self.tree_has_mappings(head.root, tail_range)? {
                    return Err(StorageError::corrupt(
                        "unaligned native file size has no tail block mapping",
                    ));
                }
                let block_size_usize = usize::try_from(block_size)
                    .map_err(|_| StorageError::invalid_argument("block size overflows usize"))?;
                let tail_bytes_usize = usize::try_from(tail_bytes).map_err(|_| {
                    StorageError::invalid_argument("tail byte count overflows usize")
                })?;
                let mut tail_block = vec![0; block_size_usize];
                let root = self.metadata.get_metadata_node(head.root)?;
                self.read_metadata_node(&root, tail_range, block_size, &mut tail_block)?;
                segment_bytes.extend_from_slice(&tail_block[..tail_bytes_usize]);
            }

            segment_bytes.extend_from_slice(data);
            segment_bytes.resize(segment_len_usize, 0);
            segment_bytes
        };

        let verified_receipt = self.write_segment_for_intent_with_id_owned_verified(
            WriteGrantIntent::NativeAppend {
                keyspace_id: reservation.keyspace_id,
                file_id: reservation.file_id,
                session_id: reservation.session_id,
                reservation_id: reservation.reservation_id,
                append_offset: reservation.offset,
                bytes: data_len,
                writer_epoch: reservation.writer_epoch,
            },
            self.next_write_intent()?,
            segment_bytes,
            durability,
        )?;
        let append_range = crate::api::BlockRange::new(
            BlockIndex::from_raw(segment_start_block),
            BlockCount::from_raw(segment_blocks),
        );
        let new_size = head
            .size
            .checked_add(data_len)
            .ok_or_else(|| StorageError::invalid_argument("file size overflows u64"))?;
        let edit = TreeRangeEdit {
            range: append_range,
            replacement: Some(SegmentReplacement {
                segment_id: verified_receipt.descriptor.segment_id,
                segment_base: append_range.start,
            }),
        };
        if tail_bytes == 0 && self.tree_has_mappings(head.root, append_range)? {
            return Err(StorageError::conflict(
                "append range overlaps existing file metadata",
            ));
        }
        if tail_bytes != 0 && segment_blocks > 1 {
            let next_block = segment_start_block
                .checked_add(1)
                .ok_or_else(|| StorageError::invalid_argument("append range overflows"))?;
            let new_blocks = crate::api::BlockRange::new(
                BlockIndex::from_raw(next_block),
                BlockCount::from_raw(segment_blocks - 1),
            );
            if self.tree_has_mappings(head.root, new_blocks)? {
                return Err(StorageError::conflict(
                    "append range overlaps existing file metadata after tail block",
                ));
            }
        }
        let new_root = self
            .replace_tree_range_with_receipts(
                head.root,
                edit,
                std::slice::from_ref(&verified_receipt),
            )?
            .root;

        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner,
            fence: MetadataFence::AppendReservation {
                session_id: reservation.session_id,
                reservation_id: reservation.reservation_id,
                offset: reservation.offset,
                len: reservation.len,
                writer_epoch: reservation.writer_epoch,
            },
            updates: vec![RootUpdate::FileRoot {
                file_id: reservation.file_id,
                old_root: head.root,
                new_root,
                new_size,
            }],
        })?;
        self.storage_nodes.mark_segment_referenced(
            verified_receipt.receipt(),
            commit_group.commit_seq,
            self.authority.as_ref(),
        )?;
        let committed = self
            .metadata
            .get_file_head(reservation.keyspace_id, reservation.file_id)?;

        Ok(AppendCommit {
            keyspace_id: reservation.keyspace_id,
            file_id: reservation.file_id,
            extent_id: self.next_extent_id()?,
            range: ByteRange::new(head.size, data_len),
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability,
        })
    }

    pub fn write_file_at(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<FileWriteCommit> {
        let data_len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, data_len);
        let end = range.end_exclusive()?;
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;

        if offset > head.size {
            return Err(StorageError::invalid_argument(
                "native file write cannot create a sparse gap",
            ));
        }

        if data.is_empty() {
            return Ok(FileWriteCommit {
                keyspace_id,
                file_id,
                range,
                version: head.version,
                commit_seq: head.latest_commit,
                durability,
            });
        }
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );

        let owner = MappingOwner::NativeKeyspace(keyspace_id);
        let block_size = u64::from(self.metadata.config.block_size);
        let first_block = offset / block_size;
        let requested_start = first_block
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("native write range overflows"))?;
        let segment_blocks = blocks_for_bytes(end - requested_start, block_size)?;
        let write_range = crate::api::BlockRange::new(
            BlockIndex::from_raw(first_block),
            BlockCount::from_raw(segment_blocks),
        );
        let root = self.metadata.get_metadata_node(head.root)?;
        if !root.covered_range.contains_range(write_range)? {
            return Err(StorageError::invalid_argument(
                "native file write exceeds file root coverage",
            ));
        }

        let segment_len = segment_blocks.checked_mul(block_size).ok_or_else(|| {
            StorageError::invalid_argument("native write segment length overflows")
        })?;
        let segment_len_usize = usize::try_from(segment_len).map_err(|_| {
            StorageError::invalid_argument("native write segment length overflows usize")
        })?;
        let segment_bytes =
            if offset.is_multiple_of(block_size) && data_len.is_multiple_of(block_size) {
                data.to_vec()
            } else {
                let mut bytes = vec![0; segment_len_usize];
                self.read_metadata_node(&root, write_range, block_size, &mut bytes)?;
                let write_offset = usize::try_from(offset - requested_start).map_err(|_| {
                    StorageError::invalid_argument("native write segment offset overflows usize")
                })?;
                let write_len = usize::try_from(data_len).map_err(|_| {
                    StorageError::invalid_argument("native write length overflows usize")
                })?;
                let write_end = write_offset
                    .checked_add(write_len)
                    .ok_or_else(|| StorageError::invalid_argument("native write end overflows"))?;
                let target = bytes.get_mut(write_offset..write_end).ok_or_else(|| {
                    StorageError::corrupt("native write segment range does not cover payload")
                })?;
                target.copy_from_slice(data);
                bytes
            };

        let verified_receipt = self.write_segment_for_intent_with_id_owned_verified(
            WriteGrantIntent::NativeWrite {
                keyspace_id,
                file_id,
                range,
                base_version: head.version,
            },
            self.next_write_intent()?,
            segment_bytes,
            durability,
        )?;
        let edit = TreeRangeEdit {
            range: write_range,
            replacement: Some(SegmentReplacement {
                segment_id: verified_receipt.descriptor.segment_id,
                segment_base: write_range.start,
            }),
        };
        let new_root = self
            .replace_tree_range_with_receipts(
                head.root,
                edit,
                std::slice::from_ref(&verified_receipt),
            )?
            .root;
        let new_size = head.size.max(end);

        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner,
            fence: MetadataFence::FileVersion(head.version),
            updates: vec![RootUpdate::FileRoot {
                file_id,
                old_root: head.root,
                new_root,
                new_size,
            }],
        })?;
        self.storage_nodes.mark_segment_referenced(
            verified_receipt.receipt(),
            commit_group.commit_seq,
            self.authority.as_ref(),
        )?;
        self.metadata
            .invalidate_append_sessions_for_file(keyspace_id, file_id)?;
        let committed = self.metadata.get_file_head(keyspace_id, file_id)?;

        Ok(FileWriteCommit {
            keyspace_id,
            file_id,
            range,
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability,
        })
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        let head = self.metadata.fork_device(MetadataForkRequest {
            source,
            target: request.target,
            name: request.name,
        })?;
        self.observability.record_with(
            StorageEventKind::DeviceForked,
            None,
            None,
            Some(head.latest_commit),
            None,
        );
        Ok(head.device_id)
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        let head = self.metadata.restore_device(source, point)?;
        self.observability.record_with(
            StorageEventKind::DeviceRestored,
            None,
            None,
            Some(head.latest_commit),
            None,
        );
        Ok(head.device_id)
    }

    pub fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId> {
        let head = self.metadata.restore_keyspace(source, point)?;
        self.observability.record_with(
            StorageEventKind::KeyspaceRestored,
            None,
            None,
            Some(head.latest_commit),
            None,
        );
        Ok(head.keyspace_id)
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.metadata.delete_device(device_id)
    }

    pub fn mark_reachable_for_gc(&self, policy: RetentionPolicy) -> Result<MetadataMarkReport> {
        self.metadata.mark_reachable_for_gc(policy)
    }

    pub fn sweep_metadata_after_mark(
        &self,
        policy: RetentionPolicy,
        epoch: u64,
    ) -> Result<MetadataSweepReport> {
        let sweep = self.metadata.sweep_unmarked_after_mark(policy, epoch)?;
        for segment_id in &sweep.released_segments {
            if self.storage_nodes.state(*segment_id)? == SegmentLifecycleState::Referenced {
                self.storage_nodes.release_segment(*segment_id)?;
            }
        }
        Ok(sweep)
    }

    pub fn run_metadata_custodian(
        &self,
        policy: RetentionPolicy,
    ) -> Result<MetadataCustodianReport> {
        let mark = self.mark_reachable_for_gc(policy.clone())?;
        let sweep = self.sweep_metadata_after_mark(policy, mark.epoch)?;
        let mut catalog_released_segments = Vec::new();
        for segment_id in &sweep.released_segments {
            if self.storage_nodes.state(*segment_id)? == SegmentLifecycleState::Released {
                catalog_released_segments.push(*segment_id);
            }
        }
        self.observability.increment(|counters| {
            counters.metadata_custodian_runs = counters.metadata_custodian_runs.saturating_add(1);
        });
        self.observability
            .record(StorageEventKind::MetadataCustodianRan);
        Ok(MetadataCustodianReport {
            mark,
            sweep,
            catalog_released_segments,
        })
    }

    pub fn run_storage_node_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let mut report = StorageNodeCustodianReport {
            expired_reservations: Vec::new(),
            failed_writes: Vec::new(),
            orphan_segments: Vec::new(),
            deleted_released_segments: Vec::new(),
        };

        for storage_node in self.storage_nodes.storage_node_ids()? {
            let response = self.storage_nodes.transport_for_node(storage_node)?.send(
                StorageNodeRequest::RunCustodian {
                    expired_write_intents: expired_write_intents.clone(),
                },
            )?;
            let StorageNodeResponse::Custodian(node_report) = response else {
                return Err(StorageError::corrupt(
                    "storage node returned unexpected custodian response",
                ));
            };
            report
                .expired_reservations
                .extend(node_report.expired_reservations);
            report.failed_writes.extend(node_report.failed_writes);
            report.orphan_segments.extend(node_report.orphan_segments);
            report
                .deleted_released_segments
                .extend(node_report.deleted_released_segments);
        }

        self.observability.increment(|counters| {
            counters.storage_node_custodian_runs =
                counters.storage_node_custodian_runs.saturating_add(1);
        });
        self.observability
            .record(StorageEventKind::StorageNodeCustodianRan);
        Ok(report)
    }

    fn split_device_range(
        &self,
        info: &DeviceInfo,
        range: ByteRange,
    ) -> Result<Vec<DeviceWriteChunk>> {
        let block_size = u64::from(info.spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let head = self.metadata.get_head(info.device_id)?;
        let mut chunks = Vec::new();

        for (shard, root) in head.shard_roots.iter().enumerate() {
            let node = self.metadata.get_metadata_node(*root)?;
            let Some(overlap) = node.covered_range.intersection(requested)? else {
                continue;
            };
            let shard_id = u32::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?;
            chunks.push(DeviceWriteChunk {
                shard_id: crate::id::ShardId::from_raw(shard_id),
                old_root: *root,
                range: overlap,
            });
        }

        if chunks.is_empty() && range.len != 0 {
            return Err(StorageError::corrupt(
                "device range did not overlap any shard roots",
            ));
        }

        Ok(chunks)
    }

    fn single_shard_for_block_range(
        &self,
        head: &DeviceHead,
        range: crate::api::BlockRange,
    ) -> Result<(ShardId, MetadataNodeId)> {
        for (shard, root) in head.shard_roots.iter().copied().enumerate() {
            let node = self.metadata.get_metadata_node(root)?;
            if node.covered_range.contains_range(range)? {
                let shard_id = u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?;
                return Ok((ShardId::from_raw(shard_id), root));
            }
        }
        Err(StorageError::invalid_argument(
            "block range is not contained by one shard",
        ))
    }

    #[cfg(test)]
    fn write_segment_for_owner(
        &self,
        owner: MappingOwner,
        data: &[u8],
    ) -> Result<SegmentWriteReceipt> {
        let write_intent = self.next_write_intent()?;
        self.write_segment_for_intent_with_id(
            WriteGrantIntent::Internal { owner },
            write_intent,
            data,
            WriteDurability::Acknowledged,
        )
    }

    #[cfg(test)]
    fn write_segment_for_intent_with_id(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<SegmentWriteReceipt> {
        self.write_segment_for_intent_with_id_owned(intent, write_intent, data.to_vec(), durability)
    }

    #[cfg(test)]
    fn write_segment_for_intent_with_id_owned(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: Vec<u8>,
        durability: WriteDurability,
    ) -> Result<SegmentWriteReceipt> {
        Ok(self
            .write_segment_for_intent_with_id_owned_verified(
                intent,
                write_intent,
                data,
                durability,
            )?
            .receipt)
    }

    fn write_segment_for_intent_with_id_owned_verified(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: Vec<u8>,
        durability: WriteDurability,
    ) -> Result<VerifiedSegmentReceipt> {
        let max_bytes = u64::try_from(data.len()).map_err(|_| {
            StorageError::invalid_argument("segment reservation byte length overflows u64")
        })?;
        let candidates = self.storage_nodes.storage_node_ids()?;
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        let segment_id = self.storage_nodes.allocate_segment_id()?;
        let grant = self.issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent,
            write_intent,
            segment_id,
            storage_node,
            max_bytes,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })?;
        self.write_granted_segment_verified(&grant, data)
    }

    pub fn issue_write_grant(&self, request: WriteGrantRequest) -> Result<WriteGrant> {
        match self.authority.issue_write_grant(request) {
            Ok(grant) => {
                self.observability.record_with_update(
                    StorageEventKind::GrantIssued,
                    Some(grant.storage_node),
                    Some(grant.segment_id),
                    None,
                    None,
                    |counters| {
                        counters.grants_issued = counters.grants_issued.saturating_add(1);
                    },
                );
                Ok(grant)
            }
            Err(error) => {
                self.observability.record_with_update(
                    StorageEventKind::GrantRejected,
                    None,
                    None,
                    None,
                    Some("scope"),
                    |counters| {
                        counters.grant_rejections = counters.grant_rejections.saturating_add(1);
                    },
                );
                Err(error)
            }
        }
    }

    pub fn issue_block_write_grant(
        &self,
        device_id: DeviceId,
        range: crate::api::BlockRange,
        durability: WriteDurability,
    ) -> Result<WriteGrant> {
        range.validate_non_empty()?;
        let head = self.metadata.get_head(device_id)?;
        let (shard_id, old_root) = self.single_shard_for_block_range(&head, range)?;
        let block_size = u64::from(self.metadata.config.block_size);
        let max_bytes = range
            .blocks
            .raw()
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("grant byte length overflows"))?;
        let candidates = self.storage_nodes.storage_node_ids()?;
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        let segment_id = self.storage_nodes.allocate_segment_id()?;
        let write_intent = self.next_write_intent()?;
        self.issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::BlockWrite {
                device_id,
                range,
                fence: head.generation,
                shard_id,
                old_root,
            },
            write_intent,
            segment_id,
            storage_node,
            max_bytes,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
    }

    pub fn issue_native_write_grant(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        segment_bytes: u64,
        durability: WriteDurability,
    ) -> Result<WriteGrant> {
        if range.len == 0 || segment_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "native write grant must contain bytes",
            ));
        }
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let candidates = self.storage_nodes.storage_node_ids()?;
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        let segment_id = self.storage_nodes.allocate_segment_id()?;
        let write_intent = self.next_write_intent()?;
        self.issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::NativeWrite {
                keyspace_id,
                file_id,
                range,
                base_version: head.version,
            },
            write_intent,
            segment_id,
            storage_node,
            max_bytes: segment_bytes,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
    }

    pub fn issue_native_append_grant(
        &self,
        reservation: AppendReservation,
        logical_bytes: u64,
        segment_bytes: u64,
        durability: WriteDurability,
    ) -> Result<WriteGrant> {
        if logical_bytes == 0 || segment_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "native append grant must contain bytes",
            ));
        }
        let head = self
            .metadata
            .get_file_head(reservation.keyspace_id, reservation.file_id)?;
        if head.size != reservation.offset || logical_bytes != reservation.len {
            return Err(StorageError::conflict("stale append reservation"));
        }
        self.metadata.validate_append_reservation(&reservation)?;
        let candidates = self.storage_nodes.storage_node_ids()?;
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        let segment_id = self.storage_nodes.allocate_segment_id()?;
        let write_intent = self.next_write_intent()?;
        self.issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent: WriteGrantIntent::NativeAppend {
                keyspace_id: reservation.keyspace_id,
                file_id: reservation.file_id,
                session_id: reservation.session_id,
                reservation_id: reservation.reservation_id,
                append_offset: reservation.offset,
                bytes: logical_bytes,
                writer_epoch: reservation.writer_epoch,
            },
            write_intent,
            segment_id,
            storage_node,
            max_bytes: segment_bytes,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })
    }

    pub fn write_granted_segment(
        &self,
        grant: &WriteGrant,
        data: Vec<u8>,
    ) -> Result<SegmentWriteReceipt> {
        Ok(self.write_granted_segment_verified(grant, data)?.receipt)
    }

    fn write_granted_segment_verified(
        &self,
        grant: &WriteGrant,
        data: Vec<u8>,
    ) -> Result<VerifiedSegmentReceipt> {
        let expected_segment = grant.segment_id;
        let storage_node = grant.storage_node;
        let response = self.storage_nodes.transport_for_node(storage_node)?.send(
            StorageNodeRequest::WriteSegment {
                grant: grant.clone(),
                bytes: data,
            },
        )?;
        let StorageNodeResponse::WriteSegment { receipt } = response else {
            return Err(StorageError::corrupt(
                "storage node returned unexpected write response",
            ));
        };
        let receipt = *receipt;
        if receipt.segment_id != expected_segment {
            return Err(StorageError::corrupt(
                "storage node write receipt disagrees with requested segment ID",
            ));
        }
        self.verify_receipt_matches_grant_observed(grant, &receipt)
    }

    pub fn storage_node_transport_for_grant(
        &self,
        grant: &WriteGrant,
    ) -> Result<Arc<dyn StorageNodeTransport>> {
        self.authority.verify_write_grant(
            grant,
            grant.storage_node,
            grant.segment_id,
            grant.max_bytes,
        )?;
        self.storage_nodes.transport_for_node(grant.storage_node)
    }

    pub fn verify_segment_receipt(
        &self,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        match self.authority.verify_segment_receipt(receipt) {
            Ok(verified) => {
                self.observability.record_with_update(
                    StorageEventKind::ReceiptVerified,
                    Some(receipt.storage_node),
                    Some(receipt.segment_id),
                    None,
                    None,
                    |counters| {
                        counters.receipts_verified = counters.receipts_verified.saturating_add(1);
                    },
                );
                Ok(verified)
            }
            Err(error) => {
                let reason = receipt_rejection_reason(&error);
                self.observability.record_with_update(
                    StorageEventKind::ReceiptRejected,
                    Some(receipt.storage_node),
                    Some(receipt.segment_id),
                    None,
                    Some(reason),
                    |counters| count_receipt_rejection(counters, reason),
                );
                Err(error)
            }
        }
    }

    fn verify_receipt_matches_grant_observed(
        &self,
        grant: &WriteGrant,
        receipt: &SegmentWriteReceipt,
    ) -> Result<VerifiedSegmentReceipt> {
        match self.authority.verify_receipt_matches_grant(grant, receipt) {
            Ok(verified) => {
                self.observability.record_with_update(
                    StorageEventKind::ReceiptVerified,
                    Some(receipt.storage_node),
                    Some(receipt.segment_id),
                    None,
                    None,
                    |counters| {
                        counters.receipts_verified = counters.receipts_verified.saturating_add(1);
                    },
                );
                Ok(verified)
            }
            Err(error) => {
                let reason = receipt_rejection_reason(&error);
                self.observability.record_with_update(
                    StorageEventKind::ReceiptRejected,
                    Some(receipt.storage_node),
                    Some(receipt.segment_id),
                    None,
                    Some(reason),
                    |counters| count_receipt_rejection(counters, reason),
                );
                Err(error)
            }
        }
    }

    pub fn submit_block_write_receipt(
        &self,
        grant: &WriteGrant,
        receipt: SegmentWriteReceipt,
    ) -> Result<WriteCommit> {
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );
        let verified = self.verify_receipt_matches_grant_observed(grant, &receipt)?;
        let WriteGrantIntent::BlockWrite {
            device_id,
            range,
            fence,
            shard_id,
            old_root,
        } = receipt.intent
        else {
            return Err(StorageError::invalid_argument(
                "trusted block publish requires a block write receipt",
            ));
        };
        if receipt.owner != MappingOwner::BlockDevice(device_id) {
            return Err(StorageError::conflict(
                "receipt owner does not match block device intent",
            ));
        }
        let current = self.metadata.get_head(device_id)?;
        let shard = usize::try_from(shard_id.raw())
            .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
        let current_root = current
            .shard_roots
            .get(shard)
            .ok_or_else(|| StorageError::invalid_argument("receipt shard is outside device"))?;
        if *current_root != old_root {
            return Err(StorageError::conflict("stale shard root"));
        }
        let new_root = self
            .replace_tree_range_with_receipts(
                old_root,
                TreeRangeEdit {
                    range,
                    replacement: Some(SegmentReplacement {
                        segment_id: verified.descriptor.segment_id,
                        segment_base: range.start,
                    }),
                },
                std::slice::from_ref(&verified),
            )?
            .root;
        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(device_id),
            fence: MetadataFence::DeviceGeneration(fence),
            updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                shard_id,
                old_root,
                new_root,
            })],
        })?;
        self.storage_nodes.mark_segment_referenced(
            &receipt,
            commit_group.commit_seq,
            self.authority.as_ref(),
        )?;
        let block_size = u64::from(self.metadata.config.block_size);
        let byte_offset = range
            .start
            .raw()
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("receipt byte offset overflows"))?;
        let byte_len = range
            .blocks
            .raw()
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("receipt byte length overflows"))?;
        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range: ByteRange::new(byte_offset, byte_len),
            durability: receipt.durability,
        })
    }

    pub fn submit_native_write_receipt(
        &self,
        grant: &WriteGrant,
        receipt: SegmentWriteReceipt,
    ) -> Result<FileWriteCommit> {
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );
        let verified = self.verify_receipt_matches_grant_observed(grant, &receipt)?;
        let WriteGrantIntent::NativeWrite {
            keyspace_id,
            file_id,
            range,
            base_version,
        } = receipt.intent
        else {
            return Err(StorageError::invalid_argument(
                "trusted native write publish requires a native write receipt",
            ));
        };
        if receipt.owner != MappingOwner::NativeKeyspace(keyspace_id) {
            return Err(StorageError::conflict(
                "receipt owner does not match native keyspace intent",
            ));
        }
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        if head.version != base_version {
            return Err(StorageError::conflict("stale native file version"));
        }
        let end = range.end_exclusive()?;
        let block_size = u64::from(self.metadata.config.block_size);
        let first_block = range.offset / block_size;
        let requested_start = first_block
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("native write range overflows"))?;
        let segment_blocks = blocks_for_bytes(end - requested_start, block_size)?;
        let write_range = crate::api::BlockRange::new(
            BlockIndex::from_raw(first_block),
            BlockCount::from_raw(segment_blocks),
        );
        let root = self.metadata.get_metadata_node(head.root)?;
        if !root.covered_range.contains_range(write_range)? {
            return Err(StorageError::invalid_argument(
                "native file write exceeds file root coverage",
            ));
        }
        let expected_segment_bytes = segment_blocks.checked_mul(block_size).ok_or_else(|| {
            StorageError::invalid_argument("native write segment length overflows")
        })?;
        if verified.descriptor.bytes != expected_segment_bytes {
            return Err(StorageError::conflict(
                "native write receipt byte count does not match metadata intent",
            ));
        }
        let new_root = self
            .replace_tree_range_with_receipts(
                head.root,
                TreeRangeEdit {
                    range: write_range,
                    replacement: Some(SegmentReplacement {
                        segment_id: verified.descriptor.segment_id,
                        segment_base: write_range.start,
                    }),
                },
                std::slice::from_ref(&verified),
            )?
            .root;
        let new_size = head.size.max(end);
        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner: MappingOwner::NativeKeyspace(keyspace_id),
            fence: MetadataFence::FileVersion(base_version),
            updates: vec![RootUpdate::FileRoot {
                file_id,
                old_root: head.root,
                new_root,
                new_size,
            }],
        })?;
        self.storage_nodes.mark_segment_referenced(
            &receipt,
            commit_group.commit_seq,
            self.authority.as_ref(),
        )?;
        self.metadata
            .invalidate_append_sessions_for_file(keyspace_id, file_id)?;
        let committed = self.metadata.get_file_head(keyspace_id, file_id)?;
        Ok(FileWriteCommit {
            keyspace_id,
            file_id,
            range,
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability: receipt.durability,
        })
    }

    pub fn submit_native_append_receipt(
        &self,
        grant: &WriteGrant,
        receipt: SegmentWriteReceipt,
    ) -> Result<AppendCommit> {
        self.observability.record_with_update(
            StorageEventKind::CoordinatorWriteStarted,
            None,
            None,
            None,
            None,
            |counters| {
                counters.coordinator_write_attempts =
                    counters.coordinator_write_attempts.saturating_add(1);
            },
        );
        let verified = self.verify_receipt_matches_grant_observed(grant, &receipt)?;
        let (
            keyspace_id,
            file_id,
            session_id,
            reservation_id,
            append_offset,
            logical_bytes,
            writer_epoch,
        ) = match receipt.intent {
            WriteGrantIntent::NativeAppend {
                keyspace_id,
                file_id,
                session_id,
                reservation_id,
                append_offset,
                bytes,
                writer_epoch,
            }
            | WriteGrantIntent::NativeReservedAppend {
                keyspace_id,
                file_id,
                session_id,
                reservation_id,
                append_offset,
                bytes,
                writer_epoch,
            } => (
                keyspace_id,
                file_id,
                session_id,
                reservation_id,
                append_offset,
                bytes,
                writer_epoch,
            ),
            _ => {
                return Err(StorageError::invalid_argument(
                    "trusted native append publish requires a native append receipt",
                ));
            }
        };
        if receipt.owner != MappingOwner::NativeKeyspace(keyspace_id) {
            return Err(StorageError::conflict(
                "receipt owner does not match native keyspace intent",
            ));
        }
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        if head.size != append_offset {
            return Err(StorageError::conflict("stale native append receipt"));
        }
        let reservation = AppendReservation {
            keyspace_id,
            file_id,
            session_id,
            reservation_id,
            writer_epoch,
            offset: append_offset,
            len: logical_bytes,
        };
        self.metadata.validate_append_reservation(&reservation)?;
        let block_size = u64::from(self.metadata.config.block_size);
        let tail_bytes = head.size % block_size;
        let segment_start_block = head.size / block_size;
        let segment_payload_len = tail_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| StorageError::invalid_argument("append segment length overflows"))?;
        let segment_blocks = blocks_for_bytes(segment_payload_len, block_size)?;
        let expected_segment_bytes = segment_blocks.checked_mul(block_size).ok_or_else(|| {
            StorageError::invalid_argument("append segment byte length overflows")
        })?;
        if verified.descriptor.bytes != expected_segment_bytes {
            return Err(StorageError::conflict(
                "native append receipt byte count does not match metadata intent",
            ));
        }
        let append_range = crate::api::BlockRange::new(
            BlockIndex::from_raw(segment_start_block),
            BlockCount::from_raw(segment_blocks),
        );
        if tail_bytes == 0 && self.tree_has_mappings(head.root, append_range)? {
            return Err(StorageError::conflict(
                "append range overlaps existing file metadata",
            ));
        }
        if tail_bytes != 0 && segment_blocks > 1 {
            let next_block = segment_start_block
                .checked_add(1)
                .ok_or_else(|| StorageError::invalid_argument("append range overflows"))?;
            let new_blocks = crate::api::BlockRange::new(
                BlockIndex::from_raw(next_block),
                BlockCount::from_raw(segment_blocks - 1),
            );
            if self.tree_has_mappings(head.root, new_blocks)? {
                return Err(StorageError::conflict(
                    "append range overlaps existing file metadata after tail block",
                ));
            }
        }
        let new_root = self
            .replace_tree_range_with_receipts(
                head.root,
                TreeRangeEdit {
                    range: append_range,
                    replacement: Some(SegmentReplacement {
                        segment_id: verified.descriptor.segment_id,
                        segment_base: append_range.start,
                    }),
                },
                std::slice::from_ref(&verified),
            )?
            .root;
        let new_size = head
            .size
            .checked_add(logical_bytes)
            .ok_or_else(|| StorageError::invalid_argument("file size overflows u64"))?;
        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner: MappingOwner::NativeKeyspace(keyspace_id),
            fence: MetadataFence::AppendReservation {
                session_id,
                reservation_id,
                offset: append_offset,
                len: logical_bytes,
                writer_epoch,
            },
            updates: vec![RootUpdate::FileRoot {
                file_id,
                old_root: head.root,
                new_root,
                new_size,
            }],
        })?;
        self.storage_nodes.mark_segment_referenced(
            &receipt,
            commit_group.commit_seq,
            self.authority.as_ref(),
        )?;
        let committed = self.metadata.get_file_head(keyspace_id, file_id)?;
        Ok(AppendCommit {
            keyspace_id,
            file_id,
            extent_id: self.next_extent_id()?,
            range: ByteRange::new(head.size, logical_bytes),
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability: receipt.durability,
        })
    }

    fn verified_receipts_for_entries(
        &self,
        entries: &[LeafEntry],
    ) -> Result<Vec<VerifiedSegmentReceipt>> {
        self.verified_receipts_for_entries_with_cache(entries, &[])
    }

    fn verified_receipts_for_entries_with_cache(
        &self,
        entries: &[LeafEntry],
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<Vec<VerifiedSegmentReceipt>> {
        let mut cache: BTreeMap<SegmentId, VerifiedSegmentReceipt> = additional_receipts
            .iter()
            .map(|receipt| (receipt.descriptor.segment_id, receipt.clone()))
            .collect();
        let mut receipts: BTreeMap<SegmentId, VerifiedSegmentReceipt> = BTreeMap::new();
        for entry in entries {
            if let std::collections::btree_map::Entry::Vacant(vacant) =
                receipts.entry(entry.segment_id)
            {
                if let Some(receipt) = cache.remove(&entry.segment_id) {
                    vacant.insert(receipt);
                } else {
                    let receipt = self.storage_nodes.receipt_for_segment(entry.segment_id)?;
                    vacant.insert(self.authority.verify_segment_receipt(&receipt)?);
                }
            }
        }
        Ok(receipts.into_values().collect())
    }

    fn next_write_intent(&self) -> Result<WriteIntentId> {
        let mut next = lock(&self.next_write_intent)?;
        let id = WriteIntentId::from_raw(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("write intent id overflow"))?;
        Ok(id)
    }

    fn next_extent_id(&self) -> Result<ExtentId> {
        let mut next = lock(&self.next_extent_id)?;
        let id = ExtentId::from_raw(*next);
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("extent id overflow"))?;
        Ok(id)
    }

    fn replace_tree_range(
        &self,
        root_id: MetadataNodeId,
        edit: TreeRangeEdit,
    ) -> Result<TreeEditResult> {
        self.replace_tree_range_with_receipts(root_id, edit, &[])
    }

    fn replace_tree_range_with_receipts(
        &self,
        root_id: MetadataNodeId,
        edit: TreeRangeEdit,
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<TreeEditResult> {
        edit.range.validate_non_empty()?;
        let root = self.metadata.get_metadata_node(root_id)?;
        if !root.covered_range.contains_range(edit.range)? {
            return Err(StorageError::invalid_argument(
                "edit range is outside metadata tree coverage",
            ));
        }
        self.replace_tree_range_at(&root, edit, additional_receipts)
    }

    fn replace_tree_range_at(
        &self,
        node: &MetadataNode,
        edit: TreeRangeEdit,
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<TreeEditResult> {
        if !node.covered_range.overlaps(edit.range)? {
            return Ok(TreeEditResult {
                root: node.node_id,
                changed: false,
            });
        }

        match &node.kind {
            MetadataNodeKind::Leaf { entries } => {
                let Some(overlap) = node.covered_range.intersection(edit.range)? else {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                };
                let replacement = edit.replacement.map(|replacement| {
                    let offset = overlap.start.raw() - replacement.segment_base.raw();
                    LeafEntry {
                        logical_start: overlap.start,
                        blocks: overlap.blocks,
                        segment_id: replacement.segment_id,
                        segment_offset: BlockIndex::from_raw(offset),
                    }
                });
                let new_entries =
                    replace_leaf_entries(entries, node.covered_range, overlap, replacement)?;
                if new_entries == *entries {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }
                let segment_receipts = self
                    .verified_receipts_for_entries_with_cache(&new_entries, additional_receipts)?;
                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Leaf {
                        entries: new_entries,
                    },
                )?;
                let segment_descriptors: Vec<_> = segment_receipts
                    .iter()
                    .map(|receipt| receipt.descriptor.clone())
                    .collect();
                new_node.validate(&segment_descriptors)?;
                self.metadata.persist_metadata_node(MetadataNodeWrite::new(
                    new_node.clone(),
                    segment_receipts,
                ))?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
            MetadataNodeKind::Internal { children } => {
                let mut changed = false;
                let mut new_children = Vec::with_capacity(children.len());
                for child in children {
                    if child.range.overlaps(edit.range)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        let child_result =
                            self.replace_tree_range_at(&child_node, edit, additional_receipts)?;
                        changed |= child_result.changed;
                        new_children.push(MetadataChild {
                            range: child.range,
                            node_id: child_result.root,
                        });
                    } else {
                        new_children.push(child.clone());
                    }
                }

                if !changed {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }

                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Internal {
                        children: new_children,
                    },
                )?;
                new_node.validate(&[])?;
                self.metadata
                    .persist_metadata_node(MetadataNodeWrite::new(new_node.clone(), Vec::new()))?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
        }
    }

    fn tree_has_mappings(
        &self,
        root_id: MetadataNodeId,
        range: crate::api::BlockRange,
    ) -> Result<bool> {
        range.validate_non_empty()?;
        let node = self.metadata.get_metadata_node(root_id)?;
        self.node_has_mappings(&node, range)
    }

    fn node_has_mappings(
        &self,
        node: &MetadataNode,
        range: crate::api::BlockRange,
    ) -> Result<bool> {
        if !node.covered_range.overlaps(range)? {
            return Ok(false);
        }

        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(range)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        if self.node_has_mappings(&child_node, range)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            MetadataNodeKind::Leaf { entries } => {
                for entry in entries {
                    if entry.logical_range().overlaps(range)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub fn validate_metadata_tree(&self, root_id: MetadataNodeId) -> Result<MetadataTreeStats> {
        let mut visited = BTreeSet::new();
        self.validate_metadata_tree_at(root_id, 1, &mut visited)
    }

    fn validate_metadata_tree_at(
        &self,
        node_id: MetadataNodeId,
        depth: usize,
        visited: &mut BTreeSet<MetadataNodeId>,
    ) -> Result<MetadataTreeStats> {
        if !visited.insert(node_id) {
            return Err(StorageError::corrupt(
                "metadata tree contains a repeated node ID",
            ));
        }

        let node = self.metadata.get_metadata_node(node_id)?;
        match &node.kind {
            MetadataNodeKind::Leaf { entries } => {
                if node.covered_range.blocks.raw() > self.metadata.config.metadata_leaf_blocks {
                    return Err(StorageError::corrupt(
                        "metadata leaf exceeds configured leaf block span",
                    ));
                }
                let receipts = self.verified_receipts_for_entries(entries)?;
                let descriptors: Vec<_> = receipts
                    .iter()
                    .map(|receipt| receipt.descriptor.clone())
                    .collect();
                node.validate(&descriptors)?;
                Ok(MetadataTreeStats {
                    nodes: 1,
                    leaves: 1,
                    max_depth: depth,
                })
            }
            MetadataNodeKind::Internal { children } => {
                if children.len() > self.metadata.config.metadata_fanout {
                    return Err(StorageError::corrupt(
                        "metadata internal node exceeds configured fanout",
                    ));
                }
                node.validate(&[])?;
                let mut stats = MetadataTreeStats {
                    nodes: 1,
                    leaves: 0,
                    max_depth: depth,
                };
                for child in children {
                    let child_node = self.metadata.get_metadata_node(child.node_id)?;
                    if child_node.covered_range != child.range {
                        return Err(StorageError::corrupt(
                            "metadata child range does not match child node coverage",
                        ));
                    }
                    let child_stats =
                        self.validate_metadata_tree_at(child.node_id, depth + 1, visited)?;
                    stats.nodes += child_stats.nodes;
                    stats.leaves += child_stats.leaves;
                    stats.max_depth = stats.max_depth.max(child_stats.max_depth);
                }
                Ok(stats)
            }
        }
    }

    pub fn metadata_tree_node_ids(&self, root_id: MetadataNodeId) -> Result<Vec<MetadataNodeId>> {
        let mut out = Vec::new();
        self.collect_metadata_tree_node_ids(root_id, &mut out)?;
        Ok(out)
    }

    fn collect_metadata_tree_node_ids(
        &self,
        node_id: MetadataNodeId,
        out: &mut Vec<MetadataNodeId>,
    ) -> Result<()> {
        out.push(node_id);
        let node = self.metadata.get_metadata_node(node_id)?;
        if let MetadataNodeKind::Internal { children } = node.kind {
            for child in children {
                self.collect_metadata_tree_node_ids(child.node_id, out)?;
            }
        }
        Ok(())
    }

    pub fn render_metadata_tree(&self, root_id: MetadataNodeId) -> Result<String> {
        let mut out = String::new();
        self.render_metadata_tree_at(root_id, 0, &mut out)?;
        Ok(out)
    }

    fn render_metadata_tree_at(
        &self,
        node_id: MetadataNodeId,
        depth: usize,
        out: &mut String,
    ) -> Result<()> {
        let node = self.metadata.get_metadata_node(node_id)?;
        let indent = "  ".repeat(depth);
        match node.kind {
            MetadataNodeKind::Internal { children } => {
                out.push_str(&format!(
                    "{indent}node {} internal [{}..{}) children={}\n",
                    node.node_id,
                    node.covered_range.start.raw(),
                    node.covered_range.end_exclusive()?.raw(),
                    children.len()
                ));
                for child in children {
                    self.render_metadata_tree_at(child.node_id, depth + 1, out)?;
                }
            }
            MetadataNodeKind::Leaf { entries } => {
                out.push_str(&format!(
                    "{indent}node {} leaf [{}..{}) entries={}\n",
                    node.node_id,
                    node.covered_range.start.raw(),
                    node.covered_range.end_exclusive()?.raw(),
                    entries.len()
                ));
                for entry in entries {
                    out.push_str(&format!(
                        "{indent}  [{}..{}) -> segment {}@{}\n",
                        entry.logical_start.raw(),
                        entry.logical_range().end_exclusive()?.raw(),
                        entry.segment_id,
                        entry.segment_offset.raw()
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let info = self.metadata.device_info(device_id)?;
        range.validate_for_device(&info.spec)?;
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        buf.fill(0);
        if range.len == 0 {
            return Ok(());
        }

        let block_size = u64::from(info.spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let head = self.metadata.get_head(device_id)?;

        for root in head.shard_roots {
            let node = self.metadata.get_metadata_node(root)?;
            if node.covered_range.overlaps(requested)? {
                self.read_metadata_node(&node, requested, block_size, buf)?;
            }
        }

        Ok(())
    }

    pub fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }
        let end = range.end_exclusive()?;
        if end > head.size {
            return Err(StorageError::invalid_argument(
                "native file read extends past end of file",
            ));
        }

        buf.fill(0);
        if range.len == 0 {
            let _ = self.metadata.get_metadata_node(head.root)?;
            return Ok(());
        }

        let block_size = u64::from(self.metadata.config.block_size);
        let first_block = range.offset / block_size;
        let requested_start = first_block
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("native read range overflows"))?;
        let requested_blocks = blocks_for_bytes(end - requested_start, block_size)?;
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(first_block),
            BlockCount::from_raw(requested_blocks),
        );
        let root = self.metadata.get_metadata_node(head.root)?;
        let scratch_len = requested_blocks.checked_mul(block_size).ok_or_else(|| {
            StorageError::invalid_argument("native read scratch length overflows")
        })?;
        let scratch_len = usize::try_from(scratch_len).map_err(|_| {
            StorageError::invalid_argument("native read scratch length overflows usize")
        })?;
        let mut scratch = vec![0; scratch_len];
        self.read_metadata_node(&root, requested, block_size, &mut scratch)?;
        let start = usize::try_from(range.offset % block_size)
            .map_err(|_| StorageError::invalid_argument("native read offset overflows usize"))?;
        let len = usize::try_from(range.len)
            .map_err(|_| StorageError::invalid_argument("native read length overflows usize"))?;
        let copy_end = start
            .checked_add(len)
            .ok_or_else(|| StorageError::invalid_argument("native read end overflows"))?;
        let bytes = scratch.get(start..copy_end).ok_or_else(|| {
            StorageError::corrupt("native read scratch range does not cover request")
        })?;
        buf.copy_from_slice(bytes);
        Ok(())
    }

    fn read_metadata_node(
        &self,
        node: &MetadataNode,
        requested: crate::api::BlockRange,
        block_size: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.read_metadata_node(&child_node, requested, block_size, buf)?;
                    }
                }
                Ok(())
            }
            MetadataNodeKind::Leaf { entries } => {
                for entry in entries {
                    let Some(overlap) = entry.logical_range().intersection(requested)? else {
                        continue;
                    };
                    let segment_offset_blocks = entry
                        .segment_offset
                        .raw()
                        .checked_add(overlap.start.raw() - entry.logical_start.raw())
                        .ok_or_else(|| {
                            StorageError::invalid_argument("segment read offset overflows")
                        })?;
                    let segment_range = ByteRange::new(
                        segment_offset_blocks
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte offset overflows")
                            })?,
                        overlap
                            .blocks
                            .raw()
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte length overflows")
                            })?,
                    );
                    let output_offset = usize::try_from(
                        (overlap.start.raw() - requested.start.raw())
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("read output offset overflows")
                            })?,
                    )
                    .map_err(|_| {
                        StorageError::invalid_argument("read output offset overflows usize")
                    })?;
                    let output_len = usize::try_from(segment_range.len).map_err(|_| {
                        StorageError::invalid_argument("read output length overflows usize")
                    })?;
                    let output_end = output_offset.checked_add(output_len).ok_or_else(|| {
                        StorageError::invalid_argument("read output end overflows")
                    })?;
                    let output = buf.get_mut(output_offset..output_end).ok_or_else(|| {
                        StorageError::corrupt("metadata read output range exceeds buffer")
                    })?;
                    self.storage_nodes
                        .read_segment(entry.segment_id, segment_range, output)?;
                }
                Ok(())
            }
        }
    }
}

impl Default for LocalCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
struct DurableStorePaths {
    metadata: PathBuf,
    data_dir: PathBuf,
}

impl DurableStorePaths {
    fn new(root: impl AsRef<Path>, _storage_node: StorageNodeId) -> Result<Self> {
        let root = root.as_ref();
        let data_dir = root.join("data");
        let tmp_dir = root.join("tmp");
        fs::create_dir_all(&data_dir).map_err(fs_error)?;
        fs::create_dir_all(&tmp_dir).map_err(fs_error)?;
        Ok(Self {
            metadata: root.join("metadata.sqlite"),
            data_dir,
        })
    }
}

fn node_catalog_table(_storage_node: StorageNodeId, table: &'static str) -> Result<&'static str> {
    match table {
        "node_meta" | "data_logs" | "segment_placements" | "segment_catalog_entries" => Ok(table),
        _ => Err(StorageError::invalid_argument(
            "unknown storage-node catalog table",
        )),
    }
}

fn node_catalog_path(data_dir: &Path, storage_node: StorageNodeId) -> PathBuf {
    node_data_log_dir(data_dir, storage_node).join("catalog.sqlite")
}

fn discover_node_catalogs(data_dir: &Path) -> Result<BTreeSet<StorageNodeId>> {
    let mut out = BTreeSet::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(data_dir).map_err(fs_error)? {
        let entry = entry.map_err(fs_error)?;
        if !entry.file_type().map_err(fs_error)?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(raw_node) = name.strip_prefix("node-") else {
            continue;
        };
        let Ok(raw_node) = raw_node.parse::<u128>() else {
            continue;
        };
        let storage_node = StorageNodeId::from_raw(raw_node);
        if node_catalog_path(data_dir, storage_node).exists() {
            out.insert(storage_node);
        }
    }
    Ok(out)
}

#[derive(Default)]
struct NodeCatalogs {
    connections: BTreeMap<StorageNodeId, Mutex<Connection>>,
}

impl fmt::Debug for NodeCatalogs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeCatalogs")
            .field(
                "storage_nodes",
                &self.connections.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl NodeCatalogs {
    fn open(
        paths: &DurableStorePaths,
        configured_storage_nodes: Vec<StorageNodeId>,
    ) -> Result<Self> {
        let mut nodes: BTreeSet<_> = configured_storage_nodes.into_iter().collect();
        nodes.extend(discover_node_catalogs(&paths.data_dir)?);

        let mut connections = BTreeMap::new();
        for storage_node in nodes {
            connections.insert(
                storage_node,
                Mutex::new(open_node_catalog(paths, storage_node)?),
            );
        }
        Ok(Self { connections })
    }

    fn storage_nodes(&self) -> impl Iterator<Item = StorageNodeId> + '_ {
        self.connections.keys().copied()
    }

    fn lock(&self, storage_node: StorageNodeId) -> Result<MutexGuard<'_, Connection>> {
        let conn = self.connections.get(&storage_node).ok_or_else(|| {
            StorageError::not_found("storage_node_catalog", storage_node.to_string())
        })?;
        lock(conn)
    }
}

fn open_node_catalog(paths: &DurableStorePaths, storage_node: StorageNodeId) -> Result<Connection> {
    let data_dir = node_data_log_dir(&paths.data_dir, storage_node);
    let catalog_path = node_catalog_path(&paths.data_dir, storage_node);
    let existed = catalog_path.exists();
    fs::create_dir_all(&data_dir).map_err(fs_error)?;
    let conn = Connection::open(&catalog_path).map_err(sqlite_error)?;
    configure_sqlite_connection(&conn)?;
    initialize_node_catalog_schema(&conn)?;
    if !existed {
        sync_dir(&data_dir)?;
    }
    Ok(conn)
}

fn configure_sqlite_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error)
}

fn initialize_node_catalog_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS node_meta (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          storage_node TEXT NOT NULL,
          ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
          next_catalog_segment_id TEXT NOT NULL,
          segment_store_next_offset INTEGER NOT NULL CHECK (segment_store_next_offset >= 0)
        );
        CREATE TABLE IF NOT EXISTS data_logs (
          log_id INTEGER PRIMARY KEY CHECK (log_id >= 0),
          state TEXT NOT NULL CHECK (state IN ('active', 'sealed', 'deleted')),
          total_bytes INTEGER NOT NULL CHECK (total_bytes >= 0),
          live_bytes INTEGER NOT NULL CHECK (live_bytes >= 0),
          dead_bytes INTEGER NOT NULL CHECK (dead_bytes >= 0)
        );
        CREATE INDEX IF NOT EXISTS idx_data_logs_state_dead
          ON data_logs(state, dead_bytes);
        CREATE TABLE IF NOT EXISTS segment_placements (
          segment_id TEXT PRIMARY KEY,
          data_log_id INTEGER NOT NULL,
          record_offset INTEGER NOT NULL CHECK (record_offset >= 0),
          record_bytes INTEGER NOT NULL CHECK (record_bytes > 0),
          payload_offset INTEGER NOT NULL CHECK (payload_offset >= 0),
          payload_bytes INTEGER NOT NULL CHECK (payload_bytes > 0),
          checksum TEXT NOT NULL,
          current INTEGER NOT NULL CHECK (current IN (0, 1)),
          FOREIGN KEY(data_log_id) REFERENCES data_logs(log_id)
        );
        CREATE INDEX IF NOT EXISTS idx_segment_placements_log_current
          ON segment_placements(data_log_id, current);
        CREATE TABLE IF NOT EXISTS segment_catalog_entries (
          segment_id TEXT PRIMARY KEY,
          payload BLOB NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)
}

/// Policy for rolled durable data logs and explicit compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableDataLogPolicy {
    pub target_data_log_bytes: u64,
    pub min_reclaimable_ratio_ppm: u32,
    pub min_reclaimable_bytes: u64,
    pub max_compaction_copy_bytes: u64,
}

impl Default for DurableDataLogPolicy {
    fn default() -> Self {
        Self {
            target_data_log_bytes: 64 * 1024 * 1024,
            min_reclaimable_ratio_ppm: 500_000,
            min_reclaimable_bytes: 4 * 1024 * 1024,
            max_compaction_copy_bytes: 64 * 1024 * 1024,
        }
    }
}

impl DurableDataLogPolicy {
    fn validate(self) -> Result<()> {
        if self.target_data_log_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "target_data_log_bytes must be greater than zero",
            ));
        }
        if self.min_reclaimable_ratio_ppm > 1_000_000 {
            return Err(StorageError::invalid_argument(
                "min_reclaimable_ratio_ppm must be <= 1_000_000",
            ));
        }
        if self.max_compaction_copy_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_compaction_copy_bytes must be greater than zero",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn compact_everything_for_test() -> Self {
        Self {
            target_data_log_bytes: 8 * 4096,
            min_reclaimable_ratio_ppm: 1,
            min_reclaimable_bytes: 1,
            max_compaction_copy_bytes: u64::MAX,
        }
    }
}

/// Durable data-log identity within a storage node.
///
/// The pair is provider-owned diagnostic state. Public block and native callers
/// can observe it in maintenance reports, but they must not choose log IDs or
/// infer physical offsets from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurableDataLogRef {
    /// Storage node that owns the log.
    pub storage_node: StorageNodeId,
    /// Node-local monotonically increasing log identifier.
    pub log_id: u64,
}

/// Summary of data-log compaction work completed by a maintenance tick.
///
/// A successful report means all listed relocations and deletions were
/// published durably in SQLite before any old log file was removed. Failure may
/// leave already-completed maintenance work in place, but must not make
/// acknowledged segment data unreadable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableCompactionReport {
    /// Sealed logs that contained no live placements and were removed.
    pub deleted_logs: Vec<DurableDataLogRef>,
    /// Sealed logs whose live placements were copied elsewhere before removal.
    pub relocated_logs: Vec<DurableDataLogRef>,
    /// Segment IDs whose current placement moved during compaction.
    pub relocated_segments: Vec<SegmentId>,
    /// Live payload bytes copied into replacement logs.
    pub bytes_copied: u64,
    /// Total old log bytes removed from disk.
    pub bytes_deleted: u64,
}

/// Runtime mode for durable maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaintenanceMode {
    /// No background work. Callers explicitly observe, plan, and run ticks.
    #[default]
    Manual,
    /// A write may run one bounded maintenance tick before it is admitted.
    Opportunistic,
    /// A local worker runs bounded ticks after writes or custodian work notify it.
    AlwaysOn,
}

/// Policy knobs for deterministic durable maintenance and write admission.
///
/// The policy lives below the public block/native APIs. It may throttle or
/// reject writes with `StorageError::Unavailable`, but it must not change read,
/// fork, snapshot, restore, or flush semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenancePolicy {
    /// How maintenance is driven at runtime.
    pub mode: MaintenanceMode,
    /// Data-log rolling and compaction thresholds.
    pub data_log_policy: DurableDataLogPolicy,
    /// Whether writes consult admission thresholds before they run.
    pub write_backpressure_enabled: bool,
    /// Dirty bytes at or above this value schedule maintenance.
    pub dirty_low_watermark_bytes: u64,
    /// Dirty bytes at or above this value throttle admitted writes.
    pub dirty_high_watermark_bytes: u64,
    /// Sealed-log count above this value throttles admitted writes.
    pub max_sealed_logs: usize,
    /// Reclaimable debt above this value rejects admitted writes.
    pub max_reclaimable_debt_bytes: u64,
    /// Maximum live bytes a maintenance tick may copy.
    pub compaction_copy_budget_per_tick: u64,
    /// SQLite WAL size above this value throttles admitted writes.
    pub max_sqlite_wal_bytes: u64,
    /// Maximum logs considered by one scheduler tick.
    pub max_logs_scanned_per_tick: usize,
    /// Local v1 supports exactly one executor.
    pub max_concurrent_compaction_jobs: usize,
}

impl Default for MaintenancePolicy {
    fn default() -> Self {
        Self {
            mode: MaintenanceMode::Manual,
            data_log_policy: DurableDataLogPolicy::default(),
            write_backpressure_enabled: false,
            dirty_low_watermark_bytes: 16 * 1024 * 1024,
            dirty_high_watermark_bytes: 128 * 1024 * 1024,
            max_sealed_logs: 128,
            max_reclaimable_debt_bytes: 512 * 1024 * 1024,
            compaction_copy_budget_per_tick: 32 * 1024 * 1024,
            max_sqlite_wal_bytes: 128 * 1024 * 1024,
            max_logs_scanned_per_tick: 16,
            max_concurrent_compaction_jobs: 1,
        }
    }
}

impl MaintenancePolicy {
    /// Build the default manual policy with a specific data-log policy.
    pub fn manual(data_log_policy: DurableDataLogPolicy) -> Self {
        Self {
            data_log_policy,
            ..Self::default()
        }
    }

    /// Validate that the policy is supported by the local durable provider.
    ///
    /// Success means the scheduler can evaluate this policy deterministically.
    /// It does not reserve disk space or start any background worker.
    pub fn validate(self) -> Result<()> {
        self.data_log_policy.validate()?;
        if self.dirty_low_watermark_bytes > self.dirty_high_watermark_bytes {
            return Err(StorageError::invalid_argument(
                "dirty_low_watermark_bytes must be <= dirty_high_watermark_bytes",
            ));
        }
        if self.max_sealed_logs == 0 {
            return Err(StorageError::invalid_argument(
                "max_sealed_logs must be greater than zero",
            ));
        }
        if self.max_reclaimable_debt_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_reclaimable_debt_bytes must be greater than zero",
            ));
        }
        if self.compaction_copy_budget_per_tick == 0 {
            return Err(StorageError::invalid_argument(
                "compaction_copy_budget_per_tick must be greater than zero",
            ));
        }
        if self.max_sqlite_wal_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "max_sqlite_wal_bytes must be greater than zero",
            ));
        }
        if self.max_logs_scanned_per_tick == 0 {
            return Err(StorageError::invalid_argument(
                "max_logs_scanned_per_tick must be greater than zero",
            ));
        }
        if self.max_concurrent_compaction_jobs != 1 {
            return Err(StorageError::unsupported(
                "local maintenance supports exactly one compaction executor",
            ));
        }
        Ok(())
    }
}

/// Deterministic admission decision for a write.
///
/// `Throttle` and `Reject` are both surfaced as `StorageError::Unavailable` by
/// the local durable provider. Adapters above this layer may retry, sleep, or
/// fail their own request, but the core never hides a wait in this decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAdmission {
    /// Run the write without maintenance pressure.
    Accept,
    /// Run the write and schedule or perform bounded maintenance according to mode.
    AcceptAndSchedule,
    /// Temporarily refuse the write with a stable reason.
    Throttle { reason: &'static str },
    /// Refuse the write until maintenance or capacity state changes.
    Reject { reason: &'static str },
}

impl WriteAdmission {
    fn unavailable_reason(self) -> Option<&'static str> {
        match self {
            Self::Throttle { reason } | Self::Reject { reason } => Some(reason),
            Self::Accept | Self::AcceptAndSchedule => None,
        }
    }
}

/// Point-in-time scheduler input derived from durable provider state.
///
/// Observations are snapshots for planning only. They do not lock data logs or
/// reserve future writes; stale observations must lead to idempotent maintenance
/// commands or skipped work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceObservation {
    /// Per-storage-node log pressure.
    pub nodes: Vec<MaintenanceNodeObservation>,
    /// Current SQLite WAL bytes, or zero when unavailable.
    pub sqlite_wal_bytes: u64,
    /// Count of queued release records not yet reflected in log debt.
    pub pending_custodian_releases: usize,
    /// Oldest commit still protected by PITR retention, if known.
    pub pitr_retention_floor: Option<CommitSeq>,
    /// Bytes in the write being admitted, if any.
    pub recent_write_bytes: u64,
    /// Flushed bytes in the write being admitted, if any.
    pub recent_flushed_write_bytes: u64,
    /// Last persisted fairness cursor for log selection.
    pub compaction_cursor: Option<DurableDataLogRef>,
}

/// Maintenance pressure for one storage node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceNodeObservation {
    /// Node this observation describes.
    pub storage_node: StorageNodeId,
    /// Bytes in active logs that are not compaction candidates.
    pub active_log_bytes: u64,
    /// Count of sealed logs on this node.
    pub sealed_log_count: usize,
    /// Bytes that make sealed logs dirty.
    pub dirty_bytes: u64,
    /// Bytes currently eligible for reclamation.
    pub reclaimable_bytes: u64,
    /// Sealed log details available for bounded scheduling.
    pub logs: Vec<MaintenanceDataLogObservation>,
}

/// Scheduler-visible state for one sealed data log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceDataLogObservation {
    /// Node-local log identity.
    pub log_ref: DurableDataLogRef,
    /// Total durable bytes in the log.
    pub total_bytes: u64,
    /// Bytes that must be copied before the log can be deleted.
    pub live_bytes: u64,
    /// Bytes no longer referenced by published metadata.
    pub dead_bytes: u64,
    /// Dead bytes past retention and eligible for reclamation.
    pub reclaimable_bytes: u64,
}

/// Bounded maintenance command emitted by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceCommand {
    /// Compact these logs if they are still sealed and eligible.
    CompactDataLogs { logs: Vec<DurableDataLogRef> },
}

/// Deterministic output of one scheduler step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickPlan {
    /// Write admission decision for the associated observation.
    pub admission: WriteAdmission,
    /// Bounded commands to run, if any.
    pub commands: Vec<MaintenanceCommand>,
    /// Human-readable counters and skip reasons for tests and operators.
    pub diagnostics: MaintenanceDiagnostics,
    /// Cursor to persist after the tick finishes or is skipped.
    pub next_cursor: Option<DurableDataLogRef>,
}

/// Scheduler counters and explanations.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MaintenanceDiagnostics {
    /// Total dirty sealed-log bytes observed.
    pub dirty_bytes: u64,
    /// Total reclaimable sealed-log bytes observed.
    pub reclaimable_bytes: u64,
    /// Total sealed logs observed.
    pub sealed_log_count: usize,
    /// SQLite WAL bytes observed.
    pub sqlite_wal_bytes: u64,
    /// Logs selected for compaction.
    pub selected_logs: Vec<DurableDataLogRef>,
    /// Logs considered but not selected.
    pub skipped_logs: Vec<MaintenanceSkippedLog>,
    /// Stable throttle/reject reason, when admission refused the write.
    pub throttle_reason: Option<&'static str>,
}

/// Explanation for a log skipped by the scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceSkippedLog {
    /// Skipped log.
    pub log_ref: DurableDataLogRef,
    /// Stable diagnostic reason.
    pub reason: &'static str,
}

/// Result of running one bounded maintenance tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTickReport {
    /// The plan that was executed.
    pub plan: MaintenanceTickPlan,
    /// Durable compaction work completed by this tick.
    pub compaction: DurableCompactionReport,
}

/// Pure deterministic scheduler for durable maintenance.
///
/// The scheduler performs no I/O and owns no background state. Identical policy
/// plus identical observation must produce identical plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceScheduler {
    policy: MaintenancePolicy,
}

impl MaintenanceScheduler {
    /// Create a scheduler after validating the policy.
    pub fn new(policy: MaintenancePolicy) -> Result<Self> {
        policy.validate()?;
        Ok(Self { policy })
    }

    /// Return the validated policy used by this scheduler.
    pub fn policy(&self) -> MaintenancePolicy {
        self.policy
    }

    /// Plan one deterministic maintenance/admission step.
    ///
    /// Success never mutates provider state. The returned commands are
    /// provider-private and must be revalidated by the executor because the
    /// observation may already be stale.
    pub fn step(&self, observation: &MaintenanceObservation) -> MaintenanceTickPlan {
        let mut diagnostics = MaintenanceDiagnostics {
            sqlite_wal_bytes: observation.sqlite_wal_bytes,
            ..MaintenanceDiagnostics::default()
        };
        let mut candidate_logs = Vec::new();
        for node in &observation.nodes {
            diagnostics.dirty_bytes = diagnostics.dirty_bytes.saturating_add(node.dirty_bytes);
            diagnostics.reclaimable_bytes = diagnostics
                .reclaimable_bytes
                .saturating_add(node.reclaimable_bytes);
            diagnostics.sealed_log_count = diagnostics
                .sealed_log_count
                .saturating_add(node.sealed_log_count);
            for log in &node.logs {
                if !self.log_is_compaction_candidate(log) {
                    diagnostics.skipped_logs.push(MaintenanceSkippedLog {
                        log_ref: log.log_ref,
                        reason: "below_reclaim_threshold",
                    });
                    continue;
                }
                candidate_logs.push(*log);
            }
        }
        candidate_logs.sort_by_key(|log| log.log_ref);
        if let Some(cursor) = observation.compaction_cursor
            && let Some(index) = candidate_logs.iter().position(|log| log.log_ref > cursor)
        {
            candidate_logs.rotate_left(index);
        }

        let admission = self.admission(&diagnostics);
        diagnostics.throttle_reason = admission.unavailable_reason();

        let mut copy_budget = self.policy.compaction_copy_budget_per_tick;
        let mut selected = Vec::new();
        for log in candidate_logs
            .into_iter()
            .take(self.policy.max_logs_scanned_per_tick)
        {
            if log.live_bytes > copy_budget {
                diagnostics.skipped_logs.push(MaintenanceSkippedLog {
                    log_ref: log.log_ref,
                    reason: "copy_budget_exhausted",
                });
                continue;
            }
            selected.push(log.log_ref);
            copy_budget = copy_budget.saturating_sub(log.live_bytes);
        }
        diagnostics.selected_logs = selected.clone();
        let next_cursor = selected.last().copied().or(observation.compaction_cursor);
        let commands = if selected.is_empty() {
            Vec::new()
        } else {
            vec![MaintenanceCommand::CompactDataLogs { logs: selected }]
        };

        MaintenanceTickPlan {
            admission: if matches!(admission, WriteAdmission::Accept) && !commands.is_empty() {
                WriteAdmission::AcceptAndSchedule
            } else {
                admission
            },
            commands,
            diagnostics,
            next_cursor,
        }
    }

    fn admission(&self, diagnostics: &MaintenanceDiagnostics) -> WriteAdmission {
        if diagnostics.reclaimable_bytes > self.policy.max_reclaimable_debt_bytes {
            return WriteAdmission::Reject {
                reason: "maintenance reclaimable debt exceeds hard limit",
            };
        }
        if diagnostics.dirty_bytes >= self.policy.dirty_high_watermark_bytes {
            return WriteAdmission::Throttle {
                reason: "maintenance dirty bytes above high watermark",
            };
        }
        if diagnostics.sealed_log_count > self.policy.max_sealed_logs {
            return WriteAdmission::Throttle {
                reason: "maintenance sealed log count above limit",
            };
        }
        if diagnostics.sqlite_wal_bytes > self.policy.max_sqlite_wal_bytes {
            return WriteAdmission::Throttle {
                reason: "maintenance SQLite WAL above limit",
            };
        }
        if diagnostics.dirty_bytes >= self.policy.dirty_low_watermark_bytes {
            return WriteAdmission::AcceptAndSchedule;
        }
        WriteAdmission::Accept
    }

    fn log_is_compaction_candidate(&self, log: &MaintenanceDataLogObservation) -> bool {
        if log.total_bytes == 0 {
            return false;
        }
        if log.live_bytes == 0 && log.dead_bytes != 0 {
            return true;
        }
        let reclaimable_ratio = log
            .dead_bytes
            .saturating_mul(1_000_000)
            .checked_div(log.total_bytes)
            .unwrap_or(0);
        log.dead_bytes >= self.policy.data_log_policy.min_reclaimable_bytes
            && reclaimable_ratio >= u64::from(self.policy.data_log_policy.min_reclaimable_ratio_ppm)
    }
}

#[derive(Debug, Clone)]
struct DurableSqliteStore {
    paths: DurableStorePaths,
    conn: Arc<Mutex<Connection>>,
    node_catalogs: Arc<NodeCatalogs>,
    policy: DurableDataLogPolicy,
}

#[derive(Debug, Clone)]
struct SegmentPlacementRow {
    segment_id: SegmentId,
    storage_node: StorageNodeId,
    data_log_id: u64,
    record_offset: u64,
    record_bytes: u64,
    payload_offset: u64,
    payload_bytes: u64,
    checksum: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DataLogRow {
    storage_node: StorageNodeId,
    log_id: u64,
    total_bytes: u64,
    live_bytes: u64,
    dead_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableExportCursor {
    config: LocalStoreConfig,
    next_device_id: u128,
    next_keyspace_id: u128,
    next_file_id: u128,
    next_metadata_node_id: u128,
    next_keyspace_root_id: u128,
    next_keyspace_catalog_shard_id: u128,
    next_commit_group_id: u128,
    next_commit_seq: u64,
    next_checkpoint_id: u128,
    next_gc_epoch: u64,
    next_write_intent: u128,
    next_extent_id: u128,
    next_segment_id: u128,
    next_placement_index: u64,
}

impl DurableExportCursor {
    fn from_image(image: &DurableStoreImage) -> Self {
        Self {
            config: image.config,
            next_device_id: image.metadata.next_device_id,
            next_keyspace_id: image.metadata.next_keyspace_id,
            next_file_id: image.metadata.next_file_id,
            next_metadata_node_id: image.metadata.next_metadata_node_id,
            next_keyspace_root_id: image.metadata.next_keyspace_root_id,
            next_keyspace_catalog_shard_id: image.metadata.next_keyspace_catalog_shard_id,
            next_commit_group_id: image.metadata.next_commit_group_id,
            next_commit_seq: image.metadata.next_commit_seq,
            next_checkpoint_id: image.metadata.next_checkpoint_id,
            next_gc_epoch: image.metadata.next_gc_epoch,
            next_write_intent: image.next_write_intent,
            next_extent_id: image.next_extent_id,
            next_segment_id: image.storage_nodes.next_segment_id,
            next_placement_index: image.storage_nodes.next_placement_index,
        }
    }
}

const DATA_LOG_MAGIC: &[u8; 8] = b"TCOWDAT!";
const DATA_LOG_VERSION: u16 = 1;
const DATA_LOG_HEADER_LEN: usize = 8 + 2 + 16 + 8 + 8;
const CRC64_ECMA_POLY: u64 = 0x42f0_e1eb_a9ea_3693;
const CRC64_ECMA_TABLE: [u64; 256] = crc64_ecma_table();

impl DurableSqliteStore {
    fn open(
        paths: DurableStorePaths,
        policy: DurableDataLogPolicy,
        configured_storage_nodes: Vec<StorageNodeId>,
    ) -> Result<Self> {
        policy.validate()?;
        let metadata_existed = paths.metadata.exists();
        let conn = Connection::open(&paths.metadata).map_err(sqlite_error)?;
        configure_sqlite_connection(&conn)?;
        Self::initialize_schema(&conn)?;
        reject_root_storage_catalog_tables_if_present(&conn)?;
        let node_catalogs = NodeCatalogs::open(&paths, configured_storage_nodes)?;
        if !metadata_existed {
            sync_parent_dir(&paths.metadata)?;
        }
        Ok(Self {
            paths,
            conn: Arc::new(Mutex::new(conn)),
            node_catalogs: Arc::new(node_catalogs),
            policy,
        })
    }

    fn initialize_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS store_meta (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              config BLOB NOT NULL,
              next_device_id TEXT NOT NULL,
              next_keyspace_id TEXT NOT NULL,
              next_file_id TEXT NOT NULL,
              next_metadata_node_id TEXT NOT NULL,
              next_keyspace_root_id TEXT NOT NULL,
              next_keyspace_catalog_shard_id TEXT NOT NULL,
              next_commit_group_id TEXT NOT NULL,
              next_commit_seq INTEGER NOT NULL CHECK (next_commit_seq >= 0),
              next_checkpoint_id TEXT NOT NULL,
              next_gc_epoch INTEGER NOT NULL CHECK (next_gc_epoch >= 0),
              next_write_intent TEXT NOT NULL,
              next_extent_id TEXT NOT NULL,
              next_segment_id TEXT NOT NULL,
              next_placement_index INTEGER NOT NULL CHECK (next_placement_index >= 0)
            );
            CREATE TABLE IF NOT EXISTS maintenance_state (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              cursor_storage_node TEXT,
              cursor_log_id INTEGER CHECK (cursor_log_id IS NULL OR cursor_log_id >= 0),
              CHECK (
                (cursor_storage_node IS NULL AND cursor_log_id IS NULL) OR
                (cursor_storage_node IS NOT NULL AND cursor_log_id IS NOT NULL)
              )
            );
            CREATE TABLE IF NOT EXISTS append_session_runtime (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              next_incarnation INTEGER NOT NULL CHECK (next_incarnation > 0)
            );
            CREATE TABLE IF NOT EXISTS device_specs (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS device_heads (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deleted_device_heads (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS keyspace_heads (
              keyspace_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS keyspace_roots (
              root_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS keyspace_catalog_shards (
              shard_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS file_writer_epochs (
              file_key TEXT PRIMARY KEY,
              keyspace_id TEXT NOT NULL,
              file_id TEXT NOT NULL,
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_file_writer_epochs_file
              ON file_writer_epochs(keyspace_id, file_id);
            CREATE TABLE IF NOT EXISTS metadata_nodes (
              node_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS commit_groups (
              commit_group_id TEXT PRIMARY KEY,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_commit_groups_seq
              ON commit_groups(commit_seq, commit_group_id);
            CREATE TABLE IF NOT EXISTS shard_commits (
              row_key TEXT PRIMARY KEY,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_shard_commits_order
              ON shard_commits(commit_seq, ordinal);
            CREATE TABLE IF NOT EXISTS keyspace_commits (
              row_key TEXT PRIMARY KEY,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_keyspace_commits_order
              ON keyspace_commits(commit_seq, ordinal);
            CREATE TABLE IF NOT EXISTS file_commits (
              row_key TEXT PRIMARY KEY,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_file_commits_order
              ON file_commits(commit_seq, ordinal);
            CREATE TABLE IF NOT EXISTS fork_records (
              commit_seq INTEGER PRIMARY KEY CHECK (commit_seq >= 0),
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS delete_records (
              commit_seq INTEGER PRIMARY KEY CHECK (commit_seq >= 0),
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
              checkpoint_id TEXT PRIMARY KEY,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_checkpoints_seq
              ON checkpoints(commit_seq, checkpoint_id);
            CREATE TABLE IF NOT EXISTS metadata_gc_marks (
              node_id TEXT PRIMARY KEY,
              epoch INTEGER NOT NULL CHECK (epoch >= 0)
            );
            CREATE TABLE IF NOT EXISTS segment_gc_marks (
              segment_id TEXT PRIMARY KEY,
              epoch INTEGER NOT NULL CHECK (epoch >= 0)
            );
            ",
        )
        .map_err(sqlite_error)
    }

    fn load(&self, expected_config: LocalStoreConfig) -> Result<Option<LocalCoordinator>> {
        let conn = lock(&self.conn)?;
        let Some(cursor) = load_export_cursor(&conn)? else {
            reject_legacy_current_state_if_present(&conn)?;
            reject_orphan_row_native_rows_if_present(&conn)?;
            let (mut storage_nodes, next_write_intent) =
                self.load_storage_registry_from_node_catalogs(1, 0, 1)?;
            if storage_nodes.node_order.is_empty() {
                return Ok(None);
            }
            let repairs = reconcile_catalog_references_from_metadata(
                &MetadataInner::new(),
                &mut storage_nodes,
            );
            let image = DurableStoreImage {
                config: expected_config,
                metadata: MetadataInner::new(),
                storage_nodes,
                next_write_intent,
                next_extent_id: 1,
            };
            validate_row_native_image(&image)?;
            self.persist_catalog_reference_repairs(&image.storage_nodes, &repairs)?;
            return Ok(Some(LocalCoordinator::from_state_image(image)?));
        };
        if !cursor.config.storage_shape_matches(expected_config) {
            return Err(StorageError::corrupt(
                "durable SQLite state disagrees with open config",
            ));
        }
        let runtime_config = cursor
            .config
            .with_observability_event_capacity(expected_config.observability_event_capacity);

        let mut metadata = load_metadata_inner(&conn, &cursor)?;
        let (mut storage_nodes, next_write_intent) = self
            .load_storage_registry_from_node_catalogs(
                cursor.next_segment_id,
                cursor.next_placement_index,
                cursor.next_write_intent,
            )?;
        if storage_nodes.node_order.is_empty() {
            return Err(StorageError::corrupt(
                "durable SQLite store has no storage nodes",
            ));
        }

        metadata.next_device_id = cursor.next_device_id;
        metadata.next_keyspace_id = cursor.next_keyspace_id;
        metadata.next_file_id = cursor.next_file_id;
        metadata.next_metadata_node_id = cursor.next_metadata_node_id;
        metadata.next_keyspace_root_id = cursor.next_keyspace_root_id;
        metadata.next_keyspace_catalog_shard_id = cursor.next_keyspace_catalog_shard_id;
        metadata.next_commit_group_id = cursor.next_commit_group_id;
        metadata.next_commit_seq = cursor.next_commit_seq;
        metadata.next_checkpoint_id = cursor.next_checkpoint_id;
        metadata.next_gc_epoch = cursor.next_gc_epoch;
        let repairs = reconcile_catalog_references_from_metadata(&metadata, &mut storage_nodes);

        let image = DurableStoreImage {
            config: runtime_config,
            metadata,
            storage_nodes,
            next_write_intent,
            next_extent_id: cursor.next_extent_id,
        };
        validate_row_native_image(&image)?;
        self.persist_catalog_reference_repairs(&image.storage_nodes, &repairs)?;
        Ok(Some(LocalCoordinator::from_state_image(image)?))
    }

    fn persist_catalog_reference_repairs(
        &self,
        storage_nodes: &StorageNodeRegistryInner,
        repairs: &BTreeMap<StorageNodeId, BTreeSet<SegmentId>>,
    ) -> Result<()> {
        for (storage_node, segment_ids) in repairs {
            let Some(node) = storage_nodes.nodes.get(storage_node) else {
                return Err(StorageError::corrupt(
                    "catalog reference repair targets missing storage node",
                ));
            };
            let mut conn = self.node_catalogs.lock(*storage_node)?;
            let tx = conn.transaction().map_err(sqlite_error)?;
            sync_node_segment_catalog_entries_for_ids(
                &tx,
                *storage_node,
                node,
                segment_ids,
                &BTreeSet::new(),
            )?;
            tx.commit().map_err(sqlite_error)?;
        }
        Ok(())
    }

    fn load_storage_registry_from_node_catalogs(
        &self,
        cursor_next_segment_id: u128,
        cursor_next_placement_index: u64,
        cursor_next_write_intent: u128,
    ) -> Result<(StorageNodeRegistryInner, u128)> {
        let storage_registry_rows = load_storage_node_rows(&self.node_catalogs)?;
        let node_order: Vec<_> = storage_registry_rows
            .iter()
            .map(|row| row.storage_node)
            .collect();
        let mut nodes = BTreeMap::new();
        let mut next_segment_id = cursor_next_segment_id;
        let mut next_write_intent = cursor_next_write_intent;
        let mut catalog_entry_count = 0_u64;
        for row in storage_registry_rows {
            let node_conn = self.node_catalogs.lock(row.storage_node)?;
            next_segment_id = next_segment_id.max(row.next_catalog_segment_id);
            let catalog =
                load_catalog_inner(&node_conn, row.storage_node, row.next_catalog_segment_id)?;
            let mut records = BTreeMap::new();
            for (segment_id, entry) in &catalog.entries {
                catalog_entry_count = catalog_entry_count.checked_add(1).ok_or_else(|| {
                    StorageError::corrupt("segment catalog entry count overflows u64")
                })?;
                next_segment_id = next_segment_id.max(segment_id.raw().saturating_add(1));
                next_write_intent =
                    next_write_intent.max(entry.intent.write_intent.raw().saturating_add(1));
                let Some(receipt) = &entry.receipt else {
                    continue;
                };
                if matches!(entry.state, SegmentLifecycleState::Freed) {
                    continue;
                }
                let commit = receipt.replica_commit();
                let record = DurableSegmentRecord {
                    synced: true,
                    commit,
                };
                let placement =
                    Self::placement_for_segment_on_node(&node_conn, row.storage_node, *segment_id)?;
                validate_durable_segment_placement(*segment_id, &record, &placement)?;
                let bytes = self.read_segment_payload(&placement)?;
                validate_durable_segment_bytes(*segment_id, &record, &bytes)?;
                records.insert(
                    *segment_id,
                    SegmentRecord {
                        bytes,
                        synced: record.synced,
                        commit: record.commit,
                    },
                );
            }

            nodes.insert(
                row.storage_node,
                StorageNodeInner {
                    segment_store: SegmentStoreInner {
                        next_offset: row.segment_store_next_offset,
                        segments: records,
                    },
                    segment_catalog: catalog,
                },
            );
        }
        Ok((
            StorageNodeRegistryInner {
                next_segment_id,
                next_placement_index: cursor_next_placement_index.max(catalog_entry_count),
                node_order,
                nodes,
            },
            next_write_intent,
        ))
    }

    fn persist(
        &self,
        image: &DurableStoreImage,
        previous_segments: &BTreeSet<SegmentId>,
        image_segments: &BTreeSet<SegmentId>,
        new_segments: Vec<DurableSegmentPayload>,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<BTreeSet<SegmentId>> {
        let appended = self.append_segments(new_segments)?;
        self.persist_node_catalog_publish(
            image,
            previous_segments,
            image_segments,
            appended,
            changed_catalog_segments,
        )?;

        let mut conn = lock(&self.conn)?;
        let previous_cursor = load_export_cursor(&conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        persist_row_native_state(&tx, previous_cursor.as_ref(), image)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(image_segments.clone())
    }

    fn persist_node_catalog_publish(
        &self,
        image: &DurableStoreImage,
        previous_segments: &BTreeSet<SegmentId>,
        image_segments: &BTreeSet<SegmentId>,
        appended: PendingDataLogAppend,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<()> {
        let removed_segment_ids: Vec<_> = previous_segments
            .difference(image_segments)
            .copied()
            .collect();
        let incremental_catalog_sync = changed_catalog_segments.is_some()
            || (!appended.placements.is_empty() && removed_segment_ids.is_empty());
        let mut changed_segments_by_node: BTreeMap<StorageNodeId, BTreeSet<SegmentId>> =
            BTreeMap::new();
        if incremental_catalog_sync {
            for placement in &appended.placements {
                changed_segments_by_node
                    .entry(placement.storage_node)
                    .or_default()
                    .insert(placement.segment_id);
            }
        }

        let mut dead_placements: BTreeMap<StorageNodeId, Vec<SegmentPlacementRow>> =
            BTreeMap::new();
        for segment_id in removed_segment_ids {
            let placement = self.placement_for_segment(segment_id)?;
            if incremental_catalog_sync {
                changed_segments_by_node
                    .entry(placement.storage_node)
                    .or_default()
                    .insert(segment_id);
            }
            dead_placements
                .entry(placement.storage_node)
                .or_default()
                .push(placement);
        }
        if let Some(segment_ids) = changed_catalog_segments {
            for segment_id in segment_ids {
                let storage_node = image_storage_node_for_catalog_segment(image, *segment_id)
                    .map(Ok)
                    .unwrap_or_else(|| {
                        self.placement_for_segment(*segment_id)
                            .map(|placement| placement.storage_node)
                    })?;
                changed_segments_by_node
                    .entry(storage_node)
                    .or_default()
                    .insert(*segment_id);
            }
        }
        let pre_root_pending_segments: BTreeSet<_> = appended
            .placements
            .iter()
            .map(|placement| placement.segment_id)
            .collect();

        for (ordinal, node_id) in image.storage_nodes.node_order.iter().enumerate() {
            let node = image.storage_nodes.nodes.get(node_id).ok_or_else(|| {
                StorageError::corrupt("storage node order references missing node")
            })?;
            let mut conn = self.node_catalogs.lock(*node_id)?;
            let tx = conn.transaction().map_err(sqlite_error)?;
            for log in appended
                .logs
                .values()
                .filter(|log| log.storage_node == *node_id)
            {
                persist_data_log_manifest(&tx, log)?;
            }
            for log_ref in appended
                .sealed_logs
                .iter()
                .filter(|log_ref| log_ref.storage_node == *node_id)
            {
                seal_data_log_manifest(&tx, *log_ref)?;
            }
            if let Some(placements) = dead_placements.get(node_id) {
                for placement in placements {
                    mark_placement_dead(&tx, placement)?;
                }
            }
            for placement in appended
                .placements
                .iter()
                .filter(|placement| placement.storage_node == *node_id)
            {
                persist_segment_placement(&tx, placement)?;
            }
            let catalog_sync = if incremental_catalog_sync {
                changed_segments_by_node
                    .get(node_id)
                    .map(SegmentCatalogSync::Only)
                    .unwrap_or(SegmentCatalogSync::Skip)
            } else {
                SegmentCatalogSync::Full
            };
            sync_node_catalog_state_for_node(
                &tx,
                ordinal,
                *node_id,
                node,
                catalog_sync,
                &pre_root_pending_segments,
            )?;
            tx.commit().map_err(sqlite_error)?;
        }
        Ok(())
    }

    fn append_segments(
        &self,
        segments: Vec<DurableSegmentPayload>,
    ) -> Result<PendingDataLogAppend> {
        let mut append = PendingDataLogAppend::default();
        if segments.is_empty() {
            return Ok(append);
        }

        let mut active_logs = BTreeMap::new();
        let mut open_log: Option<(DurableDataLogRef, File)> = None;
        let mut synced_dirs = BTreeSet::new();
        for segment in segments {
            let segment_id = segment.segment_id;
            let storage_node = segment.storage_node;
            let bytes = segment.bytes;
            let data_dir = node_data_log_dir(&self.paths.data_dir, storage_node);
            if let std::collections::btree_map::Entry::Vacant(entry) =
                active_logs.entry(storage_node)
            {
                let node_conn = self.node_catalogs.lock(storage_node)?;
                entry.insert(active_data_log(
                    &node_conn,
                    &self.paths.data_dir,
                    storage_node,
                )?);
            }
            let active = active_logs
                .get_mut(&storage_node)
                .ok_or_else(|| StorageError::corrupt("active data-log row missing"))?;
            let record = encode_data_log_record(segment_id, &bytes)?;
            let record_len = u64::try_from(record.len()).map_err(|_| {
                StorageError::invalid_argument("data-log record length overflows u64")
            })?;
            if active.total_bytes != 0
                && active
                    .total_bytes
                    .checked_add(record_len)
                    .ok_or_else(|| StorageError::conflict("data-log size overflow"))?
                    > self.policy.target_data_log_bytes
            {
                append.sealed_logs.push(DurableDataLogRef {
                    storage_node,
                    log_id: active.log_id,
                });
                let node_conn = self.node_catalogs.lock(storage_node)?;
                *active = next_data_log(
                    &node_conn,
                    &self.paths.data_dir,
                    storage_node,
                    active.log_id,
                )?;
            }

            let log_ref = DurableDataLogRef {
                storage_node,
                log_id: active.log_id,
            };
            if open_log.as_ref().map(|(log_ref, _)| *log_ref) != Some(log_ref) {
                sync_open_data_log(&mut open_log)?;
                let data_dir_existed = data_dir.exists();
                fs::create_dir_all(&data_dir).map_err(fs_error)?;
                if !data_dir_existed {
                    sync_dir(&self.paths.data_dir)?;
                }
                let path = data_log_path(&self.paths.data_dir, storage_node, active.log_id);
                let existed = path.exists();
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .read(true)
                    .open(&path)
                    .map_err(fs_error)?;
                let file_len = file.metadata().map_err(fs_error)?.len();
                active.total_bytes = active.total_bytes.max(file_len);
                if !existed {
                    synced_dirs.insert(storage_node);
                }
                open_log = Some((log_ref, file));
            }

            let offset = active.total_bytes;
            let Some((_, file)) = open_log.as_mut() else {
                return Err(StorageError::conflict("data-log writer was not opened"));
            };
            file.write_all(&record).map_err(fs_error)?;
            let payload_offset = offset
                .checked_add(DATA_LOG_HEADER_LEN as u64)
                .ok_or_else(|| StorageError::conflict("data-log payload offset overflow"))?;
            let payload_bytes = u64::try_from(bytes.len())
                .map_err(|_| StorageError::invalid_argument("payload length overflows u64"))?;
            let new_total = offset
                .checked_add(record_len)
                .ok_or_else(|| StorageError::conflict("data-log size overflow"))?;
            active.total_bytes = new_total;
            append.logs.insert(
                log_ref,
                PendingDataLogManifest {
                    storage_node,
                    log_id: active.log_id,
                    state: "active",
                    total_bytes: new_total,
                },
            );
            append.placements.push(SegmentPlacementRow {
                segment_id,
                storage_node,
                data_log_id: active.log_id,
                record_offset: offset,
                record_bytes: record_len,
                payload_offset,
                payload_bytes,
                checksum: data_log_checksum64(&bytes),
            });
        }
        sync_open_data_log(&mut open_log)?;
        for storage_node in synced_dirs {
            let data_dir = node_data_log_dir(&self.paths.data_dir, storage_node);
            sync_dir(&data_dir)?;
        }
        Ok(append)
    }

    fn read_segment_payload(&self, placement: &SegmentPlacementRow) -> Result<Vec<u8>> {
        let path = data_log_path(
            &self.paths.data_dir,
            placement.storage_node,
            placement.data_log_id,
        );
        let mut file = File::open(&path).map_err(fs_error)?;
        file.seek(SeekFrom::Start(placement.record_offset))
            .map_err(fs_error)?;
        let record_len = usize::try_from(placement.record_bytes)
            .map_err(|_| StorageError::corrupt("data-log record length overflows usize"))?;
        let mut record = vec![0; record_len];
        file.read_exact(&mut record).map_err(fs_error)?;
        let data = decode_data_log_record(&record)?;
        if data.segment_id != placement.segment_id
            || data.bytes.len() as u64 != placement.payload_bytes
            || data_log_checksum64(&data.bytes) != placement.checksum
        {
            return Err(StorageError::corrupt(
                "data-log record disagrees with SQLite placement",
            ));
        }
        Ok(data.bytes)
    }

    fn placement_for_segment(&self, segment_id: SegmentId) -> Result<SegmentPlacementRow> {
        for storage_node in self.node_catalogs.storage_nodes() {
            let node_conn = self.node_catalogs.lock(storage_node)?;
            match Self::placement_for_segment_on_node(&node_conn, storage_node, segment_id) {
                Ok(placement) => return Ok(placement),
                Err(StorageError::Corrupt { reason })
                    if reason == "committed segment missing SQLite placement" => {}
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::corrupt(
            "committed segment missing SQLite placement",
        ))
    }

    fn placement_for_segment_on_node(
        conn: &Connection,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
    ) -> Result<SegmentPlacementRow> {
        let segment_placements = node_catalog_table(storage_node, "segment_placements")?;
        conn.query_row(
            &format!(
                "SELECT segment_id, data_log_id, record_offset, record_bytes,
                    payload_offset, payload_bytes, checksum
                 FROM {segment_placements}
                 WHERE segment_id = ?1 AND current = 1"
            ),
            params![segment_id_key(segment_id)],
            |row| decode_node_placement_row(row, storage_node),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| StorageError::corrupt("committed segment missing SQLite placement"))
    }

    pub fn compact_data_logs(
        &self,
        policy: DurableDataLogPolicy,
    ) -> Result<DurableCompactionReport> {
        policy.validate()?;
        let candidates = compaction_candidates(&self.node_catalogs, policy)?;
        self.compact_data_log_rows(policy, candidates)
    }

    fn compact_data_log_refs(
        &self,
        policy: DurableDataLogPolicy,
        logs: &[DurableDataLogRef],
    ) -> Result<DurableCompactionReport> {
        policy.validate()?;
        let candidates = compaction_candidates_for_refs(&self.node_catalogs, policy, logs)?;
        self.compact_data_log_rows(policy, candidates)
    }

    fn maintenance_observation(
        &self,
        compaction_cursor: Option<DurableDataLogRef>,
        recent_write_bytes: u64,
        recent_flushed_write_bytes: u64,
        include_sqlite_wal_bytes: bool,
    ) -> Result<MaintenanceObservation> {
        let mut node_logs: BTreeMap<StorageNodeId, Vec<(DataLogRow, String)>> = BTreeMap::new();
        for storage_node in self.node_catalogs.storage_nodes() {
            node_logs.entry(storage_node).or_default();
        }

        for storage_node in self.node_catalogs.storage_nodes() {
            let node_conn = self.node_catalogs.lock(storage_node)?;
            let data_logs = node_catalog_table(storage_node, "data_logs")?;
            let mut stmt = node_conn
                .prepare(&format!(
                    "SELECT log_id, state, total_bytes, live_bytes, dead_bytes
                     FROM {data_logs}
                     WHERE state != 'deleted'
                     ORDER BY log_id"
                ))
                .map_err(sqlite_error)?;
            let mut rows = stmt.query([]).map_err(sqlite_error)?;
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                let state: String = row.get(1).map_err(sqlite_error)?;
                node_logs.entry(storage_node).or_default().push((
                    DataLogRow {
                        storage_node,
                        log_id: i64_to_u64(row.get(0).map_err(sqlite_error)?)
                            .map_err(sqlite_error)?,
                        total_bytes: i64_to_u64(row.get(2).map_err(sqlite_error)?)
                            .map_err(sqlite_error)?,
                        live_bytes: i64_to_u64(row.get(3).map_err(sqlite_error)?)
                            .map_err(sqlite_error)?,
                        dead_bytes: i64_to_u64(row.get(4).map_err(sqlite_error)?)
                            .map_err(sqlite_error)?,
                    },
                    state,
                ));
            }
        }

        let mut nodes = Vec::new();
        for (storage_node, logs) in node_logs {
            let mut active_log_bytes = 0u64;
            let mut sealed_log_count = 0usize;
            let mut dirty_bytes = 0u64;
            let mut reclaimable_bytes = 0u64;
            let mut observed_logs = Vec::new();
            for (row, state) in logs {
                if state == "active" {
                    active_log_bytes = active_log_bytes.saturating_add(row.total_bytes);
                    continue;
                }
                if state != "sealed" {
                    continue;
                }
                sealed_log_count = sealed_log_count.saturating_add(1);
                dirty_bytes = dirty_bytes.saturating_add(row.dead_bytes);
                reclaimable_bytes = reclaimable_bytes.saturating_add(row.dead_bytes);
                observed_logs.push(MaintenanceDataLogObservation {
                    log_ref: DurableDataLogRef {
                        storage_node,
                        log_id: row.log_id,
                    },
                    total_bytes: row.total_bytes,
                    live_bytes: row.live_bytes,
                    dead_bytes: row.dead_bytes,
                    reclaimable_bytes: row.dead_bytes,
                });
            }
            nodes.push(MaintenanceNodeObservation {
                storage_node,
                active_log_bytes,
                sealed_log_count,
                dirty_bytes,
                reclaimable_bytes,
                logs: observed_logs,
            });
        }

        Ok(MaintenanceObservation {
            nodes,
            sqlite_wal_bytes: if include_sqlite_wal_bytes {
                self.sqlite_wal_bytes()?
            } else {
                0
            },
            pending_custodian_releases: 0,
            pitr_retention_floor: None,
            recent_write_bytes,
            recent_flushed_write_bytes,
            compaction_cursor,
        })
    }

    fn load_maintenance_cursor(&self) -> Result<Option<DurableDataLogRef>> {
        let conn = lock(&self.conn)?;
        load_maintenance_cursor(&conn)
    }

    fn reserve_append_session_incarnation(&self) -> Result<u64> {
        let mut conn = lock(&self.conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let current = tx
            .query_row(
                "SELECT next_incarnation
                 FROM append_session_runtime
                 WHERE id = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sqlite_error)?
            .map(i64_to_u64)
            .transpose()
            .map_err(sqlite_error)?
            .unwrap_or(1);
        let next = current
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("append session incarnation overflow"))?;
        tx.execute(
            "INSERT INTO append_session_runtime(id, next_incarnation)
             VALUES (1, ?1)
             ON CONFLICT(id) DO UPDATE SET
               next_incarnation = excluded.next_incarnation",
            params![u64_to_i64(next)?],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(current)
    }

    fn persist_maintenance_cursor(&self, cursor: Option<DurableDataLogRef>) -> Result<()> {
        let mut conn = lock(&self.conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        persist_maintenance_cursor(&tx, cursor)?;
        tx.commit().map_err(sqlite_error)
    }

    fn sqlite_wal_bytes(&self) -> Result<u64> {
        let mut bytes = sqlite_wal_bytes(&self.paths.metadata)?;
        for storage_node in self.node_catalogs.storage_nodes() {
            bytes = bytes
                .checked_add(sqlite_wal_bytes(&node_catalog_path(
                    &self.paths.data_dir,
                    storage_node,
                ))?)
                .ok_or_else(|| StorageError::conflict("SQLite WAL byte count overflow"))?;
        }
        Ok(bytes)
    }

    fn compact_data_log_rows(
        &self,
        policy: DurableDataLogPolicy,
        candidates: Vec<DataLogRow>,
    ) -> Result<DurableCompactionReport> {
        let mut report = DurableCompactionReport {
            deleted_logs: Vec::new(),
            relocated_logs: Vec::new(),
            relocated_segments: Vec::new(),
            bytes_copied: 0,
            bytes_deleted: 0,
        };
        for log in candidates {
            let log_ref = DurableDataLogRef {
                storage_node: log.storage_node,
                log_id: log.log_id,
            };
            if log.live_bytes == 0 {
                let mut node_conn = self.node_catalogs.lock(log.storage_node)?;
                let tx = node_conn.transaction().map_err(sqlite_error)?;
                let data_logs = node_catalog_table(log.storage_node, "data_logs")?;
                tx.execute(
                    &format!(
                        "UPDATE {data_logs} SET state = 'deleted'
                         WHERE log_id = ?1"
                    ),
                    params![u64_to_i64(log.log_id)?],
                )
                .map_err(sqlite_error)?;
                tx.commit().map_err(sqlite_error)?;
                delete_data_log(&self.paths.data_dir, log_ref)?;
                report.bytes_deleted = report
                    .bytes_deleted
                    .checked_add(log.total_bytes)
                    .ok_or_else(|| StorageError::conflict("compaction byte count overflow"))?;
                report.deleted_logs.push(log_ref);
                continue;
            }

            if report
                .bytes_copied
                .checked_add(log.live_bytes)
                .ok_or_else(|| StorageError::conflict("compaction byte count overflow"))?
                > policy.max_compaction_copy_bytes
            {
                continue;
            }

            let placements = {
                let node_conn = self.node_catalogs.lock(log.storage_node)?;
                current_placements_for_log(&node_conn, log_ref)?
            };
            let mut payloads = Vec::new();
            for placement in &placements {
                payloads.push(DurableSegmentPayload {
                    segment_id: placement.segment_id,
                    storage_node: placement.storage_node,
                    bytes: self.read_segment_payload(placement)?,
                });
            }
            let appended = self.append_segments(payloads)?;
            let mut node_conn = self.node_catalogs.lock(log.storage_node)?;
            let tx = node_conn.transaction().map_err(sqlite_error)?;
            for manifest in appended.logs.into_values() {
                persist_data_log_manifest(&tx, &manifest)?;
            }
            for sealed_ref in &appended.sealed_logs {
                seal_data_log_manifest(&tx, *sealed_ref)?;
            }
            for old in &placements {
                mark_placement_dead(&tx, old)?;
            }
            for placement in appended.placements {
                persist_segment_placement(&tx, &placement)?;
                report.relocated_segments.push(placement.segment_id);
                report.bytes_copied = report
                    .bytes_copied
                    .checked_add(placement.payload_bytes)
                    .ok_or_else(|| StorageError::conflict("compaction byte count overflow"))?;
            }
            let data_logs = node_catalog_table(log.storage_node, "data_logs")?;
            tx.execute(
                &format!(
                    "UPDATE {data_logs} SET state = 'deleted', live_bytes = 0,
                   dead_bytes = total_bytes
                 WHERE log_id = ?1"
                ),
                params![u64_to_i64(log.log_id)?],
            )
            .map_err(sqlite_error)?;
            tx.commit().map_err(sqlite_error)?;
            delete_data_log(&self.paths.data_dir, log_ref)?;
            report.bytes_deleted = report
                .bytes_deleted
                .checked_add(log.total_bytes)
                .ok_or_else(|| StorageError::conflict("compaction byte count overflow"))?;
            report.relocated_logs.push(log_ref);
        }
        Ok(report)
    }

    #[cfg(test)]
    fn data_log_rows_for_test(&self) -> Result<Vec<DataLogRow>> {
        data_log_rows(&self.node_catalogs)
    }

    #[cfg(test)]
    fn data_log_states_for_test(&self) -> Result<Vec<(DurableDataLogRef, String)>> {
        let mut out = Vec::new();
        for storage_node in self.node_catalogs.storage_nodes() {
            let node_conn = self.node_catalogs.lock(storage_node)?;
            let data_logs = node_catalog_table(storage_node, "data_logs")?;
            let mut stmt = node_conn
                .prepare(&format!(
                    "SELECT log_id, state
                     FROM {data_logs}
                     WHERE state != 'deleted'
                     ORDER BY log_id"
                ))
                .map_err(sqlite_error)?;
            let mut rows = stmt.query([]).map_err(sqlite_error)?;
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                let raw_log_id: i64 = row.get(0).map_err(sqlite_error)?;
                let log_id = i64_to_u64(raw_log_id).map_err(sqlite_error)?;
                let state: String = row.get(1).map_err(sqlite_error)?;
                out.push((
                    DurableDataLogRef {
                        storage_node,
                        log_id,
                    },
                    state,
                ));
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    fn placement_for_test(&self, segment_id: SegmentId) -> Result<SegmentPlacementRow> {
        self.placement_for_segment(segment_id)
    }
}

#[derive(Debug, Default)]
struct PendingDataLogAppend {
    placements: Vec<SegmentPlacementRow>,
    logs: BTreeMap<DurableDataLogRef, PendingDataLogManifest>,
    sealed_logs: Vec<DurableDataLogRef>,
}

#[derive(Debug)]
struct PendingDataLogManifest {
    storage_node: StorageNodeId,
    log_id: u64,
    state: &'static str,
    total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableStorageNodeRow {
    storage_node: StorageNodeId,
    ordinal: u64,
    next_catalog_segment_id: u128,
    segment_store_next_offset: u64,
}

#[derive(Debug, Clone)]
struct DataLogSegmentData {
    segment_id: SegmentId,
    bytes: Vec<u8>,
}

fn encode_row<T: DurableCodec>(value: &T) -> Result<Vec<u8>> {
    let mut out = DurableEncoder::default();
    value.encode(&mut out)?;
    Ok(out.finish())
}

fn decode_row<T: DurableCodec>(bytes: &[u8]) -> Result<T> {
    let mut input = DurableDecoder { bytes, offset: 0 };
    let value = T::decode(&mut input)?;
    input.finish()?;
    Ok(value)
}

fn load_export_cursor(conn: &Connection) -> Result<Option<DurableExportCursor>> {
    let row = conn
        .query_row(
            "SELECT config, next_device_id, next_keyspace_id, next_file_id,
                next_metadata_node_id, next_keyspace_root_id,
                next_keyspace_catalog_shard_id, next_commit_group_id,
                next_commit_seq, next_checkpoint_id, next_gc_epoch,
                next_write_intent, next_extent_id, next_segment_id,
                next_placement_index
         FROM store_meta
         WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, String>(13)?,
                    row.get::<_, i64>(14)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;
    let Some((
        config,
        next_device_id,
        next_keyspace_id,
        next_file_id,
        next_metadata_node_id,
        next_keyspace_root_id,
        next_keyspace_catalog_shard_id,
        next_commit_group_id,
        next_commit_seq,
        next_checkpoint_id,
        next_gc_epoch,
        next_write_intent,
        next_extent_id,
        next_segment_id,
        next_placement_index,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(DurableExportCursor {
        config: decode_row(&config)?,
        next_device_id: parse_u128_key(&next_device_id).map_err(sqlite_error)?,
        next_keyspace_id: parse_u128_key(&next_keyspace_id).map_err(sqlite_error)?,
        next_file_id: parse_u128_key(&next_file_id).map_err(sqlite_error)?,
        next_metadata_node_id: parse_u128_key(&next_metadata_node_id).map_err(sqlite_error)?,
        next_keyspace_root_id: parse_u128_key(&next_keyspace_root_id).map_err(sqlite_error)?,
        next_keyspace_catalog_shard_id: parse_u128_key(&next_keyspace_catalog_shard_id)
            .map_err(sqlite_error)?,
        next_commit_group_id: parse_u128_key(&next_commit_group_id).map_err(sqlite_error)?,
        next_commit_seq: i64_to_u64(next_commit_seq).map_err(sqlite_error)?,
        next_checkpoint_id: parse_u128_key(&next_checkpoint_id).map_err(sqlite_error)?,
        next_gc_epoch: i64_to_u64(next_gc_epoch).map_err(sqlite_error)?,
        next_write_intent: parse_u128_key(&next_write_intent).map_err(sqlite_error)?,
        next_extent_id: parse_u128_key(&next_extent_id).map_err(sqlite_error)?,
        next_segment_id: parse_u128_key(&next_segment_id).map_err(sqlite_error)?,
        next_placement_index: i64_to_u64(next_placement_index).map_err(sqlite_error)?,
    }))
}

fn load_maintenance_cursor(conn: &Connection) -> Result<Option<DurableDataLogRef>> {
    let row = conn
        .query_row(
            "SELECT cursor_storage_node, cursor_log_id
             FROM maintenance_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;
    let Some((storage_node, log_id)) = row else {
        return Ok(None);
    };
    match (storage_node, log_id) {
        (Some(storage_node), Some(log_id)) => Ok(Some(DurableDataLogRef {
            storage_node: StorageNodeId::from_raw(
                parse_u128_key(&storage_node).map_err(sqlite_error)?,
            ),
            log_id: i64_to_u64(log_id).map_err(sqlite_error)?,
        })),
        (None, None) => Ok(None),
        _ => Err(StorageError::corrupt(
            "maintenance cursor row is partially populated",
        )),
    }
}

fn reject_legacy_current_state_if_present(conn: &Connection) -> Result<()> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'current_state'
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if exists.is_none() {
        return Ok(());
    }
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM current_state", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if count > 0 {
        return Err(StorageError::unsupported(
            "legacy current_state blob stores are not supported by the row-native provider",
        ));
    }
    Ok(())
}

fn reject_root_storage_catalog_tables_if_present(conn: &Connection) -> Result<()> {
    for table in [
        "data_logs",
        "segment_placements",
        "storage_nodes",
        "segment_records",
        "segment_catalog_entries",
    ] {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = ?1
                 LIMIT 1",
                params![table],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        if exists.is_some() {
            return Err(StorageError::unsupported(
                "storage-node catalog tables must live in per-node catalog SQLite files",
            ));
        }
    }
    Ok(())
}

fn reject_orphan_row_native_rows_if_present(conn: &Connection) -> Result<()> {
    for table in [
        "device_specs",
        "device_heads",
        "deleted_device_heads",
        "keyspace_heads",
        "keyspace_roots",
        "keyspace_catalog_shards",
        "file_writer_epochs",
        "metadata_nodes",
        "commit_groups",
        "shard_commits",
        "keyspace_commits",
        "file_commits",
        "fork_records",
        "delete_records",
        "checkpoints",
        "metadata_gc_marks",
        "segment_gc_marks",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let count: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if count > 0 {
            return Err(StorageError::corrupt(
                "row-native SQLite rows exist without a durable export cursor",
            ));
        }
    }
    Ok(())
}

fn persist_row_native_state(
    tx: &rusqlite::Transaction<'_>,
    previous_cursor: Option<&DurableExportCursor>,
    image: &DurableStoreImage,
) -> Result<()> {
    let previous_u128 = |cursor_value: fn(&DurableExportCursor) -> u128| {
        previous_cursor.map(cursor_value).unwrap_or(0)
    };
    let previous_u64 = |cursor_value: fn(&DurableExportCursor) -> u64| {
        previous_cursor.map(cursor_value).unwrap_or(0)
    };
    let prune_metadata_history = previous_cursor.is_none()
        || previous_cursor
            .is_some_and(|cursor| cursor.next_gc_epoch != image.metadata.next_gc_epoch);

    sync_payload_table(
        tx,
        "device_specs",
        "device_id",
        image
            .metadata
            .device_specs
            .iter()
            .map(|(id, spec)| Ok((id.raw().to_string(), encode_row(spec)?)))
            .collect::<Result<Vec<_>>>()?,
    )?;
    sync_payload_table(
        tx,
        "device_heads",
        "device_id",
        image
            .metadata
            .device_heads
            .iter()
            .map(|(id, head)| Ok((id.raw().to_string(), encode_row(head)?)))
            .collect::<Result<Vec<_>>>()?,
    )?;
    sync_payload_table(
        tx,
        "deleted_device_heads",
        "device_id",
        image
            .metadata
            .deleted_device_heads
            .iter()
            .map(|(id, head)| Ok((id.raw().to_string(), encode_row(head)?)))
            .collect::<Result<Vec<_>>>()?,
    )?;
    sync_payload_table(
        tx,
        "keyspace_heads",
        "keyspace_id",
        image
            .metadata
            .keyspace_heads
            .iter()
            .map(|(id, head)| Ok((id.raw().to_string(), encode_row(head)?)))
            .collect::<Result<Vec<_>>>()?,
    )?;
    sync_u128_payload_map_since(
        tx,
        "keyspace_roots",
        "root_id",
        &image.metadata.keyspace_roots,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_keyspace_root_id),
        prune_metadata_history,
    )?;
    sync_u128_payload_map_since(
        tx,
        "keyspace_catalog_shards",
        "shard_id",
        &image.metadata.keyspace_catalog_shards,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_keyspace_catalog_shard_id),
        prune_metadata_history,
    )?;
    sync_file_writer_epochs(tx, &image.metadata.file_writer_epochs)?;
    sync_u128_payload_map_since(
        tx,
        "metadata_nodes",
        "node_id",
        &image.metadata.metadata_nodes,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_metadata_node_id),
        prune_metadata_history,
    )?;
    sync_commit_groups_since(
        tx,
        &image.metadata.commit_groups,
        previous_u128(|cursor| cursor.next_commit_group_id),
        prune_metadata_history,
    )?;
    sync_timeline_table_since(
        tx,
        "shard_commits",
        &image.metadata.shard_commits,
        previous_u64(|cursor| cursor.next_commit_seq),
        prune_metadata_history,
    )?;
    sync_timeline_table_since(
        tx,
        "keyspace_commits",
        &image.metadata.keyspace_commits,
        previous_u64(|cursor| cursor.next_commit_seq),
        prune_metadata_history,
    )?;
    sync_timeline_table_since(
        tx,
        "file_commits",
        &image.metadata.file_commits,
        previous_u64(|cursor| cursor.next_commit_seq),
        prune_metadata_history,
    )?;
    sync_commit_seq_payload_table_since(
        tx,
        "fork_records",
        &image.metadata.fork_records,
        previous_u64(|cursor| cursor.next_commit_seq),
        prune_metadata_history,
    )?;
    sync_commit_seq_payload_table_since(
        tx,
        "delete_records",
        &image.metadata.delete_records,
        previous_u64(|cursor| cursor.next_commit_seq),
        prune_metadata_history,
    )?;
    sync_checkpoints_since(
        tx,
        &image.metadata.checkpoints,
        previous_u128(|cursor| cursor.next_checkpoint_id),
        prune_metadata_history,
    )?;
    sync_epoch_table(
        tx,
        "metadata_gc_marks",
        "node_id",
        image
            .metadata
            .metadata_last_mark_epoch
            .iter()
            .map(|(id, epoch)| (id.raw().to_string(), *epoch))
            .collect(),
    )?;
    sync_epoch_table(
        tx,
        "segment_gc_marks",
        "segment_id",
        image
            .metadata
            .segment_last_mark_epoch
            .iter()
            .map(|(id, epoch)| (id.raw().to_string(), *epoch))
            .collect(),
    )?;
    persist_export_cursor(tx, &DurableExportCursor::from_image(image))
}

trait DurableTimelineRow: DurableCodec {
    fn commit_seq_raw(&self) -> u64;
    fn row_key(&self) -> String;
}

impl DurableTimelineRow for ShardCommit {
    fn commit_seq_raw(&self) -> u64 {
        self.commit_seq.raw()
    }

    fn row_key(&self) -> String {
        format!(
            "{:020}:{}:{}",
            self.commit_seq.raw(),
            self.device_id.raw(),
            self.shard_id.raw()
        )
    }
}

impl DurableTimelineRow for KeyspaceCommit {
    fn commit_seq_raw(&self) -> u64 {
        self.commit_seq.raw()
    }

    fn row_key(&self) -> String {
        format!("{:020}:{}", self.commit_seq.raw(), self.keyspace_id.raw())
    }
}

impl DurableTimelineRow for FileCommit {
    fn commit_seq_raw(&self) -> u64 {
        self.commit_seq.raw()
    }

    fn row_key(&self) -> String {
        format!(
            "{:020}:{}:{}",
            self.commit_seq.raw(),
            self.keyspace_id.raw(),
            self.file_id.raw()
        )
    }
}

fn persist_export_cursor(
    tx: &rusqlite::Transaction<'_>,
    cursor: &DurableExportCursor,
) -> Result<()> {
    tx.execute(
        "INSERT INTO store_meta(
           id, config, next_device_id, next_keyspace_id, next_file_id,
           next_metadata_node_id, next_keyspace_root_id,
           next_keyspace_catalog_shard_id, next_commit_group_id,
           next_commit_seq, next_checkpoint_id, next_gc_epoch,
           next_write_intent, next_extent_id, next_segment_id,
           next_placement_index
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                   ?12, ?13, ?14, ?15)
         ON CONFLICT(id) DO UPDATE SET
           config = excluded.config,
           next_device_id = excluded.next_device_id,
           next_keyspace_id = excluded.next_keyspace_id,
           next_file_id = excluded.next_file_id,
           next_metadata_node_id = excluded.next_metadata_node_id,
           next_keyspace_root_id = excluded.next_keyspace_root_id,
           next_keyspace_catalog_shard_id = excluded.next_keyspace_catalog_shard_id,
           next_commit_group_id = excluded.next_commit_group_id,
           next_commit_seq = excluded.next_commit_seq,
           next_checkpoint_id = excluded.next_checkpoint_id,
           next_gc_epoch = excluded.next_gc_epoch,
           next_write_intent = excluded.next_write_intent,
           next_extent_id = excluded.next_extent_id,
           next_segment_id = excluded.next_segment_id,
           next_placement_index = excluded.next_placement_index",
        params![
            encode_row(&cursor.config)?,
            cursor.next_device_id.to_string(),
            cursor.next_keyspace_id.to_string(),
            cursor.next_file_id.to_string(),
            cursor.next_metadata_node_id.to_string(),
            cursor.next_keyspace_root_id.to_string(),
            cursor.next_keyspace_catalog_shard_id.to_string(),
            cursor.next_commit_group_id.to_string(),
            u64_to_i64(cursor.next_commit_seq)?,
            cursor.next_checkpoint_id.to_string(),
            u64_to_i64(cursor.next_gc_epoch)?,
            cursor.next_write_intent.to_string(),
            cursor.next_extent_id.to_string(),
            cursor.next_segment_id.to_string(),
            u64_to_i64(cursor.next_placement_index)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn persist_maintenance_cursor(
    tx: &rusqlite::Transaction<'_>,
    cursor: Option<DurableDataLogRef>,
) -> Result<()> {
    match cursor {
        Some(cursor) => {
            tx.execute(
                "INSERT INTO maintenance_state(id, cursor_storage_node, cursor_log_id)
                 VALUES (1, ?1, ?2)
                 ON CONFLICT(id) DO UPDATE SET
                   cursor_storage_node = excluded.cursor_storage_node,
                   cursor_log_id = excluded.cursor_log_id",
                params![
                    storage_node_key(cursor.storage_node),
                    u64_to_i64(cursor.log_id)?
                ],
            )
            .map_err(sqlite_error)?;
        }
        None => {
            tx.execute(
                "INSERT INTO maintenance_state(id, cursor_storage_node, cursor_log_id)
                 VALUES (1, NULL, NULL)
                 ON CONFLICT(id) DO UPDATE SET
                   cursor_storage_node = NULL,
                   cursor_log_id = NULL",
                [],
            )
            .map_err(sqlite_error)?;
        }
    }
    Ok(())
}

enum SegmentCatalogSync<'a> {
    Full,
    Only(&'a BTreeSet<SegmentId>),
    Skip,
}

fn sync_node_catalog_state_for_node(
    tx: &rusqlite::Transaction<'_>,
    ordinal: usize,
    storage_node: StorageNodeId,
    node: &StorageNodeInner,
    catalog_sync: SegmentCatalogSync<'_>,
    pre_root_pending_segments: &BTreeSet<SegmentId>,
) -> Result<()> {
    let row = DurableStorageNodeRow {
        storage_node,
        ordinal: u64::try_from(ordinal)
            .map_err(|_| StorageError::invalid_argument("storage node ordinal overflows u64"))?,
        next_catalog_segment_id: node.segment_catalog.next_segment_id,
        segment_store_next_offset: node.segment_store.next_offset,
    };
    sync_node_meta_row(tx, row)?;
    match catalog_sync {
        SegmentCatalogSync::Full => {
            sync_node_segment_catalog_entries(tx, storage_node, node, pre_root_pending_segments)?
        }
        SegmentCatalogSync::Only(segment_ids) => sync_node_segment_catalog_entries_for_ids(
            tx,
            storage_node,
            node,
            segment_ids,
            pre_root_pending_segments,
        )?,
        SegmentCatalogSync::Skip => {}
    }
    Ok(())
}

fn persist_data_log_manifest(
    tx: &rusqlite::Transaction<'_>,
    log: &PendingDataLogManifest,
) -> Result<()> {
    let data_logs = node_catalog_table(log.storage_node, "data_logs")?;
    tx.execute(
        &format!(
            "INSERT INTO {data_logs}(log_id, state, total_bytes, live_bytes, dead_bytes)
             VALUES (?1, ?2, ?3, 0, 0)
             ON CONFLICT(log_id) DO UPDATE SET
               state = excluded.state,
               total_bytes = MAX(total_bytes, excluded.total_bytes)"
        ),
        params![
            u64_to_i64(log.log_id)?,
            log.state,
            u64_to_i64(log.total_bytes)?
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn seal_data_log_manifest(
    tx: &rusqlite::Transaction<'_>,
    log_ref: DurableDataLogRef,
) -> Result<()> {
    let data_logs = node_catalog_table(log_ref.storage_node, "data_logs")?;
    tx.execute(
        &format!(
            "UPDATE {data_logs} SET state = 'sealed'
             WHERE log_id = ?1 AND state != 'deleted'"
        ),
        params![u64_to_i64(log_ref.log_id)?],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn persist_segment_placement(
    tx: &rusqlite::Transaction<'_>,
    placement: &SegmentPlacementRow,
) -> Result<()> {
    let segment_placements = node_catalog_table(placement.storage_node, "segment_placements")?;
    let data_logs = node_catalog_table(placement.storage_node, "data_logs")?;
    tx.execute(
        &format!(
            "INSERT INTO {segment_placements}(
               segment_id, data_log_id, record_offset, record_bytes,
               payload_offset, payload_bytes, checksum, current
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)
             ON CONFLICT(segment_id) DO UPDATE SET
               data_log_id = excluded.data_log_id,
               record_offset = excluded.record_offset,
               record_bytes = excluded.record_bytes,
               payload_offset = excluded.payload_offset,
               payload_bytes = excluded.payload_bytes,
               checksum = excluded.checksum,
               current = 1"
        ),
        params![
            segment_id_key(placement.segment_id),
            u64_to_i64(placement.data_log_id)?,
            u64_to_i64(placement.record_offset)?,
            u64_to_i64(placement.record_bytes)?,
            u64_to_i64(placement.payload_offset)?,
            u64_to_i64(placement.payload_bytes)?,
            u64_key(placement.checksum),
        ],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        &format!(
            "UPDATE {data_logs} SET live_bytes = live_bytes + ?2
             WHERE log_id = ?1"
        ),
        params![
            u64_to_i64(placement.data_log_id)?,
            u64_to_i64(placement.payload_bytes)?
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn sync_node_meta_row(tx: &rusqlite::Transaction<'_>, row: DurableStorageNodeRow) -> Result<()> {
    let node_meta = node_catalog_table(row.storage_node, "node_meta")?;
    tx.execute(
        &format!(
            "INSERT INTO {node_meta}(
               id, storage_node, ordinal, next_catalog_segment_id, segment_store_next_offset
             ) VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET
               storage_node = excluded.storage_node,
               ordinal = excluded.ordinal,
               next_catalog_segment_id = excluded.next_catalog_segment_id,
               segment_store_next_offset = excluded.segment_store_next_offset
             WHERE storage_node != excluded.storage_node
                OR ordinal != excluded.ordinal
                OR next_catalog_segment_id != excluded.next_catalog_segment_id
                OR segment_store_next_offset != excluded.segment_store_next_offset"
        ),
        params![
            storage_node_key(row.storage_node),
            u64_to_i64(row.ordinal)?,
            row.next_catalog_segment_id.to_string(),
            u64_to_i64(row.segment_store_next_offset)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn sync_node_segment_catalog_entries(
    tx: &rusqlite::Transaction<'_>,
    storage_node: StorageNodeId,
    node: &StorageNodeInner,
    pre_root_pending_segments: &BTreeSet<SegmentId>,
) -> Result<()> {
    let mut desired = BTreeMap::new();
    for (segment_id, entry) in &node.segment_catalog.entries {
        desired.insert(
            segment_id.raw().to_string(),
            encode_catalog_entry_for_pre_root_publish(
                *segment_id,
                entry,
                pre_root_pending_segments,
            )?,
        );
    }
    let segment_catalog_entries = node_catalog_table(storage_node, "segment_catalog_entries")?;
    delete_missing_text_keys(tx, segment_catalog_entries, "segment_id", desired.keys())?;
    let mut stmt = tx
        .prepare(&format!(
            "INSERT INTO {segment_catalog_entries}(segment_id, payload)
             VALUES (?1, ?2)
             ON CONFLICT(segment_id) DO UPDATE SET
               payload = excluded.payload
             WHERE payload != excluded.payload"
        ))
        .map_err(sqlite_error)?;
    for (segment_id, payload) in desired {
        stmt.execute(params![segment_id, payload])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_node_segment_catalog_entries_for_ids(
    tx: &rusqlite::Transaction<'_>,
    storage_node: StorageNodeId,
    node: &StorageNodeInner,
    segment_ids: &BTreeSet<SegmentId>,
    pre_root_pending_segments: &BTreeSet<SegmentId>,
) -> Result<()> {
    if segment_ids.is_empty() {
        return Ok(());
    }
    let segment_catalog_entries = node_catalog_table(storage_node, "segment_catalog_entries")?;
    let delete_sql = format!("DELETE FROM {segment_catalog_entries} WHERE segment_id = ?1");
    let mut stmt = tx
        .prepare(&format!(
            "INSERT INTO {segment_catalog_entries}(segment_id, payload)
             VALUES (?1, ?2)
             ON CONFLICT(segment_id) DO UPDATE SET
               payload = excluded.payload
             WHERE payload != excluded.payload"
        ))
        .map_err(sqlite_error)?;
    for segment_id in segment_ids {
        if let Some(entry) = node.segment_catalog.entries.get(segment_id) {
            stmt.execute(params![
                segment_id.raw().to_string(),
                encode_catalog_entry_for_pre_root_publish(
                    *segment_id,
                    entry,
                    pre_root_pending_segments,
                )?
            ])
            .map_err(sqlite_error)?;
        } else {
            tx.execute(&delete_sql, params![segment_id.raw().to_string()])
                .map_err(sqlite_error)?;
        }
    }
    Ok(())
}

fn encode_catalog_entry_for_pre_root_publish(
    segment_id: SegmentId,
    entry: &CatalogEntry,
    pre_root_pending_segments: &BTreeSet<SegmentId>,
) -> Result<Vec<u8>> {
    if pre_root_pending_segments.contains(&segment_id)
        && entry.state == SegmentLifecycleState::Referenced
    {
        let mut pending = entry.clone();
        pending.state = SegmentLifecycleState::DurablePendingMetadata;
        encode_row(&pending)
    } else {
        encode_row(entry)
    }
}

fn sync_payload_table(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    rows: Vec<(String, Vec<u8>)>,
) -> Result<()> {
    let desired: BTreeMap<String, Vec<u8>> = rows.into_iter().collect();
    delete_missing_text_keys(tx, table, key_col, desired.keys())?;
    let sql = format!(
        "INSERT INTO {table}({key_col}, payload) VALUES (?1, ?2)
         ON CONFLICT({key_col}) DO UPDATE SET payload = excluded.payload
         WHERE payload != excluded.payload"
    );
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    for (key, payload) in desired {
        stmt.execute(params![key, payload]).map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_u128_payload_map_since<K, V>(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    rows: &BTreeMap<K, V>,
    raw_key: impl Fn(K) -> u128,
    previous_next_id: u128,
    prune_missing: bool,
) -> Result<()>
where
    K: Copy + Ord,
    V: DurableCodec,
{
    if prune_missing {
        let desired: Vec<String> = rows.keys().map(|id| raw_key(*id).to_string()).collect();
        delete_missing_text_keys(tx, table, key_col, desired.iter())?;
    }
    let sql = format!(
        "INSERT INTO {table}({key_col}, payload) VALUES (?1, ?2)
         ON CONFLICT({key_col}) DO UPDATE SET payload = excluded.payload
         WHERE payload != excluded.payload"
    );
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    for (id, payload) in rows {
        let raw = raw_key(*id);
        if raw < previous_next_id {
            continue;
        }
        stmt.execute(params![raw.to_string(), encode_row(payload)?])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_file_writer_epochs(
    tx: &rusqlite::Transaction<'_>,
    epochs: &BTreeMap<(KeyspaceId, FileId), WriterEpoch>,
) -> Result<()> {
    let desired: BTreeMap<String, (KeyspaceId, FileId, Vec<u8>)> = epochs
        .iter()
        .map(|((keyspace_id, file_id), epoch)| {
            Ok((
                file_writer_key(*keyspace_id, *file_id),
                (*keyspace_id, *file_id, encode_row(epoch)?),
            ))
        })
        .collect::<Result<_>>()?;
    delete_missing_text_keys(tx, "file_writer_epochs", "file_key", desired.keys())?;
    let mut stmt = tx
        .prepare(
            "INSERT INTO file_writer_epochs(file_key, keyspace_id, file_id, payload)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(file_key) DO UPDATE SET
               keyspace_id = excluded.keyspace_id,
               file_id = excluded.file_id,
               payload = excluded.payload
             WHERE keyspace_id != excluded.keyspace_id
                OR file_id != excluded.file_id
                OR payload != excluded.payload",
        )
        .map_err(sqlite_error)?;
    for (key, (keyspace_id, file_id, payload)) in desired {
        stmt.execute(params![
            key,
            keyspace_id.raw().to_string(),
            file_id.raw().to_string(),
            payload,
        ])
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_commit_groups_since(
    tx: &rusqlite::Transaction<'_>,
    groups: &BTreeMap<CommitGroupId, CommitGroup>,
    previous_next_id: u128,
    prune_missing: bool,
) -> Result<()> {
    if prune_missing {
        let desired: Vec<String> = groups.keys().map(|id| id.raw().to_string()).collect();
        delete_missing_text_keys(tx, "commit_groups", "commit_group_id", desired.iter())?;
    }
    let mut stmt = tx
        .prepare(
            "INSERT INTO commit_groups(commit_group_id, commit_seq, payload)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(commit_group_id) DO UPDATE SET
               commit_seq = excluded.commit_seq,
               payload = excluded.payload
             WHERE commit_seq != excluded.commit_seq
                OR payload != excluded.payload",
        )
        .map_err(sqlite_error)?;
    for (id, group) in groups {
        if id.raw() < previous_next_id {
            continue;
        }
        stmt.execute(params![
            id.raw().to_string(),
            u64_to_i64(group.commit_seq.raw())?,
            encode_row(group)?,
        ])
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_timeline_table_since<T: DurableTimelineRow>(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    rows: &[T],
    previous_next_commit_seq: u64,
    prune_missing: bool,
) -> Result<()> {
    if prune_missing {
        let desired: Vec<String> = rows.iter().map(DurableTimelineRow::row_key).collect();
        delete_missing_text_keys(tx, table, "row_key", desired.iter())?;
    }
    let sql = format!(
        "INSERT INTO {table}(row_key, commit_seq, ordinal, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(row_key) DO UPDATE SET
           commit_seq = excluded.commit_seq,
           ordinal = excluded.ordinal,
           payload = excluded.payload
         WHERE commit_seq != excluded.commit_seq
            OR ordinal != excluded.ordinal
            OR payload != excluded.payload"
    );
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    for (ordinal, row) in rows.iter().enumerate() {
        if row.commit_seq_raw() < previous_next_commit_seq {
            continue;
        }
        let ordinal = u64::try_from(ordinal)
            .map_err(|_| StorageError::invalid_argument("timeline ordinal overflows u64"))?;
        stmt.execute(params![
            row.row_key(),
            u64_to_i64(row.commit_seq_raw())?,
            u64_to_i64(ordinal)?,
            encode_row(row)?,
        ])
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_commit_seq_payload_table_since<T: DurableCodec>(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    rows: &BTreeMap<CommitSeq, T>,
    previous_next_commit_seq: u64,
    prune_missing: bool,
) -> Result<()> {
    if prune_missing {
        delete_missing_u64_keys(tx, table, "commit_seq", rows.keys().map(|seq| seq.raw()))?;
    }
    let sql = format!(
        "INSERT INTO {table}(commit_seq, payload) VALUES (?1, ?2)
         ON CONFLICT(commit_seq) DO UPDATE SET payload = excluded.payload
         WHERE payload != excluded.payload"
    );
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    for (commit_seq, payload) in rows {
        if commit_seq.raw() < previous_next_commit_seq {
            continue;
        }
        stmt.execute(params![u64_to_i64(commit_seq.raw())?, encode_row(payload)?])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_checkpoints_since(
    tx: &rusqlite::Transaction<'_>,
    checkpoints: &BTreeMap<CheckpointId, Checkpoint>,
    previous_next_id: u128,
    prune_missing: bool,
) -> Result<()> {
    if prune_missing {
        let desired: Vec<String> = checkpoints.keys().map(|id| id.raw().to_string()).collect();
        delete_missing_text_keys(tx, "checkpoints", "checkpoint_id", desired.iter())?;
    }
    let mut stmt = tx
        .prepare(
            "INSERT INTO checkpoints(checkpoint_id, commit_seq, payload)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(checkpoint_id) DO UPDATE SET
               commit_seq = excluded.commit_seq,
               payload = excluded.payload
             WHERE commit_seq != excluded.commit_seq
                OR payload != excluded.payload",
        )
        .map_err(sqlite_error)?;
    for (id, checkpoint) in checkpoints {
        if id.raw() < previous_next_id {
            continue;
        }
        stmt.execute(params![
            id.raw().to_string(),
            u64_to_i64(checkpoint.commit_seq.raw())?,
            encode_row(checkpoint)?,
        ])
        .map_err(sqlite_error)?;
    }
    Ok(())
}

fn sync_epoch_table(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    rows: BTreeMap<String, u64>,
) -> Result<()> {
    delete_missing_text_keys(tx, table, key_col, rows.keys())?;
    let sql = format!(
        "INSERT INTO {table}({key_col}, epoch) VALUES (?1, ?2)
         ON CONFLICT({key_col}) DO UPDATE SET epoch = excluded.epoch
         WHERE epoch != excluded.epoch"
    );
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    for (key, epoch) in rows {
        stmt.execute(params![key, u64_to_i64(epoch)?])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn existing_text_keys(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
) -> Result<BTreeSet<String>> {
    let sql = format!("SELECT {key_col} FROM {table}");
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = BTreeSet::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        out.insert(row.get(0).map_err(sqlite_error)?);
    }
    Ok(out)
}

fn delete_missing_text_keys<'a>(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    desired: impl IntoIterator<Item = &'a String>,
) -> Result<()> {
    let desired: BTreeSet<String> = desired.into_iter().cloned().collect();
    let existing = existing_text_keys(tx, table, key_col)?;
    let sql = format!("DELETE FROM {table} WHERE {key_col} = ?1");
    for key in existing.difference(&desired) {
        tx.execute(&sql, params![key]).map_err(sqlite_error)?;
    }
    Ok(())
}

fn delete_missing_u64_keys(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    key_col: &str,
    desired: impl IntoIterator<Item = u64>,
) -> Result<()> {
    let desired: BTreeSet<u64> = desired.into_iter().collect();
    let sql = format!("SELECT {key_col} FROM {table}");
    let mut stmt = tx.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut existing = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let raw: i64 = row.get(0).map_err(sqlite_error)?;
        existing.push(i64_to_u64(raw).map_err(sqlite_error)?);
    }
    let sql = format!("DELETE FROM {table} WHERE {key_col} = ?1");
    for key in existing {
        if !desired.contains(&key) {
            tx.execute(&sql, params![u64_to_i64(key)?])
                .map_err(sqlite_error)?;
        }
    }
    Ok(())
}

fn load_storage_node_rows(node_catalogs: &NodeCatalogs) -> Result<Vec<DurableStorageNodeRow>> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for catalog_node in node_catalogs.storage_nodes() {
        let conn = node_catalogs.lock(catalog_node)?;
        let node_meta = node_catalog_table(catalog_node, "node_meta")?;
        let row = conn
            .query_row(
                &format!(
                    "SELECT storage_node, ordinal, next_catalog_segment_id,
                            segment_store_next_offset
                     FROM {node_meta}
                     WHERE id = 1"
                ),
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?;
        let Some((storage_node, ordinal, next_catalog_segment_id, segment_store_next_offset)) = row
        else {
            continue;
        };
        let storage_node =
            StorageNodeId::from_raw(parse_u128_key(&storage_node).map_err(sqlite_error)?);
        if storage_node != catalog_node {
            return Err(StorageError::corrupt(
                "storage-node catalog metadata disagrees with catalog path",
            ));
        }
        if !seen.insert(storage_node) {
            return Err(StorageError::corrupt("duplicate storage node row"));
        }
        out.push(DurableStorageNodeRow {
            storage_node,
            ordinal: i64_to_u64(ordinal).map_err(sqlite_error)?,
            next_catalog_segment_id: parse_u128_key(&next_catalog_segment_id)
                .map_err(sqlite_error)?,
            segment_store_next_offset: i64_to_u64(segment_store_next_offset)
                .map_err(sqlite_error)?,
        });
    }
    out.sort_by_key(|row| row.ordinal);
    for (index, row) in out.iter().enumerate() {
        if row.ordinal
            != u64::try_from(index)
                .map_err(|_| StorageError::corrupt("storage node ordinal overflows u64"))?
        {
            return Err(StorageError::corrupt(
                "storage node ordinals are not contiguous",
            ));
        }
    }
    Ok(out)
}

fn load_metadata_inner(conn: &Connection, cursor: &DurableExportCursor) -> Result<MetadataInner> {
    let mut metadata = MetadataInner::new();
    metadata.next_device_id = cursor.next_device_id;
    metadata.next_keyspace_id = cursor.next_keyspace_id;
    metadata.next_file_id = cursor.next_file_id;
    metadata.next_metadata_node_id = cursor.next_metadata_node_id;
    metadata.next_keyspace_root_id = cursor.next_keyspace_root_id;
    metadata.next_keyspace_catalog_shard_id = cursor.next_keyspace_catalog_shard_id;
    metadata.next_commit_group_id = cursor.next_commit_group_id;
    metadata.next_commit_seq = cursor.next_commit_seq;
    metadata.next_checkpoint_id = cursor.next_checkpoint_id;
    metadata.next_gc_epoch = cursor.next_gc_epoch;
    metadata.device_specs = load_device_specs(conn)?;
    metadata.device_heads = load_device_heads(conn, "device_heads")?;
    metadata.deleted_device_heads = load_device_heads(conn, "deleted_device_heads")?;
    metadata.keyspace_heads = load_keyspace_heads(conn)?;
    metadata.keyspace_roots = load_keyspace_roots(conn)?;
    metadata.keyspace_catalog_shards = load_keyspace_catalog_shards(conn)?;
    metadata.file_writer_epochs = load_file_writer_epochs(conn)?;
    metadata.metadata_nodes = load_metadata_nodes(conn)?;
    metadata.commit_groups = load_commit_groups(conn)?;
    metadata.shard_commits = load_timeline_rows(conn, "shard_commits")?;
    metadata.keyspace_commits = load_timeline_rows(conn, "keyspace_commits")?;
    metadata.file_commits = load_timeline_rows(conn, "file_commits")?;
    metadata.fork_records = load_commit_seq_payload_map(conn, "fork_records")?;
    metadata.delete_records = load_commit_seq_payload_map(conn, "delete_records")?;
    metadata.checkpoints = load_checkpoints(conn)?;
    metadata.metadata_last_mark_epoch = load_metadata_gc_marks(conn)?;
    metadata.segment_last_mark_epoch = load_segment_gc_marks(conn)?;
    Ok(metadata)
}

fn load_payload_rows(
    conn: &Connection,
    table: &str,
    key_col: &str,
    order_by: &str,
) -> Result<Vec<(String, Vec<u8>)>> {
    let sql = format!("SELECT {key_col}, payload FROM {table} ORDER BY {order_by}");
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        out.push((
            row.get(0).map_err(sqlite_error)?,
            row.get(1).map_err(sqlite_error)?,
        ));
    }
    Ok(out)
}

fn load_device_specs(conn: &Connection) -> Result<BTreeMap<DeviceId, crate::api::DeviceSpec>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, "device_specs", "device_id", "device_id")? {
        let id = DeviceId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let spec = decode_row(&payload)?;
        if out.insert(id, spec).is_some() {
            return Err(StorageError::corrupt("duplicate device spec row"));
        }
    }
    Ok(out)
}

fn load_device_heads(conn: &Connection, table: &str) -> Result<BTreeMap<DeviceId, DeviceHead>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, table, "device_id", "device_id")? {
        let id = DeviceId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let head: DeviceHead = decode_row(&payload)?;
        if head.device_id != id {
            return Err(StorageError::corrupt(
                "device head row key disagrees with payload",
            ));
        }
        if out.insert(id, head).is_some() {
            return Err(StorageError::corrupt("duplicate device head row"));
        }
    }
    Ok(out)
}

fn load_keyspace_heads(conn: &Connection) -> Result<BTreeMap<KeyspaceId, KeyspaceHead>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, "keyspace_heads", "keyspace_id", "keyspace_id")? {
        let id = KeyspaceId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let head: KeyspaceHead = decode_row(&payload)?;
        if head.keyspace_id != id {
            return Err(StorageError::corrupt(
                "keyspace head row key disagrees with payload",
            ));
        }
        if out.insert(id, head).is_some() {
            return Err(StorageError::corrupt("duplicate keyspace head row"));
        }
    }
    Ok(out)
}

fn load_keyspace_roots(conn: &Connection) -> Result<BTreeMap<KeyspaceRootId, KeyspaceRoot>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, "keyspace_roots", "root_id", "root_id")? {
        let id = KeyspaceRootId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let root: KeyspaceRoot = decode_row(&payload)?;
        if root.root_id != id {
            return Err(StorageError::corrupt(
                "keyspace root row key disagrees with payload",
            ));
        }
        if out.insert(id, root).is_some() {
            return Err(StorageError::corrupt("duplicate keyspace root row"));
        }
    }
    Ok(out)
}

fn load_keyspace_catalog_shards(
    conn: &Connection,
) -> Result<BTreeMap<KeyspaceCatalogShardId, KeyspaceCatalogShard>> {
    let mut out = BTreeMap::new();
    for (key, payload) in
        load_payload_rows(conn, "keyspace_catalog_shards", "shard_id", "shard_id")?
    {
        let id = KeyspaceCatalogShardId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let shard: KeyspaceCatalogShard = decode_row(&payload)?;
        if shard.shard_id != id {
            return Err(StorageError::corrupt(
                "keyspace catalog shard row key disagrees with payload",
            ));
        }
        if out.insert(id, shard).is_some() {
            return Err(StorageError::corrupt(
                "duplicate keyspace catalog shard row",
            ));
        }
    }
    Ok(out)
}

fn load_file_writer_epochs(
    conn: &Connection,
) -> Result<BTreeMap<(KeyspaceId, FileId), WriterEpoch>> {
    let mut stmt = conn
        .prepare(
            "SELECT file_key, keyspace_id, file_id, payload
             FROM file_writer_epochs
             ORDER BY keyspace_id, file_id",
        )
        .map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let file_key: String = row.get(0).map_err(sqlite_error)?;
        let keyspace_id: String = row.get(1).map_err(sqlite_error)?;
        let file_id: String = row.get(2).map_err(sqlite_error)?;
        let keyspace_id = KeyspaceId::from_raw(parse_u128_key(&keyspace_id).map_err(sqlite_error)?);
        let file_id = FileId::from_raw(parse_u128_key(&file_id).map_err(sqlite_error)?);
        if file_key != file_writer_key(keyspace_id, file_id) {
            return Err(StorageError::corrupt(
                "file writer epoch key is inconsistent",
            ));
        }
        let payload: Vec<u8> = row.get(3).map_err(sqlite_error)?;
        let epoch = decode_row(&payload)?;
        if out.insert((keyspace_id, file_id), epoch).is_some() {
            return Err(StorageError::corrupt("duplicate file writer epoch row"));
        }
    }
    Ok(out)
}

fn load_metadata_nodes(conn: &Connection) -> Result<BTreeMap<MetadataNodeId, MetadataNode>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, "metadata_nodes", "node_id", "node_id")? {
        let id = MetadataNodeId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let node: MetadataNode = decode_row(&payload)?;
        if node.node_id != id {
            return Err(StorageError::corrupt(
                "metadata node row key disagrees with payload",
            ));
        }
        if out.insert(id, node).is_some() {
            return Err(StorageError::corrupt("duplicate metadata node row"));
        }
    }
    Ok(out)
}

fn load_commit_groups(conn: &Connection) -> Result<BTreeMap<CommitGroupId, CommitGroup>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(
        conn,
        "commit_groups",
        "commit_group_id",
        "commit_seq, commit_group_id",
    )? {
        let id = CommitGroupId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let group: CommitGroup = decode_row(&payload)?;
        if group.commit_group != id {
            return Err(StorageError::corrupt(
                "commit group row key disagrees with payload",
            ));
        }
        if out.insert(id, group).is_some() {
            return Err(StorageError::corrupt("duplicate commit group row"));
        }
    }
    Ok(out)
}

fn load_timeline_rows<T: DurableTimelineRow>(conn: &Connection, table: &str) -> Result<Vec<T>> {
    let sql = format!("SELECT payload FROM {table} ORDER BY commit_seq, ordinal");
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = Vec::new();
    let mut last_commit_seq = None;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let payload: Vec<u8> = row.get(0).map_err(sqlite_error)?;
        let record: T = decode_row(&payload)?;
        if let Some(last) = last_commit_seq
            && record.commit_seq_raw() < last
        {
            return Err(StorageError::corrupt("timeline rows are not monotonic"));
        }
        last_commit_seq = Some(record.commit_seq_raw());
        out.push(record);
    }
    Ok(out)
}

fn load_commit_seq_payload_map<T: DurableCodec>(
    conn: &Connection,
    table: &str,
) -> Result<BTreeMap<CommitSeq, T>> {
    let sql = format!("SELECT commit_seq, payload FROM {table} ORDER BY commit_seq");
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let seq = CommitSeq::from_raw(
            i64_to_u64(row.get(0).map_err(sqlite_error)?).map_err(sqlite_error)?,
        );
        let payload: Vec<u8> = row.get(1).map_err(sqlite_error)?;
        let record = decode_row(&payload)?;
        if out.insert(seq, record).is_some() {
            return Err(StorageError::corrupt("duplicate commit-seq row"));
        }
    }
    Ok(out)
}

fn load_checkpoints(conn: &Connection) -> Result<BTreeMap<CheckpointId, Checkpoint>> {
    let mut out = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, "checkpoints", "checkpoint_id", "commit_seq")? {
        let id = CheckpointId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let checkpoint: Checkpoint = decode_row(&payload)?;
        if checkpoint.checkpoint_id != id {
            return Err(StorageError::corrupt(
                "checkpoint row key disagrees with payload",
            ));
        }
        if out.insert(id, checkpoint).is_some() {
            return Err(StorageError::corrupt("duplicate checkpoint row"));
        }
    }
    Ok(out)
}

fn load_metadata_gc_marks(conn: &Connection) -> Result<BTreeMap<MetadataNodeId, u64>> {
    let rows = load_epoch_rows(conn, "metadata_gc_marks", "node_id")?;
    let mut out = BTreeMap::new();
    for (key, epoch) in rows {
        out.insert(
            MetadataNodeId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?),
            epoch,
        );
    }
    Ok(out)
}

fn load_segment_gc_marks(conn: &Connection) -> Result<BTreeMap<SegmentId, u64>> {
    let rows = load_epoch_rows(conn, "segment_gc_marks", "segment_id")?;
    let mut out = BTreeMap::new();
    for (key, epoch) in rows {
        out.insert(
            SegmentId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?),
            epoch,
        );
    }
    Ok(out)
}

fn load_epoch_rows(conn: &Connection, table: &str, key_col: &str) -> Result<Vec<(String, u64)>> {
    let sql = format!("SELECT {key_col}, epoch FROM {table} ORDER BY {key_col}");
    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        out.push((
            row.get(0).map_err(sqlite_error)?,
            i64_to_u64(row.get(1).map_err(sqlite_error)?).map_err(sqlite_error)?,
        ));
    }
    Ok(out)
}

fn load_catalog_inner(
    conn: &Connection,
    storage_node: StorageNodeId,
    next_segment_id: u128,
) -> Result<CatalogInner> {
    let segment_catalog_entries = node_catalog_table(storage_node, "segment_catalog_entries")?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT segment_id, payload
             FROM {segment_catalog_entries}
             ORDER BY segment_id"
        ))
        .map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut entries = BTreeMap::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let segment_id: String = row.get(0).map_err(sqlite_error)?;
        let segment_id = SegmentId::from_raw(parse_u128_key(&segment_id).map_err(sqlite_error)?);
        let payload: Vec<u8> = row.get(1).map_err(sqlite_error)?;
        let entry: CatalogEntry = decode_row(&payload)?;
        if entry.reservation.segment_id != segment_id {
            return Err(StorageError::corrupt(
                "segment catalog row key disagrees with payload",
            ));
        }
        if entry
            .receipt
            .as_ref()
            .map(|receipt| receipt.placement.storage_node)
            != entry.receipt.as_ref().map(|_| storage_node)
        {
            return Err(StorageError::corrupt(
                "segment catalog receipt storage node disagrees with row",
            ));
        }
        if entries.insert(segment_id, entry).is_some() {
            return Err(StorageError::corrupt("duplicate segment catalog row"));
        }
    }
    Ok(CatalogInner {
        next_segment_id,
        entries,
    })
}

fn validate_row_native_image(image: &DurableStoreImage) -> Result<()> {
    validate_row_native_cursors(image)?;
    let descriptors = row_native_segment_descriptors(image);
    for (device_id, head) in &image.metadata.device_heads {
        if *device_id != head.device_id {
            return Err(StorageError::corrupt("live device head key mismatch"));
        }
        head.validate(image.config.shard_count)?;
        if !image.metadata.device_specs.contains_key(device_id) {
            return Err(StorageError::corrupt("live device head missing spec"));
        }
        for root in &head.shard_roots {
            if !image.metadata.metadata_nodes.contains_key(root) {
                return Err(StorageError::corrupt("live device head root missing"));
            }
        }
    }
    for (device_id, head) in &image.metadata.deleted_device_heads {
        if *device_id != head.device_id {
            return Err(StorageError::corrupt("deleted device head key mismatch"));
        }
        head.validate(image.config.shard_count)?;
        if !image.metadata.device_specs.contains_key(device_id) {
            return Err(StorageError::corrupt("deleted device head missing spec"));
        }
        for root in &head.shard_roots {
            if !image.metadata.metadata_nodes.contains_key(root) {
                return Err(StorageError::corrupt("deleted device head root missing"));
            }
        }
    }
    for (keyspace_id, head) in &image.metadata.keyspace_heads {
        if *keyspace_id != head.keyspace_id {
            return Err(StorageError::corrupt("keyspace head key mismatch"));
        }
        if !image.metadata.keyspace_roots.contains_key(&head.root) {
            return Err(StorageError::corrupt("keyspace head root missing"));
        }
    }
    for root in image.metadata.keyspace_roots.values() {
        root.validate()?;
        for shard_id in &root.shard_roots {
            if !image
                .metadata
                .keyspace_catalog_shards
                .contains_key(shard_id)
            {
                return Err(StorageError::corrupt("keyspace root shard missing"));
            }
        }
    }
    for shard in image.metadata.keyspace_catalog_shards.values() {
        shard.validate()?;
        for entry in shard.files.values() {
            if !image.metadata.metadata_nodes.contains_key(&entry.head.root) {
                return Err(StorageError::corrupt("file head root missing"));
            }
        }
    }
    for node in image.metadata.metadata_nodes.values() {
        let mut node_descriptors = Vec::new();
        if let MetadataNodeKind::Leaf { entries } = &node.kind {
            for entry in entries {
                let descriptor = descriptors.get(&entry.segment_id).ok_or_else(|| {
                    StorageError::corrupt("metadata leaf references missing segment descriptor")
                })?;
                node_descriptors.push(descriptor.clone());
            }
        }
        node.validate(&node_descriptors)?;
    }
    for checkpoint in image.metadata.checkpoints.values() {
        match &checkpoint.roots {
            CheckpointRoots::BlockShard(roots) => {
                for root in roots {
                    if !image.metadata.metadata_nodes.contains_key(root) {
                        return Err(StorageError::corrupt("checkpoint block root missing"));
                    }
                }
            }
            CheckpointRoots::NativeKeyspace(root) => {
                if !image.metadata.keyspace_roots.contains_key(root) {
                    return Err(StorageError::corrupt("checkpoint keyspace root missing"));
                }
            }
        }
    }
    for commit in &image.metadata.shard_commits {
        if !image.metadata.device_specs.contains_key(&commit.device_id) {
            return Err(StorageError::corrupt(
                "shard commit references missing device spec",
            ));
        }
        if !image.metadata.metadata_nodes.contains_key(&commit.old_root)
            || !image.metadata.metadata_nodes.contains_key(&commit.new_root)
        {
            return Err(StorageError::corrupt(
                "shard commit references missing metadata root",
            ));
        }
    }
    for commit in &image.metadata.keyspace_commits {
        if !image.metadata.keyspace_roots.contains_key(&commit.old_root)
            || !image.metadata.keyspace_roots.contains_key(&commit.new_root)
        {
            return Err(StorageError::corrupt(
                "keyspace commit references missing catalog root",
            ));
        }
    }
    for commit in &image.metadata.file_commits {
        if commit
            .old_root
            .is_some_and(|root| !image.metadata.metadata_nodes.contains_key(&root))
            || !image.metadata.metadata_nodes.contains_key(&commit.new_root)
        {
            return Err(StorageError::corrupt(
                "file commit references missing file root",
            ));
        }
    }
    for record in image.metadata.fork_records.values() {
        for root in &record.shard_roots {
            if !image.metadata.metadata_nodes.contains_key(root) {
                return Err(StorageError::corrupt(
                    "fork record references missing metadata root",
                ));
            }
        }
    }
    for record in image.metadata.delete_records.values() {
        for root in &record.shard_roots {
            if !image.metadata.metadata_nodes.contains_key(root) {
                return Err(StorageError::corrupt(
                    "delete record references missing metadata root",
                ));
            }
        }
    }
    validate_timeline_monotonic(&image.metadata.shard_commits)?;
    validate_timeline_monotonic(&image.metadata.keyspace_commits)?;
    validate_timeline_monotonic(&image.metadata.file_commits)?;
    for (node_id, node) in &image.metadata.metadata_nodes {
        if *node_id != node.node_id {
            return Err(StorageError::corrupt("metadata node map key mismatch"));
        }
    }
    for (node_id, node) in &image.storage_nodes.nodes {
        for (segment_id, entry) in &node.segment_catalog.entries {
            if entry.reservation.segment_id != *segment_id {
                return Err(StorageError::corrupt("catalog segment key mismatch"));
            }
            if let Some(receipt) = &entry.receipt {
                if receipt.placement.storage_node != *node_id || receipt.storage_node != *node_id {
                    return Err(StorageError::corrupt(
                        "catalog receipt storage node mismatch",
                    ));
                }
                let commit = receipt.replica_commit();
                let record = node.segment_store.segments.get(segment_id);
                if matches!(
                    entry.state,
                    SegmentLifecycleState::DurablePendingMetadata
                        | SegmentLifecycleState::Referenced
                ) && record.is_none()
                {
                    return Err(StorageError::corrupt(
                        "referenced or durable-pending segment missing segment record",
                    ));
                }
                if let Some(record) = record
                    && record.commit != commit
                {
                    return Err(StorageError::corrupt(
                        "catalog receipt disagrees with segment record",
                    ));
                }
                if matches!(
                    entry.state,
                    SegmentLifecycleState::Reserved | SegmentLifecycleState::Writing
                ) {
                    return Err(StorageError::corrupt(
                        "uncommitted catalog state has a segment receipt",
                    ));
                }
            }
        }
        for (segment_id, record) in &node.segment_store.segments {
            let entry = node
                .segment_catalog
                .entries
                .get(segment_id)
                .ok_or_else(|| StorageError::corrupt("segment record missing catalog entry"))?;
            if entry
                .receipt
                .as_ref()
                .map(SegmentWriteReceipt::replica_commit)
                != Some(record.commit.clone())
            {
                return Err(StorageError::corrupt(
                    "segment record disagrees with catalog entry",
                ));
            }
        }
    }
    Ok(())
}

fn validate_row_native_cursors(image: &DurableStoreImage) -> Result<()> {
    ensure_next_u128_above(
        "next_device_id",
        image.metadata.next_device_id,
        image
            .metadata
            .device_specs
            .keys()
            .chain(image.metadata.device_heads.keys())
            .chain(image.metadata.deleted_device_heads.keys())
            .map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_keyspace_id",
        image.metadata.next_keyspace_id,
        image.metadata.keyspace_heads.keys().map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_file_id",
        image.metadata.next_file_id,
        image
            .metadata
            .keyspace_catalog_shards
            .values()
            .flat_map(|shard| shard.files.keys().map(|id| id.raw())),
    )?;
    ensure_next_u128_above(
        "next_metadata_node_id",
        image.metadata.next_metadata_node_id,
        image.metadata.metadata_nodes.keys().map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_keyspace_root_id",
        image.metadata.next_keyspace_root_id,
        image.metadata.keyspace_roots.keys().map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_keyspace_catalog_shard_id",
        image.metadata.next_keyspace_catalog_shard_id,
        image
            .metadata
            .keyspace_catalog_shards
            .keys()
            .map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_commit_group_id",
        image.metadata.next_commit_group_id,
        image.metadata.commit_groups.keys().map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_checkpoint_id",
        image.metadata.next_checkpoint_id,
        image.metadata.checkpoints.keys().map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_segment_id",
        image.storage_nodes.next_segment_id,
        image
            .storage_nodes
            .nodes
            .values()
            .flat_map(|node| node.segment_catalog.entries.keys())
            .map(|id| id.raw()),
    )?;
    ensure_next_u128_above(
        "next_write_intent",
        image.next_write_intent,
        image
            .storage_nodes
            .nodes
            .values()
            .flat_map(|node| node.segment_catalog.entries.values())
            .map(|entry| entry.intent.write_intent.raw()),
    )?;
    let placement_count = image
        .storage_nodes
        .nodes
        .values()
        .try_fold(0_u64, |sum, node| {
            let entries = u64::try_from(node.segment_catalog.entries.len())
                .map_err(|_| StorageError::corrupt("segment catalog entry count overflows u64"))?;
            sum.checked_add(entries)
                .ok_or_else(|| StorageError::corrupt("segment catalog entry count overflows u64"))
        })?;
    if image.storage_nodes.next_placement_index < placement_count {
        return Err(StorageError::corrupt(
            "next_placement_index is behind persisted catalog rows",
        ));
    }
    let max_commit_seq = image
        .metadata
        .commit_groups
        .values()
        .map(|group| group.commit_seq.raw())
        .chain(
            image
                .metadata
                .shard_commits
                .iter()
                .map(|commit| commit.commit_seq.raw()),
        )
        .chain(
            image
                .metadata
                .keyspace_commits
                .iter()
                .map(|commit| commit.commit_seq.raw()),
        )
        .chain(
            image
                .metadata
                .file_commits
                .iter()
                .map(|commit| commit.commit_seq.raw()),
        )
        .chain(image.metadata.fork_records.keys().map(|seq| seq.raw()))
        .chain(image.metadata.delete_records.keys().map(|seq| seq.raw()))
        .chain(
            image
                .metadata
                .checkpoints
                .values()
                .map(|checkpoint| checkpoint.commit_seq.raw()),
        )
        .max()
        .unwrap_or(0);
    if image.metadata.next_commit_seq <= max_commit_seq {
        return Err(StorageError::corrupt(
            "next_commit_seq is behind persisted rows",
        ));
    }
    Ok(())
}

fn ensure_next_u128_above(
    name: &'static str,
    next: u128,
    values: impl IntoIterator<Item = u128>,
) -> Result<()> {
    let max = values.into_iter().max().unwrap_or(0);
    if next <= max {
        return Err(StorageError::corrupt(format!(
            "{name} is behind persisted rows"
        )));
    }
    Ok(())
}

fn row_native_segment_descriptors(
    image: &DurableStoreImage,
) -> BTreeMap<SegmentId, SegmentDescriptor> {
    let mut out = BTreeMap::new();
    for node in image.storage_nodes.nodes.values() {
        for (segment_id, record) in &node.segment_store.segments {
            out.insert(*segment_id, record.commit.descriptor.clone());
        }
    }
    out
}

fn validate_timeline_monotonic<T: DurableTimelineRow>(rows: &[T]) -> Result<()> {
    let mut last = None;
    for row in rows {
        if let Some(last) = last
            && row.commit_seq_raw() < last
        {
            return Err(StorageError::corrupt("timeline commit sequence regressed"));
        }
        last = Some(row.commit_seq_raw());
    }
    Ok(())
}

fn file_writer_key(keyspace_id: KeyspaceId, file_id: FileId) -> String {
    format!("{}:{}", keyspace_id.raw(), file_id.raw())
}

fn node_data_log_dir(data_dir: &Path, storage_node: StorageNodeId) -> PathBuf {
    data_dir.join(format!("node-{}", storage_node.raw()))
}

fn data_log_path(data_dir: &Path, storage_node: StorageNodeId, log_id: u64) -> PathBuf {
    node_data_log_dir(data_dir, storage_node).join(format!("data-{log_id:06}.log"))
}

fn delete_data_log(data_dir: &Path, log_ref: DurableDataLogRef) -> Result<()> {
    let path = data_log_path(data_dir, log_ref.storage_node, log_ref.log_id);
    match fs::remove_file(path) {
        Ok(()) => sync_dir(&node_data_log_dir(data_dir, log_ref.storage_node)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
    }
}

fn sync_open_data_log(open_log: &mut Option<(DurableDataLogRef, File)>) -> Result<()> {
    if let Some((_, file)) = open_log.take() {
        file.sync_data().map_err(fs_error)?;
    }
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::invalid_argument("path has no parent directory"))?;
    sync_dir(parent)
}

fn sqlite_wal_bytes(metadata_path: &Path) -> Result<u64> {
    let Some(file_name) = metadata_path.file_name().and_then(|name| name.to_str()) else {
        return Err(StorageError::invalid_argument(
            "metadata path has no valid file name",
        ));
    };
    let wal_path = metadata_path.with_file_name(format!("{file_name}-wal"));
    match wal_path.metadata() {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(fs_error(error)),
    }
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)
}

fn encode_data_log_record(segment_id: SegmentId, bytes: &[u8]) -> Result<Vec<u8>> {
    let payload_len = u64::try_from(bytes.len())
        .map_err(|_| StorageError::invalid_argument("data-log payload length overflows u64"))?;
    let mut out = Vec::with_capacity(DATA_LOG_HEADER_LEN + bytes.len());
    out.extend_from_slice(DATA_LOG_MAGIC);
    out.extend_from_slice(&DATA_LOG_VERSION.to_be_bytes());
    out.extend_from_slice(&segment_id.raw().to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&data_log_checksum64(bytes).to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

fn decode_data_log_record(record: &[u8]) -> Result<DataLogSegmentData> {
    if record.len() < DATA_LOG_HEADER_LEN {
        return Err(StorageError::corrupt("data-log record is truncated"));
    }
    if &record[..DATA_LOG_MAGIC.len()] != DATA_LOG_MAGIC {
        return Err(StorageError::corrupt("bad data-log magic"));
    }
    let version_offset = DATA_LOG_MAGIC.len();
    let version = u16::from_be_bytes(
        record[version_offset..version_offset + 2]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log version"))?,
    );
    if version != DATA_LOG_VERSION {
        return Err(StorageError::corrupt("unsupported data-log version"));
    }
    let segment_start = version_offset + 2;
    let segment_id = SegmentId::from_raw(u128::from_be_bytes(
        record[segment_start..segment_start + 16]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log segment id"))?,
    ));
    let payload_len_start = segment_start + 16;
    let payload_len = u64::from_be_bytes(
        record[payload_len_start..payload_len_start + 8]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log payload length"))?,
    );
    let checksum_start = payload_len_start + 8;
    let expected_checksum = u64::from_be_bytes(
        record[checksum_start..checksum_start + 8]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log checksum"))?,
    );
    let payload_len_usize = usize::try_from(payload_len)
        .map_err(|_| StorageError::corrupt("data-log payload length overflows usize"))?;
    let expected_record_len = DATA_LOG_HEADER_LEN
        .checked_add(payload_len_usize)
        .ok_or_else(|| StorageError::corrupt("data-log record length overflow"))?;
    if record.len() != expected_record_len {
        return Err(StorageError::corrupt("data-log record length mismatch"));
    }
    let bytes = record[DATA_LOG_HEADER_LEN..].to_vec();
    if data_log_checksum64(&bytes) != expected_checksum {
        return Err(StorageError::corrupt("data-log checksum mismatch"));
    }
    Ok(DataLogSegmentData { segment_id, bytes })
}

fn current_placements_for_log(
    conn: &Connection,
    log_ref: DurableDataLogRef,
) -> Result<Vec<SegmentPlacementRow>> {
    let segment_placements = node_catalog_table(log_ref.storage_node, "segment_placements")?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT segment_id, data_log_id, record_offset, record_bytes,
                    payload_offset, payload_bytes, checksum
                 FROM {segment_placements}
                 WHERE data_log_id = ?1 AND current = 1
                 ORDER BY record_offset"
        ))
        .map_err(sqlite_error)?;
    let mut rows = stmt
        .query(params![u64_to_i64(log_ref.log_id)?])
        .map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        out.push(decode_node_placement_row(row, log_ref.storage_node).map_err(sqlite_error)?);
    }
    Ok(out)
}

fn decode_node_placement_row(
    row: &rusqlite::Row<'_>,
    storage_node: StorageNodeId,
) -> rusqlite::Result<SegmentPlacementRow> {
    let segment_id: String = row.get(0)?;
    let checksum: String = row.get(6)?;
    Ok(SegmentPlacementRow {
        segment_id: SegmentId::from_raw(parse_u128_key(&segment_id)?),
        storage_node,
        data_log_id: i64_to_u64(row.get(1)?)?,
        record_offset: i64_to_u64(row.get(2)?)?,
        record_bytes: i64_to_u64(row.get(3)?)?,
        payload_offset: i64_to_u64(row.get(4)?)?,
        payload_bytes: i64_to_u64(row.get(5)?)?,
        checksum: parse_u64_key(&checksum)?,
    })
}

fn active_data_log(
    conn: &Connection,
    data_dir: &Path,
    storage_node: StorageNodeId,
) -> Result<DataLogRow> {
    let data_logs = node_catalog_table(storage_node, "data_logs")?;
    if let Some(row) = conn
        .query_row(
            &format!(
                "SELECT log_id, total_bytes, live_bytes, dead_bytes
                 FROM {data_logs}
                 WHERE state = 'active'
                 ORDER BY log_id DESC
                 LIMIT 1"
            ),
            [],
            |row| decode_node_data_log_row(row, storage_node),
        )
        .optional()
        .map_err(sqlite_error)?
    {
        let path = data_log_path(data_dir, row.storage_node, row.log_id);
        let total_bytes = path
            .metadata()
            .map(|metadata| metadata.len().max(row.total_bytes))
            .unwrap_or(row.total_bytes);
        Ok(DataLogRow { total_bytes, ..row })
    } else {
        Ok(DataLogRow {
            storage_node,
            log_id: next_data_log_id(conn, data_dir, storage_node, 0)?,
            total_bytes: 0,
            live_bytes: 0,
            dead_bytes: 0,
        })
    }
}

fn next_data_log(
    conn: &Connection,
    data_dir: &Path,
    storage_node: StorageNodeId,
    previous: u64,
) -> Result<DataLogRow> {
    Ok(DataLogRow {
        storage_node,
        log_id: next_data_log_id(conn, data_dir, storage_node, previous)?,
        total_bytes: 0,
        live_bytes: 0,
        dead_bytes: 0,
    })
}

fn next_data_log_id(
    conn: &Connection,
    data_dir: &Path,
    storage_node: StorageNodeId,
    floor: u64,
) -> Result<u64> {
    let db_max = conn
        .query_row(
            &format!(
                "SELECT COALESCE(MAX(log_id), 0) FROM {}",
                node_catalog_table(storage_node, "data_logs")?
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error)
        .and_then(|value| i64_to_u64(value).map_err(sqlite_error))?;
    let fs_max = fs_data_log_max_id(&node_data_log_dir(data_dir, storage_node))?;
    db_max
        .max(fs_max)
        .max(floor)
        .checked_add(1)
        .ok_or_else(|| StorageError::conflict("data-log id overflow"))
}

fn fs_data_log_max_id(data_dir: &Path) -> Result<u64> {
    let mut max_id = 0;
    if !data_dir.exists() {
        return Ok(max_id);
    }
    for entry in fs::read_dir(data_dir).map_err(fs_error)? {
        let entry = entry.map_err(fs_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(id) = name
            .strip_prefix("data-")
            .and_then(|rest| rest.strip_suffix(".log"))
            .and_then(|raw| raw.parse::<u64>().ok())
        else {
            continue;
        };
        max_id = max_id.max(id);
    }
    Ok(max_id)
}

#[cfg(test)]
fn data_log_rows(node_catalogs: &NodeCatalogs) -> Result<Vec<DataLogRow>> {
    let mut out = Vec::new();
    for storage_node in node_catalogs.storage_nodes() {
        let conn = node_catalogs.lock(storage_node)?;
        let data_logs = node_catalog_table(storage_node, "data_logs")?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT log_id, total_bytes, live_bytes, dead_bytes
                 FROM {data_logs}
                 WHERE state != 'deleted'
                 ORDER BY log_id"
            ))
            .map_err(sqlite_error)?;
        let mut rows = stmt.query([]).map_err(sqlite_error)?;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            out.push(decode_node_data_log_row(row, storage_node).map_err(sqlite_error)?);
        }
    }
    Ok(out)
}

fn compaction_candidates(
    node_catalogs: &NodeCatalogs,
    policy: DurableDataLogPolicy,
) -> Result<Vec<DataLogRow>> {
    let mut out = Vec::new();
    for storage_node in node_catalogs.storage_nodes() {
        let conn = node_catalogs.lock(storage_node)?;
        let data_logs = node_catalog_table(storage_node, "data_logs")?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT log_id, total_bytes, live_bytes, dead_bytes
                 FROM {data_logs}
                 WHERE state = 'sealed'
                 ORDER BY dead_bytes DESC, log_id"
            ))
            .map_err(sqlite_error)?;
        let mut rows = stmt.query([]).map_err(sqlite_error)?;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let row = decode_node_data_log_row(row, storage_node).map_err(sqlite_error)?;
            if row.total_bytes == 0 {
                continue;
            }
            let reclaimable_ratio = row
                .dead_bytes
                .saturating_mul(1_000_000)
                .checked_div(row.total_bytes)
                .unwrap_or(0);
            if row.dead_bytes >= policy.min_reclaimable_bytes
                && reclaimable_ratio >= u64::from(policy.min_reclaimable_ratio_ppm)
            {
                out.push(row);
            }
        }
    }
    out.sort_by_key(|row| {
        (
            std::cmp::Reverse(row.dead_bytes),
            row.storage_node,
            row.log_id,
        )
    });
    Ok(out)
}

fn compaction_candidates_for_refs(
    node_catalogs: &NodeCatalogs,
    policy: DurableDataLogPolicy,
    logs: &[DurableDataLogRef],
) -> Result<Vec<DataLogRow>> {
    let mut out = Vec::new();
    for log_ref in logs {
        let conn = node_catalogs.lock(log_ref.storage_node)?;
        let data_logs = node_catalog_table(log_ref.storage_node, "data_logs")?;
        let row = conn
            .query_row(
                &format!(
                    "SELECT log_id, total_bytes, live_bytes, dead_bytes
                     FROM {data_logs}
                     WHERE log_id = ?1 AND state = 'sealed'"
                ),
                params![u64_to_i64(log_ref.log_id)?],
                |row| decode_node_data_log_row(row, log_ref.storage_node),
            )
            .optional()
            .map_err(sqlite_error)?;
        let Some(row) = row else {
            continue;
        };
        if row.total_bytes == 0 {
            continue;
        }
        let reclaimable_ratio = row
            .dead_bytes
            .saturating_mul(1_000_000)
            .checked_div(row.total_bytes)
            .unwrap_or(0);
        if row.live_bytes == 0
            || (row.dead_bytes >= policy.min_reclaimable_bytes
                && reclaimable_ratio >= u64::from(policy.min_reclaimable_ratio_ppm))
        {
            out.push(row);
        }
    }
    Ok(out)
}

fn decode_node_data_log_row(
    row: &rusqlite::Row<'_>,
    storage_node: StorageNodeId,
) -> rusqlite::Result<DataLogRow> {
    Ok(DataLogRow {
        storage_node,
        log_id: i64_to_u64(row.get(0)?)?,
        total_bytes: i64_to_u64(row.get(1)?)?,
        live_bytes: i64_to_u64(row.get(2)?)?,
        dead_bytes: i64_to_u64(row.get(3)?)?,
    })
}

fn mark_placement_dead(
    tx: &rusqlite::Transaction<'_>,
    placement: &SegmentPlacementRow,
) -> Result<()> {
    let segment_placements = node_catalog_table(placement.storage_node, "segment_placements")?;
    let data_logs = node_catalog_table(placement.storage_node, "data_logs")?;
    tx.execute(
        &format!(
            "UPDATE {segment_placements} SET current = 0 WHERE segment_id = ?1 AND current = 1"
        ),
        params![segment_id_key(placement.segment_id)],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        &format!(
            "UPDATE {data_logs}
             SET live_bytes = MAX(live_bytes - ?2, 0),
                 dead_bytes = dead_bytes + ?2
             WHERE log_id = ?1"
        ),
        params![
            u64_to_i64(placement.data_log_id)?,
            u64_to_i64(placement.payload_bytes)?
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn validate_durable_segment_placement(
    segment_id: SegmentId,
    record: &DurableSegmentRecord,
    placement: &SegmentPlacementRow,
) -> Result<()> {
    if placement.segment_id != segment_id
        || placement.storage_node != record.commit.placement.storage_node
        || placement.payload_bytes != record.commit.placement.bytes
    {
        return Err(StorageError::corrupt(
            "SQLite placement disagrees with durable segment commit",
        ));
    }
    Ok(())
}

fn segment_id_key(segment_id: SegmentId) -> String {
    segment_id.raw().to_string()
}

fn storage_node_key(storage_node: StorageNodeId) -> String {
    storage_node.raw().to_string()
}

fn u64_key(value: u64) -> String {
    value.to_string()
}

fn parse_u128_key(value: &str) -> rusqlite::Result<u128> {
    value.parse::<u128>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn parse_u64_key(value: &str) -> rusqlite::Result<u64> {
    value.parse::<u64>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn u64_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::invalid_argument("u64 value overflows SQLite i64"))
}

fn i64_to_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

#[derive(Clone)]
struct DurableMaintenanceParts {
    local: LocalCoordinator,
    durable: DurableSqliteStore,
    persisted_segments: Arc<Mutex<BTreeSet<SegmentId>>>,
    persist_lock: Arc<Mutex<()>>,
    maintenance_cursor: Arc<Mutex<Option<DurableDataLogRef>>>,
    maintenance_policy: MaintenancePolicy,
}

#[derive(Debug)]
struct MaintenanceWorkerState {
    shutdown: bool,
    notified: bool,
}

struct MaintenanceWorker {
    state: Arc<(Mutex<MaintenanceWorkerState>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MaintenanceWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceWorker").finish_non_exhaustive()
    }
}

impl MaintenanceWorker {
    fn start(parts: DurableMaintenanceParts) -> Result<Arc<Self>> {
        let state = Arc::new((
            Mutex::new(MaintenanceWorkerState {
                shutdown: false,
                notified: false,
            }),
            Condvar::new(),
        ));
        let worker_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .name("toy-cow-maintenance".to_string())
            .spawn(move || maintenance_worker_loop(parts, worker_state))
            .map_err(|error| {
                StorageError::unavailable(format!("failed to start maintenance worker: {error}"))
            })?;
        Ok(Arc::new(Self {
            state,
            handle: Mutex::new(Some(handle)),
        }))
    }

    fn notify(&self) {
        let (lock_state, cvar) = &*self.state;
        if let Ok(mut state) = lock_state.lock() {
            state.notified = true;
            cvar.notify_one();
        }
    }

    fn shutdown(&self) {
        let (lock_state, cvar) = &*self.state;
        if let Ok(mut state) = lock_state.lock() {
            state.shutdown = true;
            state.notified = true;
            cvar.notify_one();
        }
        if let Ok(mut handle) = self.handle.lock()
            && let Some(handle) = handle.take()
        {
            let _ = handle.join();
        }
    }
}

impl Drop for MaintenanceWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn maintenance_worker_loop(
    parts: DurableMaintenanceParts,
    state: Arc<(Mutex<MaintenanceWorkerState>, Condvar)>,
) {
    loop {
        let (lock_state, cvar) = &*state;
        let mut guard = match lock_state.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        while !guard.shutdown && !guard.notified {
            guard = match cvar.wait(guard) {
                Ok(guard) => guard,
                Err(_) => return,
            };
        }
        if guard.shutdown {
            return;
        }
        guard.notified = false;
        drop(guard);

        while let Ok(report) = run_maintenance_tick_parts(&parts, 0, 0) {
            if report.plan.commands.is_empty() {
                break;
            }
        }
    }
}

fn empty_compaction_report() -> DurableCompactionReport {
    DurableCompactionReport {
        deleted_logs: Vec::new(),
        relocated_logs: Vec::new(),
        relocated_segments: Vec::new(),
        bytes_copied: 0,
        bytes_deleted: 0,
    }
}

fn maintenance_tick_data_log_policy(policy: MaintenancePolicy) -> DurableDataLogPolicy {
    let mut data_log_policy = policy.data_log_policy;
    data_log_policy.max_compaction_copy_bytes = data_log_policy
        .max_compaction_copy_bytes
        .min(policy.compaction_copy_budget_per_tick);
    data_log_policy
}

fn run_maintenance_tick_parts(
    parts: &DurableMaintenanceParts,
    recent_write_bytes: u64,
    recent_flushed_write_bytes: u64,
) -> Result<MaintenanceTickReport> {
    let scheduler = MaintenanceScheduler::new(parts.maintenance_policy)?;
    let cursor = *lock(&parts.maintenance_cursor)?;
    let observation = parts.durable.maintenance_observation(
        cursor,
        recent_write_bytes,
        recent_flushed_write_bytes,
        policy_uses_sqlite_wal_pressure(parts.maintenance_policy),
    )?;
    let plan = scheduler.step(&observation);
    parts.local.observability.increment(|counters| {
        counters.maintenance_plans = counters.maintenance_plans.saturating_add(1);
        counters.maintenance_logs_selected = counters
            .maintenance_logs_selected
            .saturating_add(usize_to_u64(plan.diagnostics.selected_logs.len()));
        counters.maintenance_logs_skipped = counters
            .maintenance_logs_skipped
            .saturating_add(usize_to_u64(plan.diagnostics.skipped_logs.len()));
    });
    parts
        .local
        .observability
        .record(StorageEventKind::MaintenancePlanned);
    let mut compaction = empty_compaction_report();
    if !plan.commands.is_empty() {
        let _persist_guard = lock(&parts.persist_lock)?;
        for command in &plan.commands {
            match command {
                MaintenanceCommand::CompactDataLogs { logs } => {
                    let report = parts.durable.compact_data_log_refs(
                        maintenance_tick_data_log_policy(parts.maintenance_policy),
                        logs,
                    )?;
                    compaction.deleted_logs.extend(report.deleted_logs);
                    compaction.relocated_logs.extend(report.relocated_logs);
                    compaction
                        .relocated_segments
                        .extend(report.relocated_segments);
                    compaction.bytes_copied = compaction
                        .bytes_copied
                        .checked_add(report.bytes_copied)
                        .ok_or_else(|| {
                            StorageError::conflict("maintenance bytes_copied overflow")
                        })?;
                    compaction.bytes_deleted = compaction
                        .bytes_deleted
                        .checked_add(report.bytes_deleted)
                        .ok_or_else(|| {
                            StorageError::conflict("maintenance bytes_deleted overflow")
                        })?;
                }
            }
        }
        *lock(&parts.persisted_segments)? = parts.local.segment_ids()?;
    }
    if plan.next_cursor != cursor {
        parts.durable.persist_maintenance_cursor(plan.next_cursor)?;
        *lock(&parts.maintenance_cursor)? = plan.next_cursor;
    }
    parts.local.observability.increment(|counters| {
        counters.maintenance_ticks = counters.maintenance_ticks.saturating_add(1);
        counters.maintenance_bytes_copied = counters
            .maintenance_bytes_copied
            .saturating_add(compaction.bytes_copied);
        counters.maintenance_bytes_deleted = counters
            .maintenance_bytes_deleted
            .saturating_add(compaction.bytes_deleted);
    });
    parts
        .local
        .observability
        .record(StorageEventKind::MaintenanceTicked);
    Ok(MaintenanceTickReport { plan, compaction })
}

fn policy_uses_sqlite_wal_pressure(policy: MaintenancePolicy) -> bool {
    policy.max_sqlite_wal_bytes != u64::MAX
}

/// Durable in-process coordinator using SQLite metadata and node-scoped rolled
/// data logs.
#[derive(Debug, Clone)]
pub struct DurableCoordinator {
    local: LocalCoordinator,
    durable: DurableSqliteStore,
    persisted_segments: Arc<Mutex<BTreeSet<SegmentId>>>,
    persist_lock: Arc<Mutex<()>>,
    maintenance_policy: MaintenancePolicy,
    maintenance_cursor: Arc<Mutex<Option<DurableDataLogRef>>>,
    maintenance_worker: Option<Arc<MaintenanceWorker>>,
}

impl DurableCoordinator {
    pub fn open(root: impl AsRef<Path>, config: LocalStoreConfig) -> Result<Self> {
        Self::open_with_data_log_policy(root, config, DurableDataLogPolicy::default())
    }

    pub fn open_with_data_log_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        policy: DurableDataLogPolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_data_log_policy(
            root,
            config,
            vec![config.storage_node],
            policy,
        )
    }

    pub fn open_with_storage_nodes_and_data_log_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        policy: DurableDataLogPolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_maintenance_policy(
            root,
            config,
            storage_nodes,
            MaintenancePolicy::manual(policy),
        )
    }

    /// Open a one-node durable store with an explicit maintenance policy.
    ///
    /// Manual mode starts no background worker. Opportunistic and always-on
    /// modes remain implementation details below the block/native APIs; callers
    /// still observe the same read/write/fork/snapshot/restore semantics.
    pub fn open_with_maintenance_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        policy: MaintenancePolicy,
    ) -> Result<Self> {
        Self::open_with_storage_nodes_and_maintenance_policy(
            root,
            config,
            vec![config.storage_node],
            policy,
        )
    }

    /// Open a durable store with provider-private storage-node placement.
    ///
    /// The supplied node list seeds a new store. Reopen reconstructs the
    /// registry from SQLite and verifies row-native metadata plus data-log
    /// placements before returning.
    pub fn open_with_storage_nodes_and_maintenance_policy(
        root: impl AsRef<Path>,
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        maintenance_policy: MaintenancePolicy,
    ) -> Result<Self> {
        config.validate()?;
        maintenance_policy.validate()?;
        let storage_nodes = normalize_storage_nodes(config.storage_node, storage_nodes);
        let paths = DurableStorePaths::new(root, config.storage_node)?;
        let durable = DurableSqliteStore::open(
            paths,
            maintenance_policy.data_log_policy,
            storage_nodes.clone(),
        )?;

        let local = durable
            .load(config)?
            .unwrap_or(LocalCoordinator::with_storage_nodes(config, storage_nodes)?);
        let append_session_incarnation = durable.reserve_append_session_incarnation()?;
        local
            .metadata
            .use_append_session_incarnation(append_session_incarnation)?;
        let persisted_segments = local.segment_ids()?;
        let maintenance_cursor = Arc::new(Mutex::new(durable.load_maintenance_cursor()?));

        let mut store = Self {
            local,
            durable,
            persisted_segments: Arc::new(Mutex::new(persisted_segments)),
            persist_lock: Arc::new(Mutex::new(())),
            maintenance_policy,
            maintenance_cursor,
            maintenance_worker: None,
        };
        store.start_maintenance_worker_if_needed()?;
        Ok(store)
    }

    fn start_maintenance_worker_if_needed(&mut self) -> Result<()> {
        if matches!(self.maintenance_policy.mode, MaintenanceMode::AlwaysOn) {
            let worker = MaintenanceWorker::start(DurableMaintenanceParts {
                local: self.local.clone(),
                durable: self.durable.clone(),
                persisted_segments: Arc::clone(&self.persisted_segments),
                persist_lock: Arc::clone(&self.persist_lock),
                maintenance_cursor: Arc::clone(&self.maintenance_cursor),
                maintenance_policy: self.maintenance_policy,
            })?;
            if self.startup_maintenance_has_work()? {
                worker.notify();
            }
            self.maintenance_worker = Some(worker);
        }
        Ok(())
    }

    fn startup_maintenance_has_work(&self) -> Result<bool> {
        self.maintenance_plan_has_commands(self.maintenance_policy)
    }

    fn maintenance_plan_has_commands(&self, policy: MaintenancePolicy) -> Result<bool> {
        let scheduler = MaintenanceScheduler::new(policy)?;
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            0,
            0,
            policy_uses_sqlite_wal_pressure(policy),
        )?;
        Ok(!scheduler.step(&observation).commands.is_empty())
    }

    fn maintenance_parts(&self) -> DurableMaintenanceParts {
        DurableMaintenanceParts {
            local: self.local.clone(),
            durable: self.durable.clone(),
            persisted_segments: Arc::clone(&self.persisted_segments),
            persist_lock: Arc::clone(&self.persist_lock),
            maintenance_cursor: Arc::clone(&self.maintenance_cursor),
            maintenance_policy: self.maintenance_policy,
        }
    }

    /// Return the maintenance policy configured for this store.
    pub fn maintenance_policy(&self) -> MaintenancePolicy {
        self.maintenance_policy
    }

    /// Observe current durable maintenance pressure without mutating state.
    ///
    /// The observation is suitable for diagnostics or deterministic planning.
    /// It is not a lock or lease: executors must tolerate the state changing
    /// before a plan is run.
    pub fn observe_maintenance(&self) -> Result<MaintenanceObservation> {
        let cursor = *lock(&self.maintenance_cursor)?;
        self.durable.maintenance_observation(cursor, 0, 0, true)
    }

    pub fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        let observation = self.observe_maintenance()?;
        self.local
            .diagnostics_snapshot_with_maintenance(Some(&observation))
    }

    pub fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        self.local.drain_events(max)
    }

    /// Plan one maintenance tick from the current observation.
    ///
    /// This performs SQLite reads only. It does not compact logs, update the
    /// fairness cursor, throttle a write, or start background work.
    pub fn plan_maintenance(&self) -> Result<MaintenanceTickPlan> {
        let scheduler = MaintenanceScheduler::new(self.maintenance_policy)?;
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            0,
            0,
            policy_uses_sqlite_wal_pressure(self.maintenance_policy),
        )?;
        let plan = scheduler.step(&observation);
        self.local.observability.increment(|counters| {
            counters.maintenance_plans = counters.maintenance_plans.saturating_add(1);
            counters.maintenance_logs_selected = counters
                .maintenance_logs_selected
                .saturating_add(usize_to_u64(plan.diagnostics.selected_logs.len()));
            counters.maintenance_logs_skipped = counters
                .maintenance_logs_skipped
                .saturating_add(usize_to_u64(plan.diagnostics.skipped_logs.len()));
        });
        self.local
            .observability
            .record(StorageEventKind::MaintenancePlanned);
        Ok(plan)
    }

    /// Run one bounded maintenance tick synchronously.
    ///
    /// Success means completed compaction work and the fairness cursor were
    /// durably published. If the tick fails, already committed maintenance work
    /// remains valid, and acknowledged user data must remain readable.
    pub fn run_maintenance_tick(&self) -> Result<MaintenanceTickReport> {
        run_maintenance_tick_parts(&self.maintenance_parts(), 0, 0)
    }

    /// Stop the optional always-on maintenance worker.
    ///
    /// Manual and opportunistic stores have no worker, so this is a no-op. For
    /// always-on stores, the call waits for an in-flight bounded tick to finish
    /// before returning.
    pub fn shutdown_maintenance(&self) {
        if let Some(worker) = &self.maintenance_worker {
            worker.shutdown();
        }
    }

    fn admit_write(&self, bytes: u64, flushed: bool) -> Result<WriteAdmission> {
        let should_observe = self.maintenance_policy.write_backpressure_enabled
            || matches!(self.maintenance_policy.mode, MaintenanceMode::Opportunistic);
        if !should_observe {
            return Ok(WriteAdmission::Accept);
        }
        let cursor = *lock(&self.maintenance_cursor)?;
        let observation = self.durable.maintenance_observation(
            cursor,
            bytes,
            if flushed { bytes } else { 0 },
            policy_uses_sqlite_wal_pressure(self.maintenance_policy),
        )?;
        let plan = MaintenanceScheduler::new(self.maintenance_policy)?.step(&observation);
        if self.maintenance_policy.write_backpressure_enabled
            && let Some(reason) = plan.admission.unavailable_reason()
        {
            self.local.observability.record_with_update(
                StorageEventKind::CoordinatorWriteUnavailable,
                None,
                None,
                None,
                Some(reason),
                |counters| {
                    counters.coordinator_write_attempts =
                        counters.coordinator_write_attempts.saturating_add(1);
                    counters.coordinator_write_unavailable =
                        counters.coordinator_write_unavailable.saturating_add(1);
                },
            );
            return Err(StorageError::unavailable(reason));
        }
        if matches!(self.maintenance_policy.mode, MaintenanceMode::Opportunistic)
            && !plan.commands.is_empty()
        {
            self.run_maintenance_tick()?;
            return Ok(WriteAdmission::AcceptAndSchedule);
        }
        if plan.commands.is_empty() {
            Ok(WriteAdmission::Accept)
        } else {
            Ok(WriteAdmission::AcceptAndSchedule)
        }
    }

    fn after_successful_write(&self, _admission: WriteAdmission) {
        match self.maintenance_policy.mode {
            MaintenanceMode::Manual | MaintenanceMode::Opportunistic => {}
            MaintenanceMode::AlwaysOn => self.notify_background_maintenance(),
        }
    }

    pub fn metadata(&self) -> Arc<InMemoryMetadataPlane> {
        self.local.metadata()
    }

    pub fn segment_catalog(&self) -> Arc<InMemoryLocalSegmentCatalog> {
        self.local.segment_catalog()
    }

    pub fn segment_store(&self) -> Arc<InMemorySegmentStore> {
        self.local.segment_store()
    }

    #[cfg(test)]
    fn storage_node_ids_for_test(&self) -> Vec<StorageNodeId> {
        self.local.storage_node_ids_for_test()
    }

    pub fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_device(MetadataCreateDeviceRequest::from(request))
                .map(|head| head.device_id)
        })
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        self.local.metadata.device_info(device_id)
    }

    pub fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id)
        })
    }

    pub fn create_file(
        &self,
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    ) -> Result<FileId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .create_file(MetadataCreateFileRequest {
                    keyspace_id,
                    request,
                })
                .map(|head| head.file_id)
        })
    }

    pub fn open_append_session(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendSession> {
        self.local.open_append_session(keyspace_id, file_id)
    }

    pub fn reserve_append(&self, session: &AppendSession, len: u64) -> Result<AppendReservation> {
        self.local.reserve_append(session, len)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.run_and_persist(|local| local.metadata.checkpoint(device_id))
    }

    pub fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        self.run_and_persist(|local| local.metadata.checkpoint_keyspace(keyspace_id))
    }

    pub fn snapshot_keyspace(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        self.run_and_persist(|local| {
            local
                .metadata
                .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                    source,
                    target: request.target,
                    name: request.name,
                })
                .map(|head| head.keyspace_id)
        })
    }

    pub fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId> {
        self.run_and_persist(|local| local.restore_keyspace(source, point))
    }

    pub fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<WriteCommit> {
        let admission = self.admit_write(
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?,
            matches!(durability, crate::api::WriteDurability::Flushed),
        )?;
        let result = self.run_and_maybe_persist(
            matches!(durability, crate::api::WriteDurability::Flushed),
            |local| local.write_device(device_id, offset, data, durability),
        );
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        self.local.read_device(device_id, range, buf)
    }

    pub fn write_zeroes(&self, device_id: DeviceId, offset: u64, len: u64) -> Result<WriteCommit> {
        let admission = self.admit_write(len, true)?;
        let result = self.run_and_persist(|local| local.write_zeroes(device_id, offset, len));
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn discard_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        len: u64,
    ) -> Result<WriteCommit> {
        let admission = self.admit_write(len, true)?;
        let result = self.run_and_persist(|local| local.discard_device(device_id, offset, len));
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn write_file_at(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<FileWriteCommit> {
        let admission = self.admit_write(
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?,
            matches!(durability, crate::api::WriteDurability::Flushed),
        )?;
        let result = self.run_and_maybe_persist(
            matches!(durability, crate::api::WriteDurability::Flushed),
            |local| local.write_file_at(keyspace_id, file_id, offset, data, durability),
        );
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn append_reserved(
        &self,
        reservation: AppendReservation,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<AppendCommit> {
        let admission = self.admit_write(
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?,
            matches!(durability, crate::api::WriteDurability::Flushed),
        )?;
        let result = self.run_and_maybe_persist(
            matches!(durability, crate::api::WriteDurability::Flushed),
            |local| local.append_reserved(reservation, data, durability),
        );
        if result.is_ok() {
            self.after_successful_write(admission);
        }
        result
    }

    pub fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        self.local.read_file(keyspace_id, file_id, range, buf)
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        self.run_and_persist(|local| local.fork_device(source, request))
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        self.run_and_persist(|local| local.restore_device(source, point))
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.run_and_persist(|local| local.delete_device(device_id))
    }

    pub fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        self.persist()?;
        let info = self.local.metadata.device_info(device_id)?;
        Ok(FlushResult {
            device_id,
            durable_through: info.latest_commit,
        })
    }

    pub fn flush_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FlushResult> {
        self.persist()?;
        let head = self.local.metadata.get_file_head(keyspace_id, file_id)?;
        Ok(FlushResult {
            device_id: DeviceId::from_raw(file_id.raw()),
            durable_through: head.latest_commit,
        })
    }

    pub fn compact_data_logs(
        &self,
        policy: DurableDataLogPolicy,
    ) -> Result<DurableCompactionReport> {
        let _persist_guard = lock(&self.persist_lock)?;
        let report = self.durable.compact_data_logs(policy)?;
        *lock(&self.persisted_segments)? = self.local.segment_ids()?;
        Ok(report)
    }

    pub fn run_metadata_custodian(
        &self,
        policy: RetentionPolicy,
    ) -> Result<MetadataCustodianReport> {
        let result = self.local.run_metadata_custodian(policy);
        let changed = result.as_ref().ok().map(|report| {
            report
                .catalog_released_segments
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
        });
        let persist = self.persist_with_catalog_changes(changed.as_ref());
        let report = match (result, persist) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }?;
        self.notify_background_maintenance();
        Ok(report)
    }

    pub fn run_storage_node_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let result = self.local.run_storage_node_custodian(expired_write_intents);
        let changed = result.as_ref().ok().map(|report| {
            report
                .expired_reservations
                .iter()
                .chain(&report.failed_writes)
                .chain(&report.orphan_segments)
                .chain(&report.deleted_released_segments)
                .copied()
                .collect::<BTreeSet<_>>()
        });
        let persist = self.persist_with_catalog_changes(changed.as_ref());
        let report = match (result, persist) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }?;
        self.notify_background_maintenance();
        Ok(report)
    }

    fn notify_background_maintenance(&self) {
        if matches!(self.maintenance_policy.mode, MaintenanceMode::AlwaysOn)
            && let Some(worker) = &self.maintenance_worker
        {
            worker.notify();
        }
    }

    fn run_and_persist<T>(&self, op: impl FnOnce(&LocalCoordinator) -> Result<T>) -> Result<T> {
        self.run_and_maybe_persist(true, op)
    }

    fn run_and_maybe_persist<T>(
        &self,
        persist: bool,
        op: impl FnOnce(&LocalCoordinator) -> Result<T>,
    ) -> Result<T> {
        let result = op(&self.local);
        if !persist {
            return result;
        }
        let persist = self.persist();
        match (result, persist) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(error),
        }
    }

    fn persist(&self) -> Result<()> {
        self.persist_with_catalog_changes(None)
    }

    fn persist_with_catalog_changes(
        &self,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<()> {
        let _persist_guard = lock(&self.persist_lock)?;
        let previous_segments = lock(&self.persisted_segments)?.clone();
        let (image, image_segments, new_segments) =
            self.local.state_image_for_persist(&previous_segments)?;
        let kept_segments = self.durable.persist(
            &image,
            &previous_segments,
            &image_segments,
            new_segments,
            changed_catalog_segments,
        )?;
        *lock(&self.persisted_segments)? = kept_segments;
        Ok(())
    }
}

impl ObservableProvider for LocalCoordinator {
    fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        LocalCoordinator::diagnostics_snapshot(self)
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        LocalCoordinator::drain_events(self, max)
    }
}

impl ObservableProvider for DurableCoordinator {
    fn diagnostics_snapshot(&self) -> Result<DiagnosticsSnapshot> {
        DurableCoordinator::diagnostics_snapshot(self)
    }

    fn drain_events(&self, max: usize) -> Result<Vec<StorageEvent>> {
        DurableCoordinator::drain_events(self, max)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeviceWriteChunk {
    shard_id: crate::id::ShardId,
    old_root: MetadataNodeId,
    range: crate::api::BlockRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentReplacement {
    segment_id: SegmentId,
    segment_base: BlockIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeRangeEdit {
    range: crate::api::BlockRange,
    replacement: Option<SegmentReplacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeEditResult {
    root: MetadataNodeId,
    changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTreeStats {
    pub nodes: usize,
    pub leaves: usize,
    pub max_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMarkReport {
    pub epoch: u64,
    pub roots: Vec<MetadataNodeId>,
    pub metadata_nodes: Vec<MetadataNodeId>,
    pub segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSweepReport {
    pub epoch: u64,
    pub deleted_metadata_nodes: Vec<MetadataNodeId>,
    pub released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCustodianReport {
    pub mark: MetadataMarkReport,
    pub sweep: MetadataSweepReport,
    pub catalog_released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone)]
struct DurableStoreImage {
    config: LocalStoreConfig,
    metadata: MetadataInner,
    storage_nodes: StorageNodeRegistryInner,
    next_write_intent: u128,
    next_extent_id: u128,
}

#[derive(Debug, Clone)]
struct StorageNodeRegistryInner {
    next_segment_id: u128,
    next_placement_index: u64,
    node_order: Vec<StorageNodeId>,
    nodes: BTreeMap<StorageNodeId, StorageNodeInner>,
}

#[derive(Debug, Clone)]
struct StorageNodeInner {
    segment_store: SegmentStoreInner,
    segment_catalog: CatalogInner,
}

fn metadata_referenced_segments(metadata: &MetadataInner) -> BTreeSet<SegmentId> {
    let mut segments = BTreeSet::new();
    for node in metadata.metadata_nodes.values() {
        if let MetadataNodeKind::Leaf { entries } = &node.kind {
            segments.extend(entries.iter().map(|entry| entry.segment_id));
        }
    }
    segments
}

fn reconcile_catalog_references_from_metadata(
    metadata: &MetadataInner,
    storage_nodes: &mut StorageNodeRegistryInner,
) -> BTreeMap<StorageNodeId, BTreeSet<SegmentId>> {
    let mut repaired: BTreeMap<StorageNodeId, BTreeSet<SegmentId>> = BTreeMap::new();
    for segment_id in metadata_referenced_segments(metadata) {
        let Some((storage_node, entry)) =
            storage_nodes
                .nodes
                .iter_mut()
                .find_map(|(storage_node, node)| {
                    node.segment_catalog
                        .entries
                        .get_mut(&segment_id)
                        .map(|entry| (*storage_node, entry))
                })
        else {
            continue;
        };
        if entry.state == SegmentLifecycleState::DurablePendingMetadata {
            entry.state = SegmentLifecycleState::Referenced;
            repaired.entry(storage_node).or_default().insert(segment_id);
        }
    }
    repaired
}

fn image_storage_node_for_catalog_segment(
    image: &DurableStoreImage,
    segment_id: SegmentId,
) -> Option<StorageNodeId> {
    image
        .storage_nodes
        .nodes
        .iter()
        .find_map(|(storage_node, node)| {
            node.segment_catalog
                .entries
                .contains_key(&segment_id)
                .then_some(*storage_node)
        })
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DurableSegmentStoreImage {
    config: LocalStoreConfig,
    next_offset: u64,
    records: BTreeMap<SegmentId, DurableSegmentRecord>,
}

#[cfg(test)]
impl DurableSegmentStoreImage {
    fn from_inner(config: LocalStoreConfig, inner: SegmentStoreInner) -> Self {
        Self {
            config,
            next_offset: inner.next_offset,
            records: inner
                .segments
                .into_iter()
                .map(|(segment_id, record)| {
                    (
                        segment_id,
                        DurableSegmentRecord {
                            synced: record.synced,
                            commit: record.commit,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
struct DurableSegmentRecord {
    synced: bool,
    commit: SegmentReplicaCommit,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DurableMetadataImage {
    config: LocalStoreConfig,
    metadata: MetadataInner,
    next_write_intent: u128,
    next_extent_id: u128,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DurableCatalogImage {
    config: LocalStoreConfig,
    catalog: CatalogInner,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MetadataInner {
    next_device_id: u128,
    next_keyspace_id: u128,
    next_file_id: u128,
    next_metadata_node_id: u128,
    next_keyspace_root_id: u128,
    next_keyspace_catalog_shard_id: u128,
    next_commit_group_id: u128,
    next_commit_seq: u64,
    next_checkpoint_id: u128,
    next_gc_epoch: u64,
    device_heads: BTreeMap<DeviceId, DeviceHead>,
    deleted_device_heads: BTreeMap<DeviceId, DeviceHead>,
    device_specs: BTreeMap<DeviceId, crate::api::DeviceSpec>,
    keyspace_heads: BTreeMap<KeyspaceId, KeyspaceHead>,
    keyspace_roots: BTreeMap<KeyspaceRootId, KeyspaceRoot>,
    keyspace_catalog_shards: BTreeMap<KeyspaceCatalogShardId, KeyspaceCatalogShard>,
    file_writer_epochs: BTreeMap<(KeyspaceId, FileId), WriterEpoch>,
    metadata_nodes: BTreeMap<MetadataNodeId, MetadataNode>,
    commit_groups: BTreeMap<CommitGroupId, CommitGroup>,
    shard_commits: Vec<ShardCommit>,
    keyspace_commits: Vec<KeyspaceCommit>,
    file_commits: Vec<FileCommit>,
    fork_records: BTreeMap<CommitSeq, ForkRecord>,
    delete_records: BTreeMap<CommitSeq, DeleteRecord>,
    checkpoints: BTreeMap<CheckpointId, Checkpoint>,
    metadata_last_mark_epoch: BTreeMap<MetadataNodeId, u64>,
    segment_last_mark_epoch: BTreeMap<SegmentId, u64>,
}

impl MetadataInner {
    fn new() -> Self {
        Self {
            next_device_id: 1,
            next_keyspace_id: 1,
            next_file_id: 1,
            next_metadata_node_id: 1,
            next_keyspace_root_id: 1,
            next_keyspace_catalog_shard_id: 1,
            next_commit_group_id: 1,
            next_commit_seq: 1,
            next_checkpoint_id: 1,
            next_gc_epoch: 1,
            device_heads: BTreeMap::new(),
            deleted_device_heads: BTreeMap::new(),
            device_specs: BTreeMap::new(),
            keyspace_heads: BTreeMap::new(),
            keyspace_roots: BTreeMap::new(),
            keyspace_catalog_shards: BTreeMap::new(),
            file_writer_epochs: BTreeMap::new(),
            metadata_nodes: BTreeMap::new(),
            commit_groups: BTreeMap::new(),
            shard_commits: Vec::new(),
            keyspace_commits: Vec::new(),
            file_commits: Vec::new(),
            fork_records: BTreeMap::new(),
            delete_records: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            metadata_last_mark_epoch: BTreeMap::new(),
            segment_last_mark_epoch: BTreeMap::new(),
        }
    }

    fn alloc_device_id(&mut self) -> DeviceId {
        let id = DeviceId::from_raw(self.next_device_id);
        self.next_device_id += 1;
        id
    }

    fn reserve_device_id_at_least_after(&mut self, device_id: DeviceId) -> Result<()> {
        if device_id.raw() >= self.next_device_id {
            self.next_device_id = device_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("device id overflow"))?;
        }
        Ok(())
    }

    fn alloc_keyspace_id(&mut self) -> KeyspaceId {
        let id = KeyspaceId::from_raw(self.next_keyspace_id);
        self.next_keyspace_id += 1;
        id
    }

    fn reserve_keyspace_id_at_least_after(&mut self, keyspace_id: KeyspaceId) -> Result<()> {
        if keyspace_id.raw() >= self.next_keyspace_id {
            self.next_keyspace_id = keyspace_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("keyspace id overflow"))?;
        }
        Ok(())
    }

    fn alloc_file_id(&mut self) -> FileId {
        let id = FileId::from_raw(self.next_file_id);
        self.next_file_id += 1;
        id
    }

    fn alloc_metadata_node_id(&mut self) -> MetadataNodeId {
        let id = MetadataNodeId::from_raw(self.next_metadata_node_id);
        self.next_metadata_node_id += 1;
        id
    }

    fn alloc_keyspace_root_id(&mut self) -> KeyspaceRootId {
        let id = KeyspaceRootId::from_raw(self.next_keyspace_root_id);
        self.next_keyspace_root_id += 1;
        id
    }

    fn alloc_keyspace_catalog_shard_id(&mut self) -> KeyspaceCatalogShardId {
        let id = KeyspaceCatalogShardId::from_raw(self.next_keyspace_catalog_shard_id);
        self.next_keyspace_catalog_shard_id += 1;
        id
    }

    fn alloc_commit_group_id(&mut self) -> CommitGroupId {
        let id = CommitGroupId::from_raw(self.next_commit_group_id);
        self.next_commit_group_id += 1;
        id
    }

    fn alloc_commit_seq(&mut self) -> Result<CommitSeq> {
        let seq = CommitSeq::from_raw(self.next_commit_seq);
        self.next_commit_seq = self
            .next_commit_seq
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("commit sequence overflow"))?;
        Ok(seq)
    }

    fn alloc_checkpoint_id(&mut self) -> CheckpointId {
        let id = CheckpointId::from_raw(self.next_checkpoint_id);
        self.next_checkpoint_id += 1;
        id
    }

    fn alloc_gc_epoch(&mut self) -> Result<u64> {
        let epoch = self.next_gc_epoch;
        self.next_gc_epoch = self
            .next_gc_epoch
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("GC epoch overflow"))?;
        Ok(epoch)
    }

    fn insert_checkpoint(
        &mut self,
        owner: MappingOwner,
        commit_seq: CommitSeq,
        roots: CheckpointRoots,
    ) -> CheckpointId {
        let checkpoint_id = self.alloc_checkpoint_id();
        let checkpoint = Checkpoint {
            checkpoint_id,
            commit_seq,
            time: LogicalTime::from_raw(commit_seq.raw()),
            owner,
            roots,
        };
        self.checkpoints.insert(checkpoint_id, checkpoint);
        checkpoint_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveAppendSession {
    session_id: AppendSessionId,
    writer_epoch: WriterEpoch,
    base_version: FileVersion,
    reserved_tail: u64,
    next_commit_offset: u64,
    reservations: BTreeMap<AppendReservationId, ActiveAppendReservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveAppendReservation {
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendSessionAllocator {
    incarnation: u64,
    next_counter: u64,
}

impl AppendSessionAllocator {
    fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            next_counter: 1,
        }
    }

    fn next_raw(&mut self) -> Result<u128> {
        let counter = self.next_counter;
        self.next_counter = self
            .next_counter
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("append session counter overflow"))?;
        Ok((u128::from(self.incarnation) << 64) | u128::from(counter))
    }

    fn next_session_id(&mut self) -> Result<AppendSessionId> {
        self.next_raw().map(AppendSessionId::from_raw)
    }

    fn next_reservation_id(&mut self) -> Result<AppendReservationId> {
        self.next_raw().map(AppendReservationId::from_raw)
    }
}

/// In-memory implementation of `MetadataPlane`.
#[derive(Debug)]
pub struct InMemoryMetadataPlane {
    config: LocalStoreConfig,
    inner: Mutex<MetadataInner>,
    active_append_sessions: Mutex<BTreeMap<(KeyspaceId, FileId), ActiveAppendSession>>,
    append_session_allocator: Mutex<AppendSessionAllocator>,
}

impl InMemoryMetadataPlane {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(MetadataInner::new()),
            active_append_sessions: Mutex::new(BTreeMap::new()),
            append_session_allocator: Mutex::new(AppendSessionAllocator::new(0)),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: MetadataInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
            active_append_sessions: Mutex::new(BTreeMap::new()),
            append_session_allocator: Mutex::new(AppendSessionAllocator::new(0)),
        })
    }

    fn state_inner(&self) -> Result<MetadataInner> {
        Ok(lock(&self.inner)?.clone())
    }

    fn use_append_session_incarnation(&self, incarnation: u64) -> Result<()> {
        *lock(&self.append_session_allocator)? = AppendSessionAllocator::new(incarnation);
        lock(&self.active_append_sessions)?.clear();
        Ok(())
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let inner = lock(&self.inner)?;
        let head = inner
            .device_heads
            .get(&device_id)
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        let spec = inner
            .device_specs
            .get(&device_id)
            .ok_or_else(|| StorageError::corrupt("device head exists without spec"))?;

        Ok(DeviceInfo {
            device_id,
            generation: head.generation,
            spec: spec.clone(),
            latest_commit: head.latest_commit,
        })
    }

    pub fn commit_group(&self, commit_group: CommitGroupId) -> Result<CommitGroup> {
        let inner = lock(&self.inner)?;
        inner
            .commit_groups
            .get(&commit_group)
            .cloned()
            .ok_or_else(|| StorageError::not_found("commit_group", commit_group.to_string()))
    }

    pub fn commit_groups_for_seq(&self, commit_seq: CommitSeq) -> Result<Vec<CommitGroup>> {
        let inner = lock(&self.inner)?;
        let mut groups: Vec<_> = inner
            .commit_groups
            .values()
            .filter(|group| group.commit_seq == commit_seq)
            .cloned()
            .collect();
        groups.sort_by_key(|group| group.commit_group.raw());
        Ok(groups)
    }

    pub fn fork_records_for_source(&self, source: DeviceId) -> Result<Vec<ForkRecord>> {
        let inner = lock(&self.inner)?;
        let mut records: Vec<_> = inner
            .fork_records
            .values()
            .filter(|record| record.source == source)
            .cloned()
            .collect();
        records.sort_by_key(|record| record.commit_seq.raw());
        Ok(records)
    }

    pub fn fork_record(&self, commit_seq: CommitSeq) -> Result<ForkRecord> {
        let inner = lock(&self.inner)?;
        inner
            .fork_records
            .get(&commit_seq)
            .cloned()
            .ok_or_else(|| StorageError::not_found("fork_record", commit_seq.to_string()))
    }

    pub fn delete_record(&self, commit_seq: CommitSeq) -> Result<DeleteRecord> {
        let inner = lock(&self.inner)?;
        inner
            .delete_records
            .get(&commit_seq)
            .cloned()
            .ok_or_else(|| StorageError::not_found("delete_record", commit_seq.to_string()))
    }

    pub fn live_device_ids(&self) -> Result<Vec<DeviceId>> {
        let inner = lock(&self.inner)?;
        Ok(inner.device_heads.keys().copied().collect())
    }

    pub fn deleted_device_ids(&self) -> Result<Vec<DeviceId>> {
        let inner = lock(&self.inner)?;
        Ok(inner.deleted_device_heads.keys().copied().collect())
    }

    pub fn shard_commits_for_device(&self, device_id: DeviceId) -> Result<Vec<ShardCommit>> {
        let inner = lock(&self.inner)?;
        Ok(Self::shard_commits_for_device_locked(&inner, device_id))
    }

    pub fn keyspace_commits_for_keyspace(
        &self,
        keyspace_id: KeyspaceId,
    ) -> Result<Vec<KeyspaceCommit>> {
        let inner = lock(&self.inner)?;
        Ok(Self::keyspace_commits_for_keyspace_locked(
            &inner,
            keyspace_id,
        ))
    }

    pub fn file_commits_for_keyspace_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<Vec<FileCommit>> {
        let inner = lock(&self.inner)?;
        let mut commits: Vec<_> = inner
            .file_commits
            .iter()
            .filter(|commit| commit.keyspace_id == keyspace_id && commit.file_id == file_id)
            .cloned()
            .collect();
        commits.sort_by_key(|commit| commit.commit_seq.raw());
        Ok(commits)
    }

    pub fn replay_device_roots(
        &self,
        device_id: DeviceId,
        commit_seq: CommitSeq,
    ) -> Result<Vec<MetadataNodeId>> {
        let inner = lock(&self.inner)?;
        Self::replay_device_roots_locked(&inner, device_id, commit_seq, None)
    }

    pub fn replay_keyspace_root(
        &self,
        keyspace_id: KeyspaceId,
        commit_seq: CommitSeq,
    ) -> Result<KeyspaceRootId> {
        let inner = lock(&self.inner)?;
        Self::replay_keyspace_root_locked(&inner, keyspace_id, commit_seq, None)
    }

    pub fn validate_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        let inner = lock(&self.inner)?;
        match checkpoint.owner {
            MappingOwner::BlockDevice(device_id) => {
                let checkpoint_roots = Self::checkpoint_block_roots(checkpoint)?;
                let replayed = match Self::replay_device_roots_locked(
                    &inner,
                    device_id,
                    checkpoint.commit_seq,
                    Some(checkpoint.checkpoint_id),
                ) {
                    Ok(replayed) => replayed,
                    Err(_) if checkpoint.commit_seq.raw() == 0 => {
                        Self::validate_checkpoint_root_shape_locked(
                            &inner,
                            device_id,
                            &checkpoint_roots,
                            self.config.shard_count,
                        )?;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                if replayed != checkpoint_roots {
                    return Err(StorageError::corrupt(
                        "checkpoint roots do not match replayed timeline",
                    ));
                }
            }
            MappingOwner::NativeKeyspace(keyspace_id) => {
                let checkpoint_root = Self::checkpoint_keyspace_root(checkpoint)?;
                let replayed = match Self::replay_keyspace_root_locked(
                    &inner,
                    keyspace_id,
                    checkpoint.commit_seq,
                    Some(checkpoint.checkpoint_id),
                ) {
                    Ok(replayed) => replayed,
                    Err(_) if checkpoint.commit_seq.raw() == 0 => {
                        if !inner.keyspace_roots.contains_key(&checkpoint_root) {
                            return Err(StorageError::not_found(
                                "keyspace_root",
                                checkpoint_root.to_string(),
                            ));
                        }
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                if replayed != checkpoint_root {
                    return Err(StorageError::corrupt(
                        "keyspace checkpoint root does not match replayed timeline",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn metadata_node_count(&self) -> Result<usize> {
        Ok(lock(&self.inner)?.metadata_nodes.len())
    }

    #[cfg(test)]
    fn file_name_for_test(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<Option<String>> {
        let inner = lock(&self.inner)?;
        Self::file_name_locked(&inner, keyspace_id, file_id)
    }

    #[cfg(test)]
    fn keyspace_root_for_test(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceRoot> {
        let inner = lock(&self.inner)?;
        Self::current_keyspace_root_locked(&inner, keyspace_id)
    }

    #[cfg(test)]
    fn keyspace_catalog_shard_count_for_test(&self) -> Result<usize> {
        Ok(lock(&self.inner)?.keyspace_catalog_shards.len())
    }

    #[cfg(test)]
    fn validate_keyspace_catalog_for_test(&self, keyspace_id: KeyspaceId) -> Result<()> {
        let inner = lock(&self.inner)?;
        let root = Self::current_keyspace_root_locked(&inner, keyspace_id)?;
        Self::validate_keyspace_catalog_root_locked(&inner, &root)
    }

    #[cfg(test)]
    fn clear_keyspace_commits_for_test(&self, keyspace_id: KeyspaceId) -> Result<()> {
        lock(&self.inner)?
            .keyspace_commits
            .retain(|commit| commit.keyspace_id != keyspace_id);
        Ok(())
    }

    #[cfg(test)]
    fn set_next_commit_seq_for_test(&self, next_commit_seq: u64) -> Result<()> {
        lock(&self.inner)?.next_commit_seq = next_commit_seq;
        Ok(())
    }

    pub fn allocate_metadata_node(
        &self,
        covered_range: crate::api::BlockRange,
        kind: MetadataNodeKind,
    ) -> Result<MetadataNode> {
        let mut inner = lock(&self.inner)?;
        Ok(MetadataNode {
            node_id: inner.alloc_metadata_node_id(),
            covered_range,
            kind,
        })
    }

    pub fn open_append_session(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendSession> {
        let session_id = lock(&self.append_session_allocator)?.next_session_id()?;
        let inner = lock(&self.inner)?;
        let head = Self::file_head_locked(&inner, keyspace_id, file_id)?;
        let key = (keyspace_id, file_id);
        let persisted_epoch = inner
            .file_writer_epochs
            .get(&key)
            .copied()
            .unwrap_or_else(|| WriterEpoch::from_raw(0));
        let mut active = lock(&self.active_append_sessions)?;
        let current_epoch = active
            .get(&key)
            .map(|session| session.writer_epoch)
            .unwrap_or(persisted_epoch);
        let writer_epoch = current_epoch
            .raw()
            .checked_add(1)
            .map(WriterEpoch::from_raw)
            .ok_or_else(|| StorageError::conflict("writer epoch overflow"))?;
        let session = AppendSession {
            keyspace_id,
            file_id,
            session_id,
            writer_epoch,
            base_version: head.version,
        };
        active.insert(
            key,
            ActiveAppendSession {
                session_id,
                writer_epoch,
                base_version: head.version,
                reserved_tail: head.size,
                next_commit_offset: head.size,
                reservations: BTreeMap::new(),
            },
        );
        Ok(session)
    }

    pub fn reserve_append(&self, session: &AppendSession, len: u64) -> Result<AppendReservation> {
        if len == 0 {
            return Err(StorageError::invalid_argument(
                "append reservation length must not be zero",
            ));
        }
        let inner = lock(&self.inner)?;
        Self::file_head_locked(&inner, session.keyspace_id, session.file_id)?;
        drop(inner);

        let reservation_id = lock(&self.append_session_allocator)?.next_reservation_id()?;
        let mut active = lock(&self.active_append_sessions)?;
        let current = active
            .get_mut(&(session.keyspace_id, session.file_id))
            .ok_or_else(|| StorageError::conflict("stale append session"))?;
        if current.session_id != session.session_id
            || current.writer_epoch != session.writer_epoch
            || current.base_version != session.base_version
        {
            return Err(StorageError::conflict("stale append session"));
        }
        let offset = current.reserved_tail;
        current.reserved_tail = current
            .reserved_tail
            .checked_add(len)
            .ok_or_else(|| StorageError::invalid_argument("append reservation overflows file"))?;
        let reservation = AppendReservation {
            keyspace_id: session.keyspace_id,
            file_id: session.file_id,
            session_id: session.session_id,
            reservation_id,
            writer_epoch: session.writer_epoch,
            offset,
            len,
        };
        current
            .reservations
            .insert(reservation_id, ActiveAppendReservation { offset, len });
        Ok(reservation)
    }

    pub fn validate_append_reservation(&self, reservation: &AppendReservation) -> Result<()> {
        let inner = lock(&self.inner)?;
        Self::file_head_locked(&inner, reservation.keyspace_id, reservation.file_id)?;
        drop(inner);

        let active = lock(&self.active_append_sessions)?;
        let current = active
            .get(&(reservation.keyspace_id, reservation.file_id))
            .ok_or_else(|| StorageError::conflict("stale append session"))?;
        if current.session_id != reservation.session_id
            || current.writer_epoch != reservation.writer_epoch
        {
            return Err(StorageError::conflict("stale append session"));
        }
        match current.reservations.get(&reservation.reservation_id) {
            Some(active_reservation)
                if active_reservation.offset == reservation.offset
                    && active_reservation.len == reservation.len =>
            {
                Ok(())
            }
            Some(_) | None => Err(StorageError::conflict("stale append reservation")),
        }
    }

    pub fn invalidate_append_sessions_for_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<()> {
        lock(&self.active_append_sessions)?.remove(&(keyspace_id, file_id));
        Ok(())
    }

    fn create_empty_tree(
        inner: &mut MetadataInner,
        config: LocalStoreConfig,
        range: crate::api::BlockRange,
    ) -> Result<MetadataNode> {
        range.validate_non_empty()?;
        if range.blocks.raw() <= config.metadata_leaf_blocks {
            let node = MetadataNode {
                node_id: inner.alloc_metadata_node_id(),
                covered_range: range,
                kind: MetadataNodeKind::Leaf {
                    entries: Vec::new(),
                },
            };
            node.validate(&[])?;
            inner.metadata_nodes.insert(node.node_id, node.clone());
            return Ok(node);
        }

        let child_count = config
            .metadata_fanout
            .min(usize::try_from(range.blocks.raw()).map_err(|_| {
                StorageError::invalid_argument("metadata range block count overflows usize")
            })?);
        let range_start = range.start.raw();
        let range_blocks = range.blocks.raw();
        let child_count_u64 = u64::try_from(child_count)
            .map_err(|_| StorageError::invalid_argument("metadata fanout overflows u64"))?;
        let mut children = Vec::with_capacity(child_count);

        for child_index in 0..child_count {
            let child_index_u64 = u64::try_from(child_index)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let next_child_index_u64 = u64::try_from(child_index + 1)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let child_start_offset = range_blocks
                .checked_mul(child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?
                / child_count_u64;
            let child_end_offset = range_blocks
                .checked_mul(next_child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range end overflows"))?
                / child_count_u64;
            let child_start = range_start
                .checked_add(child_start_offset)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?;
            let child_blocks = child_end_offset - child_start_offset;
            let child_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(child_start),
                BlockCount::from_raw(child_blocks),
            );
            let child = Self::create_empty_tree(inner, config, child_range)?;
            children.push(MetadataChild {
                range: child_range,
                node_id: child.node_id,
            });
        }

        let node = MetadataNode {
            node_id: inner.alloc_metadata_node_id(),
            covered_range: range,
            kind: MetadataNodeKind::Internal { children },
        };
        node.validate(&[])?;
        inner.metadata_nodes.insert(node.node_id, node.clone());
        Ok(node)
    }

    fn next_generation(generation: DeviceGeneration) -> Result<DeviceGeneration> {
        generation
            .raw()
            .checked_add(1)
            .map(DeviceGeneration::from_raw)
            .ok_or_else(|| StorageError::conflict("device generation overflow"))
    }

    fn next_keyspace_generation(generation: KeyspaceGeneration) -> Result<KeyspaceGeneration> {
        generation
            .raw()
            .checked_add(1)
            .map(KeyspaceGeneration::from_raw)
            .ok_or_else(|| StorageError::conflict("keyspace generation overflow"))
    }

    fn next_file_version(version: FileVersion) -> Result<FileVersion> {
        version
            .raw()
            .checked_add(1)
            .map(FileVersion::from_raw)
            .ok_or_else(|| StorageError::conflict("file version overflow"))
    }

    fn checkpoint_block_roots(checkpoint: &Checkpoint) -> Result<Vec<MetadataNodeId>> {
        match &checkpoint.roots {
            CheckpointRoots::BlockShard(roots) => Ok(roots.clone()),
            CheckpointRoots::NativeKeyspace(_) => Err(StorageError::invalid_argument(
                "checkpoint does not contain block shard roots",
            )),
        }
    }

    fn checkpoint_keyspace_root(checkpoint: &Checkpoint) -> Result<KeyspaceRootId> {
        match checkpoint.roots {
            CheckpointRoots::NativeKeyspace(root) => Ok(root),
            CheckpointRoots::BlockShard(_) => Err(StorageError::invalid_argument(
                "checkpoint does not contain native keyspace root",
            )),
        }
    }

    fn keyspace_root_locked(
        inner: &MetadataInner,
        root_id: KeyspaceRootId,
    ) -> Result<KeyspaceRoot> {
        inner
            .keyspace_roots
            .get(&root_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace_root", root_id.to_string()))
    }

    fn keyspace_catalog_shard_locked(
        inner: &MetadataInner,
        shard_id: KeyspaceCatalogShardId,
    ) -> Result<KeyspaceCatalogShard> {
        inner
            .keyspace_catalog_shards
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace_catalog_shard", shard_id.to_string()))
    }

    fn current_keyspace_root_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
    ) -> Result<KeyspaceRoot> {
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        Self::keyspace_root_locked(inner, head.root)
    }

    fn keyspace_catalog_shard_index(file_id: FileId, root: &KeyspaceRoot) -> Result<usize> {
        if root.shard_roots.is_empty() {
            return Err(StorageError::corrupt("keyspace root has no catalog shards"));
        }
        Ok((file_id.raw() % root.shard_roots.len() as u128) as usize)
    }

    fn keyspace_file_in_root_locked(
        inner: &MetadataInner,
        root_id: KeyspaceRootId,
        file_id: FileId,
    ) -> Result<KeyspaceFile> {
        let root = Self::keyspace_root_locked(inner, root_id)?;
        let shard_index = Self::keyspace_catalog_shard_index(file_id, &root)?;
        let shard = Self::keyspace_catalog_shard_locked(inner, root.shard_roots[shard_index])?;
        shard
            .files
            .get(&file_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("file", file_id.to_string()))
    }

    fn file_head_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<FileHead> {
        let root = Self::current_keyspace_root_locked(inner, keyspace_id)?;
        Self::keyspace_file_in_root_locked(inner, root.root_id, file_id).map(|entry| entry.head)
    }

    #[cfg(test)]
    fn file_name_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<Option<String>> {
        let root = Self::current_keyspace_root_locked(inner, keyspace_id)?;
        Self::keyspace_file_in_root_locked(inner, root.root_id, file_id).map(|entry| entry.name)
    }

    fn insert_keyspace_catalog_shard_locked(
        inner: &mut MetadataInner,
        files: BTreeMap<FileId, KeyspaceFile>,
    ) -> Result<KeyspaceCatalogShard> {
        let shard = KeyspaceCatalogShard {
            shard_id: inner.alloc_keyspace_catalog_shard_id(),
            files,
        };
        shard.validate()?;
        inner
            .keyspace_catalog_shards
            .insert(shard.shard_id, shard.clone());
        Ok(shard)
    }

    fn insert_keyspace_root_locked(
        inner: &mut MetadataInner,
        shard_roots: Vec<KeyspaceCatalogShardId>,
        file_count: usize,
    ) -> Result<KeyspaceRoot> {
        let root = KeyspaceRoot {
            root_id: inner.alloc_keyspace_root_id(),
            shard_roots,
            file_count,
        };
        root.validate()?;
        inner.keyspace_roots.insert(root.root_id, root.clone());
        Ok(root)
    }

    fn insert_empty_keyspace_root_locked(inner: &mut MetadataInner) -> Result<KeyspaceRoot> {
        let mut shard_roots = Vec::with_capacity(KEYSPACE_CATALOG_SHARD_COUNT);
        for _ in 0..KEYSPACE_CATALOG_SHARD_COUNT {
            let shard = Self::insert_keyspace_catalog_shard_locked(inner, BTreeMap::new())?;
            shard_roots.push(shard.shard_id);
        }
        Self::insert_keyspace_root_locked(inner, shard_roots, 0)
    }

    #[cfg(test)]
    fn validate_keyspace_catalog_root_locked(
        inner: &MetadataInner,
        root: &KeyspaceRoot,
    ) -> Result<()> {
        root.validate()?;
        if root.shard_roots.len() != KEYSPACE_CATALOG_SHARD_COUNT {
            return Err(StorageError::corrupt(
                "keyspace root has unexpected catalog shard count",
            ));
        }

        let mut unique_shards = BTreeSet::new();
        let mut actual_file_count = 0usize;
        for (shard_index, shard_id) in root.shard_roots.iter().copied().enumerate() {
            if !unique_shards.insert(shard_id) {
                return Err(StorageError::corrupt(
                    "keyspace root contains duplicate catalog shard",
                ));
            }
            let shard = Self::keyspace_catalog_shard_locked(inner, shard_id)?;
            shard.validate()?;
            actual_file_count = actual_file_count
                .checked_add(shard.files.len())
                .ok_or_else(|| StorageError::corrupt("keyspace file count overflows usize"))?;
            for file_id in shard.files.keys().copied() {
                if Self::keyspace_catalog_shard_index(file_id, root)? != shard_index {
                    return Err(StorageError::corrupt(
                        "keyspace file is stored in the wrong catalog shard",
                    ));
                }
            }
        }
        if actual_file_count != root.file_count {
            return Err(StorageError::corrupt(
                "keyspace root file_count does not match catalog shards",
            ));
        }

        Ok(())
    }

    fn update_keyspace_file_locked(
        inner: &mut MetadataInner,
        root: &KeyspaceRoot,
        file_id: FileId,
        entry: KeyspaceFile,
    ) -> Result<KeyspaceRoot> {
        let shard_index = Self::keyspace_catalog_shard_index(file_id, root)?;
        let shard_id = root.shard_roots[shard_index];
        let shard = Self::keyspace_catalog_shard_locked(inner, shard_id)?;
        let mut files = shard.files.clone();
        let replaced = files.insert(file_id, entry).is_some();
        let new_shard = Self::insert_keyspace_catalog_shard_locked(inner, files)?;
        let mut shard_roots = root.shard_roots.clone();
        shard_roots[shard_index] = new_shard.shard_id;
        let file_count = if replaced {
            root.file_count
        } else {
            root.file_count
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("keyspace file count overflow"))?
        };
        Self::insert_keyspace_root_locked(inner, shard_roots, file_count)
    }

    fn collect_keyspace_metadata_roots_locked(
        inner: &MetadataInner,
        root_id: KeyspaceRootId,
        out: &mut Vec<MetadataNodeId>,
    ) -> Result<()> {
        let root = Self::keyspace_root_locked(inner, root_id)?;
        for shard_id in root.shard_roots {
            let shard = Self::keyspace_catalog_shard_locked(inner, shard_id)?;
            out.extend(shard.files.values().map(|entry| entry.head.root));
        }
        Ok(())
    }

    fn shard_commits_for_device_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
    ) -> Vec<ShardCommit> {
        let mut commits: Vec<_> = inner
            .shard_commits
            .iter()
            .filter(|commit| commit.device_id == device_id)
            .cloned()
            .collect();
        commits.sort_by_key(|commit| (commit.commit_seq.raw(), commit.shard_id.raw()));
        commits
    }

    fn latest_device_checkpoint_at_or_before_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Option<Checkpoint> {
        inner
            .checkpoints
            .values()
            .filter(|checkpoint| {
                checkpoint.owner == MappingOwner::BlockDevice(device_id)
                    && checkpoint.commit_seq.raw() <= commit_seq.raw()
                    && Some(checkpoint.checkpoint_id) != excluded_checkpoint
            })
            .max_by_key(|checkpoint| checkpoint.commit_seq.raw())
            .cloned()
    }

    fn replay_device_roots_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Result<Vec<MetadataNodeId>> {
        let checkpoint = Self::latest_device_checkpoint_at_or_before_locked(
            inner,
            device_id,
            commit_seq,
            excluded_checkpoint,
        )
        .ok_or_else(|| StorageError::not_found("checkpoint", device_id.to_string()))?;
        let mut roots = Self::checkpoint_block_roots(&checkpoint)?;

        for commit in Self::shard_commits_for_device_locked(inner, device_id)
            .into_iter()
            .filter(|commit| {
                commit.commit_seq.raw() > checkpoint.commit_seq.raw()
                    && commit.commit_seq.raw() <= commit_seq.raw()
            })
        {
            let shard = usize::try_from(commit.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            if shard >= roots.len() {
                return Err(StorageError::corrupt(
                    "shard commit references shard outside root set",
                ));
            }
            if roots[shard] != commit.old_root {
                return Err(StorageError::corrupt(
                    "shard commit old_root does not match replay state",
                ));
            }
            roots[shard] = commit.new_root;
        }

        Ok(roots)
    }

    fn keyspace_commits_for_keyspace_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
    ) -> Vec<KeyspaceCommit> {
        let mut commits: Vec<_> = inner
            .keyspace_commits
            .iter()
            .filter(|commit| commit.keyspace_id == keyspace_id)
            .cloned()
            .collect();
        commits.sort_by_key(|commit| commit.commit_seq.raw());
        commits
    }

    fn latest_keyspace_checkpoint_at_or_before_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Option<Checkpoint> {
        inner
            .checkpoints
            .values()
            .filter(|checkpoint| {
                checkpoint.owner == MappingOwner::NativeKeyspace(keyspace_id)
                    && checkpoint.commit_seq.raw() <= commit_seq.raw()
                    && Some(checkpoint.checkpoint_id) != excluded_checkpoint
            })
            .max_by_key(|checkpoint| checkpoint.commit_seq.raw())
            .cloned()
    }

    fn replay_keyspace_root_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        commit_seq: CommitSeq,
        excluded_checkpoint: Option<CheckpointId>,
    ) -> Result<KeyspaceRootId> {
        let checkpoint = Self::latest_keyspace_checkpoint_at_or_before_locked(
            inner,
            keyspace_id,
            commit_seq,
            excluded_checkpoint,
        )
        .ok_or_else(|| StorageError::not_found("checkpoint", keyspace_id.to_string()))?;
        let mut root = Self::checkpoint_keyspace_root(&checkpoint)?;

        for commit in Self::keyspace_commits_for_keyspace_locked(inner, keyspace_id)
            .into_iter()
            .filter(|commit| {
                commit.commit_seq.raw() > checkpoint.commit_seq.raw()
                    && commit.commit_seq.raw() <= commit_seq.raw()
            })
        {
            if root != commit.old_root {
                return Err(StorageError::corrupt(
                    "keyspace commit old_root does not match replay state",
                ));
            }
            root = commit.new_root;
        }

        Ok(root)
    }

    fn validate_checkpoint_root_shape_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        shard_roots: &[MetadataNodeId],
        expected_shard_count: usize,
    ) -> Result<()> {
        if !inner.device_specs.contains_key(&device_id) {
            return Err(StorageError::not_found("device", device_id.to_string()));
        }
        if shard_roots.len() != expected_shard_count {
            return Err(StorageError::corrupt(
                "checkpoint shard root count does not match device layout",
            ));
        }
        for root in shard_roots {
            if !inner.metadata_nodes.contains_key(root) {
                return Err(StorageError::not_found("metadata_node", root.to_string()));
            }
        }
        Ok(())
    }

    fn target_commit_for_restore_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        point: RestorePoint,
    ) -> Result<CommitSeq> {
        match point {
            RestorePoint::Commit(commit_seq) => {
                if Self::device_timeline_contains_commit_locked(inner, device_id, commit_seq) {
                    Ok(commit_seq)
                } else {
                    Err(StorageError::not_found("commit", commit_seq.to_string()))
                }
            }
            RestorePoint::Checkpoint(checkpoint_id) => {
                let checkpoint = inner.checkpoints.get(&checkpoint_id).ok_or_else(|| {
                    StorageError::not_found("checkpoint", checkpoint_id.to_string())
                })?;
                if checkpoint.owner != MappingOwner::BlockDevice(device_id) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source device",
                    ));
                }
                Ok(checkpoint.commit_seq)
            }
            RestorePoint::Time(time) => {
                let mut candidates: Vec<(CommitSeq, bool)> = inner
                    .checkpoints
                    .values()
                    .filter_map(|checkpoint| {
                        (checkpoint.owner == MappingOwner::BlockDevice(device_id)
                            && checkpoint.time.raw() <= time.raw())
                        .then_some((checkpoint.commit_seq, false))
                    })
                    .collect();
                candidates.extend(inner.shard_commits.iter().filter_map(|commit| {
                    (commit.device_id == device_id && commit.time.raw() <= time.raw())
                        .then_some((commit.commit_seq, false))
                }));
                candidates.extend(inner.delete_records.values().filter_map(|record| {
                    (record.device_id == device_id && record.time.raw() <= time.raw())
                        .then_some((record.commit_seq, true))
                }));
                let (commit_seq, is_delete) = candidates
                    .into_iter()
                    .max_by_key(|(seq, is_delete)| (seq.raw(), *is_delete))
                    .ok_or_else(|| StorageError::not_found("restore_time", time.to_string()))?;
                if is_delete {
                    return Err(StorageError::not_found(
                        "restore_time",
                        format!("{time} is after device deletion"),
                    ));
                }
                Ok(commit_seq)
            }
        }
    }

    fn device_timeline_contains_commit_locked(
        inner: &MetadataInner,
        device_id: DeviceId,
        commit_seq: CommitSeq,
    ) -> bool {
        inner.checkpoints.values().any(|checkpoint| {
            checkpoint.owner == MappingOwner::BlockDevice(device_id)
                && checkpoint.commit_seq == commit_seq
        }) || inner
            .shard_commits
            .iter()
            .any(|commit| commit.device_id == device_id && commit.commit_seq == commit_seq)
    }

    fn target_commit_for_keyspace_restore_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        point: RestorePoint,
    ) -> Result<CommitSeq> {
        match point {
            RestorePoint::Commit(commit_seq) => {
                if Self::keyspace_timeline_contains_commit_locked(inner, keyspace_id, commit_seq) {
                    Ok(commit_seq)
                } else {
                    Err(StorageError::not_found("commit", commit_seq.to_string()))
                }
            }
            RestorePoint::Checkpoint(checkpoint_id) => {
                let checkpoint = inner.checkpoints.get(&checkpoint_id).ok_or_else(|| {
                    StorageError::not_found("checkpoint", checkpoint_id.to_string())
                })?;
                if checkpoint.owner != MappingOwner::NativeKeyspace(keyspace_id) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source keyspace",
                    ));
                }
                Ok(checkpoint.commit_seq)
            }
            RestorePoint::Time(time) => {
                let mut candidates: Vec<CommitSeq> = inner
                    .checkpoints
                    .values()
                    .filter_map(|checkpoint| {
                        (checkpoint.owner == MappingOwner::NativeKeyspace(keyspace_id)
                            && checkpoint.time.raw() <= time.raw())
                        .then_some(checkpoint.commit_seq)
                    })
                    .collect();
                candidates.extend(inner.keyspace_commits.iter().filter_map(|commit| {
                    (commit.keyspace_id == keyspace_id && commit.time.raw() <= time.raw())
                        .then_some(commit.commit_seq)
                }));
                candidates
                    .into_iter()
                    .max_by_key(|seq| seq.raw())
                    .ok_or_else(|| StorageError::not_found("restore_time", time.to_string()))
            }
        }
    }

    fn keyspace_timeline_contains_commit_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        commit_seq: CommitSeq,
    ) -> bool {
        inner.checkpoints.values().any(|checkpoint| {
            checkpoint.owner == MappingOwner::NativeKeyspace(keyspace_id)
                && checkpoint.commit_seq == commit_seq
        }) || inner
            .keyspace_commits
            .iter()
            .any(|commit| commit.keyspace_id == keyspace_id && commit.commit_seq == commit_seq)
    }

    fn roots_for_gc_locked(
        inner: &MetadataInner,
        policy: RetentionPolicy,
    ) -> Result<Vec<MetadataNodeId>> {
        let mut roots = Vec::new();
        for head in inner.device_heads.values() {
            roots.extend(head.shard_roots.iter().copied());
        }
        for head in inner.keyspace_heads.values() {
            Self::collect_keyspace_metadata_roots_locked(inner, head.root, &mut roots)?;
        }
        for checkpoint in inner.checkpoints.values() {
            match checkpoint.owner {
                MappingOwner::BlockDevice(device_id) => {
                    if Self::owner_has_retained_pitr_locked(
                        inner,
                        &policy,
                        MappingOwner::BlockDevice(device_id),
                    ) && Self::retain_checkpoint_for_pitr_locked(inner, &policy, checkpoint)
                    {
                        roots.extend(Self::checkpoint_block_roots(checkpoint)?);
                    }
                }
                MappingOwner::NativeKeyspace(_) => {
                    if Self::owner_has_retained_pitr_locked(inner, &policy, checkpoint.owner)
                        && Self::retain_checkpoint_for_pitr_locked(inner, &policy, checkpoint)
                    {
                        let root = Self::checkpoint_keyspace_root(checkpoint)?;
                        Self::collect_keyspace_metadata_roots_locked(inner, root, &mut roots)?;
                    }
                }
            }
        }
        for commit in &inner.shard_commits {
            if Self::retain_shard_commit_for_pitr_locked(inner, &policy, commit) {
                roots.push(commit.new_root);
            }
        }
        for commit in &inner.keyspace_commits {
            if Self::retain_keyspace_commit_for_pitr_locked(inner, &policy, commit) {
                Self::collect_keyspace_metadata_roots_locked(inner, commit.new_root, &mut roots)?;
            }
        }
        for (device_id, head) in &inner.deleted_device_heads {
            if Self::retain_deleted_device_locked(inner, &policy, *device_id) {
                roots.extend(head.shard_roots.iter().copied());
            }
        }
        for record in inner.delete_records.values() {
            if Self::retain_deleted_device_locked(inner, &policy, record.device_id) {
                roots.extend(record.shard_roots.iter().copied());
            }
        }
        roots.sort();
        roots.dedup();
        Ok(roots)
    }

    fn current_commit_seq_locked(inner: &MetadataInner) -> CommitSeq {
        CommitSeq::from_raw(inner.next_commit_seq.saturating_sub(1))
    }

    fn pitr_retention_floor_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> Option<CommitSeq> {
        (policy.pitr_grace_commits > 0).then(|| {
            let current = Self::current_commit_seq_locked(inner).raw();
            let retained_span = policy.pitr_grace_commits.saturating_sub(1);
            CommitSeq::from_raw(current.saturating_sub(retained_span))
        })
    }

    fn retain_pitr_commit_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        commit_seq: CommitSeq,
    ) -> bool {
        policy.pitr_grace_commits > 0
            && Self::current_commit_seq_locked(inner)
                .raw()
                .saturating_sub(commit_seq.raw())
                < policy.pitr_grace_commits
    }

    fn owner_has_retained_pitr_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        owner: MappingOwner,
    ) -> bool {
        if policy.pitr_grace_commits == 0 {
            return false;
        }

        match owner {
            MappingOwner::BlockDevice(device_id) => {
                inner.device_heads.contains_key(&device_id)
                    || Self::retain_deleted_device_locked(inner, policy, device_id)
            }
            MappingOwner::NativeKeyspace(keyspace_id) => {
                inner.keyspace_heads.contains_key(&keyspace_id)
            }
        }
    }

    fn block_pitr_anchor_targets_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> BTreeMap<DeviceId, CommitSeq> {
        let Some(floor) = Self::pitr_retention_floor_locked(inner, policy) else {
            return BTreeMap::new();
        };

        let mut targets = BTreeMap::new();
        for (device_id, head) in &inner.device_heads {
            let anchor = floor.raw().min(head.latest_commit.raw());
            targets.insert(*device_id, CommitSeq::from_raw(anchor));
        }
        for (device_id, head) in &inner.deleted_device_heads {
            if Self::retain_deleted_device_locked(inner, policy, *device_id) {
                let anchor = floor.raw().min(head.latest_commit.raw());
                targets.insert(*device_id, CommitSeq::from_raw(anchor));
            }
        }
        targets
    }

    fn keyspace_pitr_anchor_targets_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> BTreeMap<KeyspaceId, CommitSeq> {
        let Some(floor) = Self::pitr_retention_floor_locked(inner, policy) else {
            return BTreeMap::new();
        };

        let mut targets = BTreeMap::new();
        for (keyspace_id, head) in &inner.keyspace_heads {
            let anchor = floor.raw().min(head.latest_commit.raw());
            targets.insert(*keyspace_id, CommitSeq::from_raw(anchor));
        }
        targets
    }

    fn checkpoint_exists_locked(
        inner: &MetadataInner,
        owner: MappingOwner,
        commit_seq: CommitSeq,
    ) -> bool {
        inner
            .checkpoints
            .values()
            .any(|checkpoint| checkpoint.owner == owner && checkpoint.commit_seq == commit_seq)
    }

    fn ensure_pitr_anchor_checkpoints_locked(
        inner: &mut MetadataInner,
        policy: &RetentionPolicy,
    ) -> Result<()> {
        let targets = Self::block_pitr_anchor_targets_locked(inner, policy);
        for (device_id, anchor_seq) in targets {
            let owner = MappingOwner::BlockDevice(device_id);
            if Self::checkpoint_exists_locked(inner, owner, anchor_seq) {
                continue;
            }
            let roots = Self::replay_device_roots_locked(inner, device_id, anchor_seq, None)?;
            inner.insert_checkpoint(owner, anchor_seq, CheckpointRoots::BlockShard(roots));
        }

        let targets = Self::keyspace_pitr_anchor_targets_locked(inner, policy);
        for (keyspace_id, anchor_seq) in targets {
            let owner = MappingOwner::NativeKeyspace(keyspace_id);
            if Self::checkpoint_exists_locked(inner, owner, anchor_seq) {
                continue;
            }
            let root = Self::replay_keyspace_root_locked(inner, keyspace_id, anchor_seq, None)?;
            inner.insert_checkpoint(owner, anchor_seq, CheckpointRoots::NativeKeyspace(root));
        }
        Ok(())
    }

    fn latest_checkpoint_at_or_before_floor_locked(
        inner: &MetadataInner,
        owner: MappingOwner,
        floor: CommitSeq,
    ) -> Option<CheckpointId> {
        inner
            .checkpoints
            .values()
            .filter(|checkpoint| {
                checkpoint.owner == owner && checkpoint.commit_seq.raw() <= floor.raw()
            })
            .max_by_key(|checkpoint| checkpoint.commit_seq.raw())
            .map(|checkpoint| checkpoint.checkpoint_id)
    }

    fn retained_pitr_anchor_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        owner: MappingOwner,
    ) -> Option<Checkpoint> {
        let floor = Self::pitr_retention_floor_locked(inner, policy)?;
        let checkpoint_id = Self::latest_checkpoint_at_or_before_floor_locked(inner, owner, floor)?;
        inner.checkpoints.get(&checkpoint_id).cloned()
    }

    fn retain_checkpoint_for_pitr_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        checkpoint: &Checkpoint,
    ) -> bool {
        Self::retain_pitr_commit_locked(inner, policy, checkpoint.commit_seq)
            || Self::retained_pitr_anchor_locked(inner, policy, checkpoint.owner)
                .is_some_and(|anchor| anchor.checkpoint_id == checkpoint.checkpoint_id)
    }

    fn retain_shard_commit_for_pitr_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        commit: &ShardCommit,
    ) -> bool {
        let owner = MappingOwner::BlockDevice(commit.device_id);
        if !Self::owner_has_retained_pitr_locked(inner, policy, owner) {
            return false;
        }
        let Some(anchor) = Self::retained_pitr_anchor_locked(inner, policy, owner) else {
            return Self::retain_pitr_commit_locked(inner, policy, commit.commit_seq);
        };
        commit.commit_seq.raw() > anchor.commit_seq.raw()
            || Self::retain_pitr_commit_locked(inner, policy, commit.commit_seq)
    }

    fn retain_keyspace_commit_for_pitr_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        commit: &KeyspaceCommit,
    ) -> bool {
        let owner = MappingOwner::NativeKeyspace(commit.keyspace_id);
        if !Self::owner_has_retained_pitr_locked(inner, policy, owner) {
            return false;
        }
        let Some(anchor) = Self::retained_pitr_anchor_locked(inner, policy, owner) else {
            return Self::retain_pitr_commit_locked(inner, policy, commit.commit_seq);
        };
        commit.commit_seq.raw() > anchor.commit_seq.raw()
            || Self::retain_pitr_commit_locked(inner, policy, commit.commit_seq)
    }

    fn retained_checkpoint_ids_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> BTreeSet<CheckpointId> {
        inner
            .checkpoints
            .values()
            .filter(|checkpoint| match checkpoint.owner {
                MappingOwner::BlockDevice(device_id) => {
                    Self::owner_has_retained_pitr_locked(
                        inner,
                        policy,
                        MappingOwner::BlockDevice(device_id),
                    ) && Self::retain_checkpoint_for_pitr_locked(inner, policy, checkpoint)
                }
                MappingOwner::NativeKeyspace(_) => {
                    Self::owner_has_retained_pitr_locked(inner, policy, checkpoint.owner)
                        && Self::retain_checkpoint_for_pitr_locked(inner, policy, checkpoint)
                }
            })
            .map(|checkpoint| checkpoint.checkpoint_id)
            .collect()
    }

    fn retained_shard_commit_cutoffs_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> BTreeMap<DeviceId, CommitSeq> {
        let mut cutoffs = BTreeMap::new();
        for device_id in Self::block_pitr_anchor_targets_locked(inner, policy).keys() {
            let owner = MappingOwner::BlockDevice(*device_id);
            if let Some(anchor) = Self::retained_pitr_anchor_locked(inner, policy, owner) {
                cutoffs.insert(*device_id, anchor.commit_seq);
            }
        }
        cutoffs
    }

    fn retained_keyspace_commit_cutoffs_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
    ) -> BTreeMap<KeyspaceId, CommitSeq> {
        let mut cutoffs = BTreeMap::new();
        for keyspace_id in Self::keyspace_pitr_anchor_targets_locked(inner, policy).keys() {
            let owner = MappingOwner::NativeKeyspace(*keyspace_id);
            if let Some(anchor) = Self::retained_pitr_anchor_locked(inner, policy, owner) {
                cutoffs.insert(*keyspace_id, anchor.commit_seq);
            }
        }
        cutoffs
    }

    fn retain_deleted_device_locked(
        inner: &MetadataInner,
        policy: &RetentionPolicy,
        device_id: DeviceId,
    ) -> bool {
        if policy.retain_deleted_devices {
            return inner.deleted_device_heads.contains_key(&device_id);
        }

        let Some(head) = inner.deleted_device_heads.get(&device_id) else {
            return false;
        };
        let current = Self::current_commit_seq_locked(inner);
        current.raw().saturating_sub(head.latest_commit.raw()) < policy.deleted_device_grace_commits
    }

    fn collect_node_segments(node: &MetadataNode, out: &mut BTreeSet<SegmentId>) {
        if let MetadataNodeKind::Leaf { entries } = &node.kind {
            for entry in entries {
                out.insert(entry.segment_id);
            }
        }
    }

    fn collect_all_segments_locked(inner: &MetadataInner) -> BTreeSet<SegmentId> {
        let mut segments = BTreeSet::new();
        for node in inner.metadata_nodes.values() {
            Self::collect_node_segments(node, &mut segments);
        }
        segments
    }

    fn collect_reachable_locked(
        inner: &MetadataInner,
        roots: &[MetadataNodeId],
    ) -> Result<(BTreeSet<MetadataNodeId>, BTreeSet<SegmentId>)> {
        let mut nodes = BTreeSet::new();
        let mut segments = BTreeSet::new();
        let mut stack: Vec<_> = roots.iter().copied().rev().collect();

        while let Some(node_id) = stack.pop() {
            if !nodes.insert(node_id) {
                continue;
            }
            let node = inner
                .metadata_nodes
                .get(&node_id)
                .ok_or_else(|| StorageError::not_found("metadata_node", node_id.to_string()))?;
            match &node.kind {
                MetadataNodeKind::Internal { children } => {
                    for child in children.iter().rev() {
                        stack.push(child.node_id);
                    }
                }
                MetadataNodeKind::Leaf { entries } => {
                    for entry in entries {
                        segments.insert(entry.segment_id);
                    }
                }
            }
        }

        Ok((nodes, segments))
    }

    pub fn mark_reachable_for_gc(&self, policy: RetentionPolicy) -> Result<MetadataMarkReport> {
        let mut inner = lock(&self.inner)?;
        Self::ensure_pitr_anchor_checkpoints_locked(&mut inner, &policy)?;
        let epoch = inner.alloc_gc_epoch()?;
        let roots = Self::roots_for_gc_locked(&inner, policy.clone())?;
        let (nodes, segments) = Self::collect_reachable_locked(&inner, &roots)?;

        for node_id in &nodes {
            inner.metadata_last_mark_epoch.insert(*node_id, epoch);
        }
        for segment_id in &segments {
            inner.segment_last_mark_epoch.insert(*segment_id, epoch);
        }

        Ok(MetadataMarkReport {
            epoch,
            roots,
            metadata_nodes: nodes.into_iter().collect(),
            segments: segments.into_iter().collect(),
        })
    }

    pub fn sweep_unmarked_after_mark(
        &self,
        policy: RetentionPolicy,
        epoch: u64,
    ) -> Result<MetadataSweepReport> {
        if epoch == 0 {
            return Err(StorageError::invalid_argument(
                "GC epoch must be greater than zero",
            ));
        }

        let mut inner = lock(&self.inner)?;
        if epoch >= inner.next_gc_epoch {
            return Err(StorageError::invalid_argument("unknown GC epoch"));
        }
        Self::ensure_pitr_anchor_checkpoints_locked(&mut inner, &policy)?;

        let roots = Self::roots_for_gc_locked(&inner, policy.clone())?;
        let (currently_reachable_nodes, currently_reachable_segments) =
            Self::collect_reachable_locked(&inner, &roots)?;
        let all_segments = Self::collect_all_segments_locked(&inner);
        let mut deleted_metadata_nodes = Vec::new();

        let candidate_nodes: Vec<_> = inner
            .metadata_nodes
            .keys()
            .copied()
            .filter(|node_id| {
                inner.metadata_last_mark_epoch.get(node_id).copied() != Some(epoch)
                    && !currently_reachable_nodes.contains(node_id)
            })
            .collect();
        for node_id in candidate_nodes {
            inner.metadata_nodes.remove(&node_id);
            inner.metadata_last_mark_epoch.remove(&node_id);
            deleted_metadata_nodes.push(node_id);
        }

        let mut released_segments: Vec<_> = all_segments
            .into_iter()
            .filter(|segment_id| {
                inner.segment_last_mark_epoch.get(segment_id).copied() != Some(epoch)
                    && !currently_reachable_segments.contains(segment_id)
            })
            .collect();
        released_segments.sort();
        released_segments.dedup();
        deleted_metadata_nodes.sort();

        {
            let retained_checkpoints = Self::retained_checkpoint_ids_locked(&inner, &policy);
            let retained_commit_cutoffs =
                Self::retained_shard_commit_cutoffs_locked(&inner, &policy);
            let retained_keyspace_commit_cutoffs =
                Self::retained_keyspace_commit_cutoffs_locked(&inner, &policy);
            let expired_devices: BTreeSet<_> = inner
                .deleted_device_heads
                .keys()
                .copied()
                .filter(|device_id| {
                    !Self::retain_deleted_device_locked(&inner, &policy, *device_id)
                })
                .collect();
            for device_id in &expired_devices {
                inner.deleted_device_heads.remove(device_id);
                inner.device_specs.remove(device_id);
            }
            inner
                .delete_records
                .retain(|_, record| !expired_devices.contains(&record.device_id));
            inner
                .checkpoints
                .retain(|_, checkpoint| match checkpoint.owner {
                    MappingOwner::BlockDevice(device_id) => {
                        !expired_devices.contains(&device_id)
                            && retained_checkpoints.contains(&checkpoint.checkpoint_id)
                    }
                    MappingOwner::NativeKeyspace(_) => {
                        retained_checkpoints.contains(&checkpoint.checkpoint_id)
                    }
                });
            inner.shard_commits.retain(|commit| {
                !expired_devices.contains(&commit.device_id)
                    && retained_commit_cutoffs
                        .get(&commit.device_id)
                        .is_some_and(|cutoff| commit.commit_seq.raw() > cutoff.raw())
            });
            inner.keyspace_commits.retain(|commit| {
                retained_keyspace_commit_cutoffs
                    .get(&commit.keyspace_id)
                    .is_some_and(|cutoff| commit.commit_seq.raw() > cutoff.raw())
            });
            inner.file_commits.retain(|commit| {
                retained_keyspace_commit_cutoffs
                    .get(&commit.keyspace_id)
                    .is_some_and(|cutoff| commit.commit_seq.raw() > cutoff.raw())
            });
            inner.fork_records.retain(|_, record| {
                !expired_devices.contains(&record.source)
                    && !expired_devices.contains(&record.target)
            });
        }

        Ok(MetadataSweepReport {
            epoch,
            deleted_metadata_nodes,
            released_segments,
        })
    }

    pub fn last_mark_epoch_for_node(&self, node_id: MetadataNodeId) -> Result<Option<u64>> {
        let inner = lock(&self.inner)?;
        Ok(inner.metadata_last_mark_epoch.get(&node_id).copied())
    }

    pub fn last_mark_epoch_for_segment(&self, segment_id: SegmentId) -> Result<Option<u64>> {
        let inner = lock(&self.inner)?;
        Ok(inner.segment_last_mark_epoch.get(&segment_id).copied())
    }
}

impl MetadataPlane for InMemoryMetadataPlane {
    fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead> {
        self.config.validate()?;
        request.spec.validate()?;

        let shard_count = u64::try_from(self.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard_count overflows u64"))?;
        if request.spec.logical_blocks < shard_count {
            return Err(StorageError::invalid_argument(
                "logical_blocks must be at least shard_count",
            ));
        }

        let mut inner = lock(&self.inner)?;
        let device_id = inner.alloc_device_id();
        let mut shard_roots = Vec::with_capacity(self.config.shard_count);

        for shard in 0..self.config.shard_count {
            let shard = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = request
                .spec
                .logical_blocks
                .checked_mul(shard)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count;
            let end = request
                .spec
                .logical_blocks
                .checked_mul(shard + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count;
            let node = Self::create_empty_tree(
                &mut inner,
                self.config,
                crate::api::BlockRange::new(
                    BlockIndex::from_raw(start),
                    BlockCount::from_raw(end - start),
                ),
            )?;
            shard_roots.push(node.node_id);
        }

        let head = DeviceHead {
            device_id,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit: CommitSeq::from_raw(0),
        };
        head.validate(self.config.shard_count)?;

        inner.device_specs.insert(device_id, request.spec);
        inner.device_heads.insert(device_id, head.clone());
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(device_id),
            head.latest_commit,
            CheckpointRoots::BlockShard(head.shard_roots.clone()),
        );
        Ok(head)
    }

    fn create_keyspace(&self, _request: MetadataCreateKeyspaceRequest) -> Result<KeyspaceHead> {
        self.config.validate()?;
        let mut inner = lock(&self.inner)?;
        let keyspace_id = inner.alloc_keyspace_id();
        let root = Self::insert_empty_keyspace_root_locked(&mut inner)?;
        let head = KeyspaceHead {
            keyspace_id,
            generation: KeyspaceGeneration::from_raw(0),
            root: root.root_id,
            latest_commit: CommitSeq::from_raw(0),
        };
        inner.keyspace_heads.insert(keyspace_id, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(keyspace_id),
            head.latest_commit,
            CheckpointRoots::NativeKeyspace(head.root),
        );
        Ok(head)
    }

    fn get_keyspace_head(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceHead> {
        let inner = lock(&self.inner)?;
        inner
            .keyspace_heads
            .get(&keyspace_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))
    }

    fn get_keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo> {
        let inner = lock(&self.inner)?;
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        let root = Self::keyspace_root_locked(&inner, head.root)?;
        Ok(KeyspaceInfo {
            keyspace_id,
            generation: head.generation,
            latest_commit: head.latest_commit,
            file_count: root.file_count,
        })
    }

    fn create_file(&self, request: MetadataCreateFileRequest) -> Result<FileHead> {
        self.config.validate()?;
        let mut inner = lock(&self.inner)?;
        let keyspace_head = inner
            .keyspace_heads
            .get(&request.keyspace_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace", request.keyspace_id.to_string()))?;
        let keyspace_root = Self::keyspace_root_locked(&inner, keyspace_head.root)?;
        let file_id = inner.alloc_file_id();
        let root = Self::create_empty_tree(
            &mut inner,
            self.config,
            crate::api::BlockRange::new(
                BlockIndex::from_raw(0),
                BlockCount::from_raw(self.config.file_root_blocks),
            ),
        )?;
        let commit_seq = inner.alloc_commit_seq()?;
        let commit_group_id = inner.alloc_commit_group_id();
        let file_head = FileHead {
            file_id,
            version: FileVersion::from_raw(0),
            root: root.node_id,
            size: 0,
            latest_commit: commit_seq,
        };
        file_head.validate_current(root.covered_range, self.config.block_size)?;

        let new_keyspace_root = Self::update_keyspace_file_locked(
            &mut inner,
            &keyspace_root,
            file_id,
            KeyspaceFile {
                name: request.request.spec.name.clone(),
                head: file_head.clone(),
            },
        )?;

        let commit_group = CommitGroup {
            commit_group: commit_group_id,
            commit_seq,
            owner: MappingOwner::NativeKeyspace(request.keyspace_id),
            updates: vec![RootUpdate::FileCreated {
                file_id,
                new_root: root.node_id,
                new_size: 0,
            }],
        };
        let mut next_keyspace_head = keyspace_head.clone();
        next_keyspace_head.generation =
            Self::next_keyspace_generation(next_keyspace_head.generation)?;
        next_keyspace_head.latest_commit = commit_seq;
        next_keyspace_head.root = new_keyspace_root.root_id;

        inner
            .file_writer_epochs
            .insert((request.keyspace_id, file_id), WriterEpoch::from_raw(0));
        inner
            .keyspace_heads
            .insert(request.keyspace_id, next_keyspace_head);
        inner.keyspace_commits.push(KeyspaceCommit {
            commit_seq,
            commit_group: commit_group_id,
            time: LogicalTime::from_raw(commit_seq.raw()),
            keyspace_id: request.keyspace_id,
            old_root: keyspace_root.root_id,
            new_root: new_keyspace_root.root_id,
        });
        inner.file_commits.push(FileCommit {
            commit_seq,
            commit_group: commit_group_id,
            time: LogicalTime::from_raw(commit_seq.raw()),
            keyspace_id: request.keyspace_id,
            file_id,
            old_root: None,
            new_root: root.node_id,
            old_version: None,
            new_version: FileVersion::from_raw(0),
            old_size: 0,
            new_size: 0,
        });
        inner
            .commit_groups
            .insert(commit_group.commit_group, commit_group);
        Ok(file_head)
    }

    fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead> {
        let inner = lock(&self.inner)?;
        inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))
    }

    fn list_live_devices(&self) -> Result<Vec<DeviceId>> {
        self.live_device_ids()
    }

    fn list_deleted_devices(&self) -> Result<Vec<DeviceId>> {
        self.deleted_device_ids()
    }

    fn get_file_head(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileHead> {
        let inner = lock(&self.inner)?;
        Self::file_head_locked(&inner, keyspace_id, file_id)
    }

    fn get_file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo> {
        let head = self.get_file_head(keyspace_id, file_id)?;
        Ok(FileInfo {
            keyspace_id,
            file_id,
            size: head.size,
            version: head.version,
        })
    }

    fn persist_metadata_node(&self, write: MetadataNodeWrite) -> Result<()> {
        let segment_descriptors = write.segment_descriptors();
        write.node.validate(&segment_descriptors)?;
        let mut inner = lock(&self.inner)?;
        match inner.metadata_nodes.get(&write.node.node_id) {
            Some(existing) if existing == &write.node => Ok(()),
            Some(_) => Err(StorageError::conflict(
                "metadata node ID already exists with different content",
            )),
            None => {
                inner.metadata_nodes.insert(write.node.node_id, write.node);
                Ok(())
            }
        }
    }

    fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode> {
        let inner = lock(&self.inner)?;
        inner
            .metadata_nodes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("metadata_node", node_id.to_string()))
    }

    fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup> {
        let mut inner = lock(&self.inner)?;

        match intent.owner {
            MappingOwner::BlockDevice(device_id) => {
                let current = inner
                    .device_heads
                    .get(&device_id)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
                match intent.fence {
                    MetadataFence::DeviceGeneration(_) => {}
                    _ => {
                        return Err(StorageError::invalid_argument(
                            "block device commit requires device-generation fence",
                        ));
                    }
                }

                if intent.updates.is_empty() {
                    return Err(StorageError::invalid_argument(
                        "commit group must include at least one root update",
                    ));
                }

                let mut next_roots = current.shard_roots.clone();
                let mut shard_commits = Vec::with_capacity(intent.updates.len());
                for update in &intent.updates {
                    let RootUpdate::BlockShard(update) = update else {
                        return Err(StorageError::invalid_argument(
                            "block device commit cannot include file-root updates",
                        ));
                    };
                    let shard = usize::try_from(update.shard_id.raw())
                        .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
                    if shard >= next_roots.len() {
                        return Err(StorageError::invalid_argument(
                            "shard update is outside device root set",
                        ));
                    }
                    if next_roots[shard] != update.old_root {
                        return Err(StorageError::conflict("stale shard root"));
                    }
                    if !inner.metadata_nodes.contains_key(&update.new_root) {
                        return Err(StorageError::not_found(
                            "metadata_node",
                            update.new_root.to_string(),
                        ));
                    }
                    shard_commits.push((update.shard_id, update.old_root, update.new_root));
                    next_roots[shard] = update.new_root;
                }

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group_id = inner.alloc_commit_group_id();
                let commit_group = CommitGroup {
                    commit_group: commit_group_id,
                    commit_seq,
                    owner: intent.owner,
                    updates: intent.updates,
                };
                for (shard_id, old_root, new_root) in shard_commits {
                    inner.shard_commits.push(ShardCommit {
                        commit_seq,
                        commit_group: commit_group_id,
                        time: LogicalTime::from_raw(commit_seq.raw()),
                        device_id,
                        shard_id,
                        old_root,
                        new_root,
                    });
                }
                let mut next_head = current.clone();
                next_head.generation = Self::next_generation(next_head.generation)?;
                next_head.latest_commit = commit_seq;
                next_head.shard_roots = next_roots;
                inner.device_heads.insert(device_id, next_head);
                inner
                    .commit_groups
                    .insert(commit_group.commit_group, commit_group.clone());
                Ok(commit_group)
            }
            MappingOwner::NativeKeyspace(keyspace_id) => {
                let current_keyspace = inner
                    .keyspace_heads
                    .get(&keyspace_id)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
                let current_catalog = Self::keyspace_root_locked(&inner, current_keyspace.root)?;
                if intent.updates.len() != 1 {
                    return Err(StorageError::invalid_argument(
                        "native keyspace commit must include exactly one file-root update",
                    ));
                }

                let (file_id, old_root, new_root, new_size) = match intent.updates.as_slice() {
                    [
                        RootUpdate::FileRoot {
                            file_id,
                            old_root,
                            new_root,
                            new_size,
                        },
                    ] => (*file_id, *old_root, *new_root, *new_size),
                    [_] => {
                        return Err(StorageError::invalid_argument(
                            "native keyspace append commit requires a file-root update",
                        ));
                    }
                    _ => unreachable!("length checked above"),
                };
                let current_entry =
                    Self::keyspace_file_in_root_locked(&inner, current_catalog.root_id, file_id)?;
                let current = current_entry.head.clone();
                let append_reservation_commit = match intent.fence {
                    MetadataFence::FileVersion(version) if version == current.version => None,
                    MetadataFence::FileVersion(_) => {
                        return Err(StorageError::conflict("stale file version fence"));
                    }
                    MetadataFence::AppendReservation {
                        session_id,
                        reservation_id,
                        offset,
                        len,
                        writer_epoch,
                    } => {
                        if current.size != offset {
                            return Err(StorageError::conflict(
                                "append reservation commits out of order",
                            ));
                        }
                        let active = lock(&self.active_append_sessions)?;
                        let Some(session) = active.get(&(keyspace_id, file_id)) else {
                            return Err(StorageError::conflict("stale append session"));
                        };
                        if session.session_id != session_id
                            || session.writer_epoch != writer_epoch
                            || session.next_commit_offset != offset
                        {
                            return Err(StorageError::conflict("stale append reservation"));
                        }
                        match session.reservations.get(&reservation_id) {
                            Some(reservation)
                                if reservation.offset == offset && reservation.len == len =>
                            {
                                Some((session_id, reservation_id, offset, len, writer_epoch))
                            }
                            Some(_) | None => {
                                return Err(StorageError::conflict("stale append reservation"));
                            }
                        }
                    }
                    _ => {
                        return Err(StorageError::invalid_argument(
                            "native file commit requires file-version or append-reservation fence",
                        ));
                    }
                };
                if current.root != old_root {
                    return Err(StorageError::conflict("stale file root"));
                }
                if !inner.metadata_nodes.contains_key(&new_root) {
                    return Err(StorageError::not_found(
                        "metadata_node",
                        new_root.to_string(),
                    ));
                }
                let new_root_node =
                    inner
                        .metadata_nodes
                        .get(&new_root)
                        .cloned()
                        .ok_or_else(|| {
                            StorageError::not_found("metadata_node", new_root.to_string())
                        })?;

                let commit_seq = inner.alloc_commit_seq()?;
                let commit_group = CommitGroup {
                    commit_group: inner.alloc_commit_group_id(),
                    commit_seq,
                    owner: intent.owner,
                    updates: vec![RootUpdate::FileRoot {
                        file_id,
                        old_root,
                        new_root,
                        new_size,
                    }],
                };
                let mut next_head = current.clone();
                next_head.version = Self::next_file_version(next_head.version)?;
                next_head.latest_commit = commit_seq;
                next_head.root = new_root;
                next_head.size = new_size;
                next_head.validate_transition_from(
                    &current,
                    new_root_node.covered_range,
                    self.config.block_size,
                )?;
                let new_catalog = Self::update_keyspace_file_locked(
                    &mut inner,
                    &current_catalog,
                    file_id,
                    KeyspaceFile {
                        head: next_head.clone(),
                        ..current_entry
                    },
                )?;
                let mut next_keyspace = current_keyspace.clone();
                next_keyspace.generation =
                    Self::next_keyspace_generation(next_keyspace.generation)?;
                next_keyspace.latest_commit = commit_seq;
                next_keyspace.root = new_catalog.root_id;
                inner.keyspace_heads.insert(keyspace_id, next_keyspace);
                inner.keyspace_commits.push(KeyspaceCommit {
                    commit_seq,
                    commit_group: commit_group.commit_group,
                    time: LogicalTime::from_raw(commit_seq.raw()),
                    keyspace_id,
                    old_root: current_catalog.root_id,
                    new_root: new_catalog.root_id,
                });
                inner.file_commits.push(FileCommit {
                    commit_seq,
                    commit_group: commit_group.commit_group,
                    time: LogicalTime::from_raw(commit_seq.raw()),
                    keyspace_id,
                    file_id,
                    old_root: Some(current.root),
                    new_root,
                    old_version: Some(current.version),
                    new_version: next_head.version,
                    old_size: current.size,
                    new_size,
                });
                inner
                    .commit_groups
                    .insert(commit_group.commit_group, commit_group.clone());
                if let Some((session_id, reservation_id, offset, len, writer_epoch)) =
                    append_reservation_commit
                {
                    inner
                        .file_writer_epochs
                        .insert((keyspace_id, file_id), writer_epoch);
                    let mut active = lock(&self.active_append_sessions)?;
                    if let Some(session) = active.get_mut(&(keyspace_id, file_id))
                        && session.session_id == session_id
                        && session.writer_epoch == writer_epoch
                    {
                        session.reservations.remove(&reservation_id);
                        session.next_commit_offset = offset.checked_add(len).ok_or_else(|| {
                            StorageError::invalid_argument("append reservation end overflows")
                        })?;
                    }
                }
                Ok(commit_group)
            }
        }
    }

    fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead> {
        let mut inner = lock(&self.inner)?;
        let source_head = inner
            .device_heads
            .get(&request.source)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", request.source.to_string()))?;
        let source_spec = inner
            .device_specs
            .get(&request.source)
            .cloned()
            .ok_or_else(|| StorageError::corrupt("source device head exists without spec"))?;
        let target = match request.target {
            Some(target) => {
                if inner.device_heads.contains_key(&target)
                    || inner.deleted_device_heads.contains_key(&target)
                {
                    return Err(StorageError::conflict("target device already exists"));
                }
                inner.reserve_device_id_at_least_after(target)?;
                target
            }
            None => inner.alloc_device_id(),
        };
        let latest_commit = inner.alloc_commit_seq()?;
        let shard_roots = source_head.shard_roots.clone();
        let head = DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: shard_roots.clone(),
            latest_commit,
        };
        head.validate(self.config.shard_count)?;
        let record = ForkRecord {
            commit_seq: latest_commit,
            source: request.source,
            target,
            shard_roots,
        };
        inner.device_specs.insert(target, source_spec);
        inner.device_heads.insert(target, head.clone());
        inner.fork_records.insert(latest_commit, record);
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(target),
            latest_commit,
            CheckpointRoots::BlockShard(head.shard_roots.clone()),
        );
        Ok(head)
    }

    fn restore_device(
        &self,
        source: DeviceId,
        point: crate::api::RestorePoint,
    ) -> Result<DeviceHead> {
        let mut inner = lock(&self.inner)?;
        let source_spec = inner
            .device_specs
            .get(&source)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", source.to_string()))?;
        let target_commit = Self::target_commit_for_restore_locked(&inner, source, point)?;
        let shard_roots = Self::replay_device_roots_locked(&inner, source, target_commit, None)?;
        for root in &shard_roots {
            if !inner.metadata_nodes.contains_key(root) {
                return Err(StorageError::not_found("metadata_node", root.to_string()));
            }
        }
        let target = inner.alloc_device_id();
        let latest_commit = inner.alloc_commit_seq()?;
        let head = DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit,
        };
        head.validate(self.config.shard_count)?;
        inner.device_specs.insert(target, source_spec);
        inner.device_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::BlockDevice(target),
            latest_commit,
            CheckpointRoots::BlockShard(head.shard_roots.clone()),
        );
        Ok(head)
    }

    fn snapshot_keyspace(&self, request: MetadataSnapshotKeyspaceRequest) -> Result<KeyspaceHead> {
        let mut inner = lock(&self.inner)?;
        let source_head = inner
            .keyspace_heads
            .get(&request.source)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace", request.source.to_string()))?;
        let target = match request.target {
            Some(target) => {
                if inner.keyspace_heads.contains_key(&target) {
                    return Err(StorageError::conflict("target keyspace already exists"));
                }
                inner.reserve_keyspace_id_at_least_after(target)?;
                target
            }
            None => inner.alloc_keyspace_id(),
        };
        let latest_commit = inner.alloc_commit_seq()?;
        let head = KeyspaceHead {
            keyspace_id: target,
            generation: KeyspaceGeneration::from_raw(0),
            root: source_head.root,
            latest_commit,
        };
        inner.keyspace_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(target),
            latest_commit,
            CheckpointRoots::NativeKeyspace(head.root),
        );
        Ok(head)
    }

    fn restore_keyspace(
        &self,
        source: KeyspaceId,
        point: crate::api::RestorePoint,
    ) -> Result<KeyspaceHead> {
        let mut inner = lock(&self.inner)?;
        if !inner.keyspace_heads.contains_key(&source) {
            return Err(StorageError::not_found("keyspace", source.to_string()));
        }
        let root = match point {
            RestorePoint::Checkpoint(checkpoint_id) => {
                let checkpoint =
                    inner
                        .checkpoints
                        .get(&checkpoint_id)
                        .cloned()
                        .ok_or_else(|| {
                            StorageError::not_found("checkpoint", checkpoint_id.to_string())
                        })?;
                if checkpoint.owner != MappingOwner::NativeKeyspace(source) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source keyspace",
                    ));
                }
                Self::checkpoint_keyspace_root(&checkpoint)?
            }
            RestorePoint::Commit(_) | RestorePoint::Time(_) => {
                let target_commit =
                    Self::target_commit_for_keyspace_restore_locked(&inner, source, point)?;
                Self::replay_keyspace_root_locked(&inner, source, target_commit, None)?
            }
        };
        if !inner.keyspace_roots.contains_key(&root) {
            return Err(StorageError::not_found("keyspace_root", root.to_string()));
        }
        let target = inner.alloc_keyspace_id();
        let latest_commit = inner.alloc_commit_seq()?;
        let head = KeyspaceHead {
            keyspace_id: target,
            generation: KeyspaceGeneration::from_raw(0),
            root,
            latest_commit,
        };
        inner.keyspace_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(target),
            latest_commit,
            CheckpointRoots::NativeKeyspace(head.root),
        );
        Ok(head)
    }

    fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        let mut inner = lock(&self.inner)?;
        let mut head = inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        let commit_seq = inner.alloc_commit_seq()?;
        inner.device_heads.remove(&device_id);
        head.latest_commit = commit_seq;
        let record = DeleteRecord {
            commit_seq,
            time: LogicalTime::from_raw(commit_seq.raw()),
            device_id,
            shard_roots: head.shard_roots.clone(),
        };
        inner.deleted_device_heads.insert(device_id, head);
        inner.delete_records.insert(commit_seq, record);
        Ok(DeleteResult {
            device_id,
            commit_seq,
        })
    }

    fn get_delete_record(&self, commit_seq: CommitSeq) -> Result<DeleteRecord> {
        self.delete_record(commit_seq)
    }

    fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        let mut inner = lock(&self.inner)?;
        let head = inner
            .device_heads
            .get(&device_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("device", device_id.to_string()))?;
        Ok(inner.insert_checkpoint(
            MappingOwner::BlockDevice(device_id),
            head.latest_commit,
            CheckpointRoots::BlockShard(head.shard_roots),
        ))
    }

    fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        let mut inner = lock(&self.inner)?;
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        Ok(inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(keyspace_id),
            head.latest_commit,
            CheckpointRoots::NativeKeyspace(head.root),
        ))
    }

    fn get_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<Checkpoint> {
        let inner = lock(&self.inner)?;
        inner
            .checkpoints
            .get(&checkpoint_id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("checkpoint", checkpoint_id.to_string()))
    }

    fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        let mut inner = lock(&self.inner)?;
        Self::ensure_pitr_anchor_checkpoints_locked(&mut inner, &policy)?;
        Self::roots_for_gc_locked(&inner, policy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SegmentRecord {
    bytes: Vec<u8>,
    synced: bool,
    commit: SegmentReplicaCommit,
}

#[derive(Debug)]
struct DurableSegmentPayload {
    segment_id: SegmentId,
    storage_node: StorageNodeId,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SegmentStoreInner {
    next_offset: u64,
    segments: BTreeMap<SegmentId, SegmentRecord>,
}

/// In-memory implementation of `SegmentStore`.
#[derive(Debug)]
pub struct InMemorySegmentStore {
    config: LocalStoreConfig,
    inner: Mutex<SegmentStoreInner>,
}

impl InMemorySegmentStore {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(SegmentStoreInner {
                next_offset: 0,
                segments: BTreeMap::new(),
            }),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: SegmentStoreInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
        })
    }

    #[cfg(test)]
    fn state_inner(&self) -> Result<SegmentStoreInner> {
        Ok(lock(&self.inner)?.clone())
    }

    fn state_inner_for_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
        storage_node: StorageNodeId,
    ) -> Result<(
        SegmentStoreInner,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let inner = lock(&self.inner)?;
        let mut image_segments = BTreeSet::new();
        let mut new_segments = Vec::new();
        for (segment_id, record) in &inner.segments {
            image_segments.insert(*segment_id);
            if !previous_segments.contains(segment_id) {
                new_segments.push(DurableSegmentPayload {
                    segment_id: *segment_id,
                    storage_node,
                    bytes: record.bytes.clone(),
                });
            }
        }
        Ok((
            SegmentStoreInner {
                next_offset: inner.next_offset,
                segments: BTreeMap::new(),
            },
            image_segments,
            new_segments,
        ))
    }

    fn segment_ids(&self) -> Result<BTreeSet<SegmentId>> {
        Ok(lock(&self.inner)?.segments.keys().copied().collect())
    }

    pub fn is_synced(&self, segment_id: SegmentId) -> Result<bool> {
        let inner = lock(&self.inner)?;
        inner
            .segments
            .get(&segment_id)
            .map(|record| record.synced)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn contains_segment(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(lock(&self.inner)?.segments.contains_key(&segment_id))
    }

    fn write_segment_owned(
        &self,
        reservation: &SegmentReservation,
        bytes: Vec<u8>,
    ) -> Result<SegmentReplicaCommit> {
        self.config.validate()?;

        if bytes.is_empty() {
            return Err(StorageError::invalid_argument(
                "segment write must contain bytes",
            ));
        }

        let bytes_len = u64::try_from(bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment write length overflows u64"))?;
        if reservation.bytes != bytes_len {
            return Err(StorageError::invalid_argument(
                "reservation byte count does not match write length",
            ));
        }

        if bytes_len % u64::from(self.config.block_size) != 0 {
            return Err(StorageError::invalid_argument(
                "segment write length must be block aligned",
            ));
        }

        let mut inner = lock(&self.inner)?;
        if let Some(existing) = inner.segments.get(&reservation.segment_id) {
            if existing.bytes == bytes {
                return Ok(existing.commit.clone());
            }
            return Err(StorageError::conflict(
                "segment ID already exists with different bytes",
            ));
        }

        let offset = inner.next_offset;
        inner.next_offset = inner
            .next_offset
            .checked_add(reservation.bytes)
            .ok_or_else(|| StorageError::conflict("local segment offset overflow"))?;
        let blocks = reservation.bytes / u64::from(self.config.block_size);
        let commit = SegmentReplicaCommit {
            descriptor: SegmentDescriptor {
                segment_id: reservation.segment_id,
                blocks: BlockCount::from_raw(blocks),
                bytes: reservation.bytes,
                checksum: Some(checksum64(&bytes)),
            },
            placement: SegmentReplicaPlacement {
                segment_id: reservation.segment_id,
                storage_node: self.config.storage_node,
                offset,
                bytes: reservation.bytes,
            },
        };
        inner.segments.insert(
            reservation.segment_id,
            SegmentRecord {
                bytes,
                synced: false,
                commit: commit.clone(),
            },
        );
        Ok(commit)
    }
}

impl SegmentStore for InMemorySegmentStore {
    fn write_segment(
        &self,
        reservation: &SegmentReservation,
        bytes: &[u8],
    ) -> Result<SegmentReplicaCommit> {
        self.write_segment_owned(reservation, bytes.to_vec())
    }

    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let inner = lock(&self.inner)?;
        let record = inner
            .segments
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        if !record.synced {
            return Err(StorageError::unavailable("segment is not synced"));
        }
        let end = range.end_exclusive()?;
        let record_len = u64::try_from(record.bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment byte length overflows u64"))?;
        if end > record_len {
            return Err(StorageError::invalid_argument(
                "segment read extends past end of segment",
            ));
        }
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        let start = usize::try_from(range.offset)
            .map_err(|_| StorageError::invalid_argument("segment read offset overflows usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| StorageError::invalid_argument("segment read end overflows usize"))?;
        let source = record
            .bytes
            .get(start..end)
            .ok_or_else(|| StorageError::corrupt("segment read range exceeds segment bytes"))?;
        buf.copy_from_slice(source);
        Ok(())
    }

    fn sync_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let record = inner
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        record.synced = true;
        Ok(())
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        lock(&self.inner)?.segments.remove(&segment_id);
        Ok(())
    }
}

/// Local segment lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentLifecycleState {
    Reserved,
    Writing,
    DurablePendingMetadata,
    Referenced,
    Released,
    Freed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CatalogEntry {
    intent: SegmentReservationIntent,
    reservation: SegmentReservation,
    state: SegmentLifecycleState,
    receipt: Option<SegmentWriteReceipt>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CatalogInner {
    next_segment_id: u128,
    entries: BTreeMap<SegmentId, CatalogEntry>,
}

#[derive(Debug, Clone, Copy, Default)]
struct CatalogLifecycleCounts {
    reserved: usize,
    writing: usize,
    durable_pending: usize,
    referenced: usize,
    released: usize,
    freed: usize,
}

/// In-memory implementation of `LocalSegmentCatalog`.
#[derive(Debug)]
pub struct InMemoryLocalSegmentCatalog {
    config: LocalStoreConfig,
    inner: Mutex<CatalogInner>,
}

impl InMemoryLocalSegmentCatalog {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            }),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: CatalogInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
        })
    }

    fn state_inner(&self) -> Result<CatalogInner> {
        Ok(lock(&self.inner)?.clone())
    }

    fn reserve_segment_with_id(
        &self,
        segment_id: SegmentId,
        intent: SegmentReservationIntent,
    ) -> Result<SegmentReservation> {
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let mut inner = lock(&self.inner)?;
        if inner.entries.contains_key(&segment_id) {
            return Err(StorageError::conflict("segment ID already exists"));
        }
        if segment_id.raw() >= inner.next_segment_id {
            inner.next_segment_id = segment_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("segment id overflow"))?;
        }
        let reservation = SegmentReservation {
            segment_id,
            bytes: intent.bytes,
        };
        inner.entries.insert(
            segment_id,
            CatalogEntry {
                intent,
                reservation: reservation.clone(),
                state: SegmentLifecycleState::Reserved,
                receipt: None,
            },
        );
        Ok(reservation)
    }

    pub fn contains_segment(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(lock(&self.inner)?.entries.contains_key(&segment_id))
    }

    pub fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.state)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .as_ref()
            .map(SegmentWriteReceipt::replica_commit)
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn receipt_for_segment(&self, segment_id: SegmentId) -> Result<SegmentWriteReceipt> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .clone()
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn intent_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReservationIntent> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.intent.clone())
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn entries(&self) -> Result<Vec<(SegmentId, SegmentLifecycleState, WriteIntentId)>> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .entries
            .iter()
            .map(|(segment_id, entry)| (*segment_id, entry.state, entry.intent.write_intent))
            .collect())
    }

    fn lifecycle_counts(&self) -> Result<CatalogLifecycleCounts> {
        let inner = lock(&self.inner)?;
        let mut counts = CatalogLifecycleCounts::default();
        for entry in inner.entries.values() {
            match entry.state {
                SegmentLifecycleState::Reserved => counts.reserved += 1,
                SegmentLifecycleState::Writing => counts.writing += 1,
                SegmentLifecycleState::DurablePendingMetadata => counts.durable_pending += 1,
                SegmentLifecycleState::Referenced => counts.referenced += 1,
                SegmentLifecycleState::Released => counts.released += 1,
                SegmentLifecycleState::Freed => counts.freed += 1,
            }
        }
        Ok(counts)
    }

    fn get_entry_mut(inner: &mut CatalogInner, segment_id: SegmentId) -> Result<&mut CatalogEntry> {
        inner
            .entries
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }
}

impl LocalSegmentCatalog for InMemoryLocalSegmentCatalog {
    fn reserve_segment(&self, intent: SegmentReservationIntent) -> Result<SegmentReservation> {
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let segment_id = {
            let inner = lock(&self.inner)?;
            SegmentId::from_raw(inner.next_segment_id)
        };
        self.reserve_segment_with_id(segment_id, intent)
    }

    fn begin_write(&self, reservation: &SegmentReservation) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != *reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Writing;
                Ok(())
            }
            SegmentLifecycleState::Writing => Ok(()),
            _ => Err(StorageError::conflict(
                "segment write can only begin from Reserved state",
            )),
        }
    }

    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
    ) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        if receipt.segment_id != reservation.segment_id
            || receipt.descriptor.segment_id != reservation.segment_id
            || receipt.placement.segment_id != reservation.segment_id
        {
            return Err(StorageError::invalid_argument(
                "segment receipt IDs must match reservation",
            ));
        }
        if receipt.storage_node != self.config.storage_node
            || receipt.placement.storage_node != self.config.storage_node
        {
            return Err(StorageError::invalid_argument(
                "segment receipt storage node does not match local catalog",
            ));
        }
        if receipt.bytes != reservation.bytes
            || receipt.descriptor.bytes != reservation.bytes
            || receipt.placement.bytes != reservation.bytes
        {
            return Err(StorageError::invalid_argument(
                "segment receipt bytes must match reservation",
            ));
        }

        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.receipt = Some(receipt);
                entry.state = SegmentLifecycleState::DurablePendingMetadata;
                Ok(())
            }
            SegmentLifecycleState::DurablePendingMetadata
                if entry.receipt.as_ref() == Some(&receipt) =>
            {
                Ok(())
            }
            _ => Err(StorageError::conflict(
                "segment receipt requires Writing state",
            )),
        }
    }

    fn mark_segment_referenced(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Referenced;
                Ok(())
            }
            SegmentLifecycleState::Referenced => Ok(()),
            _ => Err(StorageError::conflict(
                "segment can be referenced only from DurablePendingMetadata state",
            )),
        }
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Referenced => {
                entry.state = SegmentLifecycleState::Released;
                Ok(())
            }
            SegmentLifecycleState::Released => Ok(()),
            _ => Err(StorageError::conflict(
                "segment can be released only from Referenced state",
            )),
        }
    }

    fn expire_reservation(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Reserved segments can expire as reservations",
            )),
        }
    }

    fn fail_write(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Writing segments can fail as writes",
            )),
        }
    }

    fn free_orphan_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only DurablePendingMetadata orphan segments can be freed",
            )),
        }
    }

    fn locate_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaPlacement> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata
            | SegmentLifecycleState::Referenced
            | SegmentLifecycleState::Released => entry
                .receipt
                .as_ref()
                .map(|receipt| receipt.placement.clone())
                .ok_or_else(|| StorageError::corrupt("committed segment missing placement")),
            SegmentLifecycleState::Freed => {
                Err(StorageError::not_found("segment", segment_id.to_string()))
            }
            SegmentLifecycleState::Reserved | SegmentLifecycleState::Writing => Err(
                StorageError::unavailable("segment placement is not committed yet"),
            ),
        }
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Released => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Released segments are safe to delete",
            )),
        }
    }
}

/// Local block request coordinator.
type RequestKey = (ClientEpoch, RequestId);
const SERVER_LOCK_STRIPES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedBlockRequest {
    request: BlockRequest,
    result: Result<BlockResponseEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedNativeRequest {
    request: NativeRequest,
    result: Result<NativeResponseEnvelope>,
}

#[derive(Debug, Clone)]
pub struct LocalBlockServer {
    store: LocalCoordinator,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    responses: Arc<Mutex<BTreeMap<RequestKey, CachedBlockRequest>>>,
    stripes: Arc<Vec<Mutex<()>>>,
}

impl LocalBlockServer {
    pub fn new(store: LocalCoordinator) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            stripes: Arc::new(server_lock_stripes()),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }

    fn cached_response(
        &self,
        request: &BlockRequestEnvelope,
    ) -> Result<Option<BlockResponseEnvelope>> {
        let key = (request.client_epoch, request.request_id);
        let responses = lock(&self.responses)?;
        let Some(cached) = responses.get(&key) else {
            return Ok(None);
        };
        if cached.request != request.request {
            return Err(StorageError::conflict(
                "request ID and client epoch reused for a different block request",
            ));
        }
        cached.result.clone().map(Some)
    }

    fn remember_response(
        &self,
        request: &BlockRequestEnvelope,
        result: Result<BlockResponseEnvelope>,
    ) -> Result<BlockResponseEnvelope> {
        let key = (request.client_epoch, request.request_id);
        lock(&self.responses)?.insert(
            key,
            CachedBlockRequest {
                request: request.request.clone(),
                result: result.clone(),
            },
        );
        result
    }
}

impl BlockServer for LocalBlockServer {
    fn handle(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let _stripe_guard = lock(&self.stripes[block_request_stripe(&request.request)])?;
        if let Some(response) = self.cached_response(&request)? {
            return Ok(response);
        }
        lock(&self.request_log)?.push(request.request_id);
        let response = (|| -> Result<BlockResponse> {
            match request.request.clone() {
                BlockRequest::Create { request } => {
                    let head = self
                        .store
                        .metadata
                        .create_device(MetadataCreateDeviceRequest::from(request))?;
                    Ok(BlockResponse::Created(head.device_id))
                }
                BlockRequest::Info { device_id } => Ok(BlockResponse::Info(
                    self.store.metadata.device_info(device_id)?,
                )),
                BlockRequest::Read { device_id, range } => {
                    let len = usize::try_from(range.len).map_err(|_| {
                        StorageError::invalid_argument("read byte length overflows usize")
                    })?;
                    let mut bytes = vec![0; len];
                    self.store.read_device(device_id, range, &mut bytes)?;
                    Ok(BlockResponse::Read(ReadResponse { bytes }))
                }
                BlockRequest::Write {
                    device_id,
                    offset,
                    bytes,
                    durability,
                } => Ok(BlockResponse::Write(
                    self.store
                        .write_device(device_id, offset, &bytes, durability)?,
                )),
                BlockRequest::WriteZeroes { device_id, range } => Ok(BlockResponse::Write(
                    self.store
                        .write_zeroes(device_id, range.offset, range.len)?,
                )),
                BlockRequest::Discard { device_id, range } => Ok(BlockResponse::Write(
                    self.store
                        .discard_device(device_id, range.offset, range.len)?,
                )),
                BlockRequest::Flush { device_id, .. } => {
                    let info = self.store.metadata.device_info(device_id)?;
                    Ok(BlockResponse::Flush(FlushResult {
                        device_id,
                        durable_through: info.latest_commit,
                    }))
                }
                BlockRequest::Fork { source, request } => Ok(BlockResponse::Forked(
                    self.store.fork_device(source, request)?,
                )),
                BlockRequest::Restore { source, point } => Ok(BlockResponse::Restored(
                    self.store.restore_device(source, point)?,
                )),
                BlockRequest::Delete { device_id } => {
                    Ok(BlockResponse::Deleted(self.store.delete_device(device_id)?))
                }
            }
        })();
        let result = response.map(|response| BlockResponseEnvelope {
            request_id: request.request_id,
            response,
        });
        self.remember_response(&request, result)
    }
}

/// Local native keyspace/file request coordinator.
#[derive(Debug, Clone)]
pub struct LocalNativeServer {
    store: LocalCoordinator,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    responses: Arc<Mutex<BTreeMap<RequestKey, CachedNativeRequest>>>,
    stripes: Arc<Vec<Mutex<()>>>,
}

impl LocalNativeServer {
    pub fn new(store: LocalCoordinator) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            stripes: Arc::new(server_lock_stripes()),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }

    fn cached_response(
        &self,
        request: &NativeRequestEnvelope,
    ) -> Result<Option<NativeResponseEnvelope>> {
        let key = (request.client_epoch, request.request_id);
        let responses = lock(&self.responses)?;
        let Some(cached) = responses.get(&key) else {
            return Ok(None);
        };
        if cached.request != request.request {
            return Err(StorageError::conflict(
                "request ID and client epoch reused for a different native request",
            ));
        }
        cached.result.clone().map(Some)
    }

    fn remember_response(
        &self,
        request: &NativeRequestEnvelope,
        result: Result<NativeResponseEnvelope>,
    ) -> Result<NativeResponseEnvelope> {
        let key = (request.client_epoch, request.request_id);
        lock(&self.responses)?.insert(
            key,
            CachedNativeRequest {
                request: request.request.clone(),
                result: result.clone(),
            },
        );
        result
    }
}

impl NativeServer for LocalNativeServer {
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let _stripe_guard = lock(&self.stripes[native_request_stripe(&request.request)])?;
        if let Some(response) = self.cached_response(&request)? {
            return Ok(response);
        }
        lock(&self.request_log)?.push(request.request_id);
        let response = (|| -> Result<NativeResponse> {
            match request.request.clone() {
                NativeRequest::CreateKeyspace { request } => {
                    let head = self
                        .store
                        .metadata
                        .create_keyspace(MetadataCreateKeyspaceRequest { request })?;
                    Ok(NativeResponse::KeyspaceCreated(head.keyspace_id))
                }
                NativeRequest::KeyspaceInfo { keyspace_id } => Ok(NativeResponse::KeyspaceInfo(
                    self.store.metadata.get_keyspace_info(keyspace_id)?,
                )),
                NativeRequest::CreateFile {
                    keyspace_id,
                    request,
                } => {
                    let head = self.store.metadata.create_file(MetadataCreateFileRequest {
                        keyspace_id,
                        request,
                    })?;
                    Ok(NativeResponse::FileCreated(head.file_id))
                }
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                } => Ok(NativeResponse::FileInfo(
                    self.store.metadata.get_file_info(keyspace_id, file_id)?,
                )),
                NativeRequest::Read {
                    keyspace_id,
                    file_id,
                    range,
                } => {
                    let len = usize::try_from(range.len).map_err(|_| {
                        StorageError::invalid_argument("read byte length overflows usize")
                    })?;
                    let mut bytes = vec![0; len];
                    self.store
                        .read_file(keyspace_id, file_id, range, &mut bytes)?;
                    Ok(NativeResponse::Read(ReadResponse { bytes }))
                }
                NativeRequest::Write {
                    keyspace_id,
                    file_id,
                    offset,
                    bytes,
                    durability,
                } => Ok(NativeResponse::Write(self.store.write_file_at(
                    keyspace_id,
                    file_id,
                    offset,
                    &bytes,
                    durability,
                )?)),
                NativeRequest::OpenAppendSession {
                    keyspace_id,
                    file_id,
                } => Ok(NativeResponse::AppendSession(
                    self.store.open_append_session(keyspace_id, file_id)?,
                )),
                NativeRequest::ReserveAppend {
                    keyspace_id,
                    file_id,
                    session,
                    len,
                } => {
                    if keyspace_id != session.keyspace_id || file_id != session.file_id {
                        Err(StorageError::invalid_argument(
                            "append session target does not match request target",
                        ))
                    } else {
                        Ok(NativeResponse::AppendReservation(
                            self.store.reserve_append(&session, len)?,
                        ))
                    }
                }
                NativeRequest::AppendReserved {
                    keyspace_id,
                    file_id,
                    reservation,
                    bytes,
                    durability,
                } => {
                    if keyspace_id != reservation.keyspace_id || file_id != reservation.file_id {
                        Err(StorageError::invalid_argument(
                            "append reservation target does not match request target",
                        ))
                    } else {
                        Ok(NativeResponse::Append(self.store.append_reserved(
                            reservation,
                            &bytes,
                            durability,
                        )?))
                    }
                }
                NativeRequest::Flush {
                    keyspace_id,
                    file_id,
                } => {
                    let info = self.store.metadata.get_file_info(keyspace_id, file_id)?;
                    Ok(NativeResponse::Flush(FlushResult {
                        device_id: DeviceId::from_raw(info.file_id.raw()),
                        durable_through: self
                            .store
                            .metadata
                            .get_file_head(keyspace_id, file_id)?
                            .latest_commit,
                    }))
                }
                NativeRequest::CheckpointKeyspace { keyspace_id } => {
                    Ok(NativeResponse::KeyspaceCheckpointed(
                        self.store.metadata.checkpoint_keyspace(keyspace_id)?,
                    ))
                }
                NativeRequest::SnapshotKeyspace { source, request } => {
                    let head =
                        self.store
                            .metadata
                            .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                                source,
                                target: request.target,
                                name: request.name,
                            })?;
                    Ok(NativeResponse::KeyspaceSnapshotted(head.keyspace_id))
                }
                NativeRequest::RestoreKeyspace { source, point } => Ok(
                    NativeResponse::KeyspaceRestored(self.store.restore_keyspace(source, point)?),
                ),
            }
        })();
        let result = response.map(|response| NativeResponseEnvelope {
            request_id: request.request_id,
            response,
        });
        self.remember_response(&request, result)
    }
}

/// In-process block transport.
#[derive(Clone)]
pub struct InProcessBlockTransport {
    server: Arc<dyn BlockServer>,
}

impl InProcessBlockTransport {
    pub fn new(server: Arc<dyn BlockServer>) -> Self {
        Self { server }
    }
}

impl BlockTransport for InProcessBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        self.server.handle(request)
    }
}

/// In-process native keyspace/file transport.
#[derive(Clone)]
pub struct InProcessNativeTransport {
    server: Arc<dyn NativeServer>,
}

impl InProcessNativeTransport {
    pub fn new(server: Arc<dyn NativeServer>) -> Self {
        Self { server }
    }
}

impl NativeTransport for InProcessNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        self.server.handle(request)
    }
}

/// Serialized wire boundary used by remote-shaped transports.
///
/// Minimal implementor guarantees:
///
/// - Accept exactly one encoded request envelope and return exactly one encoded
///   response envelope, or report a transport-level failure.
/// - Preserve request bytes as opaque data; block/native semantics are enforced
///   above this trait by the typed transport and below it by the endpoint.
/// - Failures, dropped responses, delayed responses, and reordered responses
///   must be surfaced as errors or bytes for the typed transport to validate;
///   they must not mutate request IDs or response IDs.
pub trait RemoteWireTransport: Send + Sync {
    /// Send one encoded request and return encoded response bytes.
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>>;
}

/// Deterministic counters for chaos wire transport fault injection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChaosTransportMetrics {
    pub request_drops: u64,
    pub response_drops: u64,
    pub corrupted_responses: u64,
    pub duplicated_requests: u64,
    pub delayed_responses: u64,
    pub reordered_responses: u64,
    pub injected_failures: u64,
}

#[derive(Debug)]
struct ChaosWireState {
    delayed: VecDeque<Result<Vec<u8>>>,
    trace: Vec<String>,
    metrics: ChaosTransportMetrics,
    fail_next_call: bool,
    drop_next_request: bool,
    drop_next_response: bool,
    corrupt_next_response: bool,
    duplicate_next_request: bool,
    delay_next_response: bool,
    return_delayed_next: bool,
    reorder_next_response: bool,
}

impl ChaosWireState {
    fn new() -> Self {
        Self {
            delayed: VecDeque::new(),
            trace: Vec::new(),
            metrics: ChaosTransportMetrics::default(),
            fail_next_call: false,
            drop_next_request: false,
            drop_next_response: false,
            corrupt_next_response: false,
            duplicate_next_request: false,
            delay_next_response: false,
            return_delayed_next: false,
            reorder_next_response: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChaosWireAction {
    Pass,
    Fail,
    DropRequest,
    DropResponse,
    CorruptResponse,
    DuplicateRequest,
    DelayResponse,
    ReturnDelayed,
    ReorderWithDelayed,
}

/// Deterministic chaos wrapper for serialized remote transports.
///
/// The wrapper is intentionally scriptable instead of wall-clock or thread
/// driven. Tests can force the next request or response to be dropped,
/// duplicated, delayed, or reordered and then assert that the typed transport
/// rejects stale bytes or retries safely with the same request identity.
#[derive(Clone)]
pub struct ChaosRemoteWireTransport {
    inner: Arc<dyn RemoteWireTransport>,
    state: Arc<Mutex<ChaosWireState>>,
}

impl ChaosRemoteWireTransport {
    pub fn new(inner: Arc<dyn RemoteWireTransport>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ChaosWireState::new())),
        }
    }

    pub fn trace(&self) -> Result<Vec<String>> {
        Ok(lock(&self.state)?.trace.clone())
    }

    pub fn metrics(&self) -> Result<ChaosTransportMetrics> {
        Ok(lock(&self.state)?.metrics)
    }

    pub fn delayed_len(&self) -> Result<usize> {
        Ok(lock(&self.state)?.delayed.len())
    }

    pub fn fail_next_call(&self) -> Result<()> {
        lock(&self.state)?.fail_next_call = true;
        Ok(())
    }

    pub fn drop_next_request(&self) -> Result<()> {
        lock(&self.state)?.drop_next_request = true;
        Ok(())
    }

    pub fn drop_next_response(&self) -> Result<()> {
        lock(&self.state)?.drop_next_response = true;
        Ok(())
    }

    pub fn corrupt_next_response(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_response = true;
        Ok(())
    }

    pub fn duplicate_next_request(&self) -> Result<()> {
        lock(&self.state)?.duplicate_next_request = true;
        Ok(())
    }

    pub fn delay_next_response(&self) -> Result<()> {
        lock(&self.state)?.delay_next_response = true;
        Ok(())
    }

    pub fn return_delayed_response_next_call(&self) -> Result<()> {
        lock(&self.state)?.return_delayed_next = true;
        Ok(())
    }

    pub fn reorder_next_response_with_delayed(&self) -> Result<()> {
        lock(&self.state)?.reorder_next_response = true;
        Ok(())
    }

    fn take_action(&self) -> Result<ChaosWireAction> {
        let mut state = lock(&self.state)?;
        if state.fail_next_call {
            state.fail_next_call = false;
            state.metrics.injected_failures = state.metrics.injected_failures.saturating_add(1);
            state.trace.push("fail call before send".to_string());
            return Ok(ChaosWireAction::Fail);
        }
        if state.drop_next_request {
            state.drop_next_request = false;
            state.metrics.request_drops = state.metrics.request_drops.saturating_add(1);
            state.trace.push("drop request before send".to_string());
            return Ok(ChaosWireAction::DropRequest);
        }
        if state.return_delayed_next {
            state.return_delayed_next = false;
            state
                .trace
                .push("return delayed response before sending request".to_string());
            return Ok(ChaosWireAction::ReturnDelayed);
        }
        if state.reorder_next_response {
            state.reorder_next_response = false;
            state.metrics.reordered_responses = state.metrics.reordered_responses.saturating_add(1);
            state
                .trace
                .push("reorder current response behind delayed response".to_string());
            return Ok(ChaosWireAction::ReorderWithDelayed);
        }
        if state.drop_next_response {
            state.drop_next_response = false;
            state.metrics.response_drops = state.metrics.response_drops.saturating_add(1);
            state.trace.push("drop response after send".to_string());
            return Ok(ChaosWireAction::DropResponse);
        }
        if state.corrupt_next_response {
            state.corrupt_next_response = false;
            state.metrics.corrupted_responses = state.metrics.corrupted_responses.saturating_add(1);
            state.trace.push("corrupt response after send".to_string());
            return Ok(ChaosWireAction::CorruptResponse);
        }
        if state.duplicate_next_request {
            state.duplicate_next_request = false;
            state.metrics.duplicated_requests = state.metrics.duplicated_requests.saturating_add(1);
            state.trace.push("duplicate request delivery".to_string());
            return Ok(ChaosWireAction::DuplicateRequest);
        }
        if state.delay_next_response {
            state.delay_next_response = false;
            state.metrics.delayed_responses = state.metrics.delayed_responses.saturating_add(1);
            state.trace.push("delay response after send".to_string());
            return Ok(ChaosWireAction::DelayResponse);
        }
        Ok(ChaosWireAction::Pass)
    }

    fn pop_delayed(&self) -> Result<Result<Vec<u8>>> {
        let mut state = lock(&self.state)?;
        state
            .delayed
            .pop_front()
            .ok_or_else(|| StorageError::unavailable("chaos transport has no delayed response"))
    }

    fn push_delayed(&self, response: Result<Vec<u8>>) -> Result<()> {
        lock(&self.state)?.delayed.push_back(response);
        Ok(())
    }
}

impl RemoteWireTransport for ChaosRemoteWireTransport {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self.take_action()? {
            ChaosWireAction::Pass => self.inner.call_wire(request_bytes),
            ChaosWireAction::Fail => Err(StorageError::unavailable("chaos transport failed call")),
            ChaosWireAction::DropRequest => Err(StorageError::unavailable(
                "chaos transport dropped request before send",
            )),
            ChaosWireAction::DropResponse => {
                let _ = self.inner.call_wire(request_bytes);
                Err(StorageError::unavailable(
                    "chaos transport dropped response after send",
                ))
            }
            ChaosWireAction::CorruptResponse => {
                let mut response = self.inner.call_wire(request_bytes)?;
                if let Some(first) = response.first_mut() {
                    *first ^= 0xff;
                } else {
                    response.push(0xff);
                }
                Ok(response)
            }
            ChaosWireAction::DuplicateRequest => {
                let first = self.inner.call_wire(request_bytes.clone());
                let second = self.inner.call_wire(request_bytes);
                match (first, second) {
                    (Ok(response), Ok(_)) => Ok(response),
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                }
            }
            ChaosWireAction::DelayResponse => {
                let response = self.inner.call_wire(request_bytes);
                self.push_delayed(response)?;
                Err(StorageError::unavailable(
                    "chaos transport delayed response after send",
                ))
            }
            ChaosWireAction::ReturnDelayed => self.pop_delayed()?,
            ChaosWireAction::ReorderWithDelayed => {
                let delayed = self.pop_delayed()?;
                let current = self.inner.call_wire(request_bytes);
                self.push_delayed(current)?;
                delayed
            }
        }
    }
}

#[derive(Debug)]
struct ChaosStorageNodeState {
    delayed: VecDeque<Result<StorageNodeResponse>>,
    trace: Vec<String>,
    metrics: ChaosTransportMetrics,
    fail_next_call: bool,
    drop_next_request: bool,
    drop_next_response: bool,
    corrupt_next_grant: bool,
    corrupt_next_receipt: bool,
    duplicate_next_request: bool,
    delay_next_response: bool,
    return_delayed_next: bool,
}

impl ChaosStorageNodeState {
    fn new() -> Self {
        Self {
            delayed: VecDeque::new(),
            trace: Vec::new(),
            metrics: ChaosTransportMetrics::default(),
            fail_next_call: false,
            drop_next_request: false,
            drop_next_response: false,
            corrupt_next_grant: false,
            corrupt_next_receipt: false,
            duplicate_next_request: false,
            delay_next_response: false,
            return_delayed_next: false,
        }
    }
}

/// Deterministic chaos wrapper for coordinator-to-storage-node messages.
///
/// Tests can inject drops, duplicates, delays, and proof corruption without
/// spawning background work or relying on wall-clock timing.
#[derive(Clone)]
pub struct ChaosStorageNodeTransport {
    inner: Arc<dyn StorageNodeTransport>,
    state: Arc<Mutex<ChaosStorageNodeState>>,
}

impl ChaosStorageNodeTransport {
    pub fn new(inner: Arc<dyn StorageNodeTransport>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ChaosStorageNodeState::new())),
        }
    }

    pub fn trace(&self) -> Result<Vec<String>> {
        Ok(lock(&self.state)?.trace.clone())
    }

    pub fn metrics(&self) -> Result<ChaosTransportMetrics> {
        Ok(lock(&self.state)?.metrics)
    }

    pub fn delayed_len(&self) -> Result<usize> {
        Ok(lock(&self.state)?.delayed.len())
    }

    pub fn fail_next_call(&self) -> Result<()> {
        lock(&self.state)?.fail_next_call = true;
        Ok(())
    }

    pub fn drop_next_request(&self) -> Result<()> {
        lock(&self.state)?.drop_next_request = true;
        Ok(())
    }

    pub fn drop_next_response(&self) -> Result<()> {
        lock(&self.state)?.drop_next_response = true;
        Ok(())
    }

    pub fn corrupt_next_grant(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_grant = true;
        Ok(())
    }

    pub fn corrupt_next_receipt(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_receipt = true;
        Ok(())
    }

    pub fn duplicate_next_request(&self) -> Result<()> {
        lock(&self.state)?.duplicate_next_request = true;
        Ok(())
    }

    pub fn delay_next_response(&self) -> Result<()> {
        lock(&self.state)?.delay_next_response = true;
        Ok(())
    }

    pub fn return_delayed_response_next_call(&self) -> Result<()> {
        lock(&self.state)?.return_delayed_next = true;
        Ok(())
    }

    fn mutate_request(request: &mut StorageNodeRequest) {
        if let StorageNodeRequest::WriteSegment { grant, .. } = request {
            grant.proof.0[0] ^= 0xff;
        }
    }

    fn mutate_response(response: &mut StorageNodeResponse) {
        if let StorageNodeResponse::WriteSegment { receipt } = response {
            receipt.proof.0[0] ^= 0xff;
        }
    }

    fn pop_delayed(&self) -> Result<Result<StorageNodeResponse>> {
        let mut state = lock(&self.state)?;
        state
            .delayed
            .pop_front()
            .ok_or_else(|| StorageError::unavailable("chaos storage node has no delayed response"))
    }
}

impl StorageNodeTransport for ChaosStorageNodeTransport {
    fn storage_node_id(&self) -> StorageNodeId {
        self.inner.storage_node_id()
    }

    fn send(&self, mut request: StorageNodeRequest) -> Result<StorageNodeResponse> {
        {
            let mut state = lock(&self.state)?;
            if state.fail_next_call {
                state.fail_next_call = false;
                state.metrics.injected_failures = state.metrics.injected_failures.saturating_add(1);
                state.trace.push("fail storage-node call".to_string());
                return Err(StorageError::unavailable("chaos storage node failed call"));
            }
            if state.drop_next_request {
                state.drop_next_request = false;
                state.metrics.request_drops = state.metrics.request_drops.saturating_add(1);
                state.trace.push("drop storage-node request".to_string());
                return Err(StorageError::unavailable(
                    "chaos storage node dropped request before send",
                ));
            }
            if state.return_delayed_next {
                state.return_delayed_next = false;
                state
                    .trace
                    .push("return delayed storage-node response".to_string());
                drop(state);
                return self.pop_delayed()?;
            }
            if state.corrupt_next_grant {
                state.corrupt_next_grant = false;
                state.metrics.corrupted_responses =
                    state.metrics.corrupted_responses.saturating_add(1);
                state.trace.push("corrupt storage-node grant".to_string());
                Self::mutate_request(&mut request);
            }
        }

        let mut response = if lock(&self.state)?.duplicate_next_request {
            {
                let mut state = lock(&self.state)?;
                state.duplicate_next_request = false;
                state.metrics.duplicated_requests =
                    state.metrics.duplicated_requests.saturating_add(1);
                state
                    .trace
                    .push("duplicate storage-node request".to_string());
            }
            let first = self.inner.send(request.clone());
            let second = self.inner.send(request);
            match (first, second) {
                (Ok(response), Ok(_)) => Ok(response),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }?
        } else {
            self.inner.send(request)?
        };

        let mut state = lock(&self.state)?;
        if state.drop_next_response {
            state.drop_next_response = false;
            state.metrics.response_drops = state.metrics.response_drops.saturating_add(1);
            state.trace.push("drop storage-node response".to_string());
            return Err(StorageError::unavailable(
                "chaos storage node dropped response after send",
            ));
        }
        if state.corrupt_next_receipt {
            state.corrupt_next_receipt = false;
            state.metrics.corrupted_responses = state.metrics.corrupted_responses.saturating_add(1);
            state.trace.push("corrupt storage-node receipt".to_string());
            Self::mutate_response(&mut response);
        }
        if state.delay_next_response {
            state.delay_next_response = false;
            state.metrics.delayed_responses = state.metrics.delayed_responses.saturating_add(1);
            state.trace.push("delay storage-node response".to_string());
            state.delayed.push_back(Ok(response));
            return Err(StorageError::unavailable(
                "chaos storage node delayed response after send",
            ));
        }
        Ok(response)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RemoteWireRequest<T> {
    incarnation: ServerIncarnation,
    envelope: T,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RemoteWireReply<T> {
    Ok {
        incarnation: ServerIncarnation,
        envelope: T,
    },
    Err {
        incarnation: ServerIncarnation,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteCacheEntry {
    request_bytes: Vec<u8>,
    response_bytes: Vec<u8>,
}

type RemoteRequestKey = (ServerIncarnation, ClientEpoch, RequestId);

#[derive(Debug)]
struct RemoteEndpointState {
    cache: BTreeMap<RemoteRequestKey, RemoteCacheEntry>,
    order: VecDeque<RemoteRequestKey>,
    in_flight: usize,
    shutdown: bool,
    logical_time: LogicalTime,
}

impl RemoteEndpointState {
    fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            order: VecDeque::new(),
            in_flight: 0,
            shutdown: false,
            logical_time: LogicalTime::from_raw(0),
        }
    }
}

/// Deterministic remote-capable block endpoint.
#[derive(Clone)]
pub struct RemoteBlockEndpoint {
    server: Arc<dyn BlockServer>,
    incarnation: ServerIncarnation,
    dedupe_capacity: usize,
    mailbox_capacity: usize,
    state: Arc<Mutex<RemoteEndpointState>>,
}

impl RemoteBlockEndpoint {
    pub fn new(
        server: Arc<dyn BlockServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            server,
            incarnation,
            dedupe_capacity,
            mailbox_capacity,
            state: Arc::new(Mutex::new(RemoteEndpointState::new())),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.incarnation
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        lock(&self.state)?.shutdown = shutdown;
        Ok(())
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        lock(&self.state)?.logical_time = logical_time;
        Ok(())
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<BlockRequestEnvelope> =
            bincode::deserialize(request_bytes).map_err(serde_error)?;
        if wire.incarnation != self.incarnation {
            return self.encode_error("stale block server incarnation");
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.state)?.logical_time.raw()
        {
            return self.encode_error("block request deadline expired");
        }
        let key = (
            self.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.state)?;
            if state.shutdown {
                return self.encode_error("block endpoint is shut down");
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return self.encode_error(
                    "request ID and client epoch reused for a different remote block request",
                );
            }
            if state.in_flight >= self.mailbox_capacity {
                return self.encode_error("block endpoint mailbox is full");
            }
            state.in_flight += 1;
        }

        let response = self.server.handle(wire.envelope);
        {
            let mut state = lock(&self.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => bincode::serialize(&RemoteWireReply::Ok {
                incarnation: self.incarnation,
                envelope,
            })
            .map_err(serde_error)?,
            Err(error) => bincode::serialize(&RemoteWireReply::<BlockResponseEnvelope>::Err {
                incarnation: self.incarnation,
                reason: error.to_string(),
            })
            .map_err(serde_error)?,
        };

        let mut state = lock(&self.state)?;
        if self.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }

    fn encode_error(&self, reason: impl Into<String>) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireReply::<BlockResponseEnvelope>::Err {
            incarnation: self.incarnation,
            reason: reason.into(),
        })
        .map_err(serde_error)
    }
}

impl RemoteWireTransport for RemoteBlockEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Phase 17 serialized block transport over a deterministic remote endpoint.
///
/// This deliberately remains an in-process test/model transport. The Phase 19
/// TCP path uses `NetworkBlockTransport` and the crate-owned network codec.
#[derive(Clone)]
pub struct RemoteBlockTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl RemoteBlockTransport {
    pub fn new(endpoint: Arc<RemoteBlockEndpoint>) -> Self {
        Self::with_wire(endpoint.clone(), endpoint.incarnation())
    }

    pub fn with_wire(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    fn encode_request(&self, request: BlockRequestEnvelope) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireRequest {
            incarnation: self.incarnation,
            envelope: request,
        })
        .map_err(serde_error)
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<BlockResponseEnvelope> {
        let reply: RemoteWireReply<BlockResponseEnvelope> =
            bincode::deserialize(bytes).map_err(serde_error)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "remote block response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
        }
    }
}

impl BlockTransport for RemoteBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

/// Deterministic remote-capable native endpoint.
#[derive(Clone)]
pub struct RemoteNativeEndpoint {
    server: Arc<dyn NativeServer>,
    incarnation: ServerIncarnation,
    dedupe_capacity: usize,
    mailbox_capacity: usize,
    state: Arc<Mutex<RemoteEndpointState>>,
}

impl RemoteNativeEndpoint {
    pub fn new(
        server: Arc<dyn NativeServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            server,
            incarnation,
            dedupe_capacity,
            mailbox_capacity,
            state: Arc::new(Mutex::new(RemoteEndpointState::new())),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.incarnation
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        lock(&self.state)?.shutdown = shutdown;
        Ok(())
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        lock(&self.state)?.logical_time = logical_time;
        Ok(())
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<NativeRequestEnvelope> =
            bincode::deserialize(request_bytes).map_err(serde_error)?;
        if wire.incarnation != self.incarnation {
            return self.encode_error("stale native server incarnation");
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.state)?.logical_time.raw()
        {
            return self.encode_error("native request deadline expired");
        }
        let key = (
            self.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.state)?;
            if state.shutdown {
                return self.encode_error("native endpoint is shut down");
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return self.encode_error(
                    "request ID and client epoch reused for a different remote native request",
                );
            }
            if state.in_flight >= self.mailbox_capacity {
                return self.encode_error("native endpoint mailbox is full");
            }
            state.in_flight += 1;
        }

        let response = self.server.handle(wire.envelope);
        {
            let mut state = lock(&self.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => bincode::serialize(&RemoteWireReply::Ok {
                incarnation: self.incarnation,
                envelope,
            })
            .map_err(serde_error)?,
            Err(error) => bincode::serialize(&RemoteWireReply::<NativeResponseEnvelope>::Err {
                incarnation: self.incarnation,
                reason: error.to_string(),
            })
            .map_err(serde_error)?,
        };

        let mut state = lock(&self.state)?;
        if self.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }

    fn encode_error(&self, reason: impl Into<String>) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireReply::<NativeResponseEnvelope>::Err {
            incarnation: self.incarnation,
            reason: reason.into(),
        })
        .map_err(serde_error)
    }
}

impl RemoteWireTransport for RemoteNativeEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Phase 17 serialized native transport over a deterministic remote endpoint.
///
/// This deliberately remains an in-process test/model transport. The Phase 19
/// TCP path uses `NetworkNativeTransport` and the crate-owned network codec.
#[derive(Clone)]
pub struct RemoteNativeTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl RemoteNativeTransport {
    pub fn new(endpoint: Arc<RemoteNativeEndpoint>) -> Self {
        Self::with_wire(endpoint.clone(), endpoint.incarnation())
    }

    pub fn with_wire(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    fn encode_request(&self, request: NativeRequestEnvelope) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireRequest {
            incarnation: self.incarnation,
            envelope: request,
        })
        .map_err(serde_error)
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<NativeResponseEnvelope> {
        let reply: RemoteWireReply<NativeResponseEnvelope> =
            bincode::deserialize(bytes).map_err(serde_error)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "remote native response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
        }
    }
}

impl NativeTransport for RemoteNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

const NETWORK_WIRE_MAGIC: &[u8; 8] = b"TCOWWIRE";
const NETWORK_WIRE_VERSION: u16 = 1;
const NETWORK_BLOCK_REQUEST: u8 = 1;
const NETWORK_BLOCK_RESPONSE: u8 = 2;
const NETWORK_NATIVE_REQUEST: u8 = 3;
const NETWORK_NATIVE_RESPONSE: u8 = 4;
const DEFAULT_NETWORK_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn network_codec_error(reason: impl Into<String>) -> StorageError {
    StorageError::corrupt(format!("network wire codec failed: {}", reason.into()))
}

fn encode_network_frame<T: DurableCodec>(kind: u8, value: &T) -> Result<Vec<u8>> {
    let mut out = DurableEncoder { bytes: Vec::new() };
    out.bytes.extend_from_slice(NETWORK_WIRE_MAGIC);
    out.put_u16(NETWORK_WIRE_VERSION);
    out.put_u8(kind);
    value.encode(&mut out)?;
    Ok(out.finish())
}

fn decode_network_frame<T: DurableCodec>(expected_kind: u8, bytes: &[u8]) -> Result<T> {
    let mut input = DurableDecoder { bytes, offset: 0 };
    let magic = input.take(NETWORK_WIRE_MAGIC.len())?;
    if magic != NETWORK_WIRE_MAGIC {
        return Err(network_codec_error("bad frame magic"));
    }
    let version = input.u16()?;
    if version != NETWORK_WIRE_VERSION {
        return Err(network_codec_error("unsupported frame version"));
    }
    let kind = input.u8()?;
    if kind != expected_kind {
        return Err(network_codec_error("frame kind mismatch"));
    }
    let value = T::decode(&mut input)?;
    input
        .finish()
        .map_err(|_| network_codec_error("trailing bytes in frame"))?;
    Ok(value)
}

fn encode_network_block_error(
    incarnation: ServerIncarnation,
    reason: impl Into<String>,
) -> Result<Vec<u8>> {
    encode_network_frame(
        NETWORK_BLOCK_RESPONSE,
        &RemoteWireReply::<BlockResponseEnvelope>::Err {
            incarnation,
            reason: reason.into(),
        },
    )
}

fn encode_network_native_error(
    incarnation: ServerIncarnation,
    reason: impl Into<String>,
) -> Result<Vec<u8>> {
    encode_network_frame(
        NETWORK_NATIVE_RESPONSE,
        &RemoteWireReply::<NativeResponseEnvelope>::Err {
            incarnation,
            reason: reason.into(),
        },
    )
}

/// Network block endpoint using the crate-owned Phase 19 wire codec.
#[derive(Clone)]
pub struct NetworkBlockEndpoint {
    inner: RemoteBlockEndpoint,
}

impl NetworkBlockEndpoint {
    pub fn new(
        server: Arc<dyn BlockServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            inner: RemoteBlockEndpoint::new(server, incarnation, dedupe_capacity, mailbox_capacity),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.inner.incarnation()
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        self.inner.set_shutdown(shutdown)
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        self.inner.set_logical_time(logical_time)
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<BlockRequestEnvelope> =
            match decode_network_frame(NETWORK_BLOCK_REQUEST, request_bytes) {
                Ok(wire) => wire,
                Err(error) => {
                    return encode_network_block_error(self.inner.incarnation, error.to_string());
                }
            };
        if wire.incarnation != self.inner.incarnation {
            return encode_network_block_error(
                self.inner.incarnation,
                "stale block server incarnation",
            );
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.inner.state)?.logical_time.raw()
        {
            return encode_network_block_error(
                self.inner.incarnation,
                "block request deadline expired",
            );
        }
        let key = (
            self.inner.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.inner.state)?;
            if state.shutdown {
                return encode_network_block_error(
                    self.inner.incarnation,
                    "block endpoint is shut down",
                );
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return encode_network_block_error(
                    self.inner.incarnation,
                    "request ID and client epoch reused for a different network block request",
                );
            }
            if state.in_flight >= self.inner.mailbox_capacity {
                return encode_network_block_error(
                    self.inner.incarnation,
                    "block endpoint mailbox is full",
                );
            }
            state.in_flight += 1;
        }

        let response = self.inner.server.handle(wire.envelope);
        {
            let mut state = lock(&self.inner.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => encode_network_frame(
                NETWORK_BLOCK_RESPONSE,
                &RemoteWireReply::Ok {
                    incarnation: self.inner.incarnation,
                    envelope,
                },
            )?,
            Err(error) => encode_network_block_error(self.inner.incarnation, error.to_string())?,
        };

        let mut state = lock(&self.inner.state)?;
        if self.inner.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.inner.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }
}

impl RemoteWireTransport for NetworkBlockEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Network native endpoint using the crate-owned Phase 19 wire codec.
#[derive(Clone)]
pub struct NetworkNativeEndpoint {
    inner: RemoteNativeEndpoint,
}

impl NetworkNativeEndpoint {
    pub fn new(
        server: Arc<dyn NativeServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            inner: RemoteNativeEndpoint::new(
                server,
                incarnation,
                dedupe_capacity,
                mailbox_capacity,
            ),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.inner.incarnation()
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        self.inner.set_shutdown(shutdown)
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        self.inner.set_logical_time(logical_time)
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<NativeRequestEnvelope> =
            match decode_network_frame(NETWORK_NATIVE_REQUEST, request_bytes) {
                Ok(wire) => wire,
                Err(error) => {
                    return encode_network_native_error(self.inner.incarnation, error.to_string());
                }
            };
        if wire.incarnation != self.inner.incarnation {
            return encode_network_native_error(
                self.inner.incarnation,
                "stale native server incarnation",
            );
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.inner.state)?.logical_time.raw()
        {
            return encode_network_native_error(
                self.inner.incarnation,
                "native request deadline expired",
            );
        }
        let key = (
            self.inner.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.inner.state)?;
            if state.shutdown {
                return encode_network_native_error(
                    self.inner.incarnation,
                    "native endpoint is shut down",
                );
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return encode_network_native_error(
                    self.inner.incarnation,
                    "request ID and client epoch reused for a different network native request",
                );
            }
            if state.in_flight >= self.inner.mailbox_capacity {
                return encode_network_native_error(
                    self.inner.incarnation,
                    "native endpoint mailbox is full",
                );
            }
            state.in_flight += 1;
        }

        let response = self.inner.server.handle(wire.envelope);
        {
            let mut state = lock(&self.inner.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => encode_network_frame(
                NETWORK_NATIVE_RESPONSE,
                &RemoteWireReply::Ok {
                    incarnation: self.inner.incarnation,
                    envelope,
                },
            )?,
            Err(error) => encode_network_native_error(self.inner.incarnation, error.to_string())?,
        };

        let mut state = lock(&self.inner.state)?;
        if self.inner.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.inner.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }
}

impl RemoteWireTransport for NetworkNativeEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Block transport over a real network-capable wire transport.
#[derive(Clone)]
pub struct NetworkBlockTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl NetworkBlockTransport {
    pub fn new(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    pub fn tcp(addr: SocketAddr, incarnation: ServerIncarnation) -> Self {
        Self::new(
            Arc::new(TcpRemoteWireTransport::new(
                addr,
                DEFAULT_NETWORK_MAX_FRAME_BYTES,
            )),
            incarnation,
        )
    }

    fn encode_request(&self, request: BlockRequestEnvelope) -> Result<Vec<u8>> {
        encode_network_frame(
            NETWORK_BLOCK_REQUEST,
            &RemoteWireRequest {
                incarnation: self.incarnation,
                envelope: request,
            },
        )
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<BlockResponseEnvelope> {
        let reply: RemoteWireReply<BlockResponseEnvelope> =
            decode_network_frame(NETWORK_BLOCK_RESPONSE, bytes)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "network block response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
        }
    }
}

impl BlockTransport for NetworkBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

/// Native transport over a real network-capable wire transport.
#[derive(Clone)]
pub struct NetworkNativeTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl NetworkNativeTransport {
    pub fn new(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    pub fn tcp(addr: SocketAddr, incarnation: ServerIncarnation) -> Self {
        Self::new(
            Arc::new(TcpRemoteWireTransport::new(
                addr,
                DEFAULT_NETWORK_MAX_FRAME_BYTES,
            )),
            incarnation,
        )
    }

    fn encode_request(&self, request: NativeRequestEnvelope) -> Result<Vec<u8>> {
        encode_network_frame(
            NETWORK_NATIVE_REQUEST,
            &RemoteWireRequest {
                incarnation: self.incarnation,
                envelope: request,
            },
        )
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<NativeResponseEnvelope> {
        let reply: RemoteWireReply<NativeResponseEnvelope> =
            decode_network_frame(NETWORK_NATIVE_RESPONSE, bytes)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "network native response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
        }
    }
}

impl NativeTransport for NetworkNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

/// TCP implementation of the opaque `RemoteWireTransport` byte pipe.
#[derive(Clone)]
pub struct TcpRemoteWireTransport {
    addr: SocketAddr,
    max_frame_bytes: usize,
    timeout: Duration,
}

impl TcpRemoteWireTransport {
    pub fn new(addr: SocketAddr, max_frame_bytes: usize) -> Self {
        Self {
            addr,
            max_frame_bytes,
            timeout: Duration::from_secs(5),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl RemoteWireTransport for TcpRemoteWireTransport {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream =
            TcpStream::connect_timeout(&self.addr, self.timeout).map_err(network_io_error)?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(network_io_error)?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(network_io_error)?;
        write_tcp_frame(&mut stream, &request_bytes, self.max_frame_bytes)?;
        read_tcp_frame(&mut stream, self.max_frame_bytes)
    }
}

/// Small blocking TCP server for Phase 19 loopback/network testing.
pub struct TcpRemoteWireServer {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl TcpRemoteWireServer {
    pub fn start(
        listener: TcpListener,
        endpoint: Arc<dyn RemoteWireTransport>,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        let local_addr = listener.local_addr().map_err(network_io_error)?;
        listener.set_nonblocking(true).map_err(network_io_error)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stream.set_nonblocking(false).is_err() {
                            continue;
                        }
                        let response = read_tcp_frame(&mut stream, max_frame_bytes)
                            .and_then(|request| endpoint.call_wire(request));
                        if let Ok(response) = response {
                            let _ = write_tcp_frame(&mut stream, &response, max_frame_bytes);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            local_addr,
            shutdown,
            handle: Mutex::new(Some(handle)),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.local_addr);
        if let Some(handle) = lock(&self.handle)?.take() {
            handle
                .join()
                .map_err(|_| StorageError::unavailable("network server thread panicked"))?;
        }
        Ok(())
    }
}

impl Drop for TcpRemoteWireServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn write_tcp_frame(stream: &mut TcpStream, bytes: &[u8], max_frame_bytes: usize) -> Result<()> {
    if bytes.len() > max_frame_bytes {
        return Err(StorageError::invalid_argument(
            "network frame exceeds limit",
        ));
    }
    let len = u32::try_from(bytes.len())
        .map_err(|_| StorageError::invalid_argument("network frame length exceeds u32"))?;
    stream
        .write_all(&len.to_be_bytes())
        .map_err(network_io_error)?;
    stream.write_all(bytes).map_err(network_io_error)
}

fn read_tcp_frame(stream: &mut TcpStream, max_frame_bytes: usize) -> Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).map_err(network_io_error)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > max_frame_bytes {
        return Err(StorageError::invalid_argument(
            "network frame exceeds limit",
        ));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).map_err(network_io_error)?;
    Ok(bytes)
}

/// Local `BlockClient` backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockClient {
    transport: Arc<dyn BlockTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockClient {
    pub fn new(transport: InProcessBlockTransport) -> Self {
        Self::with_transport(Arc::new(transport))
    }

    pub fn with_transport(transport: Arc<dyn BlockTransport>) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_device(&self, device_id: DeviceId) -> Result<LocalBlockDevice> {
        self.device_info(device_id)?;
        Ok(LocalBlockDevice {
            device_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockClient for LocalBlockClient {
    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Create { request },
        ))?;
        match response.response {
            BlockResponse::Created(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected create-device response")),
        }
    }

    fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info { device_id },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }
}

/// Local `BlockDevice` handle backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockDevice {
    device_id: DeviceId,
    transport: Arc<dyn BlockTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockDevice {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockDevice for LocalBlockDevice {
    fn device_id(&self) -> DeviceId {
        self.device_id
    }

    fn info(&self) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Read {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected block-read response")),
        }
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Write {
                device_id: self.device_id,
                offset,
                bytes: data.to_vec(),
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected block-write response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Flush {
                device_id: self.device_id,
                scope: crate::api::FlushScope::Device,
            },
        ))?;
        match response.response {
            BlockResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected block-flush response")),
        }
    }

    fn write_zeroes(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::WriteZeroes {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected write-zeroes response")),
        }
    }

    fn discard(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Discard {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected discard response")),
        }
    }

    fn fork(&self, request: ForkRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Fork {
                source: self.device_id,
                request,
            },
        ))?;
        match response.response {
            BlockResponse::Forked(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected fork response")),
        }
    }

    fn restore(&self, point: RestorePoint) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Restore {
                source: self.device_id,
                point,
            },
        ))?;
        match response.response {
            BlockResponse::Restored(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected restore response")),
        }
    }

    fn delete(&self) -> Result<DeleteResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Delete {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Deleted(delete) => Ok(delete),
            _ => Err(StorageError::corrupt("unexpected block-delete response")),
        }
    }
}

/// Local `NativeKeyspaceClient` backed by a native transport.
#[derive(Clone)]
pub struct LocalNativeClient {
    transport: Arc<dyn NativeTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeClient {
    pub fn new(transport: InProcessNativeTransport) -> Self {
        Self::with_transport(Arc::new(transport))
    }

    pub fn with_transport(transport: Arc<dyn NativeTransport>) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<LocalNativeFile> {
        self.file_info(keyspace_id, file_id)?;
        Ok(LocalNativeFile {
            keyspace_id,
            file_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeKeyspaceClient for LocalNativeClient {
    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CreateKeyspace { request },
        ))?;
        match response.response {
            NativeResponse::KeyspaceCreated(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt("unexpected create-keyspace response")),
        }
    }

    fn keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::KeyspaceInfo { keyspace_id },
        ))?;
        match response.response {
            NativeResponse::KeyspaceInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected keyspace-info response")),
        }
    }

    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CreateFile {
                keyspace_id,
                request,
            },
        ))?;
        match response.response {
            NativeResponse::FileCreated(file_id) => Ok(file_id),
            _ => Err(StorageError::corrupt("unexpected create-file response")),
        }
    }

    fn file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo {
                keyspace_id,
                file_id,
            },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn open_append_session(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendSession> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::OpenAppendSession {
                keyspace_id,
                file_id,
            },
        ))?;
        match response.response {
            NativeResponse::AppendSession(session) => Ok(session),
            _ => Err(StorageError::corrupt("unexpected append-session response")),
        }
    }

    fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CheckpointKeyspace { keyspace_id },
        ))?;
        match response.response {
            NativeResponse::KeyspaceCheckpointed(checkpoint_id) => Ok(checkpoint_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-checkpoint response",
            )),
        }
    }

    fn snapshot_keyspace(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::SnapshotKeyspace { source, request },
        ))?;
        match response.response {
            NativeResponse::KeyspaceSnapshotted(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-snapshot response",
            )),
        }
    }

    fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::RestoreKeyspace { source, point },
        ))?;
        match response.response {
            NativeResponse::KeyspaceRestored(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-restore response",
            )),
        }
    }
}

/// Local `NativeFile` handle backed by a native keyspace/file transport.
#[derive(Clone)]
pub struct LocalNativeFile {
    keyspace_id: KeyspaceId,
    file_id: FileId,
    transport: Arc<dyn NativeTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeFile {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeFile for LocalNativeFile {
    fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn info(&self) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Read {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            NativeResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected native-read response")),
        }
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> Result<FileWriteCommit> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Write {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                offset,
                bytes: data.to_vec(),
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            NativeResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected native-write response")),
        }
    }

    fn open_append_session(&self) -> Result<AppendSession> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::OpenAppendSession {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::AppendSession(session) => Ok(session),
            _ => Err(StorageError::corrupt("unexpected append-session response")),
        }
    }

    fn reserve_append(&self, session: &AppendSession, len: u64) -> Result<AppendReservation> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::ReserveAppend {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                session: session.clone(),
                len,
            },
        ))?;
        match response.response {
            NativeResponse::AppendReservation(reservation) => Ok(reservation),
            _ => Err(StorageError::corrupt(
                "unexpected append-reservation response",
            )),
        }
    }

    fn append_reserved(&self, reservation: AppendReservation, data: &[u8]) -> Result<AppendCommit> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::AppendReserved {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                reservation,
                bytes: data.to_vec(),
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            NativeResponse::Append(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected native-append response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Flush {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected native-flush response")),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))
}

fn server_lock_stripes() -> Vec<Mutex<()>> {
    (0..SERVER_LOCK_STRIPES).map(|_| Mutex::new(())).collect()
}

fn stripe_for_raw(raw: u128) -> usize {
    (raw % SERVER_LOCK_STRIPES as u128) as usize
}

fn block_request_stripe(request: &BlockRequest) -> usize {
    request
        .target_device_id()
        .map(|device_id| stripe_for_raw(device_id.raw()))
        .unwrap_or(0)
}

fn native_request_stripe(request: &NativeRequest) -> usize {
    match (request.target_keyspace_id(), request.target_file_id()) {
        (Some(keyspace_id), Some(file_id)) => {
            stripe_for_raw(keyspace_id.raw().wrapping_mul(1_099_511_628_211) ^ file_id.raw())
        }
        (Some(keyspace_id), None) => stripe_for_raw(keyspace_id.raw()),
        (None, _) => 0,
    }
}

fn fs_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("filesystem operation failed: {error}"))
}

fn network_io_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("network I/O failed: {error}"))
}

fn serde_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::corrupt(format!("binary envelope codec failed: {error}"))
}

fn sqlite_error(error: rusqlite::Error) -> StorageError {
    StorageError::unavailable(format!("sqlite operation failed: {error}"))
}

fn durable_codec_error(reason: impl Into<String>) -> StorageError {
    StorageError::corrupt(format!("durable codec failed: {}", reason.into()))
}

#[cfg(test)]
const DURABLE_TEST_IMAGE_MAGIC: &[u8; 8] = b"TCOWIMG!";
#[cfg(test)]
const DURABLE_TEST_IMAGE_VERSION: u16 = 1;
#[cfg(test)]
const DURABLE_TEST_IMAGE_METADATA: u8 = 1;
#[cfg(test)]
const DURABLE_TEST_IMAGE_CATALOG: u8 = 2;
#[cfg(test)]
const DURABLE_TEST_IMAGE_SEGMENT_STORE: u8 = 3;
const MAX_DURABLE_COLLECTION_LEN: u64 = 1_000_000;
const MAX_DURABLE_STRING_LEN: u64 = 1_048_576;

trait DurableCodec: Sized {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()>;
    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self>;
}

#[cfg(test)]
trait DurableTestImageCodec: DurableCodec {
    const IMAGE_KIND: u8;
}

#[derive(Debug, Default)]
struct DurableEncoder {
    bytes: Vec<u8>,
}

impl DurableEncoder {
    #[cfg(test)]
    fn new(kind: u8) -> Self {
        let mut encoder = Self { bytes: Vec::new() };
        encoder.bytes.extend_from_slice(DURABLE_TEST_IMAGE_MAGIC);
        encoder.put_u16(DURABLE_TEST_IMAGE_VERSION);
        encoder.put_u8(kind);
        encoder
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}

struct DurableDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> DurableDecoder<'a> {
    #[cfg(test)]
    fn new(bytes: &'a [u8], expected_kind: u8) -> Result<Self> {
        let mut decoder = Self { bytes, offset: 0 };
        let magic = decoder.take(DURABLE_TEST_IMAGE_MAGIC.len())?;
        if magic != DURABLE_TEST_IMAGE_MAGIC {
            return Err(durable_codec_error("bad test image magic"));
        }
        let version = decoder.u16()?;
        if version != DURABLE_TEST_IMAGE_VERSION {
            return Err(durable_codec_error("unsupported test image version"));
        }
        let kind = decoder.u8()?;
        if kind != expected_kind {
            return Err(durable_codec_error("test image kind mismatch"));
        }
        Ok(decoder)
    }

    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(durable_codec_error("trailing bytes in durable buffer"))
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| durable_codec_error("durable buffer offset overflow"))?;
        if end > self.bytes.len() {
            return Err(durable_codec_error("unexpected end of durable buffer"));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(*self
            .take(1)?
            .first()
            .ok_or_else(|| durable_codec_error("unexpected end of durable buffer"))?)
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u16"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u32"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u64"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn u128(&mut self) -> Result<u128> {
        let bytes: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u128"))?;
        Ok(u128::from_be_bytes(bytes))
    }
}

impl DurableCodec for bool {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u8(u8::from(*self));
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match input.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(durable_codec_error("invalid bool tag")),
        }
    }
}

impl DurableCodec for u8 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u8(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u8()
    }
}

impl DurableCodec for u32 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u32(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u32()
    }
}

impl DurableCodec for u64 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u64(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u64()
    }
}

impl DurableCodec for u128 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u128(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u128()
    }
}

impl DurableCodec for usize {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let value =
            u64::try_from(*self).map_err(|_| durable_codec_error("usize value exceeds u64"))?;
        value.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        usize::try_from(u64::decode(input)?)
            .map_err(|_| durable_codec_error("usize value exceeds platform size"))
    }
}

impl<T: DurableCodec> DurableCodec for Option<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Some(value) => {
                out.put_u8(1);
                value.encode(out)
            }
            None => {
                out.put_u8(0);
                Ok(())
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match input.u8()? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(input)?)),
            _ => Err(durable_codec_error("invalid option tag")),
        }
    }
}

impl<T: DurableCodec> DurableCodec for Vec<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let len = u64::try_from(self.len())
            .map_err(|_| durable_codec_error("vector length exceeds u64"))?;
        len.encode(out)?;
        for value in self {
            value.encode(out)?;
        }
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_COLLECTION_LEN {
            return Err(durable_codec_error("vector length exceeds durable limit"));
        }
        let len =
            usize::try_from(len).map_err(|_| durable_codec_error("vector length overflow"))?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(T::decode(input)?);
        }
        Ok(values)
    }
}

impl<K, V> DurableCodec for BTreeMap<K, V>
where
    K: DurableCodec + Ord,
    V: DurableCodec,
{
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let len =
            u64::try_from(self.len()).map_err(|_| durable_codec_error("map length exceeds u64"))?;
        len.encode(out)?;
        for (key, value) in self {
            key.encode(out)?;
            value.encode(out)?;
        }
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_COLLECTION_LEN {
            return Err(durable_codec_error("map length exceeds durable limit"));
        }
        let mut values = BTreeMap::new();
        for _ in 0..len {
            let key = K::decode(input)?;
            let value = V::decode(input)?;
            if values.insert(key, value).is_some() {
                return Err(durable_codec_error("duplicate key in durable map"));
            }
        }
        Ok(values)
    }
}

impl DurableCodec for String {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let bytes = self.as_bytes();
        let len = u64::try_from(bytes.len())
            .map_err(|_| durable_codec_error("string length exceeds u64"))?;
        len.encode(out)?;
        out.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_STRING_LEN {
            return Err(durable_codec_error("string length exceeds durable limit"));
        }
        let len =
            usize::try_from(len).map_err(|_| durable_codec_error("string length overflow"))?;
        let bytes = input.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| durable_codec_error("invalid UTF-8 string"))
    }
}

impl DurableCodec for (KeyspaceId, FileId) {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.0.encode(out)?;
        self.1.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok((KeyspaceId::decode(input)?, FileId::decode(input)?))
    }
}

macro_rules! durable_id_codec_u128 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u128::decode(input)?))
                }
            }
        )+
    };
}

macro_rules! durable_id_codec_u64 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u64::decode(input)?))
                }
            }
        )+
    };
}

macro_rules! durable_id_codec_u32 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u32::decode(input)?))
                }
            }
        )+
    };
}

durable_id_codec_u128!(
    AppendReservationId,
    AppendSessionId,
    CheckpointId,
    CommitGroupId,
    DeviceId,
    ExtentId,
    FileId,
    GrantId,
    GrantNonce,
    KeyspaceCatalogShardId,
    KeyspaceId,
    KeyspaceRootId,
    MetadataNodeId,
    PrincipalId,
    RequestId,
    SegmentId,
    StorageNodeId,
    StorageNodeKeyId,
    TenantId,
    WriteIntentId,
);

durable_id_codec_u64!(
    BlockCount,
    BlockIndex,
    ClientEpoch,
    CommitSeq,
    DeviceGeneration,
    FileVersion,
    GrantEpoch,
    KeyspaceGeneration,
    LogicalDeadline,
    LogicalTime,
    ServerIncarnation,
    WriterEpoch,
);

durable_id_codec_u32!(crate::id::ShardId);

impl DurableCodec for LocalStoreConfig {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_count.encode(out)?;
        self.block_size.encode(out)?;
        self.file_root_blocks.encode(out)?;
        self.metadata_fanout.encode(out)?;
        self.metadata_leaf_blocks.encode(out)?;
        self.storage_node.encode(out)?;
        self.observability_event_capacity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_count: usize::decode(input)?,
            block_size: u32::decode(input)?,
            file_root_blocks: u64::decode(input)?,
            metadata_fanout: usize::decode(input)?,
            metadata_leaf_blocks: u64::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            observability_event_capacity: usize::decode(input)?,
        })
    }
}

impl DurableCodec for crate::api::DeviceSpec {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.logical_blocks.encode(out)?;
        self.block_size.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            logical_blocks: u64::decode(input)?,
            block_size: u32::decode(input)?,
        })
    }
}

impl DurableCodec for ByteRange {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.offset.encode(out)?;
        self.len.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            offset: u64::decode(input)?,
            len: u64::decode(input)?,
        })
    }
}

impl DurableCodec for crate::api::BlockRange {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.start.encode(out)?;
        self.blocks.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            start: BlockIndex::decode(input)?,
            blocks: BlockCount::decode(input)?,
        })
    }
}

impl DurableCodec for MappingOwner {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockDevice(device_id) => {
                1u8.encode(out)?;
                device_id.encode(out)
            }
            Self::NativeKeyspace(keyspace_id) => {
                2u8.encode(out)?;
                keyspace_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockDevice(DeviceId::decode(input)?)),
            2 => Ok(Self::NativeKeyspace(KeyspaceId::decode(input)?)),
            _ => Err(durable_codec_error("invalid mapping owner tag")),
        }
    }
}

impl DurableCodec for DeviceHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.generation.encode(out)?;
        self.shard_roots.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            generation: DeviceGeneration::decode(input)?,
            shard_roots: Vec::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.generation.encode(out)?;
        self.root.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            generation: KeyspaceGeneration::decode(input)?,
            root: KeyspaceRootId::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for FileHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.file_id.encode(out)?;
        self.version.encode(out)?;
        self.root.encode(out)?;
        self.size.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            file_id: FileId::decode(input)?,
            version: FileVersion::decode(input)?,
            root: MetadataNodeId::decode(input)?,
            size: u64::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceFile {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)?;
        self.head.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
            head: FileHead::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceCatalogShard {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_id.encode(out)?;
        self.files.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_id: KeyspaceCatalogShardId::decode(input)?,
            files: BTreeMap::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceRoot {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.root_id.encode(out)?;
        self.shard_roots.encode(out)?;
        self.file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            root_id: KeyspaceRootId::decode(input)?,
            shard_roots: Vec::decode(input)?,
            file_count: usize::decode(input)?,
        })
    }
}

impl DurableCodec for MetadataChild {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.range.encode(out)?;
        self.node_id.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            range: crate::api::BlockRange::decode(input)?,
            node_id: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for LeafEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.logical_start.encode(out)?;
        self.blocks.encode(out)?;
        self.segment_id.encode(out)?;
        self.segment_offset.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            logical_start: BlockIndex::decode(input)?,
            blocks: BlockCount::decode(input)?,
            segment_id: SegmentId::decode(input)?,
            segment_offset: BlockIndex::decode(input)?,
        })
    }
}

impl DurableCodec for MetadataNodeKind {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Internal { children } => {
                1u8.encode(out)?;
                children.encode(out)
            }
            Self::Leaf { entries } => {
                2u8.encode(out)?;
                entries.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Internal {
                children: Vec::decode(input)?,
            }),
            2 => Ok(Self::Leaf {
                entries: Vec::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid metadata node kind tag")),
        }
    }
}

impl DurableCodec for MetadataNode {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.node_id.encode(out)?;
        self.covered_range.encode(out)?;
        self.kind.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            node_id: MetadataNodeId::decode(input)?,
            covered_range: crate::api::BlockRange::decode(input)?,
            kind: MetadataNodeKind::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentDescriptor {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.blocks.encode(out)?;
        self.bytes.encode(out)?;
        self.checksum.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            blocks: BlockCount::decode(input)?,
            bytes: u64::decode(input)?,
            checksum: Option::decode(input)?,
        })
    }
}

impl DurableCodec for ShardRootUpdate {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_id: crate::id::ShardId::decode(input)?,
            old_root: MetadataNodeId::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for RootUpdate {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockShard(update) => {
                1u8.encode(out)?;
                update.encode(out)
            }
            Self::FileCreated {
                file_id,
                new_root,
                new_size,
            } => {
                2u8.encode(out)?;
                file_id.encode(out)?;
                new_root.encode(out)?;
                new_size.encode(out)
            }
            Self::FileRoot {
                file_id,
                old_root,
                new_root,
                new_size,
            } => {
                3u8.encode(out)?;
                file_id.encode(out)?;
                old_root.encode(out)?;
                new_root.encode(out)?;
                new_size.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockShard(ShardRootUpdate::decode(input)?)),
            2 => Ok(Self::FileCreated {
                file_id: FileId::decode(input)?,
                new_root: MetadataNodeId::decode(input)?,
                new_size: u64::decode(input)?,
            }),
            3 => Ok(Self::FileRoot {
                file_id: FileId::decode(input)?,
                old_root: MetadataNodeId::decode(input)?,
                new_root: MetadataNodeId::decode(input)?,
                new_size: u64::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid root update tag")),
        }
    }
}

impl DurableCodec for CommitGroup {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_group.encode(out)?;
        self.commit_seq.encode(out)?;
        self.owner.encode(out)?;
        self.updates.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_group: CommitGroupId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            owner: MappingOwner::decode(input)?,
            updates: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for ForkRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.source.encode(out)?;
        self.target.encode(out)?;
        self.shard_roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            source: DeviceId::decode(input)?,
            target: DeviceId::decode(input)?,
            shard_roots: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for ShardCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.device_id.encode(out)?;
        self.shard_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            device_id: DeviceId::decode(input)?,
            shard_id: crate::id::ShardId::decode(input)?,
            old_root: MetadataNodeId::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            old_root: KeyspaceRootId::decode(input)?,
            new_root: KeyspaceRootId::decode(input)?,
        })
    }
}

impl DurableCodec for FileCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)?;
        self.old_version.encode(out)?;
        self.new_version.encode(out)?;
        self.old_size.encode(out)?;
        self.new_size.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            old_root: Option::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
            old_version: Option::decode(input)?,
            new_version: FileVersion::decode(input)?,
            old_size: u64::decode(input)?,
            new_size: u64::decode(input)?,
        })
    }
}

impl DurableCodec for DeleteRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.time.encode(out)?;
        self.device_id.encode(out)?;
        self.shard_roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            time: LogicalTime::decode(input)?,
            device_id: DeviceId::decode(input)?,
            shard_roots: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for CheckpointRoots {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockShard(roots) => {
                1u8.encode(out)?;
                roots.encode(out)
            }
            Self::NativeKeyspace(root) => {
                2u8.encode(out)?;
                root.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockShard(Vec::decode(input)?)),
            2 => Ok(Self::NativeKeyspace(KeyspaceRootId::decode(input)?)),
            _ => Err(durable_codec_error("invalid checkpoint roots tag")),
        }
    }
}

impl DurableCodec for Checkpoint {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.checkpoint_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.time.encode(out)?;
        self.owner.encode(out)?;
        self.roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            checkpoint_id: CheckpointId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            time: LogicalTime::decode(input)?,
            owner: MappingOwner::decode(input)?,
            roots: CheckpointRoots::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReservationIntent {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.write_intent.encode(out)?;
        self.owner.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            write_intent: WriteIntentId::decode(input)?,
            owner: MappingOwner::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReservation {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReplicaPlacement {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.storage_node.encode(out)?;
        self.offset.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            offset: u64::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReplicaCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.descriptor.encode(out)?;
        self.placement.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            descriptor: SegmentDescriptor::decode(input)?,
            placement: SegmentReplicaPlacement::decode(input)?,
        })
    }
}

impl DurableCodec for ProofScheme {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let tag = match self {
            Self::DeterministicTestMacV1 => 1u8,
            Self::NodeSignatureV1 => 2,
        };
        tag.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::DeterministicTestMacV1),
            2 => Ok(Self::NodeSignatureV1),
            _ => Err(durable_codec_error("invalid proof scheme tag")),
        }
    }
}

impl DurableCodec for crate::provider::ProofTag {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.bytes.extend_from_slice(&self.0);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(input.take(32)?);
        Ok(Self(bytes))
    }
}

impl DurableCodec for crate::provider::GrantHash {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.bytes.extend_from_slice(&self.0);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(input.take(32)?);
        Ok(Self(bytes))
    }
}

impl DurableCodec for WriteGrantIntent {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockWrite {
                device_id,
                range,
                fence,
                shard_id,
                old_root,
            } => {
                1u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)?;
                fence.encode(out)?;
                shard_id.encode(out)?;
                old_root.encode(out)
            }
            Self::NativeWrite {
                keyspace_id,
                file_id,
                range,
                base_version,
            } => {
                2u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                range.encode(out)?;
                base_version.encode(out)
            }
            Self::NativeAppend {
                keyspace_id,
                file_id,
                session_id,
                reservation_id,
                append_offset,
                bytes,
                writer_epoch,
            } => {
                3u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                session_id.encode(out)?;
                reservation_id.encode(out)?;
                append_offset.encode(out)?;
                bytes.encode(out)?;
                writer_epoch.encode(out)
            }
            Self::NativeReservedAppend {
                keyspace_id,
                file_id,
                session_id,
                reservation_id,
                append_offset,
                bytes,
                writer_epoch,
            } => {
                4u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                session_id.encode(out)?;
                reservation_id.encode(out)?;
                append_offset.encode(out)?;
                bytes.encode(out)?;
                writer_epoch.encode(out)
            }
            Self::Internal { owner } => {
                5u8.encode(out)?;
                owner.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockWrite {
                device_id: DeviceId::decode(input)?,
                range: crate::api::BlockRange::decode(input)?,
                fence: DeviceGeneration::decode(input)?,
                shard_id: ShardId::decode(input)?,
                old_root: MetadataNodeId::decode(input)?,
            }),
            2 => Ok(Self::NativeWrite {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                range: ByteRange::decode(input)?,
                base_version: FileVersion::decode(input)?,
            }),
            3 => Ok(Self::NativeAppend {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                session_id: AppendSessionId::decode(input)?,
                reservation_id: AppendReservationId::decode(input)?,
                append_offset: u64::decode(input)?,
                bytes: u64::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
            }),
            4 => Ok(Self::NativeReservedAppend {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                session_id: AppendSessionId::decode(input)?,
                reservation_id: AppendReservationId::decode(input)?,
                append_offset: u64::decode(input)?,
                bytes: u64::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
            }),
            5 => Ok(Self::Internal {
                owner: MappingOwner::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid write grant intent tag")),
        }
    }
}

impl DurableCodec for SegmentReceiptLifecycle {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::DurablePendingMetadata => 1u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::DurablePendingMetadata),
            _ => Err(durable_codec_error("invalid segment receipt lifecycle tag")),
        }
    }
}

impl DurableCodec for SegmentWriteReceipt {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.tenant.encode(out)?;
        self.grant_id.encode(out)?;
        self.grant_hash.encode(out)?;
        self.principal.encode(out)?;
        self.owner.encode(out)?;
        self.storage_node.encode(out)?;
        self.storage_node_incarnation.encode(out)?;
        self.segment_id.encode(out)?;
        self.write_intent.encode(out)?;
        self.intent.encode(out)?;
        self.bytes.encode(out)?;
        self.checksum.encode(out)?;
        self.durability.encode(out)?;
        self.lifecycle.encode(out)?;
        self.receipt_epoch.encode(out)?;
        self.expires_at.encode(out)?;
        self.node_key_id.encode(out)?;
        self.proof_scheme.encode(out)?;
        self.proof.encode(out)?;
        self.descriptor.encode(out)?;
        self.placement.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            tenant: TenantId::decode(input)?,
            grant_id: GrantId::decode(input)?,
            grant_hash: crate::provider::GrantHash::decode(input)?,
            principal: PrincipalId::decode(input)?,
            owner: MappingOwner::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            storage_node_incarnation: ServerIncarnation::decode(input)?,
            segment_id: SegmentId::decode(input)?,
            write_intent: WriteIntentId::decode(input)?,
            intent: WriteGrantIntent::decode(input)?,
            bytes: u64::decode(input)?,
            checksum: Option::decode(input)?,
            durability: WriteDurability::decode(input)?,
            lifecycle: SegmentReceiptLifecycle::decode(input)?,
            receipt_epoch: GrantEpoch::decode(input)?,
            expires_at: LogicalDeadline::decode(input)?,
            node_key_id: StorageNodeKeyId::decode(input)?,
            proof_scheme: ProofScheme::decode(input)?,
            proof: crate::provider::ProofTag::decode(input)?,
            descriptor: SegmentDescriptor::decode(input)?,
            placement: SegmentReplicaPlacement::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentLifecycleState {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let tag: u8 = match self {
            Self::Reserved => 1,
            Self::Writing => 2,
            Self::DurablePendingMetadata => 3,
            Self::Referenced => 4,
            Self::Released => 5,
            Self::Freed => 6,
        };
        tag.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Reserved),
            2 => Ok(Self::Writing),
            3 => Ok(Self::DurablePendingMetadata),
            4 => Ok(Self::Referenced),
            5 => Ok(Self::Released),
            6 => Ok(Self::Freed),
            _ => Err(durable_codec_error("invalid segment lifecycle tag")),
        }
    }
}

impl DurableCodec for CatalogEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.intent.encode(out)?;
        self.reservation.encode(out)?;
        self.state.encode(out)?;
        self.receipt.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            intent: SegmentReservationIntent::decode(input)?,
            reservation: SegmentReservation::decode(input)?,
            state: SegmentLifecycleState::decode(input)?,
            receipt: Option::decode(input)?,
        })
    }
}

impl DurableCodec for CatalogInner {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.next_segment_id.encode(out)?;
        self.entries.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            next_segment_id: u128::decode(input)?,
            entries: BTreeMap::decode(input)?,
        })
    }
}

impl DurableCodec for DurableSegmentRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.synced.encode(out)?;
        self.commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            synced: bool::decode(input)?,
            commit: SegmentReplicaCommit::decode(input)?,
        })
    }
}

#[cfg(test)]
impl DurableCodec for DurableSegmentStoreImage {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.config.encode(out)?;
        self.next_offset.encode(out)?;
        self.records.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            config: LocalStoreConfig::decode(input)?,
            next_offset: u64::decode(input)?,
            records: BTreeMap::decode(input)?,
        })
    }
}

#[cfg(test)]
impl DurableTestImageCodec for DurableSegmentStoreImage {
    const IMAGE_KIND: u8 = DURABLE_TEST_IMAGE_SEGMENT_STORE;
}

#[cfg(test)]
impl DurableCodec for DurableCatalogImage {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.config.encode(out)?;
        self.catalog.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            config: LocalStoreConfig::decode(input)?,
            catalog: CatalogInner::decode(input)?,
        })
    }
}

#[cfg(test)]
impl DurableTestImageCodec for DurableCatalogImage {
    const IMAGE_KIND: u8 = DURABLE_TEST_IMAGE_CATALOG;
}

impl DurableCodec for MetadataInner {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.next_device_id.encode(out)?;
        self.next_keyspace_id.encode(out)?;
        self.next_file_id.encode(out)?;
        self.next_metadata_node_id.encode(out)?;
        self.next_keyspace_root_id.encode(out)?;
        self.next_keyspace_catalog_shard_id.encode(out)?;
        self.next_commit_group_id.encode(out)?;
        self.next_commit_seq.encode(out)?;
        self.next_checkpoint_id.encode(out)?;
        self.next_gc_epoch.encode(out)?;
        self.device_heads.encode(out)?;
        self.deleted_device_heads.encode(out)?;
        self.device_specs.encode(out)?;
        self.keyspace_heads.encode(out)?;
        self.keyspace_roots.encode(out)?;
        self.keyspace_catalog_shards.encode(out)?;
        self.file_writer_epochs.encode(out)?;
        self.metadata_nodes.encode(out)?;
        self.commit_groups.encode(out)?;
        self.shard_commits.encode(out)?;
        self.keyspace_commits.encode(out)?;
        self.file_commits.encode(out)?;
        self.fork_records.encode(out)?;
        self.delete_records.encode(out)?;
        self.checkpoints.encode(out)?;
        self.metadata_last_mark_epoch.encode(out)?;
        self.segment_last_mark_epoch.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            next_device_id: u128::decode(input)?,
            next_keyspace_id: u128::decode(input)?,
            next_file_id: u128::decode(input)?,
            next_metadata_node_id: u128::decode(input)?,
            next_keyspace_root_id: u128::decode(input)?,
            next_keyspace_catalog_shard_id: u128::decode(input)?,
            next_commit_group_id: u128::decode(input)?,
            next_commit_seq: u64::decode(input)?,
            next_checkpoint_id: u128::decode(input)?,
            next_gc_epoch: u64::decode(input)?,
            device_heads: BTreeMap::decode(input)?,
            deleted_device_heads: BTreeMap::decode(input)?,
            device_specs: BTreeMap::decode(input)?,
            keyspace_heads: BTreeMap::decode(input)?,
            keyspace_roots: BTreeMap::decode(input)?,
            keyspace_catalog_shards: BTreeMap::decode(input)?,
            file_writer_epochs: BTreeMap::decode(input)?,
            metadata_nodes: BTreeMap::decode(input)?,
            commit_groups: BTreeMap::decode(input)?,
            shard_commits: Vec::decode(input)?,
            keyspace_commits: Vec::decode(input)?,
            file_commits: Vec::decode(input)?,
            fork_records: BTreeMap::decode(input)?,
            delete_records: BTreeMap::decode(input)?,
            checkpoints: BTreeMap::decode(input)?,
            metadata_last_mark_epoch: BTreeMap::decode(input)?,
            segment_last_mark_epoch: BTreeMap::decode(input)?,
        })
    }
}

#[cfg(test)]
impl DurableCodec for DurableMetadataImage {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.config.encode(out)?;
        self.metadata.encode(out)?;
        self.next_write_intent.encode(out)?;
        self.next_extent_id.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            config: LocalStoreConfig::decode(input)?,
            metadata: MetadataInner::decode(input)?,
            next_write_intent: u128::decode(input)?,
            next_extent_id: u128::decode(input)?,
        })
    }
}

#[cfg(test)]
impl DurableTestImageCodec for DurableMetadataImage {
    const IMAGE_KIND: u8 = DURABLE_TEST_IMAGE_METADATA;
}

impl DurableCodec for WriteDurability {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Acknowledged => 1u8.encode(out),
            Self::Flushed => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Acknowledged),
            2 => Ok(Self::Flushed),
            _ => Err(durable_codec_error("invalid write durability tag")),
        }
    }
}

impl DurableCodec for FlushScope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Device => 1u8.encode(out),
            Self::All => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Device),
            2 => Ok(Self::All),
            _ => Err(durable_codec_error("invalid flush scope tag")),
        }
    }
}

impl DurableCodec for RestorePoint {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Commit(commit) => {
                1u8.encode(out)?;
                commit.encode(out)
            }
            Self::Checkpoint(checkpoint) => {
                2u8.encode(out)?;
                checkpoint.encode(out)
            }
            Self::Time(time) => {
                3u8.encode(out)?;
                time.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Commit(CommitSeq::decode(input)?)),
            2 => Ok(Self::Checkpoint(CheckpointId::decode(input)?)),
            3 => Ok(Self::Time(LogicalTime::decode(input)?)),
            _ => Err(durable_codec_error("invalid restore point tag")),
        }
    }
}

impl DurableCodec for DeviceInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.generation.encode(out)?;
        self.spec.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            generation: DeviceGeneration::decode(input)?,
            spec: crate::api::DeviceSpec::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for CreateDeviceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.spec.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            spec: crate::api::DeviceSpec::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for WriteCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.range.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            range: ByteRange::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for FlushResult {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.durable_through.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            durable_through: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for DeleteResult {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.commit_seq.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for ForkRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.target.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            target: Option::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for ReadResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            bytes: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for BlockRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Create { request } => {
                1u8.encode(out)?;
                request.encode(out)
            }
            Self::Info { device_id } => {
                2u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Read { device_id, range } => {
                3u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)
            }
            Self::Write {
                device_id,
                offset,
                bytes,
                durability,
            } => {
                4u8.encode(out)?;
                device_id.encode(out)?;
                offset.encode(out)?;
                bytes.encode(out)?;
                durability.encode(out)
            }
            Self::Flush { device_id, scope } => {
                5u8.encode(out)?;
                device_id.encode(out)?;
                scope.encode(out)
            }
            Self::WriteZeroes { device_id, range } => {
                6u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)
            }
            Self::Discard { device_id, range } => {
                7u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)
            }
            Self::Fork { source, request } => {
                8u8.encode(out)?;
                source.encode(out)?;
                request.encode(out)
            }
            Self::Restore { source, point } => {
                9u8.encode(out)?;
                source.encode(out)?;
                point.encode(out)
            }
            Self::Delete { device_id } => {
                10u8.encode(out)?;
                device_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Create {
                request: CreateDeviceRequest::decode(input)?,
            }),
            2 => Ok(Self::Info {
                device_id: DeviceId::decode(input)?,
            }),
            3 => Ok(Self::Read {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            4 => Ok(Self::Write {
                device_id: DeviceId::decode(input)?,
                offset: u64::decode(input)?,
                bytes: Vec::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            5 => Ok(Self::Flush {
                device_id: DeviceId::decode(input)?,
                scope: FlushScope::decode(input)?,
            }),
            6 => Ok(Self::WriteZeroes {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            7 => Ok(Self::Discard {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            8 => Ok(Self::Fork {
                source: DeviceId::decode(input)?,
                request: ForkRequest::decode(input)?,
            }),
            9 => Ok(Self::Restore {
                source: DeviceId::decode(input)?,
                point: RestorePoint::decode(input)?,
            }),
            10 => Ok(Self::Delete {
                device_id: DeviceId::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid block request tag")),
        }
    }
}

impl DurableCodec for BlockResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Created(device_id) => {
                1u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Info(info) => {
                2u8.encode(out)?;
                info.encode(out)
            }
            Self::Read(read) => {
                3u8.encode(out)?;
                read.encode(out)
            }
            Self::Write(commit) => {
                4u8.encode(out)?;
                commit.encode(out)
            }
            Self::Flush(flush) => {
                5u8.encode(out)?;
                flush.encode(out)
            }
            Self::Forked(device_id) => {
                6u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Restored(device_id) => {
                7u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Deleted(delete) => {
                8u8.encode(out)?;
                delete.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Created(DeviceId::decode(input)?)),
            2 => Ok(Self::Info(DeviceInfo::decode(input)?)),
            3 => Ok(Self::Read(ReadResponse::decode(input)?)),
            4 => Ok(Self::Write(WriteCommit::decode(input)?)),
            5 => Ok(Self::Flush(FlushResult::decode(input)?)),
            6 => Ok(Self::Forked(DeviceId::decode(input)?)),
            7 => Ok(Self::Restored(DeviceId::decode(input)?)),
            8 => Ok(Self::Deleted(DeleteResult::decode(input)?)),
            _ => Err(durable_codec_error("invalid block response tag")),
        }
    }
}

impl DurableCodec for BlockRequestEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.client_epoch.encode(out)?;
        self.deadline.encode(out)?;
        self.request.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            client_epoch: ClientEpoch::decode(input)?,
            deadline: Option::decode(input)?,
            request: BlockRequest::decode(input)?,
        })
    }
}

impl DurableCodec for BlockResponseEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.response.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            response: BlockResponse::decode(input)?,
        })
    }
}

impl DurableCodec for CreateKeyspaceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.generation.encode(out)?;
        self.latest_commit.encode(out)?;
        self.file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            generation: KeyspaceGeneration::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
            file_count: usize::decode(input)?,
        })
    }
}

impl DurableCodec for SnapshotKeyspaceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.target.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            target: Option::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for FileSpec {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for CreateFileRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.spec.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            spec: FileSpec::decode(input)?,
        })
    }
}

impl DurableCodec for FileInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.size.encode(out)?;
        self.version.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            size: u64::decode(input)?,
            version: FileVersion::decode(input)?,
        })
    }
}

impl DurableCodec for AppendSession {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.session_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.base_version.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            session_id: AppendSessionId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            base_version: FileVersion::decode(input)?,
        })
    }
}

impl DurableCodec for AppendReservation {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.session_id.encode(out)?;
        self.reservation_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.offset.encode(out)?;
        self.len.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            session_id: AppendSessionId::decode(input)?,
            reservation_id: AppendReservationId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            offset: u64::decode(input)?,
            len: u64::decode(input)?,
        })
    }
}

impl DurableCodec for AppendCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.extent_id.encode(out)?;
        self.range.encode(out)?;
        self.version.encode(out)?;
        self.commit_seq.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            extent_id: ExtentId::decode(input)?,
            range: ByteRange::decode(input)?,
            version: FileVersion::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for FileWriteCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.range.encode(out)?;
        self.version.encode(out)?;
        self.commit_seq.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            range: ByteRange::decode(input)?,
            version: FileVersion::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for NativeRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::CreateKeyspace { request } => {
                1u8.encode(out)?;
                request.encode(out)
            }
            Self::KeyspaceInfo { keyspace_id } => {
                2u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::CreateFile {
                keyspace_id,
                request,
            } => {
                3u8.encode(out)?;
                keyspace_id.encode(out)?;
                request.encode(out)
            }
            Self::FileInfo {
                keyspace_id,
                file_id,
            } => {
                4u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::Read {
                keyspace_id,
                file_id,
                range,
            } => {
                5u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                range.encode(out)
            }
            Self::Write {
                keyspace_id,
                file_id,
                offset,
                bytes,
                durability,
            } => {
                6u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                offset.encode(out)?;
                bytes.encode(out)?;
                durability.encode(out)
            }
            Self::OpenAppendSession {
                keyspace_id,
                file_id,
            } => {
                7u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::ReserveAppend {
                keyspace_id,
                file_id,
                session,
                len,
            } => {
                8u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                session.encode(out)?;
                len.encode(out)
            }
            Self::AppendReserved {
                keyspace_id,
                file_id,
                reservation,
                bytes,
                durability,
            } => {
                9u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                reservation.encode(out)?;
                bytes.encode(out)?;
                durability.encode(out)
            }
            Self::Flush {
                keyspace_id,
                file_id,
            } => {
                10u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::CheckpointKeyspace { keyspace_id } => {
                11u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::SnapshotKeyspace { source, request } => {
                12u8.encode(out)?;
                source.encode(out)?;
                request.encode(out)
            }
            Self::RestoreKeyspace { source, point } => {
                13u8.encode(out)?;
                source.encode(out)?;
                point.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::CreateKeyspace {
                request: CreateKeyspaceRequest::decode(input)?,
            }),
            2 => Ok(Self::KeyspaceInfo {
                keyspace_id: KeyspaceId::decode(input)?,
            }),
            3 => Ok(Self::CreateFile {
                keyspace_id: KeyspaceId::decode(input)?,
                request: CreateFileRequest::decode(input)?,
            }),
            4 => Ok(Self::FileInfo {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            5 => Ok(Self::Read {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            6 => Ok(Self::Write {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                offset: u64::decode(input)?,
                bytes: Vec::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            7 => Ok(Self::OpenAppendSession {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            8 => Ok(Self::ReserveAppend {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                session: AppendSession::decode(input)?,
                len: u64::decode(input)?,
            }),
            9 => Ok(Self::AppendReserved {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                reservation: AppendReservation::decode(input)?,
                bytes: Vec::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            10 => Ok(Self::Flush {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            11 => Ok(Self::CheckpointKeyspace {
                keyspace_id: KeyspaceId::decode(input)?,
            }),
            12 => Ok(Self::SnapshotKeyspace {
                source: KeyspaceId::decode(input)?,
                request: SnapshotKeyspaceRequest::decode(input)?,
            }),
            13 => Ok(Self::RestoreKeyspace {
                source: KeyspaceId::decode(input)?,
                point: RestorePoint::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid native request tag")),
        }
    }
}

impl DurableCodec for NativeResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::KeyspaceCreated(keyspace_id) => {
                1u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::KeyspaceInfo(info) => {
                2u8.encode(out)?;
                info.encode(out)
            }
            Self::FileCreated(file_id) => {
                3u8.encode(out)?;
                file_id.encode(out)
            }
            Self::FileInfo(info) => {
                4u8.encode(out)?;
                info.encode(out)
            }
            Self::Read(read) => {
                5u8.encode(out)?;
                read.encode(out)
            }
            Self::Write(commit) => {
                6u8.encode(out)?;
                commit.encode(out)
            }
            Self::Append(commit) => {
                7u8.encode(out)?;
                commit.encode(out)
            }
            Self::AppendSession(session) => {
                8u8.encode(out)?;
                session.encode(out)
            }
            Self::AppendReservation(reservation) => {
                9u8.encode(out)?;
                reservation.encode(out)
            }
            Self::Flush(flush) => {
                10u8.encode(out)?;
                flush.encode(out)
            }
            Self::KeyspaceCheckpointed(checkpoint_id) => {
                11u8.encode(out)?;
                checkpoint_id.encode(out)
            }
            Self::KeyspaceSnapshotted(keyspace_id) => {
                12u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::KeyspaceRestored(keyspace_id) => {
                13u8.encode(out)?;
                keyspace_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::KeyspaceCreated(KeyspaceId::decode(input)?)),
            2 => Ok(Self::KeyspaceInfo(KeyspaceInfo::decode(input)?)),
            3 => Ok(Self::FileCreated(FileId::decode(input)?)),
            4 => Ok(Self::FileInfo(FileInfo::decode(input)?)),
            5 => Ok(Self::Read(ReadResponse::decode(input)?)),
            6 => Ok(Self::Write(FileWriteCommit::decode(input)?)),
            7 => Ok(Self::Append(AppendCommit::decode(input)?)),
            8 => Ok(Self::AppendSession(AppendSession::decode(input)?)),
            9 => Ok(Self::AppendReservation(AppendReservation::decode(input)?)),
            10 => Ok(Self::Flush(FlushResult::decode(input)?)),
            11 => Ok(Self::KeyspaceCheckpointed(CheckpointId::decode(input)?)),
            12 => Ok(Self::KeyspaceSnapshotted(KeyspaceId::decode(input)?)),
            13 => Ok(Self::KeyspaceRestored(KeyspaceId::decode(input)?)),
            _ => Err(durable_codec_error("invalid native response tag")),
        }
    }
}

impl DurableCodec for NativeRequestEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.client_epoch.encode(out)?;
        self.deadline.encode(out)?;
        self.request.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            client_epoch: ClientEpoch::decode(input)?,
            deadline: Option::decode(input)?,
            request: NativeRequest::decode(input)?,
        })
    }
}

impl DurableCodec for NativeResponseEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.response.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            response: NativeResponse::decode(input)?,
        })
    }
}

impl<T: DurableCodec> DurableCodec for RemoteWireRequest<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.incarnation.encode(out)?;
        self.envelope.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            incarnation: ServerIncarnation::decode(input)?,
            envelope: T::decode(input)?,
        })
    }
}

impl<T: DurableCodec> DurableCodec for RemoteWireReply<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Ok {
                incarnation,
                envelope,
            } => {
                1u8.encode(out)?;
                incarnation.encode(out)?;
                envelope.encode(out)
            }
            Self::Err {
                incarnation,
                reason,
            } => {
                2u8.encode(out)?;
                incarnation.encode(out)?;
                reason.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Ok {
                incarnation: ServerIncarnation::decode(input)?,
                envelope: T::decode(input)?,
            }),
            2 => Ok(Self::Err {
                incarnation: ServerIncarnation::decode(input)?,
                reason: String::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid remote wire reply tag")),
        }
    }
}

#[cfg(test)]
fn encode_test_image<T: DurableTestImageCodec>(value: &T) -> Result<Vec<u8>> {
    let mut out = DurableEncoder::new(T::IMAGE_KIND);
    value.encode(&mut out)?;
    Ok(out.finish())
}

#[cfg(test)]
fn decode_test_image<T: DurableTestImageCodec>(bytes: &[u8]) -> Result<T> {
    let mut input = DurableDecoder::new(bytes, T::IMAGE_KIND)?;
    let value = T::decode(&mut input)?;
    input.finish()?;
    Ok(value)
}

fn validate_durable_segment_bytes(
    segment_id: SegmentId,
    record: &DurableSegmentRecord,
    bytes: &[u8],
) -> Result<()> {
    let bytes_len = u64::try_from(bytes.len())
        .map_err(|_| StorageError::corrupt("durable segment length overflows u64"))?;
    if bytes_len != record.commit.descriptor.bytes {
        return Err(StorageError::corrupt(
            "durable segment length does not match journal commit",
        ));
    }
    if record.commit.descriptor.segment_id != segment_id {
        return Err(StorageError::corrupt(
            "durable segment record key disagrees with descriptor",
        ));
    }
    if record.commit.descriptor.checksum != Some(checksum64(bytes)) {
        return Err(StorageError::corrupt(
            "durable segment checksum does not match journal data",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestImageWriteFault {
    TempWrite,
    TempSync,
    Rename,
    DirSync,
}

#[cfg(test)]
fn maybe_test_image_write_fault(
    fault: Option<TestImageWriteFault>,
    point: TestImageWriteFault,
) -> Result<()> {
    if fault == Some(point) {
        Err(StorageError::unavailable(format!(
            "injected test image write fault at {point:?}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
fn write_test_image_atomic<T: DurableTestImageCodec>(path: &Path, value: &T) -> Result<()> {
    write_test_image_atomic_with_fault(path, value, None)
}

#[cfg(test)]
fn write_test_image_atomic_with_fault<T: DurableTestImageCodec>(
    path: &Path,
    value: &T,
    fault: Option<TestImageWriteFault>,
) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::invalid_argument("test image path has no parent"))?;
    fs::create_dir_all(parent).map_err(fs_error)?;
    let tmp = path.with_extension("tmp");
    let bytes = encode_test_image(value)?;
    maybe_test_image_write_fault(fault, TestImageWriteFault::TempWrite)?;
    {
        let mut file = File::create(&tmp).map_err(fs_error)?;
        file.write_all(&bytes).map_err(fs_error)?;
        maybe_test_image_write_fault(fault, TestImageWriteFault::TempSync)?;
        file.sync_data().map_err(fs_error)?;
    }
    maybe_test_image_write_fault(fault, TestImageWriteFault::Rename)?;
    fs::rename(&tmp, path).map_err(fs_error)?;
    maybe_test_image_write_fault(fault, TestImageWriteFault::DirSync)?;
    File::open(parent)
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)
}

fn next_request_id(next: &Mutex<u128>) -> Result<RequestId> {
    let mut next = lock(next)?;
    let request_id = RequestId::from_raw(*next);
    *next = next
        .checked_add(1)
        .ok_or_else(|| StorageError::conflict("request id overflow"))?;
    Ok(request_id)
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut a = 0xcbf2_9ce4_8422_2325u64 ^ (bytes.len() as u64);
    let mut b = 0x9e37_79b1_85eb_ca87u64.wrapping_add(bytes.len() as u64);

    let mut chunks = bytes.chunks_exact(32);
    for chunk in &mut chunks {
        let w0 = u64::from_le_bytes(chunk[0..8].try_into().expect("fixed chunk width"));
        let w1 = u64::from_le_bytes(chunk[8..16].try_into().expect("fixed chunk width"));
        let w2 = u64::from_le_bytes(chunk[16..24].try_into().expect("fixed chunk width"));
        let w3 = u64::from_le_bytes(chunk[24..32].try_into().expect("fixed chunk width"));
        a = a.wrapping_add(w0).rotate_left(5) ^ w2;
        b = b.wrapping_add(w1).rotate_left(17) ^ w3;
    }

    let mut words = chunks.remainder().chunks_exact(8);
    for word in &mut words {
        let value = u64::from_le_bytes(word.try_into().expect("chunks_exact yields 8 bytes"));
        a = a.wrapping_add(value).rotate_left(9) ^ b;
    }
    for byte in words.remainder() {
        b ^= u64::from(*byte);
        b = b.rotate_left(3).wrapping_add(0x100);
    }

    a ^= b.rotate_left(31);
    a ^= a >> 33;
    a = a.wrapping_mul(0xff51_afd7_ed55_8ccd);
    a ^= a >> 33;
    a
}

fn data_log_checksum64(bytes: &[u8]) -> u64 {
    let mut crc = 0u64;
    for byte in bytes {
        let index = ((crc >> 56) as u8 ^ *byte) as usize;
        crc = (crc << 8) ^ CRC64_ECMA_TABLE[index];
    }
    crc
}

const fn crc64_ecma_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = (index as u64) << 56;
        let mut bit = 0;
        while bit < 8 {
            if crc & 0x8000_0000_0000_0000 != 0 {
                crc = (crc << 1) ^ CRC64_ECMA_POLY;
            } else {
                crc <<= 1;
            }
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

fn replace_leaf_entries(
    entries: &[LeafEntry],
    covered_range: crate::api::BlockRange,
    replacement_range: crate::api::BlockRange,
    replacement: Option<LeafEntry>,
) -> Result<Vec<LeafEntry>> {
    replacement_range.validate_non_empty()?;
    if !covered_range.contains_range(replacement_range)? {
        return Err(StorageError::invalid_argument(
            "replacement range is outside leaf coverage",
        ));
    }

    let mut out = Vec::with_capacity(entries.len() + usize::from(replacement.is_some()));
    let replacement_end = replacement_range.end_exclusive()?.raw();

    for entry in entries {
        let entry_range = entry.logical_range();
        let entry_end = entry_range.end_exclusive()?.raw();
        if !entry_range.overlaps(replacement_range)? {
            out.push(entry.clone());
            continue;
        }

        if entry.logical_start.raw() < replacement_range.start.raw() {
            out.push(LeafEntry {
                logical_start: entry.logical_start,
                blocks: BlockCount::from_raw(
                    replacement_range.start.raw() - entry.logical_start.raw(),
                ),
                segment_id: entry.segment_id,
                segment_offset: entry.segment_offset,
            });
        }

        if entry_end > replacement_end {
            let skipped_blocks = replacement_end - entry.logical_start.raw();
            let segment_offset = entry
                .segment_offset
                .raw()
                .checked_add(skipped_blocks)
                .ok_or_else(|| StorageError::invalid_argument("leaf segment offset overflows"))?;
            out.push(LeafEntry {
                logical_start: BlockIndex::from_raw(replacement_end),
                blocks: BlockCount::from_raw(entry_end - replacement_end),
                segment_id: entry.segment_id,
                segment_offset: BlockIndex::from_raw(segment_offset),
            });
        }
    }

    if let Some(replacement) = replacement {
        out.push(replacement);
    }
    out.sort_by_key(|entry| entry.logical_start.raw());
    coalesce_leaf_entries(out)
}

fn coalesce_leaf_entries(entries: Vec<LeafEntry>) -> Result<Vec<LeafEntry>> {
    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(previous) = out.last_mut()
            && previous.segment_id == entry.segment_id
            && previous.logical_range().end_exclusive()? == entry.logical_start
            && previous.segment_end_exclusive()? == entry.segment_offset
        {
            previous.blocks = BlockCount::from_raw(
                previous
                    .blocks
                    .raw()
                    .checked_add(entry.blocks.raw())
                    .ok_or_else(|| StorageError::invalid_argument("leaf entry size overflows"))?,
            );
            continue;
        }
        out.push(entry);
    }
    Ok(out)
}

fn blocks_for_bytes(bytes: u64, block_size: u64) -> Result<u64> {
    if block_size == 0 {
        return Err(StorageError::invalid_argument(
            "block_size must be greater than zero",
        ));
    }
    if bytes == 0 {
        return Ok(0);
    }

    bytes
        .checked_add(block_size - 1)
        .map(|adjusted| adjusted / block_size)
        .ok_or_else(|| StorageError::invalid_argument("byte count overflows block count"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{BlockRequest, CreateDeviceRequest, DeviceSpec, FlushScope, WriteDurability};
    use crate::extent::{CreateFileRequest, CreateKeyspaceRequest, FileSpec};
    use crate::id::{ClientEpoch, LogicalDeadline, ShardId, WriteIntentId};
    use crate::object::{LeafEntry, ShardRootUpdate};
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

        let (counters, events, len, capacity, last_sequence) =
            observability.snapshot_parts().unwrap();
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
        assert!(
            snapshot
                .recent_events
                .iter()
                .any(|event| event.kind == StorageEventKind::KeyspaceRestored
                    && event.commit_seq.is_some())
        );
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

    fn durable_images_from_store(
        store: &LocalCoordinator,
    ) -> (
        DurableMetadataImage,
        DurableCatalogImage,
        DurableSegmentStoreImage,
    ) {
        let image = store.state_image().unwrap();
        let metadata = DurableMetadataImage {
            config: image.config,
            metadata: image.metadata,
            next_write_intent: image.next_write_intent,
            next_extent_id: image.next_extent_id,
        };
        let primary = image
            .storage_nodes
            .nodes
            .get(&image.config.storage_node)
            .unwrap();
        let primary_config = image.config.for_storage_node(image.config.storage_node);
        let catalog = DurableCatalogImage {
            config: primary_config,
            catalog: primary.segment_catalog.clone(),
        };
        let segment_store =
            DurableSegmentStoreImage::from_inner(primary_config, primary.segment_store.clone());
        (metadata, catalog, segment_store)
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
        fn create_keyspace_for_conformance(
            &self,
            request: CreateKeyspaceRequest,
        ) -> Result<KeyspaceId>;
        fn create_file_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
            request: CreateFileRequest,
        ) -> Result<FileId>;
        fn checkpoint_keyspace_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
        ) -> Result<CheckpointId>;
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
        ) -> Result<AppendCommit>;
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

        fn checkpoint_keyspace_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
        ) -> Result<CheckpointId> {
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
            self.write_file_at(
                keyspace_id,
                file_id,
                offset,
                data,
                WriteDurability::Acknowledged,
            )
        }

        fn append_file_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
            file_id: FileId,
            data: &[u8],
        ) -> Result<AppendCommit> {
            let session = self.open_append_session(keyspace_id, file_id)?;
            let reservation = self.reserve_append(&session, data.len() as u64)?;
            self.append_reserved(reservation, data, WriteDurability::Acknowledged)
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

        fn checkpoint_keyspace_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
        ) -> Result<CheckpointId> {
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
            self.write_file_at(keyspace_id, file_id, offset, data, WriteDurability::Flushed)
        }

        fn append_file_for_conformance(
            &self,
            keyspace_id: KeyspaceId,
            file_id: FileId,
            data: &[u8],
        ) -> Result<AppendCommit> {
            let session = self.open_append_session(keyspace_id, file_id)?;
            let reservation = self.reserve_append(&session, data.len() as u64)?;
            self.append_reserved(reservation, data, WriteDurability::Flushed)
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

    fn run_provider_conformance(
        store: &dyn ProviderConformanceStore,
    ) -> ProviderConformanceOutcome {
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
            checksum: None,
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
                            let file_id = if let Some(file_id) = file_models.keys().next().copied()
                            {
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
                            let stale = file.open_append_session().unwrap();
                            let fresh = file.open_append_session().unwrap();
                            assert!(
                                append_native_file_with_session(
                                    &file,
                                    &stale,
                                    &repeated_blocks(1, 1)
                                )
                                .is_err()
                            );
                            append_native_file_with_session(&file, &fresh, &repeated_blocks(1, 2))
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
                                .unwrap_or_else(|| {
                                    MappingOwner::BlockDevice(DeviceId::from_raw(1))
                                });
                            let reservation =
                                store.write_segment_for_owner(owner, &[6; 4096]).unwrap();
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
                                .run_metadata_custodian(
                                    RetentionPolicy::expire_deleted_immediately(),
                                )
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
                    assert_model_blocks(
                        &actual,
                        model,
                        seed,
                        harness.trace.events(),
                        "native file",
                    );
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
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(10),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
            .unwrap();
        let writing = store
            .segment_catalog()
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(11),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
            .unwrap();
        store.segment_catalog().begin_write(&writing).unwrap();
        let orphan = store
            .segment_catalog()
            .reserve_segment(SegmentReservationIntent {
                write_intent: WriteIntentId::from_raw(12),
                owner: MappingOwner::BlockDevice(DeviceId::from_raw(1)),
                bytes: 4096,
            })
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

    #[test]
    fn durable_state_image_codec_round_trips_real_block_and_native_state() {
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

        let (metadata, catalog, segment_store) = durable_images_from_store(&store);
        for bytes in [
            encode_test_image(&metadata).unwrap(),
            encode_test_image(&catalog).unwrap(),
            encode_test_image(&segment_store).unwrap(),
        ] {
            assert!(bytes.starts_with(DURABLE_TEST_IMAGE_MAGIC));
        }

        let metadata_bytes = encode_test_image(&metadata).unwrap();
        let catalog_bytes = encode_test_image(&catalog).unwrap();
        let segment_store_bytes = encode_test_image(&segment_store).unwrap();
        assert_eq!(
            encode_test_image(&decode_test_image::<DurableMetadataImage>(&metadata_bytes).unwrap())
                .unwrap(),
            metadata_bytes
        );
        assert_eq!(
            encode_test_image(&decode_test_image::<DurableCatalogImage>(&catalog_bytes).unwrap())
                .unwrap(),
            catalog_bytes
        );
        assert_eq!(
            encode_test_image(
                &decode_test_image::<DurableSegmentStoreImage>(&segment_store_bytes).unwrap()
            )
            .unwrap(),
            segment_store_bytes
        );
    }

    #[test]
    fn durable_state_image_codec_has_stable_catalog_golden_bytes() {
        let image = DurableCatalogImage {
            config: LocalStoreConfig::default(),
            catalog: CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            },
        };

        assert_eq!(
            bytes_to_hex(&encode_test_image(&image).unwrap()),
            concat!(
                "54434f57494d4721",
                "0001",
                "02",
                "0000000000000001",
                "00001000",
                "0000000000000001",
                "0000000000000004",
                "0000000000000400",
                "00000000000000000000000000000001",
                "0000000000000400",
                "00000000000000000000000000000001",
                "0000000000000000",
            )
        );
    }

    #[test]
    fn durable_state_image_codec_rejects_malformed_inputs() {
        let image = DurableCatalogImage {
            config: LocalStoreConfig::default(),
            catalog: CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            },
        };
        let bytes = encode_test_image(&image).unwrap();

        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xff;
        assert!(decode_test_image::<DurableCatalogImage>(&bad_magic).is_err());

        let mut bad_version = bytes.clone();
        bad_version[9] = 2;
        assert!(decode_test_image::<DurableCatalogImage>(&bad_version).is_err());

        let mut bad_kind = bytes.clone();
        bad_kind[10] = DURABLE_TEST_IMAGE_METADATA;
        assert!(decode_test_image::<DurableCatalogImage>(&bad_kind).is_err());
        assert!(decode_test_image::<DurableMetadataImage>(&bytes).is_err());

        let mut truncated = bytes.clone();
        truncated.pop();
        assert!(decode_test_image::<DurableCatalogImage>(&truncated).is_err());

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_test_image::<DurableCatalogImage>(&trailing).is_err());

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
    fn fault_injected_test_image_writer_reopens_old_or_complete_new_image() {
        fn assert_test_image_faults<T: DurableTestImageCodec + Clone>(path: &Path, old: T, new: T) {
            write_test_image_atomic(path, &old).unwrap();
            let old_bytes = encode_test_image(&old).unwrap();
            let new_bytes = encode_test_image(&new).unwrap();

            for fault in [
                TestImageWriteFault::TempWrite,
                TestImageWriteFault::TempSync,
                TestImageWriteFault::Rename,
                TestImageWriteFault::DirSync,
            ] {
                write_test_image_atomic(path, &old).unwrap();
                assert!(write_test_image_atomic_with_fault(path, &new, Some(fault)).is_err());
                let visible = fs::read(path).unwrap();
                assert!(
                    visible == old_bytes || visible == new_bytes,
                    "fault {fault:?} left a partial test image"
                );
                assert!(decode_test_image::<T>(&visible).is_ok());
            }

            write_test_image_atomic(path, &new).unwrap();
            assert_eq!(fs::read(path).unwrap(), new_bytes);
        }

        let root = durable_temp_dir("test-image-writer-faults");
        let old_catalog = DurableCatalogImage {
            config: LocalStoreConfig::default(),
            catalog: CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            },
        };
        let mut new_catalog = old_catalog.clone();
        new_catalog.catalog.next_segment_id = 2;

        let old_segment_store = DurableSegmentStoreImage {
            config: LocalStoreConfig::default(),
            next_offset: 0,
            records: BTreeMap::new(),
        };
        let mut new_segment_store = old_segment_store.clone();
        new_segment_store.next_offset = 4096;

        let old_metadata = DurableMetadataImage {
            config: LocalStoreConfig::default(),
            metadata: MetadataInner::new(),
            next_write_intent: 1,
            next_extent_id: 1,
        };
        let mut new_metadata = old_metadata.clone();
        new_metadata.next_write_intent = 2;

        assert_test_image_faults(
            &root.join("metadata").join("catalog.img"),
            old_catalog,
            new_catalog,
        );
        assert_test_image_faults(
            &root.join("storage-node").join("segment-store.img"),
            old_segment_store,
            new_segment_store,
        );
        assert_test_image_faults(
            &root.join("metadata").join("metadata.img"),
            old_metadata,
            new_metadata,
        );
        let _ = fs::remove_dir_all(root);
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
        for table in ["store_meta", "device_heads", "metadata_nodes"] {
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
    fn durable_sqlite_repairs_native_referenced_pending_catalog_rows_on_reopen() {
        let root = durable_temp_dir("pending-native-catalog-reference-repair");
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
        let segment_id = file_segment_ids(&store.metadata(), keyspace_id, file_id)
            .into_iter()
            .next()
            .unwrap();
        drop(store);

        assert_eq!(
            node_catalog_entry(&root, cfg.storage_node, segment_id).state,
            SegmentLifecycleState::DurablePendingMetadata
        );

        let reopened = DurableCoordinator::open(&root, cfg).unwrap();
        let mut bytes = vec![0; 4096];
        reopened
            .read_file(keyspace_id, file_id, ByteRange::new(0, 4096), &mut bytes)
            .unwrap();
        assert_eq!(bytes, repeated_blocks(1, 12));
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
    fn data_log_checksum_uses_crc64_ecma_golden_value() {
        assert_eq!(data_log_checksum64(b"123456789"), 0x6c40_df5f_0b49_7347);
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
        OpenOptions::new()
            .append(true)
            .open(&data_log)
            .unwrap()
            .write_all(
                &encode_data_log_record(SegmentId::from_raw(999), &repeated_blocks(1, 9)).unwrap(),
            )
            .unwrap();
        let torn =
            encode_data_log_record(SegmentId::from_raw(1000), &repeated_blocks(1, 10)).unwrap();
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
            placement.record_offset + u64::try_from(DATA_LOG_MAGIC.len() + 2 + 16 + 8).unwrap();
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

        let retained = RetentionPolicy::expire_deleted_immediately().with_pitr_grace_commits(10);
        store.run_metadata_custodian(retained).unwrap();
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

        let reopened =
            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
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
    fn maintenance_throttles_writes_until_manual_tick_reclaims_debt() {
        let root = durable_temp_dir("maintenance-throttle");
        let cfg = config();
        let policy = MaintenancePolicy {
            mode: MaintenanceMode::Manual,
            data_log_policy: DurableDataLogPolicy {
                target_data_log_bytes: 4096,
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

        let reopened =
            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
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

        let reopened =
            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
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

        let reopened =
            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy).unwrap();
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

        let clean =
            DurableCoordinator::open_with_maintenance_policy(&clean_root, cfg, policy).unwrap();
        assert!(!clean.startup_maintenance_has_work().unwrap());
        clean.shutdown_maintenance();

        let mut manual_policy = policy;
        manual_policy.mode = MaintenanceMode::Manual;
        let dirty =
            DurableCoordinator::open_with_maintenance_policy(&dirty_root, cfg, manual_policy)
                .unwrap();
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
                        store =
                            DurableCoordinator::open_with_maintenance_policy(&root, cfg, policy)
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
                        append_durable_store_once(
                            &store,
                            keyspace_id,
                            file_id,
                            &[byte; 4096],
                            WriteDurability::Acknowledged,
                        )
                        .unwrap();
                        live_file.push(byte);
                        trace.push(format!("step={step} append_ack byte={byte}"));
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
    fn durable_append_session_open_does_not_persist_writer_epoch() {
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
        let session = store.open_append_session(keyspace_id, file_id).unwrap();
        assert_eq!(session.writer_epoch, WriterEpoch::from_raw(1));
        let after_acquire = store.metadata().state_inner().unwrap();
        assert_eq!(
            after_acquire
                .file_writer_epochs
                .get(&(keyspace_id, file_id)),
            Some(&WriterEpoch::from_raw(0))
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
    fn durable_uncommitted_append_sessions_fail_after_reopen_even_when_epoch_repeats() {
        let root = durable_temp_dir("native-restart-stale-session");
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
        let stale = store.open_append_session(keyspace_id, file_id).unwrap();
        assert_eq!(stale.writer_epoch, WriterEpoch::from_raw(1));
        drop(store);

        let reopened = DurableCoordinator::open(&root, cfg).unwrap();
        let fresh = reopened.open_append_session(keyspace_id, file_id).unwrap();
        assert_eq!(fresh.writer_epoch, stale.writer_epoch);
        assert_ne!(fresh.session_id, stale.session_id);
        assert!(
            append_durable_store_with_session(
                &reopened,
                &stale,
                b"stale",
                WriteDurability::Acknowledged
            )
            .is_err()
        );
        append_durable_store_with_session(&reopened, &fresh, b"durable", WriteDurability::Flushed)
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
    fn append_session_stealing_is_scoped_to_one_file() {
        let store = LocalCoordinator::with_config(config()).unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (file_a, _) = create_local_file(&client, keyspace_id);
        let (file_b, _) = create_local_file(&client, keyspace_id);

        let file_b_session = store.open_append_session(keyspace_id, file_b).unwrap();
        let stale_file_a = store.open_append_session(keyspace_id, file_a).unwrap();
        let fresh_file_a = store.open_append_session(keyspace_id, file_a).unwrap();

        assert!(
            append_local_store_with_session(
                &store,
                &stale_file_a,
                b"stale",
                WriteDurability::Acknowledged
            )
            .is_err()
        );
        append_local_store_with_session(
            &store,
            &file_b_session,
            b"b",
            WriteDurability::Acknowledged,
        )
        .unwrap();
        append_local_store_with_session(&store, &fresh_file_a, b"a", WriteDurability::Acknowledged)
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
    fn same_file_write_at_invalidates_append_session_without_touching_other_files() {
        let store = LocalCoordinator::with_config(config()).unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (file_a_id, file_a) = create_local_file(&client, keyspace_id);
        let (_file_b_id, file_b) = create_local_file(&client, keyspace_id);

        let stale_a = file_a.open_append_session().unwrap();
        let live_b = file_b.open_append_session().unwrap();
        file_a.write_at(0, b"base").unwrap();

        assert!(append_native_file_with_session(&file_a, &stale_a, b"x").is_err());
        append_native_file_with_session(&file_b, &live_b, b"b").unwrap();
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
    fn durable_reopen_invalidates_uncommitted_append_reservations() {
        let root = durable_temp_dir("native-restart-stale-reservation");
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
        let session = store.open_append_session(keyspace_id, file_id).unwrap();
        let stale = store
            .reserve_append(&session, b"stale".len() as u64)
            .unwrap();
        drop(store);

        let reopened = DurableCoordinator::open(&root, cfg).unwrap();
        assert!(
            reopened
                .append_reserved(stale, b"stale", WriteDurability::Acknowledged)
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
            .write_file_at(keyspace_id, file_a, 0, b"before", WriteDurability::Flushed)
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
            .write_file_at(keyspace_id, file_a, 0, b"after!", WriteDurability::Flushed)
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
                                checksum: None,
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
            NativeRequest::Write {
                keyspace_id,
                file_id,
                offset: 0,
                bytes: b"native".to_vec(),
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
            let MetadataNodeKind::Leaf { entries } = node.kind else {
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
                let expected_version =
                    if model.is_empty() || harness.rng.next_u64().is_multiple_of(2) {
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
    fn native_append_sessions_reuse_and_stealing_are_deterministic() {
        let store = LocalCoordinator::with_config(config()).unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (file_id, file) = create_local_file(&client, keyspace_id);

        let first = file.open_append_session().unwrap();
        let stolen = file.open_append_session().unwrap();
        let stolen_session_id = stolen.session_id;
        assert!(append_native_file_with_session(&file, &first, &repeated_blocks(1, 1)).is_err());

        let commit =
            append_native_file_with_session(&file, &stolen, &repeated_blocks(2, 2)).unwrap();
        assert_eq!(commit.version, FileVersion::from_raw(1));
        assert_eq!(commit.range, ByteRange::new(0, 2 * 4096));
        let second =
            append_native_file_with_session(&file, &stolen, &repeated_blocks(1, 3)).unwrap();
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
        let MetadataNodeKind::Leaf { entries } = root.kind else {
            panic!("default test native file root should remain a leaf");
        };
        assert_eq!(entries.len(), 2);
        let intent = store
            .segment_catalog()
            .intent_for_segment(entries[0].segment_id)
            .unwrap();
        assert_ne!(
            intent.write_intent,
            WriteIntentId::from_raw(stolen_session_id.raw())
        );
    }

    #[test]
    fn native_append_publish_failure_leaves_file_version_and_orphan_unchanged() {
        let store = LocalCoordinator::with_config(LocalStoreConfig {
            file_root_blocks: 1,
            ..config()
        })
        .unwrap();
        let client = create_native_client(&store);
        let keyspace_id = create_local_keyspace(&client);
        let (_file_id, file) = create_local_file(&client, keyspace_id);
        let failed = append_native_file_once(&file, &repeated_blocks(2, 4));
        assert!(failed.is_err());
        let info = file.info().unwrap();
        assert_eq!(info.version, FileVersion::from_raw(0));
        assert_eq!(info.size, 0);

        let reservation = SegmentId::from_raw(1);
        assert_eq!(
            store.segment_catalog().state(reservation).unwrap(),
            SegmentLifecycleState::DurablePendingMetadata
        );
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
    fn native_keyspace_catalog_publish_copies_only_one_catalog_shard() {
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
            InMemoryMetadataPlane::keyspace_catalog_shard_index(file_ids[0], &before_write)
                .unwrap();
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

        let before_create = after_write;
        let shard_count_before_create = store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap();
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
        let checkpoint_root = store
            .metadata()
            .get_keyspace_head(keyspace_id)
            .unwrap()
            .root;
        let stale_source_session = file_a.open_append_session().unwrap();

        append_native_file_with_session(&file_a, &stale_source_session, &repeated_blocks(1, 3))
            .unwrap();

        let nodes_before_restore = store.metadata().metadata_node_count().unwrap();
        let catalog_shards_before_restore = store
            .metadata()
            .keyspace_catalog_shard_count_for_test()
            .unwrap();
        let restored_keyspace = client
            .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(checkpoint))
            .unwrap();
        assert_eq!(
            store
                .metadata()
                .get_keyspace_head(restored_keyspace)
                .unwrap()
                .root,
            checkpoint_root
        );
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
            append_native_file_with_session(
                &restored_a,
                &stale_source_session,
                &repeated_blocks(1, 4)
            )
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

        let snapshot_source_root = store
            .metadata()
            .get_keyspace_head(keyspace_id)
            .unwrap()
            .root;
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
        assert_eq!(
            store
                .metadata()
                .get_keyspace_head(snapshot_keyspace)
                .unwrap()
                .root,
            snapshot_source_root
        );
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
        let checkpoint_root = store
            .metadata()
            .get_keyspace_head(keyspace_id)
            .unwrap()
            .root;
        let changed = file.write_at(0, b"change").unwrap();

        store
            .metadata()
            .clear_keyspace_commits_for_test(keyspace_id)
            .unwrap();

        let restored = client
            .restore_keyspace(keyspace_id, RestorePoint::Checkpoint(checkpoint))
            .unwrap();
        assert_eq!(
            store.metadata().get_keyspace_head(restored).unwrap().root,
            checkpoint_root
        );
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
        assert_eq!(
            store
                .segment_catalog()
                .state(SegmentId::from_raw(1))
                .unwrap(),
            SegmentLifecycleState::Referenced
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
                    || (file_model.bytes.len() < capacity
                        && harness.rng.next_u64().is_multiple_of(2))
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
            NativeRequest::AppendReserved {
                keyspace_id,
                file_id: FileId::from_raw(1),
                reservation: crate::extent::AppendReservation {
                    keyspace_id,
                    file_id: FileId::from_raw(1),
                    session_id: crate::id::AppendSessionId::from_raw(1),
                    reservation_id: crate::id::AppendReservationId::from_raw(2),
                    writer_epoch: WriterEpoch::from_raw(0),
                    offset: 0,
                    len: 1,
                },
                bytes: vec![1],
                durability: WriteDurability::Acknowledged,
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
        assert_eq!(file_segments.len(), 3);
        assert_eq!(segment_storage_nodes(&store, &file_segments).len(), 3);
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
            record.bytes = repeated_blocks(1, 8);
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
        let left_range =
            crate::api::BlockRange::new(BlockIndex::from_raw(2), BlockCount::from_raw(1));
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
    fn trusted_native_grant_receipt_flow_publishes_append_and_write() {
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
        let session = store
            .open_append_session(keyspace.keyspace_id, file.file_id)
            .unwrap();
        let reservation = store.reserve_append(&session, 4096).unwrap();
        let append_grant = store
            .issue_native_append_grant(reservation, 4096, 4096, WriteDurability::Acknowledged)
            .unwrap();
        let append_receipt = store
            .write_granted_segment(&append_grant, repeated_blocks(1, 21))
            .unwrap();
        let append = store
            .submit_native_append_receipt(&append_grant, append_receipt.clone())
            .unwrap();
        assert_eq!(append.range, ByteRange::new(0, 4096));
        assert_eq!(
            store
                .storage_nodes
                .state(append_receipt.segment_id)
                .unwrap(),
            SegmentLifecycleState::Referenced
        );
        assert!(
            store
                .submit_native_append_receipt(&append_grant, append_receipt.clone())
                .is_err()
        );
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

    fn append_native_file_once(file: &LocalNativeFile, data: &[u8]) -> Result<AppendCommit> {
        let session = file.open_append_session()?;
        append_native_file_with_session(file, &session, data)
    }

    fn append_native_file_with_session(
        file: &LocalNativeFile,
        session: &AppendSession,
        data: &[u8],
    ) -> Result<AppendCommit> {
        let reservation = file.reserve_append(session, data.len() as u64)?;
        file.append_reserved(reservation, data)
    }

    fn append_local_store_once(
        store: &LocalCoordinator,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<AppendCommit> {
        let session = store.open_append_session(keyspace_id, file_id)?;
        append_local_store_with_session(store, &session, data, durability)
    }

    fn append_local_store_with_session(
        store: &LocalCoordinator,
        session: &AppendSession,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<AppendCommit> {
        let reservation = store.reserve_append(session, data.len() as u64)?;
        store.append_reserved(reservation, data, durability)
    }

    fn append_durable_store_once(
        store: &DurableCoordinator,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<AppendCommit> {
        let session = store.open_append_session(keyspace_id, file_id)?;
        append_durable_store_with_session(store, &session, data, durability)
    }

    fn append_durable_store_with_session(
        store: &DurableCoordinator,
        session: &AppendSession,
        data: &[u8],
        durability: WriteDurability,
    ) -> Result<AppendCommit> {
        let reservation = store.reserve_append(session, data.len() as u64)?;
        store.append_reserved(reservation, data, durability)
    }

    fn repeated_blocks(blocks: u64, byte: u8) -> Vec<u8> {
        vec![byte; blocks as usize * 4096]
    }

    fn first_device_segment(store: &DurableCoordinator, device_id: DeviceId) -> SegmentId {
        device_segment_ids(&store.metadata(), device_id)
            .into_iter()
            .next()
            .unwrap()
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
        ["metadata.sqlite", "metadata.sqlite-wal"]
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
        ] {
            let _ = fs::remove_file(root.join(file_name));
        }
        for (file_name, bytes) in snapshot {
            fs::write(root.join(file_name), bytes).unwrap();
        }
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

    fn collect_tree_segments(
        metadata: &Arc<InMemoryMetadataPlane>,
        node_id: MetadataNodeId,
        out: &mut Vec<SegmentId>,
    ) {
        let node = metadata.get_metadata_node(node_id).unwrap();
        match node.kind {
            MetadataNodeKind::Leaf { entries } => {
                out.extend(entries.into_iter().map(|entry| entry.segment_id));
            }
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    collect_tree_segments(metadata, child.node_id, out);
                }
            }
        }
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
            .zip(&after.shard_roots)
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
}
