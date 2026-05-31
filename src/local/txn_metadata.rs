use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::api::*;
use crate::error::{Result, StorageError};
use crate::id::*;
use crate::object::*;
use crate::provider::*;

use super::{
    DeviceWriteChunk, LocalCoordinator, LocalMarkReferencedProfile, LocalSegmentWriteProfile,
    LocalStoreConfig, SegmentReplacement, TreeEditResult, TreeRangeEdit, block_range_to_byte_range,
    duration_nanos_u64, lock, normalize_storage_nodes, replace_leaf_entries,
    replace_run_backed_file_extents, usize_to_u64,
};

/// Execution mode for the in-memory metadata transaction backend used to
/// diagnose block metadata convergence without SQLite in the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTxnMode {
    Serial,
    Sharded { shard_count: usize },
}

/// Profile phase for a metadata transaction. Publish rows are the hot path;
/// node rows are included so hidden metadata-node write costs do not disappear
/// from the profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTxnProfilePhase {
    CreateDevice,
    PersistNode,
    PublishBlockCommit,
    Checkpoint,
    Fork,
    Restore,
    Delete,
}

impl fmt::Display for MetadataTxnProfilePhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateDevice => f.write_str("create-device"),
            Self::PersistNode => f.write_str("persist-node"),
            Self::PublishBlockCommit => f.write_str("publish-block-commit"),
            Self::Checkpoint => f.write_str("checkpoint"),
            Self::Fork => f.write_str("fork"),
            Self::Restore => f.write_str("restore"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

/// Per-transaction profile row for the in-memory metadata-service simulator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTxnProfile {
    pub sequence: u64,
    pub phase: MetadataTxnProfilePhase,
    pub total_nanos: u64,
    pub tx_lock_wait_nanos: u64,
    pub read_validation_nanos: u64,
    pub apply_write_nanos: u64,
    pub commit_version_alloc_nanos: u64,
    pub touched_key_shards: u64,
    pub read_key_count: u64,
    pub write_key_count: u64,
    pub conflict_count: u64,
}

impl MetadataTxnProfile {
    fn new(phase: MetadataTxnProfilePhase) -> Self {
        Self {
            sequence: 0,
            phase,
            total_nanos: 0,
            tx_lock_wait_nanos: 0,
            read_validation_nanos: 0,
            apply_write_nanos: 0,
            commit_version_alloc_nanos: 0,
            touched_key_shards: 0,
            read_key_count: 0,
            write_key_count: 0,
            conflict_count: 0,
        }
    }
}

/// Per-successful-write profile row for the transaction block coordinator.
///
/// This profile intentionally spans the whole block write pipeline around the
/// metadata transaction so loadbench can attribute same-device shard-lane
/// costs without introducing a fake data or metadata path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnBlockWriteProfile {
    pub sequence: u64,
    pub total_nanos: u64,
    pub device_spec_lookup_nanos: u64,
    pub range_split_shard_head_read_nanos: u64,
    pub write_intent_alloc_nanos: u64,
    pub payload_copy_nanos: u64,
    pub segment_write_nanos: u64,
    pub storage_node_ids_nanos: u64,
    pub placement_select_nanos: u64,
    pub segment_id_alloc_nanos: u64,
    pub grant_issue_nanos: u64,
    pub storage_node_transport_dispatch_nanos: u64,
    pub grant_verify_nanos: u64,
    pub catalog_duplicate_probe_nanos: u64,
    pub catalog_duplicate_probe_lock_wait_nanos: u64,
    pub catalog_reserve_nanos: u64,
    pub catalog_reserve_lock_wait_nanos: u64,
    pub catalog_begin_nanos: u64,
    pub catalog_begin_lock_wait_nanos: u64,
    pub segment_store_write_nanos: u64,
    pub segment_store_lock_wait_nanos: u64,
    pub checksum_integrity_nanos: u64,
    pub segment_store_insert_nanos: u64,
    pub segment_sync_nanos: u64,
    pub segment_sync_lock_wait_nanos: u64,
    pub receipt_create_nanos: u64,
    pub receipt_verify_nanos: u64,
    pub catalog_commit_nanos: u64,
    pub catalog_commit_lock_wait_nanos: u64,
    pub tree_path_copy_nanos: u64,
    pub metadata_publish_call_nanos: u64,
    pub mark_referenced_nanos: u64,
    pub mark_reference_evidence_nanos: u64,
    pub mark_reference_transport_dispatch_nanos: u64,
    pub mark_reference_verify_nanos: u64,
    pub mark_reference_catalog_nanos: u64,
    pub mark_reference_catalog_lock_wait_nanos: u64,
    pub touched_shard_count: u64,
    pub segment_count: u64,
    pub storage_node_count: u64,
}

impl TxnBlockWriteProfile {
    fn absorb_segment_write(&mut self, profile: LocalSegmentWriteProfile) {
        self.storage_node_ids_nanos = self
            .storage_node_ids_nanos
            .saturating_add(profile.storage_node_ids_nanos);
        self.placement_select_nanos = self
            .placement_select_nanos
            .saturating_add(profile.placement_select_nanos);
        self.segment_id_alloc_nanos = self
            .segment_id_alloc_nanos
            .saturating_add(profile.segment_id_alloc_nanos);
        self.grant_issue_nanos = self
            .grant_issue_nanos
            .saturating_add(profile.grant_issue_nanos);
        self.storage_node_transport_dispatch_nanos = self
            .storage_node_transport_dispatch_nanos
            .saturating_add(profile.storage_node_transport_dispatch_nanos);
        self.grant_verify_nanos = self
            .grant_verify_nanos
            .saturating_add(profile.grant_verify_nanos);
        self.catalog_duplicate_probe_nanos = self
            .catalog_duplicate_probe_nanos
            .saturating_add(profile.catalog_duplicate_probe_nanos);
        self.catalog_duplicate_probe_lock_wait_nanos = self
            .catalog_duplicate_probe_lock_wait_nanos
            .saturating_add(profile.catalog_duplicate_probe_lock_wait_nanos);
        self.catalog_reserve_nanos = self
            .catalog_reserve_nanos
            .saturating_add(profile.catalog_reserve_nanos);
        self.catalog_reserve_lock_wait_nanos = self
            .catalog_reserve_lock_wait_nanos
            .saturating_add(profile.catalog_reserve_lock_wait_nanos);
        self.catalog_begin_nanos = self
            .catalog_begin_nanos
            .saturating_add(profile.catalog_begin_nanos);
        self.catalog_begin_lock_wait_nanos = self
            .catalog_begin_lock_wait_nanos
            .saturating_add(profile.catalog_begin_lock_wait_nanos);
        self.segment_store_write_nanos = self
            .segment_store_write_nanos
            .saturating_add(profile.segment_store_write_nanos);
        self.segment_store_lock_wait_nanos = self
            .segment_store_lock_wait_nanos
            .saturating_add(profile.segment_store_lock_wait_nanos);
        self.checksum_integrity_nanos = self
            .checksum_integrity_nanos
            .saturating_add(profile.checksum_integrity_nanos);
        self.segment_store_insert_nanos = self
            .segment_store_insert_nanos
            .saturating_add(profile.segment_store_insert_nanos);
        self.segment_sync_nanos = self
            .segment_sync_nanos
            .saturating_add(profile.segment_sync_nanos);
        self.segment_sync_lock_wait_nanos = self
            .segment_sync_lock_wait_nanos
            .saturating_add(profile.segment_sync_lock_wait_nanos);
        self.receipt_create_nanos = self
            .receipt_create_nanos
            .saturating_add(profile.receipt_create_nanos);
        self.receipt_verify_nanos = self
            .receipt_verify_nanos
            .saturating_add(profile.receipt_verify_nanos);
        self.catalog_commit_nanos = self
            .catalog_commit_nanos
            .saturating_add(profile.catalog_commit_nanos);
        self.catalog_commit_lock_wait_nanos = self
            .catalog_commit_lock_wait_nanos
            .saturating_add(profile.catalog_commit_lock_wait_nanos);
    }

    fn absorb_mark_referenced(&mut self, profile: LocalMarkReferencedProfile) {
        self.mark_reference_evidence_nanos = self
            .mark_reference_evidence_nanos
            .saturating_add(profile.evidence_create_nanos);
        self.mark_reference_transport_dispatch_nanos = self
            .mark_reference_transport_dispatch_nanos
            .saturating_add(profile.transport_dispatch_nanos);
        self.mark_reference_verify_nanos = self
            .mark_reference_verify_nanos
            .saturating_add(profile.verify_nanos);
        self.mark_reference_catalog_nanos = self
            .mark_reference_catalog_nanos
            .saturating_add(profile.catalog_mark_nanos);
        self.mark_reference_catalog_lock_wait_nanos = self
            .mark_reference_catalog_lock_wait_nanos
            .saturating_add(profile.catalog_mark_lock_wait_nanos);
    }
}

#[derive(Debug)]
struct TxnBlockWriteProfileBuffer {
    capacity: usize,
    next_sequence: u64,
    rows: VecDeque<TxnBlockWriteProfile>,
}

impl TxnBlockWriteProfileBuffer {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "block write profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            rows: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: TxnBlockWriteProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.rows.len() == self.capacity {
            self.rows.pop_front();
        }
        self.rows.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<TxnBlockWriteProfile> {
        let mut out = Vec::new();
        for _ in 0..max {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            out.push(row);
        }
        out
    }
}

#[derive(Debug)]
struct MetadataTxnProfileBuffer {
    capacity: usize,
    next_sequence: u64,
    rows: VecDeque<MetadataTxnProfile>,
}

impl MetadataTxnProfileBuffer {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "metadata transaction profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            rows: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: MetadataTxnProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.rows.len() == self.capacity {
            self.rows.pop_front();
        }
        self.rows.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<MetadataTxnProfile> {
        let mut out = Vec::new();
        for _ in 0..max {
            let Some(row) = self.rows.pop_front() else {
                break;
            };
            out.push(row);
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MetadataTxnKey {
    DeviceManifest(DeviceId),
    DeviceShardHead(DeviceId, ShardId),
    MetadataNode(MetadataNodeId),
    CommitGroup(CommitGroupId),
    ShardCommit(DeviceId, CommitSeq, ShardId),
    ForkRecord(CommitSeq),
    DeleteRecord(CommitSeq),
    Checkpoint(CheckpointId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnDeviceManifest {
    spec: DeviceSpec,
    shard_count: usize,
    live: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TxnShardHead {
    root: MetadataNodeId,
    generation: DeviceGeneration,
    latest_commit: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MetadataTxnValue {
    DeviceManifest(TxnDeviceManifest),
    DeviceShardHead(TxnShardHead),
    MetadataNode(MetadataNode),
    CommitGroup(CommitGroup),
    ShardCommit(ShardCommit),
    ForkRecord(ForkRecord),
    DeleteRecord(DeleteRecord),
    Checkpoint(Checkpoint),
}

#[derive(Debug, Clone)]
struct MetadataTxnEntry {
    version: u64,
    value: MetadataTxnValue,
}

#[derive(Debug, Clone)]
struct MetadataTxnRead {
    key: MetadataTxnKey,
    version: u64,
}

#[derive(Debug, Clone)]
struct VersionedMetadataTxnValue {
    key: MetadataTxnKey,
    version: u64,
    value: Option<MetadataTxnValue>,
}

#[derive(Debug)]
struct SerialMetadataTxnStore {
    map: Mutex<BTreeMap<MetadataTxnKey, MetadataTxnEntry>>,
    next_entry_version: AtomicU64,
    profiler: Mutex<Option<MetadataTxnProfileBuffer>>,
}

#[derive(Debug)]
struct ShardedMetadataTxnStore {
    shards: Vec<Mutex<BTreeMap<MetadataTxnKey, MetadataTxnEntry>>>,
    next_entry_version: AtomicU64,
    profiler: Mutex<Option<MetadataTxnProfileBuffer>>,
}

#[derive(Debug)]
enum MetadataTxnStore {
    Serial(SerialMetadataTxnStore),
    Sharded(ShardedMetadataTxnStore),
}

impl MetadataTxnStore {
    fn new(mode: MetadataTxnMode) -> Result<Self> {
        Ok(match mode {
            MetadataTxnMode::Serial => Self::Serial(SerialMetadataTxnStore {
                map: Mutex::new(BTreeMap::new()),
                next_entry_version: AtomicU64::new(1),
                profiler: Mutex::new(None),
            }),
            MetadataTxnMode::Sharded { shard_count } => {
                if shard_count == 0 {
                    return Err(StorageError::invalid_argument(
                        "metadata transaction shard count must be greater than zero",
                    ));
                }
                let mut shards = Vec::with_capacity(shard_count);
                for _ in 0..shard_count {
                    shards.push(Mutex::new(BTreeMap::new()));
                }
                Self::Sharded(ShardedMetadataTxnStore {
                    shards,
                    next_entry_version: AtomicU64::new(1),
                    profiler: Mutex::new(None),
                })
            }
        })
    }

    fn mode_name(&self) -> &'static str {
        match self {
            Self::Serial(_) => "txn-serial",
            Self::Sharded(_) => "txn-sharded",
        }
    }

    fn enable_profiling(&self, capacity: usize) -> Result<()> {
        match self {
            Self::Serial(store) => {
                *lock(&store.profiler)? = Some(MetadataTxnProfileBuffer::new(capacity)?);
            }
            Self::Sharded(store) => {
                *lock(&store.profiler)? = Some(MetadataTxnProfileBuffer::new(capacity)?);
            }
        }
        Ok(())
    }

    fn drain_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        match self {
            Self::Serial(store) => Ok(lock(&store.profiler)?
                .as_mut()
                .map(|profiler| profiler.drain(max))
                .unwrap_or_default()),
            Self::Sharded(store) => Ok(lock(&store.profiler)?
                .as_mut()
                .map(|profiler| profiler.drain(max))
                .unwrap_or_default()),
        }
    }

    fn record_profile(&self, profile: MetadataTxnProfile) -> Result<()> {
        match self {
            Self::Serial(store) => {
                if let Some(profiler) = lock(&store.profiler)?.as_mut() {
                    profiler.record(profile);
                }
            }
            Self::Sharded(store) => {
                if let Some(profiler) = lock(&store.profiler)?.as_mut() {
                    profiler.record(profile);
                }
            }
        }
        Ok(())
    }

    fn read(&self, keys: &[MetadataTxnKey]) -> Result<Vec<VersionedMetadataTxnValue>> {
        match self {
            Self::Serial(store) => store.read(keys),
            Self::Sharded(store) => store.read(keys),
        }
    }

    fn values(&self) -> Result<Vec<MetadataTxnValue>> {
        match self {
            Self::Serial(store) => store.values(),
            Self::Sharded(store) => store.values(),
        }
    }

    fn commit(
        &self,
        phase: MetadataTxnProfilePhase,
        read_set: Vec<MetadataTxnRead>,
        writes: Vec<(MetadataTxnKey, MetadataTxnValue)>,
        commit_version_alloc_nanos: u64,
    ) -> Result<()> {
        match self {
            Self::Serial(store) => {
                let result = store.commit(phase, read_set, writes, commit_version_alloc_nanos);
                if let Some(profile) = result.profile {
                    self.record_profile(profile)?;
                }
                result.result
            }
            Self::Sharded(store) => {
                let result = store.commit(phase, read_set, writes, commit_version_alloc_nanos);
                if let Some(profile) = result.profile {
                    self.record_profile(profile)?;
                }
                result.result
            }
        }
    }
}

struct MetadataTxnCommitResult {
    profile: Option<MetadataTxnProfile>,
    result: Result<()>,
}

impl SerialMetadataTxnStore {
    fn read(&self, keys: &[MetadataTxnKey]) -> Result<Vec<VersionedMetadataTxnValue>> {
        let map = lock(&self.map)?;
        Ok(keys
            .iter()
            .cloned()
            .map(|key| {
                let entry = map.get(&key);
                VersionedMetadataTxnValue {
                    key,
                    version: entry.map_or(0, |entry| entry.version),
                    value: entry.map(|entry| entry.value.clone()),
                }
            })
            .collect())
    }

    fn values(&self) -> Result<Vec<MetadataTxnValue>> {
        Ok(lock(&self.map)?
            .values()
            .map(|entry| entry.value.clone())
            .collect())
    }

    fn commit(
        &self,
        phase: MetadataTxnProfilePhase,
        read_set: Vec<MetadataTxnRead>,
        writes: Vec<(MetadataTxnKey, MetadataTxnValue)>,
        commit_version_alloc_nanos: u64,
    ) -> MetadataTxnCommitResult {
        let total_started = Instant::now();
        let version_started = Instant::now();
        let entry_version = self.next_entry_version.fetch_add(1, Ordering::SeqCst);
        let entry_version_alloc_nanos = duration_nanos_u64(version_started.elapsed());

        let lock_started = Instant::now();
        let mut map = match self.map.lock() {
            Ok(map) => map,
            Err(_) => {
                return MetadataTxnCommitResult {
                    profile: None,
                    result: Err(StorageError::unavailable(
                        "metadata transaction store lock poisoned",
                    )),
                };
            }
        };
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());

        let validation_started = Instant::now();
        let mut conflict = false;
        for read in &read_set {
            let current = map.get(&read.key).map_or(0, |entry| entry.version);
            if current != read.version {
                conflict = true;
                break;
            }
        }
        let read_validation_nanos = duration_nanos_u64(validation_started.elapsed());

        let mut apply_write_nanos = 0;
        let write_key_count = usize_to_u64(writes.len());
        let result = if conflict {
            Err(StorageError::conflict(
                "metadata transaction read version changed",
            ))
        } else {
            let apply_started = Instant::now();
            for (key, value) in writes {
                map.insert(
                    key,
                    MetadataTxnEntry {
                        version: entry_version,
                        value,
                    },
                );
            }
            apply_write_nanos = duration_nanos_u64(apply_started.elapsed());
            Ok(())
        };

        let mut profile = MetadataTxnProfile::new(phase);
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        profile.tx_lock_wait_nanos = lock_wait_nanos;
        profile.read_validation_nanos = read_validation_nanos;
        profile.apply_write_nanos = apply_write_nanos;
        profile.commit_version_alloc_nanos =
            commit_version_alloc_nanos.saturating_add(entry_version_alloc_nanos);
        profile.touched_key_shards = if read_set.is_empty() && map.is_empty() {
            0
        } else {
            1
        };
        profile.read_key_count = usize_to_u64(read_set.len());
        profile.write_key_count = write_key_count;
        profile.conflict_count = u64::from(conflict);

        MetadataTxnCommitResult {
            profile: Some(profile),
            result,
        }
    }
}

impl ShardedMetadataTxnStore {
    fn key_shard(&self, key: &MetadataTxnKey) -> usize {
        metadata_txn_key_shard(key, self.shards.len())
    }

    fn read(&self, keys: &[MetadataTxnKey]) -> Result<Vec<VersionedMetadataTxnValue>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let shard = self.key_shard(key);
            let map = lock(&self.shards[shard])?;
            let entry = map.get(key);
            out.push(VersionedMetadataTxnValue {
                key: key.clone(),
                version: entry.map_or(0, |entry| entry.version),
                value: entry.map(|entry| entry.value.clone()),
            });
        }
        Ok(out)
    }

    fn values(&self) -> Result<Vec<MetadataTxnValue>> {
        let mut out = Vec::new();
        for shard in &self.shards {
            out.extend(lock(shard)?.values().map(|entry| entry.value.clone()));
        }
        Ok(out)
    }

    fn commit(
        &self,
        phase: MetadataTxnProfilePhase,
        read_set: Vec<MetadataTxnRead>,
        writes: Vec<(MetadataTxnKey, MetadataTxnValue)>,
        commit_version_alloc_nanos: u64,
    ) -> MetadataTxnCommitResult {
        let total_started = Instant::now();
        let version_started = Instant::now();
        let entry_version = self.next_entry_version.fetch_add(1, Ordering::SeqCst);
        let entry_version_alloc_nanos = duration_nanos_u64(version_started.elapsed());

        let mut touched: Vec<usize> = read_set
            .iter()
            .map(|read| self.key_shard(&read.key))
            .chain(writes.iter().map(|(key, _)| self.key_shard(key)))
            .collect();
        touched.sort_unstable();
        touched.dedup();

        let lock_started = Instant::now();
        let mut guards = Vec::with_capacity(touched.len());
        for shard in &touched {
            let guard = match self.shards[*shard].lock() {
                Ok(guard) => guard,
                Err(_) => {
                    return MetadataTxnCommitResult {
                        profile: None,
                        result: Err(StorageError::unavailable(
                            "metadata transaction shard lock poisoned",
                        )),
                    };
                }
            };
            guards.push((*shard, guard));
        }
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());

        let validation_started = Instant::now();
        let mut conflict = false;
        for read in &read_set {
            let shard = self.key_shard(&read.key);
            let current = guards
                .iter()
                .find(|(candidate, _)| *candidate == shard)
                .and_then(|(_, map)| map.get(&read.key))
                .map_or(0, |entry| entry.version);
            if current != read.version {
                conflict = true;
                break;
            }
        }
        let read_validation_nanos = duration_nanos_u64(validation_started.elapsed());

        let mut write_count = 0;
        let mut apply_write_nanos = 0;
        let result = if conflict {
            Err(StorageError::conflict(
                "metadata transaction read version changed",
            ))
        } else {
            let apply_started = Instant::now();
            for (key, value) in writes {
                let shard = self.key_shard(&key);
                let (_, map) = guards
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == shard)
                    .expect("write shard is locked");
                map.insert(
                    key,
                    MetadataTxnEntry {
                        version: entry_version,
                        value,
                    },
                );
                write_count += 1;
            }
            apply_write_nanos = duration_nanos_u64(apply_started.elapsed());
            Ok(())
        };

        let mut profile = MetadataTxnProfile::new(phase);
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        profile.tx_lock_wait_nanos = lock_wait_nanos;
        profile.read_validation_nanos = read_validation_nanos;
        profile.apply_write_nanos = apply_write_nanos;
        profile.commit_version_alloc_nanos =
            commit_version_alloc_nanos.saturating_add(entry_version_alloc_nanos);
        profile.touched_key_shards = usize_to_u64(touched.len());
        profile.read_key_count = usize_to_u64(read_set.len());
        profile.write_key_count = usize_to_u64(write_count);
        profile.conflict_count = u64::from(conflict);

        MetadataTxnCommitResult {
            profile: Some(profile),
            result,
        }
    }
}

fn metadata_txn_key_shard(key: &MetadataTxnKey, shard_count: usize) -> usize {
    if shard_count <= 1 {
        return 0;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    fn mix(hash: &mut u64, value: u64) {
        for byte in value.to_be_bytes() {
            *hash ^= u64::from(byte);
            *hash = hash.wrapping_mul(0x1000_0000_01b3);
        }
    }
    match key {
        MetadataTxnKey::DeviceManifest(device_id) => {
            mix(&mut hash, 1);
            mix(&mut hash, raw_u128_low64(device_id.raw()));
        }
        MetadataTxnKey::DeviceShardHead(device_id, shard_id) => {
            mix(&mut hash, 2);
            mix(&mut hash, raw_u128_low64(device_id.raw()));
            mix(&mut hash, u64::from(shard_id.raw()));
        }
        MetadataTxnKey::MetadataNode(node_id) => {
            mix(&mut hash, 3);
            mix(&mut hash, raw_u128_low64(node_id.raw()));
        }
        MetadataTxnKey::CommitGroup(commit_group) => {
            mix(&mut hash, 4);
            mix(&mut hash, raw_u128_low64(commit_group.raw()));
        }
        MetadataTxnKey::ShardCommit(device_id, commit_seq, shard_id) => {
            mix(&mut hash, 5);
            mix(&mut hash, raw_u128_low64(device_id.raw()));
            mix(&mut hash, commit_seq.raw());
            mix(&mut hash, u64::from(shard_id.raw()));
        }
        MetadataTxnKey::ForkRecord(commit_seq) => {
            mix(&mut hash, 6);
            mix(&mut hash, commit_seq.raw());
        }
        MetadataTxnKey::DeleteRecord(commit_seq) => {
            mix(&mut hash, 7);
            mix(&mut hash, commit_seq.raw());
        }
        MetadataTxnKey::Checkpoint(checkpoint_id) => {
            mix(&mut hash, 8);
            mix(&mut hash, raw_u128_low64(checkpoint_id.raw()));
        }
    }
    (hash as usize) % shard_count
}

fn raw_u128_low64(raw: u128) -> u64 {
    (raw & u128::from(u64::MAX)) as u64
}

/// Block-focused metadata plane backed by the in-memory transaction store.
#[derive(Debug)]
pub struct TxnBlockMetadataPlane {
    config: LocalStoreConfig,
    store: MetadataTxnStore,
    next_device_id: AtomicU64,
    next_metadata_node_id: AtomicU64,
    next_commit_group_id: AtomicU64,
    next_public_commit_seq: AtomicU64,
    next_checkpoint_id: AtomicU64,
}

impl TxnBlockMetadataPlane {
    pub fn new(config: LocalStoreConfig, mode: MetadataTxnMode) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            store: MetadataTxnStore::new(mode)?,
            next_device_id: AtomicU64::new(1),
            next_metadata_node_id: AtomicU64::new(1),
            next_commit_group_id: AtomicU64::new(1),
            next_public_commit_seq: AtomicU64::new(1),
            next_checkpoint_id: AtomicU64::new(1),
        })
    }

    pub fn provider_name(&self) -> &'static str {
        self.store.mode_name()
    }

    pub fn enable_profiling(&self, capacity: usize) -> Result<()> {
        self.store.enable_profiling(capacity)
    }

    pub fn drain_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        self.store.drain_profiles(max)
    }

    fn alloc_device_id(&self) -> Result<DeviceId> {
        let raw = self.next_device_id.fetch_add(1, Ordering::SeqCst);
        Ok(DeviceId::from_raw(u128::from(raw)))
    }

    fn alloc_metadata_node_id(&self) -> Result<MetadataNodeId> {
        let raw = self.next_metadata_node_id.fetch_add(1, Ordering::SeqCst);
        Ok(MetadataNodeId::from_raw(u128::from(raw)))
    }

    fn alloc_commit_group_id(&self) -> Result<CommitGroupId> {
        let raw = self.next_commit_group_id.fetch_add(1, Ordering::SeqCst);
        Ok(CommitGroupId::from_raw(u128::from(raw)))
    }

    fn alloc_checkpoint_id(&self) -> Result<CheckpointId> {
        let raw = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        Ok(CheckpointId::from_raw(u128::from(raw)))
    }

    fn alloc_public_commit_seq(&self) -> Result<(CommitSeq, u64)> {
        let started = Instant::now();
        let raw = self.next_public_commit_seq.fetch_add(1, Ordering::SeqCst);
        Ok((
            CommitSeq::from_raw(raw),
            duration_nanos_u64(started.elapsed()),
        ))
    }

    fn read_values(&self, keys: &[MetadataTxnKey]) -> Result<Vec<VersionedMetadataTxnValue>> {
        self.store.read(keys)
    }

    fn read_one(&self, key: MetadataTxnKey) -> Result<VersionedMetadataTxnValue> {
        self.read_values(&[key])?
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::corrupt("metadata transaction read returned no value"))
    }

    fn read_required(&self, key: MetadataTxnKey) -> Result<(MetadataTxnRead, MetadataTxnValue)> {
        let read = self.read_one(key)?;
        let value = read
            .value
            .clone()
            .ok_or_else(|| StorageError::not_found("metadata_key", format!("{:?}", read.key)))?;
        Ok((
            MetadataTxnRead {
                key: read.key,
                version: read.version,
            },
            value,
        ))
    }

    fn read_manifest(&self, device_id: DeviceId) -> Result<(MetadataTxnRead, TxnDeviceManifest)> {
        let (read, value) = self.read_required(MetadataTxnKey::DeviceManifest(device_id))?;
        let MetadataTxnValue::DeviceManifest(manifest) = value else {
            return Err(StorageError::corrupt(
                "device manifest key has wrong value kind",
            ));
        };
        if !manifest.live {
            return Err(StorageError::not_found("device", device_id.to_string()));
        }
        Ok((read, manifest))
    }

    fn read_shard_head(
        &self,
        device_id: DeviceId,
        shard_id: ShardId,
    ) -> Result<(MetadataTxnRead, TxnShardHead)> {
        let (read, value) =
            self.read_required(MetadataTxnKey::DeviceShardHead(device_id, shard_id))?;
        let MetadataTxnValue::DeviceShardHead(head) = value else {
            return Err(StorageError::corrupt(
                "device shard-head key has wrong value kind",
            ));
        };
        Ok((read, head))
    }

    fn create_empty_tree_nodes(
        &self,
        range: crate::api::BlockRange,
        nodes: &mut Vec<MetadataNode>,
    ) -> Result<MetadataNodeId> {
        range.validate_non_empty()?;
        if range.blocks.raw() <= self.config.metadata_leaf_blocks {
            let node = MetadataNode {
                node_id: self.alloc_metadata_node_id()?,
                covered_range: range,
                kind: MetadataNodeKind::Leaf {
                    entries: Vec::new(),
                    run_extents: Vec::new(),
                },
            };
            node.validate(&[])?;
            let node_id = node.node_id;
            nodes.push(node);
            return Ok(node_id);
        }

        let child_count =
            self.config
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
            let child_node = self.create_empty_tree_nodes(child_range, nodes)?;
            children.push(MetadataChild {
                range: child_range,
                node_id: child_node,
            });
        }

        let node = MetadataNode {
            node_id: self.alloc_metadata_node_id()?,
            covered_range: range,
            kind: MetadataNodeKind::Internal { children },
        };
        node.validate(&[])?;
        let node_id = node.node_id;
        nodes.push(node);
        Ok(node_id)
    }

    pub fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead> {
        request.spec.validate()?;
        if request.spec.logical_blocks < self.config.shard_count as u64 {
            return Err(StorageError::invalid_argument(
                "device logical blocks must cover every shard",
            ));
        }
        let device_id = self.alloc_device_id()?;
        let shard_count_u64 = u64::try_from(self.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard count overflows u64"))?;
        let mut shard_roots = Vec::with_capacity(self.config.shard_count);
        let mut writes = Vec::new();
        for shard in 0..self.config.shard_count {
            let shard_u64 = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = request
                .spec
                .logical_blocks
                .checked_mul(shard_u64)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count_u64;
            let end = request
                .spec
                .logical_blocks
                .checked_mul(shard_u64 + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count_u64;
            let mut nodes = Vec::new();
            let root = self.create_empty_tree_nodes(
                crate::api::BlockRange::new(
                    BlockIndex::from_raw(start),
                    BlockCount::from_raw(end - start),
                ),
                &mut nodes,
            )?;
            for node in nodes {
                writes.push((
                    MetadataTxnKey::MetadataNode(node.node_id),
                    MetadataTxnValue::MetadataNode(node),
                ));
            }
            shard_roots.push(root);
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(device_id, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: CommitSeq::from_raw(0),
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::DeviceManifest(device_id),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: request.spec.clone(),
                shard_count: self.config.shard_count,
                live: true,
            }),
        ));

        let head = DeviceHead {
            device_id,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: shard_roots.clone(),
            latest_commit: CommitSeq::from_raw(0),
        };
        head.validate(self.config.shard_count)?;
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq: CommitSeq::from_raw(0),
                time: LogicalTime::from_raw(0),
                owner: MappingOwner::BlockDevice(device_id),
                roots: CheckpointRoots::BlockShard(shard_roots),
            }),
        ));
        self.store
            .commit(MetadataTxnProfilePhase::CreateDevice, Vec::new(), writes, 0)?;
        Ok(head)
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let (_, manifest) = self.read_manifest(device_id)?;
        let head = self.get_head(device_id)?;
        Ok(DeviceInfo {
            device_id,
            generation: head.generation,
            spec: manifest.spec,
            latest_commit: head.latest_commit,
        })
    }

    pub fn device_spec(&self, device_id: DeviceId) -> Result<DeviceSpec> {
        self.read_manifest(device_id)
            .map(|(_, manifest)| manifest.spec)
    }

    pub fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead> {
        let (_, manifest) = self.read_manifest(device_id)?;
        let mut shard_roots = Vec::with_capacity(manifest.shard_count);
        let mut generation = DeviceGeneration::from_raw(0);
        let mut latest_commit = CommitSeq::from_raw(0);
        for shard in 0..manifest.shard_count {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            let (_, shard_head) = self.read_shard_head(device_id, shard_id)?;
            shard_roots.push(shard_head.root);
            if shard_head.generation.raw() > generation.raw() {
                generation = shard_head.generation;
            }
            if shard_head.latest_commit.raw() > latest_commit.raw() {
                latest_commit = shard_head.latest_commit;
            }
        }
        let head = DeviceHead {
            device_id,
            generation,
            shard_roots,
            latest_commit,
        };
        head.validate(manifest.shard_count)?;
        Ok(head)
    }

    pub fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode> {
        let (_, value) = self.read_required(MetadataTxnKey::MetadataNode(node_id))?;
        let MetadataTxnValue::MetadataNode(node) = value else {
            return Err(StorageError::corrupt(
                "metadata node key has wrong value kind",
            ));
        };
        Ok(node)
    }

    pub fn allocate_metadata_node(
        &self,
        covered_range: crate::api::BlockRange,
        kind: MetadataNodeKind,
    ) -> Result<MetadataNode> {
        Ok(MetadataNode {
            node_id: self.alloc_metadata_node_id()?,
            covered_range,
            kind,
        })
    }

    pub fn persist_metadata_node(&self, write: MetadataNodeWrite) -> Result<()> {
        let segment_descriptors = write.segment_descriptors();
        write.node.validate(&segment_descriptors)?;
        let key = MetadataTxnKey::MetadataNode(write.node.node_id);
        let existing = self.read_one(key.clone())?;
        if let Some(MetadataTxnValue::MetadataNode(existing_node)) = &existing.value {
            if existing_node == &write.node {
                return Ok(());
            }
            return Err(StorageError::conflict(
                "metadata node ID already exists with different content",
            ));
        }
        if existing.value.is_some() {
            return Err(StorageError::corrupt(
                "metadata node key has wrong value kind",
            ));
        }
        self.store.commit(
            MetadataTxnProfilePhase::PersistNode,
            vec![MetadataTxnRead {
                key: existing.key,
                version: existing.version,
            }],
            vec![(key, MetadataTxnValue::MetadataNode(write.node))],
            0,
        )
    }

    pub fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup> {
        let MappingOwner::BlockDevice(device_id) = intent.owner else {
            return Err(StorageError::unsupported(
                "transaction metadata backend only supports block commit groups",
            ));
        };
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

        let (_, manifest) = self.read_manifest(device_id)?;
        let mut read_set = Vec::new();
        let mut writes = Vec::new();
        let mut shard_commits = Vec::with_capacity(intent.updates.len());
        let mut shard_head_writes = Vec::with_capacity(intent.updates.len());

        for update in &intent.updates {
            let RootUpdate::BlockShard(update) = update else {
                return Err(StorageError::invalid_argument(
                    "block device commit cannot include file-root updates",
                ));
            };
            let shard = usize::try_from(update.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            if shard >= manifest.shard_count {
                return Err(StorageError::invalid_argument(
                    "shard update is outside device root set",
                ));
            }
            let (read, shard_head) = self.read_shard_head(device_id, update.shard_id)?;
            if shard_head.root != update.old_root {
                let mut profile =
                    MetadataTxnProfile::new(MetadataTxnProfilePhase::PublishBlockCommit);
                profile.logical_conflict_for_manual();
                self.store.record_profile(profile)?;
                return Err(StorageError::conflict("stale shard root"));
            }
            let _ = self.get_metadata_node(update.new_root)?;
            read_set.push(read);
            shard_commits.push((update.shard_id, update.old_root, update.new_root));
            shard_head_writes.push((
                update.shard_id,
                update.new_root,
                DeviceGeneration::from_raw(
                    shard_head
                        .generation
                        .raw()
                        .checked_add(1)
                        .ok_or_else(|| StorageError::conflict("device generation overflow"))?,
                ),
            ));
        }

        let (commit_seq, commit_version_alloc_nanos) = self.alloc_public_commit_seq()?;
        let commit_group_id = self.alloc_commit_group_id()?;
        for (shard_id, root, generation) in shard_head_writes {
            writes.push((
                MetadataTxnKey::DeviceShardHead(device_id, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation,
                    latest_commit: commit_seq,
                }),
            ));
        }
        let commit_group = CommitGroup {
            commit_group: commit_group_id,
            commit_seq,
            owner: MappingOwner::BlockDevice(device_id),
            updates: intent.updates,
        };
        for (shard_id, old_root, new_root) in shard_commits {
            writes.push((
                MetadataTxnKey::ShardCommit(device_id, commit_seq, shard_id),
                MetadataTxnValue::ShardCommit(ShardCommit {
                    commit_seq,
                    commit_group: commit_group_id,
                    time: LogicalTime::from_raw(commit_seq.raw()),
                    device_id,
                    shard_id,
                    old_root,
                    new_root,
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::CommitGroup(commit_group_id),
            MetadataTxnValue::CommitGroup(commit_group.clone()),
        ));

        self.store.commit(
            MetadataTxnProfilePhase::PublishBlockCommit,
            read_set,
            writes,
            commit_version_alloc_nanos,
        )?;
        Ok(commit_group)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        let head = self.get_head(device_id)?;
        let checkpoint_id = self.alloc_checkpoint_id()?;
        self.store.commit(
            MetadataTxnProfilePhase::Checkpoint,
            Vec::new(),
            vec![(
                MetadataTxnKey::Checkpoint(checkpoint_id),
                MetadataTxnValue::Checkpoint(Checkpoint {
                    checkpoint_id,
                    commit_seq: head.latest_commit,
                    time: LogicalTime::from_raw(head.latest_commit.raw()),
                    owner: MappingOwner::BlockDevice(device_id),
                    roots: CheckpointRoots::BlockShard(head.shard_roots),
                }),
            )],
            0,
        )?;
        Ok(checkpoint_id)
    }

    pub fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead> {
        let (_, source_manifest) = self.read_manifest(request.source)?;
        let source_head = self.get_head(request.source)?;
        let target = match request.target {
            Some(target) => target,
            None => self.alloc_device_id()?,
        };
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        let mut writes = Vec::new();
        writes.push((
            MetadataTxnKey::DeviceManifest(target),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: source_manifest.spec,
                shard_count: source_manifest.shard_count,
                live: true,
            }),
        ));
        for (shard, root) in source_head.shard_roots.iter().copied().enumerate() {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(target, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: commit_seq,
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::ForkRecord(commit_seq),
            MetadataTxnValue::ForkRecord(ForkRecord {
                commit_seq,
                source: request.source,
                target,
                shard_roots: source_head.shard_roots.clone(),
            }),
        ));
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq,
                time: LogicalTime::from_raw(commit_seq.raw()),
                owner: MappingOwner::BlockDevice(target),
                roots: CheckpointRoots::BlockShard(source_head.shard_roots.clone()),
            }),
        ));
        self.store.commit(
            MetadataTxnProfilePhase::Fork,
            Vec::new(),
            writes,
            alloc_nanos,
        )?;
        Ok(DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: source_head.shard_roots,
            latest_commit: commit_seq,
        })
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceHead> {
        let (_, source_manifest) = self.read_manifest(source)?;
        let target_commit = self.target_commit_for_restore(source, point)?;
        let shard_roots = self.replay_device_roots(source, target_commit)?;
        let target = self.alloc_device_id()?;
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        let mut writes = Vec::new();
        writes.push((
            MetadataTxnKey::DeviceManifest(target),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: source_manifest.spec,
                shard_count: source_manifest.shard_count,
                live: true,
            }),
        ));
        for (shard, root) in shard_roots.iter().copied().enumerate() {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(target, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: commit_seq,
                }),
            ));
        }
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq,
                time: LogicalTime::from_raw(commit_seq.raw()),
                owner: MappingOwner::BlockDevice(target),
                roots: CheckpointRoots::BlockShard(shard_roots.clone()),
            }),
        ));
        self.store.commit(
            MetadataTxnProfilePhase::Restore,
            Vec::new(),
            writes,
            alloc_nanos,
        )?;
        Ok(DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit: commit_seq,
        })
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        let (manifest_read, mut manifest) = self.read_manifest(device_id)?;
        let head = self.get_head(device_id)?;
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        manifest.live = false;
        self.store.commit(
            MetadataTxnProfilePhase::Delete,
            vec![manifest_read],
            vec![
                (
                    MetadataTxnKey::DeviceManifest(device_id),
                    MetadataTxnValue::DeviceManifest(manifest),
                ),
                (
                    MetadataTxnKey::DeleteRecord(commit_seq),
                    MetadataTxnValue::DeleteRecord(DeleteRecord {
                        commit_seq,
                        time: LogicalTime::from_raw(commit_seq.raw()),
                        device_id,
                        shard_roots: head.shard_roots,
                    }),
                ),
            ],
            alloc_nanos,
        )?;
        Ok(DeleteResult {
            device_id,
            commit_seq,
        })
    }

    pub fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        let mut roots = BTreeSet::new();
        for value in self.store.values()? {
            match value {
                MetadataTxnValue::DeviceShardHead(head) => {
                    roots.insert(head.root);
                }
                MetadataTxnValue::ShardCommit(commit) => {
                    roots.insert(commit.old_root);
                    roots.insert(commit.new_root);
                }
                MetadataTxnValue::ForkRecord(record) => {
                    roots.extend(record.shard_roots);
                }
                MetadataTxnValue::DeleteRecord(record) if policy.retain_deleted_devices => {
                    roots.extend(record.shard_roots);
                }
                MetadataTxnValue::Checkpoint(checkpoint) => {
                    if let CheckpointRoots::BlockShard(shard_roots) = checkpoint.roots {
                        roots.extend(shard_roots);
                    }
                }
                _ => {}
            }
        }
        Ok(roots.into_iter().collect())
    }

    fn target_commit_for_restore(
        &self,
        source: DeviceId,
        point: RestorePoint,
    ) -> Result<CommitSeq> {
        match point {
            RestorePoint::Commit(commit) => Ok(commit),
            RestorePoint::Time(time) => Ok(CommitSeq::from_raw(time.raw())),
            RestorePoint::Checkpoint(checkpoint_id) => {
                let (_, value) = self.read_required(MetadataTxnKey::Checkpoint(checkpoint_id))?;
                let MetadataTxnValue::Checkpoint(checkpoint) = value else {
                    return Err(StorageError::corrupt("checkpoint key has wrong value kind"));
                };
                if checkpoint.owner != MappingOwner::BlockDevice(source) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source device",
                    ));
                }
                Ok(checkpoint.commit_seq)
            }
        }
    }

    fn replay_device_roots(
        &self,
        source: DeviceId,
        target_commit: CommitSeq,
    ) -> Result<Vec<MetadataNodeId>> {
        let values = self.store.values()?;
        let mut checkpoints = Vec::new();
        let mut commits = Vec::new();
        for value in values {
            match value {
                MetadataTxnValue::Checkpoint(checkpoint)
                    if checkpoint.owner == MappingOwner::BlockDevice(source)
                        && checkpoint.commit_seq.raw() <= target_commit.raw() =>
                {
                    checkpoints.push(checkpoint);
                }
                MetadataTxnValue::ShardCommit(commit)
                    if commit.device_id == source
                        && commit.commit_seq.raw() <= target_commit.raw() =>
                {
                    commits.push(commit);
                }
                _ => {}
            }
        }
        checkpoints.sort_by_key(|checkpoint| checkpoint.commit_seq.raw());
        let checkpoint = checkpoints
            .last()
            .cloned()
            .ok_or_else(|| StorageError::not_found("checkpoint", source.to_string()))?;
        let CheckpointRoots::BlockShard(mut roots) = checkpoint.roots else {
            return Err(StorageError::corrupt("block checkpoint has native roots"));
        };
        commits.sort_by_key(|commit| (commit.commit_seq.raw(), commit.shard_id.raw()));
        for commit in commits
            .into_iter()
            .filter(|commit| commit.commit_seq.raw() > checkpoint.commit_seq.raw())
        {
            let shard = usize::try_from(commit.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            let Some(root) = roots.get_mut(shard) else {
                return Err(StorageError::corrupt(
                    "shard commit is outside checkpoint roots",
                ));
            };
            *root = commit.new_root;
        }
        Ok(roots)
    }
}

impl MetadataTxnProfile {
    fn logical_conflict_for_manual(&mut self) {
        self.conflict_count = 1;
        self.total_nanos = 0;
    }
}

/// Block coordinator using the transaction metadata service simulator.
#[derive(Debug, Clone)]
pub struct TxnBlockCoordinator {
    local: LocalCoordinator,
    metadata: Arc<TxnBlockMetadataPlane>,
    storage_node_count: usize,
    block_write_profile_enabled: Arc<AtomicBool>,
    block_write_profiler: Arc<Mutex<Option<TxnBlockWriteProfileBuffer>>>,
}

impl TxnBlockCoordinator {
    pub fn with_storage_nodes(
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        mode: MetadataTxnMode,
    ) -> Result<Self> {
        let storage_node_count =
            normalize_storage_nodes(config.storage_node, storage_nodes.clone()).len();
        Ok(Self {
            local: LocalCoordinator::with_storage_nodes(config, storage_nodes)?,
            metadata: Arc::new(TxnBlockMetadataPlane::new(config, mode)?),
            storage_node_count,
            block_write_profile_enabled: Arc::new(AtomicBool::new(false)),
            block_write_profiler: Arc::new(Mutex::new(None)),
        })
    }

    pub fn provider_name(&self) -> &'static str {
        self.metadata.provider_name()
    }

    pub fn enable_metadata_profiling(&self, capacity: usize) -> Result<()> {
        self.metadata.enable_profiling(capacity)
    }

    pub fn drain_metadata_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        self.metadata.drain_profiles(max)
    }

    pub fn enable_block_write_profiling(&self, capacity: usize) -> Result<()> {
        *lock(&self.block_write_profiler)? = Some(TxnBlockWriteProfileBuffer::new(capacity)?);
        self.block_write_profile_enabled
            .store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn drain_block_write_profiles(&self, max: usize) -> Result<Vec<TxnBlockWriteProfile>> {
        Ok(lock(&self.block_write_profiler)?
            .as_mut()
            .map(|profiler| profiler.drain(max))
            .unwrap_or_default())
    }

    fn record_block_write_profile(&self, profile: TxnBlockWriteProfile) -> Result<()> {
        if !self.block_write_profile_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(profiler) = lock(&self.block_write_profiler)?.as_mut() {
            profiler.record(profile);
        }
        Ok(())
    }

    pub fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.metadata
            .create_device(MetadataCreateDeviceRequest::from(request))
            .map(|head| head.device_id)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.metadata.checkpoint(device_id)
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        self.metadata
            .fork_device(MetadataForkRequest {
                source,
                target: request.target,
                name: request.name,
            })
            .map(|head| head.device_id)
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        self.metadata
            .restore_device(source, point)
            .map(|head| head.device_id)
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.metadata.delete_device(device_id)
    }

    pub fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        self.metadata.roots_for_gc(policy)
    }

    pub fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        let info = self.metadata.device_info(device_id)?;
        Ok(FlushResult {
            device_id,
            durable_through: info.latest_commit,
        })
    }

    fn split_device_range(
        &self,
        device_id: DeviceId,
        spec: &DeviceSpec,
        range: ByteRange,
    ) -> Result<Vec<DeviceWriteChunk>> {
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let shard_count_u64 = u64::try_from(self.metadata.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard count overflows u64"))?;
        let mut chunks = Vec::new();
        for shard in 0..self.metadata.config.shard_count {
            let shard_u64 = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = spec
                .logical_blocks
                .checked_mul(shard_u64)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count_u64;
            let end = spec
                .logical_blocks
                .checked_mul(shard_u64 + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count_u64;
            let shard_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(start),
                BlockCount::from_raw(end - start),
            );
            let Some(overlap) = shard_range.intersection(requested)? else {
                continue;
            };
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            let (_, shard_head) = self.metadata.read_shard_head(device_id, shard_id)?;
            chunks.push(DeviceWriteChunk {
                shard_id,
                old_root: shard_head.root,
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

    pub fn write_device_with_integrity(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<WriteCommit> {
        let profile_enabled = self.block_write_profile_enabled.load(Ordering::Relaxed);
        let total_started = profile_enabled.then(Instant::now);
        let mut profile = TxnBlockWriteProfile {
            storage_node_count: usize_to_u64(self.storage_node_count),
            ..TxnBlockWriteProfile::default()
        };

        let started = profile_enabled.then(Instant::now);
        let spec = self.metadata.device_spec(device_id)?;
        let len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&spec)?;
        if let Some(started) = started {
            profile.device_spec_lookup_nanos = duration_nanos_u64(started.elapsed());
        }
        if len == 0 {
            let info = self.metadata.device_info(device_id)?;
            if let Some(started) = total_started {
                profile.total_nanos = duration_nanos_u64(started.elapsed());
                self.record_block_write_profile(profile)?;
            }
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability,
            });
        }

        let block_size = u64::from(spec.block_size);
        let started = profile_enabled.then(Instant::now);
        let chunks = self.split_device_range(device_id, &spec, range)?;
        if let Some(started) = started {
            profile.range_split_shard_head_read_nanos = duration_nanos_u64(started.elapsed());
        }
        profile.touched_shard_count = usize_to_u64(chunks.len());
        profile.segment_count = usize_to_u64(chunks.len());

        let started = profile_enabled.then(Instant::now);
        let write_intent = self.local.next_write_intent()?;
        if let Some(started) = started {
            profile.write_intent_alloc_nanos = duration_nanos_u64(started.elapsed());
        }
        let mut updates = Vec::with_capacity(chunks.len());
        let mut segment_receipts = Vec::with_capacity(chunks.len());

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
            let started = profile_enabled.then(Instant::now);
            let chunk_bytes = chunk_bytes.to_vec();
            if let Some(started) = started {
                profile.payload_copy_nanos = profile
                    .payload_copy_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }

            let started = profile_enabled.then(Instant::now);
            let intent = WriteGrantIntent::BlockWrite {
                device_id,
                range: chunk.range,
                fence: DeviceGeneration::from_raw(0),
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
            };
            let verified_receipt = if profile_enabled {
                let (receipt, segment_profile) = self
                    .local
                    .write_segment_for_intent_with_id_owned_verified_profiled(
                        intent,
                        write_intent,
                        chunk_bytes,
                        durability,
                        payload_integrity,
                    )?;
                profile.absorb_segment_write(segment_profile);
                receipt
            } else {
                self.local.write_segment_for_intent_with_id_owned_verified(
                    intent,
                    write_intent,
                    chunk_bytes,
                    durability,
                    payload_integrity,
                )?
            };
            if let Some(started) = started {
                profile.segment_write_nanos = profile
                    .segment_write_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }
            let segment_id = verified_receipt.descriptor.segment_id;
            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: Some(SegmentReplacement {
                    segment_id,
                    segment_base: chunk.range.start,
                }),
            };
            let started = profile_enabled.then(Instant::now);
            let new_root = self
                .replace_tree_range_with_receipts(
                    chunk.old_root,
                    edit,
                    std::slice::from_ref(&verified_receipt),
                )?
                .root;
            if let Some(started) = started {
                profile.tree_path_copy_nanos = profile
                    .tree_path_copy_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }
            segment_receipts.push(verified_receipt);
            updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
                new_root,
            }));
        }

        let started = profile_enabled.then(Instant::now);
        let commit_group = self.metadata.publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(device_id),
            fence: MetadataFence::DeviceGeneration(DeviceGeneration::from_raw(0)),
            updates,
        })?;
        if let Some(started) = started {
            profile.metadata_publish_call_nanos = duration_nanos_u64(started.elapsed());
        }

        let started = profile_enabled.then(Instant::now);
        for receipt in &segment_receipts {
            if profile_enabled {
                let mark_profile = self.local.storage_nodes.mark_segment_referenced_profiled(
                    receipt.receipt(),
                    commit_group.commit_seq,
                    self.local.authority.as_ref(),
                )?;
                profile.absorb_mark_referenced(mark_profile);
            } else {
                self.local.storage_nodes.mark_segment_referenced(
                    receipt.receipt(),
                    commit_group.commit_seq,
                    self.local.authority.as_ref(),
                )?;
            }
        }
        if let Some(started) = started {
            profile.mark_referenced_nanos = duration_nanos_u64(started.elapsed());
        }
        if let Some(started) = total_started {
            profile.total_nanos = duration_nanos_u64(started.elapsed());
            self.record_block_write_profile(profile)?;
        }
        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability,
        })
    }

    pub fn read_device_with_verification(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let segment_ids = self.segment_ids_for_device_read(device_id, range)?;
        self.read_device_unverified(device_id, range, buf)?;
        for segment_id in segment_ids {
            self.local
                .storage_nodes
                .verify_segment_payload_for_read(segment_id, verification)?;
        }
        Ok(())
    }

    fn read_device_unverified(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        let spec = self.metadata.device_spec(device_id)?;
        range.validate_for_device(&spec)?;
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
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        for chunk in self.split_device_range(device_id, &spec, range)? {
            let node = self.metadata.get_metadata_node(chunk.old_root)?;
            if node.covered_range.overlaps(requested)? {
                self.read_metadata_node(&node, requested, block_size, buf)?;
            }
        }
        Ok(())
    }

    fn segment_ids_for_device_read(
        &self,
        device_id: DeviceId,
        range: ByteRange,
    ) -> Result<BTreeSet<SegmentId>> {
        let spec = self.metadata.device_spec(device_id)?;
        range.validate_for_device(&spec)?;
        let mut out = BTreeSet::new();
        if range.len == 0 {
            return Ok(out);
        }
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        for chunk in self.split_device_range(device_id, &spec, range)? {
            let node = self.metadata.get_metadata_node(chunk.old_root)?;
            if node.covered_range.overlaps(requested)? {
                self.collect_segment_ids_for_metadata_node(&node, requested, &mut out)?;
            }
        }
        Ok(out)
    }

    fn collect_segment_ids_for_metadata_node(
        &self,
        node: &MetadataNode,
        requested: crate::api::BlockRange,
        out: &mut BTreeSet<SegmentId>,
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.collect_segment_ids_for_metadata_node(&child_node, requested, out)?;
                    }
                }
            }
            MetadataNodeKind::Leaf { entries, .. } => {
                for entry in entries {
                    if entry.logical_range().overlaps(requested)? {
                        out.insert(entry.segment_id);
                    }
                }
            }
        }
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
            MetadataNodeKind::Leaf { entries, .. } => {
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
                    self.local.storage_nodes.read_segment(
                        entry.segment_id,
                        segment_range,
                        output,
                    )?;
                }
                Ok(())
            }
        }
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
                    let receipt = self
                        .local
                        .storage_nodes
                        .receipt_for_segment(entry.segment_id)?;
                    vacant.insert(self.local.authority.verify_segment_receipt(&receipt)?);
                }
            }
        }
        Ok(receipts.into_values().collect())
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
            MetadataNodeKind::Leaf {
                entries,
                run_extents,
            } => {
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
                let block_size = u64::from(self.metadata.config.block_size);
                let overlap_bytes = block_range_to_byte_range(overlap, block_size)?;
                let new_run_extents =
                    replace_run_backed_file_extents(run_extents, overlap_bytes, Vec::new())?;
                if new_entries == *entries && new_run_extents == *run_extents {
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
                        run_extents: new_run_extents,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> LocalStoreConfig {
        LocalStoreConfig {
            shard_count: 2,
            block_size: 4096,
            file_root_blocks: 8,
            metadata_fanout: 2,
            metadata_leaf_blocks: 1024,
            storage_node: StorageNodeId::from_raw(77),
            observability_event_capacity: 1024,
        }
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

    #[test]
    fn txn_metadata_publish_conflicts_only_on_touched_shards() {
        for mode in [
            MetadataTxnMode::Serial,
            MetadataTxnMode::Sharded { shard_count: 8 },
        ] {
            let metadata = TxnBlockMetadataPlane::new(config(), mode).unwrap();
            let head = metadata.create_device(device_request()).unwrap();
            let shard_zero = metadata_leaf(10_001, 0, 8);
            let shard_one = metadata_leaf(10_002, 8, 8);
            let stale_zero = metadata_leaf(10_003, 0, 8);
            metadata
                .persist_metadata_node(MetadataNodeWrite::new(shard_zero.clone(), Vec::new()))
                .unwrap();
            metadata
                .persist_metadata_node(MetadataNodeWrite::new(shard_one.clone(), Vec::new()))
                .unwrap();
            metadata
                .persist_metadata_node(MetadataNodeWrite::new(stale_zero.clone(), Vec::new()))
                .unwrap();

            metadata
                .publish_commit_group(CommitGroupIntent {
                    owner: MappingOwner::BlockDevice(head.device_id),
                    fence: MetadataFence::DeviceGeneration(head.generation),
                    updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                        shard_id: ShardId::from_raw(0),
                        old_root: head.shard_roots[0],
                        new_root: shard_zero.node_id,
                    })],
                })
                .unwrap();

            metadata
                .publish_commit_group(CommitGroupIntent {
                    owner: MappingOwner::BlockDevice(head.device_id),
                    fence: MetadataFence::DeviceGeneration(head.generation),
                    updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                        shard_id: ShardId::from_raw(1),
                        old_root: head.shard_roots[1],
                        new_root: shard_one.node_id,
                    })],
                })
                .unwrap();

            let merged = metadata.get_head(head.device_id).unwrap();
            assert_eq!(merged.shard_roots[0], shard_zero.node_id);
            assert_eq!(merged.shard_roots[1], shard_one.node_id);

            let stale = metadata.publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: stale_zero.node_id,
                })],
            });
            assert!(matches!(stale, Err(StorageError::Conflict { .. })));
            assert_eq!(metadata.get_head(head.device_id).unwrap(), merged);
        }
    }

    #[test]
    fn txn_block_coordinator_preserves_block_write_read_semantics() {
        for mode in [
            MetadataTxnMode::Serial,
            MetadataTxnMode::Sharded { shard_count: 8 },
        ] {
            let cfg = config();
            let store =
                TxnBlockCoordinator::with_storage_nodes(cfg, vec![cfg.storage_node], mode).unwrap();
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
                .write_device_with_integrity(
                    device_id,
                    0,
                    &[9; 4096],
                    WriteDurability::Acknowledged,
                    PayloadIntegrity::Verified,
                )
                .unwrap();
            store
                .write_device_with_integrity(
                    device_id,
                    8 * 4096,
                    &[3; 4096],
                    WriteDurability::Acknowledged,
                    PayloadIntegrity::Verified,
                )
                .unwrap();

            let mut buf = vec![0; 9 * 4096];
            store
                .read_device_with_verification(
                    device_id,
                    ByteRange::new(0, 9 * 4096),
                    &mut buf,
                    ReadVerification::Default,
                )
                .unwrap();
            assert_eq!(&buf[0..4096], &[9; 4096]);
            assert_eq!(&buf[4096..8 * 4096], vec![0; 7 * 4096].as_slice());
            assert_eq!(&buf[8 * 4096..9 * 4096], &[3; 4096]);
        }
    }

    #[test]
    fn txn_metadata_profile_is_empty_when_disabled_and_records_publish_when_enabled() {
        let metadata =
            TxnBlockMetadataPlane::new(config(), MetadataTxnMode::Sharded { shard_count: 8 })
                .unwrap();
        let head = metadata.create_device(device_request()).unwrap();
        let node = metadata_leaf(11_001, 0, 8);
        metadata
            .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
            .unwrap();
        assert!(metadata.drain_profiles(100).unwrap().is_empty());

        metadata.enable_profiling(16).unwrap();
        metadata
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
        let profiles = metadata.drain_profiles(100).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(
            profiles[0].phase,
            MetadataTxnProfilePhase::PublishBlockCommit
        );
        assert_eq!(profiles[0].conflict_count, 0);
        assert!(profiles[0].read_key_count >= 1);
        assert!(profiles[0].write_key_count >= 1);
    }

    #[test]
    fn txn_block_write_profile_is_empty_when_disabled_and_records_successful_writes() {
        let cfg = config();
        let store = TxnBlockCoordinator::with_storage_nodes(
            cfg,
            vec![cfg.storage_node, StorageNodeId::from_raw(88)],
            MetadataTxnMode::Sharded { shard_count: 8 },
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
            .write_device_with_integrity(
                device_id,
                0,
                &[9; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();
        assert!(store.drain_block_write_profiles(100).unwrap().is_empty());

        store.enable_block_write_profiling(16).unwrap();
        store
            .write_device_with_integrity(
                device_id,
                4096,
                &[7; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();
        let profiles = store.drain_block_write_profiles(100).unwrap();
        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.touched_shard_count, 1);
        assert_eq!(profile.segment_count, 1);
        assert_eq!(profile.storage_node_count, 2);
        assert!(profile.total_nanos > 0);
        assert!(profile.segment_write_nanos > 0);
        assert!(profile.storage_node_ids_nanos > 0);
        assert!(profile.placement_select_nanos > 0);
        assert!(profile.segment_id_alloc_nanos > 0);
        assert!(profile.grant_issue_nanos > 0);
        assert!(profile.storage_node_transport_dispatch_nanos > 0);
        assert!(profile.grant_verify_nanos > 0);
        assert!(profile.catalog_duplicate_probe_nanos > 0);
        assert!(profile.catalog_reserve_nanos > 0);
        assert!(profile.catalog_begin_nanos > 0);
        assert!(profile.segment_store_write_nanos > 0);
        assert!(profile.checksum_integrity_nanos > 0);
        assert!(profile.segment_store_insert_nanos > 0);
        assert!(profile.segment_sync_nanos > 0);
        assert!(profile.receipt_create_nanos > 0);
        assert!(profile.receipt_verify_nanos > 0);
        assert!(profile.catalog_commit_nanos > 0);
        assert!(profile.tree_path_copy_nanos > 0);
        assert!(profile.metadata_publish_call_nanos > 0);
        assert!(profile.mark_referenced_nanos > 0);
        assert!(profile.mark_reference_evidence_nanos > 0);
        assert!(profile.mark_reference_transport_dispatch_nanos > 0);
        assert!(profile.mark_reference_verify_nanos > 0);
        assert!(profile.mark_reference_catalog_nanos > 0);
    }

    #[test]
    fn txn_block_write_profile_reports_multi_shard_write_shape() {
        let cfg = config();
        let store = TxnBlockCoordinator::with_storage_nodes(
            cfg,
            vec![cfg.storage_node],
            MetadataTxnMode::Serial,
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
        store.enable_block_write_profiling(16).unwrap();
        store
            .write_device_with_integrity(
                device_id,
                7 * 4096,
                &[5; 8192],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Unchecked,
            )
            .unwrap();
        let profiles = store.drain_block_write_profiles(100).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].touched_shard_count, 2);
        assert_eq!(profiles[0].segment_count, 2);
        assert!(profiles[0].payload_copy_nanos > 0);
    }

    #[test]
    fn txn_metadata_multi_shard_failure_exposes_no_partial_update() {
        let metadata =
            TxnBlockMetadataPlane::new(config(), MetadataTxnMode::Sharded { shard_count: 8 })
                .unwrap();
        let head = metadata.create_device(device_request()).unwrap();
        let shard_zero = metadata_leaf(12_001, 0, 8);
        let stale_zero = metadata_leaf(12_002, 0, 8);
        let shard_one = metadata_leaf(12_003, 8, 8);
        for node in [&shard_zero, &stale_zero, &shard_one] {
            metadata
                .persist_metadata_node(MetadataNodeWrite::new(node.clone(), Vec::new()))
                .unwrap();
        }

        metadata
            .publish_commit_group(CommitGroupIntent {
                owner: MappingOwner::BlockDevice(head.device_id),
                fence: MetadataFence::DeviceGeneration(head.generation),
                updates: vec![RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: shard_zero.node_id,
                })],
            })
            .unwrap();

        let failed = metadata.publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(head.device_id),
            fence: MetadataFence::DeviceGeneration(head.generation),
            updates: vec![
                RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(0),
                    old_root: head.shard_roots[0],
                    new_root: stale_zero.node_id,
                }),
                RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id: ShardId::from_raw(1),
                    old_root: head.shard_roots[1],
                    new_root: shard_one.node_id,
                }),
            ],
        });
        assert!(matches!(failed, Err(StorageError::Conflict { .. })));
        let after = metadata.get_head(head.device_id).unwrap();
        assert_eq!(after.shard_roots[0], shard_zero.node_id);
        assert_eq!(after.shard_roots[1], head.shard_roots[1]);
    }

    #[test]
    fn txn_block_fork_restore_delete_and_gc_roots_preserve_block_contents() {
        let cfg = config();
        let store = TxnBlockCoordinator::with_storage_nodes(
            cfg,
            vec![cfg.storage_node],
            MetadataTxnMode::Sharded { shard_count: 8 },
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
            .write_device_with_integrity(
                device_id,
                0,
                &[1; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();
        let checkpoint = store.checkpoint(device_id).unwrap();
        store
            .write_device_with_integrity(
                device_id,
                0,
                &[2; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();

        let fork = store
            .fork_device(
                device_id,
                ForkRequest {
                    target: None,
                    name: None,
                },
            )
            .unwrap();
        store
            .write_device_with_integrity(
                device_id,
                0,
                &[3; 4096],
                WriteDurability::Acknowledged,
                PayloadIntegrity::Verified,
            )
            .unwrap();
        let restored = store
            .restore_device(device_id, RestorePoint::Checkpoint(checkpoint))
            .unwrap();

        let mut source = vec![0; 4096];
        let mut forked = vec![0; 4096];
        let mut restored_bytes = vec![0; 4096];
        store
            .read_device_with_verification(
                device_id,
                ByteRange::new(0, 4096),
                &mut source,
                ReadVerification::Default,
            )
            .unwrap();
        store
            .read_device_with_verification(
                fork,
                ByteRange::new(0, 4096),
                &mut forked,
                ReadVerification::Default,
            )
            .unwrap();
        store
            .read_device_with_verification(
                restored,
                ByteRange::new(0, 4096),
                &mut restored_bytes,
                ReadVerification::Default,
            )
            .unwrap();
        assert_eq!(source, vec![3; 4096]);
        assert_eq!(forked, vec![2; 4096]);
        assert_eq!(restored_bytes, vec![1; 4096]);

        let delete = store.delete_device(device_id).unwrap();
        assert_eq!(delete.device_id, device_id);
        assert!(
            store
                .read_device_with_verification(
                    device_id,
                    ByteRange::new(0, 4096),
                    &mut source,
                    ReadVerification::Default,
                )
                .is_err()
        );
        let roots = store
            .roots_for_gc(RetentionPolicy::retain_deleted_devices())
            .unwrap();
        assert!(!roots.is_empty());
    }
}
