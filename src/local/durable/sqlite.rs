#[derive(Debug, Clone)]
pub(super) struct DurableSqliteStore {
    paths: DurableStorePaths,
    conn: Arc<Mutex<Connection>>,
    node_catalogs: Arc<NodeCatalogs>,
    policy: DurableDataLogPolicy,
    data_log_allocation_lock: Arc<Mutex<()>>,
    #[cfg(test)]
    persist_delay: Arc<Mutex<Option<Duration>>>,
    #[cfg(test)]
    fail_next_persist: Arc<AtomicBool>,
    #[cfg(test)]
    fail_next_prestage: Arc<AtomicBool>,
}

/// Process-local timing for one physical durable persist.
///
/// Profiles are opt-in diagnostics. They are not durable state and are not part
/// of the public block/native provider contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurablePersistProfile {
    pub sequence: u64,
    pub total_nanos: u64,
    pub lock_wait_nanos: u64,
    pub block_delta_prestage_wait_nanos: u64,
    pub block_delta_selected_count: u64,
    pub block_delta_selected_bytes: u64,
    pub sqlite_lock_wait_nanos: u64,
    pub local_snapshot_nanos: u64,
    pub metadata_publish_lock_wait_nanos: u64,
    pub commit_sequence_alloc_nanos: u64,
    pub data_log_append_sync_nanos: u64,
    pub data_log_encode_nanos: u64,
    pub data_log_write_nanos: u64,
    pub data_log_file_sync_nanos: u64,
    pub data_log_file_sync_sum_nanos: u64,
    pub data_log_file_sync_max_nanos: u64,
    pub data_log_dir_sync_nanos: u64,
    pub data_log_files_synced: u64,
    pub data_log_sync_bytes: u64,
    pub data_log_records_written: u64,
    pub data_log_write_bytes: u64,
    pub data_log_prestaged_segment_count: u64,
    pub data_log_prestaged_segment_bytes: u64,
    pub data_log_sync_only_bytes: u64,
    pub data_log_flush_write_bytes: u64,
    pub data_log_sync_storage_node_count: u64,
    pub node_catalog_publish_nanos: u64,
    pub node_catalog_manifest_lock_wait_nanos: u64,
    pub node_catalog_manifest_row_sync_nanos: u64,
    pub node_catalog_manifest_commit_nanos: u64,
    pub node_catalog_segment_lock_wait_nanos: u64,
    pub node_catalog_segment_row_sync_nanos: u64,
    pub node_catalog_segment_commit_nanos: u64,
    pub node_catalog_manifest_rows: u64,
    pub node_catalog_sealed_rows: u64,
    pub node_catalog_placement_rows: u64,
    pub node_catalog_segment_rows: u64,
    pub root_sqlite_row_sync_nanos: u64,
    pub root_sqlite_commit_nanos: u64,
    pub new_segment_count: u64,
    pub new_segment_bytes: u64,
    pub touched_node_count: u64,
    pub logical_conflict_count: u64,
    pub touched_shard_head_rows: u64,
    pub touched_manifest_rows: u64,
    pub commit_rows_written: u64,
    pub durable_commit_high_water: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct NodeCatalogPublishProfile {
    manifest_lock_wait_nanos: u64,
    manifest_row_sync_nanos: u64,
    manifest_commit_nanos: u64,
    segment_lock_wait_nanos: u64,
    segment_row_sync_nanos: u64,
    segment_commit_nanos: u64,
    manifest_rows: u64,
    sealed_rows: u64,
    placement_rows: u64,
    segment_rows: u64,
    manifest_touched_nodes: u64,
    segment_touched_nodes: u64,
}

impl NodeCatalogPublishProfile {
    fn merge(&mut self, other: Self) {
        self.manifest_lock_wait_nanos = self
            .manifest_lock_wait_nanos
            .saturating_add(other.manifest_lock_wait_nanos);
        self.manifest_row_sync_nanos = self
            .manifest_row_sync_nanos
            .saturating_add(other.manifest_row_sync_nanos);
        self.manifest_commit_nanos = self
            .manifest_commit_nanos
            .saturating_add(other.manifest_commit_nanos);
        self.segment_lock_wait_nanos = self
            .segment_lock_wait_nanos
            .saturating_add(other.segment_lock_wait_nanos);
        self.segment_row_sync_nanos = self
            .segment_row_sync_nanos
            .saturating_add(other.segment_row_sync_nanos);
        self.segment_commit_nanos = self
            .segment_commit_nanos
            .saturating_add(other.segment_commit_nanos);
        self.manifest_rows = self.manifest_rows.saturating_add(other.manifest_rows);
        self.sealed_rows = self.sealed_rows.saturating_add(other.sealed_rows);
        self.placement_rows = self.placement_rows.saturating_add(other.placement_rows);
        self.segment_rows = self.segment_rows.saturating_add(other.segment_rows);
        self.manifest_touched_nodes = self
            .manifest_touched_nodes
            .saturating_add(other.manifest_touched_nodes);
        self.segment_touched_nodes = self
            .segment_touched_nodes
            .saturating_add(other.segment_touched_nodes);
    }

    fn touched_node_count(self) -> u64 {
        self.manifest_touched_nodes.max(self.segment_touched_nodes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct MetadataPublishProfile {
    lock_wait_nanos: u64,
    commit_sequence_alloc_nanos: u64,
    logical_conflict_count: u64,
    touched_shard_head_rows: u64,
    touched_manifest_rows: u64,
    commit_rows_written: u64,
}

#[derive(Debug)]
pub(super) struct MetadataPublishProfiler {
    capacity: usize,
    profiles: VecDeque<MetadataPublishProfile>,
}

impl MetadataPublishProfiler {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "metadata publish profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            profiles: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, profile: MetadataPublishProfile) {
        if self.profiles.len() == self.capacity {
            self.profiles.pop_front();
        }
        self.profiles.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<MetadataPublishProfile> {
        let count = max.min(self.profiles.len());
        self.profiles.drain(..count).collect()
    }
}

pub(super) fn summarize_metadata_publish_profiles(
    profiles: impl IntoIterator<Item = MetadataPublishProfile>,
) -> MetadataPublishProfile {
    let mut out = MetadataPublishProfile::default();
    for profile in profiles {
        out.lock_wait_nanos = out.lock_wait_nanos.saturating_add(profile.lock_wait_nanos);
        out.commit_sequence_alloc_nanos = out
            .commit_sequence_alloc_nanos
            .saturating_add(profile.commit_sequence_alloc_nanos);
        out.logical_conflict_count = out
            .logical_conflict_count
            .saturating_add(profile.logical_conflict_count);
        out.touched_shard_head_rows = out
            .touched_shard_head_rows
            .saturating_add(profile.touched_shard_head_rows);
        out.touched_manifest_rows = out
            .touched_manifest_rows
            .saturating_add(profile.touched_manifest_rows);
        out.commit_rows_written = out
            .commit_rows_written
            .saturating_add(profile.commit_rows_written);
    }
    out
}

#[derive(Debug)]
pub(super) struct PersistProfiler {
    capacity: usize,
    next_sequence: u64,
    profiles: VecDeque<DurablePersistProfile>,
}

impl PersistProfiler {
    fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::invalid_argument(
                "persist profile capacity must be greater than zero",
            ));
        }
        Ok(Self {
            capacity,
            next_sequence: 1,
            profiles: VecDeque::with_capacity(capacity.min(1024)),
        })
    }

    fn record(&mut self, mut profile: DurablePersistProfile) {
        profile.sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.profiles.len() == self.capacity {
            self.profiles.pop_front();
        }
        self.profiles.push_back(profile);
    }

    fn drain(&mut self, max: usize) -> Vec<DurablePersistProfile> {
        let count = max.min(self.profiles.len());
        self.profiles.drain(..count).collect()
    }
}

#[derive(Debug, Clone)]
pub(super) struct DurablePersistOutcome {
    kept_segments: BTreeSet<SegmentId>,
    profile: DurablePersistProfile,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DataLogAppendProfile {
    encode_nanos: u64,
    write_nanos: u64,
    file_sync_nanos: u64,
    file_sync_sum_nanos: u64,
    file_sync_max_nanos: u64,
    dir_sync_nanos: u64,
    files_synced: u64,
    sync_bytes: u64,
    records_written: u64,
    write_bytes: u64,
}

impl DataLogAppendProfile {
    fn merge(&mut self, other: Self) {
        self.encode_nanos = self.encode_nanos.saturating_add(other.encode_nanos);
        self.write_nanos = self.write_nanos.saturating_add(other.write_nanos);
        self.file_sync_nanos = self.file_sync_nanos.saturating_add(other.file_sync_nanos);
        self.file_sync_sum_nanos = self
            .file_sync_sum_nanos
            .saturating_add(other.file_sync_sum_nanos);
        self.file_sync_max_nanos = self.file_sync_max_nanos.max(other.file_sync_max_nanos);
        self.dir_sync_nanos = self.dir_sync_nanos.saturating_add(other.dir_sync_nanos);
        self.files_synced = self.files_synced.saturating_add(other.files_synced);
        self.sync_bytes = self.sync_bytes.saturating_add(other.sync_bytes);
        self.records_written = self.records_written.saturating_add(other.records_written);
        self.write_bytes = self.write_bytes.saturating_add(other.write_bytes);
    }
}

#[derive(Debug)]
pub(super) struct DataLogFileToSync {
    file: File,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DataLogFileSyncProfile {
    files_synced: u64,
    sync_bytes: u64,
    sync_sum_nanos: u64,
    sync_max_nanos: u64,
}

impl DataLogFileSyncProfile {
    fn record_file(&mut self, bytes: u64, nanos: u64) {
        self.files_synced = self.files_synced.saturating_add(1);
        self.sync_bytes = self.sync_bytes.saturating_add(bytes);
        self.sync_sum_nanos = self.sync_sum_nanos.saturating_add(nanos);
        self.sync_max_nanos = self.sync_max_nanos.max(nanos);
    }
}

fn data_log_file_to_sync(file: File, bytes: u64) -> DataLogFileToSync {
    DataLogFileToSync { file, bytes }
}

fn data_log_file_to_sync_with_metadata(file: File) -> Result<DataLogFileToSync> {
    let bytes = file.metadata().map_err(fs_error)?.len();
    Ok(DataLogFileToSync { file, bytes })
}

#[derive(Debug)]
pub(super) struct PersistCoordinator {
    inner: Mutex<PersistCoordinatorState>,
    cvar: Condvar,
}

#[derive(Debug)]
pub(super) struct PersistCoordinatorState {
    in_flight: bool,
    generation: u64,
    durable_through: CommitSeq,
    requested_through: CommitSeq,
    last_error: Option<(u64, StorageError)>,
}

impl PersistCoordinator {
    fn new(durable_through: CommitSeq) -> Self {
        Self {
            inner: Mutex::new(PersistCoordinatorState {
                in_flight: false,
                generation: 0,
                durable_through,
                requested_through: durable_through,
                last_error: None,
            }),
            cvar: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct BlockDeltaPrestageTracker {
    inner: Mutex<BlockDeltaPrestageState>,
    cvar: Condvar,
}

#[derive(Debug, Default)]
pub(super) struct BlockDeltaPrestageState {
    in_flight: BTreeSet<CommitSeq>,
    failed: BTreeSet<CommitSeq>,
}

impl BlockDeltaPrestageTracker {
    fn new() -> Self {
        Self {
            inner: Mutex::new(BlockDeltaPrestageState::default()),
            cvar: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct StreamFlushCoordinator {
    inner: Mutex<StreamFlushCoordinatorState>,
    cvar: Condvar,
}

#[derive(Debug)]
pub(super) struct StreamFlushCoordinatorState {
    in_flight: bool,
    generation: u64,
    requests: BTreeMap<AppendStreamId, StreamFlushRequest>,
    last_error: Option<(u64, StorageError)>,
}

#[derive(Debug, Clone)]
pub(super) struct StreamFlushRequest {
    stream: AppendStream,
    durable_through: u64,
    waiters: usize,
}

impl StreamFlushCoordinatorState {
    fn add_request(&mut self, stream: &AppendStream, durable_through: u64) {
        self.requests
            .entry(stream.stream_id)
            .and_modify(|request| {
                request.durable_through = request.durable_through.max(durable_through);
                request.waiters = request.waiters.saturating_add(1);
            })
            .or_insert_with(|| StreamFlushRequest {
                stream: stream.clone(),
                durable_through,
                waiters: 1,
            });
    }

    fn release_request(&mut self, stream_id: AppendStreamId) {
        if let Some(request) = self.requests.get_mut(&stream_id)
            && request.waiters > 1
        {
            request.waiters -= 1;
            return;
        }
        self.requests.remove(&stream_id);
    }

    fn snapshot_requests(&self) -> Vec<(AppendStream, u64)> {
        self.requests
            .values()
            .map(|request| (request.stream.clone(), request.durable_through))
            .collect()
    }
}

impl StreamFlushCoordinator {
    fn new() -> Self {
        Self {
            inner: Mutex::new(StreamFlushCoordinatorState {
                in_flight: false,
                generation: 0,
                requests: BTreeMap::new(),
                last_error: None,
            }),
            cvar: Condvar::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SegmentPlacementRow {
    segment_id: SegmentId,
    storage_node: StorageNodeId,
    data_log_id: u64,
    record_offset: u64,
    record_bytes: u64,
    payload_offset: u64,
    payload_bytes: u64,
    integrity: SegmentPayloadIntegrity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DataLogRow {
    storage_node: StorageNodeId,
    log_id: u64,
    total_bytes: u64,
    live_bytes: u64,
    dead_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DataLogSyncMode {
    Sync,
    NoSync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DurableExportCursor {
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

#[derive(Debug, Clone)]
pub(super) struct NativeMetadataDelta {
    cursor: DurableExportCursor,
    keyspace_heads: BTreeMap<KeyspaceId, KeyspaceHead>,
    keyspace_roots: BTreeMap<KeyspaceRootId, KeyspaceRoot>,
    keyspace_catalog_shards: BTreeMap<KeyspaceCatalogShardId, KeyspaceCatalogShard>,
    file_writer_epochs: Vec<((KeyspaceId, FileId), WriterEpoch)>,
    append_streams: Vec<AppendStreamState>,
    metadata_nodes: BTreeMap<MetadataNodeId, MetadataNode>,
    referenced_segment_ids: BTreeSet<SegmentId>,
    commit_groups: BTreeMap<CommitGroupId, CommitGroup>,
    keyspace_commits: Vec<KeyspaceCommit>,
    file_commits: Vec<FileCommit>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DurableDeviceManifest {
    device_id: DeviceId,
    shard_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DurableDeviceShardHead {
    device_id: DeviceId,
    shard_id: ShardId,
    root: MetadataNodeId,
    generation: DeviceGeneration,
    latest_commit: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DurableKeyspaceManifest {
    keyspace_id: KeyspaceId,
    shard_count: u64,
    file_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DurableKeyspaceShardHead {
    keyspace_id: KeyspaceId,
    shard_index: u32,
    root: KeyspaceCatalogShardId,
    generation: KeyspaceGeneration,
    latest_commit: CommitSeq,
}

impl DurableDeviceManifest {
    fn from_head(head: &DeviceHead) -> Result<Self> {
        Ok(Self {
            device_id: head.device_id,
            shard_count: u64::try_from(head.shard_roots.len())
                .map_err(|_| StorageError::invalid_argument("device shard count overflows u64"))?,
        })
    }
}

impl DurableDeviceShardHead {
    fn from_head(
        head: &DeviceHead,
        shard_index: usize,
        root: MetadataNodeId,
        latest_commit: CommitSeq,
    ) -> Result<Self> {
        Ok(Self {
            device_id: head.device_id,
            shard_id: ShardId::from_raw(
                u32::try_from(shard_index).map_err(|_| {
                    StorageError::invalid_argument("device shard index overflows u32")
                })?,
            ),
            root,
            generation: DeviceGeneration::from_raw(latest_commit.raw()),
            latest_commit,
        })
    }
}

impl DurableKeyspaceManifest {
    fn from_head(head: &KeyspaceHead) -> Result<Self> {
        Ok(Self {
            keyspace_id: head.keyspace_id,
            shard_count: u64::try_from(head.shard_roots.len()).map_err(|_| {
                StorageError::invalid_argument("keyspace shard count overflows u64")
            })?,
            file_count: u64::try_from(head.file_count)
                .map_err(|_| StorageError::invalid_argument("keyspace file count overflows u64"))?,
        })
    }
}

impl DurableKeyspaceShardHead {
    fn from_head(
        head: &KeyspaceHead,
        shard_index: usize,
        root: KeyspaceCatalogShardId,
        latest_commit: CommitSeq,
    ) -> Result<Self> {
        Ok(Self {
            keyspace_id: head.keyspace_id,
            shard_index: u32::try_from(shard_index).map_err(|_| {
                StorageError::invalid_argument("keyspace shard index overflows u32")
            })?,
            root,
            generation: KeyspaceGeneration::from_raw(latest_commit.raw()),
            latest_commit,
        })
    }
}

impl DurableExportCursor {
    fn from_state(image: &DurableStoreState) -> Self {
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

pub(super) const DATA_LOG_MAGIC: &[u8; 8] = b"TCOWDAT!";
pub(super) const DATA_LOG_VERSION: u16 = 3;
pub(super) const DATA_LOG_HEADER_LEN: usize = 8 + 2 + 1 + 16 + 8 + 1 + 8;
pub(super) const DATA_LOG_CHECKSUM_OFFSET: usize = 8 + 2 + 1 + 16 + 8 + 1;
pub(super) const DATA_LOG_KIND_SEGMENT: u8 = 1;
pub(super) const DATA_LOG_KIND_APPEND_RUN: u8 = 2;
pub(super) const MAX_DATA_LOG_SYNC_GROUP_BYTES: u64 = 32 * 1024 * 1024;
pub(super) const MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES: u64 = 32 * 1024 * 1024;
pub(super) const MAX_STREAM_FLUSH_GROUPS_PER_RUN: usize = 64;
pub(super) const GENERIC_DATA_LOG_STATE_ACTIVE: &str = "active";
pub(super) const STREAM_DATA_LOG_STATE_ACTIVE: &str = "stream-active";

pub(super) fn stream_data_log_state(stream_id: AppendStreamId) -> String {
    format!("{STREAM_DATA_LOG_STATE_ACTIVE}:{}", stream_id.raw())
}

pub(super) fn is_stream_data_log_state(state: &str) -> bool {
    state == STREAM_DATA_LOG_STATE_ACTIVE
        || state
            .strip_prefix(STREAM_DATA_LOG_STATE_ACTIVE)
            .is_some_and(|suffix| suffix.starts_with(':'))
}

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
        reject_legacy_device_head_tables_if_present(&conn)?;
        reject_legacy_keyspace_head_tables_if_present(&conn)?;
        let node_catalogs = NodeCatalogs::open(&paths, configured_storage_nodes)?;
        if !metadata_existed {
            sync_parent_dir(&paths.metadata)?;
        }
        Ok(Self {
            paths,
            conn: Arc::new(Mutex::new(conn)),
            node_catalogs: Arc::new(node_catalogs),
            policy,
            data_log_allocation_lock: Arc::new(Mutex::new(())),
            #[cfg(test)]
            persist_delay: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            fail_next_persist: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            fail_next_prestage: Arc::new(AtomicBool::new(false)),
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
            CREATE TABLE IF NOT EXISTS append_stream_runtime (
              id INTEGER PRIMARY KEY CHECK (id = 1),
              next_incarnation INTEGER NOT NULL CHECK (next_incarnation > 0)
            );
            CREATE TABLE IF NOT EXISTS device_specs (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS device_manifests (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deleted_device_manifests (
              device_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS device_shard_heads (
              row_key TEXT PRIMARY KEY,
              device_id TEXT NOT NULL,
              shard_id INTEGER NOT NULL CHECK (shard_id >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_device_shard_heads_device
              ON device_shard_heads(device_id, shard_id);
            CREATE TABLE IF NOT EXISTS deleted_device_shard_heads (
              row_key TEXT PRIMARY KEY,
              device_id TEXT NOT NULL,
              shard_id INTEGER NOT NULL CHECK (shard_id >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_deleted_device_shard_heads_device
              ON deleted_device_shard_heads(device_id, shard_id);
            CREATE TABLE IF NOT EXISTS keyspace_manifests (
              keyspace_id TEXT PRIMARY KEY,
              payload BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS keyspace_shard_heads (
              row_key TEXT PRIMARY KEY,
              keyspace_id TEXT NOT NULL,
              shard_index INTEGER NOT NULL CHECK (shard_index >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_keyspace_shard_heads_keyspace
              ON keyspace_shard_heads(keyspace_id, shard_index);
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
            CREATE TABLE IF NOT EXISTS append_streams (
              stream_id TEXT PRIMARY KEY,
              keyspace_id TEXT NOT NULL,
              file_id TEXT NOT NULL,
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_append_streams_file
              ON append_streams(keyspace_id, file_id);
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
            CREATE TABLE IF NOT EXISTS block_delta_commits (
              row_key TEXT PRIMARY KEY,
              device_id TEXT NOT NULL,
              commit_seq INTEGER NOT NULL CHECK (commit_seq >= 0),
              payload BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_block_delta_commits_order
              ON block_delta_commits(commit_seq, row_key);
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
            let image = DurableStoreState {
                config: expected_config,
                metadata: MetadataInner::new(),
                storage_nodes,
                next_write_intent,
                next_extent_id: 1,
            };
            validate_row_native_state(&image)?;
            self.persist_catalog_reference_repairs(&image.storage_nodes, &repairs)?;
            return Ok(Some(LocalCoordinator::from_durable_state(image)?));
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

        let image = DurableStoreState {
            config: runtime_config,
            metadata,
            storage_nodes,
            next_write_intent,
            next_extent_id: cursor.next_extent_id,
        };
        validate_row_native_state(&image)?;
        self.persist_catalog_reference_repairs(&image.storage_nodes, &repairs)?;
        let local = LocalCoordinator::from_durable_state(image)?;
        let deltas = load_block_delta_commits_since(&conn, cursor.next_commit_seq)?;
        let delta_segment_ids: BTreeSet<_> = deltas
            .iter()
            .flat_map(|delta| delta.segment_ids())
            .collect();
        local.replay_block_delta_commits(&deltas)?;
        if !delta_segment_ids.is_empty() {
            let (image, _, _) = local.state_for_durable_persist(&BTreeSet::new())?;
            let mut repairs = BTreeMap::<StorageNodeId, BTreeSet<SegmentId>>::new();
            for segment_id in delta_segment_ids {
                let storage_node = durable_state_storage_node_for_catalog_segment(
                    &image,
                    segment_id,
                )
                .ok_or_else(|| {
                    StorageError::corrupt("block delta segment missing catalog after replay")
                })?;
                repairs.entry(storage_node).or_default().insert(segment_id);
            }
            self.persist_catalog_reference_repairs(&image.storage_nodes, &repairs)?;
        }
        let _ = local.drain_events(usize::MAX)?;
        Ok(Some(local))
    }

    fn export_cursor(&self) -> Result<Option<DurableExportCursor>> {
        let conn = lock(&self.conn)?;
        load_export_cursor(&conn)
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
                        bytes: Arc::from(bytes),
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
        image: &DurableStoreState,
        previous_segments: &BTreeSet<SegmentId>,
        current_segments: &BTreeSet<SegmentId>,
        new_segments: Vec<DurableSegmentPayload>,
        mut pending_append: PendingDataLogAppend,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<DurablePersistOutcome> {
        let total_started = Instant::now();
        #[cfg(test)]
        {
            let delay = *lock(&self.persist_delay)?;
            if let Some(delay) = delay {
                thread::sleep(delay);
            }
            if self.fail_next_persist.swap(false, Ordering::SeqCst) {
                return Err(StorageError::unavailable(
                    "injected durable persist failure",
                ));
            }
        }
        let new_segment_count = usize_to_u64(new_segments.len());
        let new_segment_bytes = new_segments
            .iter()
            .map(|segment| usize_to_u64(segment.bytes.len()))
            .fold(0_u64, u64::saturating_add);

        pending_append.retain_current_placements(current_segments);
        let started = Instant::now();
        let (new_append, mut data_log_profile) =
            self.append_segments_bounded(new_segments, &pending_append)?;
        data_log_profile.merge(sync_pending_data_logs(
            &self.paths.data_dir,
            &pending_append,
        )?);
        pending_append.merge(new_append);
        let data_log_append_sync_nanos = duration_nanos_u64(started.elapsed());

        let started = Instant::now();
        let catalog_profile = self.persist_node_catalog_publish(
            image,
            previous_segments,
            current_segments,
            pending_append,
            changed_catalog_segments,
        )?;
        let node_catalog_publish_nanos = duration_nanos_u64(started.elapsed());

        let sqlite_lock_started = Instant::now();
        let mut conn = lock(&self.conn)?;
        let sqlite_lock_wait_nanos = duration_nanos_u64(sqlite_lock_started.elapsed());
        let previous_cursor = load_export_cursor(&conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let started = Instant::now();
        persist_row_native_state(&tx, previous_cursor.as_ref(), image)?;
        prune_block_delta_commits_through(
            &tx,
            CommitSeq::from_raw(image.metadata.next_commit_seq.saturating_sub(1)),
        )?;
        let root_sqlite_row_sync_nanos = duration_nanos_u64(started.elapsed());
        let started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        let root_sqlite_commit_nanos = duration_nanos_u64(started.elapsed());
        Ok(DurablePersistOutcome {
            kept_segments: current_segments.clone(),
            profile: DurablePersistProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                data_log_append_sync_nanos,
                sqlite_lock_wait_nanos,
                data_log_encode_nanos: data_log_profile.encode_nanos,
                data_log_write_nanos: data_log_profile.write_nanos,
                data_log_file_sync_nanos: data_log_profile.file_sync_nanos,
                data_log_file_sync_sum_nanos: data_log_profile.file_sync_sum_nanos,
                data_log_file_sync_max_nanos: data_log_profile.file_sync_max_nanos,
                data_log_dir_sync_nanos: data_log_profile.dir_sync_nanos,
                data_log_files_synced: data_log_profile.files_synced,
                data_log_sync_bytes: data_log_profile.sync_bytes,
                data_log_records_written: data_log_profile.records_written,
                data_log_write_bytes: data_log_profile.write_bytes,
                node_catalog_publish_nanos,
                node_catalog_manifest_lock_wait_nanos: catalog_profile
                    .manifest_lock_wait_nanos,
                node_catalog_manifest_row_sync_nanos: catalog_profile.manifest_row_sync_nanos,
                node_catalog_manifest_commit_nanos: catalog_profile.manifest_commit_nanos,
                node_catalog_segment_lock_wait_nanos: catalog_profile.segment_lock_wait_nanos,
                node_catalog_segment_row_sync_nanos: catalog_profile.segment_row_sync_nanos,
                node_catalog_segment_commit_nanos: catalog_profile.segment_commit_nanos,
                node_catalog_manifest_rows: catalog_profile.manifest_rows,
                node_catalog_sealed_rows: catalog_profile.sealed_rows,
                node_catalog_placement_rows: catalog_profile.placement_rows,
                node_catalog_segment_rows: catalog_profile.segment_rows,
                root_sqlite_row_sync_nanos,
                root_sqlite_commit_nanos,
                new_segment_count,
                new_segment_bytes,
                touched_node_count: catalog_profile.touched_node_count(),
                durable_commit_high_water: image.metadata.next_commit_seq.saturating_sub(1),
                ..DurablePersistProfile::default()
            },
        })
    }

    fn persist_node_catalog_publish(
        &self,
        image: &DurableStoreState,
        previous_segments: &BTreeSet<SegmentId>,
        current_segments: &BTreeSet<SegmentId>,
        appended: PendingDataLogAppend,
        changed_catalog_segments: Option<&BTreeSet<SegmentId>>,
    ) -> Result<NodeCatalogPublishProfile> {
        let removed_segment_ids: Vec<_> = previous_segments
            .difference(current_segments)
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
                let storage_node =
                    durable_state_storage_node_for_catalog_segment(image, *segment_id)
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

        let mut profile = NodeCatalogPublishProfile::default();
        for (ordinal, node_id) in image.storage_nodes.node_order.iter().enumerate() {
            let node = image.storage_nodes.nodes.get(node_id).ok_or_else(|| {
                StorageError::corrupt("storage node order references missing node")
            })?;
            let lock_started = Instant::now();
            let mut conn = self.node_catalogs.lock(*node_id)?;
            profile.segment_lock_wait_nanos = profile
                .segment_lock_wait_nanos
                .saturating_add(duration_nanos_u64(lock_started.elapsed()));
            let tx = conn.transaction().map_err(sqlite_error)?;
            let row_started = Instant::now();
            for log in appended
                .logs
                .values()
                .filter(|log| log.storage_node == *node_id)
            {
                persist_data_log_manifest(&tx, log)?;
                profile.manifest_rows = profile.manifest_rows.saturating_add(1);
            }
            for log_ref in appended
                .sealed_logs
                .iter()
                .filter(|log_ref| log_ref.storage_node == *node_id)
            {
                seal_data_log_manifest(&tx, *log_ref)?;
                profile.sealed_rows = profile.sealed_rows.saturating_add(1);
            }
            if let Some(placements) = dead_placements.get(node_id) {
                for placement in placements {
                    mark_placement_dead(&tx, placement)?;
                    profile.placement_rows = profile.placement_rows.saturating_add(1);
                }
            }
            for placement in appended
                .placements
                .iter()
                .filter(|placement| placement.storage_node == *node_id)
            {
                persist_segment_placement(&tx, placement)?;
                profile.placement_rows = profile.placement_rows.saturating_add(1);
            }
            let catalog_sync = if incremental_catalog_sync {
                changed_segments_by_node
                    .get(node_id)
                    .map(SegmentCatalogSync::Only)
                    .unwrap_or(SegmentCatalogSync::Skip)
            } else {
                SegmentCatalogSync::Full
            };
            profile.segment_rows = profile.segment_rows.saturating_add(match &catalog_sync {
                SegmentCatalogSync::Full => usize_to_u64(node.segment_catalog.entries.len()),
                SegmentCatalogSync::Only(segment_ids) => usize_to_u64(segment_ids.len()),
                SegmentCatalogSync::Skip => 0,
            });
            sync_node_catalog_state_for_node(
                &tx,
                ordinal,
                *node_id,
                node,
                catalog_sync,
                &pre_root_pending_segments,
            )?;
            profile.segment_row_sync_nanos = profile
                .segment_row_sync_nanos
                .saturating_add(duration_nanos_u64(row_started.elapsed()));
            let commit_started = Instant::now();
            tx.commit().map_err(sqlite_error)?;
            profile.segment_commit_nanos = profile
                .segment_commit_nanos
                .saturating_add(duration_nanos_u64(commit_started.elapsed()));
            profile.segment_touched_nodes = profile.segment_touched_nodes.saturating_add(1);
        }
        Ok(profile)
    }

    fn persist_selected_node_catalog_publish(
        &self,
        nodes: &SelectedStorageNodeState,
        segment_ids: &BTreeSet<SegmentId>,
        appended: PendingDataLogAppend,
        pre_root_pending_segments: &BTreeSet<SegmentId>,
    ) -> Result<NodeCatalogPublishProfile> {
        let mut selected = Vec::new();
        for (node_id, (ordinal, node)) in nodes {
            let node_segment_ids: BTreeSet<_> = segment_ids
                .iter()
                .copied()
                .filter(|segment_id| node.segment_catalog.entries.contains_key(segment_id))
                .collect();
            if node_segment_ids.is_empty() {
                continue;
            }
            selected.push((*node_id, *ordinal, node, node_segment_ids));
        }

        if selected.len() <= 1 {
            let mut profile = NodeCatalogPublishProfile::default();
            for (node_id, ordinal, node, node_segment_ids) in selected {
                profile.merge(Self::persist_selected_node_catalog_publish_for_node(
                    self.node_catalogs.as_ref(),
                    node_id,
                    ordinal,
                    node,
                    node_segment_ids,
                    &appended,
                    pre_root_pending_segments,
                )?);
            }
            return Ok(profile);
        }

        thread::scope(|scope| {
            let mut handles = Vec::new();
            let appended_ref = &appended;
            let pre_root_pending_segments_ref = pre_root_pending_segments;
            for (node_id, ordinal, node, node_segment_ids) in selected {
                let node_catalogs = Arc::clone(&self.node_catalogs);
                handles.push(scope.spawn(move || {
                    Self::persist_selected_node_catalog_publish_for_node(
                        node_catalogs.as_ref(),
                        node_id,
                        ordinal,
                        node,
                        node_segment_ids,
                        appended_ref,
                        pre_root_pending_segments_ref,
                    )
                }));
            }

            let mut profile = NodeCatalogPublishProfile::default();
            for handle in handles {
                let node_profile = handle.join().map_err(|_| {
                    StorageError::unavailable("node catalog segment publish worker panicked")
                })??;
                profile.merge(node_profile);
            }
            Ok(profile)
        })
    }

    fn persist_selected_node_catalog_publish_for_node(
        node_catalogs: &NodeCatalogs,
        node_id: StorageNodeId,
        ordinal: usize,
        node: &StorageNodeInner,
        node_segment_ids: BTreeSet<SegmentId>,
        appended: &PendingDataLogAppend,
        pre_root_pending_segments: &BTreeSet<SegmentId>,
    ) -> Result<NodeCatalogPublishProfile> {
        let mut profile = NodeCatalogPublishProfile::default();
        let lock_started = Instant::now();
        let mut conn = node_catalogs.lock(node_id)?;
        profile.segment_lock_wait_nanos = profile
            .segment_lock_wait_nanos
            .saturating_add(duration_nanos_u64(lock_started.elapsed()));
        let tx = conn.transaction().map_err(sqlite_error)?;
        let row_started = Instant::now();
        for log in appended
            .logs
            .values()
            .filter(|log| log.storage_node == node_id)
        {
            persist_data_log_manifest(&tx, log)?;
            profile.manifest_rows = profile.manifest_rows.saturating_add(1);
        }
        for log_ref in appended
            .sealed_logs
            .iter()
            .filter(|log_ref| log_ref.storage_node == node_id)
        {
            seal_data_log_manifest(&tx, *log_ref)?;
            profile.sealed_rows = profile.sealed_rows.saturating_add(1);
        }
        for placement in appended
            .placements
            .iter()
            .filter(|placement| placement.storage_node == node_id)
        {
            persist_segment_placement(&tx, placement)?;
            profile.placement_rows = profile.placement_rows.saturating_add(1);
        }
        profile.segment_rows = profile
            .segment_rows
            .saturating_add(usize_to_u64(node_segment_ids.len()));
        sync_node_catalog_state_for_node(
            &tx,
            ordinal,
            node_id,
            node,
            SegmentCatalogSync::Only(&node_segment_ids),
            pre_root_pending_segments,
        )?;
        profile.segment_row_sync_nanos = profile
            .segment_row_sync_nanos
            .saturating_add(duration_nanos_u64(row_started.elapsed()));
        let commit_started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        profile.segment_commit_nanos = profile
            .segment_commit_nanos
            .saturating_add(duration_nanos_u64(commit_started.elapsed()));
        profile.segment_touched_nodes = profile.segment_touched_nodes.saturating_add(1);
        Ok(profile)
    }

    fn persist_data_log_manifests_for_node(
        node_catalogs: &NodeCatalogs,
        storage_node: StorageNodeId,
        appended: &PendingDataLogAppend,
    ) -> Result<NodeCatalogPublishProfile> {
        let mut profile = NodeCatalogPublishProfile::default();
        let lock_started = Instant::now();
        let mut conn = node_catalogs.lock(storage_node)?;
        profile.manifest_lock_wait_nanos = profile
            .manifest_lock_wait_nanos
            .saturating_add(duration_nanos_u64(lock_started.elapsed()));
        let tx = conn.transaction().map_err(sqlite_error)?;
        let row_started = Instant::now();
        for log in appended
            .logs
            .values()
            .filter(|log| log.storage_node == storage_node)
        {
            persist_data_log_manifest(&tx, log)?;
            profile.manifest_rows = profile.manifest_rows.saturating_add(1);
        }
        for log_ref in appended
            .sealed_logs
            .iter()
            .filter(|log_ref| log_ref.storage_node == storage_node)
        {
            seal_data_log_manifest(&tx, *log_ref)?;
            profile.sealed_rows = profile.sealed_rows.saturating_add(1);
        }
        profile.manifest_row_sync_nanos = profile
            .manifest_row_sync_nanos
            .saturating_add(duration_nanos_u64(row_started.elapsed()));
        let commit_started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        profile.manifest_commit_nanos = profile
            .manifest_commit_nanos
            .saturating_add(duration_nanos_u64(commit_started.elapsed()));
        profile.manifest_touched_nodes = profile.manifest_touched_nodes.saturating_add(1);
        Ok(profile)
    }

    fn persist_data_log_manifests_only(
        &self,
        appended: &PendingDataLogAppend,
    ) -> Result<NodeCatalogPublishProfile> {
        let mut touched = BTreeSet::new();
        for log in appended.logs.values() {
            touched.insert(log.storage_node);
        }
        for log_ref in &appended.sealed_logs {
            touched.insert(log_ref.storage_node);
        }

        if touched.len() <= 1 {
            let mut profile = NodeCatalogPublishProfile::default();
            for storage_node in touched.iter().copied() {
                profile.merge(Self::persist_data_log_manifests_for_node(
                    self.node_catalogs.as_ref(),
                    storage_node,
                    appended,
                )?);
            }
            return Ok(profile);
        }

        thread::scope(|scope| {
            let mut handles = Vec::new();
            for storage_node in touched.iter().copied() {
                let node_catalogs = Arc::clone(&self.node_catalogs);
                handles.push(scope.spawn(move || {
                    Self::persist_data_log_manifests_for_node(
                        node_catalogs.as_ref(),
                        storage_node,
                        appended,
                    )
                }));
            }

            let mut profile = NodeCatalogPublishProfile::default();
            for handle in handles {
                let node_profile = handle.join().map_err(|_| {
                    StorageError::unavailable("node catalog manifest publish worker panicked")
                })??;
                profile.merge(node_profile);
            }
            Ok(profile)
        })
    }

    fn persist_preingested_append_stream_flush(
        &self,
        cursor: &DurableExportCursor,
        streams: &[AppendStreamState],
        nodes: &SelectedStorageNodeState,
        segment_ids: &BTreeSet<SegmentId>,
        appended: PendingDataLogAppend,
    ) -> Result<DurablePersistProfile> {
        let total_started = Instant::now();
        #[cfg(test)]
        {
            let delay = *lock(&self.persist_delay)?;
            if let Some(delay) = delay {
                thread::sleep(delay);
            }
            if self.fail_next_persist.swap(false, Ordering::SeqCst) {
                return Err(StorageError::unavailable(
                    "injected durable persist failure",
                ));
            }
        }

        let new_segment_count = usize_to_u64(appended.placements.len());
        let new_segment_bytes = appended
            .placements
            .iter()
            .map(|placement| placement.payload_bytes)
            .fold(0_u64, u64::saturating_add);

        let started = Instant::now();
        let data_log_profile = self.sync_pending_data_log_append(&appended)?;
        let data_log_append_sync_nanos = duration_nanos_u64(started.elapsed());

        let started = Instant::now();
        let mut catalog_profile = self.persist_data_log_manifests_only(&appended)?;
        catalog_profile.merge(self.persist_selected_node_catalog_publish(
            nodes,
            segment_ids,
            appended.clone(),
            segment_ids,
        )?);
        let node_catalog_publish_nanos = duration_nanos_u64(started.elapsed());

        let sqlite_lock_started = Instant::now();
        let mut conn = lock(&self.conn)?;
        let sqlite_lock_wait_nanos = duration_nanos_u64(sqlite_lock_started.elapsed());
        let previous_cursor = load_export_cursor(&conn)?;
        let stream_cursor = stream_flush_cursor(previous_cursor.as_ref(), cursor);
        let tx = conn.transaction().map_err(sqlite_error)?;
        let started = Instant::now();
        for stream in streams {
            upsert_file_writer_epoch(&tx, stream.keyspace_id, stream.file_id, stream.writer_epoch)?;
            upsert_append_stream(&tx, stream)?;
        }
        persist_export_cursor(&tx, &stream_cursor)?;
        let root_sqlite_row_sync_nanos = duration_nanos_u64(started.elapsed());
        let started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        let root_sqlite_commit_nanos = duration_nanos_u64(started.elapsed());

        Ok(DurablePersistProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            data_log_append_sync_nanos,
            sqlite_lock_wait_nanos,
            data_log_encode_nanos: data_log_profile.encode_nanos,
            data_log_write_nanos: data_log_profile.write_nanos,
            data_log_file_sync_nanos: data_log_profile.file_sync_nanos,
            data_log_file_sync_sum_nanos: data_log_profile.file_sync_sum_nanos,
            data_log_file_sync_max_nanos: data_log_profile.file_sync_max_nanos,
            data_log_dir_sync_nanos: data_log_profile.dir_sync_nanos,
            data_log_files_synced: data_log_profile.files_synced,
            data_log_sync_bytes: data_log_profile.sync_bytes,
            data_log_records_written: data_log_profile.records_written,
            data_log_write_bytes: data_log_profile.write_bytes,
            node_catalog_publish_nanos,
            node_catalog_manifest_lock_wait_nanos: catalog_profile.manifest_lock_wait_nanos,
            node_catalog_manifest_row_sync_nanos: catalog_profile.manifest_row_sync_nanos,
            node_catalog_manifest_commit_nanos: catalog_profile.manifest_commit_nanos,
            node_catalog_segment_lock_wait_nanos: catalog_profile.segment_lock_wait_nanos,
            node_catalog_segment_row_sync_nanos: catalog_profile.segment_row_sync_nanos,
            node_catalog_segment_commit_nanos: catalog_profile.segment_commit_nanos,
            node_catalog_manifest_rows: catalog_profile.manifest_rows,
            node_catalog_sealed_rows: catalog_profile.sealed_rows,
            node_catalog_placement_rows: catalog_profile.placement_rows,
            node_catalog_segment_rows: catalog_profile.segment_rows,
            root_sqlite_row_sync_nanos,
            root_sqlite_commit_nanos,
            new_segment_count,
            new_segment_bytes,
            touched_node_count: catalog_profile.touched_node_count(),
            durable_commit_high_water: stream_cursor.next_commit_seq.saturating_sub(1),
            ..DurablePersistProfile::default()
        })
    }

    fn persist_native_metadata_delta(
        &self,
        delta: &NativeMetadataDelta,
        nodes: &SelectedStorageNodeState,
        changed_segments: &BTreeSet<SegmentId>,
        segments: Vec<DurableSegmentPayload>,
    ) -> Result<DurablePersistProfile> {
        let total_started = Instant::now();
        #[cfg(test)]
        {
            let delay = *lock(&self.persist_delay)?;
            if let Some(delay) = delay {
                thread::sleep(delay);
            }
            if self.fail_next_persist.swap(false, Ordering::SeqCst) {
                return Err(StorageError::unavailable(
                    "injected durable persist failure",
                ));
            }
        }

        let new_segment_count = usize_to_u64(segments.len());
        let new_segment_bytes = segments
            .iter()
            .map(|segment| usize_to_u64(segment.bytes.len()))
            .fold(0_u64, u64::saturating_add);

        let started = Instant::now();
        let (appended, data_log_profile, data_log_append_sync_nanos) = if segments.is_empty() {
            (
                PendingDataLogAppend::default(),
                DataLogAppendProfile::default(),
                0,
            )
        } else {
            let (appended, profile) =
                self.append_segments_profiled(segments, DataLogSyncMode::Sync, None)?;
            (appended, profile, duration_nanos_u64(started.elapsed()))
        };
        let pre_root_pending_segments = appended.segment_ids();

        let started = Instant::now();
        let catalog_profile = self.persist_selected_node_catalog_publish(
            nodes,
            changed_segments,
            appended,
            &pre_root_pending_segments,
        )?;
        let node_catalog_publish_nanos = duration_nanos_u64(started.elapsed());

        let sqlite_lock_started = Instant::now();
        let mut conn = lock(&self.conn)?;
        let sqlite_lock_wait_nanos = duration_nanos_u64(sqlite_lock_started.elapsed());
        let previous_cursor = load_export_cursor(&conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let started = Instant::now();
        persist_row_native_metadata_delta(&tx, previous_cursor.as_ref(), delta)?;
        let root_sqlite_row_sync_nanos = duration_nanos_u64(started.elapsed());
        let started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        let root_sqlite_commit_nanos = duration_nanos_u64(started.elapsed());

        Ok(DurablePersistProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            data_log_append_sync_nanos,
            sqlite_lock_wait_nanos,
            data_log_encode_nanos: data_log_profile.encode_nanos,
            data_log_write_nanos: data_log_profile.write_nanos,
            data_log_file_sync_nanos: data_log_profile.file_sync_nanos,
            data_log_file_sync_sum_nanos: data_log_profile.file_sync_sum_nanos,
            data_log_file_sync_max_nanos: data_log_profile.file_sync_max_nanos,
            data_log_dir_sync_nanos: data_log_profile.dir_sync_nanos,
            data_log_files_synced: data_log_profile.files_synced,
            data_log_sync_bytes: data_log_profile.sync_bytes,
            data_log_records_written: data_log_profile.records_written,
            data_log_write_bytes: data_log_profile.write_bytes,
            node_catalog_publish_nanos,
            node_catalog_manifest_lock_wait_nanos: catalog_profile.manifest_lock_wait_nanos,
            node_catalog_manifest_row_sync_nanos: catalog_profile.manifest_row_sync_nanos,
            node_catalog_manifest_commit_nanos: catalog_profile.manifest_commit_nanos,
            node_catalog_segment_lock_wait_nanos: catalog_profile.segment_lock_wait_nanos,
            node_catalog_segment_row_sync_nanos: catalog_profile.segment_row_sync_nanos,
            node_catalog_segment_commit_nanos: catalog_profile.segment_commit_nanos,
            node_catalog_manifest_rows: catalog_profile.manifest_rows,
            node_catalog_sealed_rows: catalog_profile.sealed_rows,
            node_catalog_placement_rows: catalog_profile.placement_rows,
            node_catalog_segment_rows: catalog_profile.segment_rows,
            root_sqlite_row_sync_nanos,
            root_sqlite_commit_nanos,
            new_segment_count,
            new_segment_bytes,
            touched_node_count: catalog_profile.touched_node_count(),
            durable_commit_high_water: delta.cursor.next_commit_seq.saturating_sub(1),
            ..DurablePersistProfile::default()
        })
    }

    fn persist_block_delta_commits(
        &self,
        deltas: &[BlockDeltaCommit],
        nodes: &SelectedStorageNodeState,
        segment_ids: &BTreeSet<SegmentId>,
        segments: Vec<DurableSegmentPayload>,
        mut pending_append: PendingDataLogAppend,
    ) -> Result<DurablePersistProfile> {
        let total_started = Instant::now();
        #[cfg(test)]
        {
            let delay = *lock(&self.persist_delay)?;
            if let Some(delay) = delay {
                thread::sleep(delay);
            }
            if self.fail_next_persist.swap(false, Ordering::SeqCst) {
                return Err(StorageError::unavailable(
                    "injected durable persist failure",
                ));
            }
        }
        if deltas.is_empty() {
            return Err(StorageError::invalid_argument(
                "block delta persist requires at least one commit",
            ));
        }

        let new_segment_count = usize_to_u64(segments.len());
        let new_segment_bytes = segments
            .iter()
            .map(|segment| usize_to_u64(segment.bytes.len()))
            .fold(0_u64, u64::saturating_add);
        pending_append.retain_current_placements(segment_ids);
        let prestaged_segment_count = pending_append.placement_count();
        let prestaged_segment_bytes = pending_append.placement_payload_bytes();
        let sync_only_bytes = prestaged_segment_bytes;
        let flush_write_bytes = new_segment_bytes;

        let started = Instant::now();
        let mut data_log_profile = DataLogAppendProfile::default();
        if !segments.is_empty() {
            let (appended, profile) =
                self.append_segments_profiled(segments, DataLogSyncMode::NoSync, Some(&pending_append))?;
            data_log_profile.merge(profile);
            pending_append.merge(appended);
        }
        let sync_storage_node_count = pending_append.storage_node_count();
        data_log_profile.merge(self.sync_pending_data_log_append(&pending_append)?);
        let data_log_append_sync_nanos = duration_nanos_u64(started.elapsed());
        let pre_root_pending_segments = pending_append.segment_ids();

        let started = Instant::now();
        let catalog_profile = self.persist_selected_node_catalog_publish(
            nodes,
            segment_ids,
            pending_append,
            &pre_root_pending_segments,
        )?;
        let node_catalog_publish_nanos = duration_nanos_u64(started.elapsed());

        let sqlite_lock_started = Instant::now();
        let mut conn = lock(&self.conn)?;
        let sqlite_lock_wait_nanos = duration_nanos_u64(sqlite_lock_started.elapsed());
        let tx = conn.transaction().map_err(sqlite_error)?;
        let started = Instant::now();
        for delta in deltas {
            persist_block_delta_commit(&tx, delta)?;
        }
        let root_sqlite_row_sync_nanos = duration_nanos_u64(started.elapsed());
        let started = Instant::now();
        tx.commit().map_err(sqlite_error)?;
        let root_sqlite_commit_nanos = duration_nanos_u64(started.elapsed());
        let durable_commit_high_water = deltas
            .iter()
            .map(|delta| delta.commit_seq.raw())
            .max()
            .unwrap_or_default();

        Ok(DurablePersistProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            data_log_append_sync_nanos,
            sqlite_lock_wait_nanos,
            data_log_encode_nanos: data_log_profile.encode_nanos,
            data_log_write_nanos: data_log_profile.write_nanos,
            data_log_file_sync_nanos: data_log_profile.file_sync_nanos,
            data_log_file_sync_sum_nanos: data_log_profile.file_sync_sum_nanos,
            data_log_file_sync_max_nanos: data_log_profile.file_sync_max_nanos,
            data_log_dir_sync_nanos: data_log_profile.dir_sync_nanos,
            data_log_files_synced: data_log_profile.files_synced,
            data_log_sync_bytes: data_log_profile.sync_bytes,
            data_log_records_written: data_log_profile.records_written,
            data_log_write_bytes: data_log_profile.write_bytes,
            data_log_prestaged_segment_count: prestaged_segment_count,
            data_log_prestaged_segment_bytes: prestaged_segment_bytes,
            data_log_sync_only_bytes: sync_only_bytes,
            data_log_flush_write_bytes: flush_write_bytes,
            data_log_sync_storage_node_count: sync_storage_node_count,
            node_catalog_publish_nanos,
            node_catalog_manifest_lock_wait_nanos: catalog_profile.manifest_lock_wait_nanos,
            node_catalog_manifest_row_sync_nanos: catalog_profile.manifest_row_sync_nanos,
            node_catalog_manifest_commit_nanos: catalog_profile.manifest_commit_nanos,
            node_catalog_segment_lock_wait_nanos: catalog_profile.segment_lock_wait_nanos,
            node_catalog_segment_row_sync_nanos: catalog_profile.segment_row_sync_nanos,
            node_catalog_segment_commit_nanos: catalog_profile.segment_commit_nanos,
            node_catalog_manifest_rows: catalog_profile.manifest_rows,
            node_catalog_sealed_rows: catalog_profile.sealed_rows,
            node_catalog_placement_rows: catalog_profile.placement_rows,
            node_catalog_segment_rows: catalog_profile.segment_rows,
            root_sqlite_row_sync_nanos,
            root_sqlite_commit_nanos,
            new_segment_count,
            new_segment_bytes,
            touched_node_count: catalog_profile.touched_node_count(),
            durable_commit_high_water,
            ..DurablePersistProfile::default()
        })
    }

    fn has_block_delta_commits(&self) -> Result<bool> {
        let conn = lock(&self.conn)?;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM block_delta_commits", [], |row| {
                row.get(0)
            })
            .map_err(sqlite_error)?;
        Ok(count > 0)
    }

    fn prestage_segments(
        &self,
        segments: Vec<DurableSegmentPayload>,
        pending_base: &PendingDataLogAppend,
    ) -> Result<PendingDataLogAppend> {
        #[cfg(test)]
        {
            if self.fail_next_prestage.swap(false, Ordering::SeqCst) {
                return Err(StorageError::unavailable(
                    "injected durable prestage failure",
                ));
            }
        }
        self.append_segments_profiled(segments, DataLogSyncMode::NoSync, Some(pending_base))
            .map(|(append, _)| append)
    }

    fn append_segments(
        &self,
        segments: Vec<DurableSegmentPayload>,
        sync_mode: DataLogSyncMode,
        pending_base: Option<&PendingDataLogAppend>,
    ) -> Result<PendingDataLogAppend> {
        self.append_segments_profiled(segments, sync_mode, pending_base)
            .map(|(append, _)| append)
    }

    fn append_segments_profiled(
        &self,
        segments: Vec<DurableSegmentPayload>,
        sync_mode: DataLogSyncMode,
        pending_base: Option<&PendingDataLogAppend>,
    ) -> Result<(PendingDataLogAppend, DataLogAppendProfile)> {
        self.append_segments_profiled_with_state(
            segments,
            sync_mode,
            pending_base,
            GENERIC_DATA_LOG_STATE_ACTIVE,
        )
    }

    fn append_run_payload_chunks_unsynced(
        &self,
        payload: DurableAppendRunChunkPayload,
        pending_base: Option<&PendingDataLogAppend>,
    ) -> Result<(AppendLogRun, PendingDataLogAppend, DataLogAppendProfile)> {
        let payload_bytes = payload.chunks.iter().try_fold(0_u64, |total, chunk| {
            total
                .checked_add(usize_to_u64(chunk.len()))
                .ok_or_else(|| StorageError::invalid_argument("payload length overflows u64"))
        })?;
        if payload_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "append run payload must not be empty",
            ));
        }
        let record_len = payload_bytes;
        let mut append = PendingDataLogAppend::default();
        let mut profile = DataLogAppendProfile::default();
        let storage_node = payload.storage_node;
        let data_dir = node_data_log_dir(&self.paths.data_dir, storage_node);
        let active_state = stream_data_log_state(payload.stream_id);

        let (mut file, log_id, record_offset, new_total, needs_dir_sync) = {
            let _allocation_guard = lock(&self.data_log_allocation_lock)?;
            let mut active = match pending_base {
                Some(pending) => pending.active_log_for_node(
                    storage_node,
                    &self.paths.data_dir,
                    &active_state,
                )?,
                None => None,
            };
            let mut active = match active.take() {
                Some(active) => active,
                None => {
                    let node_conn = self.node_catalogs.lock(storage_node)?;
                    active_data_log_with_state(
                        &node_conn,
                        &self.paths.data_dir,
                        storage_node,
                        &active_state,
                    )?
                }
            };
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
                active = next_data_log(
                    &node_conn,
                    &self.paths.data_dir,
                    storage_node,
                    active.log_id,
                )?;
            }

            fs::create_dir_all(&data_dir).map_err(fs_error)?;
            let path = data_log_path(&self.paths.data_dir, storage_node, active.log_id);
            let needs_dir_sync = !path.exists();
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(fs_error)?;
            let file_len = file.metadata().map_err(fs_error)?.len();
            active.total_bytes = active.total_bytes.max(file_len);
            let record_offset = active.total_bytes;
            let new_total = record_offset
                .checked_add(record_len)
                .ok_or_else(|| StorageError::conflict("data-log size overflow"))?;
            (file, active.log_id, record_offset, new_total, needs_dir_sync)
        };

        let started = Instant::now();
        let integrity =
            segment_payload_integrity_chunks(payload.payload_integrity, &payload.chunks);
        profile.encode_nanos = duration_nanos_u64(started.elapsed());
        let started = Instant::now();
        file.seek(SeekFrom::Start(record_offset))
            .map_err(fs_error)?;
        for chunk in &payload.chunks {
            file.write_all(chunk).map_err(fs_error)?;
        }
        profile.write_nanos = duration_nanos_u64(started.elapsed());

        let payload_offset = record_offset;
        let log_ref = DurableDataLogRef {
            storage_node,
            log_id,
        };
        append.logs.insert(
            log_ref,
            PendingDataLogManifest {
                storage_node,
                log_id,
                state: active_state,
                total_bytes: new_total,
                needs_dir_sync,
            },
        );

        let run = AppendLogRun {
            run_id: payload.run_id,
            storage_node,
            stream_id: payload.stream_id,
            writer_epoch: payload.writer_epoch,
            keyspace_id: payload.keyspace_id,
            file_id: payload.file_id,
            file_offset_start: payload.file_offset_start,
            payload_len: payload_bytes,
            log_id,
            log_payload_offset: payload_offset,
            log_record_bytes: record_len,
            integrity,
        };
        run.validate()?;
        Ok((run, append, profile))
    }

    fn read_append_run_source_payload(
        &self,
        storage_node: StorageNodeId,
        log_id: u64,
        range: ByteRange,
        integrity: SegmentPayloadIntegrity,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<()> {
        if usize::try_from(range.len)
            .map_err(|_| StorageError::corrupt("append run range length overflows usize"))?
            != buf.len()
        {
            return Err(StorageError::invalid_argument(
                "append run read buffer length disagrees with source range",
            ));
        }
        let path = data_log_path(&self.paths.data_dir, storage_node, log_id);
        let mut file = File::open(&path).map_err(fs_error)?;
        file.seek(SeekFrom::Start(range.offset)).map_err(fs_error)?;
        file.read_exact(buf).map_err(fs_error)?;
        match integrity {
            SegmentPayloadIntegrity::Unchecked => {
                if matches!(verification, ReadVerification::RequireVerified) {
                    return Err(StorageError::conflict(
                        "read requires verified payload but append run is unchecked",
                    ));
                }
            }
            integrity @ SegmentPayloadIntegrity::Crc32c(_) => {
                if !matches!(verification, ReadVerification::Skip) {
                    verify_segment_payload_integrity(integrity, buf)?;
                }
            }
        }
        Ok(())
    }

    fn append_segments_profiled_with_state(
        &self,
        segments: Vec<DurableSegmentPayload>,
        sync_mode: DataLogSyncMode,
        pending_base: Option<&PendingDataLogAppend>,
        active_state: &str,
    ) -> Result<(PendingDataLogAppend, DataLogAppendProfile)> {
        let mut append = PendingDataLogAppend::default();
        if segments.is_empty() {
            return Ok((append, DataLogAppendProfile::default()));
        }

        let mut active_logs = BTreeMap::new();
        let mut open_log: Option<(DurableDataLogRef, File, u64, bool)> = None;
        let mut files_to_sync = Vec::new();
        let mut synced_dirs = BTreeSet::new();
        let mut profile = DataLogAppendProfile::default();
        {
            let _allocation_guard = lock(&self.data_log_allocation_lock)?;
            for segment in segments {
                let segment_id = segment.segment_id;
                let storage_node = segment.storage_node;
                let integrity = segment.integrity;
                let bytes = segment.bytes;
                let data_dir = node_data_log_dir(&self.paths.data_dir, storage_node);
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    active_logs.entry(storage_node)
                {
                    let pending_active = match pending_base {
                        Some(pending) => pending.active_log_for_node(
                            storage_node,
                            &self.paths.data_dir,
                            active_state,
                        )?,
                        None => None,
                    };
                    if let Some(active) = pending_active {
                        entry.insert(active);
                    } else {
                        let node_conn = self.node_catalogs.lock(storage_node)?;
                        entry.insert(active_data_log_with_state(
                            &node_conn,
                            &self.paths.data_dir,
                            storage_node,
                            active_state,
                        )?);
                    }
                }
                let active = active_logs
                    .get_mut(&storage_node)
                    .ok_or_else(|| StorageError::corrupt("active data-log row missing"))?;
                let payload_bytes = u64::try_from(bytes.len())
                    .map_err(|_| StorageError::invalid_argument("payload length overflows u64"))?;
                let record_len = (DATA_LOG_HEADER_LEN as u64)
                    .checked_add(payload_bytes)
                    .ok_or_else(|| {
                        StorageError::invalid_argument("data-log record length overflows")
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
                if open_log.as_ref().map(|(log_ref, _, _, _)| *log_ref) != Some(log_ref) {
                    if let Some((_, file, bytes, _)) = open_log.take()
                        && sync_mode == DataLogSyncMode::Sync
                    {
                        files_to_sync.push(data_log_file_to_sync(file, bytes));
                    }
                    let data_dir_existed = data_dir.exists();
                    fs::create_dir_all(&data_dir).map_err(fs_error)?;
                    if sync_mode == DataLogSyncMode::Sync && !data_dir_existed {
                        let started = Instant::now();
                        sync_dir(&self.paths.data_dir)?;
                        profile.dir_sync_nanos = profile
                            .dir_sync_nanos
                            .saturating_add(duration_nanos_u64(started.elapsed()));
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
                    let needs_dir_sync = sync_mode == DataLogSyncMode::NoSync && !existed;
                    open_log = Some((log_ref, file, active.total_bytes, needs_dir_sync));
                }

                let offset = active.total_bytes;
                let Some((_, file, open_log_bytes, needs_dir_sync)) = open_log.as_mut() else {
                    return Err(StorageError::conflict("data-log writer was not opened"));
                };
                let started = Instant::now();
                let record = encode_data_log_record(segment_id, integrity, bytes.as_ref())?;
                profile.encode_nanos = profile
                    .encode_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
                let started = Instant::now();
                file.write_all(&record).map_err(fs_error)?;
                profile.write_nanos = profile
                    .write_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
                profile.records_written = profile.records_written.saturating_add(1);
                profile.write_bytes = profile.write_bytes.saturating_add(record_len);
                let payload_offset = offset
                    .checked_add(DATA_LOG_HEADER_LEN as u64)
                    .ok_or_else(|| StorageError::conflict("data-log payload offset overflow"))?;
                let new_total = offset
                    .checked_add(record_len)
                    .ok_or_else(|| StorageError::conflict("data-log size overflow"))?;
                active.total_bytes = new_total;
                *open_log_bytes = new_total;
                append.logs.insert(
                    log_ref,
                    PendingDataLogManifest {
                        storage_node,
                        log_id: active.log_id,
                        state: active_state.to_string(),
                        total_bytes: new_total,
                        needs_dir_sync: *needs_dir_sync,
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
                    integrity,
                });
            }
            if let Some((_, file, bytes, _)) = open_log.take()
                && sync_mode == DataLogSyncMode::Sync
            {
                files_to_sync.push(data_log_file_to_sync(file, bytes));
            }
        }
        if sync_mode == DataLogSyncMode::Sync {
            let started = Instant::now();
            let sync_profile = sync_data_log_files(files_to_sync)?;
            profile.file_sync_nanos = profile
                .file_sync_nanos
                .saturating_add(duration_nanos_u64(started.elapsed()));
            profile.file_sync_sum_nanos = profile
                .file_sync_sum_nanos
                .saturating_add(sync_profile.sync_sum_nanos);
            profile.file_sync_max_nanos = profile
                .file_sync_max_nanos
                .max(sync_profile.sync_max_nanos);
            profile.files_synced = profile
                .files_synced
                .saturating_add(sync_profile.files_synced);
            profile.sync_bytes = profile.sync_bytes.saturating_add(sync_profile.sync_bytes);
            for storage_node in synced_dirs {
                let data_dir = node_data_log_dir(&self.paths.data_dir, storage_node);
                let started = Instant::now();
                sync_dir(&data_dir)?;
                profile.dir_sync_nanos = profile
                    .dir_sync_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }
        }
        Ok((append, profile))
    }

    fn append_segments_bounded(
        &self,
        segments: Vec<DurableSegmentPayload>,
        pending_base: &PendingDataLogAppend,
    ) -> Result<(PendingDataLogAppend, DataLogAppendProfile)> {
        let mut appended = PendingDataLogAppend::default();
        let mut profile = DataLogAppendProfile::default();
        let mut base = pending_base.clone();
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0_u64;

        for segment in segments {
            let segment_bytes = usize_to_u64(segment.bytes.len());
            if !chunk.is_empty()
                && chunk_bytes.saturating_add(segment_bytes) > MAX_DATA_LOG_SYNC_GROUP_BYTES
            {
                let (new_append, chunk_profile) = self.append_segments_profiled(
                    std::mem::take(&mut chunk),
                    DataLogSyncMode::Sync,
                    Some(&base),
                )?;
                profile.merge(chunk_profile);
                base.merge(new_append.clone());
                appended.merge(new_append);
                chunk_bytes = 0;
            }
            chunk_bytes = chunk_bytes.saturating_add(segment_bytes);
            chunk.push(segment);
        }

        if !chunk.is_empty() {
            let (new_append, chunk_profile) =
                self.append_segments_profiled(chunk, DataLogSyncMode::Sync, Some(&base))?;
            profile.merge(chunk_profile);
            appended.merge(new_append);
        }
        Ok((appended, profile))
    }

    fn sync_pending_data_log_append(
        &self,
        appended: &PendingDataLogAppend,
    ) -> Result<DataLogAppendProfile> {
        let mut profile = DataLogAppendProfile::default();
        if appended.logs.is_empty() && appended.sealed_logs.is_empty() {
            return Ok(profile);
        }

        let mut files = Vec::with_capacity(appended.logs.len() + appended.sealed_logs.len());
        let mut storage_nodes_needing_dir_sync = BTreeSet::new();
        for (log_ref, manifest) in &appended.logs {
            if manifest.needs_dir_sync {
                storage_nodes_needing_dir_sync.insert(log_ref.storage_node);
            }
            let path = data_log_path(&self.paths.data_dir, log_ref.storage_node, log_ref.log_id);
            files.push(data_log_file_to_sync(
                File::open(&path).map_err(fs_error)?,
                manifest.total_bytes,
            ));
        }
        for log_ref in &appended.sealed_logs {
            if appended.logs.contains_key(log_ref) {
                continue;
            }
            let path = data_log_path(&self.paths.data_dir, log_ref.storage_node, log_ref.log_id);
            files.push(data_log_file_to_sync_with_metadata(
                File::open(&path).map_err(fs_error)?,
            )?);
        }
        let started = Instant::now();
        let sync_profile = sync_data_log_files(files)?;
        profile.file_sync_nanos = duration_nanos_u64(started.elapsed());
        profile.file_sync_sum_nanos = sync_profile.sync_sum_nanos;
        profile.file_sync_max_nanos = sync_profile.sync_max_nanos;
        profile.files_synced = sync_profile.files_synced;
        profile.sync_bytes = sync_profile.sync_bytes;
        if !storage_nodes_needing_dir_sync.is_empty() {
            let started = Instant::now();
            sync_dir(&self.paths.data_dir)?;
            profile.dir_sync_nanos = profile
                .dir_sync_nanos
                .saturating_add(duration_nanos_u64(started.elapsed()));
        }
        for storage_node in storage_nodes_needing_dir_sync {
            let started = Instant::now();
            sync_dir(&node_data_log_dir(&self.paths.data_dir, storage_node))?;
            profile.dir_sync_nanos = profile
                .dir_sync_nanos
                .saturating_add(duration_nanos_u64(started.elapsed()));
        }
        Ok(profile)
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
        let data = decode_segment_data_log_record(&record)?;
        if data.segment_id != placement.segment_id
            || data.bytes.len() as u64 != placement.payload_bytes
            || data.integrity != placement.integrity
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
                    payload_offset, payload_bytes, payload_integrity
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

    fn append_stream_incarnation(&self) -> Result<u64> {
        let mut conn = lock(&self.conn)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let current = tx
            .query_row(
                "SELECT next_incarnation
                 FROM append_stream_runtime
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
            .ok_or_else(|| StorageError::conflict("append stream incarnation overflow"))?;
        tx.execute(
            "INSERT INTO append_stream_runtime(id, next_incarnation)
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
                    integrity: placement.integrity,
                    bytes: Arc::from(self.read_segment_payload(placement)?),
                });
            }
            let appended = self.append_segments(payloads, DataLogSyncMode::Sync, None)?;
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
