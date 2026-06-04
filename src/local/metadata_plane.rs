#[derive(Debug, Clone)]
pub(super) struct DurableSegmentRecord {
    synced: bool,
    commit: SegmentReplicaCommit,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct MetadataInner {
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
    append_streams: BTreeMap<AppendStreamId, AppendStreamState>,
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
            append_streams: BTreeMap::new(),
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

    fn prune_append_streams_for_durable_export(&mut self) {
        for stream in self.append_streams.values_mut() {
            stream.records.retain(|record| {
                record
                    .offset
                    .checked_add(record.len)
                    .is_some_and(|end| end <= stream.durable_through)
            });
            stream.reserved_tail = stream.reserved_tail.min(stream.durable_through);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) enum AppendStreamStatus {
    Active,
    Released,
    Fenced,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct AppendStreamRunRecord {
    ticket_id: AppendTicketId,
    offset: u64,
    len: u64,
    run: AppendLogRun,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AppendStreamPrefixPersistBatch {
    records: Vec<AppendStreamRunRecord>,
    durable_through: u64,
    payload_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AppendStreamPrefixPersistPlan {
    stream: AppendStream,
    batch: AppendStreamPrefixPersistBatch,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedAppendStreamRun {
    stream: AppendStream,
    ticket_id: AppendTicketId,
    range: ByteRange,
    storage_node: StorageNodeId,
    run_id: AppendRunId,
}

impl AppendStreamRunRecord {
    fn end_exclusive(&self) -> Result<u64> {
        self.offset
            .checked_add(self.len)
            .ok_or_else(|| StorageError::invalid_argument("append record end overflows"))
    }

    fn log_ref(&self) -> DurableDataLogRef {
        DurableDataLogRef {
            storage_node: self.run.storage_node,
            log_id: self.run.log_id,
        }
    }

    fn payload_bytes(&self) -> u64 {
        self.run.payload_len
    }

    fn storage_node(&self) -> StorageNodeId {
        self.run.storage_node
    }

    fn slice(&self, start: u64, end: u64) -> Result<Option<Self>> {
        if start >= end {
            return Ok(None);
        }
        let record_end = self.end_exclusive()?;
        if start < self.offset || end > record_end {
            return Err(StorageError::invalid_argument(
                "append stream record slice exceeds source record",
            ));
        }
        let delta = start - self.offset;
        let len = end - start;
        let integrity = if start == self.offset && end == record_end {
            self.run.integrity
        } else {
            SegmentPayloadIntegrity::Unchecked
        };
        let run = AppendLogRun {
            file_offset_start: start,
            payload_len: len,
            log_payload_offset: self
                .run
                .log_payload_offset
                .checked_add(delta)
                .ok_or_else(|| StorageError::invalid_argument("append run offset overflows"))?,
            log_record_bytes: len,
            integrity,
            ..self.run.clone()
        };
        run.validate()?;
        Ok(Some(Self {
            ticket_id: self.ticket_id,
            offset: start,
            len,
            run,
        }))
    }

    fn coalesce_with(&self, next: &Self) -> Result<Option<Self>> {
        let self_end = self.end_exclusive()?;
        if self_end != next.offset
            || self.run.log_record_bytes != self.run.payload_len
            || next.run.log_record_bytes != next.run.payload_len
            || self.run.storage_node != next.run.storage_node
            || self.run.stream_id != next.run.stream_id
            || self.run.writer_epoch != next.run.writer_epoch
            || self.run.keyspace_id != next.run.keyspace_id
            || self.run.file_id != next.run.file_id
            || self.run.log_id != next.run.log_id
            || self.run.file_offset_start != self.offset
            || next.run.file_offset_start != next.offset
            || self
                .run
                .log_payload_offset
                .checked_add(self.run.payload_len)
                != Some(next.run.log_payload_offset)
        {
            return Ok(None);
        }
        let len = self
            .len
            .checked_add(next.len)
            .ok_or_else(|| StorageError::invalid_argument("append run length overflows"))?;
        if len > MAX_STREAM_DATA_LOG_SYNC_GROUP_BYTES {
            return Ok(None);
        }
        let integrity =
            combine_segment_payload_integrity(self.run.integrity, next.run.integrity, next.len)?;
        let run = AppendLogRun {
            run_id: self.run.run_id,
            storage_node: self.run.storage_node,
            stream_id: self.run.stream_id,
            writer_epoch: self.run.writer_epoch,
            keyspace_id: self.run.keyspace_id,
            file_id: self.run.file_id,
            file_offset_start: self.run.file_offset_start,
            payload_len: len,
            log_id: self.run.log_id,
            log_payload_offset: self.run.log_payload_offset,
            log_record_bytes: len,
            integrity,
        };
        run.validate()?;
        Ok(Some(Self {
            ticket_id: self.ticket_id,
            offset: self.offset,
            len,
            run,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct AppendStreamState {
    keyspace_id: KeyspaceId,
    file_id: FileId,
    stream_id: AppendStreamId,
    writer_epoch: WriterEpoch,
    base_version: FileVersion,
    visible_base_size: u64,
    reserved_tail: u64,
    durable_through: u64,
    published_through: u64,
    status: AppendStreamStatus,
    // Commit order by stream offset; append_stream_run_record enforces it.
    records: Vec<AppendStreamRunRecord>,
}

impl AppendStreamState {
    fn public_stream(&self) -> AppendStream {
        AppendStream {
            keyspace_id: self.keyspace_id,
            file_id: self.file_id,
            stream_id: self.stream_id,
            writer_epoch: self.writer_epoch,
            base_version: self.base_version,
            visible_base_size: self.visible_base_size,
        }
    }

    fn validate_token(&self, stream: &AppendStream) -> Result<()> {
        if self.keyspace_id != stream.keyspace_id
            || self.file_id != stream.file_id
            || self.stream_id != stream.stream_id
            || self.writer_epoch != stream.writer_epoch
            || self.base_version != stream.base_version
            || self.visible_base_size != stream.visible_base_size
            || self.status != AppendStreamStatus::Active
        {
            return Err(StorageError::conflict("stale append stream"));
        }
        Ok(())
    }

    fn contiguous_record_tail_from(&self, start: u64) -> Result<u64> {
        let mut expected = start;
        for record in &self.records {
            let end = record.end_exclusive()?;
            if end <= expected {
                continue;
            }
            if record.offset > expected {
                break;
            }
            expected = end;
        }
        Ok(expected)
    }

    fn publish_records(&self, start: u64, end: u64) -> Result<Vec<AppendStreamRunRecord>> {
        if start >= end {
            return Err(StorageError::invalid_argument(
                "append stream publish range must not be empty",
            ));
        }
        let mut expected = start;
        let mut selected = Vec::new();
        for record in &self.records {
            let record_end = record.end_exclusive()?;
            if record_end <= expected {
                continue;
            }
            if record.offset > expected {
                break;
            }
            let slice_end = record_end.min(end);
            if let Some(slice) = record.slice(expected, slice_end)? {
                expected = slice.end_exclusive()?;
                selected.push(slice);
            }
            if expected == end {
                return Ok(selected);
            }
        }
        Err(StorageError::conflict(
            "append-run publish requires contiguous durable records",
        ))
    }

    fn prefix_persist_batch(&self, max_batch_bytes: u64) -> Result<AppendStreamPrefixPersistBatch> {
        if max_batch_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "append stream prefix-persist batch byte cap must be greater than zero",
            ));
        }
        let mut expected = self.durable_through;
        let mut payload_bytes = 0_u64;
        let mut selected = Vec::new();
        for record in &self.records {
            let end = record.end_exclusive()?;
            if end <= expected {
                continue;
            }
            if record.offset != expected {
                break;
            }
            let record_payload_bytes = record.payload_bytes();
            let would_exceed = !selected.is_empty()
                && payload_bytes
                    .checked_add(record_payload_bytes)
                    .ok_or_else(|| StorageError::invalid_argument("append batch bytes overflow"))?
                    > max_batch_bytes;
            if would_exceed {
                break;
            }
            payload_bytes = payload_bytes
                .checked_add(record_payload_bytes)
                .ok_or_else(|| StorageError::invalid_argument("append batch bytes overflow"))?;
            expected = end;
            selected.push(record.clone());
        }
        Ok(AppendStreamPrefixPersistBatch {
            records: selected,
            durable_through: expected,
            payload_bytes,
        })
    }

    fn durable_export_at(&self, durable_through: u64) -> Result<Self> {
        if durable_through < self.durable_through {
            return Err(StorageError::conflict(
                "append stream durable export cannot regress",
            ));
        }
        if durable_through > self.contiguous_record_tail_from(self.durable_through)? {
            return Err(StorageError::conflict(
                "append stream durable export exceeds contiguous records",
            ));
        }
        let mut exported = self.clone();
        exported.durable_through = durable_through;
        exported.reserved_tail = durable_through;
        exported.records.retain(|record| {
            record
                .offset
                .checked_add(record.len)
                .is_some_and(|end| end <= durable_through)
        });
        Ok(exported)
    }
}

impl AppendStreamPrefixPersistBatch {
    fn payload_bytes_by_storage_node(&self) -> Result<BTreeMap<StorageNodeId, u64>> {
        let mut out = BTreeMap::new();
        for record in &self.records {
            let entry = out.entry(record.storage_node()).or_insert(0_u64);
            *entry = entry
                .checked_add(record.payload_bytes())
                .ok_or_else(|| StorageError::invalid_argument("append batch bytes overflow"))?;
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AppendStreamAllocator {
    incarnation: u64,
    next_counter: u64,
}

impl AppendStreamAllocator {
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
            .ok_or_else(|| StorageError::conflict("append stream counter overflow"))?;
        Ok((u128::from(self.incarnation) << 64) | u128::from(counter))
    }

    fn next_stream_id(&mut self) -> Result<AppendStreamId> {
        self.next_raw().map(AppendStreamId::from_raw)
    }

    fn next_ticket_id(&mut self) -> Result<AppendTicketId> {
        self.next_raw().map(AppendTicketId::from_raw)
    }

    fn next_publish_ticket_id(&mut self) -> Result<AppendPublishTicketId> {
        self.next_raw().map(AppendPublishTicketId::from_raw)
    }
}

#[derive(Debug, Clone)]
struct AppendPublishTicketRecord {
    ticket: AppendPublishTicket,
    completed: Option<AppendPublishCommit>,
}

#[derive(Debug, Clone)]
enum AppendPublishTicketStatus {
    Pending(AppendStream),
    Completed(AppendPublishCommit),
}

/// In-memory implementation of `MetadataPlane`.
#[derive(Debug)]
pub struct InMemoryMetadataPlane {
    config: LocalStoreConfig,
    inner: Mutex<MetadataInner>,
    append_stream_allocator: Mutex<AppendStreamAllocator>,
    append_publish_tickets: Mutex<BTreeMap<AppendPublishTicketId, AppendPublishTicketRecord>>,
    publish_profiler: Mutex<Option<MetadataPublishProfiler>>,
}

impl InMemoryMetadataPlane {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(MetadataInner::new()),
            append_stream_allocator: Mutex::new(AppendStreamAllocator::new(0)),
            append_publish_tickets: Mutex::new(BTreeMap::new()),
            publish_profiler: Mutex::new(None),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: MetadataInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
            append_stream_allocator: Mutex::new(AppendStreamAllocator::new(0)),
            append_publish_tickets: Mutex::new(BTreeMap::new()),
            publish_profiler: Mutex::new(None),
        })
    }

    fn state_inner(&self) -> Result<MetadataInner> {
        Ok(lock(&self.inner)?.clone())
    }

    fn config(&self) -> LocalStoreConfig {
        self.config
    }

    fn enable_publish_profiling(&self, capacity: usize) -> Result<()> {
        *lock(&self.publish_profiler)? = Some(MetadataPublishProfiler::new(capacity)?);
        Ok(())
    }

    fn record_publish_profile(&self, profile: MetadataPublishProfile) -> Result<()> {
        if let Some(profiler) = lock(&self.publish_profiler)?.as_mut() {
            profiler.record(profile);
        }
        Ok(())
    }

    fn drain_publish_profiles(&self, max: usize) -> Result<Vec<MetadataPublishProfile>> {
        let mut profiler = lock(&self.publish_profiler)?;
        Ok(profiler
            .as_mut()
            .map(|profiler| profiler.drain(max))
            .unwrap_or_default())
    }

    fn use_append_stream_incarnation(&self, incarnation: u64) -> Result<()> {
        *lock(&self.append_stream_allocator)? = AppendStreamAllocator::new(incarnation);
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
    ) -> Result<KeyspaceRoot> {
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
                let checkpoint_root = Self::keyspace_root_locked(&inner, checkpoint_root)?;
                if replayed.shard_roots != checkpoint_root.shard_roots
                    || replayed.file_count != checkpoint_root.file_count
                {
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
    fn keyspace_root_count_for_test(&self) -> Result<usize> {
        Ok(lock(&self.inner)?.keyspace_roots.len())
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

    fn set_next_commit_seq_for_replay(&self, next_commit_seq: CommitSeq) -> Result<()> {
        lock(&self.inner)?.next_commit_seq = next_commit_seq.raw();
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

    pub fn open_append_stream(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendStream> {
        let stream_id = lock(&self.append_stream_allocator)?.next_stream_id()?;
        let mut inner = lock(&self.inner)?;
        let head = Self::file_head_locked(&inner, keyspace_id, file_id)?;
        let key = (keyspace_id, file_id);
        let persisted_epoch = inner
            .file_writer_epochs
            .get(&key)
            .copied()
            .unwrap_or_else(|| WriterEpoch::from_raw(0));
        let current_epoch = inner
            .append_streams
            .values()
            .filter(|stream| {
                stream.keyspace_id == keyspace_id
                    && stream.file_id == file_id
                    && stream.status == AppendStreamStatus::Active
            })
            .map(|stream| stream.writer_epoch.raw())
            .chain(std::iter::once(persisted_epoch.raw()))
            .max()
            .map(WriterEpoch::from_raw)
            .unwrap_or(persisted_epoch);
        let writer_epoch = current_epoch
            .raw()
            .checked_add(1)
            .map(WriterEpoch::from_raw)
            .ok_or_else(|| StorageError::conflict("writer epoch overflow"))?;
        for active_stream in inner.append_streams.values_mut() {
            if active_stream.keyspace_id == keyspace_id
                && active_stream.file_id == file_id
                && active_stream.status == AppendStreamStatus::Active
            {
                active_stream.status = AppendStreamStatus::Fenced;
            }
        }
        inner.file_writer_epochs.insert(key, writer_epoch);
        let stream = AppendStreamState {
            keyspace_id,
            file_id,
            stream_id,
            writer_epoch,
            base_version: head.version,
            visible_base_size: head.size,
            reserved_tail: head.size,
            durable_through: head.size,
            published_through: head.size,
            status: AppendStreamStatus::Active,
            records: Vec::new(),
        };
        let public = stream.public_stream();
        inner.append_streams.insert(stream_id, stream);
        Ok(public)
    }

    pub fn next_append_ticket_id(&self) -> Result<AppendTicketId> {
        lock(&self.append_stream_allocator)?.next_ticket_id()
    }

    pub fn next_append_publish_ticket_id(&self) -> Result<AppendPublishTicketId> {
        lock(&self.append_stream_allocator)?.next_publish_ticket_id()
    }

    pub fn reserve_append_stream_range(
        &self,
        stream: &AppendStream,
        len: u64,
    ) -> Result<ByteRange> {
        if len == 0 {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        let mut inner = lock(&self.inner)?;
        Self::file_head_locked(&inner, stream.keyspace_id, stream.file_id)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        let offset = state.reserved_tail;
        let next_tail = state
            .reserved_tail
            .checked_add(len)
            .ok_or_else(|| StorageError::invalid_argument("append stream tail overflows file"))?;
        state.reserved_tail = next_tail;
        Ok(ByteRange::new(offset, len))
    }

    pub fn append_stream_run_record(
        &self,
        stream: &AppendStream,
        ticket_id: AppendTicketId,
        range: ByteRange,
        run: AppendLogRun,
    ) -> Result<AppendTicket> {
        if range.len == 0 {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        run.validate()?;
        if run.stream_id != stream.stream_id
            || run.writer_epoch != stream.writer_epoch
            || run.keyspace_id != stream.keyspace_id
            || run.file_id != stream.file_id
            || run.file_offset_start != range.offset
            || run.payload_len != range.len
        {
            return Err(StorageError::invalid_argument(
                "append run does not match stream record range",
            ));
        }
        let mut inner = lock(&self.inner)?;
        Self::file_head_locked(&inner, stream.keyspace_id, stream.file_id)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        if range.end_exclusive()? > state.reserved_tail {
            return Err(StorageError::conflict(
                "append stream record exceeds reserved tail",
            ));
        }
        if let Some(last) = state.records.last()
            && range.offset < last.end_exclusive()?
        {
            return Err(StorageError::conflict(
                "append stream records must commit in stream order",
            ));
        }
        let record = AppendStreamRunRecord {
            ticket_id,
            offset: range.offset,
            len: range.len,
            run,
        };
        if let Some(last) = state.records.last_mut()
            && last.end_exclusive()? > state.durable_through
            && let Some(coalesced) = last.coalesce_with(&record)?
        {
            *last = coalesced;
        } else {
            state.records.push(record);
        }
        Ok(AppendTicket {
            keyspace_id: stream.keyspace_id,
            file_id: stream.file_id,
            stream_id: stream.stream_id,
            ticket_id,
            writer_epoch: stream.writer_epoch,
            range,
        })
    }

    pub fn mark_append_stream_durable(&self, stream: &AppendStream) -> Result<u64> {
        let mut inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.durable_through = state.contiguous_record_tail_from(state.durable_through)?;
        Ok(state.durable_through)
    }

    fn append_stream_prefix_persist_target(&self, stream: &AppendStream) -> Result<u64> {
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.contiguous_record_tail_from(state.durable_through)
    }

    fn append_stream_auto_persist_target(
        &self,
        stream: &AppendStream,
        threshold: u64,
    ) -> Result<Option<u64>> {
        if threshold == 0 {
            return Err(StorageError::invalid_argument(
                "append stream auto-persist threshold must be greater than zero",
            ));
        }
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        let contiguous = state.contiguous_record_tail_from(state.durable_through)?;
        let dirty_bytes = contiguous.saturating_sub(state.durable_through);
        if dirty_bytes < threshold {
            return Ok(None);
        }
        Ok(Some(contiguous))
    }

    pub fn submit_append_publish(
        &self,
        stream: &AppendStream,
        ticket_id: AppendPublishTicketId,
        publish_through: u64,
    ) -> Result<AppendPublishTicket> {
        {
            let inner = lock(&self.inner)?;
            let state = inner
                .append_streams
                .get(&stream.stream_id)
                .ok_or_else(|| StorageError::conflict("stale append stream"))?;
            state.validate_token(stream)?;
            if publish_through <= state.published_through {
                return Err(StorageError::invalid_argument(
                    "append publish target must advance published stream prefix",
                ));
            }
            if publish_through > state.reserved_tail {
                return Err(StorageError::conflict(
                    "append publish target exceeds accepted stream bytes",
                ));
            }
            if publish_through > state.contiguous_record_tail_from(state.published_through)? {
                return Err(StorageError::conflict(
                    "append publish target exceeds contiguous stream records",
                ));
            }
        }
        let ticket = AppendPublishTicket {
            keyspace_id: stream.keyspace_id,
            file_id: stream.file_id,
            stream_id: stream.stream_id,
            ticket_id,
            writer_epoch: stream.writer_epoch,
            publish_through,
        };
        let mut tickets = lock(&self.append_publish_tickets)?;
        if tickets
            .insert(
                ticket_id,
                AppendPublishTicketRecord {
                    ticket: ticket.clone(),
                    completed: None,
                },
            )
            .is_some()
        {
            return Err(StorageError::conflict("append publish ticket id reused"));
        }
        Ok(ticket)
    }

    fn append_publish_ticket_status(
        &self,
        ticket: &AppendPublishTicket,
    ) -> Result<AppendPublishTicketStatus> {
        let record = lock(&self.append_publish_tickets)?
            .get(&ticket.ticket_id)
            .cloned()
            .ok_or_else(|| StorageError::conflict("stale append publish ticket"))?;
        if record.ticket != *ticket {
            return Err(StorageError::conflict("stale append publish ticket"));
        }
        if let Some(commit) = record.completed {
            return Ok(AppendPublishTicketStatus::Completed(commit));
        }
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&ticket.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append publish ticket"))?;
        if state.keyspace_id != ticket.keyspace_id
            || state.file_id != ticket.file_id
            || state.stream_id != ticket.stream_id
            || state.writer_epoch != ticket.writer_epoch
            || state.status != AppendStreamStatus::Active
            || ticket.publish_through <= state.published_through
            || ticket.publish_through > state.reserved_tail
            || ticket.publish_through > state.contiguous_record_tail_from(state.published_through)?
        {
            return Err(StorageError::conflict("stale append publish ticket"));
        }
        Ok(AppendPublishTicketStatus::Pending(state.public_stream()))
    }

    fn complete_append_publish_ticket(
        &self,
        ticket: &AppendPublishTicket,
        commit: AppendPublishCommit,
    ) -> Result<()> {
        let mut tickets = lock(&self.append_publish_tickets)?;
        let record = tickets
            .get_mut(&ticket.ticket_id)
            .ok_or_else(|| StorageError::conflict("stale append publish ticket"))?;
        if record.ticket != *ticket {
            return Err(StorageError::conflict("stale append publish ticket"));
        }
        record.completed = Some(commit);
        Ok(())
    }

    fn append_stream_durable_high_water_if_reached(
        &self,
        stream: &AppendStream,
        durable_through: u64,
    ) -> Result<Option<u64>> {
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        if state.durable_through < durable_through {
            return Ok(None);
        }
        Ok(Some(state.durable_through))
    }

    fn append_stream_publish_records(
        &self,
        stream: &AppendStream,
        start: u64,
        end: u64,
    ) -> Result<Vec<AppendStreamRunRecord>> {
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        if start != state.published_through {
            return Err(StorageError::conflict(
                "append publish start no longer matches stream high-water",
            ));
        }
        if end <= state.published_through {
            return Err(StorageError::invalid_argument(
                "append publish target must advance published stream prefix",
            ));
        }
        if end > state.reserved_tail {
            return Err(StorageError::conflict(
                "append publish target exceeds accepted stream bytes",
            ));
        }
        if end > state.contiguous_record_tail_from(state.published_through)? {
            return Err(StorageError::conflict(
                "append publish target exceeds contiguous stream records",
            ));
        }
        state.publish_records(start, end)
    }

    fn append_stream_prefix_persist_plans_for(
        &self,
        requests: &[(AppendStream, u64)],
        max_batch_bytes: u64,
    ) -> Result<Vec<AppendStreamPrefixPersistPlan>> {
        let inner = lock(&self.inner)?;
        let mut payload_bytes_by_storage_node = BTreeMap::<StorageNodeId, u64>::new();
        let mut plans = Vec::new();
        for (stream, durable_through) in requests {
            let state = inner
                .append_streams
                .get(&stream.stream_id)
                .ok_or_else(|| StorageError::conflict("stale append stream"))?;
            state.validate_token(stream)?;
            if state.status != AppendStreamStatus::Active
                || state.durable_through >= *durable_through
            {
                continue;
            }
            let batch = state.prefix_persist_batch(max_batch_bytes)?;
            if batch.records.is_empty() {
                continue;
            }
            let batch_payload_bytes = batch.payload_bytes_by_storage_node()?;
            let would_exceed = !plans.is_empty()
                && batch_payload_bytes
                    .iter()
                    .try_fold(false, |exceeded, (storage_node, bytes)| {
                        let current = payload_bytes_by_storage_node
                            .get(storage_node)
                            .copied()
                            .unwrap_or_default();
                        let next = current.checked_add(*bytes).ok_or_else(|| {
                            StorageError::invalid_argument("append batch bytes overflow")
                        })?;
                        Ok(exceeded || next > max_batch_bytes)
                    })?;
            if would_exceed {
                continue;
            }
            for (storage_node, bytes) in batch_payload_bytes {
                let entry = payload_bytes_by_storage_node
                    .entry(storage_node)
                    .or_insert(0_u64);
                *entry = entry
                    .checked_add(bytes)
                    .ok_or_else(|| StorageError::invalid_argument("append batch bytes overflow"))?;
            }
            plans.push(AppendStreamPrefixPersistPlan {
                stream: state.public_stream(),
                batch,
            });
        }
        Ok(plans)
    }

    fn append_stream_durable_export_at(
        &self,
        stream: &AppendStream,
        durable_through: u64,
    ) -> Result<AppendStreamState> {
        let inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.durable_export_at(durable_through)
    }

    fn mark_append_stream_durable_through(
        &self,
        stream: &AppendStream,
        durable_through: u64,
    ) -> Result<u64> {
        let mut inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        if durable_through < state.durable_through
            || durable_through > state.contiguous_record_tail_from(state.durable_through)?
        {
            return Err(StorageError::conflict(
                "append stream durable high-water is not valid",
            ));
        }
        state.durable_through = durable_through;
        Ok(durable_through)
    }

    pub fn mark_append_stream_published(
        &self,
        stream: &AppendStream,
        durable_through: u64,
    ) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.published_through = durable_through;
        let mut retained = Vec::new();
        for record in &state.records {
            let record_end = record.end_exclusive()?;
            if record_end <= durable_through {
                continue;
            }
            if record.offset < durable_through {
                if let Some(suffix) = record.slice(durable_through, record_end)? {
                    retained.push(suffix);
                }
            } else {
                retained.push(record.clone());
            }
        }
        state.records = retained;
        Ok(())
    }

    pub fn abort_append_stream(&self, stream: &AppendStream) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.status = AppendStreamStatus::Aborted;
        Ok(())
    }

    pub fn release_append_stream(&self, stream: &AppendStream) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let state = inner
            .append_streams
            .get_mut(&stream.stream_id)
            .ok_or_else(|| StorageError::conflict("stale append stream"))?;
        state.validate_token(stream)?;
        state.status = AppendStreamStatus::Released;
        Ok(())
    }

    pub fn invalidate_append_streams_for_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        for stream in inner.append_streams.values_mut() {
            if stream.keyspace_id == keyspace_id
                && stream.file_id == file_id
                && stream.status == AppendStreamStatus::Active
            {
                stream.status = AppendStreamStatus::Fenced;
            }
        }
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
                    run_extents: Vec::new(),
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

    #[cfg(test)]
    fn current_keyspace_root_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
    ) -> Result<KeyspaceRoot> {
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        Ok(KeyspaceRoot {
            root_id: KeyspaceRootId::from_raw(0),
            shard_roots: head.shard_roots.clone().into(),
            file_count: head.file_count,
        })
    }

    #[cfg(test)]
    fn keyspace_catalog_shard_index(file_id: FileId, root: &KeyspaceRoot) -> Result<usize> {
        Self::keyspace_catalog_shard_index_for_len(file_id, root.shard_roots.len())
    }

    fn keyspace_catalog_shard_index_for_len(file_id: FileId, shard_count: usize) -> Result<usize> {
        if shard_count == 0 {
            return Err(StorageError::corrupt("keyspace root has no catalog shards"));
        }
        Ok((file_id.raw() % shard_count as u128) as usize)
    }

    fn keyspace_file_in_shards_locked(
        inner: &MetadataInner,
        shard_roots: &[KeyspaceCatalogShardId],
        file_id: FileId,
    ) -> Result<KeyspaceFile> {
        let shard_index = Self::keyspace_catalog_shard_index_for_len(file_id, shard_roots.len())?;
        let shard = Self::keyspace_catalog_shard_locked(inner, shard_roots[shard_index])?;
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
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        Self::keyspace_file_in_shards_locked(inner, &head.shard_roots, file_id)
            .map(|entry| entry.head)
    }

    #[cfg(test)]
    fn file_name_locked(
        inner: &MetadataInner,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<Option<String>> {
        let head = inner
            .keyspace_heads
            .get(&keyspace_id)
            .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
        Self::keyspace_file_in_shards_locked(inner, &head.shard_roots, file_id)
            .map(|entry| entry.name)
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
            shard_roots: shard_roots.into(),
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
        shard_roots: &[KeyspaceCatalogShardId],
        file_count: usize,
        file_id: FileId,
        entry: KeyspaceFile,
    ) -> Result<(usize, KeyspaceCatalogShardId, KeyspaceCatalogShard, usize)> {
        let shard_index = Self::keyspace_catalog_shard_index_for_len(file_id, shard_roots.len())?;
        let shard_id = shard_roots[shard_index];
        let shard = Self::keyspace_catalog_shard_locked(inner, shard_id)?;
        let mut files = shard.files.clone();
        let replaced = files.insert(file_id, entry).is_some();
        let new_shard = Self::insert_keyspace_catalog_shard_locked(inner, files)?;
        let file_count = if replaced {
            file_count
        } else {
            file_count
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("keyspace file count overflow"))?
        };
        Ok((shard_index, shard_id, new_shard, file_count))
    }

    fn collect_keyspace_metadata_roots_locked(
        inner: &MetadataInner,
        root_id: KeyspaceRootId,
        out: &mut Vec<MetadataNodeId>,
    ) -> Result<()> {
        let root = Self::keyspace_root_locked(inner, root_id)?;
        for shard_id in root.shard_roots.iter().copied() {
            Self::collect_keyspace_shard_metadata_roots_locked(inner, shard_id, out)?;
        }
        Ok(())
    }

    fn collect_keyspace_shard_metadata_roots_locked(
        inner: &MetadataInner,
        shard_id: KeyspaceCatalogShardId,
        out: &mut Vec<MetadataNodeId>,
    ) -> Result<()> {
        let shard = Self::keyspace_catalog_shard_locked(inner, shard_id)?;
        out.extend(shard.files.values().map(|entry| entry.head.root));
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
    ) -> Result<KeyspaceRoot> {
        let checkpoint = Self::latest_keyspace_checkpoint_at_or_before_locked(
            inner,
            keyspace_id,
            commit_seq,
            excluded_checkpoint,
        )
        .ok_or_else(|| StorageError::not_found("checkpoint", keyspace_id.to_string()))?;
        let checkpoint_root_id = Self::checkpoint_keyspace_root(&checkpoint)?;
        let checkpoint_root = Self::keyspace_root_locked(inner, checkpoint_root_id)?;
        let mut shard_roots = checkpoint_root.shard_roots.to_vec();
        let mut file_count = checkpoint_root.file_count;

        for commit in Self::keyspace_commits_for_keyspace_locked(inner, keyspace_id)
            .into_iter()
            .filter(|commit| {
                commit.commit_seq.raw() > checkpoint.commit_seq.raw()
                    && commit.commit_seq.raw() <= commit_seq.raw()
            })
        {
            let shard_index = usize::try_from(commit.shard_index).map_err(|_| {
                StorageError::invalid_argument("keyspace shard index overflows usize")
            })?;
            if shard_index >= shard_roots.len() {
                return Err(StorageError::corrupt(
                    "keyspace commit references shard outside root set",
                ));
            }
            if shard_roots[shard_index] != commit.old_shard || file_count != commit.old_file_count {
                return Err(StorageError::corrupt(
                    "keyspace commit old state does not match replay state",
                ));
            }
            shard_roots[shard_index] = commit.new_shard;
            file_count = commit.new_file_count;
        }

        Ok(KeyspaceRoot {
            root_id: checkpoint_root_id,
            shard_roots: shard_roots.into(),
            file_count,
        })
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
            for shard_id in head.shard_roots.iter().copied() {
                Self::collect_keyspace_shard_metadata_roots_locked(inner, shard_id, &mut roots)?;
            }
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
                Self::collect_keyspace_shard_metadata_roots_locked(
                    inner,
                    commit.new_shard,
                    &mut roots,
                )?;
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
            let root = Self::insert_keyspace_root_locked(
                inner,
                root.shard_roots.to_vec(),
                root.file_count,
            )?;
            inner.insert_checkpoint(
                owner,
                anchor_seq,
                CheckpointRoots::NativeKeyspace(root.root_id),
            );
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
        if let MetadataNodeKind::Leaf { entries, .. } = &node.kind {
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
                MetadataNodeKind::Leaf { entries, .. } => {
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
            inner
                .append_streams
                .retain(|_, stream| stream.status == AppendStreamStatus::Active);
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
            shard_roots: root.shard_roots.to_vec(),
            file_count: root.file_count,
            latest_commit: CommitSeq::from_raw(0),
        };
        inner.keyspace_heads.insert(keyspace_id, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(keyspace_id),
            head.latest_commit,
            CheckpointRoots::NativeKeyspace(root.root_id),
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
        Ok(KeyspaceInfo {
            keyspace_id,
            generation: head.generation,
            latest_commit: head.latest_commit,
            file_count: head.file_count,
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

        let (shard_index, old_shard, new_shard, new_file_count) =
            Self::update_keyspace_file_locked(
                &mut inner,
                &keyspace_head.shard_roots,
                keyspace_head.file_count,
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
        next_keyspace_head.shard_roots[shard_index] = new_shard.shard_id;
        next_keyspace_head.file_count = new_file_count;

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
            shard_index: u32::try_from(shard_index).map_err(|_| {
                StorageError::invalid_argument("keyspace shard index overflows u32")
            })?,
            old_shard,
            new_shard: new_shard.shard_id,
            old_file_count: keyspace_head.file_count,
            new_file_count,
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
        let publish_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let publish_lock_wait_nanos = duration_nanos_u64(publish_started.elapsed());

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
                        self.record_publish_profile(MetadataPublishProfile {
                            lock_wait_nanos: publish_lock_wait_nanos,
                            logical_conflict_count: 1,
                            ..MetadataPublishProfile::default()
                        })?;
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

                let commit_seq_started = Instant::now();
                let commit_seq = inner.alloc_commit_seq()?;
                let commit_sequence_alloc_nanos = duration_nanos_u64(commit_seq_started.elapsed());
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
                self.record_publish_profile(MetadataPublishProfile {
                    lock_wait_nanos: publish_lock_wait_nanos,
                    commit_sequence_alloc_nanos,
                    touched_shard_head_rows: usize_to_u64(commit_group.updates.len()),
                    commit_rows_written: 1,
                    ..MetadataPublishProfile::default()
                })?;
                Ok(commit_group)
            }
            MappingOwner::NativeKeyspace(keyspace_id) => {
                let current_keyspace = inner
                    .keyspace_heads
                    .get(&keyspace_id)
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
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
                let current_entry = Self::keyspace_file_in_shards_locked(
                    &inner,
                    &current_keyspace.shard_roots,
                    file_id,
                )?;
                let current = current_entry.head.clone();
                let append_stream_commit = match intent.fence {
                    MetadataFence::FileVersion(version) if version == current.version => None,
                    MetadataFence::FileVersion(_) => {
                        self.record_publish_profile(MetadataPublishProfile {
                            lock_wait_nanos: publish_lock_wait_nanos,
                            logical_conflict_count: 1,
                            ..MetadataPublishProfile::default()
                        })?;
                        return Err(StorageError::conflict("stale file version fence"));
                    }
                    MetadataFence::AppendStream {
                        stream_id,
                        writer_epoch,
                    } => {
                        let Some(stream) = inner.append_streams.get(&stream_id) else {
                            return Err(StorageError::conflict("stale append stream"));
                        };
                        if stream.keyspace_id != keyspace_id
                            || stream.file_id != file_id
                            || stream.writer_epoch != writer_epoch
                            || stream.published_through != current.size
                            || stream.status != AppendStreamStatus::Active
                        {
                            self.record_publish_profile(MetadataPublishProfile {
                                lock_wait_nanos: publish_lock_wait_nanos,
                                logical_conflict_count: 1,
                                ..MetadataPublishProfile::default()
                            })?;
                            return Err(StorageError::conflict("stale append stream"));
                        }
                        Some((stream_id, writer_epoch))
                    }
                    _ => {
                        return Err(StorageError::invalid_argument(
                            "native file commit requires file-version or append-stream fence",
                        ));
                    }
                };
                if current.root != old_root {
                    self.record_publish_profile(MetadataPublishProfile {
                        lock_wait_nanos: publish_lock_wait_nanos,
                        logical_conflict_count: 1,
                        ..MetadataPublishProfile::default()
                    })?;
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

                let commit_seq_started = Instant::now();
                let commit_seq = inner.alloc_commit_seq()?;
                let commit_sequence_alloc_nanos = duration_nanos_u64(commit_seq_started.elapsed());
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
                let (shard_index, old_shard, new_shard, new_file_count) =
                    Self::update_keyspace_file_locked(
                        &mut inner,
                        &current_keyspace.shard_roots,
                        current_keyspace.file_count,
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
                next_keyspace.shard_roots[shard_index] = new_shard.shard_id;
                next_keyspace.file_count = new_file_count;
                inner.keyspace_heads.insert(keyspace_id, next_keyspace);
                inner.keyspace_commits.push(KeyspaceCommit {
                    commit_seq,
                    commit_group: commit_group.commit_group,
                    time: LogicalTime::from_raw(commit_seq.raw()),
                    keyspace_id,
                    shard_index: u32::try_from(shard_index).map_err(|_| {
                        StorageError::invalid_argument("keyspace shard index overflows u32")
                    })?,
                    old_shard,
                    new_shard: new_shard.shard_id,
                    old_file_count: current_keyspace.file_count,
                    new_file_count,
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
                if let Some((stream_id, writer_epoch)) = append_stream_commit {
                    inner
                        .file_writer_epochs
                        .insert((keyspace_id, file_id), writer_epoch);
                    if let Some(stream) = inner.append_streams.get_mut(&stream_id)
                        && stream.writer_epoch == writer_epoch
                    {
                        stream.published_through = new_size;
                    }
                }
                self.record_publish_profile(MetadataPublishProfile {
                    lock_wait_nanos: publish_lock_wait_nanos,
                    commit_sequence_alloc_nanos,
                    touched_shard_head_rows: 1,
                    commit_rows_written: 1,
                    ..MetadataPublishProfile::default()
                })?;
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
            shard_roots: source_head.shard_roots.clone(),
            file_count: source_head.file_count,
            latest_commit,
        };
        let checkpoint_root = Self::insert_keyspace_root_locked(
            &mut inner,
            head.shard_roots.clone(),
            head.file_count,
        )?;
        inner.keyspace_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(target),
            latest_commit,
            CheckpointRoots::NativeKeyspace(checkpoint_root.root_id),
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
        let (shard_roots, file_count) = match point {
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
                let root = Self::keyspace_root_locked(
                    &inner,
                    Self::checkpoint_keyspace_root(&checkpoint)?,
                )?;
                (root.shard_roots.to_vec(), root.file_count)
            }
            RestorePoint::Commit(_) | RestorePoint::Time(_) => {
                let target_commit =
                    Self::target_commit_for_keyspace_restore_locked(&inner, source, point)?;
                let root = Self::replay_keyspace_root_locked(&inner, source, target_commit, None)?;
                (root.shard_roots.to_vec(), root.file_count)
            }
        };
        let target = inner.alloc_keyspace_id();
        let latest_commit = inner.alloc_commit_seq()?;
        let head = KeyspaceHead {
            keyspace_id: target,
            generation: KeyspaceGeneration::from_raw(0),
            shard_roots,
            file_count,
            latest_commit,
        };
        let checkpoint_root = Self::insert_keyspace_root_locked(
            &mut inner,
            head.shard_roots.clone(),
            head.file_count,
        )?;
        inner.keyspace_heads.insert(target, head.clone());
        inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(target),
            latest_commit,
            CheckpointRoots::NativeKeyspace(checkpoint_root.root_id),
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
        let checkpoint_root = Self::insert_keyspace_root_locked(
            &mut inner,
            head.shard_roots.clone(),
            head.file_count,
        )?;
        Ok(inner.insert_checkpoint(
            MappingOwner::NativeKeyspace(keyspace_id),
            head.latest_commit,
            CheckpointRoots::NativeKeyspace(checkpoint_root.root_id),
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
