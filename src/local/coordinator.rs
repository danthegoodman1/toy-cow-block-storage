pub(super) type InMemoryAppendRunLogKey = (StorageNodeId, u64);
pub(super) type InMemoryAppendRunLogs = BTreeMap<InMemoryAppendRunLogKey, Vec<u8>>;
pub(super) type AppendStreamLaneMap = BTreeMap<AppendStreamId, Arc<Mutex<()>>>;

#[derive(Debug)]
struct BlockBatchShardEdits {
    old_root: MetadataNodeId,
    edits: Vec<BlockBatchShardEdit>,
}

#[derive(Debug)]
struct BlockBatchShardEdit {
    range: BlockRange,
    receipt: VerifiedSegmentReceipt,
}

/// In-process coordinator that owns request orchestration across metadata and
/// storage-node roles.
#[derive(Debug, Clone)]
pub struct LocalCoordinator {
    metadata: Arc<InMemoryMetadataPlane>,
    storage_nodes: StorageNodeRegistry,
    append_run_logs: Arc<Mutex<InMemoryAppendRunLogs>>,
    append_stream_lanes: Arc<Mutex<AppendStreamLaneMap>>,
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
            append_run_logs: Arc::new(Mutex::new(BTreeMap::new())),
            append_stream_lanes: Arc::new(Mutex::new(BTreeMap::new())),
            authority: Arc::new(LocalGrantReceiptAuthority),
            next_write_intent: Arc::new(Mutex::new(1)),
            next_extent_id: Arc::new(Mutex::new(1)),
            observability,
        })
    }

    fn from_durable_state(image: DurableStoreState) -> Result<Self> {
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
            append_run_logs: Arc::new(Mutex::new(BTreeMap::new())),
            append_stream_lanes: Arc::new(Mutex::new(BTreeMap::new())),
            authority: Arc::new(LocalGrantReceiptAuthority),
            next_write_intent: Arc::new(Mutex::new(image.next_write_intent)),
            next_extent_id: Arc::new(Mutex::new(image.next_extent_id)),
            observability,
        })
    }

    fn state_for_durable_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(
        DurableStoreState,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let mut metadata = self.metadata.state_inner()?;
        metadata.prune_append_streams_for_durable_export();

        let (storage_nodes, current_segments, new_segments) = self
            .storage_nodes
            .state_inner_for_persist(previous_segments)?;
        Ok((
            DurableStoreState {
                config: self.metadata.config,
                metadata,
                storage_nodes,
                next_write_intent: *lock(&self.next_write_intent)?,
                next_extent_id: *lock(&self.next_extent_id)?,
            },
            current_segments,
            new_segments,
        ))
    }

    fn state_for_durable_persist_through(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
        target_commit: CommitSeq,
        previous_cursor: Option<&DurableExportCursor>,
    ) -> Result<(
        DurableStoreState,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let mut metadata = self.metadata.state_inner()?;
        let current_commit = CommitSeq::from_raw(metadata.next_commit_seq.saturating_sub(1));
        if target_commit.raw() >= current_commit.raw() {
            return self.state_for_durable_persist(previous_segments);
        }

        metadata.prune_append_streams_for_durable_export();
        let previous_cursor =
            previous_cursor.filter(|cursor| cursor.next_gc_epoch == metadata.next_gc_epoch);
        let incremental_rows = previous_cursor.is_some();
        let metadata = Self::metadata_through_commit(metadata, target_commit, previous_cursor)?;
        let mut segment_ids = Self::durable_export_segment_ids(&metadata);
        if incremental_rows {
            segment_ids.extend(previous_segments.iter().copied());
        }
        let (storage_nodes, current_segments, new_segments) = self
            .storage_nodes
            .state_inner_for_segment_ids(&segment_ids, previous_segments)?;

        Ok((
            DurableStoreState {
                config: self.metadata.config,
                metadata,
                storage_nodes,
                next_write_intent: *lock(&self.next_write_intent)?,
                next_extent_id: *lock(&self.next_extent_id)?,
            },
            current_segments,
            new_segments,
        ))
    }

    fn metadata_through_commit(
        mut metadata: MetadataInner,
        target_commit: CommitSeq,
        previous_cursor: Option<&DurableExportCursor>,
    ) -> Result<MetadataInner> {
        let target_raw = target_commit.raw();
        let incremental_rows = previous_cursor.is_some();

        let original_live_heads = metadata.device_heads.clone();
        let original_deleted_heads = metadata.deleted_device_heads.clone();
        let original_keyspace_heads = metadata.keyspace_heads.clone();

        metadata
            .commit_groups
            .retain(|_, group| group.commit_seq.raw() <= target_raw);
        metadata
            .shard_commits
            .retain(|commit| commit.commit_seq.raw() <= target_raw);
        metadata
            .keyspace_commits
            .retain(|commit| commit.commit_seq.raw() <= target_raw);
        metadata
            .file_commits
            .retain(|commit| commit.commit_seq.raw() <= target_raw);
        metadata
            .fork_records
            .retain(|seq, _| seq.raw() <= target_raw);
        metadata
            .delete_records
            .retain(|seq, _| seq.raw() <= target_raw);
        metadata
            .checkpoints
            .retain(|_, checkpoint| checkpoint.commit_seq.raw() <= target_raw);

        let mut live_heads = BTreeMap::new();
        for (device_id, mut head) in original_live_heads {
            if head.latest_commit.raw() > target_raw {
                match InMemoryMetadataPlane::replay_device_roots_locked(
                    &metadata,
                    device_id,
                    target_commit,
                    None,
                ) {
                    Ok(roots) => {
                        head.shard_roots = roots;
                        head.latest_commit = Self::latest_device_commit_at_or_before(
                            &metadata,
                            device_id,
                            target_commit,
                        );
                    }
                    Err(StorageError::NotFound { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }
            live_heads.insert(device_id, head);
        }

        let mut deleted_heads = BTreeMap::new();
        for (device_id, mut head) in original_deleted_heads {
            if head.latest_commit.raw() <= target_raw {
                deleted_heads.insert(device_id, head);
                continue;
            }
            match InMemoryMetadataPlane::replay_device_roots_locked(
                &metadata,
                device_id,
                target_commit,
                None,
            ) {
                Ok(roots) => {
                    head.shard_roots = roots;
                    head.latest_commit = Self::latest_device_commit_at_or_before(
                        &metadata,
                        device_id,
                        target_commit,
                    );
                    live_heads.insert(device_id, head);
                }
                Err(StorageError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        metadata.device_heads = live_heads;
        metadata.deleted_device_heads = deleted_heads;

        let mut keyspace_heads = BTreeMap::new();
        for (keyspace_id, mut head) in original_keyspace_heads {
            if head.latest_commit.raw() > target_raw {
                match InMemoryMetadataPlane::replay_keyspace_root_locked(
                    &metadata,
                    keyspace_id,
                    target_commit,
                    None,
                ) {
                    Ok(root) => {
                        head.shard_roots = root.shard_roots.to_vec();
                        head.file_count = root.file_count;
                        head.latest_commit = Self::latest_keyspace_commit_at_or_before(
                            &metadata,
                            keyspace_id,
                            target_commit,
                        );
                    }
                    Err(StorageError::NotFound { .. }) => continue,
                    Err(error) => return Err(error),
                }
            }
            keyspace_heads.insert(keyspace_id, head);
        }
        metadata.keyspace_heads = keyspace_heads;

        if let Some(previous) = previous_cursor {
            metadata
                .commit_groups
                .retain(|id, _| id.raw() >= previous.next_commit_group_id);
            metadata
                .shard_commits
                .retain(|commit| commit.commit_seq.raw() >= previous.next_commit_seq);
            metadata
                .keyspace_commits
                .retain(|commit| commit.commit_seq.raw() >= previous.next_commit_seq);
            metadata
                .file_commits
                .retain(|commit| commit.commit_seq.raw() >= previous.next_commit_seq);
            metadata
                .fork_records
                .retain(|seq, _| seq.raw() >= previous.next_commit_seq);
            metadata
                .delete_records
                .retain(|seq, _| seq.raw() >= previous.next_commit_seq);
            metadata
                .checkpoints
                .retain(|id, _| id.raw() >= previous.next_checkpoint_id);
        }

        let mut retained = DurableExportRetention::from_previous_cursor(previous_cursor);

        for (device_id, head) in &metadata.device_heads {
            retained.devices.insert(*device_id);
            for root in &head.shard_roots {
                Self::collect_metadata_root_for_export(&metadata, *root, &mut retained)?;
            }
        }
        for (device_id, head) in &metadata.deleted_device_heads {
            retained.devices.insert(*device_id);
            for root in &head.shard_roots {
                Self::collect_metadata_root_for_export(&metadata, *root, &mut retained)?;
            }
        }
        for record in metadata.fork_records.values() {
            retained.devices.insert(record.source);
            retained.devices.insert(record.target);
            for root in &record.shard_roots {
                Self::collect_metadata_root_for_export(&metadata, *root, &mut retained)?;
            }
        }
        for record in metadata.delete_records.values() {
            retained.devices.insert(record.device_id);
            for root in &record.shard_roots {
                Self::collect_metadata_root_for_export(&metadata, *root, &mut retained)?;
            }
        }
        for commit in &metadata.shard_commits {
            retained.devices.insert(commit.device_id);
            Self::collect_metadata_root_for_export(&metadata, commit.old_root, &mut retained)?;
            Self::collect_metadata_root_for_export(&metadata, commit.new_root, &mut retained)?;
        }
        for head in metadata.keyspace_heads.values() {
            for shard_id in head.shard_roots.iter().copied() {
                Self::collect_keyspace_shard_for_export(
                    &metadata,
                    head.keyspace_id,
                    shard_id,
                    &mut retained,
                )?;
            }
        }
        for commit in &metadata.keyspace_commits {
            Self::collect_keyspace_shard_for_export(
                &metadata,
                commit.keyspace_id,
                commit.old_shard,
                &mut retained,
            )?;
            Self::collect_keyspace_shard_for_export(
                &metadata,
                commit.keyspace_id,
                commit.new_shard,
                &mut retained,
            )?;
        }
        for commit in &metadata.file_commits {
            retained.files.insert((commit.keyspace_id, commit.file_id));
            if let Some(old_root) = commit.old_root {
                Self::collect_metadata_root_for_export(&metadata, old_root, &mut retained)?;
            }
            Self::collect_metadata_root_for_export(&metadata, commit.new_root, &mut retained)?;
        }
        for group in metadata.commit_groups.values() {
            if let MappingOwner::BlockDevice(device_id) = group.owner {
                retained.devices.insert(device_id);
            }
            for update in &group.updates {
                match update {
                    RootUpdate::BlockShard(update) => {
                        Self::collect_metadata_root_for_export(
                            &metadata,
                            update.old_root,
                            &mut retained,
                        )?;
                        Self::collect_metadata_root_for_export(
                            &metadata,
                            update.new_root,
                            &mut retained,
                        )?;
                    }
                    RootUpdate::FileCreated {
                        file_id, new_root, ..
                    } => {
                        retained
                            .files
                            .insert((Self::keyspace_for_group(group.owner)?, *file_id));
                        Self::collect_metadata_root_for_export(
                            &metadata,
                            *new_root,
                            &mut retained,
                        )?;
                    }
                    RootUpdate::FileRoot {
                        file_id,
                        old_root,
                        new_root,
                        ..
                    } => {
                        retained
                            .files
                            .insert((Self::keyspace_for_group(group.owner)?, *file_id));
                        Self::collect_metadata_root_for_export(
                            &metadata,
                            *old_root,
                            &mut retained,
                        )?;
                        Self::collect_metadata_root_for_export(
                            &metadata,
                            *new_root,
                            &mut retained,
                        )?;
                    }
                }
            }
        }
        for checkpoint in metadata.checkpoints.values() {
            match &checkpoint.roots {
                CheckpointRoots::BlockShard(roots) => {
                    if let MappingOwner::BlockDevice(device_id) = checkpoint.owner {
                        retained.devices.insert(device_id);
                    }
                    for root in roots {
                        Self::collect_metadata_root_for_export(&metadata, *root, &mut retained)?;
                    }
                }
                CheckpointRoots::NativeKeyspace(root) => {
                    let MappingOwner::NativeKeyspace(keyspace_id) = checkpoint.owner else {
                        return Err(StorageError::corrupt(
                            "native keyspace checkpoint has non-keyspace owner",
                        ));
                    };
                    Self::collect_keyspace_root_for_export(
                        &metadata,
                        keyspace_id,
                        *root,
                        &mut retained,
                    )?;
                }
            }
        }
        for stream in metadata.append_streams.values() {
            retained.files.insert((stream.keyspace_id, stream.file_id));
        }

        metadata
            .device_specs
            .retain(|device_id, _| retained.devices.contains(device_id));
        metadata
            .keyspace_roots
            .retain(|root_id, _| retained.keyspace_roots.contains(root_id));
        metadata
            .keyspace_catalog_shards
            .retain(|shard_id, _| retained.keyspace_shards.contains(shard_id));
        metadata
            .metadata_nodes
            .retain(|node_id, _| retained.nodes.contains(node_id));
        if !incremental_rows {
            metadata
                .file_writer_epochs
                .retain(|key, _| retained.files.contains(key));
            metadata
                .metadata_last_mark_epoch
                .retain(|node_id, _| retained.nodes.contains(node_id));
            metadata
                .segment_last_mark_epoch
                .retain(|segment_id, _| retained.segments.contains(segment_id));
        }

        metadata.next_device_id = Self::next_u128_after_max(
            metadata
                .device_specs
                .keys()
                .chain(metadata.device_heads.keys())
                .chain(metadata.deleted_device_heads.keys())
                .map(|id| id.raw()),
        )?
        .max(previous_cursor.map_or(1, |cursor| cursor.next_device_id));
        metadata.next_keyspace_id =
            Self::next_u128_after_max(metadata.keyspace_heads.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_keyspace_id));
        metadata.next_file_id = Self::next_u128_after_max(
            metadata
                .file_writer_epochs
                .keys()
                .map(|(_, file_id)| file_id.raw()),
        )?
        .max(previous_cursor.map_or(1, |cursor| cursor.next_file_id));
        metadata.next_metadata_node_id =
            Self::next_u128_after_max(metadata.metadata_nodes.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_metadata_node_id));
        metadata.next_keyspace_root_id =
            Self::next_u128_after_max(metadata.keyspace_roots.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_keyspace_root_id));
        metadata.next_keyspace_catalog_shard_id =
            Self::next_u128_after_max(metadata.keyspace_catalog_shards.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_keyspace_catalog_shard_id));
        metadata.next_commit_group_id =
            Self::next_u128_after_max(metadata.commit_groups.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_commit_group_id));
        metadata.next_commit_seq = target_raw
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("durable target commit overflows"))?
            .max(previous_cursor.map_or(1, |cursor| cursor.next_commit_seq));
        metadata.next_checkpoint_id =
            Self::next_u128_after_max(metadata.checkpoints.keys().map(|id| id.raw()))?
                .max(previous_cursor.map_or(1, |cursor| cursor.next_checkpoint_id));
        Ok(metadata)
    }

    fn keyspace_for_group(owner: MappingOwner) -> Result<KeyspaceId> {
        match owner {
            MappingOwner::NativeKeyspace(keyspace_id) => Ok(keyspace_id),
            MappingOwner::BlockDevice(_) => Err(StorageError::corrupt(
                "block commit group contains native root update",
            )),
        }
    }

    fn latest_device_commit_at_or_before(
        metadata: &MetadataInner,
        device_id: DeviceId,
        target_commit: CommitSeq,
    ) -> CommitSeq {
        let target_raw = target_commit.raw();
        let max_commit = metadata
            .shard_commits
            .iter()
            .filter(|commit| commit.device_id == device_id && commit.commit_seq.raw() <= target_raw)
            .map(|commit| commit.commit_seq.raw())
            .chain(
                metadata
                    .fork_records
                    .values()
                    .filter(|record| {
                        record.target == device_id && record.commit_seq.raw() <= target_raw
                    })
                    .map(|record| record.commit_seq.raw()),
            )
            .chain(
                metadata
                    .delete_records
                    .values()
                    .filter(|record| {
                        record.device_id == device_id && record.commit_seq.raw() <= target_raw
                    })
                    .map(|record| record.commit_seq.raw()),
            )
            .chain(metadata.checkpoints.values().filter_map(|checkpoint| {
                (checkpoint.owner == MappingOwner::BlockDevice(device_id)
                    && checkpoint.commit_seq.raw() <= target_raw)
                    .then_some(checkpoint.commit_seq.raw())
            }))
            .max()
            .unwrap_or(0);
        CommitSeq::from_raw(max_commit)
    }

    fn latest_keyspace_commit_at_or_before(
        metadata: &MetadataInner,
        keyspace_id: KeyspaceId,
        target_commit: CommitSeq,
    ) -> CommitSeq {
        let target_raw = target_commit.raw();
        let max_commit = metadata
            .keyspace_commits
            .iter()
            .filter(|commit| {
                commit.keyspace_id == keyspace_id && commit.commit_seq.raw() <= target_raw
            })
            .map(|commit| commit.commit_seq.raw())
            .chain(metadata.checkpoints.values().filter_map(|checkpoint| {
                (checkpoint.owner == MappingOwner::NativeKeyspace(keyspace_id)
                    && checkpoint.commit_seq.raw() <= target_raw)
                    .then_some(checkpoint.commit_seq.raw())
            }))
            .max()
            .unwrap_or(0);
        CommitSeq::from_raw(max_commit)
    }

    fn collect_metadata_root_for_export(
        metadata: &MetadataInner,
        root: MetadataNodeId,
        retained: &mut DurableExportRetention,
    ) -> Result<()> {
        if !retained.should_collect_metadata_node(root) {
            return Ok(());
        }
        if !retained.nodes.insert(root) {
            return Ok(());
        }
        let node = metadata
            .metadata_nodes
            .get(&root)
            .ok_or_else(|| StorageError::not_found("metadata_node", root.to_string()))?;
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    Self::collect_metadata_root_for_export(metadata, child.node_id, retained)?;
                }
            }
            MetadataNodeKind::Leaf { entries, .. } => {
                retained
                    .segments
                    .extend(entries.iter().map(|entry| entry.segment_id));
            }
        }
        Ok(())
    }

    fn collect_keyspace_root_for_export(
        metadata: &MetadataInner,
        keyspace_id: KeyspaceId,
        root_id: KeyspaceRootId,
        retained: &mut DurableExportRetention,
    ) -> Result<()> {
        if !retained.should_collect_keyspace_root(root_id) {
            return Ok(());
        }
        retained.keyspace_roots.insert(root_id);
        let root = metadata
            .keyspace_roots
            .get(&root_id)
            .ok_or_else(|| StorageError::not_found("keyspace_root", root_id.to_string()))?;
        for shard_id in root.shard_roots.iter().copied() {
            Self::collect_keyspace_shard_for_export(metadata, keyspace_id, shard_id, retained)?;
        }
        Ok(())
    }

    fn collect_keyspace_shard_for_export(
        metadata: &MetadataInner,
        keyspace_id: KeyspaceId,
        shard_id: KeyspaceCatalogShardId,
        retained: &mut DurableExportRetention,
    ) -> Result<()> {
        if !retained.should_collect_keyspace_shard(shard_id) {
            return Ok(());
        }
        retained.keyspace_shards.insert(shard_id);
        let shard = metadata
            .keyspace_catalog_shards
            .get(&shard_id)
            .ok_or_else(|| {
                StorageError::not_found("keyspace_catalog_shard", shard_id.to_string())
            })?;
        for (file_id, entry) in &shard.files {
            retained.files.insert((keyspace_id, *file_id));
            Self::collect_metadata_root_for_export(metadata, entry.head.root, retained)?;
        }
        Ok(())
    }

    fn durable_export_segment_ids(metadata: &MetadataInner) -> BTreeSet<SegmentId> {
        metadata_referenced_segments(metadata)
    }

    fn next_u128_after_max(values: impl Iterator<Item = u128>) -> Result<u128> {
        values
            .max()
            .map(|max| {
                max.checked_add(1)
                    .ok_or_else(|| StorageError::conflict("durable cursor id overflow"))
            })
            .unwrap_or(Ok(1))
    }

    fn state_for_segment_ids(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<(SelectedStorageNodeState, Vec<DurableSegmentPayload>)> {
        self.storage_nodes.state_for_segment_ids(segment_ids)
    }

    fn selected_state_for_segment_ids(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<SelectedStorageNodeState> {
        self.storage_nodes
            .selected_state_for_segment_ids(segment_ids)
    }

    fn durable_export_cursor(&self) -> Result<DurableExportCursor> {
        let metadata = lock(&self.metadata.inner)?;
        Ok(DurableExportCursor {
            config: self.metadata.config,
            next_device_id: metadata.next_device_id,
            next_keyspace_id: metadata.next_keyspace_id,
            next_file_id: metadata.next_file_id,
            next_metadata_node_id: metadata.next_metadata_node_id,
            next_keyspace_root_id: metadata.next_keyspace_root_id,
            next_keyspace_catalog_shard_id: metadata.next_keyspace_catalog_shard_id,
            next_commit_group_id: metadata.next_commit_group_id,
            next_commit_seq: metadata.next_commit_seq,
            next_checkpoint_id: metadata.next_checkpoint_id,
            next_gc_epoch: metadata.next_gc_epoch,
            next_write_intent: *lock(&self.next_write_intent)?,
            next_extent_id: *lock(&self.next_extent_id)?,
            next_segment_id: u128::from(self.storage_nodes.next_segment_id.load(Ordering::Relaxed)),
            next_placement_index: *lock(&self.storage_nodes.next_placement_index)?,
        })
    }

    fn native_append_publish_delta_through(
        &self,
        stream: &AppendStream,
        target_commit: CommitSeq,
        previous: &DurableExportCursor,
    ) -> Result<Option<NativeMetadataDelta>> {
        self.native_metadata_delta_through_inner(target_commit, previous, Some(stream))
    }

    fn native_metadata_delta_through(
        &self,
        target_commit: CommitSeq,
        previous: &DurableExportCursor,
    ) -> Result<Option<NativeMetadataDelta>> {
        self.native_metadata_delta_through_inner(target_commit, previous, None)
    }

    fn native_metadata_delta_through_inner(
        &self,
        target_commit: CommitSeq,
        previous: &DurableExportCursor,
        append_stream: Option<&AppendStream>,
    ) -> Result<Option<NativeMetadataDelta>> {
        fn in_range(seq: CommitSeq, previous: &DurableExportCursor, target: CommitSeq) -> bool {
            seq.raw() >= previous.next_commit_seq && seq.raw() <= target.raw()
        }

        fn next_after_u128(current: u128, raw: u128) -> Result<u128> {
            Ok(current.max(
                raw.checked_add(1)
                    .ok_or_else(|| StorageError::conflict("durable cursor id overflow"))?,
            ))
        }

        let metadata = lock(&self.metadata.inner)?;
        if let Some(stream) = append_stream {
            let stream_state = metadata
                .append_streams
                .get(&stream.stream_id)
                .ok_or_else(|| StorageError::conflict("stale append stream"))?;
            stream_state.validate_token(stream)?;
        } else if !metadata.append_streams.is_empty() {
            return Ok(None);
        }
        if previous.next_gc_epoch != metadata.next_gc_epoch {
            return Ok(None);
        }
        if metadata
            .shard_commits
            .iter()
            .any(|commit| in_range(commit.commit_seq, previous, target_commit))
            || metadata
                .fork_records
                .keys()
                .any(|commit| in_range(*commit, previous, target_commit))
            || metadata
                .delete_records
                .keys()
                .any(|commit| in_range(*commit, previous, target_commit))
            || metadata
                .checkpoints
                .values()
                .any(|checkpoint| in_range(checkpoint.commit_seq, previous, target_commit))
        {
            return Ok(None);
        }

        let commit_groups: BTreeMap<_, _> = metadata
            .commit_groups
            .iter()
            .filter(|(_, group)| in_range(group.commit_seq, previous, target_commit))
            .map(|(id, group)| (*id, group.clone()))
            .collect();
        if commit_groups.is_empty() {
            return Ok(None);
        }
        if commit_groups.values().any(|group| {
            !matches!(group.owner, MappingOwner::NativeKeyspace(_))
                || group.updates.iter().any(|update| {
                    !matches!(
                        update,
                        RootUpdate::FileRoot {
                            old_root: _,
                            new_root: _,
                            ..
                        }
                    )
                })
        }) {
            return Ok(None);
        }

        let keyspace_commits: Vec<_> = metadata
            .keyspace_commits
            .iter()
            .filter(|commit| in_range(commit.commit_seq, previous, target_commit))
            .cloned()
            .collect();
        let file_commits: Vec<_> = metadata
            .file_commits
            .iter()
            .filter(|commit| in_range(commit.commit_seq, previous, target_commit))
            .cloned()
            .collect();
        if file_commits.is_empty() || file_commits.iter().any(|commit| commit.old_root.is_none()) {
            return Ok(None);
        }
        if let Some(stream) = append_stream
            && !file_commits.iter().any(|commit| {
                commit.keyspace_id == stream.keyspace_id && commit.file_id == stream.file_id
            })
        {
            return Ok(None);
        }

        let mut keyspace_heads = BTreeMap::new();
        let keyspace_roots: BTreeMap<KeyspaceRootId, KeyspaceRoot> = BTreeMap::new();
        let mut keyspace_catalog_shards = BTreeMap::new();
        let mut metadata_nodes = BTreeMap::new();
        let mut referenced_segment_ids = BTreeSet::new();
        let mut file_writer_epochs = Vec::new();

        let touched_keyspaces: BTreeSet<_> = keyspace_commits
            .iter()
            .map(|commit| commit.keyspace_id)
            .chain(file_commits.iter().map(|commit| commit.keyspace_id))
            .collect();
        for keyspace_id in touched_keyspaces {
            let mut head = metadata
                .keyspace_heads
                .get(&keyspace_id)
                .cloned()
                .ok_or_else(|| StorageError::not_found("keyspace", keyspace_id.to_string()))?;
            if head.latest_commit.raw() > target_commit.raw() {
                let root = InMemoryMetadataPlane::replay_keyspace_root_locked(
                    &metadata,
                    keyspace_id,
                    target_commit,
                    None,
                )?;
                head.shard_roots = root.shard_roots.to_vec();
                head.file_count = root.file_count;
                head.latest_commit = Self::latest_keyspace_commit_at_or_before(
                    &metadata,
                    keyspace_id,
                    target_commit,
                );
            }
            keyspace_heads.insert(keyspace_id, head.clone());
            for shard_id in head.shard_roots.iter().copied() {
                Self::collect_native_delta_keyspace_shard(
                    &metadata,
                    shard_id,
                    previous,
                    &mut keyspace_catalog_shards,
                )?;
            }
        }

        let touched_files: BTreeSet<_> = file_commits
            .iter()
            .map(|commit| (commit.keyspace_id, commit.file_id))
            .collect();
        let mut published_sizes_by_file: BTreeMap<(KeyspaceId, FileId), BTreeSet<u64>> =
            BTreeMap::new();
        for commit in &file_commits {
            published_sizes_by_file
                .entry((commit.keyspace_id, commit.file_id))
                .or_default()
                .insert(commit.new_size);
        }
        let append_streams = if append_stream.is_some() {
            metadata
                .append_streams
                .values()
                .filter(|stream| {
                    published_sizes_by_file
                        .get(&(stream.keyspace_id, stream.file_id))
                        .is_some_and(|sizes| sizes.contains(&stream.published_through))
                })
                .map(|stream| stream.durable_export_at(stream.durable_through))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        for key in &touched_files {
            if let Some(epoch) = metadata.file_writer_epochs.get(key).copied() {
                file_writer_epochs.push((*key, epoch));
            }
        }

        for commit in &file_commits {
            Self::collect_native_delta_metadata_root(
                &metadata,
                commit.new_root,
                previous,
                &mut metadata_nodes,
                &mut referenced_segment_ids,
            )?;
        }

        let mut cursor = previous.clone();
        cursor.config = self.metadata.config;
        for id in keyspace_roots.keys() {
            cursor.next_keyspace_root_id = next_after_u128(cursor.next_keyspace_root_id, id.raw())?;
        }
        for id in keyspace_catalog_shards.keys() {
            cursor.next_keyspace_catalog_shard_id =
                next_after_u128(cursor.next_keyspace_catalog_shard_id, id.raw())?;
        }
        for id in metadata_nodes.keys() {
            cursor.next_metadata_node_id = next_after_u128(cursor.next_metadata_node_id, id.raw())?;
        }
        for id in commit_groups.keys() {
            cursor.next_commit_group_id = next_after_u128(cursor.next_commit_group_id, id.raw())?;
        }
        cursor.next_commit_seq = target_commit
            .raw()
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("durable cursor commit overflow"))?
            .max(cursor.next_commit_seq);
        drop(metadata);
        cursor.next_write_intent = *lock(&self.next_write_intent)?;
        cursor.next_extent_id = *lock(&self.next_extent_id)?;
        cursor.next_segment_id =
            u128::from(self.storage_nodes.next_segment_id.load(Ordering::Relaxed));
        cursor.next_placement_index = *lock(&self.storage_nodes.next_placement_index)?;

        Ok(Some(NativeMetadataDelta {
            cursor,
            keyspace_heads,
            keyspace_roots,
            keyspace_catalog_shards,
            file_writer_epochs,
            append_streams,
            metadata_nodes,
            referenced_segment_ids,
            commit_groups,
            keyspace_commits,
            file_commits,
        }))
    }

    fn collect_native_delta_keyspace_shard(
        metadata: &MetadataInner,
        shard_id: KeyspaceCatalogShardId,
        previous: &DurableExportCursor,
        keyspace_catalog_shards: &mut BTreeMap<KeyspaceCatalogShardId, KeyspaceCatalogShard>,
    ) -> Result<()> {
        if shard_id.raw() < previous.next_keyspace_catalog_shard_id {
            return Ok(());
        }
        let shard = InMemoryMetadataPlane::keyspace_catalog_shard_locked(metadata, shard_id)?;
        keyspace_catalog_shards.insert(shard_id, shard);
        Ok(())
    }

    fn collect_native_delta_metadata_root(
        metadata: &MetadataInner,
        root: MetadataNodeId,
        previous: &DurableExportCursor,
        metadata_nodes: &mut BTreeMap<MetadataNodeId, MetadataNode>,
        referenced_segment_ids: &mut BTreeSet<SegmentId>,
    ) -> Result<()> {
        if root.raw() < previous.next_metadata_node_id || metadata_nodes.contains_key(&root) {
            return Ok(());
        }
        let node = metadata
            .metadata_nodes
            .get(&root)
            .cloned()
            .ok_or_else(|| StorageError::not_found("metadata_node", root.to_string()))?;
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    Self::collect_native_delta_metadata_root(
                        metadata,
                        child.node_id,
                        previous,
                        metadata_nodes,
                        referenced_segment_ids,
                    )?;
                }
            }
            MetadataNodeKind::Leaf { entries, .. } => {
                referenced_segment_ids.extend(entries.iter().map(|entry| entry.segment_id));
            }
        }
        metadata_nodes.insert(root, node);
        Ok(())
    }

    fn verify_segment_payloads_for_read(
        &self,
        segment_ids: BTreeSet<SegmentId>,
        verification: ReadVerification,
    ) -> Result<()> {
        for segment_id in segment_ids {
            self.storage_nodes
                .verify_segment_payload_for_read(segment_id, verification)?;
        }
        Ok(())
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
        self.write_device_with_integrity(
            device_id,
            offset,
            data,
            durability,
            PayloadIntegrity::Verified,
        )
    }

    pub fn write_device_with_integrity(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
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
        let commit = self.commit_block_batch(
            device_id,
            &[BlockBatchWrite {
                offset,
                bytes: data.to_vec(),
                payload_integrity,
            }],
            durability,
        )?;
        Ok(WriteCommit {
            device_id: commit.device_id,
            commit_seq: commit.commit_seq,
            range,
            durability: commit.durability,
        })
    }

    pub fn commit_block_batch(
        &self,
        device_id: DeviceId,
        writes: &[BlockBatchWrite],
        durability: crate::api::WriteDurability,
    ) -> Result<BlockBatchCommit> {
        let info = self.metadata.device_info(device_id)?;
        let collapsed = collapse_block_batch_writes(writes, &info.spec, DEFAULT_BLOCK_BATCH_MAX_BYTES)?;
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
        let owner = MappingOwner::BlockDevice(device_id);
        let write_intent = self.next_write_intent()?;
        let current = self.metadata.get_head(device_id)?;
        let mut shard_edits = BTreeMap::<ShardId, BlockBatchShardEdits>::new();
        let mut segment_receipts = Vec::new();

        for write in &collapsed {
            let write_range = write.byte_range()?;
            let chunks = self.split_device_range(&info, write_range)?;
            for chunk in chunks {
                let chunk_range = block_range_to_byte_range(chunk.range, block_size)?;
                let chunk_offset = chunk_range
                    .offset
                    .checked_sub(write.offset)
                    .ok_or_else(|| StorageError::invalid_argument("write chunk underflows"))?;
                let byte_start = usize::try_from(chunk_offset).map_err(|_| {
                    StorageError::invalid_argument("write chunk offset overflows usize")
                })?;
                let byte_len = usize::try_from(chunk_range.len).map_err(|_| {
                    StorageError::invalid_argument("write chunk length overflows usize")
                })?;
                let byte_end = byte_start
                    .checked_add(byte_len)
                    .ok_or_else(|| StorageError::invalid_argument("write chunk end overflows"))?;
                let chunk_bytes = write.bytes.get(byte_start..byte_end).ok_or_else(|| {
                    StorageError::corrupt("write chunk is outside collapsed batch bytes")
                })?;
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
                    write.payload_integrity,
                )?;
                let shard_edit = shard_edits.entry(chunk.shard_id).or_insert_with(|| {
                    BlockBatchShardEdits {
                        old_root: chunk.old_root,
                        edits: Vec::new(),
                    }
                });
                if shard_edit.old_root != chunk.old_root {
                    return Err(StorageError::conflict(
                        "block batch observed inconsistent shard roots",
                    ));
                }
                shard_edit.edits.push(BlockBatchShardEdit {
                    range: chunk.range,
                    receipt: verified_receipt.clone(),
                });
                segment_receipts.push(verified_receipt);
            }
        }

        let mut updates = Vec::with_capacity(shard_edits.len());
        for (shard_id, mut shard) in shard_edits {
            shard.edits.sort_by_key(|edit| edit.range.start.raw());
            let mut root = shard.old_root;
            for edit in shard.edits {
                root = self
                    .replace_tree_range_with_receipts(
                        root,
                        TreeRangeEdit {
                            range: edit.range,
                            replacement: Some(SegmentReplacement {
                                segment_id: edit.receipt.descriptor.segment_id,
                                segment_base: edit.range.start,
                            }),
                        },
                        &segment_receipts,
                    )?
                    .root;
            }
            if root != shard.old_root {
                updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                    shard_id,
                    old_root: shard.old_root,
                    new_root: root,
                }));
            }
        }
        if updates.is_empty() {
            return Ok(BlockBatchCommit {
                device_id,
                commit_seq: info.latest_commit,
                write_count: usize_to_u64(writes.len()),
                collapsed_range_count: usize_to_u64(collapsed.len()),
                committed_bytes: 0,
                durability,
            });
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

        let committed_bytes = collapsed
            .iter()
            .map(|write| usize_to_u64(write.bytes.len()))
            .fold(0u64, u64::saturating_add);
        Ok(BlockBatchCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            write_count: usize_to_u64(writes.len()),
            collapsed_range_count: usize_to_u64(collapsed.len()),
            committed_bytes,
            durability,
        })
    }

    pub fn write_zeroes(&self, device_id: DeviceId, offset: u64, len: u64) -> Result<WriteCommit> {
        let zeroes = usize::try_from(len)
            .map_err(|_| StorageError::invalid_argument("zero range length overflows usize"))?;
        let bytes = vec![0; zeroes];
        self.write_device(
            device_id,
            offset,
            &bytes,
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

    pub fn open_append_stream(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
    ) -> Result<AppendStream> {
        self.metadata.open_append_stream(keyspace_id, file_id)
    }

    pub fn append_stream(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: crate::api::WriteDurability,
    ) -> Result<AppendTicket> {
        self.append_stream_with_integrity(stream, data, durability, PayloadIntegrity::Verified)
    }

    fn stream_append_lane(&self, stream_id: AppendStreamId) -> Result<Arc<Mutex<()>>> {
        let mut lanes = lock(&self.append_stream_lanes)?;
        Ok(Arc::clone(
            lanes
                .entry(stream_id)
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }

    pub fn append_stream_with_integrity(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        if data.is_empty() {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        let lane = self.stream_append_lane(stream.stream_id)?;
        let _lane_guard = lock(&lane)?;
        let prepared =
            self.prepare_append_stream_run(stream, data.len(), WriteDurability::Acknowledged)?;
        let run = self.append_run_payload_memory(&prepared, data, payload_integrity)?;
        let ticket = self.commit_prepared_append_stream_run(prepared, run)?;
        if matches!(durability, crate::api::WriteDurability::Flushed) {
            self.flush_append_stream(stream)?;
        }
        Ok(ticket)
    }

    fn prepare_append_stream_run(
        &self,
        stream: &AppendStream,
        data_len: usize,
        durability: crate::api::WriteDurability,
    ) -> Result<PreparedAppendStreamRun> {
        if data_len == 0 {
            return Err(StorageError::invalid_argument(
                "append payload must not be empty",
            ));
        }
        if matches!(durability, crate::api::WriteDurability::Flushed) {
            return Err(StorageError::invalid_argument(
                "append stream flushed durability is handled by DurableCoordinator",
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
        let data_len_u64 = u64::try_from(data_len)
            .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?;
        let ticket_id = self.metadata.next_append_ticket_id()?;
        let range = self
            .metadata
            .reserve_append_stream_range(stream, data_len_u64)?;
        let candidates = self.storage_nodes.storage_node_ids()?;
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        Ok(PreparedAppendStreamRun {
            stream: stream.clone(),
            ticket_id,
            range,
            storage_node,
            run_id: AppendRunId::from_raw(ticket_id.raw()),
        })
    }

    fn append_run_log_id(stream_id: AppendStreamId) -> u64 {
        stream_id.raw() as u64
    }

    fn append_run_payload_memory(
        &self,
        prepared: &PreparedAppendStreamRun,
        data: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendLogRun> {
        let payload_len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("append byte length overflows u64"))?;
        if payload_len != prepared.range.len {
            return Err(StorageError::invalid_argument(
                "append payload length disagrees with prepared stream range",
            ));
        }
        let log_id = Self::append_run_log_id(prepared.stream.stream_id);
        let mut logs = lock(&self.append_run_logs)?;
        let log = logs.entry((prepared.storage_node, log_id)).or_default();
        let log_payload_offset = usize_to_u64(log.len());
        log.extend_from_slice(data);
        let run = AppendLogRun {
            run_id: prepared.run_id,
            storage_node: prepared.storage_node,
            stream_id: prepared.stream.stream_id,
            writer_epoch: prepared.stream.writer_epoch,
            keyspace_id: prepared.stream.keyspace_id,
            file_id: prepared.stream.file_id,
            file_offset_start: prepared.range.offset,
            payload_len,
            log_id,
            log_payload_offset,
            log_record_bytes: payload_len,
            integrity: segment_payload_integrity(payload_integrity, data),
        };
        run.validate()?;
        Ok(run)
    }

    fn commit_prepared_append_stream_run(
        &self,
        prepared: PreparedAppendStreamRun,
        run: AppendLogRun,
    ) -> Result<AppendTicket> {
        if run.run_id != prepared.run_id
            || run.storage_node != prepared.storage_node
            || run.stream_id != prepared.stream.stream_id
            || run.writer_epoch != prepared.stream.writer_epoch
            || run.keyspace_id != prepared.stream.keyspace_id
            || run.file_id != prepared.stream.file_id
            || run.file_offset_start != prepared.range.offset
            || run.payload_len != prepared.range.len
        {
            return Err(StorageError::corrupt(
                "append run manifest disagrees with prepared stream range",
            ));
        }
        self.metadata.append_stream_run_record(
            &prepared.stream,
            prepared.ticket_id,
            prepared.range,
            run,
        )
    }

    pub fn flush_append_stream(&self, stream: &AppendStream) -> Result<DurableAppendMark> {
        self.metadata.mark_append_stream_durable(stream)
    }

    pub fn publish_append_stream(
        &self,
        stream: &AppendStream,
        mark: &DurableAppendMark,
        durability: crate::api::WriteDurability,
    ) -> Result<AppendPublishCommit> {
        let state = self.metadata.validate_append_stream_mark(stream, mark)?;
        let head = self
            .metadata
            .get_file_head(stream.keyspace_id, stream.file_id)?;
        if head.size != state.published_through {
            return Err(StorageError::conflict(
                "append stream no longer matches visible file head",
            ));
        }
        if mark.durable_through == state.published_through {
            return Ok(AppendPublishCommit {
                keyspace_id: stream.keyspace_id,
                file_id: stream.file_id,
                range: ByteRange::new(mark.durable_through, 0),
                version: head.version,
                commit_seq: head.latest_commit,
                durability,
            });
        }

        let unpublished: Vec<_> = state
            .records
            .iter()
            .filter(|record| {
                record.offset >= state.published_through
                    && record.offset < mark.durable_through
                    && record.offset.saturating_add(record.len) <= mark.durable_through
            })
            .cloned()
            .collect();
        if unpublished.is_empty() {
            return Err(StorageError::conflict(
                "append stream mark has no durable records to publish",
            ));
        }

        let mut contiguous = true;
        let mut expected_offset = head.size;
        for record in &unpublished {
            contiguous &= record.offset == expected_offset;
            expected_offset = record
                .offset
                .checked_add(record.len)
                .ok_or_else(|| StorageError::invalid_argument("append record end overflows"))?;
        }
        contiguous &= expected_offset == mark.durable_through;
        if !contiguous {
            return Err(StorageError::conflict(
                "append-run publish requires contiguous durable records",
            ));
        }
        let run_extents: Vec<_> = unpublished
            .iter()
            .map(|record| {
                let extent = RunBackedFileExtent {
                    file_offset_start: record.offset,
                    payload_len: record.len,
                    run: record.run.full_range(),
                };
                extent.validate()?;
                Ok(extent)
            })
            .collect::<Result<_>>()?;
        let publish_range = ByteRange::new(head.size, mark.durable_through - head.size);
        let new_root = self
            .replace_tree_byte_range_with_run_extents(head.root, publish_range, run_extents)?
            .root;

        self.publish_commit_group_observed(CommitGroupIntent {
            owner: MappingOwner::NativeKeyspace(stream.keyspace_id),
            fence: MetadataFence::AppendStream {
                stream_id: stream.stream_id,
                writer_epoch: stream.writer_epoch,
            },
            updates: vec![RootUpdate::FileRoot {
                file_id: stream.file_id,
                old_root: head.root,
                new_root,
                new_size: mark.durable_through,
            }],
        })?;
        self.metadata
            .mark_append_stream_published(stream, mark.durable_through)?;
        let committed = self
            .metadata
            .get_file_head(stream.keyspace_id, stream.file_id)?;

        Ok(AppendPublishCommit {
            keyspace_id: stream.keyspace_id,
            file_id: stream.file_id,
            range: ByteRange::new(head.size, mark.durable_through - head.size),
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability,
        })
    }

    pub fn abort_append_stream(&self, stream: &AppendStream) -> Result<()> {
        self.metadata.abort_append_stream(stream)
    }

    pub fn commit_file_batch(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: &[FileBatchWrite],
        durability: crate::api::WriteDurability,
    ) -> Result<FileWriteCommit> {
        self.commit_file_batch_with_integrity(
            keyspace_id,
            file_id,
            writes,
            durability,
            PayloadIntegrity::Verified,
        )
    }

    pub fn commit_file_batch_with_integrity(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: &[FileBatchWrite],
        durability: crate::api::WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit> {
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let (collapsed, range, new_size) = collapse_native_file_batch_writes(
            writes,
            head.size,
            DEFAULT_NATIVE_FILE_BATCH_MAX_BYTES,
        )?;

        if collapsed.is_empty() {
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

        let block_size = u64::from(self.metadata.config.block_size);
        let root = self.metadata.get_metadata_node(head.root)?;
        let groups = native_batch_segment_groups(&collapsed, block_size)?;
        let mut edits = Vec::with_capacity(groups.len());
        for group in groups {
            let first_block = group.start / block_size;
            let segment_len = group.end.checked_sub(group.start).ok_or_else(|| {
                StorageError::invalid_argument("native batch segment range underflows")
            })?;
            let segment_blocks = blocks_for_bytes(segment_len, block_size)?;
            let write_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(first_block),
                BlockCount::from_raw(segment_blocks),
            );
            if !root.covered_range.contains_range(write_range)? {
                return Err(StorageError::invalid_argument(
                    "native file batch exceeds file root coverage",
                ));
            }
            let segment_len_usize = usize::try_from(segment_len).map_err(|_| {
                StorageError::invalid_argument("native batch segment length overflows usize")
            })?;
            let mut segment_bytes = vec![0; segment_len_usize];
            if group.start < head.size {
                self.read_metadata_node(&root, write_range, block_size, &mut segment_bytes)?;
                let preserved_len = head.size.saturating_sub(group.start).min(segment_len);
                let preserved_range = ByteRange::new(group.start, preserved_len);
                let run_extents =
                    self.run_extents_for_file_read(keyspace_id, file_id, preserved_range)?;
                self.read_append_run_extents_from_memory(
                    &run_extents,
                    ByteRange::new(group.start, segment_len),
                    &mut segment_bytes,
                    ReadVerification::Default,
                )?;
            }
            overlay_native_batch_writes(
                group.start,
                &collapsed[group.first_write..group.last_write],
                &mut segment_bytes,
            )?;
            let segment_range = ByteRange::new(group.start, segment_len);
            let verified_receipt = self.write_segment_for_intent_with_id_owned_verified(
                WriteGrantIntent::NativeWrite {
                    keyspace_id,
                    file_id,
                    range: segment_range,
                    base_version: head.version,
                },
                self.next_write_intent()?,
                segment_bytes,
                durability,
                payload_integrity,
            )?;
            edits.push(NativeFileReceiptEdit {
                range: write_range,
                receipt: verified_receipt,
            });
        }

        self.publish_native_file_receipt_edits(NativeFileReceiptPublish {
            keyspace_id,
            file_id,
            base_version: head.version,
            committed_range: range,
            new_size,
            edits,
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
        self.retain_reachable_append_run_logs()?;
        for segment_id in &sweep.released_segments {
            if self.storage_nodes.state(*segment_id)? == SegmentLifecycleState::Referenced {
                self.storage_nodes.release_segment(*segment_id)?;
            }
        }
        Ok(sweep)
    }

    fn retain_reachable_append_run_logs(&self) -> Result<()> {
        let metadata = self.metadata.state_inner()?;
        let mut retained = BTreeSet::new();
        for node in metadata.metadata_nodes.values() {
            Self::collect_append_run_log_refs_from_node(node, &mut retained);
        }
        for stream in metadata.append_streams.values() {
            if stream.status != AppendStreamStatus::Active {
                continue;
            }
            for record in &stream.records {
                retained.insert((record.run.storage_node, record.run.log_id));
            }
        }
        lock(&self.append_run_logs)?.retain(|key, _| retained.contains(key));
        Ok(())
    }

    fn collect_append_run_log_refs_from_node(
        node: &MetadataNode,
        out: &mut BTreeSet<(StorageNodeId, u64)>,
    ) {
        if let MetadataNodeKind::Leaf { run_extents, .. } = &node.kind {
            for extent in run_extents {
                out.insert((extent.run.storage_node, extent.run.log_id));
            }
        }
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
            PayloadIntegrity::Verified,
        )
    }

    #[cfg(test)]
    fn write_segment_for_intent_with_id(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<SegmentWriteReceipt> {
        self.write_segment_for_intent_with_id_owned(
            intent,
            write_intent,
            data.to_vec(),
            durability,
            payload_integrity,
        )
    }

    #[cfg(test)]
    fn write_segment_for_intent_with_id_owned(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: Vec<u8>,
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<SegmentWriteReceipt> {
        Ok(self
            .write_segment_for_intent_with_id_owned_verified(
                intent,
                write_intent,
                data,
                durability,
                payload_integrity,
            )?
            .receipt)
    }

    fn write_segment_for_intent_with_id_owned_verified(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: Vec<u8>,
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
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
            payload_integrity,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })?;
        self.write_granted_segment_verified(&grant, data)
    }

    fn write_segment_for_intent_with_id_owned_verified_profiled(
        &self,
        intent: WriteGrantIntent,
        write_intent: WriteIntentId,
        data: Vec<u8>,
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<(VerifiedSegmentReceipt, LocalSegmentWriteProfile)> {
        let mut profile = LocalSegmentWriteProfile::default();
        let max_bytes = u64::try_from(data.len()).map_err(|_| {
            StorageError::invalid_argument("segment reservation byte length overflows u64")
        })?;

        let started = Instant::now();
        let candidates = self.storage_nodes.storage_node_ids()?;
        profile.storage_node_ids_nanos = duration_nanos_u64(started.elapsed());

        let started = Instant::now();
        let storage_node = PlacementPolicy::choose_storage_node(&self.storage_nodes, &candidates)?;
        profile.placement_select_nanos = duration_nanos_u64(started.elapsed());

        let started = Instant::now();
        let segment_id = self.storage_nodes.allocate_segment_id()?;
        profile.segment_id_alloc_nanos = duration_nanos_u64(started.elapsed());

        let started = Instant::now();
        let grant = self.issue_write_grant(WriteGrantRequest {
            tenant: LOCAL_TENANT_ID,
            principal: LOCAL_PRINCIPAL_ID,
            intent,
            write_intent,
            segment_id,
            storage_node,
            max_bytes,
            payload_integrity,
            durability,
            expires_at: LOCAL_GRANT_EXPIRATION,
        })?;
        profile.grant_issue_nanos = duration_nanos_u64(started.elapsed());

        let (receipt, write_profile) =
            self.write_granted_segment_verified_profiled(&grant, data)?;
        profile.absorb(write_profile);
        Ok((receipt, profile))
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
            payload_integrity: PayloadIntegrity::Verified,
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
            payload_integrity: PayloadIntegrity::Verified,
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

    fn write_granted_segment_verified_profiled(
        &self,
        grant: &WriteGrant,
        data: Vec<u8>,
    ) -> Result<(VerifiedSegmentReceipt, LocalSegmentWriteProfile)> {
        let mut profile = LocalSegmentWriteProfile::default();
        let expected_segment = grant.segment_id;
        let storage_node = grant.storage_node;

        let started = Instant::now();
        let node = self.storage_nodes.node(storage_node)?.clone();
        profile.storage_node_transport_dispatch_nanos = duration_nanos_u64(started.elapsed());

        let (receipt, node_profile) = node.write_segment_profiled(grant.clone(), data)?;
        profile.absorb(node_profile);
        if receipt.segment_id != expected_segment {
            return Err(StorageError::corrupt(
                "storage node write receipt disagrees with requested segment ID",
            ));
        }

        let started = Instant::now();
        let verified = self.verify_receipt_matches_grant_observed(grant, &receipt)?;
        profile.receipt_verify_nanos = duration_nanos_u64(started.elapsed());
        Ok((verified, profile))
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

    fn publish_native_file_receipt_edits(
        &self,
        publish: NativeFileReceiptPublish,
    ) -> Result<FileWriteCommit> {
        if publish.edits.is_empty() {
            let head = self
                .metadata
                .get_file_head(publish.keyspace_id, publish.file_id)?;
            if head.version != publish.base_version {
                return Err(StorageError::conflict("stale native file version"));
            }
            return Ok(FileWriteCommit {
                keyspace_id: publish.keyspace_id,
                file_id: publish.file_id,
                range: publish.committed_range,
                version: head.version,
                commit_seq: head.latest_commit,
                durability: publish.durability,
            });
        }

        let head = self
            .metadata
            .get_file_head(publish.keyspace_id, publish.file_id)?;
        if head.version != publish.base_version {
            return Err(StorageError::conflict("stale native file version"));
        }
        let root = self.metadata.get_metadata_node(head.root)?;
        let block_size = u64::from(self.metadata.config.block_size);
        let mut new_root = head.root;
        for edit in &publish.edits {
            if !root.covered_range.contains_range(edit.range)? {
                return Err(StorageError::invalid_argument(
                    "native file batch exceeds file root coverage",
                ));
            }
            let expected_segment_bytes = edit
                .range
                .blocks
                .raw()
                .checked_mul(block_size)
                .ok_or_else(|| {
                    StorageError::invalid_argument("native write segment length overflows")
                })?;
            if edit.receipt.descriptor.bytes != expected_segment_bytes {
                return Err(StorageError::conflict(
                    "native write receipt byte count does not match metadata intent",
                ));
            }
            new_root = self
                .replace_tree_range_with_receipts(
                    new_root,
                    TreeRangeEdit {
                        range: edit.range,
                        replacement: Some(SegmentReplacement {
                            segment_id: edit.receipt.descriptor.segment_id,
                            segment_base: edit.range.start,
                        }),
                    },
                    std::slice::from_ref(&edit.receipt),
                )?
                .root;
        }

        let commit_group = self.publish_commit_group_observed(CommitGroupIntent {
            owner: MappingOwner::NativeKeyspace(publish.keyspace_id),
            fence: MetadataFence::FileVersion(publish.base_version),
            updates: vec![RootUpdate::FileRoot {
                file_id: publish.file_id,
                old_root: head.root,
                new_root,
                new_size: publish.new_size,
            }],
        })?;
        for edit in &publish.edits {
            self.storage_nodes.mark_segment_referenced(
                edit.receipt.receipt(),
                commit_group.commit_seq,
                self.authority.as_ref(),
            )?;
        }
        self.metadata
            .invalidate_append_streams_for_file(publish.keyspace_id, publish.file_id)?;
        let committed = self
            .metadata
            .get_file_head(publish.keyspace_id, publish.file_id)?;
        Ok(FileWriteCommit {
            keyspace_id: publish.keyspace_id,
            file_id: publish.file_id,
            range: publish.committed_range,
            version: committed.version,
            commit_seq: committed.latest_commit,
            durability: publish.durability,
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
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let new_size = head.size.max(end);
        self.publish_native_file_receipt_edits(NativeFileReceiptPublish {
            keyspace_id,
            file_id,
            base_version,
            committed_range: range,
            new_size,
            edits: vec![NativeFileReceiptEdit {
                range: write_range,
                receipt: verified,
            }],
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

    fn replace_tree_range(
        &self,
        root_id: MetadataNodeId,
        edit: TreeRangeEdit,
    ) -> Result<TreeEditResult> {
        self.replace_tree_range_with_receipts(root_id, edit, &[])
    }

    fn replace_tree_byte_range_with_run_extents(
        &self,
        root_id: MetadataNodeId,
        replacement_range: ByteRange,
        replacements: Vec<RunBackedFileExtent>,
    ) -> Result<TreeEditResult> {
        if replacement_range.len == 0 {
            return Ok(TreeEditResult {
                root: root_id,
                changed: false,
            });
        }
        let root = self.metadata.get_metadata_node(root_id)?;
        self.replace_tree_byte_range_with_run_extents_at(&root, replacement_range, &replacements)
    }

    fn replace_tree_byte_range_with_run_extents_at(
        &self,
        node: &MetadataNode,
        replacement_range: ByteRange,
        replacements: &[RunBackedFileExtent],
    ) -> Result<TreeEditResult> {
        let block_size = u64::from(self.metadata.config.block_size);
        let node_range = block_range_to_byte_range(node.covered_range, block_size)?;
        let Some(overlap) = byte_range_intersection(node_range, replacement_range)? else {
            return Ok(TreeEditResult {
                root: node.node_id,
                changed: false,
            });
        };

        match &node.kind {
            MetadataNodeKind::Leaf {
                entries,
                run_extents,
            } => {
                let mut leaf_replacements = Vec::new();
                for replacement in replacements {
                    let replacement_extent_range =
                        ByteRange::new(replacement.file_offset_start, replacement.payload_len);
                    if let Some(extent_overlap) =
                        byte_range_intersection(replacement_extent_range, overlap)?
                        && let Some(sliced) = slice_run_extent(
                            replacement,
                            extent_overlap.offset,
                            extent_overlap.end_exclusive()?,
                        )?
                    {
                        leaf_replacements.push(sliced);
                    }
                }
                let new_run_extents =
                    replace_run_backed_file_extents(run_extents, overlap, leaf_replacements)?;
                if new_run_extents == *run_extents {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }
                let segment_receipts = self.verified_receipts_for_entries(entries)?;
                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Leaf {
                        entries: entries.clone(),
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
                    let child_range = block_range_to_byte_range(child.range, block_size)?;
                    if byte_range_intersection(child_range, overlap)?.is_some() {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        let child_result = self.replace_tree_byte_range_with_run_extents_at(
                            &child_node,
                            overlap,
                            replacements,
                        )?;
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
            MetadataNodeKind::Leaf { entries, .. } => {
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
            MetadataNodeKind::Leaf {
                entries,
                run_extents,
            } => {
                out.push_str(&format!(
                    "{indent}node {} leaf [{}..{}) entries={} run_extents={}\n",
                    node.node_id,
                    node.covered_range.start.raw(),
                    node.covered_range.end_exclusive()?.raw(),
                    entries.len(),
                    run_extents.len()
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
                for extent in run_extents {
                    out.push_str(&format!(
                        "{indent}  [{}..{}) -> append-run {}@{}\n",
                        extent.file_offset_start,
                        extent.file_offset_start + extent.payload_len,
                        extent.run.run_id,
                        extent.run.log_payload_offset
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn read_device(&self, device_id: DeviceId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        self.read_device_with_verification(device_id, range, buf, ReadVerification::Default)
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
        self.verify_segment_payloads_for_read(segment_ids, verification)
    }

    fn read_device_unverified(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
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
        self.read_file_with_verification(
            keyspace_id,
            file_id,
            range,
            buf,
            ReadVerification::Default,
        )
    }

    pub fn read_file_with_verification(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let segment_ids = self.segment_ids_for_file_read(keyspace_id, file_id, range)?;
        let run_extents = self.run_extents_for_file_read(keyspace_id, file_id, range)?;
        self.read_file_unverified(keyspace_id, file_id, range, buf)?;
        self.read_append_run_extents_from_memory(&run_extents, range, buf, verification)?;
        self.verify_segment_payloads_for_read(segment_ids, verification)
    }

    fn read_append_run_extents_from_memory(
        &self,
        extents: &[RunBackedFileExtent],
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        for extent in extents {
            let bytes = self.read_append_run_range_from_memory(&extent.run, verification)?;
            let output_offset = usize::try_from(
                extent
                    .file_offset_start
                    .checked_sub(range.offset)
                    .ok_or_else(|| StorageError::corrupt("run extent precedes read range"))?,
            )
            .map_err(|_| StorageError::invalid_argument("run read output offset overflows"))?;
            let output_end = output_offset
                .checked_add(bytes.len())
                .ok_or_else(|| StorageError::invalid_argument("run read output end overflows"))?;
            let output = buf.get_mut(output_offset..output_end).ok_or_else(|| {
                StorageError::corrupt("run-backed extent exceeds read output buffer")
            })?;
            output.copy_from_slice(&bytes);
        }
        Ok(())
    }

    fn read_append_run_range_from_memory(
        &self,
        run: &AppendLogRunRange,
        verification: ReadVerification,
    ) -> Result<Vec<u8>> {
        run.validate()?;
        let logs = lock(&self.append_run_logs)?;
        let log = logs
            .get(&(run.storage_node, run.log_id))
            .ok_or_else(|| StorageError::corrupt("append-run log is missing from local store"))?;
        let start = usize::try_from(run.log_payload_offset)
            .map_err(|_| StorageError::corrupt("append-run offset overflows usize"))?;
        let len = usize::try_from(run.payload_len)
            .map_err(|_| StorageError::corrupt("append-run length overflows usize"))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| StorageError::corrupt("append-run range overflows"))?;
        let bytes = log
            .get(start..end)
            .ok_or_else(|| StorageError::corrupt("append-run range exceeds local log"))?
            .to_vec();
        match run.integrity {
            SegmentPayloadIntegrity::Unchecked => {
                if matches!(verification, ReadVerification::RequireVerified) {
                    return Err(StorageError::conflict(
                        "read requires verified payload but append run is unchecked",
                    ));
                }
            }
            integrity @ SegmentPayloadIntegrity::Crc32c(_) => {
                if !matches!(verification, ReadVerification::Skip) {
                    verify_segment_payload_integrity(integrity, &bytes)?;
                }
            }
        }
        Ok(bytes)
    }

    fn read_file_unverified(
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

    fn segment_ids_for_device_read(
        &self,
        device_id: DeviceId,
        range: ByteRange,
    ) -> Result<BTreeSet<SegmentId>> {
        let info = self.metadata.device_info(device_id)?;
        range.validate_for_device(&info.spec)?;
        let mut out = BTreeSet::new();
        if range.len == 0 {
            return Ok(out);
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
                self.collect_segment_ids_for_metadata_node(&node, requested, &mut out)?;
            }
        }
        Ok(out)
    }

    fn segment_ids_for_file_read(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
    ) -> Result<BTreeSet<SegmentId>> {
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let end = range.end_exclusive()?;
        if end > head.size {
            return Err(StorageError::invalid_argument(
                "native file read extends past end of file",
            ));
        }
        let mut out = BTreeSet::new();
        if range.len == 0 {
            let _ = self.metadata.get_metadata_node(head.root)?;
            return Ok(out);
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
        self.collect_segment_ids_for_metadata_node(&root, requested, &mut out)?;
        Ok(out)
    }

    fn run_extents_for_file_read(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
    ) -> Result<Vec<RunBackedFileExtent>> {
        let head = self.metadata.get_file_head(keyspace_id, file_id)?;
        let end = range.end_exclusive()?;
        if end > head.size {
            return Err(StorageError::invalid_argument(
                "native file read extends past end of file",
            ));
        }
        let mut out = Vec::new();
        if range.len == 0 {
            let _ = self.metadata.get_metadata_node(head.root)?;
            return Ok(out);
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
        self.collect_run_extents_for_metadata_node(&root, requested, range, &mut out)?;
        out.sort_by_key(|extent| extent.file_offset_start);
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

    fn collect_run_extents_for_metadata_node(
        &self,
        node: &MetadataNode,
        requested_blocks: crate::api::BlockRange,
        requested_bytes: ByteRange,
        out: &mut Vec<RunBackedFileExtent>,
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested_blocks)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.collect_run_extents_for_metadata_node(
                            &child_node,
                            requested_blocks,
                            requested_bytes,
                            out,
                        )?;
                    }
                }
            }
            MetadataNodeKind::Leaf { run_extents, .. } => {
                for extent in run_extents {
                    let extent_range = ByteRange::new(extent.file_offset_start, extent.payload_len);
                    if let Some(overlap) = byte_range_intersection(extent_range, requested_bytes)?
                        && let Some(sliced) =
                            slice_run_extent(extent, overlap.offset, overlap.end_exclusive()?)?
                    {
                        out.push(sliced);
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
