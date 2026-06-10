
pub(super) fn persist_row_native_state(
    tx: &rusqlite::Transaction<'_>,
    previous_cursor: Option<&DurableExportCursor>,
    image: &DurableStoreState,
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
    sync_device_head_tables(
        tx,
        "device_manifests",
        "device_shard_heads",
        &image.metadata.device_heads,
        &image.metadata.shard_commits,
    )?;
    sync_device_head_tables(
        tx,
        "deleted_device_manifests",
        "deleted_device_shard_heads",
        &image.metadata.deleted_device_heads,
        &image.metadata.shard_commits,
    )?;
    sync_keyspace_head_tables(
        tx,
        "keyspace_manifests",
        "keyspace_shard_heads",
        &image.metadata.keyspace_heads,
        &image.metadata.keyspace_commits,
        true,
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
    sync_append_streams(tx, &image.metadata.append_streams)?;
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
    let cursor = DurableExportCursor::from_state(image);
    persist_export_cursor(tx, &cursor)
}

pub(super) fn stream_prefix_persist_cursor(
    previous_cursor: Option<&DurableExportCursor>,
    current: &DurableExportCursor,
) -> DurableExportCursor {
    let Some(previous) = previous_cursor else {
        return current.clone();
    };
    let mut cursor = previous.clone();
    cursor.config = current.config;
    cursor.next_write_intent = cursor.next_write_intent.max(current.next_write_intent);
    cursor.next_extent_id = cursor.next_extent_id.max(current.next_extent_id);
    cursor.next_segment_id = cursor.next_segment_id.max(current.next_segment_id);
    cursor.next_placement_index = cursor
        .next_placement_index
        .max(current.next_placement_index);
    cursor
}

pub(super) fn persist_row_native_metadata_delta(
    tx: &rusqlite::Transaction<'_>,
    previous_cursor: Option<&DurableExportCursor>,
    delta: &NativeMetadataDelta,
) -> Result<()> {
    let previous_u128 = |cursor_value: fn(&DurableExportCursor) -> u128| {
        previous_cursor.map(cursor_value).unwrap_or(0)
    };
    let previous_u64 = |cursor_value: fn(&DurableExportCursor) -> u64| {
        previous_cursor.map(cursor_value).unwrap_or(0)
    };

    sync_keyspace_head_tables(
        tx,
        "keyspace_manifests",
        "keyspace_shard_heads",
        &delta.keyspace_heads,
        &delta.keyspace_commits,
        false,
    )?;
    sync_u128_payload_map_since(
        tx,
        "keyspace_roots",
        "root_id",
        &delta.keyspace_roots,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_keyspace_root_id),
        false,
    )?;
    sync_u128_payload_map_since(
        tx,
        "keyspace_catalog_shards",
        "shard_id",
        &delta.keyspace_catalog_shards,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_keyspace_catalog_shard_id),
        false,
    )?;
    for ((keyspace_id, file_id), epoch) in &delta.file_writer_epochs {
        upsert_file_writer_epoch(tx, *keyspace_id, *file_id, *epoch)?;
    }
    for append_stream in &delta.append_streams {
        upsert_append_stream(tx, append_stream)?;
    }
    sync_u128_payload_map_since(
        tx,
        "metadata_nodes",
        "node_id",
        &delta.metadata_nodes,
        |id| id.raw(),
        previous_u128(|cursor| cursor.next_metadata_node_id),
        false,
    )?;
    sync_commit_groups_since(
        tx,
        &delta.commit_groups,
        previous_u128(|cursor| cursor.next_commit_group_id),
        false,
    )?;
    sync_timeline_table_since(
        tx,
        "keyspace_commits",
        &delta.keyspace_commits,
        previous_u64(|cursor| cursor.next_commit_seq),
        false,
    )?;
    sync_timeline_table_since(
        tx,
        "file_commits",
        &delta.file_commits,
        previous_u64(|cursor| cursor.next_commit_seq),
        false,
    )?;
    persist_export_cursor(tx, &delta.cursor)
}

pub(super) fn persist_block_delta_commit(
    tx: &rusqlite::Transaction<'_>,
    delta: &BlockDeltaCommit,
) -> Result<()> {
    tx.execute(
        "INSERT INTO block_delta_commits (row_key, device_id, commit_seq, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(row_key) DO UPDATE SET
           device_id = excluded.device_id,
           commit_seq = excluded.commit_seq,
           payload = excluded.payload",
        params![
            delta.row_key(),
            delta.device_id.raw().to_string(),
            u64_to_i64(delta.commit_seq.raw())?,
            encode_row(delta)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn prune_block_delta_commits_through(
    tx: &rusqlite::Transaction<'_>,
    commit_seq: CommitSeq,
) -> Result<()> {
    tx.execute(
        "DELETE FROM block_delta_commits WHERE commit_seq <= ?1",
        params![u64_to_i64(commit_seq.raw())?],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn persist_native_file_delta_commit(
    tx: &rusqlite::Transaction<'_>,
    delta: &NativeFileDeltaCommit,
) -> Result<()> {
    tx.execute(
        "INSERT INTO native_file_delta_commits (row_key, keyspace_id, file_id, commit_seq, payload)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(row_key) DO UPDATE SET
           keyspace_id = excluded.keyspace_id,
           file_id = excluded.file_id,
           commit_seq = excluded.commit_seq,
           payload = excluded.payload",
        params![
            delta.row_key(),
            delta.keyspace_id.raw().to_string(),
            delta.file_id.raw().to_string(),
            u64_to_i64(delta.commit_seq.raw())?,
            encode_row(delta)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn prune_native_file_delta_commits_through(
    tx: &rusqlite::Transaction<'_>,
    commit_seq: CommitSeq,
) -> Result<()> {
    tx.execute(
        "DELETE FROM native_file_delta_commits WHERE commit_seq <= ?1",
        params![u64_to_i64(commit_seq.raw())?],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) trait DurableTimelineRow: DurableCodec {
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

pub(super) fn persist_export_cursor(
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

pub(super) fn persist_maintenance_cursor(
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

pub(super) enum SegmentCatalogSync<'a> {
    Full,
    Only(&'a BTreeSet<SegmentId>),
    Skip,
}

pub(super) fn sync_node_catalog_state_for_node(
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

pub(super) fn persist_data_log_manifest(
    tx: &rusqlite::Transaction<'_>,
    log: &PendingDataLogManifest,
) -> Result<()> {
    let data_logs = node_catalog_table(log.storage_node, "data_logs")?;
    let live_bytes = if is_stream_data_log_state(&log.state) {
        log.total_bytes
    } else {
        0
    };
    tx.execute(
        &format!(
            "INSERT INTO {data_logs}(log_id, state, total_bytes, live_bytes, dead_bytes)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(log_id) DO UPDATE SET
               state = excluded.state,
               total_bytes = MAX(total_bytes, excluded.total_bytes),
               live_bytes = MAX(live_bytes, excluded.live_bytes)"
        ),
        params![
            u64_to_i64(log.log_id)?,
            &log.state,
            u64_to_i64(log.total_bytes)?,
            u64_to_i64(live_bytes)?
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn seal_data_log_manifest(
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

pub(super) fn persist_segment_placement(
    tx: &rusqlite::Transaction<'_>,
    placement: &SegmentPlacementRow,
) -> Result<()> {
    let segment_placements = node_catalog_table(placement.storage_node, "segment_placements")?;
    let data_logs = node_catalog_table(placement.storage_node, "data_logs")?;
    tx.execute(
        &format!(
            "INSERT INTO {segment_placements}(
               segment_id, data_log_id, record_offset, record_bytes,
               payload_offset, payload_bytes, payload_integrity, current
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)
             ON CONFLICT(segment_id) DO UPDATE SET
               data_log_id = excluded.data_log_id,
               record_offset = excluded.record_offset,
               record_bytes = excluded.record_bytes,
               payload_offset = excluded.payload_offset,
               payload_bytes = excluded.payload_bytes,
               payload_integrity = excluded.payload_integrity,
               current = 1"
        ),
        params![
            segment_id_key(placement.segment_id),
            u64_to_i64(placement.data_log_id)?,
            u64_to_i64(placement.record_offset)?,
            u64_to_i64(placement.record_bytes)?,
            u64_to_i64(placement.payload_offset)?,
            u64_to_i64(placement.payload_bytes)?,
            segment_payload_integrity_key(placement.integrity),
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

pub(super) fn sync_node_meta_row(tx: &rusqlite::Transaction<'_>, row: DurableStorageNodeRow) -> Result<()> {
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

pub(super) fn sync_node_segment_catalog_entries(
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

pub(super) fn sync_node_segment_catalog_entries_for_ids(
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

pub(super) fn encode_catalog_entry_for_pre_root_publish(
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

pub(super) fn sync_payload_table(
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

pub(super) fn device_shard_row_key(device_id: DeviceId, shard_id: ShardId) -> String {
    format!("{}:{:020}", device_id.raw(), shard_id.raw())
}

pub(super) fn keyspace_shard_row_key(keyspace_id: KeyspaceId, shard_index: u32) -> String {
    format!("{}:{:020}", keyspace_id.raw(), shard_index)
}

pub(super) fn sync_device_head_tables(
    tx: &rusqlite::Transaction<'_>,
    manifest_table: &str,
    shard_table: &str,
    heads: &BTreeMap<DeviceId, DeviceHead>,
    shard_commits: &[ShardCommit],
) -> Result<()> {
    let mut shard_latest_commits: BTreeMap<(DeviceId, ShardId), CommitSeq> = BTreeMap::new();
    let mut devices_with_shard_commits = BTreeSet::new();
    for commit in shard_commits {
        devices_with_shard_commits.insert(commit.device_id);
        let entry = shard_latest_commits
            .entry((commit.device_id, commit.shard_id))
            .or_insert(CommitSeq::from_raw(0));
        if commit.commit_seq.raw() > entry.raw() {
            *entry = commit.commit_seq;
        }
    }

    let manifests: BTreeMap<String, Vec<u8>> = heads
        .iter()
        .map(|(device_id, head)| {
            let manifest = DurableDeviceManifest::from_head(head)?;
            Ok((device_id.raw().to_string(), encode_row(&manifest)?))
        })
        .collect::<Result<_>>()?;
    delete_missing_text_keys(tx, manifest_table, "device_id", manifests.keys())?;
    let manifest_sql = format!(
        "INSERT INTO {manifest_table}(device_id, payload) VALUES (?1, ?2)
         ON CONFLICT(device_id) DO UPDATE SET payload = excluded.payload
         WHERE payload != excluded.payload"
    );
    let mut manifest_stmt = tx.prepare(&manifest_sql).map_err(sqlite_error)?;
    for (device_id, payload) in manifests {
        manifest_stmt
            .execute(params![device_id, payload])
            .map_err(sqlite_error)?;
    }

    let mut shard_rows: BTreeMap<String, (String, i64, Vec<u8>)> = BTreeMap::new();
    for head in heads.values() {
        for (shard_index, root) in head.shard_roots.iter().copied().enumerate() {
            let shard_id =
                ShardId::from_raw(u32::try_from(shard_index).map_err(|_| {
                    StorageError::invalid_argument("device shard index overflows u32")
                })?);
            let latest_commit = shard_latest_commits
                .get(&(head.device_id, shard_id))
                .copied()
                .unwrap_or_else(|| {
                    if devices_with_shard_commits.contains(&head.device_id) {
                        CommitSeq::from_raw(0)
                    } else {
                        head.latest_commit
                    }
                });
            let shard_head =
                DurableDeviceShardHead::from_head(head, shard_index, root, latest_commit)?;
            let row_key = device_shard_row_key(head.device_id, shard_head.shard_id);
            if shard_rows
                .insert(
                    row_key,
                    (
                        head.device_id.raw().to_string(),
                        u64_to_i64(u64::from(shard_head.shard_id.raw()))?,
                        encode_row(&shard_head)?,
                    ),
                )
                .is_some()
            {
                return Err(StorageError::corrupt("duplicate device shard head row"));
            }
        }
    }
    delete_missing_text_keys(tx, shard_table, "row_key", shard_rows.keys())?;
    let shard_sql = format!(
        "INSERT INTO {shard_table}(row_key, device_id, shard_id, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(row_key) DO UPDATE SET
           device_id = excluded.device_id,
           shard_id = excluded.shard_id,
           payload = excluded.payload
         WHERE device_id != excluded.device_id
            OR shard_id != excluded.shard_id
            OR payload != excluded.payload"
    );
    let mut shard_stmt = tx.prepare(&shard_sql).map_err(sqlite_error)?;
    for (row_key, (device_id, shard_id, payload)) in shard_rows {
        shard_stmt
            .execute(params![row_key, device_id, shard_id, payload])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

pub(super) fn sync_keyspace_head_tables(
    tx: &rusqlite::Transaction<'_>,
    manifest_table: &str,
    shard_table: &str,
    heads: &BTreeMap<KeyspaceId, KeyspaceHead>,
    keyspace_commits: &[KeyspaceCommit],
    prune_missing: bool,
) -> Result<()> {
    let mut shard_latest_commits: BTreeMap<(KeyspaceId, u32), CommitSeq> = BTreeMap::new();
    let mut keyspaces_with_shard_commits = BTreeSet::new();
    for commit in keyspace_commits {
        keyspaces_with_shard_commits.insert(commit.keyspace_id);
        let entry = shard_latest_commits
            .entry((commit.keyspace_id, commit.shard_index))
            .or_insert(CommitSeq::from_raw(0));
        if commit.commit_seq.raw() > entry.raw() {
            *entry = commit.commit_seq;
        }
    }

    let manifests: BTreeMap<String, Vec<u8>> = heads
        .iter()
        .map(|(keyspace_id, head)| {
            let manifest = DurableKeyspaceManifest::from_head(head)?;
            Ok((keyspace_id.raw().to_string(), encode_row(&manifest)?))
        })
        .collect::<Result<_>>()?;
    if prune_missing {
        delete_missing_text_keys(tx, manifest_table, "keyspace_id", manifests.keys())?;
    }
    let manifest_sql = format!(
        "INSERT INTO {manifest_table}(keyspace_id, payload) VALUES (?1, ?2)
         ON CONFLICT(keyspace_id) DO UPDATE SET payload = excluded.payload
         WHERE payload != excluded.payload"
    );
    let mut manifest_stmt = tx.prepare(&manifest_sql).map_err(sqlite_error)?;
    for (keyspace_id, payload) in manifests {
        manifest_stmt
            .execute(params![keyspace_id, payload])
            .map_err(sqlite_error)?;
    }

    let mut shard_rows: BTreeMap<String, (String, i64, Vec<u8>)> = BTreeMap::new();
    for head in heads.values() {
        for (shard_index, root) in head.shard_roots.iter().copied().enumerate() {
            let shard_index_u32 = u32::try_from(shard_index).map_err(|_| {
                StorageError::invalid_argument("keyspace shard index overflows u32")
            })?;
            if !prune_missing
                && keyspaces_with_shard_commits.contains(&head.keyspace_id)
                && !shard_latest_commits.contains_key(&(head.keyspace_id, shard_index_u32))
            {
                continue;
            }
            let latest_commit = shard_latest_commits
                .get(&(head.keyspace_id, shard_index_u32))
                .copied()
                .unwrap_or_else(|| {
                    if keyspaces_with_shard_commits.contains(&head.keyspace_id) {
                        CommitSeq::from_raw(0)
                    } else {
                        head.latest_commit
                    }
                });
            let shard_head =
                DurableKeyspaceShardHead::from_head(head, shard_index, root, latest_commit)?;
            let row_key = keyspace_shard_row_key(head.keyspace_id, shard_head.shard_index);
            if shard_rows
                .insert(
                    row_key,
                    (
                        head.keyspace_id.raw().to_string(),
                        u64_to_i64(u64::from(shard_head.shard_index))?,
                        encode_row(&shard_head)?,
                    ),
                )
                .is_some()
            {
                return Err(StorageError::corrupt("duplicate keyspace shard head row"));
            }
        }
    }
    if prune_missing {
        delete_missing_text_keys(tx, shard_table, "row_key", shard_rows.keys())?;
    }
    let shard_sql = format!(
        "INSERT INTO {shard_table}(row_key, keyspace_id, shard_index, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(row_key) DO UPDATE SET
           keyspace_id = excluded.keyspace_id,
           shard_index = excluded.shard_index,
           payload = excluded.payload
         WHERE keyspace_id != excluded.keyspace_id
            OR shard_index != excluded.shard_index
            OR payload != excluded.payload"
    );
    let mut shard_stmt = tx.prepare(&shard_sql).map_err(sqlite_error)?;
    for (row_key, (keyspace_id, shard_index, payload)) in shard_rows {
        shard_stmt
            .execute(params![row_key, keyspace_id, shard_index, payload])
            .map_err(sqlite_error)?;
    }
    Ok(())
}

pub(super) fn sync_u128_payload_map_since<K, V>(
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

pub(super) fn sync_file_writer_epochs(
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

pub(super) fn upsert_file_writer_epoch(
    tx: &rusqlite::Transaction<'_>,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    epoch: WriterEpoch,
) -> Result<()> {
    tx.execute(
        "INSERT INTO file_writer_epochs(file_key, keyspace_id, file_id, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(file_key) DO UPDATE SET
           keyspace_id = excluded.keyspace_id,
           file_id = excluded.file_id,
           payload = excluded.payload
         WHERE keyspace_id != excluded.keyspace_id
            OR file_id != excluded.file_id
            OR payload != excluded.payload",
        params![
            file_writer_key(keyspace_id, file_id),
            keyspace_id.raw().to_string(),
            file_id.raw().to_string(),
            encode_row(&epoch)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn sync_append_streams(
    tx: &rusqlite::Transaction<'_>,
    streams: &BTreeMap<AppendStreamId, AppendStreamState>,
) -> Result<()> {
    let desired: BTreeMap<String, (KeyspaceId, FileId, Vec<u8>)> = streams
        .iter()
        .map(|(stream_id, stream)| {
            Ok((
                stream_id.raw().to_string(),
                (stream.keyspace_id, stream.file_id, encode_row(stream)?),
            ))
        })
        .collect::<Result<_>>()?;
    delete_missing_text_keys(tx, "append_streams", "stream_id", desired.keys())?;
    let mut stmt = tx
        .prepare(
            "INSERT INTO append_streams(stream_id, keyspace_id, file_id, payload)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(stream_id) DO UPDATE SET
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

pub(super) fn upsert_append_stream(tx: &rusqlite::Transaction<'_>, stream: &AppendStreamState) -> Result<()> {
    tx.execute(
        "INSERT INTO append_streams(stream_id, keyspace_id, file_id, payload)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(stream_id) DO UPDATE SET
           keyspace_id = excluded.keyspace_id,
           file_id = excluded.file_id,
           payload = excluded.payload
         WHERE keyspace_id != excluded.keyspace_id
            OR file_id != excluded.file_id
            OR payload != excluded.payload",
        params![
            stream.stream_id.raw().to_string(),
            stream.keyspace_id.raw().to_string(),
            stream.file_id.raw().to_string(),
            encode_row(stream)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

pub(super) fn sync_commit_groups_since(
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

pub(super) fn sync_timeline_table_since<T: DurableTimelineRow>(
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

pub(super) fn sync_commit_seq_payload_table_since<T: DurableCodec>(
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

pub(super) fn sync_checkpoints_since(
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

pub(super) fn sync_epoch_table(
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

pub(super) fn existing_text_keys(
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

pub(super) fn delete_missing_text_keys<'a>(
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

pub(super) fn delete_missing_u64_keys(
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
