
pub(super) fn load_storage_node_rows(node_catalogs: &NodeCatalogs) -> Result<Vec<DurableStorageNodeRow>> {
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

pub(super) fn load_metadata_inner(conn: &Connection, cursor: &DurableExportCursor) -> Result<MetadataInner> {
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
    metadata.device_heads = load_device_heads(
        conn,
        "device_manifests",
        "device_shard_heads",
        cursor.config,
    )?;
    metadata.deleted_device_heads = load_device_heads(
        conn,
        "deleted_device_manifests",
        "deleted_device_shard_heads",
        cursor.config,
    )?;
    metadata.keyspace_heads = load_keyspace_heads(conn)?;
    metadata.keyspace_roots = load_keyspace_roots(conn)?;
    metadata.keyspace_catalog_shards = load_keyspace_catalog_shards(conn)?;
    metadata.file_writer_epochs = load_file_writer_epochs(conn)?;
    metadata.append_streams = load_append_streams(conn)?;
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

pub(super) fn load_payload_rows(
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

pub(super) fn load_block_delta_commits_since(
    conn: &Connection,
    next_commit_seq: u64,
) -> Result<Vec<BlockDeltaCommit>> {
    let mut stmt = conn
        .prepare(
            "SELECT row_key, device_id, commit_seq, payload
             FROM block_delta_commits
             WHERE commit_seq >= ?1
             ORDER BY commit_seq, row_key",
        )
        .map_err(sqlite_error)?;
    let mut rows = stmt
        .query(params![u64_to_i64(next_commit_seq)?])
        .map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let row_key: String = row.get(0).map_err(sqlite_error)?;
        let device_id: String = row.get(1).map_err(sqlite_error)?;
        let commit_seq: i64 = row.get(2).map_err(sqlite_error)?;
        let payload: Vec<u8> = row.get(3).map_err(sqlite_error)?;
        let delta: BlockDeltaCommit = decode_row(&payload)?;
        if row_key != delta.row_key() {
            return Err(StorageError::corrupt(
                "block delta row key disagrees with payload",
            ));
        }
        if device_id != delta.device_id.raw().to_string() {
            return Err(StorageError::corrupt(
                "block delta device id disagrees with payload",
            ));
        }
        if i64_to_u64(commit_seq).map_err(sqlite_error)? != delta.commit_seq.raw() {
            return Err(StorageError::corrupt(
                "block delta commit sequence disagrees with payload",
            ));
        }
        out.push(delta);
    }
    Ok(out)
}

pub(super) fn load_native_file_delta_commits_since(
    conn: &Connection,
    next_commit_seq: u64,
) -> Result<Vec<NativeFileDeltaCommit>> {
    let mut stmt = conn
        .prepare(
            "SELECT row_key, keyspace_id, file_id, commit_seq, payload
             FROM native_file_delta_commits
             WHERE commit_seq >= ?1
             ORDER BY commit_seq, row_key",
        )
        .map_err(sqlite_error)?;
    let mut rows = stmt
        .query(params![u64_to_i64(next_commit_seq)?])
        .map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let row_key: String = row.get(0).map_err(sqlite_error)?;
        let keyspace_id: String = row.get(1).map_err(sqlite_error)?;
        let file_id: String = row.get(2).map_err(sqlite_error)?;
        let commit_seq: i64 = row.get(3).map_err(sqlite_error)?;
        let payload: Vec<u8> = row.get(4).map_err(sqlite_error)?;
        let delta: NativeFileDeltaCommit = decode_row(&payload)?;
        if row_key != delta.row_key() {
            return Err(StorageError::corrupt(
                "native file delta row key disagrees with payload",
            ));
        }
        if keyspace_id != delta.keyspace_id.raw().to_string() {
            return Err(StorageError::corrupt(
                "native file delta keyspace id disagrees with payload",
            ));
        }
        if file_id != delta.file_id.raw().to_string() {
            return Err(StorageError::corrupt(
                "native file delta file id disagrees with payload",
            ));
        }
        if i64_to_u64(commit_seq).map_err(sqlite_error)? != delta.commit_seq.raw() {
            return Err(StorageError::corrupt(
                "native file delta commit sequence disagrees with payload",
            ));
        }
        out.push(delta);
    }
    Ok(out)
}

pub(super) fn effective_cursor_from_native_metadata_delta_commits(
    materialized: &DurableExportCursor,
    commits: &[NativeMetadataDeltaCommit],
) -> Result<DurableExportCursor> {
    let mut cursor = materialized.clone();
    for commit in commits {
        if commit.delta.cursor.next_commit_seq <= cursor.next_commit_seq {
            continue;
        }
        if commit.delta.cursor.config != materialized.config {
            return Err(StorageError::corrupt(
                "native metadata delta config disagrees with store cursor",
            ));
        }
        cursor = commit.delta.cursor.clone();
    }
    Ok(cursor)
}

pub(super) fn apply_native_metadata_delta_commits(
    metadata: &mut MetadataInner,
    materialized: &DurableExportCursor,
    commits: &[NativeMetadataDeltaCommit],
) -> Result<DurableExportCursor> {
    let mut cursor = materialized.clone();
    for commit in commits {
        if commit.delta.cursor.config != materialized.config {
            return Err(StorageError::corrupt(
                "native metadata delta config disagrees with store cursor",
            ));
        }
        if commit.delta.cursor.next_commit_seq <= cursor.next_commit_seq {
            continue;
        }
        let first_commit = commit
            .delta
            .keyspace_commits
            .iter()
            .map(|record| record.commit_seq.raw())
            .chain(commit.delta.file_commits.iter().map(|record| record.commit_seq.raw()))
            .min()
            .ok_or_else(|| StorageError::corrupt("native metadata delta has no timeline rows"))?;
        if first_commit != cursor.next_commit_seq {
            return Err(StorageError::corrupt(
                "native metadata delta replay is not contiguous",
            ));
        }
        metadata
            .keyspace_heads
            .extend(commit.delta.keyspace_heads.iter().map(|(key, value)| (*key, value.clone())));
        metadata.keyspace_roots.extend(
            commit
                .delta
                .keyspace_roots
                .iter()
                .map(|(key, value)| (*key, value.clone())),
        );
        metadata.keyspace_catalog_shards.extend(
            commit
                .delta
                .keyspace_catalog_shards
                .iter()
                .map(|(key, value)| (*key, value.clone())),
        );
        metadata
            .file_writer_epochs
            .extend(commit.delta.file_writer_epochs.iter().copied());
        metadata
            .append_streams
            .extend(commit.delta.append_streams.iter().map(|stream| (stream.stream_id, stream.clone())));
        metadata.metadata_nodes.extend(
            commit
                .delta
                .metadata_nodes
                .iter()
                .map(|(key, value)| (*key, value.clone())),
        );
        metadata.commit_groups.extend(
            commit
                .delta
                .commit_groups
                .iter()
                .map(|(key, value)| (*key, value.clone())),
        );
        metadata
            .keyspace_commits
            .extend(commit.delta.keyspace_commits.iter().cloned());
        metadata
            .file_commits
            .extend(commit.delta.file_commits.iter().cloned());
        cursor = commit.delta.cursor.clone();
    }
    metadata.next_device_id = metadata.next_device_id.max(cursor.next_device_id);
    metadata.next_keyspace_id = metadata.next_keyspace_id.max(cursor.next_keyspace_id);
    metadata.next_file_id = metadata.next_file_id.max(cursor.next_file_id);
    metadata.next_metadata_node_id = metadata
        .next_metadata_node_id
        .max(cursor.next_metadata_node_id);
    metadata.next_keyspace_root_id = metadata
        .next_keyspace_root_id
        .max(cursor.next_keyspace_root_id);
    metadata.next_keyspace_catalog_shard_id = metadata
        .next_keyspace_catalog_shard_id
        .max(cursor.next_keyspace_catalog_shard_id);
    metadata.next_commit_group_id = metadata
        .next_commit_group_id
        .max(cursor.next_commit_group_id);
    metadata.next_commit_seq = metadata.next_commit_seq.max(cursor.next_commit_seq);
    metadata.next_checkpoint_id = metadata.next_checkpoint_id.max(cursor.next_checkpoint_id);
    metadata.next_gc_epoch = metadata.next_gc_epoch.max(cursor.next_gc_epoch);
    Ok(cursor)
}

pub(super) fn load_device_specs(conn: &Connection) -> Result<BTreeMap<DeviceId, crate::api::DeviceSpec>> {
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

pub(super) fn load_device_heads(
    conn: &Connection,
    manifest_table: &str,
    shard_table: &str,
    config: LocalStoreConfig,
) -> Result<BTreeMap<DeviceId, DeviceHead>> {
    let mut manifests = BTreeMap::new();
    for (key, payload) in load_payload_rows(conn, manifest_table, "device_id", "device_id")? {
        let id = DeviceId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let manifest: DurableDeviceManifest = decode_row(&payload)?;
        if manifest.device_id != id {
            return Err(StorageError::corrupt(
                "device manifest row key disagrees with payload",
            ));
        }
        if usize::try_from(manifest.shard_count).ok() != Some(config.shard_count) {
            return Err(StorageError::corrupt(
                "device manifest shard count disagrees with durable config",
            ));
        }
        if manifests.insert(id, manifest).is_some() {
            return Err(StorageError::corrupt("duplicate device manifest row"));
        }
    }

    let mut shards: BTreeMap<DeviceId, BTreeMap<usize, DurableDeviceShardHead>> = BTreeMap::new();
    for (row_key, payload) in load_payload_rows(conn, shard_table, "row_key", "row_key")? {
        let shard: DurableDeviceShardHead = decode_row(&payload)?;
        if row_key != device_shard_row_key(shard.device_id, shard.shard_id) {
            return Err(StorageError::corrupt(
                "device shard head row key disagrees with payload",
            ));
        }
        let shard_index = usize::try_from(shard.shard_id.raw())
            .map_err(|_| StorageError::corrupt("device shard id overflows usize"))?;
        if shard_index >= config.shard_count {
            return Err(StorageError::corrupt("device shard row is outside config"));
        }
        if !manifests.contains_key(&shard.device_id) {
            return Err(StorageError::corrupt(
                "device shard row exists without manifest",
            ));
        }
        if shards
            .entry(shard.device_id)
            .or_default()
            .insert(shard_index, shard)
            .is_some()
        {
            return Err(StorageError::corrupt("duplicate device shard row"));
        }
    }

    let mut out = BTreeMap::new();
    for (device_id, manifest) in manifests {
        let shard_count = usize::try_from(manifest.shard_count)
            .map_err(|_| StorageError::corrupt("device manifest shard count overflows usize"))?;
        let mut shard_roots = Vec::with_capacity(shard_count);
        let mut generation = DeviceGeneration::from_raw(0);
        let mut latest_commit = CommitSeq::from_raw(0);
        let Some(device_shards) = shards.remove(&device_id) else {
            return Err(StorageError::corrupt("device manifest has no shard rows"));
        };
        for shard_index in 0..shard_count {
            let Some(shard) = device_shards.get(&shard_index) else {
                return Err(StorageError::corrupt(
                    "device manifest is missing shard row",
                ));
            };
            shard_roots.push(shard.root);
            if shard.generation.raw() > generation.raw() {
                generation = shard.generation;
            }
            if shard.latest_commit.raw() > latest_commit.raw() {
                latest_commit = shard.latest_commit;
            }
        }
        let head = DeviceHead {
            device_id,
            generation,
            shard_roots,
            latest_commit,
        };
        head.validate(config.shard_count)?;
        if out.insert(device_id, head).is_some() {
            return Err(StorageError::corrupt("duplicate device head"));
        }
    }
    if !shards.is_empty() {
        return Err(StorageError::corrupt("unconsumed device shard rows"));
    }
    Ok(out)
}

pub(super) fn load_keyspace_heads(conn: &Connection) -> Result<BTreeMap<KeyspaceId, KeyspaceHead>> {
    let mut manifests = BTreeMap::new();
    for (key, payload) in
        load_payload_rows(conn, "keyspace_manifests", "keyspace_id", "keyspace_id")?
    {
        let id = KeyspaceId::from_raw(parse_u128_key(&key).map_err(sqlite_error)?);
        let manifest: DurableKeyspaceManifest = decode_row(&payload)?;
        if manifest.keyspace_id != id {
            return Err(StorageError::corrupt(
                "keyspace manifest row key disagrees with payload",
            ));
        }
        if usize::try_from(manifest.shard_count).ok() != Some(KEYSPACE_CATALOG_SHARD_COUNT) {
            return Err(StorageError::corrupt(
                "keyspace manifest shard count disagrees with config",
            ));
        }
        if manifests.insert(id, manifest).is_some() {
            return Err(StorageError::corrupt("duplicate keyspace manifest row"));
        }
    }

    let mut shards: BTreeMap<KeyspaceId, BTreeMap<usize, DurableKeyspaceShardHead>> =
        BTreeMap::new();
    for (row_key, payload) in load_payload_rows(conn, "keyspace_shard_heads", "row_key", "row_key")?
    {
        let shard: DurableKeyspaceShardHead = decode_row(&payload)?;
        if row_key != keyspace_shard_row_key(shard.keyspace_id, shard.shard_index) {
            return Err(StorageError::corrupt(
                "keyspace shard head row key disagrees with payload",
            ));
        }
        let shard_index = usize::try_from(shard.shard_index)
            .map_err(|_| StorageError::corrupt("keyspace shard index overflows usize"))?;
        if shard_index >= KEYSPACE_CATALOG_SHARD_COUNT {
            return Err(StorageError::corrupt(
                "keyspace shard row is outside config",
            ));
        }
        if !manifests.contains_key(&shard.keyspace_id) {
            return Err(StorageError::corrupt(
                "keyspace shard row exists without manifest",
            ));
        }
        if shards
            .entry(shard.keyspace_id)
            .or_default()
            .insert(shard_index, shard)
            .is_some()
        {
            return Err(StorageError::corrupt("duplicate keyspace shard row"));
        }
    }

    let mut out = BTreeMap::new();
    for (keyspace_id, manifest) in manifests {
        let shard_count = usize::try_from(manifest.shard_count)
            .map_err(|_| StorageError::corrupt("keyspace manifest shard count overflows usize"))?;
        let mut shard_roots = Vec::with_capacity(shard_count);
        let mut generation = KeyspaceGeneration::from_raw(0);
        let mut latest_commit = CommitSeq::from_raw(0);
        let Some(keyspace_shards) = shards.remove(&keyspace_id) else {
            return Err(StorageError::corrupt("keyspace manifest has no shard rows"));
        };
        for shard_index in 0..shard_count {
            let Some(shard) = keyspace_shards.get(&shard_index) else {
                return Err(StorageError::corrupt(
                    "keyspace manifest is missing shard row",
                ));
            };
            shard_roots.push(shard.root);
            if shard.generation.raw() > generation.raw() {
                generation = shard.generation;
            }
            if shard.latest_commit.raw() > latest_commit.raw() {
                latest_commit = shard.latest_commit;
            }
        }
        let head = KeyspaceHead {
            keyspace_id,
            generation,
            shard_roots,
            file_count: usize::try_from(manifest.file_count)
                .map_err(|_| StorageError::corrupt("keyspace file count overflows usize"))?,
            latest_commit,
        };
        if out.insert(keyspace_id, head).is_some() {
            return Err(StorageError::corrupt("duplicate keyspace head"));
        }
    }
    if !shards.is_empty() {
        return Err(StorageError::corrupt("unconsumed keyspace shard rows"));
    }
    Ok(out)
}

pub(super) fn load_keyspace_roots(conn: &Connection) -> Result<BTreeMap<KeyspaceRootId, KeyspaceRoot>> {
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

pub(super) fn load_keyspace_catalog_shards(
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

pub(super) fn load_file_writer_epochs(
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

pub(super) fn load_append_streams(conn: &Connection) -> Result<BTreeMap<AppendStreamId, AppendStreamState>> {
    let mut stmt = conn
        .prepare(
            "SELECT stream_id, keyspace_id, file_id, payload
             FROM append_streams
             ORDER BY keyspace_id, file_id, stream_id",
        )
        .map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let mut out = BTreeMap::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let stream_id: String = row.get(0).map_err(sqlite_error)?;
        let keyspace_id: String = row.get(1).map_err(sqlite_error)?;
        let file_id: String = row.get(2).map_err(sqlite_error)?;
        let stream_id = AppendStreamId::from_raw(parse_u128_key(&stream_id).map_err(sqlite_error)?);
        let keyspace_id = KeyspaceId::from_raw(parse_u128_key(&keyspace_id).map_err(sqlite_error)?);
        let file_id = FileId::from_raw(parse_u128_key(&file_id).map_err(sqlite_error)?);
        let payload: Vec<u8> = row.get(3).map_err(sqlite_error)?;
        let stream: AppendStreamState = decode_row(&payload)?;
        if stream.stream_id != stream_id
            || stream.keyspace_id != keyspace_id
            || stream.file_id != file_id
        {
            return Err(StorageError::corrupt(
                "append stream row disagrees with payload",
            ));
        }
        if out.insert(stream_id, stream).is_some() {
            return Err(StorageError::corrupt("duplicate append stream row"));
        }
    }
    Ok(out)
}

pub(super) fn load_metadata_nodes(conn: &Connection) -> Result<BTreeMap<MetadataNodeId, MetadataNode>> {
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

pub(super) fn load_commit_groups(conn: &Connection) -> Result<BTreeMap<CommitGroupId, CommitGroup>> {
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

pub(super) fn load_timeline_rows<T: DurableTimelineRow>(conn: &Connection, table: &str) -> Result<Vec<T>> {
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

pub(super) fn load_commit_seq_payload_map<T: DurableCodec>(
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

pub(super) fn load_checkpoints(conn: &Connection) -> Result<BTreeMap<CheckpointId, Checkpoint>> {
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

pub(super) fn load_metadata_gc_marks(conn: &Connection) -> Result<BTreeMap<MetadataNodeId, u64>> {
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

pub(super) fn load_segment_gc_marks(conn: &Connection) -> Result<BTreeMap<SegmentId, u64>> {
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

pub(super) fn load_epoch_rows(conn: &Connection, table: &str, key_col: &str) -> Result<Vec<(String, u64)>> {
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

pub(super) fn load_catalog_inner(
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

pub(super) fn validate_row_native_state(image: &DurableStoreState) -> Result<()> {
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
        if head.shard_roots.len() != KEYSPACE_CATALOG_SHARD_COUNT {
            return Err(StorageError::corrupt("keyspace head shard count mismatch"));
        }
        let mut actual_file_count = 0usize;
        for shard_id in &head.shard_roots {
            if !image
                .metadata
                .keyspace_catalog_shards
                .contains_key(shard_id)
            {
                return Err(StorageError::corrupt("keyspace head shard missing"));
            }
            actual_file_count = actual_file_count
                .checked_add(
                    image
                        .metadata
                        .keyspace_catalog_shards
                        .get(shard_id)
                        .map(|shard| shard.files.len())
                        .unwrap_or_default(),
                )
                .ok_or_else(|| StorageError::corrupt("keyspace file count overflows"))?;
        }
        if actual_file_count != head.file_count {
            return Err(StorageError::corrupt(
                "keyspace head file count disagrees with shards",
            ));
        }
    }
    for root in image.metadata.keyspace_roots.values() {
        root.validate()?;
        for shard_id in root.shard_roots.iter() {
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
        if let MetadataNodeKind::Leaf { entries, .. } = &node.kind {
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
        if !image
            .metadata
            .keyspace_catalog_shards
            .contains_key(&commit.old_shard)
            || !image
                .metadata
                .keyspace_catalog_shards
                .contains_key(&commit.new_shard)
        {
            return Err(StorageError::corrupt(
                "keyspace commit references missing catalog shard",
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

pub(super) fn validate_row_native_cursors(image: &DurableStoreState) -> Result<()> {
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

pub(super) fn ensure_next_u128_above(
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

pub(super) fn row_native_segment_descriptors(
    image: &DurableStoreState,
) -> BTreeMap<SegmentId, SegmentDescriptor> {
    let mut out = BTreeMap::new();
    for node in image.storage_nodes.nodes.values() {
        for (segment_id, record) in &node.segment_store.segments {
            out.insert(*segment_id, record.commit.descriptor.clone());
        }
    }
    out
}

pub(super) fn validate_timeline_monotonic<T: DurableTimelineRow>(rows: &[T]) -> Result<()> {
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

pub(super) fn file_writer_key(keyspace_id: KeyspaceId, file_id: FileId) -> String {
    format!("{}:{}", keyspace_id.raw(), file_id.raw())
}

pub(super) fn node_data_log_dir(data_dir: &Path, storage_node: StorageNodeId) -> PathBuf {
    data_dir.join(format!("node-{}", storage_node.raw()))
}

pub(super) fn data_log_path(data_dir: &Path, storage_node: StorageNodeId, log_id: u64) -> PathBuf {
    node_data_log_dir(data_dir, storage_node).join(format!("data-{log_id:06}.log"))
}

pub(super) fn delete_data_log(data_dir: &Path, log_ref: DurableDataLogRef) -> Result<()> {
    let path = data_log_path(data_dir, log_ref.storage_node, log_ref.log_id);
    match fs::remove_file(path) {
        Ok(()) => sync_dir(&node_data_log_dir(data_dir, log_ref.storage_node)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(fs_error(error)),
    }
}

pub(super) fn list_node_data_log_ids(
    data_dir: &Path,
    storage_node: StorageNodeId,
) -> Result<Vec<u64>> {
    let dir = node_data_log_dir(data_dir, storage_node);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(fs_error(error)),
    };
    let mut log_ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(fs_error)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(digits) = name.strip_prefix("data-").and_then(|rest| rest.strip_suffix(".log"))
            && let Ok(log_id) = digits.parse::<u64>()
        {
            log_ids.push(log_id);
        }
    }
    log_ids.sort_unstable();
    Ok(log_ids)
}

/// One self-describing data-log record recovered by scanning, with the
/// durable placement that a catalog row publication would have recorded.
#[derive(Debug)]
pub(super) struct RecoveredSegmentRecord {
    pub placement: SegmentPlacementRow,
    pub bytes: Vec<u8>,
}

/// Find durable payload records for `wanted` segments by walking a node's
/// data logs header-by-header.
///
/// Reopen uses this for journal-referenced segments whose catalog rows were
/// still queued for asynchronous publication at crash. Scanning stops at the
/// first torn or truncated record in a log: payload syncs cover whole-file
/// prefixes, so anything beyond a torn record was never covered by a payload
/// sync and therefore cannot be referenced by a durable journal record.
pub(super) fn scan_node_data_logs_for_segments(
    data_dir: &Path,
    storage_node: StorageNodeId,
    wanted: &BTreeSet<SegmentId>,
) -> Result<BTreeMap<SegmentId, RecoveredSegmentRecord>> {
    let mut found = BTreeMap::new();
    if wanted.is_empty() {
        return Ok(found);
    }
    // Newer logs hold the most recently staged segments, so scanning from the
    // highest log id finds crash-window segments fastest.
    for log_id in list_node_data_log_ids(data_dir, storage_node)?
        .into_iter()
        .rev()
    {
        if found.len() == wanted.len() {
            break;
        }
        let path = data_log_path(data_dir, storage_node, log_id);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(fs_error(error)),
        };
        let file_len = file.metadata().map_err(fs_error)?.len();
        let mut offset = 0_u64;
        let mut header = vec![0_u8; DATA_LOG_HEADER_LEN];
        while offset.saturating_add(DATA_LOG_HEADER_LEN as u64) <= file_len {
            file.seek(SeekFrom::Start(offset)).map_err(fs_error)?;
            if file.read_exact(&mut header).is_err() {
                break;
            }
            let Ok(parsed) = parse_data_log_record_header(&header) else {
                break;
            };
            let record_bytes = (DATA_LOG_HEADER_LEN as u64).saturating_add(parsed.payload_len);
            let record_end = offset.saturating_add(record_bytes);
            if record_end > file_len {
                break;
            }
            if parsed.kind == DATA_LOG_KIND_SEGMENT {
                let segment_id = SegmentId::from_raw(parsed.identity);
                if wanted.contains(&segment_id) && !found.contains_key(&segment_id) {
                    let payload_len = usize::try_from(parsed.payload_len).map_err(|_| {
                        StorageError::corrupt("data-log payload length overflows usize")
                    })?;
                    let mut bytes = vec![0_u8; payload_len];
                    file.read_exact(&mut bytes).map_err(fs_error)?;
                    verify_segment_payload_integrity(parsed.integrity, &bytes)?;
                    found.insert(
                        segment_id,
                        RecoveredSegmentRecord {
                            placement: SegmentPlacementRow {
                                segment_id,
                                storage_node,
                                data_log_id: log_id,
                                record_offset: offset,
                                record_bytes,
                                payload_offset: offset
                                    .saturating_add(DATA_LOG_HEADER_LEN as u64),
                                payload_bytes: parsed.payload_len,
                                integrity: parsed.integrity,
                            },
                            bytes,
                        },
                    );
                    if found.len() == wanted.len() {
                        break;
                    }
                }
            }
            offset = record_end;
        }
    }
    Ok(found)
}

pub(super) fn sync_data_log_files(
    files: Vec<DataLogFileToSync>,
) -> Result<DataLogFileSyncProfile> {
    sync_data_log_files_with_fanout(files, 4)
}

pub(super) fn sync_data_log_files_with_fanout(
    files: Vec<DataLogFileToSync>,
    fanout: usize,
) -> Result<DataLogFileSyncProfile> {
    if fanout == 0 {
        return Err(StorageError::invalid_argument(
            "data-log file sync fanout must be greater than zero",
        ));
    }
    let mut profile = DataLogFileSyncProfile::default();
    if files.len() <= 1 {
        if let Some(file) = files.into_iter().next() {
            let (bytes, nanos) = sync_data_log_file(file)?;
            profile.record_file(bytes, nanos);
        }
        return Ok(profile);
    }

    let mut files = files.into_iter();
    loop {
        let mut handles = Vec::with_capacity(fanout);
        for _ in 0..fanout {
            let Some(file) = files.next() else {
                break;
            };
            handles.push(thread::spawn(move || sync_data_log_file(file)));
        }
        if handles.is_empty() {
            break;
        }
        for handle in handles {
            let (bytes, nanos) = handle
                .join()
                .map_err(|_| StorageError::unavailable("data-log sync worker panicked"))??;
            profile.record_file(bytes, nanos);
        }
    }
    Ok(profile)
}

fn sync_data_log_file(file: DataLogFileToSync) -> Result<(u64, u64)> {
    let started = Instant::now();
    file.file.sync_data().map_err(fs_error)?;
    Ok((file.bytes, duration_nanos_u64(started.elapsed())))
}

pub(super) fn sync_pending_data_logs(
    data_dir: &Path,
    pending: &PendingDataLogAppend,
) -> Result<DataLogAppendProfile> {
    if pending.is_empty() {
        return Ok(DataLogAppendProfile::default());
    }
    let mut profile = DataLogAppendProfile::default();
    let mut storage_nodes = BTreeSet::new();
    let mut logs = BTreeSet::new();
    let mut files = Vec::new();
    for log_ref in pending.logs.keys() {
        storage_nodes.insert(log_ref.storage_node);
        logs.insert(*log_ref);
    }
    for log_ref in logs {
        let path = data_log_path(data_dir, log_ref.storage_node, log_ref.log_id);
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
    for storage_node in storage_nodes {
        let started = Instant::now();
        sync_dir(&node_data_log_dir(data_dir, storage_node))?;
        profile.dir_sync_nanos = profile
            .dir_sync_nanos
            .saturating_add(duration_nanos_u64(started.elapsed()));
    }
    let started = Instant::now();
    sync_dir(data_dir)?;
    profile.dir_sync_nanos = profile
        .dir_sync_nanos
        .saturating_add(duration_nanos_u64(started.elapsed()));
    Ok(profile)
}

pub(super) fn sync_parent_dir(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::invalid_argument("path has no parent directory"))?;
    sync_dir(parent)
}

pub(super) fn sqlite_wal_bytes(metadata_path: &Path) -> Result<u64> {
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

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .map_err(fs_error)?
        .sync_all()
        .map_err(fs_error)
}

pub(super) fn encode_data_log_record(
    segment_id: SegmentId,
    integrity: SegmentPayloadIntegrity,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    encode_typed_data_log_record(DATA_LOG_KIND_SEGMENT, segment_id.raw(), integrity, bytes)
}

#[cfg(test)]
pub(super) fn encode_append_run_data_log_record(
    run_id: AppendRunId,
    integrity: SegmentPayloadIntegrity,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    encode_typed_data_log_record(DATA_LOG_KIND_APPEND_RUN, run_id.raw(), integrity, bytes)
}

pub(super) fn encode_typed_data_log_record(
    kind: u8,
    identity: u128,
    integrity: SegmentPayloadIntegrity,
    bytes: &[u8],
) -> Result<Vec<u8>> {
    let payload_len = u64::try_from(bytes.len())
        .map_err(|_| StorageError::invalid_argument("data-log payload length overflows u64"))?;
    let mut out = encode_typed_data_log_header(kind, identity, integrity, payload_len)?;
    out.extend_from_slice(bytes);
    Ok(out)
}

pub(super) fn encode_typed_data_log_header(
    kind: u8,
    identity: u128,
    integrity: SegmentPayloadIntegrity,
    payload_len: u64,
) -> Result<Vec<u8>> {
    if !matches!(kind, DATA_LOG_KIND_SEGMENT | DATA_LOG_KIND_APPEND_RUN) {
        return Err(StorageError::invalid_argument(
            "unknown data-log record kind",
        ));
    }
    let mut out = Vec::with_capacity(DATA_LOG_HEADER_LEN);
    out.extend_from_slice(DATA_LOG_MAGIC);
    out.extend_from_slice(&DATA_LOG_VERSION.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(&identity.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    match integrity {
        SegmentPayloadIntegrity::Crc32c(checksum) => {
            out.push(1);
            out.extend_from_slice(&checksum.to_be_bytes());
        }
        SegmentPayloadIntegrity::Unchecked => {
            out.push(2);
            out.extend_from_slice(&0_u64.to_be_bytes());
        }
    }
    Ok(out)
}

/// Parsed fixed-size data-log record header.
#[derive(Debug, Clone, Copy)]
pub(super) struct DataLogRecordHeader {
    pub kind: u8,
    pub identity: u128,
    pub payload_len: u64,
    pub integrity: SegmentPayloadIntegrity,
}

pub(super) fn parse_data_log_record_header(header: &[u8]) -> Result<DataLogRecordHeader> {
    if header.len() < DATA_LOG_HEADER_LEN {
        return Err(StorageError::corrupt("data-log record is truncated"));
    }
    if &header[..DATA_LOG_MAGIC.len()] != DATA_LOG_MAGIC {
        return Err(StorageError::corrupt("bad data-log magic"));
    }
    let version_offset = DATA_LOG_MAGIC.len();
    let version = u16::from_be_bytes(
        header[version_offset..version_offset + 2]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log version"))?,
    );
    if version != DATA_LOG_VERSION {
        return Err(StorageError::corrupt("unsupported data-log version"));
    }
    let kind_offset = version_offset + 2;
    let kind = header[kind_offset];
    if !matches!(kind, DATA_LOG_KIND_SEGMENT | DATA_LOG_KIND_APPEND_RUN) {
        return Err(StorageError::corrupt("invalid data-log record kind"));
    }
    let identity_start = kind_offset + 1;
    let identity = u128::from_be_bytes(
        header[identity_start..identity_start + 16]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log identity"))?,
    );
    let payload_len_start = identity_start + 16;
    let payload_len = u64::from_be_bytes(
        header[payload_len_start..payload_len_start + 8]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log payload length"))?,
    );
    let integrity_start = payload_len_start + 8;
    let integrity_tag = header[integrity_start];
    let expected_checksum = u64::from_be_bytes(
        header[DATA_LOG_CHECKSUM_OFFSET..DATA_LOG_CHECKSUM_OFFSET + 8]
            .try_into()
            .map_err(|_| StorageError::corrupt("bad data-log checksum"))?,
    );
    let integrity = match integrity_tag {
        1 => SegmentPayloadIntegrity::Crc32c(expected_checksum),
        2 if expected_checksum == 0 => SegmentPayloadIntegrity::Unchecked,
        2 => {
            return Err(StorageError::corrupt(
                "unchecked data-log record has nonzero checksum",
            ));
        }
        _ => return Err(StorageError::corrupt("invalid data-log integrity tag")),
    };
    Ok(DataLogRecordHeader {
        kind,
        identity,
        payload_len,
        integrity,
    })
}

pub(super) fn decode_data_log_record(record: &[u8]) -> Result<DataLogRecordData> {
    let header = parse_data_log_record_header(record)?;
    let payload_len_usize = usize::try_from(header.payload_len)
        .map_err(|_| StorageError::corrupt("data-log payload length overflows usize"))?;
    let expected_record_len = DATA_LOG_HEADER_LEN
        .checked_add(payload_len_usize)
        .ok_or_else(|| StorageError::corrupt("data-log record length overflow"))?;
    if record.len() != expected_record_len {
        return Err(StorageError::corrupt("data-log record length mismatch"));
    }
    let bytes = record[DATA_LOG_HEADER_LEN..].to_vec();
    let integrity = header.integrity;
    verify_segment_payload_integrity(integrity, &bytes)?;
    let kind = header.kind;
    let identity = header.identity;
    match kind {
        DATA_LOG_KIND_SEGMENT => Ok(DataLogRecordData::Segment(DataLogSegmentData {
            segment_id: SegmentId::from_raw(identity),
            integrity,
            bytes,
        })),
        DATA_LOG_KIND_APPEND_RUN => {
            #[cfg(test)]
            {
                Ok(DataLogRecordData::AppendRun(DataLogAppendRunData {
                    run_id: AppendRunId::from_raw(identity),
                    integrity,
                    bytes,
                }))
            }
            #[cfg(not(test))]
            {
                Ok(DataLogRecordData::AppendRun)
            }
        }
        _ => Err(StorageError::corrupt("invalid data-log record kind")),
    }
}

pub(super) fn decode_segment_data_log_record(record: &[u8]) -> Result<DataLogSegmentData> {
    match decode_data_log_record(record)? {
        DataLogRecordData::Segment(data) => Ok(data),
        #[cfg(test)]
        DataLogRecordData::AppendRun(_) => Err(StorageError::corrupt(
            "expected segment data-log record, found append-run record",
        )),
        #[cfg(not(test))]
        DataLogRecordData::AppendRun => Err(StorageError::corrupt(
            "expected segment data-log record, found append-run record",
        )),
    }
}

#[cfg(test)]
pub(super) fn decode_append_run_data_log_record(record: &[u8]) -> Result<DataLogAppendRunData> {
    match decode_data_log_record(record)? {
        DataLogRecordData::AppendRun(data) => Ok(data),
        DataLogRecordData::Segment(_) => Err(StorageError::corrupt(
            "expected append-run data-log record, found segment record",
        )),
    }
}

pub(super) fn current_placements_for_log(
    conn: &Connection,
    log_ref: DurableDataLogRef,
) -> Result<Vec<SegmentPlacementRow>> {
    let segment_placements = node_catalog_table(log_ref.storage_node, "segment_placements")?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT segment_id, data_log_id, record_offset, record_bytes,
                    payload_offset, payload_bytes, payload_integrity
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

pub(super) fn decode_node_placement_row(
    row: &rusqlite::Row<'_>,
    storage_node: StorageNodeId,
) -> rusqlite::Result<SegmentPlacementRow> {
    let segment_id: String = row.get(0)?;
    let payload_integrity: String = row.get(6)?;
    Ok(SegmentPlacementRow {
        segment_id: SegmentId::from_raw(parse_u128_key(&segment_id)?),
        storage_node,
        data_log_id: i64_to_u64(row.get(1)?)?,
        record_offset: i64_to_u64(row.get(2)?)?,
        record_bytes: i64_to_u64(row.get(3)?)?,
        payload_offset: i64_to_u64(row.get(4)?)?,
        payload_bytes: i64_to_u64(row.get(5)?)?,
        integrity: parse_segment_payload_integrity_key(&payload_integrity)?,
    })
}

pub(super) fn active_data_log_with_state(
    conn: &Connection,
    data_dir: &Path,
    storage_node: StorageNodeId,
    active_state: &str,
) -> Result<DataLogRow> {
    let data_logs = node_catalog_table(storage_node, "data_logs")?;
    if let Some(row) = conn
        .query_row(
            &format!(
                "SELECT log_id, total_bytes, live_bytes, dead_bytes
                 FROM {data_logs}
                 WHERE state = ?1
                 ORDER BY log_id DESC
                 LIMIT 1"
            ),
            params![active_state],
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

pub(super) fn next_data_log(
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

pub(super) fn next_data_log_id(
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

pub(super) fn fs_data_log_max_id(data_dir: &Path) -> Result<u64> {
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
pub(super) fn data_log_rows(node_catalogs: &NodeCatalogs) -> Result<Vec<DataLogRow>> {
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

pub(super) fn compaction_candidates(
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

pub(super) fn compaction_candidates_for_refs(
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

pub(super) fn decode_node_data_log_row(
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

pub(super) fn mark_placement_dead(
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

pub(super) fn validate_durable_segment_placement(
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

pub(super) fn segment_id_key(segment_id: SegmentId) -> String {
    segment_id.raw().to_string()
}

pub(super) fn storage_node_key(storage_node: StorageNodeId) -> String {
    storage_node.raw().to_string()
}

pub(super) fn u64_key(value: u64) -> String {
    value.to_string()
}

pub(super) fn parse_u128_key(value: &str) -> rusqlite::Result<u128> {
    value.parse::<u128>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub(super) fn parse_u64_key(value: &str) -> rusqlite::Result<u64> {
    value.parse::<u64>().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub(super) fn u64_to_i64(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| StorageError::invalid_argument("u64 value overflows SQLite i64"))
}

pub(super) fn i64_to_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}
