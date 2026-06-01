#[derive(Debug, Clone, Default)]
pub(super) struct PendingDataLogAppend {
    placements: Vec<SegmentPlacementRow>,
    logs: BTreeMap<DurableDataLogRef, PendingDataLogManifest>,
    sealed_logs: Vec<DurableDataLogRef>,
}

impl PendingDataLogAppend {
    fn is_empty(&self) -> bool {
        self.placements.is_empty() && self.logs.is_empty() && self.sealed_logs.is_empty()
    }

    fn segment_ids(&self) -> BTreeSet<SegmentId> {
        self.placements
            .iter()
            .map(|placement| placement.segment_id)
            .collect()
    }

    fn placement_count(&self) -> u64 {
        usize_to_u64(self.placements.len())
    }

    fn placement_payload_bytes(&self) -> u64 {
        self.placements
            .iter()
            .map(|placement| placement.payload_bytes)
            .fold(0_u64, u64::saturating_add)
    }

    fn storage_node_count(&self) -> u64 {
        usize_to_u64(
            self.log_refs()
                .iter()
                .map(|log_ref| log_ref.storage_node)
                .collect::<BTreeSet<_>>()
                .len(),
        )
    }

    fn selected_log_refs(&self, selected: &BTreeSet<DurableDataLogRef>) -> Self {
        let logs = self
            .logs
            .iter()
            .filter(|(log_ref, _)| selected.contains(log_ref))
            .map(|(log_ref, manifest)| (*log_ref, manifest.clone()))
            .collect();
        let sealed_logs = self
            .sealed_logs
            .iter()
            .copied()
            .filter(|log_ref| selected.contains(log_ref))
            .collect();
        Self {
            placements: Vec::new(),
            logs,
            sealed_logs,
        }
    }

    fn remove_log_refs(&mut self, log_refs: &BTreeSet<DurableDataLogRef>) {
        self.logs.retain(|log_ref, _| !log_refs.contains(log_ref));
        self.sealed_logs
            .retain(|log_ref| !log_refs.contains(log_ref));
    }

    fn log_refs(&self) -> BTreeSet<DurableDataLogRef> {
        let mut out: BTreeSet<_> = self.logs.keys().copied().collect();
        out.extend(self.sealed_logs.iter().copied());
        for placement in &self.placements {
            out.insert(DurableDataLogRef {
                storage_node: placement.storage_node,
                log_id: placement.data_log_id,
            });
        }
        out
    }

    fn active_log_for_node(
        &self,
        storage_node: StorageNodeId,
        data_dir: &Path,
        active_state: &str,
    ) -> Result<Option<DataLogRow>> {
        let sealed: BTreeSet<_> = self
            .sealed_logs
            .iter()
            .copied()
            .filter(|log_ref| log_ref.storage_node == storage_node)
            .collect();
        let Some((log_ref, manifest)) = self
            .logs
            .iter()
            .filter(|(log_ref, manifest)| {
                log_ref.storage_node == storage_node
                    && manifest.state == active_state
                    && !sealed.contains(log_ref)
            })
            .max_by_key(|(log_ref, _)| log_ref.log_id)
        else {
            return Ok(None);
        };
        let path = data_log_path(data_dir, storage_node, log_ref.log_id);
        let total_bytes = path
            .metadata()
            .map(|metadata| metadata.len().max(manifest.total_bytes))
            .unwrap_or(manifest.total_bytes);
        Ok(Some(DataLogRow {
            storage_node,
            log_id: log_ref.log_id,
            total_bytes,
            live_bytes: 0,
            dead_bytes: 0,
        }))
    }

    fn retain_current_placements(&mut self, current_segments: &BTreeSet<SegmentId>) {
        self.placements
            .retain(|placement| current_segments.contains(&placement.segment_id));
        self.prune_unreferenced_logs();
    }

    fn remove_segments(&mut self, segment_ids: &BTreeSet<SegmentId>) {
        self.placements
            .retain(|placement| !segment_ids.contains(&placement.segment_id));
        self.prune_unreferenced_logs();
    }

    fn prune_unreferenced_logs(&mut self) {
        let retained_refs: BTreeSet<_> = self
            .placements
            .iter()
            .map(|placement| DurableDataLogRef {
                storage_node: placement.storage_node,
                log_id: placement.data_log_id,
            })
            .collect();
        self.logs
            .retain(|log_ref, _| retained_refs.contains(log_ref));
        self.sealed_logs
            .retain(|log_ref| retained_refs.contains(log_ref));
    }

    fn merge(&mut self, other: PendingDataLogAppend) {
        self.placements.extend(other.placements);
        for (log_ref, manifest) in other.logs {
            self.logs
                .entry(log_ref)
                .and_modify(|existing| {
                    existing.total_bytes = existing.total_bytes.max(manifest.total_bytes);
                    existing.state = manifest.state.clone();
                    existing.needs_dir_sync |= manifest.needs_dir_sync;
                })
                .or_insert(manifest);
        }
        for log_ref in other.sealed_logs {
            if !self.sealed_logs.contains(&log_ref) {
                self.sealed_logs.push(log_ref);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PendingDataLogManifest {
    storage_node: StorageNodeId,
    log_id: u64,
    state: String,
    total_bytes: u64,
    needs_dir_sync: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DurableStorageNodeRow {
    storage_node: StorageNodeId,
    ordinal: u64,
    next_catalog_segment_id: u128,
    segment_store_next_offset: u64,
}

#[derive(Debug, Clone)]
pub(super) struct DataLogSegmentData {
    segment_id: SegmentId,
    integrity: SegmentPayloadIntegrity,
    bytes: Vec<u8>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct DataLogAppendRunData {
    run_id: AppendRunId,
    integrity: SegmentPayloadIntegrity,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(super) enum DataLogRecordData {
    Segment(DataLogSegmentData),
    #[cfg(test)]
    AppendRun(DataLogAppendRunData),
    #[cfg(not(test))]
    AppendRun,
}

pub(super) fn encode_row<T: DurableCodec>(value: &T) -> Result<Vec<u8>> {
    let mut out = DurableEncoder::default();
    value.encode(&mut out)?;
    Ok(out.finish())
}

pub(super) fn decode_row<T: DurableCodec>(bytes: &[u8]) -> Result<T> {
    let mut input = DurableDecoder { bytes, offset: 0 };
    let value = T::decode(&mut input)?;
    input.finish()?;
    Ok(value)
}

pub(super) fn load_export_cursor(conn: &Connection) -> Result<Option<DurableExportCursor>> {
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

pub(super) fn load_maintenance_cursor(conn: &Connection) -> Result<Option<DurableDataLogRef>> {
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

pub(super) fn reject_legacy_current_state_if_present(conn: &Connection) -> Result<()> {
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

pub(super) fn reject_root_storage_catalog_tables_if_present(conn: &Connection) -> Result<()> {
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

pub(super) fn reject_legacy_device_head_tables_if_present(conn: &Connection) -> Result<()> {
    for table in ["device_heads", "deleted_device_heads"] {
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
        if exists.is_none() {
            continue;
        }
        let sql = format!("SELECT COUNT(*) FROM {table}");
        let count: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .map_err(sqlite_error)?;
        if count > 0 {
            return Err(StorageError::unsupported(
                "legacy whole-device head tables are not supported by the per-shard provider",
            ));
        }
    }
    Ok(())
}

pub(super) fn reject_legacy_keyspace_head_tables_if_present(conn: &Connection) -> Result<()> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = 'keyspace_heads'
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
        .query_row("SELECT COUNT(*) FROM keyspace_heads", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    if count > 0 {
        return Err(StorageError::unsupported(
            "legacy whole-keyspace head tables are not supported by the per-shard provider",
        ));
    }
    Ok(())
}

pub(super) fn reject_orphan_row_native_rows_if_present(conn: &Connection) -> Result<()> {
    for table in [
        "device_specs",
        "device_manifests",
        "deleted_device_manifests",
        "device_shard_heads",
        "deleted_device_shard_heads",
        "keyspace_manifests",
        "keyspace_shard_heads",
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
