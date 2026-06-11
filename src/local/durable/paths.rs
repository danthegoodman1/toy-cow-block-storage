#[derive(Debug, Clone)]
pub(super) struct DurableStorePaths {
    metadata: PathBuf,
    data_dir: PathBuf,
    block_journal: PathBuf,
    native_publish_journal: PathBuf,
    append_visible_publish_journal: PathBuf,
}

impl DurableStorePaths {
    fn new(root: impl AsRef<Path>, _storage_node: StorageNodeId) -> Result<Self> {
        Self::new_with_append_visible_publish_journal(root, None)
    }

    fn new_with_append_visible_publish_journal(
        root: impl AsRef<Path>,
        append_visible_publish_journal: Option<PathBuf>,
    ) -> Result<Self> {
        let root = root.as_ref();
        let data_dir = root.join("data");
        let tmp_dir = root.join("tmp");
        ensure_dir_exists(&data_dir)?;
        ensure_dir_exists(&tmp_dir)?;
        Ok(Self {
            metadata: root.join("metadata.sqlite"),
            data_dir,
            block_journal: root.join("block.journal"),
            native_publish_journal: root.join("native-publish.journal"),
            append_visible_publish_journal: append_visible_publish_journal
                .unwrap_or_else(|| root.join("append-visible-publish.journal")),
        })
    }
}

fn ensure_dir_exists(path: &Path) -> Result<bool> {
    if path.exists() {
        if path.is_dir() {
            return Ok(true);
        }
        return Err(StorageError::invalid_argument(format!(
            "path exists but is not a directory: {}",
            path.display()
        )));
    }
    fs::create_dir_all(path).map_err(fs_error)?;
    Ok(false)
}

pub(super) fn node_catalog_table(_storage_node: StorageNodeId, table: &'static str) -> Result<&'static str> {
    match table {
        "node_meta" | "data_logs" | "segment_placements" | "segment_catalog_entries" => Ok(table),
        _ => Err(StorageError::invalid_argument(
            "unknown storage-node catalog table",
        )),
    }
}

pub(super) fn node_catalog_path(data_dir: &Path, storage_node: StorageNodeId) -> PathBuf {
    node_data_log_dir(data_dir, storage_node).join("catalog.sqlite")
}

pub(super) fn discover_node_catalogs(data_dir: &Path) -> Result<BTreeSet<StorageNodeId>> {
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
pub(super) struct NodeCatalogs {
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

pub(super) fn open_node_catalog(paths: &DurableStorePaths, storage_node: StorageNodeId) -> Result<Connection> {
    let data_dir = node_data_log_dir(&paths.data_dir, storage_node);
    let catalog_path = node_catalog_path(&paths.data_dir, storage_node);
    let data_dir_existed = data_dir.exists();
    let existed = catalog_path.exists();
    ensure_dir_exists(&data_dir)?;
    if !data_dir_existed {
        sync_dir(&paths.data_dir)?;
    }
    let conn = Connection::open(&catalog_path).map_err(sqlite_error)?;
    configure_sqlite_connection(&conn)?;
    initialize_node_catalog_schema(&conn)?;
    if !existed {
        sync_dir(&data_dir)?;
    }
    Ok(conn)
}

pub(super) fn configure_sqlite_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error)
}

pub(super) fn initialize_node_catalog_schema(conn: &Connection) -> Result<()> {
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
          state TEXT NOT NULL,
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
          payload_integrity TEXT NOT NULL,
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
