#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MetadataTxnKey {
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
pub(super) struct TxnDeviceManifest {
    spec: DeviceSpec,
    shard_count: usize,
    live: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TxnShardHead {
    root: MetadataNodeId,
    generation: DeviceGeneration,
    latest_commit: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MetadataTxnValue {
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
pub(super) struct MetadataTxnEntry {
    version: u64,
    value: MetadataTxnValue,
}

#[derive(Debug, Clone)]
pub(super) struct MetadataTxnRead {
    key: MetadataTxnKey,
    version: u64,
}

#[derive(Debug, Clone)]
pub(super) struct VersionedMetadataTxnValue {
    key: MetadataTxnKey,
    version: u64,
    value: Option<MetadataTxnValue>,
}

#[derive(Debug)]
pub(super) struct SerialMetadataTxnStore {
    map: Mutex<BTreeMap<MetadataTxnKey, MetadataTxnEntry>>,
    next_entry_version: AtomicU64,
    profiler: Mutex<Option<MetadataTxnProfileBuffer>>,
}

#[derive(Debug)]
pub(super) struct ShardedMetadataTxnStore {
    shards: Vec<Mutex<BTreeMap<MetadataTxnKey, MetadataTxnEntry>>>,
    next_entry_version: AtomicU64,
    profiler: Mutex<Option<MetadataTxnProfileBuffer>>,
}

#[derive(Debug)]
pub(super) enum MetadataTxnStore {
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

pub(super) struct MetadataTxnCommitResult {
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

pub(super) fn metadata_txn_key_shard(key: &MetadataTxnKey, shard_count: usize) -> usize {
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

pub(super) fn raw_u128_low64(raw: u128) -> u64 {
    (raw & u128::from(u64::MAX)) as u64
}
