#[derive(Clone)]
enum BenchStore {
    Local {
        store: Arc<LocalCoordinator>,
        block_leases: Arc<Mutex<BTreeMap<DeviceId, BlockWriterLease>>>,
    },
    Durable {
        store: Arc<DurableCoordinator>,
        block_leases: Arc<Mutex<BTreeMap<DeviceId, BlockWriterLease>>>,
    },
    Txn(Arc<TxnBlockCoordinator>),
}

impl BenchStore {
    fn open(
        args: &Args,
        root: &Path,
        append_visible_journal: Option<PathBuf>,
    ) -> Result<Self> {
        match args.provider {
            ProviderKind::Local => {
                let store = Arc::new(LocalCoordinator::with_storage_nodes(
                    args.config(),
                    args.storage_node_ids(),
                )?);
                if args.read_profile_csv.is_some() {
                    store.enable_read_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.native_file_batch_commit_profile_csv.is_some() {
                    store.enable_native_file_batch_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Local {
                    store,
                    block_leases: Arc::new(Mutex::new(BTreeMap::new())),
                })
            }
            ProviderKind::TxnSerial => {
                let store = Arc::new(TxnBlockCoordinator::with_storage_nodes(
                    args.config(),
                    args.storage_node_ids(),
                    MetadataTxnMode::Serial,
                )?);
                if args.metadata_profile_csv.is_some() {
                    store.enable_metadata_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.block_write_profile_csv.is_some() {
                    store.enable_block_write_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Txn(store))
            }
            ProviderKind::TxnSharded => {
                let store = Arc::new(TxnBlockCoordinator::with_storage_nodes(
                    args.config(),
                    args.storage_node_ids(),
                    MetadataTxnMode::Sharded {
                        shard_count: args.shards,
                    },
                )?);
                if args.metadata_profile_csv.is_some() {
                    store.enable_metadata_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.block_write_profile_csv.is_some() {
                    store.enable_block_write_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Txn(store))
            }
            ProviderKind::Durable => {
                let store = Arc::new(
                    DurableCoordinator::open_with_storage_nodes_data_log_policy_append_visible_publish_journal_and_append_policies(
                        root,
                        args.config(),
                        args.storage_node_ids(),
                        DurableDataLogPolicy {
                            target_data_log_bytes: args.target_data_log_bytes,
                            file_sync_fanout: args.data_log_file_sync_fanout,
                            ..DurableDataLogPolicy::default()
                        },
                        append_visible_journal,
                        args.append_publish_batch_policy,
                        args.append_ingest_policy,
                    )?,
                );
                if args.durable_profile_csv.is_some() {
                    store.enable_persist_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.append_publish_profile_csv.is_some() {
                    store.enable_append_publish_wait_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.append_ingest_profile_csv.is_some() {
                    store.enable_append_ingest_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.read_profile_csv.is_some() {
                    store.enable_read_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                if args.native_file_batch_commit_profile_csv.is_some() {
                    store.enable_native_file_batch_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Durable {
                    store,
                    block_leases: Arc::new(Mutex::new(BTreeMap::new())),
                })
            }
        }
    }

    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        match self {
            Self::Local {
                store,
                block_leases,
            } => {
                let device_id = store
                    .metadata()
                    .create_device(MetadataCreateDeviceRequest::from(request))
                    .map(|head| head.device_id)?;
                let lease = store.acquire_block_writer(device_id)?;
                block_leases
                    .lock()
                    .map_err(|_| StorageError::unavailable("block lease cache poisoned"))?
                    .insert(device_id, lease);
                Ok(device_id)
            }
            Self::Durable {
                store,
                block_leases,
            } => {
                let device_id = store.create_device(request)?;
                let lease = store.acquire_block_writer(device_id)?;
                block_leases
                    .lock()
                    .map_err(|_| StorageError::unavailable("block lease cache poisoned"))?
                    .insert(device_id, lease);
                Ok(device_id)
            }
            Self::Txn(store) => store.create_device(request),
        }
    }

    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        match self {
            Self::Local { store, .. } => store
                .metadata()
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id),
            Self::Durable { store, .. } => store.create_keyspace(request),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId> {
        match self {
            Self::Local { store, .. } => store
                .metadata()
                .create_file(MetadataCreateFileRequest {
                    keyspace_id,
                    request,
                })
                .map(|head| head.file_id),
            Self::Durable { store, .. } => store.create_file(keyspace_id, request),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn block_lease(&self, device_id: DeviceId) -> Result<Option<BlockWriterLease>> {
        match self {
            Self::Local { block_leases, .. } | Self::Durable { block_leases, .. } => {
                Ok(Some(
                    block_leases
                        .lock()
                        .map_err(|_| StorageError::unavailable("block lease cache poisoned"))?
                        .get(&device_id)
                        .copied()
                        .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?,
                ))
            }
            Self::Txn(_) => Ok(None),
        }
    }

    fn write_device(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        match self {
            Self::Local { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.write_device_with_writer(&lease, offset, data, durability, payload_integrity)
            }
            Self::Durable { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.write_device_with_writer(&lease, offset, data, durability, payload_integrity)
            }
            Self::Txn(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
        }
        .map(|_| ())
    }

    fn commit_block_batch(
        &self,
        device_id: DeviceId,
        writes: &[BlockBatchWrite],
        durability: WriteDurability,
    ) -> Result<toy_cow_block_storage::BlockBatchCommit> {
        match self {
            Self::Local { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.commit_block_batch_with_writer(&lease, writes, durability)
            }
            Self::Durable { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.commit_block_batch_with_writer(&lease, writes, durability)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider does not implement block batch loadbench workloads",
            )),
        }
    }

    fn read_device(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        match self {
            Self::Local { store, .. } => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Durable { store, .. } => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Txn(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
        }
    }

    fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        match self {
            Self::Local { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.flush_device_with_writer(&lease)
            }
            Self::Durable { store, .. } => {
                let lease = self
                    .block_lease(device_id)?
                    .ok_or_else(|| StorageError::corrupt("missing block writer lease"))?;
                store.flush_device_with_writer(&lease)
            }
            Self::Txn(store) => store.flush_device(device_id),
        }
    }

    fn commit_file_batch(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: &[FileBatchWrite],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit> {
        match self {
            Self::Local { store, .. } => store.commit_file_batch_with_integrity(
                keyspace_id,
                file_id,
                writes,
                durability,
                payload_integrity,
            ),
            Self::Durable { store, .. } => store.commit_file_batch_with_integrity(
                keyspace_id,
                file_id,
                writes,
                durability,
                payload_integrity,
            ),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn open_append_stream(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendStream> {
        match self {
            Self::Local { store, .. } => store.open_append_stream(keyspace_id, file_id),
            Self::Durable { store, .. } => store.open_append_stream(keyspace_id, file_id),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn append_file_once(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<()> {
        let stream = self.open_append_stream(keyspace_id, file_id)?;
        let ticket = self.append_stream(&stream, data, durability, payload_integrity)?;
        self.publish_append_stream(&stream, ticket.range.end_exclusive()?)
    }

    fn append_stream(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        match self {
            Self::Local { store, .. } => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Durable { store, .. } => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn publish_append_stream(&self, stream: &AppendStream, publish_through: u64) -> Result<()> {
        match self {
            Self::Local { store, .. } => {
                store.publish_append_stream(stream, publish_through, WriteDurability::Acknowledged)
            }
            Self::Durable { store, .. } => store.publish_append_stream(stream, publish_through),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
        .map(|_| ())
    }

    fn read_file(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        match self {
            Self::Local { store, .. } => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Durable { store, .. } => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn flush_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FlushResult> {
        match self {
            Self::Local { store, .. } => {
                let head = store.metadata().get_file_head(keyspace_id, file_id)?;
                Ok(FlushResult {
                    device_id: DeviceId::from_raw(file_id.raw()),
                    durable_through: head.latest_commit,
                })
            }
            Self::Durable { store, .. } => store.flush_file(keyspace_id, file_id),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn drain_persist_profiles(&self, max: usize) -> Result<Vec<DurablePersistProfile>> {
        match self {
            Self::Local { .. } => Ok(Vec::new()),
            Self::Durable { store, .. } => store.drain_persist_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_append_publish_wait_profiles(
        &self,
        max: usize,
    ) -> Result<Vec<AppendPublishWaitProfile>> {
        match self {
            Self::Local { .. } => Ok(Vec::new()),
            Self::Durable { store, .. } => store.drain_append_publish_wait_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_append_ingest_profiles(&self, max: usize) -> Result<Vec<AppendIngestProfile>> {
        match self {
            Self::Local { .. } => Ok(Vec::new()),
            Self::Durable { store, .. } => store.drain_append_ingest_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_metadata_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        match self {
            Self::Txn(store) => store.drain_metadata_profiles(max),
            Self::Local { .. } | Self::Durable { .. } => Ok(Vec::new()),
        }
    }

    fn drain_block_write_profiles(&self, max: usize) -> Result<Vec<TxnBlockWriteProfile>> {
        match self {
            Self::Txn(store) => store.drain_block_write_profiles(max),
            Self::Local { .. } | Self::Durable { .. } => Ok(Vec::new()),
        }
    }

    fn drain_read_profiles(&self, max: usize) -> Result<Vec<ReadProfile>> {
        match self {
            Self::Local { store, .. } => store.drain_read_profiles(max),
            Self::Durable { store, .. } => store.drain_read_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_native_file_batch_commit_profiles(
        &self,
        max: usize,
    ) -> Result<Vec<NativeFileBatchCommitProfile>> {
        match self {
            Self::Local { store, .. } => store.drain_native_file_batch_commit_profiles(max),
            Self::Durable { store, .. } => store.drain_native_file_batch_commit_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }
}

#[derive(Clone)]
struct BenchContext {
    store: BenchStore,
    target: Target,
    payload: Arc<Vec<u8>>,
    op_size: usize,
}

#[derive(Clone)]
enum Target {
    Block {
        device_id: DeviceId,
        devices: Arc<Vec<DeviceId>>,
        logical_blocks: u64,
        hot_blocks: u64,
        shard_count: usize,
        serialized_lock: Arc<Mutex<()>>,
    },
    Native {
        keyspace_id: KeyspaceId,
        files: Arc<Vec<FileId>>,
        hot_append: Option<Arc<Mutex<HotAppendState>>>,
    },
    AppendLogMicrobench {
        root: Arc<PathBuf>,
    },
}

struct HotAppendState {
    stream: AppendStream,
    published_offset: u64,
}
