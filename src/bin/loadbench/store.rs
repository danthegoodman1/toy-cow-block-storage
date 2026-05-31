#[derive(Clone)]
enum BenchStore {
    Local(Arc<LocalCoordinator>),
    Durable(Arc<DurableCoordinator>),
    Txn(Arc<TxnBlockCoordinator>),
}

impl BenchStore {
    fn open(args: &Args, root: &Path) -> Result<Self> {
        match args.provider {
            ProviderKind::Local => Ok(Self::Local(Arc::new(LocalCoordinator::with_storage_nodes(
                args.config(),
                args.storage_node_ids(),
            )?))),
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
                    DurableCoordinator::open_with_storage_nodes_and_data_log_policy(
                        root,
                        args.config(),
                        args.storage_node_ids(),
                        DurableDataLogPolicy::default(),
                    )?,
                );
                if args.durable_profile_csv.is_some() {
                    store.enable_persist_profiling(DEFAULT_PROFILE_CAPACITY)?;
                }
                Ok(Self::Durable(store))
            }
        }
    }

    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_device(MetadataCreateDeviceRequest::from(request))
                .map(|head| head.device_id),
            Self::Durable(store) => store.create_device(request),
            Self::Txn(store) => store.create_device(request),
        }
    }

    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_keyspace(MetadataCreateKeyspaceRequest { request })
                .map(|head| head.keyspace_id),
            Self::Durable(store) => store.create_keyspace(request),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId> {
        match self {
            Self::Local(store) => store
                .metadata()
                .create_file(MetadataCreateFileRequest {
                    keyspace_id,
                    request,
                })
                .map(|head| head.file_id),
            Self::Durable(store) => store.create_file(keyspace_id, request),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
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
            Self::Local(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
            Self::Durable(store) => store.write_device_with_integrity(
                device_id,
                offset,
                data,
                durability,
                payload_integrity,
            ),
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

    fn read_device(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Durable(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
            Self::Txn(store) => {
                store.read_device_with_verification(device_id, range, buf, verification)
            }
        }
    }

    fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        match self {
            Self::Local(store) => {
                let info = store.metadata().device_info(device_id)?;
                Ok(FlushResult {
                    device_id,
                    durable_through: info.latest_commit,
                })
            }
            Self::Durable(store) => store.flush_device(device_id),
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
    ) -> Result<()> {
        match self {
            Self::Local(store) => store.commit_file_batch_with_integrity(
                keyspace_id,
                file_id,
                writes,
                durability,
                payload_integrity,
            ),
            Self::Durable(store) => store.commit_file_batch_with_integrity(
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
        .map(|_| ())
    }

    fn open_append_stream(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendStream> {
        match self {
            Self::Local(store) => store.open_append_stream(keyspace_id, file_id),
            Self::Durable(store) => store.open_append_stream(keyspace_id, file_id),
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
        self.append_stream(&stream, data, durability, payload_integrity)?;
        let mark = self.flush_append_stream(&stream)?;
        self.publish_append_stream(&stream, &mark)
    }

    fn append_stream(
        &self,
        stream: &AppendStream,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        match self {
            Self::Local(store) => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Durable(store) => {
                store.append_stream_with_integrity(stream, data, durability, payload_integrity)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn flush_append_stream(&self, stream: &AppendStream) -> Result<DurableAppendMark> {
        match self {
            Self::Local(store) => store.flush_append_stream(stream),
            Self::Durable(store) => store.flush_append_stream(stream),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn publish_append_stream(&self, stream: &AppendStream, mark: &DurableAppendMark) -> Result<()> {
        match self {
            Self::Local(store) => {
                store.publish_append_stream(stream, mark, WriteDurability::Acknowledged)
            }
            Self::Durable(store) => store.publish_append_stream(stream, mark),
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
            Self::Local(store) => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Durable(store) => {
                store.read_file_with_verification(keyspace_id, file_id, range, buf, verification)
            }
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn flush_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FlushResult> {
        match self {
            Self::Local(store) => {
                let head = store.metadata().get_file_head(keyspace_id, file_id)?;
                Ok(FlushResult {
                    device_id: DeviceId::from_raw(file_id.raw()),
                    durable_through: head.latest_commit,
                })
            }
            Self::Durable(store) => store.flush_file(keyspace_id, file_id),
            Self::Txn(_) => Err(StorageError::unsupported(
                "txn metadata provider is block-only in loadbench",
            )),
        }
    }

    fn drain_persist_profiles(&self, max: usize) -> Result<Vec<DurablePersistProfile>> {
        match self {
            Self::Local(_) => Ok(Vec::new()),
            Self::Durable(store) => store.drain_persist_profiles(max),
            Self::Txn(_) => Ok(Vec::new()),
        }
    }

    fn drain_metadata_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        match self {
            Self::Txn(store) => store.drain_metadata_profiles(max),
            Self::Local(_) | Self::Durable(_) => Ok(Vec::new()),
        }
    }

    fn drain_block_write_profiles(&self, max: usize) -> Result<Vec<TxnBlockWriteProfile>> {
        match self {
            Self::Txn(store) => store.drain_block_write_profiles(max),
            Self::Local(_) | Self::Durable(_) => Ok(Vec::new()),
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
    },
}
