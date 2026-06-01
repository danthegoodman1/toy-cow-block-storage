/// Local block request coordinator.
pub(super) type RequestKey = (ClientEpoch, RequestId);
pub(super) const SERVER_LOCK_STRIPES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CachedBlockRequest {
    request: BlockRequest,
    result: Result<BlockResponseEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CachedNativeRequest {
    request: NativeRequest,
    result: Result<NativeResponseEnvelope>,
}

#[derive(Debug, Clone)]
pub struct LocalBlockServer {
    store: LocalCoordinator,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    responses: Arc<Mutex<BTreeMap<RequestKey, CachedBlockRequest>>>,
    stripes: Arc<Vec<Mutex<()>>>,
}

impl LocalBlockServer {
    pub fn new(store: LocalCoordinator) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            stripes: Arc::new(server_lock_stripes()),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }

    fn cached_response(
        &self,
        request: &BlockRequestEnvelope,
    ) -> Result<Option<BlockResponseEnvelope>> {
        let key = (request.client_epoch, request.request_id);
        let responses = lock(&self.responses)?;
        let Some(cached) = responses.get(&key) else {
            return Ok(None);
        };
        if cached.request != request.request {
            return Err(StorageError::conflict(
                "request ID and client epoch reused for a different block request",
            ));
        }
        cached.result.clone().map(Some)
    }

    fn remember_response(
        &self,
        request: &BlockRequestEnvelope,
        result: Result<BlockResponseEnvelope>,
    ) -> Result<BlockResponseEnvelope> {
        let key = (request.client_epoch, request.request_id);
        lock(&self.responses)?.insert(
            key,
            CachedBlockRequest {
                request: request.request.clone(),
                result: result.clone(),
            },
        );
        result
    }
}

impl BlockServer for LocalBlockServer {
    fn handle(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let _stripe_guard = lock(&self.stripes[block_request_stripe(&request.request)])?;
        if let Some(response) = self.cached_response(&request)? {
            return Ok(response);
        }
        lock(&self.request_log)?.push(request.request_id);
        let response = (|| -> Result<BlockResponse> {
            match request.request.clone() {
                BlockRequest::Create { request } => {
                    let head = self
                        .store
                        .metadata
                        .create_device(MetadataCreateDeviceRequest::from(request))?;
                    Ok(BlockResponse::Created(head.device_id))
                }
                BlockRequest::Info { device_id } => Ok(BlockResponse::Info(
                    self.store.metadata.device_info(device_id)?,
                )),
                BlockRequest::Read {
                    device_id,
                    range,
                    verification,
                } => {
                    let len = usize::try_from(range.len).map_err(|_| {
                        StorageError::invalid_argument("read byte length overflows usize")
                    })?;
                    let mut bytes = vec![0; len];
                    self.store.read_device_with_verification(
                        device_id,
                        range,
                        &mut bytes,
                        verification,
                    )?;
                    Ok(BlockResponse::Read(ReadResponse { bytes }))
                }
                BlockRequest::Write {
                    device_id,
                    offset,
                    bytes,
                    payload_integrity,
                    durability,
                } => Ok(BlockResponse::Write(
                    self.store.write_device_with_integrity(
                        device_id,
                        offset,
                        &bytes,
                        durability,
                        payload_integrity,
                    )?,
                )),
                BlockRequest::CommitBatch {
                    device_id,
                    writes,
                    durability,
                } => Ok(BlockResponse::BatchCommitted(
                    self.store.commit_block_batch(device_id, &writes, durability)?,
                )),
                BlockRequest::WriteZeroes { device_id, range } => Ok(BlockResponse::Write(
                    self.store
                        .write_zeroes(device_id, range.offset, range.len)?,
                )),
                BlockRequest::Discard { device_id, range } => Ok(BlockResponse::Write(
                    self.store
                        .discard_device(device_id, range.offset, range.len)?,
                )),
                BlockRequest::Flush { device_id, .. } => {
                    let info = self.store.metadata.device_info(device_id)?;
                    Ok(BlockResponse::Flush(FlushResult {
                        device_id,
                        durable_through: info.latest_commit,
                    }))
                }
                BlockRequest::Fork { source, request } => Ok(BlockResponse::Forked(
                    self.store.fork_device(source, request)?,
                )),
                BlockRequest::Restore { source, point } => Ok(BlockResponse::Restored(
                    self.store.restore_device(source, point)?,
                )),
                BlockRequest::Delete { device_id } => {
                    Ok(BlockResponse::Deleted(self.store.delete_device(device_id)?))
                }
            }
        })();
        let result = response.map(|response| BlockResponseEnvelope {
            request_id: request.request_id,
            response,
        });
        self.remember_response(&request, result)
    }
}

/// Local native keyspace/file request coordinator.
#[derive(Debug, Clone)]
pub struct LocalNativeServer {
    store: LocalCoordinator,
    request_log: Arc<Mutex<Vec<RequestId>>>,
    responses: Arc<Mutex<BTreeMap<RequestKey, CachedNativeRequest>>>,
    stripes: Arc<Vec<Mutex<()>>>,
}

impl LocalNativeServer {
    pub fn new(store: LocalCoordinator) -> Self {
        Self {
            store,
            request_log: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            stripes: Arc::new(server_lock_stripes()),
        }
    }

    pub fn request_log(&self) -> Result<Vec<RequestId>> {
        Ok(lock(&self.request_log)?.clone())
    }

    fn cached_response(
        &self,
        request: &NativeRequestEnvelope,
    ) -> Result<Option<NativeResponseEnvelope>> {
        let key = (request.client_epoch, request.request_id);
        let responses = lock(&self.responses)?;
        let Some(cached) = responses.get(&key) else {
            return Ok(None);
        };
        if cached.request != request.request {
            return Err(StorageError::conflict(
                "request ID and client epoch reused for a different native request",
            ));
        }
        cached.result.clone().map(Some)
    }

    fn remember_response(
        &self,
        request: &NativeRequestEnvelope,
        result: Result<NativeResponseEnvelope>,
    ) -> Result<NativeResponseEnvelope> {
        let key = (request.client_epoch, request.request_id);
        lock(&self.responses)?.insert(
            key,
            CachedNativeRequest {
                request: request.request.clone(),
                result: result.clone(),
            },
        );
        result
    }
}

impl NativeServer for LocalNativeServer {
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let _stripe_guard = lock(&self.stripes[native_request_stripe(&request.request)])?;
        if let Some(response) = self.cached_response(&request)? {
            return Ok(response);
        }
        lock(&self.request_log)?.push(request.request_id);
        let response = (|| -> Result<NativeResponse> {
            match request.request.clone() {
                NativeRequest::CreateKeyspace { request } => {
                    let head = self
                        .store
                        .metadata
                        .create_keyspace(MetadataCreateKeyspaceRequest { request })?;
                    Ok(NativeResponse::KeyspaceCreated(head.keyspace_id))
                }
                NativeRequest::KeyspaceInfo { keyspace_id } => Ok(NativeResponse::KeyspaceInfo(
                    self.store.metadata.get_keyspace_info(keyspace_id)?,
                )),
                NativeRequest::CreateFile {
                    keyspace_id,
                    request,
                } => {
                    let head = self.store.metadata.create_file(MetadataCreateFileRequest {
                        keyspace_id,
                        request,
                    })?;
                    Ok(NativeResponse::FileCreated(head.file_id))
                }
                NativeRequest::FileInfo {
                    keyspace_id,
                    file_id,
                } => Ok(NativeResponse::FileInfo(
                    self.store.metadata.get_file_info(keyspace_id, file_id)?,
                )),
                NativeRequest::Read {
                    keyspace_id,
                    file_id,
                    range,
                    verification,
                } => {
                    let len = usize::try_from(range.len).map_err(|_| {
                        StorageError::invalid_argument("read byte length overflows usize")
                    })?;
                    let mut bytes = vec![0; len];
                    self.store.read_file_with_verification(
                        keyspace_id,
                        file_id,
                        range,
                        &mut bytes,
                        verification,
                    )?;
                    Ok(NativeResponse::Read(ReadResponse { bytes }))
                }
                NativeRequest::CommitFileBatch {
                    keyspace_id,
                    file_id,
                    writes,
                    payload_integrity,
                    durability,
                } => Ok(NativeResponse::FileBatchCommitted(
                    self.store.commit_file_batch_with_integrity(
                        keyspace_id,
                        file_id,
                        &writes,
                        durability,
                        payload_integrity,
                    )?,
                )),
                NativeRequest::OpenAppendStream {
                    keyspace_id,
                    file_id,
                } => Ok(NativeResponse::AppendStreamOpened(
                    self.store.open_append_stream(keyspace_id, file_id)?,
                )),
                NativeRequest::AppendStream {
                    keyspace_id,
                    file_id,
                    stream,
                    bytes,
                    payload_integrity,
                } => {
                    if keyspace_id != stream.keyspace_id || file_id != stream.file_id {
                        Err(StorageError::invalid_argument(
                            "append stream target does not match request target",
                        ))
                    } else {
                        Ok(NativeResponse::AppendTicket(
                            self.store.append_stream_with_integrity(
                                &stream,
                                &bytes,
                                WriteDurability::Acknowledged,
                                payload_integrity,
                            )?,
                        ))
                    }
                }
                NativeRequest::FlushAppendStream {
                    keyspace_id,
                    file_id,
                    stream,
                } => {
                    if keyspace_id != stream.keyspace_id || file_id != stream.file_id {
                        Err(StorageError::invalid_argument(
                            "append stream target does not match request target",
                        ))
                    } else {
                        Ok(NativeResponse::DurableAppendMark(
                            self.store.flush_append_stream(&stream)?,
                        ))
                    }
                }
                NativeRequest::PublishAppendStream {
                    keyspace_id,
                    file_id,
                    stream,
                    mark,
                } => {
                    if keyspace_id != stream.keyspace_id || file_id != stream.file_id {
                        Err(StorageError::invalid_argument(
                            "append stream target does not match request target",
                        ))
                    } else {
                        Ok(NativeResponse::AppendPublished(
                            self.store.publish_append_stream(
                                &stream,
                                &mark,
                                WriteDurability::Acknowledged,
                            )?,
                        ))
                    }
                }
                NativeRequest::AbortAppendStream {
                    keyspace_id,
                    file_id,
                    stream,
                } => {
                    if keyspace_id != stream.keyspace_id || file_id != stream.file_id {
                        Err(StorageError::invalid_argument(
                            "append stream target does not match request target",
                        ))
                    } else {
                        self.store.abort_append_stream(&stream)?;
                        Ok(NativeResponse::AppendAborted)
                    }
                }
                NativeRequest::Flush {
                    keyspace_id,
                    file_id,
                } => {
                    let info = self.store.metadata.get_file_info(keyspace_id, file_id)?;
                    Ok(NativeResponse::Flush(FlushResult {
                        device_id: DeviceId::from_raw(info.file_id.raw()),
                        durable_through: self
                            .store
                            .metadata
                            .get_file_head(keyspace_id, file_id)?
                            .latest_commit,
                    }))
                }
                NativeRequest::CheckpointKeyspace { keyspace_id } => {
                    Ok(NativeResponse::KeyspaceCheckpointed(
                        self.store.metadata.checkpoint_keyspace(keyspace_id)?,
                    ))
                }
                NativeRequest::SnapshotKeyspace { source, request } => {
                    let head =
                        self.store
                            .metadata
                            .snapshot_keyspace(MetadataSnapshotKeyspaceRequest {
                                source,
                                target: request.target,
                                name: request.name,
                            })?;
                    Ok(NativeResponse::KeyspaceSnapshotted(head.keyspace_id))
                }
                NativeRequest::RestoreKeyspace { source, point } => Ok(
                    NativeResponse::KeyspaceRestored(self.store.restore_keyspace(source, point)?),
                ),
            }
        })();
        let result = response.map(|response| NativeResponseEnvelope {
            request_id: request.request_id,
            response,
        });
        self.remember_response(&request, result)
    }
}
