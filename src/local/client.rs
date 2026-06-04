/// Local `BlockClient` backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockClient {
    transport: Arc<dyn BlockTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockClient {
    pub fn new(transport: InProcessBlockTransport) -> Self {
        Self::with_transport(Arc::new(transport))
    }

    pub fn with_transport(transport: Arc<dyn BlockTransport>) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_device(&self, device_id: DeviceId) -> Result<LocalBlockDevice> {
        self.device_info(device_id)?;
        Ok(LocalBlockDevice {
            device_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockClient for LocalBlockClient {
    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Create { request },
        ))?;
        match response.response {
            BlockResponse::Created(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected create-device response")),
        }
    }

    fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info { device_id },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }
}

/// Local `BlockDevice` handle backed by a block transport.
#[derive(Clone)]
pub struct LocalBlockDevice {
    device_id: DeviceId,
    transport: Arc<dyn BlockTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalBlockDevice {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl BlockDevice for LocalBlockDevice {
    fn device_id(&self) -> DeviceId {
        self.device_id
    }

    fn info(&self) -> Result<DeviceInfo> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Info {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Info(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected device-info response")),
        }
    }

    fn read_at_with_verification(
        &self,
        offset: u64,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Read {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
                verification,
            },
        ))?;
        match response.response {
            BlockResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected block-read response")),
        }
    }

    fn write_at_with_integrity(
        &self,
        offset: u64,
        data: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<WriteCommit> {
        let range = ByteRange::new(
            offset,
            u64::try_from(data.len())
                .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?,
        );
        if data.is_empty() {
            let info = self.info()?;
            return Ok(WriteCommit {
                device_id: self.device_id,
                commit_seq: info.latest_commit,
                range,
                durability: crate::api::WriteDurability::Acknowledged,
            });
        }
        let commit = self.commit_batch(&[BlockBatchWrite {
            offset,
            bytes: data.to_vec(),
            payload_integrity,
        }])?;
        Ok(WriteCommit {
            device_id: commit.device_id,
            commit_seq: commit.commit_seq,
            range,
            durability: commit.durability,
        })
    }

    fn commit_batch(&self, writes: &[BlockBatchWrite]) -> Result<BlockBatchCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::CommitBatch {
                device_id: self.device_id,
                durability: crate::api::WriteDurability::Acknowledged,
                writes: writes.to_vec(),
            },
        ))?;
        match response.response {
            BlockResponse::BatchCommitted(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected block-batch response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Flush {
                device_id: self.device_id,
                scope: crate::api::FlushScope::Device,
            },
        ))?;
        match response.response {
            BlockResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected block-flush response")),
        }
    }

    fn write_zeroes(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::WriteZeroes {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected write-zeroes response")),
        }
    }

    fn discard(&self, offset: u64, len: u64) -> Result<WriteCommit> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Discard {
                device_id: self.device_id,
                range: ByteRange::new(offset, len),
            },
        ))?;
        match response.response {
            BlockResponse::Write(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected discard response")),
        }
    }

    fn fork(&self, request: ForkRequest) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Fork {
                source: self.device_id,
                request,
            },
        ))?;
        match response.response {
            BlockResponse::Forked(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected fork response")),
        }
    }

    fn restore(&self, point: RestorePoint) -> Result<DeviceId> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Restore {
                source: self.device_id,
                point,
            },
        ))?;
        match response.response {
            BlockResponse::Restored(device_id) => Ok(device_id),
            _ => Err(StorageError::corrupt("unexpected restore response")),
        }
    }

    fn delete(&self) -> Result<DeleteResult> {
        let response = self.transport.call(BlockRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            BlockRequest::Delete {
                device_id: self.device_id,
            },
        ))?;
        match response.response {
            BlockResponse::Deleted(delete) => Ok(delete),
            _ => Err(StorageError::corrupt("unexpected block-delete response")),
        }
    }
}

/// Local `NativeKeyspaceClient` backed by a native transport.
#[derive(Clone)]
pub struct LocalNativeClient {
    transport: Arc<dyn NativeTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeClient {
    pub fn new(transport: InProcessNativeTransport) -> Self {
        Self::with_transport(Arc::new(transport))
    }

    pub fn with_transport(transport: Arc<dyn NativeTransport>) -> Self {
        Self {
            transport,
            client_epoch: crate::id::ClientEpoch::from_raw(1),
            next_request_id: Arc::new(Mutex::new(1)),
        }
    }

    pub fn open_file(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<LocalNativeFile> {
        self.file_info(keyspace_id, file_id)?;
        Ok(LocalNativeFile {
            keyspace_id,
            file_id,
            transport: self.transport.clone(),
            client_epoch: self.client_epoch,
            next_request_id: Arc::clone(&self.next_request_id),
        })
    }

    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeKeyspaceClient for LocalNativeClient {
    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CreateKeyspace { request },
        ))?;
        match response.response {
            NativeResponse::KeyspaceCreated(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt("unexpected create-keyspace response")),
        }
    }

    fn keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::KeyspaceInfo { keyspace_id },
        ))?;
        match response.response {
            NativeResponse::KeyspaceInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected keyspace-info response")),
        }
    }

    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CreateFile {
                keyspace_id,
                request,
            },
        ))?;
        match response.response {
            NativeResponse::FileCreated(file_id) => Ok(file_id),
            _ => Err(StorageError::corrupt("unexpected create-file response")),
        }
    }

    fn file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo {
                keyspace_id,
                file_id,
            },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn open_append_stream(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendStream> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::OpenAppendStream {
                keyspace_id,
                file_id,
            },
        ))?;
        match response.response {
            NativeResponse::AppendStreamOpened(stream) => Ok(stream),
            _ => Err(StorageError::corrupt("unexpected append-stream response")),
        }
    }

    fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CheckpointKeyspace { keyspace_id },
        ))?;
        match response.response {
            NativeResponse::KeyspaceCheckpointed(checkpoint_id) => Ok(checkpoint_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-checkpoint response",
            )),
        }
    }

    fn snapshot_keyspace(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::SnapshotKeyspace { source, request },
        ))?;
        match response.response {
            NativeResponse::KeyspaceSnapshotted(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-snapshot response",
            )),
        }
    }

    fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::RestoreKeyspace { source, point },
        ))?;
        match response.response {
            NativeResponse::KeyspaceRestored(keyspace_id) => Ok(keyspace_id),
            _ => Err(StorageError::corrupt(
                "unexpected keyspace-restore response",
            )),
        }
    }
}

/// Local `NativeFile` handle backed by a native keyspace/file transport.
#[derive(Clone)]
pub struct LocalNativeFile {
    keyspace_id: KeyspaceId,
    file_id: FileId,
    transport: Arc<dyn NativeTransport>,
    client_epoch: crate::id::ClientEpoch,
    next_request_id: Arc<Mutex<u128>>,
}

impl LocalNativeFile {
    fn next_request_id(&self) -> Result<RequestId> {
        next_request_id(&self.next_request_id)
    }
}

impl NativeFile for LocalNativeFile {
    fn keyspace_id(&self) -> KeyspaceId {
        self.keyspace_id
    }

    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn info(&self) -> Result<FileInfo> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::FileInfo {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::FileInfo(info) => Ok(info),
            _ => Err(StorageError::corrupt("unexpected file-info response")),
        }
    }

    fn read_at_with_verification(
        &self,
        offset: u64,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Read {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                range: ByteRange::new(offset, len),
                verification,
            },
        ))?;
        match response.response {
            NativeResponse::Read(read) => {
                if read.bytes.len() != buf.len() {
                    return Err(StorageError::corrupt(
                        "read response length does not match request",
                    ));
                }
                buf.copy_from_slice(&read.bytes);
                Ok(())
            }
            _ => Err(StorageError::corrupt("unexpected native-read response")),
        }
    }

    fn commit_batch_with_integrity(
        &self,
        writes: &[FileBatchWrite],
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::CommitFileBatch {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                writes: writes.to_vec(),
                payload_integrity,
                durability: crate::api::WriteDurability::Acknowledged,
            },
        ))?;
        match response.response {
            NativeResponse::FileBatchCommitted(commit) => Ok(commit),
            _ => Err(StorageError::corrupt(
                "unexpected native-file-batch response",
            )),
        }
    }

    fn open_append_stream(&self) -> Result<AppendStream> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::OpenAppendStream {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::AppendStreamOpened(stream) => Ok(stream),
            _ => Err(StorageError::corrupt("unexpected append-stream response")),
        }
    }

    fn append_stream_with_integrity(
        &self,
        stream: &AppendStream,
        data: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::AppendStream {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                stream: stream.clone(),
                bytes: data.to_vec(),
                payload_integrity,
            },
        ))?;
        match response.response {
            NativeResponse::AppendTicket(ticket) => Ok(ticket),
            _ => Err(StorageError::corrupt("unexpected append-ticket response")),
        }
    }

    fn submit_append_publish(
        &self,
        stream: &AppendStream,
        publish_through: u64,
    ) -> Result<AppendPublishTicket> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::SubmitAppendPublish {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                stream: stream.clone(),
                publish_through,
            },
        ))?;
        match response.response {
            NativeResponse::AppendPublishSubmitted(ticket) => Ok(ticket),
            _ => Err(StorageError::corrupt("unexpected append-publish-submit response")),
        }
    }

    fn wait_append_publish(&self, ticket: &AppendPublishTicket) -> Result<AppendPublishCommit> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::WaitAppendPublish {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                ticket: ticket.clone(),
            },
        ))?;
        match response.response {
            NativeResponse::AppendPublished(commit) => Ok(commit),
            _ => Err(StorageError::corrupt("unexpected append-publish-wait response")),
        }
    }

    fn release_append_stream(&self, stream: &AppendStream) -> Result<()> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::ReleaseAppendStream {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                stream: stream.clone(),
            },
        ))?;
        match response.response {
            NativeResponse::AppendReleased => Ok(()),
            _ => Err(StorageError::corrupt("unexpected append-release response")),
        }
    }

    fn abort_append_stream(&self, stream: &AppendStream) -> Result<()> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::AbortAppendStream {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
                stream: stream.clone(),
            },
        ))?;
        match response.response {
            NativeResponse::AppendAborted => Ok(()),
            _ => Err(StorageError::corrupt("unexpected append-abort response")),
        }
    }

    fn flush(&self) -> Result<FlushResult> {
        let response = self.transport.call(NativeRequestEnvelope::new(
            self.next_request_id()?,
            self.client_epoch,
            None,
            NativeRequest::Flush {
                keyspace_id: self.keyspace_id,
                file_id: self.file_id,
            },
        ))?;
        match response.response {
            NativeResponse::Flush(flush) => Ok(flush),
            _ => Err(StorageError::corrupt("unexpected native-flush response")),
        }
    }
}
