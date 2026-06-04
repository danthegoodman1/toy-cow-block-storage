use crate::api::{
    ByteRange, FlushResult, PayloadIntegrity, ReadResponse, ReadVerification, RestorePoint,
    WriteDurability,
};
use crate::error::{Result, StorageError};
use crate::id::{
    AppendPublishTicketId, AppendStreamId, AppendTicketId, CheckpointId, ClientEpoch, CommitSeq,
    FileId, FileVersion, KeyspaceGeneration, KeyspaceId, LogicalDeadline, RequestId, WriterEpoch,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateKeyspaceRequest {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyspaceInfo {
    pub keyspace_id: KeyspaceId,
    pub generation: KeyspaceGeneration,
    pub latest_commit: CommitSeq,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotKeyspaceRequest {
    pub target: Option<KeyspaceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileSpec {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateFileRequest {
    pub spec: FileSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileInfo {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub size: u64,
    pub version: FileVersion,
}

/// Bearer authority for one active native append stream.
///
/// The stream token is the only authority that can flush or publish
/// flushed-but-unpublished private bytes. Implementations must not provide
/// implicit resume by file name or keyspace path: opening a new stream for the
/// same file is writer takeover at the visible file head and fences this token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendStream {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub stream_id: AppendStreamId,
    pub writer_epoch: WriterEpoch,
    pub base_version: FileVersion,
    pub visible_base_size: u64,
}

/// Diagnostic receipt for bytes accepted into a private append stream.
///
/// A ticket proves the in-process stream accepted a byte range, but it is not
/// publish authority and is not a restart-resume token.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendTicket {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub stream_id: AppendStreamId,
    pub ticket_id: AppendTicketId,
    pub writer_epoch: WriterEpoch,
    pub range: ByteRange,
}

/// Ticket for a captured append publish prefix.
///
/// The ticket is meaningful only with the matching stream identity. Waiting on
/// it drives or observes persistence of bytes through `publish_through` and the
/// atomic metadata publish that makes those bytes visible.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendPublishTicket {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub stream_id: AppendStreamId,
    pub ticket_id: AppendPublishTicketId,
    pub writer_epoch: WriterEpoch,
    pub publish_through: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendPublishCommit {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub range: ByteRange,
    pub version: FileVersion,
    pub commit_seq: CommitSeq,
    pub durability: WriteDurability,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileWriteCommit {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub range: ByteRange,
    pub version: FileVersion,
    pub commit_seq: CommitSeq,
    pub durability: WriteDurability,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FileBatchWrite {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl FileBatchWrite {
    pub fn new(offset: u64, bytes: Vec<u8>) -> Self {
        Self { offset, bytes }
    }

    pub fn byte_range(&self) -> Result<ByteRange> {
        let len = u64::try_from(self.bytes.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(self.offset, len);
        range.end_exclusive()?;
        Ok(range)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NativeOperation {
    CreateKeyspace,
    KeyspaceInfo,
    CreateFile,
    FileInfo,
    Read,
    CommitFileBatch,
    OpenAppendStream,
    AppendStream,
    SubmitAppendPublish,
    WaitAppendPublish,
    PublishAppendStream,
    ReleaseAppendStream,
    AbortAppendStream,
    Flush,
    CheckpointKeyspace,
    SnapshotKeyspace,
    RestoreKeyspace,
}

/// User-facing native file handle.
///
/// This API is a sibling of the block API over the shared segment substrate. It
/// keeps file-level writer intent visible to the coordinator instead of forcing
/// append ownership through block writes.
///
/// Minimal implementor guarantees:
///
/// - Successful writes are atomic file-version transitions.
/// - Append streams separate private ingest from visible publish: private bytes
///   remain invisible until append publish succeeds.
/// - Publish is the only globally discoverable append boundary: a replacement
///   writer that does not possess the stream token must open a new stream from
///   the latest visible file head.
/// - Publish is the only public append-stream durability boundary; successful
///   append ingest before publish is not a restart-resume guarantee.
/// - Publish does not release the append lease. `release_append_stream` and
///   `abort_append_stream` are explicit lease-ending operations.
/// - Stale append stream tokens fail without exposing partial file contents.
/// - Reads observe the latest visible file root/version in this keyspace.
/// - Failed writes and publishes leave the previous visible file version
///   readable, even when durable private bytes later need custodian cleanup.
pub trait NativeFile: Send + Sync {
    /// Return the stable ID of this file handle's keyspace.
    fn keyspace_id(&self) -> KeyspaceId;

    /// Return the stable ID of this file handle within its keyspace.
    fn file_id(&self) -> FileId;

    /// Return committed visible file information.
    fn info(&self) -> Result<FileInfo>;

    /// Read bytes from committed visible file extents.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.read_at_with_verification(offset, buf, ReadVerification::Default)
    }

    /// Read visible file bytes with an explicit payload verification policy.
    fn read_at_with_verification(
        &self,
        offset: u64,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()>;

    /// Write bytes at an arbitrary file offset.
    ///
    /// This is the one-write convenience case for `commit_batch`.
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<FileWriteCommit> {
        self.write_at_with_integrity(offset, data, PayloadIntegrity::Verified)
    }

    /// Write bytes at an arbitrary file offset with an explicit payload integrity
    /// policy.
    fn write_at_with_integrity(
        &self,
        offset: u64,
        data: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit> {
        let writes = [FileBatchWrite::new(offset, data.to_vec())];
        self.commit_batch_with_integrity(&writes, payload_integrity)
    }

    /// Atomically commit a set of arbitrary file writes as one visible
    /// file-version transition.
    ///
    /// Overlapping writes resolve by request order. Any active append stream for
    /// this same file is fenced before the batch can publish.
    fn commit_batch(&self, writes: &[FileBatchWrite]) -> Result<FileWriteCommit> {
        self.commit_batch_with_integrity(writes, PayloadIntegrity::Verified)
    }

    /// Commit a file write batch with an explicit payload integrity policy.
    fn commit_batch_with_integrity(
        &self,
        writes: &[FileBatchWrite],
        payload_integrity: PayloadIntegrity,
    ) -> Result<FileWriteCommit>;

    /// Open an append stream for this file.
    ///
    /// Opening a new stream for the same file fences the previous active stream.
    /// The new stream starts at the current visible file size.
    fn open_append_stream(&self) -> Result<AppendStream>;

    /// Ingest append bytes into the private stream.
    ///
    /// Success reserves a monotonically increasing byte range in the stream and
    /// stores the bytes privately. Readers do not see the bytes until a later
    /// publish. This is acknowledged private state, not a restart-resume
    /// guarantee. A zero-length append is invalid.
    fn append_stream(&self, stream: &AppendStream, data: &[u8]) -> Result<AppendTicket> {
        self.append_stream_with_integrity(stream, data, PayloadIntegrity::Verified)
    }

    /// Ingest append bytes with an explicit payload integrity policy.
    fn append_stream_with_integrity(
        &self,
        stream: &AppendStream,
        data: &[u8],
        payload_integrity: PayloadIntegrity,
    ) -> Result<AppendTicket>;

    /// Submit a publish request for a captured stream prefix.
    ///
    /// The prefix must be greater than the stream's published high-water and no
    /// greater than its accepted high-water. Appends beyond the captured prefix
    /// may continue before the ticket is waited.
    fn submit_append_publish(
        &self,
        stream: &AppendStream,
        publish_through: u64,
    ) -> Result<AppendPublishTicket>;

    /// Wait for a submitted publish to become durable and visible.
    ///
    /// Success is the globally durable/discoverable boundary for append data:
    /// reads, file stats, snapshots, forks, restores, and replacement writers
    /// observe the new visible file head.
    fn wait_append_publish(&self, ticket: &AppendPublishTicket) -> Result<AppendPublishCommit>;

    /// Submit and wait for a stream prefix publish.
    fn publish_append_stream(
        &self,
        stream: &AppendStream,
        publish_through: u64,
    ) -> Result<AppendPublishCommit> {
        let ticket = self.submit_append_publish(stream, publish_through)?;
        self.wait_append_publish(&ticket)
    }

    /// Release the active append stream without discarding already-published
    /// bytes. Any unpublished private tail becomes reclaimable.
    fn release_append_stream(&self, stream: &AppendStream) -> Result<()>;

    /// Abandon the active stream. Durable private bytes are no longer GC roots
    /// after abort and may be reclaimed by custodians.
    fn abort_append_stream(&self, stream: &AppendStream) -> Result<()>;

    /// Flush previously acknowledged native file writes.
    fn flush(&self) -> Result<FlushResult>;
}

/// Public native keyspace control surface.
///
/// Implementors create/open native keyspaces and files without exposing catalog
/// root layout, segment placement, or provider topology.
pub trait NativeKeyspaceClient: Send + Sync {
    /// Create an empty native keyspace.
    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId>;

    /// Return committed information for a native keyspace.
    fn keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo>;

    /// Create a native file inside a keyspace.
    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId>;

    /// Return visible committed information for a native file in a keyspace.
    fn file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo>;

    /// Open an append stream for a native file.
    fn open_append_stream(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendStream>;

    /// Checkpoint a native keyspace catalog root for PITR replay.
    fn checkpoint_keyspace(&self, keyspace_id: KeyspaceId) -> Result<CheckpointId>;

    /// Snapshot the current keyspace into a new keyspace lineage.
    fn snapshot_keyspace(
        &self,
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    ) -> Result<KeyspaceId>;

    /// Restore a retained keyspace point-in-time into a new keyspace lineage.
    fn restore_keyspace(&self, source: KeyspaceId, point: RestorePoint) -> Result<KeyspaceId>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NativeRequest {
    CreateKeyspace {
        request: CreateKeyspaceRequest,
    },
    KeyspaceInfo {
        keyspace_id: KeyspaceId,
    },
    CreateFile {
        keyspace_id: KeyspaceId,
        request: CreateFileRequest,
    },
    FileInfo {
        keyspace_id: KeyspaceId,
        file_id: FileId,
    },
    Read {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
        verification: ReadVerification,
    },
    CommitFileBatch {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        writes: Vec<FileBatchWrite>,
        payload_integrity: PayloadIntegrity,
        durability: WriteDurability,
    },
    OpenAppendStream {
        keyspace_id: KeyspaceId,
        file_id: FileId,
    },
    AppendStream {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        stream: AppendStream,
        bytes: Vec<u8>,
        payload_integrity: PayloadIntegrity,
    },
    SubmitAppendPublish {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        stream: AppendStream,
        publish_through: u64,
    },
    WaitAppendPublish {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        ticket: AppendPublishTicket,
    },
    PublishAppendStream {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        stream: AppendStream,
        publish_through: u64,
    },
    ReleaseAppendStream {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        stream: AppendStream,
    },
    AbortAppendStream {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        stream: AppendStream,
    },
    Flush {
        keyspace_id: KeyspaceId,
        file_id: FileId,
    },
    CheckpointKeyspace {
        keyspace_id: KeyspaceId,
    },
    SnapshotKeyspace {
        source: KeyspaceId,
        request: SnapshotKeyspaceRequest,
    },
    RestoreKeyspace {
        source: KeyspaceId,
        point: RestorePoint,
    },
}

impl NativeRequest {
    pub const fn operation(&self) -> NativeOperation {
        match self {
            Self::CreateKeyspace { .. } => NativeOperation::CreateKeyspace,
            Self::KeyspaceInfo { .. } => NativeOperation::KeyspaceInfo,
            Self::CreateFile { .. } => NativeOperation::CreateFile,
            Self::FileInfo { .. } => NativeOperation::FileInfo,
            Self::Read { .. } => NativeOperation::Read,
            Self::CommitFileBatch { .. } => NativeOperation::CommitFileBatch,
            Self::OpenAppendStream { .. } => NativeOperation::OpenAppendStream,
            Self::AppendStream { .. } => NativeOperation::AppendStream,
            Self::SubmitAppendPublish { .. } => NativeOperation::SubmitAppendPublish,
            Self::WaitAppendPublish { .. } => NativeOperation::WaitAppendPublish,
            Self::PublishAppendStream { .. } => NativeOperation::PublishAppendStream,
            Self::ReleaseAppendStream { .. } => NativeOperation::ReleaseAppendStream,
            Self::AbortAppendStream { .. } => NativeOperation::AbortAppendStream,
            Self::Flush { .. } => NativeOperation::Flush,
            Self::CheckpointKeyspace { .. } => NativeOperation::CheckpointKeyspace,
            Self::SnapshotKeyspace { .. } => NativeOperation::SnapshotKeyspace,
            Self::RestoreKeyspace { .. } => NativeOperation::RestoreKeyspace,
        }
    }

    pub const fn target_keyspace_id(&self) -> Option<KeyspaceId> {
        match self {
            Self::KeyspaceInfo { keyspace_id }
            | Self::CreateFile { keyspace_id, .. }
            | Self::FileInfo { keyspace_id, .. }
            | Self::Read { keyspace_id, .. }
            | Self::CommitFileBatch { keyspace_id, .. }
            | Self::OpenAppendStream { keyspace_id, .. }
            | Self::AppendStream { keyspace_id, .. }
            | Self::SubmitAppendPublish { keyspace_id, .. }
            | Self::WaitAppendPublish { keyspace_id, .. }
            | Self::PublishAppendStream { keyspace_id, .. }
            | Self::ReleaseAppendStream { keyspace_id, .. }
            | Self::AbortAppendStream { keyspace_id, .. }
            | Self::Flush { keyspace_id, .. }
            | Self::CheckpointKeyspace { keyspace_id } => Some(*keyspace_id),
            Self::SnapshotKeyspace { source, .. } | Self::RestoreKeyspace { source, .. } => {
                Some(*source)
            }
            Self::CreateKeyspace { .. } => None,
        }
    }

    pub const fn target_file_id(&self) -> Option<FileId> {
        match self {
            Self::FileInfo { file_id, .. }
            | Self::Read { file_id, .. }
            | Self::CommitFileBatch { file_id, .. }
            | Self::OpenAppendStream { file_id, .. }
            | Self::AppendStream { file_id, .. }
            | Self::SubmitAppendPublish { file_id, .. }
            | Self::WaitAppendPublish { file_id, .. }
            | Self::PublishAppendStream { file_id, .. }
            | Self::ReleaseAppendStream { file_id, .. }
            | Self::AbortAppendStream { file_id, .. }
            | Self::Flush { file_id, .. } => Some(*file_id),
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::CreateFile { .. }
            | Self::CheckpointKeyspace { .. }
            | Self::SnapshotKeyspace { .. }
            | Self::RestoreKeyspace { .. } => None,
        }
    }

    pub fn byte_range(&self) -> Result<Option<ByteRange>> {
        match self {
            Self::Read { range, .. } => Ok(Some(*range)),
            Self::CommitFileBatch { writes, .. } => {
                let mut start = u64::MAX;
                let mut end = 0u64;
                for write in writes {
                    let range = write.byte_range()?;
                    start = start.min(range.offset);
                    end = end.max(range.end_exclusive()?);
                }
                if start == u64::MAX {
                    Ok(None)
                } else {
                    Ok(Some(ByteRange::new(start, end - start)))
                }
            }
            Self::AppendStream { stream, bytes, .. } => {
                let len = u64::try_from(bytes.len()).map_err(|_| {
                    StorageError::invalid_argument("append byte length overflows u64")
                })?;
                Ok(Some(ByteRange::new(stream.visible_base_size, len)))
            }
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::CreateFile { .. }
            | Self::FileInfo { .. }
            | Self::OpenAppendStream { .. }
            | Self::SubmitAppendPublish { .. }
            | Self::WaitAppendPublish { .. }
            | Self::PublishAppendStream { .. }
            | Self::ReleaseAppendStream { .. }
            | Self::AbortAppendStream { .. }
            | Self::Flush { .. }
            | Self::CheckpointKeyspace { .. }
            | Self::SnapshotKeyspace { .. }
            | Self::RestoreKeyspace { .. } => Ok(None),
        }
    }

    pub fn validate_for_new_keyspace(&self) -> Result<()> {
        match self {
            Self::CreateKeyspace { .. } => Ok(()),
            _ => Err(StorageError::invalid_argument(
                "request does not create a keyspace",
            )),
        }
    }

    pub fn validate_for_new_file(&self) -> Result<()> {
        match self {
            Self::CreateFile { .. } => Ok(()),
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::FileInfo { .. }
            | Self::Read { .. }
            | Self::CommitFileBatch { .. }
            | Self::OpenAppendStream { .. }
            | Self::AppendStream { .. }
            | Self::SubmitAppendPublish { .. }
            | Self::WaitAppendPublish { .. }
            | Self::PublishAppendStream { .. }
            | Self::ReleaseAppendStream { .. }
            | Self::AbortAppendStream { .. }
            | Self::Flush { .. }
            | Self::CheckpointKeyspace { .. }
            | Self::SnapshotKeyspace { .. }
            | Self::RestoreKeyspace { .. } => Err(StorageError::invalid_argument(
                "request does not create a file",
            )),
        }
    }

    pub fn validate_for_existing_file(&self) -> Result<()> {
        match self {
            Self::CreateFile { .. } => Err(StorageError::invalid_argument(
                "create-file request does not target an existing file",
            )),
            Self::CommitFileBatch { writes, .. } => {
                if writes.is_empty() {
                    return Err(StorageError::invalid_argument(
                        "native file batch must not be empty",
                    ));
                }
                for write in writes {
                    write.byte_range()?;
                }
                Ok(())
            }
            Self::AppendStream {
                keyspace_id,
                file_id,
                stream,
                bytes,
                ..
            } => {
                if bytes.is_empty() {
                    return Err(StorageError::invalid_argument(
                        "append payload must not be empty",
                    ));
                }
                if *keyspace_id != stream.keyspace_id || *file_id != stream.file_id {
                    return Err(StorageError::invalid_argument(
                        "append stream target does not match request target",
                    ));
                }
                Ok(())
            }
            Self::SubmitAppendPublish {
                keyspace_id,
                file_id,
                stream,
                publish_through,
            }
            | Self::PublishAppendStream {
                keyspace_id,
                file_id,
                stream,
                publish_through,
            } => {
                if *keyspace_id != stream.keyspace_id || *file_id != stream.file_id {
                    return Err(StorageError::invalid_argument(
                        "append stream target does not match request target",
                    ));
                }
                if *publish_through <= stream.visible_base_size {
                    return Err(StorageError::invalid_argument(
                        "append publish target must advance past stream base",
                    ));
                }
                Ok(())
            }
            Self::ReleaseAppendStream {
                keyspace_id,
                file_id,
                stream,
            }
            | Self::AbortAppendStream {
                keyspace_id,
                file_id,
                stream,
            } => {
                if *keyspace_id != stream.keyspace_id || *file_id != stream.file_id {
                    return Err(StorageError::invalid_argument(
                        "append stream target does not match request target",
                    ));
                }
                Ok(())
            }
            Self::WaitAppendPublish {
                keyspace_id,
                file_id,
                ticket,
            } => {
                if *keyspace_id != ticket.keyspace_id || *file_id != ticket.file_id {
                    return Err(StorageError::invalid_argument(
                        "append publish target does not match ticket",
                    ));
                }
                Ok(())
            }
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::FileInfo { .. }
            | Self::Read { .. }
            | Self::OpenAppendStream { .. }
            | Self::Flush { .. }
            | Self::CheckpointKeyspace { .. }
            | Self::SnapshotKeyspace { .. }
            | Self::RestoreKeyspace { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NativeResponse {
    KeyspaceCreated(KeyspaceId),
    KeyspaceInfo(KeyspaceInfo),
    FileCreated(FileId),
    FileInfo(FileInfo),
    Read(ReadResponse),
    FileBatchCommitted(FileWriteCommit),
    AppendStreamOpened(AppendStream),
    AppendTicket(AppendTicket),
    AppendPublishSubmitted(AppendPublishTicket),
    AppendPublished(AppendPublishCommit),
    AppendReleased,
    AppendAborted,
    Flush(FlushResult),
    KeyspaceCheckpointed(CheckpointId),
    KeyspaceSnapshotted(KeyspaceId),
    KeyspaceRestored(KeyspaceId),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeRequestEnvelope {
    /// Caller-chosen request identity used to match responses and retries.
    pub request_id: RequestId,
    /// Monotonic client incarnation used to reject stale retry streams.
    pub client_epoch: ClientEpoch,
    /// Optional deterministic deadline supplied by the caller.
    pub deadline: Option<LogicalDeadline>,
    /// Public native keyspace/file operation being requested.
    pub request: NativeRequest,
}

impl NativeRequestEnvelope {
    pub const fn new(
        request_id: RequestId,
        client_epoch: ClientEpoch,
        deadline: Option<LogicalDeadline>,
        request: NativeRequest,
    ) -> Self {
        Self {
            request_id,
            client_epoch,
            deadline,
            request,
        }
    }

    pub fn respond(&self, response: NativeResponse) -> NativeResponseEnvelope {
        NativeResponseEnvelope {
            request_id: self.request_id,
            response,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeResponseEnvelope {
    pub request_id: RequestId,
    pub response: NativeResponse,
}

/// Actor boundary for native keyspace/file requests.
pub trait NativeServer: Send + Sync {
    /// Handle one native request envelope.
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope>;
}

/// Transport boundary for native keyspace/file requests.
pub trait NativeTransport: Send + Sync {
    /// Send one native request and return the matching response envelope.
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(keyspace_id: KeyspaceId, file_id: FileId) -> AppendStream {
        AppendStream {
            keyspace_id,
            file_id,
            stream_id: AppendStreamId::from_raw(9),
            writer_epoch: WriterEpoch::from_raw(3),
            base_version: FileVersion::from_raw(2),
            visible_base_size: 11,
        }
    }

    fn publish_ticket(keyspace_id: KeyspaceId, file_id: FileId) -> AppendPublishTicket {
        AppendPublishTicket {
            keyspace_id,
            file_id,
            stream_id: AppendStreamId::from_raw(9),
            ticket_id: AppendPublishTicketId::from_raw(10),
            writer_epoch: WriterEpoch::from_raw(3),
            publish_through: 14,
        }
    }

    #[test]
    fn native_requests_expose_operation_and_targets() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let file_id = FileId::from_raw(7);
        let request = NativeRequest::OpenAppendStream {
            keyspace_id,
            file_id,
        };

        assert_eq!(request.operation(), NativeOperation::OpenAppendStream);
        assert_eq!(request.target_keyspace_id(), Some(keyspace_id));
        assert_eq!(request.target_file_id(), Some(file_id));
        let write = NativeRequest::CommitFileBatch {
            keyspace_id,
            file_id,
            writes: vec![
                FileBatchWrite::new(16, vec![1, 2, 3]),
                FileBatchWrite::new(32, vec![4]),
            ],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        };
        assert_eq!(write.operation(), NativeOperation::CommitFileBatch);
        assert_eq!(write.target_keyspace_id(), Some(keyspace_id));
        assert_eq!(write.target_file_id(), Some(file_id));
        assert_eq!(write.byte_range().unwrap(), Some(ByteRange::new(16, 17)));
        assert_eq!(
            NativeRequest::CreateKeyspace {
                request: CreateKeyspaceRequest { name: None },
            }
            .target_keyspace_id(),
            None
        );
    }

    #[test]
    fn native_create_validation_is_separate_from_existing_file_validation() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let create = NativeRequest::CreateFile {
            keyspace_id,
            request: CreateFileRequest {
                spec: FileSpec {
                    name: Some("log".to_string()),
                },
            },
        };

        assert!(create.validate_for_new_file().is_ok());
        assert!(create.validate_for_existing_file().is_err());

        let create_keyspace = NativeRequest::CreateKeyspace {
            request: CreateKeyspaceRequest { name: None },
        };
        assert!(create_keyspace.validate_for_new_keyspace().is_ok());
        assert!(create_keyspace.validate_for_new_file().is_err());

        let info = NativeRequest::FileInfo {
            keyspace_id,
            file_id: FileId::from_raw(1),
        };
        assert!(info.validate_for_new_file().is_err());
        assert!(info.validate_for_existing_file().is_ok());
    }

    #[test]
    fn append_stream_validation_requires_matching_targets_and_payload() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let file_id = FileId::from_raw(7);
        let valid = NativeRequest::AppendStream {
            keyspace_id,
            file_id,
            stream: stream(keyspace_id, file_id),
            bytes: vec![1, 2, 3],
            payload_integrity: PayloadIntegrity::Verified,
        };
        assert!(valid.validate_for_existing_file().is_ok());

        let empty = NativeRequest::AppendStream {
            keyspace_id,
            file_id,
            stream: stream(keyspace_id, file_id),
            bytes: Vec::new(),
            payload_integrity: PayloadIntegrity::Verified,
        };
        assert!(empty.validate_for_existing_file().is_err());

        let mismatched_stream = NativeRequest::AppendStream {
            keyspace_id,
            file_id,
            stream: stream(keyspace_id, FileId::from_raw(8)),
            bytes: vec![1],
            payload_integrity: PayloadIntegrity::Verified,
        };
        assert!(mismatched_stream.validate_for_existing_file().is_err());

        let publish = NativeRequest::PublishAppendStream {
            keyspace_id,
            file_id,
            stream: stream(keyspace_id, file_id),
            publish_through: 14,
        };
        assert!(publish.validate_for_existing_file().is_ok());

        let wait = NativeRequest::WaitAppendPublish {
            keyspace_id,
            file_id,
            ticket: publish_ticket(keyspace_id, file_id),
        };
        assert!(wait.validate_for_existing_file().is_ok());
    }

    #[test]
    fn batch_validation_allows_empty_write_noop_and_rejects_empty_batch_and_overflow() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let file_id = FileId::from_raw(7);
        let empty_write = NativeRequest::CommitFileBatch {
            keyspace_id,
            file_id,
            writes: vec![FileBatchWrite::new(9, Vec::new())],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        };
        assert!(empty_write.validate_for_existing_file().is_ok());

        let empty_batch = NativeRequest::CommitFileBatch {
            keyspace_id,
            file_id,
            writes: Vec::new(),
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        };
        assert!(empty_batch.validate_for_existing_file().is_err());

        let overflowing = NativeRequest::CommitFileBatch {
            keyspace_id,
            file_id,
            writes: vec![FileBatchWrite::new(u64::MAX, vec![1])],
            payload_integrity: PayloadIntegrity::Verified,
            durability: WriteDurability::Acknowledged,
        };
        assert!(overflowing.validate_for_existing_file().is_err());
    }

    #[test]
    fn native_envelope_preserves_identity() {
        let request_id = RequestId::from_raw(11);
        let keyspace_id = KeyspaceId::from_raw(3);
        let envelope = NativeRequestEnvelope::new(
            request_id,
            ClientEpoch::from_raw(1),
            None,
            NativeRequest::FileInfo {
                keyspace_id,
                file_id: FileId::from_raw(4),
            },
        );

        let response = envelope.respond(NativeResponse::FileInfo(FileInfo {
            keyspace_id,
            file_id: FileId::from_raw(4),
            size: 0,
            version: FileVersion::from_raw(0),
        }));

        assert_eq!(response.request_id, request_id);
    }
}
