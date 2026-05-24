use crate::api::{ByteRange, FlushResult, ReadResponse, RestorePoint, WriteDurability};
use crate::error::{Result, StorageError};
use crate::id::{
    AppendLeaseId, CheckpointId, ClientEpoch, CommitSeq, ExtentId, FileId, FileVersion,
    KeyspaceGeneration, KeyspaceId, LogicalDeadline, RequestId, WriterEpoch,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateKeyspaceRequest {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceInfo {
    pub keyspace_id: KeyspaceId,
    pub generation: KeyspaceGeneration,
    pub latest_commit: CommitSeq,
    pub file_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotKeyspaceRequest {
    pub target: Option<KeyspaceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSpec {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFileRequest {
    pub spec: FileSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub size: u64,
    pub version: FileVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendLease {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub lease_id: AppendLeaseId,
    pub writer_epoch: WriterEpoch,
    pub base_version: FileVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendCommit {
    pub keyspace_id: KeyspaceId,
    pub file_id: FileId,
    pub extent_id: ExtentId,
    pub range: ByteRange,
    pub version: FileVersion,
    pub commit_seq: CommitSeq,
    pub durability: WriteDurability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOperation {
    CreateKeyspace,
    KeyspaceInfo,
    CreateFile,
    FileInfo,
    Read,
    AcquireAppend,
    Append,
    Flush,
    CheckpointKeyspace,
    SnapshotKeyspace,
    RestoreKeyspace,
}

/// User-facing native file handle.
///
/// This API is a sibling of the block API over the shared segment substrate. It
/// preserves file-level intent such as append leases and writer epochs while
/// keeping snapshots at the keyspace/filesystem boundary.
///
/// Minimal implementor guarantees:
///
/// - Successful appends are atomic file-version transitions inside one
///   keyspace catalog commit.
/// - Stale append leases and stale writer epochs fail without exposing partial
///   file contents.
/// - Reads observe the latest committed file root/version in this file's
///   keyspace.
/// - Failed appends leave the previous committed file version readable, even
///   when durable segment bytes later need custodian cleanup.
/// - Native file operations share write-intent, segment lifecycle, metadata,
///   and custodian machinery with the block mapping layer instead of being
///   implemented as ordinary block writes.
pub trait NativeFile: Send + Sync {
    /// Return the stable ID of this file handle's keyspace.
    fn keyspace_id(&self) -> KeyspaceId;

    /// Return the stable ID of this file handle within its keyspace.
    fn file_id(&self) -> FileId;

    /// Return committed file information.
    ///
    /// The returned size and version must describe committed state, not an
    /// in-flight append.
    fn info(&self) -> Result<FileInfo>;

    /// Read bytes from committed file extents.
    ///
    /// Implementors must fill the whole buffer or return an error. Reads may
    /// start and end at arbitrary byte offsets; segment/block alignment is an
    /// implementation detail. A zero-length buffer is a no-op. Reads past the
    /// committed file size must fail rather than synthesize data.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Acquire an append lease for this file.
    ///
    /// A lease carries the keyspace, file version, and writer epoch needed to
    /// fence stale append commits. Acquiring a new lease may invalidate older
    /// leases when the implementation supports lock stealing.
    fn acquire_append(&self) -> Result<AppendLease>;

    /// Append bytes using a previously acquired lease.
    ///
    /// Success means the byte payload is durable and committed as one
    /// file-version transition. Payload length does not need to be block
    /// aligned. Stale leases must fail without exposing partial file data. A
    /// zero-length append is invalid because it creates no useful extent or
    /// version transition.
    fn append_with_lease(&self, lease: AppendLease, data: &[u8]) -> Result<AppendCommit>;

    /// Flush previously acknowledged native file writes.
    ///
    /// Success means every acknowledged commit through the returned sequence has
    /// reached the durability level promised by the implementation.
    fn flush(&self) -> Result<FlushResult>;
}

/// Public native keyspace control surface.
///
/// Implementors create/open native keyspaces and files without exposing catalog
/// root layout, segment placement, or provider topology.
pub trait NativeKeyspaceClient: Send + Sync {
    /// Create an empty native keyspace.
    ///
    /// Success means an empty immutable keyspace catalog root is committed.
    fn create_keyspace(&self, request: CreateKeyspaceRequest) -> Result<KeyspaceId>;

    /// Return committed information for a native keyspace.
    fn keyspace_info(&self, keyspace_id: KeyspaceId) -> Result<KeyspaceInfo>;

    /// Create a native file inside a keyspace.
    ///
    /// Success means the keyspace catalog has atomically advanced to include
    /// the initial empty file root.
    fn create_file(&self, keyspace_id: KeyspaceId, request: CreateFileRequest) -> Result<FileId>;

    /// Return committed information for a native file in a keyspace.
    fn file_info(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<FileInfo>;

    /// Acquire an append lease for a native file.
    ///
    /// The lease must be fenced by the committed file version observed by the
    /// server and a writer epoch that stale writers cannot reuse successfully.
    fn acquire_append(&self, keyspace_id: KeyspaceId, file_id: FileId) -> Result<AppendLease>;

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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    },
    AcquireAppend {
        keyspace_id: KeyspaceId,
        file_id: FileId,
    },
    Append {
        keyspace_id: KeyspaceId,
        file_id: FileId,
        lease: AppendLease,
        bytes: Vec<u8>,
        durability: WriteDurability,
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
            Self::AcquireAppend { .. } => NativeOperation::AcquireAppend,
            Self::Append { .. } => NativeOperation::Append,
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
            | Self::AcquireAppend { keyspace_id, .. }
            | Self::Append { keyspace_id, .. }
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
            | Self::AcquireAppend { file_id, .. }
            | Self::Append { file_id, .. }
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
            Self::Append { bytes, .. } => {
                let len = u64::try_from(bytes.len()).map_err(|_| {
                    StorageError::invalid_argument("append byte length overflows u64")
                })?;
                Ok(Some(ByteRange::new(0, len)))
            }
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::CreateFile { .. }
            | Self::FileInfo { .. }
            | Self::AcquireAppend { .. }
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
            | Self::AcquireAppend { .. }
            | Self::Append { .. }
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
            Self::Append {
                keyspace_id,
                file_id,
                lease,
                bytes,
                ..
            } => {
                if bytes.is_empty() {
                    return Err(StorageError::invalid_argument(
                        "append payload must not be empty",
                    ));
                }

                if *keyspace_id != lease.keyspace_id || *file_id != lease.file_id {
                    return Err(StorageError::invalid_argument(
                        "append lease target does not match request target",
                    ));
                }

                Ok(())
            }
            Self::CreateKeyspace { .. }
            | Self::KeyspaceInfo { .. }
            | Self::FileInfo { .. }
            | Self::Read { .. }
            | Self::AcquireAppend { .. }
            | Self::Flush { .. }
            | Self::CheckpointKeyspace { .. }
            | Self::SnapshotKeyspace { .. }
            | Self::RestoreKeyspace { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeResponse {
    KeyspaceCreated(KeyspaceId),
    KeyspaceInfo(KeyspaceInfo),
    FileCreated(FileId),
    FileInfo(FileInfo),
    Read(ReadResponse),
    Append(AppendCommit),
    AppendLease(AppendLease),
    Flush(FlushResult),
    KeyspaceCheckpointed(CheckpointId),
    KeyspaceSnapshotted(KeyspaceId),
    KeyspaceRestored(KeyspaceId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeResponseEnvelope {
    pub request_id: RequestId,
    pub response: NativeResponse,
}

/// Actor boundary for native keyspace/file requests.
///
/// `NativeServer` is a request coordinator for native keyspace semantics.
/// Future storage replication should be coordinated below this API and above
/// individual `SegmentStore` endpoints.
///
/// Minimal implementor guarantees:
///
/// - Preserve request identity in the response envelope.
/// - Validate public request shape before mutating provider state.
/// - Fence conflicting append writers with keyspace, file versions, and writer
///   epochs.
/// - Translate native operations into shared substrate operations without
///   routing them through block-device logical mappings.
/// - Keep retries idempotent or reject them deterministically by request ID and
///   client epoch.
pub trait NativeServer: Send + Sync {
    /// Handle one native request envelope.
    ///
    /// Success returns exactly one response for the supplied request ID.
    /// Failure must not leave caller-visible partial state.
    fn handle(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope>;
}

/// Transport boundary for native keyspace/file requests.
///
/// Minimal implementor guarantees:
///
/// - The transport may be local or remote, but it must not change native
///   keyspace or file semantics.
/// - Responses must match the submitted request ID.
/// - Duplicate, delayed, reordered, or stale responses must be rejected or
///   surfaced as errors rather than silently applied to the wrong request.
/// - Transport failure does not imply storage failure; callers may need to
///   retry with the same request identity.
pub trait NativeTransport: Send + Sync {
    /// Send one native request and return the matching response envelope.
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(keyspace_id: KeyspaceId, file_id: FileId) -> AppendLease {
        AppendLease {
            keyspace_id,
            file_id,
            lease_id: AppendLeaseId::from_raw(9),
            writer_epoch: WriterEpoch::from_raw(3),
            base_version: FileVersion::from_raw(2),
        }
    }

    #[test]
    fn native_requests_expose_operation_and_targets() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let file_id = FileId::from_raw(7);
        let request = NativeRequest::AcquireAppend {
            keyspace_id,
            file_id,
        };

        assert_eq!(request.operation(), NativeOperation::AcquireAppend);
        assert_eq!(request.target_keyspace_id(), Some(keyspace_id));
        assert_eq!(request.target_file_id(), Some(file_id));
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
    fn append_validation_requires_matching_lease_and_payload() {
        let keyspace_id = KeyspaceId::from_raw(5);
        let file_id = FileId::from_raw(7);
        let valid = NativeRequest::Append {
            keyspace_id,
            file_id,
            lease: lease(keyspace_id, file_id),
            bytes: vec![1, 2, 3],
            durability: WriteDurability::Acknowledged,
        };
        assert!(valid.validate_for_existing_file().is_ok());

        let empty = NativeRequest::Append {
            keyspace_id,
            file_id,
            lease: lease(keyspace_id, file_id),
            bytes: Vec::new(),
            durability: WriteDurability::Acknowledged,
        };
        assert!(empty.validate_for_existing_file().is_err());

        let mismatched = NativeRequest::Append {
            keyspace_id,
            file_id,
            lease: lease(keyspace_id, FileId::from_raw(8)),
            bytes: vec![1],
            durability: WriteDurability::Acknowledged,
        };
        assert!(mismatched.validate_for_existing_file().is_err());
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
