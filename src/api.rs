use crate::error::{Result, StorageError};
use crate::id::{
    BlockCount, BlockIndex, CheckpointId, ClientEpoch, CommitSeq, DeviceGeneration, DeviceId,
    LogicalDeadline, LogicalTime, RequestId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSpec {
    pub logical_blocks: u64,
    pub block_size: u32,
}

impl DeviceSpec {
    pub fn validate(&self) -> Result<()> {
        if self.logical_blocks == 0 {
            return Err(StorageError::invalid_argument(
                "logical_blocks must be greater than zero",
            ));
        }

        if self.block_size == 0 {
            return Err(StorageError::invalid_argument(
                "block_size must be greater than zero",
            ));
        }

        if !self.block_size.is_power_of_two() {
            return Err(StorageError::invalid_argument(
                "block_size must be a power of two",
            ));
        }

        Ok(())
    }

    pub fn logical_bytes(&self) -> Result<u64> {
        self.validate()?;
        self.logical_blocks
            .checked_mul(u64::from(self.block_size))
            .ok_or_else(|| StorageError::invalid_argument("device byte size overflows u64"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    pub device_id: DeviceId,
    pub generation: DeviceGeneration,
    pub spec: DeviceSpec,
    pub latest_commit: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDeviceRequest {
    pub spec: DeviceSpec,
    pub name: Option<String>,
}

impl CreateDeviceRequest {
    pub fn validate(&self) -> Result<()> {
        self.spec.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub const fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    pub fn end_exclusive(self) -> Result<u64> {
        self.offset
            .checked_add(self.len)
            .ok_or_else(|| StorageError::invalid_argument("byte range overflows u64"))
    }

    pub fn is_aligned_to(self, block_size: u32) -> bool {
        let block_size = u64::from(block_size);
        block_size != 0 && self.offset % block_size == 0 && self.len % block_size == 0
    }

    pub fn validate_for_device(self, spec: &DeviceSpec) -> Result<()> {
        spec.validate()?;

        if !self.is_aligned_to(spec.block_size) {
            return Err(StorageError::invalid_argument(
                "range offset and length must be block aligned",
            ));
        }

        if self.end_exclusive()? > spec.logical_bytes()? {
            return Err(StorageError::invalid_argument(
                "range extends past end of device",
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockOperation {
    Create,
    Info,
    Read,
    Write,
    Flush,
    WriteZeroes,
    Discard,
    Fork,
    Restore,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    pub start: BlockIndex,
    pub blocks: BlockCount,
}

impl BlockRange {
    pub const fn new(start: BlockIndex, blocks: BlockCount) -> Self {
        Self { start, blocks }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDurability {
    Acknowledged,
    Flushed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushScope {
    Device,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteCommit {
    pub device_id: DeviceId,
    pub commit_seq: CommitSeq,
    pub range: ByteRange,
    pub durability: WriteDurability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushResult {
    pub device_id: DeviceId,
    pub durable_through: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteResult {
    pub device_id: DeviceId,
    pub commit_seq: CommitSeq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkRequest {
    pub target: Option<DeviceId>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestorePoint {
    Commit(CommitSeq),
    Checkpoint(CheckpointId),
    Time(LogicalTime),
}

/// User-facing block-device handle.
///
/// Minimal implementor guarantees:
///
/// - Public reads and writes are block-aligned and bounded by the device spec.
/// - A successful write, zero write, discard, restore, or delete is atomic at
///   method-call granularity from the caller's perspective.
/// - Reads on the same device observe the latest successful committed mapping.
/// - Sparse committed ranges read as zero-filled bytes.
/// - Failed mutating operations leave the previous committed mapping readable.
/// - Segment bytes are made durable before metadata publishes reference them
///   when the selected durability level requires it.
/// - Shards, segment IDs, metadata node IDs, write intents, and commit groups
///   remain implementation details.
pub trait BlockDevice: Send + Sync {
    /// Return the stable ID of this device handle.
    ///
    /// The ID must not change for the lifetime of the handle.
    fn device_id(&self) -> DeviceId;

    /// Return committed device information.
    ///
    /// The returned generation and latest commit must describe committed state,
    /// not an in-flight write or partially published commit group.
    fn info(&self) -> Result<DeviceInfo>;

    /// Read bytes at a block-aligned offset.
    ///
    /// The implementation must fill the whole buffer or return an error. A
    /// zero-length buffer is a no-op. Reads must reject unaligned or
    /// out-of-bounds ranges before exposing data.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// Write bytes at a block-aligned offset.
    ///
    /// Success means the whole request is committed atomically from the caller's
    /// perspective. A failed write must not expose a partial mapping, even if
    /// segment bytes were already written and later need custodian cleanup. A
    /// zero-length write is a no-op and must not allocate segment data.
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<WriteCommit>;

    /// Flush previously acknowledged writes for this device.
    ///
    /// Success means every acknowledged commit through `durable_through` has
    /// reached the durability level promised by the implementation.
    fn flush(&self) -> Result<FlushResult>;

    /// Commit a block-aligned zero-filled range.
    ///
    /// Success must make future reads of the range return zeroes without
    /// exposing a partially updated mapping.
    fn write_zeroes(&self, offset: u64, len: u64) -> Result<WriteCommit>;

    /// Discard a block-aligned range.
    ///
    /// Discard changes logical mappings but does not promise immediate physical
    /// reclamation. Future reads of a discarded sparse range must return
    /// zeroes unless a later write covers it.
    fn discard(&self, offset: u64, len: u64) -> Result<WriteCommit>;

    /// Create a new device head that initially shares this device's roots.
    ///
    /// Fork must be O(1) with respect to logical size and metadata tree size:
    /// it copies root pointers and must not walk leaves or bump deep segment
    /// references.
    fn fork(&self, request: ForkRequest) -> Result<DeviceId>;

    /// Restore this device to a retained point in time as a new device.
    ///
    /// Restore must not mutate historical roots. Missing or expired restore
    /// points must fail without changing the source device.
    fn restore(&self, point: RestorePoint) -> Result<DeviceId>;

    /// Remove this live device from the catalog.
    ///
    /// Deletion must stop new operations from observing the live device head,
    /// but it does not synchronously free segment bytes.
    fn delete(&self) -> Result<DeleteResult>;
}

/// Public block-device control surface.
///
/// Implementors create/open devices without exposing internal shard layout or
/// provider placement. A later local or remote implementation should be able to
/// satisfy this trait without changing caller-facing semantics.
pub trait BlockClient: Send + Sync {
    /// Create a block device with user-visible shape from `request`.
    ///
    /// Success means the initial empty roots are committed and subsequent info
    /// or read calls can observe the device.
    fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId>;

    /// Return committed information for a device.
    ///
    /// The returned information must come from the latest committed device head.
    fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockRequest {
    Create {
        request: CreateDeviceRequest,
    },
    Info {
        device_id: DeviceId,
    },
    Read {
        device_id: DeviceId,
        range: ByteRange,
    },
    Write {
        device_id: DeviceId,
        offset: u64,
        bytes: Vec<u8>,
        durability: WriteDurability,
    },
    Flush {
        device_id: DeviceId,
        scope: FlushScope,
    },
    WriteZeroes {
        device_id: DeviceId,
        range: ByteRange,
    },
    Discard {
        device_id: DeviceId,
        range: ByteRange,
    },
    Fork {
        source: DeviceId,
        request: ForkRequest,
    },
    Restore {
        source: DeviceId,
        point: RestorePoint,
    },
    Delete {
        device_id: DeviceId,
    },
}

impl BlockRequest {
    pub const fn operation(&self) -> BlockOperation {
        match self {
            Self::Create { .. } => BlockOperation::Create,
            Self::Info { .. } => BlockOperation::Info,
            Self::Read { .. } => BlockOperation::Read,
            Self::Write { .. } => BlockOperation::Write,
            Self::Flush { .. } => BlockOperation::Flush,
            Self::WriteZeroes { .. } => BlockOperation::WriteZeroes,
            Self::Discard { .. } => BlockOperation::Discard,
            Self::Fork { .. } => BlockOperation::Fork,
            Self::Restore { .. } => BlockOperation::Restore,
            Self::Delete { .. } => BlockOperation::Delete,
        }
    }

    pub const fn target_device_id(&self) -> Option<DeviceId> {
        match self {
            Self::Info { device_id }
            | Self::Read { device_id, .. }
            | Self::Write { device_id, .. }
            | Self::Flush { device_id, .. }
            | Self::WriteZeroes { device_id, .. }
            | Self::Discard { device_id, .. }
            | Self::Delete { device_id } => Some(*device_id),
            Self::Fork { source, .. } | Self::Restore { source, .. } => Some(*source),
            Self::Create { .. } => None,
        }
    }

    pub fn byte_range(&self) -> Result<Option<ByteRange>> {
        match self {
            Self::Read { range, .. }
            | Self::WriteZeroes { range, .. }
            | Self::Discard { range, .. } => Ok(Some(*range)),
            Self::Write { offset, bytes, .. } => {
                let len = u64::try_from(bytes.len()).map_err(|_| {
                    StorageError::invalid_argument("write byte length overflows u64")
                })?;
                Ok(Some(ByteRange::new(*offset, len)))
            }
            Self::Create { .. }
            | Self::Info { .. }
            | Self::Flush { .. }
            | Self::Fork { .. }
            | Self::Restore { .. }
            | Self::Delete { .. } => Ok(None),
        }
    }

    pub fn validate_for_new_device(&self) -> Result<()> {
        match self {
            Self::Create { request } => request.validate(),
            Self::Info { .. }
            | Self::Read { .. }
            | Self::Write { .. }
            | Self::Flush { .. }
            | Self::WriteZeroes { .. }
            | Self::Discard { .. }
            | Self::Fork { .. }
            | Self::Restore { .. }
            | Self::Delete { .. } => Err(StorageError::invalid_argument(
                "request does not create a device",
            )),
        }
    }

    pub fn validate_for_existing_device(&self, spec: &DeviceSpec) -> Result<()> {
        spec.validate()?;

        match self {
            Self::Create { .. } => Err(StorageError::invalid_argument(
                "create request does not target an existing device",
            )),
            Self::Read { .. }
            | Self::Write { .. }
            | Self::WriteZeroes { .. }
            | Self::Discard { .. } => {
                if let Some(range) = self.byte_range()? {
                    range.validate_for_device(spec)?;
                }
                Ok(())
            }
            Self::Info { .. }
            | Self::Flush { .. }
            | Self::Fork { .. }
            | Self::Restore { .. }
            | Self::Delete { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResponse {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockResponse {
    Created(DeviceId),
    Info(DeviceInfo),
    Read(ReadResponse),
    Write(WriteCommit),
    Flush(FlushResult),
    Forked(DeviceId),
    Restored(DeviceId),
    Deleted(DeleteResult),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRequestEnvelope {
    /// Caller-chosen request identity used to match responses and retries.
    pub request_id: RequestId,
    /// Monotonic client incarnation used to reject stale retry streams.
    pub client_epoch: ClientEpoch,
    /// Optional deterministic deadline supplied by the caller.
    pub deadline: Option<LogicalDeadline>,
    /// Public block operation being requested.
    pub request: BlockRequest,
}

impl BlockRequestEnvelope {
    pub const fn new(
        request_id: RequestId,
        client_epoch: ClientEpoch,
        deadline: Option<LogicalDeadline>,
        request: BlockRequest,
    ) -> Self {
        Self {
            request_id,
            client_epoch,
            deadline,
            request,
        }
    }

    pub fn respond(&self, response: BlockResponse) -> BlockResponseEnvelope {
        BlockResponseEnvelope {
            request_id: self.request_id,
            response,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockResponseEnvelope {
    pub request_id: RequestId,
    pub response: BlockResponse,
}

/// Actor boundary for block requests.
///
/// `BlockServer` is a request coordinator for block-device semantics, not the
/// public name for a segment replica host. Future storage replication should be
/// coordinated below this API and above individual `SegmentStore` endpoints.
///
/// Minimal implementor guarantees:
///
/// - Preserve request identity in the response envelope.
/// - Validate public request shape before mutating provider state.
/// - Serialize or fence conflicting operations so callers never observe partial
///   commit groups.
/// - Translate block operations into shared substrate operations without
///   leaking shard, segment, or metadata-node details to callers.
/// - Keep retries idempotent or reject them deterministically by request ID and
///   client epoch.
pub trait BlockServer: Send + Sync {
    /// Handle one block request envelope.
    ///
    /// Success returns exactly one response for the supplied request ID.
    /// Failure must not leave caller-visible partial state.
    fn handle(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope>;
}

/// Transport boundary for block requests.
///
/// Minimal implementor guarantees:
///
/// - The transport may be local or remote, but it must not change storage
///   semantics.
/// - Responses must match the submitted request ID.
/// - Duplicate, delayed, reordered, or stale responses must be rejected or
///   surfaced as errors rather than silently applied to the wrong request.
/// - Transport failure does not imply storage failure; callers may need to
///   retry with the same request identity.
pub trait BlockTransport: Send + Sync {
    /// Send one block request and return the matching response envelope.
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec() -> DeviceSpec {
        DeviceSpec {
            logical_blocks: 8,
            block_size: 4096,
        }
    }

    #[test]
    fn device_spec_validates_basic_shape() {
        assert!(test_spec().validate().is_ok());

        assert!(
            DeviceSpec {
                logical_blocks: 0,
                block_size: 4096,
            }
            .validate()
            .is_err()
        );

        assert!(
            DeviceSpec {
                logical_blocks: 1,
                block_size: 3000,
            }
            .validate()
            .is_err()
        );

        assert!(
            DeviceSpec {
                logical_blocks: u64::MAX,
                block_size: 4096,
            }
            .logical_bytes()
            .is_err()
        );
    }

    #[test]
    fn byte_ranges_must_be_block_aligned_and_in_bounds() {
        let spec = test_spec();

        assert!(ByteRange::new(0, 4096).validate_for_device(&spec).is_ok());
        assert!(
            ByteRange::new(4096, 8192)
                .validate_for_device(&spec)
                .is_ok()
        );

        assert!(ByteRange::new(1, 4096).validate_for_device(&spec).is_err());
        assert!(ByteRange::new(0, 1).validate_for_device(&spec).is_err());
        assert!(
            ByteRange::new(8 * 4096, 4096)
                .validate_for_device(&spec)
                .is_err()
        );
        assert!(
            ByteRange::new(u64::MAX, 1)
                .validate_for_device(&spec)
                .is_err()
        );
    }

    #[test]
    fn zero_length_ranges_are_valid_noops_when_aligned_and_in_bounds() {
        let spec = test_spec();

        assert!(ByteRange::new(0, 0).validate_for_device(&spec).is_ok());
        assert!(
            ByteRange::new(8 * 4096, 0)
                .validate_for_device(&spec)
                .is_ok()
        );
        assert!(ByteRange::new(1, 0).validate_for_device(&spec).is_err());
    }

    #[test]
    fn block_request_exposes_its_target_device() {
        let device_id = DeviceId::from_raw(7);
        let request = BlockRequest::Write {
            device_id,
            offset: 0,
            bytes: vec![1, 2, 3, 4],
            durability: WriteDurability::Acknowledged,
        };

        assert_eq!(request.target_device_id(), Some(device_id));
        assert_eq!(
            BlockRequest::Create {
                request: CreateDeviceRequest {
                    spec: test_spec(),
                    name: None,
                },
            }
            .target_device_id(),
            None
        );
    }

    #[test]
    fn block_request_reports_operation_and_range_without_layout_leaks() {
        let read = BlockRequest::Read {
            device_id: DeviceId::from_raw(1),
            range: ByteRange::new(4096, 8192),
        };
        assert_eq!(read.operation(), BlockOperation::Read);
        assert_eq!(read.byte_range().unwrap(), Some(ByteRange::new(4096, 8192)));

        let write = BlockRequest::Write {
            device_id: DeviceId::from_raw(1),
            offset: 4096,
            bytes: vec![0; 8192],
            durability: WriteDurability::Acknowledged,
        };
        assert_eq!(write.operation(), BlockOperation::Write);
        assert_eq!(
            write.byte_range().unwrap(),
            Some(ByteRange::new(4096, 8192))
        );
    }

    #[test]
    fn block_request_validation_enforces_public_block_alignment() {
        let spec = test_spec();
        let aligned = BlockRequest::Write {
            device_id: DeviceId::from_raw(1),
            offset: 0,
            bytes: vec![0; 4096],
            durability: WriteDurability::Acknowledged,
        };
        assert!(aligned.validate_for_existing_device(&spec).is_ok());

        let unaligned = BlockRequest::Write {
            device_id: DeviceId::from_raw(1),
            offset: 1,
            bytes: vec![0; 4096],
            durability: WriteDurability::Acknowledged,
        };
        assert!(unaligned.validate_for_existing_device(&spec).is_err());

        let past_end = BlockRequest::Read {
            device_id: DeviceId::from_raw(1),
            range: ByteRange::new(8 * 4096, 4096),
        };
        assert!(past_end.validate_for_existing_device(&spec).is_err());
    }

    #[test]
    fn create_validation_is_separate_from_existing_device_validation() {
        let create = BlockRequest::Create {
            request: CreateDeviceRequest {
                spec: test_spec(),
                name: Some("root".to_string()),
            },
        };

        assert!(create.validate_for_new_device().is_ok());
        assert!(create.validate_for_existing_device(&test_spec()).is_err());

        let read = BlockRequest::Read {
            device_id: DeviceId::from_raw(1),
            range: ByteRange::new(0, 4096),
        };

        assert!(read.validate_for_new_device().is_err());
        assert!(read.validate_for_existing_device(&test_spec()).is_ok());
    }

    #[test]
    fn request_envelope_preserves_identity_in_response() {
        let request_id = RequestId::from_raw(99);
        let envelope = BlockRequestEnvelope::new(
            request_id,
            ClientEpoch::from_raw(1),
            Some(LogicalDeadline::from_raw(10)),
            BlockRequest::Info {
                device_id: DeviceId::from_raw(7),
            },
        );

        let response = envelope.respond(BlockResponse::Info(DeviceInfo {
            device_id: DeviceId::from_raw(7),
            generation: DeviceGeneration::from_raw(1),
            spec: test_spec(),
            latest_commit: CommitSeq::from_raw(0),
        }));

        assert_eq!(response.request_id, request_id);
    }

    #[test]
    fn opaque_ids_are_displayable_for_diagnostics() {
        assert_eq!(DeviceId::from_raw(42).to_string(), "42");
        assert_eq!(CommitSeq::from_raw(9).to_string(), "9");
        assert_eq!(crate::id::StorageNodeId::from_raw(7).to_string(), "7");
    }
}
