#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadPlan {
    logical_len: u64,
    extents: Vec<ReadExtent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadExtent {
    output_offset: u64,
    len: u64,
    source: ReadSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReadSource {
    Zero,
    Segment {
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        segment_offset: u64,
        integrity: SegmentPayloadIntegrity,
    },
    AppendRun {
        storage_node: StorageNodeId,
        log_id: u64,
        payload_offset: u64,
        integrity: SegmentPayloadIntegrity,
    },
}

pub(super) trait MetadataReadService {
    fn resolve_block_read(&self, device_id: DeviceId, range: ByteRange) -> Result<ReadPlan>;

    fn resolve_file_read(
        &self,
        keyspace_id: KeyspaceId,
        file_id: FileId,
        range: ByteRange,
    ) -> Result<ReadPlan>;
}

pub(super) trait StorageNodeReadService {
    fn read_segment_source(
        &self,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        range: ByteRange,
        integrity: SegmentPayloadIntegrity,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<()>;

    fn read_append_run_source(
        &self,
        storage_node: StorageNodeId,
        log_id: u64,
        range: ByteRange,
        integrity: SegmentPayloadIntegrity,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<()>;
}

impl ReadPlan {
    fn from_non_zero_extents(logical_len: u64, mut extents: Vec<ReadExtent>) -> Result<Self> {
        extents.sort_by_key(|extent| extent.output_offset);
        let mut out = Vec::with_capacity(extents.len().saturating_add(1));
        let mut cursor = 0_u64;
        for extent in extents {
            if extent.len == 0 {
                return Err(StorageError::invalid_argument(
                    "read plan extent must not be empty",
                ));
            }
            let end = extent
                .output_offset
                .checked_add(extent.len)
                .ok_or_else(|| StorageError::invalid_argument("read plan extent overflows"))?;
            if end > logical_len {
                return Err(StorageError::corrupt(
                    "read plan extent exceeds logical output length",
                ));
            }
            if extent.output_offset < cursor {
                return Err(StorageError::corrupt(
                    "read plan contains overlapping non-zero extents",
                ));
            }
            if extent.output_offset > cursor {
                out.push(ReadExtent {
                    output_offset: cursor,
                    len: extent.output_offset - cursor,
                    source: ReadSource::Zero,
                });
            }
            cursor = end;
            out.push(extent);
        }
        if cursor < logical_len {
            out.push(ReadExtent {
                output_offset: cursor,
                len: logical_len - cursor,
                source: ReadSource::Zero,
            });
        }
        Ok(Self {
            logical_len,
            extents: out,
        })
    }
}

fn read_output_slice(buf: &mut [u8], output_offset: u64, len: u64) -> Result<&mut [u8]> {
    let start = usize::try_from(output_offset)
        .map_err(|_| StorageError::invalid_argument("read output offset overflows usize"))?;
    let len = usize::try_from(len)
        .map_err(|_| StorageError::invalid_argument("read output length overflows usize"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| StorageError::invalid_argument("read output end overflows usize"))?;
    buf.get_mut(start..end)
        .ok_or_else(|| StorageError::corrupt("read plan output range exceeds buffer"))
}

fn verify_read_integrity_policy(
    integrity: SegmentPayloadIntegrity,
    verification: ReadVerification,
) -> Result<()> {
    if matches!(
        (integrity, verification),
        (SegmentPayloadIntegrity::Unchecked, ReadVerification::RequireVerified)
    ) {
        return Err(StorageError::conflict(
            "read requires verified payload but source is unchecked",
        ));
    }
    Ok(())
}

pub(super) fn assemble_read_plan(
    storage: &impl StorageNodeReadService,
    plan: ReadPlan,
    verification: ReadVerification,
    buf: &mut [u8],
) -> Result<()> {
    let buf_len = u64::try_from(buf.len())
        .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
    if buf_len != plan.logical_len {
        return Err(StorageError::invalid_argument(
            "read buffer length must match read plan length",
        ));
    }
    buf.fill(0);
    for extent in plan.extents {
        match extent.source {
            ReadSource::Zero => {}
            ReadSource::Segment {
                storage_node,
                segment_id,
                segment_offset,
                integrity,
            } => {
                let output = read_output_slice(buf, extent.output_offset, extent.len)?;
                storage.read_segment_source(
                    storage_node,
                    segment_id,
                    ByteRange::new(segment_offset, extent.len),
                    integrity,
                    verification,
                    output,
                )?;
            }
            ReadSource::AppendRun {
                storage_node,
                log_id,
                payload_offset,
                integrity,
            } => {
                let output = read_output_slice(buf, extent.output_offset, extent.len)?;
                storage.read_append_run_source(
                    storage_node,
                    log_id,
                    ByteRange::new(payload_offset, extent.len),
                    integrity,
                    verification,
                    output,
                )?;
            }
        }
    }
    Ok(())
}
