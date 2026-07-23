const BLOCK_JOURNAL_INLINE_MAX_BYTES: u64 = 256 * 1024;
const BLOCK_JOURNAL_MAGIC: [u8; 8] = *b"BLKJNL01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BlockJournalEntry {
    Write {
        range: ByteRange,
        payload_integrity: PayloadIntegrity,
        bytes: Vec<u8>,
    },
    Segment {
        range: ByteRange,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        segment_offset: u64,
        integrity: SegmentPayloadIntegrity,
    },
    Sparse {
        range: ByteRange,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockJournalCommit {
    device_id: DeviceId,
    writer_epoch: WriterEpoch,
    commit_seq: CommitSeq,
    write_count: u64,
    collapsed_range_count: u64,
    committed_bytes: u64,
    entries: Vec<BlockJournalEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PackedBlockJournalEntry {
    commit_seq_delta: u64,
    lba: u64,
    block_count: u64,
    payload_offset: u64,
    payload_integrity: PayloadIntegrity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PackedBlockJournalWrites {
    device_id: DeviceId,
    writer_epoch: WriterEpoch,
    block_size: u64,
    first_commit_seq: CommitSeq,
    entries: Vec<PackedBlockJournalEntry>,
    payload_checksum: u64,
    payload_slab: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BlockJournalRecord {
    Lease {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
    },
    Write(BlockJournalCommit),
    PackedWrites(PackedBlockJournalWrites),
    Flush {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
        durable_through: CommitSeq,
    },
}

#[derive(Debug, Clone)]
pub(super) struct BlockJournalInlineFragment {
    bytes: Arc<[u8]>,
    source_offset: u64,
    offset: u64,
    len: u64,
}

#[derive(Debug, Clone)]
pub(super) enum BlockJournalOverlaySource {
    Bytes {
        payload_integrity: PayloadIntegrity,
        bytes: Arc<[u8]>,
        source_offset: u64,
    },
    BytesFragments {
        payload_integrity: PayloadIntegrity,
        fragments: Arc<[BlockJournalInlineFragment]>,
        source_offset: u64,
    },
    Segment {
        storage_node: StorageNodeId,
        segment_id: SegmentId,
        segment_offset: u64,
        integrity: SegmentPayloadIntegrity,
    },
    Sparse,
}

#[derive(Debug, Clone)]
pub(super) struct BlockJournalOverlayEntry {
    range: ByteRange,
    source: BlockJournalOverlaySource,
}

#[derive(Debug, Clone)]
pub(super) struct BlockJournalDeviceOverlay {
    writer_epoch: WriterEpoch,
    durable_through: CommitSeq,
    visible_through: CommitSeq,
    block_size: Option<u64>,
    // Current newest-wins read view keyed by starting logical block number.
    // Each value is one non-overlapping block-aligned run; shadowed history
    // lives in journal shard files for replay and future materialization.
    lba_runs: BTreeMap<u64, BlockJournalOverlayEntry>,
}

impl Default for BlockJournalDeviceOverlay {
    fn default() -> Self {
        Self {
            writer_epoch: WriterEpoch::from_raw(0),
            durable_through: CommitSeq::from_raw(0),
            visible_through: CommitSeq::from_raw(0),
            block_size: None,
            lba_runs: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct BlockJournalOverlay {
    inner: Mutex<BTreeMap<DeviceId, BlockJournalDeviceOverlay>>,
}

/// One unit of work for the group-committed block journal lane.
///
/// Every durable block boundary becomes a lane request so concurrent waiters
/// share one journal append and one data sync. Write requests carry full
/// commits; Flush requests only advance a device durability high-water and may
/// be merged per device inside one batch.
#[derive(Debug, Clone)]
pub(super) enum BlockJournalLaneRequest {
    Write(BlockJournalCommit),
    Flush {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
        durable_through: CommitSeq,
    },
    Lease {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
    },
}

#[derive(Debug)]
pub(super) struct BlockJournalFlushCoordinator {
    inner: Mutex<BlockJournalFlushState>,
    cvar: Condvar,
}

/// Leader-side timing for one journal lane batch, attributing time spent
/// outside the journal append/sync I/O itself.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct BlockJournalLaneBatchTiming {
    pub(super) payload_recheck_nanos: u64,
    pub(super) publish_nanos: u64,
    pub(super) publish_mark_nanos: u64,
    pub(super) publish_reserve_nanos: u64,
    pub(super) publish_apply_nanos: u64,
    pub(super) lba_map_update_nanos: u64,
    /// Routing work for the per-node mark dispatch: grouping a commit's
    /// segment refs by their carried storage node plus the point resolution
    /// of each carried node. Publish never scans catalogs to find a
    /// segment's owner.
    pub(super) publish_routing_nanos: u64,
    /// Mark call time not attributed to the catalog mark or event recording
    /// (in-process transport stand-in).
    pub(super) publish_mark_call_residual_nanos: u64,
    pub(super) publish_mark_catalog_nanos: u64,
    pub(super) publish_mark_lock_wait_nanos: u64,
    /// Node-side observability event/counter recording during the mark.
    pub(super) publish_mark_observability_nanos: u64,
}

#[derive(Debug, Default)]
pub(super) struct BlockJournalFlushState {
    in_flight: bool,
    generation: u64,
    next_request_id: u64,
    pending: BTreeMap<u64, BlockJournalLaneRequest>,
    completed: BTreeMap<u64, Result<()>>,
    // Per-device count of lane writes whose overlay apply has not finished.
    // Live applies must happen in commit-seq order per device, so an
    // acknowledged write may bypass the lane only while this count is zero
    // for its device.
    unapplied_writes: BTreeMap<DeviceId, u64>,
}

impl BlockJournalFlushState {
    fn enqueue(&mut self, request: BlockJournalLaneRequest) -> u64 {
        if let BlockJournalLaneRequest::Write(commit) = &request {
            *self.unapplied_writes.entry(commit.device_id).or_default() += 1;
        }
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending.insert(request_id, request);
        request_id
    }

    fn has_unapplied_writes(&self, device_id: DeviceId) -> bool {
        self.unapplied_writes
            .get(&device_id)
            .is_some_and(|count| *count > 0)
    }

    fn release_unapplied_writes(&mut self, device_id: DeviceId, count: u64) {
        let Some(outstanding) = self.unapplied_writes.get_mut(&device_id) else {
            return;
        };
        *outstanding = outstanding.saturating_sub(count);
        if *outstanding == 0 {
            self.unapplied_writes.remove(&device_id);
        }
    }
}

impl BlockJournalFlushCoordinator {
    fn new() -> Self {
        Self {
            inner: Mutex::new(BlockJournalFlushState::default()),
            cvar: Condvar::new(),
        }
    }
}

impl BlockJournalEntry {
    fn range(&self) -> ByteRange {
        match self {
            Self::Write { range, .. } | Self::Segment { range, .. } | Self::Sparse { range } => {
                *range
            }
        }
    }

    fn committed_bytes(&self) -> u64 {
        match self {
            Self::Write { range, .. } | Self::Segment { range, .. } => range.len,
            Self::Sparse { .. } => 0,
        }
    }

}

impl BlockJournalCommit {
    fn validate(&self, spec: &DeviceSpec) -> Result<()> {
        if self.entries.is_empty() {
            return Err(StorageError::corrupt("block journal commit has no entries"));
        }
        let mut committed_bytes = 0_u64;
        for entry in &self.entries {
            let range = entry.range();
            range.validate_for_device(spec)?;
            match entry {
                BlockJournalEntry::Write {
                    bytes,
                    range,
                    ..
                } => {
                    let bytes_len = u64::try_from(bytes.len()).map_err(|_| {
                        StorageError::corrupt("block journal payload length overflows u64")
                    })?;
                    if bytes_len != range.len {
                        return Err(StorageError::corrupt(
                            "block journal payload length disagrees with range",
                        ));
                    }
                }
                BlockJournalEntry::Segment {
                    segment_offset,
                    range,
                    ..
                } => {
                    segment_offset.checked_add(range.len).ok_or_else(|| {
                        StorageError::corrupt("block journal segment reference overflows")
                    })?;
                }
                BlockJournalEntry::Sparse { .. } => {}
            }
            committed_bytes = committed_bytes
                .checked_add(entry.committed_bytes())
                .ok_or_else(|| StorageError::corrupt("block journal committed bytes overflow"))?;
        }
        if committed_bytes != self.committed_bytes {
            return Err(StorageError::corrupt(
                "block journal committed byte count disagrees with entries",
            ));
        }
        Ok(())
    }

    fn overlay_entries(&self) -> Result<Vec<BlockJournalOverlayEntry>> {
        let mut entries = Vec::new();
        for entry in &self.entries {
            let source = match entry {
                BlockJournalEntry::Write {
                    payload_integrity,
                    bytes,
                    ..
                } => BlockJournalOverlaySource::Bytes {
                    payload_integrity: *payload_integrity,
                    bytes: Arc::<[u8]>::from(bytes.clone()),
                    source_offset: 0,
                },
                BlockJournalEntry::Segment {
                    storage_node,
                    segment_id,
                    segment_offset,
                    integrity,
                    ..
                } => BlockJournalOverlaySource::Segment {
                    storage_node: *storage_node,
                    segment_id: *segment_id,
                    segment_offset: *segment_offset,
                    integrity: *integrity,
                },
                BlockJournalEntry::Sparse { .. } => BlockJournalOverlaySource::Sparse,
            };
            push_coalesced_block_journal_commit_entry(
                &mut entries,
                BlockJournalOverlayEntry {
                    range: entry.range(),
                    source,
                },
            )?;
        }
        Ok(entries)
    }
}

impl PackedBlockJournalWrites {
    fn commit_seq_for_entry(&self, entry: &PackedBlockJournalEntry) -> Result<CommitSeq> {
        let raw = self
            .first_commit_seq
            .raw()
            .checked_add(entry.commit_seq_delta)
            .ok_or_else(|| StorageError::corrupt("packed block journal commit sequence overflows"))?;
        Ok(CommitSeq::from_raw(raw))
    }

    fn validate(&self) -> Result<()> {
        if self.block_size == 0 {
            return Err(StorageError::corrupt(
                "packed block journal block size must be nonzero",
            ));
        }
        if self.entries.is_empty() {
            return Err(StorageError::corrupt(
                "packed block journal record has no entries",
            ));
        }
        if data_log_checksum(&self.payload_slab) != self.payload_checksum {
            return Err(StorageError::corrupt(
                "packed block journal payload checksum mismatch",
            ));
        }

        let mut expected_payload_offset = 0_u64;
        let payload_len = usize_to_u64(self.payload_slab.len());
        let mut previous_commit_seq = None;
        for (entry_index, entry) in self.entries.iter().enumerate() {
            if entry_index == 0 && entry.commit_seq_delta != 0 {
                return Err(StorageError::corrupt(
                    "packed block journal first commit sequence disagrees with base",
                ));
            }
            let commit_seq = self.commit_seq_for_entry(entry)?;
            if previous_commit_seq.is_some_and(|previous| commit_seq.raw() <= previous) {
                return Err(StorageError::corrupt(
                    "packed block journal commit sequences are not monotonic",
                ));
            }
            previous_commit_seq = Some(commit_seq.raw());

            if entry.block_count == 0 {
                return Err(StorageError::corrupt(
                    "packed block journal entry has zero block count",
                ));
            }
            let len = entry.block_count.checked_mul(self.block_size).ok_or_else(|| {
                StorageError::corrupt("packed block journal entry length overflows")
            })?;
            let _offset = entry.lba.checked_mul(self.block_size).ok_or_else(|| {
                StorageError::corrupt("packed block journal entry offset overflows")
            })?;
            if entry.payload_offset != expected_payload_offset {
                return Err(StorageError::corrupt(
                    "packed block journal payload offsets are not contiguous",
                ));
            }
            expected_payload_offset =
                entry.payload_offset.checked_add(len).ok_or_else(|| {
                    StorageError::corrupt("packed block journal payload range overflows")
                })?;
            if expected_payload_offset > payload_len {
                return Err(StorageError::corrupt(
                    "packed block journal payload range out of bounds",
                ));
            }
        }
        if expected_payload_offset != payload_len {
            return Err(StorageError::corrupt(
                "packed block journal payload slab has trailing bytes",
            ));
        }
        Ok(())
    }

    fn expand_commits(&self) -> Result<Vec<BlockJournalCommit>> {
        self.validate()?;
        let mut out = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let commit_seq = self.commit_seq_for_entry(entry)?;
            let offset = entry.lba.checked_mul(self.block_size).ok_or_else(|| {
                StorageError::corrupt("packed block journal entry offset overflows")
            })?;
            let len = entry.block_count.checked_mul(self.block_size).ok_or_else(|| {
                StorageError::corrupt("packed block journal entry length overflows")
            })?;
            let payload_end = entry.payload_offset.checked_add(len).ok_or_else(|| {
                StorageError::corrupt("packed block journal payload range overflows")
            })?;
            let payload_start = usize::try_from(entry.payload_offset).map_err(|_| {
                StorageError::corrupt("packed block journal payload offset overflows usize")
            })?;
            let payload_end = usize::try_from(payload_end).map_err(|_| {
                StorageError::corrupt("packed block journal payload end overflows usize")
            })?;
            let bytes = self
                .payload_slab
                .get(payload_start..payload_end)
                .ok_or_else(|| {
                    StorageError::corrupt("packed block journal payload range out of bounds")
                })?
                .to_vec();
            out.push(BlockJournalCommit {
                device_id: self.device_id,
                writer_epoch: self.writer_epoch,
                commit_seq,
                write_count: 1,
                collapsed_range_count: 1,
                committed_bytes: len,
                entries: vec![BlockJournalEntry::Write {
                    range: ByteRange::new(offset, len),
                    payload_integrity: entry.payload_integrity,
                    bytes,
                }],
            });
        }
        Ok(out)
    }
}

fn block_journal_inline_fragment_end(fragment: &BlockJournalInlineFragment) -> Result<u64> {
    let end = fragment.offset.checked_add(fragment.len).ok_or_else(|| {
        StorageError::corrupt("block journal inline fragment range overflows")
    })?;
    fragment
        .source_offset
        .checked_add(fragment.len)
        .ok_or_else(|| StorageError::corrupt("block journal inline fragment source overflows"))?;
    let bytes_len = u64::try_from(fragment.bytes.len())
        .map_err(|_| StorageError::corrupt("block journal inline fragment length overflows"))?;
    if end > bytes_len {
        return Err(StorageError::corrupt(
            "block journal inline fragment exceeds payload",
        ));
    }
    Ok(end)
}

fn block_journal_inline_fragments_total_len(
    fragments: &[BlockJournalInlineFragment],
) -> Result<u64> {
    let mut expected_offset = 0_u64;
    for fragment in fragments {
        block_journal_inline_fragment_end(fragment)?;
        if fragment.source_offset != expected_offset {
            return Err(StorageError::corrupt(
                "block journal inline fragments are not contiguous",
            ));
        }
        expected_offset = expected_offset
            .checked_add(fragment.len)
            .ok_or_else(|| StorageError::corrupt("block journal inline fragments overflow"))?;
    }
    Ok(expected_offset)
}

fn block_journal_inline_fragment_slice(
    fragments: &[BlockJournalInlineFragment],
    source_offset: u64,
    len: u64,
) -> Result<Vec<BlockJournalInlineFragment>> {
    let source_end = source_offset
        .checked_add(len)
        .ok_or_else(|| StorageError::corrupt("block journal inline fragment slice overflows"))?;
    let mut cursor = 0_u64;
    let mut out = Vec::new();
    for fragment in fragments {
        block_journal_inline_fragment_end(fragment)?;
        let fragment_start = fragment.source_offset;
        let fragment_end = fragment
            .source_offset
            .checked_add(fragment.len)
            .ok_or_else(|| StorageError::corrupt("block journal inline fragment cursor overflows"))?;
        if fragment_start < source_end && fragment_end > source_offset {
            let overlap_start = fragment_start.max(source_offset);
            let overlap_end = fragment_end.min(source_end);
            out.push(BlockJournalInlineFragment {
                bytes: Arc::clone(&fragment.bytes),
                source_offset: overlap_start - source_offset,
                offset: fragment
                    .offset
                    .checked_add(overlap_start - fragment_start)
                    .ok_or_else(|| {
                        StorageError::corrupt("block journal inline fragment offset overflows")
                    })?,
                len: overlap_end - overlap_start,
            });
        }
        cursor = cursor.max(fragment_end);
        if fragment_end >= source_end {
            break;
        }
    }
    let sliced_len = out.iter().try_fold(0_u64, |sum, fragment| {
        sum.checked_add(fragment.len)
            .ok_or_else(|| StorageError::corrupt("block journal inline fragment slice overflows"))
    })?;
    if sliced_len != len {
        return Err(StorageError::corrupt(
            "block journal inline fragment slice out of bounds",
        ));
    }
    Ok(out)
}

fn block_journal_inline_fragments_for_source(
    source: &BlockJournalOverlaySource,
    len: u64,
) -> Result<(PayloadIntegrity, Vec<BlockJournalInlineFragment>)> {
    match source {
        BlockJournalOverlaySource::Bytes {
            payload_integrity,
            bytes,
            source_offset,
        } => {
            let source_end = source_offset.checked_add(len).ok_or_else(|| {
                StorageError::corrupt("block journal inline source range overflows")
            })?;
            let bytes_len = u64::try_from(bytes.len())
                .map_err(|_| StorageError::corrupt("block journal inline source length overflows"))?;
            if source_end > bytes_len {
                return Err(StorageError::corrupt(
                    "block journal inline source slice out of bounds",
                ));
            }
            Ok((
                *payload_integrity,
                vec![BlockJournalInlineFragment {
                    bytes: Arc::clone(bytes),
                    source_offset: 0,
                    offset: *source_offset,
                    len,
                }],
            ))
        }
        BlockJournalOverlaySource::BytesFragments {
            payload_integrity,
            fragments,
            source_offset,
        } => Ok((
            *payload_integrity,
            block_journal_inline_fragment_slice(fragments, *source_offset, len)?,
        )),
        BlockJournalOverlaySource::Segment { .. } | BlockJournalOverlaySource::Sparse => {
            Err(StorageError::corrupt(
                "block journal source is not inline payload",
            ))
        }
    }
}

fn copy_block_journal_inline_fragments(
    fragments: &[BlockJournalInlineFragment],
    source_offset: u64,
    output: &mut [u8],
) -> Result<()> {
    let len = u64::try_from(output.len())
        .map_err(|_| StorageError::corrupt("block journal read output length overflows"))?;
    let source_end = source_offset
        .checked_add(len)
        .ok_or_else(|| StorageError::corrupt("block journal inline read range overflows"))?;
    let mut output_written = 0_usize;
    let mut index = fragments.partition_point(|fragment| {
        fragment
            .source_offset
            .checked_add(fragment.len)
            .is_some_and(|end| end <= source_offset)
    });
    while let Some(fragment) = fragments.get(index) {
        block_journal_inline_fragment_end(fragment)?;
        let fragment_start = fragment.source_offset;
        let fragment_end = fragment
            .source_offset
            .checked_add(fragment.len)
            .ok_or_else(|| StorageError::corrupt("block journal inline fragment cursor overflows"))?;
        if fragment_start < source_end && fragment_end > source_offset {
            let overlap_start = fragment_start.max(source_offset);
            let overlap_end = fragment_end.min(source_end);
            let source_start = fragment
                .offset
                .checked_add(overlap_start - fragment_start)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal inline read offset overflows")
                })?;
            let source_end = fragment
                .offset
                .checked_add(overlap_end - fragment_start)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal inline read end overflows")
                })?;
            let source_start = usize::try_from(source_start).map_err(|_| {
                StorageError::corrupt("block journal inline read offset overflows usize")
            })?;
            let source_end = usize::try_from(source_end).map_err(|_| {
                StorageError::corrupt("block journal inline read end overflows usize")
            })?;
            let source = fragment.bytes.get(source_start..source_end).ok_or_else(|| {
                StorageError::corrupt("block journal inline read source out of bounds")
            })?;
            let output_end = output_written
                .checked_add(source.len())
                .ok_or_else(|| StorageError::corrupt("block journal inline output overflows"))?;
            output
                .get_mut(output_written..output_end)
                .ok_or_else(|| StorageError::corrupt("block journal inline output out of bounds"))?
                .copy_from_slice(source);
            output_written = output_end;
        }
        if fragment_end >= source_end {
            break;
        }
        index += 1;
    }
    if output_written != output.len() {
        return Err(StorageError::corrupt(
            "block journal inline read source out of bounds",
        ));
    }
    Ok(())
}

fn block_journal_overlay_source_slice(
    source: &BlockJournalOverlaySource,
    source_range: ByteRange,
    slice: ByteRange,
) -> Result<BlockJournalOverlaySource> {
    let source_end = source_range.end_exclusive()?;
    let slice_end = slice.end_exclusive()?;
    if slice.offset < source_range.offset || slice_end > source_end {
        return Err(StorageError::corrupt(
            "block journal LBA map slice is outside source range",
        ));
    }
    match source {
        BlockJournalOverlaySource::Sparse => Ok(BlockJournalOverlaySource::Sparse),
        BlockJournalOverlaySource::Bytes {
            payload_integrity,
            bytes,
            source_offset,
        } => {
            let source_offset = source_offset
                .checked_add(slice.offset - source_range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal LBA map source offset overflows")
                })?;
            let source_end = source_offset.checked_add(slice.len).ok_or_else(|| {
                StorageError::corrupt("block journal LBA map source end overflows")
            })?;
            let start = usize::try_from(source_offset).map_err(|_| {
                StorageError::corrupt("block journal LBA map source offset overflows usize")
            })?;
            let end = usize::try_from(source_end).map_err(|_| {
                StorageError::corrupt("block journal LBA map source end overflows usize")
            })?;
            bytes.get(start..end).ok_or_else(|| {
                StorageError::corrupt("block journal LBA map source slice out of bounds")
            })?;
            Ok(BlockJournalOverlaySource::Bytes {
                payload_integrity: *payload_integrity,
                bytes: Arc::clone(bytes),
                source_offset,
            })
        }
        BlockJournalOverlaySource::BytesFragments {
            payload_integrity,
            fragments,
            source_offset,
        } => {
            let source_offset = source_offset
                .checked_add(slice.offset - source_range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal LBA map source offset overflows")
                })?;
            let source_end = source_offset.checked_add(slice.len).ok_or_else(|| {
                StorageError::corrupt("block journal LBA map source end overflows")
            })?;
            let total_len = block_journal_inline_fragments_total_len(fragments)?;
            if source_end > total_len {
                return Err(StorageError::corrupt(
                    "block journal LBA map source slice out of bounds",
                ));
            }
            Ok(BlockJournalOverlaySource::BytesFragments {
                payload_integrity: *payload_integrity,
                fragments: Arc::clone(fragments),
                source_offset,
            })
        }
        BlockJournalOverlaySource::Segment {
            storage_node,
            segment_id,
            segment_offset,
            integrity,
        } => Ok(BlockJournalOverlaySource::Segment {
            storage_node: *storage_node,
            segment_id: *segment_id,
            segment_offset: segment_offset
                .checked_add(slice.offset - source_range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal segment slice offset overflows")
                })?,
            integrity: *integrity,
        }),
    }
}

fn block_journal_overlay_slice(
    entry: &BlockJournalOverlayEntry,
    range: ByteRange,
) -> Result<BlockJournalOverlayEntry> {
    Ok(BlockJournalOverlayEntry {
        range,
        source: block_journal_overlay_source_slice(&entry.source, entry.range, range)?,
    })
}

fn set_block_journal_overlay_block_size(
    device: &mut BlockJournalDeviceOverlay,
    block_size: u64,
) -> Result<()> {
    if block_size == 0 {
        return Err(StorageError::corrupt(
            "block journal LBA map block size must be nonzero",
        ));
    }
    match device.block_size {
        Some(existing) if existing != block_size => Err(StorageError::corrupt(
            "block journal LBA map block size changed for device",
        )),
        Some(_) => Ok(()),
        None => {
            device.block_size = Some(block_size);
            Ok(())
        }
    }
}

fn block_journal_lba_bounds(range: ByteRange, block_size: u64) -> Result<(u64, u64)> {
    if block_size == 0 {
        return Err(StorageError::corrupt(
            "block journal LBA map block size must be nonzero",
        ));
    }
    if !range.offset.is_multiple_of(block_size) || !range.len.is_multiple_of(block_size) {
        return Err(StorageError::corrupt(
            "block journal LBA map entry is not block aligned",
        ));
    }
    let start_lba = range.offset / block_size;
    let block_count = range.len / block_size;
    let end_lba = start_lba
        .checked_add(block_count)
        .ok_or_else(|| StorageError::corrupt("block journal LBA range overflows"))?;
    Ok((start_lba, end_lba))
}

fn block_journal_overlay_run_bounds(
    start_lba: u64,
    entry: &BlockJournalOverlayEntry,
    block_size: u64,
) -> Result<(u64, u64)> {
    let (range_start_lba, range_end_lba) = block_journal_lba_bounds(entry.range, block_size)?;
    if range_start_lba != start_lba {
        return Err(StorageError::corrupt(
            "block journal LBA run key disagrees with range",
        ));
    }
    Ok((range_start_lba, range_end_lba))
}

fn block_journal_overlay_run_slice(
    entry: &BlockJournalOverlayEntry,
    block_size: u64,
    start_lba: u64,
    end_lba: u64,
) -> Result<BlockJournalOverlayEntry> {
    if end_lba < start_lba {
        return Err(StorageError::corrupt(
            "block journal LBA run slice is inverted",
        ));
    }
    let offset = start_lba
        .checked_mul(block_size)
        .ok_or_else(|| StorageError::corrupt("block journal LBA run offset overflows"))?;
    let len = end_lba
        .checked_sub(start_lba)
        .and_then(|blocks| blocks.checked_mul(block_size))
        .ok_or_else(|| StorageError::corrupt("block journal LBA run length overflows"))?;
    block_journal_overlay_slice(entry, ByteRange::new(offset, len))
}

fn insert_block_journal_lba_run(
    runs: &mut BTreeMap<u64, BlockJournalOverlayEntry>,
    block_size: u64,
    entry: BlockJournalOverlayEntry,
) -> Result<()> {
    let (start_lba, end_lba) = block_journal_lba_bounds(entry.range, block_size)?;
    if start_lba == end_lba {
        return Ok(());
    }

    let first_key = match runs.range(..=start_lba).next_back() {
        Some((key, existing)) => {
            let (_, existing_end_lba) =
                block_journal_overlay_run_bounds(*key, existing, block_size)?;
            if existing_end_lba > start_lba {
                *key
            } else {
                start_lba
            }
        }
        None => start_lba,
    };

    let mut overlap_keys = Vec::new();
    for (key, existing) in runs.range(first_key..end_lba) {
        let (existing_start_lba, existing_end_lba) =
            block_journal_overlay_run_bounds(*key, existing, block_size)?;
        if existing_start_lba < end_lba && existing_end_lba > start_lba {
            overlap_keys.push(*key);
        }
    }

    let mut fragments = Vec::new();
    for key in overlap_keys {
        let existing = runs.remove(&key).ok_or_else(|| {
            StorageError::corrupt("block journal LBA overlap disappeared during insert")
        })?;
        let (existing_start_lba, existing_end_lba) =
            block_journal_overlay_run_bounds(key, &existing, block_size)?;
        if existing_start_lba < start_lba {
            fragments.push((
                existing_start_lba,
                block_journal_overlay_run_slice(&existing, block_size, existing_start_lba, start_lba)?,
            ));
        }
        if existing_end_lba > end_lba {
            fragments.push((
                end_lba,
                block_journal_overlay_run_slice(&existing, block_size, end_lba, existing_end_lba)?,
            ));
        }
    }

    for (key, fragment) in fragments {
        runs.insert(key, fragment);
    }
    runs.insert(start_lba, entry);
    Ok(())
}

fn apply_block_journal_read_entry(
    storage: &impl StorageNodeReadService,
    entry: &BlockJournalOverlayEntry,
    requested: ByteRange,
    verification: ReadVerification,
    buf: &mut [u8],
) -> Result<()> {
    let Some(overlap) = byte_range_intersection(entry.range, requested)? else {
        return Ok(());
    };
    let output_start = usize::try_from(overlap.offset - requested.offset)
        .map_err(|_| StorageError::corrupt("block journal read output overflows usize"))?;
    let output_len = usize::try_from(overlap.len)
        .map_err(|_| StorageError::corrupt("block journal read length overflows usize"))?;
    let output_end = output_start
        .checked_add(output_len)
        .ok_or_else(|| StorageError::corrupt("block journal read output end overflows"))?;
    let output = buf
        .get_mut(output_start..output_end)
        .ok_or_else(|| StorageError::corrupt("block journal read output out of bounds"))?;
    match &entry.source {
        BlockJournalOverlaySource::Sparse => output.fill(0),
        BlockJournalOverlaySource::Bytes {
            payload_integrity,
            bytes,
            source_offset,
        } => {
            let source_start = source_offset
                .checked_add(overlap.offset - entry.range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal read source offset overflows")
                })?;
            let source_start = usize::try_from(source_start)
                .map_err(|_| StorageError::corrupt("block journal read source overflows usize"))?;
            let source_end = source_start.checked_add(output_len).ok_or_else(|| {
                StorageError::corrupt("block journal read source end overflows")
            })?;
            let source = bytes.get(source_start..source_end).ok_or_else(|| {
                StorageError::corrupt("block journal read source out of bounds")
            })?;
            if !matches!(verification, ReadVerification::Skip) {
                let integrity = segment_payload_integrity(*payload_integrity, source);
                verify_read_integrity_policy(integrity, verification)?;
            }
            output.copy_from_slice(source);
        }
        BlockJournalOverlaySource::BytesFragments {
            payload_integrity,
            fragments,
            source_offset,
        } => {
            let source_start = source_offset
                .checked_add(overlap.offset - entry.range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal read source offset overflows")
                })?;
            copy_block_journal_inline_fragments(fragments, source_start, output)?;
            if !matches!(verification, ReadVerification::Skip) {
                let integrity = segment_payload_integrity(*payload_integrity, output);
                verify_read_integrity_policy(integrity, verification)?;
            }
        }
        BlockJournalOverlaySource::Segment {
            storage_node,
            segment_id,
            segment_offset,
            integrity,
        } => {
            let source_offset = segment_offset
                .checked_add(overlap.offset - entry.range.offset)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal segment read offset overflows")
                })?;
            storage.read_segment_source(
                *storage_node,
                *segment_id,
                ByteRange::new(source_offset, overlap.len),
                *integrity,
                verification,
                output,
            )?;
        }
    }
    Ok(())
}

fn try_extend_block_journal_lba_read_entry(
    left: &mut BlockJournalOverlayEntry,
    right: &BlockJournalOverlayEntry,
) -> Result<bool> {
    if left.range.end_exclusive()? != right.range.offset {
        return Ok(false);
    }
    let merged_len = left
        .range
        .len
        .checked_add(right.range.len)
        .ok_or_else(|| StorageError::corrupt("block journal LBA map read range overflows"))?;
    match (&left.source, &right.source) {
        (BlockJournalOverlaySource::Sparse, BlockJournalOverlaySource::Sparse) => {
            left.range.len = merged_len;
            Ok(true)
        }
        (
            BlockJournalOverlaySource::Bytes {
                payload_integrity: left_integrity,
                bytes: left_bytes,
                source_offset: left_source_offset,
            },
            BlockJournalOverlaySource::Bytes {
                payload_integrity: right_integrity,
                bytes: right_bytes,
                source_offset: right_source_offset,
            },
        ) if left_integrity == right_integrity
            && Arc::ptr_eq(left_bytes, right_bytes)
            && left_source_offset
                .checked_add(left.range.len)
                .is_some_and(|end| end == *right_source_offset) =>
        {
            left.range.len = merged_len;
            Ok(true)
        }
        (
            BlockJournalOverlaySource::BytesFragments {
                payload_integrity: left_integrity,
                fragments: left_fragments,
                source_offset: left_source_offset,
            },
            BlockJournalOverlaySource::BytesFragments {
                payload_integrity: right_integrity,
                fragments: right_fragments,
                source_offset: right_source_offset,
            },
        ) if left_integrity == right_integrity
            && Arc::ptr_eq(left_fragments, right_fragments)
            && left_source_offset
                .checked_add(left.range.len)
                .is_some_and(|end| end == *right_source_offset) =>
        {
            left.range.len = merged_len;
            Ok(true)
        }
        (
            BlockJournalOverlaySource::Segment {
                storage_node: left_storage_node,
                segment_id: left_segment_id,
                segment_offset: left_segment_offset,
                integrity: left_integrity,
            },
            BlockJournalOverlaySource::Segment {
                storage_node: right_storage_node,
                segment_id: right_segment_id,
                segment_offset: right_segment_offset,
                integrity: right_integrity,
            },
        ) if left_storage_node == right_storage_node
            && left_segment_id == right_segment_id
            && left_integrity == right_integrity
            && left_segment_offset
                .checked_add(left.range.len)
                .is_some_and(|end| end == *right_segment_offset) =>
        {
            left.range.len = merged_len;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn push_coalesced_block_journal_lba_read_entry(
    out: &mut Vec<BlockJournalOverlayEntry>,
    entry: BlockJournalOverlayEntry,
) -> Result<()> {
    if let Some(last) = out.last_mut()
        && try_extend_block_journal_lba_read_entry(last, &entry)?
    {
        return Ok(());
    }
    out.push(entry);
    Ok(())
}

fn try_extend_block_journal_commit_entry(
    left: &mut BlockJournalOverlayEntry,
    right: &BlockJournalOverlayEntry,
) -> Result<bool> {
    if left.range.end_exclusive()? != right.range.offset {
        return Ok(false);
    }
    let merged_len = left
        .range
        .len
        .checked_add(right.range.len)
        .ok_or_else(|| StorageError::corrupt("block journal LBA map commit range overflows"))?;
    match (&left.source, &right.source) {
        (BlockJournalOverlaySource::Sparse, BlockJournalOverlaySource::Sparse) => {
            left.range.len = merged_len;
            Ok(true)
        }
        (
            BlockJournalOverlaySource::Segment {
                storage_node: left_storage_node,
                segment_id: left_segment_id,
                segment_offset: left_segment_offset,
                integrity: left_integrity,
            },
            BlockJournalOverlaySource::Segment {
                storage_node: right_storage_node,
                segment_id: right_segment_id,
                segment_offset: right_segment_offset,
                integrity: right_integrity,
            },
        ) if left_storage_node == right_storage_node
            && left_segment_id == right_segment_id
            && left_integrity == right_integrity
            && left_segment_offset
                .checked_add(left.range.len)
                .is_some_and(|end| end == *right_segment_offset) =>
        {
            left.range.len = merged_len;
            Ok(true)
        }
        _ => {
            let Ok((left_integrity, mut fragments)) =
                block_journal_inline_fragments_for_source(&left.source, left.range.len)
            else {
                return Ok(false);
            };
            let Ok((right_integrity, mut right_fragments)) =
                block_journal_inline_fragments_for_source(&right.source, right.range.len)
            else {
                return Ok(false);
            };
            if left_integrity != right_integrity {
                return Ok(false);
            }
            for fragment in &mut right_fragments {
                fragment.source_offset = fragment
                    .source_offset
                    .checked_add(left.range.len)
                    .ok_or_else(|| {
                        StorageError::corrupt("block journal inline fragment source overflows")
                    })?;
            }
            fragments.append(&mut right_fragments);
            left.range.len = merged_len;
            left.source = BlockJournalOverlaySource::BytesFragments {
                payload_integrity: left_integrity,
                fragments: Arc::<[BlockJournalInlineFragment]>::from(fragments),
                source_offset: 0,
            };
            Ok(true)
        }
    }
}

fn push_coalesced_block_journal_commit_entry(
    out: &mut Vec<BlockJournalOverlayEntry>,
    entry: BlockJournalOverlayEntry,
) -> Result<()> {
    if let Some(last) = out.last_mut()
        && try_extend_block_journal_commit_entry(last, &entry)?
    {
        return Ok(());
    }
    out.push(entry);
    Ok(())
}

fn mark_block_journal_segment_refs_referenced(
    local: &LocalCoordinator,
    commit: &BlockJournalCommit,
    timing: &mut BlockJournalLaneBatchTiming,
) -> Result<()> {
    // Group segment refs by their carried storage node: a commit's entries
    // can span nodes (chunk striping round-robins segments across nodes),
    // and the carried id is authoritative for routing, so each group
    // dispatches one batched mark to its node and never scans other
    // catalogs. Marks on distinct segments commute, so per-node order is
    // free to differ from entry order.
    let total_started = Instant::now();
    let grouping_started = Instant::now();
    let mut segment_refs_by_node: BTreeMap<StorageNodeId, Vec<SegmentId>> = BTreeMap::new();
    for entry in &commit.entries {
        let BlockJournalEntry::Segment {
            storage_node,
            segment_id,
            ..
        } = entry
        else {
            continue;
        };
        segment_refs_by_node
            .entry(*storage_node)
            .or_default()
            .push(*segment_id);
    }
    if segment_refs_by_node.is_empty() {
        return Ok(());
    }
    let mut routing_nanos = duration_nanos_u64(grouping_started.elapsed());
    let mut catalog_nanos = 0_u64;
    let mut catalog_lock_wait_nanos = 0_u64;
    let mut observability_nanos = 0_u64;
    for (storage_node, segment_ids) in segment_refs_by_node {
        let mark_profile = local.storage_nodes.mark_segments_referenced_profiled(
            storage_node,
            &segment_ids,
            commit.commit_seq,
        )?;
        routing_nanos = routing_nanos.saturating_add(mark_profile.routing_nanos);
        catalog_nanos = catalog_nanos.saturating_add(mark_profile.catalog_mark_nanos);
        catalog_lock_wait_nanos =
            catalog_lock_wait_nanos.saturating_add(mark_profile.catalog_mark_lock_wait_nanos);
        observability_nanos =
            observability_nanos.saturating_add(mark_profile.observability_record_nanos);
    }
    let total_nanos = duration_nanos_u64(total_started.elapsed());
    timing.publish_routing_nanos = timing.publish_routing_nanos.saturating_add(routing_nanos);
    timing.publish_mark_catalog_nanos = timing
        .publish_mark_catalog_nanos
        .saturating_add(catalog_nanos);
    timing.publish_mark_lock_wait_nanos = timing
        .publish_mark_lock_wait_nanos
        .saturating_add(catalog_lock_wait_nanos);
    timing.publish_mark_observability_nanos = timing
        .publish_mark_observability_nanos
        .saturating_add(observability_nanos);
    // Everything not attributed to routing, the catalog transition, or event
    // recording lands here (dispatch call overhead plus loop bookkeeping), so
    // the mark buckets reconcile against the mark wall clock.
    timing.publish_mark_call_residual_nanos =
        timing
            .publish_mark_call_residual_nanos
            .saturating_add(total_nanos.saturating_sub(
                routing_nanos
                    .saturating_add(catalog_nanos)
                    .saturating_add(observability_nanos),
            ));
    Ok(())
}

impl BlockJournalOverlay {
    fn set_writer_epoch(&self, device_id: DeviceId, writer_epoch: WriterEpoch) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let device = inner.entry(device_id).or_default();
        device.writer_epoch = device.writer_epoch.max(writer_epoch);
        Ok(())
    }

    fn writer_epoch(&self, device_id: DeviceId) -> Result<WriterEpoch> {
        Ok(lock(&self.inner)?
            .get(&device_id)
            .map(|device| device.writer_epoch)
            .unwrap_or_else(|| WriterEpoch::from_raw(0)))
    }

    fn durable_through(&self, device_id: DeviceId) -> Result<CommitSeq> {
        Ok(lock(&self.inner)?
            .get(&device_id)
            .map(|device| device.durable_through)
            .unwrap_or_else(|| CommitSeq::from_raw(0)))
    }

    fn visible_through(&self, device_id: DeviceId) -> Result<CommitSeq> {
        Ok(lock(&self.inner)?
            .get(&device_id)
            .map(|device| device.visible_through)
            .unwrap_or_else(|| CommitSeq::from_raw(0)))
    }

    fn apply_commit(&self, commit: &BlockJournalCommit, block_size: u64) -> Result<u64> {
        let started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let device = inner.entry(commit.device_id).or_default();
        set_block_journal_overlay_block_size(device, block_size)?;
        device.writer_epoch = device.writer_epoch.max(commit.writer_epoch);
        device.visible_through = device.visible_through.max(commit.commit_seq);
        for entry in commit.overlay_entries()? {
            insert_block_journal_lba_run(&mut device.lba_runs, block_size, entry)?;
        }
        Ok(duration_nanos_u64(started.elapsed()))
    }

    fn mark_durable(
        &self,
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
        durable_through: CommitSeq,
    ) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let device = inner.entry(device_id).or_default();
        device.writer_epoch = device.writer_epoch.max(writer_epoch);
        device.durable_through = device.durable_through.max(durable_through);
        Ok(())
    }

    fn apply_read_overlay(
        &self,
        storage: &impl StorageNodeReadService,
        device_id: DeviceId,
        requested: ByteRange,
        verification: ReadVerification,
        buf: &mut [u8],
    ) -> Result<u64> {
        let started = Instant::now();
        let requested_end = requested.end_exclusive()?;
        let inner = lock(&self.inner)?;
        let Some(device) = inner.get(&device_id) else {
            return Ok(duration_nanos_u64(started.elapsed()));
        };
        if device.lba_runs.is_empty() {
            return Ok(duration_nanos_u64(started.elapsed()));
        }
        let Some(block_size) = device.block_size else {
            return Err(StorageError::corrupt(
                "block journal LBA map has runs without block size",
            ));
        };
        if !requested.offset.is_multiple_of(block_size) || !requested.len.is_multiple_of(block_size)
        {
            return Err(StorageError::corrupt(
                "block journal LBA map read is not block aligned",
            ));
        }
        let start_lba = requested.offset / block_size;
        let end_lba = requested_end / block_size;
        if start_lba == end_lba {
            return Ok(duration_nanos_u64(started.elapsed()));
        }
        let first_key = match device.lba_runs.range(..=start_lba).next_back() {
            Some((key, entry)) => {
                let (_, run_end_lba) =
                    block_journal_overlay_run_bounds(*key, entry, block_size)?;
                if run_end_lba > start_lba {
                    *key
                } else {
                    start_lba
                }
            }
            None => start_lba,
        };
        let mut entries = Vec::new();
        for (key, entry) in device.lba_runs.range(first_key..end_lba) {
            let (run_start_lba, run_end_lba) =
                block_journal_overlay_run_bounds(*key, entry, block_size)?;
            if run_start_lba < end_lba && run_end_lba > start_lba {
                push_coalesced_block_journal_lba_read_entry(&mut entries, entry.clone())?;
            }
        }
        drop(inner);
        for entry in entries {
            apply_block_journal_read_entry(storage, &entry, requested, verification, buf)?;
        }
        Ok(duration_nanos_u64(started.elapsed()))
    }

    #[cfg(test)]
    fn lba_run_count_for_test(&self, device_id: DeviceId) -> Result<usize> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .get(&device_id)
            .map(|device| device.lba_runs.len())
            .unwrap_or(0))
    }

    #[cfg(test)]
    fn inline_payload_arc_count_for_test(&self, device_id: DeviceId) -> Result<usize> {
        let inner = lock(&self.inner)?;
        let Some(device) = inner.get(&device_id) else {
            return Ok(0);
        };
        let mut ptrs = BTreeSet::new();
        for entry in device.lba_runs.values() {
            match &entry.source {
                BlockJournalOverlaySource::Bytes { bytes, .. } => {
                    ptrs.insert(bytes.as_ptr() as usize);
                }
                BlockJournalOverlaySource::BytesFragments { fragments, .. } => {
                    for fragment in fragments.iter() {
                        ptrs.insert(fragment.bytes.as_ptr() as usize);
                    }
                }
                BlockJournalOverlaySource::Segment { .. } | BlockJournalOverlaySource::Sparse => {}
            }
        }
        Ok(ptrs.len())
    }

    #[cfg(test)]
    fn inline_payload_fragment_count_for_test(&self, device_id: DeviceId) -> Result<usize> {
        let inner = lock(&self.inner)?;
        let Some(device) = inner.get(&device_id) else {
            return Ok(0);
        };
        let mut count = 0_usize;
        for entry in device.lba_runs.values() {
            match &entry.source {
                BlockJournalOverlaySource::Bytes { .. } => count = count.saturating_add(1),
                BlockJournalOverlaySource::BytesFragments { fragments, .. } => {
                    count = count.saturating_add(fragments.len());
                }
                BlockJournalOverlaySource::Segment { .. } | BlockJournalOverlaySource::Sparse => {}
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod block_journal_tests {
    use super::*;

    #[derive(Debug)]
    struct CountingSegmentReadStorage {
        inner: LocalCoordinator,
        segment_reads: Mutex<Vec<ByteRange>>,
    }

    impl CountingSegmentReadStorage {
        fn new(inner: LocalCoordinator) -> Self {
            Self {
                inner,
                segment_reads: Mutex::new(Vec::new()),
            }
        }

        fn segment_reads(&self) -> Vec<ByteRange> {
            lock(&self.segment_reads).unwrap().clone()
        }
    }

    impl StorageNodeReadService for CountingSegmentReadStorage {
        fn read_segment_source(
            &self,
            storage_node: StorageNodeId,
            segment_id: SegmentId,
            range: ByteRange,
            integrity: SegmentPayloadIntegrity,
            verification: ReadVerification,
            buf: &mut [u8],
        ) -> Result<ReadSourceProfile> {
            lock(&self.segment_reads)?.push(range);
            StorageNodeReadService::read_segment_source(
                &self.inner,
                storage_node,
                segment_id,
                range,
                integrity,
                verification,
                buf,
            )
        }

        fn read_append_run_source(
            &self,
            storage_node: StorageNodeId,
            log_id: u64,
            range: ByteRange,
            integrity: SegmentPayloadIntegrity,
            verification: ReadVerification,
            buf: &mut [u8],
        ) -> Result<ReadSourceProfile> {
            StorageNodeReadService::read_append_run_source(
                &self.inner,
                storage_node,
                log_id,
                range,
                integrity,
                verification,
                buf,
            )
        }
    }

    fn journal_commit(
        device_id: DeviceId,
        commit_seq: u64,
        offset: u64,
        bytes: Vec<u8>,
    ) -> BlockJournalCommit {
        BlockJournalCommit {
            device_id,
            writer_epoch: WriterEpoch::from_raw(1),
            commit_seq: CommitSeq::from_raw(commit_seq),
            write_count: 1,
            collapsed_range_count: 1,
            committed_bytes: bytes.len() as u64,
            entries: vec![BlockJournalEntry::Write {
                range: ByteRange::new(offset, bytes.len() as u64),
                payload_integrity: PayloadIntegrity::Verified,
                bytes,
            }],
        }
    }

    fn journal_commit_from_writes(
        device_id: DeviceId,
        commit_seq: u64,
        writes: Vec<(u64, Vec<u8>)>,
    ) -> BlockJournalCommit {
        let committed_bytes = writes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
        let write_count = writes.len() as u64;
        BlockJournalCommit {
            device_id,
            writer_epoch: WriterEpoch::from_raw(1),
            commit_seq: CommitSeq::from_raw(commit_seq),
            write_count,
            collapsed_range_count: write_count,
            committed_bytes,
            entries: writes
                .into_iter()
                .map(|(offset, bytes)| BlockJournalEntry::Write {
                    range: ByteRange::new(offset, bytes.len() as u64),
                    payload_integrity: PayloadIntegrity::Verified,
                    bytes,
                })
                .collect(),
        }
    }

    fn sparse_commit(
        device_id: DeviceId,
        commit_seq: u64,
        range: ByteRange,
    ) -> BlockJournalCommit {
        BlockJournalCommit {
            device_id,
            writer_epoch: WriterEpoch::from_raw(1),
            commit_seq: CommitSeq::from_raw(commit_seq),
            write_count: 1,
            collapsed_range_count: 1,
            committed_bytes: 0,
            entries: vec![BlockJournalEntry::Sparse { range }],
        }
    }

    fn segment_commit(
        device_id: DeviceId,
        commit_seq: u64,
        range: ByteRange,
        receipt: &SegmentWriteReceipt,
    ) -> BlockJournalCommit {
        BlockJournalCommit {
            device_id,
            writer_epoch: WriterEpoch::from_raw(1),
            commit_seq: CommitSeq::from_raw(commit_seq),
            write_count: 1,
            collapsed_range_count: 1,
            committed_bytes: range.len,
            entries: vec![BlockJournalEntry::Segment {
                range,
                storage_node: receipt.storage_node,
                segment_id: receipt.segment_id,
                segment_offset: 0,
                integrity: receipt.integrity,
            }],
        }
    }

    fn repeated_block(value: u8, block: usize) -> Vec<u8> {
        vec![value; block]
    }

    #[test]
    fn block_journal_lba_map_latest_overlap_wins() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(7);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block * 4]), block as u64)
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                block as u64,
                vec![2; block],
            ), block as u64)
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 3);

        let mut read = vec![9; block * 4];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 4) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], vec![1; block]);
        assert_eq!(&read[block..block * 2], vec![2; block]);
        assert_eq!(&read[block * 2..], vec![1; block * 2]);
    }

    #[test]
    fn block_journal_lba_map_adjacent_writes_share_read_view() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(8);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block]), block as u64)
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                block as u64,
                vec![2; block],
            ), block as u64)
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 2);

        let mut read = vec![0; block * 2];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 2) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], vec![1; block]);
        assert_eq!(&read[block..], vec![2; block]);
    }

    #[test]
    fn block_journal_lba_map_preserves_disjoint_ranges_around_overlap() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(9);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block]), block as u64)
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                (block * 2) as u64,
                vec![2; block],
            ), block as u64)
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                3,
                block as u64,
                vec![3; block * 2],
            ), block as u64)
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 2);

        let mut read = vec![0; block * 3];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 3) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], vec![1; block]);
        assert_eq!(&read[block..], vec![3; block * 2]);
    }

    #[test]
    fn block_journal_lba_map_sparse_zero_overlays_base_bytes() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(10);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block * 3]), block as u64)
            .unwrap();
        overlay
            .apply_commit(
                &sparse_commit(
                    device_id,
                    2,
                    ByteRange::new(block as u64, block as u64),
                ),
                block as u64,
            )
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 3);

        let mut read = vec![7; block * 3];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 3) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], repeated_block(1, block));
        assert_eq!(&read[block..block * 2], repeated_block(0, block));
        assert_eq!(&read[block * 2..], repeated_block(1, block));
    }

    #[test]
    fn block_journal_lba_map_leaves_base_bytes_for_unmapped_runs() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(11);
        let block = 4096_usize;
        overlay
            .apply_commit(
                &journal_commit(device_id, 1, block as u64, vec![4; block]),
                block as u64,
            )
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 1);

        let mut read = vec![9; block * 3];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 3) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], repeated_block(9, block));
        assert_eq!(&read[block..block * 2], repeated_block(4, block));
        assert_eq!(&read[block * 2..], repeated_block(9, block));
    }

    #[test]
    fn block_journal_lba_map_uses_device_block_size_not_fixed_4k() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(12);
        let block = 64 * 1024_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![5; block * 2]), block as u64)
            .unwrap();
        overlay
            .apply_commit(
                &journal_commit(device_id, 2, block as u64, vec![6; block]),
                block as u64,
            )
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 2);

        let mut read = vec![0; block * 2];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 2) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], repeated_block(5, block));
        assert_eq!(&read[block..], repeated_block(6, block));
    }

    #[test]
    fn block_journal_lba_map_rejects_unaligned_overlay_entry() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(13);
        let block = 4096_u64;
        let error = overlay
            .apply_commit(&journal_commit(device_id, 1, 1, vec![1; block as usize]), block)
            .unwrap_err();
        assert!(matches!(error, StorageError::Corrupt { .. }));
    }

    #[test]
    fn block_journal_lba_map_contiguous_inline_write_creates_single_shared_run() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(14);
        let block = 4096_usize;
        let write_len = 1024 * 1024_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![8; write_len]), block as u64)
            .unwrap();

        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 1);
        assert_eq!(overlay.inline_payload_arc_count_for_test(device_id).unwrap(), 1);
    }

    #[test]
    fn block_journal_lba_map_contiguous_batch_entries_create_single_fragmented_run() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(17);
        let block = 4096_usize;
        let writes = (0..256_usize)
            .map(|index| {
                (
                    (index * block) as u64,
                    repeated_block(index.wrapping_rem(251) as u8, block),
                )
            })
            .collect::<Vec<_>>();
        overlay
            .apply_commit(
                &journal_commit_from_writes(device_id, 1, writes.clone()),
                block as u64,
            )
            .unwrap();

        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 1);
        assert_eq!(
            overlay
                .inline_payload_fragment_count_for_test(device_id)
                .unwrap(),
            256
        );
        let mut read = vec![0; block * 256];
        overlay
            .apply_read_overlay(
                &LocalCoordinator::new(),
                device_id,
                ByteRange::new(0, (block * 256) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        let expected = writes
            .into_iter()
            .flat_map(|(_, bytes)| bytes)
            .collect::<Vec<_>>();
        assert_eq!(read, expected);
    }

    #[test]
    fn block_journal_lba_map_reads_segment_ref_run() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(15);
        let block = 4096_usize;
        let storage = LocalCoordinator::new();
        let payload = [repeated_block(1, block), repeated_block(2, block)].concat();
        let receipt = storage
            .write_segment_for_owner(MappingOwner::BlockDevice(device_id), &payload)
            .unwrap();
        overlay
            .apply_commit(
                &segment_commit(
                    device_id,
                    1,
                    ByteRange::new(0, (block * 2) as u64),
                    &receipt,
                ),
                block as u64,
            )
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 1);

        let mut read = vec![0; block];
        overlay
            .apply_read_overlay(
                &storage,
                device_id,
                ByteRange::new(block as u64, block as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(read, repeated_block(2, block));
    }

    #[test]
    fn block_journal_lba_map_coalesces_contiguous_segment_ref_read() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(16);
        let block = 4096_usize;
        let read_len = 1024 * 1024_usize;
        let storage = LocalCoordinator::new();
        let payload = (0..read_len)
            .map(|index| (index / block) as u8)
            .collect::<Vec<_>>();
        let receipt = storage
            .write_segment_for_owner(MappingOwner::BlockDevice(device_id), &payload)
            .unwrap();
        overlay
            .apply_commit(
                &segment_commit(
                    device_id,
                    1,
                    ByteRange::new(0, read_len as u64),
                    &receipt,
                ),
                block as u64,
            )
            .unwrap();
        assert_eq!(overlay.lba_run_count_for_test(device_id).unwrap(), 1);

        let counting = CountingSegmentReadStorage::new(storage);
        let mut read = vec![0; read_len];
        overlay
            .apply_read_overlay(
                &counting,
                device_id,
                ByteRange::new(0, read_len as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();

        assert_eq!(read, payload);
        assert_eq!(
            counting.segment_reads(),
            vec![ByteRange::new(0, read_len as u64)]
        );
    }

    #[test]
    fn block_journal_packed_write_record_round_trips_and_expands() {
        let device_id = DeviceId::from_raw(18);
        let first = journal_commit(device_id, 7, 0, repeated_block(41, 4096));
        let second = journal_commit(device_id, 9, 4096, repeated_block(42, 4096));
        let records = vec![
            BlockJournalRecord::Write(first.clone()),
            BlockJournalRecord::Write(second.clone()),
        ];

        let mut encoder = DurableEncoder::default();
        encode_block_journal_record_sequence(&records, &mut encoder).unwrap();
        let decoded = decode_row::<Vec<BlockJournalRecord>>(&encoder.finish()).unwrap();

        assert_eq!(decoded.len(), 1);
        let BlockJournalRecord::PackedWrites(packed) = &decoded[0] else {
            panic!("eligible inline writes should encode as one packed record");
        };
        assert_eq!(packed.device_id, device_id);
        assert_eq!(packed.writer_epoch, WriterEpoch::from_raw(1));
        assert_eq!(packed.first_commit_seq, CommitSeq::from_raw(7));
        assert_eq!(packed.block_size, 4096);
        assert_eq!(packed.entries.len(), 2);
        assert_eq!(packed.entries[0].commit_seq_delta, 0);
        assert_eq!(packed.entries[1].commit_seq_delta, 2);
        assert_eq!(packed.expand_commits().unwrap(), vec![first, second]);
    }

    #[test]
    fn block_journal_packed_write_record_rejects_bad_checksum() {
        let payload = [repeated_block(51, 4096), repeated_block(52, 4096)].concat();
        let packed = PackedBlockJournalWrites {
            device_id: DeviceId::from_raw(19),
            writer_epoch: WriterEpoch::from_raw(3),
            block_size: 4096,
            first_commit_seq: CommitSeq::from_raw(11),
            entries: vec![
                PackedBlockJournalEntry {
                    commit_seq_delta: 0,
                    lba: 0,
                    block_count: 1,
                    payload_offset: 0,
                    payload_integrity: PayloadIntegrity::Verified,
                },
                PackedBlockJournalEntry {
                    commit_seq_delta: 1,
                    lba: 1,
                    block_count: 1,
                    payload_offset: 4096,
                    payload_integrity: PayloadIntegrity::Verified,
                },
            ],
            payload_checksum: 0,
            payload_slab: payload,
        };

        let bytes = encode_row(&BlockJournalRecord::PackedWrites(packed)).unwrap();
        assert!(decode_row::<BlockJournalRecord>(&bytes).is_err());
    }

    #[test]
    fn block_journal_packed_write_record_rejects_nonmonotonic_sequences() {
        let payload = [repeated_block(61, 4096), repeated_block(62, 4096)].concat();
        let packed = PackedBlockJournalWrites {
            device_id: DeviceId::from_raw(20),
            writer_epoch: WriterEpoch::from_raw(3),
            block_size: 4096,
            first_commit_seq: CommitSeq::from_raw(11),
            entries: vec![
                PackedBlockJournalEntry {
                    commit_seq_delta: 0,
                    lba: 0,
                    block_count: 1,
                    payload_offset: 0,
                    payload_integrity: PayloadIntegrity::Verified,
                },
                PackedBlockJournalEntry {
                    commit_seq_delta: 0,
                    lba: 1,
                    block_count: 1,
                    payload_offset: 4096,
                    payload_integrity: PayloadIntegrity::Verified,
                },
            ],
            payload_checksum: data_log_checksum(&payload),
            payload_slab: payload,
        };

        let bytes = encode_row(&BlockJournalRecord::PackedWrites(packed)).unwrap();
        assert!(decode_row::<BlockJournalRecord>(&bytes).is_err());
    }
}

impl DurableCodec for BlockJournalEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Write {
                range,
                payload_integrity,
                bytes,
            } => {
                1u8.encode(out)?;
                range.encode(out)?;
                payload_integrity.encode(out)?;
                bytes.encode(out)
            }
            Self::Segment {
                range,
                storage_node,
                segment_id,
                segment_offset,
                integrity,
            } => {
                3u8.encode(out)?;
                range.encode(out)?;
                storage_node.encode(out)?;
                segment_id.encode(out)?;
                segment_offset.encode(out)?;
                integrity.encode(out)
            }
            Self::Sparse { range } => {
                2u8.encode(out)?;
                range.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Write {
                range: ByteRange::decode(input)?,
                payload_integrity: PayloadIntegrity::decode(input)?,
                bytes: Vec::<u8>::decode(input)?,
            }),
            2 => Ok(Self::Sparse {
                range: ByteRange::decode(input)?,
            }),
            3 => Ok(Self::Segment {
                range: ByteRange::decode(input)?,
                storage_node: StorageNodeId::decode(input)?,
                segment_id: SegmentId::decode(input)?,
                segment_offset: u64::decode(input)?,
                integrity: SegmentPayloadIntegrity::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid block journal entry kind")),
        }
    }
}

impl DurableCodec for BlockJournalCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        1u8.encode(out)?;
        self.device_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.commit_seq.encode(out)?;
        self.write_count.encode(out)?;
        self.collapsed_range_count.encode(out)?;
        self.committed_bytes.encode(out)?;
        self.entries.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self {
                device_id: DeviceId::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
                commit_seq: CommitSeq::decode(input)?,
                write_count: u64::decode(input)?,
                collapsed_range_count: u64::decode(input)?,
                committed_bytes: u64::decode(input)?,
                entries: Vec::<BlockJournalEntry>::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid block journal commit version")),
        }
    }
}

impl DurableCodec for PackedBlockJournalEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq_delta.encode(out)?;
        self.lba.encode(out)?;
        self.block_count.encode(out)?;
        self.payload_offset.encode(out)?;
        self.payload_integrity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq_delta: u64::decode(input)?,
            lba: u64::decode(input)?,
            block_count: u64::decode(input)?,
            payload_offset: u64::decode(input)?,
            payload_integrity: PayloadIntegrity::decode(input)?,
        })
    }
}

impl DurableCodec for PackedBlockJournalWrites {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        1u8.encode(out)?;
        self.device_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.block_size.encode(out)?;
        self.first_commit_seq.encode(out)?;
        self.entries.encode(out)?;
        usize_to_u64(self.payload_slab.len()).encode(out)?;
        self.payload_checksum.encode(out)?;
        out.bytes.extend_from_slice(&self.payload_slab);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => {
                let device_id = DeviceId::decode(input)?;
                let writer_epoch = WriterEpoch::decode(input)?;
                let block_size = u64::decode(input)?;
                let first_commit_seq = CommitSeq::decode(input)?;
                let entries = Vec::<PackedBlockJournalEntry>::decode(input)?;
                let payload_len = u64::decode(input)?;
                let payload_checksum = u64::decode(input)?;
                let payload_len = usize::try_from(payload_len).map_err(|_| {
                    durable_codec_error("packed block journal payload length overflows usize")
                })?;
                let payload_slab = input.take(payload_len)?.to_vec();
                let packed = Self {
                    device_id,
                    writer_epoch,
                    block_size,
                    first_commit_seq,
                    entries,
                    payload_checksum,
                    payload_slab,
                };
                packed.validate()?;
                Ok(packed)
            }
            _ => Err(durable_codec_error(
                "invalid packed block journal write version",
            )),
        }
    }
}

impl DurableCodec for BlockJournalRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Lease {
                device_id,
                writer_epoch,
            } => {
                1u8.encode(out)?;
                device_id.encode(out)?;
                writer_epoch.encode(out)
            }
            Self::Write(commit) => {
                2u8.encode(out)?;
                commit.encode(out)
            }
            Self::PackedWrites(packed) => {
                4u8.encode(out)?;
                packed.encode(out)
            }
            Self::Flush {
                device_id,
                writer_epoch,
                durable_through,
            } => {
                3u8.encode(out)?;
                device_id.encode(out)?;
                writer_epoch.encode(out)?;
                durable_through.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Lease {
                device_id: DeviceId::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
            }),
            2 => Ok(Self::Write(BlockJournalCommit::decode(input)?)),
            3 => Ok(Self::Flush {
                device_id: DeviceId::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
                durable_through: CommitSeq::decode(input)?,
            }),
            4 => Ok(Self::PackedWrites(PackedBlockJournalWrites::decode(input)?)),
            _ => Err(durable_codec_error("invalid block journal record kind")),
        }
    }
}

fn block_journal_gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn block_journal_packing_unit_add(unit: u64, value: u64) -> u64 {
    if value == 0 {
        unit
    } else if unit == 0 {
        value
    } else {
        block_journal_gcd(unit, value)
    }
}

fn block_journal_commit_packing_entry(
    commit: &BlockJournalCommit,
) -> Option<(&ByteRange, PayloadIntegrity, &[u8])> {
    if commit.write_count != 1 || commit.collapsed_range_count != 1 || commit.entries.len() != 1 {
        return None;
    }
    let BlockJournalEntry::Write {
        range,
        payload_integrity,
        bytes,
    } = &commit.entries[0]
    else {
        return None;
    };
    if range.len == 0 {
        return None;
    }
    let bytes_len = u64::try_from(bytes.len()).ok()?;
    if bytes_len != range.len || commit.committed_bytes != range.len {
        return None;
    }
    Some((range, *payload_integrity, bytes))
}

fn block_journal_packed_group_len(
    records: &[BlockJournalRecord],
    start: usize,
) -> Option<usize> {
    let BlockJournalRecord::Write(first) = records.get(start)? else {
        return None;
    };
    let mut group_len = 0_usize;
    let mut previous_seq = None;
    for record in &records[start..] {
        let BlockJournalRecord::Write(commit) = record else {
            break;
        };
        if commit.device_id != first.device_id || commit.writer_epoch != first.writer_epoch {
            break;
        }
        if previous_seq.is_some_and(|seq| commit.commit_seq.raw() <= seq) {
            break;
        }
        block_journal_commit_packing_entry(commit)?;
        group_len = group_len.saturating_add(1);
        previous_seq = Some(commit.commit_seq.raw());
    }
    if group_len > 1 {
        Some(group_len)
    } else {
        None
    }
}

fn block_journal_packed_group_unit(records: &[BlockJournalRecord]) -> Result<u64> {
    let mut unit = 0_u64;
    for record in records {
        let BlockJournalRecord::Write(commit) = record else {
            return Err(StorageError::corrupt(
                "packed block journal group includes non-write record",
            ));
        };
        let (range, _, _) = block_journal_commit_packing_entry(commit).ok_or_else(|| {
            StorageError::corrupt("packed block journal group includes ineligible write")
        })?;
        unit = block_journal_packing_unit_add(unit, range.offset);
        unit = block_journal_packing_unit_add(unit, range.len);
    }
    if unit == 0 {
        return Err(StorageError::corrupt(
            "packed block journal group has no range unit",
        ));
    }
    Ok(unit)
}

fn block_journal_encoded_record_count(records: &[BlockJournalRecord]) -> Result<u64> {
    let mut count = 0_u64;
    let mut index = 0_usize;
    while index < records.len() {
        if let Some(group_len) = block_journal_packed_group_len(records, index) {
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("block journal record count overflows"))?;
            index = index.saturating_add(group_len);
        } else {
            count = count
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("block journal record count overflows"))?;
            index = index.saturating_add(1);
        }
    }
    Ok(count)
}

fn encode_packed_block_journal_writes_from_records(
    records: &[BlockJournalRecord],
    out: &mut DurableEncoder,
) -> Result<()> {
    let Some(BlockJournalRecord::Write(first)) = records.first() else {
        return Err(StorageError::invalid_argument(
            "packed block journal group must start with a write",
        ));
    };
    4u8.encode(out)?;
    1u8.encode(out)?;
    first.device_id.encode(out)?;
    first.writer_epoch.encode(out)?;
    let block_size = block_journal_packed_group_unit(records)?;
    block_size.encode(out)?;
    first.commit_seq.encode(out)?;
    usize_to_u64(records.len()).encode(out)?;
    let mut payload_offset = 0_u64;
    let mut payload_chunks = Vec::new();
    for record in records {
        let BlockJournalRecord::Write(commit) = record else {
            return Err(StorageError::corrupt(
                "packed block journal group includes non-write record",
            ));
        };
        let (range, payload_integrity, bytes) = block_journal_commit_packing_entry(commit)
            .ok_or_else(|| {
                StorageError::corrupt("packed block journal group includes ineligible write")
            })?;
        let commit_seq_delta = commit
            .commit_seq
            .raw()
            .checked_sub(first.commit_seq.raw())
            .ok_or_else(|| StorageError::corrupt("packed block journal sequence underflows"))?;
        commit_seq_delta.encode(out)?;
        if !range.offset.is_multiple_of(block_size) || !range.len.is_multiple_of(block_size) {
            return Err(StorageError::corrupt(
                "packed block journal range is not aligned to packing unit",
            ));
        }
        let lba = range.offset / block_size;
        let block_count = range.len / block_size;
        lba.encode(out)?;
        block_count.encode(out)?;
        payload_offset.encode(out)?;
        payload_integrity.encode(out)?;
        payload_offset = payload_offset.checked_add(range.len).ok_or_else(|| {
            StorageError::conflict("packed block journal payload offset overflows")
        })?;
        payload_chunks.push(bytes);
    }

    payload_offset.encode(out)?;
    data_log_checksum_chunks(&payload_chunks).encode(out)?;
    for chunk in payload_chunks {
        out.bytes.extend_from_slice(chunk);
    }
    Ok(())
}

fn encode_block_journal_record_sequence(
    records: &[BlockJournalRecord],
    out: &mut DurableEncoder,
) -> Result<()> {
    block_journal_encoded_record_count(records)?.encode(out)?;
    let mut index = 0_usize;
    while index < records.len() {
        if let Some(group_len) = block_journal_packed_group_len(records, index) {
            encode_packed_block_journal_writes_from_records(
                &records[index..index + group_len],
                out,
            )?;
            index += group_len;
        } else {
            records[index].encode(out)?;
            index += 1;
        }
    }
    Ok(())
}

fn block_journal_records_frame(records: &[BlockJournalRecord]) -> Result<Vec<u8>> {
    let mut encoder = DurableEncoder::default();
    encode_block_journal_record_sequence(records, &mut encoder)?;
    durable_journal_frame_from_payload(
        encoder.finish(),
        &BLOCK_JOURNAL_MAGIC,
        "block journal record exceeds durable payload limit",
    )
}

fn observe_block_journal_replay_write(
    local: &LocalCoordinator,
    overlay: &BlockJournalOverlay,
    materialized: &BTreeMap<DeviceId, CommitSeq>,
    latest_epoch: &mut BTreeMap<DeviceId, WriterEpoch>,
    writes: &mut BTreeMap<DeviceId, BTreeMap<u64, BlockJournalCommit>>,
    last_write_seq: &mut BTreeMap<DeviceId, u64>,
    commit: BlockJournalCommit,
) -> Result<()> {
    if last_write_seq
        .get(&commit.device_id)
        .is_some_and(|previous| commit.commit_seq.raw() <= *previous)
    {
        return Err(StorageError::corrupt(
            "block journal write commit sequences are not monotonic",
        ));
    }
    last_write_seq.insert(commit.device_id, commit.commit_seq.raw());

    let info = local.metadata.device_info(commit.device_id)?;
    commit.validate(&info.spec)?;
    local
        .metadata
        .observe_allocated_commit_seq(commit.commit_seq)?;
    local.seed_block_writer_epoch(commit.device_id, commit.writer_epoch)?;
    overlay.set_writer_epoch(commit.device_id, commit.writer_epoch)?;
    latest_epoch
        .entry(commit.device_id)
        .and_modify(|epoch| *epoch = (*epoch).max(commit.writer_epoch))
        .or_insert(commit.writer_epoch);
    if materialized
        .get(&commit.device_id)
        .is_some_and(|high| commit.commit_seq.raw() <= high.raw())
    {
        return Ok(());
    }
    let device_writes = writes.entry(commit.device_id).or_default();
    if device_writes
        .insert(commit.commit_seq.raw(), commit)
        .is_some()
    {
        return Err(StorageError::corrupt(
            "block journal contains duplicate write commit sequence",
        ));
    }
    Ok(())
}

impl DurableSqliteStore {
    fn load_block_journal_overlay(&self, local: &LocalCoordinator) -> Result<BlockJournalOverlay> {
        let records = self.block_journal_records()?;
        let overlay = BlockJournalOverlay::default();
        let materialized = local.metadata.block_materialized_high_water()?;
        let mut latest_epoch = BTreeMap::<DeviceId, WriterEpoch>::new();
        let mut durable_through = BTreeMap::<DeviceId, CommitSeq>::new();
        let mut writes = BTreeMap::<DeviceId, BTreeMap<u64, BlockJournalCommit>>::new();
        let mut last_write_seq = BTreeMap::<DeviceId, u64>::new();

        for record in records {
            match record {
                BlockJournalRecord::Lease {
                    device_id,
                    writer_epoch,
                } => {
                    local.seed_block_writer_epoch(device_id, writer_epoch)?;
                    overlay.set_writer_epoch(device_id, writer_epoch)?;
                    latest_epoch
                        .entry(device_id)
                        .and_modify(|epoch| *epoch = (*epoch).max(writer_epoch))
                        .or_insert(writer_epoch);
                }
                BlockJournalRecord::Write(commit) => {
                    observe_block_journal_replay_write(
                        local,
                        &overlay,
                        &materialized,
                        &mut latest_epoch,
                        &mut writes,
                        &mut last_write_seq,
                        commit,
                    )?;
                }
                BlockJournalRecord::PackedWrites(packed) => {
                    for commit in packed.expand_commits()? {
                        observe_block_journal_replay_write(
                            local,
                            &overlay,
                            &materialized,
                            &mut latest_epoch,
                            &mut writes,
                            &mut last_write_seq,
                            commit,
                        )?;
                    }
                }
                BlockJournalRecord::Flush {
                    device_id,
                    writer_epoch,
                    durable_through: flushed_through,
                } => {
                    local
                        .metadata
                        .observe_allocated_commit_seq(flushed_through)?;
                    local.seed_block_writer_epoch(device_id, writer_epoch)?;
                    overlay.mark_durable(device_id, writer_epoch, flushed_through)?;
                    latest_epoch
                        .entry(device_id)
                        .and_modify(|epoch| *epoch = (*epoch).max(writer_epoch))
                        .or_insert(writer_epoch);
                    if materialized
                        .get(&device_id)
                        .is_some_and(|high| flushed_through.raw() <= high.raw())
                    {
                        continue;
                    }
                    durable_through
                        .entry(device_id)
                        .and_modify(|durable| *durable = (*durable).max(flushed_through))
                        .or_insert(flushed_through);
                }
            }
        }

        // Catalog rows publish asynchronously behind flushed acks, so a crash
        // can leave durable journal records referencing segments that have no
        // SQLite rows yet. Rebuild those segments from the self-describing
        // data logs before replay resolves their receipts.
        let mut missing_segments: BTreeMap<
            StorageNodeId,
            BTreeMap<SegmentId, (SegmentPayloadIntegrity, DeviceId)>,
        > = BTreeMap::new();
        for (device_id, durable) in &durable_through {
            let Some(device_writes) = writes.get(device_id) else {
                continue;
            };
            for commit in device_writes.values() {
                if commit.commit_seq.raw() > durable.raw() {
                    continue;
                }
                for entry in &commit.entries {
                    let BlockJournalEntry::Segment {
                        storage_node,
                        segment_id,
                        integrity,
                        ..
                    } = entry
                    else {
                        continue;
                    };
                    if local.storage_nodes.segment_exists(*segment_id)? {
                        continue;
                    }
                    missing_segments
                        .entry(*storage_node)
                        .or_default()
                        .insert(*segment_id, (*integrity, *device_id));
                }
            }
        }
        self.recover_block_segment_rows(local, missing_segments)?;

        for (device_id, durable) in durable_through {
            let Some(device_writes) = writes.get(&device_id) else {
                continue;
            };
            let info = local.metadata.device_info(device_id)?;
            let block_size = u64::from(info.spec.block_size);
            for commit in device_writes.values() {
                if commit.commit_seq.raw() > durable.raw() {
                    continue;
                }
                if let Some(epoch) = latest_epoch.get(&device_id)
                    && commit.writer_epoch.raw() > epoch.raw()
                {
                    return Err(StorageError::corrupt(
                        "block journal write uses epoch above durable lease high-water",
                    ));
                }
                mark_block_journal_segment_refs_referenced(
                    local,
                    commit,
                    &mut BlockJournalLaneBatchTiming::default(),
                )?;
                local
                    .metadata
                    .replay_block_journal_commit(device_id, commit.commit_seq)?;
                overlay.apply_commit(commit, block_size)?;
            }
            overlay.mark_durable(
                device_id,
                overlay.writer_epoch(device_id)?,
                durable,
            )?;
        }

        Ok(overlay)
    }

    /// Rebuild in-memory state and catalog rows for journal-referenced
    /// segments whose asynchronous row publication did not survive a crash.
    ///
    /// Every such segment's payload was synced on its node before the
    /// referencing journal record became durable, so a header walk of that
    /// node's data logs must find it; failing to is corruption. Rows publish
    /// before this returns, so a recovered reopen is row-for-row equivalent
    /// to one where the publisher had drained.
    fn recover_block_segment_rows(
        &self,
        local: &LocalCoordinator,
        missing: BTreeMap<StorageNodeId, BTreeMap<SegmentId, (SegmentPayloadIntegrity, DeviceId)>>,
    ) -> Result<()> {
        if missing.is_empty() {
            return Ok(());
        }
        let mut pending = PendingDataLogAppend::default();
        let mut adopted = BTreeSet::new();
        for (storage_node, wanted) in missing {
            let wanted_ids: BTreeSet<SegmentId> = wanted.keys().copied().collect();
            let mut records =
                scan_node_data_logs_for_segments(&self.paths.data_dir, storage_node, &wanted_ids)?;
            for (segment_id, (integrity, device_id)) in wanted {
                let Some(record) = records.remove(&segment_id) else {
                    return Err(StorageError::corrupt(
                        "durable journal references a segment with no durable data-log record",
                    ));
                };
                if record.placement.integrity != integrity {
                    return Err(StorageError::corrupt(
                        "recovered segment integrity disagrees with journal entry",
                    ));
                }
                let log_ref = DurableDataLogRef {
                    storage_node,
                    log_id: record.placement.data_log_id,
                };
                let manifest = pending.logs.entry(log_ref).or_insert(PendingDataLogManifest {
                    storage_node,
                    log_id: log_ref.log_id,
                    state: self
                        .node_data_log_state(log_ref)?
                        .unwrap_or_else(|| GENERIC_DATA_LOG_STATE_ACTIVE.to_string()),
                    total_bytes: 0,
                    needs_dir_sync: false,
                });
                manifest.total_bytes = manifest.total_bytes.max(record.physical_record_end);
                local.adopt_recovered_segment(
                    MappingOwner::BlockDevice(device_id),
                    storage_node,
                    segment_id,
                    record.bytes,
                    integrity,
                )?;
                pending.placements.push(record.placement);
                adopted.insert(segment_id);
            }
        }
        let nodes = local.selected_state_for_segment_ids(&adopted)?;
        self.persist_block_journal_segment_refs(&nodes, &adopted, Vec::new(), pending, true)?;
        Ok(())
    }
}
