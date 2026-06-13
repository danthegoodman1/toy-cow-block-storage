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
pub(super) enum BlockJournalRecord {
    Lease {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
    },
    Write(BlockJournalCommit),
    Flush {
        device_id: DeviceId,
        writer_epoch: WriterEpoch,
        durable_through: CommitSeq,
    },
}

#[derive(Debug, Clone)]
pub(super) enum BlockJournalOverlaySource {
    Bytes {
        payload_integrity: PayloadIntegrity,
        bytes: Vec<u8>,
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
    // Collapsed newest-wins read index keyed by range start, so inserts and
    // range reads cost O(log n + overlaps) instead of shifting a sorted
    // vector. Only the current bytes per range are retained; shadowed
    // history lives in the journal file, not in memory.
    read_entries: BTreeMap<u64, BlockJournalOverlayEntry>,
}

impl Default for BlockJournalDeviceOverlay {
    fn default() -> Self {
        Self {
            writer_epoch: WriterEpoch::from_raw(0),
            durable_through: CommitSeq::from_raw(0),
            visible_through: CommitSeq::from_raw(0),
            read_entries: BTreeMap::new(),
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
    pub(super) publish_receipt_nanos: u64,
    pub(super) publish_evidence_nanos: u64,
    pub(super) publish_dispatch_nanos: u64,
    pub(super) publish_verify_nanos: u64,
    pub(super) publish_mark_catalog_nanos: u64,
    pub(super) publish_mark_lock_wait_nanos: u64,
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

    fn segment_id(&self) -> Option<SegmentId> {
        match self {
            Self::Segment { segment_id, .. } => Some(*segment_id),
            Self::Write { .. } | Self::Sparse { .. } => None,
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

    fn overlay_entries(&self) -> Vec<BlockJournalOverlayEntry> {
        self.entries
            .iter()
            .map(|entry| {
                let source = match entry {
                    BlockJournalEntry::Write {
                        payload_integrity,
                        bytes,
                        ..
                    } => BlockJournalOverlaySource::Bytes {
                        payload_integrity: *payload_integrity,
                        bytes: bytes.clone(),
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
                BlockJournalOverlayEntry {
                    range: entry.range(),
                    source,
                }
            })
            .collect()
    }
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
            "block journal read index slice is outside source range",
        ));
    }
    match source {
        BlockJournalOverlaySource::Sparse => Ok(BlockJournalOverlaySource::Sparse),
        BlockJournalOverlaySource::Bytes {
            payload_integrity,
            bytes,
        } => {
            let start = usize::try_from(slice.offset - source_range.offset).map_err(|_| {
                StorageError::corrupt("block journal read index slice offset overflows usize")
            })?;
            let len = usize::try_from(slice.len).map_err(|_| {
                StorageError::corrupt("block journal read index slice length overflows usize")
            })?;
            let end = start.checked_add(len).ok_or_else(|| {
                StorageError::corrupt("block journal read index slice end overflows")
            })?;
            let bytes = bytes
                .get(start..end)
                .ok_or_else(|| {
                    StorageError::corrupt("block journal read index slice out of bounds")
                })?
                .to_vec();
            Ok(BlockJournalOverlaySource::Bytes {
                payload_integrity: *payload_integrity,
                bytes,
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

fn insert_block_journal_read_entry(
    entries: &mut BTreeMap<u64, BlockJournalOverlayEntry>,
    entry: BlockJournalOverlayEntry,
) -> Result<()> {
    let entry_end = entry.range.end_exclusive()?;

    let mut overlapping = Vec::new();
    if let Some((&key, existing)) = entries.range(..entry.range.offset).next_back()
        && existing.range.end_exclusive()? > entry.range.offset
    {
        overlapping.push(key);
    }
    overlapping.extend(
        entries
            .range(entry.range.offset..entry_end)
            .map(|(&key, _)| key),
    );
    for key in overlapping {
        let Some(existing) = entries.remove(&key) else {
            continue;
        };
        let existing_end = existing.range.end_exclusive()?;
        if existing.range.offset < entry.range.offset {
            let left = block_journal_overlay_slice(
                &existing,
                ByteRange::new(
                    existing.range.offset,
                    entry.range.offset - existing.range.offset,
                ),
            )?;
            entries.insert(left.range.offset, left);
        }
        if existing_end > entry_end {
            let right = block_journal_overlay_slice(
                &existing,
                ByteRange::new(entry_end, existing_end - entry_end),
            )?;
            entries.insert(right.range.offset, right);
        }
    }

    // Merge with adjacent same-source neighbors so contiguous writes keep
    // the index minimal.
    let mut merged = entry;
    let previous_key = entries
        .range(..merged.range.offset)
        .next_back()
        .map(|(&key, _)| key);
    if let Some(previous_key) = previous_key
        && let Some(previous) = entries.get(&previous_key)
        && previous.range.end_exclusive()? == merged.range.offset
    {
        let mut candidate = previous.clone();
        if try_merge_block_journal_read_entry(&mut candidate, &merged)? {
            entries.remove(&previous_key);
            merged = candidate;
        }
    }
    let merged_end = merged.range.end_exclusive()?;
    if let Some(next) = entries.get(&merged_end) {
        let next = next.clone();
        if try_merge_block_journal_read_entry(&mut merged, &next)? {
            entries.remove(&merged_end);
        }
    }
    entries.insert(merged.range.offset, merged);
    Ok(())
}

fn try_merge_block_journal_read_entry(
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
        .ok_or_else(|| StorageError::corrupt("block journal read index range overflows"))?;
    match (&mut left.source, &right.source) {
        (BlockJournalOverlaySource::Sparse, BlockJournalOverlaySource::Sparse) => {
            left.range.len = merged_len;
            Ok(true)
        }
        (
            BlockJournalOverlaySource::Bytes {
                payload_integrity: left_integrity,
                bytes: left_bytes,
            },
            BlockJournalOverlaySource::Bytes {
                payload_integrity: right_integrity,
                bytes: right_bytes,
            },
        ) if left_integrity == right_integrity && merged_len <= BLOCK_JOURNAL_INLINE_MAX_BYTES => {
            left_bytes.extend_from_slice(right_bytes);
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
        } => {
            let source_start = usize::try_from(overlap.offset - entry.range.offset)
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

fn mark_block_journal_segment_refs_referenced(
    local: &LocalCoordinator,
    commit: &BlockJournalCommit,
    timing: &mut BlockJournalLaneBatchTiming,
) -> Result<()> {
    for segment_id in commit
        .entries
        .iter()
        .filter_map(BlockJournalEntry::segment_id)
    {
        let receipt_started = Instant::now();
        let receipt = local.storage_nodes.receipt_for_segment(segment_id)?;
        timing.publish_receipt_nanos = timing
            .publish_receipt_nanos
            .saturating_add(duration_nanos_u64(receipt_started.elapsed()));
        let mark_profile = local.storage_nodes.mark_segment_referenced_profiled(
            &receipt,
            commit.commit_seq,
            local.authority.as_ref(),
        )?;
        timing.publish_evidence_nanos = timing
            .publish_evidence_nanos
            .saturating_add(mark_profile.evidence_create_nanos);
        timing.publish_dispatch_nanos = timing
            .publish_dispatch_nanos
            .saturating_add(mark_profile.transport_dispatch_nanos);
        timing.publish_verify_nanos = timing
            .publish_verify_nanos
            .saturating_add(mark_profile.verify_nanos);
        timing.publish_mark_catalog_nanos = timing
            .publish_mark_catalog_nanos
            .saturating_add(mark_profile.catalog_mark_nanos);
        timing.publish_mark_lock_wait_nanos = timing
            .publish_mark_lock_wait_nanos
            .saturating_add(mark_profile.catalog_mark_lock_wait_nanos);
    }
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

    fn apply_commit(&self, commit: &BlockJournalCommit) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let device = inner.entry(commit.device_id).or_default();
        device.writer_epoch = device.writer_epoch.max(commit.writer_epoch);
        device.visible_through = device.visible_through.max(commit.commit_seq);
        for entry in commit.overlay_entries() {
            insert_block_journal_read_entry(&mut device.read_entries, entry)?;
        }
        Ok(())
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
        if let Some((_, entry)) = device.read_entries.range(..requested.offset).next_back()
            && entry.range.end_exclusive()? > requested.offset
        {
            apply_block_journal_read_entry(storage, entry, requested, verification, buf)?;
        }
        for (_, entry) in device.read_entries.range(requested.offset..requested_end) {
            apply_block_journal_read_entry(storage, entry, requested, verification, buf)?;
        }
        Ok(duration_nanos_u64(started.elapsed()))
    }

    #[cfg(test)]
    fn read_entry_count_for_test(&self, device_id: DeviceId) -> Result<usize> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .get(&device_id)
            .map(|device| device.read_entries.len())
            .unwrap_or(0))
    }
}

#[cfg(test)]
mod block_journal_tests {
    use super::*;

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

    #[test]
    fn block_journal_read_index_collapses_shadowed_ranges() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(7);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block * 4]))
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                block as u64,
                vec![2; block],
            ))
            .unwrap();
        assert_eq!(overlay.read_entry_count_for_test(device_id).unwrap(), 1);

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
    fn block_journal_read_index_coalesces_adjacent_current_ranges() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(8);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block]))
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                block as u64,
                vec![2; block],
            ))
            .unwrap();
        assert_eq!(overlay.read_entry_count_for_test(device_id).unwrap(), 1);

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
    fn block_journal_read_index_preserves_disjoint_ranges_around_overlap() {
        let overlay = BlockJournalOverlay::default();
        let device_id = DeviceId::from_raw(9);
        let block = 4096_usize;
        overlay
            .apply_commit(&journal_commit(device_id, 1, 0, vec![1; block]))
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                2,
                (block * 2) as u64,
                vec![2; block],
            ))
            .unwrap();
        overlay
            .apply_commit(&journal_commit(
                device_id,
                3,
                block as u64,
                vec![3; block * 2],
            ))
            .unwrap();
        assert_eq!(overlay.read_entry_count_for_test(device_id).unwrap(), 1);

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
            _ => Err(durable_codec_error("invalid block journal record kind")),
        }
    }
}

impl DurableSqliteStore {
    fn load_block_journal_overlay(&self, local: &LocalCoordinator) -> Result<BlockJournalOverlay> {
        let records = self.block_journal_records()?;
        let overlay = BlockJournalOverlay::default();
        let mut latest_epoch = BTreeMap::<DeviceId, WriterEpoch>::new();
        let mut durable_through = BTreeMap::<DeviceId, CommitSeq>::new();
        let mut writes = BTreeMap::<DeviceId, BTreeMap<u64, BlockJournalCommit>>::new();

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
                    let device_writes = writes.entry(commit.device_id).or_default();
                    if device_writes
                        .insert(commit.commit_seq.raw(), commit)
                        .is_some()
                    {
                        return Err(StorageError::corrupt(
                            "block journal contains duplicate write commit sequence",
                        ));
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
                overlay.apply_commit(commit)?;
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
                let record_end = record
                    .placement
                    .record_offset
                    .saturating_add(record.placement.record_bytes);
                let manifest = pending.logs.entry(log_ref).or_insert(PendingDataLogManifest {
                    storage_node,
                    log_id: log_ref.log_id,
                    state: self
                        .node_data_log_state(log_ref)?
                        .unwrap_or_else(|| GENERIC_DATA_LOG_STATE_ACTIVE.to_string()),
                    total_bytes: 0,
                    needs_dir_sync: false,
                });
                manifest.total_bytes = manifest.total_bytes.max(record_end);
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
