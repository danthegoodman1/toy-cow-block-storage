const BLOCK_JOURNAL_INLINE_MAX_BYTES: u64 = 256 * 1024;
const BLOCK_JOURNAL_MAGIC: [u8; 8] = *b"BLKJNL01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BlockJournalEntry {
    Write {
        range: ByteRange,
        payload_integrity: PayloadIntegrity,
        bytes: Vec<u8>,
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
    entries: Vec<BlockJournalOverlayEntry>,
    read_entries: Vec<BlockJournalOverlayEntry>,
}

impl Default for BlockJournalDeviceOverlay {
    fn default() -> Self {
        Self {
            writer_epoch: WriterEpoch::from_raw(0),
            durable_through: CommitSeq::from_raw(0),
            visible_through: CommitSeq::from_raw(0),
            entries: Vec::new(),
            read_entries: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct BlockJournalOverlay {
    inner: Mutex<BTreeMap<DeviceId, BlockJournalDeviceOverlay>>,
}

#[derive(Debug)]
pub(super) struct BlockJournalFlushCoordinator {
    inner: Mutex<BlockJournalFlushState>,
    cvar: Condvar,
}

#[derive(Debug, Default)]
pub(super) struct BlockJournalFlushState {
    in_flight: bool,
    generation: u64,
    pending: BTreeMap<u64, BlockJournalCommit>,
    completed: BTreeMap<u64, Result<()>>,
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
            Self::Write { range, .. } | Self::Sparse { range } => *range,
        }
    }

    fn committed_bytes(&self) -> u64 {
        match self {
            Self::Write { range, .. } => range.len,
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
    entries: &mut Vec<BlockJournalOverlayEntry>,
    entry: BlockJournalOverlayEntry,
) -> Result<()> {
    let entry_end = entry.range.end_exclusive()?;
    let mut next = Vec::with_capacity(entries.len().saturating_add(2));
    for current in entries.drain(..) {
        let current_end = current.range.end_exclusive()?;
        if current_end <= entry.range.offset || current.range.offset >= entry_end {
            next.push(current);
            continue;
        }
        if current.range.offset < entry.range.offset {
            next.push(block_journal_overlay_slice(
                &current,
                ByteRange::new(current.range.offset, entry.range.offset - current.range.offset),
            )?);
        }
        if current_end > entry_end {
            next.push(block_journal_overlay_slice(
                &current,
                ByteRange::new(entry_end, current_end - entry_end),
            )?);
        }
    }
    next.push(entry);
    next.sort_by_key(|entry| entry.range.offset);
    coalesce_block_journal_read_entries(&mut next)?;
    *entries = next;
    Ok(())
}

fn coalesce_block_journal_read_entries(entries: &mut Vec<BlockJournalOverlayEntry>) -> Result<()> {
    let mut coalesced = Vec::<BlockJournalOverlayEntry>::with_capacity(entries.len());
    for entry in entries.drain(..) {
        if let Some(last) = coalesced.last_mut()
            && try_merge_block_journal_read_entry(last, &entry)?
        {
            continue;
        }
        coalesced.push(entry);
    }
    *entries = coalesced;
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
        _ => Ok(false),
    }
}

fn apply_block_journal_read_entries(
    entries: &[BlockJournalOverlayEntry],
    requested: ByteRange,
    requested_end: u64,
    verification: ReadVerification,
    buf: &mut [u8],
    entries_are_ordered: bool,
) -> Result<()> {
    for entry in entries {
        let entry_end = entry.range.end_exclusive()?;
        if entry_end <= requested.offset {
            continue;
        }
        if entries_are_ordered && entry.range.offset >= requested_end {
            break;
        }
        let Some(overlap) = byte_range_intersection(entry.range, requested)? else {
            continue;
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
                let source_start = usize::try_from(overlap.offset - entry.range.offset).map_err(
                    |_| StorageError::corrupt("block journal read source overflows usize"),
                )?;
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
        }
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
        let entries = commit.overlay_entries();
        device.entries.extend(entries.iter().cloned());
        for entry in entries {
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
        let first_overlap = device.read_entries.partition_point(|entry| {
            entry.range.offset.saturating_add(entry.range.len) <= requested.offset
        });
        apply_block_journal_read_entries(
            &device.read_entries[first_overlap..],
            requested,
            requested_end,
            verification,
            buf,
            true,
        )?;
        Ok(duration_nanos_u64(started.elapsed()))
    }

    #[cfg(test)]
    fn entry_counts_for_test(&self, device_id: DeviceId) -> Result<(usize, usize)> {
        let inner = lock(&self.inner)?;
        let Some(device) = inner.get(&device_id) else {
            return Ok((0, 0));
        };
        Ok((device.entries.len(), device.read_entries.len()))
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
    fn block_journal_read_index_collapses_shadowed_ranges_without_pruning_history() {
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
        let (history_count, read_count) = overlay.entry_counts_for_test(device_id).unwrap();
        assert_eq!(history_count, 2);
        assert_eq!(read_count, 1);

        let mut read = vec![9; block * 4];
        overlay
            .apply_read_overlay(
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
        let (history_count, read_count) = overlay.entry_counts_for_test(device_id).unwrap();
        assert_eq!(history_count, 2);
        assert_eq!(read_count, 1);

        let mut read = vec![0; block * 2];
        overlay
            .apply_read_overlay(
                device_id,
                ByteRange::new(0, (block * 2) as u64),
                ReadVerification::RequireVerified,
                &mut read,
            )
            .unwrap();
        assert_eq!(&read[..block], vec![1; block]);
        assert_eq!(&read[block..], vec![2; block]);
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
}
