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
    entries: Vec<BlockJournalOverlayEntry>,
}

impl Default for BlockJournalDeviceOverlay {
    fn default() -> Self {
        Self {
            writer_epoch: WriterEpoch::from_raw(0),
            durable_through: CommitSeq::from_raw(0),
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct BlockJournalOverlay {
    inner: Mutex<BTreeMap<DeviceId, BlockJournalDeviceOverlay>>,
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

    fn apply_commit(&self, commit: &BlockJournalCommit) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let device = inner.entry(commit.device_id).or_default();
        device.writer_epoch = device.writer_epoch.max(commit.writer_epoch);
        device.entries.extend(commit.overlay_entries());
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
        let entries = lock(&self.inner)?
            .get(&device_id)
            .map(|device| device.entries.clone())
            .unwrap_or_default();
        for entry in entries {
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
            match entry.source {
                BlockJournalOverlaySource::Sparse => output.fill(0),
                BlockJournalOverlaySource::Bytes {
                    payload_integrity,
                    bytes,
                } => {
                    let source_start = usize::try_from(overlap.offset - entry.range.offset)
                        .map_err(|_| {
                            StorageError::corrupt("block journal read source overflows usize")
                        })?;
                    let source_end = source_start.checked_add(output_len).ok_or_else(|| {
                        StorageError::corrupt("block journal read source end overflows")
                    })?;
                    let source = bytes.get(source_start..source_end).ok_or_else(|| {
                        StorageError::corrupt("block journal read source out of bounds")
                    })?;
                    let integrity = segment_payload_integrity(payload_integrity, &bytes);
                    verify_read_integrity_policy(integrity, verification)?;
                    output.copy_from_slice(source);
                }
            }
        }
        Ok(duration_nanos_u64(started.elapsed()))
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
