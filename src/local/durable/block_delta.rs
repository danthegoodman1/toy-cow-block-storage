#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockDeltaEntry {
    shard_id: ShardId,
    range: BlockRange,
    segment_id: SegmentId,
    segment_offset: BlockIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BlockDeltaCommit {
    device_id: DeviceId,
    commit_seq: CommitSeq,
    write_count: u64,
    collapsed_range_count: u64,
    committed_bytes: u64,
    entries: Vec<BlockDeltaEntry>,
}

impl BlockDeltaCommit {
    fn row_key(&self) -> String {
        block_delta_row_key(self.commit_seq, self.device_id)
    }

    fn segment_ids(&self) -> BTreeSet<SegmentId> {
        self.entries.iter().map(|entry| entry.segment_id).collect()
    }
}

pub(super) fn block_delta_row_key(commit_seq: CommitSeq, device_id: DeviceId) -> String {
    format!("{:020}:{}", commit_seq.raw(), device_id.raw())
}

impl DurableCodec for BlockDeltaEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_id.encode(out)?;
        self.range.encode(out)?;
        self.segment_id.encode(out)?;
        self.segment_offset.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_id: ShardId::decode(input)?,
            range: BlockRange::decode(input)?,
            segment_id: SegmentId::decode(input)?,
            segment_offset: BlockIndex::decode(input)?,
        })
    }
}

impl DurableCodec for BlockDeltaCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        1u8.encode(out)?;
        self.device_id.encode(out)?;
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
                commit_seq: CommitSeq::decode(input)?,
                write_count: u64::decode(input)?,
                collapsed_range_count: u64::decode(input)?,
                committed_bytes: u64::decode(input)?,
                entries: Vec::<BlockDeltaEntry>::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid block delta commit version")),
        }
    }
}
