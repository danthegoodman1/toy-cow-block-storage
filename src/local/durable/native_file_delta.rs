#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NativeFileDeltaEntry {
    range: BlockRange,
    replacement: NativeFileDeltaReplacement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NativeFileDeltaReplacement {
    Segment {
        segment_id: SegmentId,
        segment_offset: BlockIndex,
    },
}

impl NativeFileDeltaReplacement {
    fn segment_id(&self) -> SegmentId {
        match self {
            Self::Segment { segment_id, .. } => *segment_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NativeFileDeltaCommit {
    keyspace_id: KeyspaceId,
    file_id: FileId,
    commit_seq: CommitSeq,
    base_file_version: FileVersion,
    new_file_version: FileVersion,
    old_size: u64,
    new_size: u64,
    write_count: u64,
    collapsed_range_count: u64,
    committed_bytes: u64,
    entries: Vec<NativeFileDeltaEntry>,
}

impl NativeFileDeltaCommit {
    fn row_key(&self) -> String {
        native_file_delta_row_key(self.commit_seq, self.keyspace_id, self.file_id)
    }

    fn segment_ids(&self) -> BTreeSet<SegmentId> {
        self.entries
            .iter()
            .map(|entry| entry.replacement.segment_id())
            .collect()
    }
}

pub(super) fn native_file_delta_row_key(
    commit_seq: CommitSeq,
    keyspace_id: KeyspaceId,
    file_id: FileId,
) -> String {
    format!(
        "{:020}:{}:{}",
        commit_seq.raw(),
        keyspace_id.raw(),
        file_id.raw()
    )
}

impl DurableCodec for NativeFileDeltaReplacement {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Segment {
                segment_id,
                segment_offset,
            } => {
                1u8.encode(out)?;
                segment_id.encode(out)?;
                segment_offset.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Segment {
                segment_id: SegmentId::decode(input)?,
                segment_offset: BlockIndex::decode(input)?,
            }),
            _ => Err(durable_codec_error(
                "invalid native file delta replacement kind",
            )),
        }
    }
}

impl DurableCodec for NativeFileDeltaEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.range.encode(out)?;
        self.replacement.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            range: BlockRange::decode(input)?,
            replacement: NativeFileDeltaReplacement::decode(input)?,
        })
    }
}

impl DurableCodec for NativeFileDeltaCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        1u8.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.base_file_version.encode(out)?;
        self.new_file_version.encode(out)?;
        self.old_size.encode(out)?;
        self.new_size.encode(out)?;
        self.write_count.encode(out)?;
        self.collapsed_range_count.encode(out)?;
        self.committed_bytes.encode(out)?;
        self.entries.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                commit_seq: CommitSeq::decode(input)?,
                base_file_version: FileVersion::decode(input)?,
                new_file_version: FileVersion::decode(input)?,
                old_size: u64::decode(input)?,
                new_size: u64::decode(input)?,
                write_count: u64::decode(input)?,
                collapsed_range_count: u64::decode(input)?,
                committed_bytes: u64::decode(input)?,
                entries: Vec::<NativeFileDeltaEntry>::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid native file delta commit version")),
        }
    }
}
