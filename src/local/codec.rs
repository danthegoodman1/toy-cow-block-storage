pub(super) const MAX_DURABLE_COLLECTION_LEN: u64 = 1_000_000;
pub(super) const MAX_DURABLE_STRING_LEN: u64 = 1_048_576;

pub(super) trait DurableCodec: Sized {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()>;
    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self>;
}

#[derive(Debug, Default)]
pub(super) struct DurableEncoder {
    bytes: Vec<u8>,
}

impl DurableEncoder {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn put_u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }
}

pub(super) struct DurableDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> DurableDecoder<'a> {
    fn finish(&self) -> Result<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(durable_codec_error("trailing bytes in durable buffer"))
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| durable_codec_error("durable buffer offset overflow"))?;
        if end > self.bytes.len() {
            return Err(durable_codec_error("unexpected end of durable buffer"));
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(*self
            .take(1)?
            .first()
            .ok_or_else(|| durable_codec_error("unexpected end of durable buffer"))?)
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u16"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u32"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u64"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn u128(&mut self) -> Result<u128> {
        let bytes: [u8; 16] = self
            .take(16)?
            .try_into()
            .map_err(|_| durable_codec_error("invalid u128"))?;
        Ok(u128::from_be_bytes(bytes))
    }
}

impl DurableCodec for bool {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u8(u8::from(*self));
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match input.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(durable_codec_error("invalid bool tag")),
        }
    }
}

impl DurableCodec for u8 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u8(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u8()
    }
}

impl DurableCodec for u32 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u32(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u32()
    }
}

impl DurableCodec for u64 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u64(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u64()
    }
}

impl DurableCodec for u128 {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.put_u128(*self);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        input.u128()
    }
}

impl DurableCodec for usize {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let value =
            u64::try_from(*self).map_err(|_| durable_codec_error("usize value exceeds u64"))?;
        value.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        usize::try_from(u64::decode(input)?)
            .map_err(|_| durable_codec_error("usize value exceeds platform size"))
    }
}

impl<T: DurableCodec> DurableCodec for Option<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Some(value) => {
                out.put_u8(1);
                value.encode(out)
            }
            None => {
                out.put_u8(0);
                Ok(())
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match input.u8()? {
            0 => Ok(None),
            1 => Ok(Some(T::decode(input)?)),
            _ => Err(durable_codec_error("invalid option tag")),
        }
    }
}

impl<T: DurableCodec> DurableCodec for Vec<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let len = u64::try_from(self.len())
            .map_err(|_| durable_codec_error("vector length exceeds u64"))?;
        len.encode(out)?;
        for value in self {
            value.encode(out)?;
        }
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_COLLECTION_LEN {
            return Err(durable_codec_error("vector length exceeds durable limit"));
        }
        let len =
            usize::try_from(len).map_err(|_| durable_codec_error("vector length overflow"))?;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(T::decode(input)?);
        }
        Ok(values)
    }
}

impl<K, V> DurableCodec for BTreeMap<K, V>
where
    K: DurableCodec + Ord,
    V: DurableCodec,
{
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let len =
            u64::try_from(self.len()).map_err(|_| durable_codec_error("map length exceeds u64"))?;
        len.encode(out)?;
        for (key, value) in self {
            key.encode(out)?;
            value.encode(out)?;
        }
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_COLLECTION_LEN {
            return Err(durable_codec_error("map length exceeds durable limit"));
        }
        let mut values = BTreeMap::new();
        for _ in 0..len {
            let key = K::decode(input)?;
            let value = V::decode(input)?;
            if values.insert(key, value).is_some() {
                return Err(durable_codec_error("duplicate key in durable map"));
            }
        }
        Ok(values)
    }
}

impl DurableCodec for String {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let bytes = self.as_bytes();
        let len = u64::try_from(bytes.len())
            .map_err(|_| durable_codec_error("string length exceeds u64"))?;
        len.encode(out)?;
        out.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let len = u64::decode(input)?;
        if len > MAX_DURABLE_STRING_LEN {
            return Err(durable_codec_error("string length exceeds durable limit"));
        }
        let len =
            usize::try_from(len).map_err(|_| durable_codec_error("string length overflow"))?;
        let bytes = input.take(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| durable_codec_error("invalid UTF-8 string"))
    }
}

impl DurableCodec for (KeyspaceId, FileId) {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.0.encode(out)?;
        self.1.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok((KeyspaceId::decode(input)?, FileId::decode(input)?))
    }
}

macro_rules! durable_id_codec_u128 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u128::decode(input)?))
                }
            }
        )+
    };
}

macro_rules! durable_id_codec_u64 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u64::decode(input)?))
                }
            }
        )+
    };
}

macro_rules! durable_id_codec_u32 {
    ($($name:ty),+ $(,)?) => {
        $(
            impl DurableCodec for $name {
                fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
                    self.raw().encode(out)
                }

                fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
                    Ok(Self::from_raw(u32::decode(input)?))
                }
            }
        )+
    };
}

durable_id_codec_u128!(
    AppendPublishTicketId,
    AppendRunId,
    AppendStreamId,
    AppendTicketId,
    CheckpointId,
    CommitGroupId,
    DeviceId,
    ExtentId,
    FileId,
    GrantId,
    GrantNonce,
    KeyspaceCatalogShardId,
    KeyspaceId,
    KeyspaceRootId,
    MetadataNodeId,
    PrincipalId,
    RequestId,
    SegmentId,
    StorageNodeId,
    StorageNodeKeyId,
    TenantId,
    WriteIntentId,
);

durable_id_codec_u64!(
    BlockCount,
    BlockIndex,
    ClientEpoch,
    CommitSeq,
    DeviceGeneration,
    FileVersion,
    GrantEpoch,
    KeyspaceGeneration,
    LogicalDeadline,
    LogicalTime,
    ServerIncarnation,
    WriterEpoch,
);

durable_id_codec_u32!(crate::id::ShardId);

impl DurableCodec for LocalStoreConfig {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_count.encode(out)?;
        self.block_size.encode(out)?;
        self.file_root_blocks.encode(out)?;
        self.metadata_fanout.encode(out)?;
        self.metadata_leaf_blocks.encode(out)?;
        self.storage_node.encode(out)?;
        self.observability_event_capacity.encode(out)?;
        self.stream_auto_persist_bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_count: usize::decode(input)?,
            block_size: u32::decode(input)?,
            file_root_blocks: u64::decode(input)?,
            metadata_fanout: usize::decode(input)?,
            metadata_leaf_blocks: u64::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            observability_event_capacity: usize::decode(input)?,
            stream_auto_persist_bytes: Option::<u64>::decode(input)?,
        })
    }
}

impl DurableCodec for crate::api::DeviceSpec {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.logical_blocks.encode(out)?;
        self.block_size.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            logical_blocks: u64::decode(input)?,
            block_size: u32::decode(input)?,
        })
    }
}

impl DurableCodec for ByteRange {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.offset.encode(out)?;
        self.len.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            offset: u64::decode(input)?,
            len: u64::decode(input)?,
        })
    }
}

impl DurableCodec for crate::api::BlockRange {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.start.encode(out)?;
        self.blocks.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            start: BlockIndex::decode(input)?,
            blocks: BlockCount::decode(input)?,
        })
    }
}

impl DurableCodec for MappingOwner {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockDevice(device_id) => {
                1u8.encode(out)?;
                device_id.encode(out)
            }
            Self::NativeKeyspace(keyspace_id) => {
                2u8.encode(out)?;
                keyspace_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockDevice(DeviceId::decode(input)?)),
            2 => Ok(Self::NativeKeyspace(KeyspaceId::decode(input)?)),
            _ => Err(durable_codec_error("invalid mapping owner tag")),
        }
    }
}

impl DurableCodec for DeviceHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.generation.encode(out)?;
        self.shard_roots.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            generation: DeviceGeneration::decode(input)?,
            shard_roots: Vec::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for DurableDeviceManifest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.shard_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            shard_count: u64::decode(input)?,
        })
    }
}

impl DurableCodec for DurableDeviceShardHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.shard_id.encode(out)?;
        self.root.encode(out)?;
        self.generation.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            shard_id: ShardId::decode(input)?,
            root: MetadataNodeId::decode(input)?,
            generation: DeviceGeneration::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for DurableKeyspaceManifest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.shard_count.encode(out)?;
        self.file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            shard_count: u64::decode(input)?,
            file_count: u64::decode(input)?,
        })
    }
}

impl DurableCodec for DurableKeyspaceShardHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.shard_index.encode(out)?;
        self.root.encode(out)?;
        self.generation.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            shard_index: u32::decode(input)?,
            root: KeyspaceCatalogShardId::decode(input)?,
            generation: KeyspaceGeneration::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.generation.encode(out)?;
        self.shard_roots.encode(out)?;
        self.file_count.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            generation: KeyspaceGeneration::decode(input)?,
            shard_roots: Vec::<KeyspaceCatalogShardId>::decode(input)?,
            file_count: usize::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for FileHead {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.file_id.encode(out)?;
        self.version.encode(out)?;
        self.root.encode(out)?;
        self.size.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            file_id: FileId::decode(input)?,
            version: FileVersion::decode(input)?,
            root: MetadataNodeId::decode(input)?,
            size: u64::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceFile {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)?;
        self.head.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
            head: FileHead::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceCatalogShard {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_id.encode(out)?;
        self.files.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_id: KeyspaceCatalogShardId::decode(input)?,
            files: BTreeMap::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceRoot {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.root_id.encode(out)?;
        self.shard_roots.as_ref().to_vec().encode(out)?;
        self.file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            root_id: KeyspaceRootId::decode(input)?,
            shard_roots: Vec::decode(input)?.into(),
            file_count: usize::decode(input)?,
        })
    }
}

impl DurableCodec for MetadataChild {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.range.encode(out)?;
        self.node_id.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            range: crate::api::BlockRange::decode(input)?,
            node_id: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for LeafEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.logical_start.encode(out)?;
        self.blocks.encode(out)?;
        self.segment_id.encode(out)?;
        self.segment_offset.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            logical_start: BlockIndex::decode(input)?,
            blocks: BlockCount::decode(input)?,
            segment_id: SegmentId::decode(input)?,
            segment_offset: BlockIndex::decode(input)?,
        })
    }
}

impl DurableCodec for AppendLogRun {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.run_id.encode(out)?;
        self.storage_node.encode(out)?;
        self.stream_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.file_offset_start.encode(out)?;
        self.payload_len.encode(out)?;
        self.log_id.encode(out)?;
        self.log_payload_offset.encode(out)?;
        self.log_record_bytes.encode(out)?;
        self.integrity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            run_id: AppendRunId::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            file_offset_start: u64::decode(input)?,
            payload_len: u64::decode(input)?,
            log_id: u64::decode(input)?,
            log_payload_offset: u64::decode(input)?,
            log_record_bytes: u64::decode(input)?,
            integrity: SegmentPayloadIntegrity::decode(input)?,
        })
    }
}

impl DurableCodec for AppendLogRunRange {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.run_id.encode(out)?;
        self.storage_node.encode(out)?;
        self.stream_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.file_offset_start.encode(out)?;
        self.payload_len.encode(out)?;
        self.log_id.encode(out)?;
        self.log_payload_offset.encode(out)?;
        self.integrity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            run_id: AppendRunId::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            file_offset_start: u64::decode(input)?,
            payload_len: u64::decode(input)?,
            log_id: u64::decode(input)?,
            log_payload_offset: u64::decode(input)?,
            integrity: SegmentPayloadIntegrity::decode(input)?,
        })
    }
}

impl DurableCodec for RunBackedFileExtent {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.file_offset_start.encode(out)?;
        self.payload_len.encode(out)?;
        self.run.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            file_offset_start: u64::decode(input)?,
            payload_len: u64::decode(input)?,
            run: AppendLogRunRange::decode(input)?,
        })
    }
}

impl DurableCodec for MetadataNodeKind {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Internal { children } => {
                1u8.encode(out)?;
                children.encode(out)
            }
            Self::Leaf {
                entries,
                run_extents,
            } => {
                2u8.encode(out)?;
                entries.encode(out)?;
                run_extents.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Internal {
                children: Vec::decode(input)?,
            }),
            2 => Ok(Self::Leaf {
                entries: Vec::decode(input)?,
                run_extents: Vec::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid metadata node kind tag")),
        }
    }
}

impl DurableCodec for MetadataNode {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.node_id.encode(out)?;
        self.covered_range.encode(out)?;
        self.kind.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            node_id: MetadataNodeId::decode(input)?,
            covered_range: crate::api::BlockRange::decode(input)?,
            kind: MetadataNodeKind::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentPayloadIntegrity {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Crc32c(checksum) => {
                1u8.encode(out)?;
                checksum.encode(out)
            }
            Self::Unchecked => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Crc32c(u64::decode(input)?)),
            2 => Ok(Self::Unchecked),
            _ => Err(durable_codec_error("invalid segment payload integrity tag")),
        }
    }
}

impl DurableCodec for SegmentDescriptor {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.blocks.encode(out)?;
        self.bytes.encode(out)?;
        self.integrity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            blocks: BlockCount::decode(input)?,
            bytes: u64::decode(input)?,
            integrity: SegmentPayloadIntegrity::decode(input)?,
        })
    }
}

impl DurableCodec for ShardRootUpdate {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.shard_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            shard_id: crate::id::ShardId::decode(input)?,
            old_root: MetadataNodeId::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for RootUpdate {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockShard(update) => {
                1u8.encode(out)?;
                update.encode(out)
            }
            Self::FileCreated {
                file_id,
                new_root,
                new_size,
            } => {
                2u8.encode(out)?;
                file_id.encode(out)?;
                new_root.encode(out)?;
                new_size.encode(out)
            }
            Self::FileRoot {
                file_id,
                old_root,
                new_root,
                new_size,
            } => {
                3u8.encode(out)?;
                file_id.encode(out)?;
                old_root.encode(out)?;
                new_root.encode(out)?;
                new_size.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockShard(ShardRootUpdate::decode(input)?)),
            2 => Ok(Self::FileCreated {
                file_id: FileId::decode(input)?,
                new_root: MetadataNodeId::decode(input)?,
                new_size: u64::decode(input)?,
            }),
            3 => Ok(Self::FileRoot {
                file_id: FileId::decode(input)?,
                old_root: MetadataNodeId::decode(input)?,
                new_root: MetadataNodeId::decode(input)?,
                new_size: u64::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid root update tag")),
        }
    }
}

impl DurableCodec for CommitGroup {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_group.encode(out)?;
        self.commit_seq.encode(out)?;
        self.owner.encode(out)?;
        self.updates.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_group: CommitGroupId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            owner: MappingOwner::decode(input)?,
            updates: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for ForkRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.source.encode(out)?;
        self.target.encode(out)?;
        self.shard_roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            source: DeviceId::decode(input)?,
            target: DeviceId::decode(input)?,
            shard_roots: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for ShardCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.device_id.encode(out)?;
        self.shard_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            device_id: DeviceId::decode(input)?,
            shard_id: crate::id::ShardId::decode(input)?,
            old_root: MetadataNodeId::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.shard_index.encode(out)?;
        self.old_shard.encode(out)?;
        self.new_shard.encode(out)?;
        self.old_file_count.encode(out)?;
        self.new_file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            shard_index: u32::decode(input)?,
            old_shard: KeyspaceCatalogShardId::decode(input)?,
            new_shard: KeyspaceCatalogShardId::decode(input)?,
            old_file_count: usize::decode(input)?,
            new_file_count: usize::decode(input)?,
        })
    }
}

impl DurableCodec for FileCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.commit_group.encode(out)?;
        self.time.encode(out)?;
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.old_root.encode(out)?;
        self.new_root.encode(out)?;
        self.old_version.encode(out)?;
        self.new_version.encode(out)?;
        self.old_size.encode(out)?;
        self.new_size.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            commit_group: CommitGroupId::decode(input)?,
            time: LogicalTime::decode(input)?,
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            old_root: Option::decode(input)?,
            new_root: MetadataNodeId::decode(input)?,
            old_version: Option::decode(input)?,
            new_version: FileVersion::decode(input)?,
            old_size: u64::decode(input)?,
            new_size: u64::decode(input)?,
        })
    }
}

impl DurableCodec for DeleteRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.commit_seq.encode(out)?;
        self.time.encode(out)?;
        self.device_id.encode(out)?;
        self.shard_roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            commit_seq: CommitSeq::decode(input)?,
            time: LogicalTime::decode(input)?,
            device_id: DeviceId::decode(input)?,
            shard_roots: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for CheckpointRoots {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockShard(roots) => {
                1u8.encode(out)?;
                roots.encode(out)
            }
            Self::NativeKeyspace(root) => {
                2u8.encode(out)?;
                root.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockShard(Vec::decode(input)?)),
            2 => Ok(Self::NativeKeyspace(KeyspaceRootId::decode(input)?)),
            _ => Err(durable_codec_error("invalid checkpoint roots tag")),
        }
    }
}

impl DurableCodec for Checkpoint {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.checkpoint_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.time.encode(out)?;
        self.owner.encode(out)?;
        self.roots.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            checkpoint_id: CheckpointId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            time: LogicalTime::decode(input)?,
            owner: MappingOwner::decode(input)?,
            roots: CheckpointRoots::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReservationIntent {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.write_intent.encode(out)?;
        self.owner.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            write_intent: WriteIntentId::decode(input)?,
            owner: MappingOwner::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReservation {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReplicaPlacement {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.segment_id.encode(out)?;
        self.storage_node.encode(out)?;
        self.offset.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            segment_id: SegmentId::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            offset: u64::decode(input)?,
            bytes: u64::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentReplicaCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.descriptor.encode(out)?;
        self.placement.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            descriptor: SegmentDescriptor::decode(input)?,
            placement: SegmentReplicaPlacement::decode(input)?,
        })
    }
}

impl DurableCodec for ProofScheme {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let tag = match self {
            Self::DeterministicTestMacV1 => 1u8,
            Self::NodeSignatureV1 => 2,
        };
        tag.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::DeterministicTestMacV1),
            2 => Ok(Self::NodeSignatureV1),
            _ => Err(durable_codec_error("invalid proof scheme tag")),
        }
    }
}

impl DurableCodec for crate::provider::ProofTag {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.bytes.extend_from_slice(&self.0);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(input.take(32)?);
        Ok(Self(bytes))
    }
}

impl DurableCodec for crate::provider::GrantHash {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        out.bytes.extend_from_slice(&self.0);
        Ok(())
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let mut bytes = [0; 32];
        bytes.copy_from_slice(input.take(32)?);
        Ok(Self(bytes))
    }
}

impl DurableCodec for WriteGrantIntent {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::BlockWrite {
                device_id,
                range,
                fence,
                shard_id,
                old_root,
            } => {
                1u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)?;
                fence.encode(out)?;
                shard_id.encode(out)?;
                old_root.encode(out)
            }
            Self::NativeWrite {
                keyspace_id,
                file_id,
                range,
                base_version,
            } => {
                2u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                range.encode(out)?;
                base_version.encode(out)
            }
            Self::NativeAppendStream {
                keyspace_id,
                file_id,
                stream_id,
                ticket_id,
                append_offset,
                bytes,
                writer_epoch,
            } => {
                3u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream_id.encode(out)?;
                ticket_id.encode(out)?;
                append_offset.encode(out)?;
                bytes.encode(out)?;
                writer_epoch.encode(out)
            }
            Self::Internal { owner } => {
                4u8.encode(out)?;
                owner.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::BlockWrite {
                device_id: DeviceId::decode(input)?,
                range: crate::api::BlockRange::decode(input)?,
                fence: DeviceGeneration::decode(input)?,
                shard_id: ShardId::decode(input)?,
                old_root: MetadataNodeId::decode(input)?,
            }),
            2 => Ok(Self::NativeWrite {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                range: ByteRange::decode(input)?,
                base_version: FileVersion::decode(input)?,
            }),
            3 => Ok(Self::NativeAppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream_id: AppendStreamId::decode(input)?,
                ticket_id: AppendTicketId::decode(input)?,
                append_offset: u64::decode(input)?,
                bytes: u64::decode(input)?,
                writer_epoch: WriterEpoch::decode(input)?,
            }),
            4 => Ok(Self::Internal {
                owner: MappingOwner::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid write grant intent tag")),
        }
    }
}

impl DurableCodec for SegmentReceiptLifecycle {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::DurablePendingMetadata => 1u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::DurablePendingMetadata),
            _ => Err(durable_codec_error("invalid segment receipt lifecycle tag")),
        }
    }
}

impl DurableCodec for SegmentWriteReceipt {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.tenant.encode(out)?;
        self.grant_id.encode(out)?;
        self.grant_hash.encode(out)?;
        self.principal.encode(out)?;
        self.owner.encode(out)?;
        self.storage_node.encode(out)?;
        self.storage_node_incarnation.encode(out)?;
        self.segment_id.encode(out)?;
        self.write_intent.encode(out)?;
        self.intent.encode(out)?;
        self.bytes.encode(out)?;
        self.integrity.encode(out)?;
        self.durability.encode(out)?;
        self.lifecycle.encode(out)?;
        self.receipt_epoch.encode(out)?;
        self.expires_at.encode(out)?;
        self.node_key_id.encode(out)?;
        self.proof_scheme.encode(out)?;
        self.proof.encode(out)?;
        self.descriptor.encode(out)?;
        self.placement.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            tenant: TenantId::decode(input)?,
            grant_id: GrantId::decode(input)?,
            grant_hash: crate::provider::GrantHash::decode(input)?,
            principal: PrincipalId::decode(input)?,
            owner: MappingOwner::decode(input)?,
            storage_node: StorageNodeId::decode(input)?,
            storage_node_incarnation: ServerIncarnation::decode(input)?,
            segment_id: SegmentId::decode(input)?,
            write_intent: WriteIntentId::decode(input)?,
            intent: WriteGrantIntent::decode(input)?,
            bytes: u64::decode(input)?,
            integrity: SegmentPayloadIntegrity::decode(input)?,
            durability: WriteDurability::decode(input)?,
            lifecycle: SegmentReceiptLifecycle::decode(input)?,
            receipt_epoch: GrantEpoch::decode(input)?,
            expires_at: LogicalDeadline::decode(input)?,
            node_key_id: StorageNodeKeyId::decode(input)?,
            proof_scheme: ProofScheme::decode(input)?,
            proof: crate::provider::ProofTag::decode(input)?,
            descriptor: SegmentDescriptor::decode(input)?,
            placement: SegmentReplicaPlacement::decode(input)?,
        })
    }
}

impl DurableCodec for SegmentLifecycleState {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        let tag: u8 = match self {
            Self::Reserved => 1,
            Self::Writing => 2,
            Self::DurablePendingMetadata => 3,
            Self::Referenced => 4,
            Self::Released => 5,
            Self::Freed => 6,
        };
        tag.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Reserved),
            2 => Ok(Self::Writing),
            3 => Ok(Self::DurablePendingMetadata),
            4 => Ok(Self::Referenced),
            5 => Ok(Self::Released),
            6 => Ok(Self::Freed),
            _ => Err(durable_codec_error("invalid segment lifecycle tag")),
        }
    }
}

impl DurableCodec for CatalogEntry {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.intent.encode(out)?;
        self.reservation.encode(out)?;
        self.state.encode(out)?;
        self.receipt.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            intent: SegmentReservationIntent::decode(input)?,
            reservation: SegmentReservation::decode(input)?,
            state: SegmentLifecycleState::decode(input)?,
            receipt: Option::decode(input)?,
        })
    }
}

impl DurableCodec for WriteDurability {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Acknowledged => 1u8.encode(out),
            Self::Flushed => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Acknowledged),
            2 => Ok(Self::Flushed),
            _ => Err(durable_codec_error("invalid write durability tag")),
        }
    }
}

impl DurableCodec for PayloadIntegrity {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Verified => 1u8.encode(out),
            Self::Unchecked => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Verified),
            2 => Ok(Self::Unchecked),
            _ => Err(durable_codec_error("invalid payload integrity tag")),
        }
    }
}

impl DurableCodec for ReadVerification {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Default => 1u8.encode(out),
            Self::RequireVerified => 2u8.encode(out),
            Self::Skip => 3u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Default),
            2 => Ok(Self::RequireVerified),
            3 => Ok(Self::Skip),
            _ => Err(durable_codec_error("invalid read verification tag")),
        }
    }
}

impl DurableCodec for FlushScope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Device => 1u8.encode(out),
            Self::All => 2u8.encode(out),
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Device),
            2 => Ok(Self::All),
            _ => Err(durable_codec_error("invalid flush scope tag")),
        }
    }
}

impl DurableCodec for RestorePoint {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Commit(commit) => {
                1u8.encode(out)?;
                commit.encode(out)
            }
            Self::Checkpoint(checkpoint) => {
                2u8.encode(out)?;
                checkpoint.encode(out)
            }
            Self::Time(time) => {
                3u8.encode(out)?;
                time.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Commit(CommitSeq::decode(input)?)),
            2 => Ok(Self::Checkpoint(CheckpointId::decode(input)?)),
            3 => Ok(Self::Time(LogicalTime::decode(input)?)),
            _ => Err(durable_codec_error("invalid restore point tag")),
        }
    }
}

impl DurableCodec for DeviceInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.generation.encode(out)?;
        self.spec.encode(out)?;
        self.latest_commit.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            generation: DeviceGeneration::decode(input)?,
            spec: crate::api::DeviceSpec::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for CreateDeviceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.spec.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            spec: crate::api::DeviceSpec::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for WriteCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.range.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            range: ByteRange::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for BlockBatchWrite {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.offset.encode(out)?;
        self.bytes.encode(out)?;
        self.payload_integrity.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            offset: u64::decode(input)?,
            bytes: Vec::decode(input)?,
            payload_integrity: PayloadIntegrity::decode(input)?,
        })
    }
}

impl DurableCodec for BlockBatchCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.commit_seq.encode(out)?;
        self.write_count.encode(out)?;
        self.collapsed_range_count.encode(out)?;
        self.committed_bytes.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            write_count: u64::decode(input)?,
            collapsed_range_count: u64::decode(input)?,
            committed_bytes: u64::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for FlushResult {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.durable_through.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            durable_through: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for DeleteResult {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.device_id.encode(out)?;
        self.commit_seq.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            device_id: DeviceId::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
        })
    }
}

impl DurableCodec for ForkRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.target.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            target: Option::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for ReadResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            bytes: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for BlockRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Create { request } => {
                1u8.encode(out)?;
                request.encode(out)
            }
            Self::Info { device_id } => {
                2u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Read {
                device_id,
                range,
                verification,
            } => {
                3u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)?;
                verification.encode(out)
            }
            Self::Write {
                device_id,
                offset,
                bytes,
                payload_integrity,
                durability,
            } => {
                4u8.encode(out)?;
                device_id.encode(out)?;
                offset.encode(out)?;
                bytes.encode(out)?;
                payload_integrity.encode(out)?;
                durability.encode(out)
            }
            Self::CommitBatch {
                device_id,
                writes,
                durability,
            } => {
                11u8.encode(out)?;
                device_id.encode(out)?;
                writes.encode(out)?;
                durability.encode(out)
            }
            Self::Flush { device_id, scope } => {
                5u8.encode(out)?;
                device_id.encode(out)?;
                scope.encode(out)
            }
            Self::WriteZeroes { device_id, range } => {
                6u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)
            }
            Self::Discard { device_id, range } => {
                7u8.encode(out)?;
                device_id.encode(out)?;
                range.encode(out)
            }
            Self::Fork { source, request } => {
                8u8.encode(out)?;
                source.encode(out)?;
                request.encode(out)
            }
            Self::Restore { source, point } => {
                9u8.encode(out)?;
                source.encode(out)?;
                point.encode(out)
            }
            Self::Delete { device_id } => {
                10u8.encode(out)?;
                device_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Create {
                request: CreateDeviceRequest::decode(input)?,
            }),
            2 => Ok(Self::Info {
                device_id: DeviceId::decode(input)?,
            }),
            3 => Ok(Self::Read {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
                verification: ReadVerification::decode(input)?,
            }),
            4 => Ok(Self::Write {
                device_id: DeviceId::decode(input)?,
                offset: u64::decode(input)?,
                bytes: Vec::decode(input)?,
                payload_integrity: PayloadIntegrity::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            11 => Ok(Self::CommitBatch {
                device_id: DeviceId::decode(input)?,
                writes: Vec::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            5 => Ok(Self::Flush {
                device_id: DeviceId::decode(input)?,
                scope: FlushScope::decode(input)?,
            }),
            6 => Ok(Self::WriteZeroes {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            7 => Ok(Self::Discard {
                device_id: DeviceId::decode(input)?,
                range: ByteRange::decode(input)?,
            }),
            8 => Ok(Self::Fork {
                source: DeviceId::decode(input)?,
                request: ForkRequest::decode(input)?,
            }),
            9 => Ok(Self::Restore {
                source: DeviceId::decode(input)?,
                point: RestorePoint::decode(input)?,
            }),
            10 => Ok(Self::Delete {
                device_id: DeviceId::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid block request tag")),
        }
    }
}

impl DurableCodec for BlockResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Created(device_id) => {
                1u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Info(info) => {
                2u8.encode(out)?;
                info.encode(out)
            }
            Self::Read(read) => {
                3u8.encode(out)?;
                read.encode(out)
            }
            Self::Write(commit) => {
                4u8.encode(out)?;
                commit.encode(out)
            }
            Self::BatchCommitted(commit) => {
                9u8.encode(out)?;
                commit.encode(out)
            }
            Self::Flush(flush) => {
                5u8.encode(out)?;
                flush.encode(out)
            }
            Self::Forked(device_id) => {
                6u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Restored(device_id) => {
                7u8.encode(out)?;
                device_id.encode(out)
            }
            Self::Deleted(delete) => {
                8u8.encode(out)?;
                delete.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Created(DeviceId::decode(input)?)),
            2 => Ok(Self::Info(DeviceInfo::decode(input)?)),
            3 => Ok(Self::Read(ReadResponse::decode(input)?)),
            4 => Ok(Self::Write(WriteCommit::decode(input)?)),
            9 => Ok(Self::BatchCommitted(BlockBatchCommit::decode(input)?)),
            5 => Ok(Self::Flush(FlushResult::decode(input)?)),
            6 => Ok(Self::Forked(DeviceId::decode(input)?)),
            7 => Ok(Self::Restored(DeviceId::decode(input)?)),
            8 => Ok(Self::Deleted(DeleteResult::decode(input)?)),
            _ => Err(durable_codec_error("invalid block response tag")),
        }
    }
}

impl DurableCodec for BlockRequestEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.client_epoch.encode(out)?;
        self.deadline.encode(out)?;
        self.request.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            client_epoch: ClientEpoch::decode(input)?,
            deadline: Option::decode(input)?,
            request: BlockRequest::decode(input)?,
        })
    }
}

impl DurableCodec for BlockResponseEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.response.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            response: BlockResponse::decode(input)?,
        })
    }
}

impl DurableCodec for CreateKeyspaceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for KeyspaceInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.generation.encode(out)?;
        self.latest_commit.encode(out)?;
        self.file_count.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            generation: KeyspaceGeneration::decode(input)?,
            latest_commit: CommitSeq::decode(input)?,
            file_count: usize::decode(input)?,
        })
    }
}

impl DurableCodec for SnapshotKeyspaceRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.target.encode(out)?;
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            target: Option::decode(input)?,
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for FileSpec {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.name.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            name: Option::decode(input)?,
        })
    }
}

impl DurableCodec for CreateFileRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.spec.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            spec: FileSpec::decode(input)?,
        })
    }
}

impl DurableCodec for FileInfo {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.size.encode(out)?;
        self.version.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            size: u64::decode(input)?,
            version: FileVersion::decode(input)?,
        })
    }
}

impl DurableCodec for AppendStream {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.stream_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.base_version.encode(out)?;
        self.visible_base_size.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            base_version: FileVersion::decode(input)?,
            visible_base_size: u64::decode(input)?,
        })
    }
}

impl DurableCodec for AppendTicket {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.stream_id.encode(out)?;
        self.ticket_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.range.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            ticket_id: AppendTicketId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            range: ByteRange::decode(input)?,
        })
    }
}

impl DurableCodec for AppendPublishTicket {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.stream_id.encode(out)?;
        self.ticket_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.publish_through.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            ticket_id: AppendPublishTicketId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            publish_through: u64::decode(input)?,
        })
    }
}

impl DurableCodec for AppendPublishCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.range.encode(out)?;
        self.version.encode(out)?;
        self.commit_seq.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            range: ByteRange::decode(input)?,
            version: FileVersion::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for FileWriteCommit {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.range.encode(out)?;
        self.version.encode(out)?;
        self.commit_seq.encode(out)?;
        self.durability.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            range: ByteRange::decode(input)?,
            version: FileVersion::decode(input)?,
            commit_seq: CommitSeq::decode(input)?,
            durability: WriteDurability::decode(input)?,
        })
    }
}

impl DurableCodec for FileBatchWrite {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.offset.encode(out)?;
        self.bytes.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            offset: u64::decode(input)?,
            bytes: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for AppendStreamStatus {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Active => 1u8,
            Self::Released => 2,
            Self::Fenced => 3,
            Self::Aborted => 4,
        }
        .encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Active),
            2 => Ok(Self::Released),
            3 => Ok(Self::Fenced),
            4 => Ok(Self::Aborted),
            _ => Err(durable_codec_error("invalid append stream status tag")),
        }
    }
}

impl DurableCodec for AppendStreamRunRecord {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.ticket_id.encode(out)?;
        self.offset.encode(out)?;
        self.len.encode(out)?;
        self.run.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        let ticket_id = AppendTicketId::decode(input)?;
        let offset = u64::decode(input)?;
        let len = u64::decode(input)?;
        let run = AppendLogRun::decode(input)?;
        Ok(Self {
            ticket_id,
            offset,
            len,
            run,
        })
    }
}

impl DurableCodec for AppendStreamState {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.keyspace_id.encode(out)?;
        self.file_id.encode(out)?;
        self.stream_id.encode(out)?;
        self.writer_epoch.encode(out)?;
        self.base_version.encode(out)?;
        self.visible_base_size.encode(out)?;
        self.reserved_tail.encode(out)?;
        self.durable_through.encode(out)?;
        self.published_through.encode(out)?;
        self.status.encode(out)?;
        self.records.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            keyspace_id: KeyspaceId::decode(input)?,
            file_id: FileId::decode(input)?,
            stream_id: AppendStreamId::decode(input)?,
            writer_epoch: WriterEpoch::decode(input)?,
            base_version: FileVersion::decode(input)?,
            visible_base_size: u64::decode(input)?,
            reserved_tail: u64::decode(input)?,
            durable_through: u64::decode(input)?,
            published_through: u64::decode(input)?,
            status: AppendStreamStatus::decode(input)?,
            records: Vec::decode(input)?,
        })
    }
}

impl DurableCodec for NativeRequest {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::CreateKeyspace { request } => {
                1u8.encode(out)?;
                request.encode(out)
            }
            Self::KeyspaceInfo { keyspace_id } => {
                2u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::CreateFile {
                keyspace_id,
                request,
            } => {
                3u8.encode(out)?;
                keyspace_id.encode(out)?;
                request.encode(out)
            }
            Self::FileInfo {
                keyspace_id,
                file_id,
            } => {
                4u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::Read {
                keyspace_id,
                file_id,
                range,
                verification,
            } => {
                5u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                range.encode(out)?;
                verification.encode(out)
            }
            Self::CommitFileBatch {
                keyspace_id,
                file_id,
                writes,
                payload_integrity,
                durability,
            } => {
                6u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                writes.encode(out)?;
                payload_integrity.encode(out)?;
                durability.encode(out)
            }
            Self::OpenAppendStream {
                keyspace_id,
                file_id,
            } => {
                7u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::AppendStream {
                keyspace_id,
                file_id,
                stream,
                bytes,
                payload_integrity,
            } => {
                8u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream.encode(out)?;
                bytes.encode(out)?;
                payload_integrity.encode(out)
            }
            Self::SubmitAppendPublish {
                keyspace_id,
                file_id,
                stream,
                publish_through,
            } => {
                9u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream.encode(out)?;
                publish_through.encode(out)
            }
            Self::WaitAppendPublish {
                keyspace_id,
                file_id,
                ticket,
            } => {
                10u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                ticket.encode(out)
            }
            Self::PublishAppendStream {
                keyspace_id,
                file_id,
                stream,
                publish_through,
            } => {
                11u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream.encode(out)?;
                publish_through.encode(out)
            }
            Self::ReleaseAppendStream {
                keyspace_id,
                file_id,
                stream,
            } => {
                12u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream.encode(out)
            }
            Self::AbortAppendStream {
                keyspace_id,
                file_id,
                stream,
            } => {
                13u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)?;
                stream.encode(out)
            }
            Self::Flush {
                keyspace_id,
                file_id,
            } => {
                14u8.encode(out)?;
                keyspace_id.encode(out)?;
                file_id.encode(out)
            }
            Self::CheckpointKeyspace { keyspace_id } => {
                15u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::SnapshotKeyspace { source, request } => {
                16u8.encode(out)?;
                source.encode(out)?;
                request.encode(out)
            }
            Self::RestoreKeyspace { source, point } => {
                17u8.encode(out)?;
                source.encode(out)?;
                point.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::CreateKeyspace {
                request: CreateKeyspaceRequest::decode(input)?,
            }),
            2 => Ok(Self::KeyspaceInfo {
                keyspace_id: KeyspaceId::decode(input)?,
            }),
            3 => Ok(Self::CreateFile {
                keyspace_id: KeyspaceId::decode(input)?,
                request: CreateFileRequest::decode(input)?,
            }),
            4 => Ok(Self::FileInfo {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            5 => Ok(Self::Read {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                range: ByteRange::decode(input)?,
                verification: ReadVerification::decode(input)?,
            }),
            6 => Ok(Self::CommitFileBatch {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                writes: Vec::decode(input)?,
                payload_integrity: PayloadIntegrity::decode(input)?,
                durability: WriteDurability::decode(input)?,
            }),
            7 => Ok(Self::OpenAppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            8 => Ok(Self::AppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream: AppendStream::decode(input)?,
                bytes: Vec::decode(input)?,
                payload_integrity: PayloadIntegrity::decode(input)?,
            }),
            9 => Ok(Self::SubmitAppendPublish {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream: AppendStream::decode(input)?,
                publish_through: u64::decode(input)?,
            }),
            10 => Ok(Self::WaitAppendPublish {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                ticket: AppendPublishTicket::decode(input)?,
            }),
            11 => Ok(Self::PublishAppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream: AppendStream::decode(input)?,
                publish_through: u64::decode(input)?,
            }),
            12 => Ok(Self::ReleaseAppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream: AppendStream::decode(input)?,
            }),
            13 => Ok(Self::AbortAppendStream {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
                stream: AppendStream::decode(input)?,
            }),
            14 => Ok(Self::Flush {
                keyspace_id: KeyspaceId::decode(input)?,
                file_id: FileId::decode(input)?,
            }),
            15 => Ok(Self::CheckpointKeyspace {
                keyspace_id: KeyspaceId::decode(input)?,
            }),
            16 => Ok(Self::SnapshotKeyspace {
                source: KeyspaceId::decode(input)?,
                request: SnapshotKeyspaceRequest::decode(input)?,
            }),
            17 => Ok(Self::RestoreKeyspace {
                source: KeyspaceId::decode(input)?,
                point: RestorePoint::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid native request tag")),
        }
    }
}

impl DurableCodec for NativeResponse {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::KeyspaceCreated(keyspace_id) => {
                1u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::KeyspaceInfo(info) => {
                2u8.encode(out)?;
                info.encode(out)
            }
            Self::FileCreated(file_id) => {
                3u8.encode(out)?;
                file_id.encode(out)
            }
            Self::FileInfo(info) => {
                4u8.encode(out)?;
                info.encode(out)
            }
            Self::Read(read) => {
                5u8.encode(out)?;
                read.encode(out)
            }
            Self::FileBatchCommitted(commit) => {
                6u8.encode(out)?;
                commit.encode(out)
            }
            Self::AppendStreamOpened(stream) => {
                7u8.encode(out)?;
                stream.encode(out)
            }
            Self::AppendTicket(ticket) => {
                8u8.encode(out)?;
                ticket.encode(out)
            }
            Self::AppendPublishSubmitted(ticket) => {
                9u8.encode(out)?;
                ticket.encode(out)
            }
            Self::AppendPublished(commit) => {
                10u8.encode(out)?;
                commit.encode(out)
            }
            Self::AppendReleased => 11u8.encode(out),
            Self::AppendAborted => 12u8.encode(out),
            Self::Flush(flush) => {
                13u8.encode(out)?;
                flush.encode(out)
            }
            Self::KeyspaceCheckpointed(checkpoint_id) => {
                14u8.encode(out)?;
                checkpoint_id.encode(out)
            }
            Self::KeyspaceSnapshotted(keyspace_id) => {
                15u8.encode(out)?;
                keyspace_id.encode(out)
            }
            Self::KeyspaceRestored(keyspace_id) => {
                16u8.encode(out)?;
                keyspace_id.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::KeyspaceCreated(KeyspaceId::decode(input)?)),
            2 => Ok(Self::KeyspaceInfo(KeyspaceInfo::decode(input)?)),
            3 => Ok(Self::FileCreated(FileId::decode(input)?)),
            4 => Ok(Self::FileInfo(FileInfo::decode(input)?)),
            5 => Ok(Self::Read(ReadResponse::decode(input)?)),
            6 => Ok(Self::FileBatchCommitted(FileWriteCommit::decode(input)?)),
            7 => Ok(Self::AppendStreamOpened(AppendStream::decode(input)?)),
            8 => Ok(Self::AppendTicket(AppendTicket::decode(input)?)),
            9 => Ok(Self::AppendPublishSubmitted(AppendPublishTicket::decode(input)?)),
            10 => Ok(Self::AppendPublished(AppendPublishCommit::decode(input)?)),
            11 => Ok(Self::AppendReleased),
            12 => Ok(Self::AppendAborted),
            13 => Ok(Self::Flush(FlushResult::decode(input)?)),
            14 => Ok(Self::KeyspaceCheckpointed(CheckpointId::decode(input)?)),
            15 => Ok(Self::KeyspaceSnapshotted(KeyspaceId::decode(input)?)),
            16 => Ok(Self::KeyspaceRestored(KeyspaceId::decode(input)?)),
            _ => Err(durable_codec_error("invalid native response tag")),
        }
    }
}

impl DurableCodec for NativeRequestEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.client_epoch.encode(out)?;
        self.deadline.encode(out)?;
        self.request.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            client_epoch: ClientEpoch::decode(input)?,
            deadline: Option::decode(input)?,
            request: NativeRequest::decode(input)?,
        })
    }
}

impl DurableCodec for NativeResponseEnvelope {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.request_id.encode(out)?;
        self.response.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            request_id: RequestId::decode(input)?,
            response: NativeResponse::decode(input)?,
        })
    }
}

impl<T: DurableCodec> DurableCodec for RemoteWireRequest<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        self.incarnation.encode(out)?;
        self.envelope.encode(out)
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        Ok(Self {
            incarnation: ServerIncarnation::decode(input)?,
            envelope: T::decode(input)?,
        })
    }
}

impl<T: DurableCodec> DurableCodec for RemoteWireReply<T> {
    fn encode(&self, out: &mut DurableEncoder) -> Result<()> {
        match self {
            Self::Ok {
                incarnation,
                envelope,
            } => {
                1u8.encode(out)?;
                incarnation.encode(out)?;
                envelope.encode(out)
            }
            Self::Err {
                incarnation,
                reason,
            } => {
                2u8.encode(out)?;
                incarnation.encode(out)?;
                reason.encode(out)
            }
        }
    }

    fn decode(input: &mut DurableDecoder<'_>) -> Result<Self> {
        match u8::decode(input)? {
            1 => Ok(Self::Ok {
                incarnation: ServerIncarnation::decode(input)?,
                envelope: T::decode(input)?,
            }),
            2 => Ok(Self::Err {
                incarnation: ServerIncarnation::decode(input)?,
                reason: String::decode(input)?,
            }),
            _ => Err(durable_codec_error("invalid remote wire reply tag")),
        }
    }
}
