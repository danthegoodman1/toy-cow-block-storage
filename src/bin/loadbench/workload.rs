#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeFileBatchOverlap {
    Sequential,
    Random,
    OverwriteHotset,
}

impl FromStr for NativeFileBatchOverlap {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "sequential" => Ok(Self::Sequential),
            "random" => Ok(Self::Random),
            "overwrite-hotset" => Ok(Self::OverwriteHotset),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown native file batch overlap mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeFileBatchSpec {
    ops: usize,
    write_bytes: usize,
    overlap: NativeFileBatchOverlap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockBatchOverlap {
    Sequential,
    Random,
    OverwriteHotset,
}

impl FromStr for BlockBatchOverlap {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "sequential" => Ok(Self::Sequential),
            "random" => Ok(Self::Random),
            "overwrite-hotset" => Ok(Self::OverwriteHotset),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown block batch overlap mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockBatchSpec {
    ops: usize,
    write_bytes: usize,
    overlap: BlockBatchOverlap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Local,
    Durable,
    TxnSerial,
    TxnSharded,
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => f.write_str("local"),
            Self::Durable => f.write_str("durable"),
            Self::TxnSerial => f.write_str("txn-serial"),
            Self::TxnSharded => f.write_str("txn-sharded"),
        }
    }
}

impl FromStr for ProviderKind {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "local" => Ok(Self::Local),
            "durable" => Ok(Self::Durable),
            "txn-serial" => Ok(Self::TxnSerial),
            "txn-sharded" => Ok(Self::TxnSharded),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown provider {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurabilityMode {
    Acknowledged,
    Flushed,
    AckFlushEvery(u64),
}

impl DurabilityMode {
    fn write_durability(self) -> WriteDurability {
        match self {
            Self::Acknowledged | Self::AckFlushEvery(_) => WriteDurability::Acknowledged,
            Self::Flushed => WriteDurability::Flushed,
        }
    }
}

impl fmt::Display for DurabilityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged => f.write_str("ack"),
            Self::Flushed => f.write_str("flushed"),
            Self::AckFlushEvery(every) => write!(f, "ack-flush:{every}"),
        }
    }
}

impl FromStr for DurabilityMode {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "ack" => Ok(Self::Acknowledged),
            "flushed" => Ok(Self::Flushed),
            _ if value.starts_with("ack-flush:") => {
                let every = value["ack-flush:".len()..]
                    .parse::<u64>()
                    .map_err(|error| {
                        StorageError::invalid_argument(format!(
                            "invalid ack-flush interval: {error}"
                        ))
                    })?;
                if every == 0 {
                    return Err(StorageError::invalid_argument(
                        "ack-flush interval must be greater than zero",
                    ));
                }
                Ok(Self::AckFlushEvery(every))
            }
            _ => Err(StorageError::invalid_argument(format!(
                "unknown durability mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DelayMode {
    Spin,
    Sleep,
}

impl FromStr for DelayMode {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "spin" => Ok(Self::Spin),
            "sleep" => Ok(Self::Sleep),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown delay mode {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    BlockWrite4k,
    BlockWrite4kSameShardContended,
    BlockWrite4kSameShardSerialized,
    BlockWrite4kShardLanes,
    BlockWrite4kDeviceLanes,
    BlockRead4k,
    BlockRead1m,
    BlockWrite1m,
    BlockWrite1mShardLanes,
    BlockWrite1mDeviceLanes,
    BlockBatch4k16Ops,
    BlockBatch4k256Ops,
    BlockBatch4k4096Ops,
    BlockBatch1m16Ops,
    BlockBatch1m128Ops,
    BlockBatchOverwriteCollapse,
    BlockBatchFsyncInterval,
    BlockWritebackFsync1m,
    BlockWritebackFsync2m,
    BlockWritebackFsync4m,
    BlockWritebackFsync16m,
    BlockWritebackPrestagedFsync1m,
    BlockWritebackPrestagedFsync2m,
    BlockWritebackPrestagedFsync4m,
    BlockWritebackPrestagedFsync16m,
    NativeRead4k,
    NativeRead1m,
    NativeWrite4k,
    NativeWrite4kSameFile,
    NativeWrite4kFileLanes,
    NativeWrite1m,
    NativeWrite4m,
    NativeWrite32m,
    NativeFileBatch4k16Ops,
    NativeFileBatch4k256Ops,
    NativeFileBatch4k4096Ops,
    NativeFileBatch1m16Ops,
    NativeFileBatchOverwriteCollapse,
    NativeFileBatchFsyncInterval,
    NativeAppend4k,
    NativeAppend4kSameFile,
    NativeAppend4kFileLanes,
    NativeAppend1m,
    NativeAppend4m,
    NativeAppend32m,
    NativeStreamIngest1m,
    NativeStreamIngest4m,
    NativeStreamIngest32m,
    NativeStreamAppendFlush1m,
    NativeStreamAppendFlush4m,
    NativeStreamAppendFlush32m,
    NativeStreamPublishPreflushed1m,
    NativeStreamFlushPublish1m,
    NativeHotAppend4k,
}

impl Workload {
    fn north_star_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4k,
            Self::BlockWrite4kShardLanes,
            Self::BlockRead4k,
            Self::BlockRead1m,
            Self::BlockWrite1m,
            Self::BlockWrite1mShardLanes,
            Self::NativeRead4k,
            Self::NativeRead1m,
            Self::NativeWrite4k,
            Self::NativeAppend4k,
            Self::NativeHotAppend4k,
        ]
    }

    fn append_batch_suite() -> Vec<Self> {
        vec![
            Self::NativeAppend4k,
            Self::NativeAppend1m,
            Self::NativeAppend4m,
            Self::NativeAppend32m,
            Self::NativeWrite4k,
            Self::NativeWrite1m,
            Self::NativeWrite4m,
            Self::NativeWrite32m,
        ]
    }

    fn append_stream_suite() -> Vec<Self> {
        vec![
            Self::NativeWrite1m,
            Self::NativeWrite4m,
            Self::NativeWrite32m,
            Self::NativeStreamIngest1m,
            Self::NativeStreamIngest4m,
            Self::NativeStreamIngest32m,
            Self::NativeStreamAppendFlush1m,
            Self::NativeStreamAppendFlush4m,
            Self::NativeStreamAppendFlush32m,
            Self::NativeStreamPublishPreflushed1m,
            Self::NativeStreamFlushPublish1m,
        ]
    }

    fn block_metadata_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4kSameShardContended,
            Self::BlockWrite4kSameShardSerialized,
            Self::BlockWrite4kShardLanes,
            Self::BlockWrite4kDeviceLanes,
            Self::BlockWrite1mShardLanes,
            Self::BlockWrite1mDeviceLanes,
        ]
    }

    fn block_batch_suite() -> Vec<Self> {
        vec![
            Self::BlockWrite4kShardLanes,
            Self::BlockWrite1mShardLanes,
            Self::BlockBatch4k16Ops,
            Self::BlockBatch4k256Ops,
            Self::BlockBatch4k4096Ops,
            Self::BlockBatch1m16Ops,
            Self::BlockBatch1m128Ops,
            Self::BlockBatchOverwriteCollapse,
            Self::BlockBatchFsyncInterval,
        ]
    }

    fn block_writeback_suite() -> Vec<Self> {
        vec![
            Self::BlockWritebackFsync1m,
            Self::BlockWritebackFsync2m,
            Self::BlockWritebackFsync4m,
            Self::BlockWritebackFsync16m,
        ]
    }

    fn block_writeback_prestaged_suite() -> Vec<Self> {
        vec![
            Self::BlockWritebackPrestagedFsync1m,
            Self::BlockWritebackPrestagedFsync2m,
            Self::BlockWritebackPrestagedFsync4m,
            Self::BlockWritebackPrestagedFsync16m,
        ]
    }

    fn native_metadata_suite() -> Vec<Self> {
        vec![
            Self::NativeWrite4kSameFile,
            Self::NativeWrite4kFileLanes,
            Self::NativeAppend4kSameFile,
            Self::NativeAppend4kFileLanes,
            Self::NativeStreamFlushPublish1m,
        ]
    }

    fn native_file_batch_suite() -> Vec<Self> {
        vec![
            Self::NativeWrite4k,
            Self::NativeWrite1m,
            Self::NativeFileBatch4k16Ops,
            Self::NativeFileBatch4k256Ops,
            Self::NativeFileBatch4k4096Ops,
            Self::NativeFileBatch1m16Ops,
            Self::NativeFileBatchOverwriteCollapse,
            Self::NativeFileBatchFsyncInterval,
            Self::NativeStreamFlushPublish1m,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::BlockWrite4k => "block-write-4k",
            Self::BlockWrite4kSameShardContended => "block-write-4k-same-shard-contended",
            Self::BlockWrite4kSameShardSerialized => "block-write-4k-same-shard-serialized",
            Self::BlockWrite4kShardLanes => "block-write-4k-shard-lanes",
            Self::BlockWrite4kDeviceLanes => "block-write-4k-device-lanes",
            Self::BlockRead4k => "block-read-4k",
            Self::BlockRead1m => "block-read-1m",
            Self::BlockWrite1m => "block-write-1m",
            Self::BlockWrite1mShardLanes => "block-write-1m-shard-lanes",
            Self::BlockWrite1mDeviceLanes => "block-write-1m-device-lanes",
            Self::BlockBatch4k16Ops => "block-batch-4k-16ops",
            Self::BlockBatch4k256Ops => "block-batch-4k-256ops",
            Self::BlockBatch4k4096Ops => "block-batch-4k-4096ops",
            Self::BlockBatch1m16Ops => "block-batch-1m-16ops",
            Self::BlockBatch1m128Ops => "block-batch-1m-128ops",
            Self::BlockBatchOverwriteCollapse => "block-batch-overwrite-collapse",
            Self::BlockBatchFsyncInterval => "block-batch-fsync-interval",
            Self::BlockWritebackFsync1m => "block-writeback-fsync-1m",
            Self::BlockWritebackFsync2m => "block-writeback-fsync-2m",
            Self::BlockWritebackFsync4m => "block-writeback-fsync-4m",
            Self::BlockWritebackFsync16m => "block-writeback-fsync-16m",
            Self::BlockWritebackPrestagedFsync1m => "block-writeback-prestaged-fsync-1m",
            Self::BlockWritebackPrestagedFsync2m => "block-writeback-prestaged-fsync-2m",
            Self::BlockWritebackPrestagedFsync4m => "block-writeback-prestaged-fsync-4m",
            Self::BlockWritebackPrestagedFsync16m => "block-writeback-prestaged-fsync-16m",
            Self::NativeRead4k => "native-read-4k",
            Self::NativeRead1m => "native-read-1m",
            Self::NativeWrite4k => "native-write-4k",
            Self::NativeWrite4kSameFile => "native-write-4k-same-file",
            Self::NativeWrite4kFileLanes => "native-write-4k-file-lanes",
            Self::NativeWrite1m => "native-write-1m",
            Self::NativeWrite4m => "native-write-4m",
            Self::NativeWrite32m => "native-write-32m",
            Self::NativeFileBatch4k16Ops => "native-file-batch-4k-16ops",
            Self::NativeFileBatch4k256Ops => "native-file-batch-4k-256ops",
            Self::NativeFileBatch4k4096Ops => "native-file-batch-4k-4096ops",
            Self::NativeFileBatch1m16Ops => "native-file-batch-1m-16ops",
            Self::NativeFileBatchOverwriteCollapse => "native-file-batch-overwrite-collapse",
            Self::NativeFileBatchFsyncInterval => "native-file-batch-fsync-interval",
            Self::NativeAppend4k => "native-append-4k",
            Self::NativeAppend4kSameFile => "native-append-4k-same-file",
            Self::NativeAppend4kFileLanes => "native-append-4k-file-lanes",
            Self::NativeAppend1m => "native-append-1m",
            Self::NativeAppend4m => "native-append-4m",
            Self::NativeAppend32m => "native-append-32m",
            Self::NativeStreamIngest1m => "native-stream-ingest-1m",
            Self::NativeStreamIngest4m => "native-stream-ingest-4m",
            Self::NativeStreamIngest32m => "native-stream-ingest-32m",
            Self::NativeStreamAppendFlush1m => "native-stream-append-flush-1m",
            Self::NativeStreamAppendFlush4m => "native-stream-append-flush-4m",
            Self::NativeStreamAppendFlush32m => "native-stream-append-flush-32m",
            Self::NativeStreamPublishPreflushed1m => "native-stream-publish-preflushed-1m",
            Self::NativeStreamFlushPublish1m => "native-stream-flush-publish-1m",
            Self::NativeHotAppend4k => "native-hot-append-4k",
        }
    }

    fn op_size(self, args: &Args) -> Result<usize> {
        if self.is_block_batch() {
            let spec = self.block_batch_spec(args)?;
            return spec.ops.checked_mul(spec.write_bytes).ok_or_else(|| {
                StorageError::invalid_argument("block batch op size overflows usize")
            });
        }
        if let Some(bytes) = self.block_writeback_fsync_bytes() {
            return usize::try_from(bytes).map_err(|_| {
                StorageError::invalid_argument("block writeback op size overflows usize")
            });
        }
        if self.is_native_file_batch() {
            let spec = self.native_file_batch_spec(args)?;
            return spec.ops.checked_mul(spec.write_bytes).ok_or_else(|| {
                StorageError::invalid_argument("native file batch op size overflows usize")
            });
        }
        Ok(match self {
            Self::BlockWrite1m
            | Self::BlockWrite1mShardLanes
            | Self::BlockWrite1mDeviceLanes
            | Self::BlockRead1m
            | Self::NativeRead1m
            | Self::NativeWrite1m
            | Self::NativeAppend1m
            | Self::NativeStreamIngest1m
            | Self::NativeStreamAppendFlush1m
            | Self::NativeStreamPublishPreflushed1m
            | Self::NativeStreamFlushPublish1m => 1024 * 1024,
            Self::NativeWrite4m
            | Self::NativeAppend4m
            | Self::NativeStreamIngest4m
            | Self::NativeStreamAppendFlush4m => 4 * 1024 * 1024,
            Self::NativeWrite32m
            | Self::NativeAppend32m
            | Self::NativeStreamIngest32m
            | Self::NativeStreamAppendFlush32m => 32 * 1024 * 1024,
            Self::BlockWrite4k
            | Self::BlockWrite4kSameShardContended
            | Self::BlockWrite4kSameShardSerialized
            | Self::BlockWrite4kShardLanes
            | Self::BlockWrite4kDeviceLanes
            | Self::BlockRead4k
            | Self::NativeWrite4k
            | Self::NativeWrite4kSameFile
            | Self::NativeWrite4kFileLanes
            | Self::NativeRead4k
            | Self::NativeAppend4k
            | Self::NativeAppend4kSameFile
            | Self::NativeAppend4kFileLanes
            | Self::NativeHotAppend4k => 4096,
            Self::NativeFileBatch4k16Ops
            | Self::NativeFileBatch4k256Ops
            | Self::NativeFileBatch4k4096Ops
            | Self::NativeFileBatch1m16Ops
            | Self::NativeFileBatchOverwriteCollapse
            | Self::NativeFileBatchFsyncInterval
            | Self::BlockBatch4k16Ops
            | Self::BlockBatch4k256Ops
            | Self::BlockBatch4k4096Ops
            | Self::BlockBatch1m16Ops
            | Self::BlockBatch1m128Ops
            | Self::BlockBatchOverwriteCollapse
            | Self::BlockBatchFsyncInterval
            | Self::BlockWritebackFsync1m
            | Self::BlockWritebackFsync2m
            | Self::BlockWritebackFsync4m
            | Self::BlockWritebackFsync16m
            | Self::BlockWritebackPrestagedFsync1m
            | Self::BlockWritebackPrestagedFsync2m
            | Self::BlockWritebackPrestagedFsync4m
            | Self::BlockWritebackPrestagedFsync16m => unreachable!(),
        })
    }

    fn is_read(self) -> bool {
        matches!(
            self,
            Self::BlockRead4k | Self::BlockRead1m | Self::NativeRead4k | Self::NativeRead1m
        )
    }

    fn is_native_write(self) -> bool {
        matches!(
            self,
            Self::NativeWrite4k
                | Self::NativeWrite4kSameFile
                | Self::NativeWrite4kFileLanes
                | Self::NativeWrite1m
                | Self::NativeWrite4m
                | Self::NativeWrite32m
        )
    }

    fn is_native_file_batch(self) -> bool {
        matches!(
            self,
            Self::NativeFileBatch4k16Ops
                | Self::NativeFileBatch4k256Ops
                | Self::NativeFileBatch4k4096Ops
                | Self::NativeFileBatch1m16Ops
                | Self::NativeFileBatchOverwriteCollapse
                | Self::NativeFileBatchFsyncInterval
        )
    }

    fn is_block_batch(self) -> bool {
        matches!(
            self,
            Self::BlockBatch4k16Ops
                | Self::BlockBatch4k256Ops
                | Self::BlockBatch4k4096Ops
                | Self::BlockBatch1m16Ops
                | Self::BlockBatch1m128Ops
                | Self::BlockBatchOverwriteCollapse
                | Self::BlockBatchFsyncInterval
        )
    }

    fn is_block_writeback(self) -> bool {
        matches!(
            self,
            Self::BlockWritebackFsync1m
                | Self::BlockWritebackFsync2m
                | Self::BlockWritebackFsync4m
                | Self::BlockWritebackFsync16m
                | Self::BlockWritebackPrestagedFsync1m
                | Self::BlockWritebackPrestagedFsync2m
                | Self::BlockWritebackPrestagedFsync4m
                | Self::BlockWritebackPrestagedFsync16m
        )
    }

    fn is_block_writeback_prestaged(self) -> bool {
        matches!(
            self,
            Self::BlockWritebackPrestagedFsync1m
                | Self::BlockWritebackPrestagedFsync2m
                | Self::BlockWritebackPrestagedFsync4m
                | Self::BlockWritebackPrestagedFsync16m
        )
    }

    fn block_writeback_fsync_bytes(self) -> Option<u64> {
        match self {
            Self::BlockWritebackFsync1m | Self::BlockWritebackPrestagedFsync1m => {
                Some(1024 * 1024)
            }
            Self::BlockWritebackFsync2m | Self::BlockWritebackPrestagedFsync2m => {
                Some(2 * 1024 * 1024)
            }
            Self::BlockWritebackFsync4m | Self::BlockWritebackPrestagedFsync4m => {
                Some(4 * 1024 * 1024)
            }
            Self::BlockWritebackFsync16m | Self::BlockWritebackPrestagedFsync16m => {
                Some(16 * 1024 * 1024)
            }
            _ => None,
        }
    }

    fn block_batch_spec(self, args: &Args) -> Result<BlockBatchSpec> {
        let (ops, write_bytes, overlap) = match self {
            Self::BlockBatch4k16Ops => (16, 4096, BlockBatchOverlap::Sequential),
            Self::BlockBatch4k256Ops => (256, 4096, BlockBatchOverlap::Sequential),
            Self::BlockBatch4k4096Ops => (4096, 4096, BlockBatchOverlap::Sequential),
            Self::BlockBatch1m16Ops => (16, 1024 * 1024, BlockBatchOverlap::Sequential),
            Self::BlockBatch1m128Ops => (128, 1024 * 1024, BlockBatchOverlap::Sequential),
            Self::BlockBatchOverwriteCollapse => (256, 4096, BlockBatchOverlap::OverwriteHotset),
            Self::BlockBatchFsyncInterval => {
                let write_bytes = 4096usize;
                let fsync_bytes = usize::try_from(args.block_batch_fsync_bytes).map_err(|_| {
                    StorageError::invalid_argument("block batch fsync bytes overflow usize")
                })?;
                (
                    (fsync_bytes / write_bytes).max(1),
                    write_bytes,
                    BlockBatchOverlap::Sequential,
                )
            }
            _ => {
                return Err(StorageError::invalid_argument(
                    "workload is not a block batch workload",
                ));
            }
        };
        let ops = args.block_batch_ops.unwrap_or(ops);
        let write_bytes = args.block_batch_bytes.unwrap_or(write_bytes);
        if ops == 0 || write_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "block batch ops and bytes must be greater than zero",
            ));
        }
        let write_bytes_u64 = u64::try_from(write_bytes).map_err(|_| {
            StorageError::invalid_argument("block batch write bytes overflow u64")
        })?;
        if write_bytes_u64 % u64::from(BLOCK_SIZE) != 0 {
            return Err(StorageError::invalid_argument(
                "block batch write bytes must be block aligned",
            ));
        }
        Ok(BlockBatchSpec {
            ops,
            write_bytes,
            overlap: args.block_batch_overlap.unwrap_or(overlap),
        })
    }

    fn native_file_batch_spec(self, args: &Args) -> Result<NativeFileBatchSpec> {
        let (ops, write_bytes, overlap) = match self {
            Self::NativeFileBatch4k16Ops => (16, 4096, NativeFileBatchOverlap::Sequential),
            Self::NativeFileBatch4k256Ops => (256, 4096, NativeFileBatchOverlap::Sequential),
            Self::NativeFileBatch4k4096Ops => (4096, 4096, NativeFileBatchOverlap::Sequential),
            Self::NativeFileBatch1m16Ops => (16, 1024 * 1024, NativeFileBatchOverlap::Sequential),
            Self::NativeFileBatchOverwriteCollapse => {
                (256, 4096, NativeFileBatchOverlap::OverwriteHotset)
            }
            Self::NativeFileBatchFsyncInterval => {
                let write_bytes = 4096usize;
                let fsync_bytes =
                    usize::try_from(args.native_file_batch_fsync_bytes).map_err(|_| {
                        StorageError::invalid_argument(
                            "native file batch fsync bytes overflow usize",
                        )
                    })?;
                (
                    (fsync_bytes / write_bytes).max(1),
                    write_bytes,
                    NativeFileBatchOverlap::Sequential,
                )
            }
            _ => {
                return Err(StorageError::invalid_argument(
                    "workload is not a native file batch workload",
                ));
            }
        };
        let ops = args.native_file_batch_ops.unwrap_or(ops);
        let write_bytes = args.native_file_batch_bytes.unwrap_or(write_bytes);
        if ops == 0 || write_bytes == 0 {
            return Err(StorageError::invalid_argument(
                "native file batch ops and bytes must be greater than zero",
            ));
        }
        Ok(NativeFileBatchSpec {
            ops,
            write_bytes,
            overlap: args.native_file_batch_overlap.unwrap_or(overlap),
        })
    }

    fn is_native_append(self) -> bool {
        matches!(
            self,
            Self::NativeAppend4k
                | Self::NativeAppend4kSameFile
                | Self::NativeAppend4kFileLanes
                | Self::NativeAppend1m
                | Self::NativeAppend4m
                | Self::NativeAppend32m
        )
    }

    fn is_native_stream(self) -> bool {
        matches!(
            self,
            Self::NativeStreamIngest1m
                | Self::NativeStreamIngest4m
                | Self::NativeStreamIngest32m
                | Self::NativeStreamAppendFlush1m
                | Self::NativeStreamAppendFlush4m
                | Self::NativeStreamAppendFlush32m
                | Self::NativeStreamPublishPreflushed1m
                | Self::NativeStreamFlushPublish1m
        )
    }

    fn is_native_stream_ingest(self) -> bool {
        matches!(
            self,
            Self::NativeStreamIngest1m | Self::NativeStreamIngest4m | Self::NativeStreamIngest32m
        )
    }

    fn is_native_stream_append_flush(self) -> bool {
        matches!(
            self,
            Self::NativeStreamAppendFlush1m
                | Self::NativeStreamAppendFlush4m
                | Self::NativeStreamAppendFlush32m
        )
    }

    fn is_native_stream_publish_preflushed(self) -> bool {
        matches!(self, Self::NativeStreamPublishPreflushed1m)
    }

    fn is_native_stream_flush_publish(self) -> bool {
        matches!(self, Self::NativeStreamFlushPublish1m)
    }

    fn is_block(self) -> bool {
        matches!(
            self,
            Self::BlockWrite4k
                | Self::BlockWrite4kSameShardContended
                | Self::BlockWrite4kSameShardSerialized
                | Self::BlockWrite4kShardLanes
                | Self::BlockWrite4kDeviceLanes
                | Self::BlockRead4k
                | Self::BlockRead1m
                | Self::BlockWrite1m
                | Self::BlockWrite1mShardLanes
                | Self::BlockWrite1mDeviceLanes
                | Self::BlockBatch4k16Ops
                | Self::BlockBatch4k256Ops
                | Self::BlockBatch4k4096Ops
                | Self::BlockBatch1m16Ops
                | Self::BlockBatch1m128Ops
                | Self::BlockBatchOverwriteCollapse
                | Self::BlockBatchFsyncInterval
                | Self::BlockWritebackFsync1m
                | Self::BlockWritebackFsync2m
                | Self::BlockWritebackFsync4m
                | Self::BlockWritebackFsync16m
                | Self::BlockWritebackPrestagedFsync1m
                | Self::BlockWritebackPrestagedFsync2m
                | Self::BlockWritebackPrestagedFsync4m
                | Self::BlockWritebackPrestagedFsync16m
        )
    }

    fn is_block_device_lanes(self) -> bool {
        matches!(
            self,
            Self::BlockWrite4kDeviceLanes | Self::BlockWrite1mDeviceLanes
        )
    }
}

impl FromStr for Workload {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "block-write-4k" => Ok(Self::BlockWrite4k),
            "block-write-4k-same-shard-contended" => Ok(Self::BlockWrite4kSameShardContended),
            "block-write-4k-same-shard-serialized" => Ok(Self::BlockWrite4kSameShardSerialized),
            "block-write-4k-shard-lanes" => Ok(Self::BlockWrite4kShardLanes),
            "block-write-4k-device-lanes" => Ok(Self::BlockWrite4kDeviceLanes),
            "block-read-4k" => Ok(Self::BlockRead4k),
            "block-read-1m" => Ok(Self::BlockRead1m),
            "block-write-1m" => Ok(Self::BlockWrite1m),
            "block-write-1m-shard-lanes" => Ok(Self::BlockWrite1mShardLanes),
            "block-write-1m-device-lanes" => Ok(Self::BlockWrite1mDeviceLanes),
            "block-batch-4k-16ops" => Ok(Self::BlockBatch4k16Ops),
            "block-batch-4k-256ops" => Ok(Self::BlockBatch4k256Ops),
            "block-batch-4k-4096ops" => Ok(Self::BlockBatch4k4096Ops),
            "block-batch-1m-16ops" => Ok(Self::BlockBatch1m16Ops),
            "block-batch-1m-128ops" => Ok(Self::BlockBatch1m128Ops),
            "block-batch-overwrite-collapse" => Ok(Self::BlockBatchOverwriteCollapse),
            "block-batch-fsync-interval" => Ok(Self::BlockBatchFsyncInterval),
            "block-writeback-fsync-1m" => Ok(Self::BlockWritebackFsync1m),
            "block-writeback-fsync-2m" => Ok(Self::BlockWritebackFsync2m),
            "block-writeback-fsync-4m" => Ok(Self::BlockWritebackFsync4m),
            "block-writeback-fsync-16m" => Ok(Self::BlockWritebackFsync16m),
            "block-writeback-prestaged-fsync-1m" => Ok(Self::BlockWritebackPrestagedFsync1m),
            "block-writeback-prestaged-fsync-2m" => Ok(Self::BlockWritebackPrestagedFsync2m),
            "block-writeback-prestaged-fsync-4m" => Ok(Self::BlockWritebackPrestagedFsync4m),
            "block-writeback-prestaged-fsync-16m" => Ok(Self::BlockWritebackPrestagedFsync16m),
            "native-read-4k" => Ok(Self::NativeRead4k),
            "native-read-1m" => Ok(Self::NativeRead1m),
            "native-write-4k" => Ok(Self::NativeWrite4k),
            "native-write-4k-same-file" => Ok(Self::NativeWrite4kSameFile),
            "native-write-4k-file-lanes" => Ok(Self::NativeWrite4kFileLanes),
            "native-write-1m" => Ok(Self::NativeWrite1m),
            "native-write-4m" => Ok(Self::NativeWrite4m),
            "native-write-32m" => Ok(Self::NativeWrite32m),
            "native-file-batch-4k-16ops" => Ok(Self::NativeFileBatch4k16Ops),
            "native-file-batch-4k-256ops" => Ok(Self::NativeFileBatch4k256Ops),
            "native-file-batch-4k-4096ops" => Ok(Self::NativeFileBatch4k4096Ops),
            "native-file-batch-1m-16ops" => Ok(Self::NativeFileBatch1m16Ops),
            "native-file-batch-overwrite-collapse" => Ok(Self::NativeFileBatchOverwriteCollapse),
            "native-file-batch-fsync-interval" => Ok(Self::NativeFileBatchFsyncInterval),
            "native-append-4k" => Ok(Self::NativeAppend4k),
            "native-append-4k-same-file" => Ok(Self::NativeAppend4kSameFile),
            "native-append-4k-file-lanes" => Ok(Self::NativeAppend4kFileLanes),
            "native-append-1m" => Ok(Self::NativeAppend1m),
            "native-append-4m" => Ok(Self::NativeAppend4m),
            "native-append-32m" => Ok(Self::NativeAppend32m),
            "native-stream-ingest-1m" => Ok(Self::NativeStreamIngest1m),
            "native-stream-ingest-4m" => Ok(Self::NativeStreamIngest4m),
            "native-stream-ingest-32m" => Ok(Self::NativeStreamIngest32m),
            "native-stream-append-flush-1m" => Ok(Self::NativeStreamAppendFlush1m),
            "native-stream-append-flush-4m" => Ok(Self::NativeStreamAppendFlush4m),
            "native-stream-append-flush-32m" => Ok(Self::NativeStreamAppendFlush32m),
            "native-stream-publish-preflushed-1m" => Ok(Self::NativeStreamPublishPreflushed1m),
            "native-stream-flush-publish-1m" => Ok(Self::NativeStreamFlushPublish1m),
            "native-hot-append-4k" => Ok(Self::NativeHotAppend4k),
            _ => Err(StorageError::invalid_argument(format!(
                "unknown workload {value}"
            ))),
        }
    }
}
