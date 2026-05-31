
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeviceWriteChunk {
    shard_id: crate::id::ShardId,
    old_root: MetadataNodeId,
    range: crate::api::BlockRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SegmentReplacement {
    segment_id: SegmentId,
    segment_base: BlockIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TreeRangeEdit {
    range: crate::api::BlockRange,
    replacement: Option<SegmentReplacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TreeEditResult {
    root: MetadataNodeId,
    changed: bool,
}

#[derive(Debug, Clone)]
pub(super) struct NativeFileReceiptEdit {
    range: crate::api::BlockRange,
    receipt: VerifiedSegmentReceipt,
}

#[derive(Debug, Clone)]
pub(super) struct NativeFileReceiptPublish {
    keyspace_id: KeyspaceId,
    file_id: FileId,
    base_version: FileVersion,
    committed_range: ByteRange,
    new_size: u64,
    edits: Vec<NativeFileReceiptEdit>,
    durability: WriteDurability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CollapsedFileWrite {
    offset: u64,
    bytes: Vec<u8>,
}

impl CollapsedFileWrite {
    fn end(&self) -> Result<u64> {
        self.offset
            .checked_add(u64::try_from(self.bytes.len()).map_err(|_| {
                StorageError::invalid_argument("native batch write length overflows u64")
            })?)
            .ok_or_else(|| StorageError::invalid_argument("native batch write range overflows"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NativeBatchSegmentGroup {
    start: u64,
    end: u64,
    first_write: usize,
    last_write: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataTreeStats {
    pub nodes: usize,
    pub leaves: usize,
    pub max_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMarkReport {
    pub epoch: u64,
    pub roots: Vec<MetadataNodeId>,
    pub metadata_nodes: Vec<MetadataNodeId>,
    pub segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSweepReport {
    pub epoch: u64,
    pub deleted_metadata_nodes: Vec<MetadataNodeId>,
    pub released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCustodianReport {
    pub mark: MetadataMarkReport,
    pub sweep: MetadataSweepReport,
    pub catalog_released_segments: Vec<SegmentId>,
}

#[derive(Debug, Clone)]
pub(super) struct DurableStoreState {
    config: LocalStoreConfig,
    metadata: MetadataInner,
    storage_nodes: StorageNodeRegistryInner,
    next_write_intent: u128,
    next_extent_id: u128,
}

#[derive(Debug, Clone)]
pub(super) struct StorageNodeRegistryInner {
    next_segment_id: u128,
    next_placement_index: u64,
    node_order: Vec<StorageNodeId>,
    nodes: BTreeMap<StorageNodeId, StorageNodeInner>,
}

#[derive(Debug, Clone)]
pub(super) struct StorageNodeInner {
    segment_store: SegmentStoreInner,
    segment_catalog: CatalogInner,
}

pub(super) type SelectedStorageNodeState = BTreeMap<StorageNodeId, (usize, StorageNodeInner)>;

#[derive(Debug, Default)]
pub(super) struct DurableExportRetention {
    devices: BTreeSet<DeviceId>,
    keyspace_roots: BTreeSet<KeyspaceRootId>,
    keyspace_shards: BTreeSet<KeyspaceCatalogShardId>,
    files: BTreeSet<(KeyspaceId, FileId)>,
    nodes: BTreeSet<MetadataNodeId>,
    segments: BTreeSet<SegmentId>,
    keyspace_root_floor: u128,
    keyspace_shard_floor: u128,
    metadata_node_floor: u128,
}

impl DurableExportRetention {
    fn from_previous_cursor(previous: Option<&DurableExportCursor>) -> Self {
        let mut retained = Self::default();
        if let Some(previous) = previous {
            retained.keyspace_root_floor = previous.next_keyspace_root_id;
            retained.keyspace_shard_floor = previous.next_keyspace_catalog_shard_id;
            retained.metadata_node_floor = previous.next_metadata_node_id;
        }
        retained
    }

    fn should_collect_keyspace_root(&self, root_id: KeyspaceRootId) -> bool {
        root_id.raw() >= self.keyspace_root_floor
    }

    fn should_collect_keyspace_shard(&self, shard_id: KeyspaceCatalogShardId) -> bool {
        shard_id.raw() >= self.keyspace_shard_floor
    }

    fn should_collect_metadata_node(&self, node_id: MetadataNodeId) -> bool {
        node_id.raw() >= self.metadata_node_floor
    }
}

pub(super) fn metadata_referenced_segments(metadata: &MetadataInner) -> BTreeSet<SegmentId> {
    let mut segments = BTreeSet::new();
    for node in metadata.metadata_nodes.values() {
        if let MetadataNodeKind::Leaf { entries, .. } = &node.kind {
            segments.extend(entries.iter().map(|entry| entry.segment_id));
        }
    }
    segments
}

pub(super) fn reconcile_catalog_references_from_metadata(
    metadata: &MetadataInner,
    storage_nodes: &mut StorageNodeRegistryInner,
) -> BTreeMap<StorageNodeId, BTreeSet<SegmentId>> {
    let mut repaired: BTreeMap<StorageNodeId, BTreeSet<SegmentId>> = BTreeMap::new();
    for segment_id in metadata_referenced_segments(metadata) {
        let Some((storage_node, entry)) =
            storage_nodes
                .nodes
                .iter_mut()
                .find_map(|(storage_node, node)| {
                    node.segment_catalog
                        .entries
                        .get_mut(&segment_id)
                        .map(|entry| (*storage_node, entry))
                })
        else {
            continue;
        };
        if entry.state == SegmentLifecycleState::DurablePendingMetadata {
            entry.state = SegmentLifecycleState::Referenced;
            repaired.entry(storage_node).or_default().insert(segment_id);
        }
    }
    repaired
}

pub(super) fn durable_state_storage_node_for_catalog_segment(
    image: &DurableStoreState,
    segment_id: SegmentId,
) -> Option<StorageNodeId> {
    image
        .storage_nodes
        .nodes
        .iter()
        .find_map(|(storage_node, node)| {
            node.segment_catalog
                .entries
                .contains_key(&segment_id)
                .then_some(*storage_node)
        })
}
pub(super) fn replace_leaf_entries(
    entries: &[LeafEntry],
    covered_range: crate::api::BlockRange,
    replacement_range: crate::api::BlockRange,
    replacement: Option<LeafEntry>,
) -> Result<Vec<LeafEntry>> {
    replacement_range.validate_non_empty()?;
    if !covered_range.contains_range(replacement_range)? {
        return Err(StorageError::invalid_argument(
            "replacement range is outside leaf coverage",
        ));
    }

    let mut out = Vec::with_capacity(entries.len() + usize::from(replacement.is_some()));
    let replacement_end = replacement_range.end_exclusive()?.raw();

    for entry in entries {
        let entry_range = entry.logical_range();
        let entry_end = entry_range.end_exclusive()?.raw();
        if !entry_range.overlaps(replacement_range)? {
            out.push(entry.clone());
            continue;
        }

        if entry.logical_start.raw() < replacement_range.start.raw() {
            out.push(LeafEntry {
                logical_start: entry.logical_start,
                blocks: BlockCount::from_raw(
                    replacement_range.start.raw() - entry.logical_start.raw(),
                ),
                segment_id: entry.segment_id,
                segment_offset: entry.segment_offset,
            });
        }

        if entry_end > replacement_end {
            let skipped_blocks = replacement_end - entry.logical_start.raw();
            let segment_offset = entry
                .segment_offset
                .raw()
                .checked_add(skipped_blocks)
                .ok_or_else(|| StorageError::invalid_argument("leaf segment offset overflows"))?;
            out.push(LeafEntry {
                logical_start: BlockIndex::from_raw(replacement_end),
                blocks: BlockCount::from_raw(entry_end - replacement_end),
                segment_id: entry.segment_id,
                segment_offset: BlockIndex::from_raw(segment_offset),
            });
        }
    }

    if let Some(replacement) = replacement {
        out.push(replacement);
    }
    out.sort_by_key(|entry| entry.logical_start.raw());
    coalesce_leaf_entries(out)
}

pub(super) fn run_extent_end(extent: &RunBackedFileExtent) -> Result<u64> {
    extent
        .file_offset_start
        .checked_add(extent.payload_len)
        .ok_or_else(|| StorageError::invalid_argument("run-backed extent end overflows"))
}

pub(super) fn byte_ranges_overlap(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    a_start < b_end && b_start < a_end
}

pub(super) fn block_range_to_byte_range(range: crate::api::BlockRange, block_size: u64) -> Result<ByteRange> {
    let offset = range
        .start
        .raw()
        .checked_mul(block_size)
        .ok_or_else(|| StorageError::invalid_argument("block range byte offset overflows"))?;
    let len = range
        .blocks
        .raw()
        .checked_mul(block_size)
        .ok_or_else(|| StorageError::invalid_argument("block range byte length overflows"))?;
    Ok(ByteRange::new(offset, len))
}

pub(super) fn byte_range_intersection(a: ByteRange, b: ByteRange) -> Result<Option<ByteRange>> {
    let a_end = a.end_exclusive()?;
    let b_end = b.end_exclusive()?;
    let start = a.offset.max(b.offset);
    let end = a_end.min(b_end);
    if start >= end {
        return Ok(None);
    }
    Ok(Some(ByteRange::new(start, end - start)))
}

pub(super) fn slice_run_extent(
    extent: &RunBackedFileExtent,
    start: u64,
    end: u64,
) -> Result<Option<RunBackedFileExtent>> {
    if start >= end {
        return Ok(None);
    }
    let extent_end = run_extent_end(extent)?;
    if start < extent.file_offset_start || end > extent_end {
        return Err(StorageError::invalid_argument(
            "run-backed extent slice exceeds source extent",
        ));
    }
    let delta = start - extent.file_offset_start;
    let integrity = if start == extent.file_offset_start && end == extent_end {
        extent.run.integrity
    } else {
        SegmentPayloadIntegrity::Unchecked
    };
    Ok(Some(RunBackedFileExtent {
        file_offset_start: start,
        payload_len: end - start,
        run: AppendLogRunRange {
            file_offset_start: start,
            payload_len: end - start,
            log_payload_offset: extent
                .run
                .log_payload_offset
                .checked_add(delta)
                .ok_or_else(|| StorageError::invalid_argument("append run offset overflows"))?,
            integrity,
            ..extent.run.clone()
        },
    }))
}

pub(super) fn replace_run_backed_file_extents(
    extents: &[RunBackedFileExtent],
    replacement_range: ByteRange,
    replacements: Vec<RunBackedFileExtent>,
) -> Result<Vec<RunBackedFileExtent>> {
    let replacement_start = replacement_range.offset;
    let replacement_end = replacement_range.end_exclusive()?;
    let mut out = Vec::with_capacity(extents.len() + replacements.len());
    for extent in extents {
        extent.validate()?;
        let extent_end = run_extent_end(extent)?;
        if !byte_ranges_overlap(
            extent.file_offset_start,
            extent_end,
            replacement_start,
            replacement_end,
        ) {
            out.push(extent.clone());
            continue;
        }
        if extent.file_offset_start < replacement_start
            && let Some(left) = slice_run_extent(
                extent,
                extent.file_offset_start,
                replacement_start.min(extent_end),
            )?
        {
            out.push(left);
        }
        if extent_end > replacement_end
            && let Some(right) = slice_run_extent(
                extent,
                replacement_end.max(extent.file_offset_start),
                extent_end,
            )?
        {
            out.push(right);
        }
    }
    out.extend(replacements);
    let ranges =
        coalesce_append_log_run_ranges(out.into_iter().map(|extent| extent.run).collect())?;
    ranges
        .into_iter()
        .map(|run| {
            let extent = RunBackedFileExtent {
                file_offset_start: run.file_offset_start,
                payload_len: run.payload_len,
                run,
            };
            extent.validate()?;
            Ok(extent)
        })
        .collect()
}

pub(super) fn coalesce_leaf_entries(entries: Vec<LeafEntry>) -> Result<Vec<LeafEntry>> {
    let mut out: Vec<LeafEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        if let Some(previous) = out.last_mut()
            && previous.segment_id == entry.segment_id
            && previous.logical_range().end_exclusive()? == entry.logical_start
            && previous.segment_end_exclusive()? == entry.segment_offset
        {
            previous.blocks = BlockCount::from_raw(
                previous
                    .blocks
                    .raw()
                    .checked_add(entry.blocks.raw())
                    .ok_or_else(|| StorageError::invalid_argument("leaf entry size overflows"))?,
            );
            continue;
        }
        out.push(entry);
    }
    Ok(out)
}

pub(super) fn blocks_for_bytes(bytes: u64, block_size: u64) -> Result<u64> {
    if block_size == 0 {
        return Err(StorageError::invalid_argument(
            "block_size must be greater than zero",
        ));
    }
    if bytes == 0 {
        return Ok(0);
    }

    bytes
        .checked_add(block_size - 1)
        .map(|adjusted| adjusted / block_size)
        .ok_or_else(|| StorageError::invalid_argument("byte count overflows block count"))
}

pub(super) fn collapse_native_file_batch_writes(
    writes: &[FileBatchWrite],
    base_size: u64,
    max_batch_bytes: u64,
) -> Result<(Vec<CollapsedFileWrite>, ByteRange, u64)> {
    if writes.is_empty() {
        return Err(StorageError::invalid_argument(
            "native file batch must not be empty",
        ));
    }

    let mut collapsed = Vec::<CollapsedFileWrite>::new();
    let mut running_size = base_size;
    let mut total_bytes = 0u64;
    let mut first_offset = None;

    for write in writes {
        let range = write.byte_range()?;
        first_offset.get_or_insert(range.offset);
        let end = range.end_exclusive()?;
        if range.offset > running_size {
            return Err(StorageError::invalid_argument(
                "native file batch cannot create a sparse gap",
            ));
        }
        running_size = running_size.max(end);
        if write.bytes.is_empty() {
            continue;
        }
        total_bytes = total_bytes
            .checked_add(u64::try_from(write.bytes.len()).map_err(|_| {
                StorageError::invalid_argument("native file batch byte length overflows u64")
            })?)
            .ok_or_else(|| {
                StorageError::invalid_argument("native file batch byte length overflows")
            })?;
        if total_bytes > max_batch_bytes {
            return Err(StorageError::invalid_argument(
                "native file batch exceeds maximum payload bytes",
            ));
        }
        insert_collapsed_file_write(&mut collapsed, write.offset, write.bytes.clone())?;
    }

    if collapsed.is_empty() {
        return Ok((
            collapsed,
            ByteRange::new(first_offset.unwrap_or(base_size), 0),
            base_size,
        ));
    }

    let start = collapsed
        .first()
        .ok_or_else(|| StorageError::corrupt("missing collapsed file write"))?
        .offset;
    let end = collapsed
        .last()
        .ok_or_else(|| StorageError::corrupt("missing collapsed file write"))?
        .end()?;
    Ok((collapsed, ByteRange::new(start, end - start), running_size))
}

pub(super) fn insert_collapsed_file_write(
    collapsed: &mut Vec<CollapsedFileWrite>,
    offset: u64,
    bytes: Vec<u8>,
) -> Result<()> {
    let len = u64::try_from(bytes.len())
        .map_err(|_| StorageError::invalid_argument("native file batch length overflows u64"))?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| StorageError::invalid_argument("native file batch range overflows"))?;
    let mut next = Vec::with_capacity(collapsed.len() + 1);
    for existing in collapsed.drain(..) {
        let existing_start = existing.offset;
        let existing_end = existing.end()?;
        if existing_end <= offset || existing_start >= end {
            next.push(existing);
            continue;
        }
        if existing_start < offset {
            let keep_len = usize::try_from(offset - existing_start).map_err(|_| {
                StorageError::invalid_argument("native file batch left split overflows usize")
            })?;
            next.push(CollapsedFileWrite {
                offset: existing_start,
                bytes: existing.bytes[..keep_len].to_vec(),
            });
        }
        if existing_end > end {
            let keep_start = usize::try_from(end - existing_start).map_err(|_| {
                StorageError::invalid_argument("native file batch right split overflows usize")
            })?;
            next.push(CollapsedFileWrite {
                offset: end,
                bytes: existing.bytes[keep_start..].to_vec(),
            });
        }
    }
    next.push(CollapsedFileWrite { offset, bytes });
    next.sort_by_key(|write| write.offset);
    *collapsed = next;
    Ok(())
}

pub(super) fn native_batch_segment_groups(
    writes: &[CollapsedFileWrite],
    block_size: u64,
) -> Result<Vec<NativeBatchSegmentGroup>> {
    let mut groups: Vec<NativeBatchSegmentGroup> = Vec::new();
    for (index, write) in writes.iter().enumerate() {
        let write_end = write.end()?;
        let group_start = (write.offset / block_size)
            .checked_mul(block_size)
            .ok_or_else(|| StorageError::invalid_argument("native batch group overflows"))?;
        let group_blocks = blocks_for_bytes(write_end - group_start, block_size)?;
        let group_end = group_start
            .checked_add(group_blocks.checked_mul(block_size).ok_or_else(|| {
                StorageError::invalid_argument("native batch group length overflows")
            })?)
            .ok_or_else(|| StorageError::invalid_argument("native batch group end overflows"))?;
        if let Some(last) = groups.last_mut()
            && group_start <= last.end
        {
            last.end = last.end.max(group_end);
            last.last_write = index + 1;
            continue;
        }
        groups.push(NativeBatchSegmentGroup {
            start: group_start,
            end: group_end,
            first_write: index,
            last_write: index + 1,
        });
    }
    Ok(groups)
}

pub(super) fn overlay_native_batch_writes(
    group_start: u64,
    writes: &[CollapsedFileWrite],
    bytes: &mut [u8],
) -> Result<()> {
    for write in writes {
        let start = usize::try_from(write.offset - group_start).map_err(|_| {
            StorageError::invalid_argument("native batch write offset overflows usize")
        })?;
        let end = start
            .checked_add(write.bytes.len())
            .ok_or_else(|| StorageError::invalid_argument("native batch write end overflows"))?;
        let target = bytes.get_mut(start..end).ok_or_else(|| {
            StorageError::corrupt("native batch segment range does not cover payload")
        })?;
        target.copy_from_slice(&write.bytes);
    }
    Ok(())
}

