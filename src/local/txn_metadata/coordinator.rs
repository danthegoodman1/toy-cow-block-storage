/// Block coordinator using the transaction metadata service simulator.
#[derive(Debug, Clone)]
pub struct TxnBlockCoordinator {
    local: LocalCoordinator,
    metadata: Arc<TxnBlockMetadataPlane>,
    storage_node_count: usize,
    block_write_profile_enabled: Arc<AtomicBool>,
    block_write_profiler: Arc<Mutex<Option<TxnBlockWriteProfileBuffer>>>,
}

impl TxnBlockCoordinator {
    pub fn with_storage_nodes(
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        mode: MetadataTxnMode,
    ) -> Result<Self> {
        let storage_node_count =
            normalize_storage_nodes(config.storage_node, storage_nodes.clone()).len();
        Ok(Self {
            local: LocalCoordinator::with_storage_nodes(config, storage_nodes)?,
            metadata: Arc::new(TxnBlockMetadataPlane::new(config, mode)?),
            storage_node_count,
            block_write_profile_enabled: Arc::new(AtomicBool::new(false)),
            block_write_profiler: Arc::new(Mutex::new(None)),
        })
    }

    pub fn provider_name(&self) -> &'static str {
        self.metadata.provider_name()
    }

    pub fn enable_metadata_profiling(&self, capacity: usize) -> Result<()> {
        self.metadata.enable_profiling(capacity)
    }

    pub fn drain_metadata_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        self.metadata.drain_profiles(max)
    }

    pub fn enable_block_write_profiling(&self, capacity: usize) -> Result<()> {
        *lock(&self.block_write_profiler)? = Some(TxnBlockWriteProfileBuffer::new(capacity)?);
        self.block_write_profile_enabled
            .store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn drain_block_write_profiles(&self, max: usize) -> Result<Vec<TxnBlockWriteProfile>> {
        Ok(lock(&self.block_write_profiler)?
            .as_mut()
            .map(|profiler| profiler.drain(max))
            .unwrap_or_default())
    }

    fn record_block_write_profile(&self, profile: TxnBlockWriteProfile) -> Result<()> {
        if !self.block_write_profile_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(profiler) = lock(&self.block_write_profiler)?.as_mut() {
            profiler.record(profile);
        }
        Ok(())
    }

    pub fn create_device(&self, request: CreateDeviceRequest) -> Result<DeviceId> {
        self.metadata
            .create_device(MetadataCreateDeviceRequest::from(request))
            .map(|head| head.device_id)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        self.metadata.checkpoint(device_id)
    }

    pub fn fork_device(&self, source: DeviceId, request: ForkRequest) -> Result<DeviceId> {
        self.metadata
            .fork_device(MetadataForkRequest {
                source,
                target: request.target,
                name: request.name,
            })
            .map(|head| head.device_id)
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceId> {
        self.metadata
            .restore_device(source, point)
            .map(|head| head.device_id)
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        self.metadata.delete_device(device_id)
    }

    pub fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        self.metadata.roots_for_gc(policy)
    }

    pub fn flush_device(&self, device_id: DeviceId) -> Result<FlushResult> {
        let info = self.metadata.device_info(device_id)?;
        Ok(FlushResult {
            device_id,
            durable_through: info.latest_commit,
        })
    }

    fn split_device_range(
        &self,
        device_id: DeviceId,
        spec: &DeviceSpec,
        range: ByteRange,
    ) -> Result<Vec<DeviceWriteChunk>> {
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        let shard_count_u64 = u64::try_from(self.metadata.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard count overflows u64"))?;
        let mut chunks = Vec::new();
        for shard in 0..self.metadata.config.shard_count {
            let shard_u64 = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = spec
                .logical_blocks
                .checked_mul(shard_u64)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count_u64;
            let end = spec
                .logical_blocks
                .checked_mul(shard_u64 + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count_u64;
            let shard_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(start),
                BlockCount::from_raw(end - start),
            );
            let Some(overlap) = shard_range.intersection(requested)? else {
                continue;
            };
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            let (_, shard_head) = self.metadata.read_shard_head(device_id, shard_id)?;
            chunks.push(DeviceWriteChunk {
                shard_id,
                old_root: shard_head.root,
                range: overlap,
            });
        }
        if chunks.is_empty() && range.len != 0 {
            return Err(StorageError::corrupt(
                "device range did not overlap any shard roots",
            ));
        }
        Ok(chunks)
    }

    pub fn write_device_with_integrity(
        &self,
        device_id: DeviceId,
        offset: u64,
        data: &[u8],
        durability: WriteDurability,
        payload_integrity: PayloadIntegrity,
    ) -> Result<WriteCommit> {
        let profile_enabled = self.block_write_profile_enabled.load(Ordering::Relaxed);
        let total_started = profile_enabled.then(Instant::now);
        let mut profile = TxnBlockWriteProfile {
            storage_node_count: usize_to_u64(self.storage_node_count),
            ..TxnBlockWriteProfile::default()
        };

        let started = profile_enabled.then(Instant::now);
        let spec = self.metadata.device_spec(device_id)?;
        let len = u64::try_from(data.len())
            .map_err(|_| StorageError::invalid_argument("write byte length overflows u64"))?;
        let range = ByteRange::new(offset, len);
        range.validate_for_device(&spec)?;
        if let Some(started) = started {
            profile.device_spec_lookup_nanos = duration_nanos_u64(started.elapsed());
        }
        if len == 0 {
            let info = self.metadata.device_info(device_id)?;
            if let Some(started) = total_started {
                profile.total_nanos = duration_nanos_u64(started.elapsed());
                self.record_block_write_profile(profile)?;
            }
            return Ok(WriteCommit {
                device_id,
                commit_seq: info.latest_commit,
                range,
                durability,
            });
        }

        let block_size = u64::from(spec.block_size);
        let started = profile_enabled.then(Instant::now);
        let chunks = self.split_device_range(device_id, &spec, range)?;
        if let Some(started) = started {
            profile.range_split_shard_head_read_nanos = duration_nanos_u64(started.elapsed());
        }
        profile.touched_shard_count = usize_to_u64(chunks.len());
        profile.segment_count = usize_to_u64(chunks.len());

        let started = profile_enabled.then(Instant::now);
        let write_intent = self.local.next_write_intent()?;
        if let Some(started) = started {
            profile.write_intent_alloc_nanos = duration_nanos_u64(started.elapsed());
        }
        let mut updates = Vec::with_capacity(chunks.len());
        let mut segment_receipts = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            let chunk_offset = chunk
                .range
                .start
                .raw()
                .checked_mul(block_size)
                .and_then(|start| start.checked_sub(offset))
                .ok_or_else(|| StorageError::invalid_argument("write chunk offset overflows"))?;
            let byte_start = usize::try_from(chunk_offset).map_err(|_| {
                StorageError::invalid_argument("write chunk offset overflows usize")
            })?;
            let chunk_len = chunk
                .range
                .blocks
                .raw()
                .checked_mul(block_size)
                .ok_or_else(|| StorageError::invalid_argument("write chunk length overflows"))?;
            let byte_len = usize::try_from(chunk_len).map_err(|_| {
                StorageError::invalid_argument("write chunk length overflows usize")
            })?;
            let byte_end = byte_start
                .checked_add(byte_len)
                .ok_or_else(|| StorageError::invalid_argument("write chunk end overflows"))?;
            let chunk_bytes = data
                .get(byte_start..byte_end)
                .ok_or_else(|| StorageError::corrupt("write chunk is outside request bytes"))?;
            let started = profile_enabled.then(Instant::now);
            let chunk_bytes = chunk_bytes.to_vec();
            if let Some(started) = started {
                profile.payload_copy_nanos = profile
                    .payload_copy_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }

            let started = profile_enabled.then(Instant::now);
            let intent = WriteGrantIntent::BlockWrite {
                device_id,
                range: chunk.range,
                fence: DeviceGeneration::from_raw(0),
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
            };
            let verified_receipt = if profile_enabled {
                let (receipt, segment_profile) = self
                    .local
                    .write_segment_for_intent_with_id_owned_verified_profiled(
                        intent,
                        write_intent,
                        chunk_bytes,
                        durability,
                        payload_integrity,
                    )?;
                profile.absorb_segment_write(segment_profile);
                receipt
            } else {
                self.local.write_segment_for_intent_with_id_owned_verified(
                    intent,
                    write_intent,
                    chunk_bytes,
                    durability,
                    payload_integrity,
                )?
            };
            if let Some(started) = started {
                profile.segment_write_nanos = profile
                    .segment_write_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }
            let segment_id = verified_receipt.descriptor.segment_id;
            let edit = TreeRangeEdit {
                range: chunk.range,
                replacement: Some(SegmentReplacement {
                    segment_id,
                    segment_base: chunk.range.start,
                }),
            };
            let started = profile_enabled.then(Instant::now);
            let new_root = self
                .replace_tree_range_with_receipts(
                    chunk.old_root,
                    edit,
                    std::slice::from_ref(&verified_receipt),
                )?
                .root;
            if let Some(started) = started {
                profile.tree_path_copy_nanos = profile
                    .tree_path_copy_nanos
                    .saturating_add(duration_nanos_u64(started.elapsed()));
            }
            segment_receipts.push(verified_receipt);
            updates.push(RootUpdate::BlockShard(ShardRootUpdate {
                shard_id: chunk.shard_id,
                old_root: chunk.old_root,
                new_root,
            }));
        }

        let started = profile_enabled.then(Instant::now);
        let commit_group = self.metadata.publish_commit_group(CommitGroupIntent {
            owner: MappingOwner::BlockDevice(device_id),
            fence: MetadataFence::DeviceGeneration(DeviceGeneration::from_raw(0)),
            updates,
        })?;
        if let Some(started) = started {
            profile.metadata_publish_call_nanos = duration_nanos_u64(started.elapsed());
        }

        let started = profile_enabled.then(Instant::now);
        for receipt in &segment_receipts {
            if profile_enabled {
                let mark_profile = self.local.storage_nodes.mark_segment_referenced_profiled(
                    receipt.receipt(),
                    commit_group.commit_seq,
                    self.local.authority.as_ref(),
                )?;
                profile.absorb_mark_referenced(mark_profile);
            } else {
                self.local.storage_nodes.mark_segment_referenced(
                    receipt.receipt(),
                    commit_group.commit_seq,
                    self.local.authority.as_ref(),
                )?;
            }
        }
        if let Some(started) = started {
            profile.mark_referenced_nanos = duration_nanos_u64(started.elapsed());
        }
        if let Some(started) = total_started {
            profile.total_nanos = duration_nanos_u64(started.elapsed());
            self.record_block_write_profile(profile)?;
        }
        Ok(WriteCommit {
            device_id,
            commit_seq: commit_group.commit_seq,
            range,
            durability,
        })
    }

    pub fn read_device_with_verification(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
        verification: ReadVerification,
    ) -> Result<()> {
        let segment_ids = self.segment_ids_for_device_read(device_id, range)?;
        self.read_device_unverified(device_id, range, buf)?;
        for segment_id in segment_ids {
            self.local
                .storage_nodes
                .verify_segment_payload_for_read(segment_id, verification)?;
        }
        Ok(())
    }

    fn read_device_unverified(
        &self,
        device_id: DeviceId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<()> {
        let spec = self.metadata.device_spec(device_id)?;
        range.validate_for_device(&spec)?;
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }
        buf.fill(0);
        if range.len == 0 {
            return Ok(());
        }
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        for chunk in self.split_device_range(device_id, &spec, range)? {
            let node = self.metadata.get_metadata_node(chunk.old_root)?;
            if node.covered_range.overlaps(requested)? {
                self.read_metadata_node(&node, requested, block_size, buf)?;
            }
        }
        Ok(())
    }

    fn segment_ids_for_device_read(
        &self,
        device_id: DeviceId,
        range: ByteRange,
    ) -> Result<BTreeSet<SegmentId>> {
        let spec = self.metadata.device_spec(device_id)?;
        range.validate_for_device(&spec)?;
        let mut out = BTreeSet::new();
        if range.len == 0 {
            return Ok(out);
        }
        let block_size = u64::from(spec.block_size);
        let requested = crate::api::BlockRange::new(
            BlockIndex::from_raw(range.offset / block_size),
            BlockCount::from_raw(range.len / block_size),
        );
        for chunk in self.split_device_range(device_id, &spec, range)? {
            let node = self.metadata.get_metadata_node(chunk.old_root)?;
            if node.covered_range.overlaps(requested)? {
                self.collect_segment_ids_for_metadata_node(&node, requested, &mut out)?;
            }
        }
        Ok(out)
    }

    fn collect_segment_ids_for_metadata_node(
        &self,
        node: &MetadataNode,
        requested: crate::api::BlockRange,
        out: &mut BTreeSet<SegmentId>,
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.collect_segment_ids_for_metadata_node(&child_node, requested, out)?;
                    }
                }
            }
            MetadataNodeKind::Leaf { entries, .. } => {
                for entry in entries {
                    if entry.logical_range().overlaps(requested)? {
                        out.insert(entry.segment_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn read_metadata_node(
        &self,
        node: &MetadataNode,
        requested: crate::api::BlockRange,
        block_size: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        match &node.kind {
            MetadataNodeKind::Internal { children } => {
                for child in children {
                    if child.range.overlaps(requested)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        self.read_metadata_node(&child_node, requested, block_size, buf)?;
                    }
                }
                Ok(())
            }
            MetadataNodeKind::Leaf { entries, .. } => {
                for entry in entries {
                    let Some(overlap) = entry.logical_range().intersection(requested)? else {
                        continue;
                    };
                    let segment_offset_blocks = entry
                        .segment_offset
                        .raw()
                        .checked_add(overlap.start.raw() - entry.logical_start.raw())
                        .ok_or_else(|| {
                            StorageError::invalid_argument("segment read offset overflows")
                        })?;
                    let segment_range = ByteRange::new(
                        segment_offset_blocks
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte offset overflows")
                            })?,
                        overlap
                            .blocks
                            .raw()
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("segment byte length overflows")
                            })?,
                    );
                    let output_offset = usize::try_from(
                        (overlap.start.raw() - requested.start.raw())
                            .checked_mul(block_size)
                            .ok_or_else(|| {
                                StorageError::invalid_argument("read output offset overflows")
                            })?,
                    )
                    .map_err(|_| {
                        StorageError::invalid_argument("read output offset overflows usize")
                    })?;
                    let output_len = usize::try_from(segment_range.len).map_err(|_| {
                        StorageError::invalid_argument("read output length overflows usize")
                    })?;
                    let output_end = output_offset.checked_add(output_len).ok_or_else(|| {
                        StorageError::invalid_argument("read output end overflows")
                    })?;
                    let output = buf.get_mut(output_offset..output_end).ok_or_else(|| {
                        StorageError::corrupt("metadata read output range exceeds buffer")
                    })?;
                    self.local.storage_nodes.read_segment(
                        entry.segment_id,
                        segment_range,
                        output,
                    )?;
                }
                Ok(())
            }
        }
    }

    fn verified_receipts_for_entries_with_cache(
        &self,
        entries: &[LeafEntry],
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<Vec<VerifiedSegmentReceipt>> {
        let mut cache: BTreeMap<SegmentId, VerifiedSegmentReceipt> = additional_receipts
            .iter()
            .map(|receipt| (receipt.descriptor.segment_id, receipt.clone()))
            .collect();
        let mut receipts: BTreeMap<SegmentId, VerifiedSegmentReceipt> = BTreeMap::new();
        for entry in entries {
            if let std::collections::btree_map::Entry::Vacant(vacant) =
                receipts.entry(entry.segment_id)
            {
                if let Some(receipt) = cache.remove(&entry.segment_id) {
                    vacant.insert(receipt);
                } else {
                    let receipt = self
                        .local
                        .storage_nodes
                        .receipt_for_segment(entry.segment_id)?;
                    vacant.insert(self.local.authority.verify_segment_receipt(&receipt)?);
                }
            }
        }
        Ok(receipts.into_values().collect())
    }

    fn replace_tree_range_with_receipts(
        &self,
        root_id: MetadataNodeId,
        edit: TreeRangeEdit,
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<TreeEditResult> {
        edit.range.validate_non_empty()?;
        let root = self.metadata.get_metadata_node(root_id)?;
        if !root.covered_range.contains_range(edit.range)? {
            return Err(StorageError::invalid_argument(
                "edit range is outside metadata tree coverage",
            ));
        }
        self.replace_tree_range_at(&root, edit, additional_receipts)
    }

    fn replace_tree_range_at(
        &self,
        node: &MetadataNode,
        edit: TreeRangeEdit,
        additional_receipts: &[VerifiedSegmentReceipt],
    ) -> Result<TreeEditResult> {
        if !node.covered_range.overlaps(edit.range)? {
            return Ok(TreeEditResult {
                root: node.node_id,
                changed: false,
            });
        }
        match &node.kind {
            MetadataNodeKind::Leaf {
                entries,
                run_extents,
            } => {
                let Some(overlap) = node.covered_range.intersection(edit.range)? else {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                };
                let replacement = edit.replacement.map(|replacement| {
                    let offset = overlap.start.raw() - replacement.segment_base.raw();
                    LeafEntry {
                        logical_start: overlap.start,
                        blocks: overlap.blocks,
                        segment_id: replacement.segment_id,
                        segment_offset: BlockIndex::from_raw(offset),
                    }
                });
                let new_entries =
                    replace_leaf_entries(entries, node.covered_range, overlap, replacement)?;
                let block_size = u64::from(self.metadata.config.block_size);
                let overlap_bytes = block_range_to_byte_range(overlap, block_size)?;
                let new_run_extents =
                    replace_run_backed_file_extents(run_extents, overlap_bytes, Vec::new())?;
                if new_entries == *entries && new_run_extents == *run_extents {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }
                let segment_receipts = self
                    .verified_receipts_for_entries_with_cache(&new_entries, additional_receipts)?;
                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Leaf {
                        entries: new_entries,
                        run_extents: new_run_extents,
                    },
                )?;
                let segment_descriptors: Vec<_> = segment_receipts
                    .iter()
                    .map(|receipt| receipt.descriptor.clone())
                    .collect();
                new_node.validate(&segment_descriptors)?;
                self.metadata.persist_metadata_node(MetadataNodeWrite::new(
                    new_node.clone(),
                    segment_receipts,
                ))?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
            MetadataNodeKind::Internal { children } => {
                let mut changed = false;
                let mut new_children = Vec::with_capacity(children.len());
                for child in children {
                    if child.range.overlaps(edit.range)? {
                        let child_node = self.metadata.get_metadata_node(child.node_id)?;
                        let child_result =
                            self.replace_tree_range_at(&child_node, edit, additional_receipts)?;
                        changed |= child_result.changed;
                        new_children.push(MetadataChild {
                            range: child.range,
                            node_id: child_result.root,
                        });
                    } else {
                        new_children.push(child.clone());
                    }
                }
                if !changed {
                    return Ok(TreeEditResult {
                        root: node.node_id,
                        changed: false,
                    });
                }
                let new_node = self.metadata.allocate_metadata_node(
                    node.covered_range,
                    MetadataNodeKind::Internal {
                        children: new_children,
                    },
                )?;
                new_node.validate(&[])?;
                self.metadata
                    .persist_metadata_node(MetadataNodeWrite::new(new_node.clone(), Vec::new()))?;
                Ok(TreeEditResult {
                    root: new_node.node_id,
                    changed: true,
                })
            }
        }
    }
}

