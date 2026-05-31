/// Block-focused metadata plane backed by the in-memory transaction store.
#[derive(Debug)]
pub struct TxnBlockMetadataPlane {
    config: LocalStoreConfig,
    store: MetadataTxnStore,
    next_device_id: AtomicU64,
    next_metadata_node_id: AtomicU64,
    next_commit_group_id: AtomicU64,
    next_public_commit_seq: AtomicU64,
    next_checkpoint_id: AtomicU64,
}

impl TxnBlockMetadataPlane {
    pub fn new(config: LocalStoreConfig, mode: MetadataTxnMode) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            store: MetadataTxnStore::new(mode)?,
            next_device_id: AtomicU64::new(1),
            next_metadata_node_id: AtomicU64::new(1),
            next_commit_group_id: AtomicU64::new(1),
            next_public_commit_seq: AtomicU64::new(1),
            next_checkpoint_id: AtomicU64::new(1),
        })
    }

    pub fn provider_name(&self) -> &'static str {
        self.store.mode_name()
    }

    pub fn enable_profiling(&self, capacity: usize) -> Result<()> {
        self.store.enable_profiling(capacity)
    }

    pub fn drain_profiles(&self, max: usize) -> Result<Vec<MetadataTxnProfile>> {
        self.store.drain_profiles(max)
    }

    fn alloc_device_id(&self) -> Result<DeviceId> {
        let raw = self.next_device_id.fetch_add(1, Ordering::SeqCst);
        Ok(DeviceId::from_raw(u128::from(raw)))
    }

    fn alloc_metadata_node_id(&self) -> Result<MetadataNodeId> {
        let raw = self.next_metadata_node_id.fetch_add(1, Ordering::SeqCst);
        Ok(MetadataNodeId::from_raw(u128::from(raw)))
    }

    fn alloc_commit_group_id(&self) -> Result<CommitGroupId> {
        let raw = self.next_commit_group_id.fetch_add(1, Ordering::SeqCst);
        Ok(CommitGroupId::from_raw(u128::from(raw)))
    }

    fn alloc_checkpoint_id(&self) -> Result<CheckpointId> {
        let raw = self.next_checkpoint_id.fetch_add(1, Ordering::SeqCst);
        Ok(CheckpointId::from_raw(u128::from(raw)))
    }

    fn alloc_public_commit_seq(&self) -> Result<(CommitSeq, u64)> {
        let started = Instant::now();
        let raw = self.next_public_commit_seq.fetch_add(1, Ordering::SeqCst);
        Ok((
            CommitSeq::from_raw(raw),
            duration_nanos_u64(started.elapsed()),
        ))
    }

    fn read_values(&self, keys: &[MetadataTxnKey]) -> Result<Vec<VersionedMetadataTxnValue>> {
        self.store.read(keys)
    }

    fn read_one(&self, key: MetadataTxnKey) -> Result<VersionedMetadataTxnValue> {
        self.read_values(&[key])?
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::corrupt("metadata transaction read returned no value"))
    }

    fn read_required(&self, key: MetadataTxnKey) -> Result<(MetadataTxnRead, MetadataTxnValue)> {
        let read = self.read_one(key)?;
        let value = read
            .value
            .clone()
            .ok_or_else(|| StorageError::not_found("metadata_key", format!("{:?}", read.key)))?;
        Ok((
            MetadataTxnRead {
                key: read.key,
                version: read.version,
            },
            value,
        ))
    }

    fn read_manifest(&self, device_id: DeviceId) -> Result<(MetadataTxnRead, TxnDeviceManifest)> {
        let (read, value) = self.read_required(MetadataTxnKey::DeviceManifest(device_id))?;
        let MetadataTxnValue::DeviceManifest(manifest) = value else {
            return Err(StorageError::corrupt(
                "device manifest key has wrong value kind",
            ));
        };
        if !manifest.live {
            return Err(StorageError::not_found("device", device_id.to_string()));
        }
        Ok((read, manifest))
    }

    fn read_shard_head(
        &self,
        device_id: DeviceId,
        shard_id: ShardId,
    ) -> Result<(MetadataTxnRead, TxnShardHead)> {
        let (read, value) =
            self.read_required(MetadataTxnKey::DeviceShardHead(device_id, shard_id))?;
        let MetadataTxnValue::DeviceShardHead(head) = value else {
            return Err(StorageError::corrupt(
                "device shard-head key has wrong value kind",
            ));
        };
        Ok((read, head))
    }

    fn create_empty_tree_nodes(
        &self,
        range: crate::api::BlockRange,
        nodes: &mut Vec<MetadataNode>,
    ) -> Result<MetadataNodeId> {
        range.validate_non_empty()?;
        if range.blocks.raw() <= self.config.metadata_leaf_blocks {
            let node = MetadataNode {
                node_id: self.alloc_metadata_node_id()?,
                covered_range: range,
                kind: MetadataNodeKind::Leaf {
                    entries: Vec::new(),
                    run_extents: Vec::new(),
                },
            };
            node.validate(&[])?;
            let node_id = node.node_id;
            nodes.push(node);
            return Ok(node_id);
        }

        let child_count =
            self.config
                .metadata_fanout
                .min(usize::try_from(range.blocks.raw()).map_err(|_| {
                    StorageError::invalid_argument("metadata range block count overflows usize")
                })?);
        let range_start = range.start.raw();
        let range_blocks = range.blocks.raw();
        let child_count_u64 = u64::try_from(child_count)
            .map_err(|_| StorageError::invalid_argument("metadata fanout overflows u64"))?;
        let mut children = Vec::with_capacity(child_count);
        for child_index in 0..child_count {
            let child_index_u64 = u64::try_from(child_index)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let next_child_index_u64 = u64::try_from(child_index + 1)
                .map_err(|_| StorageError::invalid_argument("child index overflows u64"))?;
            let child_start_offset = range_blocks
                .checked_mul(child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?
                / child_count_u64;
            let child_end_offset = range_blocks
                .checked_mul(next_child_index_u64)
                .ok_or_else(|| StorageError::invalid_argument("child range end overflows"))?
                / child_count_u64;
            let child_start = range_start
                .checked_add(child_start_offset)
                .ok_or_else(|| StorageError::invalid_argument("child range start overflows"))?;
            let child_blocks = child_end_offset - child_start_offset;
            let child_range = crate::api::BlockRange::new(
                BlockIndex::from_raw(child_start),
                BlockCount::from_raw(child_blocks),
            );
            let child_node = self.create_empty_tree_nodes(child_range, nodes)?;
            children.push(MetadataChild {
                range: child_range,
                node_id: child_node,
            });
        }

        let node = MetadataNode {
            node_id: self.alloc_metadata_node_id()?,
            covered_range: range,
            kind: MetadataNodeKind::Internal { children },
        };
        node.validate(&[])?;
        let node_id = node.node_id;
        nodes.push(node);
        Ok(node_id)
    }

    pub fn create_device(&self, request: MetadataCreateDeviceRequest) -> Result<DeviceHead> {
        request.spec.validate()?;
        if request.spec.logical_blocks < self.config.shard_count as u64 {
            return Err(StorageError::invalid_argument(
                "device logical blocks must cover every shard",
            ));
        }
        let device_id = self.alloc_device_id()?;
        let shard_count_u64 = u64::try_from(self.config.shard_count)
            .map_err(|_| StorageError::invalid_argument("shard count overflows u64"))?;
        let mut shard_roots = Vec::with_capacity(self.config.shard_count);
        let mut writes = Vec::new();
        for shard in 0..self.config.shard_count {
            let shard_u64 = u64::try_from(shard)
                .map_err(|_| StorageError::invalid_argument("shard index overflows u64"))?;
            let start = request
                .spec
                .logical_blocks
                .checked_mul(shard_u64)
                .ok_or_else(|| StorageError::invalid_argument("shard start overflows"))?
                / shard_count_u64;
            let end = request
                .spec
                .logical_blocks
                .checked_mul(shard_u64 + 1)
                .ok_or_else(|| StorageError::invalid_argument("shard end overflows"))?
                / shard_count_u64;
            let mut nodes = Vec::new();
            let root = self.create_empty_tree_nodes(
                crate::api::BlockRange::new(
                    BlockIndex::from_raw(start),
                    BlockCount::from_raw(end - start),
                ),
                &mut nodes,
            )?;
            for node in nodes {
                writes.push((
                    MetadataTxnKey::MetadataNode(node.node_id),
                    MetadataTxnValue::MetadataNode(node),
                ));
            }
            shard_roots.push(root);
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(device_id, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: CommitSeq::from_raw(0),
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::DeviceManifest(device_id),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: request.spec.clone(),
                shard_count: self.config.shard_count,
                live: true,
            }),
        ));

        let head = DeviceHead {
            device_id,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: shard_roots.clone(),
            latest_commit: CommitSeq::from_raw(0),
        };
        head.validate(self.config.shard_count)?;
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq: CommitSeq::from_raw(0),
                time: LogicalTime::from_raw(0),
                owner: MappingOwner::BlockDevice(device_id),
                roots: CheckpointRoots::BlockShard(shard_roots),
            }),
        ));
        self.store
            .commit(MetadataTxnProfilePhase::CreateDevice, Vec::new(), writes, 0)?;
        Ok(head)
    }

    pub fn device_info(&self, device_id: DeviceId) -> Result<DeviceInfo> {
        let (_, manifest) = self.read_manifest(device_id)?;
        let head = self.get_head(device_id)?;
        Ok(DeviceInfo {
            device_id,
            generation: head.generation,
            spec: manifest.spec,
            latest_commit: head.latest_commit,
        })
    }

    pub fn device_spec(&self, device_id: DeviceId) -> Result<DeviceSpec> {
        self.read_manifest(device_id)
            .map(|(_, manifest)| manifest.spec)
    }

    pub fn get_head(&self, device_id: DeviceId) -> Result<DeviceHead> {
        let (_, manifest) = self.read_manifest(device_id)?;
        let mut shard_roots = Vec::with_capacity(manifest.shard_count);
        let mut generation = DeviceGeneration::from_raw(0);
        let mut latest_commit = CommitSeq::from_raw(0);
        for shard in 0..manifest.shard_count {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            let (_, shard_head) = self.read_shard_head(device_id, shard_id)?;
            shard_roots.push(shard_head.root);
            if shard_head.generation.raw() > generation.raw() {
                generation = shard_head.generation;
            }
            if shard_head.latest_commit.raw() > latest_commit.raw() {
                latest_commit = shard_head.latest_commit;
            }
        }
        let head = DeviceHead {
            device_id,
            generation,
            shard_roots,
            latest_commit,
        };
        head.validate(manifest.shard_count)?;
        Ok(head)
    }

    pub fn get_metadata_node(&self, node_id: MetadataNodeId) -> Result<MetadataNode> {
        let (_, value) = self.read_required(MetadataTxnKey::MetadataNode(node_id))?;
        let MetadataTxnValue::MetadataNode(node) = value else {
            return Err(StorageError::corrupt(
                "metadata node key has wrong value kind",
            ));
        };
        Ok(node)
    }

    pub fn allocate_metadata_node(
        &self,
        covered_range: crate::api::BlockRange,
        kind: MetadataNodeKind,
    ) -> Result<MetadataNode> {
        Ok(MetadataNode {
            node_id: self.alloc_metadata_node_id()?,
            covered_range,
            kind,
        })
    }

    pub fn persist_metadata_node(&self, write: MetadataNodeWrite) -> Result<()> {
        let segment_descriptors = write.segment_descriptors();
        write.node.validate(&segment_descriptors)?;
        let key = MetadataTxnKey::MetadataNode(write.node.node_id);
        let existing = self.read_one(key.clone())?;
        if let Some(MetadataTxnValue::MetadataNode(existing_node)) = &existing.value {
            if existing_node == &write.node {
                return Ok(());
            }
            return Err(StorageError::conflict(
                "metadata node ID already exists with different content",
            ));
        }
        if existing.value.is_some() {
            return Err(StorageError::corrupt(
                "metadata node key has wrong value kind",
            ));
        }
        self.store.commit(
            MetadataTxnProfilePhase::PersistNode,
            vec![MetadataTxnRead {
                key: existing.key,
                version: existing.version,
            }],
            vec![(key, MetadataTxnValue::MetadataNode(write.node))],
            0,
        )
    }

    pub fn publish_commit_group(&self, intent: CommitGroupIntent) -> Result<CommitGroup> {
        let MappingOwner::BlockDevice(device_id) = intent.owner else {
            return Err(StorageError::unsupported(
                "transaction metadata backend only supports block commit groups",
            ));
        };
        match intent.fence {
            MetadataFence::DeviceGeneration(_) => {}
            _ => {
                return Err(StorageError::invalid_argument(
                    "block device commit requires device-generation fence",
                ));
            }
        }
        if intent.updates.is_empty() {
            return Err(StorageError::invalid_argument(
                "commit group must include at least one root update",
            ));
        }

        let (_, manifest) = self.read_manifest(device_id)?;
        let mut read_set = Vec::new();
        let mut writes = Vec::new();
        let mut shard_commits = Vec::with_capacity(intent.updates.len());
        let mut shard_head_writes = Vec::with_capacity(intent.updates.len());

        for update in &intent.updates {
            let RootUpdate::BlockShard(update) = update else {
                return Err(StorageError::invalid_argument(
                    "block device commit cannot include file-root updates",
                ));
            };
            let shard = usize::try_from(update.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            if shard >= manifest.shard_count {
                return Err(StorageError::invalid_argument(
                    "shard update is outside device root set",
                ));
            }
            let (read, shard_head) = self.read_shard_head(device_id, update.shard_id)?;
            if shard_head.root != update.old_root {
                let mut profile =
                    MetadataTxnProfile::new(MetadataTxnProfilePhase::PublishBlockCommit);
                profile.logical_conflict_for_manual();
                self.store.record_profile(profile)?;
                return Err(StorageError::conflict("stale shard root"));
            }
            let _ = self.get_metadata_node(update.new_root)?;
            read_set.push(read);
            shard_commits.push((update.shard_id, update.old_root, update.new_root));
            shard_head_writes.push((
                update.shard_id,
                update.new_root,
                DeviceGeneration::from_raw(
                    shard_head
                        .generation
                        .raw()
                        .checked_add(1)
                        .ok_or_else(|| StorageError::conflict("device generation overflow"))?,
                ),
            ));
        }

        let (commit_seq, commit_version_alloc_nanos) = self.alloc_public_commit_seq()?;
        let commit_group_id = self.alloc_commit_group_id()?;
        for (shard_id, root, generation) in shard_head_writes {
            writes.push((
                MetadataTxnKey::DeviceShardHead(device_id, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation,
                    latest_commit: commit_seq,
                }),
            ));
        }
        let commit_group = CommitGroup {
            commit_group: commit_group_id,
            commit_seq,
            owner: MappingOwner::BlockDevice(device_id),
            updates: intent.updates,
        };
        for (shard_id, old_root, new_root) in shard_commits {
            writes.push((
                MetadataTxnKey::ShardCommit(device_id, commit_seq, shard_id),
                MetadataTxnValue::ShardCommit(ShardCommit {
                    commit_seq,
                    commit_group: commit_group_id,
                    time: LogicalTime::from_raw(commit_seq.raw()),
                    device_id,
                    shard_id,
                    old_root,
                    new_root,
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::CommitGroup(commit_group_id),
            MetadataTxnValue::CommitGroup(commit_group.clone()),
        ));

        self.store.commit(
            MetadataTxnProfilePhase::PublishBlockCommit,
            read_set,
            writes,
            commit_version_alloc_nanos,
        )?;
        Ok(commit_group)
    }

    pub fn checkpoint(&self, device_id: DeviceId) -> Result<CheckpointId> {
        let head = self.get_head(device_id)?;
        let checkpoint_id = self.alloc_checkpoint_id()?;
        self.store.commit(
            MetadataTxnProfilePhase::Checkpoint,
            Vec::new(),
            vec![(
                MetadataTxnKey::Checkpoint(checkpoint_id),
                MetadataTxnValue::Checkpoint(Checkpoint {
                    checkpoint_id,
                    commit_seq: head.latest_commit,
                    time: LogicalTime::from_raw(head.latest_commit.raw()),
                    owner: MappingOwner::BlockDevice(device_id),
                    roots: CheckpointRoots::BlockShard(head.shard_roots),
                }),
            )],
            0,
        )?;
        Ok(checkpoint_id)
    }

    pub fn fork_device(&self, request: MetadataForkRequest) -> Result<DeviceHead> {
        let (_, source_manifest) = self.read_manifest(request.source)?;
        let source_head = self.get_head(request.source)?;
        let target = match request.target {
            Some(target) => target,
            None => self.alloc_device_id()?,
        };
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        let mut writes = Vec::new();
        writes.push((
            MetadataTxnKey::DeviceManifest(target),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: source_manifest.spec,
                shard_count: source_manifest.shard_count,
                live: true,
            }),
        ));
        for (shard, root) in source_head.shard_roots.iter().copied().enumerate() {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(target, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: commit_seq,
                }),
            ));
        }
        writes.push((
            MetadataTxnKey::ForkRecord(commit_seq),
            MetadataTxnValue::ForkRecord(ForkRecord {
                commit_seq,
                source: request.source,
                target,
                shard_roots: source_head.shard_roots.clone(),
            }),
        ));
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq,
                time: LogicalTime::from_raw(commit_seq.raw()),
                owner: MappingOwner::BlockDevice(target),
                roots: CheckpointRoots::BlockShard(source_head.shard_roots.clone()),
            }),
        ));
        self.store.commit(
            MetadataTxnProfilePhase::Fork,
            Vec::new(),
            writes,
            alloc_nanos,
        )?;
        Ok(DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots: source_head.shard_roots,
            latest_commit: commit_seq,
        })
    }

    pub fn restore_device(&self, source: DeviceId, point: RestorePoint) -> Result<DeviceHead> {
        let (_, source_manifest) = self.read_manifest(source)?;
        let target_commit = self.target_commit_for_restore(source, point)?;
        let shard_roots = self.replay_device_roots(source, target_commit)?;
        let target = self.alloc_device_id()?;
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        let mut writes = Vec::new();
        writes.push((
            MetadataTxnKey::DeviceManifest(target),
            MetadataTxnValue::DeviceManifest(TxnDeviceManifest {
                spec: source_manifest.spec,
                shard_count: source_manifest.shard_count,
                live: true,
            }),
        ));
        for (shard, root) in shard_roots.iter().copied().enumerate() {
            let shard_id = ShardId::from_raw(
                u32::try_from(shard)
                    .map_err(|_| StorageError::invalid_argument("shard index overflows u32"))?,
            );
            writes.push((
                MetadataTxnKey::DeviceShardHead(target, shard_id),
                MetadataTxnValue::DeviceShardHead(TxnShardHead {
                    root,
                    generation: DeviceGeneration::from_raw(0),
                    latest_commit: commit_seq,
                }),
            ));
        }
        let checkpoint_id = self.alloc_checkpoint_id()?;
        writes.push((
            MetadataTxnKey::Checkpoint(checkpoint_id),
            MetadataTxnValue::Checkpoint(Checkpoint {
                checkpoint_id,
                commit_seq,
                time: LogicalTime::from_raw(commit_seq.raw()),
                owner: MappingOwner::BlockDevice(target),
                roots: CheckpointRoots::BlockShard(shard_roots.clone()),
            }),
        ));
        self.store.commit(
            MetadataTxnProfilePhase::Restore,
            Vec::new(),
            writes,
            alloc_nanos,
        )?;
        Ok(DeviceHead {
            device_id: target,
            generation: DeviceGeneration::from_raw(0),
            shard_roots,
            latest_commit: commit_seq,
        })
    }

    pub fn delete_device(&self, device_id: DeviceId) -> Result<DeleteResult> {
        let (manifest_read, mut manifest) = self.read_manifest(device_id)?;
        let head = self.get_head(device_id)?;
        let (commit_seq, alloc_nanos) = self.alloc_public_commit_seq()?;
        manifest.live = false;
        self.store.commit(
            MetadataTxnProfilePhase::Delete,
            vec![manifest_read],
            vec![
                (
                    MetadataTxnKey::DeviceManifest(device_id),
                    MetadataTxnValue::DeviceManifest(manifest),
                ),
                (
                    MetadataTxnKey::DeleteRecord(commit_seq),
                    MetadataTxnValue::DeleteRecord(DeleteRecord {
                        commit_seq,
                        time: LogicalTime::from_raw(commit_seq.raw()),
                        device_id,
                        shard_roots: head.shard_roots,
                    }),
                ),
            ],
            alloc_nanos,
        )?;
        Ok(DeleteResult {
            device_id,
            commit_seq,
        })
    }

    pub fn roots_for_gc(&self, policy: RetentionPolicy) -> Result<Vec<MetadataNodeId>> {
        let mut roots = BTreeSet::new();
        for value in self.store.values()? {
            match value {
                MetadataTxnValue::DeviceShardHead(head) => {
                    roots.insert(head.root);
                }
                MetadataTxnValue::ShardCommit(commit) => {
                    roots.insert(commit.old_root);
                    roots.insert(commit.new_root);
                }
                MetadataTxnValue::ForkRecord(record) => {
                    roots.extend(record.shard_roots);
                }
                MetadataTxnValue::DeleteRecord(record) if policy.retain_deleted_devices => {
                    roots.extend(record.shard_roots);
                }
                MetadataTxnValue::Checkpoint(checkpoint) => {
                    if let CheckpointRoots::BlockShard(shard_roots) = checkpoint.roots {
                        roots.extend(shard_roots);
                    }
                }
                _ => {}
            }
        }
        Ok(roots.into_iter().collect())
    }

    fn target_commit_for_restore(
        &self,
        source: DeviceId,
        point: RestorePoint,
    ) -> Result<CommitSeq> {
        match point {
            RestorePoint::Commit(commit) => Ok(commit),
            RestorePoint::Time(time) => Ok(CommitSeq::from_raw(time.raw())),
            RestorePoint::Checkpoint(checkpoint_id) => {
                let (_, value) = self.read_required(MetadataTxnKey::Checkpoint(checkpoint_id))?;
                let MetadataTxnValue::Checkpoint(checkpoint) = value else {
                    return Err(StorageError::corrupt("checkpoint key has wrong value kind"));
                };
                if checkpoint.owner != MappingOwner::BlockDevice(source) {
                    return Err(StorageError::invalid_argument(
                        "checkpoint does not belong to source device",
                    ));
                }
                Ok(checkpoint.commit_seq)
            }
        }
    }

    fn replay_device_roots(
        &self,
        source: DeviceId,
        target_commit: CommitSeq,
    ) -> Result<Vec<MetadataNodeId>> {
        let values = self.store.values()?;
        let mut checkpoints = Vec::new();
        let mut commits = Vec::new();
        for value in values {
            match value {
                MetadataTxnValue::Checkpoint(checkpoint)
                    if checkpoint.owner == MappingOwner::BlockDevice(source)
                        && checkpoint.commit_seq.raw() <= target_commit.raw() =>
                {
                    checkpoints.push(checkpoint);
                }
                MetadataTxnValue::ShardCommit(commit)
                    if commit.device_id == source
                        && commit.commit_seq.raw() <= target_commit.raw() =>
                {
                    commits.push(commit);
                }
                _ => {}
            }
        }
        checkpoints.sort_by_key(|checkpoint| checkpoint.commit_seq.raw());
        let checkpoint = checkpoints
            .last()
            .cloned()
            .ok_or_else(|| StorageError::not_found("checkpoint", source.to_string()))?;
        let CheckpointRoots::BlockShard(mut roots) = checkpoint.roots else {
            return Err(StorageError::corrupt("block checkpoint has native roots"));
        };
        commits.sort_by_key(|commit| (commit.commit_seq.raw(), commit.shard_id.raw()));
        for commit in commits
            .into_iter()
            .filter(|commit| commit.commit_seq.raw() > checkpoint.commit_seq.raw())
        {
            let shard = usize::try_from(commit.shard_id.raw())
                .map_err(|_| StorageError::invalid_argument("shard ID overflows usize"))?;
            let Some(root) = roots.get_mut(shard) else {
                return Err(StorageError::corrupt(
                    "shard commit is outside checkpoint roots",
                ));
            };
            *root = commit.new_root;
        }
        Ok(roots)
    }
}

impl MetadataTxnProfile {
    fn logical_conflict_for_manual(&mut self) {
        self.conflict_count = 1;
        self.total_nanos = 0;
    }
}
