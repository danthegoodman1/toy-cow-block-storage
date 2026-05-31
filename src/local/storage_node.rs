/// In-process storage-node role.
#[derive(Debug, Clone)]
pub(super) struct LocalStorageNode {
    storage_node: StorageNodeId,
    segment_store: Arc<InMemorySegmentStore>,
    segment_catalog: Arc<InMemoryLocalSegmentCatalog>,
    authority: Arc<LocalGrantReceiptAuthority>,
    observability: Arc<Observability>,
}

impl LocalStorageNode {
    fn write_segment_profiled(
        &self,
        grant: WriteGrant,
        bytes: Vec<u8>,
    ) -> Result<(SegmentWriteReceipt, LocalSegmentWriteProfile)> {
        let mut profile = LocalSegmentWriteProfile::default();
        let bytes_len = u64::try_from(bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment write length overflows u64"))?;
        let grant_started = Instant::now();
        let grant_hash = match self.authority.verify_write_grant_and_hash(
            &grant,
            self.storage_node,
            grant.segment_id,
            bytes_len,
        ) {
            Ok(hash) => {
                profile.grant_verify_nanos = duration_nanos_u64(grant_started.elapsed());
                hash
            }
            Err(error) => {
                self.observability.record_with_update(
                    StorageEventKind::GrantRejected,
                    Some(self.storage_node),
                    Some(grant.segment_id),
                    None,
                    Some("scope"),
                    |counters| {
                        counters.grant_rejections = counters.grant_rejections.saturating_add(1);
                    },
                );
                return Err(error);
            }
        };

        let (contains_segment, contains_profile) = self
            .segment_catalog
            .contains_segment_profiled(grant.segment_id)?;
        profile.catalog_duplicate_probe_nanos = contains_profile.total_nanos;
        profile.catalog_duplicate_probe_lock_wait_nanos = contains_profile.lock_wait_nanos;
        if contains_segment {
            let receipt = self.segment_catalog.receipt_for_segment(grant.segment_id)?;
            self.authority.verify_segment_receipt(&receipt)?;
            if receipt.grant_id == grant.grant_id
                && receipt.grant_hash == grant_hash
                && receipt.bytes == bytes_len
                && receipt.integrity == segment_payload_integrity(grant.payload_integrity, &bytes)
            {
                let existing_len = usize::try_from(bytes_len).map_err(|_| {
                    StorageError::invalid_argument("duplicate segment length overflows usize")
                })?;
                let mut existing = vec![0; existing_len];
                self.segment_store.read_segment(
                    grant.segment_id,
                    ByteRange::new(0, bytes_len),
                    &mut existing,
                )?;
                if existing == bytes {
                    self.observability.record_with_update(
                        StorageEventKind::StorageSegmentWriteRetried,
                        Some(self.storage_node),
                        Some(grant.segment_id),
                        None,
                        None,
                        |counters| {
                            counters.storage_segment_duplicate_writes =
                                counters.storage_segment_duplicate_writes.saturating_add(1);
                            counters.coordinator_write_idempotency_hits = counters
                                .coordinator_write_idempotency_hits
                                .saturating_add(1);
                        },
                    );
                    return Ok((receipt, profile));
                }
            }
            return Err(StorageError::conflict(
                "duplicate segment write conflicts with existing receipt",
            ));
        }

        let intent = SegmentReservationIntent {
            write_intent: grant.write_intent,
            owner: grant.owner,
            bytes: grant.max_bytes,
        };
        let (reservation, reserve_profile) = self
            .segment_catalog
            .reserve_segment_with_id_profiled(grant.segment_id, intent)?;
        profile.catalog_reserve_nanos = reserve_profile.total_nanos;
        profile.catalog_reserve_lock_wait_nanos = reserve_profile.lock_wait_nanos;

        let begin_profile = self.segment_catalog.begin_write_profiled(&reservation)?;
        profile.catalog_begin_nanos = begin_profile.total_nanos;
        profile.catalog_begin_lock_wait_nanos = begin_profile.lock_wait_nanos;

        let (commit, write_profile) = self.segment_store.write_segment_owned_profiled(
            &reservation,
            bytes,
            grant.payload_integrity,
        )?;
        profile.segment_store_write_nanos = write_profile.total_nanos;
        profile.segment_store_lock_wait_nanos = write_profile.lock_wait_nanos;
        profile.checksum_integrity_nanos = write_profile.checksum_integrity_nanos;
        profile.segment_store_insert_nanos = write_profile.insert_nanos;

        let sync_profile = self
            .segment_store
            .sync_segment_profiled(reservation.segment_id)?;
        profile.segment_sync_nanos = sync_profile.total_nanos;
        profile.segment_sync_lock_wait_nanos = sync_profile.lock_wait_nanos;

        let receipt_started = Instant::now();
        let receipt = self.authority.create_segment_receipt_after_verified_write(
            &grant,
            grant_hash,
            commit,
            LOCAL_STORAGE_NODE_INCARNATION,
        )?;
        profile.receipt_create_nanos = duration_nanos_u64(receipt_started.elapsed());

        let commit_profile = self
            .segment_catalog
            .commit_segment_profiled(reservation, receipt.clone())?;
        profile.catalog_commit_nanos = commit_profile.total_nanos;
        profile.catalog_commit_lock_wait_nanos = commit_profile.lock_wait_nanos;

        self.observability.record_with_update(
            StorageEventKind::StorageSegmentWritten,
            Some(self.storage_node),
            Some(receipt.segment_id),
            None,
            None,
            |counters| {
                counters.storage_segment_writes = counters.storage_segment_writes.saturating_add(1);
            },
        );
        Ok((receipt, profile))
    }

    fn mark_segment_referenced_profiled(
        &self,
        evidence: ReferenceEvidence,
    ) -> Result<LocalMarkReferencedProfile> {
        let mut profile = LocalMarkReferencedProfile::default();
        let segment_id = evidence.segment_id;
        let verify_started = Instant::now();
        self.authority
            .verify_reference_evidence(&evidence, segment_id, self.storage_node)?;
        profile.verify_nanos = duration_nanos_u64(verify_started.elapsed());

        let catalog_profile = self
            .segment_catalog
            .mark_segment_referenced_profiled(segment_id)?;
        profile.catalog_mark_nanos = catalog_profile.total_nanos;
        profile.catalog_mark_lock_wait_nanos = catalog_profile.lock_wait_nanos;

        self.observability.record_with_update(
            StorageEventKind::StorageSegmentReferenced,
            Some(self.storage_node),
            Some(segment_id),
            Some(evidence.metadata_commit),
            None,
            |counters| {
                counters.storage_segment_references =
                    counters.storage_segment_references.saturating_add(1);
            },
        );
        Ok(profile)
    }

    fn observe_maintenance(&self) -> Result<StorageNodeMaintenanceObservation> {
        let counts = self.segment_catalog.lifecycle_counts()?;
        Ok(StorageNodeMaintenanceObservation {
            storage_node: self.storage_node,
            reserved_segments: counts.reserved,
            writing_segments: counts.writing,
            durable_pending_segments: counts.durable_pending,
            referenced_segments: counts.referenced,
            released_segments: counts.released,
            freed_segments: counts.freed,
        })
    }

    fn run_maintenance_tick(&self) -> Result<StorageNodeMaintenanceReport> {
        let mut deleted_released_segments = Vec::new();
        let mut skipped_segments = Vec::new();
        for (segment_id, state, _) in self.segment_catalog.entries()? {
            if state == SegmentLifecycleState::Released {
                self.segment_catalog.delete_segment(segment_id)?;
                self.segment_store.delete_segment(segment_id)?;
                deleted_released_segments.push(segment_id);
            } else if matches!(
                state,
                SegmentLifecycleState::Reserved
                    | SegmentLifecycleState::Writing
                    | SegmentLifecycleState::DurablePendingMetadata
            ) {
                skipped_segments.push(segment_id);
            }
        }
        Ok(StorageNodeMaintenanceReport {
            storage_node: self.storage_node,
            deleted_released_segments,
            skipped_segments,
        })
    }

    fn run_custodian(
        &self,
        expired_write_intents: &BTreeSet<WriteIntentId>,
    ) -> Result<StorageNodeCustodianReport> {
        let mut report = StorageNodeCustodianReport {
            expired_reservations: Vec::new(),
            failed_writes: Vec::new(),
            orphan_segments: Vec::new(),
            deleted_released_segments: Vec::new(),
        };

        for (segment_id, state, write_intent) in self.segment_catalog.entries()? {
            match state {
                SegmentLifecycleState::Reserved
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.expire_reservation(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.expired_reservations.push(segment_id);
                }
                SegmentLifecycleState::Writing if expired_write_intents.contains(&write_intent) => {
                    self.segment_catalog.fail_write(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.failed_writes.push(segment_id);
                }
                SegmentLifecycleState::DurablePendingMetadata
                    if expired_write_intents.contains(&write_intent) =>
                {
                    self.segment_catalog.free_orphan_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.orphan_segments.push(segment_id);
                }
                SegmentLifecycleState::Released => {
                    self.segment_catalog.delete_segment(segment_id)?;
                    self.segment_store.delete_segment(segment_id)?;
                    report.deleted_released_segments.push(segment_id);
                }
                _ => {}
            }
        }

        Ok(report)
    }
}

impl StorageNodeTransport for LocalStorageNode {
    fn storage_node_id(&self) -> StorageNodeId {
        self.storage_node
    }

    fn send(&self, request: StorageNodeRequest) -> Result<StorageNodeResponse> {
        match request {
            StorageNodeRequest::WriteSegment { grant, bytes } => {
                let (receipt, _) = self.write_segment_profiled(grant, bytes)?;
                Ok(StorageNodeResponse::WriteSegment {
                    receipt: Box::new(receipt),
                })
            }
            StorageNodeRequest::ReadSegment { segment_id, range } => {
                let len = usize::try_from(range.len).map_err(|_| {
                    StorageError::invalid_argument("segment read byte length overflows usize")
                })?;
                let mut bytes = vec![0; len];
                self.segment_store
                    .read_segment(segment_id, range, &mut bytes)?;
                Ok(StorageNodeResponse::ReadSegment { bytes })
            }
            StorageNodeRequest::MarkReferenced { evidence } => {
                self.mark_segment_referenced_profiled(evidence)?;
                Ok(StorageNodeResponse::MarkReferenced)
            }
            StorageNodeRequest::Release { segment_id } => {
                self.segment_catalog.release_segment(segment_id)?;
                self.observability.record_with_update(
                    StorageEventKind::StorageSegmentReleased,
                    Some(self.storage_node),
                    Some(segment_id),
                    None,
                    None,
                    |counters| {
                        counters.storage_segment_releases =
                            counters.storage_segment_releases.saturating_add(1);
                    },
                );
                Ok(StorageNodeResponse::Released)
            }
            StorageNodeRequest::RunCustodian {
                expired_write_intents,
            } => Ok(StorageNodeResponse::Custodian(
                self.run_custodian(&expired_write_intents)?,
            )),
            StorageNodeRequest::ObserveMaintenance => Ok(StorageNodeResponse::MaintenanceObserved(
                self.observe_maintenance()?,
            )),
            StorageNodeRequest::RunMaintenanceTick => Ok(StorageNodeResponse::MaintenanceTicked(
                self.run_maintenance_tick()?,
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct StorageNodeRegistry {
    config: LocalStoreConfig,
    node_order: Arc<Vec<StorageNodeId>>,
    nodes: Arc<BTreeMap<StorageNodeId, LocalStorageNode>>,
    next_placement_index: Arc<Mutex<u64>>,
    next_segment_id: Arc<AtomicU64>,
}

impl StorageNodeRegistry {
    #[cfg(test)]
    fn new(config: LocalStoreConfig, storage_nodes: Vec<StorageNodeId>) -> Result<Self> {
        let observability = Arc::new(Observability::new(config.observability_event_capacity)?);
        Self::new_with_observability(config, storage_nodes, observability)
    }

    fn new_with_observability(
        config: LocalStoreConfig,
        storage_nodes: Vec<StorageNodeId>,
        observability: Arc<Observability>,
    ) -> Result<Self> {
        config.validate()?;
        let node_order = normalize_storage_nodes(config.storage_node, storage_nodes);
        let mut nodes = BTreeMap::new();
        let authority = Arc::new(LocalGrantReceiptAuthority);
        for node_id in &node_order {
            let node_config = config.for_storage_node(*node_id);
            nodes.insert(
                *node_id,
                LocalStorageNode {
                    storage_node: *node_id,
                    segment_store: Arc::new(InMemorySegmentStore::new(node_config)?),
                    segment_catalog: Arc::new(InMemoryLocalSegmentCatalog::new(node_config)?),
                    authority: Arc::clone(&authority),
                    observability: Arc::clone(&observability),
                },
            );
        }
        Ok(Self {
            config,
            node_order: Arc::new(node_order),
            nodes: Arc::new(nodes),
            next_placement_index: Arc::new(Mutex::new(0)),
            next_segment_id: Arc::new(AtomicU64::new(1)),
        })
    }

    fn from_inner_with_observability(
        config: LocalStoreConfig,
        inner: StorageNodeRegistryInner,
        observability: Arc<Observability>,
    ) -> Result<Self> {
        config.validate()?;
        if inner.node_order.is_empty() {
            return Err(StorageError::corrupt("storage node registry has no nodes"));
        }
        let mut seen = BTreeSet::new();
        for node_id in &inner.node_order {
            if !seen.insert(*node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry has duplicate node IDs",
                ));
            }
            if !inner.nodes.contains_key(node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry order references missing node",
                ));
            }
        }
        for node_id in inner.nodes.keys() {
            if !seen.contains(node_id) {
                return Err(StorageError::corrupt(
                    "storage node registry contains unordered node",
                ));
            }
        }

        let mut nodes = BTreeMap::new();
        let authority = Arc::new(LocalGrantReceiptAuthority);
        for node_id in &inner.node_order {
            let image = inner
                .nodes
                .get(node_id)
                .ok_or_else(|| StorageError::corrupt("storage node image missing"))?;
            let node_config = config.for_storage_node(*node_id);
            nodes.insert(
                *node_id,
                LocalStorageNode {
                    storage_node: *node_id,
                    segment_store: Arc::new(InMemorySegmentStore::from_inner(
                        node_config,
                        image.segment_store.clone(),
                    )?),
                    segment_catalog: Arc::new(InMemoryLocalSegmentCatalog::from_inner(
                        node_config,
                        image.segment_catalog.clone(),
                    )?),
                    authority: Arc::clone(&authority),
                    observability: Arc::clone(&observability),
                },
            );
        }

        Ok(Self {
            config,
            node_order: Arc::new(inner.node_order),
            nodes: Arc::new(nodes),
            next_placement_index: Arc::new(Mutex::new(inner.next_placement_index)),
            next_segment_id: Arc::new(AtomicU64::new(
                u64::try_from(inner.next_segment_id).map_err(|_| {
                    StorageError::corrupt("storage node segment id cursor exceeds local limit")
                })?,
            )),
        })
    }

    fn state_inner_for_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(
        StorageNodeRegistryInner,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let mut nodes = BTreeMap::new();
        let mut current_segments = BTreeSet::new();
        let mut new_segments = Vec::new();
        for (node_id, node) in self.nodes.iter() {
            let (segment_store, node_segments, mut node_new_segments) = node
                .segment_store
                .state_inner_for_persist(previous_segments, *node_id)?;
            current_segments.extend(node_segments);
            new_segments.append(&mut node_new_segments);
            nodes.insert(
                *node_id,
                StorageNodeInner {
                    segment_store,
                    segment_catalog: node.segment_catalog.state_inner()?,
                },
            );
        }
        Ok((
            StorageNodeRegistryInner {
                next_segment_id: u128::from(self.next_segment_id.load(Ordering::Relaxed)),
                next_placement_index: *lock(&self.next_placement_index)?,
                node_order: self.node_order.as_ref().clone(),
                nodes,
            },
            current_segments,
            new_segments,
        ))
    }

    fn state_for_segment_ids(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<(SelectedStorageNodeState, Vec<DurableSegmentPayload>)> {
        let mut nodes = BTreeMap::new();
        let mut payloads = Vec::new();
        if segment_ids.is_empty() {
            return Ok((nodes, payloads));
        }
        for (ordinal, node_id) in self.node_order.iter().enumerate() {
            let node = self.node(*node_id)?;
            let catalog = node.segment_catalog.state_inner()?;
            let selected: Vec<_> = segment_ids
                .iter()
                .copied()
                .filter(|segment_id| catalog.entries.contains_key(segment_id))
                .collect();
            if selected.is_empty() {
                continue;
            }
            let selected: BTreeSet<_> = selected.into_iter().collect();
            for segment_id in &selected {
                payloads.push(
                    node.segment_store
                        .payload_for_segment(*node_id, *segment_id)?,
                );
            }
            let mut catalog = catalog;
            catalog
                .entries
                .retain(|segment_id, _| selected.contains(segment_id));
            nodes.insert(
                *node_id,
                (
                    ordinal,
                    StorageNodeInner {
                        segment_store: SegmentStoreInner {
                            next_offset: node.segment_store.next_offset()?,
                            segments: BTreeMap::new(),
                        },
                        segment_catalog: catalog,
                    },
                ),
            );
        }
        let found: BTreeSet<_> = payloads.iter().map(|payload| payload.segment_id).collect();
        if &found != segment_ids {
            return Err(StorageError::corrupt(
                "stream flush references segments missing from storage-node catalogs",
            ));
        }
        Ok((nodes, payloads))
    }

    fn state_inner_for_segment_ids(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(
        StorageNodeRegistryInner,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let mut nodes = BTreeMap::new();
        let mut found = BTreeSet::new();
        let mut new_segments = Vec::new();
        for node_id in self.node_order.iter() {
            let node = self.node(*node_id)?;
            let mut catalog = node.segment_catalog.state_inner()?;
            let selected: BTreeSet<_> = segment_ids
                .iter()
                .copied()
                .filter(|segment_id| catalog.entries.contains_key(segment_id))
                .collect();
            found.extend(selected.iter().copied());
            let (next_offset, mut node_new_segments) =
                node.segment_store
                    .payloads_for_segments(*node_id, &selected, previous_segments)?;
            new_segments.append(&mut node_new_segments);
            catalog
                .entries
                .retain(|segment_id, _| selected.contains(segment_id));
            nodes.insert(
                *node_id,
                StorageNodeInner {
                    segment_store: SegmentStoreInner {
                        next_offset,
                        segments: BTreeMap::new(),
                    },
                    segment_catalog: catalog,
                },
            );
        }
        if &found != segment_ids {
            return Err(StorageError::corrupt(
                "durable export references segments missing from storage-node catalogs",
            ));
        }
        Ok((
            StorageNodeRegistryInner {
                next_segment_id: u128::from(self.next_segment_id.load(Ordering::Relaxed)),
                next_placement_index: *lock(&self.next_placement_index)?,
                node_order: self.node_order.as_ref().clone(),
                nodes,
            },
            found,
            new_segments,
        ))
    }

    fn selected_state_for_segment_ids(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<SelectedStorageNodeState> {
        let mut nodes = BTreeMap::new();
        if segment_ids.is_empty() {
            return Ok(nodes);
        }
        let mut found = BTreeSet::new();
        for (ordinal, node_id) in self.node_order.iter().enumerate() {
            let node = self.node(*node_id)?;
            let catalog = node.segment_catalog.state_inner()?;
            let selected: BTreeSet<_> = segment_ids
                .iter()
                .copied()
                .filter(|segment_id| catalog.entries.contains_key(segment_id))
                .collect();
            if selected.is_empty() {
                continue;
            }
            found.extend(selected.iter().copied());
            let mut catalog = catalog;
            catalog
                .entries
                .retain(|segment_id, _| selected.contains(segment_id));
            nodes.insert(
                *node_id,
                (
                    ordinal,
                    StorageNodeInner {
                        segment_store: SegmentStoreInner {
                            next_offset: node.segment_store.next_offset()?,
                            segments: BTreeMap::new(),
                        },
                        segment_catalog: catalog,
                    },
                ),
            );
        }
        if &found != segment_ids {
            return Err(StorageError::corrupt(
                "publish delta references segments missing from storage-node catalogs",
            ));
        }
        Ok(nodes)
    }

    fn verify_segment_payload_for_read(
        &self,
        segment_id: SegmentId,
        verification: ReadVerification,
    ) -> Result<()> {
        self.owner_node_for_segment(segment_id)?
            .segment_store
            .verify_segment_payload_for_read(segment_id, verification)
    }

    fn segment_ids(&self) -> Result<BTreeSet<SegmentId>> {
        let mut out = BTreeSet::new();
        for node in self.nodes.values() {
            out.extend(node.segment_store.segment_ids()?);
        }
        Ok(out)
    }

    fn primary_node(&self) -> Result<&LocalStorageNode> {
        self.node(self.config.storage_node)
    }

    fn node(&self, storage_node: StorageNodeId) -> Result<&LocalStorageNode> {
        self.nodes
            .get(&storage_node)
            .ok_or_else(|| StorageError::not_found("storage_node", storage_node.to_string()))
    }

    #[cfg(test)]
    fn node_ids(&self) -> Vec<StorageNodeId> {
        self.node_order.as_ref().clone()
    }

    fn owner_node_for_segment(&self, segment_id: SegmentId) -> Result<&LocalStorageNode> {
        let mut found = None;
        for (node_id, node) in self.nodes.iter() {
            if node.segment_catalog.contains_segment(segment_id)? {
                if found.is_some() {
                    return Err(StorageError::corrupt(
                        "segment appears in multiple storage-node catalogs",
                    ));
                }
                found = Some(*node_id);
            }
        }
        let node_id =
            found.ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        self.node(node_id)
    }

    #[cfg(test)]
    fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .commit_for_segment(segment_id)
    }

    fn receipt_for_segment(&self, segment_id: SegmentId) -> Result<SegmentWriteReceipt> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .receipt_for_segment(segment_id)
    }

    fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        self.owner_node_for_segment(segment_id)?
            .segment_catalog
            .state(segment_id)
    }

    fn mark_segment_referenced(
        &self,
        receipt: &SegmentWriteReceipt,
        commit_seq: CommitSeq,
        authority: &dyn GrantReceiptAuthority,
    ) -> Result<()> {
        let evidence = authority.create_reference_evidence(receipt, commit_seq)?;
        let response = self
            .transport_for_segment(receipt.segment_id)?
            .send(StorageNodeRequest::MarkReferenced { evidence })?;
        if response != StorageNodeResponse::MarkReferenced {
            return Err(StorageError::corrupt(
                "storage node returned unexpected mark-referenced response",
            ));
        }
        Ok(())
    }

    fn mark_segment_referenced_profiled(
        &self,
        receipt: &SegmentWriteReceipt,
        commit_seq: CommitSeq,
        authority: &dyn GrantReceiptAuthority,
    ) -> Result<LocalMarkReferencedProfile> {
        let evidence_started = Instant::now();
        let evidence = authority.create_reference_evidence(receipt, commit_seq)?;
        let mut profile = LocalMarkReferencedProfile {
            evidence_create_nanos: duration_nanos_u64(evidence_started.elapsed()),
            ..LocalMarkReferencedProfile::default()
        };

        let dispatch_started = Instant::now();
        let node = self.owner_node_for_segment(receipt.segment_id)?;
        profile.transport_dispatch_nanos = duration_nanos_u64(dispatch_started.elapsed());

        let node_profile = node.mark_segment_referenced_profiled(evidence)?;
        profile.verify_nanos = node_profile.verify_nanos;
        profile.catalog_mark_nanos = node_profile.catalog_mark_nanos;
        profile.catalog_mark_lock_wait_nanos = node_profile.catalog_mark_lock_wait_nanos;
        Ok(profile)
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let response = self
            .transport_for_segment(segment_id)?
            .send(StorageNodeRequest::Release { segment_id })?;
        if response != StorageNodeResponse::Released {
            return Err(StorageError::corrupt(
                "storage node returned unexpected release response",
            ));
        }
        Ok(())
    }

    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        let response = self
            .transport_for_segment(segment_id)?
            .send(StorageNodeRequest::ReadSegment { segment_id, range })?;
        let StorageNodeResponse::ReadSegment { bytes } = response else {
            return Err(StorageError::corrupt(
                "storage node returned unexpected read response",
            ));
        };
        if bytes.len() != buf.len() {
            return Err(StorageError::corrupt(
                "storage node read response length disagrees with buffer",
            ));
        }
        buf.copy_from_slice(&bytes);
        Ok(())
    }

    fn diagnostics_nodes(
        &self,
        maintenance: Option<&MaintenanceObservation>,
    ) -> Result<Vec<DiagnosticsNodeSnapshot>> {
        let mut nodes = BTreeMap::new();
        for node_id in self.node_order.iter() {
            let counts = self.node(*node_id)?.segment_catalog.lifecycle_counts()?;
            nodes.insert(
                *node_id,
                DiagnosticsNodeSnapshot {
                    storage_node: *node_id,
                    reserved_segments: usize_to_u64(counts.reserved),
                    writing_segments: usize_to_u64(counts.writing),
                    durable_pending_segments: usize_to_u64(counts.durable_pending),
                    pending_orphan_segments: usize_to_u64(counts.durable_pending),
                    referenced_segments: usize_to_u64(counts.referenced),
                    released_segments: usize_to_u64(counts.released),
                    freed_segments: usize_to_u64(counts.freed),
                    active_log_bytes: 0,
                    sealed_log_count: 0,
                    sealed_log_bytes: 0,
                    dirty_bytes: 0,
                    reclaimable_bytes: 0,
                },
            );
        }
        if let Some(maintenance) = maintenance {
            for observed in &maintenance.nodes {
                if let Some(node) = nodes.get_mut(&observed.storage_node) {
                    node.active_log_bytes = observed.active_log_bytes;
                    node.sealed_log_count = usize_to_u64(observed.sealed_log_count);
                    node.sealed_log_bytes = observed
                        .logs
                        .iter()
                        .map(|log| log.total_bytes)
                        .fold(0_u64, u64::saturating_add);
                    node.dirty_bytes = observed.dirty_bytes;
                    node.reclaimable_bytes = observed.reclaimable_bytes;
                }
            }
        }
        Ok(nodes.into_values().collect())
    }
}

impl PlacementPolicy for StorageNodeRegistry {
    fn choose_storage_node(&self, candidates: &[StorageNodeId]) -> Result<StorageNodeId> {
        if candidates.is_empty() {
            return Err(StorageError::invalid_argument(
                "placement policy requires at least one storage node",
            ));
        }
        let mut next = lock(&self.next_placement_index)?;
        let index = usize::try_from(*next % candidates.len() as u64)
            .map_err(|_| StorageError::invalid_argument("placement index overflows usize"))?;
        let node_id = candidates[index];
        self.node(node_id)?;
        *next = next
            .checked_add(1)
            .ok_or_else(|| StorageError::conflict("placement index overflow"))?;
        Ok(node_id)
    }
}

impl StorageNodeDirectory for StorageNodeRegistry {
    fn storage_node_ids(&self) -> Result<Vec<StorageNodeId>> {
        Ok(self.node_order.as_ref().clone())
    }

    fn allocate_segment_id(&self) -> Result<SegmentId> {
        self.next_segment_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |next| {
                next.checked_add(1)
            })
            .map(|raw| SegmentId::from_raw(u128::from(raw)))
            .map_err(|_| StorageError::conflict("segment id overflow"))
    }

    fn transport_for_node(
        &self,
        storage_node: StorageNodeId,
    ) -> Result<Arc<dyn StorageNodeTransport>> {
        Ok(Arc::new(self.node(storage_node)?.clone()))
    }

    fn transport_for_segment(
        &self,
        segment_id: SegmentId,
    ) -> Result<Arc<dyn StorageNodeTransport>> {
        Ok(Arc::new(self.owner_node_for_segment(segment_id)?.clone()))
    }
}
