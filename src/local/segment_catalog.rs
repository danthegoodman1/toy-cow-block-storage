/// Local segment lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SegmentLifecycleState {
    Reserved,
    Writing,
    DurablePendingMetadata,
    Referenced,
    Released,
    Freed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct CatalogEntry {
    intent: SegmentReservationIntent,
    reservation: SegmentReservation,
    state: SegmentLifecycleState,
    receipt: Option<SegmentWriteReceipt>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct CatalogInner {
    next_segment_id: u128,
    entries: BTreeMap<SegmentId, CatalogEntry>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CatalogLifecycleCounts {
    reserved: usize,
    writing: usize,
    durable_pending: usize,
    referenced: usize,
    released: usize,
    freed: usize,
}

/// In-memory implementation of `LocalSegmentCatalog`.
#[derive(Debug)]
pub struct InMemoryLocalSegmentCatalog {
    config: LocalStoreConfig,
    inner: Mutex<CatalogInner>,
}

impl InMemoryLocalSegmentCatalog {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(CatalogInner {
                next_segment_id: 1,
                entries: BTreeMap::new(),
            }),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: CatalogInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
        })
    }

    fn state_inner(&self) -> Result<CatalogInner> {
        Ok(lock(&self.inner)?.clone())
    }

    /// Copy of the catalog restricted to the requested segment ids.
    ///
    /// Hot per-write paths use this instead of `state_inner` so snapshot cost
    /// scales with the request, not with every segment the node has ever
    /// cataloged. Returns `None` when none of the ids live on this node.
    fn selected_state_inner(
        &self,
        segment_ids: &BTreeSet<SegmentId>,
    ) -> Result<Option<CatalogInner>> {
        let inner = lock(&self.inner)?;
        let mut entries = BTreeMap::new();
        for segment_id in segment_ids {
            if let Some(entry) = inner.entries.get(segment_id) {
                entries.insert(*segment_id, entry.clone());
            }
        }
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some(CatalogInner {
            next_segment_id: inner.next_segment_id,
            entries,
        }))
    }

    fn reserve_segment_with_id(
        &self,
        segment_id: SegmentId,
        intent: SegmentReservationIntent,
    ) -> Result<SegmentReservation> {
        self.reserve_segment_with_id_profiled(segment_id, intent)
            .map(|(reservation, _)| reservation)
    }

    fn reserve_segment_with_id_profiled(
        &self,
        segment_id: SegmentId,
        intent: SegmentReservationIntent,
    ) -> Result<(SegmentReservation, LocalCatalogOpProfile)> {
        let total_started = Instant::now();
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        if inner.entries.contains_key(&segment_id) {
            return Err(StorageError::conflict("segment ID already exists"));
        }
        if segment_id.raw() >= inner.next_segment_id {
            inner.next_segment_id = segment_id
                .raw()
                .checked_add(1)
                .ok_or_else(|| StorageError::conflict("segment id overflow"))?;
        }
        let reservation = SegmentReservation {
            segment_id,
            bytes: intent.bytes,
        };
        inner.entries.insert(
            segment_id,
            CatalogEntry {
                intent,
                reservation: reservation.clone(),
                state: SegmentLifecycleState::Reserved,
                receipt: None,
            },
        );
        Ok((
            reservation,
            LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            },
        ))
    }

    pub fn contains_segment(&self, segment_id: SegmentId) -> Result<bool> {
        self.contains_segment_profiled(segment_id)
            .map(|(contains, _)| contains)
    }

    fn contains_segment_profiled(
        &self,
        segment_id: SegmentId,
    ) -> Result<(bool, LocalCatalogOpProfile)> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        Ok((
            inner.entries.contains_key(&segment_id),
            LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            },
        ))
    }

    pub fn state(&self, segment_id: SegmentId) -> Result<SegmentLifecycleState> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.state)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn commit_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaCommit> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .as_ref()
            .map(SegmentWriteReceipt::replica_commit)
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn receipt_for_segment(&self, segment_id: SegmentId) -> Result<SegmentWriteReceipt> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        entry
            .receipt
            .clone()
            .ok_or_else(|| StorageError::unavailable("segment has no durable receipt"))
    }

    pub fn intent_for_segment(&self, segment_id: SegmentId) -> Result<SegmentReservationIntent> {
        let inner = lock(&self.inner)?;
        inner
            .entries
            .get(&segment_id)
            .map(|entry| entry.intent.clone())
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn entries(&self) -> Result<Vec<(SegmentId, SegmentLifecycleState, WriteIntentId)>> {
        let inner = lock(&self.inner)?;
        Ok(inner
            .entries
            .iter()
            .map(|(segment_id, entry)| (*segment_id, entry.state, entry.intent.write_intent))
            .collect())
    }

    fn lifecycle_counts(&self) -> Result<CatalogLifecycleCounts> {
        let inner = lock(&self.inner)?;
        let mut counts = CatalogLifecycleCounts::default();
        for entry in inner.entries.values() {
            match entry.state {
                SegmentLifecycleState::Reserved => counts.reserved += 1,
                SegmentLifecycleState::Writing => counts.writing += 1,
                SegmentLifecycleState::DurablePendingMetadata => counts.durable_pending += 1,
                SegmentLifecycleState::Referenced => counts.referenced += 1,
                SegmentLifecycleState::Released => counts.released += 1,
                SegmentLifecycleState::Freed => counts.freed += 1,
            }
        }
        Ok(counts)
    }

    fn begin_write_profiled(
        &self,
        reservation: &SegmentReservation,
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != *reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Writing;
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            SegmentLifecycleState::Writing => Ok(LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            }),
            _ => Err(StorageError::conflict(
                "segment write can only begin from Reserved state",
            )),
        }
    }

    fn commit_segment_profiled(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let entry = Self::get_entry_mut(&mut inner, reservation.segment_id)?;
        if entry.reservation != reservation {
            return Err(StorageError::conflict(
                "reservation does not match catalog entry",
            ));
        }
        if receipt.segment_id != reservation.segment_id
            || receipt.descriptor.segment_id != reservation.segment_id
            || receipt.placement.segment_id != reservation.segment_id
        {
            return Err(StorageError::invalid_argument(
                "segment receipt IDs must match reservation",
            ));
        }
        if receipt.storage_node != self.config.storage_node
            || receipt.placement.storage_node != self.config.storage_node
        {
            return Err(StorageError::invalid_argument(
                "segment receipt storage node does not match local catalog",
            ));
        }
        if receipt.bytes != reservation.bytes
            || receipt.descriptor.bytes != reservation.bytes
            || receipt.placement.bytes != reservation.bytes
        {
            return Err(StorageError::invalid_argument(
                "segment receipt bytes must match reservation",
            ));
        }

        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.receipt = Some(receipt);
                entry.state = SegmentLifecycleState::DurablePendingMetadata;
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            SegmentLifecycleState::DurablePendingMetadata
                if entry.receipt.as_ref() == Some(&receipt) =>
            {
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            _ => Err(StorageError::conflict(
                "segment receipt requires Writing state",
            )),
        }
    }

    fn mark_segment_referenced_profiled(
        &self,
        segment_id: SegmentId,
    ) -> Result<LocalCatalogOpProfile> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Referenced;
                Ok(LocalCatalogOpProfile {
                    total_nanos: duration_nanos_u64(total_started.elapsed()),
                    lock_wait_nanos,
                })
            }
            SegmentLifecycleState::Referenced => Ok(LocalCatalogOpProfile {
                total_nanos: duration_nanos_u64(total_started.elapsed()),
                lock_wait_nanos,
            }),
            _ => Err(StorageError::conflict(
                "segment can be referenced only from DurablePendingMetadata state",
            )),
        }
    }

    fn get_entry_mut(inner: &mut CatalogInner, segment_id: SegmentId) -> Result<&mut CatalogEntry> {
        inner
            .entries
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }
}

impl LocalSegmentCatalog for InMemoryLocalSegmentCatalog {
    fn reserve_segment(&self, intent: SegmentReservationIntent) -> Result<SegmentReservation> {
        if intent.bytes == 0 {
            return Err(StorageError::invalid_argument(
                "segment reservation must contain bytes",
            ));
        }

        let segment_id = {
            let inner = lock(&self.inner)?;
            SegmentId::from_raw(inner.next_segment_id)
        };
        self.reserve_segment_with_id(segment_id, intent)
    }

    fn begin_write(&self, reservation: &SegmentReservation) -> Result<()> {
        self.begin_write_profiled(reservation).map(|_| ())
    }

    fn commit_segment(
        &self,
        reservation: SegmentReservation,
        receipt: SegmentWriteReceipt,
    ) -> Result<()> {
        self.commit_segment_profiled(reservation, receipt)
            .map(|_| ())
    }

    fn mark_segment_referenced(&self, segment_id: SegmentId) -> Result<()> {
        self.mark_segment_referenced_profiled(segment_id)
            .map(|_| ())
    }

    fn release_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Referenced => {
                entry.state = SegmentLifecycleState::Released;
                Ok(())
            }
            SegmentLifecycleState::Released => Ok(()),
            _ => Err(StorageError::conflict(
                "segment can be released only from Referenced state",
            )),
        }
    }

    fn expire_reservation(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Reserved => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Reserved segments can expire as reservations",
            )),
        }
    }

    fn fail_write(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Writing => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Writing segments can fail as writes",
            )),
        }
    }

    fn free_orphan_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only DurablePendingMetadata orphan segments can be freed",
            )),
        }
    }

    fn locate_segment(&self, segment_id: SegmentId) -> Result<SegmentReplicaPlacement> {
        let inner = lock(&self.inner)?;
        let entry = inner
            .entries
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        match entry.state {
            SegmentLifecycleState::DurablePendingMetadata
            | SegmentLifecycleState::Referenced
            | SegmentLifecycleState::Released => entry
                .receipt
                .as_ref()
                .map(|receipt| receipt.placement.clone())
                .ok_or_else(|| StorageError::corrupt("committed segment missing placement")),
            SegmentLifecycleState::Freed => {
                Err(StorageError::not_found("segment", segment_id.to_string()))
            }
            SegmentLifecycleState::Reserved | SegmentLifecycleState::Writing => Err(
                StorageError::unavailable("segment placement is not committed yet"),
            ),
        }
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        let entry = Self::get_entry_mut(&mut inner, segment_id)?;
        match entry.state {
            SegmentLifecycleState::Released => {
                entry.state = SegmentLifecycleState::Freed;
                Ok(())
            }
            SegmentLifecycleState::Freed => Ok(()),
            _ => Err(StorageError::conflict(
                "only Released segments are safe to delete",
            )),
        }
    }
}
