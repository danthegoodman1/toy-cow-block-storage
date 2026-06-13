/// Shared immutable payload bytes with a window.
///
/// Chunked staging clones the Arc and narrows the window instead of copying
/// payload bytes per chunk, so one collapsed write's buffer backs every chunk
/// segment along the write path, in the in-memory store, and in the data-log
/// writer.
#[derive(Debug, Clone)]
pub(super) struct SharedSegmentPayload {
    buf: Arc<Vec<u8>>,
    start: usize,
    end: usize,
}

impl SharedSegmentPayload {
    pub(super) fn from_vec(buf: Vec<u8>) -> Self {
        let end = buf.len();
        Self {
            buf: Arc::new(buf),
            start: 0,
            end,
        }
    }

    /// Narrow to a window given relative to this payload's window.
    pub(super) fn window(&self, start: usize, end: usize) -> Result<Self> {
        let absolute_start = self
            .start
            .checked_add(start)
            .ok_or_else(|| StorageError::invalid_argument("payload window start overflows"))?;
        let absolute_end = self
            .start
            .checked_add(end)
            .ok_or_else(|| StorageError::invalid_argument("payload window end overflows"))?;
        if start > end || absolute_end > self.end {
            return Err(StorageError::invalid_argument(
                "payload window exceeds payload bytes",
            ));
        }
        Ok(Self {
            buf: Arc::clone(&self.buf),
            start: absolute_start,
            end: absolute_end,
        })
    }

    pub(super) fn as_slice(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }

    pub(super) fn len(&self) -> usize {
        self.end - self.start
    }

    pub(super) fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

impl PartialEq for SharedSegmentPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for SharedSegmentPayload {}

impl serde::Serialize for SharedSegmentPayload {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(self.as_slice())
    }
}

impl<'de> serde::Deserialize<'de> for SharedSegmentPayload {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Ok(Self::from_vec(Vec::<u8>::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct SegmentRecord {
    bytes: SharedSegmentPayload,
    synced: bool,
    commit: SegmentReplicaCommit,
}

#[derive(Debug)]
pub(super) struct DurableSegmentPayload {
    segment_id: SegmentId,
    storage_node: StorageNodeId,
    integrity: SegmentPayloadIntegrity,
    bytes: SharedSegmentPayload,
}

#[derive(Debug)]
pub(super) struct DurableAppendRunChunkPayload<'a> {
    run_id: AppendRunId,
    storage_node: StorageNodeId,
    stream_id: AppendStreamId,
    writer_epoch: WriterEpoch,
    keyspace_id: KeyspaceId,
    file_id: FileId,
    file_offset_start: u64,
    payload_integrity: PayloadIntegrity,
    chunks: Vec<&'a [u8]>,
    background_sync_step_bytes: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct SegmentStoreInner {
    next_offset: u64,
    segments: BTreeMap<SegmentId, SegmentRecord>,
}

/// In-memory implementation of `SegmentStore`.
#[derive(Debug)]
pub struct InMemorySegmentStore {
    config: LocalStoreConfig,
    inner: Mutex<SegmentStoreInner>,
}

impl InMemorySegmentStore {
    pub fn new(config: LocalStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(SegmentStoreInner {
                next_offset: 0,
                segments: BTreeMap::new(),
            }),
        })
    }

    fn from_inner(config: LocalStoreConfig, inner: SegmentStoreInner) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(inner),
        })
    }

    fn state_inner_for_persist(
        &self,
        previous_segments: &BTreeSet<SegmentId>,
        storage_node: StorageNodeId,
    ) -> Result<(
        SegmentStoreInner,
        BTreeSet<SegmentId>,
        Vec<DurableSegmentPayload>,
    )> {
        let inner = lock(&self.inner)?;
        let mut current_segments = BTreeSet::new();
        let mut new_segments = Vec::new();
        for (segment_id, record) in &inner.segments {
            current_segments.insert(*segment_id);
            if !previous_segments.contains(segment_id) {
                new_segments.push(DurableSegmentPayload {
                    segment_id: *segment_id,
                    storage_node,
                    integrity: record.commit.descriptor.integrity,
                    bytes: record.bytes.clone(),
                });
            }
        }
        Ok((
            SegmentStoreInner {
                next_offset: inner.next_offset,
                segments: BTreeMap::new(),
            },
            current_segments,
            new_segments,
        ))
    }

    fn payload_for_segment(
        &self,
        storage_node: StorageNodeId,
        segment_id: SegmentId,
    ) -> Result<DurableSegmentPayload> {
        let inner = lock(&self.inner)?;
        let record = inner
            .segments
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        Ok(DurableSegmentPayload {
            segment_id,
            storage_node,
            integrity: record.commit.descriptor.integrity,
            bytes: record.bytes.clone(),
        })
    }

    fn payloads_for_segments(
        &self,
        storage_node: StorageNodeId,
        selected: &BTreeSet<SegmentId>,
        previous_segments: &BTreeSet<SegmentId>,
    ) -> Result<(u64, Vec<DurableSegmentPayload>)> {
        let inner = lock(&self.inner)?;
        let mut payloads = Vec::new();
        for segment_id in selected
            .iter()
            .filter(|segment_id| !previous_segments.contains(segment_id))
        {
            let record = inner
                .segments
                .get(segment_id)
                .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
            payloads.push(DurableSegmentPayload {
                segment_id: *segment_id,
                storage_node,
                integrity: record.commit.descriptor.integrity,
                bytes: record.bytes.clone(),
            });
        }
        Ok((inner.next_offset, payloads))
    }

    fn verify_segment_payload_for_read(
        &self,
        segment_id: SegmentId,
        verification: ReadVerification,
    ) -> Result<()> {
        self.verify_segment_payload_for_read_profiled(segment_id, verification)
            .map(|_| ())
    }

    fn verify_segment_payload_for_read_profiled(
        &self,
        segment_id: SegmentId,
        verification: ReadVerification,
    ) -> Result<LocalSegmentStoreVerifyProfile> {
        let total_started = Instant::now();
        let mut profile = LocalSegmentStoreVerifyProfile::default();
        if matches!(verification, ReadVerification::Skip) {
            profile.total_nanos = duration_nanos_u64(total_started.elapsed());
            return Ok(profile);
        }
        let lock_started = Instant::now();
        let inner = lock(&self.inner)?;
        profile.lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let record = inner
            .segments
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        let integrity = record.commit.descriptor.integrity;
        let bytes = record.bytes.clone();
        drop(inner);
        match integrity {
            SegmentPayloadIntegrity::Unchecked => {
                if matches!(verification, ReadVerification::RequireVerified) {
                    Err(StorageError::conflict(
                        "read requires verified payload but segment is unchecked",
                    ))
                } else {
                    profile.total_nanos = duration_nanos_u64(total_started.elapsed());
                    Ok(profile)
                }
            }
            integrity @ SegmentPayloadIntegrity::Crc32c(_) => {
                let checksum_started = Instant::now();
                verify_segment_payload_integrity(integrity, bytes.as_slice())?;
                profile.checksum_nanos = duration_nanos_u64(checksum_started.elapsed());
                profile.total_nanos = duration_nanos_u64(total_started.elapsed());
                Ok(profile)
            }
        }
    }

    fn next_offset(&self) -> Result<u64> {
        Ok(lock(&self.inner)?.next_offset)
    }

    fn segment_ids(&self) -> Result<BTreeSet<SegmentId>> {
        Ok(lock(&self.inner)?.segments.keys().copied().collect())
    }

    pub fn is_synced(&self, segment_id: SegmentId) -> Result<bool> {
        let inner = lock(&self.inner)?;
        inner
            .segments
            .get(&segment_id)
            .map(|record| record.synced)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))
    }

    pub fn contains_segment(&self, segment_id: SegmentId) -> Result<bool> {
        Ok(lock(&self.inner)?.segments.contains_key(&segment_id))
    }

    fn write_segment_owned(
        &self,
        reservation: &SegmentReservation,
        bytes: Vec<u8>,
        payload_integrity: PayloadIntegrity,
    ) -> Result<SegmentReplicaCommit> {
        self.write_segment_shared_profiled(
            reservation,
            SharedSegmentPayload::from_vec(bytes),
            payload_integrity,
        )
        .map(|(commit, _)| commit)
    }

    fn write_segment_shared_profiled(
        &self,
        reservation: &SegmentReservation,
        bytes: SharedSegmentPayload,
        payload_integrity: PayloadIntegrity,
    ) -> Result<(SegmentReplicaCommit, LocalSegmentStoreWriteProfile)> {
        let total_started = Instant::now();
        let mut profile = LocalSegmentStoreWriteProfile::default();
        self.config.validate()?;

        if bytes.is_empty() {
            return Err(StorageError::invalid_argument(
                "segment write must contain bytes",
            ));
        }

        let bytes_len = u64::try_from(bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment write length overflows u64"))?;
        if reservation.bytes != bytes_len {
            return Err(StorageError::invalid_argument(
                "reservation byte count does not match write length",
            ));
        }

        if bytes_len % u64::from(self.config.block_size) != 0 {
            return Err(StorageError::invalid_argument(
                "segment write length must be block aligned",
            ));
        }

        let checksum_started = Instant::now();
        let integrity = segment_payload_integrity(payload_integrity, bytes.as_slice());
        profile.checksum_integrity_nanos = duration_nanos_u64(checksum_started.elapsed());

        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        profile.lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        if let Some(existing) = inner.segments.get(&reservation.segment_id) {
            if existing.bytes == bytes {
                profile.total_nanos = duration_nanos_u64(total_started.elapsed());
                return Ok((existing.commit.clone(), profile));
            }
            return Err(StorageError::conflict(
                "segment ID already exists with different bytes",
            ));
        }

        let offset = inner.next_offset;
        inner.next_offset = inner
            .next_offset
            .checked_add(reservation.bytes)
            .ok_or_else(|| StorageError::conflict("local segment offset overflow"))?;
        let blocks = reservation.bytes / u64::from(self.config.block_size);
        let commit = SegmentReplicaCommit {
            descriptor: SegmentDescriptor {
                segment_id: reservation.segment_id,
                blocks: BlockCount::from_raw(blocks),
                bytes: reservation.bytes,
                integrity,
            },
            placement: SegmentReplicaPlacement {
                segment_id: reservation.segment_id,
                storage_node: self.config.storage_node,
                offset,
                bytes: reservation.bytes,
            },
        };
        let insert_started = Instant::now();
        inner.segments.insert(
            reservation.segment_id,
            SegmentRecord {
                bytes,
                synced: false,
                commit: commit.clone(),
            },
        );
        profile.insert_nanos = duration_nanos_u64(insert_started.elapsed());
        profile.total_nanos = duration_nanos_u64(total_started.elapsed());
        Ok((commit, profile))
    }

    fn sync_segment_profiled(&self, segment_id: SegmentId) -> Result<LocalSegmentStoreSyncProfile> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let mut inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let record = inner
            .segments
            .get_mut(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        record.synced = true;
        Ok(LocalSegmentStoreSyncProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            lock_wait_nanos,
        })
    }
}

impl InMemorySegmentStore {
    fn read_segment_profiled(
        &self,
        segment_id: SegmentId,
        range: ByteRange,
        buf: &mut [u8],
    ) -> Result<LocalSegmentStoreReadProfile> {
        let total_started = Instant::now();
        let lock_started = Instant::now();
        let inner = lock(&self.inner)?;
        let lock_wait_nanos = duration_nanos_u64(lock_started.elapsed());
        let record = inner
            .segments
            .get(&segment_id)
            .ok_or_else(|| StorageError::not_found("segment", segment_id.to_string()))?;
        if !record.synced {
            return Err(StorageError::unavailable("segment is not synced"));
        }
        let end = range.end_exclusive()?;
        let bytes = record.bytes.clone();
        let record_len = u64::try_from(bytes.len())
            .map_err(|_| StorageError::invalid_argument("segment byte length overflows u64"))?;
        if end > record_len {
            return Err(StorageError::invalid_argument(
                "segment read extends past end of segment",
            ));
        }
        let buf_len = u64::try_from(buf.len())
            .map_err(|_| StorageError::invalid_argument("read buffer length overflows u64"))?;
        if buf_len != range.len {
            return Err(StorageError::invalid_argument(
                "read buffer length must match range length",
            ));
        }

        let start = usize::try_from(range.offset)
            .map_err(|_| StorageError::invalid_argument("segment read offset overflows usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| StorageError::invalid_argument("segment read end overflows usize"))?;
        drop(inner);
        let source = bytes
            .as_slice()
            .get(start..end)
            .ok_or_else(|| StorageError::corrupt("segment read range exceeds segment bytes"))?;
        let copy_started = Instant::now();
        buf.copy_from_slice(source);
        Ok(LocalSegmentStoreReadProfile {
            total_nanos: duration_nanos_u64(total_started.elapsed()),
            lock_wait_nanos,
            copy_nanos: duration_nanos_u64(copy_started.elapsed()),
        })
    }
}

impl SegmentStore for InMemorySegmentStore {
    fn write_segment(
        &self,
        reservation: &SegmentReservation,
        bytes: &[u8],
    ) -> Result<SegmentReplicaCommit> {
        self.write_segment_owned(reservation, bytes.to_vec(), PayloadIntegrity::Verified)
    }

    fn read_segment(&self, segment_id: SegmentId, range: ByteRange, buf: &mut [u8]) -> Result<()> {
        self.read_segment_profiled(segment_id, range, buf).map(|_| ())
    }

    fn sync_segment(&self, segment_id: SegmentId) -> Result<()> {
        self.sync_segment_profiled(segment_id).map(|_| ())
    }

    fn delete_segment(&self, segment_id: SegmentId) -> Result<()> {
        lock(&self.inner)?.segments.remove(&segment_id);
        Ok(())
    }
}
