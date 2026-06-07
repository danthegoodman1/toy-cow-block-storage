pub(super) fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))
}

pub(super) fn wait_on_cvar<'a, T>(cvar: &Condvar, guard: MutexGuard<'a, T>) -> Result<MutexGuard<'a, T>> {
    cvar.wait(guard)
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))
}

pub(super) fn wait_timeout_on_cvar<'a, T>(
    cvar: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> Result<(MutexGuard<'a, T>, bool)> {
    let (guard, timeout) = cvar
        .wait_timeout(guard, timeout)
        .map_err(|_| StorageError::unavailable("local provider lock poisoned"))?;
    Ok((guard, timeout.timed_out()))
}

pub(super) fn server_lock_stripes() -> Vec<Mutex<()>> {
    (0..SERVER_LOCK_STRIPES).map(|_| Mutex::new(())).collect()
}

pub(super) fn stripe_for_raw(raw: u128) -> usize {
    (raw % SERVER_LOCK_STRIPES as u128) as usize
}

pub(super) fn block_request_stripe(request: &BlockRequest) -> usize {
    request
        .target_device_id()
        .map(|device_id| stripe_for_raw(device_id.raw()))
        .unwrap_or(0)
}

pub(super) fn native_request_stripe(request: &NativeRequest) -> usize {
    match (request.target_keyspace_id(), request.target_file_id()) {
        (Some(keyspace_id), Some(file_id)) => {
            stripe_for_raw(keyspace_id.raw().wrapping_mul(1_099_511_628_211) ^ file_id.raw())
        }
        (Some(keyspace_id), None) => stripe_for_raw(keyspace_id.raw()),
        (None, _) => 0,
    }
}

pub(super) fn fs_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("filesystem operation failed: {error}"))
}

pub(super) fn network_io_error(error: std::io::Error) -> StorageError {
    StorageError::unavailable(format!("network I/O failed: {error}"))
}

pub(super) fn serde_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::corrupt(format!("binary envelope codec failed: {error}"))
}

pub(super) fn sqlite_error(error: rusqlite::Error) -> StorageError {
    StorageError::unavailable(format!("sqlite operation failed: {error}"))
}

pub(super) fn durable_codec_error(reason: impl Into<String>) -> StorageError {
    StorageError::corrupt(format!("durable codec failed: {}", reason.into()))
}
pub(super) fn validate_durable_segment_bytes(
    segment_id: SegmentId,
    record: &DurableSegmentRecord,
    bytes: &[u8],
) -> Result<()> {
    let bytes_len = u64::try_from(bytes.len())
        .map_err(|_| StorageError::corrupt("durable segment length overflows u64"))?;
    if bytes_len != record.commit.descriptor.bytes {
        return Err(StorageError::corrupt(
            "durable segment length does not match journal commit",
        ));
    }
    if record.commit.descriptor.segment_id != segment_id {
        return Err(StorageError::corrupt(
            "durable segment record key disagrees with descriptor",
        ));
    }
    verify_segment_payload_integrity(record.commit.descriptor.integrity, bytes)?;
    Ok(())
}

pub(super) fn next_request_id(next: &Mutex<u128>) -> Result<RequestId> {
    let mut next = lock(next)?;
    let request_id = RequestId::from_raw(*next);
    *next = next
        .checked_add(1)
        .ok_or_else(|| StorageError::conflict("request id overflow"))?;
    Ok(request_id)
}

pub(super) fn data_log_checksum(bytes: &[u8]) -> u64 {
    u64::from(crc32c::crc32c(bytes))
}

pub(super) fn data_log_checksum_chunks(chunks: &[&[u8]]) -> u64 {
    let mut checksum = 0_u32;
    for chunk in chunks {
        checksum = crc32c::crc32c_append(checksum, chunk);
    }
    u64::from(checksum)
}

pub(super) fn segment_payload_integrity(mode: PayloadIntegrity, bytes: &[u8]) -> SegmentPayloadIntegrity {
    match mode {
        PayloadIntegrity::Verified => SegmentPayloadIntegrity::Crc32c(data_log_checksum(bytes)),
        PayloadIntegrity::Unchecked => SegmentPayloadIntegrity::Unchecked,
    }
}

pub(super) fn segment_payload_integrity_chunks(
    mode: PayloadIntegrity,
    chunks: &[&[u8]],
) -> SegmentPayloadIntegrity {
    match mode {
        PayloadIntegrity::Verified => {
            SegmentPayloadIntegrity::Crc32c(data_log_checksum_chunks(chunks))
        }
        PayloadIntegrity::Unchecked => SegmentPayloadIntegrity::Unchecked,
    }
}

pub(super) fn combine_segment_payload_integrity(
    left: SegmentPayloadIntegrity,
    right: SegmentPayloadIntegrity,
    right_len: u64,
) -> Result<SegmentPayloadIntegrity> {
    match (left, right) {
        (SegmentPayloadIntegrity::Unchecked, _) | (_, SegmentPayloadIntegrity::Unchecked) => {
            Ok(SegmentPayloadIntegrity::Unchecked)
        }
        (SegmentPayloadIntegrity::Crc32c(left), SegmentPayloadIntegrity::Crc32c(right)) => {
            let right_len = usize::try_from(right_len).map_err(|_| {
                StorageError::invalid_argument("checksum combine length overflows usize")
            })?;
            Ok(SegmentPayloadIntegrity::Crc32c(u64::from(
                crc32c::crc32c_combine(left as u32, right as u32, right_len),
            )))
        }
    }
}

pub(super) fn verify_segment_payload_integrity(
    integrity: SegmentPayloadIntegrity,
    bytes: &[u8],
) -> Result<()> {
    match integrity {
        SegmentPayloadIntegrity::Crc32c(expected) if data_log_checksum(bytes) != expected => {
            Err(StorageError::corrupt("segment payload checksum mismatch"))
        }
        SegmentPayloadIntegrity::Crc32c(_) | SegmentPayloadIntegrity::Unchecked => Ok(()),
    }
}

pub(super) fn segment_payload_integrity_key(integrity: SegmentPayloadIntegrity) -> String {
    match integrity {
        SegmentPayloadIntegrity::Crc32c(checksum) => format!("crc32c:{}", u64_key(checksum)),
        SegmentPayloadIntegrity::Unchecked => "unchecked".to_string(),
    }
}

pub(super) fn parse_segment_payload_integrity_key(value: &str) -> rusqlite::Result<SegmentPayloadIntegrity> {
    if value == "unchecked" {
        return Ok(SegmentPayloadIntegrity::Unchecked);
    }
    let Some(checksum) = value.strip_prefix("crc32c:") else {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("invalid payload integrity {value}").into(),
        ));
    };
    parse_u64_key(checksum).map(SegmentPayloadIntegrity::Crc32c)
}
