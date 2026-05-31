pub(super) const NETWORK_WIRE_MAGIC: &[u8; 8] = b"TCOWWIRE";
pub(super) const NETWORK_WIRE_VERSION: u16 = 1;
pub(super) const NETWORK_BLOCK_REQUEST: u8 = 1;
pub(super) const NETWORK_BLOCK_RESPONSE: u8 = 2;
pub(super) const NETWORK_NATIVE_REQUEST: u8 = 3;
pub(super) const NETWORK_NATIVE_RESPONSE: u8 = 4;
pub(super) const DEFAULT_NETWORK_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn network_codec_error(reason: impl Into<String>) -> StorageError {
    StorageError::corrupt(format!("network wire codec failed: {}", reason.into()))
}

pub(super) fn encode_network_frame<T: DurableCodec>(kind: u8, value: &T) -> Result<Vec<u8>> {
    let mut out = DurableEncoder { bytes: Vec::new() };
    out.bytes.extend_from_slice(NETWORK_WIRE_MAGIC);
    out.put_u16(NETWORK_WIRE_VERSION);
    out.put_u8(kind);
    value.encode(&mut out)?;
    Ok(out.finish())
}

pub(super) fn decode_network_frame<T: DurableCodec>(expected_kind: u8, bytes: &[u8]) -> Result<T> {
    let mut input = DurableDecoder { bytes, offset: 0 };
    let magic = input.take(NETWORK_WIRE_MAGIC.len())?;
    if magic != NETWORK_WIRE_MAGIC {
        return Err(network_codec_error("bad frame magic"));
    }
    let version = input.u16()?;
    if version != NETWORK_WIRE_VERSION {
        return Err(network_codec_error("unsupported frame version"));
    }
    let kind = input.u8()?;
    if kind != expected_kind {
        return Err(network_codec_error("frame kind mismatch"));
    }
    let value = T::decode(&mut input)?;
    input
        .finish()
        .map_err(|_| network_codec_error("trailing bytes in frame"))?;
    Ok(value)
}

pub(super) fn encode_network_block_error(
    incarnation: ServerIncarnation,
    reason: impl Into<String>,
) -> Result<Vec<u8>> {
    encode_network_frame(
        NETWORK_BLOCK_RESPONSE,
        &RemoteWireReply::<BlockResponseEnvelope>::Err {
            incarnation,
            reason: reason.into(),
        },
    )
}

pub(super) fn encode_network_native_error(
    incarnation: ServerIncarnation,
    reason: impl Into<String>,
) -> Result<Vec<u8>> {
    encode_network_frame(
        NETWORK_NATIVE_RESPONSE,
        &RemoteWireReply::<NativeResponseEnvelope>::Err {
            incarnation,
            reason: reason.into(),
        },
    )
}

/// Network block endpoint using the crate-owned Phase 19 wire codec.
#[derive(Clone)]
pub struct NetworkBlockEndpoint {
    inner: RemoteBlockEndpoint,
}

impl NetworkBlockEndpoint {
    pub fn new(
        server: Arc<dyn BlockServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            inner: RemoteBlockEndpoint::new(server, incarnation, dedupe_capacity, mailbox_capacity),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.inner.incarnation()
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        self.inner.set_shutdown(shutdown)
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        self.inner.set_logical_time(logical_time)
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<BlockRequestEnvelope> =
            match decode_network_frame(NETWORK_BLOCK_REQUEST, request_bytes) {
                Ok(wire) => wire,
                Err(error) => {
                    return encode_network_block_error(self.inner.incarnation, error.to_string());
                }
            };
        if wire.incarnation != self.inner.incarnation {
            return encode_network_block_error(
                self.inner.incarnation,
                "stale block server incarnation",
            );
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.inner.state)?.logical_time.raw()
        {
            return encode_network_block_error(
                self.inner.incarnation,
                "block request deadline expired",
            );
        }
        let key = (
            self.inner.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.inner.state)?;
            if state.shutdown {
                return encode_network_block_error(
                    self.inner.incarnation,
                    "block endpoint is shut down",
                );
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return encode_network_block_error(
                    self.inner.incarnation,
                    "request ID and client epoch reused for a different network block request",
                );
            }
            if state.in_flight >= self.inner.mailbox_capacity {
                return encode_network_block_error(
                    self.inner.incarnation,
                    "block endpoint mailbox is full",
                );
            }
            state.in_flight += 1;
        }

        let response = self.inner.server.handle(wire.envelope);
        {
            let mut state = lock(&self.inner.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => encode_network_frame(
                NETWORK_BLOCK_RESPONSE,
                &RemoteWireReply::Ok {
                    incarnation: self.inner.incarnation,
                    envelope,
                },
            )?,
            Err(error) => encode_network_block_error(self.inner.incarnation, error.to_string())?,
        };

        let mut state = lock(&self.inner.state)?;
        if self.inner.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.inner.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }
}

impl RemoteWireTransport for NetworkBlockEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Network native endpoint using the crate-owned Phase 19 wire codec.
#[derive(Clone)]
pub struct NetworkNativeEndpoint {
    inner: RemoteNativeEndpoint,
}

impl NetworkNativeEndpoint {
    pub fn new(
        server: Arc<dyn NativeServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            inner: RemoteNativeEndpoint::new(
                server,
                incarnation,
                dedupe_capacity,
                mailbox_capacity,
            ),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.inner.incarnation()
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        self.inner.set_shutdown(shutdown)
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        self.inner.set_logical_time(logical_time)
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<NativeRequestEnvelope> =
            match decode_network_frame(NETWORK_NATIVE_REQUEST, request_bytes) {
                Ok(wire) => wire,
                Err(error) => {
                    return encode_network_native_error(self.inner.incarnation, error.to_string());
                }
            };
        if wire.incarnation != self.inner.incarnation {
            return encode_network_native_error(
                self.inner.incarnation,
                "stale native server incarnation",
            );
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.inner.state)?.logical_time.raw()
        {
            return encode_network_native_error(
                self.inner.incarnation,
                "native request deadline expired",
            );
        }
        let key = (
            self.inner.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.inner.state)?;
            if state.shutdown {
                return encode_network_native_error(
                    self.inner.incarnation,
                    "native endpoint is shut down",
                );
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return encode_network_native_error(
                    self.inner.incarnation,
                    "request ID and client epoch reused for a different network native request",
                );
            }
            if state.in_flight >= self.inner.mailbox_capacity {
                return encode_network_native_error(
                    self.inner.incarnation,
                    "native endpoint mailbox is full",
                );
            }
            state.in_flight += 1;
        }

        let response = self.inner.server.handle(wire.envelope);
        {
            let mut state = lock(&self.inner.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => encode_network_frame(
                NETWORK_NATIVE_RESPONSE,
                &RemoteWireReply::Ok {
                    incarnation: self.inner.incarnation,
                    envelope,
                },
            )?,
            Err(error) => encode_network_native_error(self.inner.incarnation, error.to_string())?,
        };

        let mut state = lock(&self.inner.state)?;
        if self.inner.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.inner.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }
}

impl RemoteWireTransport for NetworkNativeEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Block transport over a real network-capable wire transport.
#[derive(Clone)]
pub struct NetworkBlockTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl NetworkBlockTransport {
    pub fn new(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    pub fn tcp(addr: SocketAddr, incarnation: ServerIncarnation) -> Self {
        Self::new(
            Arc::new(TcpRemoteWireTransport::new(
                addr,
                DEFAULT_NETWORK_MAX_FRAME_BYTES,
            )),
            incarnation,
        )
    }

    fn encode_request(&self, request: BlockRequestEnvelope) -> Result<Vec<u8>> {
        encode_network_frame(
            NETWORK_BLOCK_REQUEST,
            &RemoteWireRequest {
                incarnation: self.incarnation,
                envelope: request,
            },
        )
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<BlockResponseEnvelope> {
        let reply: RemoteWireReply<BlockResponseEnvelope> =
            decode_network_frame(NETWORK_BLOCK_RESPONSE, bytes)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "network block response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale block server incarnation"))
            }
        }
    }
}

impl BlockTransport for NetworkBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

/// Native transport over a real network-capable wire transport.
#[derive(Clone)]
pub struct NetworkNativeTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl NetworkNativeTransport {
    pub fn new(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    pub fn tcp(addr: SocketAddr, incarnation: ServerIncarnation) -> Self {
        Self::new(
            Arc::new(TcpRemoteWireTransport::new(
                addr,
                DEFAULT_NETWORK_MAX_FRAME_BYTES,
            )),
            incarnation,
        )
    }

    fn encode_request(&self, request: NativeRequestEnvelope) -> Result<Vec<u8>> {
        encode_network_frame(
            NETWORK_NATIVE_REQUEST,
            &RemoteWireRequest {
                incarnation: self.incarnation,
                envelope: request,
            },
        )
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<NativeResponseEnvelope> {
        let reply: RemoteWireReply<NativeResponseEnvelope> =
            decode_network_frame(NETWORK_NATIVE_RESPONSE, bytes)?;
        match reply {
            RemoteWireReply::Ok {
                incarnation,
                envelope,
            } if incarnation == self.incarnation && envelope.request_id == request_id => {
                Ok(envelope)
            }
            RemoteWireReply::Ok { incarnation, .. } if incarnation != self.incarnation => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
            RemoteWireReply::Ok { .. } => Err(StorageError::corrupt(
                "network native response request ID does not match request",
            )),
            RemoteWireReply::Err {
                incarnation,
                reason,
            } if incarnation == self.incarnation => Err(StorageError::unavailable(reason)),
            RemoteWireReply::Err { .. } => {
                Err(StorageError::conflict("stale native server incarnation"))
            }
        }
    }
}

impl NativeTransport for NetworkNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}
