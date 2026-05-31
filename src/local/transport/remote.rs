#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct RemoteWireRequest<T> {
    incarnation: ServerIncarnation,
    envelope: T,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) enum RemoteWireReply<T> {
    Ok {
        incarnation: ServerIncarnation,
        envelope: T,
    },
    Err {
        incarnation: ServerIncarnation,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemoteCacheEntry {
    request_bytes: Vec<u8>,
    response_bytes: Vec<u8>,
}

pub(super) type RemoteRequestKey = (ServerIncarnation, ClientEpoch, RequestId);

#[derive(Debug)]
pub(super) struct RemoteEndpointState {
    cache: BTreeMap<RemoteRequestKey, RemoteCacheEntry>,
    order: VecDeque<RemoteRequestKey>,
    in_flight: usize,
    shutdown: bool,
    logical_time: LogicalTime,
}

impl RemoteEndpointState {
    fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            order: VecDeque::new(),
            in_flight: 0,
            shutdown: false,
            logical_time: LogicalTime::from_raw(0),
        }
    }
}

/// Deterministic remote-capable block endpoint.
#[derive(Clone)]
pub struct RemoteBlockEndpoint {
    server: Arc<dyn BlockServer>,
    incarnation: ServerIncarnation,
    dedupe_capacity: usize,
    mailbox_capacity: usize,
    state: Arc<Mutex<RemoteEndpointState>>,
}

impl RemoteBlockEndpoint {
    pub fn new(
        server: Arc<dyn BlockServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            server,
            incarnation,
            dedupe_capacity,
            mailbox_capacity,
            state: Arc::new(Mutex::new(RemoteEndpointState::new())),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.incarnation
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        lock(&self.state)?.shutdown = shutdown;
        Ok(())
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        lock(&self.state)?.logical_time = logical_time;
        Ok(())
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<BlockRequestEnvelope> =
            bincode::deserialize(request_bytes).map_err(serde_error)?;
        if wire.incarnation != self.incarnation {
            return self.encode_error("stale block server incarnation");
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.state)?.logical_time.raw()
        {
            return self.encode_error("block request deadline expired");
        }
        let key = (
            self.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.state)?;
            if state.shutdown {
                return self.encode_error("block endpoint is shut down");
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return self.encode_error(
                    "request ID and client epoch reused for a different remote block request",
                );
            }
            if state.in_flight >= self.mailbox_capacity {
                return self.encode_error("block endpoint mailbox is full");
            }
            state.in_flight += 1;
        }

        let response = self.server.handle(wire.envelope);
        {
            let mut state = lock(&self.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => bincode::serialize(&RemoteWireReply::Ok {
                incarnation: self.incarnation,
                envelope,
            })
            .map_err(serde_error)?,
            Err(error) => bincode::serialize(&RemoteWireReply::<BlockResponseEnvelope>::Err {
                incarnation: self.incarnation,
                reason: error.to_string(),
            })
            .map_err(serde_error)?,
        };

        let mut state = lock(&self.state)?;
        if self.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }

    fn encode_error(&self, reason: impl Into<String>) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireReply::<BlockResponseEnvelope>::Err {
            incarnation: self.incarnation,
            reason: reason.into(),
        })
        .map_err(serde_error)
    }
}

impl RemoteWireTransport for RemoteBlockEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Phase 17 serialized block transport over a deterministic remote endpoint.
///
/// This deliberately remains an in-process test/model transport. The Phase 19
/// TCP path uses `NetworkBlockTransport` and the crate-owned network codec.
#[derive(Clone)]
pub struct RemoteBlockTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl RemoteBlockTransport {
    pub fn new(endpoint: Arc<RemoteBlockEndpoint>) -> Self {
        Self::with_wire(endpoint.clone(), endpoint.incarnation())
    }

    pub fn with_wire(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    fn encode_request(&self, request: BlockRequestEnvelope) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireRequest {
            incarnation: self.incarnation,
            envelope: request,
        })
        .map_err(serde_error)
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<BlockResponseEnvelope> {
        let reply: RemoteWireReply<BlockResponseEnvelope> =
            bincode::deserialize(bytes).map_err(serde_error)?;
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
                "remote block response request ID does not match request",
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

impl BlockTransport for RemoteBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}

/// Deterministic remote-capable native endpoint.
#[derive(Clone)]
pub struct RemoteNativeEndpoint {
    server: Arc<dyn NativeServer>,
    incarnation: ServerIncarnation,
    dedupe_capacity: usize,
    mailbox_capacity: usize,
    state: Arc<Mutex<RemoteEndpointState>>,
}

impl RemoteNativeEndpoint {
    pub fn new(
        server: Arc<dyn NativeServer>,
        incarnation: ServerIncarnation,
        dedupe_capacity: usize,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            server,
            incarnation,
            dedupe_capacity,
            mailbox_capacity,
            state: Arc::new(Mutex::new(RemoteEndpointState::new())),
        }
    }

    pub fn incarnation(&self) -> ServerIncarnation {
        self.incarnation
    }

    pub fn set_shutdown(&self, shutdown: bool) -> Result<()> {
        lock(&self.state)?.shutdown = shutdown;
        Ok(())
    }

    pub fn set_logical_time(&self, logical_time: LogicalTime) -> Result<()> {
        lock(&self.state)?.logical_time = logical_time;
        Ok(())
    }

    pub fn handle_wire(&self, request_bytes: &[u8]) -> Result<Vec<u8>> {
        let wire: RemoteWireRequest<NativeRequestEnvelope> =
            bincode::deserialize(request_bytes).map_err(serde_error)?;
        if wire.incarnation != self.incarnation {
            return self.encode_error("stale native server incarnation");
        }
        if let Some(deadline) = wire.envelope.deadline
            && deadline.raw() < lock(&self.state)?.logical_time.raw()
        {
            return self.encode_error("native request deadline expired");
        }
        let key = (
            self.incarnation,
            wire.envelope.client_epoch,
            wire.envelope.request_id,
        );

        {
            let mut state = lock(&self.state)?;
            if state.shutdown {
                return self.encode_error("native endpoint is shut down");
            }
            if let Some(entry) = state.cache.get(&key) {
                if entry.request_bytes == request_bytes {
                    return Ok(entry.response_bytes.clone());
                }
                return self.encode_error(
                    "request ID and client epoch reused for a different remote native request",
                );
            }
            if state.in_flight >= self.mailbox_capacity {
                return self.encode_error("native endpoint mailbox is full");
            }
            state.in_flight += 1;
        }

        let response = self.server.handle(wire.envelope);
        {
            let mut state = lock(&self.state)?;
            state.in_flight = state.in_flight.saturating_sub(1);
        }
        let response_bytes = match response {
            Ok(envelope) => bincode::serialize(&RemoteWireReply::Ok {
                incarnation: self.incarnation,
                envelope,
            })
            .map_err(serde_error)?,
            Err(error) => bincode::serialize(&RemoteWireReply::<NativeResponseEnvelope>::Err {
                incarnation: self.incarnation,
                reason: error.to_string(),
            })
            .map_err(serde_error)?,
        };

        let mut state = lock(&self.state)?;
        if self.dedupe_capacity != 0 {
            state.cache.insert(
                key,
                RemoteCacheEntry {
                    request_bytes: request_bytes.to_vec(),
                    response_bytes: response_bytes.clone(),
                },
            );
            state.order.push_back(key);
            while state.order.len() > self.dedupe_capacity {
                if let Some(evicted) = state.order.pop_front() {
                    state.cache.remove(&evicted);
                }
            }
        }
        Ok(response_bytes)
    }

    fn encode_error(&self, reason: impl Into<String>) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireReply::<NativeResponseEnvelope>::Err {
            incarnation: self.incarnation,
            reason: reason.into(),
        })
        .map_err(serde_error)
    }
}

impl RemoteWireTransport for RemoteNativeEndpoint {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        self.handle_wire(&request_bytes)
    }
}

/// Phase 17 serialized native transport over a deterministic remote endpoint.
///
/// This deliberately remains an in-process test/model transport. The Phase 19
/// TCP path uses `NetworkNativeTransport` and the crate-owned network codec.
#[derive(Clone)]
pub struct RemoteNativeTransport {
    wire: Arc<dyn RemoteWireTransport>,
    incarnation: ServerIncarnation,
}

impl RemoteNativeTransport {
    pub fn new(endpoint: Arc<RemoteNativeEndpoint>) -> Self {
        Self::with_wire(endpoint.clone(), endpoint.incarnation())
    }

    pub fn with_wire(wire: Arc<dyn RemoteWireTransport>, incarnation: ServerIncarnation) -> Self {
        Self { wire, incarnation }
    }

    fn encode_request(&self, request: NativeRequestEnvelope) -> Result<Vec<u8>> {
        bincode::serialize(&RemoteWireRequest {
            incarnation: self.incarnation,
            envelope: request,
        })
        .map_err(serde_error)
    }

    fn decode_response(
        &self,
        request_id: RequestId,
        bytes: &[u8],
    ) -> Result<NativeResponseEnvelope> {
        let reply: RemoteWireReply<NativeResponseEnvelope> =
            bincode::deserialize(bytes).map_err(serde_error)?;
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
                "remote native response request ID does not match request",
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

impl NativeTransport for RemoteNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        let request_id = request.request_id;
        let request_bytes = self.encode_request(request)?;
        let response_bytes = self.wire.call_wire(request_bytes)?;
        self.decode_response(request_id, &response_bytes)
    }
}
