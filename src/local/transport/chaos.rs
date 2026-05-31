/// Serialized wire boundary used by remote-shaped transports.
///
/// Minimal implementor guarantees:
///
/// - Accept exactly one encoded request envelope and return exactly one encoded
///   response envelope, or report a transport-level failure.
/// - Preserve request bytes as opaque data; block/native semantics are enforced
///   above this trait by the typed transport and below it by the endpoint.
/// - Failures, dropped responses, delayed responses, and reordered responses
///   must be surfaced as errors or bytes for the typed transport to validate;
///   they must not mutate request IDs or response IDs.
pub trait RemoteWireTransport: Send + Sync {
    /// Send one encoded request and return encoded response bytes.
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>>;
}

/// Deterministic counters for chaos wire transport fault injection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChaosTransportMetrics {
    pub request_drops: u64,
    pub response_drops: u64,
    pub corrupted_responses: u64,
    pub duplicated_requests: u64,
    pub delayed_responses: u64,
    pub reordered_responses: u64,
    pub injected_failures: u64,
}

#[derive(Debug)]
pub(super) struct ChaosWireState {
    delayed: VecDeque<Result<Vec<u8>>>,
    trace: Vec<String>,
    metrics: ChaosTransportMetrics,
    fail_next_call: bool,
    drop_next_request: bool,
    drop_next_response: bool,
    corrupt_next_response: bool,
    duplicate_next_request: bool,
    delay_next_response: bool,
    return_delayed_next: bool,
    reorder_next_response: bool,
}

impl ChaosWireState {
    fn new() -> Self {
        Self {
            delayed: VecDeque::new(),
            trace: Vec::new(),
            metrics: ChaosTransportMetrics::default(),
            fail_next_call: false,
            drop_next_request: false,
            drop_next_response: false,
            corrupt_next_response: false,
            duplicate_next_request: false,
            delay_next_response: false,
            return_delayed_next: false,
            reorder_next_response: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChaosWireAction {
    Pass,
    Fail,
    DropRequest,
    DropResponse,
    CorruptResponse,
    DuplicateRequest,
    DelayResponse,
    ReturnDelayed,
    ReorderWithDelayed,
}

/// Deterministic chaos wrapper for serialized remote transports.
///
/// The wrapper is intentionally scriptable instead of wall-clock or thread
/// driven. Tests can force the next request or response to be dropped,
/// duplicated, delayed, or reordered and then assert that the typed transport
/// rejects stale bytes or retries safely with the same request identity.
#[derive(Clone)]
pub struct ChaosRemoteWireTransport {
    inner: Arc<dyn RemoteWireTransport>,
    state: Arc<Mutex<ChaosWireState>>,
}

impl ChaosRemoteWireTransport {
    pub fn new(inner: Arc<dyn RemoteWireTransport>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ChaosWireState::new())),
        }
    }

    pub fn trace(&self) -> Result<Vec<String>> {
        Ok(lock(&self.state)?.trace.clone())
    }

    pub fn metrics(&self) -> Result<ChaosTransportMetrics> {
        Ok(lock(&self.state)?.metrics)
    }

    pub fn delayed_len(&self) -> Result<usize> {
        Ok(lock(&self.state)?.delayed.len())
    }

    pub fn fail_next_call(&self) -> Result<()> {
        lock(&self.state)?.fail_next_call = true;
        Ok(())
    }

    pub fn drop_next_request(&self) -> Result<()> {
        lock(&self.state)?.drop_next_request = true;
        Ok(())
    }

    pub fn drop_next_response(&self) -> Result<()> {
        lock(&self.state)?.drop_next_response = true;
        Ok(())
    }

    pub fn corrupt_next_response(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_response = true;
        Ok(())
    }

    pub fn duplicate_next_request(&self) -> Result<()> {
        lock(&self.state)?.duplicate_next_request = true;
        Ok(())
    }

    pub fn delay_next_response(&self) -> Result<()> {
        lock(&self.state)?.delay_next_response = true;
        Ok(())
    }

    pub fn return_delayed_response_next_call(&self) -> Result<()> {
        lock(&self.state)?.return_delayed_next = true;
        Ok(())
    }

    pub fn reorder_next_response_with_delayed(&self) -> Result<()> {
        lock(&self.state)?.reorder_next_response = true;
        Ok(())
    }

    fn take_action(&self) -> Result<ChaosWireAction> {
        let mut state = lock(&self.state)?;
        if state.fail_next_call {
            state.fail_next_call = false;
            state.metrics.injected_failures = state.metrics.injected_failures.saturating_add(1);
            state.trace.push("fail call before send".to_string());
            return Ok(ChaosWireAction::Fail);
        }
        if state.drop_next_request {
            state.drop_next_request = false;
            state.metrics.request_drops = state.metrics.request_drops.saturating_add(1);
            state.trace.push("drop request before send".to_string());
            return Ok(ChaosWireAction::DropRequest);
        }
        if state.return_delayed_next {
            state.return_delayed_next = false;
            state
                .trace
                .push("return delayed response before sending request".to_string());
            return Ok(ChaosWireAction::ReturnDelayed);
        }
        if state.reorder_next_response {
            state.reorder_next_response = false;
            state.metrics.reordered_responses = state.metrics.reordered_responses.saturating_add(1);
            state
                .trace
                .push("reorder current response behind delayed response".to_string());
            return Ok(ChaosWireAction::ReorderWithDelayed);
        }
        if state.drop_next_response {
            state.drop_next_response = false;
            state.metrics.response_drops = state.metrics.response_drops.saturating_add(1);
            state.trace.push("drop response after send".to_string());
            return Ok(ChaosWireAction::DropResponse);
        }
        if state.corrupt_next_response {
            state.corrupt_next_response = false;
            state.metrics.corrupted_responses = state.metrics.corrupted_responses.saturating_add(1);
            state.trace.push("corrupt response after send".to_string());
            return Ok(ChaosWireAction::CorruptResponse);
        }
        if state.duplicate_next_request {
            state.duplicate_next_request = false;
            state.metrics.duplicated_requests = state.metrics.duplicated_requests.saturating_add(1);
            state.trace.push("duplicate request delivery".to_string());
            return Ok(ChaosWireAction::DuplicateRequest);
        }
        if state.delay_next_response {
            state.delay_next_response = false;
            state.metrics.delayed_responses = state.metrics.delayed_responses.saturating_add(1);
            state.trace.push("delay response after send".to_string());
            return Ok(ChaosWireAction::DelayResponse);
        }
        Ok(ChaosWireAction::Pass)
    }

    fn pop_delayed(&self) -> Result<Result<Vec<u8>>> {
        let mut state = lock(&self.state)?;
        state
            .delayed
            .pop_front()
            .ok_or_else(|| StorageError::unavailable("chaos transport has no delayed response"))
    }

    fn push_delayed(&self, response: Result<Vec<u8>>) -> Result<()> {
        lock(&self.state)?.delayed.push_back(response);
        Ok(())
    }
}

impl RemoteWireTransport for ChaosRemoteWireTransport {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        match self.take_action()? {
            ChaosWireAction::Pass => self.inner.call_wire(request_bytes),
            ChaosWireAction::Fail => Err(StorageError::unavailable("chaos transport failed call")),
            ChaosWireAction::DropRequest => Err(StorageError::unavailable(
                "chaos transport dropped request before send",
            )),
            ChaosWireAction::DropResponse => {
                let _ = self.inner.call_wire(request_bytes);
                Err(StorageError::unavailable(
                    "chaos transport dropped response after send",
                ))
            }
            ChaosWireAction::CorruptResponse => {
                let mut response = self.inner.call_wire(request_bytes)?;
                if let Some(first) = response.first_mut() {
                    *first ^= 0xff;
                } else {
                    response.push(0xff);
                }
                Ok(response)
            }
            ChaosWireAction::DuplicateRequest => {
                let first = self.inner.call_wire(request_bytes.clone());
                let second = self.inner.call_wire(request_bytes);
                match (first, second) {
                    (Ok(response), Ok(_)) => Ok(response),
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                }
            }
            ChaosWireAction::DelayResponse => {
                let response = self.inner.call_wire(request_bytes);
                self.push_delayed(response)?;
                Err(StorageError::unavailable(
                    "chaos transport delayed response after send",
                ))
            }
            ChaosWireAction::ReturnDelayed => self.pop_delayed()?,
            ChaosWireAction::ReorderWithDelayed => {
                let delayed = self.pop_delayed()?;
                let current = self.inner.call_wire(request_bytes);
                self.push_delayed(current)?;
                delayed
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct ChaosStorageNodeState {
    delayed: VecDeque<Result<StorageNodeResponse>>,
    trace: Vec<String>,
    metrics: ChaosTransportMetrics,
    fail_next_call: bool,
    drop_next_request: bool,
    drop_next_response: bool,
    corrupt_next_grant: bool,
    corrupt_next_receipt: bool,
    duplicate_next_request: bool,
    delay_next_response: bool,
    return_delayed_next: bool,
}

impl ChaosStorageNodeState {
    fn new() -> Self {
        Self {
            delayed: VecDeque::new(),
            trace: Vec::new(),
            metrics: ChaosTransportMetrics::default(),
            fail_next_call: false,
            drop_next_request: false,
            drop_next_response: false,
            corrupt_next_grant: false,
            corrupt_next_receipt: false,
            duplicate_next_request: false,
            delay_next_response: false,
            return_delayed_next: false,
        }
    }
}

/// Deterministic chaos wrapper for coordinator-to-storage-node messages.
///
/// Tests can inject drops, duplicates, delays, and proof corruption without
/// spawning background work or relying on wall-clock timing.
#[derive(Clone)]
pub struct ChaosStorageNodeTransport {
    inner: Arc<dyn StorageNodeTransport>,
    state: Arc<Mutex<ChaosStorageNodeState>>,
}

impl ChaosStorageNodeTransport {
    pub fn new(inner: Arc<dyn StorageNodeTransport>) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(ChaosStorageNodeState::new())),
        }
    }

    pub fn trace(&self) -> Result<Vec<String>> {
        Ok(lock(&self.state)?.trace.clone())
    }

    pub fn metrics(&self) -> Result<ChaosTransportMetrics> {
        Ok(lock(&self.state)?.metrics)
    }

    pub fn delayed_len(&self) -> Result<usize> {
        Ok(lock(&self.state)?.delayed.len())
    }

    pub fn fail_next_call(&self) -> Result<()> {
        lock(&self.state)?.fail_next_call = true;
        Ok(())
    }

    pub fn drop_next_request(&self) -> Result<()> {
        lock(&self.state)?.drop_next_request = true;
        Ok(())
    }

    pub fn drop_next_response(&self) -> Result<()> {
        lock(&self.state)?.drop_next_response = true;
        Ok(())
    }

    pub fn corrupt_next_grant(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_grant = true;
        Ok(())
    }

    pub fn corrupt_next_receipt(&self) -> Result<()> {
        lock(&self.state)?.corrupt_next_receipt = true;
        Ok(())
    }

    pub fn duplicate_next_request(&self) -> Result<()> {
        lock(&self.state)?.duplicate_next_request = true;
        Ok(())
    }

    pub fn delay_next_response(&self) -> Result<()> {
        lock(&self.state)?.delay_next_response = true;
        Ok(())
    }

    pub fn return_delayed_response_next_call(&self) -> Result<()> {
        lock(&self.state)?.return_delayed_next = true;
        Ok(())
    }

    fn mutate_request(request: &mut StorageNodeRequest) {
        if let StorageNodeRequest::WriteSegment { grant, .. } = request {
            grant.proof.0[0] ^= 0xff;
        }
    }

    fn mutate_response(response: &mut StorageNodeResponse) {
        if let StorageNodeResponse::WriteSegment { receipt } = response {
            receipt.proof.0[0] ^= 0xff;
        }
    }

    fn pop_delayed(&self) -> Result<Result<StorageNodeResponse>> {
        let mut state = lock(&self.state)?;
        state
            .delayed
            .pop_front()
            .ok_or_else(|| StorageError::unavailable("chaos storage node has no delayed response"))
    }
}

impl StorageNodeTransport for ChaosStorageNodeTransport {
    fn storage_node_id(&self) -> StorageNodeId {
        self.inner.storage_node_id()
    }

    fn send(&self, mut request: StorageNodeRequest) -> Result<StorageNodeResponse> {
        {
            let mut state = lock(&self.state)?;
            if state.fail_next_call {
                state.fail_next_call = false;
                state.metrics.injected_failures = state.metrics.injected_failures.saturating_add(1);
                state.trace.push("fail storage-node call".to_string());
                return Err(StorageError::unavailable("chaos storage node failed call"));
            }
            if state.drop_next_request {
                state.drop_next_request = false;
                state.metrics.request_drops = state.metrics.request_drops.saturating_add(1);
                state.trace.push("drop storage-node request".to_string());
                return Err(StorageError::unavailable(
                    "chaos storage node dropped request before send",
                ));
            }
            if state.return_delayed_next {
                state.return_delayed_next = false;
                state
                    .trace
                    .push("return delayed storage-node response".to_string());
                drop(state);
                return self.pop_delayed()?;
            }
            if state.corrupt_next_grant {
                state.corrupt_next_grant = false;
                state.metrics.corrupted_responses =
                    state.metrics.corrupted_responses.saturating_add(1);
                state.trace.push("corrupt storage-node grant".to_string());
                Self::mutate_request(&mut request);
            }
        }

        let mut response = if lock(&self.state)?.duplicate_next_request {
            {
                let mut state = lock(&self.state)?;
                state.duplicate_next_request = false;
                state.metrics.duplicated_requests =
                    state.metrics.duplicated_requests.saturating_add(1);
                state
                    .trace
                    .push("duplicate storage-node request".to_string());
            }
            let first = self.inner.send(request.clone());
            let second = self.inner.send(request);
            match (first, second) {
                (Ok(response), Ok(_)) => Ok(response),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }?
        } else {
            self.inner.send(request)?
        };

        let mut state = lock(&self.state)?;
        if state.drop_next_response {
            state.drop_next_response = false;
            state.metrics.response_drops = state.metrics.response_drops.saturating_add(1);
            state.trace.push("drop storage-node response".to_string());
            return Err(StorageError::unavailable(
                "chaos storage node dropped response after send",
            ));
        }
        if state.corrupt_next_receipt {
            state.corrupt_next_receipt = false;
            state.metrics.corrupted_responses = state.metrics.corrupted_responses.saturating_add(1);
            state.trace.push("corrupt storage-node receipt".to_string());
            Self::mutate_response(&mut response);
        }
        if state.delay_next_response {
            state.delay_next_response = false;
            state.metrics.delayed_responses = state.metrics.delayed_responses.saturating_add(1);
            state.trace.push("delay storage-node response".to_string());
            state.delayed.push_back(Ok(response));
            return Err(StorageError::unavailable(
                "chaos storage node delayed response after send",
            ));
        }
        Ok(response)
    }
}
