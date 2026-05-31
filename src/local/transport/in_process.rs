/// In-process block transport.
#[derive(Clone)]
pub struct InProcessBlockTransport {
    server: Arc<dyn BlockServer>,
}

impl InProcessBlockTransport {
    pub fn new(server: Arc<dyn BlockServer>) -> Self {
        Self { server }
    }
}

impl BlockTransport for InProcessBlockTransport {
    fn call(&self, request: BlockRequestEnvelope) -> Result<BlockResponseEnvelope> {
        self.server.handle(request)
    }
}

/// In-process native keyspace/file transport.
#[derive(Clone)]
pub struct InProcessNativeTransport {
    server: Arc<dyn NativeServer>,
}

impl InProcessNativeTransport {
    pub fn new(server: Arc<dyn NativeServer>) -> Self {
        Self { server }
    }
}

impl NativeTransport for InProcessNativeTransport {
    fn call(&self, request: NativeRequestEnvelope) -> Result<NativeResponseEnvelope> {
        self.server.handle(request)
    }
}
