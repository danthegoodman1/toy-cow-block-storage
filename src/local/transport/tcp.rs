/// TCP implementation of the opaque `RemoteWireTransport` byte pipe.
#[derive(Clone)]
pub struct TcpRemoteWireTransport {
    addr: SocketAddr,
    max_frame_bytes: usize,
    timeout: Duration,
}

impl TcpRemoteWireTransport {
    pub fn new(addr: SocketAddr, max_frame_bytes: usize) -> Self {
        Self {
            addr,
            max_frame_bytes,
            timeout: Duration::from_secs(5),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl RemoteWireTransport for TcpRemoteWireTransport {
    fn call_wire(&self, request_bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream =
            TcpStream::connect_timeout(&self.addr, self.timeout).map_err(network_io_error)?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(network_io_error)?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(network_io_error)?;
        write_tcp_frame(&mut stream, &request_bytes, self.max_frame_bytes)?;
        read_tcp_frame(&mut stream, self.max_frame_bytes)
    }
}

/// Small blocking TCP server for Phase 19 loopback/network testing.
pub struct TcpRemoteWireServer {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl TcpRemoteWireServer {
    pub fn start(
        listener: TcpListener,
        endpoint: Arc<dyn RemoteWireTransport>,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        let local_addr = listener.local_addr().map_err(network_io_error)?;
        listener.set_nonblocking(true).map_err(network_io_error)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if stream.set_nonblocking(false).is_err() {
                            continue;
                        }
                        let response = read_tcp_frame(&mut stream, max_frame_bytes)
                            .and_then(|request| endpoint.call_wire(request));
                        if let Ok(response) = response {
                            let _ = write_tcp_frame(&mut stream, &response, max_frame_bytes);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            local_addr,
            shutdown,
            handle: Mutex::new(Some(handle)),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.local_addr);
        if let Some(handle) = lock(&self.handle)?.take() {
            handle
                .join()
                .map_err(|_| StorageError::unavailable("network server thread panicked"))?;
        }
        Ok(())
    }
}

impl Drop for TcpRemoteWireServer {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

pub(super) fn write_tcp_frame(stream: &mut TcpStream, bytes: &[u8], max_frame_bytes: usize) -> Result<()> {
    if bytes.len() > max_frame_bytes {
        return Err(StorageError::invalid_argument(
            "network frame exceeds limit",
        ));
    }
    let len = u32::try_from(bytes.len())
        .map_err(|_| StorageError::invalid_argument("network frame length exceeds u32"))?;
    stream
        .write_all(&len.to_be_bytes())
        .map_err(network_io_error)?;
    stream.write_all(bytes).map_err(network_io_error)
}

pub(super) fn read_tcp_frame(stream: &mut TcpStream, max_frame_bytes: usize) -> Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).map_err(network_io_error)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > max_frame_bytes {
        return Err(StorageError::invalid_argument(
            "network frame exceeds limit",
        ));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).map_err(network_io_error)?;
    Ok(bytes)
}
