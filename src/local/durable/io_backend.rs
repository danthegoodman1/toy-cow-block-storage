const DIRECT_IO_DEFAULT_ALIGNMENT: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum DurableLowLevelIoBackend {
    #[default]
    Filesystem,
    DirectIo,
    DirectIoOrFilesystem,
}

impl DurableLowLevelIoBackend {
    fn resolve(self, paths: &DurableStorePaths) -> Result<ResolvedDurableLowLevelIoBackend> {
        match self {
            Self::Filesystem => Ok(ResolvedDurableLowLevelIoBackend::Filesystem),
            Self::DirectIo => {
                let backend = DirectIoFileBackend::new(DIRECT_IO_DEFAULT_ALIGNMENT)?;
                backend.probe(&paths.data_dir)?;
                Ok(ResolvedDurableLowLevelIoBackend::DirectIo(backend))
            }
            Self::DirectIoOrFilesystem => {
                match DirectIoFileBackend::new(DIRECT_IO_DEFAULT_ALIGNMENT)
                    .and_then(|backend| backend.probe(&paths.data_dir).map(|()| backend))
                {
                    Ok(backend) => Ok(ResolvedDurableLowLevelIoBackend::DirectIo(backend)),
                    Err(_) => Ok(ResolvedDurableLowLevelIoBackend::Filesystem),
                }
            }
        }
    }
}

impl std::str::FromStr for DurableLowLevelIoBackend {
    type Err = StorageError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "filesystem" | "fs" | "buffered" => Ok(Self::Filesystem),
            "direct-io" | "direct" | "odirect" => Ok(Self::DirectIo),
            "direct-io-or-filesystem" | "direct-or-fs" | "auto" => {
                Ok(Self::DirectIoOrFilesystem)
            }
            _ => Err(StorageError::invalid_argument(format!(
                "unknown durable low-level I/O backend {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(super) enum DurableResolvedLowLevelIoBackend {
    Filesystem,
    DirectIo,
}

#[derive(Debug, Clone)]
pub(super) enum ResolvedDurableLowLevelIoBackend {
    Filesystem,
    DirectIo(Arc<DirectIoFileBackend>),
}

impl ResolvedDurableLowLevelIoBackend {
    #[cfg(test)]
    fn selected(&self) -> DurableResolvedLowLevelIoBackend {
        match self {
            Self::Filesystem => DurableResolvedLowLevelIoBackend::Filesystem,
            Self::DirectIo(_) => DurableResolvedLowLevelIoBackend::DirectIo,
        }
    }

    fn physical_append_len(&self, logical_len: u64) -> Result<u64> {
        match self {
            Self::Filesystem => Ok(logical_len),
            Self::DirectIo(backend) => backend.aligned_len_u64(logical_len),
        }
    }

    fn append_bytes(&self, path: &Path, chunks: &[&[u8]]) -> Result<LowLevelAppendOutcome> {
        match self {
            Self::Filesystem => append_file_buffered(path, chunks),
            Self::DirectIo(backend) => backend.append(path, chunks),
        }
    }

    fn file_len_after_alignment_padding(&self, path: &Path) -> Result<u64> {
        match self {
            Self::Filesystem => Ok(path.metadata().map_err(fs_error)?.len()),
            Self::DirectIo(backend) => {
                let (handle, _, _) = backend.file(path)?;
                Ok(lock(handle.as_ref())?.len)
            }
        }
    }

    fn sync_file(&self, file: DataLogFileToSync) -> Result<(u64, u64)> {
        let started = Instant::now();
        file.file.sync_data().map_err(fs_error)?;
        Ok((file.bytes, duration_nanos_u64(started.elapsed())))
    }

    fn forget_path(&self, path: &Path) -> Result<()> {
        if let Self::DirectIo(backend) = self {
            backend.forget(path)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct LowLevelAppendOutcome {
    open_nanos: u64,
    write_nanos: u64,
    physical_bytes: u64,
    created: bool,
}

fn append_file_buffered(path: &Path, chunks: &[&[u8]]) -> Result<LowLevelAppendOutcome> {
    let existed = path.exists();
    let open_started = Instant::now();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .map_err(fs_error)?;
    let open_nanos = duration_nanos_u64(open_started.elapsed());
    let started = Instant::now();
    let mut logical_bytes = 0_u64;
    for chunk in chunks {
        file.write_all(chunk).map_err(fs_error)?;
        logical_bytes = logical_bytes
            .checked_add(usize_to_u64(chunk.len()))
            .ok_or_else(|| StorageError::invalid_argument("append length overflows u64"))?;
    }
    Ok(LowLevelAppendOutcome {
        open_nanos,
        write_nanos: duration_nanos_u64(started.elapsed()),
        physical_bytes: logical_bytes,
        created: !existed,
    })
}

#[derive(Debug)]
pub(super) struct DirectIoFileBackend {
    alignment: usize,
    files: Mutex<BTreeMap<PathBuf, Arc<Mutex<DirectIoFile>>>>,
}

impl DirectIoFileBackend {
    fn new(alignment: usize) -> Result<Arc<Self>> {
        if !alignment.is_power_of_two() {
            return Err(StorageError::invalid_argument(
                "direct I/O alignment must be a power of two",
            ));
        }
        if !cfg!(target_os = "linux") {
            return Err(StorageError::unsupported(
                "direct I/O backend requires Linux O_DIRECT support",
            ));
        }
        Ok(Arc::new(Self {
            alignment,
            files: Mutex::new(BTreeMap::new()),
        }))
    }

    fn probe(&self, data_dir: &Path) -> Result<()> {
        let probe_dir = data_dir.join("direct-io-probe");
        ensure_dir_exists(&probe_dir)?;
        let path = probe_dir.join("probe.bin");
        let zeros = vec![0_u8; self.alignment];
        let outcome = self.append(&path, &[&zeros])?;
        if outcome.physical_bytes != usize_to_u64(self.alignment) {
            return Err(StorageError::corrupt("direct I/O probe wrote wrong length"));
        }
        self.forget(&path)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
        }
        match fs::remove_dir(&probe_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(fs_error(error)),
        }
        Ok(())
    }

    fn aligned_len(&self, len: usize) -> Result<usize> {
        let mask = self.alignment - 1;
        len.checked_add(mask)
            .map(|value| value & !mask)
            .ok_or_else(|| StorageError::invalid_argument("direct I/O length overflows usize"))
    }

    fn aligned_len_u64(&self, len: u64) -> Result<u64> {
        let len = usize::try_from(len).map_err(|_| {
            StorageError::invalid_argument("direct I/O length overflows usize")
        })?;
        Ok(usize_to_u64(self.aligned_len(len)?))
    }

    fn append(&self, path: &Path, chunks: &[&[u8]]) -> Result<LowLevelAppendOutcome> {
        let logical_len = chunks.iter().try_fold(0_usize, |total, chunk| {
            total
                .checked_add(chunk.len())
                .ok_or_else(|| StorageError::invalid_argument("append length overflows usize"))
        })?;
        if logical_len == 0 {
            return Ok(LowLevelAppendOutcome::default());
        }
        let physical_len = self.aligned_len(logical_len)?;
        let (handle, open_nanos, created) = self.file(path)?;
        let mut file = lock(handle.as_ref())?;
        let offset = file.len;
        let started = Instant::now();
        file.scratch.ensure_len(physical_len)?;
        let mut copied = 0_usize;
        for chunk in chunks {
            file.scratch.as_mut_slice()[copied..copied + chunk.len()].copy_from_slice(chunk);
            copied += chunk.len();
        }
        file.scratch.as_mut_slice()[logical_len..physical_len].fill(0);
        write_all_at(
            &file.file,
            &file.scratch.as_slice()[..physical_len],
            offset,
            self.alignment,
        )?;
        file.len = file
            .len
            .checked_add(usize_to_u64(physical_len))
            .ok_or_else(|| StorageError::invalid_argument("direct I/O file length overflows"))?;
        Ok(LowLevelAppendOutcome {
            open_nanos,
            write_nanos: duration_nanos_u64(started.elapsed()),
            physical_bytes: usize_to_u64(physical_len),
            created,
        })
    }

    fn file(&self, path: &Path) -> Result<(Arc<Mutex<DirectIoFile>>, u64, bool)> {
        let mut files = lock(&self.files)?;
        if let Some(file) = files.get(path).cloned() {
            return Ok((file, 0, false));
        }
        let started = Instant::now();
        let existed = path.exists();
        pad_file_to_alignment(path, self.alignment)?;
        let file = open_direct_io_file(path)?;
        let len = file.metadata().map_err(fs_error)?.len();
        if len % usize_to_u64(self.alignment) != 0 {
            return Err(StorageError::corrupt(
                "direct I/O file length is not alignment padded",
            ));
        }
        let opened = Arc::new(Mutex::new(DirectIoFile {
            file,
            len,
            scratch: DirectIoScratch::new(self.alignment)?,
        }));
        files.insert(path.to_path_buf(), Arc::clone(&opened));
        Ok((
            opened,
            duration_nanos_u64(started.elapsed()),
            !existed,
        ))
    }

    fn forget(&self, path: &Path) -> Result<()> {
        lock(&self.files)?.remove(path);
        Ok(())
    }
}

#[derive(Debug)]
struct DirectIoFile {
    file: File,
    len: u64,
    scratch: DirectIoScratch,
}

#[derive(Debug)]
struct DirectIoScratch {
    bytes: memmap2::MmapMut,
}

impl DirectIoScratch {
    fn new(alignment: usize) -> Result<Self> {
        let len = alignment.max(DIRECT_IO_DEFAULT_ALIGNMENT);
        Ok(Self {
            bytes: memmap2::MmapOptions::new()
                .len(len)
                .map_anon()
                .map_err(fs_error)?,
        })
    }

    fn ensure_len(&mut self, len: usize) -> Result<()> {
        if self.bytes.len() >= len {
            return Ok(());
        }
        self.bytes = memmap2::MmapOptions::new()
            .len(len)
            .map_anon()
            .map_err(fs_error)?;
        Ok(())
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

#[cfg(target_os = "linux")]
fn open_direct_io_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .custom_flags(rustix::fs::OFlags::DIRECT.bits() as i32)
        .open(path)
        .map_err(fs_error)
}

#[cfg(not(target_os = "linux"))]
fn open_direct_io_file(_path: &Path) -> Result<File> {
    Err(StorageError::unsupported(
        "direct I/O backend requires Linux O_DIRECT support",
    ))
}

#[cfg(target_os = "linux")]
fn write_all_at(file: &File, bytes: &[u8], offset: u64, alignment: usize) -> Result<()> {
    use std::os::unix::fs::FileExt;

    write_all_at_with(bytes, offset, alignment, |chunk, chunk_offset| {
        file.write_at(chunk, chunk_offset).map_err(fs_error)
    })
}

#[cfg(any(target_os = "linux", test))]
fn write_all_at_with<F>(
    bytes: &[u8],
    offset: u64,
    alignment: usize,
    mut write_at: F,
) -> Result<()>
where
    F: FnMut(&[u8], u64) -> Result<usize>,
{
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(StorageError::invalid_argument(
            "direct I/O alignment must be a nonzero power of two",
        ));
    }
    let mut completed = 0_usize;
    let mut unaligned_short_retries = 0_u8;
    while completed < bytes.len() {
        let chunk_offset = offset
            .checked_add(usize_to_u64(completed))
            .ok_or_else(|| StorageError::invalid_argument("direct I/O offset overflows"))?;
        let written = write_at(&bytes[completed..], chunk_offset)?;
        if written == 0 {
            return Err(StorageError::unavailable(
                "direct I/O write made no progress",
            ));
        }
        if written > bytes.len() - completed {
            return Err(StorageError::unavailable(
                "direct I/O write reported too many bytes",
            ));
        }
        if written.is_multiple_of(alignment) {
            completed += written;
            unaligned_short_retries = 0;
            continue;
        }
        unaligned_short_retries = unaligned_short_retries.saturating_add(1);
        if unaligned_short_retries >= 16 {
            return Err(StorageError::unavailable(
                "direct I/O write repeatedly returned unaligned short writes",
            ));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn write_all_at(_file: &File, _bytes: &[u8], _offset: u64, _alignment: usize) -> Result<()> {
    Err(StorageError::unsupported(
        "direct I/O backend requires Linux O_DIRECT support",
    ))
}

fn pad_file_to_alignment(path: &Path, alignment: usize) -> Result<()> {
    let len = match path.metadata() {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(fs_error(error)),
    };
    let alignment_u64 = usize_to_u64(alignment);
    let remainder = len % alignment_u64;
    if remainder == 0 {
        return Ok(());
    }
    let padding = usize::try_from(alignment_u64 - remainder).map_err(|_| {
        StorageError::invalid_argument("direct I/O alignment padding overflows usize")
    })?;
    let zeros = vec![0_u8; padding];
    let mut file = OpenOptions::new().append(true).open(path).map_err(fs_error)?;
    file.write_all(&zeros).map_err(fs_error)?;
    Ok(())
}

#[cfg(test)]
mod io_backend_tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn direct_write_retries_unaligned_short_write_from_aligned_boundary() {
        let bytes = vec![7_u8; 4096];
        let mut calls = Vec::new();
        write_all_at_with(&bytes, 8192, 4096, |chunk, offset| {
            calls.push((offset, chunk.len()));
            Ok(if calls.len() == 1 { 512 } else { chunk.len() })
        })
        .unwrap();

        assert_eq!(calls, vec![(8192, 4096), (8192, 4096)]);
    }

    #[test]
    fn direct_write_advances_on_aligned_short_write() {
        let bytes = vec![9_u8; 8192];
        let mut calls = Vec::new();
        write_all_at_with(&bytes, 4096, 4096, |chunk, offset| {
            calls.push((offset, chunk.len()));
            Ok(if calls.len() == 1 { 4096 } else { chunk.len() })
        })
        .unwrap();

        assert_eq!(calls, vec![(4096, 8192), (8192, 4096)]);
    }

    #[test]
    fn direct_write_fails_repeated_unaligned_short_writes() {
        let bytes = vec![11_u8; 4096];
        let error = write_all_at_with(&bytes, 0, 4096, |_chunk, _offset| Ok(512)).unwrap_err();

        assert!(matches!(error, StorageError::Unavailable { .. }));
    }

    #[test]
    fn direct_io_concurrent_first_appends_share_one_path_length_when_supported() {
        let root = std::env::temp_dir().join(format!(
            "toy-cow-direct-io-concurrent-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let backend = match DirectIoFileBackend::new(DIRECT_IO_DEFAULT_ALIGNMENT)
            .and_then(|backend| backend.probe(&root).map(|()| backend))
        {
            Ok(backend) => backend,
            Err(_) => {
                let _ = fs::remove_dir_all(&root);
                return;
            }
        };
        let path = root.join("append.bin");
        let workers = 8_usize;
        let barrier = Arc::new(Barrier::new(workers));
        thread::scope(|scope| {
            for worker in 0..workers {
                let backend = Arc::clone(&backend);
                let barrier = Arc::clone(&barrier);
                let path = path.clone();
                scope.spawn(move || {
                    let bytes = vec![worker as u8; DIRECT_IO_DEFAULT_ALIGNMENT];
                    barrier.wait();
                    backend.append(&path, &[&bytes]).unwrap();
                });
            }
        });

        assert_eq!(
            path.metadata().unwrap().len(),
            (workers * DIRECT_IO_DEFAULT_ALIGNMENT) as u64
        );
        let _ = fs::remove_dir_all(root);
    }
}
