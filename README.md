# toy-cow-block-storage

Deterministic toy copy-on-write storage, built in small correctness-gated
phases. It is still a toy, but the shape is intentionally serious: immutable
segments, cheap forks, point-in-time restore, explicit garbage collection, and
APIs that can grow from local in-process storage to remote storage servers
without changing how callers think about data.

There are two main ways to use it:

- Use the **block device API** when you want something that looks like a normal
  disk.
- Use the **native keyspace/file API** when your application can speak in files,
  appends, snapshots, and writer fencing directly.

The native path is where higher-performance custom storage ideas live. For
example, if you are writing a large file, you can reserve a large append extent,
fill it in chunks, and commit the whole thing as one segment instead of paying
metadata cost for thousands of tiny appends.

## Two Public APIs

The crate exposes two caller-facing APIs over the same copy-on-write segment and
metadata substrate:

- **Block storage**: `BlockClient` and `BlockDevice`, shaped like a normal block
  device. Reads, writes, zeroing, and discard are block-aligned. Forks and PITR
  restores create new device heads that share immutable roots until writes
  diverge.
- **Native keyspace/files**: `NativeKeyspaceClient` and `NativeFile`, shaped for
  custom filesystems or append-heavy users. Files live inside a keyspace so
  checkpoint, snapshot, and restore are filesystem-level operations, not
  per-file snapshots. Appends are byte-oriented, fenced by append leases, and
  can use large append reservations when the caller wants one big committed
  extent.

The local implementation runs in one process, but the API already goes through
server and transport boundaries so durable or remote implementations can replace
the adapters later.

### Which One Should I Use?

Use the block API for compatibility. It is the right shape for a future `ublk`
adapter, for experiments with ext4/xfs on top, or for tests that just need
fixed-size logical blocks.

Use the native API when you control the caller and can give the storage layer
more intent. It keeps snapshots at the keyspace level, rejects stale append
writers, supports ordinary file writes, and has an explicit fast path for large
append-heavy files.

The practical rule of thumb is:

- **Small or irregular edits**: use `write_at` or normal leased appends.
- **Large streaming files**: reserve a large aligned piece, fill it in chunks,
  commit it, then repeat until the file is done.
- **Filesystem-like state**: put related files in one keyspace so snapshot and
  restore happen at the filesystem boundary.
- **Existing filesystem compatibility**: use the block API and let the
  filesystem above it decide its own layout.

```rust
use std::sync::Arc;

use toy_cow_block_storage::{
    InProcessBlockTransport, InProcessNativeTransport, LocalBlockClient,
    LocalBlockServer, LocalNativeClient, LocalNativeServer, LocalObjectStore,
};

fn local_clients() -> (LocalBlockClient, LocalNativeClient) {
    let store = LocalObjectStore::new();

    let block_client = LocalBlockClient::new(InProcessBlockTransport::new(Arc::new(
        LocalBlockServer::new(store.clone()),
    )));
    let native_client = LocalNativeClient::new(InProcessNativeTransport::new(Arc::new(
        LocalNativeServer::new(store),
    )));

    (block_client, native_client)
}
```

### Block Device Flow

Use the block API when the caller wants ordinary block-device semantics, such as
a future `ublk` adapter or a normal filesystem layered above fixed-size blocks.

```rust
use std::sync::Arc;

use toy_cow_block_storage::{
    BlockClient, BlockDevice, CreateDeviceRequest, DeviceSpec, ForkRequest,
    InProcessBlockTransport, LocalBlockClient, LocalBlockServer, LocalObjectStore,
    RestorePoint,
};

fn block_device_flow() -> toy_cow_block_storage::Result<()> {
    let store = LocalObjectStore::new();
    let block_client = LocalBlockClient::new(InProcessBlockTransport::new(Arc::new(
        LocalBlockServer::new(store),
    )));

    let device_id = block_client.create_device(CreateDeviceRequest {
        spec: DeviceSpec {
            logical_blocks: 1024,
            block_size: 4096,
        },
        name: Some("root".to_string()),
    })?;

    let device = block_client.open_device(device_id)?;

    let first_write = device.write_at(0, &[7; 4096])?;

    let mut block = vec![0; 4096];
    device.read_at(0, &mut block)?;
    assert_eq!(block[0], 7);

    let fork_id = device.fork(ForkRequest {
        target: None,
        name: Some("child".to_string()),
    })?;
    let fork = block_client.open_device(fork_id)?;

    fork.write_zeroes(0, 4096)?;

    device.write_at(0, &[9; 4096])?;
    device.read_at(0, &mut block)?;
    assert_eq!(block[0], 9);

    let restored_id = device.restore(RestorePoint::Commit(first_write.commit_seq))?;
    let restored = block_client.open_device(restored_id)?;
    restored.read_at(0, &mut block)?;
    assert_eq!(block[0], 7);

    Ok(())
}
```

Block API guarantees to care about:

- Public reads and writes are block-aligned and bounded by `DeviceSpec`.
- A successful write is atomic from the caller's perspective.
- Sparse ranges read as zeroes.
- Fork is O(1): it copies root pointers, not data or leaves.
- Restore creates a new device at a retained point in time.
- Delete removes the live head but does not imply immediate physical free.

### Native Keyspace/File Flow

Use the native API when the caller wants file/keyspace intent preserved by the
storage layer: byte-oriented writes, append leases, stale-writer rejection, and
filesystem-level snapshots. A keyspace snapshot is the native API's fork-like
operation: it creates a new keyspace lineage that initially shares immutable
catalog and file roots.

```rust
use std::sync::Arc;

use toy_cow_block_storage::{
    CreateFileRequest, CreateKeyspaceRequest, FileSpec, InProcessNativeTransport,
    LocalNativeClient, LocalNativeServer, LocalObjectStore, NativeFile,
    NativeKeyspaceClient, RestorePoint, SnapshotKeyspaceRequest,
};

fn native_keyspace_flow() -> toy_cow_block_storage::Result<()> {
    let store = LocalObjectStore::new();
    let native_client = LocalNativeClient::new(InProcessNativeTransport::new(Arc::new(
        LocalNativeServer::new(store),
    )));

    let keyspace_id = native_client.create_keyspace(CreateKeyspaceRequest {
        name: Some("fs-root".to_string()),
    })?;

    let file_id = native_client.create_file(
        keyspace_id,
        CreateFileRequest {
            spec: FileSpec {
                name: Some("journal".to_string()),
            },
        },
    )?;

    let file = native_client.open_file(keyspace_id, file_id)?;
    file.write_at(0, b"hello world")?;

    let mut bytes = vec![0; 11];
    file.read_at(0, &mut bytes)?;
    assert_eq!(bytes.as_slice(), b"hello world");

    let checkpoint = native_client.checkpoint_keyspace(keyspace_id)?;

    let snapshot_id = native_client.snapshot_keyspace(
        keyspace_id,
        SnapshotKeyspaceRequest {
            target: None,
            name: Some("before-overwrite".to_string()),
        },
    )?;

    file.write_at(0, b"goodbye!!!!")?;

    file.read_at(0, &mut bytes)?;
    assert_eq!(bytes.as_slice(), b"goodbye!!!!");

    // The snapshot keyspace still sees the original file bytes.
    let snapshot_file = native_client.open_file(snapshot_id, file_id)?;
    let mut snapshot_bytes = vec![0; 11];
    snapshot_file.read_at(0, &mut snapshot_bytes)?;
    assert_eq!(snapshot_bytes.as_slice(), b"hello world");

    let restored_id = native_client.restore_keyspace(
        keyspace_id,
        RestorePoint::Checkpoint(checkpoint),
    )?;

    let restored_file = native_client.open_file(restored_id, file_id)?;
    let mut restored_bytes = vec![0; 11];
    restored_file.read_at(0, &mut restored_bytes)?;
    assert_eq!(restored_bytes.as_slice(), b"hello world");

    Ok(())
}
```

Native API guarantees to care about:

- Keyspaces are the snapshot and restore boundary.
- File IDs are scoped by keyspace.
- Writes and appends are byte-oriented and committed as file-version
  transitions.
- Append leases carry writer epochs so stale writers fail without partial file
  visibility.
- Large append reservations let a caller fill an exact byte range in chunks and
  commit it as one logical segment.
- A successful non-empty file write publishes a new immutable keyspace catalog
  root.
- Snapshot and restore copy retained keyspace-root pointers rather than walking
  file contents.

### Large Append Reservations

Normal appends are convenient and stay the right default for small payloads. But
if your workload is "write this large file from a stream," doing thousands of
tiny appends is the slow path: every append is its own file-version transition.

The reservation API gives the caller a cleaner move:

1. Pick a large piece size, such as 32 MiB.
2. Acquire an append lease.
3. Reserve exactly that many bytes.
4. Fill the reservation in whatever chunks the producer naturally gives you.
5. Commit once.
6. Repeat for the next piece.

In v1, the file size and reservation length must be block aligned. If the final
tail is not aligned, use the normal append path for that tail.

```rust
use toy_cow_block_storage::{NativeFile, WriteDurability};

fn append_large_piece(
    file: &impl NativeFile,
    piece: &[u8],
) -> toy_cow_block_storage::Result<()> {
    // V1 reservations require an aligned length. A caller can keep a small
    // unaligned tail and write it later with a normal append.
    assert_eq!(piece.len() % 4096, 0);

    let lease = file.acquire_append()?;
    let reservation = file.reserve_append_extent(lease, piece.len() as u64)?;

    for (index, chunk) in piece.chunks(4 * 1024 * 1024).enumerate() {
        let offset = (index * 4 * 1024 * 1024) as u64;
        file.fill_reserved_append(reservation.clone(), offset, chunk)?;
    }

    file.commit_reserved_append(reservation, WriteDurability::Flushed)?;
    Ok(())
}
```

This does not expose storage nodes or physical offsets. The caller gets the
performance shape it asked for, while placement, replication, and cleanup stay
inside the provider.

## Phase Gates

Run these before advancing past the project harness and public contract phases:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps
cargo bench --bench regression
```

The Criterion benchmarks start as tiny regression baselines for API validation
and deterministic test utilities. Later phases should add read, write, fork,
PITR, and GC benchmarks before optimizing those paths.

Criterion reports performance movement; it does not make `cargo bench` fail
solely because a benchmark regressed. Treat the output as regression detection
signal until the project adds an explicit CI comparison step.
