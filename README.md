# toy-cow-block-storage

Toy copy-on-write storage with block devices, native files, snapshots, PITR,
explicit GC, and a durable local provider.

The project is deliberately small and deterministic. Writes append immutable
segment data, copy only the metadata path that changed, and publish a new root.
Forks and snapshots copy root pointers instead of walking data. GC traces from
live and retained roots instead of relying on deep refcounts.

The first compatibility surface is a block device. Native keyspace/file APIs
live beside it for callers that can preserve file intent, append ownership,
writer epochs, and durable-but-not-yet-visible append data.

```text
Block callers             Native callers
     |                         |
     v                         v
BlockClient/Device       NativeKeyspace/File
     |                         |
     +----------+--------------+
                |
        Local coordinator
                |
     +----------+--------------+
     |                         |
Immutable metadata       Immutable segments
roots and timelines      and append logs
```

## What is implemented

- Block devices with aligned reads/writes, zeroing, discard, fork, restore,
  flush, delete, PITR, and GC retention.
- Native keyspaces and files with byte-oriented reads/writes, write batches,
  keyspace checkpoints, snapshots, restores, and append-stream fencing.
- Local in-process clients, servers, and transports over one coordinator.
- A durable local provider backed by SQLite metadata catalogs and rolled data
  logs.
- Explicit maintenance scheduling, compaction budgets, backpressure policy, and
  provider diagnostics.
- Deterministic simulation tests, provider conformance tests, Criterion
  mechanism benchmarks, and the `loadbench` integration runner.

The planned POSIX/FUSE path is a sibling mapping layer over the same substrate.
POSIX needs first-class inode, directory, rename, unlink, truncate, open-handle,
and fsync semantics.

## A tiny local store

Both public APIs can point at the same in-process coordinator:

```rust
use std::sync::Arc;

use toy_cow_block_storage::{
    BlockClient, BlockDevice, CreateDeviceRequest, CreateFileRequest,
    CreateKeyspaceRequest, DeviceSpec, FileSpec, InProcessBlockTransport,
    InProcessNativeTransport, LocalBlockClient, LocalBlockServer,
    LocalCoordinator, LocalNativeClient, LocalNativeServer, NativeFile,
    NativeKeyspaceClient,
};

fn tiny_store() -> toy_cow_block_storage::Result<()> {
    let store = LocalCoordinator::new();

    let blocks = LocalBlockClient::new(InProcessBlockTransport::new(Arc::new(
        LocalBlockServer::new(store.clone()),
    )));
    let native = LocalNativeClient::new(InProcessNativeTransport::new(Arc::new(
        LocalNativeServer::new(store),
    )));

    let device_id = blocks.create_device(CreateDeviceRequest {
        spec: DeviceSpec {
            logical_blocks: 1024,
            block_size: 4096,
        },
        name: Some("root".to_string()),
    })?;
    let device = blocks.open_device(device_id)?;
    device.write_at(0, &[7; 4096])?;

    let keyspace = native.create_keyspace(CreateKeyspaceRequest {
        name: Some("fs-root".to_string()),
    })?;
    let file_id = native.create_file(
        keyspace,
        CreateFileRequest {
            spec: FileSpec {
                name: Some("journal".to_string()),
            },
        },
    )?;
    let file = native.open_file(keyspace, file_id)?;
    file.write_at(0, b"hello world")?;

    Ok(())
}
```

## Block devices

The block API is shaped like a disk: fixed logical size, fixed block size, and
block-aligned byte ranges. It is the compatibility layer for disk-image
experiments, filesystem writeback models, and future block adapters.

Block writes are atomic at request granularity. Sparse and discarded ranges read
as zeroes. Forks are O(1): a child device initially shares the parent's shard
roots, then diverges only when either side writes.

```rust
use toy_cow_block_storage::{
    BlockClient, BlockDevice, DeviceId, ForkRequest, LocalBlockClient,
    RestorePoint,
};

fn fork_and_restore(
    blocks: &LocalBlockClient,
    device_id: DeviceId,
) -> toy_cow_block_storage::Result<()> {
    let device = blocks.open_device(device_id)?;
    let first = device.write_at(0, &[7; 4096])?;

    let fork_id = device.fork(ForkRequest {
        target: None,
        name: Some("child".to_string()),
    })?;
    let fork = blocks.open_device(fork_id)?;
    fork.write_zeroes(0, 4096)?;

    let restored_id = device.restore(RestorePoint::Commit(first.commit_seq))?;
    let restored = blocks.open_device(restored_id)?;

    let mut buf = vec![0; 4096];
    restored.read_at(0, &mut buf)?;
    assert_eq!(buf[0], 7);

    Ok(())
}
```

For filesystem-shaped dirty windows, use `commit_batch` and then `flush` so the
caller's dirty ranges become one atomic storage boundary. Plain random block
writes can legitimately conflict at high concurrency if they hit the same shard;
the `*-shard-lanes` loadbench rows are the happy-path throughput shape.

## Native keyspaces and files

The native API keeps file intent visible to the storage layer. A keyspace is the
coherent checkpoint, snapshot, and restore boundary. Files inside a keyspace
support byte-oriented reads, ordinary writes, write batches, and append streams.

This surface is for custom filesystems or direct applications that can express
file operations directly. File IDs are scoped by keyspace, and a snapshot or
restore creates a new keyspace lineage that initially shares immutable catalog
and file roots.

```rust
use toy_cow_block_storage::{
    FileId, KeyspaceId, LocalNativeClient, NativeFile, NativeKeyspaceClient,
    RestorePoint, SnapshotKeyspaceRequest,
};

fn snapshot_keyspace(
    native: &LocalNativeClient,
    keyspace: KeyspaceId,
    file_id: FileId,
) -> toy_cow_block_storage::Result<()> {
    let file = native.open_file(keyspace, file_id)?;

    file.write_at(0, b"hello world")?;
    let checkpoint = native.checkpoint_keyspace(keyspace)?;

    let snapshot = native.snapshot_keyspace(
        keyspace,
        SnapshotKeyspaceRequest {
            target: None,
            name: Some("before-overwrite".to_string()),
        },
    )?;

    file.write_at(0, b"goodbye!!!!")?;

    let snapshot_file = native.open_file(snapshot, file_id)?;
    let mut buf = vec![0; 11];
    snapshot_file.read_at(0, &mut buf)?;
    assert_eq!(buf.as_slice(), b"hello world");

    let restored = native.restore_keyspace(keyspace, RestorePoint::Checkpoint(checkpoint))?;
    let restored_file = native.open_file(restored, file_id)?;
    restored_file.read_at(0, &mut buf)?;
    assert_eq!(buf.as_slice(), b"hello world");

    Ok(())
}
```

`commit_batch` is the native shape for many client writes that should publish as
one visible file version. Native append workloads should keep their append
stream token, writer epoch, private durability mark, and visible publish
boundary intact.

## Append streams

Append streams are for very high-throughput sequential writes: the current
short-run durable matrix reaches roughly 5 GB/s on the largest ingest rows.
They split ingest into three explicit boundaries:

```text
append_stream         accepted private bytes
flush_append_stream   private durability for this stream token
publish_append_stream reader-visible file version
```

```rust
use toy_cow_block_storage::NativeFile;

fn append_flow(file: &impl NativeFile) -> toy_cow_block_storage::Result<()> {
    let stream = file.open_append_stream()?;

    file.append_stream(&stream, b"batch-0001\n")?;
    file.append_stream(&stream, b"batch-0002\n")?;

    let mark = file.flush_append_stream(&stream)?;

    // Readers observe the appended bytes after publish succeeds.
    let commit = file.publish_append_stream(&stream, &mark)?;
    assert_eq!(commit.range.len, mark.durable_through);

    Ok(())
}
```

A flush is private durability for the holder of the `AppendStream` bearer token.
Publish is the only globally discoverable append boundary. Failover that needs
unpublished bytes must persist both the stream token and `DurableAppendMark`
outside the storage layer; a replacement without that authority opens a fresh
stream from the latest visible file head.

## Durable local provider

The durable provider stores root metadata in SQLite and gives each storage node
its own SQLite catalog plus rolled data logs. Segment bytes and catalog receipts
commit before root metadata references them, so a failed metadata publish leaves
invisible orphan data for custodian cleanup instead of a half-visible write.

Acknowledged writes stay live in process until an explicit `flush` or stronger
write asks for stable storage. After restart, unflushed acknowledged bytes are
ignored; flushed writes are replayed from durable metadata and data logs.

```rust
use toy_cow_block_storage::{
    DurableCoordinator, MaintenancePolicy, WriteAdmission,
};

let store = DurableCoordinator::open_with_maintenance_policy(
    "/tmp/toy-cow",
    Default::default(),
    MaintenancePolicy::default(),
)?;

let _observation = store.observe_maintenance()?;
let plan = store.plan_maintenance()?;

if matches!(plan.admission, WriteAdmission::AcceptAndSchedule) {
    store.run_maintenance_tick()?;
}
```

For long-running local services, `MaintenanceMode::AlwaysOn` runs bounded
maintenance after writes or custodian work wake it. Keep the copy budget small
enough that one tick has predictable latency, and large enough that compaction
debt can fall during normal traffic.

## Performance snapshot

These are short-run durable checkpoints from this dev host with `200us` modeled
RTT. Treat them as local sanity numbers.

| Scenario | Workload shape | Result |
| --- | --- | --- |
| Verified native reads | `native-read-4k`, c16 | about 72.8k IOPS, p99 about 468 us |
| Block writeback fsync | `block-writeback-fsync-1m`, c16 | about 1.3 GB/s, p99 about 15 ms |
| Larger block fsync window | `block-writeback-fsync-4m`, c16 | about 1.9 GB/s, p99 about 40 ms |
| Append stream ingest | `native-stream-ingest-1m`, c16 | about 3.6 GB/s ingest; payload flush p99 around 15 ms in profile rows |
| Large append stream ingest | `native-stream-ingest-32m`, c16 | roughly 5 GB/s ingest in the current short-run matrix |
| Preflushed append publish | `native-stream-publish-preflushed-1m`, c16 | about 923 MB/s, p50 about 5.2 ms, p99 about 17 ms |

`cargo bench --bench regression` is the Criterion mechanism suite.
`loadbench` is the integration runner for public block/native API behavior,
modeled RTT, concurrency, latency percentiles, conflicts, and errors.

```sh
# Broad public API smoke.
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --workloads north-star \
  --rtt-us 200

# Filesystem-shaped block fsync windows.
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --storage-nodes 4 \
  --workloads block-writeback \
  --rtt-us 200

# Native append ingest, private durability, and publish boundaries.
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --files 128 \
  --workloads append-stream \
  --rtt-us 200 \
  --stream-flush-mib 2 \
  --stream-publish-mib 128
```

`success_iops` is successful operations per second. `mbps` is submitted payload
MB/s. Append stream rows also report `durable_mbps` and `published_mbps` because
private durability and reader visibility are different boundaries.

Useful workload aliases:

| Alias | Use it for |
| --- | --- |
| `north-star` | Broad block/native API comparison. |
| `append-batch` | Client-side append payload size effects. |
| `append-stream` | Private ingest, stream flush, and visible publish behavior. |
| `block-writeback` | Filesystem-style dirty window plus fsync behavior. |
| `block-metadata` | Same-shard conflicts versus different-shard/device convergence. |
| `native-file-batch` | Client-sized random-write commit boundaries. |
| `native-metadata` | Same-file pressure versus different-file keyspace lanes. |

## Development

Run Rust commands inside the Linux container from `docker-compose.yml`. Keep git
commands on the macOS host.

```sh
docker compose up -d dev
docker compose exec dev cargo test
docker compose exec dev cargo bench --bench regression -- --test
docker compose down
```

Full gate:

```sh
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test
```

Use `cargo bench --bench regression` without `-- --test` when you want Criterion
to record comparison data.

## Operational signals

Providers expose typed diagnostics through `ObservableProvider`; exporters can
map counters, gauges, node snapshots, and event kinds into their own metrics
system.

- `pending_orphan_segments`: payload reached storage but did not become visible.
- `maintenance_dirty_bytes` and `maintenance_reclaimable_bytes`: compaction
  debt and reclaim opportunity.
- `sqlite_wal_bytes`: metadata WAL pressure.
- `coordinator_write_publish_failures`: storage writes succeeded but metadata
  publish did not.
- `coordinator_write_unavailable`: writes were throttled or rejected by policy.
- `receipt_rejected_*`: proof, scope, epoch, or replay failures.

Events are bounded breadcrumbs. Long-lived truth comes from counters, gauges,
timelines, storage-node catalogs, and data logs.

## Doctrine

The project follows a "build it like NASA" workflow: small deterministic
modules, explicit invariants, exhaustive simulation, and no advancement to the
next layer until the current layer is boringly correct.

- Keep deterministic code free of hidden I/O, wall-clock reads, background work,
  and process-global randomness.
- Prefer pure state transitions shaped like `step(command) -> effects`.
- Keep immutable objects immutable: segments, metadata nodes, and committed
  roots are never mutated in place.
- Keep forks O(1) by copying root pointers only.
- Keep reclamation explicit: GC traces from committed roots and sweeps only
  unreachable objects.
- Add abstractions only when tests, simulations, benchmarks, or real duplication
  prove they are needed.

Read these before changing implementation code:

- [docs/cow-block-storage-design.md](docs/cow-block-storage-design.md)
- [docs/implementation-plan.md](docs/implementation-plan.md)
- [AGENTS.md](AGENTS.md)
