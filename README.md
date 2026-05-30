# toy-cow-block-storage

Deterministic toy copy-on-write storage built in small, correctness-gated
phases. It exposes a block device API for compatibility and a native
keyspace/file API for callers that can speak in files, appends, snapshots, and
writer fencing directly.

The simple performance rule is: when you are writing a large stream, batch on
the caller side and append a large buffer at once. One large append becomes one
logical segment and one file-version transition, which is the shape you want
for both write cost and later sequential reads.

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
  per-file snapshots. Appends are byte-oriented and fenced by append leases.

The local implementation runs in one process, but the API already goes through
server and transport boundaries so durable or remote implementations can replace
the adapters later.

### Which One Should I Use?

Use the block API when you want ordinary disk-like behavior, such as a future
`ublk` adapter or an existing filesystem layered over fixed-size blocks.

Use the native API when you control the caller and can give the storage layer
more intent. It keeps snapshots at the keyspace level, rejects stale append
writers, supports ordinary file writes, and lets high-throughput callers batch
large appends without creating thousands of tiny file-version commits.

The planned POSIX/FUSE path is a third sibling layer, not a wrapper around
`NativeFile`. POSIX needs first-class inode, directory, rename, unlink,
truncate, open-handle, and fsync semantics while still reusing the same segment
substrate and commit machinery.

```rust
use std::sync::Arc;

use toy_cow_block_storage::{
    InProcessBlockTransport, InProcessNativeTransport, LocalBlockClient,
    LocalBlockServer, LocalNativeClient, LocalNativeServer, LocalCoordinator,
};

fn local_clients() -> (LocalBlockClient, LocalNativeClient) {
    let store = LocalCoordinator::new();

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
    InProcessBlockTransport, LocalBlockClient, LocalBlockServer, LocalCoordinator,
    RestorePoint,
};

fn block_device_flow() -> toy_cow_block_storage::Result<()> {
    let store = LocalCoordinator::new();
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
    LocalNativeClient, LocalNativeServer, LocalCoordinator, NativeFile,
    NativeKeyspaceClient, RestorePoint, SnapshotKeyspaceRequest,
};

fn native_keyspace_flow() -> toy_cow_block_storage::Result<()> {
    let store = LocalCoordinator::new();
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
- A successful non-empty file write publishes a new immutable keyspace catalog
  root.
- Snapshot and restore copy retained keyspace-root pointers rather than walking
  file contents.

## Durable Maintenance

The durable local provider stores logical metadata in root SQLite and gives
each storage node its own SQLite catalog plus rolled data logs. Those catalogs
are independent durability boundaries: segment bytes and the node's catalog
receipt commit before root metadata references them, so a failed metadata
publish leaves an invisible orphan for cleanup instead of a half-visible write.
Maintenance is explicit by default: callers can observe dirty/reclaimable
bytes, ask the deterministic scheduler for a plan, and run one bounded tick.

```rust
use toy_cow_block_storage::{
    DurableCoordinator, MaintenanceMode, MaintenancePolicy, WriteAdmission,
};

let store = DurableCoordinator::open_with_maintenance_policy(
    "/tmp/toy-cow",
    Default::default(),
    MaintenancePolicy::default(), // Manual mode by default.
)?;

let observation = store.observe_maintenance()?;
let plan = store.plan_maintenance()?;
if matches!(plan.admission, WriteAdmission::AcceptAndSchedule) {
    store.run_maintenance_tick()?;
}
```

For a long-running local process, opt into automatic bounded compaction by
using `AlwaysOn`. The worker only runs after writes or custodian work wake it,
and each tick is still limited by the policy:

```rust
let mut policy = MaintenancePolicy::default();
policy.mode = MaintenanceMode::AlwaysOn;
policy.write_backpressure_enabled = true;
policy.dirty_low_watermark_bytes = 64 * 1024 * 1024;
policy.dirty_high_watermark_bytes = 256 * 1024 * 1024;
policy.compaction_copy_budget_per_tick = 32 * 1024 * 1024;

let store = DurableCoordinator::open_with_maintenance_policy(
    "/tmp/toy-cow",
    Default::default(),
    policy,
)?;
```

When picking settings, think in plain storage terms:

- Use `Manual` for deterministic tests and tools that want to schedule their
  own maintenance. Use `AlwaysOn` for services that should steadily clean up in
  the background. `Opportunistic` is the middle ground: writes can run one
  bounded maintenance tick before they proceed.
- Larger data logs are better for sequential write throughput and fewer files;
  smaller data logs make reclaim work more granular.
- The low dirty watermark says "start cleaning soon"; the high watermark says
  "slow callers down" when `write_backpressure_enabled` is true.
- Keep the copy budget small enough that one tick has predictable latency, and
  large enough that debt can actually fall during normal traffic.
- PITR retention and custodian release timing decide what is truly reclaimable.
  Compaction will not delete bytes still protected by retained history.

If write backpressure is enabled and pressure crosses a configured hard
maintenance limit, durable writes return `StorageError::Unavailable` with a
stable reason. Reads, forks, snapshots, restores, and flush semantics do not
change.

## Operational Signals

The provider exposes typed diagnostics through `ObservableProvider`; it does
not bake in Prometheus, OpenTelemetry, or a hosted metrics shape. Exporters can
poll `diagnostics_snapshot()` and `drain_events(max)` and translate the stable
counters, gauges, node snapshots, and event kinds into whatever system your
service uses.

```rust
use toy_cow_block_storage::{ObservableProvider, StorageEventKind};

let snapshot = store.diagnostics_snapshot()?;
let recent = store.drain_events(64)?;

if snapshot.gauges.maintenance_reclaimable_bytes > 0 {
    // Schedule bounded maintenance when your service is ready for the work.
}

assert!(recent
    .iter()
    .all(|event| event.kind != StorageEventKind::MetadataPublishFailed));
```

The most useful signals during normal operation are:

- `pending_orphan_segments`: bytes reached storage but did not become visible
  yet. This should usually fall after custodian work.
- `maintenance_dirty_bytes` and `maintenance_reclaimable_bytes`: compaction
  debt. Rising debt means PITR, release evidence, or copy budgets may need
  attention.
- `sqlite_wal_bytes`: metadata WAL pressure. Data-log compaction and SQLite
  maintenance are separate knobs.
- `coordinator_write_publish_failures`: storage writes succeeded but metadata
  publish did not. The data should stay invisible and become reclaimable.
- `coordinator_write_unavailable`: writes were throttled or rejected by policy.
  Treat the stable reason on recent events as the operator-facing cause.
- `receipt_rejected_*`: proof, scope, epoch, or replay failures. These are the
  first things to inspect when introducing remote or trusted-client transports.

Events are bounded breadcrumbs, not durable history. If the ring buffer
overwrites unread events, `observability_events_dropped` increases. Long-lived
truth should come from counters, gauges, metadata timelines, storage-node
catalogs, and data logs.

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

For final happy-path integration performance expectations, use the load
benchmark runner rather than Criterion microbenchmarks:

```sh
cargo run --release --bin loadbench -- --provider local --duration-ms 1000 --warmup-ms 200 --concurrency 1,4,16,64 --files 1024 --storage-nodes 4
cargo run --release --bin loadbench -- --provider local --duration-ms 1000 --warmup-ms 200 --concurrency 1,16,64 --files 1024 --storage-nodes 4 --rtt-us 200
cargo run --release --bin loadbench -- --provider durable --durability ack-flush:64 --duration-ms 1000 --warmup-ms 100 --concurrency 1,4,16 --files 128 --storage-nodes 4 --workloads block-write-4k,native-write-4k,native-append-4k
cargo run --release --bin loadbench -- --provider durable --durability ack-flush:64 --duration-ms 1000 --warmup-ms 100 --concurrency 1,4,16 --files 1024 --storage-nodes 4 --workloads append-batch --rtt-us 200
```

`loadbench` is the north-star integration benchmark: it exercises the public
block and native APIs through the current provider paths, reports IOPS,
throughput, latency percentiles, and conflicts/errors, and can model a fixed
RTT between service boundaries. Treat its output as the current implementation's
happy-environment performance baseline, while Criterion remains the narrow
regression suite for individual mechanisms.

The opt-in `append-batch` workload suite compares `native-append-4k`,
`native-append-1m`, `native-append-4m`, and `native-append-32m` against matching
`native-write-*` controls. Use it to diagnose client-side batching effects: if
append throughput rises with payload size, fixed per-append overhead dominates;
if large append remains much slower than same-size write-at, append
lease/publish semantics dominate; if both plateau low, the durable data-log or
catalog persistence path is the bottleneck. Large durable write controls can
retain substantial in-flight segment bytes before an `ack-flush:N` boundary; on
memory-constrained Docker hosts, reduce `--files`, concurrency, or workload
size for exploratory runs.
