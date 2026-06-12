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
stream token, writer epoch, accepted private tail, and visible publish boundary
intact.

## Append streams

Append streams are for very high-throughput sequential writes. They split ingest
from visibility while leaving payload persistence policy inside the storage
server:

```text
append_stream                    accepted private bytes
submit/wait append publish        durable, reader-visible prefix
release_append_stream             explicit lease release
abort_append_stream               discard unpublished private tail and release
```

```rust
use toy_cow_block_storage::NativeFile;

fn append_flow(file: &impl NativeFile) -> toy_cow_block_storage::Result<()> {
    let stream = file.open_append_stream()?;

    file.append_stream(&stream, b"batch-0001\n")?;
    let ticket = file.append_stream(&stream, b"batch-0002\n")?;

    let publish_through = ticket.range.end_exclusive()?;

    // Readers observe the appended bytes after publish succeeds. Publish is
    // also the public restart-durability boundary for this stream prefix.
    let commit = file.publish_append_stream(&stream, publish_through)?;
    assert_eq!(commit.range.end_exclusive()?, publish_through);

    file.release_append_stream(&stream)?;

    Ok(())
}
```

Publish is the only public durability and globally discoverable append boundary.
It captures a prefix and may persist not-yet-durable bytes internally before
committing the visible metadata. Publish does not release the stream token, so a
writer can keep appending and publish later prefixes; `release_append_stream`
ends the lease explicitly. A replacement writer without the active stream token
opens a fresh stream from the latest visible file head, and any unpublished
private tail from the old stream is ignored after restart. Durable providers may
auto-persist active stream prefixes before publish to reduce the dirty tail a
later publish must wait for; that is an internal latency policy and does not make
unpublished bytes visible or publicly recoverable.

## Durable local provider

The durable provider stores root metadata in SQLite and gives each storage node
its own SQLite catalog plus rolled data logs. Segment bytes and catalog receipts
commit before root metadata references them, so a failed metadata publish leaves
invisible orphan data for custodian cleanup instead of a half-visible write.

For block writes and ordinary native file writes, acknowledged bytes stay live in
process until an explicit `flush` or stronger write asks for stable storage.
After restart, unflushed acknowledged bytes are ignored; flushed writes and
published append-stream prefixes are replayed from durable metadata and data
logs.

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

These are short-run checkpoints from this dev host. The loadbench rows include
`200us` modeled RTT; the fio controls are local filesystem runs inside the same
dev container without modeled network delay. Treat them as local sanity numbers,
not as portable hardware claims. Most append rows below use `200us` modeled RTT;
the compact native-file delta sanity rows are labeled separately at `0us`.
Append publish rows should be compared with `published_mbps`, because that
measures bytes that became visible and restart-durable inside the timed window.

| Scenario | Workload shape | Result |
| --- | --- | --- |
| Verified native reads | `native-read-4k`, c16 | about 72.8k IOPS, p99 about 468 us |
| Block writeback fsync | `block-writeback-fsync-1m`, c16 | about 1.7 GB/s, p99 about 20 ms |
| Larger block fsync window | `block-writeback-fsync-4m`, c16 | about 2.3 GB/s, p99 about 51 ms |
| Append stream ingest | `native-stream-ingest-32m`, c16 | accepted private throughput peaks around 6.1 GB/s in the latest local sweep; this is not a visible durability result |
| Publish at end, 1 MiB appends | `native-stream-publish-at-end-1m`, c16, 1024 MiB/worker | `published_mbps` about 2.77 GB/s with 1 node, 3.34 GB/s with 4 nodes, 3.31 GB/s with 16 nodes |
| Publish at end, 4 MiB appends | `native-stream-publish-at-end-4m`, c16, 1024 MiB/worker | `published_mbps` about 2.51 GB/s with 1 node, 3.49 GB/s with 4 nodes, 3.17 GB/s with 16 nodes |
| Publish at end, 32 MiB appends | `native-stream-publish-at-end-32m`, c16, 1024 MiB/worker | `published_mbps` about 2.16 GB/s with 1 node, 3.18 GB/s with 4 nodes, 1.76 GB/s with 16 nodes |
| Local FS, no fsync | `fio`, buffered writes, 16 jobs, 4 MiB writes, 1024 MiB/job | about 5.77 GB/s write-phase bandwidth |
| Local FS, fsync at end | `fio`, buffered writes plus `--end_fsync=1`, same shape | about 3.49 GB/s write-phase bandwidth |
| Local FS, direct with fsync at end | `fio`, direct writes plus `--end_fsync=1`, same shape | about 3.82 GB/s write-phase bandwidth |

Block journal group-commit results from June 11, 2026. Every durable block
boundary now joins one group-committed journal lane, payloads above the inline
cap stage on per-node data logs in parallel, and acknowledged segment-ref
payloads sync at the covering flush boundary. Local Docker A/B against the
prior head, `block-batch` with one random write plus a durable boundary per
op, `--rtt-us 0`, 4 storage nodes, 3-second windows:

| Shape | Before | After |
| --- | --- | --- |
| 4k `ack-flush:1`, c16/c32 | 3.2k / 3.2k IOPS, p99 7.0 / 12.5 ms | 11.3k / 11.1k IOPS, p99 2.6 / 6.3 ms |
| 64k `ack-flush:1`, c16/c32 | 142 / 154 MB/s | 466 / 471 MB/s |
| 1m `flushed`, c16/c32 | 546 / 517 MB/s, p99 39 / 100 ms | 1683 / 1710 MB/s, p99 19 / 35 ms |
| 32m `flushed`, c16/c32 | 1098 / 945 MB/s | 2227 / 1914 MB/s |

The same change held or improved the north-star, `block-writeback`, and
`durable-publish` aliases in a binary A/B on this host; no row regressed more
than run-to-run noise, and `block-writeback-fsync-1m` c16 went from about
0.56 GB/s to 1.73 GB/s at `200us` modeled RTT.

GCP block-vs-RBD rerun from June 11, 2026, on `c4-standard-32-lssd` with five
local NVMe SSDs: microceph Squid RBD (single node, `pool size 1`, fio librbd
randwrite with `direct=1`) against `loadbench block-batch` on the same disks
with 4 storage nodes and one random durable write per op. Toy rows are medians
of two repeats at `--rtt-us 0`. Before is the prior head on the identical
matrix, which ran `ack-flush:1`; ratios are toy/Ceph throughput.

| Shape | Ceph RBD | Toy before | Toy after `ack-flush:1` | Toy after `flushed` |
| --- | --- | --- | --- | --- |
| 4k c16 | 120 MB/s | 12 MB/s (0.10x) | 66 MB/s (0.55x) | 139 MB/s (1.15x) |
| 4k c32 | 208 MB/s | 12 MB/s (0.05x) | 64 MB/s (0.31x) | 156 MB/s (0.75x) |
| 64k c32 | 1277 MB/s | 159 MB/s (0.11x) | 318 MB/s (0.25x) | 348 MB/s (0.27x) |
| 256k c32 | 1463 MB/s | 339 MB/s (0.22x) | 334 MB/s (0.23x) | 335 MB/s (0.23x) |
| 1m c4 | 1213 MB/s | 348 MB/s (0.29x) | 1285 MB/s (1.06x) | 1248 MB/s (1.03x) |
| 1m c32 | 1531 MB/s | 346 MB/s (0.23x) | 1299 MB/s (0.85x) | 1320 MB/s (0.86x) |
| 32m c4 | 1159 MB/s | 647 MB/s (0.55x) | 1112 MB/s (0.96x) | 1187 MB/s (1.02x) |
| 32m c32 | 1461 MB/s | 548 MB/s (0.35x) | 1126 MB/s (0.77x) | 1099 MB/s (0.75x) |

Fully durable toy writes now beat or tie Ceph RBD at 4k up to c16 and at 1m
and 32m mid-concurrency, and hold 0.75-0.86x at c32 for the large sizes. The
remaining gap is concentrated at 64k-256k high concurrency: the segment-ref
path tops out near 1.3k ops/s regardless of payload size, and the inline path
near 350 MB/s of journal bandwidth. Raw artifacts live in
`infra/gcp-local-nvme-bench/results/gbvr-lane-06112011/`.

Compact native-file delta sanity from June 10, 2026, after the native batch
delta/replay change. These rows are local Docker runs with `--rtt-us 0`,
`--concurrency 16`, one durable storage node, 1024 files, and one-second timed
windows:

| Shape | Result | Read |
| --- | --- | --- |
| `native-file-batch-4k-16ops`, durable ack | 30.4k batch IOPS, `published_mbps` 1.99 GB/s, p50/p90/p99 0.49/0.87/1.31 ms | Positive tiny-I/O signal; the same local shape before the verified-receipt cache was about 20.9k batch IOPS with p99 about 1.77 ms. |
| `native-mixed-append-batch-4k-16ops`, durable ack | `published_mbps` 0.69 GB/s, overall p99 0.83 ms | Overall samples are dominated by native batch ops; append publish had only 8 samples and a noisy 2.62 s p99 in this short run. |
| `native-mixed-append-batch-4k-16ops`, durable flushed | `published_mbps` 27.6 MB/s, overall p99 1.05 s; append p99 1.90 ms, publish p99 1.05 s | Still unresolved. Flushed mixed tiny writes force append publish to wait for compact-delta ordering/folding, so this is the next architecture bottleneck rather than a device-throughput result. |

Read the append/fio comparison as two separate pairs:

| Question | Native row | Local filesystem control | Interpretation |
| --- | --- | --- | --- |
| How fast is accepted append ingest before a visibility/durability boundary? | `native-stream-ingest-32m`, `mbps` about 6.1 GB/s, with `200us` modeled RTT | fio buffered no-fsync, about 5.77 GB/s, no modeled RTT | Hot-path diagnostic only. Native accepted bytes are private and are not yet reader-visible or restart-durable. |
| How fast is append-all-then-durable? | `native-stream-publish-at-end-4m`, `published_mbps` about 3.49 GB/s at 4 storage nodes/c16, with `200us` modeled RTT | fio buffered writes plus end fsync, about 3.49 GB/s write-phase bandwidth, no modeled RTT | Closest durability-shape comparison. Native publish makes the prefix visible and restart-durable; fio fsync makes the file data durable at job end. |

The fio control was run inside the same dev container on `/tmp` and cleaned up
afterward. Its JSON write bandwidth does not isolate the final fsync as a
separate phase, while shell wall timing includes process setup and cleanup, so
read it as a rough local filesystem bracket rather than an exact phase-by-phase
accounting.

Fresh at-end/fio check from June 6, 2026, after the append publish tail-latency
work, using 4 MiB writes, 512 MiB per worker, 4 storage nodes for native rows,
`--stream-auto-persist-mib 64`, and `200us` modeled RTT:

| Shape | Throughput | Tail |
| --- | --- | --- |
| Native `native-stream-publish-at-end-4m`, c16 | `published_mbps` 3.51-3.63 GB/s | publish p99 1.27-1.88 s |
| Native `native-stream-publish-at-end-4m`, c32 | `published_mbps` 3.49-3.65 GB/s | publish p99 1.85-2.23 s |
| Native `native-stream-publish-barrier-at-end-4m`, c32 | `published_mbps` 3.70 GB/s | publish p99 1.52 s |
| fio buffered `--end_fsync=1`, c16/c32 | 1.70 / 2.43 GB/s | fio did not expose end-fsync latency separately |
| fio direct `--end_fsync=1`, c16/c32 | 2.10 / 3.27 GB/s | fio did not expose end-fsync latency separately |
| fio buffered `--fsync=128`, c16/c32 | 3.98 / 5.80 GB/s | write p99 73 / 101 ms, write max 145 ms / 1.10 s |

For that run, native publish-at-end throughput is fio-end-fsync class, but
native publish p99 is still higher than the fio `--fsync=128` write-latency
proxy. fio reported an empty sync histogram for these shapes, so the fio tail
numbers are only a rough proxy for final durability latency.

External Rapid Storage spot checks from June 6, 2026, using same-zone VMs and
Rapid Storage buckets in `us-central1-a`, with 512 MiB per worker. For GCS
appendable objects, `Flush()` is the closest comparison to native
publish-at-end because the object remains appendable after the boundary;
`Close()` is a separate release/finalization-shaped boundary. The first run used
`c3-standard-22`, and the follow-up used `c3-standard-88` with gVNIC and per-VM
Tier 1 networking. The Rapid runs were not run under the local loadbench
`--rtt-us 200` assumption: a same-shape TCP-connect probe to
`storage.googleapis.com:443`, resolving DNS outside the timed samples, measured
about `0.303/0.663 ms` p50/p99 at c1, `0.396/0.832 ms` at c16, and
`1.226/3.616 ms` at c64.

| Shape | Throughput | Boundary tail |
| --- | --- | --- |
| Rapid c3-22 `at-end`, 4 MiB appends, c16/c32 | 0.55 / 1.92 GiB/s | flush p99 110 / 73 ms |
| Rapid c3-22 `at-end`, 32 MiB appends, c16/c32 | 1.26 / 2.27 GiB/s | flush p99 86 / 114 ms |
| Rapid c3-88 Tier1 `at-end`, 4 MiB appends, c16/c32/c64 | 1.19 / 4.30 / 7.75 GiB/s | flush p99 32 / 20 / 28 ms |
| Rapid c3-88 Tier1 `at-end`, 32 MiB appends, c16/c32/c64 | 1.84 / 4.87 / 9.35 GiB/s | flush p99 109 / 121 / 122 ms |
| Rapid c3-88 Tier1 `interval`, 4 MiB appends, c16/c32/c64 | 2.60 / 6.09 / 6.95 GiB/s | flush p99 20 / 20 / 46 ms |
| Rapid c3-88 Tier1 `interval`, 32 MiB appends, c16/c32/c64 | 3.64 / 5.64 / 7.41 GiB/s | flush p99 80 / 88 / 126 ms |
| Rapid c3-88 Tier1 `close-at-end`, 4 MiB appends, c16/c32/c64 | 3.25 / 5.11 / 6.53 GiB/s | close p99 19 / 24 / 33 ms |
| Rapid c3-88 Tier1 `close-at-end`, 32 MiB appends, c16/c32/c64 | 2.90 / 5.75 / 7.74 GiB/s | close p99 142 / 114 / 167 ms |

The c3-22 Rapid run was VM-network-limited for throughput: that shape has a
documented default egress ceiling of up to `23 Gbps`, roughly `2.68 GiB/s`
before protocol overhead, and the fastest rows reached about `2.42 GiB/s`.
The c3-88/Tier1 rerun raised the documented ceiling to up to `100 Gbps`, or
roughly `11.6 GiB/s`, and publish-at-end throughput reached about `9.35 GiB/s`
while publish p99 stayed in the tens-to-low-hundreds of milliseconds range. Raw
Rapid artifacts live in `infra/gcp-rapidstorage-bench/results/`.

The TCP probe is the service-endpoint RTT proxy. The separate
`rapid-latency-c3-88-tier1.csv` artifact measures metadata and tiny-flush API
operations; those rows are useful service-operation context but are not raw
network RTT.

Treat the c3-88/Tier1 Rapid rows as an external north-star for native durable
append publish, but the June 7, 2026 GCP local-NVMe results show the native hot
path can now reach the same throughput class. The first C4 run used
`c4-standard-192-lssd` in `us-east1-b`, 32 local SSDs, 512 MiB per worker,
32 MiB appends, 128 MiB publish boundaries, concurrency `32,64`, and modeled
RTTs `0us`, `200us`, `700us`, and `3600us`.

| Layout | Best `published_mbps` | Median `published_mbps` | Publish p99 median | Publish p99 max | Append p99 median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared 32-disk RAID0 for data and journal | 11.02 GB/s | 10.86 GB/s | 82.9 ms | 595.3 ms | 302.5 ms |
| 31-disk RAID0 data plus 1 dedicated journal SSD | 10.65 GB/s | 10.45 GB/s | 12.4 ms | 55.8 ms | 334.2 ms |
| 31 one-disk storage nodes plus 1 dedicated journal SSD | 7.40 GB/s | 5.92 GB/s | 8.4 ms | 19.7 ms | 253.6 ms |
| 4 private storage-node RAID0 groups `8/8/8/7` plus 1 journal SSD | 6.65 GB/s | 6.36 GB/s | 16.8 ms | 54.3 ms | 507.6 ms |

The best deployment layout from these runs is the split-journal shape: shared
RAID0 across 31 data SSDs, plus one isolated append-visible journal SSD. It
keeps the 10 GB/s-class data path while preventing journal syncs from queuing
behind saturated data writes. The old shared-RAID layout reaches slightly
higher peak throughput, but publish p99 is dominated by append-visible journal
`sync_data` waiting behind the data stream.

The `8/8/8/7` private RAID experiment did not recover the wide-RAID bandwidth.
Its four private RAID groups were near 100 percent utilization during hot rows,
but aggregate active write telemetry only reached about `6.3 GB/s` median and
`7.0 GB/s` max. The dedicated journal disk stayed mostly idle, so the private
RAID result is not a journal bottleneck.

The split-journal c32 versus c64 rows show that c64 mostly adds queue depth once
the data path is saturated:

| RTT | Workload | c32 throughput / publish p99 | c64 throughput / publish p99 |
| ---: | --- | ---: | ---: |
| `200us` | at-end 32 MiB | 10.50 GB/s / 5.9 ms | 10.65 GB/s / 14.4 ms |
| `200us` | interval 32 MiB | 10.37 GB/s / 4.9 ms | 10.64 GB/s / 11.3 ms |
| `700us` | at-end 32 MiB | 10.45 GB/s / 11.1 ms | 10.61 GB/s / 24.4 ms |
| `700us` | interval 32 MiB | 10.27 GB/s / 5.9 ms | 10.49 GB/s / 13.5 ms |
| `3600us` | at-end 32 MiB | 10.18 GB/s / 9.6 ms | 10.45 GB/s / 17.4 ms |
| `3600us` | interval 32 MiB | 9.71 GB/s / 14.5 ms | 10.54 GB/s / 19.9 ms |

At c64, the append-publish profile generally shows the extra p99 in
coordinator/in-flight wait rather than in the actual journal write. For example,
the `200us` interval c64 row had publish total p99 `11.7 ms`, coordinator wait
p99 `11.7 ms`, visible commit p99 `1.65 ms`, and journal sync p99 `1.63 ms`.
That finding motivated sharding append-visible records by keyspace catalog
shard. A lane-only attempt removed `in_flight_wait_nanos` but regressed latency
because it lost useful group commit: `max_batch_ticket_count` p99 fell to `1`
and at-end publish p99 rose into the hundreds of milliseconds. The fixed
follow-up keeps file-scoped/lane-safe records but restores cross-lane group
commit through the shared base append-visible journal.

The final follow-up run used `c4-standard-288-lssd`, 48 local SSDs, the same
32 MiB/128 MiB workload shape, and the `raid-split-journal` layout. It restored
publish p99 while keeping the larger-node throughput class:

| RTT | Workload | c32 throughput / publish p99 | c64 throughput / publish p99 |
| ---: | --- | ---: | ---: |
| `200us` | interval 32 MiB | 12.38 GB/s / 17.7 ms | 12.40 GB/s / 12.7 ms |
| `700us` | interval 32 MiB | 14.32 GB/s / 14.0 ms | 13.71 GB/s / 12.7 ms |
| `3600us` | interval 32 MiB | 14.71 GB/s / 10.3 ms | 13.88 GB/s / 15.4 ms |
| `200us` | at-end 32 MiB | 12.74 GB/s / 11.4 ms | 12.60 GB/s / 12.9 ms |
| `700us` | at-end 32 MiB | 13.21 GB/s / 12.4 ms | 14.44 GB/s / 14.0 ms |
| `3600us` | at-end 32 MiB | 13.23 GB/s / 15.1 ms | 13.97 GB/s / 36.6 ms |

The final profile split shows the remaining publish tail is mostly useful
group-commit wait plus append-visible journal sync. Planning and local apply
are small: p99 planning is generally below `3.2 ms`, apply below `0.12 ms`, and
metadata-gate wait is effectively zero. Further throughput gains need to
account for data-device queueing rather than treating SQLite row work as the
append-publish limiter.

A follow-up unbounded drain-owner scheduler experiment kept one waiter in
charge across consecutive append-publish batches. That preserved throughput
but was rejected for latency: on the same `c4-standard-288-lssd` split-journal
shape, median throughput rose slightly from `12.67 GiB/s` to `12.86 GiB/s` and
the best row reached `15.22 GiB/s`, but median publish p99 rose from `14.0 ms`
to `20.9 ms`. The worst driver rows accumulated multiple append-visible
journal syncs inside one `wait_append_publish` call, so c64 publish p99
regressed even though coalesce waits fell. The code path remains bounded to one
physical publish per owner.

An RTT-order follow-up kept the same runtime and added benchmark controls for
repeat visibility. A randomized `REPEATS=2` run showed that the low `200us`
throughput was first-pass/layout-state variance, not an intrinsic modeled-RTT
effect: the first `200us` pass was `9.14-9.38 GB/s`, while the next `200us`
pass recovered to `14.38-15.12 GB/s`. An opt-in warmup pass alone was not
sufficient to make the first measured root steady, so future GCP comparisons
should use repeated randomized RTTs and treat the first measured pass after
layout setup separately.

A June 7, 2026 PT follow-up on the same `c4-standard-288-lssd`
split-journal shape added append-visible journal file preallocation and reduced
append-publish coalescing to a 4-ticket target with a `250us` idle wait and
`5 ms` max wait. The repeated randomized interval-only run preserved saturated
throughput while reducing steady-state publish p99. These measured values are
now the default `AppendPublishBatchPolicy` and can be swept in `loadbench` with
`--append-publish-batch-target`, `--append-publish-idle-coalesce-us`, and
`--append-publish-max-coalesce-us`:

| RTT | Workload | c32 throughput / publish p99 | c64 throughput / publish p99 |
| ---: | --- | ---: | ---: |
| `0us` | interval 32 MiB | 15.35 GiB/s / 2.9 ms | 15.22 GiB/s / 6.5 ms |
| `200us` | interval 32 MiB | 15.38 GiB/s / 3.9 ms | 15.47 GiB/s / 4.0 ms |
| `700us` | interval 32 MiB | 15.27 GiB/s / 3.8 ms | 15.39 GiB/s / 4.9 ms |
| `3600us` | interval 32 MiB | 15.23 GiB/s / 7.7 ms | 14.66 GiB/s / 8.0 ms |

The profile now shows journal sync p99 mostly in the `0.85-2.63 ms` range on
warm rows, with the residual c64 tail dominated by wait-behind-active-publish.
The data RAID stayed the throughput limiter (`data_wMBps_p90_sum` about
`15.1 GB/s`, `data_util_p90` about `98.6%`). The next structural latency
target is therefore the single global append-visible publish lane; SQLite row
work and metadata encoding are not the current limiter.

A June 8, 2026 PT quantum sweep added an explicit
`native-stream-publish-interval-16m` workload and compared 32 MiB, 16 MiB, and
4 MiB append calls while keeping the same 128 MiB publish boundary, c32,
`raid-split-journal`, randomized `REPEATS=2`, and RTTs `0us`, `200us`, `700us`,
and `3600us`. The 16 MiB shape is useful as a benchmark control but is not a
new default: it reduced append p99 where the 32 MiB row queued badly, especially
at `200us`, but it did not preserve throughput and publish p99 across all RTTs.
The steady repeat was:

| RTT | Workload | Throughput | Append p99 | Publish p99 |
| ---: | --- | ---: | ---: | ---: |
| `0us` | interval 32 MiB | 15.37 GiB/s | 72.4 ms | 3.6 ms |
| `0us` | interval 16 MiB | 13.70 GiB/s | 156.6 ms | 6.5 ms |
| `0us` | interval 4 MiB | 13.98 GiB/s | 44.1 ms | 4.5 ms |
| `200us` | interval 32 MiB | 14.50 GiB/s | 114.4 ms | 3.7 ms |
| `200us` | interval 16 MiB | 14.54 GiB/s | 42.2 ms | 4.1 ms |
| `200us` | interval 4 MiB | 13.40 GiB/s | 24.0 ms | 8.4 ms |
| `700us` | interval 32 MiB | 15.00 GiB/s | 76.0 ms | 5.0 ms |
| `700us` | interval 16 MiB | 14.38 GiB/s | 57.9 ms | 6.6 ms |
| `700us` | interval 4 MiB | 13.57 GiB/s | 26.6 ms | 9.0 ms |
| `3600us` | interval 32 MiB | 14.89 GiB/s | 76.5 ms | 7.6 ms |
| `3600us` | interval 16 MiB | 14.07 GiB/s | 48.8 ms | 11.2 ms |
| `3600us` | interval 4 MiB | 12.40 GiB/s | 30.0 ms | 10.6 ms |

The sweep confirms that smaller append calls reduce active-log queueing but
trade it for more operation overhead and publish-lane wait. The next storage
runtime target is therefore not a smaller public append default; it is reducing
active-log queueing for large appends without multiplying publish work.

A June 8, 2026 PT storage-node-count sweep tested that target by keeping the
same 32 MiB append / 128 MiB publish shape and c32 `raid-split-journal` layout,
but varying logical storage nodes across `4`, `8`, `16`, and `32`. The GCP
driver now accepts `STORAGE_NODE_COUNTS` so these modes run on one C4 node under
the same machine/disk conditions. In the steady repeat:

| Nodes | RTT | Throughput | Append p99 | Publish p99 | Dominant append profile |
| ---: | ---: | ---: | ---: | ---: | --- |
| `4` | `0us` | 14.80 GiB/s | 106.9 ms | 5.8 ms | lock 55.6 ms, sync 72.7 ms |
| `4` | `200us` | 9.14 GiB/s | 191.7 ms | 13.1 ms | lock 57.2 ms, sync 158.8 ms |
| `4` | `700us` | 9.04 GiB/s | 190.2 ms | 13.6 ms | lock 56.1 ms, sync 159.1 ms |
| `4` | `3600us` | 9.05 GiB/s | 202.1 ms | 19.5 ms | lock 47.8 ms, sync 154.0 ms |
| `8` | `0us` | 11.28 GiB/s | 154.5 ms | 15.7 ms | lock 25.8 ms, sync 129.6 ms |
| `8` | `200us` | 15.23 GiB/s | 109.5 ms | 3.3 ms | lock 32.4 ms, sync 80.1 ms |
| `8` | `700us` | 15.44 GiB/s | 114.3 ms | 6.4 ms | lock 33.7 ms, sync 87.9 ms |
| `8` | `3600us` | 15.24 GiB/s | 103.4 ms | 9.4 ms | lock 36.4 ms, sync 75.5 ms |
| `16` | `0us` | 15.40 GiB/s | 118.2 ms | 3.3 ms | lock 12.3 ms, sync 102.9 ms |
| `16` | `200us` | 15.28 GiB/s | 122.1 ms | 3.9 ms | lock 9.1 ms, sync 108.8 ms |
| `16` | `700us` | 15.26 GiB/s | 121.9 ms | 5.7 ms | lock 9.7 ms, sync 102.4 ms |
| `16` | `3600us` | 14.82 GiB/s | 129.0 ms | 7.7 ms | lock 9.7 ms, sync 107.9 ms |
| `32` | `0us` | 15.32 GiB/s | 120.5 ms | 4.4 ms | lock 0.0 ms, sync 109.9 ms |
| `32` | `200us` | 15.41 GiB/s | 124.8 ms | 5.1 ms | lock 0.0 ms, sync 113.2 ms |
| `32` | `700us` | 15.31 GiB/s | 114.8 ms | 5.6 ms | lock 0.0 ms, sync 105.9 ms |
| `32` | `3600us` | 14.60 GiB/s | 118.5 ms | 8.0 ms | lock 0.0 ms, sync 106.9 ms |

This proved the active-log queueing hypothesis only halfway. Increasing logical
storage nodes reduces or eliminates `active_log_lock_wait`, and `8` nodes fixed
the nonzero-RTT throughput collapse in this run. It did not materially lower
append p99, because the tail moved into synchronous auto-persist
`fdatasync`/directory-sync work. The new append-ingest profile columns
`background_sync_request_count` and `background_sync_step_bytes` make the next
pass distinguish "background sync did not get scheduled" from "background sync
was scheduled but could not win the race before auto-persist." Local 8 MiB and
16 MiB early-sync cadences were tested and rejected before GCP: both increased
local write/sync contention and worsened append p99 versus the original
threshold-based cadence.

A follow-up local A/B tried making dirty-tail auto-persist queue payload sync to
the background append-log worker instead of synchronously calling `fdatasync` on
the append thread. This is now an explicit benchmark knob
(`--stream-auto-persist-mode async-payload-sync`) with supporting knobs for
background sync worker count and request cadence. It did cut local append p99
substantially (`~101 ms` inline to `~19-26 ms` async in the small c8 local
shape), but publish p99 rose to `~257-279 ms` because the visible boundary then
absorbed the unfinished payload sync. Earlier 8 MiB / 16 MiB async request
cadences and four background sync workers did not fix the publish tail locally,
so that hypothesis was not escalated to GCP as a performance improvement.

A June 8, 2026 PT append-admission sweep added an explicit per-storage-node
append-ingest cap, `--append-ingest-max-in-flight-per-storage-node-mib`, and
records `max_in_flight_bytes_per_storage_node` in append-ingest profiles. The
global cap was also tightened so configured permits are held through
provider-private auto-persist sync. On the `c4-standard-288-lssd`
`raid-split-journal` shape, the global cap was not a latency win: tight caps
reduced file-sync p99 but moved the same or more time into admission wait. The
per-storage-node cap is a useful benchmark control, but not a new default.

The best balanced steady lane sweep was `nodecap128m + lanes4 + bg4 + step8m`:
throughput stayed in the `15.7-16.1 GB/s` class, average append p99 was
`111.0 ms`, max append p99 was `115.8 ms`, average publish p99 was `8.3 ms`,
and max publish p99 was `10.7 ms`. More active lanes reduced some lock waits
but did not consistently reduce append p99; the tail remained a mix of
`active_log_lock_wait`, payload write, and auto-persist file sync:

| Per-node cap | Active lanes | Avg throughput | Avg append p99 | Max append p99 | Avg publish p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| `128 MiB` | `2` | 15.98 GB/s | 114.6 ms | 119.3 ms | 7.2 ms |
| `128 MiB` | `4` | 15.87 GB/s | 111.0 ms | 115.8 ms | 8.3 ms |
| `128 MiB` | `8` | 15.67 GB/s | 112.0 ms | 121.1 ms | 6.2 ms |
| `256 MiB` | `2` | 15.70 GB/s | 114.7 ms | 123.9 ms | 7.8 ms |
| `256 MiB` | `4` | 15.85 GB/s | 115.4 ms | 124.2 ms | 5.2 ms |
| `256 MiB` | `8` | 15.69 GB/s | 110.7 ms | 122.3 ms | 7.0 ms |

This leaves append p99 close to the 32 MiB Rapid Storage interval/at-end rows,
but not reliably below `100 ms` at the same throughput. Further p99 reductions
probably need to reduce the auto-persist sync critical path or smooth device
queueing; admission alone mostly trades file-sync tail for queueing tail.

Raw C4 artifacts are local and ignored under
`infra/gcp-local-nvme-bench/results/`, specifically
`gcp-c4-192-layout-20260607-125041`, `c48887-06071318`, and
`c4288-sharded-publish-06071540`, `c4288-group-commit-06071625`,
`c4288-drain-owner-0607`, `c4288-rtt-random-spin-06071725`, and
`c4288-warm-200first-spin-06071742`, plus
`c4288-prealloc-target32-06071829`,
`c4288-prealloc-target32-rand-06071837`,
`c4288-coalesce4-rand-06071852`, `c4288-quantum-0608`,
`c4288-quantum16-0608`, `c4288-storage-nodes-0608`,
`c4288-admitcap-0608`, `c4288-nodecap-0608`, and
`c4288-nodecap-lanes-0608`. The temporary GCP instances, networks, subnets,
and firewall rules were deleted
after the runs.

`cargo bench --bench regression` is the Criterion mechanism suite.
`loadbench` is the integration runner for public block/native API behavior,
modeled RTT, concurrency, latency percentiles, conflicts, and errors.
The commands below are written for Linux hosts. On macOS hosts, run them inside
the Linux container by starting `docker compose up -d dev` and prefixing the
`cargo ...` invocation with `docker compose exec dev`.

```sh
# Broad public API smoke.
cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --workloads north-star \
  --rtt-us 200

# Filesystem-shaped block fsync windows.
cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --storage-nodes 4 \
  --workloads block-writeback \
  --rtt-us 200

# Native append ingest and publish boundaries.
cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack \
  --warmup-ms 0 \
  --concurrency 1,4,16 \
  --files 128 \
  --storage-nodes 4 \
  --workloads native-stream-ingest-32m,durable-publish \
  --rtt-us 200 \
  --stream-total-mib 1024 \
  --stream-publish-mib 128 \
  --matrix-csv target/loadbench/native-publish-boundary/matrix.csv \
  --durable-profile-csv target/loadbench/native-publish-boundary/profile.csv \
  --append-publish-profile-csv target/loadbench/native-publish-boundary/append-publish-profile.csv
```

`success_iops` is successful operations per second. `mbps` is submitted payload
MB/s. Append publish rows also report `published_mbps`; use it for throughput
comparisons when the benchmark includes a publish boundary. Plain stream ingest
rows measure accepted private bytes and are useful for hot-path diagnostics, not
for visible durability claims.

`--block-journal-batch-target`, `--block-journal-idle-coalesce-us`, and
`--block-journal-max-coalesce-us` schedule the group-committed block journal
lane that every durable block boundary joins. The default adds no artificial
coalesce wait: the in-flight batch sync is the batching window for the next
batch. `--block-journal-inline-kib` sets the largest batch payload carried
inline in journal records (default 64); larger writes stage chunked payload
segments on per-node data logs and journal only segment references, so payload
bandwidth uses the data disks instead of the journal device.

`--stream-auto-persist-mib` is an internal
durable-provider policy knob for append-stream latency experiments: it asks the
server to persist private prefixes before publish once the dirty tail reaches
the configured size. In `target/loadbench/stream-auto-persist-after-128/`, a
128 MiB synchronous threshold collapsed publish p99 but moved the sync wait into
append p99. The background implementation in
`target/loadbench/stream-auto-persist-after-32-bg2/` keeps append p99 much
closer to baseline while improving c16/c32 `published_mbps` by about 27%/23% for
publish-interval and about 2%/6% for publish-at-end. Publish p99 improves when
the background worker has enough head start, but it still waits for any
remaining prefix if the worker trails active writers.

Useful workload aliases:

| Alias | Use it for |
| --- | --- |
| `north-star` | Broad block/native API comparison. |
| `durable-publish` | Native stream publish-at-end, interval, and barrier-at-end durable boundaries. |
| `append-batch` | Client-side append payload size effects. |
| `append-stream` | Private ingest and publish-prefix behavior. |
| `block-durable-boundary` | Block write, batch fsync, writeback fsync, and prestaged fsync boundaries. |
| `block-writeback` | Filesystem-style dirty window plus fsync behavior. |
| `block-metadata` | Same-shard conflicts versus different-shard/device convergence. |
| `native-file-batch` | Client-sized random-write commit boundaries. |
| `native-metadata` | Same-file pressure versus different-file keyspace lanes. |

## Development

On Linux hosts, run Rust commands directly on the machine. On macOS hosts, use
the Linux container from `docker-compose.yml` and keep git commands on the host.
When using the macOS container, prefix the `cargo ...` commands below with
`docker compose exec dev` after starting `docker compose up -d dev`, then shut
it down with `docker compose down` when finished.

```sh
cargo test
cargo bench --bench regression -- --test
```

Full gate:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps
cargo bench --bench regression -- --test
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
