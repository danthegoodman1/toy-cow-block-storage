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
not as portable hardware claims. Append publish rows should be compared with
`published_mbps`, because that measures bytes that became visible and
restart-durable inside the timed window.

| Scenario | Workload shape | Result |
| --- | --- | --- |
| Verified native reads | `native-read-4k`, c16 | about 72.8k IOPS, p99 about 468 us |
| Block writeback fsync | `block-writeback-fsync-1m`, c16 | about 1.3 GB/s, p99 about 15 ms |
| Larger block fsync window | `block-writeback-fsync-4m`, c16 | about 1.9 GB/s, p99 about 40 ms |
| Append stream ingest | `native-stream-ingest-32m`, c16 | accepted private throughput peaks around 6.1 GB/s in the latest local sweep; this is not a visible durability result |
| Publish at end, 1 MiB appends | `native-stream-publish-at-end-1m`, c16, 1024 MiB/worker | `published_mbps` about 2.77 GB/s with 1 node, 3.34 GB/s with 4 nodes, 3.31 GB/s with 16 nodes |
| Publish at end, 4 MiB appends | `native-stream-publish-at-end-4m`, c16, 1024 MiB/worker | `published_mbps` about 2.51 GB/s with 1 node, 3.49 GB/s with 4 nodes, 3.17 GB/s with 16 nodes |
| Publish at end, 32 MiB appends | `native-stream-publish-at-end-32m`, c16, 1024 MiB/worker | `published_mbps` about 2.16 GB/s with 1 node, 3.18 GB/s with 4 nodes, 1.76 GB/s with 16 nodes |
| Local FS, no fsync | `fio`, buffered writes, 16 jobs, 4 MiB writes, 1024 MiB/job | about 5.77 GB/s write-phase bandwidth |
| Local FS, fsync at end | `fio`, buffered writes plus `--end_fsync=1`, same shape | about 3.49 GB/s write-phase bandwidth |
| Local FS, direct with fsync at end | `fio`, direct writes plus `--end_fsync=1`, same shape | about 3.82 GB/s write-phase bandwidth |

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

Raw C4 artifacts are local and ignored under
`infra/gcp-local-nvme-bench/results/`, specifically
`gcp-c4-192-layout-20260607-125041`, `c48887-06071318`, and
`c4288-sharded-publish-06071540`, `c4288-group-commit-06071625`,
`c4288-drain-owner-0607`, `c4288-rtt-random-spin-06071725`, and
`c4288-warm-200first-spin-06071742`, plus
`c4288-prealloc-target32-06071829`,
`c4288-prealloc-target32-rand-06071837`, and
`c4288-coalesce4-rand-06071852`. The
temporary GCP instances, networks, subnets, and firewall rules were deleted
after the runs.

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

# Native append ingest and publish boundaries.
docker compose exec dev cargo run --release --bin loadbench -- \
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
for visible durability claims. `--stream-auto-persist-mib` is an internal
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
