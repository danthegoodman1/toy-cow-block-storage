# CoW Storage Design Spec

Status: draft  
Project: `toy-cow-block-storage`

## 1. Summary

This project is a toy copy-on-write storage system built around immutable data
segments and immutable metadata roots. The first compatibility surface is a
block device, but the block layer is only one mapping layer over a shared
segment substrate. A native extent/file API should develop alongside it for
workloads that need file-level append leases, writer fencing, and lower metadata
amplification than a generic block interface can provide.

A block device is represented by a small device head:

```text
device_head {
  device_id
  shard_roots[0..N-1]
}
```

Each shard root points to an immutable metadata tree. Internal nodes point to
child metadata node IDs. Leaf nodes map logical block ranges to immutable
segment slices:

```text
logical_start, length -> segment_id, segment_offset
```

The native extent/file API uses the same segment substrate and commit/fencing
rules, but publishes file extents instead of logical block ranges:

```text
file_id, file_version, file_extent -> segment_id, segment_offset
```

Data is never overwritten in place. Writes append data to fresh immutable
segments, copy only the metadata path inside the affected shard, and publish a
new shard root for the device. Forking a device is O(1): the child device copies
the parent's shard-root pointers and shares the same immutable metadata DAG and
segments until one side writes.

The design priority order is correctness, deterministic testability, simplicity,
and then throughput. The operating philosophy is "build it like NASA": prove
small deterministic modules with exhaustive simulation before composing them
into a larger storage system. Scale should come from the simplest mechanics that
remain testable: immutable objects, sharded roots, append-only timelines, and
reachability-based garbage collection.

## 2. Goals

- Implement a toy block device with logical block reads, writes, forks,
  deletion, point-in-time restore, and garbage collection.
- Keep block storage as the compatibility mapping layer, not the whole storage
  system.
- Develop a native extent/file API beside the block API for append-heavy custom
  filesystems and direct users that need writer epochs, append leases, and stale
  writer rejection.
- Make fork O(1) by copying shard-root pointers only.
- Make writes copy only the changed shard's root-to-leaf metadata path.
- Preserve snapshot and fork safety by making metadata nodes and data segments
  immutable once published.
- Bound write contention by shard, not by whole device.
- Avoid deep per-leaf or per-segment refcount updates during fork.
- Reclaim unreachable metadata nodes and segments with tracing garbage
  collection.
- Implement point-in-time recovery with append-only per-shard commit records
  plus periodic device checkpoints.
- Keep the deterministic core free of hidden I/O, wall-clock reads, background
  work, and process-global randomness.
- Build in phases with deterministic simulation tests and clear exit gates.
- Prefer simple data structures until tests or benchmarks prove they are
  insufficient.
- Share segment storage, write intents, commit groups, fencing, and custodians
  across block and native extent/file mapping layers.

### Priority Order

1. Correctness: reads observe the latest committed mapping for their device and
   logical range, and native file reads observe the latest committed file
   extents; forks remain isolated after either side writes; GC never reclaims
   reachable data.
2. Determinism: the same initial state, seed, and ordered operation trace
   produce the same object graph, effects, and query results.
3. Simplicity: use explicit immutable objects, explicit commits, and explicit
   simulation models before optimizing.
4. Performance: measure write-path, read-path, fork, restore, and GC costs after
   the simple deterministic model is correct.

## 3. Non-Goals

- A production kernel block driver in the first version.
- Distributed consensus.
- Replication across machines.
- Encryption, compression, or deduplication.
- Online schema compatibility layers.
- Mutable physical block allocation.
- Eager deep reference counting on fork.
- Perfect segment compaction in the first version.
- Provider-specific storage behavior leaking into metadata-tree logic.
- Optimizations that cannot be represented in the deterministic simulator.
- Forcing native file/extent semantics through the block API when that loses
  append ownership, file versioning, or stale-writer fencing information.

## 4. Core Model

The logical state of a live device is:

```text
DeviceHead {
  device_id
  generation
  shard_roots[0..N-1]
}
```

The generation is a fencing identity for publishing device-head updates. The
number of shards is fixed for a device lineage in v1. A later format may change
that, but only by updating this spec and the deterministic tests in the same
change.

The logical state of a live native file is:

```text
file_head {
  file_id
  file_version
  root
  size
}
```

`file_version` is the fencing identity for file extent commits. Append leases
also carry writer epochs, so a stolen lease can reject stale writers even when
they durably wrote segment bytes before attempting metadata commit.

Each shard root points to a persistent immutable tree:

```text
MetadataNode {
  node_id
  kind: internal | leaf
  covered_logical_range
  children | leaf_entries
}
```

Internal nodes store child node IDs and child ranges. Leaf nodes store sorted,
non-overlapping logical extents:

```text
LeafEntry {
  logical_start
  length
  segment_id
  segment_offset
}
```

Segments store immutable block data:

```text
Segment {
  segment_id
  block_count
  bytes
}
```

A segment slice is valid when `segment_offset + length <= block_count`.

### V1 Metadata Tree Policy

The local v1 metadata tree uses deterministic range partitioning rather than
adaptive balancing. Each tree has a fixed fanout and a fixed maximum leaf span
in logical blocks. When an empty device shard or native file root is created,
the implementation recursively splits the covered range into contiguous child
ranges until each leaf covers at most the configured leaf span.

Writes do not rebalance the tree. A write walks only the child ranges that
overlap the edited logical range, copies the changed root-to-leaf paths, and
reuses untouched child node IDs. This means tree shape is a pure function of
the configured fanout, leaf span, and root coverage; the written entries inside
leaves are a pure function of the committed write trace.

Internal child ranges must cover their parent range exactly, in sorted order,
with no gaps and no overlaps. This invariant makes lookup, validation, PITR
replay, and GC traversal deterministic.

## 5. Deterministic Core Boundary

The storage core should be written as a deterministic state machine. It receives
logical commands and produces explicit effects:

```rust
fn step(&mut self, command: StorageCommand) -> Vec<StorageEffect>;
```

`StorageCommand` is the only way to mutate core state. `StorageEffect` describes
side effects such as `WriteSegment`, `PersistMetadataNode`,
`AppendShardCommit`, `PersistCheckpoint`, or `DeleteObject`, but the core does
not execute I/O.

The deterministic core must not:

- Read wall-clock time directly.
- Spawn tasks.
- Await futures.
- Perform filesystem, network, or database I/O.
- Use process-global randomness.
- Run background GC or compaction without an explicit command.

Time enters as command data. Random choices, if any, use an injected seed owned
by the deterministic test harness. Given the same initial state, seed, and
ordered command trace, the core must produce the same effects.

## 6. Public APIs and Service Planes

The public APIs are sibling mapping layers over the same segment substrate. The
block API should model the behavior expected from a block device. The native
extent/file API should preserve file-level intent for custom filesystems and
append-heavy users. Neither API should own the shared segment lifecycle,
write-intent machinery, commit-group machinery, or custodian logic.

Shards, metadata-tree paths, segment placement, and provider topology are
implementation details. A block caller should be able to treat a write request
as one committed block operation, even when the implementation splits it across
shards. A native file caller should be able to treat an append commit as one
file-version transition, even when it writes one or more segment slices.

The first implementation runs locally in one process, but the service
boundaries should be real from the beginning:

```text
BlockClient / BlockDevice API       NativeKeyspaceClient / NativeFile API
  -> BlockTransport
     -> BlockServer actor           -> NativeTransport
                                      -> NativeServer actor
        \                              /
         +------ shared substrate -----+
                -> MetadataPlane
                -> SegmentStore
                -> LocalSegmentCatalog
```

Every public trait and provider interface should document the minimum
guarantees an implementation must preserve. Method documentation should name
what a successful call makes durable or visible, what a failed call must not
expose, how stale or duplicate calls are fenced, and which details remain
implementation-private. This is part of the API contract, not commentary.

### Shared Segment Substrate

The shared substrate owns concerns that must stay identical for block and native
extent/file users:

- segment reservation, write, sync, and local catalog lifecycle
- write-intent identity and expiry
- data-before-metadata ordering
- commit groups and fencing tokens
- metadata node durability
- reachability roots and custodian reconciliation

Block storage is the first mapping layer, not the whole storage system.

### `BlockClient`

`BlockClient` is the public control handle for creating devices and looking up
device info. It talks through the same transport/server boundary as ordinary
block I/O. Public device creation specifies logical device size and block size;
shard count and metadata layout are selected by the implementation.

### `BlockDevice`

`BlockDevice` is the user-facing handle. It exposes aligned byte-range reads and
writes, flush, zeroing, discard, fork, restore, delete, and device info. It does
not expose shard IDs, segment IDs, metadata node IDs, or commit assembly.

Public guarantees:

- `read_at` and `write_at` require block-aligned offsets and lengths.
- Zero-length aligned ranges are valid no-op requests.
- A successful `write_at` is atomic at request granularity from the caller's
  perspective.
- Read-after-successful-write on the same device observes the write.
- `flush` returns only after previously acknowledged writes for the device meet
  the requested durability policy.
- `fork` creates a new device that initially reads identically to the source.
- `restore` creates a new device at the requested point in time.
- `delete` removes the live device but does not imply immediate physical
  reclamation.

### `NativeKeyspaceClient` and `NativeFile`

`NativeKeyspaceClient` and `NativeFile` are the native keyspace/file-facing API.
This API is for custom filesystems or direct users that want filesystem-level
snapshots, byte writes, append ownership, file versions, and stale-writer
fencing instead of encoding those semantics into ordinary block writes.

The snapshot and restore boundary is the native keyspace, not an individual
file. A keyspace is a filesystem-like namespace whose committed head points to
an immutable catalog root:

```text
KeyspaceHead {
  keyspace_id
  generation
  root -> KeyspaceRoot
}

KeyspaceRoot {
  file_count
  catalog_shards[0..K-1] -> KeyspaceCatalogShard
}

KeyspaceCatalogShard {
  file_id -> {
    name
    FileHead
  }
}
```

File IDs are scoped by `keyspace_id`. A snapshot or restore creates a new
keyspace lineage by copying the retained `KeyspaceRoot` pointer, so all files in
the namespace and their catalog metadata are restored to one coherent point in
time. File creation metadata belongs inside the immutable catalog root; it must
not live in a mutable side table that snapshots have to rediscover.

The local Phase 15 provider uses a fixed provider-private catalog shard count.
Creating or mutating one file copies exactly one catalog shard plus one
`KeyspaceRoot`, while untouched catalog shards and all file metadata trees stay
shared. The shard count is not part of the public API; durable or remote
providers may choose a different sharding or indexing policy while preserving
the same root-pointer snapshot and restore semantics.

The native API publishes mappings shaped like:

```text
keyspace_id, file_id, file_version, file_range -> segment_id, segment_offset
```

Public native guarantees:

- Keyspace snapshots and restores are atomic namespace-level operations.
- File writes are ordinary byte-offset mutations fenced by the committed file
  version.
- File appends are separately fenced by append leases with writer epochs.
- Native file reads, writes, and appends are byte-oriented; block alignment is an
  implementation-private segment detail.
- The metadata plane rejects stale append commits whose file version or writer
  epoch no longer matches the keyspace-scoped file.
- A successful mutating write or append commit advances the file version and
  keyspace catalog root atomically.
- Segment bytes are durable before file extent metadata references them.
- Failed or stale append commits leave only orphan segment data that custodians
  can reclaim.

The native API must not be implemented on top of the block API. Both APIs share
the lower substrate; they do not stack on each other.

### Native Keyspace Scaling Decision

Phase 15 replaces the original local whole-catalog `BTreeMap` body with sharded
immutable keyspace catalog roots before durable formats exist. The benchmarked
whole-map approach made native file create/write/append cost proportional to the
number of files in the keyspace because every publish cloned the full catalog.
The local provider now models the intended durable shape:

- hot file reads use provider-private root and shard lookups;
- file creates, writes, and appends copy one catalog shard plus one catalog
  root;
- keyspace checkpoints, snapshots, and restores copy root IDs and do not walk
  file metadata trees;
- append-only keyspace and file commit records remain the PITR source of truth;
- callers never learn catalog shard IDs or coordinate catalog placement.

The remaining local-provider limitation is concurrency: the in-memory metadata
plane still uses one process-local mutex, so independent-file operations prove
catalog-shard data shape but not concurrent metadata execution. Phase 17 owns
the remote/server mailbox work needed to let non-conflicting operations proceed
concurrently without changing the public API.

Current Criterion baseline bands for the local in-memory provider are recorded
with the regression benchmark suite. Normal CI should run
`cargo bench --bench regression -- --test`; performance investigations should
run `cargo bench --bench regression` and compare the `native_keyspace_scaling`,
`native_alignment`, `native_snapshot_restore_root_copy`, and
`native_concurrent_batches` groups.

The root-copy microbenchmarks intentionally isolate dependence on source
keyspace size, not every local bookkeeping cost: checkpoint, snapshot, and
restore still include local ID allocation and map insertion for the new record
or keyspace. The create-file benchmarks are starting-size measurements; the
measured keyspace grows during the benchmark loop. These details are local
provider artifacts, while the invariant under test is that none of those paths
walk file metadata trees or clone a whole keyspace catalog body.

Phase 15 local baseline ranges on this workstation:

| Operation | 1 file | 1k files | 100k files |
| --- | ---: | ---: | ---: |
| file info | ~200 ns | ~250 ns | ~4.5 us |
| 4 KiB file read | ~470 ns | ~520 ns | ~4.9 us |
| 4 KiB `write_at` | ~8.4 us | ~8.4 us | ~40 us |
| 1-byte append with lease | ~18 us | ~18 us | ~53 us |
| create file | ~65-80 us | ~65-80 us | ~65-80 us |
| stale lease rejection | ~0.6 us | ~0.6 us | ~9 us |
| checkpoint root copy | ~90 ns | ~100 ns | ~90 ns |
| snapshot root copy | ~170 ns | ~170 ns | ~170 ns |
| checkpoint restore root copy | ~170 ns | ~190 ns | ~200 ns |

Alignment and concurrency baselines:

- aligned 4 KiB native write: ~8.3 us
- unaligned partial-block native write: ~8.5 us
- aligned 4 KiB append with a fresh lease: ~12 us
- unaligned 17-byte append with a fresh lease: ~12 us
- aligned 4 KiB native read: ~0.6 us
- unaligned 17-byte native read: ~0.5 us
- four-thread independent write batch: ~71 us
- four-thread conflicting write batch: ~74 us
- four-thread independent append batch: ~93 us
- four-thread conflicting append batch: ~66 us

### `BlockServer`

`BlockServer` is an actor boundary. The local v1 server may be a simple
single-threaded mailbox or direct deterministic actor, but its interface should
look like a server request/response protocol. It owns request ordering,
backpressure, commit assembly, and the translation from public requests to core
commands and provider effects.

In this spec, `BlockServer` means the request coordinator for block-device
semantics. Segment bytes live behind `SegmentStore` endpoints and
`LocalSegmentCatalog`s. A future replicated implementation should add placement
coordination between the request coordinator and those storage endpoints, not
turn the public client into a replica coordinator.

### `BlockTransport`

`BlockTransport` moves typed block request and response envelopes. The local v1
transport can be an in-process call or channel. A remote transport should be an
implementation swap, not a redesign of the block API.

Transport envelopes carry request identity, optional deadline, and client epoch
or session identity so that retries and stale responses can be modeled
deterministically.

### `MetadataPlane`

`MetadataPlane` owns globally meaningful metadata durability:

- device catalog records
- native keyspace catalog records
- current device heads
- current keyspace heads, file heads, and file versions
- shard-root publish and compare-and-swap
- native keyspace catalog-root publish with file-version and writer-epoch
  fencing
- commit groups for multi-shard atomic public writes
- commit groups for native file write/append extent commits
- PITR shard commits, keyspace commits, and checkpoints
- metadata node durability
- retained roots for GC
- cache of hot heads and metadata nodes

### `SegmentStore` and `LocalSegmentCatalog`

`SegmentStore` reads and writes immutable segment bytes for one storage endpoint
or placement domain. It may be memory-backed, file-backed, or remote later.

`LocalSegmentCatalog` is local to a storage node. It maps segment IDs to local
replica placement, tracks checksums and write-complete state, and exposes
deletion eligibility from that node's perspective.

The storage-node file I/O engine is below both contracts. The durable provider
should introduce this internal boundary with ordinary blocking filesystem calls
and conservative file and directory syncs. A later Linux-only `io_uring` backend
may optimize concurrent segment reads, writes, and batching behind the same
boundary, but it must preserve the same temp-write, file-sync, atomic-rename,
directory-sync, catalog-transition, and restart-recovery semantics and must fall
back to the portable backend without changing public behavior.

The local v1 implementation may keep both in memory, but the distinction matters:
global metadata says which logical segment is referenced; local segment metadata
says where that segment's bytes live on a particular storage node. A logical
segment may have one local replica in v1 and multiple replicas later.

### Phase 16 Durable Provider Choice

The first durable provider remains local and single-process, but persists the
metadata service and storage-node state as separate planes:

```text
store/
  metadata/
    metadata.bin
  storage-nodes/
    node-<storage-node-id>/
      catalog.bin
      segment-store.bin
      segments/
      tmp/
```

The Phase 16 metadata provider uses crate-owned binary snapshots written with
the same temp-file, file-sync, atomic-rename, and directory-sync discipline as
segment files. This deliberately avoids choosing SQLite, RocksDB, or another
database before the crash/restart contract is proven. A later provider may use
a database, but it must preserve the same provider contracts and keep metadata
state separate from storage-node local segment catalogs.

Segment files are storage-node-local immutable payloads. `catalog.bin` records
local reservation, durable-pending, referenced, released, and freed lifecycle
state for that node. `segment-store.bin` records the durable segment descriptors
and placement metadata needed to validate segment files at restart. The
metadata snapshot records globally meaningful heads, roots, timelines,
checkpoints, write-intent counters, and native writer epochs; it does not store
storage-node file paths as metadata truth.

Because Phase 16 uses synchronous provider persistence, a successful public
write, checkpoint, fork, restore, delete, native write, or native append has
already persisted its segment files, storage-node catalog snapshot, segment
descriptor snapshot, and metadata snapshot before the call returns. `flush`
therefore reports the latest committed sequence visible to the relevant device
or native file as `durable_through`; it must not report a sequence whose
metadata snapshot can reference segment bytes that failed the storage-node
durability sequence.

### Future Storage Replication

Replication belongs below the public block/native APIs and above individual
`SegmentStore` implementations. Public clients may eventually request a
durability or replication class, but they should not fan out writes to replicas
or choose storage nodes.

The block and native servers remain request coordinators. A later placement
coordinator can choose a replica set for a segment, issue one reservation and
write per storage endpoint, wait for the requested replica durability, then
publish metadata that references the logical `SegmentId`. Metadata leaf entries
continue to reference logical segments, not replica placements.

This preserves the write linearization rule:

```text
replica bytes durable enough -> metadata publish -> user-visible data
```

If enough replicas become durable but metadata publish fails, the replicas are
orphaned and reclaimed by custodians. If metadata publish succeeds with fewer
than the desired background replica count but enough for the requested
durability, repair can add missing replicas later without changing the public
block or native API.

Replicated storage also needs a durable release-evidence boundary between the
metadata custodian and storage-node custodians. The local v1 implementation may
return released segment IDs in process, but a remote implementation must persist
release records by safe reachability epoch and let storage nodes consume them
idempotently. Storage nodes must not crawl metadata trees or infer deletion from
current heads. They apply metadata-produced release evidence to their local
replica catalogs, free matching physical bytes, and record progress so missed,
duplicated, delayed, or reordered release notifications can be reconciled.

### Local V1 Boundaries That Must Become Real

Several v1 components intentionally prove semantics in process before the system
has durable or remote machinery. These are not compatibility layers and should
not become permanent shortcuts:

- In-process transports prove typed request/response semantics. Remote
  transports must add serialization, bounded retry/deduplication, server
  incarnation fencing, stale-response rejection, and deterministic fault tests.
- Local servers serialize through a simple actor boundary. Remote-capable
  servers must replace that with explicit mailbox, backpressure, and
  per-shard/per-file conflict fencing so unrelated operations can proceed
  concurrently.
- In-memory `sync_segment` and `flush` prove data-before-metadata ordering.
  Durable providers must define crash-consistent segment sync, metadata WAL or
  transaction sync, and exact `durable_through` semantics.
- In-memory commit groups prove atomic root visibility inside one metadata
  plane. Durable providers must persist transaction records, replay partial
  publish attempts safely, and preserve compare-and-swap fences after restart.
- Write-intent expiry is injected by deterministic tests. Durable providers must
  store write-intent state, logical expiration, recovery scans, and custodian
  evidence for when pending segment data can no longer publish.
- Local segment catalog transitions to `Referenced` happen in process after
  metadata publish. Remote storage nodes need durable reference evidence or
  reconciliation so `DurablePendingMetadata` replicas become referenced after
  missed notifications or restarts.
- Segment release evidence is returned as an in-process vector in v1. Replicated
  storage needs durable release logs or per-node release queues with replay
  cursors.
- Placement is one local storage endpoint in v1. Replication needs storage-node
  identity, capacity and failure-domain policy, replica-set selection, and stale
  placement tests.
- Repair is only a design hook in v1. Replication needs repair queues/cursors,
  idempotent copy, source selection, checksum validation, and tests proving
  repair never exposes uncommitted data.
- PITR retention is deterministic commit-age policy in v1. Durable providers may
  add richer per-owner policies, but expiration must still be driven by injected
  logical time or commit epochs, not hidden wall-clock reads.
- Native append lease stealing is local metadata state in v1. Durable providers
  need lease/session records, restart-safe writer epochs, and tests where stale
  writers race after durable segment writes.
- Caches are ordinary in-memory maps in v1. Durable or remote implementations
  must document cache coherence, invalidation, and fence/version checks instead
  of relying on provider-local object identity.

### Write Ordering Contract

Writes use a data-before-metadata commit discipline. Metadata must never publish
a reference to segment bytes that have not reached the requested durability
level.

For any public write request, whether block or native extent/file, the handling
server:

1. Selects the local storage endpoint in v1, or a replica set in a later
   replicated implementation, that will hold the new segment bytes.
2. Creates a stable write-intent identity for the request or commit group. For
   native appends, this write intent is tied to the append lease and writer
   epoch.
3. Reserves segment space in each selected storage endpoint's
   `LocalSegmentCatalog` under that write intent.
4. Writes bytes through each selected `SegmentStore`.
5. Flushes or syncs those bytes until the requested durability level is met.
6. Commits each durable local replica catalog entry as
   durable-pending-metadata.
7. Persists the new immutable metadata nodes that reference the durable segment
   slices.
8. Publishes the block metadata update or native keyspace catalog update through
   a metadata commit group.
9. Marks durable local replica catalog entries as referenced by the successful
   commit.
10. Acknowledges the public write only after the metadata commit group succeeds.

If steps 1-6 succeed but metadata publish fails, the segment is an orphan. It is
durable local replica data but not reachable from any committed device or file
root. Orphans are not user-visible and must be reclaimed by custodian work after
their write intent can no longer commit.

The v1 local server does not hide publish conflicts behind implicit retries. It
serializes local requests before commit assembly, and stale direct metadata
publishes fail with a deterministic conflict. Later remote transports may retry
requests with the same request identity, but they must preserve the same
data-before-metadata visibility rule and must not introduce a second
cross-shard atomicity mechanism beside commit groups.

## 7. Operations

### Create Device

Creating a device initializes `N` shard roots to empty immutable trees and
publishes a device head.

Invariants:

- Every shard root exists.
- Empty shard trees read as zero-filled blocks.
- The shard count and logical block size are recorded in device metadata.

### Fork Device

Forking is O(1):

```text
B.shard_roots = A.shard_roots
```

No data is copied. No metadata tree is walked. No per-leaf or per-segment
refcount is bumped. The child receives a distinct `device_id` and a new device
head that points to the same immutable shard roots.

The metadata plane records the fork as a catalog/timeline event containing the
source device, target device, commit sequence, and copied shard-root pointers.
That record is for replay, audit, PITR, and tests; it must not imply a deep
metadata traversal.

Invariants:

- A forked child reads exactly the same logical contents as the parent at fork
  time.
- A later write to either device publishes only that device's changed shard
  root.
- Shared metadata and segments remain immutable.

Native files do not get a public per-file fork API in v1. The native API
snapshots and restores whole keyspaces so a filesystem namespace remains
coherent; those operations copy keyspace catalog-root pointers rather than
piggybacking on block-device forks.

### Read Range

A read maps each logical block range to segment slices by walking the relevant
shard trees. Reads spanning shards are decomposed into shard-local lookups and
returned in logical order.

Invariants:

- Leaf entries are sorted and non-overlapping.
- Later mappings in the committed tree shadow older mappings by construction;
  committed leaves must not contain overlapping visible entries.
- Sparse logical ranges return zero-filled blocks.

### Write Range

A write to a logical range:

1. Splits the logical range by shard.
2. Creates a write intent and reserves segment space on the selected block
   server.
3. Writes each shard-local data range to one or more fresh immutable segments.
4. Flushes those segment bytes to the requested durability level.
5. Commits the local segment catalog entry as durable-pending-metadata.
6. Copies only the metadata path from that shard root to the affected leaves.
7. Replaces, splits, or coalesces leaf entries so the written range maps to the
   new segment slices.
8. Publishes the metadata update through a commit group.
9. Marks the segment catalog entries as referenced by the successful commit.
10. Appends per-shard commit records for PITR.

Example:

```text
A shard 2 -> RA2
B shard 2 -> RA2

B writes block 150

A shard 2 -> RA2
B shard 2 -> RB2
```

Only one root-to-leaf path diverges for a single-shard write. Untouched metadata
nodes and segments remain shared.

Invariants:

- Segment objects are persisted before metadata leaves reference them.
- Durable local segment catalog entries exist before metadata leaves reference
  those segments.
- New metadata nodes are persisted before the shard-root commit is published.
- Publishing a shard root is fenced by device generation or expected old root.
- A failed publish leaves the previous committed root readable.
- A failed metadata publish after durable segment write creates an orphan segment
  that must not be visible to reads and must be reclaimable by custodians.

### Delete Device

Deleting a device removes it from the live device-head set and appends a
timeline record containing the roots that were live at the deletion point. It
does not synchronously delete metadata nodes or segments. Reclamation belongs to
GC.

Invariants:

- Deleted devices are absent from live device listings and live device-head
  lookups after the delete commit becomes visible.
- Deleted devices are not current live GC roots after the delete commit becomes
  visible.
- PITR retention policy decides whether older checkpoints, shard-root commits,
  and delete records can still make deleted device state reachable for restore
  and GC.
- Deleted-device retention may be indefinite, immediate, or based on deterministic
  commit age. Retention must not depend on hidden wall-clock reads; providers
  that expose time-based retention must use injected logical time that can be
  replayed by the simulator.
- Restoring a deleted source is valid only to a retained point before deletion,
  such as a checkpoint or commit before the delete record. Time-based restore at
  or after the delete record observes the deletion and fails cleanly.
- Deletion never directly frees metadata nodes, segment bytes, or local segment
  catalog entries.

### Native File Create

Creating a native file initializes an empty file metadata root and a file
version of zero, then publishes a new immutable keyspace catalog root containing
that file head. File metadata roots are GC roots while their keyspace catalog
root is live or retained by PITR policy.

Invariants:

- Empty native files read as empty.
- The file version changes only through committed metadata updates.
- The file root is separate from block device shard roots.

### Native File Write

A native file write is a byte-offset mutation against one file inside a
keyspace. It can overwrite existing bytes or extend the file from the current
end, but v1 rejects sparse writes beyond EOF. The local segment store remains
block-aligned internally; unaligned file writes are implemented by copying the
affected logical blocks into a fresh immutable segment and publishing a new file
root.

Invariants:

- A successful write advances the file version atomically with the containing
  keyspace catalog root.
- Writes are fenced by the committed file version observed by the metadata
  plane.
- Failed writes leave the previous file version readable.
- Segment/block alignment is not exposed to native callers.

### Acquire Append Lease

A native append lease grants one writer the right to attempt appends against a
specific file version and writer epoch. The metadata plane may steal the lease by
issuing a newer writer epoch.

Invariants:

- Append leases carry `file_id`, `base_version`, and `writer_epoch`.
- A stale lease cannot publish file metadata.
- Lease stealing does not delete durable data; failed old writers leave orphans
  if their segment writes already became durable.

### Native Append Commit

A native append commit:

1. Validates the append lease against the current file version and writer epoch.
2. Creates a write intent tied to that lease.
3. Reserves and writes durable segment bytes.
4. Builds file extent metadata for the append range.
5. Publishes the new file root with file-version and writer-epoch fencing.
6. Marks local segment catalog entries as referenced.
7. Returns the new file version.

Invariants:

- A successful append advances the file version atomically.
- Stale writers are rejected by metadata fencing.
- Failed or stale append commits never become readable file data.
- Durable segment data from failed append commits is reclaimed as orphan data.

## 8. Sharding

Without sharding, every write contends on a single `device.root`. With sharding:

```text
device -> [root0, root1, root2, root3, ...]
```

writes to different shards can publish independently. Concurrency is bounded by
shard-level contention instead of whole-device contention.

V1 uses deterministic range-to-shard mapping:

```text
shard_id = logical_block / blocks_per_shard
```

Public writes spanning multiple shards must commit atomically at request
granularity. Internally, the implementation may prepare multiple shard-local
metadata updates, but it must publish them through a commit group so readers
observe either the old mapping or the complete new mapping.

## 9. Segment Policy

Append and fresh writes naturally create multi-block segments:

```text
2100..2115 -> S900[0..15]
```

Random overwrites create small new segments and split leaf mappings:

```text
128..149 -> S100[0..21]
150      -> S900[0]
151..159 -> S100[23..31]
```

The first implementation should prefer correctness over clever packing:

- Write one segment per shard-local write chunk.
- Coalesce adjacent leaf entries only when they reference adjacent slices of the
  same segment.
- Defer segment compaction until read amplification, object count, or GC tests
  show a concrete need.

## 10. Point-In-Time Recovery

PITR is a timeline of root changes. The system should not rewrite a full device
or keyspace manifest on every write. For block devices, it appends per-shard
commit records:

```text
ShardCommit {
  commit_seq
  commit_group
  time
  device_id
  shard_id
  old_root
  new_root
}
```

Native operations append keyspace catalog-root commit records. File-root
changes are also recorded as audit/replay evidence inside the keyspace commit:

```text
KeyspaceCommit {
  commit_seq
  commit_group
  time
  keyspace_id
  old_root
  new_root
}
```

```text
FileCommit {
  commit_seq
  commit_group
  time
  keyspace_id
  file_id
  old_version
  new_version
  old_root
  new_root
  size
}
```

Periodically, it writes checkpoint manifests:

```text
Checkpoint {
  commit_seq
  time
  owner
  roots: block shard roots | native keyspace root
}
```

Restore an owner to time `T`:

1. Load the latest checkpoint for the device or keyspace at or before `T`.
2. Replay root commits for that owner after the checkpoint and up to `T`.
3. Return a reconstructed `DeviceHead` or `KeyspaceHead`.

The local v1 block restore API creates a new device from the reconstructed root
set. The restored device starts a new lineage with its own `device_id`,
generation zero, and a baseline checkpoint at the restore commit. It does not
mutate the source device or historical roots. Device creation and fork creation
also write baseline checkpoints so replay always has a deterministic starting
root set.

The native restore API creates a new keyspace from the reconstructed
`KeyspaceRoot`. This is intentionally not a per-file restore: every file in the
catalog is restored together, and a stale append lease from the source keyspace
cannot publish into the restored keyspace because leases carry `keyspace_id`.

Invariants:

- `commit_seq` is total ordered within the timeline provider.
- All shard commits in a public multi-shard write share a commit-group identity.
- Native file root publishes are fenced by keyspace, file version, and writer
  epoch. `FileCommit` records must record old and new file versions.
- Replaying checkpoint plus commits is deterministic.
- Checkpoint roots must match replayed state at the checkpoint sequence.
- Restoring to a named commit requires that commit to exist in the selected
  owner timeline; restoring to a time selects the latest retained point at or
  before that time.
- PITR retention policy is part of GC root selection. Local v1 uses a
  deterministic commit-age window: a restore point is retained while
  `current_commit - restore_commit < pitr_grace_commits`.
- Because replay starts from a checkpoint and then applies block shard commits
  or native keyspace commits, GC must materialize or retain a checkpoint at the
  window floor as a replay anchor, plus the commit roots needed after that
  anchor. This prevents sparse checkpoint cadence from quietly extending the
  PITR data-retention window back to owner creation.

## 11. Garbage Collection

The project should not eagerly maintain deep refcounts on fork. Fork would stop
being O(1), and every snapshot would require walking metadata.

Use tracing GC:

1. Start from all live device shard roots, live native keyspace catalog roots,
   and retained PITR checkpoint/timeline roots.
2. Mark reachable metadata nodes.
3. Mark segment IDs referenced by reachable leaf entries.
4. Sweep unmarked metadata nodes after the mark epoch is safe.
5. Publish release evidence for unmarked segment IDs so storage-node custodians
   can reclaim local physical bytes.

Each object may store:

```text
last_mark_epoch
```

The metadata sweeper deletes metadata objects not marked in the latest safe
sweep and not reachable from the current committed roots at sweep time. This
lets mark and sweep pause deterministically without deleting nodes created or
published after a mark started. Segment bytes are freed by storage-node
custodians after they receive release evidence. The exact safe sweep rule
depends on the provider, but the deterministic model must prove that objects
reachable from any live root or retained PITR root are never deleted.

When deleted-device retention is disabled for a safe GC epoch, the metadata
custodian may also expire that deleted device's retained PITR catalog state.
After that point a later policy change cannot resurrect roots that have already
become unreachable and eligible for sweep.

The local retention policy supports deterministic commit-age retention for both
deleted-device roots and PITR roots. Deleted device roots may be retained
indefinitely, expired immediately after a safe GC proves them unreachable, or
retained until a configured number of commit sequence advancements has elapsed
since the delete commit. PITR roots may be retained for a configured number of
commit sequence advancements. If no checkpoint exists at the PITR window floor,
the metadata custodian creates a deterministic replay-anchor checkpoint before
sweeping, then keeps only the anchor and later block shard or native keyspace
commit roots needed for replay. Commit-age retention is the v1 stand-in for
production TTLs; later wall-clock-facing policies must be implemented through
injected logical time so generated tests can replay them.

Invariants:

- Mark traversal starts only from committed roots.
- Sweep never deletes an object marked in the latest safe epoch.
- Device/keyspace deletion and PITR retention changes affect only root selection,
  not object mutability.
- Expiring retention is one-way for the expired roots; restore must fail
  cleanly after their metadata has been swept.
- PITR GC must not delete metadata or segments needed to restore any point
  inside the configured commit-age window.

## 12. Custodians and Orphan Reclamation

Garbage collection determines logical reachability, but physical reclamation is
split between metadata and storage-node custodians.

### Metadata Custodian

The metadata custodian owns global reachability. It periodically:

1. Enumerates all live device heads and native keyspace heads.
2. Adds retained PITR checkpoint and timeline roots.
3. Traverses reachable metadata nodes.
4. Records segment IDs referenced by reachable leaf entries.
5. Publishes a safe reachability epoch for metadata nodes and segment IDs.
6. Emits release candidates for metadata nodes and segment references that are
   unreachable after the chosen retention policy.

The metadata custodian does not delete local segment bytes directly. It produces
evidence that a segment is no longer referenced by committed metadata or retained
PITR roots.

In the local in-process implementation, that evidence can be a deterministic
list of released segment IDs returned by the metadata sweep. In a remote or
replicated implementation, the same concept must become a durable release log or
per-storage-node release queue keyed by safe reachability epoch. Consumers must
be able to replay from a cursor, tolerate duplicate records, and reconcile
missed release events without asking storage nodes to interpret global metadata
reachability themselves.

### Storage-Node Custodian

Each storage node owns its local physical segment catalog. It periodically:

1. Frees expired reservations that never reached durable write.
2. Frees failed writes that never reached durable segment state.
3. Finds durable segments that are still pending metadata after their write
   intent has expired or definitively failed.
4. Applies release evidence from the metadata custodian.
5. Reconciles missed asynchronous frees by comparing local catalog state with
   the latest safe reachability epoch.

The storage-node custodian is the only component that frees local physical
segment space.

### Segment Lifecycle

The local segment lifecycle is:

```text
Reserved
  -> Writing
  -> DurablePendingMetadata
  -> Referenced
  -> Released
  -> Freed
```

Failure and cleanup paths:

```text
Reserved expired             -> Freed
Writing failed               -> Freed
DurablePendingMetadata stale  -> Orphan -> Freed
Metadata publish failed       -> Orphan -> Freed
Referenced overwritten        -> Released after GC proves unreachable
Referenced device deleted     -> Released after retention allows it
```

`DurablePendingMetadata` segments are protected from ordinary cleanup while
their write intent can still publish. They become reclaimable only when the
write intent is expired, cancelled, or known to have failed, or when a completed
metadata commit references a different segment set.

Invariants:

- Metadata never references a segment that is not durably committed in the local
  segment catalog.
- The storage-node custodian never frees a segment that is reachable from a live
  device or retained PITR root.
- The storage-node custodian never frees `DurablePendingMetadata` while its
  write intent may still commit.
- Orphan durable segments are eventually freed after their write intent can no
  longer commit.
- Missed asynchronous frees are eventually corrected by periodic reconciliation.

## 13. Provider Interfaces

The deterministic core should be provider-agnostic. Storage adapters implement
the effects emitted by the core and report completions or failures as commands.

Provider and service boundaries:

- `BlockDevice`: public aligned block-device handle.
- `BlockClient`: public control handle for create and device lookup.
- `BlockServer`: actor boundary that handles block requests.
- `BlockTransport`: typed request/response transport.
- `NativeKeyspaceClient`: public native keyspace control handle.
- `NativeFile`: public native file handle with byte writes, append leases, and
  file-version commits.
- `NativeServer`: actor boundary that handles native keyspace/file requests.
- `NativeTransport`: typed request/response transport for native keyspace/file
  requests.
- `MetadataPlane`: device catalog, metadata nodes, commit groups, PITR, and GC
  roots for both block and native file metadata.
- `SegmentStore`: write and read immutable segment bytes.
- `LocalSegmentCatalog`: per-storage-node replica placement and local segment
  state.
- `MetadataCustodian`: global metadata and segment-reference reachability.
- `StorageNodeCustodian`: local reservation, orphan, release, and free
  reconciliation.

The in-memory provider is the first implementation and the source of provider
conformance tests. Durable providers must pass the same tests before they are
trusted.

## 14. Correctness Invariants

The simulator and tests should check these invariants after every delivered
command:

- Every live device has exactly `N` shard roots.
- Every live native keyspace has one current catalog root.
- Every file in a live native keyspace has one current file root and monotonic
  file version.
- Every committed shard root points to an existing metadata node.
- Every metadata child pointer points to an existing metadata node.
- Every leaf segment reference points to an existing segment.
- Leaf entries are sorted, non-overlapping, and within the leaf range.
- Segment slices stay within segment bounds.
- Metadata references only segments that were durably written before metadata
  publish.
- Reads after writes return the latest committed bytes for the target device.
- Public writes spanning shards are atomic at request granularity.
- Native append commits are atomic at file-version and keyspace-catalog
  granularity.
- Stale native append leases cannot publish file metadata across keyspace
  lineage boundaries.
- Forked devices initially read identically to their parent.
- After divergence, writes to one fork do not change reads from the other fork.
- A failed publish does not expose partially written metadata.
- A failed publish after durable segment write leaves only reclaimable orphan
  segment data.
- Replaying PITR checkpoint plus commits reconstructs the same device or
  keyspace head.
- GC never deletes an object reachable from live or retained PITR roots.
- Custodians eventually reclaim expired reservations, failed writes, orphan
  segments, and missed async frees without deleting reachable data.

Generated end-to-end traces must be replayable by seed. When a generated trace
fails, the harness should report the seed, ordered operation trace, a compact
suffix suitable for quick reproduction, and an object-graph summary with live
owners, metadata node count, GC root count, and segment lifecycle counts. When a
replay predicate is available, the harness should shrink traces with a
deterministic deletion-based minimizer.

## 15. Simplicity Guardrails

V1 should stay intentionally small.

V1 uses:

- Fixed block size per device.
- Fixed shard count per device lineage.
- A native extent/file API developed beside the block API.
- File-version and writer-epoch fencing for native appends.
- Immutable segment objects.
- Immutable metadata nodes.
- A deterministic tree shape.
- One segment per shard-local write chunk.
- Commit groups for public writes that touch multiple shards.
- Explicit segment lifecycle states for reservation, durable-pending-metadata,
  referenced, released, and freed.
- Append-only shard commit records.
- Periodic full device checkpoints.
- Deterministic commit-age retention for deleted-device and PITR roots.
- Tracing GC.
- Metadata and storage-node custodians.
- In-memory provider first.

V1 does not use:

- Kernel integration.
- Cross-machine replication.
- A full POSIX filesystem implementation.
- Compression, encryption, or deduplication.
- Segment compaction.
- Online shard splitting.
- Eager deep refcounts.
- Compatibility shims for old internal formats.
- Background actors in deterministic core logic.

Any addition to this list needs a failing deterministic simulation, a benchmark,
or a concrete correctness gap. Convenience is not enough.
