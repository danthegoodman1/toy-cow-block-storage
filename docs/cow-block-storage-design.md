# CoW Storage Design Spec

Status: draft  
Project: `toy-cow-block-storage`

## 1. Summary

This project is a toy copy-on-write storage system built around immutable data
segments and immutable metadata roots. The first compatibility surface is a
block device, but the block layer is only one mapping layer over a shared
segment substrate. A native extent/file API should develop alongside it for
workloads that need file-level append streams, writer fencing, and lower
metadata amplification than a generic block interface can provide.

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
rules for ordinary writes, but publishes file extents instead of logical block
ranges:

```text
file_id, file_version, file_extent -> segment_id, segment_offset
```

Append-stream publishes are a native file fast path, not a block-compatibility
mapping. Their final storage shape is run-backed file extents over
stream-private append logs:

```text
file_id, file_version, file_extent -> append_log_run_id, run_offset
```

That keeps large appends from fragmenting visible metadata into one ordinary
segment per client append call. Block writes and ordinary file writes can still
use immutable segments.

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
  filesystems and direct users that need writer epochs, append streams, and
  stale writer rejection.
- Add a POSIX namespace API as a sibling mapping layer for FUSE-style use,
  without forcing POSIX directory, inode, rename, unlink, truncate, or fsync
  semantics into the lower native file API.
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
- Treating a FUSE adapter as the POSIX metadata model. The adapter should be a
  thin transport over an explicit POSIX namespace layer.

## 4. Core Model

The logical state of a live device is:

```text
DeviceHead {
  device_id
  generation
  shard_roots[0..N-1]
}
```

The generation is a monotonic observation identity for the committed device-head
view. Block writes are fenced by the expected old root of each touched shard,
not by the whole-device generation, so independent writes to different shards
can publish without conflict. A stale write to the same shard still fails when
its expected old root no longer matches. Storage write grants for block data
therefore carry the target shard ID and expected old shard root alongside the
observed device generation. The number of shards is fixed for a device lineage
in v1. A later format may change that, but only by updating this spec and the
deterministic tests in the same change.

Durable providers store this logical device head as a stable device manifest
plus one mutable row per shard head. The manifest records device identity and
fixed shard shape; per-shard rows record the current root, generation, and
latest commit for that shard. Reopen reconstructs the logical `DeviceHead` by
combining the manifest with all shard rows. This keeps the physical metadata
convergence point aligned with the logical fence: same-shard writes still
contend on the same shard row, while same-device different-shard writes no
longer rewrite one whole-device head payload.

The logical state of a live native file is:

```text
file_head {
  file_id
  file_version
  root
  size
}
```

`file_version` is the fencing identity for ordinary file extent commits. Append
streams carry writer epochs and separate private durable ingest from visible
publish, so a stolen stream can reject stale writers even when they durably
wrote private segment bytes before attempting metadata publish.

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
         +------ LocalCoordinator -----+
                  |             |
                  v             v
           MetadataPlane   StorageNodeDirectory
                              -> StorageNodeTransport
```

The coordinator is the only local role allowed to talk to both metadata and
storage nodes. Metadata owns logical visibility: roots, fences, timelines,
checkpoints, PITR retention, and GC reachability. Storage nodes own physical
bytes, node-local catalogs, data logs, and segment lifecycle. A normal write is
ordered as storage-node durable receipt, metadata publish, then storage-node
referenced marking. If metadata publish fails, the durable receipt remains a
pending orphan for the storage-node custodian.

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
file. A keyspace is a filesystem-like namespace whose live committed head holds
one root per catalog shard:

```text
KeyspaceHead {
  keyspace_id
  generation
  file_count
  catalog_shards[0..K-1] -> KeyspaceCatalogShard
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
keyspace lineage by copying a retained keyspace catalog root/checkpoint, so all
files in the namespace and their catalog metadata are restored to one coherent
point in time. File creation metadata belongs inside immutable catalog shards;
it must not live in a mutable side table that snapshots have to rediscover.

The local Phase 15 provider uses a fixed provider-private catalog shard count.
Creating or mutating one file copies exactly one catalog shard and updates that
shard in the live `KeyspaceHead`; ordinary file publishes do not allocate a new
whole-keyspace root. `KeyspaceRoot` objects are materialized for checkpoints,
snapshots, and PITR replay anchors. The shard count is not part of the public
API; durable or remote providers may choose a different sharding or indexing
policy while preserving the same coherent keyspace snapshot and restore
semantics.

The native API publishes mappings shaped like:

```text
keyspace_id, file_id, file_version, file_range -> segment_id, segment_offset
```

Public native guarantees:

- Keyspace snapshots and restores are atomic namespace-level operations.
- File writes are ordinary byte-offset mutations fenced by the committed file
  version.
- File appends are separately fenced by append streams with writer epochs.
- Native file reads, writes, and appends are byte-oriented; block alignment is an
  implementation-private segment detail.
- The metadata plane rejects stale append stream operations whose stream token
  or writer epoch no longer matches the keyspace-scoped file.
- A successful mutating write or append commit advances the file version and
  owning keyspace catalog shard atomically.
- Segment bytes are durable before file extent metadata references them.
- Failed or stale append commits leave only orphan segment data that custodians
  can reclaim.

The native API must not be implemented on top of the block API. Both APIs share
the lower substrate; they do not stack on each other.

### POSIX Namespace and FUSE Layer

A POSIX filesystem should be a third first-class mapping layer over the shared
segment substrate, not a wrapper that smuggles directory and inode state through
ordinary `NativeFile` contents. The intended layering is:

```text
shared segment substrate
shared metadata / commit groups / grants / receipts / custodians
        |
   block API
   native keyspace/file API
   POSIX namespace API
        |
      FUSE adapter
```

The POSIX namespace layer owns the semantics that make a filesystem different
from a bag of files:

- inode identity and generation;
- directory entries and path lookup;
- link counts and unlink-while-open behavior;
- file mode/uid/gid-like metadata and deterministic logical timestamps;
- symlink targets if supported;
- atomic rename across source directory, destination directory, old target, and
  affected inodes;
- truncate, sparse extension, and read-as-zero behavior;
- fsync and checkpoint boundaries.

File data in the POSIX layer still uses immutable segment-backed file metadata
trees, write grants/receipts, metadata publish fences, PITR roots, GC, and
storage-node custodians. POSIX namespace roots should snapshot and restore by
copying root pointers, just like block device forks and native keyspace
snapshots. Directory and inode metadata must therefore live in immutable
namespace roots and catalog shards, not in mutable side tables or opaque
mini-databases stored inside regular files.

The FUSE adapter is an integration surface, not the source of correctness. It
translates kernel/user-space filesystem operations into the POSIX namespace API
and contains no storage placement, GC, PITR, grant/receipt, or segment lifecycle
decisions.

### Native Keyspace Scaling Decision

Phase 15 replaces the original local whole-catalog `BTreeMap` body with live
sharded keyspace catalog heads before durable formats exist. The benchmarked
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
| 1-byte append with fresh stream | ~18 us | ~18 us | ~53 us |
| create file | ~65-80 us | ~65-80 us | ~65-80 us |
| stale stream rejection | ~0.6 us | ~0.6 us | ~9 us |
| checkpoint root copy | ~90 ns | ~100 ns | ~90 ns |
| snapshot root copy | ~170 ns | ~170 ns | ~170 ns |
| checkpoint restore root copy | ~170 ns | ~190 ns | ~200 ns |

Alignment and concurrency baselines:

- aligned 4 KiB native write: ~8.3 us
- unaligned partial-block native write: ~8.5 us
- aligned 4 KiB append with a fresh stream: ~12 us
- unaligned 17-byte append with a fresh stream: ~12 us
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
or stream identity so that retries and stale responses can be modeled
deterministically.

### Phase 17 Remote Transport Choice

The first remote-capable transport is an in-process serialized endpoint, not a
network socket. Requests and responses cross a binary envelope that includes
server incarnation, client epoch, request ID, optional logical deadline, and
the public block or native request/response body. The Phase 17 envelope may use
`bincode` as temporary local scaffolding to prove the remote contract without
depending on TCP, HTTP, gRPC, or an async runtime. It is not the real network
format.

Remote endpoints keep a bounded request-deduplication cache keyed by server
incarnation, client epoch, and request ID. Reusing a key for different request
bytes is rejected. Duplicate retries with identical bytes return the cached
wire response. Endpoints also expose deterministic mailbox capacity, shutdown,
and injected logical-time deadline behavior so delay, retry, stale
incarnation, stale response, and backpressure cases can be tested without real
network nondeterminism.

The deterministic chaos transport wraps the serialized wire boundary below the
typed block/native transports. It can drop requests, drop responses after the
server has applied them, duplicate request delivery, delay response bytes, and
reorder delayed response bytes in front of newer responses. This is the Phase
17 correctness harness: stale or mismatched response bytes must be rejected by
the typed transport, while retries with the same request identity must remain
safe and idempotent. A real TCP, HTTP, gRPC, or QUIC transport is a later
implementation of the same wire contract, not part of Phase 17.

The remote transport does not change the public block or native APIs. Existing
clients can be constructed over any `BlockTransport` or `NativeTransport`,
including the in-process transport and the serialized remote transport.

A real network transport is a later adapter over the same wire contract. It
must add framing, bounded queues, timeout/reconnect behavior, malformed-frame
handling, and loopback/chaos conformance tests without changing block/native
semantics or moving replica selection into clients. Production network frames
must use a crate-owned codec with explicit magic, version, frame kind, tags,
fixed endianness, bounded lengths, and deterministic rejection of malformed or
trailing bytes.

### Phase 19 Network Protocol Choice

The first real network adapter uses blocking TCP with one request/response per
connection. This is intentionally conservative: reconnect behavior is the
default because every call opens a fresh connection, there are no hidden
background retries, and endpoint backpressure remains the same bounded mailbox
contract proven by the serialized remote endpoint.

TCP carries a 4-byte big-endian frame length followed by a crate-owned frame:

```text
magic("TCOWWIRE"), version, frame_kind, payload
```

Frame kinds distinguish block requests, block responses, native requests, and
native responses. Payloads use the same explicit binary rules as the durable
codec: fixed big-endian integers, explicit enum tags, bounded
collections/strings, and strict rejection of malformed, oversized, mismatched,
or trailing bytes. Server incarnation is inside the payload so stale clients and
responses are rejected deterministically. The TCP layer is only a byte pipe; it
does not choose storage nodes, fan out replicas, or change block/native request
semantics.

### `MetadataPlane`

`MetadataPlane` owns globally meaningful metadata durability:

- device catalog records
- native keyspace catalog records
- current device heads
- current keyspace heads, file heads, and file versions
- shard-root publish and compare-and-swap
- native keyspace catalog-shard publish with file-version and writer-epoch
  fencing
- commit groups for multi-shard atomic public writes
- commit groups for native file write/append extent commits
- PITR shard commits, keyspace commits, and checkpoints
- metadata node durability
- retained roots for GC
- cache of hot heads and metadata nodes

`MetadataPlane` must not read or write storage-node catalogs, data logs, or
segment bytes. When a coordinator persists a metadata leaf that references
segments, it supplies verified storage-node receipt evidence. The metadata
plane extracts segment descriptors from those verified receipts to validate
leaf shape without opening a storage node. Storage nodes later receive
metadata-produced reference or release evidence from the coordinator; they do
not infer logical visibility by reading current metadata heads.

### `SegmentStore` and `LocalSegmentCatalog`

`SegmentStore` reads and writes immutable segment bytes for one storage endpoint
or placement domain. It may be memory-backed, file-backed, or remote later.

`LocalSegmentCatalog` is local to a storage node. It maps segment IDs to local
replica placement, tracks checksums and write-complete state, and exposes
deletion eligibility from that node's perspective.

The storage-node byte I/O engine is below both contracts. Phase 20 removed the
old file-per-segment durable backend from the production path; the active local
durable provider writes segment bytes and commit state through an append
journal, and Phase 21 replaces that with SQLite metadata plus rolled data logs.
A later Linux-only `io_uring` backend should optimize the data-log append/read
engine behind the same provider contracts, not resurrect a public
file-per-segment API. It must preserve the same data-before-metadata ordering,
sync, replay, catalog-transition, and restart-recovery semantics and must fall
back to the portable data-log backend without changing public behavior.

The local v1 implementation may keep both in memory, but the distinction matters:
global metadata says which logical segment is referenced; local segment metadata
says where that segment's bytes live on a particular storage node. A logical
segment may have one local replica in v1 and multiple replicas later.

### Durable Provider Choice

The durable provider is local and single-process, but it preserves the same
plane boundaries expected from a remote implementation. Phase 16 started with
atomic snapshots to prove the durability boundary. Phase 20 replaced that with
one append-oriented journal. Phase 21 removed the journal production path under
the no-tombstones rule and now uses SQLite metadata plus rolled data logs.

```text
store/
  metadata.sqlite
  metadata.sqlite-wal
  metadata.sqlite-shm
  data/
    node-1/
      catalog.sqlite
      data-000001.log
      data-000002.log
    node-2/
      catalog.sqlite
      data-000001.log
  tmp/
```

The root metadata database and storage-node catalogs have separate durability
boundaries. The root `metadata.sqlite` stores logical metadata-plane rows:
counters/config, live and deleted heads, immutable metadata object payloads
keyed by stable IDs, commit timelines, checkpoints, GC mark epochs, and
scheduler state. Each storage node owns its own
`data/node-*/catalog.sqlite`, which stores that node's data-log manifests,
segment placement rows, segment descriptors, lifecycle catalog entries, and
local offsets. Data-log files own large immutable segment payloads. Complex
immutable objects may remain crate-owned binary payloads inside rows where
decomposing every field would add cost without a hot predicate. The old
whole-state blob and the old bundled root-storage catalog shape are not
production compatibility paths.

### Phase 18 Durable Format Decision

Phase 18 kept atomic binary snapshots for the local durable provider because
they were enough to prove the first crash/restart contract. Benchmarks then
showed the snapshot provider was a correctness baseline rather than a
performance-grade layout: fully flushed small writes were dominated by sync
count, and batched flush still paid one payload sync per segment file.

Phase 20 removes the snapshot production path instead of adding a second
compatibility layer. The old file-per-segment durable backend was removed with
it. The later row-native cleanup also removed the test-image wrapper and tests
current durable row payload codecs directly, so there is no snapshot-image
compatibility path in runtime or test code.

### Partitioned Durable Data Logs

The active durable layout stores transactional metadata in SQLite row tables
and segment payloads in partitioned data logs. The root metadata DB owns
committed logical metadata rows. Each node catalog DB owns node-local placement
state and data-log manifests through an independent connection. The local
provider deliberately does not depend on SQLite multi-database transactions:
that would be a local-only shortcut and would not map to remote storage nodes.
Plain data-log files own large immutable segment bytes. This keeps compaction
focused on selected data files instead of rewriting every live byte on a
storage node.

Data records live in rolled data-log files scoped by storage node. The v1
SQLite layout has row-native root metadata tables plus per-node
`catalog.sqlite` tables. In each node catalog, `segment_placements` maps
logical segments to current node-local log records and `data_logs` tracks
active, sealed, and deleted log files plus live/dead byte estimates. Reopen
reconstructs the local coordinator from the root DB plus every node catalog,
and rejects missing heads, missing immutable object payloads, missing node
catalogs, stale placements, catalog lifecycle gaps, or segment descriptors
whose data-log bytes are missing or corrupt.

Each data-log record carries a fixed magic, version, segment ID, payload length,
a payload-integrity tag, the tag's checksum field, and payload bytes. Verified
payloads use CRC32C; unchecked payloads store an unchecked tag and zero checksum
field. Reopen rejects a current verified placement if the record checksum,
segment ID, payload length, or integrity tag disagrees with SQLite placement
state. Unchecked placements are accepted without payload checksum verification.

Committed segment placement becomes:

```text
segment_id -> storage_node_id, data_log_id, offset, length, payload_integrity
```

Metadata leaves and native file extents still reference only logical
`SegmentId`s. The placement index resolves `SegmentId` to the current physical
data-log location. A relocation changes placement records, not user-visible
metadata roots.

Durable write ordering:

```text
append segment bytes to node data log(s)
fsync each touched data log once for the publish batch
commit each touched storage-node catalog with durable segment receipts
commit root metadata rows that reference those already-durable segments
```

The root SQLite transaction is the logical metadata publish boundary. It must
not commit a metadata root that references data-log bytes and node catalog
receipts that failed to reach the requested durability. If a storage node
commits segment bytes and catalog rows but the root metadata publish fails, the
segment remains invisible through the block/native APIs and is treated as a
storage-node orphan for later custodian cleanup. Reopen advances local
allocation cursors from node catalogs so those orphaned IDs are not reused.

The durable provider honors the public `WriteDurability` boundary. A successful
`Acknowledged` block or native file write is committed to the live in-process
mapping and visible to later reads, but may be lost across process crash until a
later `flush`, a `Flushed` write, or another synchronous metadata operation
persists the current state. A successful `Flushed` write has appended all newly
referenced segment data records, synced their data logs, committed the touched
node catalogs, and committed the root metadata transaction before the call
returns. `flush` persists the current live state with the same
data-before-metadata ordering and reports the latest commit sequence visible to
the relevant device or native file as `durable_through`.

Synchronous metadata operations that do not carry a `WriteDurability` argument
-- create, checkpoint, fork, restore, delete, keyspace snapshot, and custodian
operations -- persist before returning. Because the row-native publish cursor
captures the current durable high-water IDs and commit sequence, those
operations also flush any earlier acknowledged writes in the same store
instance.

Incremental data-log compaction:

1. Compute live bytes per sealed data log from reachable metadata, PITR
   retention, and placement records.
2. Delete a sealed data log immediately when it has no live segment payloads.
3. For a dirty log with enough dead space, copy only its live payload records
   into a new data log.
4. Fsync the new data log.
5. Commit a SQLite transaction that updates placements and relocation state.
6. Delete the old data log only after the SQLite transaction is durable.

This makes compaction cost proportional to selected dirty log bytes and selected
live relocated payloads. It must not be proportional to total live bytes on the
storage node.

Background compaction is a runtime policy layer, not hidden core behavior. The
deterministic core exposes node-scoped data-log accounting and explicit
maintenance ticks. The maintenance scheduler observes dirty bytes, sealed-log
count, WAL size, PITR retention horizon, custodian release evidence, and a
compaction cursor, then emits bounded compaction commands plus explicit write
admission/backpressure decisions. Tests can run the same policy by stepping a
deterministic observation trace without wall-clock reads, sleeps, or
process-global randomness. Manual maintenance is the default; opportunistic and
always-on local runtime modes are provider options below the public block/native
APIs. The compaction cursor is persisted with the durable provider so fair log
selection survives reopen. Opportunistic maintenance runs before the admitted
write; a maintenance failure cannot make an already-published write report
failure. Write backpressure is explicitly enabled by policy so the default
manual maintenance configuration can observe and run ticks without adding a
SQLite observation query to every hot write.

SQLite maintenance is separate from data compaction. WAL checkpointing,
integrity checks, and optional vacuum/incremental vacuum manage metadata file
growth. Metadata maintenance must not rewrite data payload logs.

Operational observability is a provider-native typed surface, not a telemetry
backend. Local and durable coordinators expose read-only diagnostics snapshots
with stable counters, gauges, per-node storage summaries, and bounded recent
events. Exporters such as Prometheus or OpenTelemetry adapters sit above this
surface and must not own storage decisions.

Counters record process-local totals such as write attempts, publish failures,
maintenance ticks, grant issuance, and receipt rejection reasons. Gauges are
derived from authoritative state: live heads, commit sequence, GC epoch,
checkpoint count, pending release evidence, SQLite WAL bytes, dirty and
reclaimable data-log bytes, and event-buffer accounting. Storage-node snapshots
report lifecycle counts, pending orphans, active/sealed log bytes, dirty bytes,
and reclaimable bytes from node-local catalogs and logs.

Events are deterministic breadcrumbs with monotonically increasing local
sequence numbers. They are bounded by a configured ring-buffer capacity, oldest
events are dropped first, and the dropped-event counter records overflow. Events
are not durable history; durable truth remains in metadata timelines,
storage-node catalogs, data-log records, checkpoints, and release evidence.

PITR and GC are part of the live-byte decision. A segment no longer reachable
from the current head may still be live because a retained checkpoint, restore
point, fork, or keyspace snapshot can reach it. Physical payload deletion is
allowed only after reachability and retention say the logical segment is no
longer needed.

### Local Multi-Storage-Node Placement

The local provider supports multiple storage-node endpoints in one process with
exactly one committed replica per logical segment. This proves the important
placement boundary without adding quorum behavior, repair, or remote failure
modes, and avoids multiplying a known whole-node compaction problem.

Placement is per segment:

```text
metadata leaf / file extent -> logical segment_id
placement index             -> segment_id -> storage_node_id + local placement
storage node catalog        -> segment_id -> local lifecycle state
```

A block device or native file is not assigned to one storage node. Different
ranges of the same device or file may resolve to different segment placements.
Keeping an append-heavy file colocated on one node is a placement-policy choice,
not a metadata invariant and not a public API promise.

The local multi-node write path is:

1. Choose one storage node for each new logical segment through the deterministic
   placement policy. The current policy is simple round-robin over the local
   registry.
2. Reserve that segment in the chosen node's `LocalSegmentCatalog`.
3. Write and sync bytes through that node's `SegmentStore`.
4. Commit the local placement as durable-pending-metadata.
5. Publish metadata that references only the logical `SegmentId`.
6. Mark the chosen node's local placement referenced after metadata publish.

The read path resolves each logical segment through the placement index, then
reads from that node's segment store. A single public read may fan into multiple
storage-node reads when the logical range spans segments on different nodes.

Durable replay must restore the storage-node registry, placement index,
per-node catalogs, and per-node segment bytes. A committed metadata reference to
a segment with no committed placement, multiple conflicting one-replica
placements, or missing/corrupt bytes is a provider error. It must not be treated
as a sparse zero range.

The local durable provider keeps the physical rolled data-log files behind the
same SQLite placement table, with `storage_node_id` stored in each placement.
That file layout remains implementation-private: public block/native APIs do
not expose node choice, and logical metadata never records data-log offsets or
storage-node-local paths.

The metadata custodian still owns reachability. It emits release evidence keyed
by logical segment and placement; storage-node custodians apply only evidence
for their own node. Storage nodes must not crawl metadata trees or infer
deletion from current heads.

### Authenticated Write Grants and Segment Receipts

The coordinator split also allows a future trusted-client fast path where a
client writes directly to storage nodes, similar in shape to cloud systems that
authorize data-plane access separately from metadata-plane commits. The client
may carry proof between services, but it must not create truth.

The intended flow is:

```text
client/coordinator asks metadata for a write grant
client/coordinator writes bytes to the granted storage node
storage node returns a verifiable durable-pending segment receipt
client/coordinator submits grant, receipt, and metadata update intent
metadata verifies the grant, receipt, grant/receipt binding, and fencing, then publishes roots
coordinator/metadata-produced evidence marks the storage node referenced
```

A write grant should bind the tenant, principal, mapping owner, operation
intent, write intent, segment identity or reservation class, byte length,
placement node or placement-policy result, durability requirement, caller
identity, expiration, and metadata epoch. A segment receipt should bind storage
node, storage-node incarnation, segment ID, grant ID/hash, write intent, owner,
bytes, payload integrity, durability reached, durable-pending lifecycle state,
receipt epoch/expiration, node key ID, proof scheme, and proof bytes.

Receipt proofs use a canonical crate-owned binary receipt body with a domain
separator such as `TCOW_SEGMENT_RECEIPT_V1`. The proof covers the payload
integrity mode/value and all logical/placement fields, not the full payload bytes. Phase 30
turns this into the minimal production proof boundary: remote or
trusted-client production modes must reject deterministic test proofs and use a
real keyed proof scheme verified through a persisted active key registry. A
symmetric cluster MAC is acceptable only inside one controlled trust domain
where clients never hold the storage-node or grant-authority secrets.
Asymmetric storage-node signatures (`NodeSignatureV1`) remain the preferred
shape when receipts need independent verification or stronger node
accountability.

That Phase 30 boundary is production-secure for forged, stale, wrong-scope, and
replayed grant, receipt, and reference evidence under a provisioned active
keyset. Later key rotation, retirement, revocation, admin inspection, and
external authorization policy are operational layers. They must not change the
canonical proof bodies or create a second path for making segment bytes
logically visible.

Metadata verifies grant/receipt evidence and their binding before it accepts a
new segment reference, but must still not open storage-node catalogs or read
segment bytes. Storage nodes may authenticate a grant and issue a receipt, but
must still not publish metadata, assign file versions, or mark a segment
referenced from a client's word alone. Reference state follows
metadata-produced evidence after publish.

### Remote Storage-Node Transport

The coordinator-to-storage-node boundary is a separate network surface from the
public block/native client transports. A remote storage-node transport carries
the same typed messages as the in-process `StorageNodeTransport`: write segment
with grant, read segment, mark referenced with metadata evidence, release,
custodian, and maintenance requests.

Remote storage nodes remain storage authorities only. They own local data logs,
catalog lifecycle, payload checksums, storage-node incarnation, and local
maintenance. They do not read metadata roots, choose logical placement, publish
device or keyspace heads, or infer deletion from current metadata. The
coordinator remains the only role that talks to both metadata and storage nodes.

A real remote transport must be retry-safe and restart-safe before replication:
request IDs, deadlines, bounded frames, stale-response rejection, corrupt-frame
rejection, server-incarnation fencing, duplicate write idempotency, and
deterministic chaos tests are part of the storage-node transport contract. This
single-replica remote path should be proven before quorum behavior adds more
ways to be wrong.

### Future Storage Replication

Replication belongs below the public block/native APIs and above individual
`SegmentStore` implementations. Public clients may eventually request a
durability or replication class, but they should not fan out writes to replicas
or choose storage nodes.

Replication should build on authenticated write grants and segment receipts,
not invent a separate proof path. A replicated publish waits for a set of
verifiable receipts that satisfies the requested durability class.

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

Replicated storage also needs durable reference/release evidence between the
metadata custodian and storage-node custodians. The pragmatic local durable
shape should use SQLite outbox tables keyed by safe commit/reachability epoch
and per-storage-node apply cursor tables. External queues or custom evidence
logs are later adapters only if remote deployment or benchmarks require them.
Storage nodes must not crawl metadata trees or infer deletion from current
heads. They apply metadata-produced evidence to their local replica catalogs,
free matching physical bytes, and record cursor progress so missed, duplicated,
delayed, or reordered notifications can be reconciled.

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
  Durable providers must define crash-consistent segment sync, atomic metadata
  snapshot or journal/database sync, and exact `durable_through` semantics.
- In-memory commit groups prove atomic root visibility inside one metadata
  plane. Durable providers must persist commit records or snapshots, replay
  partial publish attempts safely, and preserve compare-and-swap fences after
  restart.
- Write-intent expiry is injected by deterministic tests. Durable providers must
  store write-intent state, logical expiration, recovery scans, and custodian
  evidence for when pending segment data can no longer publish.
- Local segment catalog transitions to `Referenced` happen in process after
  metadata publish. Remote storage nodes need durable reference evidence or
  reconciliation so `DurablePendingMetadata` replicas become referenced after
  missed notifications or restarts.
- Segment release evidence is returned as an in-process vector in v1. Replicated
  storage should first use SQLite outbox/apply-cursor tables for release and
  reference evidence; custom durable logs or external queues need specific
  remote-deployment or benchmark evidence.
- Storage-node write receipts now use authenticated grant and receipt bodies in
  the local provider. Remote adapters still need a network encoding and Phase 30
  production proof verifier before those receipts can cross trust boundaries.
  Key rotation/revocation and tenant authorization policy are operational
  follow-up layers, not alternate proof paths.
- Durable compaction is one local append log in Phase 20. The SQLite metadata
  plus partitioned-data-log phase needs placement tables, relocation
  transactions, data-log manifests, and incremental compaction tests before
  multi-node placement spreads the same problem across nodes.
- Placement is one local storage endpoint in v1. The multi-node placement phase
  needs storage-node registry state, deterministic one-replica placement,
  per-node catalog routing, and stale placement tests before replication adds
  replica sets.
- Replication builds on multi-node placement. It needs capacity and
  failure-domain policy, replica-set selection, quorum durability, and stale
  replica-placement tests.
- Repair is only a design hook in v1. Replication should first use SQLite repair
  job and cursor tables for idempotent copy, source selection, checksum
  validation, and restart; custom repair queues need specific remote-deployment
  or benchmark evidence.
- PITR retention is deterministic commit-age policy in v1. Durable providers may
  add richer per-owner policies, but expiration must still be driven by injected
  logical time or commit epochs, not hidden wall-clock reads.
- Native append stream stealing is local metadata state in v1. Durable providers
  need restart-safe stream identities, writer epochs, and tests where stale
  writers race after durable private segment writes.
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
   native append streams, this write intent is tied to the stream, append
   ticket, and writer epoch.
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
   commit. This referenced transition is repairable: if the root metadata
   commit survives a crash but the final catalog state update does not, reopen
   promotes only catalog entries that are actually referenced by committed
   metadata rows before exposing the store.
10. Acknowledges the public write only after the metadata commit group succeeds.

If steps 1-6 succeed but metadata publish fails, the segment is an orphan. It is
durable local replica data but not reachable from any committed device or file
root. Orphans are not user-visible and must be reclaimed by custodian work after
their write intent can no longer commit. The pre-root catalog state must remain
`DurablePendingMetadata`; writing `Referenced` before the root commit would make
failed publishes indistinguishable from committed data during recovery.

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
coherent; those operations copy catalog shard-root sets rather than
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
version of zero, then publishes a new immutable keyspace catalog shard
containing that file head. File metadata roots are GC roots while their
catalog shard is live or retained by PITR policy.

Invariants:

- Empty native files read as empty.
- The file version changes only through committed metadata updates.
- The file root is separate from block device shard roots.

### Native File Batch Commit

An ordinary native file write is represented internally as an atomic
`commit_file_batch` against one file inside a keyspace. Public client adapters
may expose one-write helpers, but storage internals keep a single random-write
primitive. A batch can overwrite existing bytes or extend the file from the
current end, but v1 rejects sparse writes beyond EOF. Overlapping writes inside
one batch resolve by request order before metadata publication. The local segment store remains
block-aligned internally; unaligned file ranges are implemented by copying the
affected logical blocks into fresh immutable segments and publishing a new file
root once for the collapsed batch.

Invariants:

- A successful batch advances the file version atomically with the containing
  keyspace catalog shard exactly once.
- Batches are fenced by the committed file version observed by the metadata
  plane.
- Failed batches leave the previous file version readable.
- Segment/block alignment is not exposed to native callers.

### Append Streams, Durable Marks, And Visible Publish

A native append stream grants one writer the right to ingest private append
bytes against a specific file and writer epoch. Appended bytes are not visible
to file readers when they are ingested. A stream flush makes private bytes
durable through a returned high-water mark, and a publish converts a durable
range into visible file metadata in one file-version transition.

The production failover model is token-based, not name-based. `AppendStream` is
bearer authority for one private stream, and `DurableAppendMark` is meaningful
only with that matching stream token. There is no first-class registry that lets
a replacement writer discover flushed private bytes by logical file name. A
replacement writer can resume or publish flushed-but-unpublished bytes only if
the failover control plane persisted the stream token and durable mark. Without
that authority it must open a new stream, which fences the old stream and starts
at the last visible file head.

This keeps the storage contract simple: publish is the only globally
discoverable append boundary. WAL-like users that require another process to
recover by file name must publish at their desired recovery interval. Users that
want private durable checkpoints may flush more often, but those checkpoints are
private to the stream authority until publish succeeds.

Invariants:

- Append streams carry `keyspace_id`, `file_id`, `stream_id`, `base_version`,
  `visible_base_size`, and `writer_epoch`.
- Append tickets identify the private byte range accepted by a stream append;
  they are diagnostic evidence, not metadata publish authority.
- `append_stream` success is an acknowledged private ingest. It is not
  restart-resumable until `flush_append_stream` succeeds.
- `flush_append_stream` persists private append records and returns a durable
  mark, but does not advance the visible file head or make the bytes globally
  discoverable.
- `publish_append_stream` may only reference private records covered by a
  durable mark and is the globally durable, visible, file-version boundary.
- A stale stream cannot ingest, flush, or publish private data.
- Opening a new stream or committing a same-file `write_at` fences the previous
  active stream for that file.
- Writers take over at the visible file head. Durable-but-unpublished private
  bytes belong to the fenced stream token and are not inherited by a new writer.

### Native Append Publish

A native append publish:

1. Validates the append stream and durable mark against the active stream state.
2. Verifies the visible file head still matches the stream's published boundary.
3. Coalesces the durable private range into compact run-backed file extents.
4. Publishes the new file root with append-stream and writer-epoch fencing.
5. Marks the stream range published after metadata publish succeeds.
6. Returns the new file version.

Invariants:

- A successful publish advances the file version atomically.
- Stale writers are rejected by metadata fencing.
- Flush without publish never becomes readable file data.
- Publish of already-flushed append-stream data is metadata-only; it must not
  append or sync payload bytes again.
- Publish metadata scales with durable run count, not with client append call
  count.
- Durable private data from fenced, aborted, or abandoned streams is reclaimable
  once it stops acting as an active stream GC root.

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

Native operations append keyspace catalog-shard commit records. File-root
changes are also recorded as audit/replay evidence inside the keyspace commit:

```text
KeyspaceCommit {
  commit_seq
  commit_group
  time
  keyspace_id
  shard_index
  old_shard
  new_shard
  old_file_count
  new_file_count
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

The native restore API creates a new keyspace from the reconstructed catalog
shard-root set. This is intentionally not a per-file restore: every file in the
catalog is restored together, and a stale append stream from the source
keyspace cannot publish into the restored keyspace because append streams carry
`keyspace_id`.

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

1. Start from all live device shard roots, live native keyspace catalog shards,
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
replicated implementation, the same concept should first become SQLite-backed
outbox rows keyed by safe reachability epoch plus per-storage-node apply
cursors. Consumers must be able to replay from a cursor, tolerate duplicate
records, and reconcile missed release events without asking storage nodes to
interpret global metadata reachability themselves. Custom logs or external
queues are later adapters only when remote deployment or benchmarks justify
them.

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
- `NativeFile`: public native file handle with byte writes, append streams,
  durable marks, visible publish, and file-version commits.
- `NativeServer`: actor boundary that handles native keyspace/file requests.
- `NativeTransport`: typed request/response transport for native keyspace/file
  requests.
- `PosixNamespaceClient`: planned public POSIX namespace control handle for
  filesystem-level create, lookup, rename, unlink, truncate, fsync, checkpoint,
  snapshot, and restore operations.
- `PosixFile`: planned POSIX file handle with open-handle semantics distinct
  from ordinary native file handles.
- `PosixServer`: planned actor boundary for POSIX namespace requests.
- `PosixTransport`: planned typed request/response transport for POSIX
  namespace and file requests.
- `MetadataPlane`: device catalog, metadata nodes, commit groups, PITR, and GC
  roots for block, native file, and POSIX namespace metadata.
- `LocalCoordinator`: embedded trusted coordinator that sequences storage-node
  receipts, metadata publishes, reference marking, reads, GC releases, and
  maintenance routing.
- `StorageNodeTransport`: coordinator-to-storage-node message contract for
  writing, reading, referencing, releasing, custodians, and maintenance ticks.
- `StorageNodeDirectory`: provider-private resolver from logical segments or
  node IDs to storage-node transports.
- `SegmentStore`: write and read immutable segment bytes.
- `LocalSegmentCatalog`: per-storage-node replica placement and local segment
  state.
- `PartitionedDataLogStore`: durable manager for rolled segment payload logs,
  manifests, checksums, and replay of data-log tails.
- `SqliteMetadataStore`: durable metadata provider for roots, commits,
  placement index, lifecycle state, PITR, append-stream state, manifests, and
  custodian evidence.
- `CompactionPlanner`: deterministic maintenance policy that selects sealed
  data logs for deletion or live-payload relocation.
- `StorageNodeRegistry`: internal provider map from `StorageNodeId` to the
  segment store and local catalog for that node.
- `PlacementPolicy`: deterministic internal policy that chooses storage node
  placement for new logical segments without exposing node choice to clients.
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
- Every live native keyspace has exactly `K` catalog shard roots.
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
- Native append publishes are atomic at file-version and keyspace-catalog
  granularity.
- Stale native append streams cannot publish file metadata across keyspace
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
- Real network transport for block/native servers.
- Cross-machine replication.
- A full POSIX filesystem implementation before the dedicated POSIX namespace
  phase.
- Compression, encryption, or deduplication.
- Segment compaction.
- Online shard splitting.
- Eager deep refcounts.
- Compatibility shims for old internal formats.
- Background actors in deterministic core logic.

Any addition to this list needs a failing deterministic simulation, a benchmark,
or a concrete correctness gap. Convenience is not enough.
