# CoW Block Storage Design Spec

Status: draft  
Project: `toy-cow-block-storage`

## 1. Summary

This project is a toy copy-on-write block device built around immutable data
segments and immutable sharded metadata trees. A block device is represented by
a small device head:

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

### Priority Order

1. Correctness: reads observe the latest committed mapping for their device and
   logical range; forks remain isolated after either side writes; GC never
   reclaims reachable data.
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

## 6. Public API and Service Planes

The public API should model the behavior expected from a block device. Shards,
metadata-tree paths, segment placement, and provider topology are implementation
details. A caller should be able to treat a write request as one committed block
operation, even when the implementation splits it across shards.

The first implementation runs locally in one process, but the service
boundaries should be real from the beginning:

```text
BlockClient / BlockDevice API
  -> BlockTransport
     -> BlockServer actor
        -> MetadataPlane
        -> SegmentStore
        -> LocalSegmentCatalog
```

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

### `BlockServer`

`BlockServer` is an actor boundary. The local v1 server may be a simple
single-threaded mailbox or direct deterministic actor, but its interface should
look like a server request/response protocol. It owns request ordering,
backpressure, commit assembly, and the translation from public requests to core
commands and provider effects.

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
- current device heads
- shard-root publish and compare-and-swap
- commit groups for multi-shard atomic public writes
- PITR shard commits and checkpoints
- metadata node durability
- retained roots for GC
- cache of hot heads and metadata nodes

### `SegmentStore` and `LocalSegmentCatalog`

`SegmentStore` reads and writes immutable segment bytes. It may be memory-backed,
file-backed, or remote later.

`LocalSegmentCatalog` is local to a block server or storage node. It maps
segment IDs to local disk placement, tracks checksums and write-complete state,
and exposes deletion eligibility from that server's perspective.

The local v1 implementation may keep both in memory, but the distinction matters:
global metadata says which segment is logically referenced; local segment
metadata says where that segment's bytes live on a particular server.

### Write Ordering Contract

Writes use a data-before-metadata commit discipline. Metadata must never publish
a reference to segment bytes that have not reached the requested durability
level.

For a public write request, the block server:

1. Selects the local or remote block server that will hold the new segment
   bytes.
2. Creates a stable write-intent identity for the request or commit group.
3. Reserves segment space in that server's `LocalSegmentCatalog` under that
   write intent.
4. Writes bytes through `SegmentStore`.
5. Flushes or syncs those bytes according to the requested durability level.
6. Commits the local segment catalog entry as durable-pending-metadata.
7. Persists the new immutable metadata nodes that reference the durable segment
   slices.
8. Publishes the device metadata update through a metadata commit group.
9. Marks the local segment catalog entry as referenced by the successful commit.
10. Acknowledges the public write only after the metadata commit group succeeds.

If steps 1-6 succeed but metadata publish fails, the segment is an orphan. It is
durable local data but not reachable from any committed device root. Orphans are
not user-visible and must be reclaimed by custodian work after their write intent
can no longer commit.

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

Invariants:

- A forked child reads exactly the same logical contents as the parent at fork
  time.
- A later write to either device publishes only that device's changed shard
  root.
- Shared metadata and segments remain immutable.

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
timeline record. It does not synchronously delete metadata nodes or segments.
Reclamation belongs to GC.

Invariants:

- Deleted devices are not GC roots after the delete commit becomes visible.
- PITR policy decides whether older checkpoint/timeline entries can still make
  deleted device state reachable.

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
manifest on every write. Instead, it appends per-shard commit records:

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

Periodically, it writes checkpoint manifests:

```text
Checkpoint {
  commit_seq
  time
  device_id
  shard_roots[]
}
```

Restore to time `T`:

1. Load the latest checkpoint for the device at or before `T`.
2. Replay shard-root commits for that device after the checkpoint and up to
   `T`.
3. Return a reconstructed `DeviceHead`.

Invariants:

- `commit_seq` is total ordered within the timeline provider.
- All shard commits in a public multi-shard write share a commit-group identity.
- Replaying checkpoint plus commits is deterministic.
- Checkpoint roots must match replayed state at the checkpoint sequence.
- PITR retention policy is part of GC root selection.

## 11. Garbage Collection

The project should not eagerly maintain deep refcounts on fork. Fork would stop
being O(1), and every snapshot would require walking metadata.

Use tracing GC:

1. Start from all live device shard roots and retained PITR checkpoint/timeline
   roots.
2. Mark reachable metadata nodes.
3. Mark segment IDs referenced by reachable leaf entries.
4. Sweep unmarked metadata nodes after the mark epoch is safe.
5. Publish release evidence for unmarked segment IDs so block-server custodians
   can reclaim local physical bytes.

Each object may store:

```text
last_mark_epoch
```

The metadata sweeper deletes metadata objects not marked in the latest safe
sweep. Segment bytes are freed by block-server custodians after they receive
release evidence. The exact safe sweep rule depends on the provider, but the
deterministic model must prove that objects reachable from any live root or
retained PITR root are never deleted.

Invariants:

- Mark traversal starts only from committed roots.
- Sweep never deletes an object marked in the latest safe epoch.
- Device deletion and PITR retention changes affect only root selection, not
  object mutability.

## 12. Custodians and Orphan Reclamation

Garbage collection determines logical reachability, but physical reclamation is
split between metadata and block-server custodians.

### Metadata Custodian

The metadata custodian owns global reachability. It periodically:

1. Enumerates all live device heads.
2. Adds retained PITR checkpoint and timeline roots.
3. Traverses reachable metadata nodes.
4. Records segment IDs referenced by reachable leaf entries.
5. Publishes a safe reachability epoch for metadata nodes and segment IDs.
6. Emits release candidates for metadata nodes and segment references that are
   unreachable after the chosen retention policy.

The metadata custodian does not delete local segment bytes directly. It produces
evidence that a segment is no longer referenced by committed metadata or retained
PITR roots.

### Block-Server Custodian

Each block server owns its local physical segment catalog. It periodically:

1. Frees expired reservations that never reached durable write.
2. Frees failed writes that never reached durable segment state.
3. Finds durable segments that are still pending metadata after their write
   intent has expired or definitively failed.
4. Applies release evidence from the metadata custodian.
5. Reconciles missed asynchronous frees by comparing local catalog state with
   the latest safe reachability epoch.

The block-server custodian is the only component that frees local physical
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
- The block-server custodian never frees a segment that is reachable from a live
  device or retained PITR root.
- The block-server custodian never frees `DurablePendingMetadata` while its
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
- `MetadataPlane`: device catalog, metadata nodes, commit groups, PITR, and GC
  roots.
- `SegmentStore`: write and read immutable segment bytes.
- `LocalSegmentCatalog`: per-server segment placement and local segment state.
- `MetadataCustodian`: global metadata and segment-reference reachability.
- `BlockServerCustodian`: local reservation, orphan, release, and free
  reconciliation.

The in-memory provider is the first implementation and the source of provider
conformance tests. Durable providers must pass the same tests before they are
trusted.

## 14. Correctness Invariants

The simulator and tests should check these invariants after every delivered
command:

- Every live device has exactly `N` shard roots.
- Every committed shard root points to an existing metadata node.
- Every metadata child pointer points to an existing metadata node.
- Every leaf segment reference points to an existing segment.
- Leaf entries are sorted, non-overlapping, and within the leaf range.
- Segment slices stay within segment bounds.
- Metadata references only segments that were durably written before metadata
  publish.
- Reads after writes return the latest committed bytes for the target device.
- Public writes spanning shards are atomic at request granularity.
- Forked devices initially read identically to their parent.
- After divergence, writes to one fork do not change reads from the other fork.
- A failed publish does not expose partially written metadata.
- A failed publish after durable segment write leaves only reclaimable orphan
  segment data.
- Replaying PITR checkpoint plus commits reconstructs the same device head.
- GC never deletes an object reachable from live or retained PITR roots.
- Custodians eventually reclaim expired reservations, failed writes, orphan
  segments, and missed async frees without deleting reachable data.

## 15. Simplicity Guardrails

V1 should stay intentionally small.

V1 uses:

- Fixed block size per device.
- Fixed shard count per device lineage.
- Immutable segment objects.
- Immutable metadata nodes.
- A deterministic tree shape.
- One segment per shard-local write chunk.
- Commit groups for public writes that touch multiple shards.
- Explicit segment lifecycle states for reservation, durable-pending-metadata,
  referenced, released, and freed.
- Append-only shard commit records.
- Periodic full device checkpoints.
- Tracing GC.
- Metadata and block-server custodians.
- In-memory provider first.

V1 does not use:

- Kernel integration.
- Cross-machine replication.
- Compression, encryption, or deduplication.
- Segment compaction.
- Online shard splitting.
- Eager deep refcounts.
- Compatibility shims for old internal formats.
- Background actors in deterministic core logic.

Any addition to this list needs a failing deterministic simulation, a benchmark,
or a concrete correctness gap. Convenience is not enough.
