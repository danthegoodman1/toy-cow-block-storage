# Ordered Implementation Plan

Status: draft  
Project: `toy-cow-block-storage`

This plan turns the design spec into an ordered build sequence. The goal is to
make it hard to accidentally build a clever but untrusted storage system. Each
phase has a narrow output and an exit gate. Do not start a later phase until the
current phase meets its gate, unless this plan is updated with an explicit
reason.

The implementation bias is scalability through simplicity: prove the smallest
deterministic mechanism first, measure it, and only add complexity when a
correctness test, recovery test, or benchmark demonstrates that the simple
mechanism is insufficient.

## Phase 0: Project Harness

Status: complete.

Build the basic Rust project structure, deterministic test conventions, and
regression benchmark harness.

Deliverables:

- [x] Library crate entry point for reusable modules.
- [x] Thin binary entry point that does not own core logic.
- [x] Module skeletons for block API contracts, native extent/file API
  contracts, core state, object model, providers, and simulator.
- [x] Shared deterministic test utilities for fake time, seeded randomness, and
  trace recording.
- [x] Criterion benchmark dependency and starter regression benchmark suite.
- [x] CI-friendly commands documented in the repo.

Exit gate:

- [x] `cargo fmt --check` passes.
- [x] `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [x] `cargo test` passes.
- [x] `cargo doc --no-deps` passes.
- [x] `cargo bench --bench regression -- --test` passes.
- [x] A tiny deterministic test proves seed and trace capture.

## Phase 1: Public API and Service Contracts

Status: complete.

Define the external contracts before implementing storage behavior. The expected
block-device API is a first-class compatibility constraint, and the native
extent/file API is a first-class performance constraint. Shards, metadata nodes,
segments, and local-vs-remote placement are implementation details.

Deliverables:

- [x] Public `BlockClient` trait for create and device info lookup.
- [x] Public `BlockDevice` trait for aligned reads, writes, flush, zeroing,
  discard, fork, restore, delete, and info.
- [x] Public `NativeKeyspaceClient` trait for native keyspace create/info,
  file create/info/append-lease lookup, keyspace checkpoint, snapshot, and
  restore.
- [x] Public `NativeFile` trait for native file reads, byte writes, append
  leases, leased appends, flush, and info.
- [x] `BlockServer` actor boundary.
- [x] `BlockTransport` request/response boundary.
- [x] `NativeServer` actor boundary.
- [x] `NativeTransport` request/response boundary.
- [x] Typed request and response envelopes with request ID, client epoch, and
  optional logical deadline.
- [x] Public device spec limited to logical device size and block size.
- [x] `MetadataPlane` contract for device heads, native keyspace heads,
  metadata nodes, commit groups, native file heads, file versions, PITR,
  checkpoints, forks, restores, and GC roots.
- [x] `SegmentStore` contract for immutable segment bytes on one storage
  endpoint.
- [x] `LocalSegmentCatalog` contract for per-storage-node replica placement.
- [x] Segment placement records model one local replica, so a future replicated
  coordinator can collect multiple replica commits for one logical `SegmentId`.
- [x] Implementor-focused rustdoc for every public service/provider trait and
  method, including success visibility, failure atomicity, durability, fencing,
  and implementation-private details.
- [x] Opaque IDs for devices, requests, client epochs, commits, commit groups,
  checkpoints, shards, segments, metadata nodes, files, file versions, append
  leases, writer epochs, extents, storage nodes, and write intents.
- [x] Basic API validation for device specs, aligned byte ranges, zero-length
  no-ops, and overflow cases.
- [x] Separate validation paths for create requests and existing-device
  requests.
- [x] Public create request/response envelope.
- [x] Native create/info/append/lease request and response envelopes.
- [x] Native append validation for append payloads and lease/file matching.
- [x] Public service traits require `Send + Sync` so local and remote adapters
  can share the same contract.

Exit gate:

- [x] Public block requests do not expose shard IDs, segment IDs, metadata node
  IDs, shard counts, or commit assembly details.
- [x] Native file requests do not flow through or depend on block-device logical
  range metadata.
- [x] The documented public contract treats successful writes as atomic at
  request granularity.
- [x] The documented native contract treats successful append commits as atomic
  at file-version granularity and fenced by append lease/writer epoch.
- [x] Provider contracts state the minimal guarantees an in-memory, local
  durable, or remote implementation must preserve.
- [x] Public clients are not responsible for replica fan-out or storage-node
  selection.
- [x] Service boundaries can be implemented locally now and remotely later
  without changing the public block or native APIs.
- [x] Contract tests cover device spec validation, range alignment, request
  targeting, request kind/range extraction, create-vs-existing-device
  validation, native lease/file matching, deterministic trace replay, and the
  starter benchmark harness.

## Phase 2: Core Types and Invariants

Status: complete.

Define the internal identities and immutable object shapes before implementing
operations.

Deliverables:

- [x] `DeviceHead` validation with fixed `shard_roots`.
- [x] `FileHead` validation with current file root and monotonic file version.
- [x] `MetadataNode` validation for internal and leaf variants.
- [x] `LeafEntry` validation for range-to-segment mappings.
- [x] Range helpers for split, overlap, adjacency, and bounds checks.
- [x] Segment descriptor validation.
- [x] Criterion baselines for range helpers and metadata leaf validation.

Exit gate:

- [x] Unit tests cover range arithmetic edge cases.
- [x] Leaf validation rejects overlaps, unsorted entries, zero-length entries,
  and out-of-bounds segment slices.
- [x] Device-head validation requires exactly the configured shard count.
- [x] File-head validation rejects regressing versions and out-of-bounds file
  sizes.
- [x] Core object types do not perform I/O or read wall-clock time.

## Phase 3: Deterministic Core Contract

Status: complete.

Define the state-machine boundary before implementing storage behavior.

Deliverables:

- [x] `StorageState`.
- [x] `StorageCommand`.
- [x] `StorageEffect`.
- [x] `StorageState::step(command) -> Vec<StorageEffect>`.
- [x] Explicit effects for write-intent creation, segment reservations, segment
  writes, segment syncs, durable-pending-metadata catalog commits, referenced
  catalog commits, metadata writes, device-head publishes, file-head publishes,
  commit-group publishes, timeline appends, checkpoints, custodian scans, and GC
  deletes.

Exit gate:

- [x] Compile-time module boundaries make hidden side effects awkward.
- [x] Tests prove identical initial state, seed, and command trace produce
  identical effects.
- [x] No async, I/O, wall-clock reads, provider calls, or process-global
  randomness in the core.

## Phase 4: Local In-Process Services and Object Store

Status: complete.

Build local implementations of the service boundaries without durable storage.

Deliverables:

- [x] Local `BlockServer` implementation.
- [x] Local `NativeServer` implementation.
- [x] Local in-process `BlockTransport`.
- [x] Local in-process `NativeTransport`.
- [x] In-memory `MetadataPlane`.
- [x] In-memory `SegmentStore`.
- [x] In-memory `LocalSegmentCatalog`.
- [x] Local segment lifecycle states for `Reserved`, `Writing`,
  `DurablePendingMetadata`, `Referenced`, `Released`, and `Freed`.
- [x] Provider conformance tests for immutable writes, lookup, idempotency, and
  missing-object errors.
- [x] Criterion baselines for in-memory metadata lookup and segment read.

Exit gate:

- [x] Existing object IDs are immutable.
- [x] Duplicate writes with identical content are idempotent or rejected by a
  documented rule.
- [x] Duplicate writes with different content cannot mutate the original object.
- [x] Local services preserve request identity and deterministic ordering.
- [x] Block and native services share segment lifecycle and write-intent
  machinery instead of duplicating it.
- [x] Local segment catalog transitions reject invalid state jumps.
- [x] Expired reservations and failed writes can be reconciled without metadata
  changes.
- [x] Provider behavior is deterministic under ordered commands.

## Phase 5: Empty Devices and Sparse Reads

Status: complete.

Implement block device creation/reads from empty shard trees and native file
creation/reads from empty file roots through the public APIs and local servers.

Deliverables:

- [x] Configurable public block size and logical block count.
- [x] Internal layout config for shard count and blocks-per-shard.
- [x] Empty metadata tree creation for every shard.
- [x] Public create/open path.
- [x] `Read` request over empty and sparse ranges.
- [x] Zero-filled sparse block behavior.
- [x] Native file create/info/open path.
- [x] Native file read over empty files.

Exit gate:

- [x] Created devices have exactly one committed root per shard.
- [x] Reads from empty devices return zero-filled blocks.
- [x] Reads spanning shard boundaries return bytes in logical order.
- [x] Empty native files report size zero and return empty reads.
- [x] Simulation checks root existence after every create/read command.
- [x] Criterion has a baseline read benchmark.

## Phase 6: Atomic Writes and Commit Groups

Status: complete.

Implement the block write path with public request-granularity atomicity and the
native append path with file-version atomicity.

Deliverables:

- [x] Range splitter from public byte writes to shard-local chunks.
- [x] Stable write-intent identity for each public write or commit group.
- [x] Stable write-intent identity tied to each native append lease.
- [x] Block-server selection and local segment reservation.
- [x] Segment creation for written bytes.
- [x] Segment sync before metadata references are created.
- [x] Local segment catalog commit to `DurablePendingMetadata` before metadata
  publish.
- [x] Local segment catalog transition to `Referenced` after metadata publish
  succeeds.
- [x] Leaf insertion, replacement, and splitting for overwrites.
- [x] Root-to-leaf path copy for each affected shard.
- [x] Commit-group prepare/publish model for multi-shard writes.
- [x] Native append commit model with file-version and writer-epoch fencing.
- [x] Per-shard commit records linked by commit-group identity.
- [x] Native file extent commit records linked by commit-group identity.
- [x] Orphan segment records when durable segment writes outlive failed metadata
  publish attempts.
- [x] Documented no-implicit-retry policy for publish conflicts.

Exit gate:

- [x] Read-after-write returns the latest committed bytes.
- [x] Overwrites preserve untouched prefix and suffix mappings.
- [x] Failed publish leaves all old roots readable.
- [x] Failed publish after durable segment write creates no readable data and
  leaves a reclaimable orphan.
- [x] Metadata leaves never reference segments before segment sync and local
  catalog commit.
- [x] Public writes spanning shards expose either the old mapping or the complete
  new mapping, never a partial update.
- [x] Native appends expose either the old file version or the complete new file
  version, never a partial extent update.
- [x] Stale native append leases are rejected deterministically.
- [x] Conflicting writes to the same shard resolve deterministically.
- [x] Table-driven tests cover beginning, middle, end, full-range, same-range,
  and cross-shard overwrites.
- [x] Table-driven tests cover valid append, stale lease rejection, lease
  stealing, and append publish failure orphan cleanup.
- [x] Criterion has baseline write benchmarks.

## Phase 7: Metadata Tree Shape

Status: complete.

Generalize beyond a single leaf while keeping the tree deterministic.

Deliverables:

- [x] Fixed fanout or bounded leaf-capacity policy.
- [x] Deterministic node split behavior.
- [x] Internal node lookup and path-copy logic.
- [x] Tree validation utilities.
- [x] Small debug renderer for failing traces.

Exit gate:

- [x] Tree shape is deterministic for a given write trace.
- [x] Internal child ranges cover the parent range without overlap.
- [x] Root-to-leaf path copy changes only the necessary nodes.
- [x] Generated tests compare block tree reads and native file extent reads
  against simple map models.
- [x] Criterion covers write cost versus tree depth.

## Phase 8: Forks

Status: complete.

Implement O(1) device forks.

Deliverables:

- [x] Public fork request through `BlockDevice` and `BlockServer`.
- [x] Child device-head creation by copying shard roots.
- [x] Native file snapshot/fork decision documented before adding it to the
  native API.
- [x] Fork timeline/catalog record.
- [x] Tests that prove no metadata walk occurs during fork.

Exit gate:

- [x] Fork cost is independent of logical device size and tree size.
- [x] Parent and child initially read identical bytes.
- [x] Writing parent after fork does not change child reads.
- [x] Writing child after fork does not change parent reads.
- [x] Generated tests cover repeated forks and divergent write histories.
- [x] Criterion covers fork cost versus device size.

## Phase 9: Point-In-Time Recovery

Status: complete.

Implement shard commit replay and checkpoints.

Deliverables:

- [x] Append-only `ShardCommit` records.
- [x] Periodic `Checkpoint` records.
- [x] Restore algorithm from checkpoint plus commits.
- [x] Deterministic PITR commit-age retention window with replay-anchor
  checkpoint materialization.
- [x] Public restore request that creates a new device.
- [x] Timeline validation.
- [x] Tests for create, write, fork, and restore interactions.

Exit gate:

- [x] Replayed roots match live roots at tested commit sequences.
- [x] Restore to selected times returns expected device contents.
- [x] Checkpoint corruption or mismatch is detected by validation.
- [x] Generated traces compare PITR reads against a simple historical model.
- [x] Restore fails cleanly if GC has swept metadata needed by an expired restore
  point.
- [x] Criterion covers checkpoint restore.

## Phase 10: Device Deletion and Retention Roots

Status: complete.

Implement deletion without synchronous reclamation.

Deliverables:

- [x] Public delete request through `BlockDevice` and `BlockServer`.
- [x] Device catalog state for live and deleted devices.
- [x] PITR retention policy model with indefinite retention, immediate expiry,
  and deterministic commit-age grace for deleted-device roots.
- [x] Root enumerator for live devices plus retained PITR state.
- [x] Delete interaction tests for retained PITR checkpoints and timelines.

Exit gate:

- [x] Deleted devices are absent from live listings.
- [x] Deleted device objects remain readable only through retained PITR roots.
- [x] Root enumeration is deterministic and independently testable.
- [x] Deleted-device retention does not depend on wall-clock time.
- [x] Deletion never directly deletes metadata nodes or segments.

## Phase 11: Tracing Garbage Collection

Status: complete.

Build reachability-based reclamation and custodian-driven physical cleanup.

Deliverables:

- [x] Mark traversal from root enumerator.
- [x] `last_mark_epoch` tracking.
- [x] Sweep candidate selection.
- [x] Delete effects for unreachable metadata nodes.
- [x] Segment release evidence for storage-node custodians.
- [x] Metadata custodian that publishes safe reachability epochs.
- [x] Storage-node custodian that frees expired reservations, failed writes,
  orphan durable segments, released segments, and missed async frees.
- [x] GC roots include retained PITR checkpoints, timeline roots, and a
  materialized checkpoint anchor at the PITR commit-age window floor.
- [x] GC simulator hooks for interleaving writes, forks, deletes, PITR changes,
  write-intent expiry, orphan cleanup, missed frees, and sweeps.

Exit gate:

- [x] GC never deletes objects reachable from live or retained PITR roots.
- [x] GC may release overwritten segment data after it falls outside the PITR
  commit-age window and is not part of the replay anchor.
- [x] Unreachable objects are eventually selected for deletion.
- [x] Orphan durable segments are eventually freed after their write intent can
  no longer commit.
- [x] `DurablePendingMetadata` segments are not freed while their write intent
  may still publish.
- [x] Missed asynchronous frees are corrected by periodic storage-node
  reconciliation.
- [x] Mark and sweep can be paused and resumed deterministically.
- [x] Generated tests inject GC at adversarial points in operation traces.
- [x] Criterion covers GC traversal.

## Phase 12: Deterministic End-to-End Simulator

Status: complete.

Prove the storage model under generated operation traces.

Deliverables:

- [x] Simple reference model for logical device contents, native file contents,
  append leases, writer epochs, and history.
- [x] Operation generator for create, write, read, fork, delete, restore, and GC.
- [x] Fault injector for publish conflicts, duplicate effects, delayed effects,
  missing objects, write-intent expiry, orphan segments, missed async frees, and
  crash/replay boundaries.
- [x] Reproducible failure output with seed, minimized trace, and object graph
  summary.

Exit gate:

- [x] Normal CI runs a meaningful seed count for the simulator.
- [x] Every generated trace checks core invariants after each delivered command.
- [x] Failing seeds can be replayed exactly.
- [x] The simulator covers fork divergence, shard contention, PITR replay,
  commit-group atomicity, data-before-metadata ordering, orphan cleanup,
  native append fencing, custodian reconciliation, and GC safety.

## Phase 13: Performance Baselines

Status: complete.

Broaden regression detection after the simple implementation exists.

Deliverables:

- [x] Benchmarks for fork cost versus device size.
- [x] Benchmarks for single-shard write cost versus tree depth.
- [x] Benchmarks for multi-shard atomic write cost.
- [x] Benchmarks for native write, native append with valid leases, and
  stale-lease rejection.
- [x] Benchmarks for read lookup cost and read amplification.
- [x] Benchmarks for checkpoint restore and GC traversal.

Exit gate:

- [x] Benchmarks establish baseline numbers with reproducible inputs.
- [x] Fork remains O(1) in measured object count.
- [x] Write cost scales with changed shard paths, not whole-device metadata.
- [x] Any proposed optimization links to a benchmark or failing test.

## Local V1 Boundary Audit

The completed local phases prove the state transitions in one process. The
following local shortcuts are intentional, but each must become a durable,
remote, replayable, or concurrent boundary in the owning future phase:

- Phase 14 owns native keyspace PITR and snapshot semantics over the shared
  segment substrate, including keyspace catalog-root records, file-root audit
  records, and restore/snapshot API shape.
- Phase 15 owns native keyspace performance characterization and any benchmark-
  proven local catalog scaling work needed before durable formats are chosen.
- Phase 16 owned the first local durable snapshot provider: segment sync,
  atomic metadata/storage-node snapshots, commit-group persistence,
  write-intent recovery, native append lease/session records,
  checkpoint/timeline persistence, and cache coherence after restart. Its
  `bincode` snapshot scaffolding was replaced by crate-owned durable codecs in
  Phase 18. Phase 20 removed the snapshot production hot path and the old
  file-per-segment runtime backend instead of carrying them as compatibility
  layers.
- Phase 17 owns remote transport serialization, retry/deduplication, stale
  response rejection, server incarnation fencing, deadlines, mailbox semantics,
  backpressure, and concurrency rules for non-conflicting requests. Its current
  `bincode` wire envelope is scaffolding, not the real network format.
- Phase 18 owns the durable provider crash/fault-injection matrix and replaced
  durable `bincode` snapshots with a crate-owned binary codec. Its snapshot
  production path became the correctness baseline that Phase 20 superseded.
- Phase 19 owns a real network implementation of the Phase 17 wire contract,
  including a crate-owned wire codec rather than serde/bincode-derived frames.
- Phase 20 owns replacing the snapshot-only performance path with a measured
  durable journal provider before storage replication builds on the wrong
  persistence shape. Its journal/data-log direction replaces the old
  per-segment-file runtime path in full.
- Phase 21 owns a SQLite metadata store plus partitioned durable data logs and
  incremental compaction so compaction does not rewrite the entire live node.
- Phase 22 owns multiple local storage-node endpoints and one-replica segment
  placement so file/block data can be partitioned by segment without changing
  public APIs.
- Phase 23 owns row-native SQLite metadata publishing so durable writes,
  forks, restores, PITR, GC, and reopen update/query operational rows instead
  of replacing a whole logical state blob.
- Phase 24 owns native large append segment reservations so append-heavy users
  can coalesce many chunk fills into one exact logical segment commit without
  exposing storage-node placement.
- Phase 25 owns deterministic background compaction scheduling, maintenance
  budgets, and explicit write backpressure policy for the partitioned durable
  layout.
- Phase 26 owns replica-set selection, SQLite-backed reference/release outbox
  and cursor tables, SQLite-backed repair jobs, orphan replica reconciliation,
  stale placement handling, and physical free reconciliation across replicated
  storage nodes.

Do not treat an in-process handoff in the local provider as evidence that the
distributed boundary is done. A later phase is complete only when the handoff is
durable or replayable, idempotent under retries, and covered by deterministic
delay, duplication, reorder, failure, and restart tests.

## Phase 14: Native Keyspace PITR and Snapshots

Status: complete.

Add point-in-time history for native keyspaces without routing native operations
through block-device mappings. This phase proves that keyspace catalog-root
timelines, file-root audit records, append-lease fencing, and GC retention work
for the native API before durable or remote providers have to persist those
records.

The snapshot/restore boundary is the native keyspace, not an individual file.
Per-file snapshots are intentionally not part of this phase because they do not
produce coherent filesystem-level restore points.

Deliverables:

- [x] Public native keyspace restore/snapshot API shape documented in the spec.
- [x] Native file read/write/append semantics are byte-oriented while local
  segment storage remains block-aligned internally.
- [x] Append-only `KeyspaceCommit` records with old/new keyspace catalog roots,
  commit sequence, commit group, and logical time.
- [x] Append-only `FileCommit` records with old/new file roots, old/new file
  versions, size, commit sequence, commit group, and logical time.
- [x] Native keyspace checkpoint records that can reconstruct a `KeyspaceHead`.
- [x] Immutable keyspace catalog entries include file creation metadata, so
  snapshots and restores preserve namespace metadata by root-pointer copy.
- [x] Restore algorithm from checkpoint plus keyspace commits.
- [x] Native snapshot or restore operation that creates a new keyspace lineage
  without mutating the source keyspace.
- [x] PITR retention and replay-anchor materialization for native keyspace
  roots.
- [x] GC roots include retained native PITR checkpoints and keyspace-root
  timeline records.
- [x] Generated traces compare native keyspace restores against a simple
  historical keyspace/file model.

Exit gate:

- [x] Native keyspace restore to selected commits, checkpoints, and times returns
  expected file bytes, size, and file version for every restored file.
- [x] Snapshot and restore reuse the retained `KeyspaceRoot` pointer and do not
  allocate file metadata-tree nodes.
- [x] Native checkpoint validation rejects mismatched keyspace roots.
- [x] Unaligned native writes, appends, and reads across a block boundary
  preserve exact file bytes and size.
- [x] Stale append leases cannot publish across a restore or snapshot lineage
  boundary.
- [x] Native PITR GC never deletes metadata or segments needed by retained
  native keyspace restore points.
- [x] Expired native restore points fail cleanly after GC sweeps the needed
  roots.
- [x] Block PITR behavior remains unchanged; native PITR shares the lower
  substrate but not block-device logical mappings.
- [x] Criterion covers native keyspace checkpoint restore.

## Phase 15: Native Keyspace Performance and Scaling

Status: complete.

Characterize the local native keyspace implementation before durable providers
freeze the wrong shape into a storage format. The goal is not to optimize by
instinct. The goal is to prove which costs are acceptable for a correctness
model, which costs are only local-provider artifacts, and whether the public
API and deterministic core leave room for high-performance implementations.

Before this phase, each immutable `KeyspaceRoot` contained one deterministic
map of file catalog entries. That was a good correctness model, but the Phase
15 benchmarks showed whole-catalog publish cost would freeze the wrong shape
into durable providers. The local catalog now uses sharded immutable keyspace
catalog roots: file create/write/append copies one catalog shard plus one root,
while snapshot and restore continue to copy only root IDs.

Deliverables:

- [x] Criterion benchmarks for native file create, info, write, append, read,
  checkpoint, snapshot, restore, and stale-lease rejection at keyspace sizes
  `1`, `1k`, and `100k`; `100k` is the current normal-run local stress size.
- [x] Benchmarks for concurrent native writes/appends across independent files
  and, separately, conflicting write/append attempts against one file.
- [x] Benchmarks for aligned write/append/read versus unaligned
  write/append/read, including the partial-tail-block COW path.
- [x] Benchmarks that assert keyspace snapshot and restore stay O(1) in file
  metadata-tree nodes and do not walk file contents.
- [x] Regression thresholds or documented baseline ranges for native hot paths
  in the Criterion suite.
- [x] A written decision record in the design spec: keep the local catalog as a
  correctness model, or implement a sharded keyspace catalog before durable
  providers.
- [x] If benchmarks show `O(file_count)` publish cost is material, replace the
  local `BTreeMap` catalog root body with sharded immutable catalog roots or an
  equally simple measured alternative.
- [x] If catalog sharding is added, deterministic generated tests compare
  native keyspace behavior against the existing simple historical model.
- [x] If catalog sharding is added, benchmark and test that independent file
  publishes contend at catalog-shard granularity rather than whole-keyspace
  granularity.
- [x] Documentation of the intended high-performance implementation shape:
  cached hot file heads, sharded catalog-root publishes, append-only timeline
  records, and provider-private indexes that do not leak into public APIs.

Exit gate:

- [x] Native keyspace benchmarks report headline numbers for normal operations,
  large keyspaces, concurrent independent-file operations, conflicting-file
  operations, snapshot, restore, and fork-like root-pointer copy behavior.
- [x] The measured local implementation has no hidden whole-keyspace work on
  snapshot or restore.
- [x] Any remaining whole-keyspace work on append/create is explicitly
  classified as a local-provider limitation or eliminated before Phase 16.
- [x] The public `NativeKeyspaceClient`, `NativeFile`, `MetadataPlane`, and
  transport interfaces do not require callers to coordinate catalog shards,
  metadata placement, storage placement, or replica durability.
- [x] A future durable or remote provider can implement the measured scalable
  shape without changing public APIs.
- [x] Performance optimizations are backed by benchmarks and deterministic
  conformance tests, not by speculative abstractions.

## Phase 16: Durable Provider

Status: complete.

Add a durable provider only after the local in-memory model, conformance suite,
and native keyspace scaling characterization are boringly correct.

Deliverables:

- [x] Provider choice documented in the spec.
- [x] Durable segment, local segment catalog, metadata plane, device catalog, and
  timeline implementations.
- [x] Initial storage-node file I/O proof below `SegmentStore` and
  `LocalSegmentCatalog`. Phase 20 replaced this production path with a journal
  writer and removed the old file-per-segment backend from runtime code.
- [x] Crash-consistent `sync_segment`, `Acknowledged`, `Flushed`, and `flush`
  definitions, including exact `durable_through` semantics.
- [x] Durable metadata state images for commit groups, checkpoints, delete
  records, fork records, native keyspace commits, and native file-root audit
  commits.
  The first provider used atomic `bincode` snapshots as temporary scaffolding;
  Phase 18 replaced that format and Phase 20 removed snapshots from the
  production hot path.
- [x] Durable write-intent table with logical expiration, cancellation/failure
  evidence, and restart recovery scan.
- [x] Durable native append lease/session records with restart-safe writer
  epochs and stale-writer rejection after recovery.
- [x] Cache coherence rules for hot heads, metadata nodes, checkpoints, and
  segment descriptors after restart.
- [x] Crash/restart tests for committed block contents, native keyspace state,
  writer epochs, PITR restore points, and storage-node custodian deletions.
- [x] Explicit portable segment file I/O sequencing test for the original
  snapshot provider. Phase 20 deleted that backend after the journal rewrite
  passed its crash/replay tests.
- [x] PITR and GC tests against the durable provider.
- [x] Durable Criterion baselines for acknowledged writes, flushed writes,
  batched flushes, reopen reads, and reopen after committed history.

Exit gate:

- [x] Durable provider passes the currently implemented restart and lifecycle
  conformance tests for block and native APIs.
- [x] Crash/restart tests preserve committed device contents.
- [x] Partial writes do not expose uncommitted roots.
- [x] Atomic snapshot publishing meant a completed metadata state image reopened
  as one committed state; Phase 18 covered the crash matrix and Phase 20
  replaced this with journal commit replay.
- [x] Pending segment writes left by crashed, expired, or fenced write intents
  become reclaimable without exposing data.
- [x] The original portable segment file I/O backend preserved the documented
  durability sequence while it was the production provider. Phase 20 replaced
  the runtime durability boundary with ordered journal append plus sync.
- [x] `Acknowledged` writes are read-visible in the live process but need a
  later `flush` or `Flushed` write for restart visibility.
- [x] Flush reports only commit sequences whose segment bytes and metadata
  records satisfy the provider's documented durability contract.
- [x] Cached reads after restart or stale cache invalidation cannot observe roots
  older than the accepted fence/version.
- [x] No provider-specific behavior leaks into core metadata logic.

## Phase 17: Remote Transport

Status: complete.

Replace the local transports with remote-capable implementations without
changing the public block or native APIs.

Deliverables:

- [x] Remote transport choice documented in the spec.
- [x] Serialization format for request and response envelopes.
- [x] Retry, deadline, duplicate-request, duplicate-response, and stale-response
  tests.
- [x] Deterministic chaos wire transport for request drops, response drops,
  duplicate deliveries, delayed responses, and reordered response bytes.
- [x] Bounded request deduplication keyed by request ID, client epoch, and server
  incarnation.
- [x] Server actor mailbox, backpressure, and shutdown semantics.
- [x] Concurrency model that serializes or fences conflicting operations while
  allowing non-conflicting shard/file operations to proceed independently.
- [x] Local and remote transport conformance tests.

Exit gate:

- [x] `BlockDevice` and `NativeFile` callers do not change when transport
  changes.
- [x] Request identity and client epoch fence duplicate or stale responses.
- [x] Server incarnation changes prevent old retry streams from being applied to
  a restarted server instance.
- [x] Backpressure is explicit and testable; unbounded queues are not hidden in
  the transport.
- [x] Non-conflicting operations are not forced through a whole-server global
  lock by the interface.
- [x] Deterministic transport simulation covers delay, duplication, drop, and
  reorder faults for both block and native APIs.

## Phase 18: Durable Fault-Injection Matrix

Status: complete.

Harden the snapshot-based durable provider by testing every durable boundary as
an explicit crash/restart point. This phase either proves the simple atomic
snapshot provider is enough for the toy system's durability contract, or
produces the evidence needed to replace it with a journal/database-backed
metadata provider. Do not silently grow a second durable format; if a journal
or database provider is chosen, update the spec and remove the superseded
snapshot-only path in the same phase. `bincode` is not an acceptable durable
format after this phase; replace it with a crate-owned binary codec before the
crash matrix is treated as production-grade evidence.

Deliverables:

- [x] Reusable durable provider conformance harness that can run against the
  in-memory model, the snapshot durable provider, and any future journal or
  database-backed provider where applicable.
- [x] Crate-owned durable state-image codec with explicit magic, schema version,
  record kind, enum tags, fixed integer endianness, bounded collection/string
  lengths, trailing-byte rejection, and deterministic map ordering.
- [x] Durable codec tests for round trips, stable golden bytes, unsupported
  versions, invalid tags, truncated payloads, trailing bytes, oversized lengths,
  and length-prefix overflow.
- [x] Fault-injected segment file I/O backend for the original snapshot
  provider. Phase 20 removed the file-per-segment runtime backend rather than
  carrying it forward as compatibility scaffolding.
- [x] Fault-injected state-image writer for codec/atomic-write coverage of the
  old snapshot proof shape; it remains test-only and is not a durable provider.
- [x] Crash/reopen matrix for block writes, multi-shard commit groups, forks,
  deletes, PITR checkpoints/restores, native writes, native appends, native
  keyspace checkpoints, and native keyspace snapshots/restores.
- [x] Decision record for keeping atomic binary snapshots or replacing them
  with a journal/database-backed metadata provider.
- [x] The Phase 18 decision kept atomic binary snapshots for this toy durable
  provider until Phase 20 benchmark evidence justified replacing the production
  path with a journal.
- [x] Remove `bincode` from durable snapshot persistence. Keeping serde/bincode
  only for test fixtures or debug helpers is allowed if it is not a production
  durable or wire format.

Exit gate:

- [x] Every injected crash point reopens as either the old committed state or
  the complete new committed state; no partial commit group, partial keyspace
  commit, or metadata reference to missing segment bytes is observable.
- [x] Replaying after repeated crashes is idempotent and does not leak write
  intents, append leases, temporary segment files, or durable-pending catalog
  entries.
- [x] `flush` reports only commit sequences whose segment bytes, storage-node
  catalog state, segment descriptors, and metadata state survive reopen.
- [x] Storage-node custodian and metadata custodian can resume after crashes
  without freeing live or retained-PITR data.
- [x] The chosen durable metadata format has no untested compatibility shim left
  behind.
- [x] Durable reopen never depends on serde-derived struct layout or bincode
  defaults; every persisted byte is accepted or rejected by crate-owned codec
  rules.

## Phase 19: Real Network Transport

Status: complete.

Implement an actual network adapter for the Phase 17 serialized wire contract.
This phase is about crossing a process or host boundary, not changing storage
semantics and not adding replication. The Phase 17 `bincode` envelope is
temporary local scaffolding; real network frames must use a crate-owned codec.

Deliverables:

- [x] Protocol choice documented in the spec, including framing, maximum frame
  size, request/response envelope codec, and server incarnation handshake.
- [x] Crate-owned wire codec with explicit magic, protocol version, frame kind,
  request/response kind tags, fixed integer endianness, bounded payload lengths,
  and trailing-byte rejection.
- [x] Wire codec tests for round trips, stable golden frames, unsupported
  versions, invalid tags, truncated frames, oversized frames, trailing bytes,
  corrupt length prefixes, and mismatched request/response IDs.
- [x] Network block transport that implements `BlockTransport` without changing
  `BlockDevice` callers.
- [x] Network native transport that implements `NativeTransport` without
  changing `NativeFile` callers.
- [x] Network server endpoint for block and native request envelopes over the
  shared `RemoteWireTransport` contract.
- [x] Bounded connection queues, explicit backpressure, timeout/deadline
  behavior, reconnect behavior, and shutdown behavior.
- [x] Loopback integration tests plus deterministic chaos tests that reuse the
  Phase 17 drop, duplicate, delay, reorder, stale-response, and corrupt-frame
  cases.

Exit gate:

- [x] In-process, serialized remote, chaos-wrapped, and real network transports
  pass the same block and native transport conformance tests.
- [x] Network failures surface as transport errors; callers can retry with the
  same request identity without double-applying successful server mutations.
- [x] Stale server incarnations, mismatched response IDs, oversized frames, and
  malformed frames are rejected deterministically.
- [x] No production network path uses serde/bincode-derived framing or enum
  layout.
- [x] Backpressure is bounded and observable; the network adapter does not hide
  unbounded queues or background retries.
- [x] Public block/native APIs and provider contracts do not change.
- [x] The network adapter does not choose storage nodes or fan out replicas.

## Phase 20: Durable Journal and Segment Log Provider

Status: complete.

The snapshot durable provider was a correctness baseline, not the intended
high-performance durable layout. Phase 16/18 benchmarks showed that fully
flushed 4 KiB writes are dominated by per-operation segment-file and snapshot
syncs, and that batching acknowledged writes still pays one temp-file sync per
segment. Phase 20 replaces that performance path with an append-oriented durable
provider that preserves the same public contracts before adding replicated
storage.

Deliverables:

- [x] Append-only metadata journal or database-backed metadata provider for
  device heads, keyspace heads, commit groups, PITR records, checkpoints,
  write-intent state, append leases, and GC/custodian evidence.
- [x] Explicit compact checkpoint path so replay time can be bounded without
  rewriting the whole metadata plane on every write; a periodic scheduler can
  call this maintenance hook later without changing the durable format.
- [x] Single append journal that persists batches of immutable segment data and
  metadata commit records with fewer sync boundaries than one file per segment.
  This is the correctness/performance baseline; partitioned logs and
  incremental compaction are Phase 21.
- [x] Group-commit path for acknowledged writes where `flush` can persist many
  committed mappings with one ordered durability sequence.
- [x] Crash/reopen and fault-injection matrix for journal append, checkpoint
  publish, segment-log append, batch sync, replay truncation, and checkpoint
  compaction.
- [x] Migration-free replacement of the snapshot performance path under the
  no-tombstones rule; no file-per-segment durable backend remains in runtime
  code. The remaining state-image codec fixture is test-only.
- [x] Criterion baselines for acknowledged latency, single flushed latency,
  batched flush throughput, reopen replay time, checkpoint compaction, native
  append, and block/native reads after reopen.

Exit gate:

- [x] The journal/segment-log provider passes the same provider conformance,
  PITR, GC, custodian, restart, and malformed-input tests as the snapshot
  provider.
- [x] A flushed write still persists segment bytes before metadata can reference
  them, and `flush` reports only replay-survivable commit sequences.
- [x] Acknowledged writes remain read-visible in-process and become
  restart-visible only after `flush`, `Flushed`, or another documented
  synchronous metadata operation.
- [x] Replayed state is byte-for-byte equivalent to the deterministic in-memory
  model for block and native generated traces.
- [x] Benchmarks demonstrate that the new durable path materially improves
  fully flushed writes and batched flushes on the same host.
- [x] The implementation plan records any remaining host-specific ceiling, such
  as macOS sync latency, without hiding provider-level overhead.

Historical limitation: explicit Phase 20 compaction rewrote the current live
segment bytes plus one commit record into a replacement `store.log`. That kept
the implementation simple and proved replay/compaction correctness, but it was
not the scalable storage-node compaction strategy. Phase 21 removed that
production path and replaced it with partitioned logs and per-log incremental
compaction.

Current short-run host smoke numbers with the SQLite plus partitioned data-log
provider on macOS/APFS are approximately: block acknowledged 4 KiB write 15 us,
block flushed 4 KiB write 6.5 ms, block flush after 32 acknowledged writes
7.4 ms, native acknowledged append 19 us, native flushed append 5.5 ms, native
flush after 32 acknowledged writes 7.7 ms, reopen after 32 block writes 2.6 ms,
and an explicit no-op compaction pass after 32 block writes 83 us. The remaining
floor is dominated by host sync latency and full-state commit serialization; a
batched flush now pays one data-log sync per touched log plus one SQLite publish
transaction instead of one payload sync per segment.

Phase 23 later removed the full-state SQLite blob from the production durable
provider. The Phase 21 numbers remain useful as historical partitioned-log
baselines, but current durable metadata publish uses row-native SQLite tables.

## Phase 21: Partitioned Durable Logs and Incremental Compaction

Status: complete.

Replace the Phase 20 single durable journal with a SQLite metadata store and
rolled data logs. Use SQLite for transactional, indexed metadata instead of
inventing a custom metadata database. Keep large immutable segment payloads in
plain rolled data files so storage-node compaction can reclaim space
incrementally. The goal is to make compaction proportional to selected dirty
data-log files, not to total live bytes on the node.

Non-goals:

- No storage replication.
- No multi-node placement changes.
- No public block/native API changes.
- No background compaction thread hidden inside deterministic code.
- No adaptive data layout unless a benchmark proves the simple rolled-log
  layout is the bottleneck.
- No custom metadata log unless SQLite cannot satisfy a documented correctness
  or performance requirement under deterministic fault testing.

Target durable layout:

```text
store/
  metadata.sqlite
  metadata.sqlite-wal
  metadata.sqlite-shm
  data/
    data-000001.log
    data-000002.log
    data-000003.log
  tmp/
```

Logical segment placement becomes:

```text
segment_id -> data_log_id, offset, length, crc64_ecma, storage_node_id
```

Deliverables:

- [x] SQLite metadata store for device heads, native keyspace/file heads,
  commit groups, PITR/checkpoints, write-intent state, append leases, segment
  lifecycle state, placement index, data-log manifests, relocation state, and
  custodian evidence. Phase 21 initially used a whole-state SQLite blob for the
  logical metadata image while indexing physical placement and data-log
  manifests in separate tables. Phase 23 replaced that blob with row-native
  operational metadata tables under the no-tombstones rule.
- [x] SQLite schema with explicit tables, indexes, uniqueness constraints, and
  foreign-key or equivalent integrity checks for `segment_id`, `data_log_id`,
  placement state, owner/reachability state, and data-log accounting.
- [x] SQLite transaction boundaries documenting exactly which rows become
  durable/visible together for create, write, append, flush, checkpoint,
  restore, delete, GC, custodian release, and compaction relocation.
- [x] SQLite durability settings documented and tested. Use conservative
  defaults first, such as WAL mode plus `synchronous=FULL` or equivalent,
  before optimizing.
- [x] Rolled data-log writer that appends immutable segment payload records and
  rolls files by configured byte size, record count, or explicit test trigger.
  The data-log writer uses CRC64-ECMA for record payload checksums and batches
  acknowledged flushes so each touched data log is fsynced once before the
  SQLite publish transaction.
- [x] Durable placement index recording each committed logical segment's current
  data-log location without storing physical placement in metadata leaves or
  native extents.
- [x] Data-log manifest tables that track active, sealed, and deleted data-log
  files, including live-byte estimates and durable deletion state. Separate
  `compacting`/`relocated` states are intentionally unnecessary in v1 because
  relocation is published by one SQLite placement transaction after the new data
  log has been fsynced.
- [x] Data-log live-byte accounting driven by metadata reachability, PITR
  retention, custodian release evidence, and placement relocation state.
- [x] Incremental compaction planner that selects sealed data logs by
  reclaimable ratio and size thresholds.
- [x] Compaction path that deletes fully dead data-log files without copying
  payload bytes.
- [x] Relocation path that copies only live payload records from selected dirty
  data logs into new data logs, fsyncs the new data log, commits SQLite
  placement updates in one transaction, and deletes old logs only after the
  relocation transaction is durable.
- [x] SQLite maintenance path for checkpoints, WAL size control, integrity
  checks, and optional `VACUUM`/incremental vacuum. This must not rewrite data
  payload logs. The current maintenance hook is explicit and manual; no hidden
  background compactor is introduced.
- [x] Crash/reopen tests for torn data records, torn metadata records, torn
  SQLite transactions, partially copied compaction logs, relocation transaction
  before/after data-log fsync, old-log deletion before/after durable metadata
  commit, WAL checkpoint boundaries, and repeated compaction replay.
- [x] SQLite conformance tests that inject or simulate transaction failure,
  database reopen, WAL checkpoint, corrupt/truncated data-log records, missing
  data-log files, duplicate placements, and stale relocation rows.
- [x] Deterministic tests proving PITR-retained data is not compacted away until
  retention has expired, even when the current head no longer references it.
- [x] Custodian tests proving orphan segment payloads from failed writes can be
  reclaimed from the owning data log without scanning unrelated logs.
- [x] Space-efficiency tests that create overwritten/deleted data, run
  incremental compaction, and assert physical bytes drop without rewriting
  unrelated live data logs.
- [x] Benchmarks for append throughput, single flushed write latency, batched
  flush, reopen with large SQLite metadata, placement lookup, full-dead-log
  deletion, partial-log relocation, SQLite checkpoint/WAL maintenance, and
  compaction pause time.
- [x] Documentation of compaction policy knobs: target data-log size, minimum
  reclaimable ratio, maximum SQLite WAL bytes, maximum sealed data logs,
  maximum dirty bytes, and whether compaction is manual or driven by an explicit
  maintenance call.

Exit gate:

- [x] The durable write ordering is explicit: segment payload reaches the data
  log and is fsynced before the SQLite transaction publishes metadata that can
  reference it.
- [x] Flushed writes and group commit use the minimum syncs required by the
  documented SQLite/data-log durability policy; extra syncs require a benchmark
  or correctness justification.
- [x] Compaction never rewrites the entire node solely to reclaim space from one
  dirty data log.
- [x] Fully dead data logs can be deleted in O(number of selected log files)
  without copying live segment payloads.
- [x] Partially dead data logs relocate only live payload records from selected
  logs and leave unrelated data logs untouched.
- [x] A crash at any compaction point reopens to either the old placement or the
  new placement; no segment becomes missing, duplicated with conflicting bytes,
  or silently zero-filled.
- [x] Metadata leaves and native extents continue to reference logical
  `SegmentId`s, not data-log offsets.
- [x] PITR, fork, snapshot, restore, GC, native append leases, and custodian
  semantics remain byte-for-byte equivalent to Phase 20 under generated traces.
- [x] Reopen time is bounded by SQLite recovery plus the current SQLite
  placement set, not by historical metadata records. Active data-log tails that
  have no placement are ignored until a later custodian/compaction pass.
- [x] Benchmarks show compaction cost scales with selected dirty log bytes and
  selected live relocation bytes, not total live bytes on the storage node.
- [x] The old single-log production path is removed under the no-tombstones
  rule; a tiny single-log fixture may remain only in tests if useful.
- [x] The implementation plan records whether SQLite metadata is kept, tuned, or
  replaced only after benchmark or fault-testing evidence, not taste.

## Phase 22: Multiple Local Storage Nodes and Placement

Status: complete.

Split the remaining "one local storage endpoint" shortcut before introducing
replication. This phase comes after partitioned logs so adding nodes does not
multiply a known whole-node compaction problem. It is still local,
single-process, and one replica per logical segment. Its purpose is to prove
that placement is per segment, not per file, per device, or per public client,
and that all block/native behavior, PITR, GC, custodian, durability, and replay
semantics survive when a single file or device has segments spread across
multiple storage nodes.

Non-goals:

- No remote storage nodes.
- No quorum writes.
- No background repair.
- No public API change for `BlockDevice`, `NativeFile`, or clients.
- No replica-set policy beyond exactly one committed placement per segment.

Deliverables:

- [x] `StorageNodeRegistry` or equivalent internal provider boundary mapping
  `StorageNodeId` to one `SegmentStore` and one `LocalSegmentCatalog`.
- [x] Per-segment `PlacementPolicy` that chooses one storage node for each new
  logical segment using deterministic inputs. Start with simple round-robin,
  hash, or capacity-weighted placement; do not add adaptive balancing until a
  benchmark or simulation needs it.
- [x] Segment placement index that resolves `SegmentId` to its committed local
  `SegmentReplicaPlacement`. Metadata leaves and native extents must continue
  to reference only logical `SegmentId`s.
- [x] Block and native write paths route reservation, byte write, sync,
  local-catalog commit, metadata publish, and referenced transition through the
  selected storage node.
- [x] Block and native read paths resolve each segment through the placement
  index, then read from that segment's storage node. A single read may span
  segments on different nodes.
- [x] Durable provider replay persists and restores storage-node registry
  state, placement index state, per-node local catalogs, and per-node segment
  bytes. A referenced segment with no committed placement or missing bytes is
  rejected at reopen.
- [x] Durable compaction preserves only live committed placements and the
  segment bytes reachable from those placements.
- [x] Metadata custodian output is routed to the owning storage-node catalog for
  each released segment. Storage-node custodians must not crawl metadata trees
  or infer deletion from current heads.
- [x] Local storage-node custodian runs per node and reclaims expired
  reservations, failed writes, orphan durable-pending segments, released
  segments, and missed frees on the correct node.
- [x] Deterministic simulation for mixed block/native traces where writes,
  overwrites, appends, forks, snapshots/restores, deletes, PITR expiry, and GC
  spread segments across multiple local storage nodes.
- [x] Fault tests for stale placement records, duplicate placements, missing
  placement, wrong-node reads, unavailable selected node before write, failure
  after segment sync but before metadata publish, delayed/duplicated release
  routing, and restart during placement publication.
- [x] Provider conformance suite runs against one-node and multi-node local
  providers with identical public behavior.
- [x] Benchmarks for placement overhead, multi-node read fanout, concurrent
  writes to different nodes, custodian sweep cost by node count, reopen replay
  with multiple node catalogs, and fork/restore staying O(1).
- [x] Design docs explain that a native file may have extents on many storage
  nodes and that colocating a file is a placement-policy choice, not a
  metadata/API requirement.

Exit gate:

- [x] `BlockDevice`, `NativeFile`, `MetadataPlane`, and public transport APIs do
  not expose storage-node choice.
- [x] Logical metadata still references `SegmentId`, never physical file paths,
  node-local offsets, or placement records.
- [x] A single native file and a single block device can have committed segments
  on multiple storage nodes, and reads reconstruct the correct bytes.
- [x] Fork, snapshot, restore, PITR, and GC behavior are byte-for-byte identical
  to the one-node provider under generated traces.
- [x] A write is acknowledged only after the selected node's segment bytes meet
  the requested durability and metadata publish succeeds.
- [x] Failed metadata publish after a durable segment write leaves a reclaimable
  orphan on exactly the selected node.
- [x] Missing or conflicting placement is detected as corruption/unavailability;
  it is never silently treated as zero-filled data.
- [x] Released segments are freed only by storage-node-local custodian evidence
  routed from metadata reachability, expired intents, or local failed-write
  evidence.
- [x] Durable replay after crash can rebuild placement and per-node catalog
  state without scanning metadata leaves for physical placement.
- [x] Multi-node placement does not measurably regress one-node hot paths beyond
  an implementation-plan-recorded threshold. Phase 22 adds Criterion coverage
  for three-node write placement and read fanout; before storage replication,
  investigate any >20% one-node hot-path regression on the same host and
  benchmark profile.

Short-run Phase 22 Criterion smoke numbers on this host: three 4 KiB
round-robin block writes across three in-memory nodes measured about 18.6 us
for the three-write batch, and a 12 KiB read fanning out across three nodes
measured about 2.9 us. Treat these as regression smoke baselines, not durable
storage headline numbers.

## Phase 23: Row-Native SQLite Metadata Publishing

Status: complete.

Replace the production `current_state` blob with row-native SQLite metadata for
the durable provider. SQLite remains fully opaque behind the provider boundary;
public block, native file, metadata, transport, and storage-node APIs do not
change. The goal is performance and scale: durable publish updates the rows
affected by new commits, roots, immutable metadata objects, segment lifecycle
changes, checkpoints, and timelines instead of serializing and replacing the
whole logical state.

Non-goals:

- No public API changes.
- No compatibility reader for old blob-authoritative stores.
- No SQL concepts exposed to `BlockDevice`, `NativeFile`, clients, or
  transports.
- No decomposition of metadata tree leaf ranges into SQL columns unless a later
  benchmark proves that leaf-range predicates are hot.
- No permanent dual representation. The old blob is not a production write path
  or fallback.

Operational SQLite metadata groups:

- Singleton store counters/config: next IDs, next commit sequence, next GC
  epoch, write-intent/extent counters, and storage registry cursors.
- Block state: device specs, live heads, deleted heads, shard commits,
  fork/delete records, and checkpoints.
- Native state: keyspace heads, immutable keyspace roots, immutable catalog
  shards, file writer epochs, keyspace commits, and file commit audit rows.
- Shared state: immutable metadata node payloads, commit groups, checkpoint
  payloads, and GC mark epochs.
- Storage-node state: node ordering, per-node catalog cursors, segment
  descriptor rows, segment catalog lifecycle rows, committed placements, and
  data-log manifests.

Deliverables:

- [x] Row-native SQLite schema with identity/order columns for hot predicates
  and crate-owned payload blobs for complex immutable objects where full
  normalization would add cost without a query benefit.
- [x] `DurableExportCursor` loaded from SQLite on open and advanced only after a
  successful metadata publish transaction.
- [x] Durable publish sequence: append/fsync new data-log records first, upsert
  new placement/data-log rows, upsert row-native logical metadata, then advance
  the durable cursor last in the same SQLite transaction.
- [x] Flushed writes export only rows at or beyond the previous durable
  high-water cursor for immutable ID/commit tables, while mutable head,
  lifecycle, mark, and deletion-sensitive tables remain reconciled explicitly.
- [x] Reopen rebuilds `LocalObjectStore` from row-native tables plus data-log
  placement rows, then validates heads, checkpoints, metadata object
  reachability, segment descriptors, placements, catalog lifecycle state,
  readable data-log payloads, ID counters, and monotonic timelines.
- [x] Old DBs that contain legacy `current_state` rows fail with an explicit
  unsupported/corrupt error.
- [x] Provider conformance keeps memory and durable providers byte-for-byte
  equivalent for block writes, native writes/appends, forks,
  snapshots/restores, PITR, deletes, GC, and custodians.
- [x] Crash/corruption tests cover data-log metadata publish boundaries, missing
  row-native head roots, missing segment descriptors, stale cursors, corrupt or
  missing row payloads, timeline rows that reference missing roots, and
  placement rows without required catalog/descriptor bytes.
- [x] Row-native invariant checks ensure every live head and checkpoint root
  exists, every referenced segment has placement/catalog/descriptor/readable
  payload evidence, counters are above persisted IDs, and timelines are
  monotonic.
- [x] Criterion coverage for small-state durable headlines, large-state flush
  after many acknowledged writes, large-state reopen, large-history
  checkpoint/fork/restore, GC/custodian publish cost, and SQLite row/WAL smoke
  metrics.

Exit gate:

- [x] No production write path serializes or replaces a whole logical
  `current_state` blob.
- [x] SQLite remains an implementation detail behind durable provider
  contracts.
- [x] Data-before-metadata ordering is preserved exactly: no metadata row can
  make a segment visible before its data-log record reached the requested
  durability boundary.
- [x] A crash before durable cursor advancement reopens to the previous durable
  cursor and rejects incomplete required rows rather than accepting a partially
  published logical state.
- [x] Reopen rejects missing rows, missing segment bytes, stale placements, bad
  catalog lifecycle, and cursor/counter regressions.
- [x] The no-tombstones rule is upheld: the old blob format is not a
  compatibility layer.

Short-run Phase 23 Criterion smoke numbers on this host with row-native SQLite
metadata and partitioned data logs: block acknowledged 4 KiB write 13 us, block
flushed 4 KiB write 5.9 ms, block flush after 32 acknowledged writes 7.7 ms,
block flush after 256 acknowledged writes 15.7 ms, reopen after 32 block writes
3.4 ms, reopen after 256 block writes 20 ms, checkpoint/fork/restore after 256
block writes 1.4/1.5/1.4 ms, deleted-device custodian publish after 256 writes
8.6 ms, native acknowledged append 13 us, native flushed append 5.9 ms, and
native flush after 32 acknowledged writes about 10 ms with high host variance.
Treat these as regression smoke baselines; the sync-heavy floor remains
host/filesystem dependent.

## Phase 24: Native Large Append Segment Reservations

Status: complete.

Add a native-file-only hard reservation API for append-heavy callers that need
one exact append extent to commit as one logical segment. This is a performance
contract, not topology exposure: callers get an opaque reservation and the
provider still chooses storage-node placement privately.

Non-goals:

- No block API change.
- No client-selected storage node or physical offset.
- No best-effort splitting for `SingleSegmentRequired`.
- No durable/resumable staged fills before final commit.

Deliverables:

- [x] Public native reservation types and request/response variants for reserve,
  chunk fill, commit, and abort.
- [x] `NativeFile` methods for reserving an exact append extent, filling it in
  chunks, committing it with a requested durability, and aborting it.
- [x] V1 validation requiring nonzero block-aligned exact length,
  block-aligned current file size, configured maximum reservation size, matching
  append lease target, and single-segment placement.
- [x] Local provider reservation state keyed by opaque reservation ID and append
  writer fence. The selected storage node and `SegmentId` remain provider
  private.
- [x] Out-of-order chunk fill support with duplicate identical fills accepted
  and overlapping conflicting fills rejected.
- [x] Commit path that writes and syncs exactly one immutable segment, publishes
  one native file-version transition, and marks that segment referenced.
- [x] Abort/stale-lease cleanup for unwritten reservations, with durable segment
  write failures routed through the existing orphan/custodian path.
- [x] Row-native durable publishing fixed to export segment records based on
  actually persisted segment bytes, not just segment ID high-water cursors, so
  segment IDs consumed by uncommitted reservations cannot make later commits
  unreopenable.
- [x] Local append hot path avoids avoidable large-buffer duplication: aligned
  normal appends move one owned segment buffer into the in-memory segment store,
  reserved append commit consumes its staged buffer instead of cloning it, and
  the in-memory descriptor checksum uses wide deterministic mixing. Durable
  data-log CRC64 remains unchanged.
- [x] Criterion coverage comparing one 32 MiB normal append, one 32 MiB
  reserved append filled in 4 KiB chunks, the regular many-small-append
  regression case, an optional long 8192 normal 4 KiB append case, and
  multi-node reserved append placement overhead.

Exit gate:

- [x] A reserved 32 MiB append reads back correctly and creates exactly one
  logical segment.
- [x] Normal append remains the fallback for unaligned writes and is not routed
  through reservation staging.
- [x] Stale writer epochs reject without exposing partial file contents and
  release unwritten reservation state.
- [x] Uncommitted staged fills are not visible and do not survive reopen as
  committed data.
- [x] Reopened durable stores accept committed reserved appends only when the
  segment record, placement, catalog entry, and bytes are complete.
- [x] Multi-node providers can place successive reserved appends for one file on
  different nodes while preserving file byte order.
- [x] The feature is kept only if benchmarks show it improves the intended
  append workload, especially many small appends coalesced into one reserved
  extent. If it does not, optimize or remove it rather than leaving a decorative
  API.

Short-run Phase 24 Criterion smoke numbers on this host after append hot-path
hardening: one normal 32 MiB append measured about 2.5 ms, one reserved 32 MiB
append filled in 4 KiB chunks measured about 2.9 ms, 1024 normal 4 KiB appends
measured about 109 ms, the optional long 8192 normal 4 KiB append case measured
about 37 s before the hot-path hardening, and three 4 KiB reserved appends
across three local nodes measured about 20 us. The regular regression target
uses 1024 normal 4 KiB appends so the benchmark suite stays practical for
routine gates; set `TOY_COW_LONG_BENCH` to include the 8192-append headline run.
The single-large-append path is now in the low single-digit millisecond range,
while the intended many-small-append coalescing case remains meaningfully
faster at the regular regression size and orders of magnitude faster in the
optional 8192-append headline run.

## Phase 25: Background Compaction Scheduling and Backpressure Policy

Status: not started.

Turn Phase 21's explicit manual compaction hook into a deterministic maintenance
actor and policy surface. The core still must not hide background work inside
metadata or storage-node state transitions. Instead, the runtime gets an
explicit scheduler that observes durable-log/accounting metrics, chooses bounded
maintenance work, and reports write admission/backpressure decisions that tests
can replay exactly.

Non-goals:

- No storage replication.
- No remote scheduler service.
- No hidden thread in the deterministic core.
- No sleeps, wall-clock reads, or process-global randomness in admission or
  compaction decisions.
- No automatic deletion that bypasses PITR, GC, custodian release evidence, or
  data-log placement accounting.

Deliverables:

- [ ] `MaintenanceScheduler` or equivalent deterministic policy object with a
  pure transition shape such as `step(observation) -> maintenance_commands,
  admission_decision`.
- [ ] Explicit observation model for per-node active/sealed data logs,
  live/dead/reclaimable bytes, dirty-log count, active-log size, SQLite WAL
  size, pending custodian releases, PITR retention horizon, compaction cursor,
  and recent write/flush pressure.
- [ ] Configured policy knobs for target data-log size, low/high dirty-byte
  watermarks, maximum sealed-log count, maximum reclaimable-debt bytes,
  compaction copy budget per tick, maximum SQLite WAL bytes, maximum concurrent
  compaction jobs, and whether the runtime uses manual, opportunistic, or
  always-on maintenance.
- [ ] Explicit write admission decisions: accept, accept-and-schedule, throttle
  with a documented reason, or reject because durable capacity/invariants would
  be violated. Runtime adapters may translate throttle into waiting; the core
  decision must remain observable and testable.
- [ ] Background runtime loop for the local durable provider that executes
  scheduler commands with bounded work per tick and clean shutdown semantics.
  The loop must be optional and replaceable by manual stepping in tests.
- [ ] Backpressure integration for block and native file writes that does not
  change public read, write, append, fork, snapshot, restore, or flush
  semantics.
- [ ] Deterministic simulation covering writes racing compaction, PITR horizon
  changes, deletes, GC release evidence, active-log rolling, repeated scheduler
  ticks, low/high watermark crossings, and per-node maintenance fairness.
- [ ] Fault tests for compaction job interruption, duplicated scheduler
  commands, delayed custodian release evidence, stale observations, shutdown
  during a tick, and restart with pending compaction debt.
- [ ] Metrics/diagnostics that expose dirty bytes, reclaimable bytes, selected
  logs, skipped logs with reasons, throttle decisions, bytes copied/deleted,
  and scheduler tick outcomes.
- [ ] Benchmarks for steady-state writes with background maintenance disabled,
  enabled-but-idle, and actively compacting; tail latency under high dirty-log
  pressure; and throughput under explicit throttling.

Exit gate:

- [ ] Manual compaction and background-scheduled compaction produce the same
  final reachable bytes under the deterministic conformance suite.
- [ ] Scheduler output is deterministic for a given observation trace.
- [ ] Bounded maintenance work prevents one tick from rewriting or scanning an
  unbounded amount of node data.
- [ ] Backpressure cannot silently drop acknowledged writes or weaken flushed
  durability.
- [ ] PITR-retained segments are never selected for deletion, even under high
  dirty-byte pressure.
- [ ] The runtime can shut down after finishing or aborting a bounded tick and
  reopen to a valid placement set.
- [ ] With background maintenance enabled but no reclaimable debt, one-node hot
  read/write paths do not regress beyond an implementation-plan-recorded
  threshold.
- [ ] The scheduler remains below the block/native public APIs and does not ask
  clients to choose storage nodes, compact logs, or fan out writes.

## Phase 26: Storage Replication

Status: not started.

Add replicated segment storage only after Phase 21 proves incremental
compaction and Phase 22 proves segment placement across multiple local storage
nodes. The local and durable providers must pass the same conformance suite,
remote transport behavior must be deterministic, and the real network transport
must have proven the wire contract across a process boundary.

Deliverables:

- [ ] Placement coordinator that chooses replica sets for logical segments using
  the Phase 22 storage-node registry and placement-policy boundary.
- [ ] Replica count, durability class, capacity, and failure-domain policy
  inputs for replica-set decisions.
- [ ] Replica write path that reserves, writes, syncs, and records one local
  replica commit per selected storage endpoint.
- [ ] Metadata publish waits for the requested replica durability before making
  the logical segment visible.
- [ ] SQLite-backed metadata outbox tables for reference and release evidence
  keyed by safe commit/reachability epoch. Do not introduce custom evidence logs
  or queues unless local SQLite tables fail a documented remote or performance
  requirement.
- [ ] Per-storage-node apply cursor tables and idempotent apply paths for
  referenced and released logical segments.
- [ ] Repair path that can add missing background replicas after metadata
  publish without changing public block or native APIs.
- [ ] SQLite-backed repair job table and per-node repair cursors for idempotent
  copy, source selection, checksum validation, and restart after interrupted
  repair. External work queues are optional later adapters, not the base model.
- [ ] Custodian reconciliation for failed replica writes, orphan replicas, and
  stale local catalog or stale placement state.
- [ ] Fault simulation for replica delay, loss, duplication, stale writes,
  partial quorum success, delayed/duplicated/reordered reference and release
  evidence, missed storage-node notifications, and repair races.

Exit gate:

- [ ] `BlockDevice` and `NativeFile` callers do not coordinate replicas.
- [ ] Metadata leaves still reference logical `SegmentId`s, not physical
  replica placements.
- [ ] A write is acknowledged only after the configured replica durability and
  metadata publish both succeed.
- [ ] Failed metadata publish after durable replica writes leaves only
  reclaimable orphan replicas.
- [ ] Storage nodes do not consider durable-pending replicas referenced until
  metadata-produced reference evidence or reconciliation proves the metadata
  commit succeeded.
- [ ] Storage nodes never infer deletion by reading current metadata heads; they
  free physical bytes only from durable release evidence, expired intents, or
  local failed-write evidence.
- [ ] Reference and release evidence replay is idempotent and can resume from
  SQLite-backed per-node cursors after storage-node or metadata-service restart.
- [ ] Repair never makes uncommitted data visible.
- [ ] Stale placement decisions cannot overwrite, free, or repair the wrong
  logical segment or replica placement.
- [ ] Replicated providers pass the same read/write/fork/PITR/GC conformance
  suite as single-replica providers.

## Phase 27: Linux io_uring Storage Node Backend

Status: not started.

Add a Linux `io_uring` implementation behind the Phase 21 storage-node data-log
I/O boundary after the SQLite metadata plus rolled-log provider proves its
crash/reopen contract. This phase is a measured storage-node optimization, not
a new public API and not a new durability model. The portable blocking data-log
backend remains the default fallback.

Deliverables:

- [ ] Linux-only `io_uring` backend behind an explicit feature flag.
- [ ] The `io_uring` backend plugs into the Phase 21 data-log append/read
  boundary without changing storage-node lifecycle, catalog, placement, or
  metadata code.
- [ ] Shared conformance and crash/restart tests run against both the portable
  backend and the `io_uring` backend when the host supports it.
- [ ] Benchmarks for concurrent segment reads, concurrent data-log appends,
  batched appends, sync-heavy writes, compaction relocation reads/writes, and
  mixed read/write storage-node workloads.
- [ ] Documentation of when the provider may select the portable backend
  automatically, such as non-Linux hosts, unsupported kernels, disabled feature
  flags, or failed backend initialization.

Exit gate:

- [ ] `BlockDevice`, `NativeFile`, `MetadataPlane`, `SegmentStore`, and
  `LocalSegmentCatalog` public contracts do not change.
- [ ] Both data-log I/O backends, when available, pass the same storage-node
  conformance and crash/restart tests.
- [ ] The `io_uring` backend preserves the exact Phase 21 durability contract:
  segment payload bytes reach the selected data log before SQLite metadata can
  publish a placement that references them.
- [ ] Fallback to the portable backend is explicit, observable in diagnostics,
  and does not weaken correctness.
- [ ] The `io_uring` backend is kept only if benchmarks show meaningful
  concurrent storage-node throughput or tail-latency improvement.
- [ ] Backend-specific behavior does not leak into metadata, PITR, GC, block
  API, native file API, or deterministic core logic.

## Phase 28: Optional ublk Adapter

Status: not started.

Expose the proven block-device semantics through a toy `ublk` adapter. This is
intentionally late; if the earlier API is right, this should be mostly an
adapter from kernel block requests to `BlockDevice` operations.

Deliverables:

- [ ] `ublk` adapter design note.
- [ ] Adapter that translates aligned kernel requests into public block API
  calls.
- [ ] Filesystem smoke tests when the platform supports them.

Exit gate:

- [ ] The adapter contains no core storage decisions.
- [ ] Existing simulator and API tests remain the source of correctness truth.
- [ ] `ublk` failures cannot corrupt committed metadata.

## No-Tombstone Discipline

This project is allowed to evolve quickly because it is a toy system, but it
should not accumulate compatibility rubble.

This discipline is about code and format evolution. It does not forbid explicit
delete records, GC mark state, or retained PITR roots when those are part of the
current design.

When changing an internal format or API:

- Update the design spec and implementation plan first or in the same change.
- Replace old internal paths instead of leaving compatibility shims.
- Migrate deterministic fixtures and tests immediately.
- Keep only one current representation in core logic.
- Add temporary migration code only inside an explicit migration phase, with an
  exit gate that removes it.

No compatibility layer should survive merely because deleting it feels tedious.
