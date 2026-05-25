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
- Phase 16 owns the first local durable snapshot provider: segment sync,
  atomic metadata/storage-node snapshots, commit-group persistence,
  write-intent recovery, native append lease/session records,
  checkpoint/timeline persistence, and cache coherence after restart.
- Phase 17 owns remote transport serialization, retry/deduplication, stale
  response rejection, server incarnation fencing, deadlines, mailbox semantics,
  backpressure, and concurrency rules for non-conflicting requests.
- Phase 18 owns the durable provider crash/fault-injection matrix and the
  decision of whether the snapshot provider remains sufficient or needs a
  journal/database-backed metadata provider.
- Phase 19 owns a real network implementation of the Phase 17 wire contract.
- Phase 20 owns placement, replica-set selection, replica reference evidence,
  release evidence logs or per-node queues, storage-node cursors, repair
  records, orphan replica reconciliation, stale placement handling, and physical
  free reconciliation across storage nodes.

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
- [x] Internal `SegmentFileIo` or equivalent storage-node file I/O boundary
  below `SegmentStore` and `LocalSegmentCatalog`.
- [x] Portable blocking filesystem segment I/O backend used by the durable
  storage node by default.
- [x] Crash-consistent `sync_segment` and `flush` definitions, including exact
  `durable_through` semantics.
- [x] Durable metadata snapshots for commit groups, checkpoints, delete records,
  fork records, native keyspace commits, and native file-root audit commits.
  The first provider uses atomic binary snapshots, not a metadata WAL.
- [x] Durable write-intent table with logical expiration, cancellation/failure
  evidence, and restart recovery scan.
- [x] Durable native append lease/session records with restart-safe writer
  epochs and stale-writer rejection after recovery.
- [x] Cache coherence rules for hot heads, metadata nodes, checkpoints, and
  segment descriptors after restart.
- [x] Crash/restart tests for committed block contents, native keyspace state,
  writer epochs, PITR restore points, and storage-node custodian deletions.
- [x] Explicit portable segment file I/O sequencing test for temp write, temp
  file sync, atomic rename, final directory sync, and tmp cleanup.
- [x] PITR and GC tests against the durable provider.

Exit gate:

- [x] Durable provider passes the currently implemented restart and lifecycle
  conformance tests for block and native APIs.
- [x] Crash/restart tests preserve committed device contents.
- [x] Partial writes do not expose uncommitted roots.
- [x] Atomic snapshot publishing means a completed metadata snapshot reopens as
  one committed state; the full injected crash matrix for every metadata
  snapshot boundary is deferred to Phase 18.
- [x] Pending segment writes left by crashed, expired, or fenced write intents
  become reclaimable without exposing data.
- [x] The portable segment file I/O backend preserves the documented durability
  sequence: payload bytes are durable before final segment visibility, and final
  path visibility is durable before catalog state can claim the segment is
  durable.
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

Status: not started.

Harden the snapshot-based durable provider by testing every durable boundary as
an explicit crash/restart point. This phase either proves the simple atomic
snapshot provider is enough for the toy system's durability contract, or
produces the evidence needed to replace it with a journal/database-backed
metadata provider. Do not silently grow a second durable format; if a journal
or database provider is chosen, update the spec and remove the superseded
snapshot-only path in the same phase.

Deliverables:

- [ ] Reusable durable provider conformance harness that can run against the
  in-memory model, the snapshot durable provider, and any future journal or
  database-backed provider where applicable.
- [ ] Fault-injected segment file I/O backend that can fail or crash at temp
  file create/write, temp file sync, atomic rename, final directory sync, and
  tmp cleanup.
- [ ] Fault-injected snapshot writer for segment-store snapshots, storage-node
  catalog snapshots, and metadata snapshots.
- [ ] Crash/reopen matrix for block writes, multi-shard commit groups, forks,
  deletes, PITR checkpoints/restores, native writes, native appends, native
  keyspace checkpoints, and native keyspace snapshots/restores.
- [ ] Decision record for keeping atomic binary snapshots or replacing them
  with a journal/database-backed metadata provider.
- [ ] If a journal/database provider is required, implement it behind the same
  provider contracts and remove any obsolete snapshot-only compatibility path
  from production use.

Exit gate:

- [ ] Every injected crash point reopens as either the old committed state or
  the complete new committed state; no partial commit group, partial keyspace
  commit, or metadata reference to missing segment bytes is observable.
- [ ] Replaying after repeated crashes is idempotent and does not leak write
  intents, append leases, temporary segment files, or durable-pending catalog
  entries.
- [ ] `flush` reports only commit sequences whose segment bytes, storage-node
  catalog state, segment descriptors, and metadata state survive reopen.
- [ ] Storage-node custodian and metadata custodian can resume after crashes
  without freeing live or retained-PITR data.
- [ ] The chosen durable metadata format has no untested compatibility shim left
  behind.

## Phase 19: Real Network Transport

Status: not started.

Implement an actual network adapter for the Phase 17 serialized wire contract.
This phase is about crossing a process or host boundary, not changing storage
semantics and not adding replication.

Deliverables:

- [ ] Protocol choice documented in the spec, including framing, maximum frame
  size, request/response envelope codec, and server incarnation handshake.
- [ ] Network block transport that implements `BlockTransport` without changing
  `BlockDevice` callers.
- [ ] Network native transport that implements `NativeTransport` without
  changing `NativeFile` callers.
- [ ] Network server endpoint for block and native request envelopes over the
  shared `RemoteWireTransport` contract.
- [ ] Bounded connection queues, explicit backpressure, timeout/deadline
  behavior, reconnect behavior, and shutdown behavior.
- [ ] Loopback integration tests plus deterministic chaos tests that reuse the
  Phase 17 drop, duplicate, delay, reorder, stale-response, and corrupt-frame
  cases.

Exit gate:

- [ ] In-process, serialized remote, chaos-wrapped, and real network transports
  pass the same block and native transport conformance tests.
- [ ] Network failures surface as transport errors; callers can retry with the
  same request identity without double-applying successful server mutations.
- [ ] Stale server incarnations, mismatched response IDs, oversized frames, and
  malformed frames are rejected deterministically.
- [ ] Backpressure is bounded and observable; the network adapter does not hide
  unbounded queues or background retries.
- [ ] Public block/native APIs and provider contracts do not change.
- [ ] The network adapter does not choose storage nodes or fan out replicas.

## Phase 20: Storage Replication

Status: not started.

Add replicated segment storage only after the local and durable providers pass
the same conformance suite, remote transport behavior is deterministic, and the
real network transport has proven the wire contract across a process boundary.

Deliverables:

- [ ] Placement coordinator that chooses replica sets for logical segments.
- [ ] Storage-node identity, capacity, and failure-domain policy inputs for
  placement decisions.
- [ ] Replica write path that reserves, writes, syncs, and records one local
  replica commit per selected storage endpoint.
- [ ] Metadata publish waits for the requested replica durability before making
  the logical segment visible.
- [ ] Durable metadata-to-storage reference evidence stream or reconciliation
  path so durable-pending replicas become `Referenced` after metadata publish.
- [ ] Durable metadata-to-storage release evidence stream or per-node release
  queues keyed by safe reachability epoch.
- [ ] Storage-node reference/release cursors and idempotent apply paths for
  referenced and released logical segments.
- [ ] Repair path that can add missing background replicas after metadata
  publish without changing public block or native APIs.
- [ ] Repair records and cursors for idempotent copy, source selection, checksum
  validation, and restart after interrupted repair.
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
  per-node cursors after storage-node or metadata-service restart.
- [ ] Repair never makes uncommitted data visible.
- [ ] Stale placement decisions cannot overwrite, free, or repair the wrong
  logical segment or replica placement.
- [ ] Replicated providers pass the same read/write/fork/PITR/GC conformance
  suite as single-replica providers.

## Phase 21: Linux io_uring Storage Node Backend

Status: not started.

Add a Linux `io_uring` implementation of the Phase 16 storage-node segment file
I/O boundary after Phase 18 proves the durable crash/reopen contract. This
phase is a measured storage-node optimization, not a new public API and not a
new durability model. The portable blocking filesystem backend remains the
default fallback.

Deliverables:

- [ ] Linux-only `io_uring` backend behind an explicit feature flag.
- [ ] The `io_uring` backend plugs into the Phase 16 segment file I/O boundary
  without changing storage-node lifecycle, catalog, or metadata code.
- [ ] Shared conformance and crash/restart tests run against both the portable
  backend and the `io_uring` backend when the host supports it.
- [ ] Benchmarks for concurrent segment reads, concurrent segment writes,
  batched segment writes, sync-heavy writes, and mixed read/write storage-node
  workloads.
- [ ] Documentation of when the provider may select the portable backend
  automatically, such as non-Linux hosts, unsupported kernels, disabled feature
  flags, or failed backend initialization.

Exit gate:

- [ ] `BlockDevice`, `NativeFile`, `MetadataPlane`, `SegmentStore`, and
  `LocalSegmentCatalog` public contracts do not change.
- [ ] Both segment file I/O backends, when available, pass the same storage-node
  conformance and crash/restart tests.
- [ ] The `io_uring` backend preserves the exact Phase 16 durability contract:
  payload bytes are durable before final segment visibility, and final path
  visibility is durable before catalog state can claim the segment is durable.
- [ ] Fallback to the portable backend is explicit, observable in diagnostics,
  and does not weaken correctness.
- [ ] The `io_uring` backend is kept only if benchmarks show meaningful
  concurrent storage-node throughput or tail-latency improvement.
- [ ] Backend-specific behavior does not leak into metadata, PITR, GC, block
  API, native file API, or deterministic core logic.

## Phase 22: Optional ublk Adapter

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
