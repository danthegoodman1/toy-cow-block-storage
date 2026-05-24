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
- [x] Public `NativeFileClient` trait for native file create/info/append-lease
  lookup.
- [x] Public `NativeFile` trait for native file reads, append leases, leased
  appends, flush, and info.
- [x] `BlockServer` actor boundary.
- [x] `BlockTransport` request/response boundary.
- [x] `NativeServer` actor boundary.
- [x] `NativeTransport` request/response boundary.
- [x] Typed request and response envelopes with request ID, client epoch, and
  optional logical deadline.
- [x] Public device spec limited to logical device size and block size.
- [x] `MetadataPlane` contract for device heads, metadata nodes, commit groups,
  native file heads, file versions, PITR, checkpoints, forks, restores, and GC
  roots.
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

Status: not started.

Implement O(1) device forks.

Deliverables:

- [ ] Public fork request through `BlockDevice` and `BlockServer`.
- [ ] Child device-head creation by copying shard roots.
- [ ] Native file snapshot/fork decision documented before adding it to the
  native API.
- [ ] Fork timeline/catalog record.
- [ ] Tests that prove no metadata walk occurs during fork.

Exit gate:

- [ ] Fork cost is independent of logical device size and tree size.
- [ ] Parent and child initially read identical bytes.
- [ ] Writing parent after fork does not change child reads.
- [ ] Writing child after fork does not change parent reads.
- [ ] Generated tests cover repeated forks and divergent write histories.
- [ ] Criterion covers fork cost versus device size.

## Phase 9: Point-In-Time Recovery

Status: not started.

Implement shard commit replay and checkpoints.

Deliverables:

- [ ] Append-only `ShardCommit` records.
- [ ] Periodic `Checkpoint` records.
- [ ] Restore algorithm from checkpoint plus commits.
- [ ] Public restore request that creates a new device.
- [ ] Timeline validation.
- [ ] Tests for create, write, fork, delete, and restore interactions.

Exit gate:

- [ ] Replayed roots match live roots at tested commit sequences.
- [ ] Restore to selected times returns expected device contents.
- [ ] Checkpoint corruption or mismatch is detected by validation.
- [ ] Generated traces compare PITR reads against a simple historical model.
- [ ] Criterion covers checkpoint restore.

## Phase 10: Device Deletion and Retention Roots

Status: not started.

Implement deletion without synchronous reclamation.

Deliverables:

- [ ] Public delete request through `BlockDevice` and `BlockServer`.
- [ ] Device catalog state for live and deleted devices.
- [ ] PITR retention policy model.
- [ ] Root enumerator for live devices plus retained PITR state.

Exit gate:

- [ ] Deleted devices are absent from live listings.
- [ ] Deleted device objects remain readable only through retained PITR roots.
- [ ] Root enumeration is deterministic and independently testable.
- [ ] Deletion never directly deletes metadata nodes or segments.

## Phase 11: Tracing Garbage Collection

Status: not started.

Build reachability-based reclamation and custodian-driven physical cleanup.

Deliverables:

- [ ] Mark traversal from root enumerator.
- [ ] `last_mark_epoch` tracking.
- [ ] Sweep candidate selection.
- [ ] Delete effects for unreachable metadata nodes.
- [ ] Segment release evidence for storage-node custodians.
- [ ] Metadata custodian that publishes safe reachability epochs.
- [ ] Storage-node custodian that frees expired reservations, failed writes,
  orphan durable segments, released segments, and missed async frees.
- [ ] GC simulator hooks for interleaving writes, forks, deletes, PITR changes,
  write-intent expiry, orphan cleanup, missed frees, and sweeps.

Exit gate:

- [ ] GC never deletes objects reachable from live or retained PITR roots.
- [ ] Unreachable objects are eventually selected for deletion.
- [ ] Orphan durable segments are eventually freed after their write intent can
  no longer commit.
- [ ] `DurablePendingMetadata` segments are not freed while their write intent
  may still publish.
- [ ] Missed asynchronous frees are corrected by periodic storage-node
  reconciliation.
- [ ] Mark and sweep can be paused and resumed deterministically.
- [ ] Generated tests inject GC at adversarial points in operation traces.
- [ ] Criterion covers GC traversal.

## Phase 12: Deterministic End-to-End Simulator

Status: not started.

Prove the storage model under generated operation traces.

Deliverables:

- [ ] Simple reference model for logical device contents, native file contents,
  append leases, writer epochs, and history.
- [ ] Operation generator for create, write, read, fork, delete, restore, and GC.
- [ ] Fault injector for publish conflicts, duplicate effects, delayed effects,
  missing objects, write-intent expiry, orphan segments, missed async frees, and
  crash/replay boundaries.
- [ ] Reproducible failure output with seed, minimized trace, and object graph
  summary.

Exit gate:

- [ ] Normal CI runs a meaningful seed count for the simulator.
- [ ] Every generated trace checks core invariants after each delivered command.
- [ ] Failing seeds can be replayed exactly.
- [ ] The simulator covers fork divergence, shard contention, PITR replay,
  commit-group atomicity, data-before-metadata ordering, orphan cleanup,
  native append fencing, custodian reconciliation, and GC safety.

## Phase 13: Performance Baselines

Status: not started.

Broaden regression detection after the simple implementation exists.

Deliverables:

- [ ] Benchmarks for fork cost versus device size.
- [ ] Benchmarks for single-shard write cost versus tree depth.
- [ ] Benchmarks for multi-shard atomic write cost.
- [ ] Benchmarks for native append with valid leases and stale-lease rejection.
- [ ] Benchmarks for read lookup cost and read amplification.
- [ ] Benchmarks for checkpoint restore and GC traversal.

Exit gate:

- [ ] Benchmarks establish baseline numbers with reproducible inputs.
- [ ] Fork remains O(1) in measured object count.
- [ ] Write cost scales with changed shard paths, not whole-device metadata.
- [ ] Any proposed optimization links to a benchmark or failing test.

## Phase 14: Durable Provider

Status: not started.

Add a durable provider only after the local in-memory model and conformance suite
are boringly correct.

Deliverables:

- [ ] Provider choice documented in the spec.
- [ ] Durable segment, local segment catalog, metadata plane, device catalog, and
  timeline implementations.
- [ ] Crash/restart tests using the provider conformance suite.
- [ ] PITR and GC tests against the durable provider.

Exit gate:

- [ ] Durable provider passes the same conformance suite as the in-memory
  provider.
- [ ] Crash/restart tests preserve committed device contents.
- [ ] Partial writes do not expose uncommitted roots.
- [ ] No provider-specific behavior leaks into core metadata logic.

## Phase 15: Remote Transport

Status: not started.

Replace the local transports with remote-capable implementations without
changing the public block or native APIs.

Deliverables:

- [ ] Remote transport choice documented in the spec.
- [ ] Serialization format for request and response envelopes.
- [ ] Retry, deadline, and stale-response tests.
- [ ] Local and remote transport conformance tests.

Exit gate:

- [ ] `BlockDevice` and `NativeFile` callers do not change when transport
  changes.
- [ ] Request identity and client epoch fence duplicate or stale responses.
- [ ] Deterministic transport simulation covers delay, duplication, drop, and
  reorder faults.

## Phase 16: Storage Replication

Status: not started.

Add replicated segment storage only after the local and durable providers pass
the same conformance suite and remote transport behavior is deterministic.

Deliverables:

- [ ] Placement coordinator that chooses replica sets for logical segments.
- [ ] Replica write path that reserves, writes, syncs, and records one local
  replica commit per selected storage endpoint.
- [ ] Metadata publish waits for the requested replica durability before making
  the logical segment visible.
- [ ] Repair path that can add missing background replicas after metadata
  publish without changing public block or native APIs.
- [ ] Custodian reconciliation for failed replica writes, orphan replicas, and
  stale local catalog state.
- [ ] Fault simulation for replica delay, loss, duplication, stale writes,
  partial quorum success, and repair races.

Exit gate:

- [ ] `BlockDevice` and `NativeFile` callers do not coordinate replicas.
- [ ] Metadata leaves still reference logical `SegmentId`s, not physical
  replica placements.
- [ ] A write is acknowledged only after the configured replica durability and
  metadata publish both succeed.
- [ ] Failed metadata publish after durable replica writes leaves only
  reclaimable orphan replicas.
- [ ] Repair never makes uncommitted data visible.
- [ ] Replicated providers pass the same read/write/fork/PITR/GC conformance
  suite as single-replica providers.

## Phase 17: Optional ublk Adapter

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
