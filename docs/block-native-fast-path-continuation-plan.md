# Block-Native Fast Path Continuation Plan

## Overarching Goal

Beat pool-size-1 Ceph RBD on the GCP block comparator (`c4-standard-32-lssd`,
five local NVMe disks, flushed durability, same-commit harness) on both
throughput and p99 for `4k`, `64k`, `256k`, `1m`, and `32m`, with at least a
10% margin at `4k` and `64k`. The reference class is simplyblock-style block
storage: sub-500us p99 4K reads and high large-block throughput, measured
honestly at the durable boundary.

Remaining gap cells on the current code (`stage6-filesystem-20260619-001`,
toy-vs-Ceph throughput ratio / p99 ratio):

| Size | c1 | c4 | c16 | c32 |
| --- | --- | --- | --- | --- |
| 4k | 3.20x / 0.37x | 1.92x / 1.11x | 1.32x / 0.79x | 0.82x / 0.71x |
| 64k | 1.44x / 0.46x | 1.11x / 0.45x | 0.58x / 1.06x | 0.60x / 0.80x |
| 256k | 1.29x / 0.85x | 1.06x / 0.53x | 1.00x / 0.48x | 1.03x / 0.41x |

`1m` and `32m` have not been measured on the current code, and reads have
never been compared against Ceph on GCP.

The headline claim is the write matrix above. Reads are a first-class risk,
not a footnote: the read path shares the block-journal overlay mutex with
the write lane (`apply_read_overlay` and `apply_commit` lock the same map in
`src/local/durable/block_journal.rs`), and reads have only ever been
benchmarked locally on pre-seeded flushed data, never under concurrent write
load or against Ceph. Read comparator cells therefore land with the first
GCP trip (Phase 2), not at the comparator gate. If reads lose to Ceph, the
conditional read-path phase (Phase 6) runs before Phase 8 ships the claim;
the sub-500us 4K read p99 reference point is recorded either way.

Non-goals: SPDK work before the filesystem and direct-I/O paths are proven
insufficient, in-memory or buffered-acknowledgement performance claims, and
any weakening of `Flushed` durability or replay/fencing semantics. A
metadata-store engine swap (SQLite to an LSM such as fjall) is also a
non-goal for the comparator cells: SQLite is off the flushed-write and read
hot paths (the journal fsync is the only foreground durable operation), so a
swap would move background materialization fsync fan-out and cold reopen
time, not the gate rows. Revisit only if those costs show up in gate
measurements.

## Implementation Principles

- Never trade correctness, durability, or replay semantics for benchmark wins.
- Every performance change lands with before/after numbers from the same
  machine class, and GCP is the arbiter for comparator claims.
- GCP trips are batched: an instance session runs every measurement that is
  ready (gate rows, error tallies, baselines, read cells), not just the one
  cell the current phase needs.
- Deterministic regression tests land first or in the same change as the fix.
- Prefer removing foreground work (scans, copies, serialized publish steps)
  over adding scheduling machinery.
- Payload-policy thresholds stay provider-private and benchmark-tuned.
- No tombstones: paths that lose their reason to exist are removed, not
  feature-flagged into dormancy.

## Testing Strategy

- Full Rust gate per landed change: `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test`, `cargo doc
  --no-deps`, `cargo bench --bench regression -- --test` (run in the Linux dev
  container on macOS hosts).
- Local loadbench minimum gate for hot-path changes:
  `block-write-4k-shard-lanes`, `block-write-4k-device-lanes`,
  `block-batch-4k-16ops`, `block-batch-4k-256ops`, `block-writeback-fsync-1m`,
  `block-writeback-prestaged-fsync-1m`, `block-read-4k`, `block-read-1m`.
- GCP comparator gate for Ceph claims: same source commit for toy and
  harness, Ceph pool `active+clean`, pool size and machine class recorded,
  toy and Ceph on identical disk layout, all five sizes recorded.
- Lane and publish changes require failure-injection tests (append failure,
  sync failure, stale lease) and replay-equivalence tests.

## Phase 1: Foundation Audit

Goal:
Carry forward the verified state of the original plan
(`docs/block-native-fast-path-plan.md`) so later phases build on facts, not
recollection. This phase is an audit of completed work and closes superseded
investigation threads.

Scope:
- Original plan Stages 0-6 implementation state.
- Baseline comparator numbers and profile-derived bottleneck diagnosis.
- Closure of the June malloc/mprotect investigation thread.

Completion gate:
All rows below carry concrete evidence. No implementation work belongs to
this phase.

Testing plan:
- None (audit only).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Subphase | Stage 1 block LBA run map replacing the overlay read index | Commit `b874093`, checkpoint with local deltas in `docs/block-native-fast-path-plan.md` (Stage 1 checkpoint, 2026-06-19). |
| Complete | Subphase | Stage 2 packed 4K journal records with CRC, torn-tail, and fallback handling | Commit `6ace098`, checkpoint with encode/frame-byte deltas in the plan doc (Stage 2 checkpoint, 2026-06-19). |
| Complete | Subphase | Stage 3 lane group-commit mechanics: per-shard lanes, pending queue, owner takes all pending, completion map, no timer batching by default | `src/local/durable/coordinator.rs` (`BlockJournalFlushCoordinator`, `persist_block_journal_lane_batch`), `BlockJournalBatchPolicy` default `idle_coalesce_delay: ZERO` in `src/local/durable/policy.rs`. Measured flush group sizes: 64k mean 4.5 (c16) / 9.1 (c32), 4k mean 8.1 (c16) / 12.4 (c32). Stage 3's throughput exit gate is unmet and is superseded by Phases 2-3. |
| Complete | Subphase | Stage 4 data-log sync lanes, async catalog publisher, crash recovery of missing catalog rows | Pre-plan commits `e01141c`, `d2f0f7b`, hardening commit `3a23c81`, recovery path `src/local/durable/block_journal.rs` (`recover_block_segment_rows`), test `durable_sqlite_recovers_node_catalog_rows_without_cursor_as_storage_orphans`. |
| Complete | Subphase | Stage 5 deterministic journal materialization into CoW roots with fork/PITR retention and pruning | Commits `8b3a977` + `a494d0f`, checkpoint and test inventory in the plan doc (Stage 5 checkpoint, 2026-06-19). |
| Complete | Subphase | Stage 6 selectable direct-I/O backend with probe, padding, and replay-compatible framing | Commits `6484f10`, `02f79cf`, `22c269e`, `src/local/durable/io_backend.rs`, loadbench flag `--durable-io-backend`. |
| Complete | Scope | Baseline comparator matrix on current code, filesystem backend | `infra/gcp-local-nvme-bench/results/stage6-filesystem-20260619-001/relative-summary.csv` (table in Overarching Goal). |
| Complete | Scope | 64k c16/c32 bottleneck diagnosis: publish bookkeeping exceeds journal sync on the lane critical path | Owner-batch means from `stage6-filesystem-20260619-001` 64k `durable-profile.csv`: c32 publish 370.9us (receipt 236.2us, dispatch 74.5us) vs journal sync 168.2us; c16 publish 144.1us vs sync 146.2us; per-op lane wait 473.8us (c16) / 1049.8us (c32). Root-cause candidates: `owner_node_for_segment` in `src/local/storage_node.rs` is called twice per segment-ref from the publish path in `src/local/durable/block_journal.rs` (once via `receipt_for_segment`, once in mark dispatch) although entries already carry `storage_node`; each segment-ref also pays per-segment custody ceremony (evidence proof build plus a one-segment `MarkReferenced` request). The owner lookup is a per-node point lookup, not an O(catalog) scan, so the routing-vs-ceremony cost split is unmeasured (Phase 2 profile split). Caveat: the `*_lock_wait_nanos` columns in these profiles are hardcoded zeros, so lock contention is invisible in these means (Phase 3 item 3E). |
| Complete | Scope | 4k c32 bottleneck diagnosis: per-batch CPU overhead roughly equals one fsync | Batch means from `stage6-filesystem-20260619-001` 4k `durable-profile.csv` at c32: total 259.2us, sync 115.1us, encode 47.7us, publish 26.4us, write 15.9us, LBA map update 22.2us. |
| Complete | Scope | Direct-I/O high-concurrency errors reproduced and unresolved by retry hardening | `stage6-direct-io-20260619-001` errors (64k c16 58,737 / c32 238,573; 256k c16 238,899 / c32 409,418); post-`22c269e` rerun `stage6-direct-io-rerun-20260619-001` still errors (64k c16 58,900 / c32 264,810; 256k c16 277,277 / c32 409,055). |
| Complete | Decision | Close June malloc/mprotect investigation as superseded | The diagnosed allocation path (inline collapse buffers held via `Arc`) was replaced by the segment-ref default (`inline_max_total_bytes: 16 KiB`) and the LBA map rewrite. `gbvr-malloc64k-1` produced no results (`.abench/results/gbvr-malloc64k-1/` is empty). Do not resume. |

## Phase 2: Segment-Ref Publish Fast Path

Goal:
Make segment-ref publish cost negligible next to the journal sync so the 64k
lane stops being CPU-bound, closing the 64k c16 (0.58x) and c32 (0.60x)
throughput gaps.

Scope:
- Profile split first: break owner-batch publish time into owner-node
  routing, evidence creation, node-side verification, and request dispatch
  before committing to a fix. The two `owner_node_for_segment` calls per
  segment-ref are per-node point lookups, so per-segment custody ceremony
  may dominate the 236.2us receipt mean; the fix that carries the completion
  gate is chosen from this split, not assumed.
- Route receipt lookup, mark dispatch, and segment transport directly to the
  owning storage node using the `storage_node` already carried by journal
  segment-ref entries and receipts, removing both `owner_node_for_segment`
  fan-outs from the foreground publish path. A commit's entries can span
  nodes (chunk striping round-robins segments across nodes), so routing
  groups entries by carried node id rather than assuming one node per batch.
- Reduce per-segment ceremony as a first-class deliverable: batch
  `MarkReferenced` transport per node per batch, and amortize evidence
  construction where the proof contract allows. Evidence is bound to a
  single segment and node, so per-batch evidence needs an explicit contract
  decision note; transport batching does not. That note should weigh that
  the only accepted proof scheme today (`DeterministicTestMacV1`) is a
  keyless hash over public inputs — mismatch detection, not custody
  security — so deleting per-segment proof construction outright is a
  legitimate option rather than a weakening of real security.
- Relocate, do not drop, the invariants `owner_node_for_segment` enforces:
  the duplicate-catalog corruption check (segment present in more than one
  node catalog) moves to a background or materialization-time sweep, and a
  wrong or missing carried node id fails with a clean corruption error.
  Recovery already trusts `entry.storage_node` as authoritative
  (`recover_block_segment_rows`), so publish and replay share the direct
  routing path.
- Bundle the first GCP trip: the targeted 64k A/B run also records read
  comparator cells (Phase 8 item 8A lands before this trip), a `1m`/`32m`
  write baseline on current code, and a direct-I/O error-kind tally run
  (Phase 4 item 4A lands before this trip).
- Local A/B on the loadbench minimum gate before any GCP time.

Out of scope:
- Inline-vs-segment-ref threshold tuning (Phase 7).
- Journal record striping across shards for one device.

Completion gate:
Targeted GCP 64k run shows owner-batch `block_journal_publish_mark_nanos`
well below `block_journal_sync_nanos` at c16/c32, and 64k c16/c32 throughput
improves at least 1.5x over the `stage6-filesystem-20260619-001` toy rows
(830.56 and 901.24 MBps) with p99 not regressing. 1.5x is an interim gate:
Ceph's implied 64k rows are ~1432/1502 MBps, so the goal's 10% margin needs
roughly 1.9x. The residual is explicitly budgeted to Phase 3 (lane
pipelining covers 64k segment-ref lanes too) and Phase 7 (inline threshold),
each of which re-measures 64k. If Phase 2 lands materially short of 1.5x,
the diagnosis is wrong: return to the profile split before starting Phase 3.

Testing plan:
- Deterministic test that publish resolves segments through the entry's
  storage node without scanning other catalogs (assert via routing behavior
  or profile counters).
- Deterministic test that a segment-ref entry carrying a wrong storage node
  fails with a clean corruption error instead of silently scanning.
- Existing segment-ref replay, GC, and materialization tests stay green.
- Failure injection: mark/publish failure mid-batch completes waiters with
  errors and leaves replay-consistent state.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: publish-time profile split (routing vs evidence vs verify vs dispatch) | Split measured via new `durable-profile.csv` columns (2026-07-23 local 64k runs, GCP stage6 CSVs recomputed). On GCP c32 the owner-routing scans are ~70-85% of the 370.9us publish mean (the old `receipt` 236.2us bucket was routing scan #1 plus a point lookup local data bounds at ~0.13us/seg, and the old `dispatch` 74.5us bucket was purely routing scan #2); evidence+verify ceremony is ~3.3us/seg — 6.9% of c32 publish on GCP (25.7us/370.9us), 9.0% at c16, and the dominant remainder locally; true per-segment mark dispatch is a direct in-process call, ~0.1us/seg. Column renames for honest cross-run comparison: `block_journal_publish_receipt_nanos` (was routing scan + lookup) is now `block_journal_publish_receipt_lookup_nanos` (point lookup only) and `block_journal_publish_dispatch_nanos` (was routing scan #2) is now `block_journal_publish_mark_call_residual_nanos` (node-call residual), with new `block_journal_publish_routing_nanos` and `block_journal_publish_mark_observability_nanos` columns. Compare old `receipt+dispatch` against new `routing+receipt_lookup+mark_call_residual`; stale same-name comparisons against `stage6-filesystem-20260619-001` CSVs are invalid. |
| Complete | Work | 2B: direct segment-to-node routing in receipt lookup and mark dispatch, grouped by carried node id | `mark_block_journal_segment_refs_referenced` groups a commit's segment refs by carried node id and point-resolves each node once (`src/local/durable/block_journal.rs`); receipt lookup and mark dispatch use `carried_node`/`receipt_for_carried_segment` (`src/local/storage_node.rs`), registry marks route by `receipt.storage_node`, and both `owner_node_for_segment` fan-outs are gone from the publish path. Wrong or missing carried node id fails with a clean corruption error, never a fallback scan; replay shares the same mark path. GCP verification is tracked by the gate rows below. |
| Complete | Work | 2C: per-node-per-batch `MarkReferenced` transport and evidence-cost reduction, with contract decision note | Contract decision (2026-07-23): per-segment `ReferenceEvidence` construction and verification are deleted outright, not flagged off. The only accepted proof scheme (`DeterministicTestMacV1`) was a keyless StableHash32 over public inputs — mismatch detection, not custody security — and its mismatch-detection role is replaced by the mark request's field checks plus node-side validation that every marked segment exists in the addressed node's own catalog (absence is corruption, never a fallback scan). There is ONE mark contract for ALL callers: `StorageNodeRequest::MarkReferenced { segment_ids, metadata_commit }`, batched per node per publish on the block journal lane (one node call, one catalog lock, one observability record per node per batch) and issued per receipt-carried node by the delta-replay, native-file, txn, and materialization publish paths — no dual contract remains. If a real keyed proof scheme is ever adopted for marks, it returns as a per-batch proof over the marked segment set and metadata commit (recorded in `docs/cow-block-storage-design.md`). Profile columns measuring the deleted work (`block_journal_publish_receipt_lookup_nanos`, `block_journal_publish_evidence_nanos`, `block_journal_publish_verify_nanos`; `mark_reference_evidence_nanos`, `mark_reference_verify_nanos`) are removed, and `mark_reference_transport_dispatch_nanos` is renamed `mark_reference_dispatch_nanos` (routing + call residual + node-side event recording), so stale cross-run comparisons fail loudly. |
| Complete | Work | 2D: duplicate-catalog invariant relocated to a background/materialization sweep | `StorageNodeRegistry::verify_unique_catalog_ownership` sweeps every node catalog from `run_maintenance_tick_parts` (`src/local/durable/maintenance.rs`): the maintenance tick already walks every catalog for lifecycle reconciliation on a background cadence, so the sweep rides it off the foreground path (materialization runs on the latency-sensitive flush cadence). `owner_node_for_segment` remains only for cold non-lane callers (read verify, delta replay receipt lookup, release/GC transport) — live paths, not dormant duplicates. A failed worker tick (including a sweep-detected duplicate) now records a `MaintenanceTickFailed` event and `maintenance_tick_failures` counter instead of being silently swallowed by the AlwaysOn worker loop. |
| Complete | Test | Routing, wrong-node, failure-injection, replay, mark-contract, and sweep tests | `src/local/tests/mod.rs`: `durable_block_journal_publish_routes_segment_refs_through_carried_nodes`, `durable_block_journal_publish_rejects_segment_ref_carrying_wrong_storage_node`, `durable_block_journal_replay_rejects_segment_ref_carrying_wrong_storage_node`, `durable_block_journal_publish_mark_failure_mid_batch_errors_waiters_and_replays`, `maintenance_tick_sweep_detects_segment_in_multiple_node_catalogs`, `storage_node_rejects_reference_for_segment_absent_from_catalog` (reworked from the evidence-proof rejection test), `storage_node_transport_write_receipt_stays_pending_until_reference_message` (migrated to the evidence-free mark request). |
| Complete | Gate | Local A/B minimum gate with no >5-10% regression | `.abench/results/phase2-local-ab-20260723/` (before = clean `5031b2a`, after = Phase 2 working tree; all 8 minimum-gate scenarios at c1/4/16/32, 2-3 reps per side, zero errors). Median deltas: reads improve 10-24% throughput with p99 down 10-69%, writeback cells within 5%, 4k lane cells within noise. Three cells show median throughput below 0.9x (`block-batch-4k-16ops` c1 0.61x, `block-batch-4k-256ops` c16 0.79x, `block-write-4k-shard-lanes` c4 0.84x), explained as macOS-Docker VM disk variance: within-side rep spread reaches 1.9x on those cells, per-rep distributions overlap across sides (both sides hit matching high and low reps), p50s interleave, and the change only removes foreground publish work from those paths. The bundled GCP trip re-measures the definitive rows. Note: the 64k lane profile shows publish mean 65.9us -> 21.8us at c32 locally (`phase2a`/`phase2c` result dirs). |
| Incomplete | Gate | Bundled GCP trip: 64k gate rows plus read cells, `1m`/`32m` baseline, direct-I/O error tallies | Missing: results directory under `infra/gcp-local-nvme-bench/results/`. |

## Phase 3: 4K Lane Pipeline And Batch CPU Trim

Goal:
Overlap lane CPU work with the in-flight fsync and trim per-batch overhead so
4k c32 beats Ceph (currently 0.82x throughput) and 4k c4 p99 stops trailing
(1.11x), without hurting the winning 4k cells.

Scope:
- Pipeline the lane: encode and write batch N+1 while batch N's fsync is in
  flight, keeping completion and visibility ordering exactly as today. An
  fsync that covers later-written bytes is harmless; completions must still
  only fire for records covered by a finished sync.
- Pipelining correctness requirements, stated as deliverables because
  `sync_block_journal` is a whole-file `sync_data` with no byte range:
  - Durable-coverage accounting: a batch's records may only be credited to
    an fsync that began after the batch's write fully returned; never credit
    an fsync with bytes written after it started.
  - Lane poisoning: replay truncates at the first torn frame, so a complete
    later frame written after a failed earlier write could be acknowledged
    and then silently dropped at replay. Any append or sync failure poisons
    the lane and errors every later in-flight batch until the tail is
    re-established (truncate or reopen).
- Move publish apply and LBA-map visibility work off the sync critical path
  where read-your-writes and replay semantics allow. Per-device commit-seq
  publish order is preserved; only the overlap with the sync changes.
- Trim the copy chain and per-batch allocations: a 4K payload is copied
  roughly seven times between the public API and the read overlay
  (`data.to_vec()` at ingress, collapse `bytes.clone()`, `chunk.to_vec()`
  into entries, encode serialize, frame buffer, outer append buffer, and
  `Arc::from(bytes.clone())` into the overlay). Carry one refcounted buffer
  from ingress — the lane itself already moves rather than copies — and pool
  the per-batch encode/records buffers
  (`append_block_journal_records_unsynced` allocates a fresh `Vec` per
  call).
- Named batch trims from the 2026-07-14 hot-path audit, each gated on
  profile deltas: cache the journal fd per shard (the filesystem backend
  reopens the shard file on every append and again for every sync, roughly
  two stats, two opens, and two closes per batch stacked next to the fsync);
  deduplicate `validate_block_writer` and `device_info` (three global
  metadata-mutex hits and spec clones per op for values stable over the
  lease lifetime — cache them on the lease); reserve commit sequences with a
  per-device atomic instead of the global staging lock plus metadata mutex;
  wake exactly the completed waiters instead of `notify_all` on every
  enqueue and every batch completion (a thundering herd at c32 on the
  default single shard); and unify `publish_reserved_block_journal_commit`
  head/generation bookkeeping with the overlay's `apply_commit` so lane
  publish advances visibility under one lock, not two.
- Fix profile honesty before drawing pipeline conclusions: the
  `*_lock_wait_nanos` columns in `durable-profile.csv` are hardcoded zeros
  (read resolve returns a default profile and the overlay mutex has no wait
  counter), and profiler sinks are global mutexes. Lock waits must be
  measured for real, with per-thread or sharded sinks, so the Phase 2/3
  profile splits reflect contention instead of hiding it.
- Re-examine `target_requests` and group-size behavior at c32 with profile
  evidence before touching policy defaults. `target_requests` is a floor
  (the batch owner takes all pending requests), not a cap, so the c32
  question includes whether unbounded batches are too large, not only
  whether groups are too small.
- Lease fencing is enforced upstream of the lane (lease acquire and
  commit-seq reservation), so pipelining adds no new fencing hazard; the
  fencing test below is a guard, not a suspected gap.

Out of scope:
- Timer-based batching (only reconsider with before/after p99 evidence, per
  the original plan).
- io_uring or backend changes (Phase 4 owns backend work).

Completion gate:
Targeted GCP 4k run: c32 throughput at least 1.1x Ceph's 200.66 MBps row with
p99 still beating Ceph, c4 p99 at or below Ceph's 585.7us, and c1/c16 rows
within tolerance of `stage6-filesystem-20260619-001`. The 64k c16/c32 rows
are re-measured on the same trip to record Phase 3's contribution to the
residual 64k budget from Phase 2.

Testing plan:
- Failure injection under pipelining: append failure or sync failure in batch
  N while batch N+1 is encoded must complete the right waiters with errors
  and preserve replay equivalence (no acknowledged record beyond the durable
  high-water).
- Lane poisoning: a failed write in batch N followed by a fully written batch
  N+1 never acknowledges N+1's records and errors its waiters.
- Coverage accounting: records written while an fsync is in flight are not
  completed by that fsync, only by a later one.
- Read-your-write visibility test under pipelined publish.
- Stale-lease fencing ordering test across pipelined batches.
- Local loadbench minimum gate A/B.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 3A: lane pipelining (encode/write overlap with in-flight sync) | Missing: implementation in `src/local/durable/coordinator.rs` lane loop. |
| Incomplete | Work | 3B: publish apply and LBA visibility off the sync critical path | Missing: implementation plus a written note on why visibility ordering is preserved. |
| Incomplete | Work | 3C: copy-chain trim and buffer pooling (one refcounted payload from ingress to overlay) | Missing: encode nanos before/after from `durable-profile.csv`. |
| Incomplete | Work | 3D: named batch trims (cached shard fd, lease-cached device info, atomic commit-seq, targeted wakeup, unified visibility bookkeeping) | Missing: implementation plus per-trim profile deltas. |
| Incomplete | Work | 3E: real lock-wait instrumentation with per-thread or sharded profiler sinks | Missing: non-zero lock-wait columns and a contention note. |
| Incomplete | Test | Pipelined failure-injection, visibility, fencing, and replay tests | Missing: test names in `src/local/tests/mod.rs`. |
| Incomplete | Gate | Targeted GCP 4k run meeting the completion gate | Missing: results directory and ratio summary. |

## Phase 4: Direct I/O Error Root Cause And Keep/Remove Decision

Goal:
Root-cause the direct-I/O errors at 64k/256k c16/c32, then either make the
backend a measured win or remove it.

Scope:
- 4A, landing before Phase 2's GCP trip: add error-kind tallies to loadbench
  output (per workload/concurrency error kind counts plus a first-error
  sample). Today `StorageError` carries no kind/errno — underlying
  `io::Error`s are stringified into `Unavailable` and loadbench drops the
  error entirely, keeping a bare `u64` count — so this includes plumbing
  `io::ErrorKind`/errno through the error path (or at minimum sampling
  first-error strings per worker).
- Leading hypothesis for 4B/4C: the direct-I/O write loop retries only
  unaligned short writes; every underlying `io::Error` (EAGAIN, EINVAL,
  ENOSPC) propagates immediately. O_DIRECT surfacing EAGAIN under queue
  depth at larger write sizes fits the signature (errors only at 64k/256k
  c16/c32, none at 4k) and would explain why retry hardening changed
  nothing. Confirm with tallies before writing a fix.
- Reproduce locally: Linux dev container with a loop-mounted XFS image so
  `O_DIRECT` behaves like the GCP disks (the container's default overlay
  filesystem will not exercise it). A clean local run is non-exonerating:
  loop devices may not reproduce NVMe queue-pressure errno behavior, so GCP
  tallies remain the arbiter.
- Root-cause and fix. Retry hardening (`22c269e`) is already disproven as the
  fix, so gather the actual error kind and errno before writing more code.
- Re-verify on GCP with the same run shape, then decide: keep direct-I/O only
  if it beats the filesystem backend at equal concurrency on 4k/64k/256k;
  otherwise delete the backend and its selector plumbing. If kept, note the
  per-file mutex plus single scratch mmap serializes concurrent appends to
  one file; a scratch-buffer pool is part of making it a measured win.

Out of scope:
- io_uring, fixed buffers, polled I/O (only relevant after this gate).

Completion gate:
Zero errors across all comparator cells on GCP with direct-I/O, plus a
recorded keep/remove decision backed by same-concurrency numbers. If removed,
the backend code, flags, and docs are gone in the same change.

Testing plan:
- Regression test or harness case reproducing the failing high-concurrency
  shape locally on the loop-mounted filesystem.
- Existing direct-I/O padding/replay/probe tests stay green if kept.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 4A: loadbench error-kind tallies with errno/kind plumbed through `StorageError` | Missing: implementation in `src/bin/loadbench` and a column/summary in output; lands before Phase 2's GCP trip. |
| Incomplete | Work | 4B: local O_DIRECT reproduction on loop-mounted XFS | Missing: repro recipe and observed error kind/errno. |
| Incomplete | Work | 4C: root-cause fix | Missing: diagnosis note and fix commit. Retry hardening `22c269e` did not reduce errors (rerun evidence in Phase 1). |
| Incomplete | Gate | GCP re-verification with zero errors | Missing: results directory. |
| Incomplete | Decision | Keep or remove the direct-I/O backend | Missing: same-concurrency comparison against filesystem backend after the fix. |

## Phase 5: Journal Replay And Fencing Hardening

Goal:
Close the two correctness gaps found by the 2026-07-14 adversarial audit
before any comparator claim ships: a torn or garbage journal tail bricks
reopen, and block-writer fencing is advisory.

Scope:
- Torn-tail truncation policy. `load_durable_journal_records` treats a
  complete-length tail frame with a CRC mismatch, or 24+ bytes of non-zero
  non-magic tail garbage, as fatal corruption, and the error fails reopen
  for the whole store (`load_block_journal_overlay` propagates it), not one
  shard. Both shapes are reachable from an unacknowledged crash mid-write
  (writeback persisting file size ahead of data pages, O_DIRECT power loss,
  a failed direct append whose tracked length was not advanced). Change
  replay to truncate at a bad tail frame when no valid frame follows it,
  per shard; keep the hard corruption error only when valid frames follow
  the bad one (real interior corruption). The data-log scanner already
  truncates on unparseable non-zero headers; the journal should match.
- Real block-writer fencing, or an explicit contract note that fencing is
  advisory. Today `validate_block_writer` runs at batch entry only (a
  time-of-check race against the in-memory epoch map); nothing re-validates
  at lane seal, so a stale writer's staged batch can append, sync, and
  publish after a new writer's epoch bump. The replay epoch guard compares
  against a latest-epoch map that already includes the checked commit's own
  epoch, so it can never fire (dead code). If single-writer exclusivity is
  the contract, add an epoch gate at lane seal and a real replay-time
  rejection sourced from durable lease records; the append-stream path's
  publish-time fencing is the model. The gate must not add a global lock to
  the lane (the lease already carries the epoch).

Out of scope:
- Performance work (Phases 2-3 own the lane; this phase is semantics only).

Completion gate:
Deterministic tests cover every tail shape (short tail, zero tail, bad-CRC
complete tail frame, 24+ byte garbage tail, bad frame followed by a valid
frame) with truncation or a targeted per-shard error exactly per policy, and
a zombie-writer test shows a stale batch sealed after a new writer's acquire
is rejected at seal and at replay. Existing replay tests stay green. The
current torn-tail test appends a 15-byte tail, which hits the benign
short-tail break and exercises neither failing shape; it is not sufficient
evidence on its own.

Testing plan:
- Covered by the completion gate; the tail-shape matrix and zombie-writer
  cases land as deterministic tests in `src/local/tests/mod.rs`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 5A: per-shard torn-tail truncation policy in journal replay | Missing: implementation in `load_durable_journal_records` and shard-isolation handling. |
| Incomplete | Work | 5B: fencing contract decision plus seal-time and replay-time epoch gates | Missing: decision note, implementation, and removal of the dead replay guard. |
| Incomplete | Test | Tail-shape matrix and zombie-writer tests | Missing: test names in `src/local/tests/mod.rs`. |

## Phase 6: Read Path Scalability (Conditional)

Goal:
Run only if Phase 2's read cells lose to Ceph or miss the sub-500us 4K read
p99 reference: make reads scale independently of writers. If the read cells
already win, record that decision here and skip the phase.

Scope (pre-diagnosed levers from the 2026-07-14 audit, to be confirmed
against the read-cell profiles before implementation):
- Lock-free metadata reads. Every read resolve walks the CoW tree through
  `get_metadata_node`, which re-locks the single global
  `InMemoryMetadataPlane` mutex and deep-clones each `MetadataNode` visited,
  while writers publish under the same mutex. Publish an immutable
  per-device-head snapshot readers walk without locks (the CoW nodes are
  already immutable), and shard the writer side by device.
- Non-blocking overlay reads. `apply_read_overlay` scans the journal overlay
  under the same global mutex `apply_commit` holds for LBA-run inserts
  (measured hold p99 39us, max 1.7ms), so a read on one device can stall
  behind a write's map update on another. Shard the overlay by device and
  make the read side non-blocking (reader-writer or per-commit snapshot).
- Bound overlay growth between materializations so overlay scan cost stays
  capped under sustained writes.

Completion gate:
GCP read cells beat Ceph librbd `randread` on p99 at c1/c4/c16 with write
cells within tolerance, or a recorded decision documents why the read target
changed.

Testing plan:
- Read-your-writes and read-under-write-load correctness tests for any
  locking change; existing overlay and replay tests stay green.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | Run-or-skip based on Phase 2 read cells | Missing: Phase 2 read results. |
| Incomplete | Work | 6A: lock-free per-device metadata snapshots for read resolve | Missing: implementation in `src/local/metadata_plane.rs`. |
| Incomplete | Work | 6B: sharded, non-blocking journal overlay reads | Missing: implementation in `src/local/durable/block_journal.rs`. |
| Incomplete | Gate | GCP read cells meeting the completion gate | Missing: results directory. |

## Phase 7: Payload Policy Tuning

Goal:
Pick benchmark-backed payload thresholds now that publish and lane costs have
changed, per the original plan's payload policy stage.

Scope:
- Evaluate inline vs segment-ref for the 16 KiB - 256 KiB band on GCP after
  Phase 2 (`inline_max_total_bytes` currently defaults to 16 KiB, so 64k
  writes take the segment-ref path).
- Confirm the 2 MiB `segment_chunk_bytes` striping choice still holds for
  `1m`/`32m` on the current code.
- Record chosen defaults and their evidence in `policy.rs` doc comments and
  the design doc.

Out of scope:
- Public API changes (thresholds stay provider-private).

Completion gate:
Threshold sweep artifacts exist for at least 16/32/64 KiB inline caps and
1m/32m chunk sizes, defaults are chosen from those numbers, and no comparator
cell regresses beyond tolerance.

Testing plan:
- Existing payload-policy unit tests updated for any default change.
- GCP sweep runs recorded under `infra/gcp-local-nvme-bench/results/`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: inline-cap sweep on GCP after Phase 2 | Missing: sweep results and chosen default. |
| Incomplete | Work | 7B: 1m/32m chunk-size confirmation on current code | Missing: sweep or confirmation run. |
| Incomplete | Doc | Threshold rationale in `policy.rs` and design doc | Missing: updated doc comments citing the runs. |

## Phase 8: Full Comparator Gate Including Reads

Goal:
Prove the headline claim on one commit: toy beats Ceph RBD on throughput and
p99 for all five sizes with 10% margin at 4k and 64k, and record where toy
stands on the simplyblock-style read target.

Scope:
- Full GCP matrix, toy and Ceph from the same commit: `4k`, `64k`, `256k`,
  `1m`, `32m` writes at c1/4/16/32, flushed durability, pool `active+clean`,
  pool size 1, environment recorded.
- Read comparator cells: fio librbd `randread` vs toy `block-read-4k` and
  `block-read-1m` on the same disk layout, recording p50 and p99. The
  harness work is tracked here as 8A but lands before Phase 2's bundled GCP
  trip; Phase 8 re-runs reads on the final commit. Record whether 4K read
  p99 lands under 500us at c1/c4/c16, plus one mixed read-under-write-load
  cell if cheap in the harness (pure-read cells are the gate).
- Update `docs/block-native-fast-path-plan.md` target-outcome status and the
  design doc with the final snapshot, and summarize in the merge commit.

Out of scope:
- Sub-200us durable-write stretch work (record the decision input only:
  down-stack work is justified only if this gate shows the filesystem path
  topping out short of the target).

Completion gate:
One results directory shows toy beating Ceph on throughput and p99 for all
five sizes with at least 10% margin at 4k and 64k, plus recorded read
comparisons. Any missed cell reopens the relevant phase instead of shipping a
partial claim; a read loss runs (or reruns) Phase 6 rather than shipping the
write claim silently.

Testing plan:
- Harness syntax checks and a smoke run before the full matrix.
- Full Rust gate on the exact commit under test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 8A: read comparator cells in the GCP harness (lands before Phase 2's GCP trip) | Missing: `randread`/`block-read` support in `infra/gcp-local-nvme-bench/remote_block_vs_rbd.sh` and summary plumbing. |
| Incomplete | Gate | Full five-size write matrix win with margins | Missing: results directory and `relative-summary.csv` meeting the gate. |
| Incomplete | Gate | Read p99 comparison recorded, including the sub-500us check | Missing: read rows in the results summary. |
| Incomplete | Doc | Plan and design doc updated with the final snapshot | Missing: doc updates on the commit under test. |

## Phase 9: Finalization And Merge

Goal:
Land the branch stack as reviewed, gated, documented main-line history.

Scope:
- Full Rust gate in the dev container on the final commit.
- Fold `STATUS.md` into the plan documents and delete it (it duplicates the
  Stage 6 notes and is a stale status snapshot).
- Merge the `codex/*` branch stack into `main` and push, preserving the
  per-stage commits.
- `docker compose down` after container work.

Completion gate:
`main` contains the full stack, the Rust gate is green on the merge commit,
`STATUS.md` is gone, and docs describe the current architecture without
stale checkpoints contradicting the code.

Testing plan:
- `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D
  warnings`, `cargo test`, `cargo doc --no-deps`,
  `cargo bench --bench regression -- --test` on the merge commit.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 9A: fold and delete `STATUS.md` | Missing: deletion commit. |
| Incomplete | Gate | Full Rust gate green on the merge commit | Missing: gate output on the final commit. |
| Incomplete | Work | 9B: merge stack to `main` and push | Missing: merge commit on `origin/main`. |
