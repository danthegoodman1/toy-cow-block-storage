# Phase 2 Handoff: 64k Catalog-Mutex Contention

## 0. Status addendum (2026-07-23, resumed on Linux/NVMe machine)

Phase 2 resumed; the sections below are the pre-resume record and stay
unchanged. What happened since:

- Milestone 5 (commit `9f0c821`) built the missing measurement from section 3:
  every catalog acquisition goes through a tagged mutex
  (`CatalogAcquirer`, 24 call-site tags) recording per-tag
  acquisitions/wait/hold/max, surfaced via `loadbench --catalog-hold-csv` and
  passed through the confirm A/B harness only to binaries that support it.
- GCP trip `phase2-holder-attr-20260723` (same-instance A/B, base `5966896`
  vs instrumented `9f0c821`, us-east1-b; results local to the Linux machine
  per repo convention) NAMED THE HOLDER: `prestage_snapshot` — the
  `prestage_block_segments` -> `state_for_segment_ids` all-node catalog scan,
  5 acquisitions per op, holding 14.3-15.5% of total catalog time at 64k c32
  (~9.3us/acquisition) with ~9-10s cumulative lock wait per ~5s window.
  `mark` is a victim (40ms held vs 1.3-1.5s waited). Instrumentation
  overhead: fix >= base throughput in every cell; guards/reads flat.
- Section 3 ledger updates: open suspect 1 (append-stream persist snapshots)
  is REFUTED for block benches — every `Persist*` tag was zero in all 20 GCP
  profiles; those paths never run there. Open suspect 2 (short-hold convoy)
  is CONFIRMED in refined form: ~158k catalog acquisitions/s at c32
  (~32k/s per catalog), with prestage both the dominant holder and waiter.
  Block reads take zero catalog acquisitions. `staging_reserve` is the
  secondary holder (4.3-4.7% held, max holds ~1.3ms, likely
  preemption-inflated at c32 = vCPUs) — a candidate follow-up only if the
  gate stays short after the prestage fix.
- Milestone 6 (this commit): the block-journal lane carries
  `DurableSegmentPayload` values (zero-copy windows) from staging receipts in
  `StagedBlockSegmentRefs.payloads`; `prestage_staged_block_segments`
  replaces the catalog fetch, so the lane takes ZERO catalog acquisitions for
  prestage. The catalog-based `prestage_block_segments` survives only for the
  block-delta writeback path under the honest tag `PrestageDeltaSnapshot`
  (kept because that layer does not hold those segments' payload windows —
  carrying them through `commit_block_batch_with_delta` is a legitimate
  future milestone if that tag ever lights up). Corruption surface preserved:
  a missing carried payload for a not-yet-durable staged segment is a corrupt
  error, no fallback scans. Reviewer-approved twice (initial + delta);
  8-scenario local A/B clean (throughput ratios 0.98-1.13, read watch item
  flat at 1.005x).
- Local-benching caveat learned: the Linux machine's consumer NVMe cannot
  A/B bandwidth-heavy cells (SLC-cache/GC variance up to 2.2x within a side);
  regression A/Bs run on the RAM-backed `/mnt/gcpsim` null_blk device (ext4,
  ~200us fsync). Neither device reproduces the GCP contention magnitude
  (max ~6us mark lock wait locally vs 77-120us on GCP), so GCP same-instance
  A/Bs remain the only decisive measurement — as section 2 already said.
- Pre-existing flakes on record (neither caused by M5/M6, both verified at
  clean HEAD): `durable_persist_profiling_is_opt_in_and_records_physical_persists`
  (first-run data-log sync timing) and
  `durable_block_prestaged_flush_skips_directory_sync_for_existing_log`
  (~10% under parallel `_prestage` filter, dir-sync accounting race on the
  delta lane, present since 60c9c91). Worth a phase-level follow-up ruling.
- Final gate trip `phase2-m6-gate-20260723` (same-instance A/B, base
  `9f0c821` vs fix `9282933` = M6, us-east1-b, zero errors): c32 publish
  117.5/169.8us -> 47.7/49.8us with publish-mark lock wait 91.4/139.3us ->
  24.4/25.6us (reps 1/2); c16 publish 56.7/62.2 -> 26.4/25.8us. Publish is
  now well below sync (fix sync 158.6-174.5us at c32). Throughput fix/base:
  c16 1.017/1.035x, c32 1.042/1.058x; vs stage6: c16 918.9-946.5 MBps
  (1.11-1.14x), c32 1089.3-1114.0 (1.21-1.24x). Fix-side holder profile:
  prestage tags absent entirely (zero acquisitions), mark wait down ~4x,
  total catalog pressure roughly halved; `staging_reserve` is now the top
  holder (6.6% held at c32) and is the recorded reopening point if catalog
  contention ever returns. Watch item `block-read-4k` c32 fix/base
  0.964/0.992 — inside the band; 4k guard flat.
- PHASE 2 CLOSED 2026-07-23 under the section 6 budget clause:
  publish-below-sync met, 1.5x throughput unmet, and the remaining 64k
  budget is the sync-side residual (sync grows with concurrency;
  158.6-193.7us at c32 this trip), which belongs to Phase 3 lane
  pipelining. The plan ledger row in
  `docs/block-native-fast-path-continuation-plan.md` records the same
  resolution. Teardown verified: zero instances, zero `toy-cow-*` networks
  in the project after the final trip.

Audience: an agent resuming Phase 2 of
`docs/block-native-fast-path-continuation-plan.md` on a Linux machine with
real local NVMe, with no access to the conversation that produced this state.
Phase 2 is PAUSED, not closed: the interim 64k throughput gate is unmet, the
last hypothesis was refuted by a controlled A/B, and the machine this work ran
on cannot reproduce the bottleneck. Everything proven, refuted, and still open
is recorded below with its evidence.

## 1. State: what Phase 2 landed and where

Commit `55dc00c` ("Land Phase 2: segment-ref publish fast path") contains
items 2A-2D of the plan's Phase 2. A follow-up diff (the milestone-4
contention fix plus the confirm-trip harness, committed together with this
document) is the final code state. Summary of what is now true in the code:

- Publish-time profiling is honest and per-bucket
  (`block_journal_publish_routing_nanos`, `..._mark_call_residual_nanos`,
  `..._mark_catalog_nanos`, `..._mark_lock_wait_nanos`,
  `..._mark_observability_nanos` in `durable-profile.csv`).
- Segment-ref publish routes directly by the storage node carried in journal
  entries and receipts (`carried_node` in `src/local/storage_node.rs`);
  the old `owner_node_for_segment` catalog scans are gone from the lane. A
  wrong or missing carried node id is a clean corruption error, never a
  fallback scan. Replay shares the same mark path.
- The mark contract is evidence-free and unified for every caller:
  `StorageNodeRequest::MarkReferenced { segment_ids, metadata_commit }`,
  batched per node per publish on the block-journal lane (one node call, one
  catalog lock, one observability record per node per batch). Per-segment
  `ReferenceEvidence` construction/verification was deleted outright; the
  decision note is in the Phase 2 ledger and
  `docs/cow-block-storage-design.md` (custody flow section).
- The duplicate-catalog invariant (a segment id in more than one node
  catalog) lives in the maintenance-tick sweep
  (`verify_unique_catalog_ownership`, wired in
  `src/local/durable/maintenance.rs`); a failed background tick records a
  `MaintenanceTickFailed` event and `maintenance_tick_failures` counter.
- The async segment-row publisher drains at most `DRAIN_BATCH_IDS = 512` ids
  per cycle and snapshots them via a lenient chunked-hold pass
  (`selected_live_state_for_segment_ids`, 32 ids per lock hold), with the
  lock-free SQLite row write between cycles. This milestone-4 change is
  reviewer-approved and KEPT despite not moving the throughput gate: it also
  fixed a real pre-existing TOCTOU wedge (an id vanishing between the old
  `segment_exists` probe and the strict snapshot corrupt-errored the cycle
  and parked the publisher via `retry_parked` until the next enqueue).

Key tests (all in `src/local/tests/mod.rs`):
`durable_block_journal_publish_routes_segment_refs_through_carried_nodes`,
`durable_block_journal_publish_rejects_segment_ref_carrying_wrong_storage_node`,
`durable_block_journal_replay_rejects_segment_ref_carrying_wrong_storage_node`,
`durable_block_journal_publish_mark_failure_mid_batch_errors_waiters_and_replays`,
`maintenance_tick_sweep_detects_segment_in_multiple_node_catalogs`,
`always_on_maintenance_worker_surfaces_tick_failures`,
`storage_node_rejects_reference_for_segment_absent_from_catalog`,
`selected_live_state_snapshot_skips_vanished_ids_and_groups_by_node`,
`chunked_row_snapshot_bounds_foreground_catalog_waits`.

## 2. Gate status: unmet

The Phase 2 completion gate needs 64k c16/c32 throughput at 1.5x over
`stage6-filesystem-20260619-001` (830.56 / 901.24 MBps) with publish well
below sync. Measured (all `c4-standard-32-lssd`, 5 local NVMe, flushed,
rtt 0, zero errors):

Trip 1 (`infra/gcp-local-nvme-bench/results/phase2-publish-fastpath-20260723/`,
at `55dc00c`): c16 947.4/920.8 MBps (1.11-1.14x), c32 1095.3/1086.2
(1.21-1.22x); p99 improved vs stage6. Per-owner-batch split at c32: publish
148.7us vs sync 166.3us, of which publish-mark catalog lock WAIT 120.1us
(catalog bucket 123.7us is 97% wait; routing 2.2us, residual 4.3us).

Trip 2, same-instance interleaved A/B
(`infra/gcp-local-nvme-bench/results/phase2-confirm-ab-20260723/`, base
`55dc00c` vs the milestone-4 fix, zone us-east1-b): base c16 814.1/877.6,
c32 942.4/1011.2; fix c16 831.6/842.8, c32 972.4/1010.0 — fix/base ~1.01x,
both ~1.1x over stage6. Rep-1 splits (per-owner-batch means, us):

| side | conc | grp | sync | publish | mark | catalog | lock wait |
| --- | --- | --- | --- | --- | --- | --- | --- |
| base | 16 | 3.7 | 174.6 | 51.6 | 43.3 | 38.6 | 36.6 |
| base | 32 | 6.1 | 228.8 | 100.1 | 87.3 | 80.3 | 77.5 |
| fix | 16 | 3.7 | 163.7 | 58.4 | 49.7 | 44.9 | 42.7 |
| fix | 32 | 6.5 | 200.5 | 137.1 | 122.9 | 115.2 | 112.0 |

Instance-variance caveat: the same commit (`55dc00c`) showed c32 lock waits
of 120.1us (trip 1) and 77.5us (trip 2 base), and c32 throughput of
1086-1095 vs 942-1011 MBps across the two instances. Single-instance deltas
below ~1.2x on these quantities are within cross-instance spread; only the
same-instance interleaved A/B is decisive, which is why the refutation below
is trusted.

Reads (trip 2, reviewer's binding menu): c32 `block-read-4k` fix/base =
266.4/285.5 MBps = 0.93x — passes the >10% REVISE threshold but is recorded
as a WATCH ITEM (direction matches a predicted publisher-drain read-tail
interaction); `block-read-1m` flat or better; c16 read-4k 0.95x. The 4k
write guard is flat (fix 37346/39808 vs base 37967/40290 iops at c16/c32).

## 3. Diagnosis ledger

PROVEN (GCP-confirmed, both trips):
- The original 64k publish bottleneck was owner-node routing scans plus
  per-segment evidence ceremony. Eliminating them (2B/2C) cut the c32
  routing-plus-ceremony cost from ~310us/batch (stage6) to ~2-7us/batch
  (routing 2.2us, residual 4.3us in trip 1). Those buckets stay near zero in
  every run since; that work is done and must not be re-investigated.

REFUTED:
- "The async segment-row publisher's whole-backlog drain is the primary
  holder of the catalog mutexes." Bounding the drain to 512 ids/cycle with
  chunked 32-id holds (milestone 4) did NOT reduce publish-mark lock wait in
  the same-instance A/B (base 77.5us vs fix 112.0us at c32; the difference
  is within run noise, the direction is if anything worse). The fix is kept
  for the TOCTOU wedge and bounded-hold properties, not for throughput.

OPEN SUSPECTS, in priority order:
1. Strict catalog snapshots on persist paths: the three
   `selected_state_for_segment_ids` callers in
   `src/local/durable/coordinator.rs` (lines 2637, 2796, 2965 as of this
   handoff; enclosing fns `persist_one_append_stream_request_batch`,
   `persist_append_stream_publish_delta_physical`,
   `persist_prepared_append_publish_plans`). Each clones O(changed-set) fat
   `CatalogEntry` values (receipt + descriptor + placement) per node under a
   SINGLE lock hold via `selected_state_inner`. This is the reviewer's
   designated next suspect. Honest caveat: these are append-stream publish
   persist paths, and whether they actually run during a pure block 64k
   bench is UNPROVEN — establishing that is part of holder attribution, not
   an assumption to build on. Related same-shape candidates if they turn out
   to run on the flush cadence: `state_inner_for_persist` (FULL-catalog
   clone per node, full-persist path) and `state_for_segment_ids` (publisher
   missing-segments path) in `src/local/storage_node.rs`.
2. Aggregate short-hold convoy: at c32 the five catalogs absorb >20k
   acquisitions/s from segment staging (4 short holds per segment write:
   duplicate probe, reserve, begin, commit) plus lane marks. The mechanism
   test `chunked_row_snapshot_bounds_foreground_catalog_waits` demonstrated
   that parked waiters systematically lose to fast reacquirers on the unfair
   std mutex; a mark parked behind a busy writer stream can wait far longer
   than any single hold. This would explain why removing the publisher
   changed nothing. If confirmed, the fix direction is sharding the catalog
   map or narrowing the write-path critical sections, per the constraints in
   section 6.
3. Sync-side residual, NOT a lock issue: base sync grew 174.6 -> 228.8us
   from c16 to c32 in trip 2. Even at publish = 0, 64k throughput at that
   sync does not reach 1.5x. This residual belongs to Phase 3 (lane
   pipelining overlaps encode/write with the in-flight fsync) per the plan's
   budget clause; do not attack it under Phase 2.

THE MISSING MEASUREMENT — build this first: holder attribution. The existing
`block_journal_publish_mark_lock_wait_nanos` column shows who WAITS, not who
HOLDS. Nothing currently measures per-acquirer hold time on the catalog
mutex. Add per-call-site hold-time accounting (or scoped holder tags) on
`InMemoryLocalSegmentCatalog.inner` — a hold-duration histogram/sum keyed by
acquirer (staging probe/reserve/begin/commit, mark, row-publisher snapshot,
persist snapshot, sweep, read verify) — then re-profile 64k c16/c32 on NVMe.
The instrumentation rules from the plan apply: measure real durations, no
placeholder zeros, cheap enough not to distort, permanent columns only if
they serve later gates (a measurement note is otherwise fine).

## 4. Why this machine stopped

The development machine (macOS host, Docker VM) cannot reproduce the
contention regime, in either direction:

- On the VM disk (~1.4-1.6ms fsync), the journal sync throttles the lane so
  hard that catalog collisions almost never happen: measured publish-mark
  lock wait ~0.7us/batch at c32 vs 77-120us on GCP.
- On tmpfs (sync ~0.3us), SQLite is also memory-fast, the row-publisher
  queue never backs up, and the observed ~5us/batch waits are a different,
  smaller phenomenon (per-segment writer holds).

GCP sits between: ~170-230us journal sync, NVMe-fast data syncs, and
disk-speed SQLite. A Linux machine with real local NVMe (~100-200us fsync)
should land in the same regime and make the diagnose-fix-measure loop local
and cheap. That is the machine this handoff assumes.

## 5. Repro commands

Local 64k profile run (mirrors the GCP harness `run_toy_case` flags; on a
Linux host run directly per AGENTS.md, no container):

```
cargo build --release --bin loadbench
./target/release/loadbench \
  --provider durable --durability flushed --durable-io-backend filesystem \
  --workloads block-batch-4k-16ops --block-batch-ops 1 \
  --block-batch-bytes 65536 --block-batch-overlap random \
  --duration-ms 5000 --warmup-ms 1000 --concurrency 1,4,16,32 \
  --device-blocks 1048576 --shards 64 --storage-nodes 5 \
  --rtt-us 0 --delay-mode spin --payload-integrity verified \
  --target-data-log-mib 64 --data-log-file-sync-fanout 16 \
  --block-journal-chunk-mib 2 \
  --root <nvme>/bench/root \
  --storage-node-data-dirs <nvme>/bench/node-1,...,<nvme>/bench/node-5 \
  --matrix-csv out/matrix.csv --durable-profile-csv out/durable-profile.csv
```

Filter `durable-profile.csv` rows to `block_journal_flush_group_size > 0`
for per-owner-batch means. Ideal layout is six devices (journal + 5 nodes)
like GCP, but one NVMe holding everything should still reproduce the
contention as long as fsync is ~100-200us. Do NOT use tmpfs for gate
numbers (section 4). Prior result formats to compare against live in
`.abench/results/phase2a-publish-split-20260723/` through
`phase2d-catalog-contention-20260723/` and the two GCP dirs named in
section 2 (GCP results stay local to the machine that ran them, by repo
convention — they are not committed).

GCP (only if local NVMe is unavailable or for the final gate):
- Confirm A/B harness: `infra/gcp-local-nvme-bench/run_phase2_confirm_ab.sh`
  — env `RUN_ID` (default `phase2-confirm-ab-<timestamp>`), `BASE_REF`
  (default `55dc00c`; set to the new pre-fix commit when re-running),
  `FIX_REF` (default `worktree`), `AB_REPEATS=2`,
  `WRITE_CONCURRENCY`/`READ_CONCURRENCY`/`GUARD_CONCURRENCY` (default
  `16,32`). Runs base/fix interleaved per rep on one instance; results land
  in `infra/gcp-local-nvme-bench/results/${RUN_ID}/{base,fix}/<run-key>/`
  with the same CSV layout as the main harness.
- Full comparator harness: `infra/gcp-local-nvme-bench/run_block_vs_rbd.sh`
  (writes only; five sizes; optional Ceph side).
- Use GCP project `projectvoice-442316` EXCLUSIVELY. After every run verify
  teardown: `gcloud compute instances list --project projectvoice-442316`
  and `gcloud compute networks list --project projectvoice-442316` must show
  no resources with the run's name prefix (`toy-cow-p2ab-*`,
  `toy-cow-block-rbd-*`). The drivers clean up on EXIT, but verify anyway.

Full Rust gate (must pass before any report): `cargo fmt --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`,
`cargo doc --no-deps`, `cargo bench --bench regression -- --test`. On a
macOS host all cargo commands run in the dev container
(`docker compose up -d dev; docker compose exec dev cargo ...`).

## 6. Decisions that bind a resuming agent

- The evidence-free mark contract is FINAL. Do not reintroduce per-segment
  proofs; a real keyed scheme, if ever adopted, returns as a per-batch proof
  (decision note: Phase 2 ledger 2C row and
  `docs/cow-block-storage-design.md`).
- `DRAIN_BATCH_IDS = 512` stays unless NVMe evidence says otherwise
  (reviewer ruling; do not tune against VM-only artifacts).
- Any further lock restructuring must preserve exactly: the
  validate-all-then-apply mark atomicity (one bad id marks nothing), every
  corruption/conflict error semantic from Phase 2 (wrong/missing carried
  node => corrupt, no fallback scans), replay/recovery behavior, and the
  maintenance sweep's correctness.
- Read watch item: `block-read-4k` c32 was 0.93x fix/base on NVMe. A >10%
  NVMe read regression from any future change means REVISE before the phase
  closes.
- The plan's budget clause governs scope: if publish cost lands well below
  sync and the throughput gate is still short, stop attacking publish and
  hand the residual to Phase 3 (lane pipelining; includes the 174->229us
  sync growth) and Phase 7 (inline threshold). Do not start Phase 3 work
  under a Phase 2 flag.
- Testing gates are unchanged: full Rust gate per landed change, the
  8-scenario local loadbench A/B (block-write-4k-shard-lanes,
  block-write-4k-device-lanes, block-batch-4k-16ops, block-batch-4k-256ops,
  block-writeback-fsync-1m, block-writeback-prestaged-fsync-1m,
  block-read-4k, block-read-1m) with no >5-10% unexplained regression, and
  failure-injection coverage for lane/publish changes.

## 7. Suggested first moves (suggestions, not orders)

1. Build the holder-attribution instrumentation from section 3 and take a
   64k c16/c32 profile on local NVMe. Expect it to name the holder in one
   run; every hypothesis so far died from not having this measurement.
2. If the strict persist snapshots are confirmed as holders, bound/chunk
   them with the same pattern as the row publisher (short holds, lenient
   only where the contract already tolerates drift — the persist paths are
   strict by design, so chunking there must keep their corruption checks).
3. If the convoy hypothesis is confirmed instead, the smallest-correct-fix
   candidates are sharding each catalog's entry map by segment-id range or
   narrowing the staging-path critical sections; scheduling machinery stays
   out of scope.
4. Re-run the confirm A/B harness with `BASE_REF` set to the commit
   containing this handoff (55dc00c plus milestone 4) and `FIX_REF=worktree`
   to score the next fix, and close the phase per the gate or the budget
   clause.
