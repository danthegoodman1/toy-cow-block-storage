# Block Write Commit Boundary Plan

Status: implemented through Stage 3; Stage 4 deferred pending evidence
Project: `toy-cow-block-storage`

## Summary

The next block API phase should stop treating every block write as an immediate
visible metadata publish. The cleaner model is client-owned dirty-range
batching plus a server-owned atomic commit:

```text
client/block adapter:
  cache dirty ranges
  coalesce overwrites
  preserve barriers and ordering
  provide read-your-writes overlay when needed
  decide fsync, byte interval, time interval, or memory-pressure commit

storage:
  accept a batch of logical writes
  persist payloads
  path-copy touched shards
  publish one atomic commit group
  recover or GC failed private data
```

The primary API should be a one-shot batch commit:

```text
commit_block_batch(device_id, writes[], durability) -> DeviceCommit
```

This plan should be implemented in the narrow order that proves the core win
first:

1. baseline the current per-operation block write path;
2. add the one-shot local `commit_block_batch` model;
3. add durable one-shot batch persistence;
4. add loadbench/client writeback simulation and compare against the baseline.

The long-lived staged session API is explicitly deferred. Do not implement it
unless the one-shot batch benchmarks show that large or resumable writeback
windows still need a private durable-but-not-visible boundary.

If needed later, the optional session API would look like:

```text
open_block_batch_session(device_id) -> BlockBatchSession
stage_block_batch(session, writes[]) -> BlockBatchTicket
flush_block_batch_session(session) -> DurableBlockBatchMark
commit_block_batch_session(session, mark) -> DeviceCommit
abort_block_batch_session(session)
```

Do not make the storage core guess batching policy through timing-sensitive
server-side opportunistic batching as the default. The client or block adapter
already knows dirty ranges, barriers, fsync, close, memory pressure, and
writeback interval policy. The server should enforce correctness and publish an
atomic CoW metadata transition for the batch it receives.

This is a clean replacement for the old per-operation visible block write path.
No compatibility wrapper or legacy semantic shim.

## Public Semantics

### `commit_block_batch`

`commit_block_batch(device_id, writes[], durability)` is the normal fast path.
It accepts a complete dirty-range set from the client and publishes it
atomically.

Rules:

- The batch contains one or more aligned block writes.
- Overlapping writes inside the same batch are resolved by batch order before
  metadata path-copy.
- The commit touches only shards overlapped by the collapsed dirty ranges.
- Same-shard stale-root conflicts are still real conflicts.
- Same-device different-shard batches merge with unrelated shard commits.
- Multi-shard batches publish through one commit group.
- Success makes every collapsed write in the batch visible together.
- Failure must not expose any partial range or shard update.
- Durability controls whether payloads and metadata are merely acknowledged or
  flushed before returning.

This operation is enough for most writeback clients: they coalesce local dirty
ranges and call `commit_block_batch` on fsync, barrier, FUA, close, byte
threshold, time threshold, or memory pressure.

### Optional Long-Lived Batch Session

The session API is only for large streaming writeback windows that benefit from
durable-but-not-visible private data.

Semantics:

- `stage_block_batch` writes private payloads and records logical ranges, but
  does not update the visible device head.
- `flush_block_batch_session` persists private payloads and placement records
  through a durable high-water mark.
- `commit_block_batch_session` publishes staged ranges covered by the durable
  mark into visible block metadata.
- `abort_block_batch_session` releases private data for GC.
- Reads, forks, PITR, and normal metadata traversal observe only visible device
  heads.

The durable boundary belongs to the session. A flushed but uncommitted range is
resumable by the session token after restart, but invisible to normal readers.
Unrelated writers start from the visible device head, not another session's
private durable high-water.

## Client Responsibilities

The block client or block adapter owns batching policy:

- maintain the dirty-range map;
- collapse repeated overwrites before sending;
- split writes by block alignment and request size limits;
- preserve barriers and required ordering;
- maintain read-your-writes overlay for uncommitted dirty ranges when the caller
  requires it;
- choose commit triggers:
  - explicit fsync/flush/barrier;
  - FUA write;
  - close/unmount;
  - `16MiB` to `128MiB` dirty byte threshold;
  - time threshold;
  - memory pressure.

The client must not choose storage nodes, replica placement, segment IDs, or
metadata roots. Those remain storage-owned.

## Storage Responsibilities

The storage layer owns correctness:

- validate device identity and block alignment;
- allocate write intents and segment IDs;
- place payloads on storage nodes;
- verify payload integrity according to policy;
- enforce data-before-metadata ordering;
- collapse batch ranges into per-shard metadata edits;
- path-copy touched shard roots;
- publish one atomic commit group;
- mark referenced segments only after metadata publish succeeds;
- leave failed payload writes as private/orphan data that GC can reclaim;
- preserve PITR, fork, restore, and GC invariants.

The storage implementation may internally stage payloads before metadata
publish, but that staging is an implementation detail for one-shot
`commit_block_batch` unless the long-lived session API is explicitly used.

## Stage 0: Baseline Current Per-Op Publish

Run a clean baseline before changing semantics. Save raw output under ignored
`target/loadbench/block-commit-boundary-stage0/`.

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test

docker compose exec dev mkdir -p target/loadbench/block-commit-boundary-stage0
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --workloads block-write-4k-same-shard-contended,block-write-4k-same-shard-serialized,block-write-4k-shard-lanes,block-write-4k-device-lanes,block-write-1m-shard-lanes,block-write-1m-device-lanes \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,16,64,128 \
  --device-blocks 1048576 \
  --shards 64 \
  --storage-nodes 1 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --metadata-profile-csv target/loadbench/block-commit-boundary-stage0/metadata-n1-verified.csv \
  --block-write-profile-csv target/loadbench/block-commit-boundary-stage0/block-write-n1-verified.csv
```

Repeat the same matrix with:

```text
--storage-nodes 4 --payload-integrity verified
--storage-nodes 1 --payload-integrity unchecked
--storage-nodes 4 --payload-integrity unchecked
```

The baseline is only for comparison. Do not optimize the old shape unless a
small cleanup is required to make the replacement safe.

## Stage 1: Batch Commit API And Local Model

Implement `commit_block_batch` through the local deterministic path first.

Deliverables:

- public batch request/response types;
- `BlockWriteBatch` or equivalent internal command shape;
- validation for aligned ranges, empty batches, overflow, and maximum batch
  bytes;
- batch range collapse for overlapping writes in batch order;
- per-shard split from collapsed dirty ranges;
- one commit group for all touched shards;
- no public exposure of shard IDs, segment IDs, or placement decisions.

Correctness tests:

- one batch with multiple writes becomes visible atomically;
- overlapping writes in one batch resolve by request order;
- multi-shard batch writes publish all touched shards or none;
- same-shard stale-root conflict fails cleanly;
- same-device different-shard batch commits merge with unrelated commits;
- failed segment write or failed metadata publish exposes no partial update;
- reads, forks, restores, and PITR observe only committed batch results.

Benchmark after this stage under
`target/loadbench/block-commit-boundary-stage1/`.

Expected result: client-sized batches should reduce per-write metadata
amplification even before durable delta optimization is perfect.

## Stage 2: Durable One-Shot Batch Commit

Make durable `commit_block_batch` persist only the batch's payloads and metadata
delta.

Ordering:

1. append batch segment payloads to storage-node data logs;
2. sync data logs if durability requires it;
3. persist placement/catalog rows for those segment IDs;
4. path-copy touched metadata shards;
5. persist changed metadata nodes, touched shard heads, commit group, and commit
   records;
6. mark segment references after metadata publish succeeds;
7. return `DeviceCommit`.

Rules:

- no full device/keyspace state image persistence for a batch commit;
- payload bytes are not reappended during metadata publish;
- changed metadata rows scale with touched shards and resulting path-copy nodes;
- physical sync groups are bounded, default cap `32MiB`;
- failed publish leaves batch payloads unreferenced and reclaimable.

Correctness tests:

- durable batch commit survives reopen;
- acknowledged-but-unflushed batch behavior matches the durability contract;
- failed data-log sync publishes no metadata;
- failed metadata row publish exposes no partial block contents;
- GC collects failed batch payloads and retains committed payloads;
- PITR replay restores contents across multiple batch commits.

Benchmark after this stage under
`target/loadbench/block-commit-boundary-stage2/`.

Success signal:

- batch commit p99 is bounded by sync group caps and metadata delta size;
- large batches approach storage-node sequential-log throughput;
- 4KiB-heavy batches reduce tail by amortizing metadata publish cost.

## Stage 3: Loadbench And Client Writeback Simulation

Update `loadbench` so the batch model is measured directly.

Add workloads:

- `block-batch-4k-16ops`
- `block-batch-4k-256ops`
- `block-batch-4k-4096ops`
- `block-batch-1m-16ops`
- `block-batch-1m-128ops`
- `block-batch-overwrite-collapse`
- `block-batch-fsync-interval`
- alias: `block-batch`

If the deferred Stage 4 session API lands later, add:

- `block-session-stage-1m`
- `block-session-flush-1m`
- `block-session-commit-1m`
- `block-session-flush-commit-1m`
- alias: `block-session`

Add knobs:

- `--block-batch-ops`;
- `--block-batch-bytes`;
- `--block-batch-overlap sequential|random|overwrite-hotset`;
- `--block-batch-fsync-mib`, default `128`;
- `--block-batch-fsync-ms`;
- `--block-client-overlay on|off` for future adapter read-your-writes tests.

Keep old per-op block write workloads only as baseline controls until the
replacement lands. Once the public contract changes, remove old-path benchmark
aliases that no longer represent the real API.

Benchmark after this stage under
`target/loadbench/block-commit-boundary-stage3/`.

Compare:

- old per-op block write baseline from Stage 0;
- one-shot `commit_block_batch` at multiple batch sizes;
- overwrite-collapse batches;
- fsync-interval simulation;
- native file-lane write control;
- native stream ingest/flush/publish control.

## Stage 4: Deferred Optional Long-Lived Batch Session

Do not implement this stage in the initial commit-boundary pass. Revisit it
only if Stage 3 shows that one-shot batches are not enough for large writeback
windows or resumability.

Deliverables:

- session token and durable private state;
- `stage_block_batch`;
- `flush_block_batch_session`;
- `commit_block_batch_session`;
- `abort_block_batch_session`;
- GC roots for flushed private-but-not-visible data.

Correctness tests:

- staged writes are invisible to normal reads;
- flushed staged writes are resumable by token after reopen;
- commit makes exactly marked ranges visible;
- abort/fence releases unpublished private data for GC;
- same-shard external commits can invalidate stale session commits;
- different-shard external commits do not invalidate untouched staged shards.

Benchmark after this stage under
`target/loadbench/block-commit-boundary-stage4/`.

Success signal:

- stage/write ingest approaches memory/log append speed;
- flush throughput approaches sequential data-log throughput;
- commit cost scales with touched shards and collapsed ranges, not staged call
  count.

## Required Profile Fields

Add profile rows that make the new boundary obvious:

- workload, provider, durability, RTT, concurrency, op size;
- batch operation count;
- collapsed range count;
- private bytes written;
- visible bytes committed;
- payload copy time;
- payload integrity time;
- data-log append time;
- data-log sync time;
- placement/catalog persist time;
- metadata path-copy time;
- metadata publish lock wait;
- metadata row-sync time;
- SQLite commit time;
- touched shard count;
- changed metadata node count;
- physical sync group bytes;
- commit sequence allocation time.

## Interpretation Rules

- If batch commit improves sharply with batch size, fixed per-publish overhead
  was the old ceiling.
- If batch ingest/write is fast but commit is slow, focus on metadata path-copy
  and publish delta size.
- If commit cost scales with original write count instead of collapsed range
  count, fix range collapse before deeper storage changes.
- If same-device different-shard batches are no faster than same-shard batches,
  recheck shard-head publish and commit-group routing.
- If unchecked integrity materially changes batch throughput or p99, checksum
  policy is still hot-path relevant.
- If one-shot batches are good enough, skip long-lived sessions for now.

## Final Gate

Before committing each stage:

```sh
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test
```

After Stage 3, produce a 200us RTT comparison table:

- old per-op block write baseline;
- block batch 4KiB at 16/256/4096 operations;
- block batch 1MiB at 16/128 operations;
- overwrite-collapse batch;
- fsync-interval simulation;
- native file-lane write control;
- native stream append/flush/publish control.

Report IOPS, MB/s, p50/p90/p99/max, errors, profile phase p50/p90/p99, and
whether the remaining ceiling is client batching policy, data ingest, durable
flush, visible commit, metadata publish, or payload integrity.

Stage 4 should remain out of scope unless that comparison shows a concrete gap
that one-shot batches cannot address cleanly.

## Stage 3 Evidence

Stage 3 raw output is saved under
`target/loadbench/block-commit-boundary-stage3/`.

The implemented `block-batch` suite measures one-shot `commit_block_batch`
directly and records per-batch profile rows with requested operation count,
collapsed range count, requested bytes, committed bytes, and commit-call
latency. Durable runs pair those rows with `--durable-profile-csv` so data-log
sync, node-catalog publish, root SQLite publish, persist-lock wait, and touched
metadata counts remain visible.

Initial verified durable runs at 200us modeled RTT show that one-shot batches do
remove the old per-operation metadata publish amplification:

- 4KiB one-op block writes remain single-digit MiB/s because every call still
  publishes and flushes one tiny visible update.
- 4KiB batches improve with batch size: 16-op batches reach tens to low
  hundreds of MiB/s, 256-op batches reach hundreds of MiB/s to roughly 1GiB/s,
  and 4096-op batches reach roughly 1.0-1.7GiB/s depending concurrency.
- 16MiB one-shot batches land in the same range whether built from 4096 4KiB
  writes or 16 1MiB writes, which confirms range collapse is doing the intended
  simplification before metadata path-copy.
- 128MiB one-shot batches improve total throughput but have high per-call
  latency and memory pressure, so they are useful as a diagnostic upper bound,
  not as a default client writeback window.
- At high concurrency, durable persist-lock wait and data-log sync grouping are
  now the visible tail sources. Stage 4 block sessions are still deferred; the
  next decision should come from whether the client wants smaller fsync latency
  or larger writeback throughput.

## Non-Goals

- No POSIX work in this phase.
- No compatibility wrapper around the old per-op block publish path.
- No fake/null providers for performance claims.
- No durable format compatibility shim.
- No client-side replica or storage-node placement.
- No weakening of copy-on-write, PITR, data-before-metadata ordering, or
  same-shard conflict behavior.
- No requirement that normal readers observe another client's uncommitted dirty
  ranges.
