# Block Delta Journal Architecture Plan

Status: implemented through Stage 1 replayable deltas

## Summary

Move the durable block-device write path from "publish full CoW metadata on
every flush boundary" to a journal/checkpoint model:

- **Write path:** block writes produce ordered durable delta records.
- **Flush/barrier path:** `flush_device` makes prior deltas durable and
  replayable, then returns.
- **Checkpoint path:** background or explicit maintenance folds deltas into
  immutable CoW shard roots.
- **Read path:** reads consult the newest dirty/cache state, then the durable
  delta index, then checkpointed CoW roots.

Current implementation note: flushed block writes persist durable segment
payloads and compact `block_delta_commits` rows. Reopen loads checkpointed
per-shard heads and replays retained delta rows into the in-memory CoW metadata
view. Explicit checkpoint/full persist folds those deltas into row-native shard
heads and prunes covered delta rows. Durable metadata GC folds outstanding block
delta rows before sweeping so delta-only payload references cannot be reclaimed
while replay still depends on them. The implementation still uses the existing
segment data-log sync path, so benchmarks should be read as a measurement of
whether removing root metadata export was enough or whether physical data-log
sync remains dominant.

This is the production-shaped block model for DB-on-filesystem-on-block. A
filesystem already owns page-cache writeback and fsync policy. The block layer
must make submitted writes durable in order without forcing every WAL fsync
through a full metadata-tree publish.

## Current Evidence

The current durable block fsync path is correct but not the right long-term
shape for low-latency filesystem fsync:

- Direct 4KiB block writes at `200us` RTT are millisecond-class because every
  operation pays fixed durable write machinery.
- Batched 1MiB block fsync windows reach about 1.1 GB/s at c16, with p99 around
  17-18ms.
- 4MiB windows reach about 1.6-1.7 GB/s, but p99 rises to roughly 45-50ms.
- Durable profiles show the tail is mostly data-log append/sync work, not
  SQLite row-sync/commit or metadata lock wait.

This means local filesystem writeback is necessary but insufficient. The block
flush boundary itself should become cheaper.

## Target Model

Durable state for a block device becomes:

```text
checkpoint root at sequence C
ordered block delta records C+1..N
```

Each delta record contains:

- device ID and generation;
- shard ID or touched shard list;
- monotonically ordered device delta sequence;
- logical block ranges;
- durable segment/run references;
- payload integrity mode and verification material;
- optional barrier/group ID for flush ordering.

Reads at latest state resolve in this order:

```text
volatile in-process dirty state
durable delta index newest-to-oldest
checkpointed CoW shard roots
zero fill for sparse ranges
```

Fork and PITR record:

```text
checkpoint root IDs + delta high-water sequence
```

Checkpointing folds:

```text
checkpoint roots + deltas through sequence N -> new checkpoint roots at N
```

## Public Semantics

Keep the public block API stable:

- `write_at` and `commit_batch` make writes visible to later reads in the live
  provider.
- `flush_device` makes all acknowledged writes through the returned sequence
  durable and replayable after restart.
- A successful flush does not require the deltas to be folded into CoW shard
  roots before returning.
- Fork/PITR observe checkpoint roots plus retained delta high-water.

Document this distinction explicitly: durable/replayable block deltas are a
valid stable-storage boundary; checkpointed CoW roots are the compact indexed
representation.

## Stage 0: Baseline

Run and save the current matrix under ignored
`target/loadbench/block-delta-journal-stage0/`.

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --workloads block-read-4k,block-write-4k-shard-lanes,block-write-1m-shard-lanes,block-batch-4k-256ops,block-writeback \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --device-blocks 1048576 \
  --shards 64 \
  --storage-nodes 4 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --durable-profile-csv target/loadbench/block-delta-journal-stage0/profile.csv \
  --block-batch-profile-csv target/loadbench/block-delta-journal-stage0/block-batch.csv

docker compose down
```

Record:

- direct 4KiB write latency;
- direct 1MiB write latency;
- 1/2/4/16MiB fsync-window latency;
- warm committed read latency;
- durable profile phase distributions.

## Stage 1: Durable Delta Log

Add a block delta log below the public block API.

Requirements:

- Delta records are append-only and ordered per device.
- Segment bytes are durable before any delta record references them.
- A returned flush sequence is replayable after restart even when no CoW
  checkpoint has folded that delta yet.
- Failed delta append/sync cannot expose partial writes after restart.
- Delta records are scoped by device generation so stale device/fork state
  cannot be replayed into the wrong lineage.
- No old durable format compatibility path or dual live representation.

Implementation notes:

- Keep the deterministic metadata core small: model delta append, replay, read
  overlay, checkpoint fold, and GC roots explicitly.
- Start with one ordered delta chain per device, internally partitioned by shard
  in the replay index if needed.
- Avoid clever LSM levels in v1. One durable delta index plus one checkpoint
  root should be enough to prove the model.

## Stage 2: Read And Replay Index

Build an in-memory latest-delta index on open/replay.

Requirements:

- Reopen reconstructs the same latest block contents from checkpoint roots plus
  deltas.
- Reads check the delta index before checkpointed roots.
- Read cost is bounded by the number of overlapping delta entries, not by total
  device history.
- Index rebuild time is measured and bounded by checkpoint policy.

Tests:

- overwrite same block many times and read the newest value;
- read ranges spanning delta-backed, checkpoint-backed, and sparse blocks;
- corrupt, torn, duplicate, and out-of-order delta records;
- replay after crash before delta sync, after delta sync, and during checkpoint.

## Stage 3: Checkpoint Fold

Fold delta records into CoW shard roots through an explicit checkpoint operation
or maintenance tick.

Requirements:

- Checkpoint creates immutable shard roots at a delta high-water.
- Checkpoint does not block unrelated acknowledged writes longer than necessary.
- Fork remains O(1)-ish: copy checkpoint root references plus delta high-water.
- PITR restore selects checkpoint roots plus retained deltas through the restore
  high-water.
- Reads after checkpoint return the same bytes as before checkpoint.

## Stage 4: GC And Retention

Extend GC roots to include retained delta ranges.

Requirements:

- Delta-referenced segment payloads are retained until checkpointed and no PITR
  or fork needs the delta range.
- Checkpointed data is protected by normal CoW roots.
- Expired deltas release their segment references.
- Aborted or failed delta records never become GC roots.

Tests:

- GC keeps uncheckpointed flushed writes visible after reopen.
- GC reclaims delta-only payloads after checkpoint and retention expiry.
- Fork/PITR retains delta ranges independently of the live head.

## Stage 5: Benchmark And Decision

Run the same matrix under
`target/loadbench/block-delta-journal-stage1/` and compare against Stage 0.

Success criteria at `200us` modeled RTT:

- `block-writeback-fsync-1m`, c16: p99 below 5ms, throughput no worse than
  Stage 0 by more than 5%.
- `block-writeback-fsync-2m`, c16: p99 below 10ms.
- Direct 4KiB write p99 improves materially or remains clearly documented as a
  bad sync-per-op pattern.
- Warm committed read p99 stays below 1ms after delta index warmup.
- Reopen/replay time remains bounded and reported.

Exit gates:

- If delta append/sync still dominates, inspect storage-node log sync shape
  before touching metadata.
- If read cost grows with delta history, checkpoint sooner or add a simple
  per-shard delta index before adding more durable machinery.
- If checkpointing introduces cross-shard contention, fix the checkpoint fold
  path before applying the same model to native metadata.

## Assumptions

- The block API stays stable.
- Filesystem/page-cache writeback remains above the block API.
- Flush/barrier durability means replayable deltas, not necessarily folded CoW
  roots.
- CoW shard roots remain the compact long-term representation for forks, PITR,
  and GC.
- No compatibility shims or legacy durable-format support are kept in this toy
  phase.
