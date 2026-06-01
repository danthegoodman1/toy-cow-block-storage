# Block Fsync Tail Stage 2 Plan

Status: implemented, measured

## Summary

Stage 1 data-log prestaging proved the intended split: timed prestaged block
fsync no longer writes payload records. The remaining tail is not checksum,
data-log encode/write, or SQLite row work. It is mostly serialized persist
handoff plus physical data-log file sync.

This phase should reduce block writeback fsync tail latency by making the
existing durability boundary coalesce concurrent waiters better and by syncing
storage-node data logs concurrently across nodes, while preserving simple
replay semantics and the current public block/native APIs.

## Current Evidence

Final Stage 1 artifacts:

- `target/loadbench/data-log-prestage-stage1-final2/`
- `target/loadbench/data-log-prestage-stage1-final2-1node/`

Important rows at `200us` RTT, durable `ack-flush:1`, verified payloads:

- `block-writeback-prestaged-fsync-1m`, 4 storage nodes, c16:
  `827 MB/s`, p50 `13.75ms`, p99 `19.75ms`, errors `0`.
- `block-writeback-prestaged-fsync-1m`, 1 storage node, c16:
  `850 MB/s`, p50 `13.36ms`, p99 `20.68ms`, errors `0`.
- Profile p99 for 4-node `1m` c16:
  - `persist_lock_wait`: about `11.2ms`
  - `data_log_file_sync`: about `5.0ms`
  - `node_catalog_publish`: about `1.8ms`
  - `root_sqlite_commit`: under `0.5ms`
  - `data_log_write`: `0`
  - `data_log_flush_write_bytes`: `0`

Interpretation:

- Prestaging works functionally.
- Fsync p99 is still above the target.
- Adding storage nodes did not materially lower the 1MiB c16 tail, so the
  dominant issue is the serialized flush/persist shape, not raw per-node write
  bandwidth.
- The next work should not increase batch size to hide latency. It should reduce
  lock wait and sync/publish latency directly.

## Goals

- Keep public block/native APIs unchanged.
- Keep durable block delta format unchanged unless a correctness test proves the
  format cannot support the simpler fast path.
- Preserve data-before-metadata ordering:
  data-log records are durable before catalog placement rows and block delta
  rows reference them.
- Keep per-storage-node append order deterministic.
- Avoid adding background threads or hidden durability policy in this phase.
- Make benchmark/profile output explain whether remaining p99 is lock handoff,
  storage-node fsync, node catalog publish, or root SQLite publish.

## Stage 0: Baseline And Profile Cleanup

- Treat `target/loadbench/data-log-prestage-stage1-final2/` as the baseline.
- Add any missing profile fields needed to distinguish:
  - persist coordinator wait before becoming runner
  - wait on an in-flight persist that satisfied this caller
  - wait on an in-flight persist that did not satisfy this caller
  - physical persist lock wait
  - selected delta count and selected bytes
  - number of waiters satisfied by one physical persist
  - data-log files synced per storage node
  - per-node sync p50/p90/p99/max inside one physical persist
- Keep profiling disabled by default.

Exit gate:

- Normal loadbench rows remain `errors=0`.
- Profiles make it obvious whether waiters are being satisfied by shared
  persists or serialized one physical persist per caller.

## Stage 1: Coalesce Ready Fsync Waiters

Make the block delta persist coordinator intentionally batch ready waiters
before starting physical persistence.

Implementation shape:

- Keep one in-flight physical block persist at a time for a device/provider
  instance.
- When a caller requests `persist_block_deltas_until(required_seq)`, publish its
  requested sequence into the coordinator first.
- The runner should select the highest currently requested contiguous block
  delta sequence immediately before physical persistence begins.
- If more waiters arrive while a persist is in flight and their required
  sequence is covered by the in-flight target, they should wake and return
  without starting another persist.
- If waiters arrive with a higher sequence while a persist is in flight, exactly
  one follower should run the next persist after the current one completes.
- Do not add timer-based sleeps or latency windows. Coalescing should come from
  the handoff/coordinator shape, not artificial delay.
- Preserve failure behavior: failed persists wake waiters; waiters whose
  required sequence was not made durable return the error; later calls can
  retry.

Correctness tests:

- Many concurrent `flush_device` calls over already-prestaged writes are
  satisfied by fewer physical persists than callers.
- A follower persist runs when a later waiter requests a sequence higher than
  the in-flight target.
- A failed physical persist wakes all waiters and does not mark unsatisfied
  sequences durable.
- A later successful flush persists and reopens correctly.
- Same-shard conflicts, fork/PITR, checkpoint, GC, and compaction behavior are
  unchanged.

Benchmark success criteria:

- `block-writeback-prestaged-fsync-1m`, c16, p99 drops materially from
  `~19.75ms`.
- Target: p99 under `10ms`; stretch target: under `8ms`.
- Throughput does not regress by more than `5%` unless the profile explains an
  intentional latency/throughput tradeoff and the latency target is met.

## Stage 2: Parallelize Storage-Node Sync

If Stage 1 profiles still show `data_log_file_sync` as a meaningful tail,
parallelize physical data-log file sync across storage nodes.

Implementation shape:

- Keep append order per storage node.
- Sync different storage nodes concurrently.
- Do not reorder records within one node's active log.
- Keep catalog publish after every required data-log sync has succeeded.
- If any node sync fails, do not publish catalog placement rows or block delta
  rows for that persist.
- Prefer a small scoped worker join inside the physical persist over permanent
  background workers.

Correctness tests:

- Multi-node prestaged flush persists only after all required node syncs
  succeed.
- Injected sync failure on one node exposes no partial replay after reopen.
- Retry after a failed multi-node sync can persist and reopen correctly.
- Per-node log order remains stable across replay.

Benchmark success criteria:

- Multi-node prestaged fsync improves over 1-node when dirty data spans multiple
  storage nodes.
- `data_log_file_sync` p99 drops in multi-node profiles.
- Metadata publish and root SQLite timings do not regress materially.

## Stage 3: Collapse Metadata Publish Work Only If Needed

If Stage 1 and Stage 2 leave p99 dominated by catalog/root metadata publish,
then reduce metadata work. Do not start here unless profiles clearly point here.

Candidate simplifications:

- Batch storage-node catalog placement row writes by node and log.
- Avoid touching catalog tables for segment IDs already known referenced by a
  previous durable delta.
- Keep block delta rows compact and append-only.

Do not:

- Change SQLite durability pragmas.
- Add a second durable format.
- Add compatibility shims.
- Hide latency by increasing writeback batch size.

## Benchmark Protocol

Run after each stage and save raw CSV under ignored
`target/loadbench/block-fsync-tail-stage2-<stage>/`.

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
  --workloads block-writeback,block-writeback-prestaged,block-read-4k,block-write-1m-shard-lanes,block-batch-4k-256ops \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --device-blocks 1048576 \
  --shards 64 \
  --storage-nodes 4 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --durable-profile-csv target/loadbench/block-fsync-tail-stage2-<stage>/profile.csv \
  --block-batch-profile-csv target/loadbench/block-fsync-tail-stage2-<stage>/block-batch.csv

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --workloads block-writeback-prestaged \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --device-blocks 1048576 \
  --shards 64 \
  --storage-nodes 1 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --durable-profile-csv target/loadbench/block-fsync-tail-stage2-<stage>-1node/profile.csv \
  --block-batch-profile-csv target/loadbench/block-fsync-tail-stage2-<stage>-1node/block-batch.csv

docker compose down
```

## Reporting

For every stage, report:

- Matrix rows for cold and prestaged writeback at `1m`, `2m`, `4m`, and `16m`.
- c16 phase distributions for:
  - persist coordinator wait / lock wait
  - data-log write
  - data-log file sync
  - node catalog publish
  - root SQLite row-sync and commit
- Whether fsync wrote payload records; expected `data_log_write = 0` and
  `data_log_flush_write_bytes = 0` for prestaged rows.
- Whether 4 storage nodes beat 1 storage node and why.
- Whether the stage met, missed, or invalidated the success criteria.

## Exit Criteria

This phase is complete when one of these is true:

- `block-writeback-prestaged-fsync-1m`, durable, 200us RTT, c16 reaches p99
  under `10ms` with no correctness regression.
- Profiles prove the remaining p99 is physical file sync on the host/container
  and not reducible without changing the durability model.
- Profiles prove the remaining p99 is metadata publish, in which case write the
  next plan before changing metadata architecture.

## Stage 2 Result

Artifacts:

- `target/loadbench/block-fsync-tail-stage2-final/`
- `target/loadbench/block-fsync-tail-stage2-final-1node/`

Gate:

- `cargo fmt --check`: pass
- `cargo clippy --all-targets --all-features -- -D warnings`: pass
- `cargo test`: pass
- `cargo doc --no-deps`: pass
- `cargo bench --bench regression -- --test`: pass

Key result at `200us` RTT, durable `ack-flush:1`, verified payloads:

- Baseline `block-writeback-prestaged-fsync-1m`, 4 storage nodes, c16:
  `827 MB/s`, p50 `13.75ms`, p99 `19.75ms`.
- Stage 2 final `block-writeback-prestaged-fsync-1m`, 4 storage nodes, c16:
  `1673 MB/s`, p50 `7.66ms`, p99 `12.02ms`, errors `0`.

The phase materially improved the target path, but did not reach the sub-10ms
stretch target. Profiles show the timed prestaged fsync remains sync-only:
`data_log_write_nanos = 0` and `data_log_flush_write_bytes = 0` for all final
profile rows.

Final 4-node c16 phase profile for `block-writeback-prestaged-fsync-1m`:

- `flush_device` p99: `11.76ms`
- physical persist total p99: `6.83ms`
- `persist_lock_wait` p99: `0ms`
- prestage wait p99: `0.41ms`
- local snapshot p99: `0.37ms`
- data-log file sync p99: `4.50ms`
- node catalog publish p99: `1.29ms`
- root SQLite commit p99: `0.84ms`

Interpretation:

- The old `persist_lock` tail is gone.
- Timed fsync no longer writes payload records.
- Data-log sync is already parallel across storage-node files; in the final
  4-node c16 profile, file-sync wall p99 was `4.50ms` while summed per-file
  sync p99 was `20.81ms`.
- The remaining user-visible p99 is mostly device-level `flush_device` queueing
  over single in-flight physical persists plus host/container file sync, not
  checksum, payload write, or SQLite row work.

The next change should not add timers or larger batches to hide latency. To
push below `10ms`, use a new design step: either change the durability boundary
to a commit-specific fsync path, add explicit background/pre-sync semantics, or
pipeline physical data-log sync separately from ordered metadata publish.
