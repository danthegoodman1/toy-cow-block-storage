# Block Writeback Fsync Latency Plan

Status: implemented through Stage 1; storage fsync narrowing remains the next
optional phase

## Summary

The block storage API should be judged at the boundary it owns: when a caller
submits a collapsed write batch and asks the storage system to make it durable
and visible. A normal filesystem or mount adapter owns the dirty page-cache
behavior above that boundary: small `write(4KiB)` calls are buffered locally,
dirty reads are served locally, and `fsync` turns dirty ranges into a storage
commit.

This plan separates those responsibilities:

- add a small client-side writeback reference harness to prove the expected
  mount behavior and benchmark read-your-writes latency without changing core
  block semantics;
- focus storage-layer work on making `commit_block_batch` / `flush_device`
  lower latency for realistic 1-4MiB fsync windows;
- keep direct block write semantics unchanged;
- avoid moving page-cache semantics into the storage core.

## Current Evidence

Last measured durable matrix, `200us` modeled RTT, `ack-flush:1`, verified
payloads, four storage nodes:

- `block-write-4k-shard-lanes`, c16: about 1,889 IOPS / 7.7 MB/s, p50 8.2ms,
  p99 11.2ms.
- `block-write-1m-shard-lanes`, c16: about 1.08 GB/s, p50 15.4ms, p99 18.4ms.
- `block-batch-4k-256ops`, c16: about 1.05 GB/s, p50 15.8ms, p99 19.0ms.
- `native-write-1m`, c16: about 1.25 GB/s, p50 13.1ms, p99 35.4ms.
- large block batches reach 1.5 GB/s-class throughput, but p50/p99 become
  170-200ms-class stalls.

The practical interpretation is:

- 4KiB per-operation block write latency is dominated by fixed transaction
  machinery.
- 1MiB block writes already amortize the fixed cost reasonably well.
- Very large dirty windows improve amortized throughput but create unacceptable
  flush stalls.
- The durable metadata SQLite commit is not the primary remaining large-batch
  tail; data-log append/sync and physical persist grouping dominate.

## Implemented Checkpoint

Raw results live under:

- `target/loadbench/block-writeback-stage0/`
- `target/loadbench/block-writeback-stage1/`

Stage 1 added the `block-writeback` loadbench suite. It pre-fills a
filesystem-like dirty 4KiB window, then times the remote storage durability
boundary with `commit_block_batch` plus `flush_device`. It intentionally does
not report local buffered write/read IOPS, because those are page-cache or mount
behavior rather than block-device behavior.

At `200us` modeled remote RTT, durable provider, `ack-flush:1`, verified
payloads, four storage nodes:

- `block-writeback-fsync-1m`, c16: about 1.12 GB/s, p50 14.7ms, p99 17.1ms,
  errors=0.
- `block-writeback-fsync-2m`, c16: about 1.44 GB/s, p50 22.4ms, p99 30.6ms,
  errors=0.
- `block-writeback-fsync-4m`, c16: about 1.69 GB/s, p50 38.0ms, p99 45.5ms,
  errors=0.
- `block-writeback-fsync-16m`, c16: about 1.36 GB/s, p50 187ms, p99 200ms,
  errors=0.

The latency-first recommendation is a 1MiB dirty fsync window. A 2MiB window is
reasonable if higher throughput matters more than a 30ms-class c16 p99. A 4MiB
window is a throughput mode. 16MiB is only a stress/control shape.

Durable profiles for writeback fsync show near-zero persist lock wait and
sub-millisecond SQLite row-sync/commit for the hot rows. The dominant c16 tail
is `data_log_append_sync_nanos`, especially `data_log_file_sync_nanos`, with
physical persists coalescing multiple concurrent fsync requests into larger
sync groups. If we continue this phase, the next target is the storage-node
data-log append/sync grouping, not block metadata row layout.

## Hypotheses

1. **Small block writes are a client/mount writeback concern.**
   A local mount should not call the storage service for every dirty 4KiB page.
   It should buffer dirty ranges, serve read-your-writes, and call the storage
   API at `fsync`, writeback pressure, or a configured dirty interval.

2. **4KiB read latency needs a hot read path.**
   Dirty reads should be satisfied by the client/mount cache. Warm committed
   reads should avoid unnecessary metadata-service work when the block handle
   already has a valid mapping/cache view.

3. **`fsync` latency should be tuned around 1-4MiB dirty windows first.**
   The current 16MiB+ windows are useful throughput diagnostics, but they are too
   large as a latency-first default.

4. **Block `fsync` still has avoidable tail.**
   Once writes are collapsed, remaining latency should be data-log append/sync
   plus a small metadata publish. If the profile shows wider state export,
   redundant segment lifecycle work, or serialized per-node writes, simplify that
   path.

## Boundary Model

The lower block API remains an explicit commit API:

```text
commit_block_batch(collapsed ranges)
  -> publish visible block metadata
  -> optionally flush/persist through returned commit
```

A mount-like client can layer writeback above that API:

```text
write(4KiB)
  -> client/mount validates and copies into bounded dirty overlay
  -> return local read-your-writes success

read(4KiB)
  -> client/mount dirty overlay lookup
  -> committed block read only when not satisfied locally

fsync / dirty threshold
  -> client/mount collapses dirty ranges
  -> commit_block_batch(collapsed ranges)
  -> flush_device through returned commit
  -> clear published dirty overlay
```

The writeback layer is optional reference client code and benchmark machinery.
It must not become a second storage format or a compatibility shim. The storage
core remains responsible for correctness after commit, while the client/mount
owns pre-fsync dirty memory semantics.

Layer responsibilities are explicit:

- **Direct block API:** a successful direct durable write or batch commit is
  remote-visible according to its durability mode. There is no hidden dirty
  page cache in the storage core.
- **Client/mount writeback:** a successful buffered write is only local
  read-your-writes state until `fsync`. Process crash before `fsync` loses the
  dirty overlay.
- **Fsync/commit:** `fsync` collapses dirty ranges, sends one batch through the
  block API, persists through the returned commit, then clears the committed
  dirty overlay.
- **Placement:** a batch may span shards and storage nodes. The client supplies
  logical block ranges only; storage placement and fanout remain below the
  public API.
- **Benchmark RTT:** modeled network RTT applies only to operations that cross
  the storage boundary. Dirty-overlay writes and reads measure local
  client/mount behavior; fsync, cache misses, and direct block operations
  measure remote behavior.

## Stage 0: Baseline And Missing Measurements

Record a clean baseline before changing behavior.

Add or confirm loadbench coverage for:

- `block-read-4k` cold committed read;
- `block-read-4k` warm/cache read, if current workload cannot distinguish it;
- `block-write-4k-shard-lanes`;
- `block-write-1m-shard-lanes`;
- `block-batch-4k-256ops`;
- `block-batch-fsync-interval` with `--block-batch-fsync-mib 1,2,4,8,16`;
- native controls: `native-write-4k-file-lanes`, `native-write-1m`.

Required profile dimensions:

- committed mapping lookup time;
- storage-node read/write time;
- payload integrity time;
- tree path-copy time;
- metadata publish time;
- mark-referenced time;
- durable persist total;
- data-log append/sync;
- SQLite row-sync/commit;
- operation errors/conflicts.

Benchmark:

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
  --workloads block-read-4k,block-write-4k-shard-lanes,block-write-1m-shard-lanes,block-batch-4k-256ops,block-batch-fsync-interval,native-write-4k-file-lanes,native-write-1m \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16 \
  --device-blocks 1048576 \
  --shards 64 \
  --storage-nodes 4 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --block-batch-fsync-mib 1 \
  --durable-profile-csv target/loadbench/block-writeback-stage0/profile.csv \
  --block-batch-profile-csv target/loadbench/block-writeback-stage0/block-batch.csv

docker compose down
```

Repeat `block-batch-fsync-interval` for `--block-batch-fsync-mib 2,4,8,16`.
Save raw output under ignored `target/loadbench/block-writeback-stage0/`.

## Stage 1: Block Fsync Writeback Harness

Implement loadbench harness support for filesystem-shaped block fsync windows.
This stage measures the storage work a block device sees when a filesystem has
already buffered dirty 4KiB pages and issues an fsync. It is not a local
page-cache benchmark and is not the primary storage-layer optimization.

The adapter should expose local-vs-durable semantics directly. Do not implement
it as a drop-in `BlockDevice` replacement if that would force buffered writes
to pretend they returned a durable storage commit. Prefer explicit operations
such as buffered write, overlay read, and fsync/commit.

Requirements:

- Dirty windows are staged before timing begins.
- `fsync` commits all dirty ranges atomically through `commit_block_batch`.
- Explicit adapter `fsync` persists through the returned commit.
- Dirty memory is bounded by the workload's fsync window.
- Failed fsync leaves the staged window retryable for the next operation.
- A successful fsync clears exactly the committed dirty window.
- The adapter is not used by default by direct block workloads.

Suggested default policy:

- 4MiB dirty byte limit for latency-first mode.
- 1MiB and 2MiB benchmark modes for lower fsync latency.
- 16MiB benchmark mode as throughput control, not default.

New loadbench workloads:

- `block-writeback-fsync-1m`
- `block-writeback-fsync-2m`
- `block-writeback-fsync-4m`
- `block-writeback-fsync-16m`
- alias: `block-writeback`

Success criteria:

- Remote durability for writeback workloads is reported by fsync latency and
  fsync throughput, not by buffered write latency.
- Dirty memory never exceeds the configured fsync window.
- `fsync` after failure remains retryable.
- Existing direct block write semantics remain unchanged.

Exit gate:

- Do not keep the adapter if it touches core metadata or storage-node logic.
- Do not hide remote durability behind buffered writes; document that buffered
  write success is local/client-visible until `fsync`.

## Stage 2: Storage Commit/Fsync Baseline Sweep

Before optimizing storage internals, measure fsync windows directly and choose
the best latency-first default.

Measurements:

- `block-batch-fsync-interval` with `--block-batch-fsync-mib 1,2,4,8,16`;
- `block-write-1m-shard-lanes`;
- `block-batch-4k-256ops`;
- `native-write-1m`;
- optional client harness workloads from Stage 1.

Report for each window:

- throughput;
- p50/p90/p99/max fsync latency;
- effective 4KiB amortized cost inside the fsync window;
- full workload write throughput as dirty bytes made durable per second;
- durable profile p50/p90/p99 for data-log append/sync, catalog publish,
  metadata publish, SQLite row-sync/commit, and total persist.

Success criteria:

- Identify a recommended latency-first dirty window, likely 1-4MiB.
- Do not proceed to storage internals until the profile names the dominant
  fsync phase.
- If 1-4MiB fsync is already within target, stop and document the recommended
  client policy instead of adding storage complexity.

## Stage 3: Warm Block Read Fast Path

Optimize steady-state committed 4KiB reads.

Requirements:

- Cache committed shard roots and enough mapping lookup state in the block
  handle/adapter to avoid repeated metadata-plane traversal for warm reads.
- Invalidate cache on local flush/publish, restore, fork, delete, or observed
  generation change.
- Keep corruption verification behavior explicit: default verified payloads are
  checked unless read verification is skipped by policy.
- Reads overlapping dirty ranges merge overlay bytes and committed bytes.

Success criteria at `200us` modeled RTT:

- Dirty-overlay 4KiB read: p99 <= 500us.
- Warm committed 4KiB read: p50 <= 500us, p99 <= 1ms.
- Cold committed 4KiB read: p99 <= 2ms.
- Read correctness tests cover sparse ranges, dirty overlays, committed ranges,
  partial overlap, restore/fork invalidation, and checksum failure.

Exit gate:

- If warm reads are already under target in Stage 0, skip this stage.
- Do not add a broad metadata cache if a small shard/root lookup cache is
  sufficient.

## Stage 4: Fsync Path Narrowing

Only after Stage 2 shows a concrete fsync bottleneck, optimize the storage
commit path.

Targets:

- persist only the collapsed dirty batch delta;
- avoid full state export when the target commit is a block batch and no
  unrelated changes need persistence;
- keep data-before-metadata ordering;
- write/sync per-storage-node logs concurrently where safe;
- bound physical sync groups by configured dirty window;
- keep SQLite durability settings unchanged.

Measurements:

- `block-writeback-fsync-1m`, c1/c4/c16;
- `block-writeback-fsync-2m`, c1/c4/c16;
- `block-writeback-fsync-4m`, c1/c4/c16;
- `block-writeback-fsync-16m`, c1/c4/c16 throughput control;
- direct controls: `block-write-1m-shard-lanes`, `block-batch-4k-256ops`,
  `native-write-1m`.

Success criteria at `200us` modeled RTT:

- 1MiB fsync: c1 p50 <= 2ms, p99 <= 5ms; c16 p99 <= 20ms.
- 2MiB fsync: c1 p50 <= 3ms, p99 <= 8ms; c16 p99 <= 30ms.
- 4MiB fsync: c1 p50 <= 6ms, p99 <= 15ms; c16 p99 <= 50ms.
- 16MiB fsync: throughput >= 1.5GB/s, no requirement for sub-50ms p99.
- No direct block or native throughput regression >5% unless explained and
  accepted.

Exit gate:

- If profiles show data-log sync dominates and physical disk/container sync is
  the limit, stop and document the storage-device ceiling.
- If metadata publish dominates, narrow block metadata delta before touching
  storage-node code.
- If payload integrity dominates, evaluate verified vs unchecked policy but do
  not silently disable verification.

## Stage 5: Production Semantics Review

Before treating the writeback reference harness as production-shaped client
code:

- Document success semantics: buffered write success is local read-your-writes,
  `fsync` success is durable and globally visible.
- Define behavior on process crash before fsync: dirty data is lost.
- Define behavior on remote commit success but local adapter failure before
  clearing dirty state: retry must be idempotent or safely detect committed
  ranges.
- Define memory pressure behavior.
- Define max dirty age if time-based flushing is introduced.
- Keep replication below the API; the adapter does not choose replicas.

## Final Gate

Before committing each implementation stage:

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test
docker compose down
```

Final comparison table must include:

- direct 4KiB block write;
- warm committed 4KiB read;
- direct 1MiB block write;
- 1/2/4/16MiB client writeback fsync;
- native 4KiB and 1MiB random write controls;
- profile p50/p90/p99 for the dominant phases.

The work is successful only if the fast path becomes simpler at the user-visible
boundary: small writes are memory-buffer operations, reads are cache/overlay
lookups when possible, and remote durable work happens at explicit commit
boundaries with bounded latency.
