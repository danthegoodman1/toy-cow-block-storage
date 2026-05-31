# Native File Batch Commit Plan

Status: draft  
Project: `toy-cow-block-storage`

## Summary

Native append streams already have the right high-performance API shape:
private ingest, explicit flush, explicit publish, writer fencing, and takeover
from the visible file boundary.

The ordinary native random-write path should get a similar explicit visibility
boundary, but without reusing append-stream semantics. The primitive should be
an atomic file batch commit:

```text
commit_file_batch(keyspace_id, file_id, writes[], durability) -> FileCommit
```

`write_file_at(file, offset, bytes, durability)` becomes only the one-write
convenience case:

```text
write_file_at(file, offset, bytes, durability)
  = commit_file_batch(file, [{ offset, bytes }], durability)
```

Storage internals should have one real random-write implementation. No separate
legacy `write_at` path, no compatibility wrapper, and no tombstone API.

This plan is the native-file sibling of
`docs/block-write-commit-boundary-plan.md`: clients own batching policy;
storage owns atomic publication, durability, fencing, recovery, and GC.

## Target API Shape

Keep append streams as-is:

```text
open_append_stream(keyspace_id, file_id) -> AppendStream
append_stream(stream, bytes) -> AppendTicket
flush_append_stream(stream) -> DurableAppendMark
publish_append_stream(stream, mark) -> FileCommit
abort_append_stream(stream)
```

Replace ordinary random writes with:

```text
commit_file_batch(keyspace_id, file_id, writes[], durability) -> FileCommit
```

Optional helper only:

```text
write_file_at(keyspace_id, file_id, offset, bytes, durability)
```

The helper may remain in a client/adapter layer for ergonomics, but it must call
the same one-write batch path. The storage layer should not maintain two
separate random-write implementations.

## Client Responsibilities

The native file client or filesystem adapter owns dirty-range batching policy:

- maintain a per-file dirty-range map;
- coalesce repeated overwrites before sending;
- preserve application-level barriers and ordering;
- decide commit triggers:
  - explicit fsync;
  - close;
  - `4MiB` to `16MiB` low-latency dirty threshold;
  - `16MiB` to `64MiB` throughput-oriented dirty threshold;
  - short time threshold for latency-sensitive clients;
  - memory pressure;
- provide read-your-writes overlay for buffered but uncommitted data when the
  caller requires it.

The default should be bounded and conservative, not giant memory hoarding:

```text
default dirty commit threshold: 16MiB
default max batch payload: 32MiB
bulk-writer upper bound: 128MiB, opt-in only
```

The batch does not need one contiguous allocation. It can be a dirty-range list
of owned buffers or reference-counted byte slices.

The client must not choose storage nodes, segment IDs, metadata roots, or
replica placement.

## Storage Responsibilities

The storage layer owns correctness:

- validate keyspace/file identity and byte ranges;
- collapse overlapping writes inside the batch in request order;
- split collapsed ranges into file metadata extents;
- choose storage-node placement per segment/run;
- persist payloads according to durability policy;
- enforce data-before-metadata ordering;
- path-copy the file metadata tree once for the collapsed batch;
- advance the file version exactly once;
- update the owning keyspace catalog shard exactly once;
- publish one atomic commit group;
- reject stale file-version fences;
- leave failed payloads private/orphaned and reclaimable;
- preserve PITR, snapshot, restore, append-stream fencing, and GC invariants.

A single `commit_file_batch` may write payloads to many storage nodes. The batch
is a logical atomic file commit, not a demand that all bytes land on one storage
server.

## Append Stream Relationship

Do not replace append streams with file batches.

Append streams remain the specialized sequential growth path because they own
semantics that random batch writes do not:

- append writer ownership;
- tail reservation;
- stale writer fencing;
- private durable high-water;
- explicit visible publish;
- resumability before publish;
- new appender takeover from the visible boundary.

`commit_file_batch` is for arbitrary offset writes and file-version batching.
`append_stream` is for high-throughput sequential file creation/growth.

## Stage 0: Baseline Current Native Random Writes

Run a clean baseline before changing behavior. Save raw output under ignored
`target/loadbench/native-file-batch-stage0/`.

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test

docker compose exec dev mkdir -p target/loadbench/native-file-batch-stage0
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack-flush:1 \
  --workloads native-write-4k-same-file,native-write-4k-file-lanes,native-write-1m,native-stream-flush-publish-1m \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,16,64 \
  --files 128 \
  --storage-nodes 4 \
  --rtt-us 200 \
  --delay-mode spin \
  --payload-integrity verified \
  --durable-profile-csv target/loadbench/native-file-batch-stage0/profile.csv
```

The baseline is only for comparison. Do not optimize the old one-write
implementation unless a small cleanup is required to replace it safely.

## Stage 1: Local Batch Commit Primitive

Implement `commit_file_batch` in the local deterministic path first.

Deliverables:

- public/native request and response types;
- internal `FileWriteBatch` command shape;
- validation for empty batches, overflow, range shape, and maximum batch bytes;
- overlap collapse by request order;
- path-copy file metadata once for the collapsed batch;
- one file version advance;
- one keyspace catalog shard update;
- one commit group containing the file-root update.

Correctness tests:

- one batch with multiple writes becomes visible atomically;
- overlapping writes in one batch resolve by request order;
- single-write batch exactly matches current `write_file_at` behavior;
- failed metadata publish leaves old file version visible;
- same-file stale version fence fails cleanly;
- other files in the same keyspace do not conflict;
- append streams for the same file are invalidated by successful batch commit;
- append streams for other files are not invalidated;
- snapshots, restore, PITR, and GC observe only committed batch results.

Benchmark after this stage under
`target/loadbench/native-file-batch-stage1/`.

Expected result: file-batch workloads should reduce per-write file version and
keyspace shard publish amplification even before durable delta optimization is
complete.

## Stage 2: Durable Batch Commit Delta

Make durable `commit_file_batch` persist only the batch payloads and changed
metadata.

Ordering:

1. append batch payloads to storage-node data logs;
2. sync data logs if durability requires it;
3. persist placement/catalog rows for new segments/runs;
4. path-copy file metadata once for collapsed ranges;
5. persist changed metadata nodes, file commit, keyspace shard head, keyspace
   commit, commit group, and writer epoch updates;
6. mark referenced data only after metadata publish succeeds;
7. return `FileCommit`.

Rules:

- no full keyspace or whole-store state image persistence for batch commit;
- payload bytes are not reappended during metadata publish;
- changed metadata rows scale with collapsed range count and touched file tree
  nodes, not original write call count;
- physical sync groups are bounded, default cap `32MiB`;
- failed publish leaves payloads unreferenced and reclaimable.

Correctness tests:

- durable batch commit survives reopen;
- acknowledged-but-unflushed behavior matches the durability contract;
- failed data-log sync publishes no metadata;
- failed metadata row publish exposes no partial file contents;
- GC collects failed batch payloads and retains committed payloads;
- PITR replay restores contents across multiple batch commits.

Benchmark after this stage under
`target/loadbench/native-file-batch-stage2/`.

Success signal:

- batch commit p99 is bounded by sync group caps and metadata delta size;
- throughput improves with larger but bounded dirty batches;
- one-write helper has no separate behavior or performance cliff beyond being a
  batch of size one.

## Stage 3: Loadbench And Client Writeback Simulation

Update `loadbench` so the native random-write batch model is measured directly.

Add workloads:

- `native-file-batch-4k-16ops`
- `native-file-batch-4k-256ops`
- `native-file-batch-4k-4096ops`
- `native-file-batch-1m-16ops`
- `native-file-batch-overwrite-collapse`
- `native-file-batch-fsync-interval`
- alias: `native-file-batch`

Add knobs:

- `--native-file-batch-ops`;
- `--native-file-batch-bytes`;
- `--native-file-batch-overlap sequential|random|overwrite-hotset`;
- `--native-file-batch-fsync-mib`, default `16`;
- `--native-file-batch-fsync-ms`;
- `--native-client-overlay on|off` for future adapter read-your-writes tests.

Compare:

- old `native-write-*` baseline from Stage 0;
- one-write `commit_file_batch`;
- batched 4KiB random writes;
- overwrite-collapse batches;
- fsync-interval simulation;
- append-stream ingest/flush/publish controls.

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
- file metadata path-copy time;
- keyspace shard publish time;
- metadata publish lock wait;
- metadata row-sync time;
- SQLite commit time;
- changed file metadata node count;
- touched keyspace shard count;
- physical sync group bytes;
- commit sequence allocation time.

## Interpretation Rules

- If batch commit improves sharply with batch size, fixed file-version/keyspace
  publish overhead was the old ceiling.
- If batch ingest/write is fast but commit is slow, focus on file tree path-copy
  and keyspace shard publish delta size.
- If commit cost scales with original write count instead of collapsed range
  count, fix range collapse before deeper storage changes.
- If same-keyspace different-file batches conflict, recheck keyspace shard and
  file-version fencing.
- If unchecked integrity materially changes throughput or p99, checksum policy
  is still hot-path relevant.
- If append-stream controls regress, the shared substrate change is too broad.

## Final Gate

Before committing each stage:

```sh
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test
```

After the final stage, produce a 200us RTT comparison table:

- old one-write native baseline;
- one-write `commit_file_batch`;
- native file batch 4KiB at 16/256/4096 operations;
- native file batch 1MiB at 16 operations;
- overwrite-collapse batch;
- fsync-interval simulation;
- append stream ingest/flush/publish control;
- block batch control, if implemented.

Report IOPS, MB/s, p50/p90/p99/max, errors, profile phase p50/p90/p99, and
whether the remaining ceiling is client batching policy, data ingest, durable
flush, visible commit, metadata publish, or payload integrity.

## Non-Goals

- No append-stream API rewrite in this phase.
- No POSIX work in this phase.
- No second storage path for `write_file_at`.
- No fake/null providers for performance claims.
- No durable format compatibility shim.
- No client-side replica or storage-node placement.
- No weakening of CoW, PITR, data-before-metadata ordering, keyspace snapshot
  semantics, or append-stream fencing.

