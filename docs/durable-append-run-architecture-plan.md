# Durable Append-Run Architecture Plan

Status: proposed  
Scope: native append streams, durable ingest, visible publish, stream GC  
Goal: make append performance scale with large sequential log writes and compact
metadata publishes, not with per-append segment placement or full-state durable
persists.

## Summary

The final native appender architecture should be a clean data-plane/control-plane
split:

```text
open stream      -> metadata service fences old writer at visible file head
append bytes     -> storage-node ingest lanes append bytes to private logs
flush stream     -> storage nodes sync durable log ranges and record stream mark
publish stream   -> metadata service attaches durable runs as visible file extents
gc               -> visible extents and active streams protect only live log ranges
```

The current implementation is close in public shape but wrong in storage shape.
It still persists stream bytes as ordinary segments, places each segment in the
node catalog, and uses durable-provider persist paths that were originally built
for whole-state or generic segment commits. That makes p99 latency follow the
burst cost of many small segment placements and data-log syncs. The target
architecture stores stream appends as large durable runs and publishes those
runs as compact file extents.

The first measured pre-ingest experiment improved the stream path by writing
payloads to an unsynced stream-private data log before flush, then syncing those
records in bounded groups. That proved the direction: lock wait disappeared and
flush tails became physical-sync dominated. It is not the final shape because it
still duplicates payloads through the ordinary in-memory segment path and still
publishes one segment-backed record per append. The final architecture must have
one payload owner for stream ingest: append-run data, not append-run data plus
ordinary segments.

The north-star outcome is:

- append ingest is cheap and mostly independent of metadata service latency;
- durability is a stream-private high-water, not a reader-visible file update;
- publish is metadata-only and scales with durable run count;
- file metadata contains a small number of large extents per publish;
- GC treats private durable stream data as a first-class root only while the
  stream is active or resumable.

## Mental Model

The desired appender model is not "every append creates a segment and publishes
metadata." It is:

```text
lease/fence        = control plane
append log write   = storage-node data plane
durable mark       = stream-private recovery boundary
publish            = reader-visible file boundary
```

A 128 MiB append made from 128 one-MiB client calls should become one durable
stream run when the physical log range is contiguous, and then one visible file
extent at publish time:

```text
file offset A..A+128MiB -> storage_node N, append_log L, payload range X..X+128MiB
```

It may become a small number of extents if it crosses log files, storage nodes,
checksum blocks, or failure boundaries. It must not become 128 visible metadata
entries merely because the client used 128 calls.

## Required Architecture

### Control Plane: Stream Ownership

The metadata service owns stream identity and fencing:

- `open_append_stream(keyspace_id, file_id) -> AppendStream`
- `append_stream(stream, bytes) -> AppendTicket`
- `flush_append_stream(stream) -> DurableAppendMark`
- `publish_append_stream(stream, mark) -> FileCommit`
- `abort_append_stream(stream)`

Same-file rules:

- opening a new stream fences the old active stream;
- same-file `write_at` fences the active stream;
- other files in the same keyspace do not affect the stream;
- new writers start from the visible file head, not private durable bytes.

The metadata service tracks:

```text
AppendStreamState {
  stream_id
  keyspace_id
  file_id
  writer_epoch
  visible_base_version
  visible_base_size
  reserved_tail
  durable_high_water
  published_high_water
  status: active | fenced | aborted | published
}
```

This row is small. It must not contain one record per append call.

### Data Plane: Storage-Node Append Lanes

Storage nodes own durable bytes for stream ingest. A stream append writes into a
node-local append lane:

```text
AppendLogRun {
  run_id
  storage_node
  stream_id
  writer_epoch
  keyspace_id
  file_id
  file_offset_start
  payload_len
  log_id
  log_payload_offset
  log_record_bytes
  checksum_policy
  checksum_range_ref
}
```

Append lanes should be simple:

- one or more per-storage-node log writers;
- sequential append-only files;
- no global `persist_lock` for data ingest;
- no metadata tree update while ingesting bytes;
- bounded group sync for durable marks;
- data-before-metadata ordering for every durable mark.

`append_stream` may return after bytes are accepted into the local ingest path.
Only `flush_append_stream` returns a durability guarantee. Automatic flush
policy may run every configured interval, but explicit flush remains the
contract boundary.

### Durable Mark

`flush_append_stream` persists private bytes and records a stream high-water:

```text
DurableAppendMark {
  stream_id
  writer_epoch
  durable_through_file_offset
  covered_runs
}
```

The durable mark means:

- bytes through the high-water are restart-resumable by the same stream token;
- bytes are still invisible to normal file readers;
- a different writer cannot inherit those private bytes;
- a later publish may reference only data covered by a durable mark.

Physical ordering:

1. append bytes to storage-node log files;
2. sync the touched log files;
3. persist run manifests and checksum metadata;
4. persist stream durable high-water;
5. return `DurableAppendMark`.

No generic full-state image persist is allowed on the stream flush hot path.

### Visible Publish

`publish_append_stream(stream, mark)` converts durable private runs into visible
file metadata in one atomic file-version transition:

```text
FileExtent {
  file_offset
  len
  backing: AppendLogRunRange
}
```

Publish rules:

- publish is metadata-only for already flushed stream data;
- publish fails if the stream is stale, fenced, or the mark is not covered by
  the stream durable high-water;
- publish creates the fewest deterministic extents possible by coalescing
  adjacent compatible runs;
- readers, PITR, forks, and normal metadata traversal see only published
  extents;
- publish failure must not expose partial bytes.

The file tree should store compact run-backed extents, not per-append segment
IDs. Existing block writes can continue to use ordinary immutable segments, but
native append publish should not be forced through the block segment shape.

### Checksums

Checksums remain a storage-node integrity concern, not a reason to fragment file
metadata. A large visible extent may point to a run that has a checksum sidecar:

```text
ChecksumRange {
  run_id
  chunk_size
  checksums[]
}
```

Reads verify the relevant checksum chunks unless the caller explicitly chooses a
no-verify mode. A checksum failure rejects the read. Checksum chunking must not
force one visible extent per checksum chunk.

### Recovery

On reopen:

- visible file heads are reconstructed from published metadata;
- active/resumable append streams are reconstructed from durable stream rows and
  append run manifests;
- unflushed bytes are ignored;
- durable-but-unpublished bytes are invisible to readers;
- the same stream token may resume private durable bytes if it was not fenced;
- fenced or aborted streams cannot resume.

Crash cases:

- before log sync: no durable mark, no resume;
- after log sync before run manifest: no returned durable mark, no visible data;
- after run manifest before stream mark: no returned durable mark, no visible
  data;
- after stream mark before publish: resumable by token, invisible;
- after publish commit: visible after reopen.

### GC

GC has two root classes:

1. visible roots: file extents, block extents, PITR, forks, checkpoints;
2. private stream roots: active/resumable streams through their durable
   high-water.

Fenced, aborted, expired, or superseded stream-private data is reclaimable.
Published ranges are protected by normal visible metadata, so private stream
protection can be pruned after publish.

GC must operate on append log ranges, not just whole files. It may compact live
ranges into new logs, but the first version can use conservative whole-log
retention when a log contains any protected range. The exit gate for production
safety is range-aware tests even if v1 sweeping is conservative.

## What Must Be Removed

The final implementation should not keep compatibility paths for the old stream
segment model.

Remove or replace:

- per-append stream records embedded in durable stream rows;
- stream flush through generic full-state durable persist;
- stream publish through ordinary segment placement fanout;
- one-shot append as a performance path;
- any wrapper that translates the new append-run model back into old per-segment
  stream internals.

The final code should have one native stream storage model: durable append runs
plus compact visible extents.

## Implementation Stages

Each stage must be committed separately with benchmark CSV under ignored
`target/loadbench/<stage>/`.

### Stage 0: Baseline And Design Alignment

- Record current stream ingest, flush, publish, native write, and block write
  matrix at 200 us RTT.
- Add this plan to the implementation plan as the next native append phase.
- Update the design spec to state that native append publish uses run-backed
  extents, not ordinary block-style segments.
- Keep current code behavior unchanged in this stage.

Exit gate:

- docs agree on durable vs visible boundaries;
- baseline has profile CSV for data-log, metadata, lock wait, checksum, and
  publish phases.

### Stage 1: Append Run Types In The Deterministic Core

- Add core types for append log runs, run ranges, checksum ranges, durable marks,
  and run-backed file extents.
- Update metadata validation to accept run-backed extents beside existing
  segment-backed extents where native files need them.
- Add deterministic coalescing rules for adjacent compatible run ranges.
- Add reference-model tests for append, flush, publish, recovery, and fencing.

Exit gate:

- generated simulations prove visible reads match the reference model;
- publish creates O(run count) extents, not O(append count);
- block metadata remains unchanged.

### Stage 2: Storage-Node Append Ingest Lane

- Implement node-local append log writers for stream data.
- Replace stream append segment creation with append-log-run ingest. Stream
  bytes must not also be written through the ordinary segment store.
- Keep reservation and fencing in metadata, but keep payload writes out of the
  metadata publish path.
- Add bounded group sync by bytes and by maximum wait time.
- Make sync groups stream-aware and storage-node-local.

Exit gate:

- `append_stream` does not create ordinary segment placements;
- stream ingest has one payload owner and one durable data layout;
- unflushed stream bytes are not restart-resumable;
- c16 `native-stream-ingest-1m` p50 is sub-ms except when the operation performs
  its own flush boundary;
- profiles show no global persist lock wait on stream ingest.

### Stage 3: Durable Stream Mark Persistence

- Persist append run manifests and stream durable high-water without generic
  full-state persist.
- Load resumable stream state from durable rows and manifests.
- Add failure injection at each data-before-metadata boundary.
- Ensure new writers fence old streams and start from visible head.

Exit gate:

- after returned durable mark, same token can resume after reopen;
- after crash before mark, data is invisible and not resumable;
- unrelated generic durable persists do not make unflushed stream data durable;
- c16 stream flush p99 is dominated by physical sync, not metadata or lock wait.

### Stage 4: Compact Visible Publish

- Replace stream publish internals with run-backed file extents.
- Publish only metadata deltas: new file extents, file head, commit rows, stream
  published high-water.
- Coalesce adjacent compatible durable runs at publish.
- Keep publish atomic at file-version granularity.

Exit gate:

- publishing 128 one-MiB appends produces one or a small deterministic number of
  visible extents;
- publish profile has `new_segment_bytes = 0`;
- publish p50 is low single-digit ms and p99 is bounded by metadata commit, not
  data-log sync.

### Stage 5: Read Path And Verification

- Read run-backed file extents directly from append log ranges.
- Verify checksum chunks on read by default.
- Support explicit no-verify reads and writes as a policy bit, without allowing
  silent mismatch between verified and unverified payloads.
- Keep corruption failures local and deterministic.

Exit gate:

- reads from run-backed extents match published bytes;
- checksum corruption is detected on read;
- no-verify mode avoids checksum cost and is visible in metadata/policy;
- read benchmarks include verified and no-verify modes.

### Stage 6: GC For Private Runs

- Add private stream roots to GC mark traversal.
- Protect durable ranges for active/resumable streams.
- Stop protecting fenced, aborted, expired, or superseded private data.
- Prune private protection after publish.
- Add log-range compaction or conservative whole-log retention with tests that
  prove visible data is never collected.

Exit gate:

- active private durable data survives GC and reopen;
- fenced/aborted unpublished data is reclaimable;
- published data remains protected through visible metadata and PITR roots.

### Stage 7: Delete Legacy Stream Segment Path

- Remove old stream segment records and per-append segment placement paths.
- Remove tests that exist only to preserve the old representation.
- Rewrite loadbench names/descriptions to call the new path `append-run` or
  `stream-ingest`.
- Keep one-shot append only if it is implemented directly on the new model or
  delete it from the performance matrix.

Exit gate:

- no compatibility wrappers or dual stream representations remain;
- docs, tests, traces, and generated simulations use the append-run model only.

## Benchmark Matrix

Run after every stage:

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable --durability ack \
  --workloads native-stream-ingest-1m,native-stream-ingest-4m,native-stream-ingest-32m,native-stream-publish-preflushed-1m,native-stream-flush-publish-1m,native-write-1m,block-write-4k,block-read-4k,native-read-4k \
  --duration-ms 1000 --warmup-ms 100 --concurrency 1,4,16 --files 128 \
  --rtt-us 200 --delay-mode spin \
  --stream-flush-mib 16 --stream-publish-mib 128 \
  --durable-profile-csv target/loadbench/<stage>/profile.csv

docker compose down
```

Report:

- IOPS and MiB/s;
- p50/p90/p99/max;
- errors;
- durable MiB/s and published MiB/s;
- profile p50/p90/p99 for queue wait, data-log write, data-log sync, manifest
  persist, stream mark persist, publish metadata, checksum encode/verify, and
  GC root scan.

## Success Criteria

The target is not a magic constant. The target is that the bottleneck becomes
obviously hardware or configured durability policy, not metadata shape.

Acceptance targets in a happy local environment with 200 us modeled RTT:

- `append_stream` ingest p50 stays sub-ms when it does not cross a flush
  boundary;
- stream flush p99 tracks bounded physical sync group size, not number of
  append calls;
- publish p99 is single-digit to low double-digit ms for a 128 MiB publish;
- publish metadata entries scale with run count, not append call count;
- c16 1 MiB stream ingest reaches multi-GB/s when flush/publish intervals are
  large enough to amortize durability;
- block and ordinary native write paths do not regress by more than 5 percent
  unless the stage explicitly changes shared storage-node logic and explains
  the tradeoff.

## Production Safety Requirements

- Every new state transition has deterministic model tests.
- Every data-before-metadata boundary has failure injection.
- Reopen tests cover before/after log sync, manifest persist, stream mark, and
  publish commit.
- GC tests cover active, fenced, aborted, expired, published, PITR-retained, and
  fork-retained ranges.
- Benchmarks are part of the exit gate for every hot-path stage.
- The old path is deleted once the new path is complete.

## Non-Goals For This Phase

- Replication or quorum durability.
- `io_uring`.
- Weakening SQLite durability PRAGMAs.
- Compression, encryption, or deduplication.
- POSIX rename/fsync semantics.
- Making block writes use append-run metadata.

Those may come later, but they are not needed to prove the high-performance
native appender model.
