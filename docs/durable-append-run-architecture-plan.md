# Durable Append-Run Architecture Plan

Status: implemented and measured checkpoint
Scope: native append streams, private ingest, publish-prefix persistence, stream GC
Goal: make append performance scale with large sequential log writes and compact
metadata publishes, not with per-append segment placement or full-state durable
persists.

## Summary

The final native appender architecture should be a clean data-plane/control-plane
split:

```text
open stream      -> metadata service fences old writer at visible file head
append bytes     -> storage-node ingest lanes append bytes to private logs
submit publish   -> capture a private stream prefix
wait publish     -> sync captured prefix and attach runs as visible file extents
release stream   -> explicitly release the writer lease
gc               -> visible extents and active streams protect only live log ranges
```

The earlier implementation had the right public stream shape but the wrong
storage shape: it persisted stream bytes as ordinary segments, placed each
segment in the node catalog, and used durable-provider persist paths built for
whole-state or generic segment commits. That made p99 latency follow the burst
cost of many small segment placements and data-log syncs.

The current implementation stores stream appends as append runs and
publishes those runs as compact file extents. Stream ingest now has one payload
owner: append-run data, not append-run data plus ordinary segments. Contiguous
unpublished stream appends coalesce into bounded run records before publish
prefix persistence, and concurrent publish waiters batch only requested stream
ranges so they do not expose unrelated private data.

The north-star outcome is:

- append ingest is cheap and mostly independent of metadata service latency;
- publish is the only public append durability and visibility boundary;
- private ingest is not a restart-resume contract;
- publish persists the captured prefix, then performs a compact metadata update;
- file metadata contains a small number of large extents per publish;
- GC treats private stream data as a first-class root only while the stream is
  active and unreleased.

Measured checkpoint: `target/loadbench/append-run-waiter-batch-final/` records
the 200 us RTT durable matrix for the append-run implementation. At c16,
`native-stream-ingest-1m` reached about 3.6 GiB/s with p50 624 us and p99
70 ms; `native-stream-ingest-32m` reached about 4.9 GiB/s with p50 111 ms and
p99 159 ms; server-persisted publish reached about 923 MiB/s with p50 5.2 ms
and p99 17 ms; publish-pipelined reached about 1.1 GiB/s with p50 14 ms and p99
42 ms.
Profile rows show metadata work is generally sub-ms to low-single-digit ms.
When split by profile `new_segment_bytes`, payload prefix-persist rows are dominated by
bounded data-log sync groups: c16 `native-stream-ingest-1m` 32 MiB prefix-persist rows
had total p50/p99 about 9.9/15.6 ms, lock wait p99 about 0 ms, and file-sync
p99 about 14.9 ms; c16 `native-stream-ingest-32m` 32 MiB prefix-persist rows had total
p50/p99 about 5.6/15.7 ms, lock wait p99 below 1 ms, and file-sync p99 about
13.1 ms. The higher zero-byte profile tail is publish metadata waiting behind
an in-flight bounded data-log sync, not publish metadata fanout.

Read verification was measured separately in
`target/loadbench/append-run-read-verification/`. At c16 and 200 us RTT,
verified native 4 KiB reads reached about 72.8k IOPS with p99 468 us, while
skip-verify reads reached about 71.1k IOPS with p99 628 us. The checksum path is
therefore not a meaningful bottleneck in the current measured read path.

## Mental Model

The desired appender model is not "every append creates a segment and publishes
metadata." It is:

```text
lease/fence          = control plane
append log write     = storage-node data plane
submit publish       = captured prefix boundary
wait publish         = durable reader-visible file boundary
release/abort        = explicit lease end
```

There is intentionally no durable stream registry, resume-by-logical-name API,
or public private-durability mark in this model. A replacement opens a fresh
stream, fences the old stream, and starts at the visible file head. WAL-like
users that need recovery by file name must publish at the interval they want to
make globally recoverable.

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
- `submit_append_publish(stream, publish_through) -> AppendPublishTicket`
- `wait_append_publish(ticket) -> FileCommit`
- `publish_append_stream(stream, publish_through) -> FileCommit`
- `release_append_stream(stream)`
- `abort_append_stream(stream)`

Same-file rules:

- opening a new stream fences the old active stream;
- same-file `write_at` fences the active stream;
- other files in the same keyspace do not affect the stream;
- new writers start from the visible file head, not private durable bytes.
- publish does not release the active stream;
- release and abort explicitly end the stream;
- publish is the first globally discoverable and restart-durable boundary.

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
  publishing_high_water
  durable_high_water
  published_high_water
  status: active | released | fenced | aborted
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
- bounded group sync for publish-prefix persistence;
- data-before-metadata ordering for every publish commit.

`append_stream` returns after bytes are accepted into the local ingest path.
The server may auto-sync private append-log bytes on implementation-chosen
boundaries, but that is not a public durability guarantee. Publish is the public
boundary.

### Publish Prefix

`submit_append_publish(stream, publish_through)` captures a stream prefix:

```text
AppendPublishTicket {
  ticket_id
  stream_id
  writer_epoch
  publish_through_file_offset
}
```

The captured prefix means:

- appends above the prefix may continue while publish work runs;
- bytes through the prefix are invisible until `wait_append_publish` commits;
- a different writer cannot inherit those private bytes;
- the publish may reference only data accepted before the captured prefix.

Physical ordering:

1. append bytes to storage-node log files;
2. sync the touched log files;
3. persist run manifests and checksum metadata;
4. publish metadata that references those persisted runs;
5. return `AppendPublishCommit`.

No generic full-state image persist is allowed on the publish-prefix persistence
hot path.

### Visible Publish

`wait_append_publish(ticket)` converts the captured private prefix into visible
file metadata in one atomic file-version transition:

```text
FileExtent {
  file_offset
  len
  backing: AppendLogRunRange
}
```

Publish rules:

- publish persists any pending bytes through the captured prefix before metadata
  references them;
- publish fails if the stream is stale, fenced, released, aborted, or the prefix
  is no longer publishable;
- publish creates the fewest deterministic extents possible by coalescing
  adjacent compatible runs;
- readers, PITR, forks, and normal metadata traversal see only published
  extents;
- publish does not release the appender lease;
- publish failure must not expose partial bytes.

The file tree should store compact run-backed extents, not per-append segment
IDs. Existing block writes can continue to use ordinary immutable segments, but
native append publish should not be forced through the block segment shape.

### Checksums

Checksums remain a storage-node integrity concern, not a reason to fragment file
metadata. The current append-run implementation stores one integrity policy on
each run or run range:

```text
SegmentPayloadIntegrity::Crc32c(checksum) | SegmentPayloadIntegrity::Unchecked
```

Reads verify run-backed extents by default when the run is verified. Callers may
choose unchecked writes and skip-verify reads explicitly; requiring verification
against an unchecked run fails cleanly. A checksum failure rejects the read.
Chunked checksum sidecars are not part of v1 because the corrected CRC32C path
is not a measured bottleneck and the sidecar would add metadata shape without a
clear win.

### Recovery

On reopen:

- visible file heads are reconstructed from published metadata;
- active append streams may be reconstructed as implementation-private cleanup
  roots, but unpublished bytes are ignored for public recovery;
- unpublished bytes are invisible to readers;
- released, fenced, or aborted streams cannot resume.

Crash cases:

- before publish commit: no visible data and no public resume guarantee;
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
- publish-prefix persistence through generic full-state durable persist;
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

- Record current stream ingest, publish-prefix, native write, and block write
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

- Add core types for append log runs, run ranges, payload-integrity policy,
  publish prefixes, and run-backed file extents.
- Update metadata validation to accept run-backed extents beside existing
  segment-backed extents where native files need them.
- Add deterministic coalescing rules for adjacent compatible run ranges.
- Add reference-model tests for append, publish-prefix persistence, recovery,
  and fencing.

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
- unpublished stream bytes are not restart-resumable;
- c16 `native-stream-ingest-1m` p50 is sub-ms except when the operation performs
  its own publish boundary;
- profiles show no global persist lock wait on stream ingest.

### Stage 3: Durable Stream Mark Persistence

- Persist accepted append run manifests for a captured publish prefix without
  generic full-state persist.
- Reopen ignores unpublished stream state for public recovery.
- Add failure injection at each data-before-metadata boundary.
- Ensure new writers fence old streams and start from visible head.

Exit gate:

- after publish commit, bytes are visible after reopen;
- after crash before publish, data is invisible and not resumable through the
  public API;
- unrelated generic durable persists do not make unpublished stream data visible;
- c16 publish-prefix persistence p99 is dominated by physical sync, not metadata
  or lock wait.

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
- Verify run-backed payload integrity on read by default.
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
  --workloads native-stream-ingest-1m,native-stream-ingest-4m,native-stream-ingest-32m,native-stream-publish-prefix-1m,native-stream-publish-pipelined-1m,native-write-1m,block-write-4k,block-read-4k,native-read-4k \
  --duration-ms 1000 --warmup-ms 100 --concurrency 1,4,16 --files 128 \
  --rtt-us 200 --delay-mode spin \
  --stream-publish-mib 128 \
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

- `append_stream` ingest p50 stays sub-ms when it does not wait for publish;
- publish-prefix persistence p99 tracks bounded physical sync group size, not
  number of append calls;
- publish p99 is single-digit to low double-digit ms for a 128 MiB publish;
- publish metadata entries scale with run count, not append call count;
- c16 1 MiB stream ingest reaches multi-GB/s when publish intervals are
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
