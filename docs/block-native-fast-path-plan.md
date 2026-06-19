# Block-Native Fast Path Plan

Status: draft
Goal: beat Ceph RBD on the current block comparator, then decide whether to
push down-stack toward sub-200us durable latency.

## Summary

Keep the shared storage layer, but push it down. The shared layer should provide
durable bytes, replay, checksums, placement identity, reachability, and
materialization hooks. It should not impose native-file metadata, SQLite root
publication, generic segment cataloging, or interval-index churn on foreground
block writes.

The block API gets its own foreground mapping layer:

```text
leased block write
  -> validate whole-device writer lease
  -> reserve device-local commit sequence
  -> append packed block-journal record on a device/shard lane
  -> update provider-private LBA map
  -> if flushed: wait for the lane durable high-water
  -> return
```

Everything else is background or replay work:

```text
materialization / maintenance
  -> fold journal high-water into immutable CoW shard roots
  -> publish compact checkpoints
  -> advance fork/PITR/GC safe points
  -> prune journal records and orphan payloads proven unreachable
```

The current code is already moving this way. The journal fast path removed
foreground metadata-tree publication for small writes, segment-reference journal
writes moved larger flushed payloads out of inline journal frames, and the
read-index slice update removed an O(current journal ranges) foreground cost
from shared-device 4K writes. The next architecture makes that direction
explicit instead of continuing to optimize the old shared metadata path.

## Target Outcomes

Primary comparator target:

- On `c4-standard-32-lssd`, same GCP harness, pool-size-1 Ceph RBD, toy block
  beats Ceph throughput and p99 for `4k`, `64k`, `256k`, `1m`, and `32m`.
- `4k` and `64k` should beat Ceph by at least 10%, not only tie within noise.
- No native-file or existing block benchmark regresses by more than 5-10%
  without a documented reason.

Stretch target:

- Sub-200us p99 durable 4K writes under pressure, measured honestly at the
  durable block boundary.
- This likely requires direct or kernel-bypass I/O and suitable NVMe hardware;
  it should not be claimed from fake in-memory providers or buffered
  acknowledged writes.

## Architecture

### Shared Substrate

The shared storage substrate remains common across block and native APIs, but
its contract becomes lower-level:

- append fixed or variable records to durable logs;
- provide checksummed payload records and replay scans;
- allocate durable segment identities;
- expose storage-node placement and data-log ownership;
- provide reachability and GC roots;
- support background materialization into immutable CoW trees;
- expose profiles for append, sync, replay, catalog, compaction, and GC costs.

The substrate must not require foreground block writes to publish SQLite rows,
path-copy metadata trees, update native file heads, or run generic extent
catalog logic.

### Block Mapping Layer

The block layer owns a block-native current-view map:

```text
BlockLbaMap {
  device_id
  writer_epoch
  durable_through
  visible_through
  pages: logical_block -> BlockSource
}

BlockSource =
  InlineJournal { shard, frame, offset, len, integrity }
  SegmentRef { storage_node, segment_id, offset, len, integrity }
  SparseZero
  CompactTree
```

The first implementation can use a deterministic block-aligned run map or
radix/page-table shape keyed by logical block number. It should replace the
interval overlay read index for foreground block resolution. Interval history
remains in the journal for replay and materialization, but the hot read/write
map should avoid O(existing ranges) scans and avoid one durable-current-view
mutation per 4K page for large contiguous writes or sorted 4K batches.

### Journal Lanes

Use one block journal lane per shard, with a stable device-to-lane mapping.
Within a device, records are ordered by device commit sequence. Across devices,
no ordering is required.

Foreground lane behavior:

- `Acknowledged` write: append unsynced record, update live LBA map, return.
- `Flushed` inline write: enqueue packed journal record, sync lane, return
  after covering durable high-water.
- `flush_device`: sync any covered acknowledged payloads, append one flush
  marker, return after the marker is durable.
- lease fence: append a durable lease/fence record before admitting a new
  writer epoch.

The lane should drain to the currently available queue without timer batching in
v1. If measured group size remains too small, add a bounded spin/yield phase
only with before/after evidence that p99 does not regress.

### Payload Policy

Use separate policies for payload size classes:

- `<= 16 KiB`: inline packed journal record for 4K/8K/16K writes.
- `> 16 KiB` and `<= 256 KiB`: evaluate inline vs segment-ref with benchmark
  evidence; do not assume one threshold is universally best.
- `> 256 KiB`: segment-reference journal records backed by data-log payloads.
- `>= 1 MiB`: stripe data-log payload chunks across storage-node lanes when
  that improves throughput without p99 collapse.

The thresholds are provider-private and must be benchmark-tuned on the GCP
comparator. They do not change public API semantics.

### Catalog And Materialization

Catalog rows are background bookkeeping for replay, inspection, and GC. They
should not be part of the 4K flushed foreground boundary when the journal and
self-describing data logs already make the payload recoverable.

Materialization folds:

```text
journal high-water + current compact roots -> new compact roots
```

Materialization must preserve:

- fork snapshots;
- PITR restore by commit sequence;
- GC safety for journal inline payloads and segment refs;
- replay correctness after torn journal tails;
- stale writer fencing.

## Implementation Stages

### Stage 0: Baseline And Profiles

Run from current `main` or the active block branch:

- GCP comparator: `4k`, `64k`, `256k`, `1m`, `32m`.
- Local loadbench:
  - `block-write-4k`
  - `block-write-4k-shard-lanes`
  - `block-write-4k-device-lanes`
  - `block-batch-4k-16ops`
  - `block-batch-4k-256ops`
  - `block-batch-4k-4096ops`
  - `block-writeback-fsync-1m`
  - `block-writeback-prestaged-fsync-1m`
  - `block-read-4k`
  - `block-read-1m`

Record:

- throughput;
- p50/p90/p99/p999;
- journal append and sync nanos;
- flush group size;
- LBA map update nanos;
- payload sync nanos;
- catalog publish nanos;
- replay count and replay nanos;
- materialization and GC nanos.

Exit gate:

- The benchmark is proven to exercise small sparse/block edits, not only
  sequential large writes.
- Results are saved under ignored `target/loadbench/...` and summarized in the
  commit or follow-up doc.

### Stage 1: Block LBA Map

Replace the foreground block overlay read index with a block-native LBA map.

Deliverables:

- deterministic page-table/radix map implementation;
- exact latest-write-wins semantics for overlapping writes;
- sparse zero/discard entries;
- mixed compact-tree and journal-backed reads;
- replay rebuild into the same LBA map;
- profile field for LBA map update and lookup cost.

Exit gate:

- Existing journal replay tests pass.
- New tests cover overlap, disjoint ranges, sparse zero/discard, compaction
  handoff, and read-after-write.
- `block-write-4k-shard-lanes` improves materially or p99 drops materially
  versus the current read-index implementation.

Stage 1 local checkpoint, 2026-06-19, macOS Docker dev container, baseline
`a53158a`, artifacts under ignored `target/loadbench/block-lba-stage1-*`:

- Replaced the old foreground overlay read index with a provider-private
  block-aligned LBA run map.
- Large contiguous journal entries create one run. Sorted 256 x 4K batch
  entries coalesce into one fragmented inline run without copying payload bytes
  into a new 1MiB buffer.
- Segment-backed 1MiB overlay reads coalesce to one segment source read and run
  outside the overlay mutex.
- Deterministic tests cover overlap splitting, non-4K block size, sparse
  zero/discard, 256-entry fragmented batch coalescing, segment-read call shape,
  mixed compact-tree plus overlay reads, and replay.
- Key 5s local deltas:
  - `block-write-4k-shard-lanes` c16: `16320 -> 17223 IOPS`, p99
    `1642 -> 1594 us`.
  - `block-write-4k-device-lanes` c16: `14780 -> 16299 IOPS`, p99
    `1792 -> 1446 us`.
  - `block-batch-4k-256ops` c4: `1276 -> 1278 IOPS`, p99
    `9118 -> 8001 us`.
  - `block-batch-4k-256ops` c16: `1490 -> 1587 IOPS`, p99
    `24152 -> 28200 us`.
  - `block-read-1m` c16: `7376 -> 40445 IOPS`, p99
    `71268 -> 1639 us`.
- Focused reruns after noisy full-matrix rows:
  - `block-read-4k` c1: `342293 -> 317915 IOPS`, p99
    `7.38 -> 7.88 us`.
  - `block-read-4k` c4: `229117 -> 241897 IOPS`, p99
    `79.96 -> 68.63 us`.
  - `block-read-4k` c16: `85032 -> 87085 IOPS`, p99
    `1616 -> 1484 us`.
  - `block-writeback-fsync-1m` c4: `995 -> 901 IOPS`, p99
    `12728 -> 11985 us`.
  - `block-writeback-fsync-1m` c16: `1580 -> 1555 IOPS`, p99
    `25767 -> 28866 us`.
  - `block-writeback-prestaged-fsync-1m` c4: `1138 -> 1193 IOPS`, p99
    `10026 -> 7125 us`.
  - `block-writeback-prestaged-fsync-1m` c16: `1568 -> 1719 IOPS`, p99
    `17174 -> 11922 us`.
- Residual watch items: `block-batch-4k-256ops` c16 p99 remains higher despite
  better throughput, and `block-writeback-fsync-1m` c16 p99 remains higher with
  roughly flat throughput. Follow-up profiling should focus on lane wait, sync,
  publish-apply, and map-update slices before Stage 2/3 tuning.

### Stage 2: Packed 4K Journal Records

Stop encoding a group of 4K writes as many self-contained records with repeated
headers. Encode one packed batch:

```text
PackedBlockWrite {
  device_id
  writer_epoch
  first_commit_seq
  entries: [(commit_seq, lba, block_count, payload_offset, integrity)]
  payload_slab
}
```

Deliverables:

- versioned journal record kind;
- CRC over the packed record;
- replay validation for monotonic per-device sequences;
- torn-tail handling;
- fallback for mixed sparse/write batches.

Exit gate:

- Journal frame bytes per 4K commit drop materially under c16/c32.
- Encode nanos per 4K commit drop materially.
- No p99 regression from larger frames.

Stage 2 local checkpoint, 2026-06-19, macOS Docker dev container, baseline
`b874093`, artifacts under ignored Docker `target/loadbench/block-packed-stage2-*`
and extracted summaries under `/private/tmp/block-packed-stage2-*-extract`:

- Added a versioned packed block-journal record for same-device, same-epoch
  groups of eligible single-entry inline writes. The packed record stores one
  compact entry per write (`commit_seq_delta`, LBA, block count, payload offset,
  integrity), a contiguous payload slab, and a CRC32C over the slab. The outer
  durable journal frame still covers the full record with its existing checksum.
- Replay validates checksum, contiguous payload offsets, monotonic commit
  sequences, and expands packed entries before applying flush high-water marks.
- Mixed sparse, segment-ref, multi-entry, cross-device, cross-epoch, and
  singleton writes fall back to the existing record shape.
- Focused tests cover packed round-trip, bad checksum rejection, non-monotonic
  sequence rejection, torn packed tail, mixed packed/non-packed replay order,
  flush high-water preservation, stale-writer fencing, and read-after-write
  before/after reopen.
- Key 5s local c16 deltas, with focused reruns used for the noisy 4K lane rows:
  - `block-batch-4k-16ops`: `3647 -> 5395 IOPS`, p99 `16762 -> 13557 us`.
  - `block-batch-4k-256ops`: `1285 -> 1662 IOPS`, p99 `40449 -> 38794 us`.
  - `block-write-4k-shard-lanes`: `16440 -> 16429 IOPS`, p99
    `1771 -> 1629 us`.
  - `block-write-4k-device-lanes`: `13112 -> 17348 IOPS`, p99
    `2533 -> 1492 us`.
  - `block-writeback-fsync-1m`: `1545 -> 1495 IOPS`, p99
    `34074 -> 28506 us`.
  - `block-writeback-prestaged-fsync-1m`: `1580 -> 1686 IOPS`, p99
    `29487 -> 15419 us`.
  - `block-read-4k`: `80638 -> 88281 IOPS`, p99 `3349 -> 1050 us`.
  - `block-read-1m`: `37888 -> 36640 IOPS`, p99 `1809 -> 1909 us`.
- Profile evidence for `block-write-4k-shard-lanes` c16:
  frame bytes per write boundary `4196 -> 4145` (-1.2%), encode nanos per
  boundary `3460 -> 2467` (-28.7%). Because 4096 payload bytes dominate total
  frame size, the non-payload framing estimate dropped from about 100 bytes to
  about 49 bytes per write.
- Profile evidence for `block-write-4k-device-lanes` c16:
  encode nanos per boundary `4291 -> 3463` (-19.3%); frame bytes were flat
  because those writes route across devices and do not form same-device packed
  groups in this run.
- Residual watch items: `block-writeback-fsync-1m` throughput was -3.2% with
  better p99, and `block-read-1m` was -3.3% with p99 +5.5%; both are within the
  local noise/tolerance band but should be watched on the GCP comparator. Total
  frame-byte reduction should be interpreted against non-payload overhead, not
  against payload-dominated 4K frame bytes.

### Stage 3: Real Lane Group Commit

Make each block journal lane a first-class submit queue instead of relying on
whoever reaches the wait path first.

Deliverables:

- lane state with pending requests, in-flight high-water, completion results;
- first idle waiter becomes batch owner;
- owner drains currently queued records to empty, writes one packed frame, syncs
  once, and completes all covered waiters;
- no timer batching in v1;
- deterministic tests for success, injected append failure, injected sync
  failure, stale lease, and ordering.

Exit gate:

- `block_journal_flush_group_size` increases without p99 regression.
- 4K/64K throughput improves materially on local and GCP runs.

### Stage 4: Data-Log Sync Lanes

For segment-reference writes, separate payload durability from catalog
publication and group payload syncs per storage node.

Deliverables:

- per-node append/sync lane;
- high-water tracking for each data-log file;
- self-describing data-log records sufficient for replay recovery;
- background node-catalog publisher;
- crash recovery that rebuilds missing catalog rows from journal-referenced
  data-log records.

Exit gate:

- `block-writeback-fsync-1m` and `block-batch-4k-256ops` improve or stay within
  tolerance while 4K direct writes do not regress.
- Crash/reopen tests cover queued catalog rows and orphan payload cleanup.

### Stage 5: Journal Materialization

Fold journal high-water into immutable CoW shard roots in the background.

Deliverables:

- materialization planner;
- fork/PITR-aware retention;
- journal pruning only after durable materialization and reachability proof;
- replay tests across compacted and journal-only history.

Exit gate:

- Reopen time stays bounded after long 4K write runs.
- GC never reclaims inline journal payloads or segment refs needed by live
  heads, forks, or PITR windows.

### Stage 6: Direct I/O Backend

Add a provider-private low-level backend for the block journal and data logs.

Candidate path:

- Linux `io_uring`;
- fixed buffers;
- fixed files;
- `O_DIRECT` where feasible;
- polled I/O only if the target hardware supports it well;
- no per-operation allocation on the hot path.

This stage should be optional until the Ceph comparator says the current
filesystem-backed durable provider cannot close the gap.

Exit gate:

- Direct-I/O backend beats the filesystem-backed provider on GCP for 4K/64K
  without harming correctness.
- Durable latency claims are tied to hardware capabilities and measured flush
  semantics.

### Stage 7: SPDK / NVMe-oF Evaluation

Evaluate a separate SPDK-style backend only after the Rust/filesystem/direct-I/O
path is profiled and shown insufficient.

Deliverables:

- isolated prototype, not mixed into the deterministic core;
- same public block API and same replay/materialization contract;
- benchmark against Ceph and the direct-I/O backend;
- explicit operational cost assessment.

Exit gate:

- The SPDK path provides a measured win large enough to justify the additional
  deployment and maintenance complexity.

## Correctness Tests

Add tests as each stage lands:

- stale writer rejected after new whole-device lease;
- acknowledged writes visible before crash but replay only after covering flush;
- flushed writes survive reopen;
- packed record torn tail ignored without exposing partial payload;
- flush marker rejected if it covers missing write records;
- latest overlapping writes win;
- sparse zero/discard reads as zero;
- mixed LBA map and compact tree reads return exact bytes;
- fork captures journal high-water and diverges after later parent writes;
- PITR restore by commit sequence works across compacted and journal-only
  history;
- GC retains journal payloads and segment refs needed by live heads, forks, and
  PITR windows;
- materialization prunes only records proven unreachable.

## Benchmark Gates

Every meaningful performance commit must include before/after numbers.

Minimum local gate:

- `block-write-4k-shard-lanes`;
- `block-write-4k-device-lanes`;
- `block-batch-4k-16ops`;
- `block-batch-4k-256ops`;
- `block-writeback-fsync-1m`;
- `block-writeback-prestaged-fsync-1m`;
- `block-read-4k`;
- `block-read-1m`.

Minimum GCP gate before claiming Ceph competitiveness:

- same source commit for toy and comparator harness;
- Ceph pool fully `active+clean`;
- pool size and resource class recorded;
- toy and Ceph run on the same machine type and disk layout;
- `4k`, `64k`, `256k`, `1m`, `32m` throughput and p99 recorded;
- toy beats Ceph on throughput and p99 for all sizes, with 10% margin for 4K
  and 64K.

## Non-Goals

- Do not fake performance with an in-memory provider.
- Do not weaken `Flushed` durability semantics.
- Do not move native-file append/version/fencing semantics into the block hot
  path.
- Do not require clients to understand storage-node placement.
- Do not add SPDK before lower-risk direct I/O and LBA-map work are measured.
- Do not claim sub-200us p99 without measuring the durable boundary on hardware
  capable of that class of persistence.

## Next Step

Run the GCP comparator from the current pushed branch to measure how much of
the local 4K read-index win carries over. If 4K/64K are still behind Ceph, start
Stage 1 with the LBA map and Stage 2 packed journal records before adding more
scheduling complexity.
