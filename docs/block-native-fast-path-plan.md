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

The first implementation can use a deterministic radix/page-table shape keyed
by logical block number. It should replace the interval overlay read index for
foreground block resolution. Interval history remains in the journal for replay
and materialization, but the hot read/write map should be O(touched blocks), not
O(existing ranges).

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
