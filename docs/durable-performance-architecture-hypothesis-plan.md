# Durable Performance Architecture Hypothesis Plan

## Summary

Interrogate the current architecture review findings with small, measurable
proof steps. The goal is not to add clever concurrency machinery first. The
goal is to find simplifications that reduce durable operations, metadata passes,
local-provider coupling, and publish-tail latency while preserving or improving
durable throughput.

The north-star metrics are:

- `published_mbps` for native file and append-stream workloads.
- Stream publish p50, p99, and max latency.
- End-to-end time from first accepted write through durable visible publish.
- Block flush/writeback p50, p99, and durable bytes per second.
- Durable profile rows and timing for payload sync, storage-node catalog work,
  and metadata publish.

SQLite is a local durable-store implementation, not the interface target. Any
accepted simplification must still make sense for distributed storage nodes and
for a future metadata service backed by something other than SQLite.

## Baseline Gate

Before changing storage architecture code, collect a fresh baseline in a new
output directory:

```bash
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test
docker compose exec dev cargo doc --no-deps
docker compose exec dev cargo bench --bench regression -- --test
```

Native durable-publish matrix:

```bash
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack \
  --rtt-us 200 \
  --concurrency 1,4,16,32 \
  --files 128 \
  --storage-nodes 4 \
  --warmup-ms 0 \
  --stream-total-mib 512 \
  --stream-publish-mib 128 \
  --workloads durable-publish \
  --matrix-csv target/loadbench/durable-architecture-baseline/native/matrix.csv \
  --durable-profile-csv target/loadbench/durable-architecture-baseline/native/durable-profile.csv \
  --append-publish-profile-csv target/loadbench/durable-architecture-baseline/native/append-publish-profile.csv
```

Block durable-boundary matrix:

```bash
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack \
  --rtt-us 200 \
  --concurrency 1,4,16,32 \
  --files 128 \
  --storage-nodes 4 \
  --warmup-ms 0 \
  --workloads block-durable-boundary,native-write-1m \
  --matrix-csv target/loadbench/durable-architecture-baseline/block/matrix.csv \
  --durable-profile-csv target/loadbench/durable-architecture-baseline/block/durable-profile.csv
```

Acceptance for every phase:

- Keep changes only when durable throughput is flat or better and publish or
  flush p99 is flat or better, unless the phase fixes a correctness issue.
- Investigate any `published_mbps`, block durable throughput, or p99 regression
  greater than 10%.
- Keep code only when it removes real complexity, removes a durable operation,
  removes provider-specific leakage, or improves the distributed boundary.

## Hypothesis 1: Benchmark Suite Is No Longer Pointed At The Goal

### Claim

The default loadbench suites and summaries make it too easy to optimize accepted
append throughput while missing durable visible publish throughput and latency.

### Work

- Add a durable-publish suite alias that includes publish-at-end, interval, and
  barrier-at-end stream workloads.
- Add or update a block durable-boundary suite that includes writeback and
  flush-oriented workloads.
- Make summaries foreground `published_mbps`, publish p50/p99/max, append p99,
  block flush p50/p99, and append-publish wait profiles.
- Require fresh CSV output directories or add a schema/version check before
  appending to existing profile files.

### Success

- One command produces the native durable-publish matrix and all needed profile
  CSVs.
- One command produces the block durable-boundary matrix.
- The README and benchmark output no longer foreground misleading aggregate
  `mbps` for stream publish workloads.

### Failure

- If the benchmark changes do not materially improve regression detection,
  keep only the schema-safety fix and document the exact manual commands.

## Hypothesis 2: Auto-Persist Request Handling Hurts Publish Tail

### Claim

Background auto-persist can lose its head start when publish demand exists,
leaving publish-at-end and interval workloads to pay dirty-tail sync work.

### Work

- Add a deterministic test for queued auto-persist requests surviving unrelated
  append-publish demand.
- Fix the worker so skipped requests are retained, requeued, or merged into a
  later prefix target.
- Run the native durable-publish matrix with auto-persist off, 32 MiB, and
  64 MiB.

### Success

- Publish p99 drops or becomes less variable for publish-at-end and interval
  workloads.
- `published_mbps` is flat or better.
- Append p99 does not regress by more than 10%.

### Failure

- If retained auto-persist requests increase contention without improving
  durable publish latency, disable the mode by default and keep only the
  correctness clarification.

## Hypothesis 3: Native Append Runs Should Be Storage-Node Operations

### Claim

The durable coordinator currently writes append-run bytes directly to local
data-log files. That makes the coordinator/store pair act like a storage node
and complicates the future distributed boundary.

### Work

- Add provider-internal storage-node append-run operations that accept ordered
  stream chunks and return append-run receipts or manifests.
- Move data-log file allocation, unsynced append-run writes, and sync metadata
  behind the storage-node/local durable-store implementation.
- Change append publish planning to consume append-run receipts instead of
  reconstructing or inspecting local data-log offsets.
- Preserve the current public native API.

### Success

- Durable coordinator no longer opens or reasons about storage-node data-log
  paths for append runs.
- Publish planning can identify all required payload refs from stream records
  and receipts.
- Publish-at-end p99 is flat or better; `published_mbps` is flat or better.
- The code path maps cleanly to remote storage-node writes and receipt publish.

### Failure

- If the storage-node abstraction adds call overhead without simplifying durable
  publish, keep only a narrow descriptor/receipt extraction and defer transport
  changes.

## Hypothesis 4: Durable Visible Publish Should Be One Internal Primitive

### Claim

Native append publish, native ordinary file flush, block delta flush, and full
metadata persist duplicate the same durable ordering: make payload refs durable,
persist node/catalog evidence, then publish visible metadata.

### Work

- Define a provider-internal durable commit bundle with:
  - payload refs to sync or verify durable;
  - node manifest/catalog updates;
  - visible metadata delta;
  - old and new visibility cursors;
  - idempotency key or durable operation identity.
- Port native append publish to the bundle first.
- Port block delta flush next.
- Keep full metadata export as checkpoint/maintenance, not the common publish
  fallback.

### Success

- At least two current durable persist paths collapse into the shared primitive.
- Foreground publish profile row counts drop.
- Partial-success retry semantics become explicit and testable.
- Publish p99 and block flush p99 are flat or better.

### Failure

- If the shared primitive becomes an abstraction tax, revert to the smallest
  common helper that removes duplicate sync/catalog/metadata ordering code.

## Hypothesis 5: Block Delta And Root Publish Are A Dual Hot Path

### Claim

Block writes currently mutate visible CoW roots and also emit durable block
deltas. That keeps both representations hot and makes durable writeback more
complex than necessary.

### Work

- Measure CPU/profile cost of root path-copy plus block-delta creation in
  batch/writeback workloads.
- Prototype one of two simplifications:
  - block deltas become a write-visible overlay until checkpoint/fold; or
  - block deltas remain durable writeback only, and root publish becomes the
    single optimized visible path.
- Keep the prototype behind a short-lived branch; do not merge both models.

### Success

- One representation is clearly the hot-path source of truth.
- Block writeback p99 and durable throughput improve or stay flat.
- Code size and state transitions shrink.

### Failure

- If neither model improves latency or simplicity, document why the dual path is
  buying enough correctness or read-path simplicity to keep.

## Hypothesis 6: Zero And Discard Should Be Metadata-Only Deltas

### Claim

Zero/discard operations should not allocate full zero payloads or force a full
durable persist path.

### Work

- Add a zero/hole variant to the block-delta representation.
- Make `write_zeroes` and discard-like operations publish metadata-only extents.
- Fold zero/hole deltas into checkpoints and read planning.
- Add deterministic tests for overlapping data, zero, and later writes.

### Success

- Zero/discard durable latency drops materially.
- No data-log payload bytes are written for zero-only ranges.
- Read behavior and replay are unchanged.

### Failure

- If zero/hole semantics complicate metadata more than they save, keep the
  representation local to block deltas and do not generalize it into native
  file extents yet.

## Hypothesis 7: Native File Leaves Are Too Block-Shaped

### Claim

Native file metadata stores segment entries and append-run byte extents side by
side, then reconciles overlaps during read planning. A unified byte-extent leaf
could remove reconciliation and simplify append publish.

### Work

- Model a native leaf containing one ordered byte-extent list with extent kinds:
  segment-backed, append-run-backed, and hole.
- Compare publish planning and read planning complexity against the current
  `entries` plus `run_extents` design.
- Prototype only if the model removes overlap trimming or duplicate receipt
  validation.

### Success

- Read planning no longer has to merge block entries and append-run extents.
- Append publish creates fewer metadata objects or fewer passes.
- Native publish p99 and read throughput are flat or better.

### Failure

- If unified extents slow block-like native writes or bloat metadata, keep the
  current layout and document the reason.

## Hypothesis 8: Reopen Should Load Descriptors, Not Payload Bytes

### Claim

Durable reopen should scale with metadata and catalog descriptors, not live
payload bytes.

### Work

- Change durable reopen to hydrate segment and append-run descriptors, placement
  indexes, and catalogs without reading full payloads.
- Route reads through data-log-backed descriptors lazily.
- Add an optional verification mode that reads payloads when explicitly asked.

### Success

- Reopen time and memory scale with object count and metadata, not live bytes.
- Read behavior remains identical.
- The path maps cleanly to remote storage nodes.

### Failure

- If lazy descriptors break existing local invariants, first split local
  in-memory segment records into descriptor plus optional cached bytes.

## Hypothesis 9: Placement Lookup Should Not Scan Storage Nodes

### Claim

Segment owner lookup currently scans node catalogs. That is acceptable for a
small local toy provider but wrong for distributed reads and releases.

### Work

- Add a provider-private placement directory keyed by segment or append-run ref.
- Populate it from verified receipts/manifests.
- Use it for read planning, release/reference operations, and durable export.

### Success

- Read planning and release/reference paths become O(extents) rather than
  O(extents * storage_nodes).
- Code no longer needs repeated catalog probes to route a known ref.

### Failure

- If maintaining the index adds more complexity than it removes locally, keep it
  as a remote-provider requirement and leave the local implementation simple.

## Hypothesis 10: Durable Partial-Success Semantics Need Idempotency

### Claim

Some operations mutate local state before durable persist, while append publish
persists first and applies locally afterward. Distributed retries need a single
answer for whether an operation committed despite an error.

### Work

- Add operation identities to durable visible commit bundles.
- Make post-persist local apply idempotent or replayable from the durable
  result.
- Add tests for persist succeeds but local apply is interrupted, persist fails
  before metadata visibility, and retry after ambiguous error.

### Success

- Retrying a durable operation either returns the committed result or repeats
  safely without exposing partial metadata.
- Append publish, block flush, and native file flush share the same failure
  vocabulary.

### Failure

- If full idempotency is too large, document the current partial-success
  contracts and add targeted tests for the most dangerous publish path first.

## Recommended Execution Order

1. Fix benchmark suite and profile reporting.
2. Fix auto-persist request retention and measure publish-tail impact.
3. Move append-run payload writes behind storage-node receipts.
4. Introduce the shared durable visible commit bundle for native append publish.
5. Port block delta flush to the shared durable commit bundle.
6. Add metadata-only zero/discard block deltas.
7. Decide whether block deltas or CoW roots are the block hot-path source of
   truth.
8. Investigate native unified byte extents.
9. Make durable reopen descriptor-only.
10. Add placement directory and durable idempotency where the earlier phases
    prove the need.

## Final Exit Criteria

This investigation succeeds if it produces one of these outcomes:

- A smaller durable write/publish architecture with equal or better native
  `published_mbps`, publish p99, block durable throughput, and block flush p99.
- A measured reason to keep the current architecture, plus benchmark coverage
  that makes future durable-boundary regressions obvious.

This investigation fails if:

- The work only adds scheduling/concurrency machinery without reducing durable
  operations or metadata passes.
- The accepted path optimizes local SQLite behavior in a way that does not map
  to storage nodes and a remote metadata service.
- Durable throughput or p99 regressions over 10% remain unexplained.
