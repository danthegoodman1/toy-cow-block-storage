# Concurrent Metadata Provider Plan

Status: implemented for the block metadata proof backend  
Scope: block metadata publish scalability diagnosis, production-shaped metadata
transaction model, loadbench comparison backend  
Goal: prove whether the block metadata architecture scales when the metadata
service can commit independent keys concurrently, without optimizing around
SQLite.

## Summary

SQLite is still valuable as the local crash/reopen correctness provider, but it
is the wrong object to tune when the intended production shape is closer to a
metadata service with transactional key-level conflict detection. The next step
is a small in-memory metadata-service simulator with two execution modes:

- `txn-serial`: one global transaction lock, same key/CAS semantics.
- `txn-sharded`: key-sharded transaction locks, conflict only on touched keys.

The pair gives a clean before/after that removes SQLite from the question:

```text
same shard contention          -> should conflict in both modes
same device, different shards  -> should scale only in txn-sharded
different devices              -> should look like independent keys
commit sequencing              -> measured separately, not hidden in a mutex
```

If `txn-sharded` improves same-device/different-shard block writes while
same-shard remains contended, the block metadata layout is sound and SQLite was
the durable-adapter ceiling. If it does not improve, the bottleneck is still in
the coordinator/tree/segment path and native keyspace row splitting should wait.

The implemented backend answered the first diagnostic question: sharded
metadata transactions reduce measured metadata lock wait, but they do not
materially improve same-device/different-shard block write throughput versus
the serial transaction backend. Different-device 4 KiB lanes improve more than
same-device shard lanes, which means the next performance target is not another
native/keyspace row split. The next target should be the shared coordinator,
metadata-tree path-copy, and segment-write path that still runs before or around
the metadata transaction.

## Architecture

Add an internal transaction-shaped metadata store, not a durable database:

```text
MetadataTxnStore {
  read(keys) -> versioned values
  commit(read_versions, writes) -> commit_version | conflict
}
```

Keys are production-shaped logical metadata keys:

```text
block/device/{device_id}/manifest
block/device/{device_id}/shard/{shard_id}/head
block/device/{device_id}/timeline/{commit_version}/{ordinal}
commit/{commit_group_id}
```

Values are current durable row payloads or equivalent strongly typed encoded
rows. Each key has a key-local version. A transaction succeeds only if every
read key still has the observed version; otherwise it fails with conflict.

Implement two stores behind the same trait:

- `SerialTxnStore`: one mutex around the whole map. This is the non-concurrent
  control and should behave like a clean metadata service with a bad scheduler.
- `ShardedTxnStore`: fixed shard count, key hash to shard, transaction locks
  touched shards in sorted order, validates read versions, applies writes, and
  releases. No background workers, no async, no wall clock in core logic.

Commit versions come from a narrow allocator that is measured separately. For
v1, use an atomic monotonic counter outside the transaction mutexes; record
`commit_version_alloc_nanos` so we can see if it becomes a real convergence
point. This is close enough to an FDB-style commit version for benchmark
diagnosis without pulling in a distributed dependency.

## Implementation Changes

- Add the transaction store and profile types in a small metadata-service
  module:
  - transaction lock wait nanos
  - read validation nanos
  - apply/write nanos
  - commit version allocation nanos
  - touched key shard count
  - read key count
  - write key count
  - conflict count

- Add a block-focused metadata plane backed by `MetadataTxnStore`.
  - Create devices by writing one manifest key plus one shard-head key per
    shard.
  - Read logical `DeviceHead` by reading manifest plus shard-head keys.
  - Publish block commit groups by reading only touched shard-head keys,
    validating old roots, writing touched shard-head keys, and writing commit
    records.
  - Keep multi-shard writes atomic inside one transaction.
  - Keep same-shard stale-root failure identical to the current provider.

- Add loadbench provider modes:
  - `--provider txn-serial`
  - `--provider txn-sharded`
  - `--metadata-profile-csv PATH`

- Keep scope intentionally narrow:
  - Block workloads only for v1.
  - No crash/reopen durability.
  - No native keyspace rewrite.
  - No RocksDB/Fjall/FDB adapter yet.
  - SQLite durable provider remains the correctness/recovery provider.

Implemented evidence lives in `target/loadbench/metadata-txn-final/`:

- `local.csv`
- `serial.csv`
- `sharded.csv`
- `serial-profile.csv`
- `sharded-profile.csv`

These files are raw benchmark artifacts and remain ignored under `target/`.

## Benchmark Protocol

Run the current local provider as the baseline, then the two transaction modes.
Save raw CSV under ignored `target/loadbench/metadata-txn-<stage>/`.

```sh
docker compose up -d dev
docker compose exec dev cargo fmt --check
docker compose exec dev cargo clippy --all-targets --all-features -- -D warnings
docker compose exec dev cargo test

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider local \
  --durability ack \
  --workloads block-metadata \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16,64,128 \
  --device-blocks 1048576 \
  --shards 64 \
  --rtt-us 200 \
  --delay-mode spin \
  > target/loadbench/metadata-txn-stage0/local.csv

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider txn-serial \
  --durability ack \
  --workloads block-metadata \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16,64,128 \
  --device-blocks 1048576 \
  --shards 64 \
  --rtt-us 200 \
  --delay-mode spin \
  --metadata-profile-csv target/loadbench/metadata-txn-stage1/serial-profile.csv \
  > target/loadbench/metadata-txn-stage1/serial.csv

docker compose exec dev cargo run --release --bin loadbench -- \
  --provider txn-sharded \
  --durability ack \
  --workloads block-metadata \
  --duration-ms 1000 \
  --warmup-ms 100 \
  --concurrency 1,4,16,64,128 \
  --device-blocks 1048576 \
  --shards 64 \
  --rtt-us 200 \
  --delay-mode spin \
  --metadata-profile-csv target/loadbench/metadata-txn-stage1/sharded-profile.csv \
  > target/loadbench/metadata-txn-stage1/sharded.csv

docker compose down
```

Report the matrix by workload and concurrency:

- success IOPS, attempt IOPS, MB/s
- p50/p90/p99/max latency
- errors/conflicts
- transaction lock wait p50/p90/p99
- read validation p50/p90/p99
- apply/write p50/p90/p99
- commit version allocation p50/p90/p99
- touched key shards per transaction
- read/write key counts

## Success Criteria

- Same-shard contended workload has conflicts in both transaction modes.
- Same-shard serialized workload has no conflicts and does not scale
  meaningfully with sharded metadata.
- Same-device/different-shard workload improves materially in `txn-sharded`
  versus `txn-serial`, especially at c16/c64/c128.
- Different-device workload is not materially faster than same-device/different
  shard under `txn-sharded`; if it is, there is still a hidden device-level
  convergence point.
- Commit version allocation p99 remains low enough that it is not the dominant
  c64/c128 bottleneck.
- If `txn-sharded` does not improve, do not split native keyspace metadata yet;
  investigate coordinator locks, metadata tree path-copy cost, segment writes,
  payload integrity, or loadbench worker behavior.

## Correctness Tests

- Same-shard stale-root publish fails and preserves old contents.
- Same-device different-shard publishes merge without conflict.
- Multi-shard write publishes all touched shard heads atomically.
- A failed transaction exposes no partial shard-head updates.
- Fork copies shard-head references without walking metadata trees.
- PITR replay from transaction timeline reconstructs historical shard roots.
- Delete and GC roots include live heads, retained PITR roots, and retained
  deleted-device roots.
- Serial and sharded transaction stores pass the same deterministic block
  provider conformance tests.
- Transaction conflict tests cover stale, duplicate, delayed, reordered, and
  concurrent commits.
- Profile output is empty when disabled and contains sane per-transaction rows
  when enabled.

## Native Keyspace Decision Gate

Only plan the native keyspace equivalent after the transaction backend answers
the block question.

Apply the same design to native keyspaces if:

- block same-device/different-shard throughput improves materially under
  `txn-sharded`;
- different-device and same-device/different-shard behave similarly;
- profiles show low commit-version overhead and low non-conflicting lock wait.

Skip native keyspace splitting for now if:

- `txn-sharded` does not materially improve block shard-lane throughput;
- profiles show the bottleneck outside metadata transactions;
- commit version allocation becomes the dominant shared convergence point.

If native splitting is justified, the likely shape is keyspace manifest plus
per-file or per-catalog-shard head keys, with ordinary file writes touching only
the file/catalog shard they update and PITR driven by append-only per-key
timeline rows.

## Assumptions

- This is a benchmark and architecture proof backend, not a durable provider.
- SQLite stays as the local recovery/correctness adapter.
- The transaction backend should be deterministic and small enough to simulate.
- No compatibility shims or dual public APIs are added.
- A real Fjall/RocksDB/FDB adapter should implement the same transaction-shaped
  contract later, after the in-memory proof identifies the right key model.
