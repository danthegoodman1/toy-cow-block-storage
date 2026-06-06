# Durable Performance Baseline Results

## 2026-06-06 Baseline

This baseline was collected after adding durable-boundary workload aliases and
CSV schema validation, before changing storage architecture code.

Raw artifacts live in the dev container Cargo target volume:

- `target/loadbench/durable-architecture-baseline/native/matrix.csv`
- `target/loadbench/durable-architecture-baseline/native/durable-profile.csv`
- `target/loadbench/durable-architecture-baseline/native/append-publish-profile.csv`
- `target/loadbench/durable-architecture-baseline/block/matrix.csv`
- `target/loadbench/durable-architecture-baseline/block/durable-profile.csv`

Gate:

- `cargo fmt --check`: pass
- `cargo clippy --all-targets --all-features -- -D warnings`: pass
- `cargo test`: pass, 259 library tests and 25 loadbench tests
- `cargo doc --no-deps`: pass
- `cargo bench --bench regression -- --test`: pass

## Native Durable Publish

Command shape:

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
  --workloads durable-publish
```

Selected results:

| Workload | c | `published_mbps` | publish p99 | append p99 |
| --- | ---: | ---: | ---: | ---: |
| interval 4m | 1 | 1500.83 | 44.6 ms | 2.4 ms |
| interval 4m | 4 | 3054.09 | 157.2 ms | 4.0 ms |
| interval 4m | 16 | 3053.82 | 630.7 ms | 9.4 ms |
| interval 4m | 32 | 2897.55 | 1494.5 ms | 27.1 ms |
| interval 32m | 1 | 1319.65 | 41.7 ms | 17.2 ms |
| interval 32m | 4 | 2965.59 | 178.7 ms | 18.6 ms |
| interval 32m | 16 | 2988.98 | 730.8 ms | 41.3 ms |
| interval 32m | 32 | 3048.71 | 1356.2 ms | 116.1 ms |
| at-end 4m | 1 | 1582.17 | 153.9 ms | 2.3 ms |
| at-end 4m | 4 | 2575.33 | 628.8 ms | 2.7 ms |
| at-end 4m | 16 | 2858.38 | 2385.7 ms | 36.6 ms |
| at-end 4m | 32 | 3233.07 | 4236.6 ms | 68.2 ms |
| at-end 32m | 1 | 1395.71 | 145.6 ms | 17.5 ms |
| at-end 32m | 4 | 2523.41 | 609.4 ms | 17.8 ms |
| at-end 32m | 16 | 2915.02 | 2318.0 ms | 162.6 ms |
| at-end 32m | 32 | 3239.01 | 4352.4 ms | 1550.8 ms |
| barrier-at-end 4m | 1 | 1577.83 | 158.3 ms | 2.1 ms |
| barrier-at-end 4m | 4 | 2630.28 | 617.0 ms | 2.3 ms |
| barrier-at-end 4m | 16 | 2856.27 | 2394.4 ms | 19.1 ms |
| barrier-at-end 4m | 32 | 3251.99 | 2737.4 ms | 76.9 ms |
| barrier-at-end 32m | 1 | 1395.00 | 147.3 ms | 16.9 ms |
| barrier-at-end 32m | 4 | 2478.84 | 625.2 ms | 16.7 ms |
| barrier-at-end 32m | 16 | 2922.49 | 2264.7 ms | 97.1 ms |
| barrier-at-end 32m | 32 | 3267.33 | 2481.1 ms | 1327.7 ms |

Interpretation:

- Durable throughput is strongest at high concurrency, roughly 3.2 GB/s for
  at-end and barrier-at-end shapes.
- Publish tail latency is the main problem. At-end publish p99 rises from
  roughly 146-154 ms at c1 to roughly 4.2-4.4 s at c32.
- Append tail is usually much lower than publish tail, except the 32 MiB c32
  at-end and barrier-at-end rows where append p99 also rises above 1 s.

## Block Durable Boundary

Command shape:

```bash
docker compose exec dev cargo run --release --bin loadbench -- \
  --provider durable \
  --durability ack \
  --rtt-us 200 \
  --concurrency 1,4,16,32 \
  --files 128 \
  --storage-nodes 4 \
  --warmup-ms 0 \
  --workloads block-durable-boundary,native-write-1m
```

Selected writeback results:

| Workload | c | durable MB/s | p99 |
| --- | ---: | ---: | ---: |
| writeback 1m | 1 | 338.91 | 5.3 ms |
| writeback 1m | 4 | 523.09 | 13.1 ms |
| writeback 1m | 16 | 1016.81 | 21.9 ms |
| writeback 1m | 32 | 1271.22 | 39.2 ms |
| writeback 4m | 1 | 573.47 | 10.0 ms |
| writeback 4m | 4 | 1071.76 | 24.8 ms |
| writeback 4m | 16 | 1431.66 | 75.2 ms |
| writeback 4m | 32 | 1475.12 | 128.0 ms |
| writeback 16m | 1 | 802.14 | 32.7 ms |
| writeback 16m | 4 | 1240.89 | 65.9 ms |
| writeback 16m | 16 | 1490.66 | 204.2 ms |
| writeback 16m | 32 | 1465.02 | 413.9 ms |
| prestaged 1m | 1 | 408.64 | 4.2 ms |
| prestaged 1m | 4 | 963.18 | 5.6 ms |
| prestaged 1m | 16 | 1589.39 | 15.6 ms |
| prestaged 1m | 32 | 1536.85 | 25.3 ms |
| prestaged 4m | 1 | 798.88 | 3.1 ms |
| prestaged 4m | 4 | 1228.60 | 19.2 ms |
| prestaged 4m | 16 | 1487.85 | 47.5 ms |
| prestaged 4m | 32 | 1619.50 | 99.4 ms |
| prestaged 16m | 1 | 979.40 | 8.6 ms |
| prestaged 16m | 4 | 1353.40 | 40.1 ms |
| prestaged 16m | 16 | 1547.82 | 186.1 ms |
| prestaged 16m | 32 | 1555.48 | 370.5 ms |

Controls:

| Workload | c | MB/s | durable MB/s | published MB/s | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| block batch fsync interval | 1 | 1551.81 | 0.00 | 1551.81 | 116.2 ms |
| block batch fsync interval | 4 | 2854.41 | 0.00 | 2854.41 | 236.7 ms |
| block batch fsync interval | 16 | 2521.04 | 0.00 | 2521.04 | 903.3 ms |
| block batch fsync interval | 32 | 2317.04 | 0.00 | 2317.04 | 2009.9 ms |
| native write 1m ack | 1 | 1506.48 | 0.00 | 0.00 | 0.9 ms |
| native write 1m ack | 4 | 5940.09 | 0.00 | 0.00 | 0.9 ms |
| native write 1m ack | 16 | 11998.72 | 0.00 | 0.00 | 3.6 ms |
| native write 1m ack | 32 | 11102.50 | 0.00 | 0.00 | 12.5 ms |

Interpretation:

- Prestaging improves block writeback p99 and durable throughput in most rows.
- Larger writeback windows increase p99 materially at high concurrency.
- The `block-batch-fsync-interval` rows currently report bytes in
  `published_mbps`, not `durable_mbps`; use this consistently when comparing
  that workload until the counter semantics are tightened.

## Auto-Persist Threshold Follow-Up

After fixing auto-persist request retention under append-publish pressure, the
native durable-publish matrix was repeated with:

- `--stream-auto-persist-mib 32`
- `--stream-auto-persist-mib 64`

Raw artifacts:

- `target/loadbench/durable-architecture-auto32/native/matrix.csv`
- `target/loadbench/durable-architecture-auto32/native/durable-profile.csv`
- `target/loadbench/durable-architecture-auto32/native/append-publish-profile.csv`
- `target/loadbench/durable-architecture-auto64/native/matrix.csv`
- `target/loadbench/durable-architecture-auto64/native/durable-profile.csv`
- `target/loadbench/durable-architecture-auto64/native/append-publish-profile.csv`

Selected comparison:

| Workload | c | mode | `published_mbps` | publish p99 | append p99 |
| --- | ---: | --- | ---: | ---: | ---: |
| interval 32m | 32 | off | 3048.71 | 1356.2 ms | 116.1 ms |
| interval 32m | 32 | auto32 | 3298.28 | 1210.9 ms | 115.9 ms |
| interval 32m | 32 | auto64 | 3309.44 | 1188.1 ms | 117.6 ms |
| at-end 4m | 32 | off | 3233.07 | 4236.6 ms | 68.2 ms |
| at-end 4m | 32 | auto32 | 3253.92 | 1610.3 ms | 419.5 ms |
| at-end 4m | 32 | auto64 | 3508.05 | 3660.6 ms | 225.4 ms |
| at-end 32m | 32 | off | 3239.01 | 4352.4 ms | 1550.8 ms |
| at-end 32m | 32 | auto32 | 3318.50 | 1900.3 ms | 2198.2 ms |
| at-end 32m | 32 | auto64 | 3839.03 | 2133.6 ms | 1133.3 ms |
| barrier-at-end 4m | 32 | off | 3251.99 | 2737.4 ms | 76.9 ms |
| barrier-at-end 4m | 32 | auto32 | 3410.11 | 1488.9 ms | 874.4 ms |
| barrier-at-end 4m | 32 | auto64 | 3342.82 | 3426.6 ms | 164.3 ms |
| barrier-at-end 32m | 32 | off | 3267.33 | 2481.1 ms | 1327.7 ms |
| barrier-at-end 32m | 32 | auto32 | 3199.70 | 1764.3 ms | 1616.2 ms |
| barrier-at-end 32m | 32 | auto64 | 3088.23 | 2352.3 ms | 1895.2 ms |

Interpretation:

- The request-retention fix is a correctness improvement: queued background
  persist targets are no longer drained and lost merely because foreground
  publish demand is active.
- Auto-persist improves many publish-tail rows. The strongest examples are
  at-end 4m c32 with auto32 and at-end 32m c32 with auto32 or auto64.
- Auto-persist is still not a complete latency answer. It can move tail latency
  into append, especially for large chunk/high-concurrency rows, and some
  barrier-at-end rows regress with auto64.
- Next optimization should simplify/coalesce auto-persist scheduling around
  durable publish demand, not add a pile of concurrency knobs.

## Rejected Auto-Persist Bounded-Target Experiment

A smaller provider-private policy was tested and rejected: when dirty private
bytes exceeded the auto-persist threshold, request only one threshold-sized
prefix instead of the whole accepted contiguous tail. The hypothesis was that
smaller background sync groups would reduce append-tail interference while
keeping enough head start for final publish.

Targeted c32 artifacts:

- `target/loadbench/durable-architecture-auto32-bounded/native/matrix.csv`
- `target/loadbench/durable-architecture-auto64-bounded/native/matrix.csv`

Selected c32 comparison:

| Workload | mode | `published_mbps` | publish p99 | append p99 |
| --- | --- | ---: | ---: | ---: |
| at-end 4m | auto32 whole-tail | 3253.92 | 1610.3 ms | 419.5 ms |
| at-end 4m | auto32 bounded | 3221.75 | 2795.5 ms | 645.4 ms |
| at-end 32m | auto32 whole-tail | 3318.50 | 1900.3 ms | 2198.2 ms |
| at-end 32m | auto32 bounded | 2342.77 | 2140.6 ms | 4266.4 ms |
| barrier-at-end 4m | auto32 whole-tail | 3410.11 | 1488.9 ms | 874.4 ms |
| barrier-at-end 4m | auto32 bounded | 3456.94 | 2028.9 ms | 437.0 ms |
| barrier-at-end 32m | auto32 whole-tail | 3199.70 | 1764.3 ms | 1616.2 ms |
| barrier-at-end 32m | auto32 bounded | 3226.85 | 910.9 ms | 2661.1 ms |
| at-end 4m | auto64 whole-tail | 3508.05 | 3660.6 ms | 225.4 ms |
| at-end 4m | auto64 bounded | 3494.19 | 3721.7 ms | 217.7 ms |
| at-end 32m | auto64 whole-tail | 3839.03 | 2133.6 ms | 1133.3 ms |
| at-end 32m | auto64 bounded | 3256.35 | 2079.6 ms | 1993.8 ms |
| barrier-at-end 32m | auto64 whole-tail | 3088.23 | 2352.3 ms | 1895.2 ms |
| barrier-at-end 32m | auto64 bounded | 3862.90 | 1081.6 ms | 1226.7 ms |

The bounded policy improved a few rows, but it badly hurt important at-end
rows, especially auto32 at-end 32m c32. It leaves too much dirty tail for final
publish and is not worth keeping as the default policy.

## Hypothesis 3 Manifest-Boundary Proof Step

The first Hypothesis 3 proof step moved append-run log-ref to pending-manifest
selection behind `DurableSqliteStore`. The durable coordinator still owns
stream ordering and publish planning, but it no longer reconstructs append-run
data-log manifests by building storage-node data-log paths directly in the
prefix-persist and append-publish foreground paths.

Raw artifacts:

- `target/loadbench/durable-architecture-h3-manifest-boundary/native-noauto/matrix.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-noauto/durable-profile.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-noauto/append-publish-profile.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-noauto-repeat/matrix.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-auto32/matrix.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-auto32/durable-profile.csv`
- `target/loadbench/durable-architecture-h3-manifest-boundary/native-auto32/append-publish-profile.csv`

Selected c32 results:

| Workload | mode | `published_mbps` | publish p99 | append p99 |
| --- | --- | ---: | ---: | ---: |
| at-end 4m | baseline off | 3233.07 | 4236.6 ms | 68.2 ms |
| at-end 4m | H3 off | 3068.02 | 4098.7 ms | 191.5 ms |
| at-end 4m | baseline auto32 | 3253.92 | 1610.3 ms | 419.5 ms |
| at-end 4m | H3 auto32 | 3284.79 | 3280.5 ms | 450.4 ms |
| at-end 32m | baseline off | 3239.01 | 4352.4 ms | 1550.8 ms |
| at-end 32m | H3 off | 2983.36 | 4600.5 ms | 1835.1 ms |
| at-end 32m | H3 off repeat | 3517.58 | 3383.6 ms | 1234.1 ms |
| at-end 32m | baseline auto32 | 3318.50 | 1900.3 ms | 2198.2 ms |
| at-end 32m | H3 auto32 | 3356.93 | 3139.7 ms | 1235.2 ms |
| barrier-at-end 32m | baseline off | 3267.33 | 2481.1 ms | 1327.7 ms |
| barrier-at-end 32m | H3 off | 3046.62 | 2554.5 ms | 1506.6 ms |
| barrier-at-end 32m | baseline auto32 | 3199.70 | 1764.3 ms | 1616.2 ms |
| barrier-at-end 32m | H3 auto32 | 3483.30 | 1612.0 ms | 1566.6 ms |
| interval 32m | baseline off | 3048.71 | 1356.2 ms | 116.1 ms |
| interval 32m | H3 off | 2962.32 | 1406.7 ms | 232.6 ms |
| interval 32m | baseline auto32 | 3298.28 | 1210.9 ms | 115.9 ms |
| interval 32m | H3 auto32 | 3106.66 | 1267.2 ms | 236.4 ms |

Interpretation:

- The boundary split is primarily a simplification, not a clear performance
  breakthrough. It removes local-provider path reconstruction from the
  coordinator and keeps the storage-node-shaped manifest selection inside the
  durable store.
- The no-auto at-end 32m row had visible run-to-run variance: the first H3 run
  was below baseline, while the repeat was above baseline with lower publish
  p99. Treat that row as neutral-to-positive, not a precise win claim.
- Auto32 remains mixed. H3 auto32 improves at-end 32m throughput and append p99
  versus the earlier auto32 run, but publish p99 is worse than the earlier
  auto32 outlier. This reinforces the previous conclusion that auto-persist
  scheduling is still not the complete tail-latency answer.

## Hypothesis 4 Native Delta Bundle Proof Step

The first Hypothesis 4 proof step collapsed ordinary native metadata-delta
persist and append-publish metadata-delta persist into one provider-internal
native delta bundle. The bundle owns:

- data-log payload sync for new segment payloads or preingested append-run refs;
- node catalog publication, including append-run manifest-only publication for
  storage nodes with no segment catalog rows in the delta;
- visible native metadata delta persistence.

Raw artifacts:

- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-noauto/matrix.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-noauto/durable-profile.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-noauto/append-publish-profile.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-noauto-repeat/matrix.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-auto32/matrix.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-auto32/durable-profile.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-auto32/append-publish-profile.csv`
- `target/loadbench/durable-architecture-h4-native-delta-bundle/native-auto32-repeat/matrix.csv`

Selected c32 results:

| Workload | mode | `published_mbps` | publish p99 | append p99 |
| --- | --- | ---: | ---: | ---: |
| at-end 32m | H3 off repeat | 3517.58 | 3383.6 ms | 1234.1 ms |
| at-end 32m | H4 off | 3001.63 | 4630.6 ms | 1352.0 ms |
| at-end 32m | H4 off repeat | 3771.49 | 3297.8 ms | 1098.4 ms |
| at-end 32m | H3 auto32 | 3356.93 | 3139.7 ms | 1235.2 ms |
| at-end 32m | H4 auto32 | 3277.78 | 4240.8 ms | 1288.9 ms |
| at-end 32m | H4 auto32 repeat | 3309.76 | 3328.3 ms | 1212.1 ms |
| barrier-at-end 32m | H3 off | 3046.62 | 2554.5 ms | 1506.6 ms |
| barrier-at-end 32m | H4 off | 3223.30 | 2737.9 ms | 1297.0 ms |
| barrier-at-end 32m | H3 auto32 | 3483.30 | 1612.0 ms | 1566.6 ms |
| barrier-at-end 32m | H4 auto32 | 3447.03 | 921.8 ms | 1665.4 ms |
| interval 32m | H3 off | 2962.32 | 1406.7 ms | 232.6 ms |
| interval 32m | H4 off | 3031.72 | 1389.7 ms | 120.2 ms |
| interval 32m | H3 auto32 | 3106.66 | 1267.2 ms | 236.4 ms |
| interval 32m | H4 auto32 | 3315.78 | 1176.5 ms | 449.1 ms |

Interpretation:

- The native bundle is worth keeping as a simplification step: two native
  durable metadata-delta paths now share one provider-internal ordering path,
  and the storage-node manifest-only case remains below the durable-store
  boundary.
- It is not yet a complete H4 success. Foreground profile row counts have not
  dropped; block delta flush has not moved onto the bundle; and auto32 at-end
  remains mixed.
- The next H4 proof should port block delta flush into the same bundle or
  explicitly reject that merge if it adds abstraction tax without reducing
  durable operations.

## Hypothesis 4 Block Payload/Catalog Bundle Proof Step

The second Hypothesis 4 proof step moved block delta flush onto the same
payload-sync and node-catalog publication bundle used by native metadata-delta
publishes. Block delta rows remain a block-specific visible commit type; this
step deliberately shares only the durable payload/catalog ordering that maps to
storage nodes.

Raw artifacts:

- `target/loadbench/durable-architecture-h4-payload-catalog-bundle/block/matrix.csv`
- `target/loadbench/durable-architecture-h4-payload-catalog-bundle/block/durable-profile.csv`
- `target/loadbench/durable-architecture-h4-payload-catalog-bundle/block-repeat/matrix.csv`
- `target/loadbench/durable-architecture-h4-payload-catalog-bundle/block-repeat/durable-profile.csv`

Selected results:

| Workload | c | baseline MB/s | baseline p99 | H4 MB/s | H4 p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| writeback 1m | 16 | 1016.81 | 21.9 ms | 1524.99 | 20.4 ms |
| writeback 1m | 32 | 1271.22 | 39.2 ms | 1450.76 | 38.7 ms |
| writeback 4m | 16 | 1431.66 | 75.2 ms | 1556.32 | 72.1 ms |
| writeback 4m | 32 | 1475.12 | 128.0 ms | 1574.99 | 139.0 ms |
| writeback 16m | 16 | 1490.66 | 204.2 ms | 1520.20 | 214.9 ms |
| writeback 16m | 32 | 1465.02 | 413.9 ms | 1570.06 | 415.2 ms |
| prestaged 1m | 16 | 1589.39 | 15.6 ms | 1506.81 | 21.9 ms |
| prestaged 1m | 32 | 1536.85 | 25.3 ms | 1420.43 | 28.9 ms |
| prestaged 1m repeat | 32 | 1536.85 | 25.3 ms | 1421.53 | 30.3 ms |
| prestaged 4m | 16 | 1487.85 | 47.5 ms | 1490.04 | 52.5 ms |
| prestaged 4m | 32 | 1619.50 | 99.4 ms | 1698.26 | 100.2 ms |
| prestaged 16m | 16 | 1547.82 | 186.1 ms | 1476.49 | 186.5 ms |
| prestaged 16m | 32 | 1555.48 | 370.5 ms | 1514.06 | 381.4 ms |

Prestaged 1m c32 was investigated because p99 regressed by more than 10%.
Aggregating durable profile rows showed no added durable operation cost:

| Profile | rows | total profile time | file sync time | files synced | sync bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline prestaged 1m c32 | 143 | 842.0 ms | 584.7 ms | 496 | 17.23 GiB |
| H4 prestaged 1m c32 repeat | 145 | 809.5 ms | 564.2 ms | 505 | 15.45 GiB |

Interpretation:

- The block payload/catalog bundle is worth keeping as a simplification:
  native metadata deltas, append publish, and block delta flush now share the
  durable payload/catalog phases while preserving their distinct visible commit
  records.
- The block writeback rows are mostly flat or better. The prestaged 1m c32 p99
  row is worse, but profile aggregation does not show extra durable operations;
  treat it as a short-row latency variance to watch in the next full matrix.
- H4 is partially answered: shared payload/catalog ordering is useful. The
  visible commit step remains intentionally separate for native deltas and block
  delta rows unless a future proof shows a lower-level idempotency primitive can
  simplify both without abstraction tax.

## Hypothesis 5 Block Dual Hot Path Measurement

Hypothesis 5 asks whether block writes should stop publishing visible CoW roots
and durable block-delta rows on the same hot path. Before attempting a visible
overlay prototype, a focused block durable-boundary run measured existing
operation splits with `--block-batch-profile-csv`.

Raw artifacts:

- `target/loadbench/durable-architecture-h5-dual-hot-path/block/matrix.csv`
- `target/loadbench/durable-architecture-h5-dual-hot-path/block/durable-profile.csv`
- `target/loadbench/durable-architecture-h5-dual-hot-path/block/block-batch-profile.csv`
- `target/loadbench/durable-architecture-h5-dual-hot-path/local-block/matrix.csv`
- `target/loadbench/durable-architecture-h5-dual-hot-path/local-block/block-batch-profile.csv`

Selected durable c16/c32 operation-profile aggregates:

| Workload | c | avg commit prep | p99 commit prep | avg flush | p99 flush | commit share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| writeback 1m | 16 | 2.1 ms | 5.3 ms | 8.7 ms | 18.2 ms | 19.1% |
| writeback 1m | 32 | 6.4 ms | 16.2 ms | 15.6 ms | 27.6 ms | 29.1% |
| writeback 4m | 16 | 13.8 ms | 42.2 ms | 31.0 ms | 57.2 ms | 30.7% |
| writeback 4m | 32 | 28.1 ms | 81.0 ms | 60.8 ms | 115.7 ms | 31.5% |
| writeback 16m | 16 | 59.0 ms | 140.6 ms | 123.9 ms | 208.0 ms | 32.1% |
| writeback 16m | 32 | 105.5 ms | 246.4 ms | 206.9 ms | 359.3 ms | 33.7% |
| prestaged 4m | 32 | 0.0 ms | 0.0 ms | 61.5 ms | 111.1 ms | 0.0% |
| prestaged 16m | 32 | 0.0 ms | 0.0 ms | 254.8 ms | 409.1 ms | 0.0% |

The durable profile for the same run shows that persisted data-log sync remains
the dominant foreground cost:

| Workload | c | profile rows | avg total | avg data-log sync | avg catalog | avg root commit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| writeback 1m | 32 | 148 | 5.7 ms | 4.1 ms | 0.9 ms | 0.4 ms |
| writeback 4m | 32 | 29 | 18.7 ms | 16.2 ms | 1.2 ms | 0.4 ms |
| writeback 16m | 32 | 9 | 79.6 ms | 64.0 ms | 1.0 ms | 0.3 ms |
| prestaged 16m | 32 | 3 | 181.9 ms | 146.6 ms | 1.3 ms | 0.4 ms |

Local-provider controls bound the pure in-memory root/delta path. For example,
local writeback 16m c32 averaged 39.0 ms commit prep with 96.5 ms p99, while
the durable row averaged 105.5 ms commit prep and 206.9 ms flush. That says the
visible CoW root plus block-delta construction is real CPU/metadata work, but
it is not the main durable-boundary tail under the current storage profile.

Interpretation:

- Do not implement a block-delta visible overlay yet. It would make reads,
  forks, restores, GC, and reopen overlay-aware, while the measurements point
  first at data-log sync and catalog/placement boundaries.
- Prestaged writeback already models "commit accepted earlier, then make
  durable visible" and shows that moving root/delta work out of fsync can reduce
  fsync-visible prep, but it does not remove end-to-end work.
- The next useful H5 proof is narrower instrumentation of root path copy,
  block-delta entry creation, metadata publish, and mark-referenced phases if
  accepted-write latency becomes a priority. For the current durable-throughput
  goal, keep the dual path and move on to lower-risk simplifications.

## Hypothesis 6 Metadata-Only Zero And Discard

Hypothesis 6 was implemented as a narrow block-delta simplification:

- `write_zeroes` now publishes a sparse mapping transition instead of
  allocating and writing a zero-filled payload buffer.
- Block-delta entries can represent either a segment-backed replacement or a
  sparse replacement.
- Durable `write_zeroes` and `discard` now record sparse block deltas and use
  the block-delta durable path, falling back to `persist_until` only for true
  no-op mappings.

Validation:

- `cargo test durable_zero_and_discard_use_metadata_only_block_deltas -- --nocapture`: pass
- `cargo test block_delta -- --nocapture`: pass
- `cargo test`: pass, 262 library tests and 25 loadbench tests

The regression test writes real data, drains the initial persist profiles, then
zeroes and discards subranges. Both operations produce durable profile rows
with:

- `data_log_write_bytes == 0`
- `data_log_sync_bytes == 0`
- `new_segment_bytes == 0`
- `block_delta_selected_bytes` equal to the logical zero/discard range

The same test reopens the durable store and verifies the sparse ranges read as
zero while neighboring data remains intact.

Interpretation:

- H6 is a clear simplification and should be kept. It removes an obviously bad
  payload path without changing public block semantics.
- There was no prior loadbench zero/discard workload, so this phase does not
  have a before/after throughput table. The decisive evidence is provider
  profile shape: sparse zero/discard writes no data-log payload bytes and
  replays correctly after restart.
- A future benchmark suite can add fixed zero/discard workloads, but it should
  avoid hiding setup cost. The useful timed shape is "pre-existing durable data,
  then make a zero/discard range durable," not "write data and zero it in the
  same timed operation."

## Native Prefix Persist Bundle Follow-Up

The native architecture review found that append-stream prefix persist still had
a bespoke durable-store path even after H4 unified native publish and block
payload/catalog persistence. That path was simplified so private prefix persist
now uses the same provider-internal payload/catalog bundle:

- sync preingested append-run log refs;
- publish node catalog manifests, including manifest-only storage nodes;
- then write private append-stream high-water rows and the private cursor.

The visible metadata step remains intentionally separate: prefix persist is not
publish and must remain private after reopen.

Validation:

- `cargo test append_stream_prefix -- --nocapture`: pass
- `cargo test stream_auto_persist -- --nocapture`: pass

Raw artifacts:

- `target/loadbench/durable-architecture-h7-prefix-bundle/native-auto32/matrix.csv`
- `target/loadbench/durable-architecture-h7-prefix-bundle/native-auto32/durable-profile.csv`
- `target/loadbench/durable-architecture-h7-prefix-bundle/native-auto32/append-publish-profile.csv`

Selected auto32 results:

| Workload | c | `published_mbps` | publish p99 | append p99 |
| --- | ---: | ---: | ---: | ---: |
| interval 32m | 16 | 3367.25 | 650.7 ms | 37.5 ms |
| interval 32m | 32 | 2970.04 | 1770.0 ms | 106.9 ms |
| at-end 4m | 32 | 3670.21 | 3645.6 ms | 457.7 ms |
| at-end 32m | 16 | 3489.74 | 1296.4 ms | 775.8 ms |
| at-end 32m | 32 | 3353.95 | 4005.5 ms | 1980.8 ms |
| barrier-at-end 32m | 16 | 3507.05 | 1738.7 ms | 230.2 ms |
| barrier-at-end 32m | 32 | 3238.98 | 878.2 ms | 2153.0 ms |

Interpretation:

- Keep the change as a store-layer simplification: prefix persist, append
  publish, native metadata delta persist, and block delta flush now share the
  same payload/catalog ordering helper.
- This is not a publish-tail fix. At-end throughput remains in the expected
  range, but p99 is still dominated by final publish wait and some interval
  rows are worse than the earlier H4 auto32 run.
- The next native tail proof should merge or simplify prefix/publish scheduling
  itself, not merely share durable-store helper code.

## Placement, Reopen, Native Extent Shape, And Idempotency Review

The remaining architecture hypotheses were reviewed after H6 and the native
prefix-bundle cleanup. They are real design opportunities, but none is a safe
small edit on the current publish-tail critical path.

### Placement Directory

Durable placement lookup still scans per-storage-node catalogs for a segment ID.
This is provider-private state and should not leak into the public API, but it
does not match the distributed shape we want: a metadata service or storage
node directory should know the placement for a committed segment without
probing every node.

Read-profile artifacts:

- `target/loadbench/durable-architecture-placement-read/nodes1/read-profile.csv`
- `target/loadbench/durable-architecture-placement-read/nodes4/read-profile.csv`
- `target/loadbench/durable-architecture-placement-read/nodes16/read-profile.csv`

Selected c16 read-profile aggregates:

| Workload | storage nodes | avg total | avg metadata resolve | extents | storage nodes read |
| --- | ---: | ---: | ---: | ---: | ---: |
| block-read-1m | 1 | 4463.0 us | 1691.2 us | 256.0 | 1.0 |
| block-read-1m | 4 | 3394.1 us | 2488.8 us | 256.0 | 4.0 |
| block-read-1m | 16 | 4382.8 us | 3769.6 us | 256.0 | 16.0 |
| native-read-1m | 1 | 183.7 us | 16.6 us | 1.0 | 1.0 |
| native-read-1m | 4 | 198.0 us | 27.7 us | 1.0 | 1.0 |
| native-read-1m | 16 | 185.1 us | 20.3 us | 1.0 | 1.0 |

Interpretation:

- Block 1 MiB reads touch 256 segment extents and show metadata resolve cost
  rising with storage-node count. Native 1 MiB reads are one extent and do not
  show the same scaling.
- This supports a provider-private placement directory, especially for
  many-extent block reads, export, GC, and persist selection.
- It is not the first publish-tail fix. The current native publish p99 is still
  driven by sync scheduling and final durable boundary waits.

Recommended proof step:

- Hydrate a `segment_id -> placement/receipt` index from verified receipts and
  durable node-catalog manifests.
- Instrument placement probes per read/persist/export.
- Re-run read and block flush/export profiles at 1, 4, and 16 storage nodes.
- Keep it only if probe count stops scaling with node count and read/flush
  profiles are flat or better.

### Descriptor-Only Reopen

Durable reopen currently loads segment catalog entries, finds placement, reads
each segment payload from data logs, validates it, and stores the bytes back
into the in-memory local segment store. That is convenient for the toy local
provider, but it is the wrong distributed boundary: reopen should hydrate
descriptors and placement, then read payloads lazily from storage nodes.

Interpretation:

- Descriptor-only reopen is likely a large simplicity win for distributed
  storage-node architecture and large datasets.
- It requires splitting local segment records into descriptor plus optional
  cached bytes, then ensuring read paths fetch payloads through placement.
- It is not expected to reduce current append publish p99 directly.

Recommended proof step:

- Add a reopen benchmark over large block and native published histories.
- Track reopen wall time and memory footprint before/after descriptor-only
  hydration.
- Keep existing corruption checks by validating payloads lazily on read or via
  an explicit scrub/verify operation.

### Native Extent Shape

Native leaves currently carry segment entries and append-run extents side by
side. The read path collects both and trims overlaps into one read plan. A
single ordered byte-extent representation may reduce traversal and overlap
logic, but it is metadata-shape surgery.

Interpretation:

- Defer until publish-tail work is cleaner. It may reduce read planning and
  metadata code, but it is unlikely to fix the main durable publish p99.
- Prototype in a model first: compare node counts, read planning cost, and
  append publish metadata passes before editing durable formats.

### Durable Operation Idempotency

The current durable provider has many deterministic failure tests, including
metadata publish failure, persist failure wake/retry, auto-persist retry, and
corrupt row rejection. What it does not yet have is one explicit durable
operation identity spanning payload sync, catalog publish, and visible/private
metadata apply.

Interpretation:

- Full idempotency is important before remote/distributed durable providers.
- It should be introduced at the durable bundle boundary, not by optimizing for
  SQLite transaction details.
- Do not unify native and block visible commit row formats just to get a shared
  abstraction. Add durable operation identity only when partial-success tests
  prove it reduces retry ambiguity.

Recommended proof step:

- Add injected failures after data-log sync, after node-catalog publish, and
  before visible/private metadata commit for native publish, prefix persist,
  block delta flush, and zero/discard sparse deltas.
- Add an operation identity or idempotency key only if retry cannot otherwise
  be made deterministic without redoing durable work.

## Append Publish P99 Chase

Measurement directories:

- Before this chase, no auto-persist:
  `target/loadbench/durable-architecture-p99-chase/noauto/`
- Submitted-ticket batching, no auto-persist:
  `target/loadbench/durable-architecture-p99-chase/coalesced-noauto/`
- Submitted-ticket batching, 32 MiB auto-persist:
  `target/loadbench/durable-architecture-p99-chase/coalesced-auto32/`
- Submitted-ticket batching, 16 MiB auto-persist:
  `target/loadbench/durable-architecture-p99-chase/coalesced-auto16/`
- Submitted-ticket batching, 8 MiB auto-persist:
  `target/loadbench/durable-architecture-p99-chase/coalesced-auto8/`

The useful code change was to let a `wait_append_publish` driver batch all
submitted pending publish tickets, not only tickets whose waiters have already
registered. A one millisecond coalescing pause lets concurrent submitters land
before the driver snapshots the ticket table. This matches the ticketed API:
submit records publish work, and any wait may drive or observe completion.

Key c32 rows:

| Workload | Before no-auto publish p99 | Batched no-auto publish p99 | Batched auto32 publish p99 | Batched auto16 publish p99 |
| --- | ---: | ---: | ---: | ---: |
| `native-stream-publish-at-end-32m` | 6791 ms | 4788 ms | 3078 ms | 2856 ms |
| `native-stream-publish-at-end-4m` | 4195 ms | 5212 ms | 2431 ms | 1757 ms |
| `native-stream-publish-barrier-at-end-32m` | 2564 ms | 2324 ms | 1797 ms | 969 ms |
| `native-stream-publish-barrier-at-end-4m` | 3189 ms | 3107 ms | 1578 ms | 635 ms |

Interpretation:

- Submitted-ticket batching removes an avoidable second physical publish when
  tickets are already submitted. The deterministic regression is
  `durable_append_publish_wait_batches_submitted_pending_tickets`.
- Plain at-end can still split when the first worker finishes and publishes
  before other workers submit their final tickets. That tail is not metadata;
  profile rows show it is dominated by data-log `sync_data` time.
- Auto-persist is the existing semantic mechanism for reducing that final dirty
  tail. In this run, 16 MiB auto-persist was the best p99 point for c32 at-end,
  but 8 MiB was not monotonic and increased some append/barrier tails.
- Do not generalize foreground publish to persist unrelated private streams
  without explicitly changing the private-stream contract and tests. The
  current contract still keeps unrelated unpublished stream bytes non-recovered
  and invisible after reopen.

## Local Architecture Tail-Latency Chase

Measurement directories:

- Baseline, fanout 4:
  `target/loadbench/local-architecture-baseline/full/`
- Payload-only append-log controls:
  `target/loadbench/local-architecture-baseline/append-log/`
- Larger data-log target experiment:
  `target/loadbench/local-architecture-baseline/target-log-size/`
- File-sync fanout experiments:
  `target/loadbench/local-architecture-fanout/`
- Rejected round-robin ordering experiment:
  `target/loadbench/local-architecture-roundrobin/`

All rows used 4 simulated storage nodes, RTT `0`, `512 MiB` per worker, and
`128 MiB` publish interval. The purpose was to separate local filesystem sync
limits from avoidable durable-provider scheduling overhead.

Baseline no-auto rows:

| Workload | c | `published_mbps` | publish p99 | append p99 |
| --- | ---: | ---: | ---: | ---: |
| at-end 4m | 32 | 2029 | 3575 ms | 414 ms |
| at-end 4m | 64 | 2760 | 4588 ms | 1669 ms |
| at-end 32m | 32 | 3174 | 4534 ms | 2208 ms |
| at-end 32m | 64 | 3057 | 5874 ms | 2612 ms |
| barrier-at-end 4m | 32 | 3458 | 2411 ms | 48 ms |
| barrier-at-end 4m | 64 | 3377 | 1803 ms | 1476 ms |
| barrier-at-end 32m | 32 | 3264 | 2272 ms | 1729 ms |
| barrier-at-end 32m | 64 | 3281 | 3292 ms | 1989 ms |

Payload-only controls were also seconds-class. With 4 MiB appends, stream-private
append-log sync reported `3316 MB/s` / `3002 ms` publish p99 at c32 and
`3522 MB/s` / `1502 ms` at c64. Node-shared append-log sync reported
`3642 MB/s` / `2362 ms` at c32 and `3773 MB/s` / `2336 ms` at c64. This means
the local filesystem sync layer is a real ceiling, but full native still has
avoidable scheduling sensitivity.

The kept code change is a provider/loadbench knob:
`--data-log-file-sync-fanout N`. The default remains `4`. Fanout `16` improved
4 MiB at-end throughput and c32 p99, but it was not a universal win:

| Shape | fanout | c | `published_mbps` | publish p99 | Interpretation |
| --- | ---: | ---: | ---: | ---: | --- |
| at-end 4m | 4 | 32 | 2029 | 3575 ms | baseline |
| at-end 4m | 16 | 32 | 3683 | 2372 ms | useful local win |
| at-end 4m | 4 | 64 | 2760 | 4588 ms | baseline |
| at-end 4m | 16 | 64 | 3653 | 4906 ms | throughput win, p99 flat/worse |
| barrier-at-end 4m | 4 | 64 | 3377 | 1803 ms | baseline |
| barrier-at-end 4m | 16 | 64 | 3392 | 1743 ms | small p99 win |
| at-end 32m | 4 | 64 | 3057 | 5874 ms | baseline |
| at-end 32m | 16 | 64 | 3372 | 7269 ms | p99 regression |
| barrier-at-end 32m | 4 | 64 | 3281 | 3292 ms | baseline |
| barrier-at-end 32m | 16 | 64 | 3563 | 2301 ms | useful win |

Rejected experiments:

- Increasing `--target-data-log-mib` to `512` reduced file count but introduced
  very slow single-file sync outliers; c32 at-end 4m publish p99 rose to
  `8707 ms`.
- Letting the auto-persist worker drain while publish demand existed caused
  foreground publish to wait behind background work. The bad c64 at-end 32m row
  spent about `4111 ms` in `persist_lock_wait_nanos`, so the old queueing rule
  was restored.
- Storage-node round-robin sync ordering was mixed and mostly worse for
  barrier rows, so it was removed.

Current conclusion:

- The local host/container sync layer is a real tail-latency ceiling: even
  payload-only append-log controls are seconds-class.
- The durable-provider architecture still has avoidable sensitivity to fanout,
  log sizing, and foreground/background scheduling.
- The next useful architecture proof is not another global fanout tweak. It is a
  storage-node append-log service with per-node sync/backpressure lanes, so
  publish can observe bounded dirty payload per storage node without letting
  background work sit in front of foreground publish.

## External North-Star Targets

The June 6, 2026 Rapid Storage c3-88/Tier1 run is the current external
north-star for native durable append publish. It was not a perfect apples-to-
apples implementation comparison, but it proves the workload shape can reach
multi-GiB/s visible durable throughput with publish-boundary p99 in the
tens-to-low-hundreds of milliseconds when the VM NIC is not the immediate
bottleneck.

Raw artifacts:

- `infra/gcp-rapidstorage-bench/results/rapid-results.csv`
- `infra/gcp-rapidstorage-bench/results/rapid-results-c3-88-tier1.csv`
- `infra/gcp-rapidstorage-bench/results/rapid-tcp-rtt-c3-88-tier1.csv`
- `infra/gcp-rapidstorage-bench/results/rapid-latency-c3-88-tier1.csv`

Benchmark shape:

- Google Cloud project `projectvoice-442316`
- Same-zone VM and Rapid Storage bucket in `us-central1-a`
- `c3-standard-88`, gVNIC, per-VM Tier 1 networking
- `512 MiB` per worker, `128 MiB` publish interval
- workers `16,32,64`, append sizes `4 MiB,32 MiB`
- GCS `Writer.Flush()` is the closest comparison to native publish because the
  appendable object remains appendable after the boundary.
- This is not the same network model as local loadbench `--rtt-us 200`. A
  follow-up TCP-connect probe from the same c3-88/Tier1 shape resolved
  `storage.googleapis.com:443` once per worker and then timed only TCP
  handshakes. It measured `0.303/0.663 ms` p50/p99 at c1, `0.396/0.832 ms` at
  c16, and `1.226/3.616 ms` at c64.
- The `rapid-latency-c3-88-tier1.csv` artifact is API-operation latency
  context: object metadata probes and tiny appendable-object flushes. It is not
  raw network RTT and should not be compared to `--rtt-us` directly.

Target rows:

| Shape | Throughput | Boundary p99 |
| --- | ---: | ---: |
| Rapid Tier1 `at-end`, 4 MiB, c16 | 1.19 GiB/s | 32 ms |
| Rapid Tier1 `at-end`, 4 MiB, c32 | 4.30 GiB/s | 20 ms |
| Rapid Tier1 `at-end`, 4 MiB, c64 | 7.75 GiB/s | 28 ms |
| Rapid Tier1 `at-end`, 32 MiB, c16 | 1.84 GiB/s | 109 ms |
| Rapid Tier1 `at-end`, 32 MiB, c32 | 4.87 GiB/s | 121 ms |
| Rapid Tier1 `at-end`, 32 MiB, c64 | 9.35 GiB/s | 122 ms |
| Rapid Tier1 `interval`, 4 MiB, c32 | 6.09 GiB/s | 20 ms |
| Rapid Tier1 `interval`, 32 MiB, c32 | 5.64 GiB/s | 88 ms |
| Rapid Tier1 `close-at-end`, 4 MiB, c32 | 5.11 GiB/s | 24 ms |
| Rapid Tier1 `close-at-end`, 32 MiB, c32 | 5.75 GiB/s | 114 ms |

Working interpretation:

- The primary native target is publish-at-end, because that is the likely bulk
  file-write shape.
- Passing target: native publish-at-end reaches multi-GiB/s
  `published_mbps` with p99 under `500 ms` at c32/c64 scale.
- Stretch target: native publish-at-end approaches the Rapid c64 32 MiB row,
  about `9.35 GiB/s` with about `122 ms` publish p99.
- Do not claim hardware-limited merely because native throughput matches local
  fio. If publish p99 remains seconds-class, the external north-star says the
  architecture still has avoidable tail work.
