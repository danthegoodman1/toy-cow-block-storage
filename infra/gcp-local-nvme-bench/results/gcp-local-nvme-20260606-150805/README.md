# GCP Local NVMe Loadbench - 2026-06-06

Project: `projectvoice-442316`

Requested node: `c4-standard-96-lssd`

Actual node: `c3-standard-176-lssd` in `us-central1-a`. All `us-central1`
zones were stocked out for `c4-standard-96-lssd`.

Storage: 32 x 375 GB local SSD, RAID0 `/dev/md0`, XFS, mounted at
`/mnt/localssd`.

Source: current dirty local workspace copied to the VM and built with
`rustc 1.96.0`.

Loadbench shape:

```bash
loadbench \
  --provider durable \
  --durability ack \
  --concurrency 16,32,64 \
  --storage-nodes 4|16 \
  --files 128 \
  --warmup-ms 0 \
  --stream-total-mib 512 \
  --stream-publish-mib 128 \
  --root /mnt/localssd/loadbench-root \
  --workloads native-stream-publish-at-end-4m,native-stream-publish-at-end-32m,native-stream-publish-barrier-at-end-4m,native-stream-publish-barrier-at-end-32m,native-stream-publish-interval-4m,native-stream-publish-interval-32m
```

Modeled RTTs:

- `0 us`: local storage/architecture floor.
- `200 us`: historical local loadbench baseline.
- `700 us`: close to saved Rapid TCP probe c1 p99 (`0.663 ms`).
- `3600 us`: close to saved Rapid TCP probe c64 p99 (`3.616 ms`).

Artifacts:

- `combined-matrix.csv`: all 144 loadbench rows.
- `headline-summary.csv`: c32/c64 at-end and interval headline rows.
- `durable-profile-summary.csv`: compact durable profile percentiles.
- `append-publish-profile-summary.csv`: compact publish wait percentiles.
- `fio-summary.csv`: local filesystem fsync comparison.
- `native-s*/matrix.csv`: per-run raw matrix CSVs.
- `environment.txt`: VM, disk, mount, kernel, and Rust metadata.

Raw durable/profile CSVs were summarized, then deleted before copying back
because they were multi-GB. fio data payload files were also deleted.

## Headline Results

Selected `storage_nodes=16` rows:

| RTT | Workload | c | `published_mbps` | publish p50 | publish p99 | append p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 0 us | at-end 4m | 64 | 6612 | 4112.0 ms | 4586.0 ms | 15.3 ms |
| 0 us | at-end 32m | 64 | 6100 | 3875.7 ms | 4610.0 ms | 182.7 ms |
| 0 us | interval 4m | 64 | 6694 | 1031.6 ms | 1183.9 ms | 22.8 ms |
| 0 us | interval 32m | 64 | 6304 | 1018.7 ms | 1238.3 ms | 188.7 ms |
| 200 us | at-end 4m | 64 | 6544 | 4142.5 ms | 4546.1 ms | 15.6 ms |
| 200 us | at-end 32m | 64 | 5919 | 3886.8 ms | 4545.0 ms | 213.0 ms |
| 200 us | interval 4m | 64 | 6615 | 1058.1 ms | 1209.8 ms | 17.3 ms |
| 200 us | interval 32m | 64 | 6043 | 1098.9 ms | 1350.1 ms | 195.6 ms |
| 700 us | at-end 4m | 64 | 6409 | 4119.0 ms | 4501.2 ms | 26.4 ms |
| 700 us | at-end 32m | 64 | 5838 | 4008.1 ms | 4632.0 ms | 203.6 ms |
| 700 us | interval 4m | 64 | 6647 | 1031.4 ms | 1215.8 ms | 18.1 ms |
| 700 us | interval 32m | 64 | 6129 | 1045.5 ms | 1273.6 ms | 200.1 ms |
| 3600 us | at-end 4m | 64 | 6190 | 4066.2 ms | 4397.7 ms | 25.9 ms |
| 3600 us | at-end 32m | 64 | 5953 | 4176.4 ms | 4631.4 ms | 196.7 ms |
| 3600 us | interval 4m | 64 | 6359 | 1037.2 ms | 1200.7 ms | 15.2 ms |
| 3600 us | interval 32m | 64 | 6171 | 981.3 ms | 1211.0 ms | 146.1 ms |

fio comparison on the same RAID0 local SSD mount:

| fio job | write MiB/s | fsync p50 | fsync p99 | fsync p99.9 |
| --- | ---: | ---: | ---: | ---: |
| 4 MiB write + fdatasync each write | 1327 | 1.71 ms | 2.02 ms | 2.93 ms |
| 32 MiB write + fdatasync each write | 1997 | 5.34 ms | 5.54 ms | 6.26 ms |

## Read

The GCP local-NVMe run substantially raises durable throughput versus the Mac
Docker runs, roughly to `6.0-6.7 GiB/s` for c64 native publish rows. Append tail
latency is low. Full publish-at-end p99 remains around `4.4-4.6 s`, while
interval publish p99 is around `1.2-1.35 s`.

That means the Mac Docker/local filesystem was a throughput ceiling, but the
seconds-class publish-at-end tail is not explained by raw local-NVMe fsync
latency alone. The architecture still serializes or batches publish completion
work in a way that makes c64 at-end wait for a multi-second global publish wave.
