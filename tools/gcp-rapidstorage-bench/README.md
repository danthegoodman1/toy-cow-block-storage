# GCP Rapid Storage Benchmark

This is a small Go benchmark for comparing Google Cloud Storage Rapid Bucket
appendable-object behavior against the native append publish-at-end workloads.

It uses the Cloud Storage Go gRPC client with zonal bucket APIs enabled. The
closest semantic mapping is:

- `mode=at-end`: write all chunks, then call `Writer.Flush()` once. This is the
  nearest match for native `publish-at-end`.
- `mode=interval`: call `Writer.Flush()` every `--publish-mib`. This is the
  nearest match for native publish-interval.
- `mode=close-at-end`: write all chunks, then use `Writer.Close()` as the
  measured boundary. Add `--finalize-on-close` to make the object non-appendable
  at close.

Example:

```sh
go run . \
  --bucket="$BUCKET" \
  --workers=16,32 \
  --op-mib=4,32 \
  --total-mib=512 \
  --publish-mib=128 \
  --mode=at-end,interval,close-at-end \
  --csv=rapid-results.csv
```

The important columns are `published_mibps`, `flush_p50_ms`, `flush_p99_ms`,
`close_p50_ms`, and `close_p99_ms`. For `mode=at-end` and `mode=interval`,
`Flush()` is the measured publish boundary. For `mode=close-at-end`, `Close()`
is the measured boundary.

## June 6, 2026 Spot Checks

These were run from same-zone Compute Engine VMs in `us-central1-a` against
same-zone Rapid Storage buckets in project `projectvoice-442316`, with
`512 MiB` per worker and a `128 MiB` publish interval. The one-shot Terraform
wrapper destroyed the VM, bucket, network, and service account after each run.

Raw CSVs:

- `infra/gcp-rapidstorage-bench/results/rapid-results.csv`
- `infra/gcp-rapidstorage-bench/results/rapid-results-c3-88-tier1.csv`

### `c3-standard-22`

The `c3-standard-22` VM has a documented default egress ceiling of up to
`23 Gbps`, which is roughly `2.68 GiB/s` before protocol overhead. The fastest
rows reached about `2.42 GiB/s`, so these results are useful for publish-tail
shape but should not be treated as Rapid Storage's peak throughput limit.

Selected rows:

| Mode | Workers | Append size | `published_mibps` | Boundary p50 | Boundary p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `at-end` | 16 | 4 MiB | 562.1 | 56.9 ms flush | 109.9 ms flush |
| `at-end` | 16 | 32 MiB | 1288.0 | 31.3 ms flush | 85.8 ms flush |
| `at-end` | 32 | 4 MiB | 1961.8 | 17.0 ms flush | 72.6 ms flush |
| `at-end` | 32 | 32 MiB | 2323.4 | 36.9 ms flush | 113.6 ms flush |
| `interval` | 16 | 4 MiB | 1576.9 | 16.2 ms flush | 22.0 ms flush |
| `interval` | 16 | 32 MiB | 2271.4 | 37.2 ms flush | 135.2 ms flush |
| `interval` | 32 | 4 MiB | 2448.3 | 34.7 ms flush | 110.1 ms flush |
| `interval` | 32 | 32 MiB | 2434.2 | 46.4 ms flush | 178.3 ms flush |
| `close-at-end` | 16 | 4 MiB | 2223.5 | 18.9 ms close | 27.2 ms close |
| `close-at-end` | 16 | 32 MiB | 2421.7 | 40.1 ms close | 90.5 ms close |
| `close-at-end` | 32 | 4 MiB | 2473.8 | 22.6 ms close | 39.7 ms close |
| `close-at-end` | 32 | 32 MiB | 2339.8 | 36.9 ms close | 109.3 ms close |

### `c3-standard-88` With Tier 1 Networking

This run used gVNIC and per-VM Tier 1 networking. The documented ceiling for
`c3-standard-88` with Tier 1 is up to `100 Gbps`, or roughly `11.6 GiB/s`
before protocol overhead.

Selected rows:

| Mode | Workers | Append size | `published_mibps` | Boundary p50 | Boundary p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `at-end` | 16 | 4 MiB | 1217.4 | 14.2 ms flush | 32.5 ms flush |
| `at-end` | 16 | 32 MiB | 1885.3 | 37.1 ms flush | 108.8 ms flush |
| `at-end` | 32 | 4 MiB | 4398.6 | 12.9 ms flush | 20.2 ms flush |
| `at-end` | 32 | 32 MiB | 4987.8 | 32.9 ms flush | 121.0 ms flush |
| `at-end` | 64 | 4 MiB | 7939.3 | 13.7 ms flush | 28.3 ms flush |
| `at-end` | 64 | 32 MiB | 9569.5 | 26.5 ms flush | 122.3 ms flush |
| `interval` | 16 | 4 MiB | 2657.4 | 12.6 ms flush | 20.1 ms flush |
| `interval` | 16 | 32 MiB | 3723.8 | 33.8 ms flush | 80.5 ms flush |
| `interval` | 32 | 4 MiB | 6232.4 | 12.8 ms flush | 19.6 ms flush |
| `interval` | 32 | 32 MiB | 5777.9 | 28.3 ms flush | 87.9 ms flush |
| `interval` | 64 | 4 MiB | 7120.6 | 17.4 ms flush | 46.4 ms flush |
| `interval` | 64 | 32 MiB | 7592.7 | 32.0 ms flush | 125.9 ms flush |
| `close-at-end` | 16 | 4 MiB | 3325.8 | 16.1 ms close | 19.3 ms close |
| `close-at-end` | 16 | 32 MiB | 2970.5 | 60.9 ms close | 142.2 ms close |
| `close-at-end` | 32 | 4 MiB | 5237.4 | 16.9 ms close | 23.7 ms close |
| `close-at-end` | 32 | 32 MiB | 5884.7 | 58.1 ms close | 114.0 ms close |
| `close-at-end` | 64 | 4 MiB | 6684.3 | 18.9 ms close | 33.2 ms close |
| `close-at-end` | 64 | 32 MiB | 7929.0 | 44.7 ms close | 166.6 ms close |

The c3-88/Tier1 run strongly suggests the c3-22 run was VM-network-limited for
throughput. The best publish-at-end row rose from about `2.27 GiB/s` to about
`9.35 GiB/s`, while the publish boundary p99 stayed in the tens-to-low-hundreds
of milliseconds range.
