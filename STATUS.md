# Status: Stage 6 GCP Direct-I/O Measurement

Branch: `codex/block-direct-io-gcp-measurement`
Base branch: `codex/block-direct-io-phase-6`
Commit under test before measurement/doc changes: `6484f10`

## What Changed

- Added `TOY_DURABLE_IO_BACKEND` to the GCP block-vs-RBD harness so loadbench
  can run with `--durable-io-backend filesystem|direct-io`.
- Added default-off `SKIP_CEPH` for scoped reruns.
- Added toy backend labels and backend metadata to harness output summaries.
- Recorded Stage 6 GCP measurement provenance, artifact paths, reviewer caveat,
  direct-vs-filesystem same-concurrency facts, direct-I/O error rows, and
  conclusion in `docs/block-native-fast-path-plan.md`.

Changed files:

- `infra/gcp-local-nvme-bench/run_block_vs_rbd.sh`
- `infra/gcp-local-nvme-bench/remote_block_vs_rbd.sh`
- `docs/block-native-fast-path-plan.md`
- `STATUS.md`

## Measurement Runs

Both runs used `c4-standard-32-lssd` in `us-east1-b`, local SSD, pool-size-1
Ceph RBD, RTT 0, flushed durability, sizes `4k,64k,256k`, concurrency
`1,4,16,32`.

- Filesystem run ID: `stage6-filesystem-20260619-001`
  - Results: `infra/gcp-local-nvme-bench/results/stage6-filesystem-20260619-001/`
  - Archive: `infra/gcp-local-nvme-bench/results/stage6-filesystem-20260619-001-results.tgz`
- Direct-I/O run ID: `stage6-direct-io-20260619-001`
  - Results: `infra/gcp-local-nvme-bench/results/stage6-direct-io-20260619-001/`
  - Archive: `infra/gcp-local-nvme-bench/results/stage6-direct-io-20260619-001-results.tgz`

Direct-I/O resolved successfully on the VM:

- `toy_durable_io_backend=direct-io`
- `toy_resolved_durable_io_backend=direct-io`

## Important Result Caveat

Do not use the earlier "best clean rows" ratio table as an apples-to-apples
backend comparison. For 64K and 256K it compares direct-I/O at concurrency 4
against filesystem/Ceph at concurrency 32.

Same-concurrency direct-I/O versus filesystem facts:

- 64K: `0.960x` throughput / `1.090x` p99 at c1; `0.974x` throughput /
  `0.930x` p99 at c4; errors at c16/c32.
- 256K: `1.038x` throughput / `0.874x` p99 at c1; `0.893x` throughput /
  `1.175x` p99 at c4; errors at c16/c32.

Direct-I/O error rows:

- 64K c16: 58,737 errors.
- 64K c32: 238,573 errors.
- 256K c16: 238,899 errors.
- 256K c32: 409,418 errors.

## Conclusion

Direct-I/O is not a Stage 6 performance win. It selected and resolved
successfully on GCP local SSD, but it should not be kept as a performance path
until the high-concurrency direct-I/O errors are root-caused. Throughput tuning
should wait.

## Validation Run

```bash
bash -n infra/gcp-local-nvme-bench/run_block_vs_rbd.sh
bash -n infra/gcp-local-nvme-bench/remote_block_vs_rbd.sh
```

Both syntax checks passed after the documentation update.

## Cleanup

The benchmark harness deleted both temporary VMs, firewall rules, subnets, and
networks. A post-run GCP check showed no compute instances currently listed in
the project and no remaining benchmark run networks.
