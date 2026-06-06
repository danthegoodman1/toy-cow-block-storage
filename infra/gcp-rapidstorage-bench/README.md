# GCP Rapid Storage Benchmark Infra

This Terraform bundle creates a same-zone Compute Engine VM and a Rapid Storage
zonal bucket for the Go appendable-object benchmark.

Defaults:

- Project: `projectvoice-442316`
- Region: `us-central1`
- Zone: `us-central1-a`
- VM: `c3-standard-88`
- Networking: gVNIC with per-VM Tier 1 egress bandwidth

Rapid bucket creation currently uses `gcloud storage buckets create` from a
`terraform_data` local-exec because the documented Rapid bucket command needs
the zonal `--placement` flag.

```sh
cd infra/gcp-rapidstorage-bench
./run_once.sh
```

`run_once.sh` initializes Terraform without a remote backend, applies the
temporary infra, runs the default benchmark matrix, copies the CSV back, and
destroys the temporary VM, bucket, network, and service account. The exit trap
also tries to destroy the infra if apply or the benchmark fails after resources
were created.

To run a custom matrix while keeping the same apply/run/destroy behavior:

```sh
./run_once.sh --workers=16,32,64 --op-mib=4,32 --total-mib=512 --publish-mib=128 --mode=at-end,interval,close-at-end --csv=rapid-results-c3-88-tier1.csv
```

The remote run script copies `tools/gcp-rapidstorage-bench` to the VM, runs it
with the Terraform-created bucket, and copies `--csv=...` outputs back into
`infra/gcp-rapidstorage-bench/results/`.

Destroy when finished:

```sh
terraform destroy
```
