# toy-cow-block-storage

Deterministic toy copy-on-write block storage built in small,
correctness-gated phases.

## Phase Gates

Run these before advancing past the project harness and public contract phases:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo doc --no-deps
cargo bench --bench regression
```

The Criterion benchmarks start as tiny regression baselines for API validation
and deterministic test utilities. Later phases should add read, write, fork,
PITR, and GC benchmarks before optimizing those paths.

Criterion reports performance movement; it does not make `cargo bench` fail
solely because a benchmark regressed. Treat the output as regression detection
signal until the project adds an explicit CI comparison step.
