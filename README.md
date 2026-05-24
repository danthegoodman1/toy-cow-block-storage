# toy-cow-block-storage

Deterministic toy copy-on-write storage built in small, correctness-gated
phases. The block device API is the compatibility surface; a native extent/file
API develops beside it over the same segment substrate for append-heavy custom
filesystem work.

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
