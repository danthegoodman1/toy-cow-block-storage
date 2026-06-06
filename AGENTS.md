# AGENTS.md

This project optimizes for scalable durable storage through simplicity: small
explicit state machines, immutable published metadata, deterministic tests, and
clean internal evolution.

## Project Focus

- Native file and append-stream APIs are first-class storage APIs beside the
  block API, not wrappers around it.
- Durable file-write throughput and publish latency are the primary performance
  goals. Prefer `published_mbps`, publish p50/p99, and end-to-end time through
  the durable publish boundary when evaluating native append/file work.
- Accepted-but-private append throughput is useful diagnostic data, but it is
  not the main optimization target unless the user explicitly asks about
  buffered ingest behavior.
- Preserve scalability through simplicity: prefer fewer durable operations,
  fewer metadata passes, and simpler state transitions before adding scheduling
  or concurrency machinery.
- Keep replication below the public block/native APIs. Clients may request a
  durability policy later, but they must not choose storage nodes or fan out
  replica writes.
- Follow the "no tombstones" principle for internal code and formats. Replace
  old paths cleanly; do not leave deprecated APIs, compatibility wrappers, dual
  representations, or half-removed abstractions.

## Reading Before Changes

- Read `docs/cow-block-storage-design.md` when changing storage semantics,
  public APIs, durable ordering, metadata layout, or provider guarantees.
- Read the module you are about to touch and its relevant tests before editing
  code.
- Read `docs/implementation-plan.md` only when changing the roadmap, starting a
  new implementation phase, or resolving a design/spec conflict.
- If docs and code disagree in an area you are changing, update the docs in the
  same change.

## Development Environment

- Run development commands and tests inside the Linux container:
  `docker compose up -d dev`, then `docker compose exec dev ...`.
- Run the full Rust gate inside the container when a change is ready:
  `cargo fmt --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test`,
  `cargo doc --no-deps`, and
  `cargo bench --bench regression -- --test`.
- Keep git operations on the macOS host: `git status`, `git diff`, `git add`,
  `git commit`, and `git push`.
- Shut the development container down with `docker compose down` when finished.
  Do not use `docker compose down -v` unless intentionally discarding Docker
  volumes and benchmark history.

## Determinism And API Discipline

- Keep deterministic storage-core code free of wall-clock reads, hidden I/O,
  background tasks, process-global randomness, async runtimes, and network
  access.
- Prefer pure state transitions shaped like `step(command) -> effects` where
  practical.
- Make immutable objects the default: segments, metadata nodes, and committed
  roots are never mutated in place.
- Keep native file/extent semantics as a sibling mapping layer over the shared
  substrate. Do not force append streams, writer epochs, or stale-writer
  fencing through block writes.
- Public traits and provider interfaces must document what success makes
  durable or visible, what failure must not expose, and which details remain
  implementation-private.

## Testing And Benchmarks

- Every bug fix should add a deterministic regression test first, or in the
  same change.
- Prefer small model checks and focused tests while developing, then run the
  full relevant gate before calling the work done.
- Generated simulation tests should print or record failing seeds and minimized
  traces.
- Use Criterion for mechanism-level regression checks.
- Use `cargo run --release --bin loadbench -- ...` as the north-star
  integration benchmark for block/native IOPS, durable throughput, latency,
  modeled RTT, concurrency, and error behavior.
- For hot-path changes, keep before/after measurements and investigate durable
  throughput or publish p99 regressions over 10% unless the regression is
  deliberately explained.

## Change Discipline

- Keep changes scoped to the current problem. Leave unrelated refactors and
  metadata churn alone.
- Add abstractions only when a deterministic test, conformance suite, benchmark,
  or real duplication demonstrates the need.
- Do not add provider-specific behavior to metadata tree logic.
- Do not add new cross-shard atomicity mechanisms, production adapters,
  compression, encryption, or distributed machinery without updating the design
  docs and providing a correctness or benchmark reason.
- Temporary migration code is allowed only inside an explicit migration phase,
  and that phase must include an exit gate that removes the temporary path.

## Done Means

The changed module has explicit invariants, deterministic tests for normal and
failure behavior, docs updated where the contract changed, and hot-path costs
measured when performance matters.
