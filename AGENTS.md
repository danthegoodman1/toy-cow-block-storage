# AGENTS.md

This project uses a "build it like NASA" workflow: small modules,
deterministic behavior, exhaustive simulation, and no advancement to the next
layer until the current layer is boringly correct.

The core doctrine is scalability through simplicity. We should reach scale by
keeping the copy-on-write state machine small, explicit, immutable, and
testable enough to exhaustively simulate, not by adding distributed machinery,
compatibility scaffolding, or clever allocation policies before a simple model
proves they are necessary.

The block device is the first compatibility mapping layer, not the whole storage
system. Native extent/file APIs should develop beside the block API over the
same segment substrate whenever shared write-intent, segment lifecycle,
metadata, or custodian behavior changes. Do not force file-level append leases,
writer epochs, or stale-writer fencing through block writes.

This project also follows a "no tombstones" principle. Because this is a toy
system with no promised external compatibility yet, internal formats and APIs
should evolve cleanly. Do not leave deprecated paths, compatibility wrappers,
dual representations, or half-removed abstractions scattered through the
codebase. This principle is about code and format evolution, not about GC mark
state or device deletion records.

## Required Reading

Before changing code, read:

1. `docs/cow-block-storage-design.md`
2. `docs/implementation-plan.md`
3. The module you are about to touch and its tests

If the implementation plan and design spec disagree, stop and update the docs
before adding code.

## Development Environment

- Run development commands and tests inside the Linux container defined by
  `Dockerfile` and `docker-compose.yml`.
- Start the container with `docker compose up -d dev`, then run work through
  `docker compose exec dev ...` or one-shot commands through
  `docker compose run --rm dev ...`.
- Run the full Rust gate from inside the container:
  `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo test`, `cargo doc --no-deps`, and
  `cargo bench --bench regression -- --test`.
- Shut the development container down with `docker compose down` when finished.
- Keep git operations on the macOS host, such as `git status`, `git diff`,
  `git add`, `git commit`, and `git push`. The repo is bind-mounted into the
  container, while Cargo caches and Linux build artifacts live in Docker
  volumes.

## Operating Rules

- Build in the order defined by `docs/implementation-plan.md`.
- Do not skip ahead to integration work while an earlier module lacks its
  correctness gates.
- Keep the storage core deterministic: no wall-clock reads, no async, no
  network, no filesystem or database I/O, no background tasks, no
  process-global randomness.
- Prefer pure state transitions shaped like
  `step(command) -> effects`.
- Use injected time, seeded randomness, and explicit ordered operation traces.
- Make immutable objects the default: segments, metadata nodes, and committed
  roots are never mutated in place.
- Keep forks O(1): copy shard-root pointers only, with no tree walk and no deep
  refcount updates.
- Keep writes local: append fresh segment data, copy one shard's root-to-leaf
  metadata path, and publish one new shard root.
- Keep reclamation explicit: GC traces from committed roots and sweeps only
  unreachable objects.
- Treat PITR as append-only shard-root history plus checkpoints.
- Keep native file/extent semantics as a sibling mapping layer over the shared
  substrate, not as a wrapper around `BlockDevice`.
- Keep replication below the public block/native APIs. Clients may request
  durability policy later, but they must not fan out replica writes or choose
  storage nodes.
- Preserve scalability through simplicity: prefer sharded roots, immutable
  objects, append-only records, and deterministic replay over clever
  coordination.
- Add abstractions only when a deterministic test, conformance suite, or real
  duplication demonstrates the need.
- Public traits and provider interfaces must document their minimal
  implementor guarantees. Each method should say what success makes durable or
  visible, what failure must not expose, and which details remain
  implementation-private.

## Module Exit Criteria

A module is not done until it has:

- A narrow public API with documented invariants.
- Table-driven deterministic tests for normal behavior.
- Fault/race tests for stale, duplicate, delayed, reordered, failed, and
  conflicting effects when relevant.
- Generated deterministic simulation tests when the module owns state
  transitions.
- Reproducible failing seeds for generated tests.
- Validation checks for object graph invariants when the module touches
  metadata, segments, roots, PITR, or GC.
- Performance measurements when the module is on the read, write, fork, restore,
  or GC hot path.
- No hidden I/O, global randomness, background tasks, or wall-clock reads in
  deterministic code.

## Testing Discipline

- Every bug fix should add a deterministic regression test first, or in the same
  change.
- Generated simulation tests must print or record the seed and minimized trace
  for failures.
- Prefer small model checks over large opaque integration tests.
- Compare storage behavior against a simple reference model whenever practical.
- Run the narrowest relevant tests while developing, then the full gate for the
  current module before moving on.
- Keep Criterion regression benchmarks current for public API validation and
  every implemented hot path.
- GC tests must include adversarial interleavings with writes, forks, deletes,
  PITR retention changes, and sweep boundaries.
- PITR tests must verify restored contents, not only restored root IDs.

## Change Discipline

- Keep changes scoped to the current implementation phase.
- Do not introduce production adapters before their deterministic model and
  provider conformance tests exist.
- Do not add provider-specific behavior to metadata tree logic.
- Do not make shared segment lifecycle, write-intent, custodian, or commit-group
  changes only for block storage while leaving the native extent/file path
  unmodeled.
- Do not add a second cross-shard atomicity mechanism beyond commit groups,
  online shard splitting, segment compaction, deduplication, compression,
  encryption, or durable providers unless the design spec is updated with a
  failing simulation, benchmark, or correctness gap.
- Do not add compatibility layers for old internal formats. Replace the old path
  and update tests and docs in the same change.
- Temporary migration code is allowed only inside an explicit migration phase,
  and that phase must include an exit gate that removes the temporary path.
- If a phase cannot meet its gate, update the implementation plan with the
  blocker and the next smallest proof step.

## Done Means

"Done" means the module can be trusted in isolation: its invariants are
explicit, its behavior is deterministic, its failure cases are simulated, its
object graph is validated, and its hot-path costs are measured when relevant.
