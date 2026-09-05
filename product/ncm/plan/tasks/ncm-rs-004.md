# ncm-rs-004 — Create isolated Rust packages and compile-time interfaces

Status: planned, not executed.
Owner: NCM branch coordinator.
Dependencies: ncm-rs-003.
External gates: none.

## Objective

Make backend-only development possible without touching retention, host CI policy or current recall composition.

## Owned paths

- `crates/tracedecay-memory-ncm-core/Cargo.toml`
- `crates/tracedecay-memory-ncm-core/src/lib.rs`
- `crates/tracedecay-memory-ncm-core/src/types.rs`
- `crates/tracedecay-memory-ncm-runtime/Cargo.toml`
- `crates/tracedecay-memory-ncm-runtime/src/lib.rs`
- `crates/tracedecay-memory-ncm-runtime/src/ports.rs`
- `Cargo.toml`
- `Cargo.lock`
- `product/ncm/bootstrap/`

## Implementation

1. Add exactly two product-owned packages: core for numerical state and opaque record support; runtime for state I/O, actual embedding execution, worker and protocol. Retain the existing provider-ncm adapter.
2. Coordinator alone edits this branch’s workspace member list and lockfile. Predeclare module paths with empty private module files where needed; those files contain no implemented public capability or fake success. Their exact initial paths must be added to this task’s ownership manifest before creation. Later central manifest/interface adjustments are coordinator-owned integration commits, not concurrent worker edits. No temporary second workspace or duplicate lockfile.
3. Core has no Tokio, filesystem, process, network, host-store or concrete-provider dependency. Runtime must not depend on provider-ncm or registry, avoiding a dependency cycle when the adapter depends on its client/wire library.
4. Pin required numeric/serialization/inference dependencies to reviewed versions compatible with the repository toolchain. Reuse a matching existing Rust inference backend where feasible; do not silently choose a different embedding model.

## Acceptance

1. Both packages compile independently under the pinned workspace toolchain with warnings denied and owned unsafe code forbidden.
2. No NCM registration, active route, background process or state directory exists when the feature is off.
3. Backend branch diff contains only declared product-owned work plus the isolated manifest/lock patch.
4. Later workers have compile-time contracts and module paths; any newly discovered shared manifest/interface change is serialized through the coordinator and recorded as an explicit dependency change rather than edited concurrently.

## Verification targets

1. cargo check -p tracedecay-memory-ncm-core -p tracedecay-memory-ncm-runtime --locked
2. dependency_boundary_test

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
