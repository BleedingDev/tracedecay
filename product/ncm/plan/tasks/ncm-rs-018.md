# ncm-rs-018 — Implement the real NcmCognitiveSurface adapter

Status: planned, not executed.
Owner: Provider adapter owner.
Dependencies: ncm-rs-016, ncm-rs-017.
External gates: none.

## Objective

Replace an absent production implementation behind the existing surface, not the conformance doubles.

## Owned paths

- `crates/tracedecay-memory-provider-ncm/src/lib.rs`
- `crates/tracedecay-memory-provider-ncm/src/rust_backend/`
- `crates/tracedecay-memory-provider-ncm/tests/rust_backend.rs`
- `crates/tracedecay-memory-provider-ncm/Cargo.toml`
- `crates/tracedecay-memory-provider-ncm/README.md`

## Implementation

1. Implement RustNcmSurface against the worker client and retain NcmProviderAdapter’s scope projection, challenge proof, readiness epoch, capability, byte-budget and effect validation.
2. Keep runtime→core and adapter→runtime dependencies acyclic. Registry construction and host mounting remain N24/N25; backend tests instantiate the existing adapter directly.
3. Map each actually supported ProviderOperation to the real worker. Return truthful build/config/model/state identity; advertise capabilities only when their implementation and negotiated requirements are ready.
4. Preserve opaque extension handling and host-owned provenance hydration. No fallback to Native or a canned result. Keep fake providers in tests and reject their artifact identities from production readiness.

## Acceptance

1. Observe, recall, feedback/correction, maintenance, inspect, delete and own-format snapshot paths are exercised through NcmProviderAdapter with real durable state.
2. Readiness replacement invalidates old epochs; normal commits obey the frozen state-generation semantics.
3. Malformed post-dispatch mutation replies preserve effect_unknown and a reconciliation path, not false no-effect.
4. Adapter-only implementation is not counted as shipped host integration.

## Verification targets

1. cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test rust_backend --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
