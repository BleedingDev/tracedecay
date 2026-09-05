# ncm-rs-019 — Run real-provider conformance and adversarial scope/privacy tests

Status: planned, not executed.
Owner: Independent verification owner.
Dependencies: ncm-rs-018.
External gates: none.

## Objective

Earn conformance on the actual Rust process and stores.

## Owned paths

- `crates/tracedecay-memory-provider-ncm/tests/rust_backend_conformance.rs`
- `product/ncm/conformance/`

## Implementation

1. Reuse tracedecay-memory-conformance and existing adversarial fixtures with the real backend. Do not write a friendlier parallel conformance definition.
2. Test every supported operation under wrong profile/project/worktree/session/branch, stale epoch, replay, cancellation, corruption and contradictory inputs.
3. Exercise deletion through merged centers/terrain plus stale snapshot restore, real worker termination after commit-before-ack and bounded resource saturation.
4. Separate deterministic kernel doubles from real encoder/process evidence; unsupported required capabilities block readiness rather than produce skipped green tests.

## Acceptance

1. Every mandatory capability has executed test IDs and exact pass/fail/skip accounting on the built artifact.
2. Scope leaks, fabricated provenance, duplicate committed effects and deleted-source recall are zero observed violations in the declared test population.
3. A deliberately faulty backend is rejected by the same tests.
4. Conformance receipts name source tree, worker binary, algorithm, model and state schema identities.

## Verification targets

1. cargo test -p tracedecay-memory-provider-ncm --features rust-backend --test rust_backend_conformance --locked

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
