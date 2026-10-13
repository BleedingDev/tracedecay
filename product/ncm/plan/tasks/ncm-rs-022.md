# ncm-rs-022 — Accept the independently usable backend and package its evidence

Status: planned, not executed.
Owner: NCM branch coordinator / reviewer.
Dependencies: ncm-rs-019, ncm-rs-020, ncm-rs-021.
External gates: none.

## Objective

Deliver a real standalone backend without waiting for or pretending to complete host stabilization.

## Owned paths

- `.github/workflows/product-ncm-backend.yml`
- `scripts/product/ncm/check-backend.py`
- `product/ncm/receipts/backend/`
- `product/ncm/README.md`

## Implementation

1. Create a backend-only validation entrypoint and pinned CI lane for the two new packages and existing adapter. Run independent diagnostics without changing the host’s failing policy tests or weakening promotion requirements.
2. Verify the actual worker artifact includes the real encoder/backend and excludes Python-runtime/test-double substitutions. Document artifact installation and local model acquisition with hashes.
3. Assemble source/model/projection identities, numerical differences, conformance, crash/deletion, latency/storage and isolated text journey receipts.
4. Review the allowed-path diff. Leave registry registration, default features, accepted upstream floor and shared Beads updates untouched until N23.

## Acceptance

1. A fresh isolated state root supports real observe→consolidate→restart→recall→delete through the worker/adapter.
2. Backend gates execute nonempty expected tests and pass; missing real-model evidence is a blocker.
3. Status is explicitly backend-accepted, host-not-yet-integrated; no claim that the product checkpoint is demo-ready.
4. The integration owner receives a small manifest/lock patch, adapter patch, exact API requirements and all artifact identities.

## Verification targets

1. python3 scripts/product/ncm/check-backend.py (planned entrypoint)
2. backend_artifact_identity_test

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
