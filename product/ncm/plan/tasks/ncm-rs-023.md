# ncm-rs-023 — Join the stabilized host and reconcile shared ownership

Status: planned, not executed.
Owner: Shared integration owner.
Dependencies: ncm-rs-022.
External gates: HOST-STABLE.

## Objective

Merge independent work without undoing the other agent’s fixes or claiming their evidence applies automatically.

## Owned paths

- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`
- `.beads/issues.jsonl`
- `product/ncm/receipts/integration/`

## Implementation

1. Consume a specific accepted host commit/tree from HOST-STABLE. Keep the new backend commits reviewable; do not pull a moving upstream head or rewrite shared history.
2. Reconcile workspace/dependency and ownership changes by semantic role. Preserve new deadline, retention and policy-test fixes from the stabilization branch.
3. Import the new task work package through the existing Beads process, linking older descoped records as superseded intent. Do not double count old closures or modify unrelated tasks.
4. Rerun backend gates and the relevant host feature/dependency/ownership/generated checks on the actual combined tree.

## Acceptance

1. All conflicts have owner, rationale and invariant-preservation evidence.
2. Current ownership and dependency checks pass without blanket allowlists.
3. The joined tree is recorded separately from backend-accepted and host-accepted source trees.
4. No obsolete result is reused solely because a branch name is unchanged.

## Verification targets

1. existing ownership and dependency gates
2. combined backend gate

## Sources

- [S15: Versioned Beads plan at checkpoint](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
