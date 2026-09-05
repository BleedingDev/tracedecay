# ncm-rs-005 — Implement numerical primitives and persisted projection bundle

Status: planned, not executed.
Owner: Core / projections.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Reproduce the real vector transformations with explicit numerical identity.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/numeric/`
- `crates/tracedecay-memory-ncm-core/src/projections/`
- `crates/tracedecay-memory-ncm-core/tests/numeric.rs`
- `crates/tracedecay-memory-ncm-core/tests/projections.rs`

## Implementation

1. Implement validated f32 operations, normalization, stable log-softmax, cosine distance, hybrid p=0.5 dissimilarity and deterministic top-k. Reject non-finite values and dimension mismatches before state mutation.
2. Load the oracle’s explicit projection matrices: 384→64 LTM, 384→16 STM/context, 384→128 values, key→3D tanh and 16→64 consolidation. Persist matrices and hashes; never reinitialize them on open/reset/replay without an explicit new epoch.
3. Define zero-vector handling, epsilon placement and tie groups. p=0.5 is not a mathematical metric; do not use triangle-inequality indexing assumptions.
4. Implement shared projected-record structures that preserve a canonical LTM-basis key when required by the approved alignment disposition.

## Acceptance

1. Reference-compatible primitive outputs satisfy the frozen float tolerances; shape/mask/identity values match exactly.
2. Exact ties are stable in Rust; reference nondeterministic ties are compared as predefined equivalence groups, not hidden by fuzzy ranking.
3. NaN, infinity, degenerate inputs and malformed matrix files produce typed errors without partial state.
4. Open and restore reuse identical projection identity.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test numeric --test projections --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
