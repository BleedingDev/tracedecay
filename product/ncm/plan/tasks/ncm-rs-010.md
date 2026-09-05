# ncm-rs-010 — Implement consolidation, merge/prune and LTM-only survival

Status: planned, not executed.
Owner: Core / consolidation.
Dependencies: ncm-rs-009.
External gates: none.

## Objective

Prove memories survive transfer to LTM rather than being answered indefinitely from STM.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/consolidation/`
- `crates/tracedecay-memory-ncm-core/tests/consolidation.rs`

## Implementation

1. Implement top-M STM selection, approved intensity floor, kappa transfer, source lineage propagation, both-layer normalization, terrain pour using actual blur and fatigue reduction.
2. Reproduce merge/prune mechanics where safe; preserve immutable source records and conflict lineage even when latent centers merge. Bound pairwise work or page it deterministically.
3. Run the projection alignment test before choosing the production transfer rule: independently initialized direct LTM projection and U*STM projection are not guaranteed to align. If it fails, use retained canonical LTM-basis keys or another explicitly reviewed mapping; no STM/Native fallback may make the test pass.
4. Specify resumable maintenance as build-then-publish state, not partially visible center/terrain updates. Runtime durability is provided by N14.

## Acceptance

1. Store→consolidate→exclude STM at a test seam→recall succeeds for intended associations with the real encoder later in N21.
2. Merge/prune preserve source-support closure and do not merge contradictory textual assertions into an unlabeled fact.
3. Sleep boundaries, terrain pour, normalization and fatigue reduction match reference or approved difference fixtures.
4. Interruptions between maintenance stages never expose mixed generations.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test consolidation --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
