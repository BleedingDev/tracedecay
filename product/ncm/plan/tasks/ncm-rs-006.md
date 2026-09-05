# ncm-rs-006 — Implement center reads: RBF, hybrid selection and compound keys

Status: planned, not executed.
Owner: Core / center reads.
Dependencies: ncm-rs-005.
External gates: none.

## Objective

Implement associative reading, not a renamed key-value lookup or flat cosine-only search.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/centers/read.rs`
- `crates/tracedecay-memory-ncm-core/tests/center_read.rs`

## Implementation

1. Reproduce active-mask filtering, cosine candidate preselection, hybrid reranking above the 64-candidate boundary, selected-center intensity weighting and weighted value/emotion reads.
2. Implement compound semantic/context/terrain scoring separately from the plain RBF path. The Python TextMemory recall uses compound reads; do not assume it uses the plain hybrid path.
3. Expose score components and selected source-support handles without labeling normalized weights as calibrated confidence.
4. Accept immutable read views and operation budgets. All read statistics are output metadata, not implicit learning effects.

## Acceptance

1. Empty, singleton, 64/65-center transitions, distant queries, intensity imbalance and context/terrain perturbation fixtures pass.
2. A caller can inspect actual contributions; a one-item softmax weight of 1 is not reported as certainty of relevance.
3. Batch decomposition behavior is specified; reference batch-global Minkowski normalization is recorded if corrected to per-query normalization.
4. Repeated reads leave the learning-state digest and commit sequence unchanged.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test center_read --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
