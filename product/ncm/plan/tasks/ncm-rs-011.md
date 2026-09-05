# ncm-rs-011 — Implement record-aware recall, correction and provenance output

Status: planned, not executed.
Owner: Core / record policy.
Dependencies: ncm-rs-007, ncm-rs-003.
External gates: none.

## Objective

Turn learned state into admissible advisory candidates without losing evidence or chronology.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/records/`
- `crates/tracedecay-memory-ncm-core/src/recall/`
- `crates/tracedecay-memory-ncm-core/tests/record_recall.rs`

## Implementation

1. Keep RecordId/SourceId and explicit valid/superseded/deleted state separate from center slots. Returned text must come from supported records, not decoding arbitrary 128D values into invented text.
2. Combine STM/LTM candidates by stable record/source identity. Do not copy Python’s key-text-only deduplication, which can hide distinct contradictory records or provenance.
3. Return layer, raw activation, distance components, support handles, exact validity metadata and explicit uncalibrated confidence. Define no-match admission using a predeclared relevance policy, not merely softmax rank.
4. Corrections use admitted host evidence and preserve supersession lineage. Provider recency/activation never overrides current code authority; ambiguous conflicts remain visible.

## Acceptance

1. Same key/different assertions remain distinct until an explicit valid correction links them.
2. Unrelated singleton queries do not become high-confidence facts; empty and unavailable are different.
3. Source absence is explicit; tests detect fabricated citations, stale ID reuse and missing support after merge.
4. Record budgets apply before hydration and serialization; no metadata or token overflow bypass.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test record_recall --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
