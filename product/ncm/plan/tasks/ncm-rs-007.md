# ncm-rs-007 — Implement center writes, allocation and bounded source support

Status: planned, not executed.
Owner: Core / center writes.
Dependencies: ncm-rs-006.
External gates: none.

## Objective

Implement genuine stateful learning with stable identities and complete mutation attribution.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/centers/write.rs`
- `crates/tracedecay-memory-ncm-core/src/centers/allocation.rs`
- `crates/tracedecay-memory-ncm-core/tests/center_write.rs`
- `crates/tracedecay-memory-ncm-core/src/signals/`
- `crates/tracedecay-memory-ncm-core/tests/signals.rs`

## Implementation

1. Implement novelty, surprise and salience weighting, STM admission, local updates of intensity/value/emotion/context/terrain position and fixed-capacity allocation.
2. Use the explicitly approved sigma-write behavior; the pinned reference write calls compute_rbf_weights without passing sigma_write. A corrected width needs a versioned divergence test.
3. Track every source that influences every updated center, not just the top center’s displayed text. Resolve capacity with a documented deterministic rule and a receipt; never silently overwrite a public record identity.
4. Distinguish same logical record from duplicate operation. Stable memory_id alone is not an exactly-once guard; durable request deduplication is N14.
5. Implement four-channel affect/salience handling: explicit neutral default [1,1,1,1], validated finite vectors, pinned named presets and dictionary spelling/alias policy. Port the reference Czech/English keyword extractor as an explicit optional signal source, not an implicit measurement of human emotion. Retain input source and policy identity. Surprise is an admitted signal; do not invent decoder prediction error.

## Acceptance

1. Reference-compatible mutation fixtures pass and approved corrections have negative controls.
2. Zero strength, malformed batches, exhausted record/lineage budgets and full capacity return explicit effects and do not leave partial mutations.
3. A reused center slot receives a new incarnation; prior source links cannot resolve to unrelated content.
4. All supporting-record and content allocations obey byte/count bounds.
5. Neutral, named, explicit-vector, dictionary-alias and optional Czech/English keyword paths are tested. Malformed/nonfinite affect is rejected rather than silently replaced. Keyword substrings/negation are characterized as heuristic limitations; affect cannot override scope, provenance or current-code authority.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test center_write --locked
2. cargo test -p tracedecay-memory-ncm-core --test signals --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S08: Biomem real text embedding and emotion extraction](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
