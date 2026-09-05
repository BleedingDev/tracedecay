# ncm-rs-002 — Build an executable Python oracle and defect-disposition matrix

Status: planned, not executed.
Owner: Reference / numerical verifier.
Dependencies: ncm-rs-001.
External gates: none.

## Objective

Provide an independent numerical oracle without reproducing known no-ops or accepting untested claims.

## Owned paths

- `scripts/product/ncm/reference/`
- `product/ncm/reference/oracle/`
- `product/ncm/reference/deviations.json`

## Implementation

1. Run the pinned Python core in an isolated CPU environment. Pin Python, torch, sentence-transformers, tokenizer/model artifacts, thread count, dtype, and initial state. Classify tests using fake embeddings separately from real-model tests.
2. Export actual initialized projection matrices, tensors, masks, counters and ordered operations to a non-executable, bounded fixture format. A shared seed is insufficient for cross-language RNG parity. Export per-operation intermediate states, not just final answer strings.
3. Create red controls for blur returning unchanged grids, write sigma using read sigma, load errors being swallowed, missing consolidation cadence in snapshots, potential terrain axis mismatch, and STM→LTM key-space alignment. Each entry needs observed reference behavior, intended v1 behavior, rationale and a discriminating test.
4. Maintain one production algorithm profile. Preserve original behavior only in reference fixtures, not in a second shipped legacy engine. Mark deliberate corrections as expected differences, not tolerance exceptions.
5. Record exact _sanitize_emotion/preset/dictionary/keyword semantics, including neutral-on-None and Czech dictionary keys. Fixtures must distinguish caller-supplied affect from optional text heuristics; no inferred mental-state or decoder-surprise claims.

## Acceptance

1. Oracle fixtures regenerate deterministically after canonicalizing independently generated identities/timestamps with explicit harness inputs.
2. Wrong/no-op blur, constant-output recall, disabled consolidation and random projection substitutions are detected by negative controls.
3. Every intentional divergence has an approved expected result and independent oracle/analytic justification.
4. Fixtures include empty, singleton, >64-center hybrid selection, near-threshold, asymmetric terrain, merge, consolidation, and long sequential traces.

## Verification targets

1. reference_oracle_test
2. reference_negative_controls_test

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
