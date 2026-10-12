# ncm-rs-021 — Evaluate scientific fidelity and actual coding-memory usefulness

Status: planned, not executed.
Owner: Evaluation owner independent of implementation.
Dependencies: ncm-rs-018, ncm-rs-002.
External gates: none.

## Objective

Distinguish a faithful implementation from a useful product and from unsupported scientific claims.

## Owned paths

- `product/ncm/evaluation/`
- `scripts/product/ncm/evaluate/`

## Implementation

1. Run matched numerical/behavioral fixtures against pinned Biomem, accounting for approved corrections. Add real-text store/recall, paraphrase, full-capacity interference, stale correction and LTM-only retention cases.
2. Reuse the existing nine-scenario corpus and metric definitions. Compare Rust NCM with Python Biomem, Native, no memory and explicit documentation under matched inputs; unavailable host baselines are pending until the joined host evaluation.
3. Predeclare dataset split, scoring rubric, minimum acceptable relevance, safety ceilings, task outcomes, latency and token budgets. Keep a held-out set; do not tune on final evaluation answers.
4. Ablate terrain contribution, corrected blur and consolidation. Test that the feature is actually invoked; lack of measured benefit is reported, never relabeled as proof of value. External prompt integration is not decoder-internal attention gating.

## Acceptance

1. Real-encoder LTM-only recall passes without STM, Native, keyword-only or source-file fallback.
2. Stale/cross-scope/deleted/corrupt content fails admission with visible reasons; indeterminate labels do not count as safety passes.
3. Kernel fidelity, algorithm corrections, provider behavior and task benefit have separate results.
4. Observer evaluation cannot claim improved agent outcomes; joined active-mode benefit is measured only in N25.

## Verification targets

1. ncm_reference_comparison (planned)
2. ncm_coding_memory_evaluation (planned)

## Sources

- [S10: Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization](https://zenodo.org/records/18198327)
- [S11: Implementation of Persistent Latent Memory for Decoder Transformers](https://zenodo.org/records/18267378)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)
- [S18: MiniLM primary model card](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
