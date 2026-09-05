# ncm-rs-001 — Pin reference, research provenance, and new work authorization

Status: planned, not executed.
Owner: Coordinator / reference reviewer.
Dependencies: none.
External gates: none.

## Objective

Establish exactly what is being reimplemented, without treating prior descoping or the Python README as delivery evidence.

## Owned paths

- `product/ncm/reference/source-manifest.json`
- `product/ncm/reference/NOTICE.md`
- `product/ncm/plan/`

## Implementation

1. Pin product base 25778c7443cd0cfe257da363da01a56ea1d45d3f and Biomem 500847ff65b5d9548b3826fa29bf3ccf8d221147; record tree/blob identities for every source file used. Do not follow moving branch names during implementation.
2. Record this request as authorization for a new Rust-backend work package. Preserve historic tdmem-0700/0702–0711 descoped closures; link new tasks rather than rewriting them as delivered. Reuse the 0701 audit only after checking its pinned source and evidence.
3. Acquire both original papers, verify title/author/version and file hashes, and map relevant sections. Record discrepancies between papers, supplementary documentation, README and executable code. Do not fabricate equation numbers or experimental results when full text is unavailable.
4. Preserve the Biomem MIT notice for reused substantial material. Audit provenance of any copied assets/code and model license. Do not copy CC BY-NC supplementary code into commercial artifacts on the assumption that the repository MIT notice relicenses it. Escalate unresolved rights for the specific artifact, not as a vague blocker to all independent work.

## Acceptance

1. A machine-readable manifest identifies immutable code and model inputs; unresolved artifact hashes are explicitly blocked, never placeholder digests.
2. All production implementation sources are categorized: MIT reference code, paper concepts, or original product work.
3. Full-paper-derived requirements are approved only after actual full-text review; otherwise that portion remains blocked and no paper-conformance claim is made.
4. The new task package is staged on the independent branch only; shared .beads/issues.jsonl is not edited by parallel workers.

## Verification targets

1. reference_manifest_test (planned stdlib validator)

## Sources

- [S01: Biomem pinned implementation](https://github.com/BleedingDev/biomem/tree/500847ff65b5d9548b3826fa29bf3ccf8d221147)
- [S09: Biomem MIT notice](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE)
- [S10: Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization](https://zenodo.org/records/18198327)
- [S11: Implementation of Persistent Latent Memory for Decoder Transformers](https://zenodo.org/records/18267378)
- [S12: OpenTechLab primary publication catalog](https://www.opentechlab.cz/publikace.html)
- [S13: BioCortexAI supplementary scientific documentation](https://zenodo.org/records/18198327/files/BioCortexAI_Documentation_EN.md?download=1)
- [S15: Versioned Beads plan at checkpoint](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
