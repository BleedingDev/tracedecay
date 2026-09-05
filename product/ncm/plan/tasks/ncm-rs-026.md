# ncm-rs-026 — Verify installed artifacts, platform support and release decision

Status: planned, not executed.
Owner: Release owner / independent reviewer.
Dependencies: ncm-rs-025.
External gates: HOST-RELEASE.

## Objective

Finish with an installable, honestly labeled product capability rather than a green development harness.

## Owned paths

- `product/ncm/release/`
- `docs/product/ncm-rust.md`
- `scripts/product/ncm/verify-installed.py`

## Implementation

1. Use the repository’s existing packaging path to include the pinned Rust worker and model acquisition manifest. Do not start unrelated browser, desktop UI or packaging-system work.
2. Verify clean install, existing-profile upgrade, process restart, disabled rollback and artifact identity on every platform advertised as supported. Backend algorithm portability alone is not packaging proof.
3. Confirm test-transport and fake-provider artifacts cannot satisfy production readiness. Record actual binary paths/hashes, features and worker protocol compatibility.
4. Issue a go/no-go with separate numerical fidelity, backend conformance, host safety, platform, and usefulness verdicts. Keep failed or blocked gates visible.
5. At the host join, identify the existing packaging files that actually need worker inclusion and add their exact paths to this node after release-owner approval. The external HOST-RELEASE gate covers prerequisites and authority; new NCM packaging and installed-artifact results are outputs of this task, not circular prerequisites.

## Acceptance

1. The installed CLI and real Rust worker reproduce the accepted active journey on the declared supported platform matrix.
2. Disablement returns to Native-only behavior without accidental NCM fallback or state deletion.
3. Documentation describes a Biomem-based independent Rust implementation, not an official OpenTechLab product or a proven decoder-internal integration.
4. Every completed task is backed by evidence on the final joined tree or an explicitly justified unchanged-tree reuse.

## Verification targets

1. installed_ncm_journey (planned)
2. python3 scripts/product/ncm/verify-installed.py (planned entrypoint)

## Sources

- [S09: Biomem MIT notice](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S19: Existing upstream convergence procedure](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---

# Pinned sources and review boundary

Retrieved/reviewed for this plan on 2026-09-05. Source code was inspected, not executed. Original PDFs were not successfully retrieved. These links identify reference material; they are not test receipts.

## S01 — Biomem pinned implementation

https://github.com/BleedingDev/biomem/tree/500847ff65b5d9548b3826fa29bf3ccf8d221147

**commit:** 500847ff65b5d9548b3826fa29bf3ccf8d221147

**review:** README, config, projection, center read/write excerpts, terrain, consolidation, text store/recall/state, embedder, and LICENSE inspected; not executed.

## S02 — Biomem configuration

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py

**git_blob:** 9b317cd8523991805993cc03fb84f22611840ea3

## S03 — Biomem center algorithms

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py

**git_blob:** ed1fc3e6df41a0fe3e6c508928fc2b6385456ef6

## S04 — Biomem terrain

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py

**git_blob:** f4b3a936e2b316dc08826e6fe5794d0e42242c22

## S05 — Biomem consolidation

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py

**git_blob:** 968b4a2669181a190888ccff50eb9644e2adcbaa

## S06 — Biomem projections

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py

**git_blob:** 433267fa3ab98bf0b9f3e06490c558320897a63d

## S07 — Biomem text-memory orchestration

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py

**git_blob:** feeec785d0d5ad0b4416ed29e3512de941d9f49a

## S08 — Biomem real text embedding and emotion extraction

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/embedder.py

**git_blob:** 1a0784bf58030ea36c01e0639b15bef65f8d9994

## S09 — Biomem MIT notice

https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/LICENSE

**git_blob:** eba479bc5df694b05f9adb8ee248ae1243b876eb

## S10 — Persistent Memory for Decoder-Only Transformers: Latent Terrain, Diffusion, Homeostasis, and Emotional Stabilization

https://zenodo.org/records/18198327

**doi:** 10.5281/zenodo.18198327

**version:** v1, 2026-01-09

**review:** Primary record and abstract inspected; original PDF retrieval failed. Full-text verification and file checksums remain N01 work.

## S11 — Implementation of Persistent Latent Memory for Decoder Transformers

https://zenodo.org/records/18267378

**doi:** 10.5281/zenodo.18267378

**review:** Identified by the primary OpenTechLab publication page. Record rate-limited; PDF retrieval failed on size. No full-text or figure verification claimed.

## S12 — OpenTechLab primary publication catalog

https://www.opentechlab.cz/publikace.html

**review:** Lists both memory publications and their downloadable paper filenames.

## S13 — BioCortexAI supplementary scientific documentation

https://zenodo.org/records/18198327/files/BioCortexAI_Documentation_EN.md?download=1

**review:** Primary supplementary text inspected; its header states CC BY-NC 4.0. It is not a blanket commercial code license.

## S14 — Existing NCM adapter boundary

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs

**git_blob:** 708f30b5c8f6a2f2350567b189a0aa487e648c50

**review:** NcmNamespace includes agent_session_id and resolved_scope_digest; NcmCognitiveSurface is synchronous; adapter owns scope/readiness checks.

## S15 — Versioned Beads plan at checkpoint

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/.beads/issues.jsonl

**git_blob:** 9c27c2f0a3375bd2255e341c6696fe257e34afd8

**review:** tdmem-0700 and 0702 onward were descoped; 0701 audit was completed. 0606/0608/0609 remain open in inspected records.

## S16 — Existing host journey

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs

## S17 — Existing provider-neutral evaluation

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md

## S18 — MiniLM primary model card

https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2

**review:** 384-dimensional output; masked mean pooling; SentenceTransformer max_seq_length 128; Apache-2.0 metadata. Exact model revision/artifact hashes still must be pinned.

## S19 — Existing upstream convergence procedure

https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/upstream/README.md



## Planning-package validation

```json
{
  "valid": true,
  "task_count": 26,
  "external_gate_count": 4,
  "backend_independent_tasks": 22,
  "ready": [
    "ncm-rs-001"
  ],
  "topological_order": [
    "ncm-rs-001",
    "ncm-rs-002",
    "ncm-rs-003",
    "ncm-rs-004",
    "ncm-rs-005",
    "ncm-rs-008",
    "ncm-rs-012",
    "ncm-rs-013",
    "ncm-rs-006",
    "ncm-rs-007",
    "ncm-rs-009",
    "ncm-rs-011",
    "ncm-rs-010",
    "ncm-rs-014",
    "ncm-rs-015",
    "ncm-rs-017",
    "ncm-rs-016",
    "ncm-rs-018",
    "ncm-rs-019",
    "ncm-rs-020",
    "ncm-rs-021",
    "ncm-rs-022",
    "ncm-rs-023",
    "ncm-rs-024",
    "ncm-rs-025",
    "ncm-rs-026"
  ],
  "declared_concurrent_write_collisions": 0
}
```

Eighteen planning-validator tests passed. No NCM, reference-engine, Rust, model or host tests were executed by this planner.
