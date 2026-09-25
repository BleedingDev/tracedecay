---
name: pm-ncm-audit
overview: Establish the precise gap between the real NCM backend payloads and the host's complete canonical memory contracts.
todos:
  - id: ncm-gap-map
    content: Trace real NCM operation translation and response mapping and document every dropped or unsupported temporal, exclusion, provenance, feedback and lifecycle field with exact implementation seams.
    status: completed
isProject: false
---

# pm-ncm-audit

## Execution Notes

Read `pm-finish-brief.md`. Start with provider NCM `src/lib.rs`, `src/rust_backend/mod.rs`, runtime `src/engine/` and existing adapter/backend tests. Separate kernel record support, runtime persistence, adapter containment and host admission. Output `.codex/plans/pm-ncm-evidence.md`, including the real recall response shape and stable host-source to opaque-record mapping across restart.

## Constraints

Read-only source lane; own only the evidence document. No code edits, Cargo, model execution, namespace broadening, new host DB dependency, encoder substitution or speculative algorithm work.

## Operator Guidance

Root node feeding `pm-contract`. Return one operation/field matrix with code references and the minimum producer/consumer edits. Stop at the current seam and state uncertainty explicitly; do not design a rival provider API or session authority.
