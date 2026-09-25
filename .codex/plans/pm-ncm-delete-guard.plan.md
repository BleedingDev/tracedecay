---
name: pm-ncm-delete-guard
overview: Enforce existing NCM source revocations on ordinary observation and erase sensitive retained provenance during deletion.
todos:
  - id: enforce-runtime-source-revocation
    content: Enforce existing NCM source revocations on ordinary observation and erase sensitive retained provenance during deletion.
    status: completed
isProject: false
---

# pm-ncm-delete-guard

## Execution Notes

Execution expansion requested by the user. This bounded task uses existing contract semantics and can run before the common-profile producer finishes. Read pm-finish-brief.md, the applicable evidence document and pm-comparison-protocol.md where relevant. No new architecture or adjacent feature scope.

## Constraints

Own crates/tracedecay-memory-ncm-runtime/src/engine/runtime/observe.rs, store/mod.rs, privacy/mod.rs and tests/deletion.rs only. You are not alone in the codebase; preserve others' changes. No Cargo/model run, operator mutation, commit/push, manifest edit or child agent. Root reviews diffs and owns builds. New shared seams must return to root instead of taking another lane's files.

## Operator Guidance

No unfinished implementation prerequisite. Root node, feeding pm-ncm; that later writer starts only after this owner has returned. Finish the precise change or artifact with discriminating tests or data validation and report actual evidence. Do not mark the parent node complete. Root retains plan status ownership.

