---
name: pm-ncm-restore-identity
overview: Prevent snapshot rollback from reusing discarded NCM record identities for different content.
todos:
  - id: preserve-restored-record-identity
    content: Prevent snapshot rollback from reusing discarded NCM record identities for different content.
    status: completed
isProject: false
---

# pm-ncm-restore-identity

## Execution Notes

Execution expansion requested by the user. This bounded task uses existing contract semantics and can run before the common-profile producer finishes. Read pm-finish-brief.md, the applicable evidence document and pm-comparison-protocol.md where relevant. No new architecture or adjacent feature scope.

## Constraints

Own crates/tracedecay-memory-ncm-runtime/src/snapshot/mod.rs and tests/snapshot.rs only; request any store seam through root. You are not alone in the codebase; preserve others' changes. No Cargo/model run, operator mutation, commit/push, manifest edit or child agent. Root reviews diffs and owns builds. New shared seams must return to root instead of taking another lane's files.

## Operator Guidance

No unfinished implementation prerequisite. Root node, feeding pm-ncm; that later writer starts only after this owner has returned. Finish the precise change or artifact with discriminating tests or data validation and report actual evidence. Do not mark the parent node complete. Root retains plan status ownership.

