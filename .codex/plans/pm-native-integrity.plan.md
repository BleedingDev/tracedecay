---
name: pm-native-integrity
overview: Reject corrupt staged payloads whose actual bytes do not match their stored digest.
todos:
  - id: validate-native-stored-content
    content: Reject corrupt staged payloads whose actual bytes do not match their stored digest.
    status: completed
isProject: false
---

# pm-native-integrity

## Execution Notes

Execution expansion requested by the user. This bounded task uses existing contract semantics and can run before the common-profile producer finishes. Read pm-finish-brief.md, the applicable evidence document and pm-comparison-protocol.md where relevant. No new architecture or adjacent feature scope.

## Constraints

Own crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs and inline tests only. You are not alone in the codebase; preserve others' changes. No Cargo/model run, operator mutation, commit/push, manifest edit or child agent. Root reviews diffs and owns builds. New shared seams must return to root instead of taking another lane's files.

## Operator Guidance

No unfinished implementation prerequisite. Root node, feeding pm-native; that later writer starts only after this owner has returned. Finish the precise change or artifact with discriminating tests or data validation and report actual evidence. Do not mark the parent node complete. Root retains plan status ownership.

