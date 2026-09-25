---
name: pm-measurement-capture
overview: Implement reusable process phase timing and resource capture for the accepted host comparison protocol.
todos:
  - id: implement-phase-resource-capture
    content: Implement reusable process phase timing and resource capture for the accepted host comparison protocol.
    status: completed
isProject: false
---

# pm-measurement-capture

## Execution Notes

Execution expansion requested by the user. This bounded task uses existing contract semantics and can run before the common-profile producer finishes. Read pm-finish-brief.md, the applicable evidence document and pm-comparison-protocol.md where relevant. No new architecture or adjacent feature scope.

## Constraints

Own scripts/product/memory-comparison/capture.py and test_capture.py only; reuse existing benchmark collector behavior. You are not alone in the codebase; preserve others' changes. No Cargo/model run, operator mutation, commit/push, manifest edit or child agent. Root reviews diffs and owns builds. New shared seams must return to root instead of taking another lane's files.

## Operator Guidance

No unfinished implementation prerequisite. Root node, feeding pm-comparison-harness; that later writer starts only after this owner has returned. Finish the precise change or artifact with discriminating tests or data validation and report actual evidence. Do not mark the parent node complete. Root retains plan status ownership.

