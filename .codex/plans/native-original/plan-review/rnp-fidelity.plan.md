---
name: rnp-fidelity
overview: "Review the draft for loss of complete original Native behavior and concrete ownership violations."
todos:
  - id: rnp-fidelity-done
    content: "Review the draft for loss of complete original Native behavior and concrete ownership violations."
    status: completed
isProject: false
---

# Review the draft for loss of complete original Native behavior and concrete ownership violations.

## Execution Notes

Planning only. Read ../REVIEWED-DECISIONS.md, ../WORKER-RULES.md and the execution plans relevant to this review. No product edits, builds, live data or runtime changes.

## Constraints

Root owns the draft/final nodes. Review agents use gpt-5.6-luna with max reasoning, no children, fresh bounded context. Each reviewer writes only its named plan-review report; never edit peer work.

## Operator Guidance

The rnp-draft node is complete because the execution draft exists and passed strict validation. Both reviews depend on it and feed rnp-close. This review graph is separate from the all-pending implementation graph.

