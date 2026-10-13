---
name: rn-audit-facts
overview: "Trace original Native facts end to end: write, read, scoring, retrieval tracking, feedback and trust, consolidation/maintenance, correction/deletion, ownership, persistence, public entry points. Compare direct original operation to current adapter and identify suppressed side effects."
todos:
  - id: audit-facts
    content: "Produce native-facts.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Map complete Native fact behavior

## Execution Notes

Trace original Native facts end to end: write, read, scoring, retrieval tracking, feedback and trust, consolidation/maintenance, correction/deletion, ownership, persistence, public entry points. Compare direct original operation to current adapter and identify suppressed side effects.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay-session-memory/**; existing original fact application/store implementations; current native fact projection; ADR-0010. Write only ../evidence/native-facts.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
