---
name: rn-audit-data
overview: "Identify state written by original Native versus the added store. Determine a non-destructive cutover for real persisted state, restart, existing receipts and settings without inventing a migration absent release/use evidence. No operator databases may be opened."
todos:
  - id: audit-data
    content: "Produce saved-data.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Determine saved-data and lifecycle handling

## Execution Notes

Identify state written by original Native versus the added store. Determine a non-destructive cutover for real persisted state, restart, existing receipts and settings without inventing a migration absent release/use evidence. No operator databases may be opened.

Read ../prompts/research-common.md before work. Evidence scope: native_staged_observations.rs; existing original memory/session store schemas and lifecycle; product/upstream and distribution history; source-defined state roots only. Write only ../evidence/saved-data.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
