---
name: rn-audit-source
overview: "Pin the authoritative original v2 source revision and identify all native memory-owned source areas using existing upstream metadata and Git history. Distinguish the original PR 707 floor, later accepted upstream imports, current branch additions, and inaccurate stale design documents."
todos:
  - id: audit-source
    content: "Produce source-baseline.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Establish the original Native source and protected boundary

## Execution Notes

Pin the authoritative original v2 source revision and identify all native memory-owned source areas using existing upstream metadata and Git history. Distinguish the original PR 707 floor, later accepted upstream imports, current branch additions, and inaccurate stale design documents.

Read ../prompts/research-common.md before work. Evidence scope: product/upstream/**; product/architecture/adr/ADR-0008-upstream-convergence.md; Git commit and merge ancestry; original upstream files reachable through local Git objects. Write only ../evidence/source-baseline.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
