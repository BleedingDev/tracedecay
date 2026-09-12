---
name: rn-audit-ncm
overview: "Map NCM dependencies on the current shared profile and identify only integration changes needed if the profile is split or relaxed. Preserve its model, learning, scope identity and lifecycle behavior; account for the already observed unstable recall result without claiming a fix."
todos:
  - id: audit-ncm
    content: "Produce ncm-boundary.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Keep the NCM implementation isolated from the Native repair

## Execution Notes

Map NCM dependencies on the current shared profile and identify only integration changes needed if the profile is split or relaxed. Preserve its model, learning, scope identity and lifecycle behavior; account for the already observed unstable recall result without claiming a fix.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay-memory-provider-ncm/**; product/ncm/spec/CONTRACT.md; NCM registration/host tests; shared API call sites. Write only ../evidence/ncm-boundary.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
