---
name: rn-audit-interface
overview: "Determine the minimum changes to core provider API, registry, routing and common profile needed to run original Native without suppressing behavior or forcing NCM operations onto it. Identify existing extension points and exact shared-file ownership."
todos:
  - id: audit-interface
    content: "Produce core-interface.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Determine core interface and routing corrections

## Execution Notes

Determine the minimum changes to core provider API, registry, routing and common profile needed to run original Native without suppressing behavior or forcing NCM operations onto it. Identify existing extension points and exact shared-file ownership.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay-memory-provider-api/**; crates/tracedecay-memory-provider-registry/**; crates/tracedecay-memory-fabric/**; product/contracts/memory-provider-v1/**. Write only ../evidence/core-interface.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
