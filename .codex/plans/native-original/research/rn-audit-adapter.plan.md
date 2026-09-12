---
name: rn-audit-adapter
overview: "Map every production path using the added Native staged store and scorer. Propose exact adapter and composition edits to remove that substitute from Native while preserving the original direct operation and avoiding dead routes."
todos:
  - id: audit-adapter
    content: "Produce native-adapter.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Bound the removal of the substitute Native implementation

## Execution Notes

Map every production path using the added Native staged store and scorer. Propose exact adapter and composition edits to remove that substitute from Native while preserving the original direct operation and avoiding dead routes.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay-memory-provider-native/**; crates/tracedecay/src/daemon/retained_owner/native_provider*.rs; native_staged_observations.rs; current callers. Write only ../evidence/native-adapter.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
