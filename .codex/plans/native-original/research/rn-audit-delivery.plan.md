---
name: rn-audit-delivery
overview: "Trace selected memory from configuration and provider mounting through real Claude/Codex/MCP delivery, including context assembly. Find disabled defaults, duplicate memory contributions, extra filtering, tool-only versus automatic recall differences, and all supported hosts affected."
todos:
  - id: audit-delivery
    content: "Produce host-delivery.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Map real host delivery and configuration changes

## Execution Notes

Trace selected memory from configuration and provider mounting through real Claude/Codex/MCP delivery, including context assembly. Find disabled defaults, duplicate memory contributions, extra filtering, tool-only versus automatic recall differences, and all supported hosts affected.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs; crates/tracedecay-agent-hosts/**; crates/tracedecay-cli/** product memory journeys and setup; plugin/**; current context compilation and config owners. Write only ../evidence/host-delivery.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
