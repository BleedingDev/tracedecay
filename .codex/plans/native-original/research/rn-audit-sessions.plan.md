---
name: rn-audit-sessions
overview: "Trace original session ingestion, session recall, summary/LCM retrieval and expansion, and context use. Establish which operations belong to Native memory versus shared host infrastructure. Identify current additions and all original behavior needed for truly full Native."
todos:
  - id: audit-sessions
    content: "Produce native-sessions.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Map complete Native session and LCM behavior

## Execution Notes

Trace original session ingestion, session recall, summary/LCM retrieval and expansion, and context use. Establish which operations belong to Native memory versus shared host infrastructure. Identify current additions and all original behavior needed for truly full Native.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay-sessions/**; crates/tracedecay-session-runtime/**; original session and LCM application entry points; related existing tests. Write only ../evidence/native-sessions.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
