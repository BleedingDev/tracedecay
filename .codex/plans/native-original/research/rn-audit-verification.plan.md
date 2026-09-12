---
name: rn-audit-verification
overview: "Audit present Native parity and host tests for blind spots. Design behavioral direct-original versus modular-Native checks that cover full Native including side effects and real host delivery; define honest Native/NCM evaluation and independent verification ownership."
todos:
  - id: audit-verification
    content: "Produce verification.md with source-backed findings and bounded implementation ownership."
    status: completed
isProject: false
---

# Design falsifiable full-Native verification and comparison

## Execution Notes

Audit present Native parity and host tests for blind spots. Design behavioral direct-original versus modular-Native checks that cover full Native including side effects and real host delivery; define honest Native/NCM evaluation and independent verification ownership.

Read ../prompts/research-common.md before work. Evidence scope: crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs; crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs; crates/tracedecay-memory-conformance/**; scripts/product/memory-comparison/**; product/evaluation/**. Write only ../evidence/verification.md.

## Constraints

Planning only. Original Native code and behavior are protected. No product changes or builds. No subagents. You are not alone; preserve peer work.

## Operator Guidance

This zero-dependency evidence lane can run in parallel with the other audits. Its report is an input to rn-audit-review. Root alone reviews findings, accepts conclusions and updates plan status. Success is a specific source-backed report that can support an implementation node; unknown facts stay unknown.
