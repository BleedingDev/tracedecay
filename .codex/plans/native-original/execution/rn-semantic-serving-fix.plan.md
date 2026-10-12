---
name: rn-semantic-serving-fix
overview: "Repair the diagnosed semantic query-serving transition while preserving strict mode and truthful hybrid fallback."
todos:
  - id: repair-serving-transition
    content: "Implement the smallest correction at the code-index/query serving boundary identified by rn-semantic-runtime-dynamic."
    status: pending
  - id: prove-strict-serving
    content: "Verify frozen paraphrase and negative controls use real semantic evidence and never pass through lexical-only output or an unavailable authority."
    status: pending
  - id: publish-serving-handoff
    content: "Return source diff, lifecycle compatibility, focused filters and build/semantic-verification inputs without changing acquisition ownership."
    status: pending
isProject: false
---

# Repair semantic query serving

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md`, `../execution-results/semantic-diagnosis.md` and the accepted `execution-results/semantic-runtime-dynamic.md`. Use the clean 570 reference and pinned fixture read-only. Stay in Codex; the single Cargo owner handles build and runtime tickets.

This is the serving-side replacement for `rn-semantic-fix`. It starts only after dynamic evidence names a query-serving or authority-selection boundary. The acquisition-side semantic fix has separate ownership and may proceed in parallel after the same dynamic gate.

## Ownership and Constraints

Own only the demonstrated subset of `crates/tracedecay-code-index-runtime/src/code_index_scheduler/semantic_query_runtime.rs`, `crates/tracedecay-code-index-runtime/src/code_index_scheduler/queries.rs`, `crates/tracedecay-query/src/retrieval/semantic/service.rs`, and `execution-results/semantic-serving-fix.md`. Root must narrow the actual files from the dynamic handoff before editing.

Do not edit application/model lifecycle acquisition files, CLI manifests, model pins, calibration thresholds, Native memory/session/LCM routes or user/global state. Preserve strict semantic failure, explicit hybrid fallback, source-generation coherence, provider/model identity, bounded deadlines, cancellation and restart behavior. Correct text returned by lexical search is not semantic proof.

## Acceptance Checklist

- The dynamic failing transition has a causal before/after regression with a real active semantic authority.
- Frozen paraphrase queries show semantic participation and expected relevance; unrelated negative controls remain negative.
- Missing/corrupt/mismatched authority and explicit hybrid fallback remain truthful, and source update/restart recovery is preserved.
- This repair gates `rn-build` and `rn-verify-semantic` through explicit active edges.

## Operator Guidance

Return source symbols, focused test filters, serving receipts, strict/hybrid mode evidence and exact recovery commands for the semantic verification owner. If the dynamic evidence points to acquisition, stop without editing this scope and route the result to `rn-semantic-acquisition-fix`.

## Current dependency contract

Prerequisite: `rn-semantic-runtime-dynamic`. The exact active edges are in `../execution-selection.json`; this node supersedes the serving portion of `rn-semantic-fix` without deleting that historical plan.
