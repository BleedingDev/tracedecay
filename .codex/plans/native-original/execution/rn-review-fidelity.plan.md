---
name: rn-review-fidelity
overview: "Independently review full original Native fidelity. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-review-fidelity-done
    content: "Independently review full original Native fidelity and return the required reviewable evidence."
    status: pending
isProject: false
---

# Independently review full original Native fidelity

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: read-only review.
Prerequisites: rn-verify-facts, rn-verify-sessions, rn-verify-state, rn-verify-claude, rn-verify-codex, rn-verify-ncm, rn-source-docs. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: source-baseline.md, native-facts.md, native-sessions.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- .codex/plans/native-original/execution-results/fidelity-review.md

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Review the final candidate diff against the fixed original source and the original-to-product coverage map. Check protected original code, helpers/tests, schemas and services.
2. Trace representative fact mutations/Search telemetry/read queries and session/LCM operations through the actual assembled product to original services. Look for missing original routes, hidden staged behavior, copied algorithms and effects suppressed by generic capabilities.
3. Inspect independent reference execution and host artifacts, including failures/unknowns. A source-only or same-backend comparison cannot establish behavioral fidelity.
4. Report concrete blocking discrepancies with file/operation/evidence. Do not edit code or accept claims from test counts alone.

Independently review `product/architecture/native-original-source-inventory.md`, SOURCE-BOUNDARY.md and `execution-results/native-operation-routes.md`. Check every protected/native-adjacent hunk, the exact anchor restoration, preservation of host extensions and executed selected-Native composition cases. Require exactly one eligible canonical context execution and zero extra Native provider invocations; output deduplication alone fails.

## Acceptance Checklist

- Complete original Native code and operation coverage are supported by independent source and runtime evidence.
- No stale staged substitute, lost original operation or changed effect boundary remains.
- Limitations are explicitly represented in the final decision.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return blockers or an evidence-backed approval of Native fidelity. Root makes the final acceptance decision.
