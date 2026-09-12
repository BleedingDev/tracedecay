---
name: rn-close
overview: "Accept the complete restored Native implementation. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-close-done
    content: "Accept the complete restored Native implementation and return the required reviewable evidence."
    status: pending
isProject: false
---

# Accept the complete restored Native implementation

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

This node belongs to root and is not spawned as a subagent. Root only orchestrates and reviews; any remaining implementation or documentation correction goes to its named gpt-5.6-luna / max execution owner. Preserve all peer work.

Mode: root orchestration and review only.
Prerequisites: rn-review-fidelity, rn-review-integration. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: All evidence reports and execution outputs. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- Plan statuses, graph snapshot, operator log and final user-facing result

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Review independent findings and all unresolved coverage/results. Assign each required correction back to its execution owner and rerun only affected dependent checks.
2. Confirm complete original code/behavior, real Native context delivery, unchanged NCM internals and safe saved-state cutover. Do not accept partial/full claims interchangeably.
3. Mark execution nodes complete only after their reviewed outcomes exist. Update docs through their writer for final behavior/results; root does not patch product code.
4. Report changed behavior, verification and material limitations in plain English. Commit/push/release only within separate existing user authorization; this plan alone does not request them.

## Acceptance Checklist

- No required original operation, host verification, review blocker or aggregate failure remains.
- Final docs accurately distinguish full Native, shared canonical services and the separate NCM result.
- Root remained orchestrator/reviewer; all execution/review agents used Luna Max.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Complete only when all required work is actually accepted. Otherwise leave the specific nodes incomplete and continue bounded corrections.
