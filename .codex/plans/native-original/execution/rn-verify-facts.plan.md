---
name: rn-verify-facts
overview: "Verify complete fact behavior against untouched b3. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-verify-facts-done
    content: "Verify complete fact behavior against untouched b3 and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify complete fact behavior against untouched b3

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: verification-only.
Prerequisites: rn-build, rn-cases-facts. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: native-facts.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/facts/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Use rn-build's tested artifacts and rn-reference's runner with the accepted fact cases. Do not rebuild or alter code/expectations.
2. Run independent original/product journeys for project/profile facts, explicit Search telemetry, semantic reads, trust/feedback, lifecycle, privacy/curation/automatic responsibilities and original errors.
3. Compare scores/order/cursors/provenance and operation-specific state/receipts before/after reopen. Trace each failure to the matching original production boundary.
4. Retain failures, unknown/unreachable original coverage and censored runs. Report exact reproduction and owner; root assigns fixes.
5. Check the completed original fact coverage map, not merely the number of passing tests.

## Acceptance Checklist

- Every mapped original fact responsibility has executed positive evidence and appropriate no-effect/failure evidence.
- Original explicit Search tracking is preserved, and automatic/semantic reads gain no new telemetry.
- No adapter-only mock or shared-current-backend comparison supports the equivalence claim.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return pass/fail/unknown coverage with raw artifacts and exact reproductions. Any uncovered original behavior prevents claiming complete Native.

