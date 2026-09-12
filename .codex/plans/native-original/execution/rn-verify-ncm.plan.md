---
name: rn-verify-ncm
overview: "Verify NCM still works with shared canonical services. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-verify-ncm-done
    content: "Verify NCM still works with shared canonical services and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify NCM still works with shared canonical services

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: verification-only.
Prerequisites: rn-build. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: ncm-boundary.md, host-delivery.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/ncm/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Run the accepted current NCM focused suite and real worker smoke using tested artifacts and isolated exact scope.
2. Verify common-profile admission, observations, recall, replay/restore identity, cancellation and Native-selected/NCM-observer coexistence.
3. Verify NCM-selected delivery remains attributed separately from shared canonical Native facts and session/LCM context. Do not call this isolated NCM quality.
4. Preserve all seven scope fields, current namespace/model/state format and source integrity. Record model/executable prerequisites.
5. Retain the earlier intermittent recall outcome and any new failures. Repetition is for diagnosing stability, not selecting a favorable run.

## Acceptance Checklist

- NCM source/model/state identity and original worker behavior are unchanged.
- Real NCM active and observer operation succeeds under preserved registration/host authority.
- Known intermittent recall is not relabelled fixed by a single pass.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return regression/stability evidence separately from Native parity. No algorithm/model tuning or broad comparative-quality campaign.

