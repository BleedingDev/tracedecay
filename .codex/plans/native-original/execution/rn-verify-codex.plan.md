---
name: rn-verify-codex
overview: "Verify original Native through real Codex delivery. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-verify-codex-done
    content: "Verify original Native through real Codex delivery and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify original Native through real Codex delivery

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: verification-only.
Prerequisites: rn-build. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: host-delivery.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/codex/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Use accepted real Codex journey fixtures with test-owned global/local bundles and separately built original/product binaries. Validate actual activation/trust/rendering required by the fixture.
2. Execute real start/stop/append ingestion and one scheduled context action per case. Keep actual host session ID and transcript identity; capture exact final delivered UTF-8 and typed provenance.
3. Check original Native context appears once and explicit fact/session/LCM access still reaches original services. No fixed four-message staged expectation.
4. Verify scoped refusal/deadline/restart and existing Stop nonstarvation behavior using the supplied remaining deadline; preserve all relevant assertions.
5. Retain failed and repeated outcomes, process cleanup and exact binary identity. Do not tune NCM or host budgets.

## Acceptance Checklist

- Actual rendered Codex delivery preserves original Native behavior and source identity.
- Start/Stop/append/restart behavior remains correct without staged storage.
- No fake activation, shortened timeout assertion or duplicated Native result supports a pass.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return real captures and reproducible failures. No live user plugin/config/database changes.

