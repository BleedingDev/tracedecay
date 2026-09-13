---
name: rn-verify-state
overview: "Verify cutover leaves original and legacy saved state intact. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-verify-state-done
    content: "Verify cutover leaves original and legacy saved state intact and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify cutover leaves original and legacy saved state intact

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: verification-only.
Prerequisites: rn-build, rn-cases-state. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: saved-data.md, source-baseline.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/state/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Run accepted state cases through real original/product starts and operations with isolated fixture roots.
2. Verify existing staged files/sidecars remain untouched and no staged database appears when absent.
3. Verify original canonical facts, sessions, receipts, journal cursors and project/worktree/profile ownership survive restart with original semantics.
4. Verify original ResetRequired cases separately from harmless staged leftovers. Confirm no quarantine, data copy, migration or replay reset happened.
5. Report only test-owned filesystem evidence; never open operator databases.

## Acceptance Checklist

- Legacy staged bytes are left alone and are not treated as canonical memory.
- Original state, receipts and scope behavior survive cutover/reopen.
- No unauthorized migration/deletion/reset is necessary for Native startup.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return exact filesystem and semantic assertions, with any mismatch routed to the owning implementation node.

## Current dependency contract

Prerequisites: rn-build, rn-cases-state. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
