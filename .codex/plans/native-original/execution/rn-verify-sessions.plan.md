---
name: rn-verify-sessions
overview: "Verify original sessions and LCM against untouched upstream 570. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-verify-sessions-done
    content: "Verify original sessions and LCM against untouched upstream 570 and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify original sessions and LCM against untouched upstream 570

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: verification-only.
Prerequisites: rn-build, rn-cases-sessions. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: native-sessions.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/sessions/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Run accepted original/product session and LCM cases with the tested binaries and isolated process/store roots.
2. Check canonical ingest, temporal retrieval/refresh/task-session behavior, protected raw/payload expansion, summary membership/lineage, lifecycle/retention and restart.
3. Verify original denial, omission, freshness, cancellation and ResetRequired distinctions; a staged row or provider output never satisfies canonical evidence.
4. Retain actual model/service prerequisites and report unknown coverage honestly. Do not fabricate a summary or skip an unreachable original route.
5. Return focused reproductions to root; do not edit source or fixtures.

## Acceptance Checklist

- Original session and LCM public behavior and relevant durable effects match at corresponding boundaries.
- Canonical source/cursor/payload and summary lineage remain intact after reopen.
- All mapped session/LCM responsibilities have evidence or remain explicit blockers to completeness.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return bounded results and original coverage gaps. No new models, engine tuning or source edits.

## Current dependency contract

Prerequisites: rn-build, rn-cases-sessions. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
