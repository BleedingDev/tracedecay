---
name: rn-ncm-tests
overview: "Preserve NCM behavior while Native is restored. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-ncm-tests-done
    content: "Preserve NCM behavior while Native is restored and return the required reviewable evidence."
    status: pending
isProject: false
---

# Preserve NCM behavior while Native is restored

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: write-capable.
Prerequisites: None; this node can start in the first launch wave.. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: ncm-boundary.md, host-delivery.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay-memory-provider-ncm/tests/**
- product/ncm/spec/CONTRACT.md (Replay operation row only; root corrected the stale planned path after verifying runtime dispatch)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Map existing focused tests for current NCM registration, common profile, observation/recall/control, replay, restore identity, cancellation and worker isolation. Add a test only for an uncovered real integration risk.
2. Preserve all seven exact-scope fields including resolved_scope_digest, namespace derivation, model identity, learned state format and worker behavior. Source/module internals are read-only.
3. Cover Native selected plus NCM observer, and NCM selected with shared canonical host state, using existing real registration/worker fixtures where required.
4. Correct a stale Replay documentation claim only from the verified current runtime dispatch; do not change the algorithm to match documentation.
5. Consume the accepted failing/passing evidence and repairs from rn-ncm-byte-budget-fix, rn-ncm-real-fix, rn-ncm-proof-retry and rn-provider-semantics; verify their behavior across the broader integration suite. Retain any unresolved or newly observed incomplete recall explicitly. Submit exact filters and runtime prerequisites to build/verification owners; no Cargo/model run here.

## Acceptance Checklist

- NCM identity, namespace and common-profile behavior remain unchanged.
- Tests preserve the current replay/restore/cancellation assertions and do not hide intermittent results.
- No NCM model/runtime/store/worker source changed.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return focused tests or an evidence-backed no-change result and run list. Do not tune NCM or expand provider features.

## Current dependency contract

Prerequisites: rn-acceptance-matrix, rn-ncm-byte-budget-fix, rn-ncm-real-fix, rn-ncm-proof-retry, rn-provider-semantics. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.

## Added regression coverage

The repair node owns causal runtime/adapter changes and its regression; this node extends coverage across real-worker active/observer routing, all scope fields, replay/restore, cancellation, durable ACK-to-recall ordering, deadline behavior and model/state compatibility. Do not duplicate or revert the repair. Existing preservation constraints apply to this test-only node; they do not veto the explicitly scoped repair node.
