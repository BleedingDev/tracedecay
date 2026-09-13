---
name: rn-source-docs
overview: "Correct Native identity and architecture documentation. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-source-docs-done
    content: "Correct Native identity and architecture documentation and return the required reviewable evidence."
    status: pending
isProject: false
---

# Correct Native identity and architecture documentation

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: write-capable.
Prerequisites: rn-acceptance-matrix. Root accepts the complete inventory/documentation independently; the escalated privacy audit and required correction block rn-build, not this documentation output. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: source-baseline.md, native-facts.md, native-sessions.md, saved-data.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- product/upstream/README.md
- product/upstream/convergence-map.json
- product/upstream/pr707-floor.json
- product/upstream/tracedecay-v2-pr707.json
- product/architecture/native-memory-surface-map.md
- product/architecture/native-memory-surface-map.json
- product/architecture/native-original-source-inventory.md
- product/architecture/adr/ADR-0010-native-provider-parity-projection.md
- product/architecture/adr/ADR-0008-upstream-convergence.md (stale-reference clarification only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Read ../SOURCE-BOUNDARY.md and enumerate every existing 570-to-execution-head hunk across its complete protected/native-adjacent surface in the owned source inventory. Classify original implementation, exact restoration or pre-existing shared host extension with associated checks. This inventory is a required accepted output before rn-contract and rn-restore-anchor start. No blanket allowlist and no rollback of prior host/privacy/cursor safety fixes.

## Steps

1. Record unmodified upstream 570 as this restoration's active original reference while retaining truthful history for the older b3/August floor. Correct moved/deleted path references using the reviewed source inventory; do not rewrite historical facts as if 570 were the old floor.
2. Document complete original Native facts, sessions and LCM, the shared canonical host authorities, and the product adapter boundary. Reconcile ADR-0010's limited projection with the full Native decision; no unsupported generic feature becomes a missing Native operation.
3. Document removal of the staged implementation and leaving its saved bytes untouched. State Native/NCM selection and shared host services accurately; no new feature flag, migration or approval gate.
4. Keep implementation status explicitly planned until evidence exists. Send final implementation-dependent wording to root for review instead of claiming tests or restoration already passed.

## Acceptance Checklist

- All new baseline/path claims resolve in the pinned local trees.
- Docs distinguish original implementation, product interface and generic unsupported behavior.
- No upstream import, source code, runtime setting or generated provider contract changed.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return only corrected identity/architecture documents, with any wording that must wait for verification identified.

## Current dependency contract

Prerequisites: rn-acceptance-matrix. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
