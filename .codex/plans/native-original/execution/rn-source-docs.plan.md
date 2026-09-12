---
name: rn-source-docs
overview: "Correct Native identity and architecture documentation. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-source-docs-done
    content: "Correct Native identity and architecture documentation and return the required reviewable evidence."
    status: completed
isProject: false
---

# Correct Native identity and architecture documentation

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: None. Root accepts the complete inventory/documentation independently; the escalated privacy audit and required correction block rn-build, not this documentation output. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: source-baseline.md, native-facts.md, native-sessions.md, saved-data.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- product/upstream/** (metadata/documentation only)
- product/architecture/native-memory-surface-map.md
- product/architecture/native-memory-surface-map.json
- product/architecture/adr/ADR-0010-native-provider-parity-projection.md
- product/architecture/adr/ADR-0008-upstream-convergence.md (stale-reference clarification only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Also own `product/architecture/native-original-source-inventory.md`. Read ../SOURCE-BOUNDARY.md and enumerate every existing b3-to-execution-head hunk across its complete protected/native-adjacent surface. Classify original implementation, exact restoration or pre-existing shared host extension with associated checks. This inventory is a required accepted output before rn-contract and rn-restore-anchor start. No blanket allowlist and no rollback of prior host/privacy/cursor safety fixes.

## Steps

1. Record b3 as this restoration's original reference while retaining truthful history for the older August floor. Correct moved/deleted path references using the reviewed source inventory; do not rewrite historical facts as if b3 were the old floor.
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

