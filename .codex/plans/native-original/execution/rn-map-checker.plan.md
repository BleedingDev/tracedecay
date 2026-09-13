---
name: rn-map-checker
overview: "Repair stale source-path expectations in the existing Native surface-map checker."
todos:
  - id: rn-map-checker-done
    content: "Correct stale source expectations without weakening map validation and run its checks."
    status: pending
isProject: false
---

# Correct Native map checker source drift

Root confirmed pre-existing failures after source map paths were corrected. Workdir /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2 on every command. HEAD1fe250fed7ca615f1dfdcd580befeb276328e910. Read AGENTS.md, ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md. rn-source-docs must refresh the active 570 inventory first; the earlier accepted map check is historical. Codex-native execution leaf, no Cargo, preserve peers.

Own ONLY scripts/product/check-native-memory-surface-map.py and tests/product_native_memory_surface_map_test.py. Source/docs/generators read-only. No commits/push.

Correct three stale marker paths: runtime-core/src/store/memory/mod.rs -> session-memory/src/fact_store/mod.rs; old daemon/retained_owner/memory.rs -> tracedecay-store-runtime/src/retained_memory.rs; old tracedecay-application/src/retained_surfaces.rs -> tracedecay-contracts/src/retained_surfaces.rs. Ground every marker in actual symbols; SDK operation IDs are derived from the contracts catalog, not static literals in sdk/operations.rs. Preserve verification of that delegation and the actual catalog IDs; do not remove checks simply to pass. Update any repeated stale path in validate_paths.

Run Python checker and existing map tests after rn-source-docs acceptance, preserving missing operation/transport/authority/source rejection assertions. Add focused fixture only if necessary to verify corrected catalog delegation, not implementation-mirroring string counts. This checker is documentation consistency evidence, not original runtime parity. Return exact diff, checks, remaining blockers. No new checker framework or broad map schema/count changes. Root reviews before accepting.

## Current dependency contract

Prerequisites: rn-source-docs. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
