---
name: rn-native-bridge
overview: "Replace the staged Native backend with original service delegation. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-native-bridge-done
    content: "Replace the staged Native backend with original service delegation and return the required reviewable evidence."
    status: pending
isProject: false
---

# Replace the staged Native backend with original service delegation

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-native-port. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: native-adapter.md, native-facts.md, native-sessions.md, saved-data.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay/src/daemon/retained_owner/native_provider.rs
- crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs (remove added implementation)
- At most one new adjacent native_original_bridge.rs if delegation cannot fit the existing module

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Use accepted wrapper interfaces and existing owner-bound original ports. Keep project/profile authority and original shared project-memory identity; never open a provider-selected canonical database.
2. Remove staged field/open, Stage/control actor commands, custom scorer, normal merged candidates, common-mode empty fact page, staged projections, lifecycle receipts and staged generation.
3. Delegate corresponding Native reads and original typed commands without changing query fields, limits, scores, order, telemetry, trust, lineage, effects or errors. Keep explicit retained Search on the original direct route and automatic queries read-only.
4. Reuse original session/LCM ports from composition or retain their original typed routes; no second ingest path, temporal service, store, summarizer or background worker.
5. Remove the added staged implementation source. Leave all persisted files/sidecars untouched and implement no compatibility or data movement. Preserve bounded execution/cancellation where needed without retaining staged-only actor machinery.
6. Publish constructor/authority requirements, removed module declarations and test handoffs to composition and Native test owners. Do not edit their files to make interim compilation pass.

Required route proof: consume `execution-results/native-operation-routes.md`. Invoke every mapped original route through real selected Native production composition using the corresponding fixture (execution is scheduled by the build/verification owners). Record original public caller, owner, effect/receipt and case. Direct service availability alone does not prove the composed route is live. Any missing original route is a blocker; generic unsupported cannot conceal it.

## Acceptance Checklist

- Native opens no staged store and returns no staged candidates/references/generation.
- Supported Native calls use original owner-bound services and canonical receipts; unsupported generic calls are explicit.
- Protected original files remain unchanged; no copied scoring, persistence or LCM implementation appears.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return the complete delegating bridge diff. A missing original port is a bounded interface issue for root, not permission to implement a replacement or edit original code.
