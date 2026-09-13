---
name: rn-composition
overview: "Connect the reviewed Native services and remove staged mount assumptions. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-composition-done
    content: "Connect the reviewed Native services and remove staged mount assumptions and return the required reviewable evidence."
    status: pending
isProject: false
---

# Connect the reviewed Native services and remove staged mount assumptions

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: write-capable.
Prerequisites: rn-native-bridge, rn-registry, rn-session-delivery, rn-host-context, rn-native-tests. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: host-delivery.md, native-adapter.md, saved-data.md, native-sessions.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay/src/daemon/project_composition.rs
- crates/tracedecay/src/daemon/retained_owner.rs

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Consume accepted bridge/registry constructors and host decisions. Mount original Native services with the existing authority-selected project/profile and shared session/LCM ports.
2. Remove staged module declarations, staged-only provider-state-root arguments and async-open assumptions. Keep blocking work only if the actual original service requires it.
3. Wire declared Native test modules and all reviewed caller updates in these owned files; do not edit original services or peer adapters.
4. Preserve disabled/observer/active selection, authoritative code scope before mounting, NCM construction/namespace/history authority, lifecycle ownership and original settings.
5. Publish the assembled API/manifest needs to the build owner and record which original fact/session/LCM routes remain reachable through production composition.

Required route proof: consume `execution-results/native-operation-routes.md`. Invoke every mapped original route through real selected Native production composition using the corresponding fixture (execution is scheduled by the build/verification owners). Record original public caller, owner, effect/receipt and case. Direct service availability alone does not prove the composed route is live. Any missing original route is a blocker; generic unsupported cannot conceal it.

## Acceptance Checklist

- Native mounts without opening the added store and original typed operations remain reachable.
- NCM active/observer composition and host authority are preserved.
- No new owner, fallback, replay reset, config default or persisted-data action was introduced.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return the coherent two-file composition diff. Shared manifest changes go to rn-build; missing peer behavior goes back to its exact owner.

## Current dependency contract

Prerequisites: rn-native-bridge, rn-registry, rn-session-delivery, rn-host-context, rn-native-tests. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
