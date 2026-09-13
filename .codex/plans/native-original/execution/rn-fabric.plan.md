---
name: rn-fabric
overview: "Preserve routing guarantees for the restored Native boundary. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-fabric-done
    content: "Preserve routing guarantees for the restored Native boundary and return the required reviewable evidence."
    status: pending
isProject: false
---

# Preserve routing guarantees for the restored Native boundary

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-contract, rn-integration-readiness. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: core-interface.md, ncm-boundary.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay-memory-fabric/** except Cargo manifests

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Apply only concrete API changes accepted by rn-contract. If current fabric already supports the decision, preserve it and add only missing behavioral coverage.
2. Keep selected provider identity/revision, fresh handshake, exact scope, readiness, cancellation/deadline, expected generation and terminal validation consistent.
3. Do not infer fallback from an empty/unavailable result or provider name. Retain the existing explicit fallback policy.
4. Keep read-only Recall semantics for original read operations and original retained Search effects outside that read projection. A canonical receipt must not be forged as provider-local generation.
5. Give the build owner focused filters and any compile-sensitive API use.

## Acceptance Checklist

- Wrong revision/scope/readiness and unauthorized fallback are refused before contact.
- Empty Native results do not invoke NCM.
- No unconditional common-profile or mutation-on-recall policy was added.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return a bounded fabric diff or evidence-backed no-change result. Do not broaden routing architecture.
