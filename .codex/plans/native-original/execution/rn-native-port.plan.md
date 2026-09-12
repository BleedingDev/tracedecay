---
name: rn-native-port
overview: "Make the Native wrapper delegate without staged semantics. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-native-port-done
    content: "Make the Native wrapper delegate without staged semantics and return the required reviewable evidence."
    status: pending
isProject: false
---

# Make the Native wrapper delegate without staged semantics

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-contract. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: native-adapter.md, native-facts.md, native-sessions.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay-memory-provider-native/** except Cargo manifests

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Read the accepted contract/route decision, then remove StagedSession constants/variant/classification and staged-specific scope/export declarations.
2. Keep zero-contact request validation, exact scope, identity/revision, readiness and operation-specific dispatch. Preserve settled FactPromotion verification without double-writing the fact.
3. Make the port delegate to original Native application/service routes. Original typed Native operations can remain typed facets beside the generic advisory port; do not replace them with generic JSON or implement an engine here.
4. Declare only generic capabilities with exact original semantics. Unsupported structured observations and unmatched generic lifecycle operations return typed unsupported; they must not acknowledge a staged effect.
5. Update focused wrapper tests for delegation, malformed/foreign requests and truthful capability behavior. Publish constructor and export changes to the registry/bridge/host owners.

## Acceptance Checklist

- No StagedSession reaches a product port and no wrapper-owned store/scorer exists.
- Every supported generic operation reaches the correct delegated port; unsupported calls fail before contact.
- Original command surfaces remain reachable by the accepted route design, including facts, sessions and LCM.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return the wrapper diff and exact public imports/constructor contract. Do not touch product concrete bridge, registry, host or original implementation.

