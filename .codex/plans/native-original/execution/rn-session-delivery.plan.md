---
name: rn-session-delivery
overview: "Retire Native staged acknowledgements in the host observation journey. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-session-delivery-done
    content: "Retire Native staged acknowledgements in the host observation journey and return the required reviewable evidence."
    status: pending
isProject: false
---

# Retire Native staged acknowledgements in the host observation journey

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-native-port, rn-privacy-restore. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: native-sessions.md, native-adapter.md, host-delivery.md, saved-data.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- crates/tracedecay/src/daemon/retained_owner/observation_journey.rs

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

Read and preserve accepted ../execution-results/privacy-restoration.md typed history admission before editing.

1. Consume Native's accepted observation/capability decision. Remove expectations that Native persists session/source/test/feedback observations into staged provider state.
2. Keep canonical session admission, settlement, journal, replay cursor, privacy and original ingestion unchanged. A typed unsupported Native observation must settle according to existing classification without a perpetual retry loop or fabricated success.
3. Preserve configured NCM observation delivery, durable receipts and host-state authority when Native is selected or observed.
4. Replace only staged-specific journey assertions with canonical-ingestion continuity, truthful Native terminal and unaffected NCM delivery checks.
5. Send changed constructor/caller requirements to composition; keep the generic host observation contract and original session engine untouched.

## Acceptance Checklist

- A real settled session still reaches original canonical session/LCM state.
- Native unsupported observations do not create a staged receipt or stall journal progress.
- NCM observation delivery and retry/receipt semantics remain intact.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return the one-file journey diff and focused tests. Do not change generic schemas, cursor policy or original ingestion.

