---
name: rn-cases-state
overview: "Author saved-state and owner isolation comparison cases. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-cases-state-done
    content: "Author saved-state and owner isolation comparison cases and return the required reviewable evidence."
    status: pending
isProject: false
---

# Author saved-state and owner isolation comparison cases

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-reference, rn-contract. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: saved-data.md, native-facts.md, native-sessions.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- scripts/product/native-original/cases/state/**

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Also consume the root-accepted `execution-results/native-operation-routes.md` from rn-contract. The contract owner assigns planned case identities; this node implements them and reports any missing original route.

Required runner handoff: `scripts/product/native-original/case-contract.json`, owned and published by rn-reference. Use its exact schema and retained operation shapes. Every original-operation case must invoke the route through selected Native production composition, not only call an unmounted helper. Reference extension-only host checks separately when no original b3 counterpart exists.

## Steps

1. Seed original canonical facts, sessions, journal/receipt state using real public operations. Test restart and project memory visibility across linked worktrees while keeping profile and project owners distinct.
2. Place a test-owned legacy staged file and sidecars at the historical location. Verify Native startup and use leave their bytes and metadata unchanged and create no new staged file when absent. Do not import or parse them into canonical memory.
3. Verify original journal/cursor continuity and canonical receipt/trust/retrieval state survive the cutover. No migration, replay reset, backup/quarantine operation or compatibility reader is expected.
4. Exercise ResetRequired only with a fixture that the original store actually rejects that way. Verify it is not invented for a harmless leftover staged file.
5. Use the comparison runner's semantic assertions and fixture-owned filesystem checks; never inspect operator databases.

This lane owns shared-host extension regressions for ordinary/reserved cursor round-trip, exact-CAS rejection and locator conflict/rollback/reopen. The session fixture lane owns sealed-source admission, catch-up and refresh cases. Keep extension-only outcomes separate from b3 Native equivalence and preserve current safety behavior.

## Acceptance Checklist

- Existing staged bytes are untouched and absence stays absent after Native operations.
- Canonical state and ownership survive reopen with the original semantics.
- The tests reject silent reset, new database authority and staged promotion.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return state fixtures; do not edit storage code or implement a migration.

