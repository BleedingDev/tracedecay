---
name: rn-cases-sessions
overview: "Author original session and LCM comparison cases. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-cases-sessions-done
    content: "Author original session and LCM comparison cases and return the required reviewable evidence."
    status: pending
isProject: false
---

# Author original session and LCM comparison cases

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-reference, rn-contract. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: native-sessions.md, host-delivery.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- scripts/product/native-original/cases/sessions/**

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Also consume the root-accepted `execution-results/native-operation-routes.md` from rn-contract. The contract owner assigns planned case identities; this node implements them and reports any missing original route.

Required runner handoff: `scripts/product/native-original/case-contract.json`, owned and published by rn-reference. Use its exact schema and retained operation shapes. Every original-operation case must invoke the route through selected Native production composition, not only call an unmounted helper. Reference extension-only host checks separately when no original b3 counterpart exists.

## Steps

1. Use real admitted host transcript fixtures and the accepted runner contract. Keep logical source/session identity and scopes equivalent on both isolated sides.
2. Exercise canonical ingestion and cursor idempotency, message/temporal retrieval, freshness/refresh/task-session routes, restart and wrong-scope/omission outcomes.
3. Exercise original LCM load/search/describe/expand/expand-query and ingest/compact/status/doctor through existing mounted services. Compare raw/payload verification, summary membership/lineage and relevant retention behavior with actual original outputs.
4. Keep existing original summary generation and authority. Do not synthesize a new summarizer or treat a staged observation as canonical raw memory. Unavailable model or source prerequisites are reported as unknown coverage.
5. Cover host source append/restart behavior with the original protections and avoid provider-specific fixture tuning.

This lane owns shared-host extension regressions for stable versus replaced sealed sources, ordinary historical catch-up and refresh begin/status/cancel after restart. The state fixture lane owns cursor encoding/CAS and locator transaction cases. Keep extension-only outcomes separate from b3 Native equivalence and add no second Native ingest path.

## Acceptance Checklist

- A transcript persists through the canonical ingestion/LCM route and is retrievable after reopen on both sides.
- Lost raw lineage, altered temporal result/coverage or changed cursor behavior causes a comparison failure.
- No staged database, new scorer, fake LCM service or fabricated summary is used.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return source-safe fixtures and required runtime/model prerequisites. Original behavior gaps stay blocking coverage items for final parity.

