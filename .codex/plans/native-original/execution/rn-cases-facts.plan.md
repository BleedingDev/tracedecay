---
name: rn-cases-facts
overview: "Author original fact behavior comparison cases. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-cases-facts-done
    content: "Author original fact behavior comparison cases and return the required reviewable evidence."
    status: pending
isProject: false
---

# Author original fact behavior comparison cases

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: rn-reference, rn-contract. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: native-facts.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- scripts/product/native-original/cases/facts/**

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Also consume the root-accepted `execution-results/native-operation-routes.md` from rn-contract. The contract owner assigns planned case identities; this node implements them and reports any missing original route.

Required runner handoff: `scripts/product/native-original/case-contract.json`, owned and published by rn-reference. Use its exact schema and retained operation shapes. Every original-operation case must invoke the route through selected Native production composition, not only call an unmounted helper. Reference extension-only host checks separately when no original b3 counterpart exists.

## Steps

1. Use rn-reference's accepted case contract. Seed project and profile memory through real original commands with privacy-safe fixed fixtures.
2. Cover a coherent canonical lifecycle: add/duplicate, get/list/exact/history/status, explicit Search and semantic probe/related/reason/contradiction, feedback/trust, update/remove/supersede and original curation/privacy/automatic-fact responsibilities.
3. Check original score components/order/cursors and relevant source/lineage fields. Search verifies its retrieval receipt and refreshed state exactly once; automatic and semantic reads verify no added retrieval effects.
4. Add owner/scope isolation, stale CAS, cancellation/deadline and reopen cases where the real original boundary supports them. Missing callable coverage is reported, never replaced by a mock or optional silent skip.
5. Validate fixtures and expected semantic assertions with runner unit checks. Submit build/run needs; no Cargo or product edits.

Include the original retrieval-anchor latest/history path identified in SOURCE-BOUNDARY.md: ordinary record, absence, relevant invalid earlier history and original error behavior where the public original boundary exposes them. Derive expectations from the immutable b3 reference, independently of rn-restore-anchor's patch; no output from that implementation node is a fixture prerequisite. The product build waits for the restoration before these cases execute. A direct latest-row optimization is not the original oracle.

## Acceptance Checklist

- Positive operations and negative scope/failure cases are nonvacuous and use the independent reference.
- Explicit Search and automatic reads have different telemetry expectations.
- Every original fact responsibility is covered by a case or an explicit unresolved coverage item that blocks final completeness.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return fixtures and exact commands; the later verification owner runs them on built binaries. Do not change expected Native behavior to fit the product.
