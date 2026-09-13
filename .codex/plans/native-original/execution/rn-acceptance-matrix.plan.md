---
name: rn-acceptance-matrix
overview: "Define complete Native, NCM and semantic acceptance coverage"
todos:
  - id: inventory-required-operations
    content: "Map every supported Native operation, NCM mode and semantic serving state to a production route and executable case."
    status: completed
  - id: freeze-acceptance-oracles
    content: "Freeze expected outcomes, negative controls, artifact identities and run counts before implementation."
    status: completed
  - id: publish-missing-coverage
    content: "Publish missing routes, unimplemented coverage and exact downstream owners; no unsupported required row is accepted."
    status: completed
isProject: false
---

# Define complete Native, NCM and semantic acceptance coverage

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execution begins only after lead assignment and graph validation; both were satisfied for the live wave recorded in the graph state directory. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

The 33-node restoration graph remains necessary: the Native adapter, source map, reference runner and host delivery are not finished merely because the CLI boots. Reuse rn-source-docs and rn-reference outputs as they mature; this node defines required coverage without claiming their implementation complete.

Native rows include facts CRUD/supersession, trust/feedback, explicit Search telemetry versus read-only context, ordering/scores/provenance, privacy, retained sessions/refresh/cursors, LCM ingest/compact/search/expand/retention, background ownership, lifecycle failures, restart and saved-state preservation. Every row names its untouched reference entry point, candidate entry point, fixture, effect/receipt oracle and responsible existing node. NCM rows cover disabled/observer/active routing, all seven scope bindings, durable observe/recall, replay/restore, cancellation, worker restart, deletion and readiness. Shared canonical facts cannot satisfy an NCM-only recall oracle. Semantic rows cover acquisition, generation projection, calibration, activation, strict queries, hybrid fallback and recovery.

Use a result ledger with source SHA, binary/model/tokenizer/artifact hashes, features, fixture seed, profile, command, ticket, executed count, expected/actual result and failure classification. Derive identities from the actual build, not the version string alone.

## Constraints

Own only execution-results/readiness-matrix.md and execution-results/readiness-matrix.json. Read code/contracts/tests; do not edit implementation or bless a missing case. Proposed semantic corpus: at least 20 small code units, paraphrase-only positive queries and unrelated negative queries, with relevance/top-k expectations frozen before running. NCM deterministic expected-item checks are separate from probabilistic relevance-quality checks.

## Operator Guidance

No predecessors. Gate rn-reference, rn-source-docs, rn-ncm-reproduce, rn-ncm-tests and rn-semantic-diagnose. After later route inventory, resolve every placeholder before final verification; missing/unreachable required operations block rn-release-readiness. Full host verification here means shipped host entry points, payloads and delivery contracts; actual external AI-app interaction is a separately named manual smoke, never inferred from fixtures or launched through another agent CLI.

## Current dependency contract

Prerequisites: None. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
