---
name: rn-semantic-diagnose
overview: "Locate why semantic acquisition or serving remains unavailable"
todos:
  - id: reproduce-semantic-states
    content: "Reproduce fresh-profile, provisioned-profile and restart behavior using the real CLI and strict semantic requests."
    status: pending
  - id: trace-semantic-activation
    content: "Trace artifact acquisition, generation projection, calibration persistence and scoped query-authority activation to the first missing transition."
    status: completed
  - id: freeze-semantic-fix-case
    content: "Publish the responsible setup or code seam, negative controls and failing acceptance case before repair."
    status: completed
isProject: false
---

# Locate why semantic acquisition or serving remains unavailable

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execute only after lead assignment and graph validation. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Observed smoke evidence: exact search became fresh and returned branch_smoke; semantic search stayed calibration_unavailable; logs also contained semantic_projection_schedule outcome=artifact_unavailable. This does not prove a corrupt calibration formula. semantic_query_runtime.rs emits CalibrationUnavailable when semantic_query_authority_for_scope is absent. StrictSemantic requests enqueue demand acquisition before canonical generation lookup; the earlier hybrid smoke did not prove that path.

Read crates/tracedecay-code-index-runtime/src/code_index_scheduler/{semantic_query_runtime,queries}.rs; crates/tracedecay-application/src/semantic_runtime/{production,acceptance_calibration}.rs; crates/tracedecay-semantic/src/model_lifecycle/{owner,acquisition,reconciliation,persistence}.rs and crates/tracedecay-semantic/src/artifact_store.rs; crates/tracedecay-query/src/retrieval/semantic/service.rs. Use supported status/doctor/model lifecycle commands, current CLI schemas and synthetic logs. Establish configured provider/model, artifact receipt/hash, generation compatibility, measured calibration availability, activation route, authenticated checkout and query generation. Compare unmodified 570 when behavior may be upstream by design.

Test an empty profile, correctly provisioned compatible artifacts, deliberately missing/corrupt/mismatched artifacts, index generation changes, restart and same-checkout HEAD movement. Distinguish intended lazy acquisition or missing setup from a failure to recover after successful installation. Report network/offline/cancellation behavior separately.

## Constraints

Read-only implementation. Own execution-results/semantic-diagnosis.md and synthetic fixtures/results below target/test-profile/readiness/semantic/diagnose. No fake calibration, bypassed compatibility guards, setting changes in operator profiles or global installation. Keep code-search semantics separate from Native fact/session memory algorithms.

## Operator Guidance

Depends on rn-acceptance-matrix; gates rn-semantic-runtime-dynamic. Return a concrete transition failure and bounded fix scope, or a proven setup correction with runnable provisioning steps. A generic availability string is not a root-cause diagnosis. Supported acquisitions run only in isolated test profiles through the build owner/runtime verifier.

## Current dependency contract

Prerequisites: rn-acceptance-matrix. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.

## Fixture lane update

The exact JinaEmbeddingsV2BaseCode fixture for revision
`516f4baf13dec4ddddda8631e019b5737c8bc250` has been acquired with the
project-supported `tests/distribution/fastembed/prepare_fixture.py` contract
and validated with `validate_fixture.py`. It is isolated at
`target/test-profile/readiness/semantic/diagnose/jina-516f4baf13dec4ddddda8631e019b5737c8bc250`.
The complete lengths, SHA-256 values, commands, and absolute root are in
`execution-results/semantic-diagnosis.md` and the adjacent
`target/.../fixture-verification.json` artifact. The dynamic todo remains
pending until a designated candidate build/runtime owner uses this fixture to
run acquisition, activation, strict serving, and restart evidence with offline
runtime flags.
