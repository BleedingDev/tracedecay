---
name: rn-release-readiness
overview: "Establish repeatable Native/NCM/semantic readiness for an isolated trial"
todos:
  - id: check-complete-evidence
    content: "Require complete Native reference parity, repaired NCM recall and provisioned semantic retrieval with no uncovered required row."
    status: pending
  - id: repeat-integrated-acceptance
    content: "Run three consecutive integrated acceptance passes on the same reviewed source and artifact identities with fresh profiles."
    status: pending
  - id: prepare-isolated-pilot
    content: "Publish one tested Codex-native trial configuration, data isolation, rollback instructions and remaining optional limitations."
    status: pending
isProject: false
---

# Establish repeatable Native/NCM/semantic readiness for an isolated trial

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execute only after lead assignment and graph validation. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Consume all independent reviews and rn-acceptance-matrix. Full Native means complete original facts, sessions, LCM, automatic responsibilities, preserved saved-state effects and shipped host delivery contracts through corresponding independent original/candidate production routes. NCM means real worker/model, active and observer modes, reliable admitted delivery, namespace/replay/restart/cancellation coverage and the demonstrated intermittent failure repaired. Semantic means provisioned actual semantic retrieval and recovery, not lexical fallback.

Run the required repository aggregate suite selected by rn-build once at the final candidate; additionally run three consecutive integrated Native/NCM/semantic acceptance passes using identical source/binary/model identities and separate fresh profiles. Collect all attempts; any failure resets the consecutive-pass count after its bounded correction. Validate observer + Native and NCM-selected routing, exactly-once canonical memory_matches, no cross-scope contamination, model-visible provenance, restart and cancellation. Existing latency and shutdown budgets remain fixed; do not redefine them to pass.

Publish tested binary and worker paths/hashes, exact features, model/calibration prerequisites, profile/DB/socket isolation and init/readiness commands. Verify the pilot config through Codex-native interfaces. Actual external-host app smoke is distinctly reported and must not launch another agent CLI without explicit user authorization. Keep the stable service and databases intact; rollout of the default installation is a later user decision, not implied by this plan.

## Constraints

Own execution-results/release-readiness.md and test-owned reports under target/test-profile/readiness/aggregate. No source edits, global installation, operator-profile migration, data copying/backfill or production service switch. Root must not mark full readiness with required coverage missing, unresolved NCM misses, semantic unavailable, a flaky pass, a skipped dependency or a same-backend Native reference.

## Operator Guidance

Depends on rn-review-fidelity, rn-review-integration and rn-verify-semantic. Gate rn-close. The build owner submits Cargo through the broker; verifiers run already-built artifacts and preserve all receipts. Root reviews, commits and pushes the report on the one working branch; no implementation agents are launched during planning.

## Current dependency contract

Prerequisites: rn-review-fidelity, rn-review-integration, rn-verify-semantic. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
