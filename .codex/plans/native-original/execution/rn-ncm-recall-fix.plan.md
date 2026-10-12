---
name: rn-ncm-recall-fix
overview: "Repair the demonstrated NCM recall failure without weakening its contract"
todos:
  - id: repair-causal-seam
    content: "Implement the smallest correction at the demonstrated first loss and preserve durable ordering, scope and provenance."
    status: pending
  - id: prove-regression-and-recovery
    content: "Show the pinned reproducer failing before and passing after; verify restart, cancellation, replay and budget boundaries."
    status: pending
  - id: review-ncm-fix
    content: "Review diff, latency and state/model compatibility; deliver the exact fix and affected verification filters."
    status: pending
isProject: false
---

# Repair the demonstrated NCM recall failure without weakening its contract

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execute only after lead assignment and graph validation. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Latest user direction explicitly adds NCM bug repair; older preservation-only text does not require retaining the bug. rn-ncm-reproduce must first identify the mechanism and lead must narrow ownership to the responsible functions. Candidate seams: provider-ncm/src/common.rs and common/source_binding.rs, rust_backend/**; memory-ncm-runtime/src/engine/runtime/{observe,selection,recovery}.rs, client/** and worker/**; crates/tracedecay/src/daemon/project_composition/ncm_observer.rs or retained_owner/provider_history.rs only if the demonstrated loss is at those host boundaries. Validate actual current paths before editing. A repair outside this shortlist returns to the lead for technical scope assignment, not an invented broad refactor.

Preserve seven-field exact scope, namespace derivation, model/tokenizer pins, receipts/idempotency, privacy/provenance, temporal filters, exclusions, UTF-8 and response budgets. If readiness is causal, establish a truthful durable watermark/barrier or bounded not-ready response; never hide it through sleep/retry-until-lucky. If filtering/top-k is causal, test that exclusion and budgeting occur at the correct contract boundary. Keep actual ranking/model tuning out of this bug-fix task.

## Constraints

Write only the lead-reviewed subset of the candidate seams and their focused regressions, plus execution-results/ncm-recall-fix.md. No original Native algorithms, unrelated NCM features, schema migrations or live model/state changes. Record any truly necessary state-format compatibility issue as a separate blocked design item; do not silently break old namespaces or snapshots.

## Operator Guidance

Depends on rn-ncm-reproduce; gate rn-ncm-tests and final NCM verification through rn-build. Commit and push reviewed fixes on the same branch. A no-code setup correction is acceptable only with the failing/passing causal proof and a repeatable operator procedure. No closure solely from one successful recall or the rc13 encoder checks.

## Current dependency contract

Prerequisites: rn-ncm-reproduce. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
