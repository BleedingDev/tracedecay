---
name: rn-ncm-reproduce
overview: "Reproduce intermittent NCM recall with stage-by-stage evidence"
todos:
  - id: recover-original-misses
    content: "Recover the original 2-of-4 versus 4-of-4 workload and verify which items were required under its limits and scope."
    status: pending
  - id: trace-first-loss
    content: "Reproduce cold, warm, restart and load cases with pinned real worker/model; identify the first stage losing required data."
    status: pending
  - id: add-failing-regression
    content: "Reduce a failing case into a deterministic regression or controlled fault/scheduling test with a demonstrated failing baseline."
    status: pending
isProject: false
---

# Reproduce intermittent NCM recall with stage-by-stage evidence

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execute only after lead assignment and graph validation. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Start from execution-results/ncm-tests.md, existing rust_backend tests and the read-only evidence in crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs. Historical evidence is 2/4 followed by 4/4; the current root cause is unknown. Recover original logs/fixture/seed and their actual expectation; if unavailable, record that gap and construct a contract-derived reproducer without calling the historical issue fixed.

Record synthetic correlation IDs across host admission, canonical commit/journal sequence, worker readiness, observation ACK/durable watermark, exact-scope namespace, replay/projection freshness, embedding/model identity, candidate selection, filtering/exclusions/top-k, byte/token budgeting, adapter admission and final model-visible delivery. Compare immediate post-ACK recall with settled recall, process restart and concurrent ingestion. Find where an eligible expected item first disappears. A durable ACK must not be replaced by an arbitrary sleep.

Inspect .codex/patches/pm-ncm-recall-trace-budgets as historical design evidence only. Its manifest says `frozen_unapplied`, but current common.rs and source_binding.rs match the proposed hashes and selection.rs contains the behavior with only a test-helper rename. Do not reapply it. Its presence still does not prove that trace exclusions or UTF-8 budgeting explain the intermittent result; establish causality against the actual current source.

## Constraints

Own only new reproduction fixtures `crates/tracedecay-memory-provider-ncm/tests/ncm_recall_reproduction.rs`, `crates/tracedecay-memory-ncm-runtime/tests/recall_reproduction.rs`, and `crates/tracedecay-cli/tests/product_memory_provider_ncm_recall_reproduction.rs`; own execution-results/ncm-recall-reproduction.md and synthetic logs under target/test-profile/readiness/ncm/reproduce. Existing NCM and Claude host tests plus shared comparison fixtures are read-only. If existing receipts cannot expose the first loss, the lead may assign minimal synthetic-only tracing to exact functions in the candidate repair seams listed in rn-ncm-recall-fix. This node owns that instrumentation before the fix node; it must not change runtime behavior or widen logging to real memory contents. Do not alter models, relevance thresholds, scope, budgets or expected answers to manufacture reproduction/passes.

## Operator Guidance

Depends on rn-acceptance-matrix. Gate rn-ncm-recall-fix. The build owner runs real-worker filters after test discovery and records nonzero executed counts. Stop with the first causal loss, failing test and exact file/function repair scope. If unreproduced, retain the issue as unresolved and propose a bounded next diagnostic; no speculative fix or closure.

## Current dependency contract

Prerequisites: rn-acceptance-matrix. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
