---
name: rn-ncm-real-fix
overview: "Repair the first demonstrated real-worker NCM recall loss without weakening ordering, scope or delivery contracts."
todos:
  - id: repair-localized-loss
    content: "Implement the smallest correction at the stage-trace owner identified by rn-ncm-stage-trace."
    status: pending
  - id: prove-real-recovery
    content: "Show the pinned intermittent workload failing before the correction and passing after it across worker restart, replay, cancellation and concurrent observation boundaries."
    status: pending
  - id: handoff-reliability-gate
    content: "Return the exact repair diff and focused filters so rn-ncm-tests, rn-build and the 100-trial NCM verification can consume the same artifact."
    status: pending
isProject: false
---

# Repair the localized real-worker NCM recall loss

## Execution Notes

Work only in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2` on `feat/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md`, `../execution-results/ncm-recall-reproduction.md` and the accepted `execution-results/ncm-stage-trace.md`. Use the clean 570 reference only for comparison. Stay in Codex and route all Cargo through the single cargo-hauler owner.

This node is the real-worker replacement for the broad `rn-ncm-recall-fix`. `rn-ncm-stage-trace` is a hard prerequisite: do not guess the seam from a pass, an elapsed-time change or the deterministic core budget result.

## Ownership and Constraints

After root accepts the stage trace, own only the demonstrated subset of these bounded candidate files: `crates/tracedecay-memory-ncm-runtime/src/engine/runtime/recovery.rs`, `crates/tracedecay-memory-ncm-runtime/src/engine/runtime/observe.rs`, `crates/tracedecay-memory-ncm-runtime/src/client/mod.rs`, `crates/tracedecay-memory-ncm-runtime/src/worker/**`, `crates/tracedecay-memory-provider-ncm/src/rust_backend/mod.rs`, `crates/tracedecay/src/daemon/project_composition/ncm_observer.rs`, and `execution-results/ncm-real-fix.md`. Root must narrow the actual file list to the stage-trace result before editing.

Do not edit `crates/tracedecay-memory-ncm-core/src/recall/mod.rs` or its core regression (owned by `rn-ncm-byte-budget-fix`), provider `common.rs` or `common/source_binding.rs` (owned by `rn-provider-semantics`), `observation_journey.rs` (handed to `rn-ncm-proof-retry` after `rn-session-delivery`), host explain-trace code, Native algorithms, models, namespaces, budgets, ranking or saved-state formats. A loss outside the bounded list returns to root for a new ownership decision.

Preserve exact seven-field scope, receipts/idempotency, durable ordering, cancellation semantics, worker/restart behavior, replay freshness, model/tokenizer identity and truthful not-ready/failure outcomes. Never hide a race with an unbounded retry or arbitrary sleep.

## Acceptance Checklist

- The stage trace identifies the first loss and the repair changes only that boundary.
- A failing-before and passing-after run uses the original workload with real worker/model and nonzero test execution.
- Focused NCM tests pass through `rn-ncm-tests`; the candidate build consumes the reviewed repair; `rn-verify-ncm` runs the required 25 cold, 25 warm, 25 restart and 25 load trials with every failure retained.

## Operator Guidance

Return the exact source symbols, patch, compatibility statement, focused test filters and cargo-hauler ticket handoff. If the trace does not prove a causal loss, stop incomplete and return the missing receipt instead of widening scope or declaring the intermittent issue fixed.

## Current dependency contract

Prerequisite: `rn-ncm-stage-trace`. The exact active edges are in `../execution-selection.json`; this node gates `rn-ncm-tests`, `rn-build` transitively and the 100-trial `rn-verify-ncm` campaign.
