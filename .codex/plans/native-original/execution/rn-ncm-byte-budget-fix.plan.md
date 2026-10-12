---
name: rn-ncm-byte-budget-fix
overview: "Repair the deterministic NCM recall byte-budget scan and retain its lower-ranked-candidate regression."
todos:
  - id: reproduce-budget-regression
    content: "Confirm the frozen oversized-ranked-candidate case fails against the pre-fix core behavior and record the exact UTF-8 byte budget oracle."
    status: pending
  - id: repair-budget-scan
    content: "Make the smallest core recall correction so an oversized ranked row cannot hide later rows that fit, while preserving truncation and text-integrity semantics."
    status: pending
  - id: prove-budget-recovery
    content: "Run the focused core regression and hand its accepted result to the NCM integration tests, build and 100-trial verification gates."
    status: pending
isProject: false
---

# Repair the deterministic NCM recall byte-budget scan

## Execution Notes

Work only in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2` on `feat/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md` and `../execution-results/ncm-recall-reproduction.md` before editing. Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b` checkout only as a reference. Stay in Codex; the single designated Cargo owner submits builds and tests through cargo-hauler.

This is the exact replacement for the byte-budget part of the historical broad `rn-ncm-recall-fix` plan. The deterministic regression is separate from the unresolved real-worker loss. Preserve both findings and do not claim that this lane explains the intermittent 2-of-4 result.

## Ownership and Constraints

Own only `crates/tracedecay-memory-ncm-core/src/recall/mod.rs`, its focused core regression in `crates/tracedecay-memory-ncm-core/tests/record_recall.rs`, and `execution-results/ncm-byte-budget-fix.md`. The existing reproduction files under `crates/tracedecay-memory-ncm-runtime/tests/recall_reproduction.rs`, `crates/tracedecay-memory-provider-ncm/tests/ncm_recall_reproduction.rs` and `crates/tracedecay-cli/tests/product_memory_provider_ncm_recall_reproduction.rs` belong to `rn-ncm-reproduce`; the broader provider tests belong to `rn-ncm-tests`.

Do not edit NCM provider adapters, runtime worker/recovery code, host recall, namespaces, models, thresholds, ranking, state format or any Native implementation. Do not broaden the scan beyond the bounded ranked set, do not truncate individual text, do not weaken UTF-8 accounting and do not replace the failing-before evidence with a newly favorable workload.

## Acceptance Checklist

- The regression demonstrates that a ranked candidate exceeding the remaining text-byte budget is skipped and a later fitting candidate is still considered.
- The returned `truncated` signal, candidate order, exact key/value bytes, top-k bound and empty-result behavior remain truthful.
- The focused result is accepted by root before `rn-ncm-tests`, `rn-build` and `rn-verify-ncm` are released; the full 100-trial campaign remains a downstream verification obligation.

## Operator Guidance

Use the source and test history to establish the pre-fix failure without rewriting the shared checkout. Run no Cargo command in this lane unless the designated build owner explicitly schedules it. Return the exact diff, failing-before/passing-after evidence, affected test filter and any state/model compatibility observation. Stop when the core seam is repaired and the focused regression is reviewable; a real-worker miss belongs to `rn-ncm-stage-trace` and `rn-ncm-real-fix`.

## Current dependency contract

Prerequisite: `rn-acceptance-matrix`. The accepted byte-budget evidence in `../execution-results/ncm-recall-reproduction.md` is an input, while the still-pending real-worker trace remains a separate lane. The exact active edges are in `../execution-selection.json`; this node supersedes the byte-budget portion of `rn-ncm-recall-fix` without deleting that historical plan file.
