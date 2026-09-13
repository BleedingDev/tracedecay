---
name: rn-provider-semantics
overview: "Make NCM provider-side temporal, scope, exclusion and budget semantics explicit before the candidate build."
todos:
  - id: audit-provider-semantics
    content: "Compare provider admission and reconstruction against the accepted contract for all seven scope fields, temporal validity, provenance, exclusions and UTF-8 budgets."
    status: pending
  - id: repair-provider-boundary
    content: "Apply only demonstrated provider-side corrections in the common adapter and source-binding projection, preserving opaque identity and canonical host ownership."
    status: pending
  - id: publish-provider-gate
    content: "Record focused downstream filters and contract evidence for NCM tests and the candidate build; do not claim real-worker reliability here."
    status: pending
isProject: false
---

# Make NCM provider semantics explicit

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md`, `../execution-results/contract-outputs.md`, `../execution-results/ncm-recall-reproduction.md` and `../evidence/ncm-boundary.md`. Use the 570 checkout read-only and remain in Codex. The single Cargo owner handles tests/builds through cargo-hauler.

This lane is the provider semantic gate required before building a repaired candidate. It is separate from the real-worker stage trace and from the core byte-budget correction; it must not turn provider-local projections into canonical Native or LCM authority.

## Ownership and Constraints

Own only `crates/tracedecay-memory-provider-ncm/src/common.rs`, `crates/tracedecay-memory-provider-ncm/src/common/source_binding.rs`, and `execution-results/ncm-provider-semantics.md`. The focused provider test tree belongs to `rn-ncm-tests`; do not edit it. Do not edit NCM core recall, runtime worker/recovery/client code, host context, Native/session/LCM implementations, contracts/generators, manifests, models, namespaces or operator state.

Preserve the seven-field exact scope digest, opaque source/candidate/trace identities, temporal validity and unknown policy, exclusion classes, provenance and source revision, UTF-8 candidate clipping, total budgets, idempotency, cancellation and truthful unavailable/partial outcomes. Do not tune model quality, thresholds or top-k to make a case pass.

## Acceptance Checklist

- Every changed semantic is tied to a contract row and a focused regression owner.
- Provider output remains advisory and namespace-bound; it cannot become a canonical fact/session/LCM source or bypass host privacy.
- The accepted result gates `rn-ncm-tests` and `rn-build`, with real-worker reliability still owned by `rn-verify-ncm`.

## Operator Guidance

Return exact source diff or a reviewed no-change result, contract references, focused filters and compatibility impact. If a required semantic belongs to the shared API or runtime worker, return it to root for the correct owner rather than widening this adapter lane.

## Current dependency contract

Prerequisites: `rn-acceptance-matrix`, `rn-contract`. The exact active edges are in `../execution-selection.json`; this provider gate replaces no historical plan file and is intentionally explicit before build.
