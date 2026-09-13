---
name: rn-semantic-acquisition-fix
overview: "Repair the diagnosed semantic artifact acquisition, projection, calibration or activation transition."
todos:
  - id: repair-acquisition-transition
    content: "Implement the smallest correction at the acquisition-side transition identified by rn-semantic-runtime-dynamic."
    status: pending
  - id: prove-valid-and-invalid-artifacts
    content: "Verify compatible artifacts reach committed query authority while missing, corrupt, mismatched, offline and cancelled acquisition remains truthful."
    status: pending
  - id: publish-acquisition-handoff
    content: "Return source diff, lifecycle receipts, focused filters and build/semantic-verification inputs without changing the serving lane."
    status: pending
isProject: false
---

# Repair semantic acquisition and activation

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md`, `../execution-results/semantic-diagnosis.md` and the accepted `execution-results/semantic-runtime-dynamic.md`. Use the exact pinned Jina fixture and the clean 570 reference only as read-only inputs. Stay in Codex; Cargo is submitted by the single designated owner.

This is the acquisition-side replacement for `rn-semantic-fix`. It starts only after dynamic evidence identifies an acquisition/projection/calibration/activation boundary. It is independent of the serving-side repair, with disjoint source ownership.

## Ownership and Constraints

Own only the demonstrated subset of `crates/tracedecay-application/src/semantic_runtime/production.rs`, `crates/tracedecay-application/src/semantic_runtime/acceptance_calibration.rs`, `crates/tracedecay-semantic/src/model_lifecycle/owner.rs`, `crates/tracedecay-semantic/src/model_lifecycle/acquisition.rs`, `crates/tracedecay-semantic/src/model_lifecycle/reconciliation.rs`, `crates/tracedecay-semantic/src/model_lifecycle/persistence.rs`, `crates/tracedecay-semantic/src/artifact_store.rs`, and `execution-results/semantic-acquisition-fix.md`. Root must narrow the actual files from the dynamic handoff before editing.

Do not edit code-index scheduler files, query semantic serving, CLI manifests, model pins, calibration thresholds, Native memory/session/LCM routes, user databases or global installation. Preserve single-flight acquisition, artifact/hash/generation compatibility, cancellation, restart recovery, checkout isolation and explicit unavailable/failure outcomes. Never fabricate vectors or calibration.

## Acceptance Checklist

- The dynamic failing transition has a causal before/after regression with real compatible artifacts.
- Valid acquisition commits the exact model/artifact/generation/calibration identity and survives restart/source update as specified.
- Invalid/offline/cancelled inputs remain truthful and cannot activate an incompatible semantic authority.
- This repair gates `rn-build` and `rn-verify-semantic` through explicit active edges.

## Operator Guidance

Return source symbols, focused test filters, receipts and exact provisioning/recovery commands for the semantic verification owner. If dynamic evidence points to serving, stop without editing this scope and route the result to `rn-semantic-serving-fix`.

## Current dependency contract

Prerequisite: `rn-semantic-runtime-dynamic`. The exact active edges are in `../execution-selection.json`; this node supersedes the acquisition portion of `rn-semantic-fix` without deleting that historical plan.
