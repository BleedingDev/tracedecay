---
name: rn-semantic-fix
overview: "Restore semantic readiness and recovery at the diagnosed boundary"
todos:
  - id: repair-semantic-transition
    content: "Repair the diagnosed acquisition, projection, calibration, activation or setup path with a focused failing regression."
    status: pending
  - id: preserve-semantic-failure-contract
    content: "Verify missing/corrupt/mismatched artifacts and cancellation remain truthful while valid provisioning reaches ready."
    status: pending
  - id: document-semantic-setup
    content: "Provide tested online and offline setup/recovery steps and actionable status for an isolated profile."
    status: pending
isProject: false
---

# Restore semantic readiness and recovery at the diagnosed boundary

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. Execute only after lead assignment and graph validation. Stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Apply rn-semantic-diagnose's causal finding. Candidate ownership is the exact reviewed subset of code_index_scheduler/{semantic_query_runtime,queries}.rs and existing tests.rs; application/src/semantic_runtime/{production,acceptance_calibration}.rs; semantic/src/model_lifecycle/** and artifact_store/**. A CLI error/status/setup fix may use tracedecay-cli/src/status_cmd.rs and the discovered existing model command handler, assigned by the lead. If setup alone is correct, preserve code and add the proven command path plus a meaningful lifecycle regression.

Require a real compatible model and generated/calibrated projection to become committed query authority. Preserve pinned identity/dimensions, calibration provenance, source-generation coherence, checkout isolation, cancellation and acquisition single-flight. Readiness must recover after artifacts are acquired or the daemon restarts without an unrelated user action. Distinguish strict semantic failure from explicit hybrid fallback; never relabel lexical output semantic.

## Constraints

Do not remove calibration guards, fabricate thresholds/vectors or permanently increase timeouts. No Native memory ranking/lifecycle changes, broad model upgrades, user DB mutations or general cleanup. Tests belong next to the owned modules; packaging/manifest needs go to the single build owner. Write execution-results/semantic-fix.md with actual causal before/after and provisioning receipts.

## Operator Guidance

Depends on rn-semantic-diagnose. Gate rn-build and rn-verify-semantic. Review before push on the original branch. Setup-only resolution still needs the provisioned strict-semantic acceptance suite; compilation and artifact installation alone are insufficient.

## Current dependency contract

Prerequisites: rn-semantic-diagnose. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
