---
name: rn-semantic-runtime-dynamic
overview: "Execute the pinned semantic lifecycle against a real candidate runtime and localize the unavailable-to-ready transition."
todos:
  - id: run-provisioned-semantic-lifecycle
    content: "Use the pinned Jina fixture to exercise fresh, acquisition, projection, calibration, activation, strict query and restart states through the real CLI/runtime."
    status: pending
  - id: exercise-failure-recovery
    content: "Run missing, corrupt, mismatched, offline, cancellation, source-update and same-checkout recovery controls with exact artifact/generation identities."
    status: pending
  - id: publish-dynamic-handoff
    content: "Identify the first missing transition and release a bounded acquisition/serving fix handoff before any semantic code fix or verification claim."
    status: pending
isProject: false
---

# Execute the dynamic semantic lifecycle

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md`, `../execution-results/semantic-diagnosis.md` and the accepted matrix. Use the exact JinaEmbeddingsV2BaseCode fixture revision `516f4baf13dec4ddddda8631e019b5737c8bc250` under the isolated diagnosis root, with offline flags only when the fixture is complete. Stay in Codex; the designated Cargo owner builds the candidate and records ticket identities.

This node converts the earlier static semantic diagnosis into runtime evidence. It is a hard prerequisite for both semantic repair nodes and for semantic verification. A correct lexical result, a generic `calibration_unavailable` string or a single successful command does not prove semantic serving.

## Ownership and Constraints

This is verification-only. Own `execution-results/semantic-runtime-dynamic.md` and synthetic, isolated results below `target/test-profile/readiness/semantic/dynamic/`. Do not edit application, code-index, semantic lifecycle, query service, CLI, manifests, user profiles or global model state. Do not fabricate calibration, bypass compatibility checks, relabel hybrid fallback as semantic or change the fixed paraphrase/negative oracle.

Record source revision, binary/features, model/tokenizer/artifact hashes, profile, generation, calibration identity, activation receipt, strict query mode, result provenance, latency and restart identity. Keep setup failure, acquisition failure, projection failure, calibration failure and serving failure distinct.

## Acceptance Checklist

- A provisioned compatible fixture reaches a demonstrated real semantic serving state or returns the first exact transition that prevents it.
- Missing/corrupt/mismatched/offline artifacts remain truthful and compatible acquisition/restart/source-update recovery is exercised.
- The report names disjoint acquisition and serving repair scopes and releases both fix nodes only after evidence is accepted.
- `rn-verify-semantic` remains blocked until this dynamic evidence and both repairs are accepted.

## Operator Guidance

Use an isolated target/data/profile root and preserve every attempt, including blocked and failed states. The strict oracle is the frozen paraphrase query from `validation-015`; retain its negative control and inspect serving evidence rather than only returned text. Stop at the first missing transition and route it to `rn-semantic-acquisition-fix` or `rn-semantic-serving-fix`.

## Current dependency contract

Prerequisite: `rn-acceptance-matrix`. It consumes the pinned fixture and static transition map from `rn-semantic-diagnose`; its runtime evidence supersedes that plan's still-pending dynamic reproduction todo without marking the old plan complete. The exact active edges are in `../execution-selection.json`; this node supersedes the dynamic portion of `rn-semantic-fix` without deleting that historical plan.
