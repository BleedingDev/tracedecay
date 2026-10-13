---
name: rn-ncm-stage-trace
overview: "Localize the intermittent real-worker NCM recall loss with stage receipts before assigning a repair."
todos:
  - id: capture-stage-receipts
    content: "Run the pinned original 2-of-4 journey and its cold, warm, restart and concurrent variants with correlation IDs at every admitted stage."
    status: pending
  - id: localize-first-loss
    content: "Compare host admission, journal sequence, ACK/watermark, worker readiness, common recall, adapter reconstruction, filtering and final delivery to identify the first missing required item."
    status: pending
  - id: publish-repair-handoff
    content: "Publish an evidence-backed repair seam or a narrowly bounded instrumentation blocker; do not infer causality from a passing rerun."
    status: pending
isProject: false
---

# Localize the intermittent real-worker NCM recall loss

## Execution Notes

Work only in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2` on `feat/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md` and `../execution-results/ncm-recall-reproduction.md`. The active original is the clean `57006f60cb45bcee8487e73a40d4fad1a12ee2b` checkout. Use Codex-native tools and the one cargo-hauler owner; no alternate agent CLI or development worktree.

This is the stage-evidence replacement for the diagnostic part of `rn-ncm-recall-fix`. It is deliberately before `rn-ncm-real-fix`. The deterministic core budget lane is independent and must not be used as an explanation for the real-worker 2-of-4 versus 4-of-4 loss.

## Ownership and Constraints

This is read-only diagnosis. Own only `execution-results/ncm-stage-trace.md` and synthetic, privacy-safe receipts below `target/test-profile/readiness/ncm/stage-trace/`. Existing reproduction source files, NCM implementation files, host context, provider contracts, model files and operator data are read-only. If an instrumentation hook is genuinely missing, return the exact symbol and smallest owner handoff; do not add speculative logging in this lane.

Every run must carry a synthetic correlation ID through host admission, canonical journal sequence, worker readiness, observation ACK and durable watermark, exact-scope namespace, replay/projection freshness, model identity, worker `common_recall`, adapter reconstruction, candidate selection, filtering/exclusion, byte/token budgets and final host delivery. Do not persist raw memory text, source IDs or credentials. A durable ACK is evidence only when its receipt and watermark are present; arbitrary sleeps and a single successful rerun are not proof.

## Acceptance Checklist

- The original required four-item workload and its scope/top-k/budget oracle are identified for every attempt.
- Cold, warm, restart and concurrent/load outcomes retain both failures and passes with nonzero executed counts.
- The first stage at which an eligible required item disappears is named, or the exact missing receipt/instrumentation seam is returned as an unresolved blocker.
- `rn-ncm-real-fix` is not released until this report identifies its bounded source scope.

## Operator Guidance

Attach to the designated Cargo ticket through cargo-hauler and preserve isolated target/data roots. Compare immediate post-ACK and settled recall without turning a delay into a readiness proof. Keep the core byte-budget regression, the transient proof-cache hypothesis and any namespace-seed variability as separate hypotheses until a stage receipt discriminates them. Stop at the first causal loss and return the next owner, source symbol, test filter and acceptance oracle.

## Current dependency contract

Prerequisite: `rn-acceptance-matrix`. It consumes the recovered workload and partial receipts from `rn-ncm-reproduce`; its new stage evidence supersedes that plan's still-pending `trace-first-loss` todo without marking the old plan complete. The exact active edges are in `../execution-selection.json`; this node gates `rn-ncm-real-fix` and does not replace the historical broad plan file by deletion.
