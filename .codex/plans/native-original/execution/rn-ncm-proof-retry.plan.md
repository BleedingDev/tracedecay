---
name: rn-ncm-proof-retry
overview: "Repair transient NCM instance-proof negative caching so a fail-once readiness proof can recover safely."
todos:
  - id: prove-transient-cache-loss
    content: "Exercise a fail-once instance-proof provider and demonstrate that cached None suppresses a later admissible delivery on the same daemon."
    status: pending
  - id: repair-bounded-retry
    content: "Replace transient negative caching with a bounded retry or truthful retryable outcome while preserving cancellation, shutdown and fail-closed behavior."
    status: pending
  - id: verify-proof-recovery
    content: "Run fail-once, permanent-failure, restart and cancellation regressions and hand the accepted result to NCM tests, build and reliability verification."
    status: pending
isProject: false
---

# Repair transient NCM instance-proof negative caching

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read `../READINESS-PLAN.md`, `../WORKER-RULES.md`, `../REVIEWED-DECISIONS.md` and `../execution-results/ncm-recall-reproduction.md`. Use the clean 570 checkout as a read-only reference. Stay in Codex; Cargo and real-worker runs belong to the single designated cargo-hauler owner.

The source file is a hot shared surface. `rn-session-delivery` owns `crates/tracedecay/src/daemon/retained_owner/observation_journey.rs` first for its accepted staged-ack retirement. This node receives exclusive ownership only after that node is reviewed and released; it must never run concurrently with that owner.

## Ownership and Constraints

Own, after the explicit source handoff, only `crates/tracedecay/src/daemon/retained_owner/observation_journey.rs` for the proof-cache correction and its focused in-file regression, plus `execution-results/ncm-proof-retry.md`. Do not edit provider NCM source, the core byte-budget seam, host explain-trace code, generic schemas, Native algorithms, models, state formats or any other session-delivery file. Do not add an unbounded retry loop or an arbitrary delay.

The regression must distinguish a transient provider proof error/absence from a permanent invalid proof, preserve fail-closed admission, retain cancellation and shutdown joins, and prove that a successful second attempt can deliver on the same daemon. A daemon restart remains a separate recovery case, not a substitute for retry semantics.

## Acceptance Checklist

- The pre-fix fail-once scenario shows the negative cache suppressing an otherwise admissible NCM observation or recall path.
- The fix retries only within the existing bounded operation/deadline contract and leaves permanent failure unavailable/withheld.
- Focused regressions are accepted before `rn-ncm-tests`, `rn-build` and the 100-trial `rn-verify-ncm` campaign; all trial outcomes remain visible.

## Operator Guidance

Use a deterministic fail-once proof test rather than timing luck. Return source symbols, lifecycle/shutdown reasoning, exact filters and ticket handoff. If the stage trace disproves this hypothesis, retain the negative result and stop without changing the cache.

## Current dependency contract

Prerequisites: `rn-acceptance-matrix` and the serialized `rn-session-delivery` source handoff. It consumes the transient-proof evidence recorded by `rn-ncm-reproduce` without waiting on that plan's separate real-worker trace todo. The exact active edges are in `../execution-selection.json`; this node supersedes the proof-cache portion of `rn-ncm-recall-fix` without deleting that historical plan.
