# ADR-0007: Make observer mode mechanically non-influential and active mode explicitly gated

Status: Accepted  
Date: 2026-08-30

## Context

NCM must first run as an observer so its behavior can be evaluated without changing coding-agent output. A convention such as “do not use observer results” is insufficient: shared result types, callbacks, mutable state, or error propagation could accidentally affect prompts, canonical writes, tools, or external actions.

Active recall also introduces stale-memory, scope, failure, and security risks that require explicit admission gates.

## Decision

Observer mode uses a distinct capability route with no return value reachable by product decisions. It receives only post-admission/post-settlement observations through the durable outbox and may produce provider-local state, delivery receipts, inspection data, and evaluation traces.

Observer code cannot provide recall candidates to context compilation, cannot call canonical mutation ports, and cannot influence active-provider selection or terminal outcomes. Its resource limits, cancellation, failure, restart, and queue state are isolated and observable.

Active mode is opt-in through the transactional TraceDecay configuration authority. Activation requires:

- compatible handshake and declared capabilities/limits;
- provider conformance success;
- exact scope and lifecycle admission;
- crash, cancellation, backpressure, privacy, and scope-isolation gates;
- stale/harmful-memory and provider-failure evaluation thresholds;
- explicit rollback/disable behavior.

A configured active-provider failure returns its typed outcome. It never silently activates another provider or converts observer output into active recall.

## Consequences

- Observer evaluation cannot alter product hashes or canonical state.
- NCM can accumulate representative provider-local state before active use.
- Separate routing and policy types add implementation complexity.
- Active rollout is slower but reviewable and reversible.
- Operational surfaces must expose observer lag/failure without presenting it as product failure.
- Configuration and evaluation receipts become activation evidence.

## Rejected alternatives

- **Best-effort isolation by convention.** Rejected because future refactors could accidentally consume observer values or propagate errors.
- **Run observer recall and discard it near prompt rendering.** Rejected because earlier ranking, timing, caching, or failure behavior could still influence product output.
- **Implicit provider activation when configured or healthy.** Rejected because readiness does not equal accepted product risk.
- **Automatically fall back from an active provider to Native or another provider.** Rejected because authority and behavior would change invisibly.
- **Let observer failure fail canonical host ingest.** Rejected because it violates non-interference.

## Invariants

1. No observer-produced value is reachable from prompt/context, canonical mutation, tool, approval, or external-action decisions.
2. Observer failure, latency, restart, or capacity cannot change the canonical operation result.
3. Observer writes are restricted to provider-local state and product-owned observation receipts/telemetry.
4. Active mode requires explicit revisioned configuration and recorded gate evidence.
5. Provider selection is singular and explicit per scope/request policy; no silent fallback exists.
6. Disable/rollback stops new active recall without deleting state implicitly.
7. Observer and active identities, metrics, and receipts are distinguishable.
8. Unsupported or unavailable active capability is reported truthfully, never faked as empty success.

## Verification

Executable beads:

- `tdmem-0305` — observer/active routing types and policy.
- `tdmem-0404` — Native-only/default configuration and compatibility behavior.
- `tdmem-0505` — bounded observation dispatch and non-interference.
- `tdmem-0703` — NCM observer adapter admission.
- `tdmem-0706` — guarded NCM active-mode gate.
- `tdmem-0903` — mechanical observer-isolation proof through identical product outputs and denied canonical writes.

Tests compare product output/state hashes with observer disabled, healthy, slow, failing, restarting, and attempting unauthorized capabilities.

## Review triggers

Review if multiple observers are enabled, if observer output is proposed for online policy tuning, if active multi-provider blending is introduced, if fallback is requested, or if provider health becomes an activation signal. Any influence path requires a new ADR and safety/evaluation gates.
