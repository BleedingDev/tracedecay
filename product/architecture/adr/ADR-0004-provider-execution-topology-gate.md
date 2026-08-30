# ADR-0004: Keep provider contracts topology-neutral and defer the first NCM topology decision

Status: Accepted decision gate  
Date: 2026-08-30

## Context

A Native adapter can run in-process over existing TraceDecay ports. NCM may be reusable as a Rust crate, a local service, or another bounded topology; the licensed implementation has not yet been audited for API stability, thread/process safety, persistence ownership, cancellation, resource limits, or restart behavior.

Choosing transport now would encode unverified assumptions into otherwise provider-neutral contracts.

## Decision

The provider protocol and registry are topology-neutral. An adapter may execute in-process or through an isolated local process while exposing the same versioned handshake, capabilities, limits, request identity, deadline, cancellation, terminal outcomes, health, shutdown, and inspection model.

Native starts in-process because it already shares TraceDecay's retained application authorities.

The initial NCM topology is deliberately **deferred**. `tdmem-0701` audits the licensed surface and `tdmem-0702` records a follow-up ADR selecting the topology. That ADR must compare at least:

- in-process crate integration;
- isolated local-process integration;
- any other bounded topology actually supported by the audited implementation.

The comparison must cover license/provenance, API stability, state ownership, crash isolation, cancellation, latency, concurrency, memory/CPU limits, deployment, upgrade/rollback, snapshot/restore, and testability.

## Consequences

- Core contracts do not leak sockets, RPC clients, thread models, or process IDs.
- Native implementation can proceed without blocking on NCM transport.
- NCM adapter implementation waits for evidence rather than speculation.
- Registry and supervisor contracts must model lifecycle explicitly even for in-process providers.
- Some topology-specific mapping remains isolated inside the NCM adapter after the gate.

## Rejected alternatives

- **Choose in-process NCM immediately because the project is a Rust monorepo.** Rejected because source layout does not prove API, safety, or state-lifecycle compatibility.
- **Choose process isolation immediately for safety.** Rejected because the licensed surface may not support a stable bounded transport and the latency/packaging cost is unknown.
- **Mandate one execution topology for every provider.** Rejected because Native and external providers have different trust and lifecycle characteristics.
- **Expose topology-specific behavior in public provider contracts.** Rejected because callers should depend on capabilities and typed lifecycle semantics only.
- **Treat process existence as readiness.** Rejected because readiness requires successful handshake, compatible state, admitted scope, and real capability health.

## Invariants

1. Provider-neutral types contain no transport-specific fields except opaque implementation metadata for inspection.
2. Handshake precedes state mutation and proves protocol, build, state schema, limits, and capability compatibility.
3. Deadline and cancellation reach the actual provider operation in every topology.
4. Shutdown is bounded and reports partial/unknown effects honestly.
5. Provider state has one owner and explicit snapshot/recovery semantics.
6. NCM topology remains `deferred` until `tdmem-0701` and `tdmem-0702` complete.
7. No production NCM transport code lands before the decision-gate ADR.

## Verification

Executable beads:

- `tdmem-0202` — handshake, identity, limits, and version negotiation.
- `tdmem-0304` — provider lifecycle/runtime abstraction.
- `tdmem-0504` — supervision, health, and bounded shutdown.
- `tdmem-0701` — licensed NCM surface audit.
- `tdmem-0702` — evidence-backed NCM execution-topology ADR.

The foundational ADR checker rejects a decided NCM topology in this manifest before the decision-gate beads close.

## Review triggers

Review after the licensed NCM audit, when a second external provider is admitted, when one topology cannot satisfy cancellation or recovery contracts, or when deployment/security requirements change. A topology selection must supersede only the deferred portion of this ADR, not the provider-neutral lifecycle contract.
