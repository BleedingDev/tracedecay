# ADR-0001: Place the capability-based provider boundary above Native fact-store contracts

Status: Accepted  
Date: 2026-08-30

## Context

TraceDecay V2 already has a mature Native memory authority: owner-bound use cases, append-only facts and lineage, privacy admission, feedback/trust transitions, graph publication, maintenance, receipts, CLI/MCP/SDK/dashboard surfaces, and exact project/profile routing. A cognitive provider such as NCM has different internal state and may not expose stable fact rows or Native scoring semantics.

The product needs Native, NCM, and a future OCEAN slot behind one provider model without weakening TraceDecay's authorities or coupling provider internals to Native persistence.

## Decision

Create one versioned, capability-based provider contract **above** TraceDecay Native fact-store contracts.

The provider contract describes behavior: handshake, observation, advisory recall, feedback/outcome, maintenance, correction/deletion, snapshot/replay, health, inspection, limits, cancellation, and typed terminal outcomes. The contract does not expose a provider database schema or assume stable memory-row identities.

TraceDecay Native is adapted through existing application/use-case ports. External providers use isolated adapters. The capability registry selects implementations; all other code depends on capabilities rather than provider names.

OCEAN receives a reserved registry slot and capability identity only. No speculative OCEAN implementation is delivered before a versioned specification exists.

## Consequences

- Native facts, lineage, privacy, trust, and recovery remain unchanged and authoritative.
- Provider adapters can differ internally while sharing scope, provenance, limits, and terminal semantics.
- Public transports stay provider-neutral.
- Some Native-only operations are optional capabilities rather than mandatory provider behavior.
- Adapter and context-normalization code is required; a provider cannot simply be substituted for the Native store.

## Rejected alternatives

- **Direct `ProjectMemoryFactStore` implementation by a cognitive provider.** Rejected because that trait owns Native canonical invariants and would misrepresent advisory cognition as explicit durable facts.
- **Replace `DatabaseFactStore` with a generic provider store.** Rejected because it conflates TraceDecay persistence with provider-local representation and recovery.
- **Provider-name branching in CLI, MCP, SDK, dashboard, context compilation, or stores.** Rejected because selection would become scattered, unverifiable, and hostile to future providers.
- **Separate public APIs for Native, NCM, and OCEAN.** Rejected because transports would encode implementation identity instead of capability semantics.

## Invariants

1. TraceDecay Native remains the only canonical authority for accepted explicit facts.
2. Provider recall is advisory and cannot directly mutate code, sessions, Native facts, configuration, approvals, or tools.
3. Provider names appear only in registry/adapter construction and configuration values.
4. Every operation carries exact coding scope, provenance policy, deadline, cancellation, limits, and typed terminal outcome.
5. Unsupported capabilities fail explicitly; no fake readiness or silent fallback exists.
6. Provider state never shares or co-writes Native fact tables.

## Verification

Executable beads:

- `tdmem-0201` — provider capability registry.
- `tdmem-0209` — provider contract conformance suite.
- `tdmem-0303` — provider API/fabric crate boundary.
- `tdmem-0403` — direct-versus-provider Native parity goldens.

Architecture checks also run through `scripts/product/check-foundational-adrs.py` and the patch-footprint dependency guard.

## Review triggers

Review this decision if a provider cannot be represented without a new capability, if a required operation would need Native database semantics, if public transports need provider-specific schemas, or if a future provider specification proves the capability model insufficient. Any exception requires a new ADR and convergence-map entry before an upstream-owned edit.
