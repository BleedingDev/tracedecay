# ADR-0003: Isolate provider contracts, fabric, adapters, context, and conformance in product-owned crates

Status: Accepted  
Date: 2026-08-30

## Context

The derivative must stay in one Rust monorepo while remaining easy to remove, compare, and rebase across future TraceDecay V2 checkpoints. Embedding provider logic in the root crate or existing storage/host crates would increase upstream conflicts and make provider-specific assumptions leak across public surfaces.

The patch-footprint policy already declares additive product-owned zones and narrow upstream mounts.

## Decision

Create additive product-owned crates with one-way dependencies:

- `tracedecay-memory-provider-api`: versioned provider-neutral types and capability traits;
- `tracedecay-memory-provider-registry`: provider identity, capability negotiation, configured selection, and lifecycle handles;
- `tracedecay-memory-observation`: bounded journal/outbox, dispatch, receipts, replay, and inspection;
- `tracedecay-memory-context`: candidate normalization, admission, deduplication, budgeting, and explain trace;
- `tracedecay-memory-provider-native`: adapter over existing TraceDecay application/use-case ports;
- `tracedecay-memory-provider-ncm`: NCM-specific mapping and topology adapter;
- `tracedecay-memory-conformance`: dummy provider, golden fixtures, compatibility, failure, and journey harnesses.

The provider API points inward and cannot depend on the root binary, concrete adapters, Native DB/store internals, or code-index internals. Provider-neutral fabric/context crates depend on the API and TraceDecay application ports only. Concrete adapters never depend on each other. Only the narrow registry/composition layer constructs concrete adapters.

## Consequences

- Most product changes are additive and excluded from the upstream patch budget.
- Crate boundaries make forbidden dependency directions mechanically testable.
- Native and NCM can evolve independently behind common contracts.
- Some small composition and application-port edits remain necessary and must be convergence-mapped.
- Additional compile units and explicit mapping code are accepted costs for isolation.

## Rejected alternatives

- **Put provider logic in the root `tracedecay` crate.** Rejected because the composition root would become an implementation layer and every upstream sync would absorb product logic.
- **Provider-name branching in transports, stores, context compilation, or host code.** Rejected because it defeats capability polymorphism and scatters configuration authority.
- **Let adapters depend on one another.** Rejected because Native and NCM would become coupled and impossible to remove independently.
- **Let NCM import TraceDecay persistence or code-index internals.** Rejected because NCM must consume admitted provider contracts, not become a second TraceDecay runtime.
- **Create separate repositories.** Rejected for this program because atomic contract/adaptor/conformance changes and pinned upstream convergence are required in one monorepo.

## Invariants

1. Provider API contains no concrete provider identity or implementation dependency.
2. Provider-neutral crates do not import NCM, Native adapter, OCEAN, or root-composition types.
3. Concrete adapters do not depend on each other or on the root `tracedecay` package.
4. CLI, MCP, SDK, and dashboard remain adapter-blind.
5. Provider-name matching is confined to registry/adapter construction.
6. Product-owned crates expose no alternate canonical writer for TraceDecay state.
7. Every upstream-owned existing-file mount is within the patch budget and convergence map.

## Verification

Executable beads:

- `tdmem-0301` through `tdmem-0306` — additive provider/fabric/context/conformance crate skeletons and dependency guards.
- `tdmem-0307` — narrow root composition mount.
- `tdmem-0308` — product ownership and upstream convergence registry.

The patch-footprint checker scans all workspace manifests for forbidden dependency directions.

## Review triggers

Review when a new product crate is proposed, a provider-neutral crate needs a concrete adapter dependency, a public transport needs provider-specific types, or implementation requires a new upstream mount. Any widened dependency direction or touch zone requires an ADR and policy revision before code changes.
