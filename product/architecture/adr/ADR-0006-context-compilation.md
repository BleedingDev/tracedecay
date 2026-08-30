# ADR-0006: Compile final coding context in TraceDecay from separately admitted authority lanes

Status: Accepted  
Date: 2026-08-30

## Context

A coding request can draw from current code, curated rules, accepted Native facts, session evidence, and one or more cognitive providers. Those lanes have different authority, score domains, freshness, provenance, and failure semantics. Passing raw provider output directly to an agent would allow stale or unscoped content to bypass TraceDecay policy and would make behavior nondeterministic and hard to explain.

## Decision

TraceDecay owns one request-scoped context compiler. Providers return advisory candidates only; they do not pack, order, label, or inject final context.

The compiler receives an immutable admitted scope containing profile/project/repository/worktree/branch/session identity, request identity, deadline, cancellation, policy/config revision, token/item budgets, and requested capabilities.

It processes separate lanes in authority order:

1. current code truth;
2. curated rules and disclosure policy;
3. accepted Native facts;
4. admitted session evidence;
5. provider recall candidates.

Before admission, every item is checked for exact scope, provenance or explicit provenance absence, temporal validity, revocation, sensitivity, policy, content bounds, and conflict with higher authority. Provider-native scores remain visible but are never compared directly across providers. A separately labelled deterministic normalized relevance may be calculated under pinned configuration.

The compiler deduplicates, applies diversity and token budgets, produces deterministic ordering for fixed inputs, and emits an explain trace covering selected, denied, truncated, conflicting, unavailable, and degraded candidates.

## Consequences

- Provider implementations stay independent from host prompt formats.
- Current code and policy remain dominant.
- Fixed inputs and configuration produce reproducible context packs.
- Context compilation must carry richer metadata and per-lane coverage.
- Provider-specific score/explanation fields are retained alongside normalized host policy values.
- Optional lane failure may yield a truthful partial pack, while code-truth admission failure fails closed.

## Rejected alternatives

- **Provider-built final context.** Rejected because providers would become policy and authority owners and could erase provenance or override code truth.
- **Directly concatenate provider output.** Rejected because scope, injection, duplication, staleness, and token limits would be unchecked.
- **Compare raw scores across providers.** Rejected because score domains and calibrations are provider-native and not commensurable.
- **Use first-success or silent provider fallback.** Rejected because provider selection and degradation would become hidden and nondeterministic.
- **Flatten Native facts, session evidence, and provider candidates into one list.** Rejected because it erases authority and lifecycle semantics.

## Invariants

1. TraceDecay is the sole owner of final context assembly.
2. Current code truth outranks every memory lane.
3. Every admitted item retains authority, source, scope, provenance, freshness, and selection reason.
4. Provider-native scores remain labelled and are not directly cross-compared.
5. Fixed inputs, configuration, provider responses, and budgets produce deterministic output.
6. Context packing is bounded by items, bytes/tokens, time, and concurrency.
7. Provider failure never fabricates empty success or silently switches provider.
8. Compilation is read-only with respect to all source authorities.
9. Untrusted memory is formatted as evidence, never executable instruction or tool authority.

## Verification

Executable beads:

- `tdmem-0204` — recall request, candidate, score, provenance, and warning contracts.
- `tdmem-0601` — transport-neutral cognitive recall application port.
- `tdmem-0602` — deterministic provider score normalization.
- `tdmem-0603` — exact scope, identity, temporal validity, and revocation checks.
- `tdmem-0604` through `tdmem-0608` — deduplication, policy, budgets, compiler bridge, and explain trace.
- `tdmem-0609` — real coding-agent context journey.

Conformance fixtures must prove deterministic ordering, malformed-score rejection, scope denial, stale/revoked exclusion, explicit partial coverage, and zero mutation.

## Review triggers

Review if a new authority lane is introduced, if active multi-provider blending is proposed, if score normalization changes, if provider content needs a new trust/sensitivity class, or if host prompt formats cannot preserve the evidence boundary. A changed lane order requires an authority-matrix revision and ADR.
