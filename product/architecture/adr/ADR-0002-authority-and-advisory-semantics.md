# ADR-0002: Keep TraceDecay authorities canonical and provider memory advisory

Status: Accepted  
Date: 2026-08-30

## Context

Coding agents consume several kinds of state that have different authority: current source bytes, repository/worktree/branch identity, session evidence, accepted explicit facts, curated rules, and cognitive-provider output. Flattening them into one memory collection would permit stale or weak evidence to override current code and would create multiple writers for durable truth.

The authority matrix in `product/architecture/coding-memory-authority-matrix.json` already names the current TraceDecay owners. The provider design must preserve that separation.

## Decision

TraceDecay remains canonical for:

- current code in the exact admitted worktree;
- project, repository, worktree, branch, profile, and agent-session identity;
- admitted session/transcript evidence;
- accepted Native facts, lineage, provenance, feedback, and trust transitions;
- curated configuration/rules;
- final request-scoped context assembly.

Each selected provider instance owns only its provider-local cognitive state. Provider recall returns advisory candidates carrying provider identity, exact scope, provenance or explicit provenance absence, validity/freshness evidence, explanation, trace references, and typed warnings.

A provider-derived item can become a canonical Native fact only through a separate, explicitly authorized promotion command that re-runs Native validation, sanitization, idempotency, lineage, and durable receipt rules.

## Consequences

- Current code always outranks memory.
- Native facts remain durable and explainable across provider changes.
- Session evidence and provider state can be deleted or rebuilt without silently deleting accepted Native authority.
- Context consumers must retain authority labels and handle conflicts explicitly.
- Promotion and correction require dedicated audited workflows rather than implicit side effects.

## Rejected alternatives

- **Treat provider recall as canonical truth.** Rejected because provider content may be stale, probabilistic, internally derived, or weakly sourced.
- **Allow implicit promotion into Native facts.** Rejected because recall is not a write command and lacks explicit actor intent and Native validation receipts.
- **Let provider state replace TraceDecay session evidence.** Rejected because session evidence is a host-admitted record with distinct provenance and replay semantics.
- **Interpret provider failure or empty output as permission to switch authority.** Rejected because it creates silent fallback and hides unavailable or partial state.

## Invariants

1. Every durable domain has one named canonical writer.
2. Current source bytes in the exact admitted worktree are the highest-priority truth.
3. Accepted Native facts are never co-written by a cognitive provider.
4. Provider candidates remain labelled advisory evidence through context selection and outcome attribution.
5. Promotion is explicit, scope-bound, idempotent, validated, and receipt-backed.
6. Provider deletion cannot silently delete accepted Native facts; Native deletion cannot silently rewrite provider-local state.
7. Unavailable, unsupported, stale, partial, cancelled, timed-out, and successful-zero-result outcomes remain distinct.

## Verification

Executable beads:

- `tdmem-0401` — Native observation mapping without arbitrary fact conversion.
- `tdmem-0402` — Native recall/feedback/maintenance/inspection mapping.
- `tdmem-0603` — exact scope, identity, temporal validity, and revocation admission.
- `tdmem-0807` — audited promotion into Native facts/rules.

The authority-matrix checker mechanically rejects writer duplication, provider authority escalation, scope weakening, and context-owner drift.

## Review triggers

Review if TraceDecay introduces a new durable state domain, if a provider needs to claim canonical ownership, if a promotion path cannot preserve Native receipts, or if a product requirement proposes automatic canonical mutation from recall. Such a change requires a new authority-matrix revision and ADR.
