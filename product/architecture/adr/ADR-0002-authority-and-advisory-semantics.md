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

TraceDecay Native is itself a provider under this rule, and it now keeps provider-local cognitive state of its own. `tdmem-0401` left two branches open for a delivered observation Native does not promote: refuse it, or stage it as a recall candidate. The staged branch is the realized one. The `native_staged_observations` domain records it: the Native application port stages an admitted `session.message_committed.v1` observation as one durable row in a provider-local SQLite store under the host-granted Native provider-state directory, and recall returns those rows as advisory candidates merged with canonical fact candidates under one candidate budget.

That domain is derivative advisory state, not a second session record. Its sole canonical writer is `ProjectNativeMemoryApplicationPort` (`crates/tracedecay/src/daemon/retained_owner/native_provider.rs`). It never replaces TraceDecay's canonical admitted session/transcript evidence, which remains the authority for what a session said; a staged row is a provider-attested copy that can be deleted or rebuilt without touching that evidence. It never writes `memory_v2` or any canonical fact table, and it never becomes an accepted Native fact except through the same explicit promotion path every other provider-derived item must take. The row commits durably before the observation is acknowledged, so an acknowledged observation always has a staged row behind it.

Redelivery of a staged observation answers from the committing row, within what the committed-effect contract admits. The row stores its provider reference, receipt, and effect digest, so nothing about a redelivery is freshly minted. The wire, however, is narrower than the row: `CommittedEffectEvidence::duplicate` (`crates/tracedecay-memory-provider-api/src/lib.rs`, `validate_duplicate_effect`) refuses `committed_item_refs` and `verification_sha256` on a duplicate, because a duplicate commits nothing and may not describe a committed partition. A redelivery therefore reproduces the receipt and the committing operation identity byte-for-byte, and carries the request's own idempotency key; the provider reference and effect digest stay on the durable row where the staged store can still answer them. This decision keeps that cross-provider rule rather than widening it for one provider's convenience: a claim of "byte-identical committed-effect evidence" on a duplicate would be false under the contract as it stands.

Staged candidates are attested under the `exact_coding_scope` binding, because a staged row is recorded under the whole admitted exact scope and must not be widened to a weaker one. Exact-scope admission compares `agent_session_id` and `resolved_scope_digest` byte-for-byte, so in this slice staged recall is same-session only: a row staged in one session is not recalled in the next, even in the identical repository, worktree, and branch. That is a deliberate, stated limit of this decision rather than a defect of the binding; the durable cross-session binding for staged observations is tracked as `tdmem-b8q`, and the exact-scope fields are persisted as explicit columns so that later work needs a new index rather than a data migration.

## Consequences

- Current code always outranks memory.
- Native facts remain durable and explainable across provider changes.
- Native's staged observations are durable but advisory: they answer recall, they never answer "what did this session actually say", and deleting the staged store cannot delete accepted Native authority or admitted session evidence.
- Staged recall in this slice is bounded to the session that produced the row; cross-session recall is a further decision, not an implementation detail.
- A staged candidate's provenance is provider-attested and never host-confirmed. Its reference is provider-local by construction, so the host recognises it — through an explicit provider-local attestation lane — instead of resolving it, and the candidate keeps the provider-attested trust tier and the host-authored boundary label. Shaping a staged row like a host evidence reference to win confirmation is forbidden.
- Admission relates a candidate's memory class to its scope binding: a `session_observation` candidate is admissible only under `exact_coding_scope`, even from a provider also authorized for `project_facts` and `profile_facts`, because those bindings make checkout identity optional and forbid session identity.
- Staged rows share one candidate budget with canonical facts and cannot starve them: one slot of a non-zero ceiling is reserved for the highest-ranked eligible fact.
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
8. Staged observations are provider-local advisory state with one named writer; they are committed durably before acknowledgement, attested under the scope they were recorded in, and never co-write canonical facts or canonical session evidence.

## Verification

Executable beads:

- `tdmem-0401` — Native observation mapping without arbitrary fact conversion; its "staged as candidates" branch is the realized one.
- `tdmem-7si` — Native staging of admitted session observations and their same-session advisory recall.
- `tdmem-b8q` — durable cross-session binding for staged observations.
- `tdmem-0402` — Native recall/feedback/maintenance/inspection mapping.
- `tdmem-0603` — exact scope, identity, temporal validity, and revocation admission.
- `tdmem-0807` — audited promotion into Native facts/rules.

The authority-matrix checker mechanically rejects writer duplication, provider authority escalation, scope weakening, and context-owner drift.

## Review triggers

Review if TraceDecay introduces a new durable state domain, if a provider needs to claim canonical ownership, if a promotion path cannot preserve Native receipts, if staged observations acquire a scope binding wider than the scope they were recorded under, or if a product requirement proposes automatic canonical mutation from recall. Such a change requires a new authority-matrix revision and ADR.
