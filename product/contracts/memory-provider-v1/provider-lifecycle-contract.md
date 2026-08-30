# Memory Provider Lifecycle Contract V1

Lifecycle operations act only on provider-local state. They never mutate current code, exact TraceDecay scope identity, admitted sessions, accepted Native facts, curated rules, tools, approvals, or final context.

## Capability gating

`provider.health.v1` is mandatory. Feedback, maintenance, inspection, correction, deletion by source, snapshot export/restore, replay, explicit-fact projection, and explain trace are optional capabilities.

Each call is resolved through an accepted registration revision and compatible handshake. A provider name never implies support. Missing optional behavior returns the typed `capability_unsupported` result; it never silently routes to TraceDecay Native or another provider.

## Common mutation discipline

Every lifecycle request carries provider/registration/readiness identity, exact coding scope, UUIDv7 operation identity, deterministic idempotency key, expected provider-state generation, request and policy revision, deadline, live cancellation, and bounded extensions.

Mutating operations compare expected generation before applying state. Retries reuse the same idempotency key. Same key and same canonical request acknowledges the prior effect; same key with different content is `idempotency_conflict`. An effect-capable outcome records before/after state generation and a provider receipt.

Unknown optional extensions round-trip as inert data. Unknown required extensions fail explicitly. Extensions cannot widen scope, extend deadlines, activate undeclared capabilities, or change authority.

## Health

Health is a read-only mandatory capability. It reports provider/build/state/scope identity, current generation, readiness, per-capability state, effective limits, backlog, recovery state, and warnings.

A running process, open socket, existing path, or non-empty state never proves readiness. Health cannot repair or mutate state. Degraded, not-ready, and unavailable remain distinct.

## Feedback

Feedback targets exactly one of:

- a stable provider memory reference;
- a recall trace reference;
- a context-pack item reference.

This supports explicit-row providers and latent providers alike. The feedback signal is helpful, harmful, ignored, corrected, or superseded with a bounded canonical weight. A canonically settled outcome receipt and evidence references are mandatory.

Feedback may alter provider-local learning state, but it cannot change TraceDecay Native trust or any other canonical authority. Unknown target and unsettled outcome are explicit failures.

## Maintenance

Maintenance tasks are consolidate, decay, prune-expired, validate-state, repair, and compact. Every run is bounded by item count, bytes, request/handshake/deadline duration, one mutating run per provider scope, and live cancellation.

Unbounded scans are forbidden. Concurrent mutation returns `maintenance_busy`. Partial progress reports scanned/changed/removed counts, state generation, an optional resume cursor, and a receipt. Cancellation and timeout never become success.

## Inspection

Inspection provides bounded, redacted views of state summary, source influence, traces, delivery/maintenance receipts, snapshot metadata, and capability status.

It cannot expose raw credentials, unadmitted secret material, provider-internal secret state, or hidden canonical authority. Cursors are scope-bound. Inspection is read-only and may report partial coverage explicitly.

## Correction

Correction targets exactly one stable memory reference, recall trace, or source reference. It may supersede, restrict scope, change validity, replace provider-local content, or mark provider-local material incorrect.

The expected target revision is mandatory. Revision mismatch is `revision_conflict`. Correction is idempotent, provider-local, receipt-backed, and cannot edit source code or accepted Native facts.

## Deletion by source

Deletion/forgetting targets one or more exact `forget_source_key` values under the exact scope. Modes are remove influence, hard delete, and anonymize.

A successful operation has a verifiable postcondition containing matched, removed, anonymized, retained-under-lock, and remaining-influence counts; snapshots examined/rewritten; verification query digest; verification state; and before/after generation.

Success for remove-influence or hard-delete requires zero remaining provider influence except material retained under an explicit policy lock with its own receipt. Snapshot omission cannot be silent. Deleted source material cannot reappear in recall unless a new canonically admitted source revision is observed.

If the provider does not implement `deletion.by_source.v1`, the result is typed `capability_unsupported`; TraceDecay never pretends deletion succeeded.

## Snapshot export and restore

Snapshot identity binds snapshot ID, provider/build identity, state schema, exact scope, state generation, admitted observation sequence, parent snapshot, digest, byte length, and creation time.

Export is read-only and generation-consistent. Restore is idempotent and requires exact provider, compatible implementation/schema, exact scope, matching digest, and expected generation. Implicit reset and implicit overwrite are forbidden. Incompatibility is typed, not silently repaired.

## Replay

Replay consumes canonical observation receipts in monotonic source sequence. Duplicate sequence with the same digest acknowledges duplicate; different digest conflicts. Sequence gaps, stale state generation, partial progress, cancellation, deadline, and recovery requirement are explicit.

Replay never invents observations and never mutates TraceDecay authorities.

## Explicit projections and explain trace

Provider-local explicit-fact projections and explanations remain advisory. An explanation is not proof. Promotion into TraceDecay Native facts is a separate authorized operation with Native validation, lineage, idempotency, and receipts.

## Terminal behavior

Success, no effect, partial effect, unknown effect, invalid, unauthorized, unsupported, scope mismatch, stale identity, unknown target/source, conflicts, unverified settlement, busy/locked, timeout, cancellation, unavailable, incompatible/reset-required, extension failure, contract violation, and internal failure remain distinct.

`tdmem-0206` supplies the shared terminal envelope, retry, committed-effect, and fallback semantics used by these operations.
