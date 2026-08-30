# ADR-0005: Use separate provider state plus a bounded durable observation outbox

Status: Accepted  
Date: 2026-08-30

## Context

Coding-agent observations arrive through host/session and operation authorities that already have their own settlement rules. Provider delivery may fail, time out, restart, or partially commit. Observer providers must not affect canonical behavior, but silent loss would make evaluation and later learning untrustworthy.

Provider-internal state also has different schemas and recovery rules from Native facts. Sharing one database or transaction boundary would create false atomicity and multiple authorities.

## Decision

TraceDecay product code owns one durable, bounded observation journal/outbox and dispatch receipt model. Each selected provider instance owns only its provider-local cognitive state.

Canonical host/session or operation settlement occurs first. A normalized observation is then appended with exact scope, provider target, stable operation/idempotency key, occurrence/source sequence, provenance, sensitivity, retention, deadline class, and payload digest.

Delivery is at-least-once. The provider contract makes duplicate delivery idempotent. Every attempt records a typed terminal outcome and any committed-effect receipt. Retry count, concurrency, queue bytes/items, age, and backoff are bounded. Capacity exhaustion, incompatible state, poison payloads, and permanent rejection are visible and inspectable; nothing is silently dropped.

Replay and crash recovery resume from durable journal state. Snapshot/restore and forget-by-source are capability-gated and preserve verifiable postconditions. TraceDecay's outbox never becomes a second Native fact store and provider acknowledgements never imply Native acceptance.

## Consequences

- Observer non-interference and reliable delivery can coexist.
- Provider restart does not lose already-admitted observations.
- Delivery may be eventually consistent with provider state.
- Stable idempotency and receipt schemas become mandatory contract features.
- Storage, privacy, retention, and operational inspection costs are explicit.
- Cross-store atomic transactions are intentionally avoided; partial effects are represented honestly.

## Rejected alternatives

- **Unbounded in-memory queue.** Rejected because restart loses work, overload is hidden, and memory growth is uncontrolled.
- **Synchronous provider call inside canonical host ingest.** Rejected because observer failure or latency would alter TraceDecay behavior.
- **Provider state in Native fact tables.** Rejected because provider schemas and cognition are not accepted explicit facts.
- **One distributed transaction across TraceDecay and provider stores.** Rejected because external provider topology and recovery cannot guarantee a shared commit authority.
- **Best-effort fire-and-forget delivery.** Rejected because silent loss cannot support evaluation, replay, correction, or privacy postconditions.
- **Retry without idempotency.** Rejected because a crash boundary could duplicate provider effects.

## Invariants

1. Canonical TraceDecay settlement precedes provider observation dispatch.
2. Observer dispatch failure cannot change prompts, facts, source edits, sessions, tools, approvals, or settled outcomes.
3. The outbox is durable, bounded, exact-scope, privacy-aware, and inspectable.
4. Provider delivery is idempotent under retries and crash recovery.
5. Every attempt ends in a typed terminal or explicit partial/unknown-effect receipt.
6. Provider-local state and Native fact state have distinct owners and schemas.
7. No queue item is silently dropped or reported delivered without acknowledgement.
8. Forget/delete operations expose a verifiable postcondition or typed unsupported result.

## Verification

Executable beads:

- `tdmem-0203` — observation envelope and idempotency contract.
- `tdmem-0205` — feedback, maintenance, correction, forgetting, snapshot, and restore contracts.
- `tdmem-0206` — typed terminal, degradation, cancellation, retry, and partial-effect outcomes.
- `tdmem-0502` — durable observation journal/outbox.
- `tdmem-0503` — idempotent dispatch receipts and replay.
- `tdmem-0506` — crash-recovery and backpressure journey.

Conformance must inject failures before commit, after provider commit but before acknowledgement, during restart, at queue capacity, and during cancellation.

## Review triggers

Review if a provider cannot guarantee idempotent observation handling, if required throughput exceeds bounded local storage, if a topology supports stronger atomicity without coupling authorities, if privacy deletion cannot produce a postcondition, or if a new observation source has different canonical settlement semantics.
