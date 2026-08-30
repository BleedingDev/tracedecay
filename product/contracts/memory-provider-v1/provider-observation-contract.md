# Memory Provider Observation Contract V1

Provider observations are admitted **after** the canonical TraceDecay source event settles. They never participate in the source transaction and never change its result.

## Envelope

`MemoryProviderObservationEnvelopeV1` is immutable and contains:

- UUIDv7 observation ID plus deterministic idempotency key;
- exact provider ID, registration revision, and compatible readiness receipt;
- exact profile, project, repository, worktree, branch, and agent-session scope;
- canonical settled source identity and receipt;
- versioned observation kind and payload contract;
- RFC 8785 canonical JSON payload plus SHA-256;
- bounded typed extensions;
- provenance and transformation chain;
- privacy, retention, redaction, policy, expiry, and forget-source metadata;
- occurrence/admission timestamps and monotonic source sequence;
- request identity, deadline, and cancellation.

The envelope and payload are bounded. Empty payloads, unknown top-level fields/contracts, duplicate JSON keys, floating-point/non-finite values, missing provenance/privacy, or unadmitted secret/personal material fail closed.

## Coding scope is mandatory

Every coding observation inherits the compatible handshake scope. Profile, project, repository, worktree, branch, agent session, and scope revision are all explicit. No path, CWD, repository-name, or nearest-project inference is allowed.

A provider may keep a project-level abstraction internally, but delivery remains scoped to the exact admitted worktree and session. Cross-worktree or cross-session reuse requires a later explicit recall admission decision; it is never implied by observation storage.

## Canonical source settlement

A source identity includes source authority, event ID/revision/digest, and the canonical settlement receipt. Supported V1 source authorities cover host sessions, tool executions, source edits, tests, diagnostics, Git evidence, explicit Native promotion, feedback outcomes, and automation outcomes.

Path or CWD is never source identity. An event without a verified canonical settlement receipt is rejected before observation journal append.

## Deterministic normalization

Payloads and extension payloads use Unicode NFC and RFC 8785 canonical JSON. Object keys are lexicographically ordered, duplicate keys and floats are forbidden, and provider-specific top-level payload fields are not allowed. Payload, extension, and envelope digests exclude transport metadata, so the same admitted observation remains identical across process restarts and in-process/out-of-process topology.

## Extensions

An extension has a stable ID, positive version, criticality, canonical opaque payload, and payload digest.

- known extensions are validated against their versioned contract;
- unknown **optional** extensions are preserved byte-for-byte as opaque inert data and round-trip unchanged;
- unknown **required** extensions fail explicitly with `rejected_extension_unsupported`;
- unknown extensions never activate behavior, grant authority, or disappear silently.

Extension count and bytes are bounded. A provider may not drop a preserved optional extension from an acknowledgement or rewrite its payload.

## Observation versus provider memory

An observation is an admitted input event. It is **not** a memory record, and its observation ID is not a provider memory ID.

One observation may legitimately result in:

- no provider-local effect;
- one provider-local trace;
- multiple provider-local traces;
- later consolidation with effects from other observations.

Stable provider memory references are optional because NCM-like providers may use latent traces rather than explicit rows. The provider's delivery receipt reports an effect count, optional stable memory references, optional provider trace references, or an explicit no-effect reason. This summary is audit evidence only; it is not canonical provider state and never becomes a TraceDecay Native fact.

Promotion into accepted Native facts is a separate authorized operation with Native validation, lineage, idempotency, and receipts.

## Idempotency

The idempotency key is SHA-256 over contract identity, target provider/registration, exact scope, source authority/event ID/revision, observation kind, payload-contract identity, payload digest, and extension digest. It is stable across dispatch retries, dispatch restart, provider restart, and transport topology.

- same key + same canonical payload/extensions → `duplicate_acknowledged`;
- same key + different canonical payload/extensions → `idempotency_conflict`;
- a new canonical source revision → a new key.

Random retry keys and timestamp-only keys are forbidden. Providers persist deduplication state strongly enough to survive the crash window after provider commit but before acknowledgement.

## Provenance and privacy

Origin identity, actor, host, evidence anchors, and every transform step are required. Providers cannot rewrite origin or discard the transform chain.

Privacy metadata classifies content as public/internal/sensitive/restricted and ephemeral/session/project/profile retention. Raw secrets and unadmitted personal data are forbidden. Expiry and stable forget-source key are mandatory, and providers may not extend retention.

## Ordering and batching

Source sequence is monotonic within source authority + exact scope + source stream. Delivery order is not guaranteed; providers must tolerate duplicate and out-of-order delivery. Occurrence or admission wall-clock timestamps are evidence, not ordering authority.

A batch is homogeneous in provider, registration revision, readiness receipt, and exact scope. Item/byte limits come from the compatible handshake receipt. Duplicate keys inside one batch are non-canonical. Batch atomicity is not assumed: partial commits must report per-item effects rather than masquerade as whole-batch success.

## Admission and dispatch order

1. Verify canonical source settlement.
2. Resolve exact provider registration/readiness for the same scope.
3. Reject terminal deadline/cancellation before journal append.
4. Validate kind, payload, extensions, provenance, privacy, and bounds.
5. Canonicalize and derive digests/identities.
6. Append the immutable envelope to the durable bounded journal.
7. Dispatch at least once within effective limits.
8. Provider deduplicates and compares payload and extension digests.
9. Persist a typed delivery receipt for every attempt.
10. Never alter the settled source result because provider admission or delivery failed.

The durable journal, retries, backpressure, and replay implementation are gated by `tdmem-0502`, `tdmem-0503`, and `tdmem-0506`.

## Delivery receipts

Every attempt records observation/key/payload/extension identity, provider instance and registration, state generation before/after, attempt number, typed outcome, committed-effect state, provider-effect summary, provider receipt digest, timing, and warnings.

Success without provider acknowledgement is forbidden. Applied, duplicate, partial, and unknown effects remain distinct. A later retry uses the same idempotency key; it never assumes that a missing acknowledgement means no provider effect.

## Observer non-interference

Canonical source settlement always precedes observation. Provider latency/failure cannot delay or change it. In observer mode, provider output cannot enter context, trigger tools/actions, or mutate Native facts. Duplicate, conflict, partial, unknown, cancelled, or timed-out delivery remains operational evidence only.
