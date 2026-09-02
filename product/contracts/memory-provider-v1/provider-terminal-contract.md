# Memory Provider Terminal Contract V1

Every mandatory provider operation returns exactly one `MemoryProviderTerminalEnvelopeV1`. Missing, empty, malformed, or operation-specific bypass responses are contract violations.

## Closed terminal codes

The V1 terminal table is closed. It separates:

- success and successful zero results;
- partial read coverage;
- invalid, unauthorized, unsupported, identity, scope, conflict, and capacity failures;
- deadline and cancellation;
- provider availability, reset, and state compatibility;
- partial and unknown committed effects;
- contract and internal failures.

Operation-specific `domain_detail` is versioned opaque data. It cannot change terminal meaning, retryability, fallback eligibility, authority, or committed-effect interpretation.

## Mandatory operations

Health, observation acceptance, and recall are mandatory capabilities. Their typed operation result is nested inside the terminal envelope. A missing envelope or empty transport response is never equivalent to unavailable, zero results, or success.

The envelope retains operation/request/provider/instance/registration/readiness/exact-scope identity, timing, terminal code, result identity and digest, coverage, retry, fallback, committed effect, diagnostics, and warnings.

## Cancellation versus timeout

`cancelled` and `deadline_exceeded` are distinct terminal codes.

- already-cancelled before dispatch returns cancellation without provider contact;
- expired deadline before dispatch returns timeout without provider contact;
- if both are terminal, the earliest monotonic event wins;
- during provider execution, the first observed terminal event wins, while committed effect is independently reported.

Neither can become success. A mutating operation may have no, partial, or unknown effect when cancellation or deadline arrives; the envelope must say which.

## Committed-effect boundary

`MemoryProviderCommittedEffectV1` states are none, committed, duplicate, partial, and unknown.

Read-only operations require none. A successful mutation reports committed, duplicate, or none only for an explicit no-effect success.

Partial effects identify the exact committed boundary, before/after state generations, committed and uncommitted item sets, provider receipt, and reconciliation or resume action.

Unknown effects require reconciliation before any retry. `effect_unknown` carries a reconciliation action and forbids retry until that action resolves the committed boundary.

A retry of the same mutation retains the same idempotency key; it never invents a fresh key to conceal a partial or uncertain effect.

## Duplicate acknowledgement

Delivery is at least once, so a provider that already committed a mutation will see it again. `duplicate` is the truthful committed-effect state for that redelivery: the effect exists, this attempt did not create it, and the operation still succeeded.

Duplicate evidence is bound to the exact mutation, not asserted about the request in general:

- `duplicate_of_idempotency_key` is the deterministic idempotency key the provider matched, and it must equal the key on the request being answered;
- `duplicate_of_operation_id` names the earlier operation whose delivery actually committed;
- `provider_receipt_digest` anchors the prior committed effect, exactly as it does for `committed`;
- `state_generation_before` and `state_generation_after` are both known and equal, because a duplicate commits nothing new.

Both duplicate identity fields are absent for every other state. A duplicate is never inferred from an absent effect, an empty result payload, a repeated attempt number, a diagnostic string, or the provider's identity. A provider that cannot prove which mutation it deduplicated reports `committed`, `none`, or `effect_unknown` truthfully instead.

TraceDecay reads `success` plus `duplicate` as a duplicate acknowledgement and records it as such; it does not guess that from any other signal.

## Retry

Retry is an explicit bounded directive, not a guess from terminal class. The directive states:

- retry class;
- whether automatic retry is allowed;
- bounded backoff;
- attempts remaining;
- identity refresh, state reconciliation, operator action, and resume cursor requirements.

Automatic retry defaults to disabled. It requires pinned policy and positive budget. Attempts are never unbounded. Partial or unknown effects must be reconciled before retry.

## Fallback

Fallback eligibility is `forbidden` or `explicit_policy_only`, defaulting to forbidden. Empty results and provider unavailability never imply fallback.

The current product policy is **no automatic fallback**: the host rule defaults to `FallbackRule::Forbidden`, and a provider terminal that says `explicit_policy_only` is then returned as that provider's own failure with a typed `FallbackDeclinedReason::HostRuleForbidden`.

Fallback is honoured only by `MemoryFabric::route_active` under an `ActiveRoutingPolicy` whose `FallbackRule::ExplicitPinned` carries the identical `PinnedFallbackPolicy` (policy id, positive revision, and target provider) that the failing provider's terminal carries, and only when the target is itself registered active under its accepted revision with the routed capability and passes a fresh handshake. The target then receives a call bound to its own identity, ready receipt, and state generation; provider-specific state identity is never reused. Any other condition — host rule forbidden, missing or mismatched policy, target unregistered, observer-only, capability undeclared, or handshake not ready — is a typed declined reason alongside the original reply. The host pins the rule through the `memory.provider_recall_routing.v1` configuration gate, whose default names no active provider and no fallback. Empty results never raise fallback, and Native facts are never an implicit target.

## Success and coverage

`success_zero_results` is a complete successful search with `zero_results` coverage. An empty candidate list without typed terminal and coverage is a contract violation, not a fallback signal.

`partial` means degraded read/inspection coverage and carries reasons plus a scope/request/state-bound cursor. It is distinct from `partial_effect`, which means a mutation committed only part of its intended state change.

Failure envelopes cannot claim complete coverage.

## Result payload

Successful payloads are typed by a versioned contract ID, RFC 8785 canonical JSON, SHA-256 digest, and handshake response-byte limit. Failure payloads are absent in V1; diagnostics use typed detail, diagnostic ID, warnings, coverage, and effect receipts.

Unknown result contracts or inconsistent payload/digest/terminal relations fail as contract violations.

## Validation order

TraceDecay validates envelope shape and identities, terminal code, result payload, coverage, retry, fallback, committed effect, and request-control precedence in that order. Any inconsistency rejects the whole provider response as `contract_violation`; it is never silently repaired or reclassified.
