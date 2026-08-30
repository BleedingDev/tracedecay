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

`MemoryProviderCommittedEffectV1` states are none, committed, partial, and unknown.

Read-only operations require none. A successful mutation reports committed, or none only for an explicit no-effect success. `partial_effect` requires:

- exact committed boundary;
- state generation before and after;
- committed and uncommitted item sets;
- provider receipt;
- reconciliation/resume action.

`effect_unknown` requires an explicit reconciliation action and forbids retry until reconciliation. A retry of the same mutation retains the same idempotency key; it never invents a fresh key to conceal an uncertain effect.

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

The current product policy is **no automatic fallback**. A future permitted fallback would require a pinned policy/revision, explicit target provider, fresh handshake, exact scope admission, and no reuse of provider-specific state identity.

## Success and coverage

`success_zero_results` is a complete successful search with `zero_results` coverage. An empty candidate list without typed terminal and coverage is a contract violation, not a fallback signal.

`partial` means degraded read/inspection coverage and carries reasons plus a scope/request/state-bound cursor. It is distinct from `partial_effect`, which means a mutation committed only part of its intended state change.

Failure envelopes cannot claim complete coverage.

## Result payload

Successful payloads are typed by a versioned contract ID, RFC 8785 canonical JSON, SHA-256 digest, and handshake response-byte limit. Failure payloads are absent in V1; diagnostics use typed detail, diagnostic ID, warnings, coverage, and effect receipts.

Unknown result contracts or inconsistent payload/digest/terminal relations fail as contract violations.

## Validation order

TraceDecay validates envelope shape and identities, terminal code, result payload, coverage, retry, fallback, committed effect, and request-control precedence in that order. Any inconsistency rejects the whole provider response as `contract_violation`; it is never silently repaired or reclassified.
