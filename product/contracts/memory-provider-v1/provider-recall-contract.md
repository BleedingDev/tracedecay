# Memory Provider Recall Contract V1

Provider recall is a bounded advisory read. It never mutates provider state, TraceDecay state, accepted Native facts, tools, approvals, or final coding context.

## Request

`MemoryProviderRecallRequestV1` is pinned to one accepted provider registration and compatible readiness receipt. It carries:

- exact profile, project, repository, worktree, branch, agent-session, and scope-revision identity;
- immutable request identity;
- a bounded objective and query;
- explicit temporal semantics;
- item/content/reference/warning/extension budgets;
- canonical exclusions;
- required capabilities and policy revision;
- bounded extensions;
- deadline and live cancellation.

Empty queries, wildcard scope, path or CWD inference, repository-only matching, provider scope widening, and deadline extension fail closed.

## Exact scope

All identity fields match exactly. Cross-worktree, cross-branch, cross-session, or repository-only recall is forbidden in V1. A candidate from another scope is `scope_mismatch`, not a weak match.

The provider may internally maintain broader abstractions, but every returned candidate is labelled with the exact admitted scope from which it was derived. TraceDecay revalidates scope before any candidate is considered for context.

## Scope binding

Every candidate's `exact_scope_identity` carries an explicit `scope_binding` that names which identity namespace the provider attests: `exact_coding_scope`, `project_facts`, or `profile_facts`. The values mirror the namespace variants of the accepted coding-memory authority matrix. A missing or unknown binding is a contract violation.

The host, not the provider, decides which bindings a provider may attest: the registry records `recall_scope_bindings` at registration and passes them to admission with the admitted call. A candidate whose binding the registry did not authorize for its provider is denied `scope_binding_unauthorized`. Bindings are never read from a provider reply and cannot widen the admitted scope.

Per binding, every identity field is required, optional, or forbidden:

- `exact_coding_scope`: all seven fields required and byte-equal to the admitted scope; a differing `resolved_scope_digest` alone is `stale_identity`.
- `project_facts`: `profile_id` and `project_id` required and equal; `repository_identity`, `worktree_identity`, and `branch_identity` optional (empty or equal); `agent_session_id` and `resolved_scope_digest` forbidden.
- `profile_facts`: `profile_id` required and equal; every other field forbidden.

An empty required field is `unknown_identity`, a differing required or optional field is `scope_mismatch`, a non-empty forbidden field is `forbidden_identity`, and a malformed field (surrounding whitespace or control characters) is `unknown_identity`. Missing identity fails closed.

## Temporal semantics

Four modes are explicit:

- `current`: valid at the request evaluation time;
- `as_of`: valid at the provided historical instant;
- `interval`: validity overlaps a closed-start/open-end interval;
- `history`: historical candidates are allowed but retain validity, supersession, and revocation metadata.

Evaluation time, `as_of`, interval bounds, inclusion of superseded/revoked candidates, and unknown-validity policy are part of the request. Missing or invalid temporal fields are not inferred. Future evaluation times and invalid intervals are rejected.

## Budgets and exclusions

All budgets are positive and finite. Effective values are clamped by the compatible handshake receipt. Providers cannot exceed candidate, content, reference, warning, extension, byte, deadline, or concurrency limits.

Exclusion sets cover stable memory references, prior candidate IDs, source/trace references, observation IDs, and content digests. Ignoring an exclusion is a contract violation.

## Candidate identity and content

`candidate_id` identifies one candidate within one response. It is not stable across requests and never implies provider-row identity.

Stable provider memory references are optional. `stable_memory_ref` is nullable because NCM-like providers may reconstruct or synthesize content from latent traces rather than return a stable row.

Every candidate contains exactly one of:

- bounded inline canonical content;
- a typed content reference that TraceDecay must hydrate and revalidate.

A canonical content SHA-256 is always required. Hydration failure excludes or explicitly degrades the candidate; it never silently substitutes different content.

## Provider-native versus host-normalized scores

A provider returns `MemoryProviderNativeScoreV1`: score-domain identity/version, raw canonical decimal value, direction, declared range, calibration state, semantics, and bounded components.

Provider-native scores are **not cross-provider comparable** and are not automatically comparable across score domains of the same provider. Raw values remain unchanged and visible.

Only the TraceDecay context compiler may create `MemoryProviderHostNormalizedScoreV1`. Normalization records policy ID/revision, input native-score digest, calibration evidence, value in `[0,1]`, and warnings. A provider cannot supply or overwrite the host-normalized score. Without a normalization policy, the native score may be retained for inspection but cannot determine cross-provider ordering.

## Validity and provenance

Validity includes observation time, valid-from/until, supersession, revocation, source revision, and explicit temporal state. `valid_until` is exclusive. Revoked, superseded, and unknown candidates are excluded by default.

Provenance has an explicit state:

- `available`;
- `redacted`;
- `unavailable`.

Missing provenance is never represented by an empty successful object. Policy chooses `exclude`, `degrade_allow`, or `audit_only`; defaults are exclude for unavailable and degrade-allow for redacted. Providers cannot fabricate or discard known provenance.

## Explanation

Provider explanations are bounded summaries with matched features, activation trace references, and limitations. They are evidence for debugging, not proof and never executable instruction authority.

## Extensions

Known extensions use versioned contracts. Unknown optional extensions round-trip as inert opaque data. Unknown required extensions fail explicitly. Extensions cannot widen scope, change authority, or activate undeclared behavior.

## Response and coverage

A response carries provider/instance/registration/readiness identity, exact scope, provider state generation, bounded candidates, coverage, ordering, terminal state, and warnings.

Coverage is:

- `complete`;
- `partial` with reasons and optional cursor;
- `zero_results`, which requires a successful complete search.

An empty candidate list is never a failure or fallback signal. It is meaningful only together with typed terminal and coverage state.

## Ordering

The provider returns deterministic order within one native score domain, breaking ties by candidate ID. TraceDecay may reorder only after scope/provenance/validity admission and host-owned normalization, and must preserve an explain trace.

Provider order has no cross-provider authority. Fixed request, state generation, policy, and limits must reproduce the same provider ordering and coverage.

## Final context ownership

The provider cannot inject context or select the final pack. TraceDecay alone validates, normalizes, deduplicates, budgets, formats, explains, and assembles candidates beneath current code, curated rules, accepted Native facts, and admitted session evidence.
