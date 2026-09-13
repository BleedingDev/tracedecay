# ADR-0010: Project Native behavior through the provider boundary with explicit semantic parity

Status: Accepted
Date: 2026-08-31

## Context

The Native adapter is a projection of the existing owner-bound Native memory
authority, not a second fact store or scoring implementation. Bead `tdmem-0402`
maps Native recall and lifecycle surfaces, `tdmem-0403` compares the direct and
provider routes, and `tdmem-0405` makes the projection inspectable through the
common status and explain surfaces. The parity fixture must therefore compare
meaningful Native behavior while keeping provider-envelope mechanics visible
only as envelope metadata.

Native authority is keyed by its existing project/profile owner and its
canonical fact projections. The provider contract carries a wider exact-scope
envelope: profile, project, repository, worktree, branch, agent session, and
scope revision. Those identities have different roles. The provider exact-scope
envelope is an admission and routing boundary; it does not replace Native
project/profile authority or make repository, worktree, branch, or session
fields new Native fact owners.

Native retrieval has an explicit telemetry write. Provider recall is a bounded,
read-only projection and must not increment Native retrieval telemetry. Within
the generic advisory provider contract, Native recall is projected only in its
current mode; generic `as_of`, interval, and history requests have no lossless
mapping. This restriction applies to that advisory provider projection. The
original typed Native historical, session, LCM, and mutation routes remain
available at their existing boundaries with their existing effect and failure
semantics. Generic provider feedback, contradiction, maintenance, correction,
and deletion similarly have no lossless mapping to those Native operations. In
particular, provider feedback must not mutate Native trust.

The fixed restoration reference for the complete Native implementation is
immutable upstream 570
(`57006f60cb45bcee8487e73a40d4fad1a12ee2b6`). The older b3 commit
(`b3b43410e47115056f2066449aafa1822bbb6049`) is the upstream side of the
historical product merge, and the August audit head 571
(`571daf3a9612e5247443e4da3a107b542686c1ef`) remains historical evidence.
Neither historical label replaces the active 570 baseline or the accepted
August sync floor recorded in product/upstream metadata. The audited product
head also contains shared host, provenance,
cursor, transcript, configuration, storage, privacy, maintenance, and
provider-composition extensions. Those extensions remain in place and are
tracked separately from the original Native implementation; they are not
evidence that Native has been replaced by a provider wrapper. Exact LCM
algorithm and schema parity is compared with 570. The exhaustive path and
hunk disposition is recorded in
[`native-original-source-inventory.md`](../native-original-source-inventory.md).

## Decision

Treat direct Native execution and the provider-routed Native execution as two
routes over one semantic authority. The same fixture, admitted owner, request
objective, current temporal query, limits, and canonical fact state are sent to
both routes. The provider adapter maps the admitted exact-scope envelope to the
owner-bound Native application port; a mismatch fails closed with a typed
scope result and never widens, guesses, or substitutes scope.

Parity goldens compare these semantic fields:

- fact IDs and canonical content;
- deterministic candidate order, including tie ordering;
- every fixed-point score component and the resulting fixed-point score;
- Native `why` explanation;
- source and other source-provenance references;
- current validity, including the current temporal state and exclusion of
  stale, revoked, or superseded facts;
- coverage and typed terminal classifications where they describe the
  semantic result; and
- typed no-effect failures, including capability-unsupported, scope,
  invalid-request, and equivalent fail-closed outcomes, with no committed
  effect and no Native state change.

For generic provider advisory recall, current recall is the only temporal mode
projected by the Native adapter. `as_of`, `interval`, and `history` requests
remain explicitly `capability_unsupported`; the adapter never relabels a
current projection as a historical answer. This does not narrow or replace the
original typed Native historical/session/LCM retrieval or fact/session/LCM
mutation routes, which remain available with their original behavior and
typed failures. Provider recall remains read-only and does not record Native
retrieval telemetry. A direct Native feedback or maintenance result is not
reclassified as provider success: generic provider feedback, contradiction,
maintenance, correction, and delete remain capability-unsupported because no
lossless mapping exists, and provider feedback may not mutate Native trust.

The parity surface covers the complete supported Native operation set at its
original typed boundary. It does not narrow Native to recall or use the
provider envelope as a substitute for Native fact, session, LCM, feedback,
maintenance, privacy, receipt, or recovery behavior. Direct typed routes remain
available and retain their original effect and failure semantics. A generic
`session_lookup` is one of those public typed operations. It is dispatched by
`DaemonSessionLookupPrimitiveV1` through
`SessionApplicationRetrievalPortV1::retrieve_admitted` and remains distinct
from retained `MessageSearch`. Retained `SessionsFor` and `Workflows` are
project-authority-only; profile authority returns typed unsupported. The
lower-level `recent_sessions`, `session_providers`, and `session_replay_slice`
queries use `RegisteredGlobalDb`/`SessionStoreAccess` and are absent from
`RetainedLcmRequestV1` and `DirectRetainedLcmPortV1`, so they do not become
public CLI/MCP operations. `lcm_compress` is daemon-internal mounted LCM
lifecycle work; `preflight` and `session_boundary` are retired or
daemon-internal helpers, not public CLI/MCP operations.

`RetainedSurfaceOperation::FactStoreCurate` is the explicit retained
automation route. It executes through `RetainedAutomationExecutionPortV1` and
its automation effect ledger/receipt before reviewed changes settle through
the Native fact authority; the lower-level curation method is not a substitute
for this route.

A generic control that does not name one of those typed operations returns a typed
`capability_unsupported` outcome with no effect; it is not reported as a
missing Native operation, silently approximated with current recall, or
converted into a fabricated success.

The staged Native substitute at
`provider-state/native/staged-observations-v1.sqlite3` is outside this parity
authority. Restoration retires its Native construction/read/write path while
leaving the existing file and SQLite sidecars untouched. No staged row,
provider-local receipt, generation, scorer, or replay cursor participates in
Native parity or recovery. This decision remains planned until the independent
source, parity, restart, scope, and host-extension checks pass. Documentation
of these routes is not implementation-complete evidence.

Hermes inline ingestion currently has no canonical observation bridge into
session/LCM admission. It remains an integration gap owned by
`rn-session-delivery` and `rn-host-fixtures`; no parity claim may treat its
payload as canonical Native evidence or silently staged data.

Automatic context delivery has one canonical `memory_matches` settlement. For
each eligible selected Native provider, the context compiler makes exactly one
eligible Native advisory invocation and delivers that result once; output
deduplication or merging must not trigger another eligible Native provider
invocation.

The golden comparator explicitly excludes transport/readiness IDs and timing
from semantic golden comparison. This includes provider instance,
registration, readiness, request/operation envelope identifiers, transport
incarnation values, wall-clock or monotonic timestamps, and measured latency.
It still validates those fields at the provider-contract boundary; exclusion
from the semantic projection is not permission to omit, forge, or mismatch
them in an operation envelope.

## Consequences

- Native remains the sole owner of fact identity, content, scoring, provenance,
  validity, trust, and retrieval telemetry.
- Provider parity can prove that the adapter preserves Native semantics without
  making provider envelope identity appear to be Native authority.
- Read-only provider recall cannot perturb ranking through retrieval counters,
  so repeated parity runs are stable with respect to telemetry.
- Unsupported temporal and lifecycle operations in the generic provider
  contract are observable, typed, and fail closed instead of being
  approximated or silently routed to another operation; original typed Native
  routes retain their own behavior.
- Goldens remain stable across provider instances and runs while contract
  validation still covers identity, readiness, limits, deadline, and
  cancellation fields.

## Rejected alternatives

- **Compare the complete provider envelope byte-for-byte with direct Native output.**
  Rejected because transport/readiness IDs and timing are envelope
  mechanics, not Native semantic behavior, and would make valid parity depend
  on an incarnation or clock.
- **Treat the provider exact-scope envelope as a Native project/profile owner.**
  Rejected because it would promote routing metadata into a second authority
  and weaken the existing owner-bound Native fact boundary.
- **Record Native retrieval telemetry during provider recall.** Rejected because
  provider recall is read-only; adding a retrieval write would change Native
  state and ranking behavior merely by selecting the adapter route.
- **Approximate historical/interval recall in the generic advisory provider
  contract, or approximate unsupported lifecycle operations with current
  search, feedback, maintenance, correction, contradiction, or delete.**
  Rejected because there is no lossless mapping and an approximation would
  misrepresent validity or effect semantics; the original typed Native routes
  remain available at their own boundaries.
- **Allow provider feedback to update Native trust.** Rejected because Native
  trust transitions require the owner-bound Native feedback authority and a
  settled Native operation, not an advisory provider signal.

## Invariants

1. The provider exact-scope envelope is validated in full and maps to the
   existing Native project/profile authority without creating another Native
   owner.
2. Direct and provider-routed current recall preserve fact IDs, canonical
   content, deterministic order, fixed-point score components, `why`, source
   provenance, and current validity.
3. Provider recall is read-only and never increments Native retrieval
   telemetry, trust, fact lineage, or any other Native write projection.
4. In the generic advisory provider contract, `as_of`, interval, and history
   requests are typed `capability_unsupported`; a current projection is never
   presented as a historical answer, while original typed Native
   historical/session/LCM retrieval remains available at its own boundary.
5. Generic feedback, contradiction, maintenance, correction, and delete are
   typed capability-unsupported when no lossless mapping exists; provider
   feedback cannot mutate Native trust.
6. Every parity failure that has no committed Native effect reports a typed
   no-effect terminal outcome and cannot silently fall back or mutate state.
7. Transport/readiness identifiers and timing are validated as envelope
   contract data but are excluded from semantic golden equality.
8. Fixed fixture, scope, Native state, request semantics, and limits produce
   deterministic semantic goldens.
9. Automatic context emits one canonical `memory_matches` settlement per
   eligible selected Native provider and never performs an extra eligible
   Native provider invocation for deduplication or output merging.

## Verification

Executable evidence:

- `tdmem-0402` — Native recall and lifecycle mappings retain Native scores,
  provenance, validity, and typed unsupported behavior.
- `tdmem-0403` — positive and negative direct-versus-provider golden parity,
  including project/profile scope, contradiction, stale validity, and
  feedback fixtures.
- `tdmem-0405` — capability and explain surfaces report only real Native
  support and link results to Native provenance and outcome history.

The parity suite must assert both positive semantic equality and negative
no-effect behavior. It must verify that provider recall leaves retrieval
telemetry unchanged, rejects scope mismatches, preserves current validity,
classifies unsupported temporal/lifecycle requests only in the generic
advisory provider contract, preserves the original typed Native
historical/session/LCM/mutation routes, enforces one canonical
`memory_matches` delivery with no extra eligible Native provider invocation,
and does not convert a provider envelope or feedback signal into Native trust
or canonical mutation.

## Review triggers

Review this decision if Native adds historical or interval projections, if a
provider capability gains a lossless mapping for feedback/contradiction/
maintenance/correction/delete, if Native changes fixed-point score components
or `why` semantics, or if the exact-scope envelope becomes an authority rather
than an admission boundary. Any newly compared envelope field or newly
supported capability requires updated fixtures and an ADR-backed semantic
decision.
