# Native session ingestion, recall, LCM, and context behavior

## Answer

The complete original Native session path is the canonical host/session path plus
the original LCM engine. It is not the provider-local staged observation store or
its lexical/recency scorer. The locally available upstream-authored revision
`b3b43410e47115056f2066449aafa1822bbb6049` is the immutable behavior reference
for this report. It is the second parent of the current head
`571daf3a9612e5247443e4da3a107b542686c1ef`; the separate source-baseline report
still needs to decide whether b3 is the accepted floor.

The shared boundary is:

1. Host adapters admit source records and scope, session, provider, cursor, and
   privacy evidence.
2. The session stores atomically persist the canonical session row, searchable
   message projection, protected LCM raw message, parse cursor, and applicable
   Git/workflow evidence.
3. The session application service authorizes a temporal query, applies limits
   and freshness, executes the canonical temporal port, hydrates context and
   summary lineage, and returns typed coverage/omission/failure state.
4. The LCM engine owns verified raw messages, summary DAG nodes, external
   payloads, expansion, describe/search, compression, retention, and garbage
   collection.
5. A Native provider adapter selects and calls those owner-bound services. It may
   translate provider envelopes and expose capability metadata, but it must not
   replace them with a provider database, staged recall algorithm, empty-result
   fallback, or caller-authored summary.

The current Native additions take a different route. The staged store is
explicitly derivative provider-local state. `observe_staged_session` writes
admitted observation bytes into a provider namespace, and
`recall_project_memory` merges staged rows using a custom lexical/recency
score with canonical fact results. That can be a useful advisory observation
surface, but it does not implement original session retrieval or LCM and cannot
be called full Native.

## Evidence table

| Original revision/path and symbol | Current path and symbol | Original behavior | Difference or integration requirement |
|---|---|---|---|
| [b3 `session/mod.rs`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/mod.rs#L1-L36), module exports | [current `session/mod.rs`](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/session/mod.rs#L1-L36) | The session application crate exports temporal execution ports, refresh, retrieval, authorization/types, and the LCM compatibility surface. These are application contracts around an already-owned store and authority. | The current tree retains this original surface. Native integration should depend on it rather than define a second session authority. |
| [b3 `SessionTemporalQuery`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/retrieval.rs#L74-L246) and [`SessionRetrievalService::retrieve`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/retrieval.rs#L277-L345) | [current query/service](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/session/retrieval.rs#L74-L345) | A query binds session or authorized-root scope, provider, text, direct anchor, semantic filters, cursor, temporal mode, grain, result/diversity limits, context budget, execution limits, and freshness policy. Retrieval authenticates the request, runs the temporal execution port under cancellation/deadline control, validates the execution report, and maps the result to a typed outcome. | The staged Native recall path has no equivalent temporal query, authorized-root execution, context budget, freshness policy, or canonical temporal execution. Native must call this service, with the host-derived scope and the original query fields preserved. |
| [b3 `map_report` and execution error mapping](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/retrieval.rs#L429-L700) | [current `map_report`](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/session/retrieval.rs#L429-L700) | Coverage, summary omissions, context omissions, freshness, and cursors determine Complete, Partial, CompleteZero, Stale, Denied, Locked, Redacted, Deleted, ResetRequired, BudgetExhausted, Cancelled, TimedOut, WrongScope, or Unavailable. The service does not invent a score to hide omitted or unauthorized context. | A provider reply must preserve these distinctions and omission counts. Collapsing them into staged hits, empty results, or generic provider-unavailable loses behavior and makes context use unverifiable. |
| [b3 refresh target/service](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/refresh.rs#L69-L188) and [`begin_or_join`, `status`, `cancel`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/refresh.rs#L221-L458) | [current refresh modules](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/session/refresh.rs#L69-L458) | Refresh is durable, idempotent, and frontier-bound. It authenticates the target, records a begin-or-join operation, wakes the scheduler, reports progress/coverage, and persists cancellation. Wake or delivery failure returns reconciliation-required while retaining the durable operation; terminal receipts distinguish running, complete, failed, cancelled, denied, stale, wrong-scope, not-found, aborted, deadline, and unavailable states. | Refresh is shared temporal freshness infrastructure. Native must not invent a provider-local refresh queue or report success before the canonical operation is durable. If the public provider contract cannot carry a required refresh receipt/effect, the shared contract needs a separate owner. |
| [b3 task-session retrieval](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/retrieval/task_session.rs#L16-L37) and [admission/execution](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/retrieval/task_session.rs#L69-L289) | [current task-session module](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-memory/src/session/retrieval/task_session.rs#L16-L289) | Task-session lookup is a distinct binding and retrieval operation. It validates the task-session binding, retriever/policy revision, rank selector, cursor manifest, and temporal callback, and reports structural cursor refusals separately from ordinary retrieval outcomes. | Do not route task-session lookup through the staged project-memory scorer or ordinary current recall. Preserve its binding, policy revision, cursor manifest, and typed refusal semantics. |
| [b3 transcript adapter](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/transcript.rs#L17-L31) and [persist/ingest methods](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/transcript.rs#L87-L312) | [current transcript/store access](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/store_access/transcript.rs#L48-L871) | The transcript adapter operates on an already-open registered global database. The runtime writer stages protected raw payloads, then atomically upserts session identity, searchable messages, protected LCM raw rows, optional Git evidence, and the parse cursor. Full transcript batches use compare-and-swap cursor semantics; projection-only batches intentionally write searchable session projection and do not manufacture an LCM raw row. | This is canonical host infrastructure, not Native provider persistence. Native observation can consume a settled projection or invoke a canonical operation, but its provider store must never become the source of truth or bypass raw protection/cursor atomicity. |
| [original host/session admission path](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs#L69-L100) and [Codex JSONL host records](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/hosts/codex.rs#L1-L49) | [current project/user ingestion authorities](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/ingest/authority.rs#L1-L69), [project](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/ingest/project.rs#L52-L127), and [user/startup](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/ingest/user.rs#L382-L700) | Host JSONL records are admitted with source identity, scope, retention/cursor, byte and cancellation bounds. Project and profile authorities select/rotate host providers, run bounded observation drivers, drain projection queues, and finalize Git/workflow attribution. Startup coalesces profile-global sweeps and reports running/recent/deferred state. | These authorities own host/session lifecycle, source identity, provider frontier, and cleanup. Native should attach to their settled output through a narrow adapter; it must not parse host files independently or create an unbounded background ingest path. |
| [b3 `tracedecay-lcm` crate ownership](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/lib.rs#L1-L81) | [current LCM crate](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-lcm/src/lib.rs#L1-L81) | LCM owns lossless raw transcript ingest, summary DAG construction, external payload authority, retrieval, compression, garbage collection, and retention. Its contracts are typed and provider-neutral. | The current LCM source is unchanged from b3. It is the complete backend Native must use for session context. Do not duplicate its tables, payload authority, summary lineage, or GC in `native_provider`. |
| [b3 query API](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/query.rs#L67-L372), [expand](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/query/expand.rs#L5-L180), and [session/replay](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/query/session.rs#L8-L194) | [current retained LCM retrieval](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/retained/lcm/retrieval.rs#L59-L612) | Original LCM supports session loading, grep/search, recent/replay slices, describe, expansion of raw/summary/external payload targets, source pagination, lineage, and expand-query synthesis. Search hydrates summaries and raw messages, bounds context, and returns an explicit no-match answer; expansion verifies ownership and payload identity. Describe returns counts, overviews, target metadata, lineage, and token estimates. | Current retained handlers already map LoadSession, Grep, Describe, Expand, and ExpandQuery into canonical `SessionTemporalQuery`/LCM services. Native should reuse these routes or their application ports. It must not replace relevance/temporal filtering with staged lexical/recency ordering or strip lineage and omission metadata. |
| [b3 raw and payload authority](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/raw.rs#L56-L125) and [stage/commit/protection](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/raw.rs#L505-L801), [payload](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-lcm/src/payload.rs#L114-L347) | [current store access](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/store_access/lcm.rs#L157-L401) | Every raw read verifies content hash and sanitization receipt. Staging/commit binds source identity, protected bytes, and external payload manifest; expansion checks owner/hash and returns only verified content. Store access also supplies status, preflight, payload health, GC preview/apply, and transaction boundaries. | Provider-local staged bytes have a separate reference prefix and are advisory. They cannot be used as host grounding, raw LCM payload, or a substitute for the protected raw/payload manifest. Full Native context must hydrate through this authority. |
| [b3 LCM authority](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/lcm/authority.rs#L29-L261) | [current daemon LCM authority](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/lcm_authority.rs#L22-L149) and [admission/receipts](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/lcm_authority.rs#L178-L318) | Typed LCM operations are Ingest, Compact, Status, and Doctor, with exact capability/scope/target binding, cancellation/deadline handling, committed-state digests, and operation receipts. Completed host turns ingest canonical protected raw content; host pressure does not authorize copying caller text as a summary. Pressure compaction resolves an authoritative summary through the registered store. | Current authority is the host-facing command/query boundary. Native may request these operations only through the mounted authority and must preserve receipts and unavailable/denied/timeout outcomes. A host callback or model output is not proof of a Native summary. |
| [b3 compression/evidence policy](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-runtime/src/lcm_effects.rs#L126-L267), [summary recognition](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-runtime/src/lcm_summarization.rs#L26-L245), and [convergence](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-runtime/src/lcm_summary_convergence.rs#L47-L390) | [current same modules](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/lcm_effects.rs#L126-L375) | Compression is bounded and transactional: protect projected raw messages, plan the exact source range, resolve or validate authoritative summary evidence, commit state, and persist retryable/permanent outcomes. Summary recognition is provider-bound and exact-range/membership checked; convergence backfills and retries without inventing source membership. | Native must preserve summary source range, membership, lineage, retry, invalidation, and cancellation semantics. It cannot treat an arbitrary provider answer as a summary or use a custom staged row as compression state. |
| [b3 direct retained boundary](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L140-L346) plus [LCM application compatibility](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/session/lcm/mod.rs#L1-L29) | [current mounted retained LCM](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/retained/lcm.rs#L54-L164), [status/dispatch](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/retained/lcm.rs#L398-L554) | The daemon mounts the exact server-owned authority and registered temporal retrieval service. Project/profile mounting, status, doctor, load, grep, describe, expand, and expand-query are dispatched through that authority; no alternate database is opened by the surface adapter. | This is the correct connection point for Native. Provider selection belongs above the shared mounted services. The Native adapter must receive the owner-bound application/authority from composition and leave store/runtime/LCM routing under its existing owners. |

## What belongs to Native

Native owns the memory application/use cases and canonical fact authority
documented by the source/facts lane. For this session/LCM area, Native's
responsibility is limited to selecting the complete original application and
passing through the host-authorized session/LCM commands and results. A Native
operation may have original durable effects (for example, canonical transcript
ingest, refresh, compression, or explicit Native retrieval telemetry); those
effects must remain visible if the original operation has them. A common
provider capability may expose only an advisory projection when its contract
explicitly says so, but that projection cannot be labeled complete Native.

The following belong to shared host/session infrastructure and must remain
provider-neutral:

- host file/JSONL parsing, source identity, admission, cursor CAS, provider
  frontier, cancellation, and project/profile ingestion;
- canonical session/message projection and protected LCM raw storage;
- authorization and temporal retrieval/refresh services;
- LCM query, expansion, payload, summary DAG, compression, retention, and GC;
- retained target mounting, status/doctor, and final context rendering.

Keeping these boundaries permits Native and NCM to observe or use the same
canonical host/session/LCM state while preserving separate provider namespaces,
selection, attribution, and lifecycle. Shared canonical state is not evidence
that NCM implemented Native or that a provider-local staged row is a canonical
fact.

## Exact future write ownership

The future Native implementation lane may write the narrow adapter and
provider-selection seam:

- `crates/tracedecay-memory-provider-native/**`: the Native adapter, envelope
  translation, complete Native capability mapping, and focused parity fixtures.
  It must delegate to the owner-bound Native memory application and session/LCM
  ports and own no session/LCM tables.
- Existing product adapter files
  `crates/tracedecay/src/daemon/retained_owner/native_provider.rs` and its
  focused Native tests: only the Native adapter owner may change them, and only
  to connect complete Native behavior. The current
  `native_staged_observations.rs` remains provider-local advisory staging and
  must not be promoted to the Native authority.
- Provider construction/selection belongs to the registry/fabric owner, and
  shared retained mounting/composition belongs to its existing owner. If a
  shared interface cannot carry a required original operation or receipt, the
  contract/composition owner must change that interface separately.

Separate owners must retain:

- `crates/tracedecay-sessions/src/runtime/ingest/**`,
  `runtime/store_access/transcript.rs`, and
  `runtime/store_access/lcm.rs`: host admission, transcript persistence, and
  registered-store access;
- `crates/tracedecay-session-memory/src/session/**`,
  `crates/tracedecay-session-runtime/src/session_retrieval/**`, and retained
  LCM routing: session authorization, temporal execution, hydration, and
  typed outcomes;
- `crates/tracedecay-lcm/**`, `lcm_authority.rs`, `lcm_effects.rs`,
  `lcm_summarization.rs`, `lcm_summary_convergence.rs`, retention, and GC:
  canonical LCM behavior.

These shared files should stay untouched by the Native adapter lane unless a
separate owner approves a contract fix. Do not reimplement the LCM engine,
session temporal kernel, transcript writer, payload authority, summary
convergence, retention, or GC in a Native provider crate.

## Observable acceptance checklist

A full-Native integration is acceptable only when an isolated, real host/store
journey demonstrates all of the following:

- A host transcript is admitted with its source identity and exact scope; after
  a clean restart, the canonical session row, searchable messages, protected
  LCM raw messages, and parse cursor are present exactly once. Replaying the
  same source range is idempotent; a cursor conflict remains a typed conflict.
- A Native session lookup with session scope and authorized-root scope preserves
  provider, temporal mode, grain, direct anchor, semantic filters, cursor,
  context budget, execution limits, freshness, and diversity. The response
  exposes canonical result order/content, source refs, watermarks, coverage,
  omissions, and continuation cursor.
- The same lookup distinguishes an authorized zero-hit result from partial,
  stale, denied, locked, redacted, deleted, reset-required, budget, cancelled,
  timed-out, wrong-scope, and unavailable outcomes. No outcome is silently
  converted to empty staged recall.
- Refresh begin-or-join is durable and idempotent; status survives restart;
  cancellation records the observed frontier/coverage; scheduler delivery
  failure returns reconciliation-required while status can reconcile the
  committed operation.
- Load-session and grep honor temporal/provider/scope/message/role/time/source
  and Git filters, pagination, and bounded content. Describe returns target
  metadata, counts, lineage, source coverage, hydration state, and token
  estimate. Expand returns verified raw/summary/external content and paginates
  summary sources with ownership checks.
- Expand-query searches the canonical summary/raw surfaces, hydrates bounded
  context, returns a deterministic no-match result when appropriate, and
  preserves prompt/query truncation markers, source lineage, omissions, and
  temporal metadata.
- A completed host turn is ingested as protected canonical raw content. A
  pressure signal does not allow an unauthenticated caller summary. Compaction
  preserves exact source range/membership, summary lineage, retry state, and
  cancellation/deadline receipts.
- Payload hash/receipt failure, foreign scope, redaction, deletion/retention
  expiry, malformed cursor, budget exhaustion, and unavailable authority are
  visible typed failures. Raw/payload content is never cited as canonical
  merely because a provider-local staged reference exists.
- Native and NCM can use the same canonical session/LCM state while their
  provider namespaces, capability identity, lifecycle, attribution, and
  selection remain distinguishable. A staged Native row is labeled advisory
  and is not counted as a canonical fact or LCM source.
- The final host action uses the real mount/admission/hydration/renderer and
  records exact output bytes plus source attribution. A mock adapter pass or
  one fixed journey is insufficient to establish full Native parity.

## Unknowns and conflicting evidence

- The source lane still must confirm that b3 is the accepted complete-original
  floor. The historical metadata pins a divergent floor, so this report treats
  b3 as a source reference rather than silently changing the product pin.
- The daemon/TraceDecay graph socket was available for initial discovery but
  became unavailable for later narrow queries. This report therefore relies on
  bounded source reads and immutable Git history for the remaining mappings; no
  live database, build, model run, or test result is claimed.
- The current provider contract and ADR-0010 describe a read-only/advisory
  current-recall projection and suppress some telemetry, history, feedback,
  correction, and maintenance operations. That may be valid for one advisory
  common capability, but it conflicts with the stated requirement that Native
  be the complete original v2 implementation. The contract owner must resolve
  the operation/effect mapping rather than applying ADR-0010 to Native.
- Current retained LCM routes are visibly wired to canonical session/LCM
  services, while current Native composition visibly mounts
  `StagedObservationStore`/provider-local recall. A source-level mapping does
  not prove a concrete Native process delivers every operation; a differential
  runtime harness remains required.
- Shared canonical session/LCM state is intentionally compatible with NCM
  coexistence. It does not answer which provider is selected for a given host
  action or prove independent NCM attribution; that belongs to the provider
  selection and evaluation lanes.

## Bounded Luna Max implementation proposal

**Goal.** Connect the Native adapter to the complete original owner-bound memory,
session, and LCM services while keeping provider-local observations explicitly
advisory and preserving every original result, state effect, receipt, scope,
freshness, lineage, and failure.

**Prerequisites.**

- Source lane accepts the exact original revision/floor and supplies the
  operation/effects inventory.
- Facts/interface lanes settle how effectful Native operations, retrieval
  telemetry, refresh, compression, and typed receipts are represented at the
  provider boundary.
- Composition owner exposes the registered project/profile memory application,
  session retrieval/refresh ports, and LCM authority without giving a provider
  direct database access.
- Host/evaluation owner provides an isolated real transcript fixture and
  exact-final-output capture.

**Files and steps.**

1. Work only in the Native adapter crate plus the narrow existing
   `native_provider.rs` seam and focused tests. Inventory each original Native
   session/LCM call and map its request, owner/scope, cancellation/deadline,
   payload, receipt, and terminal outcome.
2. Replace any complete-Native recall path that reads
   `StagedObservationStore` with a call to the mounted owner-bound application
   and temporal/LCM services. Keep staged observation delivery as a separately
   named advisory capability with its own namespace and references.
3. Thread the full session query, refresh handle, LCM target, source cursor,
   context budget, temporal metadata, summary lineage, payload verification,
   and omission/failure fields through the adapter. Add a shared contract
   change only when an original behavior cannot otherwise be represented.
4. Run the paired conformance fixture against an isolated direct-original
   reference and modular Native process. Compare canonical rows, projections,
   raw/payload manifests, cursors, receipts, generation, queue/retry state,
   exact result order, context bytes, source refs, and reopened state.
5. Run real host journeys for ingestion, lookup, refresh, describe/expand,
   expand-query, compaction, cancellation/deadline, scope failure, and
   provider coexistence; aggregate repeated runs before declaring stability.

**Forbidden shortcuts.** Do not copy the LCM/session schema into Native; choose
the staged store as the Native database; add lexical/recency or other new
ranking; suppress original writes/telemetry; turn missing access into empty
results; accept host/model text as an authoritative summary; narrow project
memory to one branch or checkout; bypass scope/receipt/payload checks; or
declare parity from source-string scans, mock ports, or one successful journey.

**Stop condition.** Stop and report an explicit unresolved operation, typed
unsupported result, or contract gap when the original request, state transition,
receipt, or failure cannot be represented. Do not ship a partial Native route
under the complete-Native label until the direct-original differential and
restart/failure checks pass.


## Bounded b3→HEAD host/session delta

This qualifies the earlier source statement: the original
`tracedecay-session-memory/src/session/**` API/implementation and
`tracedecay-lcm/**` engine are unchanged, but shared host/session files in
`tracedecay-sessions` and `tracedecay-session-runtime` have additive,
behavioral integration changes. They must not be treated as an unchanged host
tree or blanket-reverted. The changes preserve the original temporal retrieval
and refresh engine while adding a root-controlled live-source seam.

| Changed hunk | Protected behavior and observed delta | Classification and smallest isolation/restoration task |
|---|---|---|
| `session_sync.rs`, `session_sync/project_lifecycle.rs`, `session_sync/git_topology.rs`, `session_sync/work.rs`; [config/scope](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/session_sync.rs#L85-L106), [registration](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs#L299-L339) | Project-open `ResolvedScope` is carried into sync and Git topology instead of re-derived from a mutable path. Historical/live transcript work can receive a root-owned `OriginalObservationProvenanceResolverV1`; the resolver does not advance a source cursor. | Host identity/provenance seam. Preserve it. The Native lane should consume the mounted authority/resolver through composition; do not restore path re-derivation. |
| `session_temporal_refresh_scheduler/history.rs`; [historical ingestor hook](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs#L62-L140) | Only the historical ingestor is given the optional provenance resolver. No hunk changes `SessionRefreshService`, refresh target/digest/receipt types, scheduler state transitions, or begin/status/cancel outcome mapping in `tracedecay-session-memory`. | Original temporal refresh behavior remains protected. The smallest task is a refresh regression fixture proving the resolver is ancillary and refresh receipts/frontiers/reconciliation remain identical; no refresh rewrite is required. |
| `runtime/hosts/codex.rs`, `hosts/codex/meta.rs`, `hosts/codex/observation.rs`; [strict live locator](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/hosts/codex.rs#L1040-L1075), [bounded lookup](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/hosts/codex.rs#L1270-L1510), [live header](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/hosts/codex/meta.rs#L74-L158) | Adds a bounded, no-follow, stable scan of both Codex roots; requires the first complete `session_meta` frame, matching native IDs, and a cwd inside the admitted checkout. Filename fallback, conflicting IDs, links, rewrites, oversized/deep/ambiguous scans, and expired deadlines defer or fail. Ordinary retained discovery remains available; project admission receives an optional session filter, with ordinary callers passing `None`. | New host bootstrap/provenance seam, not a replacement for historical parsing. Wire Native SessionStart through this locator and then the sealed admission path; keep ordinary catch-up separate. |
| `runtime/source/jsonl.rs`, `runtime/source.rs`, `runtime/observation/jsonl_observation_admission.rs`; [live capture](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/source/jsonl.rs#L579-L812), [sealed validation](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs#L82-L180), [admission guard](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/observation/jsonl_observation_admission.rs#L2270-L2430) | Adds content-free live origin watermarks/frame witnesses, birth/file identity, prefix and generation revalidation, max-end ceilings, and cancellation checks. A mismatch returns deferred before capture/cursor writes; normal JSONL ingestion remains the existing path. | New live-boundary safety behavior. Preserve privacy and source identity. The smallest task is one end-to-end sealed-source fixture asserting no write on mismatch and exactly one canonical write on a stable page. |
| `runtime/store_access/transcript.rs`; [cursor encoding](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/store_access/transcript.rs#L130-L322), [live locator transaction](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/store_access/transcript.rs#L324-L386), [monotonic path](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/runtime/store_access/transcript.rs#L970-L1010) | This is the concrete canonical-storage delta. Ordinary transcript cursor columns now use checked signed storage and reject negative corruption; Codex corpus epochs and OpenCode frontiers retain full-width bit patterns. Codex epochs are refused by the monotonic writer and require exact CAS. A new ProjectSessions-only transaction registers a live session locator, fills a missing transcript path only when project/path identity agrees, and never replaces existing metadata. | Preserve this storage contract; it is not a Native scorer. Add a b3/current fixture for ordinary cursor round-trip, reserved-frontier round-trip, exact-CAS rejection, locator conflict, rollback, and reopen. Do not solve it by reverting the encoding or bypassing the transaction. |
| `observation.rs`, `repository_provenance.rs`; [attachment path](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/observation.rs#L497-L605), [resolver/binding](https://github.com/BleedingDev/tracedecay/blob/571daf3a9612e5247443e4da3a107b542686c1ef/crates/tracedecay-sessions/src/repository_provenance.rs#L40-L190) | A root-supplied receipt can attach frozen original repository evidence only after exact source identity, project/repository/worktree, and authority reference validation. Generic historical ingestion keeps ordinary provenance; an original proof cannot be reused for an adjacent event. | Accepted source/provenance hardening. Keep it outside the Native adapter and test exact mismatch/absence fallback. |
| `runtime/ingest/failure.rs` and one-line call-site/test updates | Failure logs now expose static operation labels and bounded sanitized storage detail; call sites pass the new optional session argument. No retrieval or durable outcome algorithm changes. | Diagnostics/API plumbing. Preserve sanitization and verify callers rather than restoring old signatures. |

The smallest external-seam task is therefore to expose one composition-owned
live-origin resolver/authority to the Native adapter, call the existing sealed
Codex admission and canonical transcript transaction, and leave the original
session temporal refresh/retrieval and LCM engine untouched. The acceptance
fixture must cover stable live admission, source replacement/ambiguous lookup
deferral, atomic row/raw/cursor persistence, ordinary historical catch-up, and
refresh begin/status/cancel after restart. No blanket rollback is justified by
this diff.
