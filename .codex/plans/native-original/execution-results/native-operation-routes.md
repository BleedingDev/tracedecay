# Original Native operation-to-route handoff

Reference implementation: `b3b43410e47115056f2066449aafa1822bbb6049`.
Current composition: `571daf3a9612e5247443e4da3a107b542686c1ef`.

This is a route/effect handoff, not a claim that the current selected Native
provider is already complete. `DirectRetainedMemoryPortV1`,
`DirectRetainedSessionPortV1`, and `DirectRetainedLcmPortV1` remain concrete
production routes for the canonical authorities. The current selected Native
provider still has staged/absent connections for the rows marked **Native
gap**; a downstream owner must connect those rows to the listed owner-bound
port before full-Native completion. No generic operation is allowed to hide a
missing original route.

## Typed context decision

Automatic model context uses the canonical `memory_matches` contribution once.
The registry/composition owner must supply a trusted selected-registration
decision containing the provider identity, accepted registration revision, and
admitted exact scope digest. The host computes the canonical request and
canonical contribution SHA-256 values from the actual bytes it executed and
delivered. It creates `NativeContextDeliveryMarker` only after that route has
completed, and the context compiler calls `verify_for` with both host-computed
digests before reusing the contribution. The marker's public constructor/private
fields are data assembly only and are not authorization. Provider payloads,
descriptors, display names, and provider-local receipts never carry or create a
marker; the provider ID is compared as the selected registration identity and
is never interpreted as a Native name.

Generic provider `Recall` stays read-only. Direct retained fact `Search` keeps
the original nonempty retrieval recording command and receipt. That explicit
search effect is not added to automatic context reads.

## Route status vocabulary

* **Connected**: the current production route reaches the original owner.
* **Native gap**: the current Native provider route is staged, advisory-only,
  or absent; the direct canonical route exists but selected Native composition
  does not yet invoke it.
* **Shared host**: the operation is owned by host/session/LCM infrastructure;
  Native may request it through a mounted port but cannot own its storage.

For every row, the named case is the executable parity/conformance case that
must be retained or added by the downstream owner. A `Native gap` is a
connection blocker, not permission to return a fabricated success.

## Canonical facts

The b3 public low-level `FactStore` surface is in
`crates/tracedecay-session-memory/src/fact_store/mod.rs`; typed use cases are in
`src/memory/project_memory.rs`, `src/memory/curation.rs`, and
`src/memory/privacy_remediation.rs`. The current direct route is
`crates/tracedecay-store-runtime/src/retained_memory.rs::DirectRetainedMemoryPortV1`
(`project` or `profile`), which resolves the owner-bound database and creates
`MemoryApplication<DatabaseFactStore>`.

| b3 operation and caller | Current production-composition route | Owner; effect and receipt | Named executable case / status |
|---|---|---|---|
| `FactStore::commit_fact(FactWriteBatch, FactWriteControl)`; `MemoryApplication::commit_fact` callers in `memory/project_memory.rs` | `DirectRetainedMemoryPortV1::execute_add` → `execute_add_on_db` | `DatabaseFactStore`/memory authority; append assertion/event, projection, graph scheduling, idempotent commit receipt | `memory/tests/project_memory_contracts.rs::fact_add_preserves_the_authoritative_sanitizer_receipt`; **Connected** for direct, **Native gap** |
| `FactStore::query_current_facts(CurrentFactsQuery)` | `DirectRetainedMemoryPortV1::execute_read` → `list_on_db` or application list | Owner-bound current projection read; page cursor, no write receipt | `fact_store` current-page owner/cursor case; **Connected** direct, **Native gap** |
| `FactStore::query_fact_current(FactCurrentQuery)` | `DirectRetainedMemoryPortV1::execute_read` → `get_on_db` | Current projection read; no mutation receipt | `memory.get.current-owner`; **Connected** direct, **Native gap** |
| `FactStore::query_fact_current_response(FactCurrentQuery)` | Same `get_on_db` owner route or typed application response | Current response metadata/read; no mutation receipt | `fact_response_metadata_test`; **Connected** direct, **Native gap** |
| `FactStore::query_fact_as_of(FactAsOfQuery)` | No variant in current retained provider request; direct owner application remains available | Historical projection read at explicit time; no mutation receipt | `memory.get.as-of`; **Native gap** (provider envelope does not expose it) |
| `FactStore::query_fact_as_of_response(FactAsOfQuery)` | No current Native route; direct owner application | Historical response/validity metadata read; no mutation receipt | `memory.get.as-of-response`; **Native gap** |
| `FactStore::query_fact_lineage(FactLineageQuery)` | No current Native route; direct owner application | Immutable lineage read; no mutation receipt | `memory.history.lineage`; **Native gap** |
| `FactStore::query_fact_lineage_response(FactLineageQuery)` | No current Native route; direct owner application | Typed lineage response/coverage read; no mutation receipt | `memory.history.lineage-response`; **Native gap** |
| `FactStore::get_retrieval_anchor(RetrievalAnchorQuery)` | Direct retained memory target only; no Native provider route | Retrieval-anchor read bound to owner/scope; no mutation receipt | `memory.retrieval-anchor`; **Native gap** |
| `ProjectMemoryFactStore::purge_project_memory_superseded_payloads(owner, cursor, limit, write_control)` | No current Native route; staged `maintenance` is not equivalent | Canonical privacy purge; owner/detector/cursor receipt and payload deletion effect | `privacy_purge` receipt/cursor case; **Native gap** |
| `list_project_memory_facts(ProjectMemoryFactListQueryV1)` | `DirectRetainedMemoryPortV1::execute_read` → `list_on_db` | Current canonical page read; owner/cursor and coverage, no write receipt | `memory.list.owner-and-cursor`; **Connected** direct, **Native gap** |
| `search_project_memory_facts(ProjectMemoryFactSearchQuery)` | `DirectRetainedMemoryPortV1::execute_read` → `search_on_db` | Canonical fixed-point scoring/order; nonempty direct search additionally records retrieval telemetry | `memory.search.nonempty-retrieval-receipt`; **Connected** direct, **Native gap** |
| `probe_project_memory_facts(ProjectMemoryFactSearchQuery)` | `DirectRetainedMemoryPortV1::execute_read` → `semantic_search_on_db` | Canonical read objective; no retrieval write/receipt | `memory.probe.read-only`; **Connected** direct, **Native gap** |
| `related_project_memory_facts(ProjectMemoryFactSearchQuery)` | Same `semantic_search_on_db` route | Canonical related read/order; no retrieval write/receipt | `memory.related.read-only`; **Connected** direct, **Native gap** |
| `reason_project_memory_facts(ProjectMemoryFactSearchQuery)` | Same `semantic_search_on_db` route | Canonical reason read/order; no retrieval write/receipt | `memory.reason.read-only`; **Connected** direct, **Native gap** |
| `find_project_memory_contradictions(ProjectMemoryFactContradictionQueryV1)` | `DirectRetainedMemoryPortV1::execute_read` → `contradict_on_db` | Contradiction/read projection; no mutation receipt | `memory.contradict.owner-and-limit`; **Connected** direct, **Native gap** |
| `get_project_memory_fact(ProjectMemoryFactIdV1)` | `DirectRetainedMemoryPortV1::execute_read` → `get_on_db` | Current fact projection read; no mutation receipt | `memory.get.owner-bound`; **Connected** direct, **Native gap** |
| `project_memory_fact_history(ProjectMemoryFactHistoryQueryV1)` | No current Native provider route; direct application history use case | Full immutable history/lineage read; no mutation receipt | `memory.history.cursor-and-owner`; **Native gap** |
| `project_memory_status(FactOwnerV1, FactReadControl)` | `DirectRetainedMemoryPortV1::execute_status` → `execute_status_on_db` | Canonical counters/status read; no mutation receipt | `memory.status.canonical-counters`; **Connected** direct, **Native gap** |
| `inspect_project_memory_fact(ProjectMemoryFactIdV1)` | No current Native provider route; direct application inspection | Diagnostic/inspection read; no mutation receipt | `memory.inspect.authority-bound`; **Native gap** |
| `add_project_memory_fact(ProjectMemoryFactAddCommandV1, FactWriteControl)` | `DirectRetainedMemoryPortV1::execute_add` → `execute_add_on_db` | Sanitization, duplicate/conflict classification, canonical commit, graph scheduling; commit/idempotency receipt | `memory/tests/project_memory_contracts.rs::fact_add_preserves_the_authoritative_sanitizer_receipt`; **Connected** direct, **Native gap** |
| `update_project_memory_fact(ProjectMemoryFactUpdateCommandV1, FactWriteControl)` | `DirectRetainedMemoryPortV1::execute_update` → `execute_update_on_db` | Correction assertion/CAS, immutable lineage, current projection; update commit receipt | `memory/tests/curation_mutations.rs::update_command_binds_the_canonical_patch_to_owner_snapshot_and_context`; **Connected** direct, **Native gap** |
| `remove_project_memory_fact(ProjectMemoryFactRemoveCommandV1, FactWriteControl)` | `DirectRetainedMemoryPortV1::execute_remove` → `execute_remove_on_db` | Canonical deleted event/projection/tombstone, idempotent not-found handling; remove commit receipt | `memory.remove.idempotent-and-cas`; **Connected** direct, **Native gap** |
| `supersede_project_memory_fact(ProjectMemoryFactSupersedeCommandV1, FactWriteControl)` | `DirectRetainedMemoryPortV1::execute_supersede` → `execute_supersede_on_db` | Curated supersession lineage/CAS; supersession commit receipt | `memory/curation/tests.rs::supersession` cases; **Connected** direct, **Native gap** |
| `record_project_memory_fact_feedback(ProjectMemoryFactFeedbackCommandV1, FactWriteControl)` | `DirectRetainedMemoryPortV1::execute_feedback` → `execute_feedback_on_db` | Canonical trust delta, feedback history/finding, projection; feedback event/commit receipt | `memory/tests/project_memory_contracts.rs::semantic_similarity_outcomes_are_receipt_bearing_commits`; **Connected** direct, **Native gap** |
| `project_memory_fact_feedback_history(ProjectMemoryFactFeedbackHistoryQueryV1)` | No current Native provider route; direct application history use case | Feedback history read; no mutation receipt | `memory.feedback-history.owner-and-limit`; **Native gap** |
| `find_project_memory_fact_by_content_digest(ProjectMemoryFactContentDigestQueryV1)` | No current Native provider route; direct application exact lookup | Digest-only exact content lookup for automation dedupe; no mutation receipt | `memory.exact-content-digest`; **Native gap** |
| `apply_project_memory_fact_curation(ProjectMemoryFactCurationBatchV1, FactWriteControl)` | No current Native provider route; staged `replay` is not curation | Reviewed add/update/merge/remove/link transaction; curation receipt and changed-fact effects | `memory/curation/tests.rs::curation_*`; **Native gap** |
| `merge_project_memory_facts(ProjectMemoryFactMergeCommandV1, FactWriteControl)` | No current Native provider route; direct curation application | Sanitized merge, source lineage and target CAS; merge/curation receipt | `memory/tests/curation_mutations.rs::merge_command_preserves_each_snapshot_and_sanitizes_content_before_authority`; **Native gap** |
| `dashboard_project_memory_overview(ProjectMemoryDashboardMemoryOverviewQueryV1)` | No current Native provider route; direct dashboard application | Dashboard read over canonical projections/graph; no mutation receipt | `memory.dashboard.overview`; **Native gap** |
| `dashboard_project_memory_fact_detail(ProjectMemoryDashboardFactDetailQueryV1)` | No current Native provider route; direct dashboard application | Fact detail/lineage read; no mutation receipt | `memory.dashboard.fact-detail`; **Native gap** |
| `dashboard_project_memory_store_revision(FactOwnerV1, FactReadControl)` | No current Native provider route; direct dashboard application | Store revision/status read; no mutation receipt | `memory.dashboard.store-revision`; **Native gap** |
| `dashboard_project_memory_vector_snapshot(ProjectMemoryDashboardVectorPointsQueryV1)` | No current Native provider route; direct dashboard application | Canonical vector snapshot read; no mutation receipt | `memory.dashboard.vector-snapshot`; **Native gap** |
| `dashboard_project_memory_oplog(ProjectMemoryDashboardOplogQueryV1)` | No current Native provider route; direct dashboard application | Canonical operation log read; no mutation receipt | `memory.dashboard.oplog`; **Native gap** |
| `record_project_memory_fact_retrieval(ProjectMemoryFactRetrievalCommandV1, FactWriteControl)` | Direct `search_on_db` invokes `track_explicit_search`; Native `recall_project_memory` currently omits it | Explicit nonempty Search writes retrieval/access telemetry, refreshed projections and idempotent retrieval receipt; probe/related/reason remain read-only | `memory.search.nonempty-retrieval-receipt`; **Native gap** until Native preserves this exact boundary |
| `apply_project_memory_automatic_fact(ProvenanceId, add command, evidence, FactWriteControl)` | No current Native provider route; staged maintenance/replay is not automatic-fact apply | Canonical automatic fact add/quarantine and automation receipt; idempotent apply disposition | `automatic_facts/tests.rs::automatic_fact_command`; **Native gap** |
| `get_project_memory_automatic_fact_receipt(owner, apply_id, read_control)` | No current Native provider route | Automatic apply receipt read; no mutation receipt | `memory.automatic.receipt-recovery`; **Native gap** |
| `list_project_memory_automatic_fact_receipts(owner, state, cursor, limit, read_control)` | No current Native provider route | Bounded receipt page/cursor read; no mutation receipt | `memory.automatic.receipt-page`; **Native gap** |
| `project_memory_automation_run_receipts(owner, run_id, read_control)` | No current Native provider route | Run receipt recovery read; no mutation receipt | `memory.automatic.run-receipts`; **Native gap** |
| `ProjectMemoryGraphStore::project_memory_graph(ProjectMemoryGraphQueryV1)` | Direct graph/dashboard owner route; no Native provider route | Canonical graph/manifest read with typed degradation/cancellation; graph reconciliation is scheduled after committed writes | `fact_store/graph_reconciliation_tests.rs::graph_reset_required_preserves_the_exact_memory_owner`; **Native gap** |

The current provider's `FactPromotion` is verification of a host-settled fact,
not `add_project_memory_fact`; its staged observation database and lifecycle
receipts cannot satisfy any **Native gap** row above.

## Sessions, refresh, and task-session lookup

The b3 session application surface is under
`crates/tracedecay-session-memory/src/session/**` and transcript persistence is
under `src/transcript.rs`. The current production route is
`crates/tracedecay-session-runtime/src/retained/session.rs::DirectRetainedSessionPortV1`
plus project/profile host ingestion authorities. Native must receive these
owner-bound ports; it must not parse host files or create a provider session
database.

| b3 operation/caller | Current production-composition route | Owner; effect and receipt | Named executable case / status |
|---|---|---|---|
| `SessionTemporalQuery` → `SessionRetrievalService::retrieve` (session or authorized-root message search) | `DirectRetainedSessionPortV1::execute_session` → `execute_message_search` / `execute_profile_message_search` → `DaemonSessionRetrievalService` | Session temporal authority; read-only retrieval with scope, temporal mode, cursor, freshness, diversity, context budget and typed coverage/omissions/failures | `session/tests/application.rs::evidence_bearing_empty_execution_maps_to_complete_zero`; **Connected** direct, **Native gap** |
| `TaskSessionRetrievalService` task-session lookup and callback execution | No current retained Native/provider route; task-session module remains canonical application API | Task-session binding/retriever/policy revision, rank selector and cursor manifest; read-only typed result/refusal | `session/retrieval/task_session.rs::task_session_preserves_cursor_manifest_refusal_details`; **Native gap** |
| `SessionRefreshService::begin_or_join(SessionRefreshBeginOrJoinRequestV1)` | `DirectRetainedSessionPortV1::execute_session` → `execute_session_refresh` → mounted refresh port | Durable frontier-bound begin/join and scheduler wake; operation handle, join digest, reconciliation-required outcome when delivery fails | `session refresh restart/begin-or-join`; **Connected** direct, **Native gap** |
| `SessionRefreshService::status(SessionRefreshProgressRequestV1 / receipt request)` | `retained/session/refresh.rs` through mounted `RetainedSessionRefreshPortV1` | Durable progress/coverage/terminal receipt read; no new mutation | `retained/session/refresh.rs::status` cases; **Connected** direct, **Native gap** |
| `SessionRefreshService::cancel(SessionRefreshCancellationRequestV1)` | `retained/session/refresh.rs` through mounted refresh port | Persisted cancellation with observed frontier/coverage; cancellation or reconciliation receipt | `retained/session/refresh.rs::cancel` cases; **Connected** direct, **Native gap** |
| `SessionTranscriptAdapter` ingest/persist batch (full or projection-only) | `tracedecay-sessions` host ingestion → `runtime/store_access/transcript.rs` transaction | Host/session authority; atomic session row, searchable projection, protected LCM raw row, source/cursor CAS; ingest receipt/frontier | `native-sessions` stable sealed-source and cursor-CAS fixture; **Shared host**, **Native gap** for provider delivery |
| Project/profile `SessionsFor` lookup | `DirectRetainedSessionPortV1::execute_session` → `execute_sessions_for` (project) | Canonical session catalog read with owner/scope; typed result, no mutation receipt | `session.sessions-for.scope-and-cursor`; **Connected** project, **Native gap** |
| Project `Workflows` lookup | `DirectRetainedSessionPortV1::execute_session` → `execute_workflows` | Workflow index read bound to project; typed result, no mutation receipt | `session.workflows.project-scope`; **Connected** project, **Native gap** |
| Project/profile host observation admission and bounded driver drain | `runtime/ingest/project.rs`, `runtime/ingest/user.rs`, JSONL admission and provider frontier | Shared host lifecycle; source identity, privacy, cancellation, cursor, projection queue and startup sweep state; host receipts | `native-sessions` stable/mismatched sealed admission fixture; **Shared host** |
| Historical refresh scheduler and provenance-resolver hook | `session_temporal_refresh_scheduler/history.rs` + `SessionRefreshService` | Shared scheduler; durable refresh state and source coverage; no provider-local receipt | `native-sessions` historical catch-up + refresh-after-restart fixture; **Shared host** |

## LCM query and authority operations

The b3 LCM engine is under `crates/tracedecay-lcm/**`. The current production
query route is `DirectRetainedLcmPortV1` in
`crates/tracedecay-session-runtime/src/retained/lcm.rs`; the current authority
route is `MountedLcmAuthorityPort` and `DaemonLcmAuthority`. Raw/payload
protection, summary lineage, retention, and GC remain LCM/session owners.

| b3 operation/caller | Current production-composition route | Owner; effect and receipt | Named executable case / status |
|---|---|---|---|
| `query::session::load_session(LcmLoadSessionRequest)` | `DirectRetainedLcmPortV1::execute_lcm` → retained retrieval | LCM/session read; bounded messages, source coverage, cursor and temporal metadata; read receipt | `lcm.load-session.scope-temporal`; **Connected** direct, **Native gap** |
| `query::grep::grep(LcmGrepRequest)` | `DirectRetainedLcmPortV1::execute_lcm` → retained retrieval | LCM search read; provider/session/message/role/time/Git filters and deterministic order; read receipt | `lcm.grep.filters-and-order`; **Connected** direct, **Native gap** |
| `query::session::recent_sessions(LcmRecentSessionsRequest)` | Retained session/LCM query services where exposed | Session catalog/recent slice read; no mutation receipt | `lcm.recent-sessions.scope`; **Native gap** if not exposed through selected Native port |
| `query::session::session_providers(LcmSessionProvidersRequest)` | Retained session/LCM query services where exposed | Provider census read; no mutation receipt | `lcm.session-providers.scope`; **Native gap** if not exposed through selected Native port |
| `query::session::session_replay_slice(LcmSessionReplaySliceRequest)` | `DirectRetainedLcmPortV1` retrieval/session service | Replay slice read preserving atomic tool transactions and cursor; read receipt | `lcm.replay-slice.atomic-pairs`; **Native gap** |
| `query::describe::describe(LcmDescribeRequest)` | `DirectRetainedLcmPortV1::execute_lcm` → describe service | Target metadata/counts/lineage/token estimate read; no mutation receipt | `lcm.describe.lineage-and-counts`; **Connected** direct, **Native gap** |
| `query::expand::expand(LcmExpandRequest)` | `DirectRetainedLcmPortV1::execute_lcm` → expand service | Verified raw/summary/external payload read; owner/hash checks and source pagination | `lcm.expand.payload-hash-and-owner`; **Connected** direct, **Native gap** |
| `query::expand_query::expand_query(LcmExpandQueryRequest)` | `DirectRetainedLcmPortV1::execute_lcm` → expand-query service | Canonical raw/summary search + bounded hydration/no-match; read receipt | `lcm.expand-query.no-match-and-omissions`; **Connected** direct, **Native gap** |
| `query::status::store_status` and payload-health queries | `DirectRetainedLcmPortV1::execute_lcm` status path / mounted authority | LCM/raw/payload/DAG status read; no mutation receipt | `lcm.status.payload-health`; **Connected** direct, **Native gap** |
| `LcmAuthorityOperation::Ingest` | `MountedLcmAuthorityPort::execute` → `DaemonLcmAuthority` → retained LCM store | Protected raw-message ingest and external payload manifest; committed-state digest and authority receipt | `lcm_authority/tests.rs::ingest` + restart case; **Shared host**, **Native gap** |
| `LcmAuthorityOperation::Compact` | Mounted authority → `DaemonLcmAuthority` → compression/effects/convergence | Transactional summary/compression and replay state; exact source range/membership, retry/cancellation and committed receipt | `lcm_authority/tests.rs::compact`; **Shared host**, **Native gap** |
| `LcmAuthorityOperation::Status` | Mounted authority → daemon LCM authority | Lifecycle/DAG/payload status read; authority receipt | `lcm_authority/tests.rs::status`; **Shared host**, **Native gap** |
| `LcmAuthorityOperation::Doctor` | Mounted authority → daemon LCM authority | Health/projection/payload findings read; authority receipt | `lcm_authority/tests.rs::doctor`; **Shared host**, **Native gap** |

## LCM and host background responsibilities

These are original responsibilities even when no interactive provider
operation exists. Their current owner must remain reachable through composition;
`maintenance`, `snapshot`, or `replay` on the staged provider is not an
equivalent route.

| b3 responsibility and exact symbols | Current production route / owner | Effect and receipt | Named executable case / status |
|---|---|---|---|
| Compression lifecycle: `compression::{update_lifecycle,lifecycle_state,record_session_boundary,preflight,compress,compress_retained_page}` | Mounted LCM authority → `lcm_effects`, `lcm_summarization`, `lcm_summary_convergence` | Durable lifecycle/debt, protected raw range, authoritative summary, retry state and compression receipt | `lcm_authority` compact + convergence tests; **Shared host**, **Native gap** |
| Summary DAG: `dag::{insert_summary_node,expand_summary_node,expand_summary_nodes,load_uncondensed_summary_nodes}` | LCM store/runtime called by compression and expansion services | Summary lineage/source membership and expansion read/write; summary node/commit evidence | `summary_convergence_tests` + DAG expansion cases; **Shared host**, **Native gap** |
| Raw transcript protection: `raw::{stage_raw_message_with_payload_tracked,commit_staged_raw_message,upsert_raw_message_with_payload_tracked,protect_replay_field_value_tracked}` | Host transcript store access and LCM raw owner | Sanitization, payload externalization, hash/receipt verification and atomic raw commit | `raw/ingest_protection_defaults_tests`; **Shared host** |
| External payload authority: `payload::{write_external_payload_tracked,upsert_payload_metadata,expand_payload,read_verified_payload_content_with_checkpoint}` | LCM payload filesystem authority | Protected file/metadata writes and verified reads/deletes; payload receipt/manifest | `payload/filesystem_authority` verified-read/race cases; **Shared host** |
| Payload GC: `gc::{referenced_payload_refs,run_payload_gc,run_payload_gc_with_apply,finalize_gc_report,rewrite_dangling_placeholders}` | LCM GC scheduler/store transaction | Preview/apply deletion, tombstones, quarantine/retry and GC report | `gc/tests.rs` committed-delete/retry cases; **Shared host**, **Native gap** |
| Retention: `retention::{read_session_retention_backlog,run_session_retention,run_session_retention_authorized}` | LCM retention scheduler and authorized store access | Offload/drop/dedupe/backup effects, retention report and failure/retry state | `retention/tests.rs` retention window cases; **Shared host**, **Native gap** |
| Summary convergence/backfill: `summary_convergence::{backfill_queue_has_work,backfill_queue_page,next_candidate,candidate_for_session,record_current_protection_progress,complete_stale_raw_revision,record_invalidation_page_yield,record_outcome,next_retry_at_ms}` | LCM convergence worker and compression authority | Durable backfill cursor, protection progress, invalidation, outcome and retry receipts | `summary_convergence_tests`; **Shared host**, **Native gap** |
| Session projection worker and canonical queue drain | Host/session runtime projection workers after transcript ingest | Searchable projection freshness, queue/retry state and projection status | `native-sessions` restart/projection fixture; **Shared host** |
| Codex live locator/sealed source admission | `runtime/hosts/codex.rs`, `runtime/source/jsonl.rs`, JSONL admission and transcript transaction | No-follow source identity, sealed frame witness, atomic cursor/write; deferred mismatch outcome | `native-sessions` stable/mismatch/rollback fixture; **Shared host** |

## Unsupported generic operations and blockers

The following current provider capabilities have no exact b3 Native equivalent
at the provider boundary and therefore remain unsupported until an independent
original caller and owner are identified: generic snapshot export/restore,
generic source deletion, generic maintenance, generic replay, provider-local
inspection/feedback/correction wrappers, and provider-local session observation
staging. They must return typed unsupported/refusal or be omitted from the
Native descriptor; they must not call the staged store, create provider-local
receipts/generations, or return an empty successful result.

The concrete connection blockers handed to downstream owners are:

1. Native composition must receive the owner-bound project/profile fact
   application and route every supported fact row above, including explicit
   Search retrieval telemetry and canonical mutation receipts.
2. Native composition must receive mounted session retrieval, refresh,
   task-session, and transcript/LCM authorities with their original scope,
   cursor, cancellation, deadline, freshness, lineage, and receipt fields.
3. Registry/fabric must keep Native's explicit legacy profile bit and bind the
   marker to the selected registration identity/revision; it must not infer
   Native from the provider name or descriptor.
4. Host/context code must compute both canonical digests from the actual
   request/result bytes, create the marker only after one canonical
   `memory_matches` execution, and call `verify_for` before bypassing another
   Native query.

Until these owners land and independent b3-versus-current executable cases pass,
the route map is complete as an inventory but full Native parity remains
blocked by the listed missing production connections.
