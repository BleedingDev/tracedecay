# Native adapter and substitute-store boundary

Audit point: 571daf3a9612e5247443e4da3a107b542686c1ef in the assigned
worktree. This is a read-only planning report. No product files, stored data,
migrations, builds, tests, or application runs were changed or executed.

## Short answer

The provider adapter crate is thin by itself: it opens no database and its
NativeMemoryApplicationPort is the intended seam. The current product port
behind that seam is not a thin original Native implementation. It owns
StagedObservationStore, a bounded actor command set for stage/control, a
provider-local SQLite schema, a custom recency/lexical scorer, staged
candidate projection, and the lifecycle receipts built around that store. The
store is declared from
[retained_owner.rs](../../../../crates/tracedecay/src/daemon/retained_owner.rs#L26-L45)
and opened by
[ProjectNativeMemoryApplicationPort::new](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L186-L318).

The removal boundary is clear. The adapter must stop classifying a session
observation as NativeObservation::StagedSession; the product Native port must
stop opening, writing, reading, scoring, or projecting the staged store; and
composition must stop making the Native mount depend on that SQLite open.
Recall and lifecycle calls then have to reach the complete,
source-authoritative Native owner with its original ordering, state changes,
effects, and failures. Deleting store call sites while retaining the current
descriptor capabilities or returning an empty common-profile page would leave
dead routes and would still not be original Native.

The local pre-staging revision
cc6252ac74dd18854f35f9f63dbf972a202814cd (the parent of staging commit
44f4fbc17357d9e9521770d2ce253acd1ce03828) is useful as an integration floor:
its port held only a descriptor and the verify/recall actor, its observation
path handled the fact-promotion form, and its recall opened owner-bound
project memory. It is not evidence that this is the complete Zack-authored
Native v2 baseline. The source lane must pin that baseline before an
implementation writer changes capabilities, score/output shape, state schema,
or operation effects.

Two interfaces require explicit coordination. FactPromotion is a settled
canonical-fact verification path and must remain owned by the canonical
Native fact authority unless the facts lane proves a different original
operation. session.message_committed.v1 is a host observation-contract kind,
not a Native-owned database identity. The session lane must decide whether
Native rejects it, routes it to an original Native consequence, or leaves it to
another provider. Removing Native staging must not remove the host's canonical
session contract, journal, or replay machinery. The data lane owns any
quarantine or migration decision for existing staged SQLite files.

## Substitute-store call map

The following map covers every shipped product path found for the added Native
store/scorer. Test-only fixtures are grouped in the final row instead of
tracing every helper in the large staged-store file.

| Original revision/reference | Current path and symbol | Current substitute behavior | Future owner and required difference |
|---|---|---|---|
| Pre-staging Native port at cc6252ac74dd18854f35f9f63dbf972a202814cd: ProjectNativeMemoryApplicationPort had only descriptor and actor ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L131-L168)). | [ProjectNativeMemoryApplicationPort fields](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L186-L197); [new and off-runtime constructors](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L224-L318). | The port opens <provider-state>/native/staged-observations-v1.sqlite3 and passes an Arc<StagedObservationStore> to the actor. project_composition.rs makes its mount async solely for that blocking open ([mount rationale](../../../../crates/tracedecay/src/daemon/project_composition.rs#L347-L354)). | Native implementation owner restores the source-authoritative Native state owner and constructor. Composition owner updates the Native branch ([construction branch](../../../../crates/tracedecay/src/daemon/project_composition.rs#L407-L444)) after that API is accepted. Keep spawn_blocking only if the verified original backend has an independent blocking open; do not preserve it for the staged file. |
| Pre-staging adapter at cc6252ac74dd18854f35f9f63dbf972a202814cd: NativeObservation was one verification-only envelope ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay-memory-provider-native/src/lib.rs#L101-L118)); parser accepted only fact promotion ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay-memory-provider-native/src/lib.rs#L350-L375)). | Staged constants and the two-variant enum are declared in the adapter ([constants and enum](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L62-L175)); parse_observation maps session.message_committed.v1, source.edit_settled.v1, test.execution_settled.v1, and feedback.outcome_settled.v1 with original_source attribution to the staged branch ([parser](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L419-L491)). | Classification authorizes the later durable consequence. The adapter sends a StagedSession variant instead of an unsupported result for those kinds. | Adapter/interface owner removes the staged constants, variant, staged contract pairing, and staged parser branch, or replaces them with an exact source-approved Native observation shape. The generic host observation contract and its other kinds stay owned by the contract/session lanes. Do not silently map structured session/source/test/feedback observations to canonical facts or to search. |
| The generic provider dispatch boundary is present in the pre-staging adapter. | [NativeProvider::invoke](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L559-L635). | Validation, payload-contract checks, descriptor refresh, and operation dispatch are useful. The substitute-specific part is the parse_observation result that reaches port.observe. | Adapter owner keeps zero-contact validation and operation identity. It delegates only the source-approved typed observation and operation methods. Unsupported session kinds receive the contract's typed terminal before Native contact; a removed variant must not become a fabricated success. |
| Pre-staging observe dispatched only Verify ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L319-L338)). | [observe](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L677-L705) calls Verify for FactPromotion and observe_staged_session for StagedSession; [observe_staged_session](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L337-L405) extracts source identity and builds a staged record. | The session path stores admitted bytes, source IDs, operation/request identity, exact scope, and a provider receipt. It explicitly never writes a canonical fact. | Native implementation owner removes observe_staged_session, source extraction, and staged replies, and keeps or replaces fact verification according to the source/facts operation matrix. Session owner updates delivery expectations. A Native success may be returned only for an actual original Native consequence committed by its owner. |
| Pre-staging actor had only Verify and Recall ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1076-L1088)). | NativeReadCommand adds Stage and store Control carrying store types ([commands](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1712-L1736)); NativeReadActor::new captures the store ([actor construction](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1751-L1779)); actor main calls stage_controlled and control ([dispatch](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1872-L1966)). | This is the ownership center of the substitute. Stage and all current lifecycle control are serialized through a product-owned actor and SQLite transaction. | Native implementation owner removes store-bearing commands and dispatch helpers once the original operation owner is wired. Preserve the bounded actor only where the complete Native implementation needs it. Every advertised lifecycle operation must have a real original owner or be removed from the descriptor; leaving a method that answers through a missing store is a dead route. |
| Pre-staging descriptor advertised only health, observation, and recall ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L407-L425)). | Current descriptor advertises memory.advisory_common.v1 and feedback, maintenance, inspection, correction, deletion, snapshot, replay, and facts capabilities ([descriptor](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L798-L828)); current limits also changed to a 65,536-byte request/response shape ([limits](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L785-L795)). | The added capabilities are backed by staged control/projection code, not shown to be original Native capabilities. STATE_SCHEMA_VERSION is native-staged-v2 ([identity constants](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L67-L75)). | Source/Native owner sets descriptor identity, schema version, limits, and capability list from the pinned complete implementation. Adapter/registry owners consume that declaration. Do not retain capabilities merely because the neutral trait has methods; do not remove an original capability merely because the old integration floor lacked it. |
| The pre-staging handshake returned the fixed descriptor and owner-bound readiness; current behavior was extended by staged generation. | descriptor reads staged.generation() ([descriptor](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L555-L562)); handshake records readiness and adds a common_scopes entry for memory.advisory_common.v1 ([handshake](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L564-L667)). | Durable staged row/control generations are exposed as Native state generation. Current scope bindings include exact_coding_scope and checkout_observations because staged rows can be recalled under them ([adapter declaration](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L42-L60)). | Native owner supplies state generation and readiness from original state. Registry/interface owner restores only source-approved recall bindings; the old project_facts/profile_facts list at cc6252 is a provisional floor, not a complete-baseline claim. Remove store-derived common_scopes and checkout attestation unless the source lane proves they belong to original Native. Preserve the explicit per-registration common-profile decision described by [core-interface evidence](./core-interface.md). |
| Pre-staging recall opened the project target, built MemoryApplication, ran its requested fact operation, and built only fact candidates ([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1286-L1381)). | recall_project_memory has a common_recall branch that creates an empty fact page and calls staged.recall_temporal, while ordinary current recall calls canonical fact search and then staged.recall ([recall](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L2001-L2167)); recall_with_runtime passes the store ([runtime](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1968-L1999)). | Common-profile Native can answer from staged rows while skipping canonical facts. Ordinary recall merges canonical facts and staged rows, so the product path has two stores and two candidate classes. A store read failure becomes a Native staged-store-unavailable result. | Native/source owner replaces this with the complete original recall entry point for every proven supported mode. It must not use an empty page as a substitute for missing Native behavior, add a host scorer, or merge staged rows. Preserve original scores, ordering, temporal semantics, provenance, retrieval effects, cancellation, deadline, and failures. If common advisory fields are not part of original Native, registry admission must reject that profile explicitly rather than silently returning zero results. |
| No pre-staging staged scorer exists in the immutable floor. | StagedObservationStore::recall/recall_temporal and recall_filtered scan checkout rows, verify digests, extract message text, and rank with recency plus lexical overlap plus feedback ([store recall](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L700-L974)); both weights are 0.5 ([weights](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L107-L115)). build_native_recall_reply_with_response_bytes merges the classes, applies staged score conversion, tie-breaking, fact reservation, candidate ceilings, and staged-row byte trimming ([merge](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L2212-L2552)); staged candidates use a separate score domain and checkout/session provenance ([projection](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L2672-L2855)). | This is a complete custom ranking/projection path with no source evidence that it is Native. | Native owner deletes the staged scorer, merge comparator, reservation, staged byte trimming, and staged candidate projection. The original algorithm and score domain come from the source lane. Do not tune or rename the staged scorer to make it resemble Native. |
| Current lifecycle implementation is introduced with the staged schema, not established by the pre-staging floor. | All optional adapter methods call lifecycle_call ([delegation](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L738-L768)), which dispatches store control and assembles health/capability responses and receipts ([control/reply](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L3246-L3597)); the store routes Health, Inspection, Feedback, Correction, DeleteBySource, Maintenance, SnapshotExport, SnapshotRestore, and Replay ([store control](../../../../crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs#L3192-L3417)). | The store supplies custom idempotency, generation changes, feedback bias, correction overlays, source-deletion fences, maintenance, snapshots, restore, replay, and response receipts. The old floor returned typed unavailable responses, but that is not proof of complete original Native semantics. | Source/facts owner maps each original lifecycle operation to its real owner and effect policy. Adapter remains a delegating shell. Registry descriptor/capability owner updates declarations only after that map. No operation may be advertised with a staged implementation removed underneath it, and no operation may be made a fake read-only success. |
| Host provenance currently knows that Native can mint staged references. | MountedStagedObservationAttestationStoreV1 recognizes native-staged-observation-v1: ([store](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L1935-L1981)); the host installs it during provenance hydration ([mount](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L2627-L2683)). | A staged candidate remains provider-attested and cannot become host-confirmed evidence, but the host carries a Native-specific attestation route and staged checkout bindings. | Cognitive-recall/host owner removes this Native-specific route once no Native candidate can carry that reference. Keep the generic provider-local attestation abstraction if another provider needs it. This is host provenance cleanup, not permission to turn an original Native candidate into a fabricated record or source citation. |
| Composition currently mounts both Native and NCM and declares the observation journey's Native mount. | Native registration is built in the MemoryProviderKindV1::Native branch and uses provider-state root, authority, NATIVE_RECALL_SCOPE_BINDINGS, and CompositionBound lifecycle ([branch](../../../../crates/tracedecay/src/daemon/project_composition.rs#L380-L444)); retained_owner.rs declares the staged module ([declaration](../../../../crates/tracedecay/src/daemon/retained_owner.rs#L26-L45)). | Native construction, async mount, state namespace, and observation mount all carry assumptions introduced for the staged file. NCM is a separate branch and must remain independent. | Composition owner removes only staged-store construction assumptions after the Native port API is accepted. Preserve disabled/observer/active selection, one owner per provider, NCM construction, lifecycle ownership, and mount ordering. A Native observer must not become an active recall fallback. |
| Current tests and fixtures are remaining call sites rather than additional production stores. | Adapter staged tests ([typed variant](../../../../crates/tracedecay-memory-provider-native/tests/native_adapter.rs#L1255-L1325)); Native staged/store/lifecycle fixtures are grouped in [native_provider_tests.rs](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs#L2230-L3240) and [native_common_factory_tests.rs](../../../../crates/tracedecay/src/daemon/retained_owner/native_common_factory_tests.rs#L1680-L2345); mounted journey expects staged acknowledgement and probes SQLite ([journey](../../../../crates/tracedecay/src/daemon/retained_owner/observation_journey.rs#L8511-L8722)); cognitive recall has a hostile staged fixture ([fixture](../../../../crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs#L6507-L6741)). | These tests assert staged durability, custom ranking, staged provenance, staged lifecycle receipts, and staged DB migration/retention. They are evidence of current behavior, not proof of original Native behavior. | Each test owner replaces assertions with parity or explicit unsupported behavior from the approved source/session decision. Data/migration owner owns old SQLite fixture handling. Do not preserve tests by retaining the substitute, and do not delete host-session tests that verify canonical journal settlement without deciding the Native target. |

## Facts and session interface dependencies

**Canonical facts.** The verified fact path is FactPromotion: the current port
parses the settled fact and dispatches Verify, and its comments state that the
path writes neither a canonical fact nor a staged row
([branch](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L685-L701)).
The pre-staging recall floor opened owner-bound project memory and called
MemoryApplication search/probe/related/reason operations
([immutable source](https://github.com/ScriptedAlchemy/tracedecay/blob/cc6252ac74dd18854f35f9f63dbf972a202814cd/crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L1317-L1381)).
The facts/source lane must establish the complete original Native operation
map, including whether recall records access telemetry or other effects. The
current provider API treats recall as read-only, so an effectful original
recall cannot be restored by changing this adapter alone; the API/fabric owner
must participate as described in [core-interface.md](./core-interface.md).

**Host sessions and structured observations.** The host contract and journal
still settle session.message_committed.v1 before provider delivery. The current
journey documents that Native stages the settled message and expects an
acknowledged provider-local effect
([journey contract](../../../../crates/tracedecay/src/daemon/retained_owner/observation_journey.rs#L43-L57)).
That expectation is a consequence of the substitute, not a Native baseline.
The session lane must choose the replacement observable: a typed Native
unsupported terminal, an original Native direct operation, or routing to a
provider whose contract actually owns session observations. The adapter lane
only enforces that choice; it must not erase the host observation kind, journal,
canonical settlement, or replay position. The same decision covers the
adapter's currently staged source.edit_settled, test.execution_settled, and
feedback.outcome_settled cases when original-source attribution is present.

**Scope and common profile.** The current checkout/session bindings and
common_recall branch exist to serve staged rows. Their removal must be
coordinated with the registry/interface owner. Retain the shared registry's
explicit per-registration profile bit; do not infer common-profile support
from provider_id, and do not make Native common-profile calls succeed with an
empty fact page. The source lane decides which original Native scope bindings
are truthful. The old two-binding list in cc6252 is a useful comparison point
only.

## Exact future write ownership

The owners below are disjoint. A writer must wait for the named dependency
instead of editing a shared file to make an unfinished interface compile.

| Owner | Exact files or narrow module | Responsibility and dependencies |
|---|---|---|
| **Native adapter/interface writer** | crates/tracedecay-memory-provider-native/src/lib.rs; focused crates/tracedecay-memory-provider-native/tests/native_adapter.rs | Remove staged constants, enum variant, parser branch, staged docs, checkout-only scope declaration, and staged test expectations. Keep stable provider identity, zero-contact validation, payload bytes, exact scope, readiness checks, and operation-specific delegation. Depends on source operation/capability matrix and session decision. If complete Native observation API differs, propose the neutral trait change to the interface owner rather than inventing a local JSON bridge. |
| **Native implementation writer** | crates/tracedecay/src/daemon/retained_owner/native_provider.rs and direct original-owner modules identified by source lane | Remove StagedObservationStore field/open, Stage/store Control actor commands, staged observation handler, staged recall branch/scorer/merge/projection, and staged control reply machinery. Wire complete original Native state, recall, fact verification, and lifecycle owners. Depends on pinned complete Native source, facts/effects matrix, accepted adapter trait, and data cutover decision. |
| **Composition writer** | crates/tracedecay/src/daemon/project_composition.rs; only Native construction call sites | Remove provider-state-root/off-runtime arguments that exist solely for staged store and restore correct original construction/activation shape. Preserve authority injection, registration identity/revision, explicit scope bindings, disabled behavior, NCM independence, and mount ordering. Depends on Native implementation constructor and registry/interface decision. |
| **Registry/contract owner** | crates/tracedecay-memory-provider-registry/src/lib.rs reexports and registration metadata; canonical provider contract/generated files only if approved interface requires them | Remove staged symbol reexports and update registered Native capability/scope metadata. If original Native requires a new operation/effect, change canonical API/contract and generated artifacts together. Preserve per-registration common-profile checks and pinned routing. Depends on source/facts/interface decisions; this shared owner must make the edit. |
| **Host/session provenance owner** | crates/tracedecay/src/daemon/retained_owner/observation_journey.rs, cognitive_recall.rs, and focused tests | Reconcile Native session delivery/retry assertions with session decision; remove Native staged-reference attestation and staged-only bindings after no Native candidate can emit them; keep canonical settlement, journal replay, untrusted-memory gating, and generic provider-local provenance. Depends on adapter and Native port decisions. |
| **Data/migration owner** | crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs, data/migration documentation and focused migration fixtures | Decide whether existing staged SQLite state is quarantined, migrated, retained for another owner, or removed under reviewed cutover. No adapter or Native implementation writer may delete, rewrite, inspect operator databases, or silently import staged rows into canonical Native facts. |
| **Verification owner** | Focused Native parity, registry, host journey, and restart fixtures after the above owners land | Compare direct original and modular Native at the same boundary with isolated equivalent state. Exercise effects, failures, ordering, scope, readiness, cancellation, and restart. A test comparing two routes over the same staged backend is insufficient. |

Files that must remain untouched by this adapter report and its bounded worker:
the NCM core/runtime/model/store/worker; canonical host observation contract and
journal schema until the session owner decides otherwise; original Native source
before its revision is pinned; operator databases; and generated artifacts
without a canonical contract change. The staged module is a data-lane surface,
not an invitation for this lane to erase it.

## Observable acceptance checklist

- A Native mount does not create or open a new
  native/staged-observations-v1.sqlite3 store and does not expose staged
  generation. Any persisted state it opens is the original Native state owner
  named by the source lane. Existing staged files remain governed by the data
  lane's cutover decision.
- A settled FactPromotion follows the approved original verification path: it
  has the same canonical owner checks, receipt/effect policy, failures, and no
  staged row or fabricated fact write. A malformed or foreign fact fails before
  Native contact where the adapter owns that check.
- For session.message_committed.v1 and the three currently staged structured
  kinds, the approved session decision is observable at the provider boundary:
  either the adapter returns the typed unsupported terminal before Native
  contact, or the original Native operation receives the exact admitted bytes
  and scope. No path returns StagedSession, a staged reference, or a staged
  success receipt.
- Every supported Native recall objective/mode reaches the original Native
  operation with original query, scope, temporal fields, exclusions, budgets,
  cancellation, and deadline. The reply preserves original candidate order,
  score domain, provenance, output bytes, and any confirmed retrieval effect. A
  missing common-profile feature produces a typed capability result or explicit
  admission refusal, never an empty successful fact page.
- No reply contains native-staged-observation-v1: references,
  memory_class = session_observation, checkout_observations staged attestations,
  or the staged score domain. Canonical fact candidates remain grounded
  through the host's existing record authority.
- Each advertised optional capability reaches its verified original owner and
  reports original state transition, receipt, generation, idempotency,
  cancellation, and failure behavior. A capability with no approved owner is
  absent from the descriptor and refused before dispatch; it is never answered
  by the removed store's receipt helper.
- Native handshake and subsequent calls use source-authoritative
  implementation identity, schema, limits, generation, and recall scope
  bindings. Wrong provider identity, registration revision, exact scope,
  readiness receipt, or generation fails at the same boundary as the original
  route. Native and NCM remain independently selectable; empty or unavailable
  Native output does not activate NCM as an implicit fallback.
- The host still persists and replays canonical session observations according
  to its journal policy. The chosen Native session outcome settles exactly as
  specified, including retry/no-retry classification, without requiring a
  provider-local staged database.
- After restart, original Native state and any original lifecycle effects are
  recoverable with source-defined generation and receipts. The old staged
  SQLite file is checked only by the data/migration owner under isolated
  fixtures, never by importing its rows as Native facts.

No tests or builds were run in this audit, per the research instructions.

## Unknowns and conflicting evidence

1. The complete original Zack-authored Native revision is not yet pinned by
   this lane. cc6252 is the parent of the staged-store commit and proves the
   pre-staging integration floor; b3b43410e47115056f2066449aafa1822bbb6049 has
   no provider-native path in its tree. Neither fact is proof of the complete
   original v2 implementation.
2. The facts lane must resolve whether original recall changes access counters,
   timestamps, learning state, or another provider-local effect. The current
   provider API and fabric treat recall as read-only. The adapter cannot fix a
   mismatch in effect evidence, generation, retry, or restart semantics alone.
3. It is unknown which historical Native retriever objectives are public parity
   requirements. The pre-staging integration maps search, probe, related, and
   reason through retained project memory, but the source lane must distinguish
   that bridge from the complete original Native API. No original objective may
   be silently reduced to search.
4. It is unknown whether original Native owns any current optional lifecycle
   capabilities. The current implementation supplies all of them from the
   staged SQLite store, while the pre-staging floor returned unimplemented
   replies. The source/effects matrix must resolve this rather than choosing
   either staged behavior or a blanket unsupported shortcut.
5. The session owner has not selected the post-staging outcome for
   session.message_committed.v1, source/test settled events, and feedback
   settled events. Until that decision, removing the adapter variant is safe
   only as a proposal; changing the host journal's expected settlement is not.
6. The Native-specific staged provenance attestation is the only production
   consumer found in cognitive_recall.rs, but the generic provider-local
   attestation seam may serve other providers. Its removal must be confirmed
   against final provider candidate contracts.
7. Existing staged database state may matter to a release or operator. Its
   retention, migration, or quarantine is outside the adapter boundary and is
   intentionally left to the data lane.

## Bounded Luna Max implementation assignment proposal

**Goal:** Detach the neutral Native adapter from the staged observation variant
and staged-only scope/capability claims, while preserving provider-neutral
validation and delegating the approved original Native operation surface.

**Prerequisites:** The source lane pins the complete Native implementation and
operation/effect map; the facts lane resolves recall objectives and read-side
effects; the session/interface lanes decide observation shape and
session.message_committed.v1 outcome; the registry owner confirms Native
profile and scope bindings; and the data lane records staged-state cutover.

**Files:** One adapter worker owns
crates/tracedecay-memory-provider-native/src/lib.rs and its focused
tests/native_adapter.rs only. It does not edit the product port,
composition, registry reexports, host journey, cognitive recall, staged
store, contracts, or generated files; those are the owners above.

**Steps:**

1. Re-read the accepted source/interface handoff and record exact Native
   observation variants, capability list, limits, and recall bindings.
2. Remove staged constants, StagedSession classification, staged-only
   documentation, and checkout binding from the adapter; retain exact admitted
   call bytes, scope, operation/request identity, and zero-contact rejection
   behavior.
3. Update adapter fixtures to prove fact/direct dispatch and the selected
   session unsupported-or-direct outcome. Remove tests whose only assertion is
   staged persistence or staged ranking; do not replace them with mock staged
   success.
4. Hand resulting trait/descriptor expectations to Native, composition,
   registry, and session owners. Do not edit their shared files to make the
   adapter compile against an unfinished port.
5. After the build/verification owner schedules checks, run the focused
   adapter suite and approved boundary parity checks. Record actual commands;
   this research report claims none.

**Forbidden shortcuts:** retaining the staged variant behind an unused flag;
mapping session messages into canonical facts; keeping
memory.advisory_common.v1 or checkout bindings solely for staged rows;
returning an empty page; delegating lifecycle methods to a deleted store;
adding a host scorer; fabricating receipts, generations, or provider
references; weakening unsupported assertions; or deleting the staged DB/module
outside the data lane.

**Stop condition:** stop and return the exact missing source, facts, session,
or registry decision if the complete Native operation surface is not pinned.
The worker is complete only when the adapter has no staged symbol or staged
consequence, its focused tests prove the chosen contract, and downstream
Native/composition owners have a reviewable handoff for every removed call.
