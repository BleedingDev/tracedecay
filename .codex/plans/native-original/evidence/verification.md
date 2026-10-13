# Full Native verification and comparison evidence

Audit point: `571daf3a9612e5247443e4da3a107b542686c1ef` in the modular Native
worktree. The direct-original comparison baseline is
`b3b43410e47115056f2066449aafa1822bbb6049`, the exact second parent of that
merge. Root has pinned this as the tree to build for direct-original runs; the
source lane still owns confirming the complete Native caller/operation surface
within it. This is a planning report; no product code, host run, or live data
was changed.

## Short answer

The current Native tests establish that the adapter can route well-formed
requests, stage observations, and produce provider replies. They do not prove
that modular Native preserves the direct Native implementation's full
behavior. The current project port opens a provider-local staged store and a
bounded read actor; observation completion commits staged provider rows and
explicitly does not create a canonical fact. The direct retained
`FactStoreSearch` route in the pinned tree queries canonical facts and, on its
writable path, records retrieval through a separate command. Current
`recall_project_memory` calls built-in `MemoryApplication` read methods and
merges staged rows, but a built-in read query is not automatically the explicit
retained Search boundary. The facts lane must map the original caller and
current provider call before prescribing any effectful provider Recall
behavior. The existing no-authoritative-mutation test documents current
adapter policy; it is not an equivalence result.

Earlier live host captures provide host behavior and delivery evidence, but no
capture proves b3 direct-original versus modular-Native equivalence. The
current runner does resolve [`production_comparison:factory`](../../../../scripts/product/memory-comparison/production_comparison.py#L461-L480);
that factory binds an explicitly supplied executable and artifact root, so its
presence is not by itself a b3 differential run. The private/fixed host
journey also adds baseline context, startup hooks, and extra recalls. Full
Native equivalence and any Native/NCM quality conclusion remain unproven.

## Source-backed gaps

| Direct-original evidence | Current modular evidence | Gap and consequence |
| --- | --- | --- |
| [`DirectRetainedMemoryPortV1`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L140-L346) at the pinned tree dispatches `FactStoreAdd`, `Search`, `Probe`, `Related`, `Reason`, `Contradict`, `Get`, `Update`, `Remove`, `Supersede`, `List`, `Feedback`, and `MemoryStatus`. The immutable source can be inspected with `git show b3b43410e47115056f2066449aafa1822bbb6049:crates/tracedecay-store-runtime/src/retained_memory.rs`. | [`NativeMemoryApplicationPort`](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L247-L301) exposes lifecycle methods at the adapter boundary, while [`ProjectNativeMemoryApplicationPort::new`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L294-L317) constructs a staged store and bounded read actor. | The current trait shape and implementation composition are not proof that each historical operation has the same request mapping, authority, scope, result, receipt, or durable side effect. Build an operation inventory from the pinned tree and callers before calling an operation “covered.” |
| Original [`search_on_db`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L674-L854) queries canonical facts, prepares the retained effect, and on a writable database calls `track_explicit_search`; it then refreshes hits from tracked projections and handles authority, projection, delivery, timeout, and partial-effect outcomes. [`track_explicit_search`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-session-memory/src/memory_tracking.rs#L35-L72) emits a retrieval command for every hit and validates its settlement. | [`recall_project_memory`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L2001-L2167) calls current `MemoryApplication` search/probe/related/reason reads and merges staged rows. [`native_recall_does_not_mutate_authoritative_fact_telemetry_or_history`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs#L2013-L2079) expects no authoritative mutation. | There is no direct-original-vs-modular check for retrieval count/access timestamps, refreshed result projections, mutation receipts, effect state, or failure settlement. The facts lane must first map this provider Recall call to the retained `FactStoreSearch` boundary; built-in `MemoryApplication` reads alone do not establish that mapping. If the boundaries match, compare explicit-search telemetry separately from semantic reads. Do not prescribe an effectful provider Recall before that map is settled. |
| Original [`semantic_search_on_db`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L855-L923) handles `Probe`, `Related`, and `Reason` as distinct semantic requests. `Contradict`, `Get`, `List`, and `MemoryStatus` have separate routes and controls ([same direct port](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L925-L1046)). Read/write control and cancellation/expiry settlement are explicit ([`fact_read_control`](https://github.com/ScriptedAlchemy/tracedecay/blob/b3b43410e47115056f2066449aafa1822bbb6049/crates/tracedecay-store-runtime/src/retained_memory.rs#L1056-L1062)). | Current lifecycle dispatch sends controls to the staged store ([`ProjectNativeMemoryApplicationPort` lifecycle implementation](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L3246-L3423)); [`native_control_reply`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L3427-L3495) wraps staged outcomes in a generic response. The pinned original tree has no `native_staged` row family. | Existing lifecycle tests mostly prove staged-store semantics and generic response construction. They do not demonstrate original canonical update/remove/supersede/feedback/contradiction/list/status/history behavior, or whether cancellation/deadline is settled before dispatch, after durable commit, or after an unknown outcome. Observation parity must use the original session-ingestion/LCM boundary; provider-local staged readback and controls are modular diagnostics, not original requirements. |
| The original direct port is the host-retained operation boundary and opens the selected project/profile target before dispatch. Its operation context and receipt are tied to the canonical owner, scope, and retained effect. | The current state schema is [`native-staged-v2`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L67-L71); observations are intentionally staged and provider-local. The descriptor declares the advisory common profile and optional lifecycle capabilities ([`native_descriptor`](../../../../crates/tracedecay/src/daemon/retained_owner/native_provider.rs#L785-L829)). | Tests do not compare the selected canonical rows/projections, relevant lineage/events, receipts/effect settlement, provider-local state where its contract exposes it, or reopened public state between trees. A provider-boundary pass is consequently compatible with lost original data and receipt semantics. Do not compare physical SQLite bytes or require a `native_staged` state in the original tree. |
| The original path executed through the actual retained host port, including project/profile selection and host admission. | Current [`NativeProvider` parity/adapter tests](../../../../crates/tracedecay-memory-provider-native/tests/native_adapter.rs#L479-L1477) use mock ports for identity, descriptor, handshake, malformed envelopes, payloads, routing, optional unsupported operations, and observation classification. The product Native tests cover health, inspection, queue, observation, recall, staged, and lifecycle cases. | These are valuable conformance and adapter-boundary checks, but a mock `NativeMemoryApplicationPort` cannot prove direct-original behavior or actual host delivery. Add a harness that runs both concrete implementations against equivalent isolated stores and a real host transcript. |
| The original host emitted the final retained result through the real integration path. | The host-comparison capture contract defines source attribution and exact final delivery ([`README.md`](../../../../product/evaluation/host-comparison/README.md#L74-L155)). The current [`production_comparison:factory`](../../../../scripts/product/memory-comparison/production_comparison.py#L461-L480) binds an explicitly supplied executable and artifact root; it does not choose a baseline or provider. Earlier live host captures exist, while the fixed journey's `assert_host_memory_journey_with_provider` adds baseline context, startup hooks, and multiple verification recalls ([`README.md`](../../../../product/evaluation/host-comparison/README.md#L257-L268)). | Existing captures provide host delivery evidence, but no b3 direct-original versus modular-Native differential proof. A fixed journey with extra setup reads or recalls is not an unbiased differential comparison. |

## Falsifiable baseline protocol

The comparison must invoke the pinned direct-original tree and the modular
Native tree as separate process groups with separate stores, state roots,
target folders, and capture directories. Keep the logical fixture equal: the
same owner/project/worktree, exact scope and scope digest, canonical seed
facts, source identity, actor, request/call/operation IDs, control IDs, logical
clock inputs, deadlines, cancellation schedule, and host prompt/action. Do not
copy a post-run database from one implementation into the other. Keep the b3
source tree read-only; the differential harness and fixtures live in the
comparison/evaluation surface outside it. Record each tree's revision,
features, configuration/schema identity, binary digest, and provider descriptor
before execution.

First derive a capability and caller inventory from the pinned direct tree.
Only compare operations that are actually exposed at the same boundary; record
historical operations with no current route as typed unsupported until the API,
facts, and source owners decide otherwise. For each selected operation capture
only the observables relevant to its documented behavior:

- the exact request envelope and canonicalized request digest;
- terminal status/code, diagnostic classification, provider identity and
  revision, readiness receipt, state generation, payload bytes and digest;
- before/after selected canonical rows/projections, retrieval/access counters
  when the mapped operation records them, relevant feedback/correction state,
  lineage/events, receipts, effect settlement, and generation;
- provider-local state only where that operation's contract exposes it, and
  session/LCM state for observation/session cases;
- process, queue, retry, cancellation, deadline, and cleanup evidence where
  those are part of the case; and
- the selected public state again after a clean close and reopen.

The harness must not compare physical SQLite bytes, WAL/SHM files, or an
unreviewed full index dump as an equivalence criterion. A selected derived
projection is evidence only when the operation or contract makes it
observable.

Canonicalize only fields covered by a reviewed schema. Preserve raw payloads and
source refs so that a normalization cannot hide a changed score, order,
provenance, receipt, timestamp policy, or omission. Compare committed effects,
rejected effects, unknown outcomes, and partial/expiry outcomes as separate
classes. A pass requires equal documented behavior at the selected boundary,
not merely equal final hit text.

If the facts lane maps the provider Recall call to the original retained
`FactStoreSearch` boundary, assert the original writable search's retrieval
mutation, refreshed projections, receipt/state-generation transition, and
retry/reopen settlement against modular Native. For `probe`, `related`, and
`reason`, assert whatever side effect the original actually exhibits and keep a
no-mutation expectation only when the direct run proves it. For
add/update/remove/supersede, feedback, contradiction, list/get/status, history,
and feedback history, compare canonical rows and documented projections as
well as public results. Exercise replay, conflict, idempotency, cancellation,
and deadline around both pre-dispatch and post-durable-commit points. If the
direct source has no callable route for a case, retain that as an explicit
unsupported coverage result rather than inventing a modular expectation.

The host leg must be a real scheduled Claude/Codex transcript or equivalent
production entry point. It must use the actual hooks, mount/admission,
provider-selection, hydration, final renderer, and durable `Capture`. Schedule
one action per case; do not inject a host action or add fixed baseline calls.
Compare exact final UTF-8 and attribution/source references, including
withheld/refused/deadline results, and bind every emitted section to its
canonical operation ID. Restart the old process between cases where the host
owns cleanup, and prove that no process, queue, or state from one lane serves
the other. The host comparison can compare direct-original Native with modular
Native only after canonical fixture and host policy are shown equal.

## Representative checks

Use a small set of meaningful cases, each run on both trees and reopened before
its final assertion:

1. **Canonical fact lifecycle:** add a fact, update it, supersede or remove it,
   then replay the same command. Check validation, idempotency, canonical and
   documented derived projections, lineage, receipts, and reopened state.
2. **Explicit search boundary:** first use the facts mapping to establish
   whether the provider call is the retained `FactStoreSearch` operation. If it
   is, seed multiple matching facts, including a score tie and pagination
   cursor. Check provider order/score/provenance, empty results,
   retrieval/access telemetry, refreshed hits, effect receipt, generation, and
   replay. Run read-only and writable modes if the original exposes both. If it
   is not the same boundary, record the route as an unresolved coverage gap;
   do not infer an effectful Recall from the built-in read query.
3. **Semantic reads:** run probe, related, and reason with temporal/scope/source
   constraints. Compare each operation's payload and its proven side effects;
   ensure they are not silently mapped to explicit search.
4. **Fact reads and status:** get, list, contradiction, memory status/inspection,
   history, and feedback history with an absent ID, foreign scope, and a valid
   record. Compare typed errors, filtering, order, attribution, and telemetry.
5. **Feedback and correction:** submit feedback and any original correction or
   deletion path, then query affected facts and reopen. Preserve the original
   authority and durable receipt semantics.
6. **Controls and failure settlement:** cancel or expire before dispatch, during
   bounded work, and after durable commit; inject conflict, projection,
   delivery, and malformed-state failures where the direct harness exposes them.
   Distinguish committed-but-unknown from not committed.
7. **Scope and restart:** exercise project/worktree/profile mismatch, unrelated
   scope, corrupt or stale state, clean restart, and reopened reads. Preserve
   namespace, owner, generation, and rejection diagnostics.
8. **Observation/session boundary:** use the original session-ingestion and LCM
   entry points for start/observe, then compare canonical promotion/LCM effects
   and the corresponding modular host behavior. Record provider-local staged
   rows or controls only when the modular contract exposes them; the original
   tree has no `native_staged` row family, so staged readback is not an original
   parity requirement.
9. **Real final delivery:** execute one scheduled host action for a selected
   recalled fact and one withheld/refused/deadline case. Compare exact final
   output bytes, source attribution, admission, hook evidence, and cleanup.

The harness should report an operation-level result with `pass`, `fail`,
`unsupported`, `invalid`, `censored`, or `unknown`, plus raw evidence. Do not
turn invalid fixtures, unavailable direct routes, or censored host runs into
successes. Keep them in the denominator and publish why they cannot support a
comparative conclusion.

## Honest Native/NCM evaluation

Native equivalence and Native/NCM usefulness are separate gates. Run the four
lanes the current runner defines ([`LANES`](../../../../scripts/product/memory-comparison/runner.py#L24-L33)):

- `provider:tracedecay.native`: Native selected with NCM disabled;
- `provider:ncm`: NCM selected with Native disabled;
- `no_memory`: no provider call and no admitted memory; and
- `explicit_documentation`: only current fixture documentation is admitted.

The capture contract requires `observer_enabled: false` for these primary
lanes. The fixed host test's optional NCM observer is outside this four-lane
decision and its output is not an adjudication input.

Keep canonical facts, session transcript, host task, scope, and selection policy
equal. Measure retrieval usefulness, safety/withholding, source fidelity,
exact final tokens/bytes, latency/queue behavior, and failure/cleanup rates.
Report Native-direct-original-vs-modular parity separately from provider
conformance and from NCM quality. Any optional observer output from the fixed
host test remains outside this four-lane comparison and cannot support an
isolated NCM-vs-Native claim. Do not use a common-profile or adapter-conformance
pass as evidence that Native is equivalent to its original host implementation.
Exclude invalid, unsupported, unknown, and censored cases from comparative
point estimates while retaining their counts and reasons.

The existing evaluation artifacts are useful reporting primitives, not results:
`MetricReport` separates safety from unresolved cases, and the host-comparison
capture schema preserves timing/process/final-delivery evidence. The factory's
external executable/artifact configuration and repeated differential runs are
still prerequisites for an honest Native/NCM result. Aggregate repeated runs
before describing a journey as stable; one passing transcript is insufficient.

## Future verification ownership

| Owner | Bounded files/surface | Responsibility and dependency |
| --- | --- | --- |
| Root orchestration/review owner | Lane schedule, pinned revisions, capture schema, adjudication output | Schedule the exact direct-original and modular runs, review fixture/capture output, and make the final parity decision. Root does not author the differential harness or fixtures and does not modify the read-only b3 tree. |
| Luna execution owner | External differential harness, fixtures, and invocation adapter | Build the separately compiled b3 runtime and modular runtime, invoke each through the same selected boundary, and capture expected canonical side effects, receipts/generation, and supported failure injection. Work stays outside the historical source tree and depends on source and facts reports. |
| Modular Native owner | `crates/tracedecay/src/daemon/retained_owner/native_provider.rs`, `native_provider_tests.rs`, and adapter boundary tests | Implement and test only the behaviors proven required by the direct baseline, including any retrieval telemetry or lifecycle semantics. Keep staged observation policy explicit and report provider-local versus canonical effects. |
| Host/orchestration owner | `crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs`, the real host entry point, and capture integration | Supply a callable production journey with one scheduled action, actual hooks/mount/admission/hydration/final renderer, exact final bytes, attribution, process cleanup, and repeated-run aggregation. Remove fixed/private-only assumptions only within the entry-point slice. |
| Conformance/evaluation owner | `crates/tracedecay-memory-conformance/**`, `scripts/product/memory-comparison/**`, `product/evaluation/**` | Maintain provider-neutral envelope/descriptor/terminal checks and report classifications. Add the direct-original differential runner and metric projection only after source operation/effect identities are pinned. |
| NCM lane owner | NCM adapter/registration and NCM-specific comparison capture | Run the `provider:ncm` lane and preserve NCM's independent namespace, worker, readiness, attribution, and selection. Do not treat optional observer output as a comparison lane or alter NCM internals to make Native parity pass. |

Keep edits disjoint: historical/direct harness work, modular Native/staged
work, registry/API contract work, host delivery, conformance/evaluation, and NCM
selection each have one owner. Shared operation or effect contracts require
root arbitration before either implementation changes. No owner should modify
project composition, cognitive recall, or the fixed host journey merely to
make an unexecuted comparison look successful.

## Dependencies and unresolved facts

1. **Source pin:** root has pinned `b3b43410e47115056f2066449aafa1822bbb6049` as the exact second parent of the modular merge. The source lane must confirm its complete direct-original Native caller/operation surface and the Luna execution owner must build the actual b3 runtime for comparison. The historical tree remains read-only; an unknown route is a coverage gap, never completed Native parity.
2. **Facts/effects matrix:** the facts lane must map every original public operation to canonical tables/projections, telemetry, receipts, effect state, generation, retry, and failure settlement. It must map original retained `FactStoreSearch` callers to the current provider Recall boundary before deciding whether writable retrieval tracking is required there, and distinguish that route from built-in `MemoryApplication` semantic reads.
3. **Sessions/LCM mapping:** the sessions lane must identify original session ingestion, startup/history, lifecycle, canonical promotion, and LCM effects. It must keep the original session boundary distinct from current provider-local staging: the b3 tree has no `native_staged` row family.
4. **API/registry decision:** the interface lane must decide how an effectful recall or any additional historical operation is represented by the provider API, registry, terminal validation, readiness, and retry rules. Unsupported original operations must remain typed until this is settled.
5. **Adapter/delivery entry point:** the adapter and host lanes must define the current concrete boundary, the real scheduled action entry point, and exact final-delivery/source-attribution evidence. The current mock adapter tests and fixed journey cannot supply those alone.
6. **NCM attribution:** the NCM lane must establish active-provider attribution for the `provider:ncm` lane. Optional observer output from the fixed host test is outside the runner's four lanes and cannot be used as an isolated quality result.
7. **Environment and persistence:** the comparison owner must pin database/schema/configuration versions, feature flags, process ownership, cleanup, and reopen procedure. No live persisted migration or one-off local run may be used to infer parity.

No full-equivalence claim is valid until these dependencies are resolved and the
representative cases pass against both the direct-original and modular trees,
including side effects and real final delivery.
