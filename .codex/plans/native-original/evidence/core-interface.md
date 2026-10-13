# Core provider interface and routing evidence

Audited revision: `571daf3a9612e5247443e4da3a107b542686c1ef` in
`/Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2`.
The restoration baseline is the already integrated upstream revision
`b3b43410e47115056f2066449aafa1822bbb6049` (the second parent of the current
head). The older `1caf016e57` rows are retained only as historical context for
the stale ADR conflict; they are not current b3 behavior or a current blocker.

## Short answer

The current core already has the extension point needed to keep Native out of
the common staged-observation completion profile. `compose_registered` passes
`require_common_profile = false` for Native and `true` for an injected active
provider; registration metadata retains that decision. The recall port reads
that per-registration bit, asks Native for `recall.query.v1` only, and adds
`memory.advisory_common.v1` only for a registration that opted in. Active
routing also pins provider identity and revision, performs a fresh handshake,
and does not infer fallback from an empty result. These paths should be
preserved rather than replaced with provider-name checks or an unconditional
common-profile check.

There is no confirmed core blocker from the older implementation. The b3
baseline uses `FactReadControl` for query reads and a separate
`ProjectMemoryFactRetrievalCommandV1` through `MemoryApplication` for explicit
retrieval telemetry. That separation is compatible with a read-only provider
`Recall` until the facts lane proves that Native's provider boundary itself
owns an effect. The b3 operation/caller map is still needed before changing
the current recall objective or operation shape; old method names alone do not
justify a new capability or protocol.

## Evidence

| Original revision/path/symbol | Current path/symbol | Original behavior | Difference or integration requirement |
| --- | --- | --- | --- |
| **Historical only:** `1caf016e57:crates/tracedecay-runtime-core/src/memory/retrieval.rs`, `FactRetriever::search`, lines 164-170; `memory/store/queries.rs`, `MemoryStore::record_fact_recalls`, lines 234-259; `memory/types.rs`, `FactRecord`, lines 127-149. `search` records access telemetry for returned IDs. This row explains why the old read-only ADR must be checked, but it is not evidence about b3. | [`ProviderOperation` and `mutates_provider_state`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L739-L819); [`TerminalRecord::new`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L2119-L2178); [`effect_unknown_for_call`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L2389-L2430) | The old implementation coupled a query with a fire-and-forget provider-store update. | Do not use this old coupling to declare a current incompatibility. The b3 baseline has a separate retrieval command (next row). The current read-only `Recall` policy remains authoritative unless the b3 facts map proves that Native provider execution itself must carry an effect. |
| `b3b43410e47115056f2066449aafa1822bbb6049`: `crates/tracedecay-session-memory/src/fact_store/search.rs`, `search_project_memory_facts`, lines 500-515, uses `FactReadControl`; `crates/tracedecay-session-memory/src/memory_tracking.rs`, `track_explicit_search`, lines 34-72, builds `ProjectMemoryFactRetrievalCommandV1` and calls `MemoryApplication::record_project_memory_fact_retrieval`; the store trait declares the command at `crates/tracedecay-store/src/memory/traits.rs` lines 260-264. | [`ProviderOperation::Recall`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L739-L805), [`RecallCallPlan::provider_call`](../../../../crates/tracedecay-memory-provider-registry/src/recall_port.rs#L1240-L1302), and [`MemoryProvider`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L2922-L2932) | b3 separates a cancellable query read from an explicit, owner-bound retrieval-recording command. The query itself does not prove a provider effect. | This is the current baseline to reconcile with the Native facts map. If Native parity requires the separate retrieval command to run after a provider reply, decide at the application/provider ownership boundary whether it remains host-owned or is part of Native's provider-local behavior. Do not change terminal effects, generations, or operation identities until that boundary is proven. |
| **Historical only:** `1caf016e57:crates/tracedecay-runtime-core/src/memory/retrieval.rs`, `FactRetriever::{search, probe, related, reason, contradict}` at lines 37-43, 175-188, 226-232, and 315-320. | [`ProviderOperation`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L739-L805), [`RecallCallPlan::provider_call`](../../../../crates/tracedecay-memory-provider-registry/src/recall_port.rs#L1240-L1302), and the Native application port [`NativeMemoryApplicationPort`](../../../../crates/tracedecay-memory-provider-native/src/lib.rs#L239-L301) | The old retriever had several methods with distinct query shapes. | b3's current application/store surface and actual Native callers decide whether those distinctions cross the provider boundary. Reuse the current canonical recall operation and change the unreleased contract in place only when the b3 behavior proves a distinction is required. Do not add versioned capabilities or protocol bumps solely because these old methods exist, and do not silently change a proven operation into `search`. |
| **Historical only:** The `1caf016e57` `FactRetriever` returns its own scored order from `search` (retrieval.rs lines 127-162) and carries native fields in `FactSearchResult`. | [`provider-recall-contract.md`](../../../../product/contracts/memory-provider-v1/provider-recall-contract.md#L74-L80) and [`ProviderReply`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L2595-L2678) | The old provider score and order are context for the score-ownership rule; this row is not a b3 implementation claim. | Keep whatever b3-proven Native payload, score-domain evidence, provenance, and provider order the facts lane identifies at the provider boundary. Any host normalization or selection remains a downstream explicit projection with an explain trace. This audit does not authorize changing Native ranking or adding a new scorer. |
| — | [`ProjectMemoryProviderComposition::compose_registered`](../../../../crates/tracedecay-memory-provider-registry/src/lib.rs#L541-L599), [`compose_provider_set`](../../../../crates/tracedecay-memory-provider-registry/src/lib.rs#L880-L927), and [`register_selected`](../../../../crates/tracedecay-memory-provider-registry/src/lib.rs#L1078-L1149) | — | Native is already composed with the legacy profile flag false; injected active providers require the common profile. All active registrations still require the mandatory health, observation, and recall capabilities and a non-empty recorded recall-scope binding. No unconditional profile change is needed. |
| — | [`ProjectCognitiveRecallPortV1::mount`](../../../../crates/tracedecay-memory-provider-registry/src/recall_port.rs#L464-L514), [`recall_uncounted`](../../../../crates/tracedecay-memory-provider-registry/src/recall_port.rs#L629-L730), and [`RecallCallPlan`](../../../../crates/tracedecay-memory-provider-registry/src/recall_port.rs#L1162-L1302) | — | The port snapshots `(registration_revision, requires_common_advisory_profile)` from the registry. It adds the common capability only when that recorded bit is true, and admission receives the same bit. Native can therefore use the legacy recall contract while NCM or an injected provider can opt into the common profile. Keep this lookup bound to provider identity and revision; never infer it from a name, descriptor claim, sidecar, or history grant. |
| — | [`MemoryFabric::route_active`](../../../../crates/tracedecay-memory-fabric/src/routing.rs#L831-L990), [`dispatch_fallback`](../../../../crates/tracedecay-memory-fabric/src/routing.rs#L995-L1056), and [`contact_target`](../../../../crates/tracedecay-memory-fabric/src/routing.rs#L1087-L1132) | — | The configured target must be registered at the pinned revision, active, and capable before contact. A fresh handshake precedes each call. Fallback is forbidden by default and is dispatched only for a matching explicit host pin to another registered active target; empty or successful results do not trigger it. Preserve provider identity, call identity, scope, readiness receipt, and state generation across the route. |
| — | [`MemoryFabric::handshake`](../../../../crates/tracedecay-memory-fabric/src/lib.rs#L709-L829), [`settle_readiness`](../../../../crates/tracedecay-memory-fabric/src/lib.rs#L1019-L1171), and [`validate_terminal`](../../../../crates/tracedecay-memory-fabric/src/lib.rs#L1309-L1397) | — | Fabric validates the terminal again after provider dispatch. It requires the effect's `state_generation_before` to equal the call's expected generation and the effect's `state_generation_after` to match `ProviderReply.state_generation`. A reported after-generation different from the expected generation updates the accepted generation and clears readiness. | If a confirmed Native read mutates provider-local state, the selected API semantics must be implemented here as well; changing only the adapter would fail fabric validation or lose the transition. The readiness consequence must be deliberate and tested. |
| — | [`MemoryProvider`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L2922-L2932) and [`ProviderDescriptor::validate`](../../../../crates/tracedecay-memory-provider-api/src/lib.rs#L911-L1018) | — | The object-safe boundary is already descriptor/handshake/invoke. Descriptors must always declare health, observation, and recall; the common profile is an explicit additional marker. This supports a thin Native boundary without making that boundary the Native implementation. |

Historical immutable references for the historical row:

```text
git show 1caf016e57:crates/tracedecay-runtime-core/src/memory/retrieval.rs
git show 1caf016e57:crates/tracedecay-runtime-core/src/memory/store/queries.rs
git show 1caf016e57:crates/tracedecay-runtime-core/src/memory/types.rs
```

## Proposed interface semantics

1. Registration keeps two explicit compatibility levels. Every provider uses
   the legacy mandatory health/observe/recall contract. A provider advertises
   the common advisory profile only when its registration and descriptor both
   say so. Native remains a legacy registration with its recorded Native scope
   bindings; NCM or an injected provider is not made Native-compatible by
   inference.
2. Every active call remains bound to the selected provider ID, accepted
   revision, exact scope, ready receipt, expected generation, live deadline,
   and cancellation. The router does not select a provider because another
   provider returned zero candidates or became unavailable.
3. The current b3 split remains the default: query reads use
   `FactReadControl`, while explicit retrieval recording uses
   `ProjectMemoryFactRetrievalCommandV1`. Change provider terminal effects only
   if the b3 facts map proves that Native provider execution owns a different
   effect. If it does, the API contract must define how that effect is evidenced
   and settled across retries, cancellation, and restart; do not infer it from
   the older `1caf016e57` implementation.
4. The provider payload must retain Native's native score and provenance. Core
   routing and admission must not substitute a host scorer or rewrite the
   provider's operation identity. Host normalization can remain an explicit
   context projection where the contract permits it.
5. The current canonical `Recall` operation and payload remain the default.
   Change the unreleased contract in place only if the b3 caller map proves
   that a distinct operation crosses the provider boundary. Unsupported
   behavior returns a typed capability result; it is not silently converted to
   `search`. Old method names alone do not justify a new capability or protocol
   bump.

## Exact write ownership and dependencies

| Future owner | Shared files or narrow modules | Work and dependency |
| --- | --- | --- |
| API owner | `crates/tracedecay-memory-provider-api/src/lib.rs` and its focused API tests | If facts confirm read-side effects, define the operation-specific effect policy, evidence constructors, generation and retry rules. If facts confirm additional public operations, add their exact operation/capability/payload identities. Depends on the source/facts operation matrix. |
| Fabric/routing owner | `crates/tracedecay-memory-fabric/src/lib.rs`, `src/routing.rs`, and focused fabric tests | Enforce the API decision at handshake, terminal validation, generation settlement, readiness invalidation, and pinned routing. Preserve explicit fallback rules and per-target identity. Depends on the API contract; no change is justified for profile selection alone. |
| Registry/recall owner | `crates/tracedecay-memory-provider-registry/src/lib.rs`, `src/recall_port.rs`, and focused registry/recall tests | Keep the per-registration profile bit and recorded scope bindings. If new proven operations exist, build their call plans and required capabilities from registration metadata. Do not infer profile or scope from provider names. Depends on API/contract identities and facts caller mapping. |
| Contract/generated-artifact owner | `product/contracts/memory-provider-v1/**` and the generated API artifacts | Update canonical JSON, terminal/effect policy, capability catalog, and generated projections together only if the b3 facts map proves a core contract gap. No hand-edited generated output without the canonical contract change. |
| Native facts/implementation owner | `crates/tracedecay-memory-provider-native/**` and the original Native application port | Establish the complete operation/effect/caller matrix and implement the proven Native behavior. This report does not authorize edits in that tree and does not treat the current adapter as proof of parity. |
| Root/application owner | Composition and product call sites outside the files above | Wire only explicitly selected operations and preserve Native ownership. Resolve conflicts between the source facts and historical ADRs before assigning API changes. |

Files that must stay untouched by this core-interface change: the Native
backend implementation and adapter behavior until the facts lane provides its
operation/effect matrix; application ranking algorithms; generated artifacts
without their canonical source update; and any staged-observation policy that
would turn Native into the common profile.

## Behavioral acceptance checklist

- A Native descriptor with only the legacy mandatory capabilities mounts,
  handshakes, and routes a recall call successfully; it is not rejected for
  lacking `memory.advisory_common.v1`. An injected active provider that is
  marked as requiring the common profile is rejected until that profile is
  complete.
- The handshake and recall call for Native carry `recall.query.v1` and omit
  the common-profile capability; the same call-plan inspection for an opted-in
  registration carries both. The result is unchanged if provider display name,
  process identity, or sidecar metadata changes.
- A configured target with a wrong revision, observer mode, or missing recall
  capability is refused before provider contact. A fresh handshake binds the
  call to the target's own instance, receipt, scope, and generation.
- Default routing returns the original provider terminal on unavailable,
  partial, or empty outcomes and records a typed fallback decision. A fallback
  call occurs only for the exact host-pinned policy and a separately admitted
  active target.
- Native candidates preserve provider operation identity, native score-domain
  evidence, provenance, content digest, and provider order at the provider
  boundary. Host normalization, if used, remains explainable and does not
  introduce a Native-specific replacement scorer.
- A b3 parity fixture verifies that query execution receives its live
  `FactReadControl` and that any explicit retrieval recording uses the
  owner-bound `ProjectMemoryFactRetrievalCommandV1` path. It must not add a
  provider terminal effect or change readiness/generation until the facts lane
  proves that Native provider execution owns that behavior; once ownership is
  proven, the corresponding success, zero-result, cancellation, deadline, and
  restart observations are tested.
- If additional historical operations are confirmed, each operation reaches
  its matching Native entry point with its original payload and produces a
  typed unsupported result when the capability is absent. A probe, related,
  reason, or contradiction request must never be observed as a search request.

No tests or builds were run, per the research instructions.

## Unknowns and conflicts

- `b3b43410e47115056f2066449aafa1822bbb6049` is the target baseline. Its
  `FactReadControl` query path and separate retrieval command must be mapped to
  actual Native callers before any core change is called necessary.
- The `1caf016e57` access-counter coupling is historical evidence only. It
  explains the stale ADR conflict, but must not be promoted to a b3 blocker.
- It is unresolved whether the b3 retrieval-recording command is host-owned
  for Native, provider-local Native behavior, or different across application
  recall and direct tool orchestration. The read-only ADR rule is not adopted
  as Native policy, but no effect change is proposed without b3 evidence.
- It is unresolved which old retriever methods, if any, cross the current
  provider boundary. Reuse current canonical operations until the b3 caller map
  proves otherwise.
- It is unresolved whether host candidate normalization is allowed to change
  the order visible to Native callers. The core must retain Native order and
  score evidence until the host/facts acceptance surface resolves this.

## Bounded Luna Max implementation assignment (proposal)

**Goal:** implement only the core changes proved necessary by the source/facts
operation matrix, while preserving Native's original behavior and the existing
per-registration profile split.

**Prerequisites:** the b3 operation-to-caller map, the ownership decision for
`FactReadControl` and `ProjectMemoryFactRetrievalCommandV1`, and canonical
contract decisions. Each downstream worker owns only its scope:

- **API/contract worker:** `crates/tracedecay-memory-provider-api/src/lib.rs`
  and focused API tests, plus canonical files under
  `product/contracts/memory-provider-v1/**` and their generated projections
  when required. Encode only a b3-proven operation/effect rule, reusing the
  current canonical operation where possible. Stop after publishing the
  contract decision and fixtures for the fabric and registry workers.
- **Fabric worker:** `crates/tracedecay-memory-fabric/src/lib.rs`,
  `src/routing.rs`, and focused fabric tests. After the API/contract worker
  publishes a decision, enforce it at terminal validation, generation
  settlement, readiness invalidation, and pinned routing. If the API decision
  is unchanged, add only the behavioral coverage needed to prove that.
- **Registry worker:** `crates/tracedecay-memory-provider-registry/src/lib.rs`,
  `src/recall_port.rs`, and focused registry/recall tests. Preserve the
  registration profile bit and recorded scope bindings. Add route-plan changes
  only for b3-proven current operations and only after the API/contract
  identities are available.

Native implementation files are owned by the Native implementation lane and
are not edited by any of these assignments.

**Steps:** API/contract first, then fabric and registry changes in parallel
only where their dependency is satisfied; finally run the targeted behavioral
fixtures for identity, scope, effects, ordering, failures, and restart.

**Forbidden shortcuts:** enabling the common profile for Native; provider-name
inference; mapping every original method to `search`; adding a host ranking
algorithm; synthesizing receipts, IDs, generations, or migration evidence;
using empty results as fallback; or changing read mutation rules before the
facts lane resolves the conflict.

**Stop condition:** stop and report the unresolved dependency if the facts
matrix cannot prove an operation or effect boundary. Finish only when the
targeted parity fixtures pass and the existing Native legacy route still
avoids the common profile.
