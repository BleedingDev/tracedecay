# Memory provider contract V1

`tdmem-0201` establishes the provider-neutral capability registry used by every later provider contract. It defines identity and semantics only; no Native, NCM, or OCEAN adapter is implemented here.

## Stable identities

A provider uses stable `MemoryProviderIdV1` identity. Display names, process IDs, sockets, database paths, configuration order, and state digests are never identity. Capability IDs are versioned behavior names, not provider names. Only the registry/composition boundary may branch on `provider_id`; CLI, MCP, SDK, dashboard, context, storage, and application surfaces remain provider-neutral.

## Mandatory versus optional

The [common advisory profile](common-advisory-profile.md) is an explicit opt-in
completion profile layered on the legacy minimum below. Its canonical marker is
`memory.advisory_common.v1`; claiming it requires every shared lifecycle operation
and all temporal modes. It does not activate a provider or change legacy defaults.

The authoritative registry has two non-overlapping sets:

- Mandatory: `provider.health.v1`, `observation.accept.v1`, and `recall.query.v1`. A registered provider cannot become ready without all three.
- Optional: feedback, maintenance, temporal recall, associative activation, explicit-fact projection, explainability, correction, source deletion, snapshot export/restore, replay, and inspection.

Every entry carries canonical input and output contract identities, bounded typed failure modes, and explicit compatibility rules. Exact capability major versions are required; unknown optional fields are preserved; implicit downgrade is forbidden; behavior activates only after a known catalog entry, an accepted registration revision, and explicit selection.

For compatibility with already-landed M1 validators, `capability_catalog` is a derived ID-only projection of both authoritative sets. It is not an activation source; `capability_registry` remains authoritative.

## Unknown capability round-trip

A syntactically valid unknown capability is decoded as `OpaqueMemoryProviderCapabilityV1` with its canonical payload preserved. Re-encoding must round-trip that payload without semantic rewriting. Presence never means support: the declaration is retained as opaque, cannot count as mandatory, cannot satisfy a required capability, cannot infer behavior from its name, and cannot activate anything. Explicit selection returns typed `capability_unsupported`. Promotion requires a future accepted catalog revision, a new registration revision, and explicit selection.

## Fail-closed resolution

A registration records `recall_scope_bindings`: `exact_coding_scope`, `checkout_observations`, `project_facts`, and `profile_facts`. Providers cannot self-declare authorization; admission receives it with the admitted call. The selected Native candidate is declared for all four: facts retain their project/profile owner binding, while `checkout_observations` is a provider-local staged candidate projection requiring the same profile, project, repository, worktree and branch, with empty candidate session/digest fields. That staged projection is not canonical session/LCM authority; original session and scope digests remain provenance on the immutable exact-scope row. NCM remains authorized only for `exact_coding_scope`. Request/response scope and host citation authority remain exact.

Resolution requires exact provider identity, accepted registration revision, compatible adapter contract, all mandatory capabilities, every explicitly required known capability, exact TraceDecay scope, a live deadline, and live cancellation. There is no implicit fallback and no successful empty resolution.

## Canonical Native context delivery

Automatic model context uses the canonical `memory_matches` route at the host
boundary. The host/composition owner executes that route once, then may pass a
`NativeContextDeliveryMarker` from the provider API to the context compiler.
The marker is bound to the selected provider identity, accepted registration
revision, exact scope digest, canonical request digest, and canonical
contribution digest. The compiler verifies those bindings before accepting the
already-delivered contribution against the trusted host/composition decision,
so it does not issue a second Native query. The constructor and private fields
only assemble data; they do not grant authorization.

This marker is host registration/composition metadata, not a provider result or
wire capability. Provider payloads, descriptors, display names, and provider
local receipts cannot create or widen it. A generic provider `Recall` remains a
read-only advisory operation. Explicit retained fact `Search` continues to use
the original owner-bound retrieval recording command and its retrieval receipt
when that direct search returns hits; that telemetry is not added to automatic
context reads. No new generic operation or capability is implied by the
once-delivered route.

## Original Native application routes

The generic provider operation and capability catalogs do not replace the
typed routes in the complete upstream 570 Native surface. The public
[Native memory surface map](../../architecture/native-memory-surface-map.md)
anchors the owner and effect decisions; an internal parity handoff records
the complete caller, receipt, and current-composition evidence. This contract
does not depend on that internal planning artifact.
The boundary decisions are:

- `fact_store_curate` is the retained administrative application operation.
  It is a canonical mutation (`canonical_mutation=true`). The current retained
  curator performs pre-mutation automation admission reservation, applies
  accepted reviewed mutations through the Native explicit-fact authority, and
  then settles the outer automation effect-ledger and terminal run receipt
  before reporting success. Any downstream Native composition must preserve
  that reservation → mutation → outer-settlement order. It is distinct from
  the lower-level curation transaction.
- Direct retained `fact_store_search` preserves nonempty retrieval telemetry
  and its owner-bound receipt. Probe and reason remain read-only; related uses
  the retained `RecordRetrieval` access lease for inline derived-graph
  reconciliation, but its semantic query does not call explicit retrieval
  tracking and does not promise the Search receipt. Automatic context remains
  read-only. Generic `Recall` does not stand in for any of these owner-bound
  Native routes.
- `session_lookup` uses
  `DaemonSessionLookupPrimitiveV1` →
  `SessionApplicationRetrievalPortV1::retrieve_admitted`; it is distinct from
  retained `MessageSearch`. `SessionsFor` and `Workflows` are project-only
  routes, with profile authority returning typed unsupported.
- `recent_sessions`, `session_providers`, and `session_replay_slice` remain
  internal `RegisteredGlobalDb`/`SessionStoreAccess` queries because they have
  no `RetainedLcmRequestV1` or `DirectRetainedLcmPortV1` binding. `lcm_compress`
  and its lifecycle helpers remain daemon-internal mounted-authority work;
  no public alias is added.

Provider-local `ProviderOperation::SnapshotExport`,
`ProviderOperation::SnapshotRestore`, `ProviderOperation::DeleteBySource`,
`ProviderOperation::Maintenance`, `ProviderOperation::Replay`,
`ProviderOperation::Inspection`, `ProviderOperation::Feedback`,
`ProviderOperation::Correction`, and `ProviderOperation::Observe` have no
exact Native equivalent at this boundary. They remain provider-local; when the
selected Native composition cannot serve one, it returns typed
unsupported/refused rather than becoming a Native alias. `Observe` is provider
observation acceptance and does not stand for canonical host/session ingest.
NCM is a separate reserved provider slot and cannot satisfy Native fact,
session, LCM, or context routes.

## Reserved provider slots

- `tracedecay.native` is declared but remains gated by parity work.
- `ncm` is reserved; surface audit precedes transport/topology selection.
- `ocean` reserves identity only because no versioned specification exists.

None of the bootstrap slots counts as implemented. Concrete Native/NCM adapters remain out of scope for this bead.
