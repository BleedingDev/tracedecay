# tracedecay-memory-provider-api

Provider-neutral Rust runtime boundary for the canonical Memory Provider V1 contract set.

The crate reuses the generated contract values from `product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs`; it does not define another wire schema. It adds only owned runtime identities, exact coding scope, live cancellation, bounded call envelopes, typed terminal records, provider descriptors, handshake values, and the object-safe `MemoryProvider` trait.

It intentionally has no TraceDecay storage, code-index, daemon, dashboard, host, transport, Native-provider, NCM, or OCEAN dependency.

`AdvisoryAdmissionAuthority` is installed by provider composition to check history and restore requests through existing host authorities immediately before dispatch. Providers trust only that port's fresh result and call `CurrentAdvisoryAdmission::verify_for` before use. The framed call binding covers provider identity, operation, scope, readiness, generation, idempotency, canonical payload (including grant claims), extensions, capabilities and finite control. Constructors and hashes prove structure and binding; they do not prove source authorization.

Admission results are runtime values with no serialized or persisted authority form. Frozen original attribution remains separate from fresh canonical and provider privacy disposition. Restore callers must compare the complete inventory decoded from actual snapshot bytes with `CurrentRestoreAdmission::verify_inventory`, enforce blocked dispositions, and obtain a current checkpoint independently of provider generation. Existing `ProviderCall` constructors are unchanged.

## Original Native route boundary

`ProviderOperation` is intentionally limited to the generic provider boundary.
`ProviderOperation::Recall` is a read-only advisory query; it is not the
original Native fact `Search`, the canonical `memory_matches` context route,
session lookup, or an LCM operation. Those routes remain typed application and
retained ports owned by TraceDecay:

- `RetainedSurfaceOperation::FactStoreCurate` runs through
  `RetainedAutomationExecutionPortV1`. The current retained curator performs
  pre-mutation automation admission reservation, applies accepted reviewed
  mutations through the Native fact authority, and then settles the outer
  effect ledger/run terminal before reporting success. Any downstream Native
  composition must preserve that reservation → mutation → outer-settlement
  order. It is distinct from the lower-level curation transaction and is not a
  provider-local reservation.
- `DaemonSessionLookupPrimitiveV1` reaches
  `SessionApplicationRetrievalPortV1::retrieve_admitted` and remains distinct
  from retained `MessageSearch`. `SessionsFor` and `Workflows` are project
  authority routes; profile requests remain typed unsupported.
- `recent_sessions`, `session_providers`, and `session_replay_slice` are
  lower-level internal `RegisteredGlobalDb`/`SessionStoreAccess` queries with
  no public retained LCM binding. `lcm_compress` and its lifecycle helpers are
  daemon-internal mounted-authority work.

The direct retained fact `Search` preserves its original nonempty retrieval
telemetry and owner-bound receipt. Related uses a retained `RecordRetrieval`
access lease when it reconciles the derived relation graph inline, but its
semantic query does not call explicit retrieval tracking or promise the Search
receipt. Automatic context reuses one already executed canonical contribution
and does not gain that write effect. The
`NativeContextDeliveryMarker` is host/composition metadata bound to the
selected registration, accepted revision, exact scope, and host-computed
request/contribution digests; provider payloads, descriptors, names, and
provider-local receipts cannot create it. NCM is a separate reserved provider
slot. Generic provider-local controls such as
`ProviderOperation::SnapshotExport`, `ProviderOperation::SnapshotRestore`,
`ProviderOperation::DeleteBySource`, `ProviderOperation::Maintenance`,
`ProviderOperation::Replay`, `ProviderOperation::Inspection`,
`ProviderOperation::Feedback`, `ProviderOperation::Correction`, and
`ProviderOperation::Observe` do not become Native aliases. When the selected
Native composition cannot serve one, it returns typed unsupported/refused;
`ProviderOperation::Observe` remains provider-local observation acceptance and
does not stand for canonical host/session ingest.
