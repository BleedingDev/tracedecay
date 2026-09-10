# tracedecay-memory-provider-api

Provider-neutral Rust runtime boundary for the canonical Memory Provider V1 contract set.

The crate reuses the generated contract values from `product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs`; it does not define another wire schema. It adds only owned runtime identities, exact coding scope, live cancellation, bounded call envelopes, typed terminal records, provider descriptors, handshake values, and the object-safe `MemoryProvider` trait.

It intentionally has no TraceDecay storage, code-index, daemon, dashboard, host, transport, Native-provider, NCM, or OCEAN dependency.

`AdvisoryAdmissionAuthority` is installed by provider composition to check history and restore requests through existing host authorities immediately before dispatch. Providers trust only that port's fresh result and call `CurrentAdvisoryAdmission::verify_for` before use. The framed call binding covers provider identity, operation, scope, readiness, generation, idempotency, canonical payload (including grant claims), extensions, capabilities and finite control. Constructors and hashes prove structure and binding; they do not prove source authorization.

Admission results are runtime values with no serialized or persisted authority form. Frozen original attribution remains separate from fresh canonical and provider privacy disposition. Restore callers must compare the complete inventory decoded from actual snapshot bytes with `CurrentRestoreAdmission::verify_inventory`, enforce blocked dispositions, and obtain a current checkpoint independently of provider generation. Existing `ProviderCall` constructors are unchanged.
