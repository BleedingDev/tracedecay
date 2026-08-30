# tracedecay-memory-provider-api

Provider-neutral Rust runtime boundary for the canonical Memory Provider V1 contract set.

The crate reuses the generated contract values from `product/contracts/memory-provider-v1/generated/rust/memory_provider_v1.rs`; it does not define another wire schema. It adds only owned runtime identities, exact coding scope, live cancellation, bounded call envelopes, typed terminal records, provider descriptors, handshake values, and the object-safe `MemoryProvider` trait.

It intentionally has no TraceDecay storage, code-index, daemon, dashboard, host, transport, Native-provider, NCM, or OCEAN dependency.
