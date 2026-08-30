# TraceDecay Native Memory Provider Adapter

This product-owned crate places the existing TraceDecay Native memory application behind the provider-neutral `MemoryProvider` boundary. It owns no Native data or algorithms.

The adapter:

- accepts only a port that declares the stable `tracedecay.native` identity;
- routes mandatory health, observation, and recall calls without rewriting canonical payload or exact scope;
- routes only declared optional lifecycle capabilities;
- delegates all terminal records, provenance, receipts, scoring, temporal state, and rejection diagnostics to the Native application authority;
- contains no TraceDecay database, store, graph, code-index, daemon, host, dashboard, transport, NCM, or OCEAN dependency.

M2 proves the boundary with a mock application port. M3 supplies the real owner-bound TraceDecay application bridge and direct-versus-provider parity journeys. No second fact store, score implementation, curation path, or persistence format is introduced here.
