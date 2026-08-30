# TraceDecay Memory Conformance

This product-owned crate runs any `MemoryProvider` through one pinned mandatory fixture: compatible handshake, health, observation acceptance, and recall.

The fixture records the exact canonical contract-set ID and digest, fixture ID and build digest, logical provider ID, provider implementation digest, exact TraceDecay scope digest, and complete canonical calls. A run is rejected when any identity or mandatory capability differs.

Two output surfaces are intentionally separate:

- `ProductConformanceReport` retains validated canonical replies for explicit evaluation and differential comparison.
- `ObserverConformanceReport` contains only provider/fixture identities and terminal receipts. It has no payload, warning, extension, receipt, namespace, or accepted-scope field, so observer output cannot become agent context accidentally.

The crate depends only on `tracedecay-memory-provider-api`. It contains no provider implementation, transport, database, code index, dashboard, daemon, host adapter, or execution-topology choice.
