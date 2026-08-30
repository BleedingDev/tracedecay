# TraceDecay NCM Memory Provider Adapter

This product-owned crate defines the NCM-facing side of TraceDecay's provider-neutral `MemoryProvider` boundary. It is scaffolding, not an NCM implementation and not an execution-topology decision.

The adapter:

- receives the logical NCM provider identity from product configuration and verifies the runtime reports the same identity;
- forwards complete handshakes and provider calls without rewriting canonical payloads, exact scope, request identity, deadlines, cancellation, or extensions;
- refuses wrong-target calls, handshake misuse, and undeclared capabilities through the runtime's authoritative typed rejection methods;
- depends only on `tracedecay-memory-provider-api`;
- owns no NCM algorithm, model, memory database, Python binding, socket client, process supervisor, TraceDecay store, or code index.

`tdmem-0701` audits the licensed NCM surface and `tdmem-0702` chooses the execution topology. M6 capability mapping is implemented behind `NcmRuntimePort`; this crate does not prejudge whether that port is backed in-process or by an isolated local runtime.
