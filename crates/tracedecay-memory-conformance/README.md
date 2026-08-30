# TraceDecay Memory Conformance

Provider-neutral fixtures, mandatory conformance, scenario execution, and differential reporting for the Memory Provider V1 boundary.

The crate depends only on `tracedecay-memory-provider-api`; it does not depend on the dashboard, host, fabric, Native adapter, NCM adapter, stores, or code index.

A `FixtureIdentity` pins the exact versioned contract identity, contract-set digest, logical provider identity, provider build identity, and implementation digest. `ConformanceHarness` runs descriptor identity, mandatory capabilities, handshake, health, observation acceptance, and recall against any `MemoryProvider`. `ScenarioRunner` records typed terminal outcomes and payload digests without requiring provider-internal state equivalence. `DifferentialReport` compares those neutral results.

`ObserverConformanceResult` can retain only a `ProductOutputDigest` and an isolated conformance report. It has no product-output bytes, prompt mutation surface, or active-provider replacement path.
