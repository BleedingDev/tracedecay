# tracedecay-memory-conformance

Provider-neutral fixtures, scenario execution, conformance reports, and
differential reports for implementations of `MemoryProvider`.

Fixtures bind the exact Memory Provider contract set, logical provider,
and immutable provider build/implementation digest. The runner builds
calls from the provider's real handshake receipt, so the same fixture can be
used with Native, NCM, or a future provider without provider-name branches.

Active reports may retain typed provider outputs for product-path tests.
Observer reports retain the complete validated terminal consequence—including
structured effect evidence, receipt, generation linkage, and fallback policy—
plus conformance findings and an immutable fixture-controlled scenario
identity. Their Rust types have no field capable of carrying a provider-returned
operation payload or active product output.

The crate depends only on `tracedecay-memory-provider-api`; it has no storage,
code-index, dashboard, daemon, or provider-adapter dependency.

The focused integration test reuses the exact isolated canonical dummy-provider
source through test-only `#[path]` inclusion. That does not add a normal crate
dependency: the conformance crate still depends only on the provider API, while
the standalone dummy workspace and its product checker remain independent.
