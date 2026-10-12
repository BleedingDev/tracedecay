# Reviewed-facade numeric declaration layer

Draft only. Apply after `pm-context-evidence.integration.patch` (SHA256 `e361d53489dc2483d150b9200d0796db42391b44cc22ae21dae309cd2173d1c1`). That approved artifact is unchanged.

The layered artifact is `pm-provider-numeric-declaration.integration.patch`, SHA256 `28abfdf1a4ae39d0bae29d5ffed832647c939286887beda96158aa7dccde7d94`: five files, 140 insertions and two deletions. No live application or Cargo execution occurred.

## API

The feature-only module `tracedecay::daemon::test_context_evidence` gains:

```rust
pub use tracedecay_memory_provider_registry::{
    CancellationToken, OperationControl, OwnedExactScope,
};

pub fn production_provider_numeric_declaration_for_test(
    provider_id: &str,
) -> Result<Option<ProviderNumericDeclarationForTestV1>, ContextEvidenceReadErrorV1>;
```

The declaration contains `provider_id`, `declared_provider_instance_id`, `declared_implementation_identity_digest`, `declared_limits_digest`, and `declared_limits` with all eight named fields: request/response bytes, observation batch items, recall candidates, concurrent operations, operation milliseconds, snapshot bytes, inspection items. Unsupported provider IDs return `Ok(None)`. Declaration errors return an explicit invalid-evidence error. No registration-time generation is exposed as live state.

Native's wrapper copies `native_descriptor()`, `PROVIDER_INSTANCE_ID`, and the digest produced by existing `digest_native_limits`. NCM's wrapper calls `production_identity_declaration()`, performs the same profile check as `from_production_worker`, calls `descriptor_for(..., 0)` and `implementation_version`, and uses existing `digest_limits`/`hex_digest`. Neither path constructs an owner, worker, model, registry, or state root. The root `test-helpers` feature weakly forwards the new NCM `test-helpers` feature, preserving the optional dependency boundary.

## Consumer acceptance and low-cost regression sketch

The fixture must first validate the same fresh, successful actual public Health response against the observed selected provider, registration revision, full destination scope/digest, Ready state, and required available capabilities. It then compares the exact production instance ID, implementation digest, and complete eight-field effective-limits digest. Only a complete match permits numeric output. Missing fields, unsupported declarations, lowered negotiated limits, or any mismatch remain explicitly unavailable. State identity and generation are copied from that real response. Actual executable SHA is collected independently by the fixture.

Use the consumer's pure acceptance function for these low-cost regressions:

1. For each actual production declaration, independently hash its eight copied fields in the production order and verify equality with `declared_limits_digest`. This catches a field-mapping error without starting a provider.
2. A successful, fully bound observed-health fixture carrying that declaration's instance/implementation/limits digests admits the exact eight numbers. Keep a nonzero observed generation to prove no comparison against declaration generation zero exists.
3. Change each of the eight fields in a copied vector, recompute its complete digest, and place that changed digest in the otherwise-valid health fixture. Every case must become unavailable. This covers smaller negotiated limits as well as changed ceilings.
4. Independently mismatch or remove provider, revision, full scope, Ready state, instance, implementation digest, and effective-limits digest. Each must become unavailable, with no fallback to another provider's declaration or a synthesized minimum.
5. An unsupported ID returns no declaration. A missing health response never produces numeric evidence merely because a declaration can be read.

The real CLI integration still supplies actual health/build evidence; the synthetic health fixtures test the gate only and are not reported as observed provider outcomes.

## Validation performed

Rustfmt parsed and formatted the three Rust draft files. Both modified manifests parsed as TOML. The layered patch passed `git apply --check --whitespace=error` against its exact before tree, whose facade matches the approved artifact byte for byte and whose other four files match live source. No runtime verification was claimed.
