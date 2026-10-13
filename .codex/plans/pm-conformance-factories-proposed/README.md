# Shared real-adapter fixture proposal

Unapplied patch: `apply.patch`. Only the conformance crate is affected:

- `src/compatibility.rs`: the three foreign checkout absence steps accept a healthy empty recall or an effect-free `ScopeMismatch`, with an earlier populated witness and no retained source/content/provenance exposed. All other suite expectations remain as authored. The existing UTC nanosecond parser becomes crate-visible for shared source parsing.
- New `src/real_fixture.rs`: trusted scenario source inventory, immutable source and canonical payload checks, existing deterministic fixture receipt checks, fixed same-checkout fixture admission policy, and mutable current source dispositions/availability. `FixtureAuthority` implements the injected API authority. It has no adapter, root storage or runtime dependency.
- New `src/real_fixture/transport.rs`: bounded serializable DTOs for actual descriptors, handshake requests/responses, provider calls/replies. Every provider-owned identity, canonical byte, digest, terminal/effect/fallback field, extension, warning and sanitization receipt is retained. Invalid reply payload digests remain invalid for the common runner to assess.
- New `src/real_fixture/tests.rs`: nine focused tests cover unchanged suite source registration; current disposition and unavailable-authority behavior; foreign scope and snapshot inventory denial; actual registered replay evidence; DTO descriptor, handshake, observation receipt and reply round trips; cancellation/deadline reductions; and non-vacuous foreign-scope absence versus outages/leaks.
- `src/lib.rs`: one documented `real_fixture` module export.

## Factory APIs

`FixtureAuthority::from_scenario(&CompatibilityScenario, &OwnedExactScope) -> Result<Self, String>`

The authority is cloneable with shared in-process state. `snapshot() -> Result<FixtureAuthoritySnapshot, String>` and `from_snapshot(FixtureAuthoritySnapshot) -> Result<Self, String>` transfer trusted setup across actual child restarts. The snapshot derives Serialize/Deserialize. Persist and fsync it through the owning fixture's host state directory when process recovery needs durable authority state.

`set_available(bool) -> Result<(), String>` changes the actual callback's availability. `record_disposition(&str, SourceDisposition) -> Result<String, String>` changes an already registered source and returns the authority-local journal reference.

`DescriptorDto`, `HandshakeRequestDto`, `HandshakeResponseDto`, `ProviderCallDto` and `ProviderReplyDto` expose `capture(&Actual) -> Result<Self, String>`, `encode() -> Result<Vec<u8>, String>` and `decode(&[u8]) -> Result<Self, String>`. Descriptor and response DTOs use `restore() -> Result<Actual, String>`. Request and call DTOs use `restore(CancellationToken, Duration) -> Result<Actual, String>`.

The request/call duration is conservative known queue/transit elapsed. The DTO also subtracts observed wall transit and preserves the original absolute UTC deadline. A backward wall jump yields an expired child budget. The owning process MUST bound the entire RPC wait with the original parent OperationControl and bridge live parent cancellation into the child token while the provider is running. The DTO cannot supply a live cancellation bridge by serialization alone.

Mode gates, actual process lifecycle and storage/legacy/corruption controls belong to the Native and NCM factory owners. This support never fabricates provider replies or physical evidence.

## Validation

`rustfmt --edition 2024` parsed/formatted all proposal files. `git apply --check .codex/plans/pm-conformance-factories-proposed/apply.patch` passes. No Cargo, live source apply, commit or push was performed by this worker.

After lead review/application, the focused check is `cargo test -p tracedecay-memory-conformance --lib --test compatibility`, through the lead's existing Cargo ownership and target/test-profile configuration. Real adapter common-suite runs are separate evidence owned by their factories.
