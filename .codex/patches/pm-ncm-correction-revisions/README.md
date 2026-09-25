# Correction revision and replacement draft

Status: frozen draft only; no live source changes or test execution.

Patch: `correction-revisions.patch`

SHA-256: `18e11ab2a2f9f3f54a5190d965e187281a11fc507bd4eb26c1399944b794ed24`

The adapter preserves retained target attribution and passes the caller's expected revision independently to the existing runtime CAS. A fully admitted replacement may name a different source. Its actual revision must differ from the retained target revision, irrespective of the caller's CAS value. Existing exact-scope, target reference, attribution, replacement projection, and pre-encode/under-lock privacy checks remain in place.

Recon and changes:

- `common/lifecycle.rs`: only Correction revision projection and replacement restriction; the shared `target`, `attribution`, `source_binding`, and `project_attribution` helpers are reused unchanged.
- `engine/runtime/control.rs`: only removal of replacement-source equality; `resolve_target`, generation/revision CAS, and both replacement revocation checks remain unchanged.
- `tests/rust_backend.rs`: one new regression reuses existing `surface`, `canonical_observation`, `canonical_lifecycle_call`, `FixtureHistoryAuthority`, and `canonical_recall_call` helpers. It checks stale `99` and replacement-matching stale CAS conflicts without state changes, same-actual-revision and forged-target refusals, a committed `source/2` replacement, then exact old/new identities and content in current/as-of/interval recall after restart.

Baselines include the applied legacy trace patch `8b245890598298e4e1b8d97e64a6879e45f0bddfa71271fb77bde02de9ff515e`. Those production and test additions are preserved. Per-file hashes are in `manifest.json`.

Validation completed: proposed Rust files pass rustfmt; `git apply --check` passes against current live sources; all live files still equal the captured baselines. No new production helpers, shared conformance changes, Cargo commands, worker starts, or model runs.

Root validation after approval: run `enabled::canonical_correction_checks_actual_revision_and_retains_cross_source_history` in the Rust backend integration test with the rebuilt existing worker, then the unchanged common real NCM factory as scheduled by root.
