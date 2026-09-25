# Selected recall physical-corruption fixture

Frozen, unapplied correction for the cc1382 fixture setup failure.

`update_capsule_provenance` now correctly rejects the intentionally invalid capsule during setup. The existing external corruption helper in `tests/engine_transactions.rs` opens a new SQLite connection only after releasing the engine; it cannot replace this resident-engine fixture with its exclusive connection.

This patch adds `Mutation::corrupt_capsule_provenance_for_test` under `#[cfg(test)]`, visible only within the crate. It updates one selected provenance cell through the existing owned transaction, requires exactly one row, and leaves commit handling to the caller. It does not expose a raw connection or a public corruption API. The production provenance writer is byte-for-byte unchanged.

The fixture changes only the called method name. Its commit and cached-generation update, both suppression cases, missing selection metadata, unknown-policy exclusion, corrupt result, unchanged metadata/generation, and published Arc identity assertions are unchanged.

Validation performed: rustfmt check, git apply --check, live baseline hash comparison, and exact source comparison proving the production writer and existing assertions unchanged. No Cargo or tests were run; no live files were applied.

Test plan after review/application:

1. Run the previously failing exact unit test:
   `cargo test -p tracedecay-memory-ncm-runtime --lib engine::runtime::selection::tests::selected_read_rejects_corruption_before_suppression_or_unknown_metadata -- --exact`
2. Repeat the original cc1382 runtime-library test selection to confirm its existing 51 passing tests still pass alongside this correction.

The frozen patch and baseline/proposed SHA-256 values are recorded in `manifest.json`.
