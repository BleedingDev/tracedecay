# Numeric domain draft delta, frozen for root review

Original patch `pm-tool-result-delivery.patch` remains byte-for-byte unchanged at SHA-256 `98672b3a9f1907100597bf8dae04c3deafe1b95810f9866edb065f6ec4aa571e`.

- Numeric-only delta: `pm-tool-result-delivery.numeric.patch`, SHA-256 `552aec0fa7dcd1f52c2b7ce2c9c1e35338f2997293e5a1955884cbe8b04613d0` (207 lines).
- Combined original plus numeric correction: `pm-tool-result-delivery.numeric-combined.patch`, SHA-256 `fe05e0fdffb1ac63f5a3db2726eda99b81ff87aa6d82219d6d7f8e86483ff7a0`.
- Isolated draft mirror and exact file hashes: `pm-tool-result-delivery-numeric-proposed/review-manifest.json`.

The numeric-only delta owns exactly three files: Python `delivery_evidence.py`, its `test_delivery_evidence.py`, and Rust `host_retrieval/tool_result.rs` (including its unit regressions). No other original-draft source was changed.

Both lexical parsers now use the same numeric domain. Integer lexemes are exact and restricted to `[-2^63, 2^64-1]`; overflow is rejected before semantic-projection comparison and cannot fall back to a rounded float. Bare `-0` normalizes to integer zero in both parsers. Fraction/exponent lexemes use finite IEEE-754 binary64 semantics; overflow to infinity rejects. Exact original lexemes, byte offsets and SHA-256 spans remain unchanged, including the distinct bytes of `-0`, `0`, `-0.0` and exponent forms.

The Rust parser explicitly validates JSON numeric grammar and uses exact i64/u64 parsing for integers, plus standard f64 parsing and a finite-number check for fraction/exponent forms. Python retains its strict numeric lexeme parser and rejects arbitrary-precision integers outside the shared domain. Primitive booleans/null are not treated as numbers.

Verification: 26 development-only Python tests pass, including both integer boundaries, adjacent overflowing integers, wrong adjacent in-range semantic values, bare negative zero with its original span, finite extreme/subnormal/underflow float forms and nonfinite rejection. Three corresponding Rust tests were added (12 Rust tests now authored in this draft); they were not compiled/run because root owns Cargo. Python AST/JSON syntax, rustfmt parse/format, numeric-delta apply-check against the immutable original draft, and combined-patch apply-check against live base all pass.

No live apply, Cargo, model, host trial, held-out data, manifest, commit or push operation occurred. The approved shared-review work starts in another mirror after this numeric freeze; it will produce its own delta and combined patch.
