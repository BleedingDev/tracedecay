# NCM recall trace and content budget fixes

Frozen, unapplied delta for lead review. Production changes are limited to `common.rs::reconstruct_recall` and `selection.rs::Selection::prepare`. Existing private test fixtures in `common/source_binding.rs` supply the adapter regression tests; its production code is unchanged.

- Emit the verified retained `ncm-memory:` identity as the candidate and provenance trace reference; keep activation trace references empty.
- Honor the same trace exclusion before runtime top-k and defensively before adapter content budgeting.
- Emit a UTF-8 prefix bounded by the candidate ceiling and remaining total content bytes. Hash and charge emitted bytes, report partial truncation, and skip content only when no complete character fits.
- Preserve full-content digest exclusions and the original source attribution without rewriting either.

Regression coverage: next eligible runtime candidate under a limit of one, retained trace inspection, read-only state preservation, defensive adapter trace exclusion, UTF-8 split boundaries, remaining total budget, unchanged source attribution, full-content digest exclusions, and a first character that cannot fit.

Validation performed: rustfmt parsing/format check on the three proposed files; git apply --check against the live checkout; baseline hashes still match all three live files. No Cargo, worker/model run, shared corpus edit, or live application was performed. The four added tests have not been executed.

Suggested focused tests after review/application:

- Provider crate library filter: `recall_exposes_retained_trace_and_defensively_excludes_it_before_budgeting`
- Provider crate library filter: `recall_clips_utf8_under_candidate_and_remaining_total_budgets`
- Provider crate library filter: `recall_does_not_emit_or_charge_content_when_no_utf8_character_fits`
- Runtime crate library filter: `selected_trace_exclusion_returns_next_candidate_before_top_k`

Patch and baseline/proposed file hashes are recorded in `manifest.json`.
