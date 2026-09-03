# ADR-0016: Approve the daemon shutdown touch point at eight files and 420 changed lines

Status: Accepted
Date: 2026-09-04

## Context

ADR-0015 approved `daemon_shutdown_deadline` at 8 files and 360 changed lines. The deterministic supersession receipt-ordering regression already present in `crates/tracedecay/src/daemon/invocation_tests/types_tests.rs` proves that superseded work settles before its receipt terminal is emitted. That test completes the same bounded shutdown/supersession invariant but brings the measured category to **8 files and 416 changed lines**.

This decision authorizes the exact `crates/tracedecay/src/daemon/invocation_tests/types_tests.rs` path and the additional line headroom required by its deterministic regression. It changes no aggregate cap and does not alter `patch-footprint.v3`.

## Decision

We approve the `daemon_shutdown_deadline` touch point at **8 files and 420 changed lines**, replacing the 8-file/360-line cap approved by ADR-0015. The cap rounds the measured 416 lines up to the next ten-line boundary, leaving a four-line operational buffer without granting broad headroom.

Root will commit this policy slice before the implementation slice containing the deterministic `types_tests.rs` supersession receipt-ordering regression. This preserves the rule that authorization lands before the implementation commit it permits, even though both slices are currently visible in the working tree.

## Touch-point cap revision

- Touch point: `daemon_shutdown_deadline`
- Previous max files: `8`
- Previous max changed lines: `360`
- Approved max files: `8`
- Approved max changed lines: `420`
- Measured files: `8`
- Measured changed lines: `416`
- Policy revision: `patch-footprint.v3`
- Supersedes: `product/architecture/adr/ADR-0015-daemon-shutdown-test-fence-and-supersession-headroom.md`

## Consequences

- The deterministic receipt-ordering regression can land without weakening the local category gate.
- ADR-0016 becomes the approving decision in the category's `cap_revision`; ADR-0015 remains the historical 8/360 decision.
- The category has four changed lines of headroom and must return through an ADR before further growth.
- Every aggregate cap, exception rule, and product-owned path remains unchanged; this decision adds only the exact `types_tests.rs` path and its active 60-line convergence entry.

## Rejected alternatives

- **Increase the file cap.** Rejected because the measured changed-file footprint remains exactly eight files. The newly authorized `types_tests.rs` path replaces unused allowlist headroom rather than increasing the number of simultaneously changed files.
- **Raise an aggregate cap or advance the policy revision.** Rejected because only this touch point's line count is exceeded; `patch-footprint.v3` and every aggregate cap remain sufficient.
- **Fold authorization into the implementation commit.** Rejected because the cap increase must be reviewable and committed before the implementation slice it authorizes.

## Invariants

1. `daemon_shutdown_deadline` is capped at 8 files and 420 changed lines, derived from a measured 8 files and 416 lines.
2. The touch point adds only the exact `types_tests.rs` allowlist path and raises its changed-line cap; its file cap and every aggregate cap remain unchanged.
3. The policy revision remains `patch-footprint.v3`, and ADR-0016 supersedes only ADR-0015's approving role for this touch point.
4. Root commits this policy slice before the implementation slice containing the deterministic `types_tests.rs` regression.
5. Existing convergence entries and their semantic content remain unchanged.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py`
- `python3 tests/product_patch_footprint_policy_test.py`
- `python3 scripts/product/check-foundational-adrs.py`
- `python3 tests/product_foundational_adrs_test.py`
- Executable beads: `tdmem-0105`, `tdmem-0404`, and `tdmem-1204`.

## Review triggers

Review before the category exceeds 8 files or 420 changed lines, before any allowed path or aggregate cap changes, or if the supersession receipt-ordering regression no longer proves settlement before terminal receipt emission.
