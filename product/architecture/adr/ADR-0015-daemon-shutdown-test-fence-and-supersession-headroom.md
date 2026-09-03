# ADR-0015: Approve the daemon shutdown touch point at eight files and 360 changed lines

Status: Accepted
Date: 2026-09-03

## Context

[ADR-0013](./ADR-0013-daemon-shutdown-touch-point-expansion.md) approved the
`daemon_shutdown_deadline` touch point at 6 files and 320 changed lines, and
made touch-point-local caps mechanically binding: every category's caps are
pinned in `scripts/product/check-patch-footprint-policy.py`, and a category
whose caps were revised must carry a `cap_revision` block naming the ADR that
approved its exact numbers. That category is now full — 6 of 6 files and 310
of 320 lines — and two separate corrections to the same shutdown seam need
room.

**The shutdown-fence test was not literally asserting the fence.** Bead
`tdmem-0th` found the same class of defect ADR-0013 was written for: an
invariant recorded as proven that the code did not actually establish.
`state_shutdown_fences_a_queued_open_before_the_endpoint_expiry_sweep` in
`crates/tracedecay/src/daemon/invocation_tests/lsp_lease_tests.rs` reached the
moment it wanted to test by spinning eight fixed `tokio::task::yield_now()`
calls after spawning shutdown, and then settled for *either* the racing open
finishing *or* the admission lock merely being contended. Neither step observes
the fence. A scheduler that had not yet run `begin_shutdown` after eight yields
would spawn the open on the wrong side of the fence, and the lock-contention
branch would let that run report success. The verified fix replaces both with
bounded waits on an observable effect — `lsp_admission_open` closing, and then
the fenced open actually settling — which makes the assertion literal. It is 19
changed lines in a file this category never admitted, because ADR-0013 sized
the category around the production call chain only.

**A supersession defect in the same call chain is pending.**
`BoundedHookOrchestratorV1::admit` in
`crates/tracedecay-daemon-service/src/invocation/types.rs` currently drops the
superseded work future instead of driving it to settlement, which detaches
nested blocking work exactly the way a dropped `JoinHandle` does — the failure
mode ADR-0013 was written to eliminate one layer down. Driving the superseded
work to settlement under `TASK_ABORT_DEADLINE` before its receipt terminal is
about 16 more lines in a file whose convergence entry budget is 30 and whose
current diff is 29.

Measured against the pinned floor `5749e4fcfe268e17bd19a0e6ef90c646f7b37289`
with the test-fence fix in the tree, the category is **7 files and 329 changed
lines**:

| File | Changed lines |
| --- | ---: |
| `crates/tracedecay/src/daemon/bootstrap.rs` | 84 |
| `crates/tracedecay/src/daemon/engine/shutdown.rs` | 4 |
| `crates/tracedecay/src/daemon/invocation_state.rs` | 11 |
| `crates/tracedecay-daemon-service/src/invocation/lsp.rs` | 14 |
| `crates/tracedecay-daemon-service/src/invocation/types.rs` | 29 |
| `crates/tracedecay-daemon-service/src/project_runtime/shutdown.rs` | 168 |
| `crates/tracedecay/src/daemon/invocation_tests/lsp_lease_tests.rs` | 19 |
| **Total** | **329** |

With the pending supersession fix the same seven files measure 345 lines.

## Decision

We approve the `daemon_shutdown_deadline` touch point at **8 files and 360
changed lines**, replacing its previous 6 files and 320 changed lines, and this
ADR supersedes ADR-0013 as the approving decision recorded in the category's
`cap_revision` block. ADR-0013 stands as the historical record of the
5/287 -> 6/320 step and its approval of
`crates/tracedecay-daemon-service/src/invocation/types.rs`; nothing it decided
is reversed.

We admit three additional paths to the category — the shutdown call chain's own
unit tests:

- `crates/tracedecay/src/daemon/invocation_tests/lsp_lease_tests.rs`
- `crates/tracedecay/src/daemon/invocation_tests/lsp_tests.rs`
- `crates/tracedecay/src/daemon/invocation_tests/project_lifecycle_tests.rs`

Only the first has a convergence entry today, because it is the only one that
currently differs from the floor. Admitting a path to a category grants no
authority to change it: an actual edit still needs its own active convergence
entry, and the gate fails an entry with no diff exactly as it fails a diff with
no entry. The other two are named now so that, if root's experiment concludes
they must change, the cap that admits them was approved before the change
rather than by it.

The numbers follow the ADR-0011 rule — the measurement plus at most roughly
fifteen percent headroom:

| Cap | ADR-0013 | ADR-0015 | Measured now | Headroom |
| --- | ---: | ---: | ---: | ---: |
| `daemon_shutdown_deadline` max files | 6 | 8 | 7 | 14.3% |
| `daemon_shutdown_deadline` max changed lines | 320 | 360 | 329 | 9.4% |

We also raise the convergence-map `line_budget` of
`crates/tracedecay-daemon-service/src/invocation/types.rs` from 30 to 50, ahead
of the supersession fix that needs it. At the projected 45 lines that budget
has 11% headroom, and the file stays far inside the global per-file cap of 560.

**No aggregate cap changes.** `max_upstream_existing_production_files` (37),
`max_total_upstream_changed_lines` (3500),
`max_upstream_existing_test_or_fixture_files` (9),
`max_changed_lines_per_upstream_file` (560), `max_composition_root_files` (15),
`max_allowed_touch_point_files_per_category` (15),
`default_max_exception_zone_files` (4), `max_exception_files_per_adr` (2),
`max_workspace_manifest_files` (2), and `manual_generated_file_edits` (0) keep
the values ADR-0011 and ADR-0014 set, and the policy revision stays
`patch-footprint.v3`. In particular the test/fixture cap is deliberately left
at 9: with `lsp_lease_tests.rs` counted the tree measures 8 upstream
test/fixture files, so 9 already carries 12.5% headroom and a higher number
would have no measurement behind it. If root's experiment adds
`lsp_tests.rs` and `project_lifecycle_tests.rs` as changed files, that is the
change which measures a ninth and tenth, and the ADR raising the cap belongs
with it.

## Touch-point cap revision

- Touch point: `daemon_shutdown_deadline`
- Previous max files: `6`
- Previous max changed lines: `320`
- Approved max files: `8`
- Approved max changed lines: `360`
- Measured files: `7`
- Measured changed lines: `329`
- Policy revision: `patch-footprint.v3`
- Added paths: `crates/tracedecay/src/daemon/invocation_tests/lsp_lease_tests.rs`, `crates/tracedecay/src/daemon/invocation_tests/lsp_tests.rs`, `crates/tracedecay/src/daemon/invocation_tests/project_lifecycle_tests.rs`
- Supersedes: `product/architecture/adr/ADR-0013-daemon-shutdown-touch-point-expansion.md`

## Consequences

- The `tdmem-0th` finding is closed honestly: the shutdown fence is asserted by
  observing `begin_shutdown`'s effect rather than by hoping eight yields were
  enough, so a regression in the fence fails the test instead of racing past
  it.
- The supersession fix in `invocation/types.rs` becomes landable without either
  exceeding its per-entry budget or raising that budget in the same change that
  needs it.
- The category's own tests are now inside the category. That is the honest
  place for them — a test that asserts the shutdown ordering is part of the
  shutdown seam and has to be re-applied with it on every sync train — but it
  does mean the category's line budget is now shared between production and
  test code, which is why the cap moved by 40 rather than by the 15% the file
  count alone would have allowed.
- At 329 of 360 lines and 7 of 8 files, and 345 of 360 once the pending fix
  lands, the category again has almost no room. The next change in this seam
  comes back through an ADR.
- Nine paths are now admitted to a category capped at 8 files. That is
  deliberate: the allowlist says which files this seam may ever touch, and the
  cap says how many may differ from the floor at once. If all three test files
  and all six production files were ever to change together, the gate would
  refuse until a further decision.

## Rejected alternatives

- **Leave the fixed `yield_now()` loop and keep the category at six files.**
  Rejected because the recorded invariant would stay aspirational, which is
  precisely the defect class `tdmem-0th` was opened for and ADR-0013 was
  written against. A test that can pass without observing the property it names
  is worse than no test, because it converts an unproven invariant into a
  documented one.
- **Bound the wait with a sleep or a longer yield count instead of an observable effect.**
  Rejected because it moves the flake threshold without removing it. A sleep
  long enough to be reliable on a loaded machine is a slow test that still
  fails under contention, and the observable effect — `lsp_admission_open`
  closing — is already available at no cost.
- **Classify the shutdown unit tests under `integration_test_runtime_isolation`.**
  Rejected because that category is about harness and profile isolation for
  integration tests, not about the shutdown ordering contract, and its three
  exact paths are a different concern. Filing these tests there to avoid an ADR
  would put the wrong invariant in the wrong seam and hide the growth this ADR
  is making visible.
- **Bundle the cap increase into the supersession fix.** Rejected because that
  is what ADR-0011 invariant 2 forbids, and because a cap raised by the change
  it authorizes enforces nothing.
- **Raise the aggregate test/fixture cap to 11 at the same time.** Rejected
  because the tree measures 8 upstream test/fixture files, so 11 would be 37.5%
  above the measurement and outside the ADR-0011 rule this ADR is applying to
  its own numbers. The existing cap of 9 already admits one more test file; a
  tenth is a decision to take with the change that creates it.
- **Raise the category to the global per-category limit of 15 files.**
  Rejected for the reason ADR-0011 and ADR-0013 both gave: a cap with unbounded
  headroom enforces nothing, and upstream growth has to stay a decision rather
  than a drift.

## Invariants

1. The `daemon_shutdown_deadline` touch point is capped at 8 files and 360
   changed lines, derived from a measured 7 files and 329 changed lines plus at
   most roughly fifteen percent headroom.
2. This ADR is the approving decision named by the category's `cap_revision`
   block; ADR-0013 remains the historical record of the previous step, and the
   `previous_max_files` and `previous_max_changed_lines` recorded in the policy
   are the 6 and 320 that ADR-0013 approved.
3. The three admitted `invocation_tests` paths gain no authority from being
   listed. Each still needs its own active convergence entry before it may
   differ from the floor, and only `lsp_lease_tests.rs` has one.
4. A shutdown-fence assertion is proven by an observable effect of
   `begin_shutdown` under a bounded wait, never by a fixed number of scheduler
   yields, a sleep, or a lock-contention probe.
5. No aggregate cap, exception zone, product-owned path, dependency rule, or
   allowed touch-point set other than this category's paths and caps is changed
   by this decision, and the policy revision stays `patch-footprint.v3`.
6. A cap increase is approved by ADR before the change that needs it, and is
   never bundled into the change that exceeds the previous cap. The raised
   `types.rs` line budget lands ahead of the supersession fix it is for.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py` — validates the
  live diff against the pinned 8/360 caps, the `cap_revision` binding to this
  ADR, and the new `lsp_lease_tests.rs` convergence entry.
- `python3 scripts/product/check-upstream-ownership-registry.py` — the three
  admitted paths stay bounded by the category, and the new entry resolves to
  exactly one touch point and one active upstream area.
- `python3 tests/product_patch_footprint_policy_test.py` — asserts the category
  caps cannot be raised in the policy alone and that a `cap_revision`
  disagreeing with this ADR is rejected.
- `cargo test -p tracedecay --features memory-provider-host --lib -- --exact daemon::invocation_tests::lsp_lease_tests::state_shutdown_fences_a_queued_open_before_the_endpoint_expiry_sweep`
  — the deterministic fence assertion itself.
- Executable beads: `tdmem-0105` (the patch-footprint policy itself),
  `tdmem-0404` (disabled-mode and shutdown behavioral inertness), and
  `tdmem-1204` (semantic invariants and parity tests for every upstream touch
  point).

## Review triggers

Review when the `daemon_shutdown_deadline` category reaches ninety percent of
either approved cap — the pending supersession fix takes it to 345 of 360, so
this triggers on the next change in the seam — when root's experiment concludes
that `lsp_tests.rs` or `project_lifecycle_tests.rs` must change, when the
aggregate test/fixture count reaches 9, when upstream's own lease registry
starts consuming a caller-supplied deadline (which retires several of these
entries), or when any other touch point proposes a local cap increase.
