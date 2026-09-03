# ADR-0013: Approve the daemon shutdown touch point at six files and 320 changed lines

Status: Accepted
Date: 2026-09-03

## Context

`product/upstream/patch-footprint-policy.json` gives every allowed touch point
a local `max_files` and `max_changed_lines`. Those local caps are the tight
per-seam limit; the ADR-0011 v2 caps are the aggregate backstop behind them.
ADR-0011 invariant 2 states the sequencing rule for every cap in this policy:
"A cap increase is approved by ADR before the change that needs it, and is
never bundled into the change that exceeds the previous cap." Nothing in
ADR-0011 exempts a touch-point-local cap from that rule, and the fact that a
local increase leaves the aggregate v2 caps untouched is not an approval — it
only means no *second* decision is needed.

The `daemon_shutdown_deadline` touch point was sized at 5 files and 287 changed
lines when the shutdown call chain was `bootstrap.rs`, `engine/shutdown.rs`,
`invocation_state.rs`, `invocation/lsp.rs`, and
`project_runtime/shutdown.rs`. Bead `tdmem-0th` then found that the convergence
invariant recorded for `invocation/lsp.rs` — LSP shutdown consumes only the
remaining shared deadline and never mints a fresh timeout — was not literally
satisfied: `expire_all_until` awaited `LspLeaseTaskRegistry::shutdown()`
without any bound.

That defect cannot be repaired inside the five approved files.
`LspLeaseTaskRegistry::shutdown` takes every lease task out of the registry
before it joins them one at a time, so a caller-side `tokio::time::timeout_at`
in `lsp.rs` would drop the future that owns those taken tasks, and a dropped
`JoinHandle` detaches its task instead of aborting it. Bounding shutdown from
the caller would therefore trade an unbounded wait for a set of silently
detached lease tasks. The deadline has to reach the registry, which owns the
handles — and the registry lives in a sixth upstream file,
`crates/tracedecay-daemon-service/src/invocation/types.rs`, in the same
shutdown call chain and under the same category.

Measured across the six files against the pinned floor
`5749e4fcfe268e17bd19a0e6ef90c646f7b37289`, the category footprint is 6 files
and 308 changed lines (additions plus deletions):

| File | Changed lines |
| --- | --- |
| `crates/tracedecay/src/daemon/bootstrap.rs` | 84 |
| `crates/tracedecay/src/daemon/engine/shutdown.rs` | 4 |
| `crates/tracedecay/src/daemon/invocation_state.rs` | 11 |
| `crates/tracedecay-daemon-service/src/invocation/lsp.rs` | 14 |
| `crates/tracedecay-daemon-service/src/invocation/types.rs` | 27 |
| `crates/tracedecay-daemon-service/src/project_runtime/shutdown.rs` | 168 |
| **Total** | **308** |

## Decision

We approve the `daemon_shutdown_deadline` touch point at **6 files and 320
changed lines**, replacing its previous 5 files and 287 changed lines, and we
approve `crates/tracedecay-daemon-service/src/invocation/types.rs` as the sixth
path in that category. The approval is deliberately narrow:

- Only the named sixth path is admitted. The category may not absorb another
  file, and the seam it authorizes is the LSP lease registry's own retirement
  of its tasks under the caller's absolute deadline — nothing else.
- The line cap follows the ADR-0011 rule for setting caps: measured (308) plus
  at most roughly fifteen percent headroom. 320 is 3.9 percent above the
  measurement, enough for an in-flight bead in the same seam and not enough to
  absorb an unplanned mount.
- No ADR-0011 aggregate cap changes. `max_upstream_existing_production_files`,
  `max_upstream_existing_test_or_fixture_files`,
  `max_total_upstream_changed_lines`, `max_changed_lines_per_upstream_file`,
  `max_composition_root_files`, and
  `max_allowed_touch_point_files_per_category` keep the values ADR-0011 set,
  and 6 stays well inside the global per-category cap of 15.
- ADR-0011 invariant 2 is upheld rather than amended. This policy revision —
  this ADR, its manifest registration, the `cap_revision` block in
  `product/upstream/patch-footprint-policy.json`, the pinned caps in
  `scripts/product/check-patch-footprint-policy.py`, and the prose in
  `product/upstream/patch-footprint-policy.md` — lands as its own change,
  before the `invocation/lsp.rs` and `invocation/types.rs` implementation that
  needs the headroom. The implementation change is then measured against a cap
  that was already approved.

We also make the sequencing mechanically enforceable instead of leaving it to
review. `scripts/product/check-patch-footprint-policy.py` now pins every
touch-point-local cap, so a category cannot widen its own reach by editing the
policy alone, and it requires a category whose caps were revised to carry a
`cap_revision` block naming this ADR. The ADR must bind the exact category, the
exact previous caps, the exact approved caps, and the measurement the approved
caps were derived from; a binding that disagrees with the policy fails the
gate.

## Touch-point cap revision

- Touch point: `daemon_shutdown_deadline`
- Previous max files: `5`
- Previous max changed lines: `287`
- Approved max files: `6`
- Approved max changed lines: `320`
- Measured files: `6`
- Measured changed lines: `308`
- Policy revision: `patch-footprint.v3`
- Added path: `crates/tracedecay-daemon-service/src/invocation/types.rs`

This binding was approved under revision `patch-footprint.v2`. The approved
numbers above are unchanged; only the policy-revision field is restated,
because the checker binds a `cap_revision` to the policy revision currently in
force and [ADR-0014](./ADR-0014-host-hook-ingest-footprint-revision-v3.md)
adopted `patch-footprint.v3`.

## Consequences

- The bounded-lease-join fix becomes landable without an unmapped or
  over-budget upstream edit, and the `invocation/lsp.rs` convergence invariant
  becomes literally true instead of aspirational.
- The shutdown category keeps a tight local cap. At 308 of 320 lines it has
  almost no room left, so the next change in this seam has to come back through
  an ADR rather than drift.
- Touch-point caps stop being freely editable data. Raising one now requires
  editing the pinned table in the checker *and* registering an ADR that binds
  the exact numbers, which is the same shape of decision ADR-0011 imposed on
  the aggregate caps.
- One more upstream file must be re-applied on every sync train. The removal
  plan in the convergence map is the counterweight: the `types.rs` entry is
  deleted outright once upstream's lease registry retires its own tasks under a
  caller-supplied deadline.

## Rejected alternatives

- **Bound the lease join from `lsp.rs` and stay at five files.** Rejected
  because `LspLeaseTaskRegistry::shutdown` has already removed the tasks from
  the registry by the time the caller could time it out. Dropping that future
  drops the `JoinHandle`s it owns, and dropping a `JoinHandle` detaches the
  task rather than aborting it. The five-file version would report an unclean
  shutdown while leaving lease work running, which is a worse failure than the
  unbounded wait it replaces.
- **Bundle the cap increase into the implementation change.** Rejected because
  that is precisely what ADR-0011 invariant 2 forbids, and because a cap that
  is raised by the change it authorizes enforces nothing. The measured value
  being comfortably inside the new cap does not repeal the sequencing rule; an
  earlier bundled precedent does not either.
- **Amend ADR-0011 to exempt local touch-point caps.** Rejected because the
  local caps are the ones that actually bind — the aggregate v2 caps have
  thousands of lines of slack — so exempting them from the sequencing rule
  would hollow out the invariant while leaving its wording intact.
- **Raise the cap to the global per-category limit.** Rejected for the reason
  ADR-0011 gave for the aggregate caps: taking all 15 files the global cap
  allows would leave unbounded headroom, and a cap with unbounded headroom
  enforces nothing. Upstream growth has to stay a decision rather than a drift.

## Invariants

1. The `daemon_shutdown_deadline` touch point is capped at 6 files and 320
   changed lines, and the sixth path is exactly
   `crates/tracedecay-daemon-service/src/invocation/types.rs`.
2. Every touch-point-local cap is pinned in
   `scripts/product/check-patch-footprint-policy.py`; the policy and the pin
   must agree, so a cap cannot be raised by editing the policy alone.
3. A touch point whose caps were revised carries a `cap_revision` block bound
   to the approving ADR, and that ADR binds the exact category, previous caps,
   approved caps, and measurement. A touch point with no approved revision
   carries no `cap_revision` block.
4. An approved cap is at most roughly fifteen percent above the measurement it
   was derived from, so caps encode a measured tree rather than an aspiration.
5. No ADR-0011 aggregate cap, exception zone, product-owned path, dependency
   rule, or per-entry convergence-map `line_budget` is changed by this
   revision.
6. This revision lands before the implementation change that needs the
   headroom; the implementation is never the change that raises its own cap.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py` — validates the
  live diff against the pinned touch-point caps and the `cap_revision` binding.
- `python3 tests/product_patch_footprint_policy_test.py` — asserts a
  touch-point cap cannot be raised in the policy alone, that a `cap_revision`
  disagreeing with its ADR is rejected, and that an unapproved category cannot
  carry a `cap_revision`.
- `python3 scripts/product/check-foundational-adrs.py` and
  `python3 tests/product_foundational_adrs_test.py` — validate this ADR's
  registration, sections, and semantic content.
- Executable beads: `tdmem-0105` (the patch-footprint policy itself) and
  `tdmem-1204` (patch budget and invariant enforcement).

## Review triggers

Review when the `daemon_shutdown_deadline` category reaches ninety percent of
either approved cap, when upstream moves `LspLeaseTaskRegistry` out of
`invocation/types.rs`, when upstream's own lease registry starts consuming a
caller-supplied deadline (which retires the entry entirely), or when any other
touch point proposes a local cap increase.
