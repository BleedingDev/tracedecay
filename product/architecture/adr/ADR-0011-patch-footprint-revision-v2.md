# ADR-0011: Revise the patch-footprint policy to v2 for the mounted memory milestones

Status: Accepted
Date: 2026-09-02

## Context

`product/upstream/patch-footprint-policy.json` revision `patch-footprint.v1`
(bead `tdmem-0105`) caps how far product work may reach into Zack-owned
upstream files. Its own scope clause states the rule for growth: "Later phases
must either remain inside these caps or approve a new policy revision by ADR."

The v1 caps were sized for M0 through M3, when the provider boundary was a
dormant mount: an application contract, a workspace wiring, and a composition
seam that constructed nothing. Milestones M4, M5, and M8 turned that dormant
seam into running production capability, and the upstream footprint grew with
it in four identifiable places:

1. **M4 — the mounted observation journey.** `project_composition.rs` now
   mounts `observation_journey::mount_and_replay`, threads the project-open
   `CancellationToken` and the profile identity into it, and maps a cancelled
   replay onto the project-open cancellation error;
   `crates/tracedecay/src/mcp/server/connection.rs` carries the shutdown
   deadline into `journey.shutdown(deadline)`; `production_harness.rs` waits
   for the journey during harness shutdown.
2. **M5 — the cognitive recall read path.** `tracedecay-application`'s
   `memory/recall.rs` gained the `CognitiveRecallPort` contract and its
   errors, with `tests/cognitive_recall_port.rs` proving them; `mcp/server.rs`
   and `mcp/server/construction.rs` expose the per-session recall port from
   the project server.
3. **Native provider configuration.** A project setting only exists if the
   upstream configuration registry registers it, so
   `memory.provider_native_enabled.v1` and the recall-routing setting reach
   through `tracedecay-domain/src/configuration.rs`,
   `tracedecay-global-db/src/configuration/{registry,store}.rs`,
   `tracedecay-usecases/src/config/mod.rs`, and `crates/tracedecay/src/config.rs`.
4. **Exact-scope reuse in session sync.** `session_sync.rs` and its submodules
   now consume the `ResolvedScope` resolved once at project open instead of
   re-deriving repository/worktree/branch identity at sync time — the
   `tdmem-0104` authority-matrix invariant that identity is never inferred
   from the working directory at request time.

Measured at this revision's tree, the footprint is 30 upstream production
files, 7 upstream test/fixture files, 2859 changed lines, and 13
composition-root files, with `project_composition.rs` at 484 lines and
`tracedecay-application/src/memory/recall.rs` at 549.

## Decision

Adopt policy revision `patch-footprint.v2`. Set each hard cap to the footprint
measured at this revision's tree plus at most roughly fifteen percent
headroom — enough that an in-flight bead in the same seam does not trip the
gate, and not enough to absorb a new unplanned mount:

| Cap | v1 | v2 | Measured now |
| --- | --- | --- | --- |
| upstream existing production files | 12 | 34 | 30 |
| upstream existing test/fixture files | 6 | 9 | 7 |
| total upstream changed lines | 900 | 3300 | 2859 |
| changed lines per upstream file | 180 | 560 | 549 |
| composition-root files | 6 | 15 | 13 |
| files per allowed touch point | 5 | 15 | 13 |

Exception-zone caps, dependency-direction rules, `product_owned_paths`, the
`manual_generated_file_edits` zero, and every v1 principle are unchanged: the
growth is in how much of the accepted seams the product uses, never in which
seams it may touch. Per-entry `line_budget` values in
`product/upstream/convergence-map.json` remain the binding limit for an
individual file; these caps are the aggregate backstop behind them.

Raising a cap again requires another ADR. A cap is never raised in the same
change that needs the headroom.

## Consequences

- The convergence train (`tdmem-1208`) and the CI gates that embed the
  footprint checker can pass at the current tree, so upstream sync work is no
  longer blocked behind an unrevised policy.
- The gate keeps its teeth for the next milestone: at 2859 of 3300 lines, M6
  (NCM adapter) and M9 (host journeys) cannot silently add another large
  upstream mount without tripping it and forcing this conversation again.
- The measured-plus-headroom rule means the caps encode a real state of the
  tree rather than an aspiration, so a reader can tell what the product
  actually costs upstream.
- Rebasing onto a moving upstream stays proportionally harder than it was at
  M3, which is the honest price of mounting the journey and the recall port.

## Rejected alternatives

- **Leave the v1 caps and let the gate fail.** Rejected because a gate that is
  known to fail stops being read; real new violations would hide among the
  accepted ones, and no sync train could ever run.
- **Raise the caps far above the measurement to avoid future ADRs.** Rejected
  because a cap with unbounded headroom enforces nothing, and the whole point
  of the policy is that upstream growth is a decision rather than a drift.
- **Move the mounts into product-owned crates to stay inside v1.** Rejected
  because the composition root, the configuration registry, and the MCP server
  are upstream-owned by construction: a project cannot be opened, a setting
  cannot exist, and a session port cannot be exposed from a product crate. The
  additive-only shape is already the minimum reach.
- **Delete the per-entry line budgets and keep only the aggregate caps.**
  Rejected because per-file budgets are what make an individual unexplained
  diff visible; the aggregate alone would let one file absorb the whole budget.

## Invariants

1. Caps are set from a measurement of the revision tree, never from a guess
   about future work.
2. A cap increase is approved by ADR before the change that needs it, and is
   never bundled into the change that exceeds the previous cap.
3. Which files product work may touch (`allowed_touch_points`,
   `exception_zones`, `product_owned_paths`) is unchanged by this revision.
4. Every upstream-owned changed file still needs exactly one active
   convergence-map entry, and per-entry line budgets stay binding.
5. Generated outputs are still never hand-patched
   (`manual_generated_file_edits` remains zero).
6. The pinned upstream floor is unchanged by this revision.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py` — validates the
  live diff against the pinned floor under the v2 caps.
- `python3 tests/product_patch_footprint_policy_test.py` — asserts the caps
  cannot be loosened without changing the checker's expected budget.
- `python3 scripts/product/check-upstream-ownership-registry.py` — every
  changed upstream path stays classified.
- Executable beads: `tdmem-0105` (the policy itself), `tdmem-1203` and
  `tdmem-1204` (patch budget and invariant enforcement), `tdmem-1208` (the
  first full convergence-train rehearsal, which this revision unblocks).

## Review triggers

Review when the measured footprint reaches ninety percent of any v2 cap, when
M6 or M9 proposes a new upstream mount rather than more use of an existing
one, when upstream restructures one of the accepted seams, or when a sync
train's conflict receipts show the product patch is no longer cleanly
separable.
