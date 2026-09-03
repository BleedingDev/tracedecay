# ADR-0014: Revise the patch-footprint policy to v3 for the Claude Code host hook ingest journey

Status: Accepted
Date: 2026-09-03

## Context

`product/upstream/patch-footprint-policy.json` revision `patch-footprint.v2`
(ADR-0011) sized every aggregate cap against the M4 observation-journey mount,
the M5 cognitive-recall port, the Native configuration registration, and the
session-sync exact-scope reuse. Its scope clause states the rule for growth in
the same words v1 used: "Later phases must either remain inside these caps or
approve a new policy revision by ADR."

M9 (`tdmem-1000`) proves real coding-agent host journeys, and its first bead,
`tdmem-1001` ("Integrate and test Claude Code memory hooks"), does not fit
inside the v2 caps. Three things drive that.

**The Claude hook boundary is the only place a project session becomes
evidence in time.** `crates/tracedecay-agent-hosts/src/hooks/claude.rs` already
runs a profile-scoped transcript catch-up on `SessionStart` and `Stop`, but the
profile route narrows its source with `ClaudeSource::for_user_scope`, which
keeps exactly the rows belonging to *no* registered project. A Claude session
running inside a registered project therefore produces canonical project
observations only when the daemon's own session-sync sweep next runs, so
everything reading the project's observations — the advisory memory lane
included — lags a whole daemon lifetime behind the session that produced them.
Closing that gap needs a bounded, fail-open catch-up at the two lifecycle
events (5s on `SessionStart`, which is on the critical path of the first turn;
25s on `Stop`, which runs at a turn boundary inside Claude's default 60s hook
guard), a stable reader on the private `TranscriptIngestOutcome` so a fail-open
caller can report what it lost
(`crates/tracedecay-agent-hosts/src/hooks/mod.rs`), and a project-scoped
capture kernel beside the profile one
(`crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs`).

**Two of those three files sit in a zero-touch zone.**
`crates/tracedecay-agent-hosts/**` is the `host_specific_adapters` exception
zone, whose default policy is `forbidden` because host adapters are not
provider mounts. The zone's reason still holds and is not being relaxed: the
hook change adds no provider behavior, mounts nothing, and calls the same
host-neutral ingest route the module already uses. But it is an edit inside a
forbidden zone, so it needs ADR evidence and an exception-zone cap that admits
it. The v2 cap `default_max_exception_zone_files` is 2, already consumed by the
two additive configuration-registry files ADR-0012 approved.

**The journey needs a runnable proof, and one of its files is upstream-owned.**
`crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs` is
a new 761-line product test: product-owned by intent, exactly like the existing
`crates/tracedecay/tests/product_memory_provider_*.rs` journeys, and therefore
classified through `product_owned_paths` rather than charged to the upstream
budget. Its target declaration and the opt-in `memory-provider-host` feature it
requires must live in the upstream-owned `crates/tracedecay-cli/Cargo.toml`,
and the neighbouring `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs`
gains the repeated-install idempotence and interrupted-reinstall rollback
assertions the hook mount relies on.

Measured against the pinned floor `5749e4fcfe268e17bd19a0e6ef90c646f7b37289`
with the product journey test classified product-owned, the tree is **37
upstream production files**, 7 upstream test/fixture files, **3393 total
upstream changed lines**, and **4 exception-zone files**. The five upstream
files this bead adds measure:

| File | Changed lines | Category |
| --- | ---: | --- |
| `crates/tracedecay-agent-hosts/src/hooks/claude.rs` | 72 | exception |
| `crates/tracedecay-agent-hosts/src/hooks/mod.rs` | 13 | exception |
| `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs` | 52 | `host_hook_ingest` |
| `crates/tracedecay-cli/Cargo.toml` | 13 | `host_hook_ingest` |
| `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs` | 123 | `host_hook_ingest` |
| **Total** | **273** | |

## Decision

We adopt policy revision `patch-footprint.v3`. It carries every v2 cap forward
unchanged except the three the host hook ingest journey moves, and each new cap
follows the ADR-0011 rule for setting caps — the measurement it was derived
from plus at most roughly fifteen percent headroom:

| Cap | v2 | v3 | Measured now | Headroom |
| --- | ---: | ---: | ---: | ---: |
| upstream existing production files | 34 | 37 | 37 | 0% |
| total upstream changed lines | 3300 | 3500 | 3393 | 3.2% |
| exception-zone files without ADR/policy revision | 2 | 4 | 4 | 0% |

Every other v2 cap is untouched: upstream existing test/fixture files stays 9,
changed lines per upstream file stays 560, composition-root files stays 15,
files per allowed touch point stays 15, exception files per ADR stays 2,
workspace manifest files stays 2, and `manual_generated_file_edits` stays zero.
Per-entry `line_budget` values in `product/upstream/convergence-map.json`
remain the binding limit for an individual file; these caps are the aggregate
backstop behind them.

We approve one new touch point, `host_hook_ingest`, at **3 files and 200
changed lines** (measured 188, 6.4% headroom), covering exactly
`crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs`,
`crates/tracedecay-cli/Cargo.toml`, and
`crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs`. It permits a
bounded call into the existing host-neutral ingest route from a host lifecycle
event, an additional project-scoped capture kernel for a host that already has
a profile-scoped one, an opt-in default-off product test target with the
feature it needs, and host-lifecycle idempotence and rollback assertions. It
forbids naming a provider, registry, or fabric type on that path, letting an
ingest outcome change the host's own answer, unbudgeted ingest work, a second
admission or scope-identity derivation beside the session-sync worker's, and
turning the product target or its feature on by default.

We approve a **two-file exception in the `host_specific_adapters` zone** for
`crates/tracedecay-agent-hosts/src/hooks/claude.rs` and
`crates/tracedecay-agent-hosts/src/hooks/mod.rs`, and no others. The zone's
default stays `forbidden` and its reason is unchanged; two named files are
admitted because a Claude lifecycle event is observable nowhere else in this
repository and no seam above the host adapter exists to attach a listener to.
The per-ADR cap of 2 keeps this ADR from being stretched to a third file.

We also classify
`crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs` as
product-owned by adding `crates/tracedecay-cli/tests/product_memory_provider_*.rs`
to `product_owned_paths`, matching the existing
`crates/tracedecay/tests/product_memory_provider_*.rs` pattern. This is a
classification of a new product-owned test, not a widening of upstream reach:
the pattern admits only files whose name already marks them as product journeys
in the CLI crate's test directory, and it removes 761 lines from the upstream
totals that were never upstream work.

Finally, the `daemon_shutdown_deadline` `cap_revision` block ADR-0013
introduced is restated under the revision now in force. Its approved numbers
are unchanged — 6 files and 320 changed lines, measured 6 and 308 — and only
its `policy_revision` field and the matching line in ADR-0013 move from
`patch-footprint.v2` to `patch-footprint.v3`, because the checker binds a
`cap_revision` to the policy revision currently in force rather than to the one
that was current when the cap was approved.

Raising a cap again requires another ADR.

## Consequences

- `tdmem-1001` becomes landable without an unmapped, unclassified, or
  over-budget upstream edit, and a Claude session inside a registered project
  stops waiting a daemon lifetime to become canonical project observations.
- The gate keeps its teeth. At 3393 of 3500 lines and 37 of 37 production
  files, the next upstream file this program touches trips the checker and
  forces this conversation again — which is the intended cost of the file-count
  cap being set exactly at the measurement.
- The forbidden host-adapter zone now has a precedent, which is the risk this
  ADR accepts. It is bounded three ways: the zone default stays `forbidden`,
  the exception names two exact files rather than a pattern, and the per-ADR
  cap of 2 means a third host-adapter file needs its own ADR and its own
  argument.
- Sequencing is the honest weak point and is called out rather than glossed.
  ADR-0011 invariant 2 requires a cap increase to be approved before the change
  that needs it and never bundled into it. The host hook ingest implementation
  is already present in this working tree — that is what makes the measurement
  above possible — so this revision is authored as a policy-only change (this
  ADR, its manifest registration, the policy JSON and prose, the pinned caps
  and pinned product patterns in the checkers, and the convergence-map area and
  entries) and must be committed ahead of the implementation commit it
  authorizes. A later commit that both changes a cap and consumes it should be
  rejected in review.
- One more upstream category and two more zero-touch-zone files must be
  re-applied on every sync train. The convergence map carries the counterweight:
  each of the five entries records a removal plan, and the two host-adapter
  entries are deleted outright if upstream's own hook path gains a
  project-scoped catch-up.

## Rejected alternatives

- **Wait for the daemon's own session-sync sweep instead of ingesting at the hook boundary.**
  Rejected because the sweep is not synchronized with the
  agent's turn: the evidence a session just produced is exactly the evidence
  the next turn needs, and a lane that answers from a sweep-old view of the
  project is worse than no lane. This alternative also does not avoid the
  upstream footprint — the project-scoped kernel is still required for the
  sweep to cover project sessions at all.
- **Add a product-owned hook binary or host adapter beside the upstream one.**
  Rejected because Claude invokes exactly the hook commands its settings
  register, so a second adapter means either rewriting the operator's hook
  configuration to point at a product binary or running two adapters over one
  lifecycle event. Both are larger, more fragile intrusions into host state
  than two additive call sites, and both duplicate the parse, telemetry, and
  dispatch sequence the upstream handler already owns.
- **Widen the profile-scoped Claude kernel to also cover registered projects.**
  Rejected because `ClaudeSource::for_user_scope` narrowing is the profile
  route's contract, not an oversight: rows belonging to no registered project
  are precisely what profile scope means. Widening it would change the meaning
  of an established route for every existing caller instead of adding a new one
  beside it, which is a larger semantic change to upstream than the additive
  registration this ADR approves.
- **Make the catch-up ingest blocking or unbounded so no frame can be missed.**
  Rejected because a hook that can hang holds the agent's lifecycle event open,
  and Claude's hook guard would kill it anyway. Bounded and fail-open with a
  reported failure reason converges on the same state through content-derived
  idempotency, because the next hook or the daemon's own sweep picks up the
  tail that did not fit.
- **Leave the v2 caps and let the gate fail.** Rejected for the reason ADR-0011
  gave: a gate that is known to fail stops being read, real violations hide
  among the accepted ones, and no sync train can run.
- **Raise the caps far above the measurement to avoid future ADRs.** Rejected
  because a cap with unbounded headroom enforces nothing. The file-count cap is
  deliberately set at the measurement with zero headroom, so the next upstream
  file is a decision rather than a drift.

## Invariants

1. Caps are set from a measurement of this revision's tree — 37 upstream
   production files, 3393 total upstream changed lines, 4 exception-zone
   files — never from a guess about future work, and no cap exceeds its
   measurement by more than roughly fifteen percent.
2. A cap increase is approved by ADR before the change that needs it, and is
   never bundled into the change that exceeds the previous cap. This revision
   is a policy-only change and must be committed ahead of the implementation it
   authorizes.
3. The `host_specific_adapters` zone keeps its `forbidden` default and its
   reason. Exactly two exact files are admitted by this ADR —
   `crates/tracedecay-agent-hosts/src/hooks/claude.rs` and
   `crates/tracedecay-agent-hosts/src/hooks/mod.rs` — and a third host-adapter
   file requires its own ADR.
4. The `host_hook_ingest` touch point is capped at 3 files and 200 changed
   lines, and its three paths are exactly the ingest kernel table, the
   `tracedecay-cli` manifest, and the host lifecycle acceptance test.
5. The hook boundary is fail-open and bounded: no ingest outcome changes the
   host's own hook answer, and every catch-up runs under an explicit time and
   byte budget.
6. Adding `crates/tracedecay-cli/tests/product_memory_provider_*.rs` to
   `product_owned_paths` classifies product-owned test files only. No upstream
   source, manifest, or non-`product_memory_provider` test is hidden by it, and
   the broad-pattern prohibition on `crates/**`, `crates/tracedecay/**`,
   `tests/**`, and `.github/**` is unchanged.
7. Every other v2 cap, exception zone, dependency-direction rule, and per-entry
   convergence-map `line_budget` is unchanged by this revision, and the pinned
   upstream floor is unchanged.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py` — validates the
  live diff against the v3 caps, the pinned `host_hook_ingest` caps, the
  restated `cap_revision` binding, and the two-file exception evidence.
- `python3 scripts/product/check-upstream-ownership-registry.py` — every
  changed path stays classified, the new upstream area is bounded by its touch
  points, and the CLI journey test resolves to exactly one product area.
- `python3 tests/product_patch_footprint_policy_test.py` and
  `python3 tests/product_upstream_ownership_registry_test.py` — assert the caps
  and the canonical product pattern set cannot be loosened without changing the
  checkers' pinned expectations.
- `python3 scripts/product/check-foundational-adrs.py` and
  `python3 tests/product_foundational_adrs_test.py` — validate this ADR's
  registration, sections, and semantic content.
- Executable beads: `tdmem-0105` (the patch-footprint policy itself),
  `tdmem-1001` (Claude Code memory hook integration, the work this revision
  authorizes), and `tdmem-1204` (semantic invariants and parity tests for every
  upstream touch point).

## Review triggers

Review when the measured footprint reaches ninety percent of any v3 cap — the
production-file count is already at one hundred percent, so any additional
upstream production file triggers this immediately — when a second
host-adapter exception is proposed, when another M9 host bead (`tdmem-1002`
Codex, `tdmem-1003` Cursor) proposes its own hook mount rather than reusing
this one, when upstream's own Claude hook path gains a project-scoped catch-up
(which retires the two zone entries entirely), or when a sync train's conflict
receipts show the host-adapter patch is no longer cleanly separable.
