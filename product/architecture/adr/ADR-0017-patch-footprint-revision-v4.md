# ADR-0017: Revise the patch-footprint policy to v4 for the hook-route bridge and the online-only retention authority

Status: Accepted
Date: 2026-09-07

## Context

`product/upstream/patch-footprint-policy.json` revision `patch-footprint.v3` (ADR-0014)
sized every aggregate cap against the Claude Code host hook ingest journey at
the floor named by `product/upstream/tracedecay-v2-pr707.json`. The product
branch has since landed three slices that its convergence map did not
register, and the sync train that moves the floor to upstream head
`3b59bb7eb` refuses to stamp a tree whose upstream-owned files lack an active
entry or exceed a cap:

**The hook-route bridge** (`tdmem-1001`). A Claude Code hook published a valid
daemon-wide session route, and the very next projectless socket still fell
into the profile-only dispatcher and answered `tracedecay_grep requires an
initialized code project`. Fixing that needed four host-neutral pieces: the
hook notifier carries a request id and waits for `{"processed": true}` so
route publication is durable before the next tool call races it
(`daemon/core_hooks.rs`, `mcp/server/requests.rs`); route-only host metadata
is stripped from strict handler arguments after routing while the identity
view survives (`mcp/project_route.rs`); the CLI forwards that metadata
(`tracedecay-cli/src/tool_command/args.rs`); and a projectless socket carrying
explicit session or thread identity is served by the published project under
the projectless profile admission (`daemon/projectless.rs`,
`daemon/connection_serving.rs`). Its proof is in-process tests and one
physical cold journey through the shipped binary.

**The online-only retention authority** (`tdmem-1100`). Code-generation
retention swept under an offline protection set when no semantic runtime was
seated and deleted a live vector source. Deletion is now authorized only by an
exact vector inventory, the default-off state defers quietly, the vector writer
lane is bound to the durable code scope so a remount cannot bypass a live
fence, and the blocking deletion owns that fence
(`daemon/store_maintenance/mod.rs`, `daemon/maintenance/generation.rs`,
`tracedecay-usecases/src/semantic_runtime/production.rs`, and their tests).

**Two smaller repairs.** Upstream's feature-gated `dashboard.rs` reads fields
that `3b59bb7eb` made private; the product tree calls the accessors so every
shipped feature compiles. The Codex host lifecycle acceptance test gains an
executable fake Codex CLI fixture, and `agents/claude.rs` decides
non-interactive install readiness from Claude's loaded plugin cache instead of
the marketplace source, which reported Ready while a stale cache was loaded.
That file sits in the `host_specific_adapters` zone.

Measured against upstream head `3b59bb7eb` after the orphaned
pre-transplant observation deltas were dropped, the tree is **49
upstream production files**, 16 upstream test/fixture files,
**5594 total upstream changed lines**, a largest single-file delta of
723 lines (`crates/tracedecay/src/daemon/project_composition.rs`), and
**5 exception-zone files**. The twenty upstream files these slices touch
measure:

| File | Changed lines | Category |
| --- | ---: | --- |
| `crates/tracedecay/src/daemon/projectless.rs` | 94 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/connection_serving.rs` | 20 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/core_hooks.rs` | 30 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/server/requests.rs` | 16 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/project_route.rs` | 157 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/server/host_admission_tests.rs` | 105 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/tests.rs` | 1 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/tests/projectless.rs` | 297 | `hook_route_bridge` |
| `crates/tracedecay-cli/src/tool_command/args.rs` | 17 | `hook_route_bridge` |
| `crates/tracedecay-cli/src/tool_command/tests.rs` | 38 | `hook_route_bridge` |
| `crates/tracedecay/tests/daemon_suite/indexing_lifecycle_test.rs` | 129 | `hook_route_bridge` |
| `crates/tracedecay/tests/daemon_suite/code_index_journey.rs` | 6 | `hook_route_bridge` |
| `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance/native_plugin_fixture.rs` | 95 | `host_hook_ingest` |
| `crates/tracedecay-agent-hosts/src/agents/claude.rs` | 51 | `exception` |
| `crates/tracedecay/src/daemon/store_maintenance/mod.rs` | 122 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/maintenance/generation.rs` | 11 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/store_maintenance/vector_retention_tests.rs` | 135 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/production_harness/generation_retention_test.rs` | 134 | `vector_retention_authority` |
| `crates/tracedecay-usecases/src/semantic_runtime/production.rs` | 48 | `vector_retention_authority` |
| `crates/tracedecay/src/dashboard.rs` | 10 | `feature_gated_build_repair` |

## Decision

We adopt policy revision `patch-footprint.v4`. Every aggregate cap is re-measured at this
tree and set by the ADR-0011 rule for setting caps — the measurement it was
derived from plus at most roughly fifteen percent headroom:

| Cap | v3 | v4 | Measured now |
| --- | ---: | ---: | ---: |
| upstream existing production files | 37 | 56 | 49 |
| upstream existing test/fixture files | 9 | 18 | 16 |
| total upstream changed lines | 3500 | 6430 | 5594 |
| changed lines per upstream file | 560 | 830 | 723 |
| exception-zone files without ADR/policy revision | 4 | 5 | 5 |

Composition-root files (15), files per allowed touch point (15), exception
files per ADR (2), workspace manifest files (2), and
`manual_generated_file_edits` (0) are unchanged. Per-entry `line_budget`
values in `product/upstream/convergence-map.json` remain the binding limit for
an individual file; eight existing entries whose measured delta grew past
their budget at this head are re-budgeted by the same rule.

We approve three new touch points and widen one, each capped at its
measurement plus at most fifteen percent:

| Touch point | Max files | Max changed lines | Measured files | Measured lines |
| --- | ---: | ---: | ---: | ---: |
| `hook_route_bridge` | 12 | 1046 | 12 | 910 |
| `vector_retention_authority` | 5 | 517 | 5 | 450 |
| `feature_gated_build_repair` | 1 | 11 | 1 | 10 |
| `host_hook_ingest` | 4 | 486 | 4 | 423 |

The `hook_route_bridge` touch point is capped at 12 files and 1046 changed
lines. It permits acknowledging a hook event only after its effect completed,
stripping route-only metadata after routing, selecting a published route for a
projectless reader under profile admission, forwarding that metadata through
the CLI gate, and testing the bridge in process and through the shipped
binary. It forbids creating routes outside the hook publication path, serving
a route across profiles, stripping a declared tool field, naming a provider
type on the path, and blocking a hook client beyond the existing timeout.

The `vector_retention_authority` touch point is capped at 5 files and 517 changed
lines. It permits the typed `SemanticUnseated` outcome, deleting the offline
protection set, scope-bound writer lanes with executor-owned fences, and the
tests that prove them. It forbids deleting without an exact inventory,
treating a missing inventory as empty, new locks or polls as synchronization,
and any change to the fail-closed authorities.

The `feature_gated_build_repair` touch point is capped at 1 file and 11 changed
lines and permits only replacing a private field access with the accessor
upstream introduced, inside a file upstream compiles only under a feature.

`host_hook_ingest` grows to 4 files and 486 changed lines to admit the
executable Codex fixture beside the acceptance test it serves.

We approve a **one-file exception in the `host_specific_adapters` zone** for
`crates/tracedecay-agent-hosts/src/agents/claude.rs`, and no other. The
zone's default stays `forbidden` and its reason is unchanged; readiness for a
Claude non-interactive install is decided nowhere but inside this adapter, and
the change reuses the existing bundle digest and discovery validators. The
per-ADR cap of 2 keeps this ADR from being stretched to a second file.

Re-measured caps that grow without a new touch point follow the same rule:
`daemon_composition_mount` 1094 lines (measured 952),
`configuration_registry_mount` 718 lines (measured 625),
`production_harness_shutdown` 106 lines (measured 93), and
`workspace_wiring` 163 lines (measured 142).

Finally, the `daemon_shutdown_deadline` `cap_revision` block ADR-0016 approved
is restated under the revision now in force: its numbers are unchanged (8
files and 420 changed lines, measured 8 and 416) and only its
`policy_revision` field and the matching line in ADR-0016 move from
`patch-footprint.v3` to `patch-footprint.v4`, exactly as ADR-0014 did for ADR-0013.

Raising a cap again requires another ADR. A cap increase is approved by ADR
before the change that needs it and is
never bundled into the change that exceeds the previous cap; this revision is the policy slice that the sync
train's floor advancement to `3b59bb7eb` depends on, and it lands on its own.

## Consequences

- Every upstream-owned file the product tree changes against `3b59bb7eb`
  has an active convergence entry with a measured budget, so the sync train
  can stamp and gate the transplant candidate.
- The hook-route bridge, the online-only retention authority, and the build
  repair are reviewable as named seams with their own allowed and forbidden
  changes instead of unclassified residue.
- Five exception-zone files are now admitted; a sixth needs a new ADR.
- Each new test claim is also a gate-lane command, so the train runs it
  against the candidate tree before publishing the stamps.

## Rejected alternatives

- **Register the bridge and retention work as fresh per-touch-point cap revisions under v3.** Rejected because the aggregate caps (production files,
  total lines, per-file lines, exception-zone files) are all exceeded as
  well; a revision that re-measures every cap once is the mechanism ADR-0011
  and ADR-0014 established for exactly this case.
- **Move the bridge, retention, and repair code product-side to stay inside the v3 caps.** Rejected because each change edits behavior that exists only
  in upstream-owned files (the daemon transport, the MCP router, the
  retention pass, the CLI gate); a product-side copy would fork them.
- **Keep deciding Claude readiness from the marketplace source to avoid a fifth exception-zone file.** Rejected because that decision reported Ready
  while Claude was loading a stale cache, which is the defect being fixed.

## Invariants

1. Every v4 cap equals a measurement taken at this tree against upstream head
   `3b59bb7eb` plus at most roughly fifteen percent headroom.
2. The three new touch points and the widened `host_hook_ingest` name exact
   paths; no touch point admits a glob over upstream code.
3. The `host_specific_adapters` zone keeps its `forbidden` default; this ADR
   admits exactly one file in it.
4. Each convergence entry added under this revision declares tests that a
   gate lane in `product/upstream/sync-policy.json` runs and the bound
   workflow job mirrors.
5. The `daemon_shutdown_deadline` cap_revision numbers approved by ADR-0016
   are unchanged; only the revision label moves.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py`
- `python3 tests/product_patch_footprint_policy_test.py`
- `python3 scripts/product/check-upstream-ownership-registry.py --repo .`
- `python3 tests/product_upstream_ownership_registry_test.py`
- `python3 scripts/product/check-foundational-adrs.py`
- `python3 tests/product_foundational_adrs_test.py`
- Executable beads: `tdmem-1001`, `tdmem-1206`, and `tdmem-1208`.

## Review triggers

- Upstream adopts acknowledged hook delivery, route-only metadata stripping,
  or an online-only retention authority: retire the matching area and drop
  its touch point in the next revision.
- A measured category exceeds its v4 cap: approve the increase by ADR before
  the change that needs it.
- A sixth file needs the `host_specific_adapters` zone: a new ADR, never a
  widened default.
