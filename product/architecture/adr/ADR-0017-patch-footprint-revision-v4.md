# ADR-0017: Revise the patch-footprint policy to v4 for the hook-route bridge and the online-only retention authority

Status: Accepted
Date: 2026-09-07
Revised: 2026-09-08 (complete pinned-head repair reconciliation)

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
shipped feature compiles, and under `--features hotpath,hotpath-mcp` three
upstream crates exceed rustc's default query depth in instrumented async test
bodies, so they carry a crate-level `recursion_limit` attribute. The Codex host lifecycle acceptance test gains an
executable fake Codex CLI fixture, and `agents/claude.rs` decides
non-interactive install readiness from Claude's loaded plugin cache instead of
the marketplace source, which reported Ready while a stale cache was loaded.
That file sits in the `host_specific_adapters` zone.

Measured against the exact upstream commit
`3b59bb7eb0b10b4e7a8efc668e793ef7a1d11072`, including the complete dirty
worktree, the repair is **54 upstream production files**, **23 upstream
test/fixture files**, **5715 total upstream changed lines**, a largest
single-file delta of **723 lines**
(`crates/tracedecay/src/daemon/project_composition.rs`), and **6
exception-zone files**. The earlier v4 measurement (52/18/5623/largest723)
was incomplete and is superseded, not used as a new baseline.

The measurement uses the footprint checker's classification and dirty-layer
maximum, not `HEAD` or the accepted floor. For this snapshot every measured
upstream path has a real pinned-head diff and its dirty-layer maximum equals
that diff; there are no extra staged-only or unstaged-only upstream paths.
`is_test_or_fixture` classifies by path, not by Rust item: `runtime_ports.rs`
and files named `tests.rs` under `src` count as production, while
`plugin_conformance_tests.rs` and `invocation_tests/lsp_tests.rs` count as tests.
Product-owned NCM runtime and observation recovery/volume-soak tests remain
product-owned and are not added to these upstream totals. Formatting-only
changes in already mapped files are measured too; there is no exemption.

**Fixture reconciliation.** The existing `integration_test_runtime_isolation`
seam now includes canonical path identity, authoritative project-ID search,
fixture initialization through the owned daemon, virtual time limited to the
lease TTL, single application-envelope decoding, and exact unavailable-daemon
Claude Stop diagnostics. The reverse registered-tool prose inventory and
obsolete Hermes command-prose marker inventory are removed, but mentioned-tool
validity, the readOnly allowlist, exact installed-template equality, and all
executable behavioral assertions remain. Test-body and fixture repairs are
explicitly authorized; the old compilation-only claim was false.

**Supersession reconciliation.** At this pin, upstream already joins started
superseded work once while retaining its permit, without a reaper deadline.
The downstream second timed reaper is deleted because it could poll an
already-completed async future again. Owner cancellation can preempt the
upstream join. Ordinary owner retirement does not invent a completion terminal;
a waiting supersession is different: settlement retains the superseded flag
and may emit that receipt even when retirement preempts the join. The blocking
regression exercises the live-owner settlement ordering through fixture-local
virtual time and notifications, not by witnessing a production reaper timer.

The complete measured upstream inventory follows. “Previous budget” means the
pre-reconciliation v4 entry budget, including already-dirty hotpath and host
fixture approvals; “new” means no prior active entry. Every resulting budget
is at least the measurement and at most its integer fifteen-percent headroom.
Retired entries remain history and grant no current authority.

| File | Kind | Changed lines | Previous budget | Approved budget | Touch point |
| --- | --- | ---: | ---: | ---: | --- |
| `Cargo.lock` | production | 131 | 180 | 150 | `workspace_wiring` |
| `Cargo.toml` | production | 11 | 80 | 12 | `workspace_wiring` |
| `crates/tracedecay-agent-hosts/src/agents/claude.rs` | production | 51 | 58 | 58 | `exception` |
| `crates/tracedecay-agent-hosts/src/hooks/claude.rs` | production | 113 | 129 | 129 | `exception` |
| `crates/tracedecay-agent-hosts/src/hooks/mod.rs` | production | 44 | 50 | 50 | `exception` |
| `crates/tracedecay-application/src/memory.rs` | production | 2 | 4 | 2 | `cognitive_recall_contract` |
| `crates/tracedecay-application/src/memory/recall.rs` | production | 549 | 560 | 560 | `cognitive_recall_contract` |
| `crates/tracedecay-application/tests/cognitive_recall_port.rs` | test | 265 | 300 | 300 | `cognitive_recall_contract` |
| `crates/tracedecay-cli/Cargo.toml` | production | 17 | 20 | 19 | `host_hook_ingest` |
| `crates/tracedecay-cli/src/tool_command/args.rs` | production | 17 | 19 | 19 | `hook_route_bridge` |
| `crates/tracedecay-cli/src/tool_command/tests.rs` | production | 36 | 43 | 41 | `hook_route_bridge` |
| `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance.rs` | test | 259 | 297 | 297 | `host_hook_ingest` |
| `crates/tracedecay-cli/tests/host_lifecycle_cli_acceptance/native_plugin_fixture.rs` | test | 95 | 109 | 109 | `host_hook_ingest` |
| `crates/tracedecay-configuration/src/config/mod.rs` | production | 252 | 289 | 289 | `configuration_registry_mount` |
| `crates/tracedecay-daemon-service/src/invocation/lsp.rs` | production | 14 | 16 | 16 | `daemon_shutdown_deadline` |
| `crates/tracedecay-daemon-service/src/invocation/types.rs` | production | 33 | 50 | 37 | `daemon_shutdown_deadline` |
| `crates/tracedecay-daemon-service/src/project_runtime/shutdown.rs` | production | 168 | 180 | 180 | `daemon_shutdown_deadline` |
| `crates/tracedecay-domain/src/configuration.rs` | production | 150 | 172 | 172 | `configuration_registry_mount` |
| `crates/tracedecay-global-db/src/configuration/registry.rs` | production | 55 | 62 | 62 | `exception` |
| `crates/tracedecay-global-db/src/configuration/store.rs` | production | 9 | 20 | 10 | `exception` |
| `crates/tracedecay-global-db/src/lib.rs` | production | 5 | 6 | 5 | `exception` |
| `crates/tracedecay-host-admission/src/lib.rs` | production | 5 | 6 | 5 | `feature_gated_build_repair` |
| `crates/tracedecay-host-admission/tests/host_capture_background_cpu_admission.rs` | test | 5 | 6 | 5 | `feature_gated_build_repair` |
| `crates/tracedecay-session-runtime/src/session_sync.rs` | production | 5 | 5 | 5 | `daemon_composition_mount` |
| `crates/tracedecay-session-runtime/src/session_sync/git_topology.rs` | production | 10 | 10 | 10 | `daemon_composition_mount` |
| `crates/tracedecay-session-runtime/src/session_sync/project_lifecycle.rs` | production | 4 | 4 | 4 | `daemon_composition_mount` |
| `crates/tracedecay-session-temporal-store/src/lib.rs` | production | 4 | 6 | 4 | `feature_gated_build_repair` |
| `crates/tracedecay-usecases/src/diagnostics_query.rs` | production | 115 | 115 | 115 | `recall_context_mount` |
| `crates/tracedecay-usecases/src/diagnostics_store.rs` | production | 36 | 36 | 36 | `recall_context_mount` |
| `crates/tracedecay-usecases/src/primitives/production.rs` | production | 3 | 84 | 3 | `recall_context_mount` |
| `crates/tracedecay-usecases/src/primitives/runtime.rs` | production | 53 | 53 | 53 | `recall_context_mount` |
| `crates/tracedecay-usecases/src/semantic_runtime/production.rs` | production | 48 | 55 | 55 | `vector_retention_authority` |
| `crates/tracedecay/Cargo.toml` | production | 27 | 40 | 31 | `daemon_composition_mount` |
| `crates/tracedecay/src/config.rs` | production | 19 | 60 | 21 | `configuration_registry_mount` |
| `crates/tracedecay/src/config/tests.rs` | production | 204 | 234 | 234 | `configuration_registry_mount` |
| `crates/tracedecay/src/daemon/bootstrap.rs` | production | 84 | 84 | 84 | `daemon_shutdown_deadline` |
| `crates/tracedecay/src/daemon/connection_serving.rs` | production | 20 | 23 | 23 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/core_hooks.rs` | production | 30 | 34 | 34 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/engine/shutdown.rs` | production | 4 | 8 | 4 | `daemon_shutdown_deadline` |
| `crates/tracedecay/src/daemon/invocation_state.rs` | production | 11 | 16 | 12 | `daemon_shutdown_deadline` |
| `crates/tracedecay/src/daemon/invocation_tests/lsp_lease_tests.rs` | test | 25 | 25 | 25 | `daemon_shutdown_deadline` |
| `crates/tracedecay/src/daemon/invocation_tests/lsp_tests.rs` | test | 6 | new | 6 | `integration_test_runtime_isolation` |
| `crates/tracedecay/src/daemon/invocation_tests/types_tests.rs` | test | 60 | 60 | 60 | `daemon_shutdown_deadline` |
| `crates/tracedecay/src/daemon/maintenance/generation.rs` | production | 11 | 12 | 12 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/production_harness.rs` | production | 93 | 106 | 106 | `production_harness_shutdown` |
| `crates/tracedecay/src/daemon/production_harness/generation_retention_test.rs` | test | 134 | 154 | 154 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/project_composition.rs` | production | 723 | 830 | 830 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/projectless.rs` | production | 94 | 108 | 108 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/retained_owner.rs` | production | 11 | 12 | 12 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/retained_owner/memory.rs` | production | 2 | 12 | 2 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/session_runtime_tests/project_lifecycle_tests.rs` | test | 16 | 18 | 18 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/session_runtime_tests/session_sync_tests.rs` | test | 4 | 4 | 4 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/store_maintenance/mod.rs` | production | 122 | 140 | 140 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/store_maintenance/vector_retention_tests.rs` | test | 135 | 155 | 155 | `vector_retention_authority` |
| `crates/tracedecay/src/daemon/tests.rs` | production | 1 | 2 | 1 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/tests/ownership.rs` | test | 23 | 60 | 26 | `daemon_composition_mount` |
| `crates/tracedecay/src/daemon/tests/projectless.rs` | test | 297 | 341 | 341 | `hook_route_bridge` |
| `crates/tracedecay/src/daemon/tests/rmcp_route.rs` | test | 4 | new | 4 | `integration_test_runtime_isolation` |
| `crates/tracedecay/src/dashboard.rs` | production | 10 | 11 | 11 | `feature_gated_build_repair` |
| `crates/tracedecay/src/mcp/project_route.rs` | production | 161 | 180 | 180 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/server.rs` | production | 58 | 68 | 66 | `daemon_composition_mount` |
| `crates/tracedecay/src/mcp/server/connection.rs` | production | 11 | 12 | 12 | `daemon_composition_mount` |
| `crates/tracedecay/src/mcp/server/construction.rs` | production | 61 | 74 | 70 | `daemon_composition_mount` |
| `crates/tracedecay/src/mcp/server/host_admission_tests.rs` | test | 105 | 120 | 120 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/server/requests.rs` | production | 16 | 18 | 18 | `hook_route_bridge` |
| `crates/tracedecay/src/mcp/server/requests/tool_dispatch.rs` | production | 62 | 90 | 71 | `recall_context_mount` |
| `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs` | production | 52 | 65 | 59 | `host_hook_ingest` |
| `crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/tests.rs` | production | 5 | 6 | 5 | `host_hook_ingest` |
| `crates/tracedecay/src/mcp/tools/plugin_conformance_tests.rs` | test | 24 | new | 27 | `integration_test_runtime_isolation` |
| `crates/tracedecay/src/runtime_ports.rs` | production | 11 | new | 12 | `integration_test_runtime_isolation` |
| `crates/tracedecay/tests/common/mod.rs` | test | 73 | 73 | 73 | `integration_test_runtime_isolation` |
| `crates/tracedecay/tests/daemon_suite/code_index_journey.rs` | test | 6 | 7 | 6 | `hook_route_bridge` |
| `crates/tracedecay/tests/daemon_suite/indexing_lifecycle_test.rs` | test | 129 | 148 | 148 | `hook_route_bridge` |
| `crates/tracedecay/tests/hermes_suite/lcm_bridge.rs` | test | 49 | new | 56 | `integration_test_runtime_isolation` |
| `crates/tracedecay/tests/hooks_lsp_suite/hook_lifecycle_lease_test.rs` | test | 9 | new | 10 | `integration_test_runtime_isolation` |
| `crates/tracedecay/tests/memory_suite/memory_eval_test.rs` | test | 136 | 149 | 149 | `integration_test_runtime_isolation` |
| `crates/tracedecay/tests/transcript_ingest_suite/hermes.rs` | test | 13 | new | 14 | `integration_test_runtime_isolation` |

## Decision

We adopt policy revision `patch-footprint.v4` and approve this explicit
2026-09-08 revision of its complete repair inventory. Aggregate caps retain
existing headroom where it still fits; only the test/fixture and exception-zone
counts increase.
Each resulting aggregate footprint cap is the measurement plus at most roughly
fifteen percent, following ADR-0011:

| Cap | v3 | Previous v4 | Revised v4 | Measured now |
| --- | ---: | ---: | ---: | ---: |
| upstream existing production files | 37 | 56 | 56 | 54 |
| upstream existing test/fixture files | 9 | 18 | 26 | 23 |
| total upstream changed lines | 3500 | 6430 | 6430 | 5715 |
| changed lines per upstream file | 560 | 830 | 830 | 723 |
| exception-zone files without ADR/policy revision | 4 | 5 | 6 | 6 |

Composition-root files (15), files per allowed touch point (15), exception
files per ADR (2), workspace manifest files (2), and
`manual_generated_file_edits` (0) are unchanged. The complete per-file table
above binds every actual upstream delta, including formatting, with tight
`line_budget` values; it replaces the stale claim that only eight existing
entries needed remeasurement. No aggregate line-cap increase is needed.

We approve the `integration_test_runtime_isolation` cap increase from 3 files
and 240 changed lines to 9 files and 373 changed lines, measured at 9 files
and 325 lines. This consolidates all seven newly mapped fixture repairs in
one existing seam rather than inventing touch categories. `runtime_ports.rs`
and `lsp_tests.rs` leave their unused composition/shutdown policy path slots
and are authorized only by this fixture seam. The actual previous
`feature_gated_build_repair` cap was **34**, not 16. Its five build-repair
files measure 29 lines, but the global-db crate root is in a forbidden zone
and must use an explicit exception rather than touch-point authority. The
remaining four files measure 24 lines, so the touch-point cap tightens from
5 files / 34 lines to **4 files / 27 lines**. All three crate-root recursion
attributes and the separate host-admission test attribute remain; the
five-line global-db repair is counted once under exceptions.

The active measured touch points are:

| Touch point | Previous max files | Previous max lines | Approved max files | Approved max lines | Measured files | Measured lines |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `workspace_wiring` | 2 | 163 | 2 | 163 | 2 | 142 |
| `cognitive_recall_contract` | 4 | 940 | 4 | 940 | 3 | 816 |
| `daemon_composition_mount` | 15 | 1094 | 15 | 1094 | 13 | 955 |
| `daemon_shutdown_deadline` | 8 | 420 | 8 | 420 | 8 | 399 |
| `production_harness_shutdown` | 1 | 106 | 1 | 106 | 1 | 93 |
| `integration_test_runtime_isolation` | 3 | 240 | 9 | 373 | 9 | 325 |
| `recall_context_mount` | 5 | 340 | 5 | 340 | 5 | 269 |
| `configuration_registry_mount` | 5 | 718 | 5 | 718 | 4 | 625 |
| `host_hook_ingest` | 5 | 486 | 5 | 486 | 5 | 428 |
| `hook_route_bridge` | 12 | 1046 | 12 | 1046 | 12 | 912 |
| `vector_retention_authority` | 5 | 517 | 5 | 517 | 5 | 450 |
| `feature_gated_build_repair` | 5 | 34 | 4 | 27 | 4 | 24 |

The original v4 additions remain `hook_route_bridge`,
`vector_retention_authority`, and `feature_gated_build_repair`; the original
`host_hook_ingest` expansion is retained. Previously approved inactive seams
are not broadened and authorize no new measured path in this repair.

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

The `feature_gated_build_repair` touch point is capped at 4 files and 27 changed
lines and permits only the dashboard accessor repair or crate-level
`recursion_limit` attributes required by the hotpath feature set. Those crate
roots also compile in default builds; only the instrumented feature set
requires the extra recursion depth. This is not permission to change behavior.

`host_hook_ingest` grows to 5 files and 486 changed lines to admit the
executable Codex fixture, and the capture-registry test that names the
Claude project kernel, beside the acceptance test they serve.

We approve exactly **two named exceptions**, exhausting the unchanged
per-ADR cap of 2:

- `crates/tracedecay-agent-hosts/src/agents/claude.rs` in
  `host_specific_adapters` (51 changed lines, 58-line entry cap). Readiness
  for a Claude non-interactive install is decided inside this adapter, and
  the change reuses existing bundle-digest and discovery validators.
- `crates/tracedecay-global-db/src/lib.rs` in `native_database_internals`
  (5 changed lines, 5-line entry cap). The hotpath recursion attribute must
  live at this upstream crate root. A product-owned crate cannot raise this
  crate's rustc query depth. Only the attribute and its comment are admitted;
  database schema, persistence, transaction, and runtime semantics cannot
  change under this exception.

Both zones retain their `forbidden` defaults and existing reasons. No blanket
build-attribute exemption is introduced. The aggregate exception cap rises
explicitly from 5 to 6 because the earlier five-file accounting omitted the
Native-zone authority required by this already-dirty crate-root repair. The
other four exception files retain their existing ADR authorities.

The original v4 cap increases remain in force:
`daemon_composition_mount` 1094 lines (now measured 955),
`configuration_registry_mount` 718 lines (measured 625),
`production_harness_shutdown` 106 lines (measured 93), and
`workspace_wiring` 163 lines (measured 142).

Finally, the `daemon_shutdown_deadline` `cap_revision` block ADR-0016 approved
is restated under the revision now in force: its numbers are unchanged (8
files and 420 changed lines, historically measured 8 and 416). Its earlier
`policy_revision` transition from `patch-footprint.v3` to `patch-footprint.v4`
and ADR-0016 binding stay unchanged. The current pinned-head measurement is
8 files and 399 lines; the 416-line approval record is not a claim about this
repair snapshot.

Any later cap increase requires a further explicit ADR decision, not an unrecorded policy edit. A cap increase is approved by ADR
before the change that needs it and is
never bundled into the change that exceeds the previous cap; this revision is the policy slice that the sync
train's floor advancement to `3b59bb7eb` depends on, and it lands on its own.

## Touch-point cap revision

- Touch point: `integration_test_runtime_isolation`
- Previous max files: `3`
- Previous max changed lines: `240`
- Approved max files: `9`
- Approved max changed lines: `373`
- Measured files: `9`
- Measured changed lines: `325`
- Policy revision: `patch-footprint.v4`

This binding records the cap increase approved in Decision. Existing
ADR-0016 continues to bind only `daemon_shutdown_deadline`; its historical
measurement and approval are not rewritten by this fixture reconciliation.

## Consequences

- Every upstream-owned file the product tree changes against `3b59bb7eb`
  has an active convergence entry with a measured budget, so the sync train
  can stamp and gate the transplant candidate.
- The hook-route bridge, the online-only retention authority, and the build
  repair are reviewable as named seams with their own allowed and forbidden
  changes instead of unclassified residue.
- Six exact exception-zone files are admitted; a seventh requires a new ADR decision.
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

1. The aggregate production/test counts, total/per-file line caps, exception
   count, and every active entry budget fit the measured tree against
   `3b59bb7eb` plus at most roughly fifteen percent headroom. Unchanged local
   seam approvals retain their existing exact bounds; inactive seams grant no
   measured file authority without an active entry.
2. The three original v4 touch points, widened `host_hook_ingest`, and
   reconciled `integration_test_runtime_isolation` name exact paths; no new
   touch point or glob over upstream code is admitted.
3. The `host_specific_adapters` and `native_database_internals` zones retain
   their `forbidden` defaults. This ADR admits one named file in each, never
   a blanket build-attribute exemption.
4. Each convergence entry added under this revision declares tests that a
   gate lane in `product/upstream/sync-policy.json` runs and the bound
   workflow job mirrors.
5. The `daemon_shutdown_deadline` cap_revision numbers approved by ADR-0016
   and its existing v4 revision label are unchanged.

## Verification

Candidate measurement is against the full SHA above. The repository's accepted
floor remains `5749e4fcfe268e17bd19a0e6ef90c646f7b37289`; this policy slice
does not advance floor metadata, sync policy, or verification stamps. Root
must commit this policy before the code and advance the floor through the
existing train. Until then, real-repository footprint/floor failures are
expected and must be reported separately from candidate measurement. Listed
commands are required checks, not claims of successful execution.

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
- A seventh exception-zone file or a third exception under this ADR is
  needed: a new ADR decision, never a widened default.
