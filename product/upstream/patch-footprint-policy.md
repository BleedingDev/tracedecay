# Provider patch-footprint policy

Bead: `tdmem-0105`

Machine-readable policy: [`patch-footprint-policy.json`](./patch-footprint-policy.json). Current exception/edit ledger: [`convergence-map.json`](./convergence-map.json).

## Objective

Keep the pluggable-memory product removable, reviewable, and rebaseable over future TraceDecay V2 checkpoints. Provider contracts, registries, adapters, context compilation, observation delivery, conformance, and provider-specific code belong in additive product-owned crates. Zack-owned code receives only narrow capability mounts.

The comparison floor is `upstream_floor.sha` in [`patch-footprint-policy.json`](./patch-footprint-policy.json); the sync train advances it together with the canonical metadata `product/upstream/tracedecay-v2-pr707.json`. The first floor was the PR #707 creation head.

## Quantitative initial budget

This budget covers the initial provider contract, Native parity, and NCM observer admission. A later phase may revise it only through a versioned ADR and policy update.

| Cap | Initial maximum |
|---|---:|
| Existing upstream production files | 12 |
| Existing upstream test/fixture files | 6 |
| Total changed lines in upstream-owned existing files | 900 |
| Changed lines per upstream-owned file | 180 |
| Composition-root files | 6 |
| Files per allowed touch-point category | 3 |
| Exception-zone files without ADR/policy revision | 0 |
| Exception files authorized by one ADR | 2 |
| Workspace manifest/lock files | 2 |
| Manual generated-file edits | 0 |

Additive files under the declared product-owned paths are excluded. Git additions plus deletions determine changed lines. Renames count at source and destination. `Cargo.lock` may change only as pinned-toolchain output accompanying a workspace-manifest change and build/metadata receipt.

### Revisions to the initial budget

The table above is the v1 initial budget. Two versioned ADRs have revised it since, and the machine-readable caps in [`patch-footprint-policy.json`](./patch-footprint-policy.json) — pinned in `scripts/product/check-patch-footprint-policy.py` so they cannot be loosened by editing the policy alone — are always the binding values.

| Cap | v1 | v2 (ADR-0011) | v3 (ADR-0014) | Measured at v3 |
|---|---:|---:|---:|---:|
| Existing upstream production files | 12 | 34 | **37** | 37 |
| Existing upstream test/fixture files | 6 | 9 | 9 | 7 |
| Total changed lines in upstream-owned existing files | 900 | 3300 | **3500** | 3393 |
| Changed lines per upstream-owned file | 180 | 560 | 560 | — |
| Composition-root files | 6 | 15 | 15 | 13 |
| Files per allowed touch-point category | 3 | 15 | 15 | — |
| Exception-zone files without ADR/policy revision | 0 | 2 | **4** | 4 |
| Exception files authorized by one ADR | 2 | 2 | 2 | 2 |
| Workspace manifest/lock files | 2 | 2 | 2 | — |
| Manual generated-file edits | 0 | 0 | 0 | 0 |

Revision `patch-footprint.v2` ([ADR-0011](../architecture/adr/ADR-0011-patch-footprint-revision-v2.md)) sized every cap against the M4 observation-journey mount, the M5 cognitive-recall port, the Native configuration registration, and the session-sync exact-scope reuse. Revision `patch-footprint.v3` ([ADR-0014](../architecture/adr/ADR-0014-host-hook-ingest-footprint-revision-v3.md)) moves only the three caps the Claude Code host hook ingest journey (`tdmem-1001`) needs, adds the `host_hook_ingest` touch point, and approves a two-file exception in the host-adapter zone. Both follow the same rule: a cap is the footprint measured at its revision's tree plus at most roughly fifteen percent headroom. The production-file cap is set exactly at the measurement, so the next upstream production file this program touches trips the gate.

Administrative Codex orchestration state under `.codex/**` is measured by the
dirty-state diff but excluded before product/upstream ownership classification.
This exclusion is exact: it does not grant product ownership or hide source,
test, product, workflow, or similarly named paths outside `.codex/`.

Current snapshot through this bead: **zero Zack-owned existing-file edits**. The branch currently adds only product-owned planning, receipts, validators, workflows, and architecture artifacts.

## Product-owned zones

Primary zones:

- `.beads/**`, `product/**`, `scripts/product/**`, `tests/product_*`;
- product workflows (`product-*`, Beads application/materialization);
- future additive crates under `crates/tracedecay-memory-provider-*`, `tracedecay-memory-observation`, `tracedecay-memory-context`, and `tracedecay-memory-conformance`;
- dedicated root integration tests named `product_memory_provider*`;
- product journey tests named `product_memory_provider_*.rs` in the `crates/tracedecay` and `crates/tracedecay-cli` test directories.

These paths may evolve without consuming upstream touch budget, but they still obey repository quality, test, security, and dependency-direction rules.

The `crates/tracedecay-cli/tests/product_memory_provider_*.rs` pattern was added by [ADR-0014](../architecture/adr/ADR-0014-host-hook-ingest-footprint-revision-v3.md) so the Claude host journey test is classified where it belongs. It admits only files whose name already marks them as product journeys inside that one test directory; the crate's other tests, its manifest, and every other upstream tree stay upstream-owned, and the prohibition on broad `crates/**`, `crates/tracedecay/**`, `tests/**`, and `.github/**` patterns is unchanged.

## Allowed upstream touch points

Every actual edit still requires one active convergence-map entry.

### Workspace wiring

Allowed files: `Cargo.toml`, generated `Cargo.lock`.

Allowed: register additive crates, add dependencies needed by those crates, regenerate the lockfile with the pinned toolchain.

Forbidden: unrelated dependency upgrades, feature-default changes that force provider behavior, manual lockfile editing.

### Application contract mount

Allowed files:

- `crates/tracedecay-application/src/retained_surfaces.rs`
- `crates/tracedecay-application/src/lib.rs`
- `crates/tracedecay/src/application_surface.rs`

Allowed: provider-neutral capability ports, exact scope, provenance, deadline/cancellation, typed outcomes, and additive router/operation mounts.

Forbidden: concrete provider types or names in public contracts, silent fallback, fake readiness.

### Daemon composition mount

Allowed roots include project composition and retained-owner/runtime ports. They may construct and retain capability registries and provider-neutral lifecycle ports.

The default-off M2 provider-host mount is limited to these additional exact
files:

- `crates/tracedecay/Cargo.toml`
- `crates/tracedecay/src/daemon/tests/ownership.rs`
- `crates/tracedecay/src/mcp/server.rs`
- `crates/tracedecay/src/mcp/server/construction.rs`
- `crates/tracedecay/src/mcp/server/connection.rs`

Provider logic, global mutable provider singletons, unbounded workers, or authority over source/session/Native/configuration state are prohibited.

### Daemon shutdown deadline

Allowed files are the ten exact paths named in the machine-readable policy: the six daemon shutdown call-chain files, plus four call-chain unit-test files under `crates/tracedecay/src/daemon/invocation_tests/`. They may propagate one caller-supplied absolute deadline, bound blocking drain by its remaining budget, report an incomplete shutdown as `Failed` while still reaching terminal teardown, assert the shutdown fence through an observable effect of `begin_shutdown` under a bounded wait, and deterministically prove that superseded work settles before its receipt terminal is emitted.

Nested layers cannot refresh the deadline, hide incomplete shutdown, reorder unrelated lifecycle phases, or add provider behavior. A test cannot weaken a shutdown assertion, or stand a sleep, a fixed yield count, or a lock-contention probe in for the effect it claims to observe.

This category's caps are 8 files and 420 changed lines. [ADR-0016](../architecture/adr/ADR-0016-daemon-shutdown-receipt-ordering-headroom.md) retains the 8-file cap and raises only the line cap from 360 to 420 against a measured 8 files and 416 changed lines, authorizing the deterministic `types_tests.rs` supersession receipt-ordering regression. ADR-0016 supersedes only [ADR-0015](../architecture/adr/ADR-0015-daemon-shutdown-test-fence-and-supersession-headroom.md) as the approving decision named in the category's `cap_revision`; ADR-0015 and [ADR-0013](../architecture/adr/ADR-0013-daemon-shutdown-touch-point-expansion.md) remain the historical 8/360 and 6/320 decisions. Root will commit this policy slice before the implementation slice. The exact `types_tests.rs` allowlist path is the only path addition; no aggregate cap changes, and the policy revision remains `patch-footprint.v3`.

ADR-0015 then admitted the shutdown call chain's own unit tests — `invocation_tests/lsp_lease_tests.rs`, `invocation_tests/lsp_tests.rs`, and `invocation_tests/project_lifecycle_tests.rs` — because a test that asserts the shutdown ordering is part of the shutdown seam and is re-applied with it on every sync train. Bead `tdmem-0th` found that the fence assertion in `lsp_lease_tests.rs` was not literal: it spun eight fixed `yield_now()` calls and then accepted either a finished open or a merely contended admission lock, so a run in which the fence never engaged could still report success. Only `lsp_lease_tests.rs` has a convergence entry; admitting a path grants no authority to change it, and the other two still need their own entries before they may differ from the floor. ADR-0015 also raised the `invocation/types.rs` per-entry line budget from 30 to 50 ahead of a pending supersession fix, and changed no aggregate cap — `max_upstream_existing_test_or_fixture_files` deliberately stays 9 against a measured 8.

Touch-point caps are not free-floating data. Every category's `max_files` and `max_changed_lines` are pinned in `scripts/product/check-patch-footprint-policy.py`, so a category cannot widen its own reach by editing the policy alone, and a category whose caps were revised must carry a `cap_revision` block naming its approving ADR. The gate reads that ADR and requires it to bind the exact category, the previous caps, the approved caps, the measurement they were derived from, and an affirmative grant; it also holds the approved cap to the ADR-0011 rule of measurement plus at most roughly fifteen percent headroom (420 is 1.0% above the measured 416, and the 8-file cap equals the measured 8). No aggregate cap changed and no other touch point changed.

### Integration-test runtime isolation

Allowed files are the common integration-test command harness and the memory-suite evaluation test. They may require an explicit or Cargo-provided CLI from the same checkout, clear inherited daemon routing, and share one bounded production-equivalent socket and isolated profile environment.

Sibling-profile guessing, installed-daemon or global-profile reuse, silent mixed revisions, and production behavior changes are prohibited.

### Normalized observation mount

Allowed generic seams are the admitted hook ingest, canonical hook write settlement, and invocation observability producer.

Only already-admitted, exact-scope observations may fan out to a bounded durable product dispatcher. Observer mode cannot delay, alter, or fail canonical host ingest.

### Recall/context mount

Allowed seams are exact session-retrieval admission and provider-neutral context helpers. They may request bounded advisory recall and validate scope, freshness, provenance, policy, budget, deadline, cancellation, and typed coverage.

Providers cannot construct final context, override current code, reuse candidates across worktrees/sessions, or silently trigger another provider.

### Post-settlement feedback mount

Allowed seams may emit idempotent provider outcome/feedback observations after canonical settlement. Provider failure cannot retroactively change Native feedback, trust, or the completed operation.

### Host hook ingest

Allowed files are the three exact paths named in the machine-readable policy: the transcript-capture kernel table (`crates/tracedecay/src/mcp/tools/handlers/hook_runtime/ingest/kernels.rs`), the `tracedecay-cli` manifest, and the host lifecycle acceptance test.

Allowed: call the existing host-neutral transcript ingest route from a host lifecycle event under an explicit bounded budget; register an additional project-scoped capture kernel for a host that already has a profile-scoped one, reusing the session-sync ingest pass; declare an opt-in default-off product test target and the feature it requires; extend the host lifecycle acceptance test with idempotence and rollback assertions.

Forbidden: naming a provider, registry, or fabric type on that path; letting an ingest failure, timeout, or unreachable daemon change the host's own hook answer; unbounded or unbudgeted ingest work on a lifecycle event; a second admission or scope-identity derivation beside the one the session-sync worker uses; turning the product target or its feature on by default.

This category's caps are 3 files and 200 changed lines (measured 188, 6.4% headroom), approved by [ADR-0014](../architecture/adr/ADR-0014-host-hook-ingest-footprint-revision-v3.md) together with revision `patch-footprint.v3`. The two Claude host-adapter files that trigger the catch-up are not in this category: they sit inside the forbidden `host_specific_adapters` zone and carry exception evidence instead.

### Configuration registry mount

Allowed seams may register provider-neutral keys and explicit observer/active selection. Configuration stays transactional, revisioned, authorized, audited, and credential-safe. Providers cannot activate or configure themselves.

This category's line cap is 540 rather than the 360 carried before the PR #707 floor: upstream 5749e4fc moved the config module into `crates/tracedecay-configuration`, so the moved seam and its non-vacuous in-crate tests (219 lines) now count inside this touch point instead of the crate the seam used to live in. No other cap and no file count changed.

## Zero-touch exception zones

Default cap: zero files.

### Native database internals

`tracedecay-runtime-core/src/store`, `tracedecay-store`, global/graph DB, and rusqlite runtime own canonical persistence, lineage, transactions, schemas, recovery, and graph publication. Providers sit above these contracts.

### Code-index internals

Code index, extraction, query, semantic, retention, and index-runtime crates remain TraceDecay's current-code authority. Providers consume admitted ports; they do not change indexing semantics.

### Generated contracts

SDK operation descriptors, dashboard contract schema, and `Cargo.lock` are generated outputs. Change their owning source/generator and reproduce them; never patch generated text manually.

### Host-specific adapters

Agent-host, host-integration, hooks, Hermes, and Context Scout-specific adapters are not provider mounts. Observation begins after host-neutral admission so all coding-agent hosts keep one authority model.

The zone default stays `forbidden`, and that reason is unchanged. [ADR-0014](../architecture/adr/ADR-0014-host-hook-ingest-footprint-revision-v3.md) admits exactly two exact files — `crates/tracedecay-agent-hosts/src/hooks/claude.rs` and `crates/tracedecay-agent-hosts/src/hooks/mod.rs` — because a Claude `SessionStart`/`Stop` event is observable nowhere else in this repository and no seam above the host adapter exists to attach a listener to. Those two files add no provider behavior and mount nothing: they call the same host-neutral ingest route the module already uses, under an explicit budget, fail-open. This is the whole grant. The per-ADR cap of two files means a third host-adapter file needs its own ADR and its own argument, and the exception-zone total (four files, shared with ADR-0012's two configuration-registry files) is a hard cap in the machine-readable policy.

### Toolchain, build, CI, and release policy

Provider work must not weaken or silently reshape the supported toolchain or upstream build/release lanes.

## Dependency directions

1. `tracedecay-memory-provider-api` is the inward-most memory crate. It cannot depend on fabric, registry, observation, context, conformance, any concrete adapter, the root binary, or TraceDecay runtime/store/DB/code-index/query/semantic internals.
2. `tracedecay-memory-fabric` orchestrates capability contracts only. It cannot import any present or future concrete provider, including Biomem/NCM/OCEAN adapters or SDKs. Its only structural exception to the generic provider-package prohibition is `tracedecay-memory-provider-api`.
3. `tracedecay-memory-context` depends on capability contracts and TraceDecay application ports, never concrete Biomem/NCM/Native/OCEAN crates or SDKs. The same exact provider-API allowance applies.
4. Concrete provider adapters stay below fabric and cannot reach the root crate, raw `rusqlite`/`grafeo-*`/`libsql*`/private-filesystem engines, or TraceDecay runtime, storage, database, code-index, extraction, query, or semantic internals. The provider registry is a composition layer, not a concrete adapter, and may depend on fabric to register implementations.
5. Native and NCM adapters do not depend on one another. NCM may use its licensed Biomem SDK/transport, but never a separate NCM/OCEAN adapter SDK, Native persistence, or TraceDecay runtime/storage internals.
6. CLI, MCP, dashboard API, and SDK remain adapter-blind.
7. Only the root composition/registry and conformance assembly may construct concrete adapters.
8. OCEAN remains a reserved capability slot; no speculative implementation dependency is allowed before a versioned specification exists.

The machine-readable rule bodies are canonical, not user-adjustable bypasses: the checker rejects a retained rule ID whose source, exclusion, target allowance, or forbidden-dependency patterns have been weakened or broadened. Provider-neutral rules use the generic `tracedecay-memory-provider-*`, `biomem*`, `ncm*`, and `ocean*` prohibitions; `allowed_dependencies` pins the only target-side structural allowance to the provider API. It scans every declared workspace member and every in-tree path dependency Cargo would add automatically, while honoring `[workspace].exclude`. Escaped or absolute member declarations fail and are scanned only when they resolve safely inside the repository. Normal, development, build, and target-specific dependency tables are checked; obsolete `[project]`, `[dev_dependencies]`, `[build_dependencies]`, and `[workspace_dependencies]` spellings fail explicitly and cannot hide edges. Renamed dependencies are checked by their resolved `package` name, including aliases inherited from `[workspace.dependencies]`. Protected `crates/tracedecay-memory-*` boundary manifests must retain their path-derived canonical package names, so renaming a source or target package cannot evade a rule. A failure names the rule and exact `source -> dependency` edge, with its manifest and declaration section.

### Dependency-direction exceptions

An unavoidable forbidden edge requires one entry in `dependency_direction_exceptions` with exactly these fields:

```json
{
  "rule": "literal-rule-id",
  "source": "literal-source-package",
  "dependency": "literal-dependency-package",
  "adr": "product/architecture/adr/NNNN-decision.md",
  "rationale": "Why this exact edge is unavoidable"
}
```

Rule, source, and dependency are literal names; globs are forbidden. The ADR must be an existing Markdown file that resolves inside `product/architecture/adr/`. The source must be a current workspace package, the rule must select that source and forbid that dependency, and the exact dependency edge must currently exist. Duplicate, unknown, nonmatching, missing-ADR, out-of-tree, and stale/unused entries fail. An exception authorizes only its named rule and edge; it cannot waive a package, pattern, dependency section, or another overlapping rule.

The referenced ADR must bind its content to that exact edge; file existence or a heading alone grants no authority. It contains exactly one visible instance of each section below; headings hidden in Markdown fences, indented code, or HTML comments do not count. The binding values must exactly equal the exception entry, Decision must explicitly and affirmatively authorize the edge, and Decision and Rationale must contain substantive prose rather than placeholders.

```markdown
# ADR NNNN: Narrow dependency exception

## Dependency-direction exception

- Rule: `literal-rule-id`
- Source: `literal-source-package`
- Dependency: `literal-dependency-package`

## Decision

Permit this exact edge, including its bounded consequences and limits.

## Rationale

Explain why the normal boundary cannot satisfy this specific case.
```

## Convergence-map contract

An active entry is mandatory for every upstream-owned existing file changed relative to the pinned floor. It contains:

- exact path and allowed touch-point category;
- rationale and owning bead(s);
- semantic invariants that must survive upstream convergence;
- focused verification commands/tests;
- a per-file line budget;
- rebase/removal plan;
- active or retired status.

An exception additionally contains the exception zone, versioned ADR, why the normal seams are insufficient, rejected alternatives, policy revision, and rollback plan.

The map is bidirectional: an actual upstream diff without an entry fails; an active entry without an actual diff also fails. Retired entries remain historical evidence but grant no authority.

## Exception workflow

1. Stop implementation before touching the zone.
2. Write an ADR describing the missing seam, alternatives, consequences, and executable verification.
3. Add/update the convergence-map entry.
4. Revise this policy if any hard cap changes.
5. Implement the smallest patch.
6. Run the focused tests plus Native parity, clean baseline, and relevant coding-agent journey.
7. Record the receipt and explicit removal/rebase plan.

## Verification

```bash
python3 scripts/product/check-patch-footprint-policy.py \
  --repo . \
  --policy product/upstream/patch-footprint-policy.json \
  --map product/upstream/convergence-map.json
python3 tests/product_patch_footprint_policy_test.py
```

The checker also runs `git diff --numstat <floor>...HEAD`, classifies product/upstream paths, enforces hard caps, requires convergence entries, checks exception evidence, and scans current workspace manifests for forbidden dependency directions.
