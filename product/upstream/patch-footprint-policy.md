# Provider patch-footprint policy

Bead: `tdmem-0105`

Machine-readable policy: [`patch-footprint-policy.json`](./patch-footprint-policy.json). Current exception/edit ledger: [`convergence-map.json`](./convergence-map.json).

## Objective

Keep the pluggable-memory product removable, reviewable, and rebaseable over future TraceDecay V2 checkpoints. Provider contracts, registries, adapters, context compilation, observation delivery, conformance, and provider-specific code belong in additive product-owned crates. Zack-owned code receives only narrow capability mounts.

The immutable comparison floor is PR #707 commit `08fbe33a7c7f403191fd5d6e356c7b6681b96403`.

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

Current snapshot through this bead: **zero Zack-owned existing-file edits**. The branch currently adds only product-owned planning, receipts, validators, workflows, and architecture artifacts.

## Product-owned zones

Primary zones:

- `.beads/**`, `product/**`, `scripts/product/**`, `tests/product_*`;
- product workflows (`product-*`, Beads application/materialization);
- future additive crates under `crates/tracedecay-memory-provider-*`, `tracedecay-memory-observation`, `tracedecay-memory-context`, and `tracedecay-memory-conformance`;
- dedicated root integration tests named `product_memory_provider*`.

These paths may evolve without consuming upstream touch budget, but they still obey repository quality, test, security, and dependency-direction rules.

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

Allowed roots include project composition, retained-owner/runtime ports, and invocation state. They may construct and retain capability registries and provider-neutral lifecycle ports.

Provider logic, global mutable provider singletons, unbounded workers, or authority over source/session/Native/configuration state are prohibited.

### Normalized observation mount

Allowed generic seams are the admitted hook ingest, canonical hook write settlement, and invocation observability producer.

Only already-admitted, exact-scope observations may fan out to a bounded durable product dispatcher. Observer mode cannot delay, alter, or fail canonical host ingest.

### Recall/context mount

Allowed seams are exact session-retrieval admission and provider-neutral context helpers. They may request bounded advisory recall and validate scope, freshness, provenance, policy, budget, deadline, cancellation, and typed coverage.

Providers cannot construct final context, override current code, reuse candidates across worktrees/sessions, or silently trigger another provider.

### Post-settlement feedback mount

Allowed seams may emit idempotent provider outcome/feedback observations after canonical settlement. Provider failure cannot retroactively change Native feedback, trust, or the completed operation.

### Configuration registry mount

Allowed seams may register provider-neutral keys and explicit observer/active selection. Configuration stays transactional, revisioned, authorized, audited, and credential-safe. Providers cannot activate or configure themselves.

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

### Toolchain, build, CI, and release policy

Provider work must not weaken or silently reshape the supported toolchain or upstream build/release lanes.

## Dependency directions

1. `tracedecay-memory-provider-api` is inward. It cannot depend on the root binary, runtime/store/DB internals, code index, or concrete adapters.
2. `tracedecay-memory-context` depends on capability contracts and TraceDecay application ports, never concrete NCM/Native/OCEAN crates.
3. Native and NCM adapters do not depend on one another. Neither depends on the root `tracedecay` crate.
4. NCM never imports Native persistence crates or implements `ProjectMemoryFactStore`.
5. CLI, MCP, dashboard API, and SDK remain adapter-blind.
6. Only the root composition/registry and conformance assembly may construct concrete adapters.
7. OCEAN remains a reserved capability slot; no speculative implementation dependency is allowed before a versioned specification exists.

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
