# ADR-0012: Register memory-provider settings in the upstream configuration registry as a bounded exception

Status: Accepted
Date: 2026-09-02

## Context

The `native_database_internals` exception zone in
`product/upstream/patch-footprint-policy.json` forbids product edits to
`crates/tracedecay-global-db/**` and its sibling persistence crates, because
those crates own canonical Native persistence, lineage, schemas, transactions,
graph publication, and recovery. Cognitive providers must sit above them. The
zone's own `required_exception_evidence` names the way through: an ADR proving
no application or composition seam can satisfy the requirement, a
convergence-map entry with semantic invariants and a rollback plan, parity and
recovery tests, and a policy revision if the zero-file zone cap is raised.

The memory provider needs two project settings to exist:
`memory.provider_native_enabled.v1`, which decides whether the Native provider
composition mounts at all, and the recall-routing setting, which pins the
active provider and its fallback rule. In TraceDecay a project setting is not
a value someone reads from a file — it is a row the configuration registry
declares, with a scope, a sensitivity, a restart requirement, a default, and a
place in the transactional revision/rollback machinery. `ConfigurationRegistry`
lives in `crates/tracedecay-global-db/src/configuration/registry.rs`, and the
additive-key reconciliation that lets an existing profile gain a
newly-declared setting lives in `store.rs` beside it. There is no registration
seam above them: the application layer consumes a snapshot the registry has
already produced, and the composition root reads settings rather than
declaring them.

So the requirement — "the Native provider is off by default and is turned on
by a real, audited, rollback-covered project setting" — cannot be met from any
product-owned crate. The alternative is not a different seam; it is having no
setting.

## Decision

Grant a bounded, ADR-backed exception for exactly two files:

- `crates/tracedecay-global-db/src/configuration/registry.rs` — declares
  `MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY` (Boolean, default `false`,
  `SettingSensitivityV1::Public`, `RestartRequirementV1::DaemonRestart`) and
  the recall-routing setting (Text holding the validated default
  `MemoryProviderRecallRoutingV1`, same sensitivity and restart requirement),
  alongside the settings already registered there.
- `crates/tracedecay-global-db/src/configuration/store.rs` — adds those two
  keys to the existing additive-key list next to
  `INDEX_NATIVE_GRAPH_ACTIVATION_SETTING_KEY`, so a profile created before
  these settings existed gains them on reconciliation rather than failing.

The exception is additive only: no existing setting's default, scope,
sensitivity, or restart requirement changes, no schema or migration is
touched, no transaction or recovery path is altered, and no persistence type
is redefined. Under revision `patch-footprint.v2` the zone's file cap rises
from zero to two — exactly these two files, which is also the
`max_exception_files_per_adr` limit, so this ADR cannot be stretched to cover
a third file.

## Consequences

- The Native provider can be enabled per project through the normal
  configuration control plane, with the audit, protected-change, and rollback
  behavior every other setting gets, instead of through an out-of-band flag.
- The default stays disabled, so mounting the provider composition remains an
  explicit operator decision on every project.
- The `native_database_internals` zone is no longer strictly zero-file, which
  costs some of its rhetorical force; the file cap of two and the per-ADR cap
  of two are what keep it meaningful.
- A future upstream that offers a settings-extension seam makes this exception
  removable in one commit, which is why the rollback plan is written down.
- Rebasing these two files onto a moving upstream means re-applying two
  additive lists, which conflicts predictably and resolves mechanically.

## Rejected alternatives

- **Read the memory settings from a product-owned side table instead of the registry.**
  Rejected because configuration snapshots, protected changes, sensitivity
  redaction, restart requirements, and rollback would not cover it — the
  setting would look like configuration to a user while behaving like an
  unaudited global, and the authority matrix gives the configuration control
  plane a single canonical writer.
- **Fork the configuration registry into a product-owned crate.**
  Rejected because two registries mean two canonical writers for the same
  state, which the authority matrix forbids outright, and profiles would
  disagree about which settings exist.
- **Hard-code the provider composition as always-on and drop the settings.**
  Rejected because an operator could not disable a provider that misbehaves,
  and the program requires activation to be an explicit, reversible decision.
- **Gate the provider on an environment variable or build feature only.**
  Rejected because neither is per-project, neither is audited, and a build
  feature cannot be changed on a running installation.

## Invariants

1. The exception covers exactly two files; a third file in this zone needs its
   own ADR and its own policy revision.
2. The edits stay additive: registering new keys and extending the additive-key
   reconciliation list. No existing setting's semantics change.
3. No schema, migration, transaction, lineage, or recovery path in the zone is
   modified.
4. `memory.provider_native_enabled.v1` defaults to `false`, and the recall
   routing default is validated before registration.
5. Both settings keep `RestartRequirementV1::DaemonRestart`, so activation
   never changes a running composition underneath an open project.
6. The convergence-map entries for both files stay active with this ADR named,
   and the exception's `policy_revision` tracks the live policy revision.

## Verification

- `python3 scripts/product/check-patch-footprint-policy.py` — validates the
  exception evidence, the zone match, the ADR path, and the file cap.
- `python3 scripts/product/check-upstream-ownership-registry.py` — keeps both
  paths classified.
- `cargo test -p tracedecay-global-db` — configuration registry and store
  behavior, including additive reconciliation of an existing profile.
- `cargo test -p tracedecay --features memory-provider-host` (root-only) —
  `crates/tracedecay/src/config/tests.rs` covers the two settings reaching the
  composition root, and `project_composition.rs` refuses an unknown active
  provider rather than mapping it onto Native.
- Executable beads: `tdmem-0402` (Native provider mount and activation) and
  `tdmem-0607` (explicit provider routing and fallback policy).

## Review triggers

Review when upstream adds a settings-extension seam that a product crate can
use, when a third memory setting is proposed, when upstream restructures the
configuration registry or its additive reconciliation, or when a sync train's
conflict receipts show these two additive lists no longer resolve mechanically.

## Rollback plan

Delete the two key registrations from `registry.rs` and the two entries from
the additive-key list in `store.rs`, delete the two convergence-map exception
entries, and restore the zone file cap to zero in the next policy revision.
Profiles that already stored values keep inert rows that no registry declares;
they are ignored on read and removed by the registry's own unknown-key
reconciliation.
