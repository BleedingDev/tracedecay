# Provider source binding draft

Draft only; no live provider source was edited and no Cargo or model jobs ran.

Artifacts:
- `../pm-ncm-source-binding-provider.production.patch`: production-only delta submitted for parent review before tests.
- `../pm-ncm-source-binding-provider.patch`: complete delta including six focused unit tests.
- `baseline/`: exact captured live files before this change.
- `proposed/`: five final proposed provider modules.

Owned files:
- `crates/tracedecay-memory-provider-ncm/src/lib.rs`: narrow common Observe source projection hook.
- `crates/tracedecay-memory-provider-ncm/src/common.rs`: provenance root binding, legacy/full reconstruction checks, raw source-key exclusion check before final budgets.
- `crates/tracedecay-memory-provider-ncm/src/common/source_binding.rs`: namespace/original-lineage full ID derivation, strict stored identity checks, fresh claimed target selection; four tests.
- `crates/tracedecay-memory-provider-ncm/src/common/lifecycle.rs`: full+legacy stable target mapping, full replacement binding, fresh claimed deletion projection, inspection reconstruction.
- `crates/tracedecay-memory-provider-ncm/src/common/portability.rs`: full replay identities, preserved actual snapshot source IDs, blocked legacy restore rejection; two tests.

Compatibility details:
- Full source ID uses opaque_surface_id(namespace, b"common-source-key-v1", canonical JSON array [original profile, original project, canonical provider, canonical session, source key]).
- Legacy alias retains b"forget-source-key" over raw source key.
- Only root provenance source_binding is added; retained capsule, delivery capsule and selection metadata construction remains unchanged.
- Missing recorded original scope remains readable in legacy form and cannot mint a full ID.
- Private claim presence selects targeted deletion; independently verified fresh admission determines the unique full source per wire key. Revisions deduplicate; ambiguous or missing evidence refuses. Unclaimed deletion keeps raw alias behavior.
- Snapshot inventory preserves actual source IDs. A freshly blocked source requires its full stored ID; a matching legacy snapshot capsule refuses before runtime invocation.

Dependencies:
- ProviderCall::history_grant() accessor, now live from registration owner.
- Matching runtime source-binding, alias SQL, guarded deletion, legacy retry, and restore fence changes owned by parent.

Verification performed:
- rustfmt --edition 2024 --check --config skip_children=true: passed all five proposed modules.
- git apply --check .codex/patches/pm-ncm-source-binding-provider.patch: passed against live worktree.
- All four captured baseline files still matched live byte-for-byte at final verification.
- Tests were added but not run; compilation was not run, per draft-only/no-Cargo scope.

Complete patch SHA-256: 5ad1b4564e886436990fc45a018a05c1d62b0b62581694a1f6ce370be1e73c3e

Replay protocol completion: each projected replay item now also carries `legacy_source`. Blocked full-ID items can therefore use guarded deletion, and runtime exact legacy page retry conversion removes this field while replacing `source` with its legacy alias.

Production patch SHA-256: f202b78b1367e4b1e52736b7bd329a6ecbf1f9edeff245ce2e24209304749e44
