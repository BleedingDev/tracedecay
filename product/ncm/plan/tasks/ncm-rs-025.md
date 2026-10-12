# ncm-rs-025 — Enable guarded active NCM and run joined comparative evaluation

Status: planned, not executed.
Owner: Host recall owner / evaluation owner.
Dependencies: ncm-rs-024.
External gates: HOST-RECALL.

## Objective

Prove NCM output is admitted safely and earns its cost before offering active mode.

## Owned paths

- `crates/tracedecay-memory-provider-registry/src/ncm_mount/`
- `crates/tracedecay/src/daemon/product_memory_provider/`
- `crates/tracedecay-cli/tests/product_memory_provider_ncm_active_journey.rs`
- `product/ncm/receipts/active/`
- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`

## Implementation

1. Use explicit opt-in provider selection. Preserve configured provider identity; Native canonical facts remain separate, never an undeclared fallback for failed NCM recall.
2. Exercise real host admission, provenance hydration, selection, tokenizer and pack/trace persistence with NCM results. Test cancellation/budget exhaustion at actual reachable host lookup paths.
3. Complete the matched Native/no-memory/documentation/Python/Rust coding evaluation on the joined tree, with held-out tasks and predeclared thresholds. Measure task benefit, harmful stale recall, scope/deletion safety, repeated discovery, latency and context cost.
4. Implement a tested disable/quarantine rollback that leaves Native authoritative and does not destroy valid NCM state. Do not change the default mode.
5. The integration coordinator updates the exact ownership/feature/lock entries for this mount on this task’s tree. Shared policy metadata remains an ordered change, not a blanket permission or a waiver of failing checks.

## Acceptance

1. Current code and required host evidence cannot be displaced by NCM volume, activation or unsupported provenance.
2. Stale correction, restart, provider corruption/failure, timeout, deletion and cross-worktree journeys pass with nonempty selections.
3. All safety criteria pass and usefulness meets the predeclared release threshold; inconclusive benefit leaves NCM experimental/Observer rather than falsely complete.
4. The active artifact, runtime mode, model and source identities match its evidence.

## Verification targets

1. product_memory_provider_ncm_active_journey (planned real-process target)
2. joined coding-memory comparison

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S16: Existing host journey](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
