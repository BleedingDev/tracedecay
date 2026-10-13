# ncm-rs-024 — Mount NCM in Observer mode and prove authorized session reuse

Status: planned, not executed.
Owner: Host composition / observation owner.
Dependencies: ncm-rs-023.
External gates: HOST-OBSERVE.

## Objective

Exercise real host event delivery while proving Observer output cannot affect the agent.

## Owned paths

- `crates/tracedecay-memory-provider-registry/Cargo.toml`
- `crates/tracedecay-memory-provider-registry/src/ncm_mount/`
- `crates/tracedecay-memory-provider-registry/src/lib.rs`
- `crates/tracedecay/src/daemon/product_memory_provider/`
- `crates/tracedecay-cli/tests/product_memory_provider_ncm_observer_journey.rs`
- `product/ncm/receipts/observer/`
- `Cargo.toml`
- `Cargo.lock`
- `product/upstream/convergence-map.json`
- `product/upstream/patch-footprint-policy.json`
- `product/architecture/memory-dependency-policy.json`

## Implementation

1. Mount the real adapter/worker only through the provider registry and existing supervision. Add the smallest opt-in feature/configuration change; exact production composition paths must be discovered on the stabilized tree rather than assumed from the proposed new module name.
2. Drive real CLI/daemon hook ingestion after startup has quiesced, with a negative control against startup import. Inspect real NCM durable receipts, not a mock dispatcher counter.
3. Implement the N03 authorized-history replay/share bridge for same-worktree later-session use. Preserve the requesting session’s exact scope and original-source attribution; deny sibling/unrelated worktrees unless explicitly authorized by existing host policy.
4. Compare final prompt/native-fact/action outputs with NCM Observer enabled and disabled under deterministic inputs. Run shadow recall read-only, and account for resource consumption without letting it alter active selection.
5. The integration coordinator updates the exact ownership/feature/lock entries for this mount on this task’s tree. Shared policy metadata remains an ordered change, not a blanket permission or a waiver of failing checks.

## Acceptance

1. Each accepted observation has exactly one NCM effect across retries and daemon/worker restart.
2. Observer mode changes no prompt bytes, Native facts or externally visible agent actions; provider outputs cannot be selected through fallback.
3. A later authorized session recalls/replays legitimate source knowledge; forbidden sessions/worktrees do not. No silent namespace broadening.
4. If current host authority cannot express safe sharing/replay, active release is blocked pending a narrow owner-reviewed contract change, not faked by skipping session fields.

## Verification targets

1. product_memory_provider_ncm_observer_journey (planned real-process target)

## Sources

- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)
- [S16: Existing host journey](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
