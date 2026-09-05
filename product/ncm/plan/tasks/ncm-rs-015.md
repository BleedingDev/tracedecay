# ncm-rs-015 — Implement deletion through mixed state and bounded maintenance

Status: planned, not executed.
Owner: Runtime / privacy and retention.
Dependencies: ncm-rs-014.
External gates: none.

## Objective

Remove a source’s recoverable and learned influence, not just its visible text row.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/privacy/`
- `crates/tracedecay-memory-ncm-runtime/src/maintenance/`
- `crates/tracedecay-memory-ncm-runtime/tests/deletion.rs`
- `crates/tracedecay-memory-ncm-runtime/tests/maintenance.rs`

## Implementation

1. Map deletion to all provider-owned copies, embeddings, center-support links, merged records, terrain influence, feedback and source capsules, including replicas created by authorized session replay.
2. For nonlinear mixed state, v1 atomically fences the affected namespace, rebuilds from authorized retained inputs excluding revoked sources, and publishes only the sanitized generation. Do not attempt unproven subtraction of a source from nonlinear normalization/diffusion.
3. Keep privacy epoch/revocation authority outside restorable checkpoints. Reapply it during every restore/replay and prevent stale journal events from resurrecting content. Bound rebuild work and report maintenance continuation.
4. Define deletion granularity and supported physical-erasure scope explicitly. Quarantine/unlink controlled old snapshots and caches as required; never claim to erase independent exported files or guarantee SSD-level secure erasure.

## Acceptance

1. Delete a source after consolidation and merge; LTM-only recall, explain, export, restart and allowed replay cannot expose it.
2. Deletion racing with observe/recall/checkpoint is linearized: after acknowledged logical deletion no new result contains revoked evidence.
3. Interrupted rebuild stays unavailable/maintenance-pending until safe; it does not serve stale state as healthy.
4. Privacy/reconstruction quota exhaustion is visible, preserves safety and never yields a false deletion-complete receipt.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test deletion --test maintenance --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
