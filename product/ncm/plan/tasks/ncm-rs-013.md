# ncm-rs-013 — Implement provider-owned durable store and source capsules

Status: planned, not executed.
Owner: Runtime / storage.
Dependencies: ncm-rs-004.
External gates: none.

## Objective

Provide an atomic, replayable ownership boundary independent of TraceDecay’s stores.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/store/`
- `crates/tracedecay-memory-ncm-runtime/tests/store.rs`

## Implementation

1. Use an admitted provider state root and the repository’s approved private-filesystem/SQLite primitives where compatible. No paths are derived directly from request text, and no host database is opened.
2. Implement transactional storage for namespace metadata, source capsules, committed events, idempotency rows, compatibility identity, privacy epochs and state generation/checkpoint metadata.
3. Snapshots/checkpoints must contain all relevant tensors, matrices, masks, IDs, source support, record validity, counters, fatigue and consolidation scheduling. Choose one canonical durable authority and a precise fsync/commit point.
4. Bound snapshot bytes, journal growth, replay work and resident views. Never discard replay information still needed to remove a retained source’s influence. Reserved capacity must permit privacy and recovery operations under ingestion pressure.

## Acceptance

1. Fault injection around durable writes/reopen yields a complete prior state or complete committed state, never partial Ready.
2. Corrupt/incompatible state fails closed; fresh-empty is allowed only through an explicit new-store path, not a catch-all load exception.
3. Opening a second writer is refused or serialized; namespace paths cannot escape the admitted root.
4. Storage-accounting tests include WAL, temporary files, checkpoints, source capsules, tombstones and cached embeddings.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test store --locked

## Sources

- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
