# ncm-rs-016 — Implement versioned export/restore with non-resurrection

Status: planned, not executed.
Owner: Runtime / snapshot compatibility.
Dependencies: ncm-rs-015.
External gates: none.

## Objective

Make restart and portable state transfer truthful and bounded.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/snapshot/`
- `crates/tracedecay-memory-ncm-runtime/tests/snapshot.rs`

## Implementation

1. Implement the new Rust snapshot format with explicit schema, algorithm/config/model/projection identity, lengths, checksums, namespace and privacy epoch.
2. Validate every section and quota into a staging location, then atomically publish. Check newer revocations before serving; imported state cannot supply its own trusted revocation authority.
3. Restore all scheduling and projection state, especially steps_since_consolidation and STM→LTM mapping. Cold restart must not silently instantiate new projection weights.
4. Do not support arbitrary .pt/pickle or claim .bdbm compatibility in v1. An eventual legacy importer needs a separately reviewed, isolated conversion path; own-format export/restore is mandatory now.

## Acceptance

1. Uninterrupted and restart/snapshot-split traces have equivalent next mutation, recall and consolidation timing.
2. Truncated, oversized, malicious-length, wrong-model, wrong-namespace and checksum-corrupt snapshots are refused without changing current state.
3. A pre-deletion snapshot restored after deletion cannot resurrect revoked sources.
4. Restoring into a fresh profile with no trusted deletion lineage is refused or requires explicitly authorized sanitized transfer; no blanket privacy claim.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test snapshot --locked

## Sources

- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S06: Biomem projections](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/projections.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
