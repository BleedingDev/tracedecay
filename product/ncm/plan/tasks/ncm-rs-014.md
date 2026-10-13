# ncm-rs-014 — Implement end-to-end engine transactions and exactly-once effects

Status: planned, not executed.
Owner: Runtime / engine.
Dependencies: ncm-rs-010, ncm-rs-011, ncm-rs-012, ncm-rs-013.
External gates: none.

## Objective

Connect real text input to learning, durable commit and read-only recall with correct retry semantics.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/src/engine/`
- `crates/tracedecay-memory-ncm-runtime/tests/engine_transactions.rs`

## Implementation

1. Translate admitted normalized observations into typed source capsules, key/value inputs, explicit signals, real embeddings and core transitions. Do not call the browser/PAM protocol or an LLM to manufacture source truth.
2. Bind namespace+idempotency key to a canonical payload digest. Same key/same payload replays the original receipt with no second effect; same key/different payload is a conflict.
3. Validate and compute a bounded candidate transition before commit; atomically persist effect/receipt/source mapping and publish the committed state. Cancellation before commit has no effect; loss of response after commit does not undo the commit.
4. Implement explicit feedback and correction effects separately from recall. Advance logical time only once per eligible committed event. Restart recovery must reconcile commit-to-ack ambiguity.
5. Keep durable publication coherent with live reads: after a successful mutation acknowledgement, new reads observe at least that commit; a crash or publication failure after commit yields truthful committed/effect-unknown handling and recovery, never a false no-effect retry that learns twice.

## Acceptance

1. A real-text observe→recall journey returns supported content with nonzero learned state and no Python subprocess.
2. Duplicate delivery changes neither intensity nor source count nor terrain/fatigue/tick a second time.
3. Crash before commit, after commit and before ack, after publication and during checkpoint all have exact effect outcomes.
4. A failed embed/readiness step never produces a successful observation receipt.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-runtime --test engine_transactions --locked

## Sources

- [S03: Biomem center algorithms](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/memory_centers.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)
- [S14: Existing NCM adapter boundary](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/crates/tracedecay-memory-provider-ncm/src/lib.rs)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
