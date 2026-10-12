# ncm-rs-009 — Implement explicit logical time, homeostasis and fatigue

Status: planned, not executed.
Owner: Core / dynamics.
Dependencies: ncm-rs-007, ncm-rs-008.
External gates: none.

## Objective

Make forgetting and sleep scheduling reproducible across retries, idle periods and restart.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/dynamics/`
- `crates/tracedecay-memory-ncm-core/tests/dynamics.rs`

## Implementation

1. Expose explicit logical Advance operations. Freeze the v1 Observe schedule as one learning tick per unique committed observation; any departure from the Python caller’s separate store/step schedule is recorded.
2. Implement separate center intensity/value/emotion leak rates, age counters, terrain evolution, fatigue leak/accumulation and consolidation eligibility.
3. Resolve the reference differences: documentation describes omega-based fatigue, TextMemory passes input intensity, and AutomaticConsolidator has a 100-step minimum interval. Pin actual selected behavior; do not infer wall-clock years from step-based coefficients.
4. Persist fatigue, tick, steps_since_consolidation, and last maintenance identity. Bound large Advance requests; never execute an unbounded per-microsecond loop.

## Acceptance

1. Duplicate observations and read-only calls do not advance time or fatigue.
2. Closed-form decay checks agree with explicit updates in their valid domain; no claim of calendar half-life exists without a documented clock mapping.
3. Split-run and uninterrupted runs have the same sleep boundaries and state under the same explicit schedule.
4. Threshold crossings and the 99/100/101 interval boundary are covered.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test dynamics --locked

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S05: Biomem consolidation](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/consolidation.py)
- [S07: Biomem text-memory orchestration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/text_memory.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
