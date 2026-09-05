# ncm-rs-020 — Measure bounded resource use, latency and saturation behavior

Status: planned, not executed.
Owner: Performance / reliability verifier.
Dependencies: ncm-rs-018.
External gates: none.

## Objective

Prove the fixed-capacity kernel does not hide unbounded logs, text, namespaces or worker memory.

## Owned paths

- `crates/tracedecay-memory-ncm-runtime/benches/ncm_scale.rs`
- `scripts/product/ncm/benchmark/`
- `product/ncm/performance/`

## Implementation

1. Benchmark empty, sparse, full 512/4096 center state; 1/4 resident namespaces; 10k/100k operation traces; varied record sizes and adversarial repeated unique IDs.
2. Measure pre-embedded kernels, real encoding, IPC, durable commit, end-to-end recall and maintenance separately. Include p50/p95/p99, peak RSS, allocated bytes, disk including WAL/snapshots, queue occupancy and cancellation completion.
3. Run mixed observe/recall/consolidate/delete/restart workloads under the frozen hardware/profile/feature set. Missing hardware/model is blocked environment, not a waived test.
4. Compare to the pinned Python reference and a simple flat-vector baseline only where workloads/representation/semantics match. Do not claim microsecond full-text recall from kernel-only results.

## Acceptance

1. Hard quotas hold or operations reject before exceeding them; state retention/replay obligations survive journal compaction.
2. The N03 budget manifest’s predeclared limits are met; any proposed limit change is a reviewed versioned decision before rerunning acceptance.
3. No warm read or maintenance job can starve host deadlines or create an unlimited backlog.
4. Long-lived tests include fixed kernel capacity and separately bounded ancillary storage; no infinite lossless-memory claim.

## Verification targets

1. cargo bench -p tracedecay-memory-ncm-runtime --bench ncm_scale --locked
2. ncm_saturation_journey (planned)

## Sources

- [S02: Biomem configuration](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/config.py)
- [S17: Existing provider-neutral evaluation](https://github.com/BleedingDev/tracedecay/blob/25778c7443cd0cfe257da363da01a56ea1d45d3f/product/evaluation/README.md)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
