---
name: pm-measure
overview: Run the matched real-provider comparison and report usefulness, correctness, latency and memory cost with reproducible evidence.
todos:
  - id: run-matched-quality-comparison
    content: Execute accepted regression and fresh held-out host retrieval populations through all comparison lanes and compute real usefulness, safety, provenance and token metrics, with downstream model task benefit separately identified when authorized and measured.
    status: pending
  - id: run-latency-resource-populations
    content: Measure paired cold/warm host and backend phases across declared scales and namespace/concurrency populations, retaining failures and actual sample/resource counts.
    status: pending
  - id: resolve-measured-defects
    content: Fix substantive compatibility or performance defects in their owning lanes and repeat only affected populations, then publish a qualified Native-versus-NCM comparison without changing thresholds.
    status: pending
isProject: false
---

# pm-measure

## Execution Notes

Read the brief and frozen comparison protocol. This is root-owned execution, with a separate read-only reviewer checking claims and denominators. Use real artifacts and the exact host route. Report Native and NCM independently even if one loses; compatibility is not a promise of equal algorithmic quality. Existing backend results are context, not measurements of new host code.

## Constraints

Own measured outputs under `product/evaluation/` and `product/ncm/performance/` as appropriate. No worker launches a heavy run. No tuning on held-out data, suppression of timeouts, fake allocation numbers, unqualified RSS deltas or replacement of production tokenizer costs. Any real workload needing external paid model calls is kept separate until explicitly authorized; local deterministic/retrieval evidence proceeds.

## Operator Guidance

Dependency: `pm-comparison-connect`. Run one heavy process through cargo-hauler; reuse warm build artifacts and bound mutable test state. Publish commands, build/hardware/mode and actual observations with the result. Revisit provider implementation only for measured defects, and keep the common acceptance thresholds fixed unless a substantive requirement change is explicitly explained. Paid/model-driven downstream task experiments are optional and require their own authorized runner; report them as unmeasured when absent. Required core comparability is real host retrieval usefulness, safety, source fidelity, tokens, latency and resources, not a fabricated agent-task score.
