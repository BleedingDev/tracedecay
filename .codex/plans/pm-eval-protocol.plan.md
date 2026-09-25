---
name: pm-eval-protocol
overview: Pin a fair, non-vacuous comparison protocol that reuses the existing corpus and evaluator and separates correctness, usefulness and resource cost.
todos:
  - id: inventory-comparison-surfaces
    content: Map existing corpus, baseline runner, metrics, annotations, real Native/NCM runners and benchmark populations to a single host comparison input/output contract.
    status: completed
  - id: pin-matched-trials
    content: Specify identical provider inputs and host budgets, fresh held-out cases, labels, safety thresholds, latency/resource sampling and failure accounting before any new comparative run.
    status: completed
isProject: false
---

# pm-eval-protocol

## Execution Notes

Read `pm-finish-brief.md`, `product/evaluation/README.md`, `product/ncm/evaluation/protocol.md`, `product/ncm/performance/README.md`, conformance `baseline.rs` and evaluation entry points. Own `.codex/plans/pm-comparison-protocol.md`. The output is an executable runner specification: real Native, real MiniLM NCM, no-memory, explicit-documentation; Python only for fidelity. Reuse existing scenario/metric types and annotate real selected content. List where current keyword overlap or missing labels is insufficient to measure task benefit.

## Constraints

No benchmark or model execution; no source changes in the first wave. No fabricated labels, empty-admission safety passes, reuse of inspected regression cases as unseen held-out data, raw-score cross-provider comparison or estimated tokens presented as production counts. Pin resource and sample budgets before heavy work; do not tune against held-out output.

## Operator Guidance

Independent root feeding `pm-comparison-harness`. Use at least 100 measured warm calls per declared latency population, paired randomized/counterbalanced order and repeated cold-process trials; keep actual sample counts and confidence/dispersion visible. Existing p95 ceilings are targets, not measured results. Root finalizes the protocol before the harness or any comparative measurement starts.
