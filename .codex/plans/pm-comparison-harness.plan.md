---
name: pm-comparison-harness
overview: Prepare the provider-neutral comparison runner, metric joins and measurement capture behind the shared host execution interface while production host integration proceeds.
todos:
  - id: prepare-host-comparison-runner
    content: Implement the shared scenario-action and delivered-context interface for Native, real NCM, no-memory and documentation with identical inputs and existing metric joins, ready for the actual host fixture connection.
    status: completed
  - id: capture-cost-and-resource-boundaries
    content: Record cold and warm phase timings, p50/p95/p99 populations, exact tokenizer counts, source admission, incremental RSS, worker counts, queue pressure, disk and cancellation completion.
    status: completed
  - id: verify-comparison-integrity
    content: Test mismatched inputs, missing or censored samples, provider substitution, vacuous safety, unmeasured labels and phase attribution so the runner cannot report a false win.
    status: completed
isProject: false
---

# pm-comparison-harness

## Execution Notes

Read the brief, accepted comparison protocol and common contracts. Reuse `BaselineRunOutput`, `MetricReport` and existing metric catalog. Keep canonical authority contributions equal and report provider-only deltas separately. Use actual selected outputs for relevance/provenance labels; do not score a backend result the host rejected. Keep Python fidelity and full host comparison separate.

## Constraints

Own `crates/tracedecay-memory-evaluation/`, new runner scripts beneath `scripts/product/memory-comparison/`, and new comparison artifacts under `product/evaluation/`. Root supplies the real-host factory/fixture seam and manifest edits. Do not edit conformance runner/shared baseline files concurrently; request a narrow interface change. No model runs or heavy benchmark jobs from a worker. No tuning providers or thresholds while implementing the evaluator.

## Operator Guidance

Dependencies: `pm-contract`, `pm-eval-protocol`, `pm-conformance`. Verify runner/report honesty with small fixtures first; real production measurements are `pm-measure`. The output must include commands, sample counts, hardware/build/mode, cold/warm distinction, failed/timeout samples and memory baseline. Do not equate backend kernel latency, total test duration or source-file presence with host performance.

This preparation node can complete with discriminating controlled fixtures, but it cannot claim a real host comparison. `pm-host-journeys` produces the reusable real host-control seam; the separate downstream `pm-comparison-connect` node owns the actual connection and proves it before measurements. Keep the action/capture signature concrete in the common conformance handoff so preparatory work does not invent a second host policy.

## Verification

Root reviewed the runner, byte/token/source joins, statistics, adjudication and durable event recovery. All 55 Python tests passed after the final metadata and challenge-target fixes; cc-1334 passed the Rust evaluation all-targets run, including nine host-retrieval join tests and seventeen metric tests. Interrupted-process fixtures preserve earlier action evidence and all planned denominators. These are preparation checks; production host comparison and downstream task benefit remain unmeasured.
