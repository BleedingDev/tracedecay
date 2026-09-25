---
name: pm-host-journeys
overview: Prove interchangeable Native and NCM behavior through shipped CLI/daemon hooks across sessions, lifecycle operations and provider changes.
todos:
  - id: parameterize-real-provider-journeys
    content: Drive the same Claude and Codex host journey with Native active and NCM active, including authorized session A to B recall with original source attribution.
    status: pending
  - id: prove-provider-lifecycle-and-switching
    content: Verify feedback, correction, deletion, worker/daemon restart, replay, snapshot anti-resurrection and Native-to-NCM-to-Native switching without losing canonical facts.
    status: pending
  - id: prove-isolation-and-failure-matrix
    content: Verify observer on/off output equality, disabled zero work, forbidden scope denial, missing model, corrupt state, cancellation, timeouts and no hidden provider substitution.
    status: pending
isProject: false
---

# pm-host-journeys

## Execution Notes

Read the brief and completed host path. Extend `crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs` using an explicit provider fixture parameter and a reusable host control seam for the comparison harness. Preserve the causal control that startup import is settled before hook-only evidence is written. Assert exact source/candidate/provider attribution in final context, not only hook exit status or row counts.

## Constraints

Own that CLI journey fixture and directly shared new fixture modules. Root owns the exclusive nextest grouping and manifests. No production edits, raw private provider-DB reads as proof of host behavior, sleeps replacing durable barriers, extra retry loops masking failures or global environment mutation.

## Operator Guidance

Dependencies: `pm-host-recall`, `pm-conformance`. Root runs real model tests sequentially in the existing subprocess group with retries disabled. Native and NCM use separate mutable state but the same pinned input/model artifacts where applicable. Test failures return exact phase and typed outcome; fix the responsible producer before repeating the affected subset. Final representative host suite must pass two consecutive runs after the final relevant change.
