# Follow-up proposal only: action-level shared presentation review

The frozen patch remains unchanged. This proposal resolves shared-prose uncertainty by using an independent blinded review of the exact presentation; it adds no production host-framing API, token exemption, synthetic candidate or CandidateLabel.

## Add one annotation row kind

Continue accepting existing candidate annotations without a `kind` field. Accept `kind: "shared_presentation_v1"` as a separate row, keyed uniquely by `(case_trial_id, request_id)`:

```json
{
  "kind": "shared_presentation_v1",
  "case_trial_id": "actual scheduled case-trial ID",
  "request_id": "actual scheduled query ID",
  "representation": "tool_result_v1",
  "advisory_review_sha256": "exact ordered shared-presentation digest",
  "reviewer_id": "reviewer identity",
  "provider_blinded": true,
  "reviewed_all_shared_text": true,
  "source_fidelity": {
    "outcome": "pass",
    "reason": "All source claims checked, or explicitly no substantive source claims.",
    "no_substantive_source_claims": true,
    "evidence": []
  },
  "prohibited_claims": {
    "outcome": "pass",
    "reason": "Every shared block was checked against this frozen query's prohibited claims."
  }
}
```

`outcome` uses the existing check vocabulary `pass`, `fail`, `indeterminate`; it is not a candidate label. The two verdicts are separate. A hash match and reviewer identity alone never count as assessment.

The digest binds the existing length-prefixed ordered advisory-block projection, including the complete advisory member and every conservatively charged unclassified block. The reviewer must assess that exact whole presentation. Source claims require the existing frozen-source quotation checks, with exact presentation block/span or decoded JSON-string references from the original parser. An explicit no-source-claims assessment is permitted only when the reviewer finds none; it is not inferred from an empty candidates array. Missing, incomplete or ambiguous evidence is indeterminate. The existing blinding contract still applies; hiding substantive provider prose to manufacture a blinded review is not valid.

## Minimal runner/exporter changes

Partition annotation inputs before constructing the current candidate-key map. Keep candidate annotations keyed by `(case_trial_id, request_id, candidate_ref)`. Keep shared rows in a second map keyed by `(case_trial_id, request_id)`. Reject duplicate/unknown row kinds, duplicate keys, unscheduled targets, absent delivery and unused rows independently in both maps. A shared row has no `candidate_ref` and can never enter the candidate map.

Validate the shared digest and review fields, then export one optional `shared_presentation_assessment` alongside the existing ToolResult evidence. Include the consumed row in the existing run-identity binding. Candidate annotations, source/fact mappings and precision/recall candidate collections remain unchanged. Shared rows do not cover missing candidate labels or unjoined emitted candidates.

A passing shared assessment removes only the uncertainty caused by substantive shared text lacking a candidate reviewer. Source-fidelity failure contributes to the existing `source_fidelity` check; prohibited-claim failure fails the existing query safety/rubric check. Either indeterminate verdict prevents a passing query. Existing terminal expectations, required-fact coverage and nonvacuous safety requirements still apply. In particular, zero candidates cannot satisfy a query that requires useful delivered facts.

## Independent verification and clean-zero requirements

Factor retained-trace validation out of the per-candidate bound-join function. For a provider action with zero emitted candidates, a passing shared review additionally requires the actual empty emitted array and the observed retained trace artifact, request/provider/registration/full-scope binding, exact trace item partition and actual terminal evidence. A human review cannot replace missing host trace/control evidence, prove provider contact, or turn a cache stub into a delivered body. A no-memory control uses its existing verified provider-disabled/noncontact evidence; it must not invent a provider trace merely because an unclassified warning was charged to the advisory budget.

The Rust verifier independently binds `shared_presentation_assessment` to the exact recomputed ordered digest and requires the same explicit review checks. Replace the current broad zero-candidate `any nonempty advisory text` unresolved condition with `any shared text && no valid passing shared assessment`. Keep unresolved final joins and missing candidate annotations as separate conditions that a shared row cannot clear. A failed shared verdict must be reflected as a failed existing check; an indeterminate verdict cannot permit a pass.

All original carrier/block bytes and lexical spans remain unchanged and fully charged. Candidate arrays stay empty for zero-candidate output, so candidate precision has the same zero population and nonvacuous provider safety remains unresolved until its existing positive witness exists elsewhere. No scheduled action/query, held-out denominator, label vocabulary, timing boundary or ceiling changes.

## Minimal development-only regressions for a later approved patch

- Exact blinded shared review plus structurally verified healthy empty provider output removes only shared-text uncertainty; candidate precision remains a zero population.
- Missing review, wrong digest, incomplete review, absent retained trace/scope, and indeterminate verdict stay unresolved.
- A prohibited claim fails the existing safety/query check even with zero candidates.
- An unresolved emitted candidate or missing candidate annotation remains unresolved despite a passing shared row.
- Duplicate/unused/unscheduled shared rows reject; their presence never creates a candidate or an extra query.
- Every charged token and the complete action timing remain identical before and after annotation.

This is a proposal only. No source/schema change, live apply, held-out run or Cargo execution is included.
