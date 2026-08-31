---
name: tdmem-0608
overview: "Explain which provider items were requested, denied, selected, deduplicated, truncated, or injected."
todos:
  - id: tdmem-0608-deliver
    content: "Deliver Bead tdmem-0608: Expose context selection and provider contribution traces; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0608: Expose context selection and provider contribution traces

## Execution Notes

Beads issue: `tdmem-0608`. Current Beads status at generation: `open`.

Explain which provider items were requested, denied, selected, deduplicated, truncated, or injected.

Design authority:

Trace output must be bounded and redaction-safe. It distinguishes provider explanation from host admission rationale.

Acceptance authority:

- [ ] Each selected item has provider and host-selection reasons.
- [ ] Denied items expose stable reason codes.
- [ ] Token and section decisions are visible.
- [ ] Trace can be correlated with later outcomes.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0608` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0605, tdmem-0606, tdmem-0607.
- Beads parent/hierarchy references: tdmem-0600. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
