---
name: tdmem-1102
overview: "Let developers trace why an experience entered a provider and how it affected a coding session."
todos:
  - id: tdmem-1102-deliver
    content: "Deliver Bead tdmem-1102: Expose observation \u2192 recall \u2192 pack \u2192 outcome audit traces; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1102: Expose observation → recall → pack → outcome audit traces

## Execution Notes

Beads issue: `tdmem-1102`. Current Beads status at generation: `open`.

Let developers trace why an experience entered a provider and how it affected a coding session.

Design authority:

Join host receipts, outbox attempts, provider traces, context selection, and outcome attribution through stable correlation IDs.

Acceptance authority:

- [ ] A selected item can be traced to source observation/evidence when available.
- [ ] Denied and dropped candidates have reasons.
- [ ] Trace is bounded and redaction-safe.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1102` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0503, tdmem-0608, tdmem-0801.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
