---
name: tdmem-0807
overview: "Allow validated provider-derived lessons to become explicit canonical project memory."
todos:
  - id: tdmem-0807-deliver
    content: "Deliver Bead tdmem-0807: Create the audited promotion path into TraceDecay Native facts/rules; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0807: Create the audited promotion path into TraceDecay Native facts/rules

## Execution Notes

Beads issue: `tdmem-0807`. Current Beads status at generation: `open`.

Allow validated provider-derived lessons to become explicit canonical project memory.

Design authority:

Promotion creates a Native candidate with source traces, provider identity, evidence refs, scope, validity, and review state. Provider state itself remains non-authoritative.

Acceptance authority:

- [ ] Promotion cannot bypass Native validation.
- [ ] Canonical record links back to provider traces and source observations.
- [ ] Reject/undo leaves an audit receipt.
- [ ] Provider deletion cannot silently delete accepted Native authority.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0807` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0402, tdmem-0803, tdmem-0806.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
