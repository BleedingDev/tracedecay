---
name: tdmem-0805
overview: "Implement blocked-resurrection detection for explicit curated lessons."
todos:
  - id: tdmem-0805-deliver
    content: "Deliver Bead tdmem-0805: Prevent forgotten harmful lessons from reappearing unchanged; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0805: Prevent forgotten harmful lessons from reappearing unchanged

## Execution Notes

Beads issue: `tdmem-0805`. Current Beads status at generation: `open`.

Implement blocked-resurrection detection for explicit curated lessons.

Design authority:

Retain a privacy-safe tombstone/fingerprint and rationale. Match exact and bounded near-duplicate reformulations while permitting genuinely corrected scoped lessons.

Acceptance authority:

- [ ] Exact resurrection is blocked.
- [ ] Near-duplicate harmful reformulations become curation candidates.
- [ ] A corrected lesson with different condition/evidence can be admitted.
- [ ] Privacy deletion semantics are preserved.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0805` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0804.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
