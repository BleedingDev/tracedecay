---
name: tdmem-1009
overview: "Validate host behavior when the active provider is down, slow, cancelled, corrupt, or reset-required."
todos:
  - id: tdmem-1009-deliver
    content: "Deliver Bead tdmem-1009: Prove typed provider unavailability, timeout, cancellation, and rollback; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1009: Prove typed provider unavailability, timeout, cancellation, and rollback

## Execution Notes

Beads issue: `tdmem-1009`. Current Beads status at generation: `open`.

Validate host behavior when the active provider is down, slow, cancelled, corrupt, or reset-required.

Design authority:

No case may silently look like successful empty recall. Existing code/session/native surfaces remain usable according to explicit policy.

Acceptance authority:

- [ ] All terminal classes are exercised through real host routes.
- [ ] No hidden provider substitution occurs.
- [ ] Foreground latency remains bounded.
- [ ] Operator repair hints are accurate.
- [ ] Rollback to disabled/Native-only mode is tested.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1009` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0504, tdmem-0607, tdmem-1001, tdmem-1002.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
