---
name: tdmem-0903
overview: "Ensure evaluation observers cannot affect product outputs or canonical state."
todos:
  - id: tdmem-0903-deliver
    content: "Deliver Bead tdmem-0903: Mechanically prove observer-provider isolation; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0903: Mechanically prove observer-provider isolation

## Execution Notes

Beads issue: `tdmem-0903`. Current Beads status at generation: `open`.

Ensure evaluation observers cannot affect product outputs or canonical state.

Design authority:

Use separate routing types and compare product hashes with observer enabled/disabled. Deny observer write capabilities to canonical authorities.

Acceptance authority:

- [ ] Product outputs remain identical.
- [ ] Observer failures do not alter active provider behavior.
- [ ] Observer attempts to write canonical state are rejected.
- [ ] Isolation is enforced in types/policy, not convention alone.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0903` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0607.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
