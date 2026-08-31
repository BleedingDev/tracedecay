---
name: tdmem-1007
overview: "Have one coding-agent session learn a project-specific useful lesson and a later session use it successfully."
todos:
  - id: tdmem-1007-deliver
    content: "Deliver Bead tdmem-1007: Prove cross-session useful-experience reuse; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1007: Prove cross-session useful-experience reuse

## Execution Notes

Beads issue: `tdmem-1007`. Current Beads status at generation: `open`.

Have one coding-agent session learn a project-specific useful lesson and a later session use it successfully.

Design authority:

Use a real repository fixture where rediscovery is measurable. Compare Native and NCM observer/active behavior through the same host.

Acceptance authority:

- [ ] The later session receives relevant bounded context.
- [ ] Repeated discovery decreases.
- [ ] The recalled item has source provenance.
- [ ] Successful use emits attributed feedback.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1007` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0802, tdmem-0904, tdmem-1001.
- Beads parent/hierarchy references: tdmem-1000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
