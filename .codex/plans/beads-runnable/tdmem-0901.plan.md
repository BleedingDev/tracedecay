---
name: tdmem-0901
overview: "Version realistic but hermetic scenarios for long-running coding-agent memory behavior."
todos:
  - id: tdmem-0901-deliver
    content: "Deliver Bead tdmem-0901: Create the deterministic coding-memory scenario corpus; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0901: Create the deterministic coding-memory scenario corpus

## Execution Notes

Beads issue: `tdmem-0901`. Current Beads status at generation: `open`.

Version realistic but hermetic scenarios for long-running coding-agent memory behavior.

Design authority:

Include stale project change, failed approach, cross-agent reuse, project/worktree scope, contradiction, restart, cancellation, provider corruption, and privacy deletion.

Acceptance authority:

- [ ] Each scenario defines observations, code/evidence revisions, expected admissible behavior, and adjudication rubric.
- [ ] Fixtures are deterministic and secret-free.
- [ ] Scenarios can run against any provider.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0901` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
