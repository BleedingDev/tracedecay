---
name: tdmem-0406
overview: "Exercise an actual supported host path through observe, recall, context injection, outcome, and later recall."
todos:
  - id: tdmem-0406-deliver
    content: "Deliver Bead tdmem-0406: Run a real Native-provider coding-agent journey; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0406: Run a real Native-provider coding-agent journey

## Execution Notes

Beads issue: `tdmem-0406`. Current Beads status at generation: `open`.

Exercise an actual supported host path through observe, recall, context injection, outcome, and later recall.

Design authority:

Use a hermetic repository fixture and real host adapter/daemon routes. Direct test-only provider calls do not satisfy this bead.

Acceptance authority:

- [ ] The journey crosses the actual host and application boundaries.
- [ ] Exact repository/worktree identity is asserted.
- [ ] The recalled item carries provenance and receives an attributed outcome.
- [ ] Restart preserves behavior.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0406` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0403, tdmem-0404, tdmem-0405.
- Beads parent/hierarchy references: tdmem-0400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
