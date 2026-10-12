---
name: tdmem-1107
overview: "Treat provider implementations as isolated but potentially faulty components."
todos:
  - id: tdmem-1107-deliver
    content: "Deliver Bead tdmem-1107: Test malicious, crashing, hanging, and protocol-violating providers; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1107: Test malicious, crashing, hanging, and protocol-violating providers

## Execution Notes

Beads issue: `tdmem-1107`. Current Beads status at generation: `open`.

Treat provider implementations as isolated but potentially faulty components.

Design authority:

Fuzz malformed frames/results, excessive payloads, sequence violations, hangs, crashes, fork bombs where safely simulatable, and false capability claims.

Acceptance authority:

- [ ] Host remains bounded and usable.
- [ ] Violations produce typed quarantine/unavailable state.
- [ ] No provider can write outside admitted state paths or host authorities.
- [ ] Restart policy cannot hot-loop.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1107` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0504.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
