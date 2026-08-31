---
name: tdmem-1206
overview: "Gate every sync candidate on Zack parity and our provider product journeys."
todos:
  - id: tdmem-1206-deliver
    content: "Deliver Bead tdmem-1206: Build convergence CI: upstream parity plus product regression; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1206: Build convergence CI: upstream parity plus product regression

## Execution Notes

Beads issue: `tdmem-1206`. Current Beads status at generation: `open`.

Gate every sync candidate on Zack parity and our provider product journeys.

Design authority:

Run upstream-required suites first, then contracts, architecture rules, Native parity, provider conformance, crash/scope/security journeys, and generated drift checks.

Acceptance authority:

- [ ] Upstream failures cannot be hidden by product tests.
- [ ] Product regressions cannot be dismissed as upstream changes.
- [ ] Gate output identifies owning bead/area.
- [ ] Required versus informational lanes are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1206` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0403, tdmem-1205.
- Beads parent/hierarchy references: tdmem-1200. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
