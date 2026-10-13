---
name: tdmem-1300
overview: "Reserve a first-class provider path for OCEAN without inventing its behavior, data model, or capabilities before the colleague-owned research and specification are ready."
todos:
  - id: tdmem-1300-deliver
    content: "Deliver Bead tdmem-1300: Deferred \u2014 Add OCEAN after its versioned specification stabilizes; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1300: Deferred — Add OCEAN after its versioned specification stabilizes

## Execution Notes

Beads issue: `tdmem-1300`. Current Beads status at generation: `deferred`.

Reserve a first-class provider path for OCEAN without inventing its behavior, data model, or capabilities before
the colleague-owned research and specification are ready.

Design authority:

Reuse the same provider contract, conformance, observer isolation, evaluation corpus, and activation gates as
NCM. Contract gaps discovered by OCEAN must be solved generically rather than by provider-name conditionals.

Acceptance authority:

- [ ] A versioned OCEAN specification and owner-approved capability declaration exist.
- [ ] OCEAN passes mandatory provider conformance without weakening existing contracts.
- [ ] Observer-mode differential results are available before active mode is considered.
- [ ] Active mode remains disabled unless safety and outcome gates pass.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1300` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0907.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
