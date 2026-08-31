---
name: tdmem-0905
overview: "Run identical observations and recall requests against Native and NCM and record comparable outputs."
todos:
  - id: tdmem-0905-deliver
    content: "Deliver Bead tdmem-0905: Implement Native versus NCM differential runner; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0905: Implement Native versus NCM differential runner

## Execution Notes

Beads issue: `tdmem-0905`. Current Beads status at generation: `open`.

Run identical observations and recall requests against Native and NCM and record comparable outputs.

Design authority:

Keep providers independent. Differential reports compare admitted candidates and downstream task effects; they do not force internal state equivalence.

Acceptance authority:

- [ ] Provider/build/config identities are pinned.
- [ ] Native/raw scores are preserved separately.
- [ ] Differences are traceable to scenario steps.
- [ ] Runner supports NCM observer mode.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0905` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0709, tdmem-0902, tdmem-0903, tdmem-0904.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
