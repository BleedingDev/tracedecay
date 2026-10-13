---
name: tdmem-1103
overview: "Show Native explicit records and NCM cognitive summaries through one shell without flattening their semantics."
todos:
  - id: tdmem-1103-deliver
    content: "Deliver Bead tdmem-1103: Add provider-aware memory lifecycle inspection; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1103: Add provider-aware memory lifecycle inspection

## Execution Notes

Beads issue: `tdmem-1103`. Current Beads status at generation: `open`.

Show Native explicit records and NCM cognitive summaries through one shell without flattening their semantics.

Design authority:

Use capability-specific panels/views. Label provider-native strength, decay, consolidation, activation, and host lifecycle separately.

Acceptance authority:

- [ ] Native and NCM views are honest about different models.
- [ ] Unavailable details are typed.
- [ ] No latent reconstruction is presented as canonical fact.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1103` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0405, tdmem-0708, tdmem-1102.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
