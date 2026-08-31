---
name: tdmem-0803
overview: "Model candidate, established, proven, deprecated, and retired lifecycle for explicit procedural lessons."
todos:
  - id: tdmem-0803-deliver
    content: "Deliver Bead tdmem-0803: Define provider-neutral lesson maturity and promotion policy; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0803: Define provider-neutral lesson maturity and promotion policy

## Execution Notes

Beads issue: `tdmem-0803`. Current Beads status at generation: `open`.

Model candidate, established, proven, deprecated, and retired lifecycle for explicit procedural lessons.

Design authority:

Maturity applies to host-visible curated lessons, not arbitrary latent provider traces. Promotion requires evidence/outcomes; human explicit authority cannot be fabricated automatically.

Acceptance authority:

- [ ] Transition table and evidence requirements are versioned.
- [ ] Promotion and demotion have hysteresis/sample-size safeguards.
- [ ] Pinned/human rules have explicit override and demotion semantics.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0803` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0802.
- Beads parent/hierarchy references: tdmem-0800. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
