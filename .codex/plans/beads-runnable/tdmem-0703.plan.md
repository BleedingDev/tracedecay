---
name: tdmem-0703
overview: "Expose the actual NCM build and state model through the provider identity contract."
todos:
  - id: tdmem-0703-deliver
    content: "Deliver Bead tdmem-0703: Implement NCM handshake, manifest, capabilities, and health; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0703: Implement NCM handshake, manifest, capabilities, and health

## Execution Notes

Beads issue: `tdmem-0703`. Current Beads status at generation: `open`.

Expose the actual NCM build and state model through the provider identity contract.

Design authority:

Advertise only implemented capabilities. Health distinguishes process alive, state loaded, ready for observe, ready for recall, degraded, and reset required.

Acceptance authority:

- [ ] Handshake passes conformance.
- [ ] State/build identity is present.
- [ ] Unsupported operations are explicit.
- [ ] False readiness fixtures fail.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0703` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0702.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
