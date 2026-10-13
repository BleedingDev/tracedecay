---
name: tdmem-0711
overview: "Allow NCM recall to influence coding-agent context only under explicit opt-in configuration."
todos:
  - id: tdmem-0711-deliver
    content: "Deliver Bead tdmem-0711: Enable guarded active NCM experiment after objective gates pass; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0711: Enable guarded active NCM experiment after objective gates pass

## Execution Notes

Beads issue: `tdmem-0711`. Current Beads status at generation: `open`.

Allow NCM recall to influence coding-agent context only under explicit opt-in configuration.

Design authority:

Activation checks conformance report, supported scope, state compatibility, safety thresholds, and provider health. Rollback to Native-only behavior is explicit and does not destroy NCM state.

Acceptance authority:

- [ ] Active mode cannot be enabled without passing gate artifacts.
- [ ] Provider identity is visible in final context.
- [ ] Typed degradation works without silent substitution.
- [ ] Rollback is tested.
- [ ] Stale-correction and isolation journeys pass.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0711` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0710, tdmem-0908, tdmem-1008, tdmem-1009.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
