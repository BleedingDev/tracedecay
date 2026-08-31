---
name: tdmem-1402
overview: "Produce reproducible binaries/artifacts for supported platforms and features."
todos:
  - id: tdmem-1402-deliver
    content: "Deliver Bead tdmem-1402: Build the alpha CI and release artifact matrix; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1402: Build the alpha CI and release artifact matrix

## Execution Notes

Beads issue: `tdmem-1402`. Current Beads status at generation: `open`.

Produce reproducible binaries/artifacts for supported platforms and features.

Design authority:

Separate upstream parity, product feature, Native-only, NCM observer, and guarded active configurations. Generate checksums and compatibility metadata.

Acceptance authority:

- [ ] All supported artifacts build in CI.
- [ ] Checksums and manifests are published.
- [ ] Unsupported feature combinations are rejected.
- [ ] Release tests run on clean environments.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1402` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1206, tdmem-1401.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
