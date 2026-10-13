---
name: tdmem-1404
overview: "Document realistic risks of long-lived coding-agent memory and the implemented controls."
todos:
  - id: tdmem-1404-deliver
    content: "Deliver Bead tdmem-1404: Write the alpha threat model, privacy model, and known limitations; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1404: Write the alpha threat model, privacy model, and known limitations

## Execution Notes

Beads issue: `tdmem-1404`. Current Beads status at generation: `open`.

Document realistic risks of long-lived coding-agent memory and the implemented controls.

Design authority:

Cover secret retention, stale poisoning, prompt injection, cross-scope leakage, provider compromise, snapshot deletion, telemetry, and supply chain.

Acceptance authority:

- [ ] Threats map to tests/controls or explicit residual risk.
- [ ] Data locations and retention are documented.
- [ ] Provider explanations are not overstated.
- [ ] Opt-in telemetry is content-free.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1404` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-1105, tdmem-1106, tdmem-1107.
- Beads parent/hierarchy references: tdmem-1400. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
