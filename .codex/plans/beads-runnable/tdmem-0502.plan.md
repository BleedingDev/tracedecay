---
name: tdmem-0502
overview: "Create the durable boundary between committed TraceDecay actions and provider delivery."
todos:
  - id: tdmem-0502-deliver
    content: "Deliver Bead tdmem-0502: Define and persist the provider outbox/journal authority; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0502: Define and persist the provider outbox/journal authority

## Execution Notes

Beads issue: `tdmem-0502`. Current Beads status at generation: `open`.

Create the durable boundary between committed TraceDecay actions and provider delivery.

Design authority:

The outbox is the source of truth for delivery status, attempts, acknowledgements, and replay position. It must not become a second authority for Native facts.

Acceptance authority:

- [ ] Outbox insertion is atomic with or causally bound to the committed host action.
- [ ] Rows have stable idempotency and exact source sequence.
- [ ] Delivery state survives restart.
- [ ] Retention and privacy deletion rules are explicit.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0502` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0501.
- Beads parent/hierarchy references: tdmem-0500. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
