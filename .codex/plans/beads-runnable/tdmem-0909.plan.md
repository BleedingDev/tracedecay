---
name: tdmem-0909
overview: "Support review of whether a recalled item was useful, harmful, stale, irrelevant, or unverifiable."
todos:
  - id: tdmem-0909-deliver
    content: "Deliver Bead tdmem-0909: Create human adjudication workflow for ambiguous recalls; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0909: Create human adjudication workflow for ambiguous recalls

## Execution Notes

Beads issue: `tdmem-0909`. Current Beads status at generation: `open`.

Support review of whether a recalled item was useful, harmful, stale, irrelevant, or unverifiable.

Design authority:

Blind reviewers to provider where practical. Store labels, reasons, disagreements, and resolution while protecting code secrets.

Acceptance authority:

- [ ] Adjudication schema is versioned.
- [ ] Inter-reviewer disagreement is visible.
- [ ] Labels can feed metrics without silently training providers.
- [ ] Sensitive fixtures have redaction rules.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0909` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0904.
- Beads parent/hierarchy references: tdmem-0900. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
