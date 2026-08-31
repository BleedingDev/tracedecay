---
name: tdmem-0710
overview: "Feed NCM real coding observations and collect recall outputs without allowing them into prompts or canonical state."
todos:
  - id: tdmem-0710-deliver
    content: "Deliver Bead tdmem-0710: Run NCM in mechanically isolated observer mode; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0710: Run NCM in mechanically isolated observer mode

## Execution Notes

Beads issue: `tdmem-0710`. Current Beads status at generation: `open`.

Feed NCM real coding observations and collect recall outputs without allowing them into prompts or canonical state.

Design authority:

Observer execution is separate from active routing. Product outputs must be byte-equivalent with observer enabled or disabled, excluding explicit evaluation telemetry.

Acceptance authority:

- [ ] Observer output never reaches context compiler selection.
- [ ] Observer cannot write Native facts or outcomes.
- [ ] Product parity test passes.
- [ ] Evaluation captures latency, errors, stale recall, and candidate quality.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0710` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0709, tdmem-0903.
- Beads parent/hierarchy references: tdmem-0700. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
