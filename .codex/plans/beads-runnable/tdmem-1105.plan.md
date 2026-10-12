---
name: tdmem-1105
overview: "Prevent memory content from becoming an untrusted command channel or secret exfiltration vector."
todos:
  - id: tdmem-1105-deliver
    content: "Deliver Bead tdmem-1105: Harden secret, prompt-injection, and untrusted-memory boundaries; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-1105: Harden secret, prompt-injection, and untrusted-memory boundaries

## Execution Notes

Beads issue: `tdmem-1105`. Current Beads status at generation: `open`.

Prevent memory content from becoming an untrusted command channel or secret exfiltration vector.

Design authority:

Classify provider recall as untrusted advisory text. Apply sanitization, provenance/trust labels, instruction boundary formatting, and policy checks before context injection.

Acceptance authority:

- [ ] Malicious memory cannot escape its context section or invoke tools directly.
- [ ] Secret fixtures are denied/redacted.
- [ ] Untrusted provenance lowers or blocks admission by policy.
- [ ] Security regressions run in CI.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-1105` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0507, tdmem-0605.
- Beads parent/hierarchy references: tdmem-1100. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
