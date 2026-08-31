---
name: tdmem-0700
overview: "Integrate the licensed NCM/Biomem implementation behind the provider boundary without leaking TraceDecay coding assumptions into its internal cognitive model."
todos:
  - id: tdmem-0700-deliver
    content: "Deliver Bead tdmem-0700: M6 \u2014 Integrate NCM/Biomem as the first cognitive provider; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0700: M6 — Integrate NCM/Biomem as the first cognitive provider

## Execution Notes

Beads issue: `tdmem-0700`. Current Beads status at generation: `open`.

Integrate the licensed NCM/Biomem implementation behind the provider boundary without leaking TraceDecay
coding assumptions into its internal cognitive model.

Design authority:

Start with a surface audit and capability mapping. Implement an isolated adapter crate and provider lifecycle.
Run NCM in observer mode first. Active mode is guarded behind conformance, evaluation, scope-isolation, stale-
correction, restart, and latency gates.

Acceptance authority:

- [ ] NCM passes mandatory provider conformance and declares unsupported capabilities honestly.
- [ ] NCM state can be restored or deterministically replayed according to an explicit compatibility contract.
- [ ] Observer mode cannot influence prompts, Native facts, or agent actions.
- [ ] Active mode is opt-in and blocked until objective gates pass.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0700` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: tdmem-0508, tdmem-0609.
- Beads parent/hierarchy references: tdmem-0000. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
