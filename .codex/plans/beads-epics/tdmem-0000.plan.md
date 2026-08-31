---
name: tdmem-0000
overview: "Build a commercially viable TraceDecay V2 derivative for coding agents that can use multiple memory systems without coupling their internal representation to TraceDecay's native fact store. The first providers are TraceDecay Native and NCM; OCEAN is a deliberately deferred provider until its versioned specification exists. The program remains a single Rust monorepo and preserves Zackary Jackson's upstream architecture wherever possible. Product-owned crates, contracts, adapters, tests, and integration mounts must be easy to identify, remove, compare, and rebase across future upstream V2 checkpoints."
todos:
  - id: tdmem-0000-deliver
    content: "Deliver Bead tdmem-0000: Program: Pluggable Cognitive Memory Providers for TraceDecay V2; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: pending
isProject: false
---

# tdmem-0000: Program: Pluggable Cognitive Memory Providers for TraceDecay V2

## Execution Notes

Beads issue: `tdmem-0000`. Current Beads status at generation: `open`.

Build a commercially viable TraceDecay V2 derivative for coding agents that can use multiple memory
systems without coupling their internal representation to TraceDecay's native fact store. The first
providers are TraceDecay Native and NCM; OCEAN is a deliberately deferred provider until its versioned
specification exists.

The program remains a single Rust monorepo and preserves Zackary Jackson's upstream architecture wherever
possible. Product-owned crates, contracts, adapters, tests, and integration mounts must be easy to identify,
remove, compare, and rebase across future upstream V2 checkpoints.

Design authority:

Treat TraceDecay as the host and coding-context authority. Add a capability-based provider boundary above
concrete memory implementations. Keep the existing native store authoritative for explicit durable facts;
cognitive providers return advisory recall candidates and receive observations/outcomes through an
idempotent dispatch layer. A shared context compiler validates scope and provenance before provider output
reaches an agent.

Delivery is staged: pin/reproduce upstream, define contracts, create isolated crates, prove Native parity,
add dispatch and recall seams, integrate NCM first as an observer, then enable guarded active mode only after
conformance and evaluation gates pass.

Acceptance authority:

- [ ] The branch remains traceable to the exact upstream PR #707 floor and has a repeatable convergence process.
- [ ] TraceDecay Native works through the provider boundary with no observable regression or forced migration.
- [ ] NCM passes provider conformance and can run in observer mode without influencing the agent.
- [ ] Guarded active NCM mode passes the stale-memory, crash-recovery, scope-isolation, and provider-failure journeys.
- [ ] OCEAN has a reserved provider slot but no speculative implementation is counted as delivered.
- [ ] All program work is represented in versioned .beads/issues.jsonl and governed by inherited agent context.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-0000` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: none. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
