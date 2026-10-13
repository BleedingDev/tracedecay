---
name: tdmem-floor-daemon-test-env-kmw
overview: "Upstream-floor daemon tests fail in the local macOS dev environment with zero product code involved. Reproduced 2026-08-31 on an idle machine, sandbox on and off, at branch HEAD 35fe0f6e0 whose diff vs the upstream floor 08fbe33a7c7f403191f is entirely cfg-gated behind memory-provider-host: 1. `daemon::tests::ownership::project_server_cache_hit_skips_open_and_singleflights_first_miss` deterministically times out in `engine.shutdown_all()` (PHASE_TIMEOUT 20s, panic at ownership.rs:401) \u2014 identically with `--no-default-features --features lite,token-counting,semantic-fastembed,graph-sealed-store,test-helpers`, i.e. compiling exactly the floor's own test body, so the hang is not attributable to the dormant memory-provider host mount. 2. Three more ownership tests fail on the idle machine (`fresh_committed_project_open_mounts_feedback_before_lsp`, `released_automation_tombstone_allows_one_eventual_replacement`, one additional under load variance), 24-25 of 28 pass. 3. `memory_suite` eval tests: 11 of 13 fail with `tracedecay_fact_store_add output violated its retained result schema: invalid type: map, expected variant identifier` (memory_eval_test.rs:218) after spawning the freshly built HEAD CLI \u2014 the fact-store surface is untouched upstream code. Needed: determine whether these are genuine floor regressions (check upstream CI at the floor SHA) or macOS/local-environment sensitivity (git identity, timers, subprocess schema drift), and either fix upstream via the convergence workflow or pin the environmental precondition in the test harness. Until resolved, the convergence-map verification command for the daemon composition mount cannot go green locally; mount-specific assertions are compile-verified and checker-verified instead."
todos:
  - id: tdmem-floor-daemon-test-env-kmw-deliver
    content: "Deliver Bead tdmem-floor-daemon-test-env-kmw: Upstream-floor daemon and memory_suite tests fail in local env independent of product code; satisfy every recorded acceptance criterion, run the smallest behavioral dependency cone, then commit and push the green slice."
    status: in_progress
isProject: false
---

# tdmem-floor-daemon-test-env-kmw: Upstream-floor daemon and memory_suite tests fail in local env independent of product code

## Execution Notes

Beads issue: `tdmem-floor-daemon-test-env-kmw`. Current Beads status at generation: `in_progress`.

Upstream-floor daemon tests fail in the local macOS dev environment with zero product code involved. Reproduced 2026-08-31 on an idle machine, sandbox on and off, at branch HEAD 35fe0f6e0 whose diff vs the upstream floor 08fbe33a7c7f403191f is entirely cfg-gated behind memory-provider-host:

1. `daemon::tests::ownership::project_server_cache_hit_skips_open_and_singleflights_first_miss` deterministically times out in `engine.shutdown_all()` (PHASE_TIMEOUT 20s, panic at ownership.rs:401) — identically with `--no-default-features --features lite,token-counting,semantic-fastembed,graph-sealed-store,test-helpers`, i.e. compiling exactly the floor's own test body, so the hang is not attributable to the dormant memory-provider host mount.
2. Three more ownership tests fail on the idle machine (`fresh_committed_project_open_mounts_feedback_before_lsp`, `released_automation_tombstone_allows_one_eventual_replacement`, one additional under load variance), 24-25 of 28 pass.
3. `memory_suite` eval tests: 11 of 13 fail with `tracedecay_fact_store_add output violated its retained result schema: invalid type: map, expected variant identifier` (memory_eval_test.rs:218) after spawning the freshly built HEAD CLI — the fact-store surface is untouched upstream code.

Needed: determine whether these are genuine floor regressions (check upstream CI at the floor SHA) or macOS/local-environment sensitivity (git identity, timers, subprocess schema drift), and either fix upstream via the convergence workflow or pin the environmental precondition in the test harness. Until resolved, the convergence-map verification command for the daemon composition mount cannot go green locally; mount-specific assertions are compile-verified and checker-verified instead.

Design authority:

Follow the Beads acceptance contract and existing repository seams.

Acceptance authority:

- [ ] Deliver the described behavior with focused validation.

## Constraints

- Beads is the live source of truth. Re-read `br show tdmem-floor-daemon-test-env-kmw` and require `br ready` before a new claim.
- Unsatisfied blocking Beads dependencies at generation: none.
- Beads parent/hierarchy references: none. Parent readiness is enforced by `br`, not fabricated as a plan dependency.
- Build semantic producers/consumers and focused evidence before bookkeeping. Do not create hash-, receipt-, lock-, or presence-only gates.
- Root alone owns heavy Cargo execution, integration acceptance, commits, pushes, and Beads closure.
- Workers must stay inside their assigned write scope, preserve concurrent edits, and stop rather than widen scope.
- Do not contact upstream maintainers or mutate external work items.

## Operator Guidance

Intersect `plan-graph frontier` with live `br ready`; launch only Beads-ready or already-claimed nodes. Assign one coherent file/module owner, run focused checks, review the exact diff locally, then publish the green slice immediately. Close the Bead only after every acceptance item and real journey required by the issue are evidenced.
