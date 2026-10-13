# Beads execution orchestration

## Goal

Deliver the maximum dependency-valid Beads throughput while keeping Beads Rust authoritative, publishing each focused green slice immediately, and allowing only the root lane to run heavy Cargo, accept integration, close Beads, commit, and push.

## Canonical handoff bundle

Program graph:

- Selection: `--plans-root .codex/plans/beads-runnable --glob '*.plan.md'`
- Explicit dependencies: one `--depends` per line in `.codex/plans/beads-runnable/edges.txt`
- Graph ID: `tracedecay-beads-program-v1`
- Selection hash: `45df14cf2c`
- Snapshot: `.codex/plan-graphs/tracedecay-beads-program-v1/snapshot.json`
- State directory: `.codex/plan-graphs/tracedecay-beads-program-v1`

Isolated daemon/eval bug graph:

- Selection: `--plan .codex/plans/beads-isolated/tdmem-floor-daemon-test-env-kmw.plan.md`
- Explicit dependencies: none
- Graph ID: `tracedecay-beads-floor-bug-v1`
- Selection hash: `056d98970e`
- Snapshot: `.codex/plan-graphs/tracedecay-beads-floor-bug-v1/snapshot.json`
- State directory: `.codex/plan-graphs/tracedecay-beads-floor-bug-v1`

Intentionally excluded from runnable selection:

- Eleven epic wrapper plans in `.codex/plans/beads-epics/`; Beads owns hierarchy and epic completion.
- Six deferred OCEAN plans in `.codex/plans/beads-deferred/`; they cannot run until `br undefer` and prerequisites make them ready.

## Limits and launch shape

- Resolved limit: 50 threads, maximum depth 3.
- Initial launch: root plus three leaf sidecars. The large configured budget remains unused unless Beads exposes more genuinely ready, disjoint work.
- Root keeps the critical path and all heavy processes. Workers do not run Cargo, commit, push, close Beads, or change claims.

## Wave 1

1. Root — `tdmem-floor-daemon-test-env-kmw`
   - Own the 12-file shutdown, runtime-isolation, memory-eval, and matching convergence-policy slice already dirty.
   - Correct the retained envelope consumer in `memory_eval_test.rs`, run focused shutdown/harness/eval evidence, review, commit, and push.
   - Do not touch the four dirty Native seam files.
2. Read-only checker — `tdmem-0401`
   - Prove or refute each Beads acceptance item against landed code and exact existing tests.
   - Return the smallest missing semantic/test cone. No edits and no Cargo.
3. Write worker — `tdmem-1202`
   - Own only `scripts/product/classify-upstream-changes.py` and `tests/product_upstream_change_classification_test.py`.
   - Make touched seams fail closed, unrelated upstream changes remain classifiable, generated-only/mixed semantics remain distinct, and old/new tree attribution deterministic.
   - Use only lightweight Python focused tests; do not touch convergence-map or patch-policy files.
4. Write worker — `tdmem-1207`
   - Own only the external-lesson intake schema/catalog/checker/docs/test files named in its handoff.
   - Deliver source/commit/license provenance, generic capability mapping, adapter containment, accepted neutral regressions, and explicit rejection rationale.
   - Use only lightweight focused Python tests; do not touch classifier, convergence-map, or patch-policy files.

## Critical path and next waves

Critical path: `tdmem-0401 -> tdmem-0402 -> tdmem-0403 -> tdmem-0404 -> tdmem-1204`, with `tdmem-0402 -> tdmem-0405` and `tdmem-0403 + tdmem-0404 + tdmem-0405 -> tdmem-0406` as parallel branches.

- Wave 2: once root validates and closes `tdmem-0401`, immediately claim `tdmem-0402`; review and validate the four already-dirty Native seam files, then commit/push.
- Wave 3: after `tdmem-0402` closes, launch disjoint `tdmem-0403` parity-test and `tdmem-0405` status/explanation lanes. Root owns shared composition interfaces.
- Wave 4: after `tdmem-0403`, run `tdmem-0404`; when `tdmem-0404` closes, finish `tdmem-1204`. Recompute `br ready` after every closure and refill only dependency-valid lanes.
- `tdmem-1202` and `tdmem-1207` publish independently when green. Their downstream work remains blocked by the other Beads prerequisites and is not force-claimed.

## Merge points and conflict map

- Hotspot: `crates/tracedecay/tests/memory_suite/memory_eval_test.rs` — root only.
- Hotspot: dirty Native adapter and registry tests — frozen until `tdmem-0401` closes; then root owns `tdmem-0402` integration.
- Hotspot: convergence map and patch policy — root only while the daemon/harness slice is dirty.
- `tdmem-1202` classifier files and `tdmem-1207` intake files are disjoint and may be written concurrently.
- Integrate and push each completed lane separately after focused validation; do not batch unrelated lanes behind a final rollup.

## Replan triggers

Redraw the graph if a worker needs a file outside its scope, `br ready` disagrees with the persisted frontier, a focused check exposes a cross-lane contract change, or two lanes touch the same shared authority. After every Beads status transition, regenerate the plans and persisted graph from `br`.
