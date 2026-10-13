# Recovery Batch Completion Receipt

Date: 2026-09-06. Branch `feat/pluggable-memory-providers-v2`, worktree
`.worktrees/pluggable-memory-providers-v2`. Base HEAD `fdc392a0a` (checkpoint P
`25778c744` plus one retention commit). Upstream floor F `5749e4fc`, candidate U
`9bc7dedf9`. Toolchain: rustc 1.97.1 (8bab26f4f 2026-07-14), cargo-nextest
0.9.116. All Cargo runs used `--locked` and `--no-tests=fail`, with
`TRACEDECAY_DATA_DIR=<worktree>/target/test-profile/.tracedecay`.

Execution: Claude dynamic workflows (`wf_b1ce3ab4-bc6` recon and A/B/C,
`wf_a47808c9-7e7` C simplification, `wf_c1070af7-d24` B finish) on Scout
`claude-sol` lanes after the `claude-astra` lane went down (`unknown provider
for model gpt-6-astra`); final B fixes and verification by the lead.

## Workstream A — memory dependency policy

Diff scope: `product/architecture/memory-dependency-policy.json`,
`scripts/product/check-memory-dependency-direction.py`,
`tests/product_memory_dependency_direction_test.py`.

Change: the fixture now models the real manifests (fabric `tracing`, registry
`serde_json/raw_value`, tokio feature sets, conformance dev-dependency); the
policy admits exactly those two edges with reasons; violation messages carry
stable prefixes (`[source-import-not-allowed]`, `[executor-site-not-reviewed]`,
`[dependency-feature-not-allowed]`); new negative tests refuse a
`tokio::task::spawn_blocking` import and an unreviewed `tokio::spawn` site.
No allowlist was broadened beyond the two manifest-backed edges; no adversarial
case was removed.

Evidence:

```sh
python3 tests/product_memory_dependency_direction_test.py            # 27 tests OK
python3 scripts/product/check-memory-dependency-direction.py --repo . \
  --policy product/architecture/memory-dependency-policy.json          # "memory dependency direction verified"
```

Counts: 27/27 focused tests pass; real Cargo graph check passes. The pre-batch
policy fails the real graph on exactly the two edges above (verified by the
lead). Gate: PASS.

## Workstream B — vector-retention safety and honest fixture bootstrap

Diff scope: `crates/tracedecay/src/daemon/store_maintenance/mod.rs`,
`crates/tracedecay/src/daemon/store_maintenance/vector_retention_tests.rs`,
`crates/tracedecay/src/daemon/maintenance/generation.rs`,
`crates/tracedecay/src/daemon/production_harness/generation_retention_test.rs`,
`crates/tracedecay-usecases/src/semantic_runtime/production.rs`,
`crates/tracedecay-store-runtime/src/session_registry/code_graph.rs`.

Changes:

- Deletion authority is the `Online` vector inventory only. `Offline` returns
  the typed `VectorInventoryUnproven` (maintenance retries); `SemanticUnseated`
  returns a quiet typed deferral (no sweep, no retry storm); `CensusScanning`
  waits; `Refused` fails closed. The offline serving-generation protection set
  was deleted because it carried no vector fence.
- The vector writer fence is keyed by store root and survives a runtime
  remount (`vector_writer_for_store`); the owned guard moves into the blocking
  deletion closure instead of being dropped before it.
- A tick whose semantic-vector pass did not converge skips the code-generation
  pass entirely (`generation.rs`); a collecting pass re-plans once and reports
  `Complete` when the sweep was the final unit, otherwise `MoreWork`.
- `RetainedCodeGraphRuntimeV1` binds the sealed replay source when the runtime
  is retained, not only at the first publication, so post-restart retention can
  hydrate sealed generations (previously
  `sealed code generation replay source is not mounted for this projection`).
- The restart journey bootstraps a durable query-only retrieval profile through
  the existing production API `bootstrap_query_retrieval_profile`, then proves
  Online -> Offline -> SemanticUnseated -> Online with a live lease before the
  restart, and after the restart proves the source survives until the Online
  inventory no longer leases it and the journey reports `Complete`.

Selection (19 tests: `daemon::store_maintenance::vector_retention_tests::*`,
`daemon::production_harness::generation_retention_test::*`,
`vector_writer_fence_survives_runtime_remount`, and the code-index-runtime
retention tests):

```sh
cargo nextest run -p tracedecay -p tracedecay-code-index-runtime -p tracedecay-usecases --lib \
  --features tracedecay/test-helpers,tracedecay/test-transport,tracedecay/memory-provider-host \
  --locked --no-fail-fast --no-tests=fail \
  -E 'test(/^daemon::store_maintenance::vector_retention_tests::/) | test(/^daemon::production_harness::generation_retention_test::/) | test(=vector_writer_fence_survives_runtime_remount) | (package(tracedecay-code-index-runtime) & test(/retention|vector_source/))'
```

Counts: cc-784 (serial) 19/19; cc-795 (12 threads) 19/19, 2 leaky; cc-802
(6 threads) 19/19. Before the last two fixes the restart journey failed under
parallel load (cc-736, cc-786, cc-787: 18/19) and passed serially, so both
modes were required evidence. Dependent regression group (cc-803 on the final tree, cc-726 earlier; 77 tests
across `tracedecay`, `tracedecay-code-index-retention`, `tracedecay-domain`,
including the restart journey and `configuration_idempotency_journey_test`):
77/77. Storage/admission group (22 tests): 22/22. Gate: PASS.

Judged refutation points (adversarial reviewer, `wf_c1070af7-d24`):

- "Code retention runs while the vector pass is still continuing": fixed by
  the tick ordering change above; the journey assertion is now exercised
  against the real property (Online inventory without the lease) because a
  deleting tick may legitimately carry its own post-sweep release backlog.
- "Bootstrap should use the shipped `query-fallback-report-v1.json` through
  `validate_against`": not applicable. That report is aggregate-only and the
  product test `aggregate_only_report_cannot_become_activation_evidence`
  asserts it cannot become activation evidence. The journey uses the test-only
  accepted profile through the production bootstrap API instead; no runtime
  readiness is fabricated.
- "Held replay-pool test no longer drives `run_code_generation_retention`":
  accepted limitation. The unseated fixture can no longer sweep by design, so
  the test asserts the replay owner's probe, backoff, and drain directly; the
  integration path is covered by the production-harness journey.

Unresolved: default-off retention is a typed deferral until the durable
semantic-vector staging ledger is wired as proven-empty authority through an
authorized shard handle. The live query-fallback evaluator still returns
`Fail` (cc-493: `train-002` and `validation-002` yield zero `qualified_name`
candidates); untouched.

## Workstream C — acceptance pipeline

Diff scope: `.github/workflows/product-upstream.yml`,
`product/upstream/sync-policy.json`, `product/upstream/convergence-map.json`,
`product/upstream/README.md`.

Change: the workflow runs the sync-policy lanes as 51 diagnostic steps (73
steps total) that each require checkout, toolchain, and nextest to have
succeeded and never cancel siblings; the convergence result stays fail-closed.
Lane commands equal HEAD plus three edits: the workspace nextest lane carries
`--no-tests=fail`; two vacuous deadline commands now run the
`tracedecay-daemon-service` deadline tests; the scope-crash-security lane adds
the convergence-invariants test. The README documents diagnosing an already
integrated candidate (F/U/P git commands) and separates diagnostic execution
from promotion eligibility. No floor promotion, no blanket allowlists, no
history rewrite, no ownership check removed.

Evidence: `python3 -m unittest discover -s tests -p 'product_upstream_*_test.py'`
91 tests OK (lead rerun); upstream checker set 6/7 clean, ownership registry
gate red by design; `scripts/check-product-upstream-floor.py` verified.
Ownership diagnostics at HEAD: 830 total, of which 808 are F..U-only upstream
imports that clear only when the floor advances (forbidden this batch); the
remainder are about 20 product residue crate paths without exact convergence
rows plus `store_maintenance/mod.rs`. Not reconciled: fabricating ownership
rows was judged worse than a red gate. Map-obligation coverage is enforced only
at `advance-floor` and remains outstanding. Gate: diagnostics restored; promotion
eligibility unchanged (red by design).

## Milestone journey and NCM gate

`cargo nextest run -p tracedecay-cli --test product_memory_provider_claude_host_journey --features memory-provider-host,test-transport --locked --no-fail-fast --no-tests=fail`:
1/1 pass (cc-725, cc-489). Installed binary: `~/.local/bin/tracedecay` 0.0.74.
`cargo check --workspace --locked` and
`cargo check -p tracedecay --features memory-provider-host --locked` finish
clean (cc-733). NCM stays Observer-only; no active NCM path was enabled.

## Beads

A: tdmem-0903 (observer-provider isolation, mechanical policy). B: tdmem-0000
retention safety under the program epic. C: tdmem-1200 (convergence). Journey:
tdmem-0406 and tdmem-1001.
