# NCM recall reproduction

Date: 2026-09-13
Node: rn-ncm-reproduce
Branch: feat/pluggable-memory-providers-v2
Accepted start: f6d9bdf7073de9a25994b71e638fc1ff6013e4b0

## Result

The historical workload is recovered, but the historical causal boundary remains unresolved. The failing artifact is a Codex/NCM active journey with canonical replay sequences 2 through 5 and observation_count=4; its final provider candidate set contains only sequences 4 and 5. The paired immediate rerun and the final four-journey artifact contain all four sequences. Both historical results report zero exclusion counters and no quota/budget exceedance. The artifacts do not contain journal Applied receipts, an acknowledged watermark, a worker common_recall payload, or the adapter reconstruction payload, so observation_count=4 cannot be treated as ACK or delivery proof.

A deterministic runtime test now reaches the live core recall loop and keeps the expected failure focused on an oversized ranked candidate. A deterministic adapter-surface diagnostic confirms the existing prefix-clipping and partial-coverage contract with opaque worker rows; it does not claim that adapter clipping caused the historical miss. The transient proof hypothesis remains report-only because the public CLI has no injection seam. No production behavior was changed.

## Recovered workload

| Field | Evidence |
| --- | --- |
| Historical failing artifact | target/task-scratch/pm-included-binary-journeys-pvoerfgp |
| Historical failing host/provider | Codex / NCM active |
| Paired immediate rerun | target/task-scratch/pm-production-isolation-k0egqxnt |
| Final paired artifact | target/task-scratch/pm-final-four-journeys-0g71d0vw |
| Canonical replay | source sequences 2..5, observation_count=4 |
| Failed final candidates | source sequences 4, 5 |
| Successful final candidates | source sequences 2, 3, 4, 5 |
| Query | “what did the quicksilver retry budget change record, down to the obsidian-ledger-tail note?” |
| Adapter top_k | 16 |
| Advisory quota | 8192 |
| RNG seed | not recorded |
| Historical exclusions | stable refs 0, candidate IDs 0, source refs 0, trace refs 0 |
| Historical budget signal | no budget exceeded signal |

The first directly observed failing assertion is the final two-item candidate set where four items were required; the admission and trace counters also report two. Missing ACK/watermark, worker `common_recall`, and adapter reconstruction receipts leave the actual first-loss stage unresolved.

## Controlled hypotheses

1. Transient instance proof. ProjectObservationJourneyV1 stores a transient proof result in Arc<OnceLock<Option<String>>>. At observation_journey.rs:4475-4490, an Err or Ok(None) becomes None and is set once; at :4507-4510, delivery is attempted only when that value is Some. The warning explicitly says the proof is unavailable until daemon recreation. This predicts an unacknowledged pending row stays blocked for the lifetime of that daemon and recovers only after recreation. The public CLI journey has no injection seam for ObservationInstanceProofV1, so this node records the prediction only in this report and instance-proof-once-lock.jsonl; the fix owner needs an internal fail-once proof test.

2. Oversized higher-ranked candidate. The live pure core loop at crates/tracedecay-memory-ncm-core/src/recall/mod.rs:225-240 hydrates ranked candidates and breaks when the next candidate exceeds records.max_recall_bytes(). A later smaller fitting candidate is never scanned. The new runtime test admits one oversized sequence-2 record followed by fitting sequences 3, 4, and 5, sets the byte budget to the exact sum of those fitting records, and expects all three later records. Baseline therefore fails at the candidate assertion solely because of the break. The adapter diagnostic intentionally expects the current contract: prefix clipping of the high row and a partial result. It is a negative control for the core hypothesis.

The historical frozen-unapplied proposal under .codex/patches/pm-ncm-recall-trace-budgets was not reapplied and is not claimed as causal. The current adapter source already contains its trace-exclusion and UTF-8 clipping behavior.

## First-loss evidence

The synthetic logs under target/test-profile/readiness/ncm/reproduce carry redacted correlation IDs and record these boundaries:

- codex-active-2-of-4.jsonl: canonical replay range is known; final provider candidates are [4,5]; the intermediate boundary is unknown because ACK/watermark, worker payload, and reconstruction payload are absent.
- codex-active-4-of-4.jsonl: the paired run reaches [2,3,4,5], establishing nondeterminism or an unobserved state difference rather than a deterministic final quota refusal.
- instance-proof-once-lock.jsonl: controlled fail-once proof state predicts worker_readiness as the first loss and recovery after daemon recreation.
- byte-budget-backfill.jsonl: controlled core state records the exact baseline action (break) and that the lower-ranked row is not scanned.
- readiness-gate-negative-control.jsonl: a route rotation between handshake and invoke yields a typed readiness failure before provider contact and cannot explain a partial candidate set.

The historical artifacts alone do not establish either hypothesis. The byte-budget test establishes a current deterministic loss seam, but it does not prove that seam caused the historical 2/4 result. The instance-proof test remains blocked by the private construction seam and the absence of a Cargo-run public injection path.

## New files

- crates/tracedecay-memory-provider-ncm/tests/ncm_recall_reproduction.rs
- crates/tracedecay-memory-ncm-runtime/tests/recall_reproduction.rs
- .codex/plans/native-original/execution-results/ncm-recall-reproduction.md
- target/test-profile/readiness/ncm/reproduce/codex-active-2-of-4.jsonl
- target/test-profile/readiness/ncm/reproduce/codex-active-4-of-4.jsonl
- target/test-profile/readiness/ncm/reproduce/instance-proof-once-lock.jsonl
- target/test-profile/readiness/ncm/reproduce/byte-budget-backfill.jsonl
- target/test-profile/readiness/ncm/reproduce/readiness-gate-negative-control.jsonl
- target/test-profile/readiness/ncm/reproduce/README.json
- target/test-profile/readiness/ncm/reproduce/real-worker/cold/{cargo-hauler.log,evidence.json,capture/daemon.stderr.log}
- target/test-profile/readiness/ncm/reproduce/real-worker/warm/{cargo-hauler.log,evidence.json,capture/daemon.stderr.log}
- target/test-profile/readiness/ncm/reproduce/real-worker/warm-clean/{cargo-hauler.log,evidence.json}
- target/test-profile/readiness/ncm/reproduce/real-worker/shared-worker/{cargo-hauler.log}
- target/test-profile/readiness/ncm/reproduce/real-worker/shared-worker-real/{cargo-hauler.log,evidence.json}
- target/test-profile/readiness/ncm/reproduce/real-worker/observer/{list-cargo-hauler.log,list-evidence.json}

## Expected baseline

Before the designated Cargo owner began this trace run, no Cargo command had been run, per worker ownership rules. Both remaining Rust test files pass rustfmt --edition 2024 --check. The runtime test is an intentionally failing red-to-green reproduction; the provider test is a passing adapter negative control:

- the runtime test expects fitting sequences 3, 4, and 5 after an oversized sequence-2 candidate;
- the provider diagnostic passes by confirming the high worker row is prefix-clipped and marked partial;

The designated Cargo owner then verified the targets through cargo-hauler:

- `cc-1435`: runtime reproduction compiled successfully;
- `cc-1436`: provider diagnostic compiled successfully;
- provider post-fix run: exactly one test passed;
- `cc-1438`: exactly one runtime test failed at the intended oracle, returning `EngineReply { outcome: Empty, ... }` where the lower fitting candidates require `Success`.

The runtime result is a behavioral red baseline. It is not a compiler failure, zero-test filter, ignored case, or adapter-contract mismatch. Captures are under `target/test-profile/readiness/ncm/reproduce/build-owner/`.
- the historical 2/4 candidate set and same-daemon proof prediction remain report/log evidence because a permanently red hardcoded fixture cannot be repaired by production behavior.

## Smallest repair seams

- For the deterministic byte-budget loss: change only the candidate loop in crates/tracedecay-memory-ncm-core/src/recall/mod.rs so an over-budget candidate marks truncation and continues scanning for fitting candidates. Preserve ordering, max_candidates, and byte accounting.
- For transient readiness: change only the proof/readiness state in crates/tracedecay/src/daemon/retained_owner/observation_journey.rs, replacing permanent OnceLock<Option<String>> failure caching with a bounded retry or an explicit state that does not suppress later delivery. Add the fail-once internal test at that seam.
- Instrumentation should expose ACK/watermark, worker common_recall, and adapter reconstruction counts before attributing the historical 2/4 result to either seam.

## Pinned real-worker executions

The following run was submitted through the designated cargo-hauler session `native-original-execution` from the active worktree. It used the existing test-built worker and the verified model root; no production or source test files were changed.

Shared pins for ticket `cc-1440`:

| Field | Value |
| --- | --- |
| Source commit | `b53ab5cf8600e9eb7c9820e7150fe5499d7e6b7c` |
| Source tree | `fcf65b4727bf4deed57bb619820ac200591271aa` |
| Worker | `target/debug/tracedecay-ncm-worker` |
| Worker SHA-256 | `e23b2c00f8285bbb7075fb88268b6b48aa236d442eff33681baf5b27ca111330` |
| Model root | `target/ncm-backend-model-root` |
| Model snapshot | `2c4055b12046f11709e9df2c122e59ffbdc2f900` |
| Encoder manifest SHA-256 | `8177baea44e400c431cd4c179c917ef917487ce40401f026e7abd6d2faaa9346` |
| ONNX model SHA-256 | `185ae63f47e17a7e8d30d0e6a3cde6a6e4b79bc5b81666ecffc279a6856ca113` |
| Tokenizer SHA-256 | `b60b6b43406a48bf3638526314f3d232d97058bc93472ff2de930d43686fa441` |
| Host test binary SHA-256 (cc-1442) | `4c0eaec68d908eafe04825770cae4408aaf6ac47d645d7e8e49585aef58dcc04` |

### Cold Codex active history

Ticket `cc-1440` executed one ignored-marked test selected with `--ignored`; the test process finished in 8.31 seconds and the cargo-hauler ticket elapsed 12 minutes 47 seconds before returning exit 101 (`0 passed, 1 failed, 24 filtered, 0 ignored`). The exact command was:

```text
TRACEDECAY_DATA_DIR=/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/target/test-profile/.tracedecay TRACEDECAY_NCM_WORKER=/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/target/debug/tracedecay-ncm-worker TRACEDECAY_NCM_REAL_MODEL_ROOT=/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/target/ncm-backend-model-root TMPDIR=/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/target/test-profile/readiness/ncm/reproduce/real-worker/cold/tmp TRACEDECAY_DEMO_OUTPUT_DIR=/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/target/test-profile/readiness/ncm/reproduce/real-worker/cold/demo cargo test --locked -p tracedecay-cli --test product_memory_provider_claude_host_journey --features memory-provider-host,test-transport real_ncm_active_recalls_shipped_codex_session_history_with_native_disabled -- --exact --ignored --nocapture --test-threads=1
```

The canonical replay was source sequences 2 through 5. The bounded host diagnostics show `history_selection` scanned 5 rows with `unknown_revision=4`, `withheld=1`, `partial_coverage=true`; ingress appended one row for each of seq5, seq4, seq3 and seq2, while seq1 was a non-message and was not granted. The admission boundary reported `received=2`, `admitted=2`, `denied=0`, `degraded=true`; the trace boundary reported `received=2`, `items=2`, `injected=2`, and `compiled_into_pack=2`. The final provider candidates were only source sequences 4 and 5, although the pack quota was 8192 tokens (approximately 2143 used) and all reported exclusion counters were zero.

The cc-1440 executable path was the same `target/debug/deps/product_memory_provider_claude_host_journey-1dd835576e0ff649`; its binary was rebuilt by the subsequent cc-1441/cc-1442 compile before a cold-run digest could be retained. The exact worker and model digests above are retained for cc-1440.

This is a nonzero real-worker reproduction of the current 2/4 result. The first directly observed failing assertion is the final two-item candidate set; the admission and trace counters also report two. Missing ACK/watermark, worker `common_recall`, and adapter reconstruction receipts leave the actual first-loss stage unresolved. The fixture asserts this first same-session recall before its daemon/worker restart and destination-session phase; because the assertion fails, the restart portion is not reached.

The complete hauler output and daemon capture are retained under `target/test-profile/readiness/ncm/reproduce/real-worker/cold/`. The missing stage payloads are an instrumentation/test-owner gap, not evidence of historical causality.

The separate budget scout for `cc-1440` charges the exact seq2..5 source history at 1000 core bytes against a 1,048,576-byte core limit and 517 adapter bytes against the 8192-token advisory quota, with `top_k=16` and `max_candidates=5`. The 2/4 result therefore is not the deterministic oversized-candidate byte-budget bug described by the synthetic core reproducer; it remains a distinct loss between exact history delivery and worker/adapter recall.

### Warm/full journey and embedded restart

Ticket `cc-1442` reran the same exact Codex active-history filter with the demo recorder disabled (the recorder accepts only `target/task-scratch`, which is outside this node's artifact ownership). It executed one ignored test and passed (`1 passed, 0 failed, 24 filtered, exit 0`) in 8.49 seconds with source commit `7258fb62f7c11a2cfe00b57d902100b5f24e5ed2`, source tree `6cdbdb48980a24a6f86ab0d8d67ee03ec51ecadf`, the same worker SHA, and the same model/tokenizer pins above. The test's four-message candidate assertions passed both before and after its explicit daemon stop/start and destination-session recall; this is the clean warm/full-journey result and the worker-restart result available from the existing Codex fixture. The pass does not emit the private ACK/watermark, worker `common_recall`, or adapter reconstruction payloads.

Ticket `cc-1441` is retained as the preceding warm attempt. Its retained log shows the test reached the optional `record_demo_output` helper, then exited 101 because the configured path was under `target/test-profile` while the source fixture requires `target/task-scratch`. The log does not expose additional candidate or stage receipts; it is not counted as a test pass, and `cc-1442` is the corrected rerun.

### Existing real-worker load/concurrency case

After test discovery ticket `cc-1444` showed that integration-test names are fully qualified with the `enabled::` prefix. Corrected ticket `cc-1445` ran the source-defined `enabled::real_shared_worker_keeps_project_readiness_generations_and_namespaces_independent` test from `tests/rust_backend.rs` with `--features rust-backend`, the pinned worker/model, and an isolated `TMPDIR`. It executed one ignored test and passed (`1 passed, 0 failed, 23 filtered, exit 0`) in 1.11 seconds. The test concurrently mounts two project surfaces over one real worker owner, then observes and recalls project-specific rows while checking independent readiness generations and namespaces. This is the closest existing load/concurrency filter; no current Codex host fixture exposes a separate concurrent active-history load test.

The preceding `cc-1443` invocation omitted the required `enabled::` qualification and executed zero tests; it is explicitly invalid coverage, not a pass. Both logs and the discovery output are retained below `target/test-profile/readiness/ncm/reproduce/real-worker/`.

### Explicit observer restart discovery limitation

The source-defined `real_ncm_observer_replays_independently_after_native_restart` test was discovered by source inspection, but ticket `cc-1446` hit the current shared-tree compile blocker before it could compile the `tracedecay` lib-test target. It returned exit 101 before running any test with 18 cross-tree API errors (missing `host_bundle_v2`, `query_authority_provider`, vector/code retention types and functions, `ConfigurationCurrentStateV1`, changed function arities/closure shape, and non-exhaustive `FactProjectionV1::Superseded` matches). No source change was made here and no observer result is claimed. The host fixture's successful cc-1442 restart path remains the valid restart evidence.

### Node disposition

The required cold, warm/full-journey, embedded restart, and closest existing load/concurrency modes have nonzero real-worker execution evidence. The cold run reproduces 2/4 and the warm run passes 4/4, while the budget charge rules out the synthetic byte-budget seam. `trace-first-loss` remains pending: current bounded host diagnostics stop at history selection/ingress, admission, and final trace/candidate counts, and the source does not publish the durable ACK/watermark, worker `common_recall`, or adapter reconstruction payload needed to distinguish those internal stages. The next owner is the instrumentation/test owner for those three stage receipts; no historical causal claim or release of `rn-ncm-recall-fix` is justified yet.
