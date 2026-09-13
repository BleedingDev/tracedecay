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

The failed panic identifies the final assertion as the first directly observed loss: the product saw two candidates where four were required. It does not identify whether the loss occurred at readiness, journal acknowledgement, worker recall, or adapter reconstruction.

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

## Expected baseline

No Cargo command was run, per worker ownership rules. Both remaining Rust test files pass rustfmt --edition 2024 --check. The runtime test is an intentionally failing red-to-green reproduction; the provider test is a passing adapter negative control:

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
