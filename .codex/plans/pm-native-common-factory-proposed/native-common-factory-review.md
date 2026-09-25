# Native factory cancellation and cleanup review

Status: proposed test module and module hook remain unapplied. No Cargo or model run.

`native-common-factory-cancellation-revision.patch` is the **exact complete delta** from the last reviewed 1,630-line draft to the current 2,762-line draft: **+1,314 / −182 lines**, 1,888 patch lines. It is not a replacement diff. This check regenerated the unified diff and required byte-for-byte equality with the existing delta. The revision is substantial because the approved two real-child regressions and final cleanup collector are included.

Baseline: `native_common_factory_tests.before-cancellation.rs`, SHA-256 `b385794af73f2003df0aeaf6227bc6a8d29a8305d9da7174604d6cceb20fd4ed`.
Current: `native_common_factory_tests.rs`, SHA-256 `2b3d7db2286500f49f4c91221501113180cad13d5dea10030f541cb4d9a2cb0c`.

Read `native-common-factory-transport-core-review.patch` first (407 lines). It contains only exact before/after hunks for the control/frame reader, correlated RPC envelopes, and `OwnedChild` reply/drain/teardown. It is a review excerpt; the complete delta remains the authoritative change set. Both patches use the original file line coordinates.

| Review area | Current source lines | What changed |
| --- | --- | --- |
| Control and frame reader | 104–257 | Separate finite cleanup bound; retained header/body offsets; one reader resumes the same frame. |
| RPC envelope and owned transport | 466–730 | Monotonic request IDs and correlated responses; partial request writes poison transport; caller cancellation forwards its ID and drains the actual response for up to 5 s; a bad/failed drain permanently closes the channel and kills/reaps the owned child. |
| Proxy and supervisor transport plumbing | 948–1172 | IDs move from proxy state to the owned dispatch path; actual DTO replies pass through unchanged; explicit probe plumbing and cleanup-only poisoned-lock recovery. |
| Child dispatch/reply plumbing | 1976–2223 | All RPCs enforce the same sequence; real Native invocation remains single; reply/error/shutdown envelopes carry the actual request ID. |
| Cleanup tracking | 260–350, 732–945, 1194–1370, 1753–1792, 2369–2467 | Record each spawned owner immediately and each actual reap; retain every owner through final collection; write cleanup evidence and require zero unconfirmed children; preserve storage on failure. |
| Explicit transport probes | 352–446, 1725–1751, 1823–1965, 2470–2724 | Gate only an explicitly armed real invocation; authority probe delegates the actual authority outcome; split-prefix probe pauses the actual serialized response after two header bytes. |
| Full common suite gate | 2726–2762 | Same full suite and compatibility assertions; cleanup must settle before pass. |

The focused regressions use the existing common compatibility runner and require actual provider contact, the child's real terminal, one actual invocation, a fresh successful next RPC, the original UTC deadline, and confirmed child reap. The split-prefix case requires two retained header bytes and resumption of the same frame. It retains the provider's actual completed Success response.

Previously reviewed code that does not need another semantic review:

- The original v1 schema, real legacy writer, and physical corruption writer are byte-identical (143 lines).
- The mode/quarantine probe implementation is byte-identical (173 lines); only its preceding explanatory comment was added.
- The source-authority, namespace/restart, legacy-install and corruption environment actions retain their existing behavior. The namespace path now unwraps the optional directory handle used to preserve failed fixture storage.
- The active `retained_owner.rs` hook remains absent; its proposed module addition is unchanged by this revision.

Review/approval context:

- Root's 2026-09-10 10:06 UTC task relayed the Native auditor's partial-frame/cancellation finding and approval of the existing real-provider, legacy, corruption and restart fixture behavior. It required actual correlated terminal collection, no second reader or retry, a live cancellation regression and final cleanup collection.
- Root's 10:13 UTC scope approval accepted the retained reader, correlated IDs, unchanged operation control, 5 s response drain used only for cleanup, poisoning partial request writes, a real authority-entry witness that delegates actual authority, the split-prefix regression and final collector. This approved artifact preparation only; application and Cargo validation remain with root.

Validation: exact complete-delta equality and unchanged-block checks pass. Earlier rustfmt and full proposed patch `git apply --check --whitespace=error` passed. This review packaging does not change the proposed Rust source or full application patch.

Final evidence-retention follow-up: `native-common-factory-evidence-retention.patch` is the narrow delta against `native_common_factory_tests.before-evidence-retention.rs` (the 2,753-line draft reviewed above). Every fixture now keeps its nested directory; only the outer environment removes it after explicit behavioral success and confirmed cleanup. An explicit false-by-default pass flag is set after the last assertion in each of the three tests. This fixes successful child cleanup followed by failed report validation losing its DB/log/probe evidence. Root approved this exact follow-up; it remains unapplied. The transport core is unchanged. Full proposed patch applicability passed again after regeneration.
