# PR707 Native restoration plan reassessment

Date: 2026-09-13

This report refreshes the active Native restoration plan after PR707 was merged into the candidate. It is a plan and evidence reassessment; it makes no product source, test, Cargo, commit or push changes.

## Baseline decision

Use unmodified upstream PR707 tip `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` as the active original reference. The current execution candidate is `1fe250fed7ca615f1dfdcd580befeb276328e910`. Preserve the existing `b3b43410e47115056f2066449aafa1822bbb6049` reference checkout and the b3-to-571 audit as historical evidence. The active reference checkout name is `native-original-reference-57006f60`; no existing b3 checkout is repointed.

PR707's Native, session-memory, LCM, runtime, storage, transport and host improvements remain in scope. The restoration plan must fit those current routes and must not roll them back. The active worktree is `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2` on branch `feat/pluggable-memory-providers-v2`.

## Graph refresh

The active selection is graph `native-original-execution-pr707-20260913`, with plan-set hash `20bb2dd27a`, selection hash `08ad7a0ca9`, 33 plans and 71 dependency edges. Strict validation reports no errors or warnings. The current snapshot/frontier is under `.codex/plan-graphs/native-original-execution-pr707-20260913/`; the ready nodes are rn-integration-readiness, rn-ncm-tests, rn-reference and rn-source-docs. rn-restore-anchor is `completed` from the current equality proof; all other selected nodes remain pending or blocked by their explicit predecessors.

## Evidence used

- `git diff --exit-code 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 1fe250fed7ca615f1dfdcd580befeb276328e910 -- crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs` returned exit `0`. Both blobs are `d976e2a23c19441a60412420a1db5e68f7ccc5ca`.
- The same candidate check for `crates/tracedecay-privacy/src/detector_kernel.rs` returned exit `0`. Both blobs are `9ce4488a34c5bd121d43c35c33f1daf925ab38ba`.
- The active 570 tree has no `crates/tracedecay-memory-provider-api`, product Native contract or `scripts/product/native-original` entries. Those are candidate product additions, so the current proposed contract cannot be treated as a 570 interface.
- PR707 changes the session-memory, LCM, session-runtime, runtime-core, store and related host surfaces. The active source inventory must refresh those changed paths; the old b3-to-571 inventory is not rewritten in this task.

Current route anchors to use for the refresh are:

- Fact ownership and reads: `crates/tracedecay-session-memory/src/fact_store/mod.rs`, `memory/mod.rs`, `memory/hygiene.rs` and `memory/project_memory/add.rs`.
- Retained service boundaries: `crates/tracedecay-session-runtime/src/retained/lcm.rs`, `retained/session.rs`, `retained/profile.rs` and `retained/session_refresh.rs`.
- Admitted session retrieval: `crates/tracedecay-session-runtime/src/session_retrieval.rs` and `session_retrieval/admitted.rs`.
- Historical ingestion: `crates/tracedecay-session-runtime/src/session_temporal_refresh_scheduler/history.rs`, including `ProjectSessionHistoricalIngestor::new`, `with_original_provenance_resolver` and `run_pass`.
- Session scope context: `crates/tracedecay-session-runtime/src/session_sync.rs`, including `DaemonSessionSyncConfig.scope: ResolvedScope` and `SessionSyncProjectContext`.
- LCM query routes: `crates/tracedecay-lcm/src/query/{expand.rs,grep.rs,payload_health.rs,session.rs,status.rs}` and corresponding request types in `crates/tracedecay-lcm/src/types.rs`.

## Keep, change and revalidate

| Decision | Active treatment | Node |
| --- | --- | --- |
| Keep PR707 upstream Native/session/LCM/runtime/storage behavior | Preserve current 570 source and discover its current callers before any adapter edit | rn-source-docs, rn-contract |
| Keep the existing b3 audit | Leave b3/571 reports and checkout immutable and label them historical | all active metadata |
| Keep exact anchor equality | Root's current proof is sufficient for the active source gate; record it in `restore-anchor-57006f60.md` and mark the node complete | rn-restore-anchor |
| Keep detector equality as a fact | The detector blob matches 570, but the current typed history seam still needs caller review | rn-privacy-audit, rn-privacy-restore |
| Change active worktree/reference paths | Use the original `pluggable-memory-providers-v2` worktree and `native-original-reference-57006f60`; do not repoint `native-original-reference-b3` | ORCHESTRATION, runner/build plans |
| Change runner identity checks | `scripts/product/native-original/runner.py` still pins b3/571 and its checkout verifier expects b3 | rn-reference |
| Change contract route assumptions | `scripts/product/native-original/case-contract.json` is `proposed`, pins b3/571 and names product provider ports absent from 570 | rn-reference, rn-contract |
| Change source inventory/map claims | `product/architecture/native-original-source-inventory.md` and the accepted map result describe b3→571 paths; refresh current symbols without rewriting the historical 85-path, 278-hunk audit | rn-source-docs, rn-map-checker |
| Revalidate fact/session/LCM assertions | Existing route reports and line references were written for the old head; assert current symbols, effects, receipts, cursors and failures at the 570/current boundaries | rn-contract, case authors, verification nodes |
| Revalidate history-owner scope | Resolver, transcript home and CPU are present, but the historical ingestor does not directly consume `ResolvedScope`; prove propagation or assign the smallest fix | rn-history-owner-followup |
| Keep NCM separate | rc13 pin/encoder compile, repeated nine-library feature-mode checks and model/non-model evidence passed under the separate owner; aggregate product readiness remains separate | rn-ncm-tests, rn-integration-readiness |

## Stale claims and exact owners

| Stale claim or assertion | Current evidence/path | Required action |
| --- | --- | --- |
| Source inventory is an accepted b3→571 audit | `product/architecture/native-original-source-inventory.md` | Reopen rn-source-docs; record 570→candidate hunk classes and current paths. Do not bulk rewrite historical evidence. |
| Map checker completion is current | `scripts/product/check-native-memory-surface-map.py` and its test still carry old marker assumptions | Reopen rn-map-checker after source docs; preserve missing-operation, authority, transport and source assertions. |
| Runner is pinned to the active reference | `scripts/product/native-original/runner.py` contains b3/571 revision constants and b3 checkout verification | rn-reference must update identity and reject modified 570 trees; draft runner remains unaccepted. |
| Case contract describes 570 routes | `scripts/product/native-original/case-contract.json` is proposed and names `DirectRetainedMemoryPortV1`, `tracedecay_fact_store_*` and related candidate APIs absent from 570 | rn-reference/rn-contract must rediscover routes and assertions before fixture authors start. |
| Old operation route report is current | `execution-results/native-operation-routes.md` names b3 and 571 | Keep it historical; publish a current route map only after 570 symbol/caller review. |
| Anchor needs another source restoration | 570 and candidate anchor blobs match exactly | rn-restore-anchor records current proof and is complete; no code edit. |
| Privacy detector drift is still present | 570 and candidate detector blobs match exactly | rn-privacy-audit records equality, then reviews the typed Claude history seam; no detector edit. |
| History owner already has parity | `history.rs` carries `original_provenance_resolver`, `transcript_source_home` and `ProcessBackgroundCpuV1`; `ResolvedScope` is held in session-sync context and is not directly consumed by the ingestor | rn-history-owner-followup traces the concrete ingest authority/request and returns consumed/not-consumed plus a bounded fix owner. |
| Product readiness is clean | cc1421 had three Codex signature errors; cc1424 accepted the hook tuple-error fix, cc1425 addressed the helper argument, cc1427 exposed a missing `futures-util` daemon-service dependency, and cc1429 reports that dependency passing while three stale `RuntimeTraceDecayConfig` references remain assigned to the build owner. cc1430/cc1431 focused checks pass. cc1428's ORT regression followed an external reset and is not evidence of a bad NCM pin | rn-integration-readiness records `integration checks in progress`, reruns designated checks after the current type-reference fix and does not mark rn-build passed. |
| NCM work is unfinished or proves Native parity | Separate owner has root-accepted rc13 pin/compile, repeated feature-mode checks and actual 16-oracle model evidence, while namespace/replay regression remains a separate question | Keep rn-ncm-tests pending verification and report NCM separately from Native. No redundant pin retest. |

## History-owner bounded follow-up

The follow-up reads `ProjectSessionHistoricalIngestor::new`, `with_original_provenance_resolver` and `run_pass` in `session_temporal_refresh_scheduler/history.rs`. It then follows `DaemonSessionSyncConfig.scope` and `SessionSyncProjectContext` in `session_sync.rs` into the concrete global ingest authority/request. It must answer whether the authoritative `tracedecay_contracts::ResolvedScope` reaches historical ingestion unchanged, while preserving resolver identity, transcript source home, CPU authority, cancellation and provenance.

cc1424 compile evidence reports the corresponding `transcript_source_home`, `background_cpu` and `original_provenance_resolver` fields unused in session-sync/project-lifecycle context while equivalent authorities are consumed in the history ingestor. The follow-up must explain that boundary or assign a minimal invariant fix; it must not perform unrelated cleanup. If scope is not consumed, the result names the exact product owner, propagation point and one acceptance assertion. If it is consumed indirectly, the result cites the concrete field/request evidence. No implementation edit belongs in this follow-up, and no path/name match is accepted as parity.

Root review blocks session delivery, session cases and product-build acceptance until this result is accepted.

## NCM and integration status

The separate NCM owner has root-accepted fastembed/ORT rc13 pin and locked compilation, repeated nine-library checks in both feature modes, three offline real-embedding checks and the model check with 16 oracle embeddings (maximum difference `2.09e-7`). Keep this evidence separate; the earlier intermittent NCM recall remains an explicit caveat. rn-ncm-tests remains pending verification for namespace/replay regression and final handoff, with no redundant pin retest. The dependency and runtime configuration type fixes are committed on the original branch through `7877bad6d`. cc1432 passes the locked product check with `memory-provider-host,semantic-fastembed`; cc1430/cc1431 pass nine focused Codex tests. The broader integration acceptance and Native parity gates remain pending. cc1428's ORT regression followed an external reset and does not invalidate the NCM pin. Do not mark rn-build passed from these checks.

## Exact next-ready tasks

After graph refresh, the first wave is exactly:

1. `rn-integration-readiness` — capture the cc1429 dependency result, the assigned `RuntimeTraceDecayConfig` correction and rerun the designated current product readiness checks.
2. `rn-reference` — refresh runner identity, 570 checkout verification and the proposed case contract.
3. `rn-source-docs` — refresh current inventory and path/symbol map against 570 without rewriting historical audit text.
4. `rn-ncm-tests` — retain pending verification under the separate rc13 owner and keep NCM results distinct.

`rn-map-checker`, `rn-contract`, the privacy gates and `rn-history-owner-followup` remain behind their explicit source/readiness dependencies. `rn-restore-anchor` is the one current gate already complete from root-provided proof. No draft API, runner, historical report or single smoke test releases the implementation chain.

## Historical artifacts

The following remain historical and are not blanket-edited: `execution-results/native-operation-routes.md`, `contract-outputs.md`, `map-checker.md`, `reference-build-requirements.md`, `source-docs.md`, `restore-anchor.md`, `privacy-isolation-design.md`, the old source inventory and the `native-original-execution-20260911` graph. Current metadata points to this report and to the new recovery-scoped graph so later owners can distinguish old evidence from active gates.

## Recovered checkpoint validation

The continuously evolving branch is `feat/pluggable-memory-providers-v2`. Audited merge `1fe250fed` is followed by recovered NCM pin commit `13446bde0`, session/hook compatibility commit `9d9cb8b5d`, shutdown dependency commit `7097b7c64`, and configuration rename commit `7877bad6d`; all are pushed to origin. The extra recovery branch has been removed after fast-forwarding these commits onto the original branch. Existing drafts are preserved.

At `7877bad6d`, cc1432 passed `cargo check --locked -p tracedecay --features memory-provider-host,semantic-fastembed` with zero errors and 34 existing warnings. cc1430 passed eight focused Codex provider tests; cc1431 passed one current-user Codex projection test. These results close the observed compilation blockers; full Native behavior, NCM namespace/replay regression, and aggregate verification remain separate pending plan gates.
