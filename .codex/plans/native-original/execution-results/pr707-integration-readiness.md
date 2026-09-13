# rn-integration-readiness — current PR707 gate

Date: 2026-09-13
Worktree: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`
Branch: `feat/pluggable-memory-providers-v2`
Current HEAD: `d17f115f4bf455bf205eb39ee1de0b17fb9eaf6b` (`docs(graph): record live Luna Max wave`)
Active original baseline: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`
Audited candidate metadata: `1fe250fed7ca615f1dfdcd580befeb276328e910`

## Decision

**Integration checks in progress. The branch is not ready to release `rn-build` or any dependent Native readiness node.**

The observed product compilation blockers through cc-1429 have been addressed in commits `9d9cb8b5d`, `7097b7c64`, and `7877bad6d`. The current source still matches the source checkpoint used by the successful cc-1432 check and cc-1434 candidate CLI build: the commits after `7877bad6d` on this branch change plan and graph documentation only. Current inspection shows `RuntimeTraceDecayConfig` at the three production configuration seams in `crates/tracedecay/src/daemon/project_composition.rs`; the three stale `tracedecay_configuration::config::TraceDecayConfig` references reported by cc-1429 are gone.

That evidence closes the known compiler failures. It does not establish final readiness. The real NCM worker artifact, independent 570 reference artifact, Native differential coverage, semantic artifact and lifecycle checks, NCM recall reliability campaign, required aggregate checks, and three consecutive integrated acceptance runs remain unaccepted. The macOS `peak_rss` and linker warnings are informational and are not treated as readiness blockers.

This lane made no product, test, manifest, generated-contract, or graph-status edits and did not rerun Cargo. The report is an evidence gate for root review; root retains ownership of releasing `rn-build` and dependent nodes.

## Passed evidence

These are the exact current-worktree broker tickets available for review:

| Ticket | Command | Result | Boundary of the evidence |
| --- | --- | --- | --- |
| cc-1430 | `cargo test --locked -p tracedecay-sessions --lib --features test-helpers project_provider -- --nocapture --test-threads=1` | 8 passed, 0 failed, 577 filtered | Focused project-provider/session tests only. |
| cc-1431 | `cargo test --locked -p tracedecay-sessions --lib --features test-helpers current_user_message_reaches_project_admission_and_projection -- --nocapture --test-threads=1` | 1 passed, 0 failed, 584 filtered | One current-user Codex projection test only. |
| cc-1432 | `cargo check --locked -p tracedecay --features memory-provider-host,semantic-fastembed` | Finished in 11.47s with no compiler errors; 34 existing warnings were recorded | Locked library check; it does not build the CLI or NCM worker and does not prove runtime parity. |
| cc-1434 | `cargo build --locked -p tracedecay-cli --bin tracedecay --features memory-provider-host,semantic-fastembed` | Finished in 13m36s with no compiler errors | Actual candidate CLI build. Warnings included the macOS linker `__eh_frame` message; no final artifact identity/reference comparison was accepted. |

The NCM encoder evidence is separate: the pinned FastEmbed/ORT rc13 checks and 16 oracle embeddings were accepted with maximum difference about `2.09e-7`. That verifies the encoder pin; it does not verify NCM recall completeness, worker routing, namespace isolation, replay, or host delivery.

## Resolved historical blockers

The failed tickets remain part of the audit trail and must not be reported as passing checks:

- cc-1421 found three Codex/session API or signature errors (`E0061`, `E0063`, `E0425`); the compatibility correction is in `9d9cb8b5d`.
- cc-1424 found the hook dispatch `E0308` tuple return mismatch; it is corrected in `9d9cb8b5d`.
- cc-1425 found the missing `admit_codex_jsonl_page` test-helper argument; it is corrected in `9d9cb8b5d`.
- cc-1427 found the missing `futures-util` daemon-service dependency and `FutureExt` method resolution; `7097b7c64` adds the dependency and lockfile entry.
- cc-1429 found three `TraceDecayConfig` lookup failures in `project_composition.rs`; `7877bad6d` follows the upstream runtime type rename. The current production signatures use `RuntimeTraceDecayConfig`, and the test alias at line 3101 is intentional.
- cc-1428 was an ORT resolution failure after an external reset. It is excluded from adjudication of the accepted rc13 NCM pin.
- cc-1433 used the invalid target `cargo build --locked -p tracedecay --bin tracedecay ...`; the ticket reports that the binary belongs to `tracedecay-cli`. cc-1434 used the corrected package and target successfully.

## Remaining blockers and owners

| Area | Current state | Owner / required evidence |
| --- | --- | --- |
| Candidate and worker artifacts | The candidate CLI build has cc-1434 evidence. The real worker build is not recorded for the current source checkpoint. | `rn-build` designated build owner: build `tracedecay-ncm-worker` with `real-encoder`, retain absolute paths and digests, and rebuild the CLI if later source changes affect it. |
| Aggregate repository checks | `cargo test-ci --locked` and `cargo test-all --locked` are not accepted in this gate. Their default suites also do not replace ignored real NCM/semantic suites. | `rn-build` / root: run both aggregate checks after the candidate and worker are coherent, with actual test counts and logs. |
| Independent reference | No root-accepted, independently built unmodified 570 reference artifact and corresponding operation mapping is recorded here. | `rn-reference` and `rn-build-reference`: build the 570 reference from the pinned source and preserve independent artifact identity. |
| Native behavior parity | Route, facts, sessions, LCM, lifecycle, saved-state, host-effects, cancellation, restart, and differential reference cases are not all accepted. | Native execution and verification owners, followed by root review. A passing compile or focused smoke test cannot close this gate. |
| NCM runtime and recall | Namespace/replay/restore/cancellation and active/observer coverage remain pending. The historical missing-item workload and its causal repair are not closed by the encoder result. | `rn-ncm-reproduce`, `rn-ncm-recall-fix`, `rn-ncm-tests`, and `rn-verify-ncm`: reproduce the before case, prove the bounded repair, then complete 100 ordered trials (25 cold, 25 warm, 25 restart, 25 load) with the real worker. |
| Semantic retrieval | Existing evidence reports unavailable calibration/artifacts; strict semantic serving and recovery with provisioned real artifacts are not accepted. | `rn-semantic-diagnose`, `rn-semantic-fix`, and `rn-verify-semantic`. |
| History/privacy seams | Typed Claude history/source-field follow-up and related privacy scope remain pending. | `rn-history-owner-followup`, `rn-privacy-audit`, and `rn-privacy-restore`. |
| Release acceptance | Three consecutive integrated passes on one immutable binary/model/calibration identity are absent. | `rn-release-readiness` and root, after all predecessor evidence is accepted. |

## Exact next checks

This lane will not submit Cargo. The designated single broker/build owner should first check broker state, then run the missing worker artifact gate in the assigned worktree:

```sh
cargo build --locked -p tracedecay-memory-ncm-runtime --bin tracedecay-ncm-worker --features real-encoder
```

The pinned candidate artifact gate, repeated only when its inputs change, is:

```sh
cargo build --locked -p tracedecay-cli --bin tracedecay --features memory-provider-host,semantic-fastembed
```

After both artifacts are coherent, run the required repository aggregates through the same owner:

```sh
cargo test-ci --locked
cargo test-all --locked
```

Provider tests that exercise the real worker must use an absolute `TRACEDECAY_NCM_WORKER` path to the prebuilt worker so the test does not invoke Cargo recursively. These commands still precede the independent 570 comparison, real NCM/semantic acceptance suites, and three consecutive integrated passes. No command above is claimed as passed by this report unless its ticket is listed in the passed-evidence table.

## Read-only verification performed

- Confirmed current HEAD and recent history with `git rev-parse`, `git show`, and `git log`.
- Confirmed the source and manifest paths relevant to cc-1432/cc-1434 have no diff after `7877bad6d`.
- Inspected current configuration type references and the daemon-service `futures-util = "0.3.33"` declaration.
- Read the node plan, build-owner plan, readiness plan, reassessment, accepted execution results, and broker ticket logs.
- Did not run Cargo, alter source or manifests, change plan/graph status, commit, or push.

Residual blocker for root review: the missing real-worker artifact and all final Native/NCM/semantic/aggregate acceptance evidence. `rn-build` remains pending until its designated owner supplies those exact results.
