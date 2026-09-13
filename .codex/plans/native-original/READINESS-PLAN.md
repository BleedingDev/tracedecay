# Complete Native/NCM and semantic readiness

This is an implementation plan, not a completion claim. Continue exclusively on `feat/pluggable-memory-providers-v2` in its existing worktree. Planning changes only plan files; no implementation agents, Cargo, live profile changes or installation are started by this request. The existing Native restoration tasks remain part of the work.

## What is known and what is missing

| Area | Verified evidence | Missing outcome |
| --- | --- | --- |
| Executable | At `276ded8fa`, cc1434 built the actual tracedecay-cli executable with memory-provider-host and semantic-fastembed; version/help and isolated daemon start/stop passed. | Final rebuilt artifacts and aggregate checks after repairs. |
| Basic code search | A Git fixture enrolled, generation became fresh and exact search returned branch_smoke. | Real semantic retrieval and its provisioning/recovery path; exact fallback is insufficient. |
| Semantic | Fresh profile reported calibration_unavailable; daemon also logged artifact_unavailable. Current serving code uses calibration_unavailable for absent query authority. | Identify setup versus acquisition/projection/calibration/activation failure, correct it, and pass strict-semantic paraphrase queries with real artifacts. |
| NCM encoder | FastEmbed 6.0.3/ORT rc13 checks and real model comparison passed: 16 oracle embeddings, maximum difference about 2.09e-7. | Full worker/host reliability; these checks do not prove recall completeness. |
| NCM recall | Historical integration report records 2/4 then 4/4. A frozen trace/budget proposal exists but was not established as its cause. | Recover the workload, identify the first causal loss, repair it, retain a failing-before/passing-after regression and complete the reliability campaign. |
| Native | Retrieval-anchor and privacy-detector source equality with upstream 570 were recorded. | Complete route inventory, independent reference runner, remaining adapter/delivery work, fact/session/LCM/lifecycle/state/host parity and independent reviews. |

Evidence: execution-results/ncm-tests.md, pr707-plan-reassessment.md, restore-anchor-57006f60.md and privacy-audit-57006f60.md; local smoke receipts under target/test-profile/branch-smoke-hn1komyc/{result,enrolled-result,index-result}.json and index-daemon.log. Durable summary above survives removal of those test artifacts. The manifest under .codex/patches/pm-ncm-recall-trace-budgets historically says `frozen_unapplied`, but a live source audit found the proposed trace-exclusion and UTF-8-budget behavior already present from commit `42aee2dee0`; treat it as implemented, locally unexecuted and causally unproven. Graph lookup returned zero scanned files for the selected checkout during the planning turn, so current exact file paths were verified directly.

## Execution order

1. **Freeze acceptance and recover causal evidence.** rn-acceptance-matrix defines complete required operations, expected results, identities and negative controls. rn-ncm-reproduce recovers the missing recall case. rn-semantic-diagnose traces fresh/provisioned/restarted semantic readiness. Start with the matrix, then NCM reproduction, then semantic diagnosis on this one branch; disjoint scopes may be assigned later without creating separate development branches.
2. **Make bounded repairs.** rn-ncm-recall-fix repairs the demonstrated loss. rn-semantic-fix repairs the demonstrated lifecycle/setup transition. Reproduction tests fail before and pass after. No arbitrary sleeps, relaxed budgets, fabricated calibration or fallback relabeling. Setup-only resolution is allowed only when the diagnosed setup procedure demonstrably restores the expected behavior.
3. **Finish the existing Native restoration chain.** Refresh source/docs and independent runner against `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`; complete contract/Native port/fabric/bridge/registry/host/session composition, history-owner scope and privacy gates. Existing facts, sessions, LCM, saved-state and host cases remain mandatory. Use a read-only immutable reference artifact or detached reference checkout as test input, never a second development branch or the modified candidate as its own oracle.
4. **Build once coherently, then verify.** rn-build owns shared manifests and broker work. Existing Native verification nodes compare corresponding original and candidate production boundaries; rn-verify-ncm runs the real worker and 100-trial campaign; rn-verify-semantic proves real semantic participation and lifecycle recovery. Rebuild/retest changed inputs or actual failures; do not rerun the encoder pin checks merely to claim more passes.
5. **Review and repeat integrated acceptance.** Both independent reviews consume Native, NCM and semantic evidence. rn-release-readiness requires complete coverage, required repository aggregate checks and three consecutive integrated passes on one immutable source/artifact set. rn-close is gated on that result.

## Definition of done

- Every required Native operation/background responsibility maps to an executable independent-reference case with correct results, effects, receipts, provenance, restart and saved-state behavior. No unfinished adapter route, unsupported required operation, same-backend oracle or mock-only parity.
- Original NCM missing-item behavior has causal failing/passing evidence; at least 100 frozen trials (25 cold, 25 warm, 25 restart, 25 load) have zero unexplained required-item misses or scope/delivery violations. Fixed quality and latency expectations, all attempts retained. New passing workloads alone cannot close the historical issue.
- Provisioned strict-semantic queries return the predefined relevant results with actual semantic serving evidence. Negative/offline/corrupt/mismatch tests preserve truthful failure or explicit fallback, and compatible acquisition/restart/index changes recover to ready. At least ten repetitions of critical cold/warm/restart cases.
- Full real-worker scope/replay/restore/cancellation/active/observer coverage and shipped Codex/Claude hook contracts pass. Actual external app interaction is separate manual coverage; stay in Codex and do not launch another agent CLI.
- Required aggregate checks pass, followed by three consecutive integrated acceptance runs on the same reviewed binaries/model/calibration identities with isolated profiles. Any failure remains visible and resets consecutive acceptance after correction.
- A tested Codex-native pilot configuration, exact binary/worker paths, data/socket isolation, model setup, init/readiness checks and rollback procedure exist. Stable V1 installation and operator databases are not replaced by this plan.

## Guardrails and decisions

Native reference is pinned to upstream 570; b3/571 evidence is historical. Keep original Native algorithms, ownership and saved data intact. Latest direction authorizes planning evidence-based NCM and semantic repairs, superseding older preservation-only caveats only within the new exact node scopes. Preserve pinned model/tokenizer identities, namespaces, state compatibility, privacy/provenance and existing budgets; do not tune ranking/model quality to hide a transport, lifecycle or filtering bug.

Use Codex tools and available native agents during later execution. Assign disjoint exact files, serialize common seams and all Cargo through one owner, review diffs before pushing, and checkpoint often on the same branch. Do not invoke another agent CLI, make a new development branch, migrate/copy operator state or switch the installed stable service. No user approval is needed to author this concrete plan.
