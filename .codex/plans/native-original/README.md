# Restore complete original Native

Current implementation scope: [Complete Native/NCM and semantic readiness](READINESS-PLAN.md). This supersedes preservation-only NCM caveats with a causal repair and verification chain.

This plan keeps the original Native memory implementation and adapts our product around it. Native includes facts, retrieval tracking, trust and lifecycle behavior, sessions, temporal retrieval and LCM. NCM remains a separate selectable provider.

**Status: active execution.** The active original is unmodified upstream PR707 tip `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`; candidate metadata is `1fe250fed7ca615f1dfdcd580befeb276328e910` on branch `feat/pluggable-memory-providers-v2` at `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. The older b3/571 audit remains historical. The current graph has 40 nodes and 89 edges; the retrieval-anchor and integration-readiness gates are complete.

## The implementation decision

- Keep the original Native memory and LCM implementation at the active 570 boundary. The retrieval-anchor file already equals the active baseline blob, so rn-restore-anchor records a completed read-only equality proof without a source write.
- Preserve existing typed original operations and their owners. Explicit Search retains retrieval tracking; automatic context reads keep their original read-only behavior.
- Remove the added Native staged store, scorer, merged results and staged lifecycle behavior. Leave its old saved files untouched.
- Deliver Native context once through the original host path. Keep the shared canonical fact/session/LCM services and NCM observer/active behavior intact.
- Prove behavior with a separately built, unmodified original reference, including facts, sessions/LCM, saved state and actual Claude/Codex delivery.

[SOURCE-BOUNDARY.md](SOURCE-BOUNDARY.md) records the protected engine, active equality gate and existing shared host extensions. rn-source-docs reopens the current 570-to-candidate inventory without rewriting the historical 85-path/278-hunk b3-to-571 audit. Existing privacy, source-identity and cursor safety fixes remain intact and receive separate regression checks.

[Reviewed decisions](REVIEWED-DECISIONS.md) are authoritative over provisional audit suggestions. [Worker rules](WORKER-RULES.md) define protected code, scope, review and build discipline.

## Parallel execution

The [execution graph](execution-graph.mmd) contains **40 nodes and 89 dependency edges**. The current frontier is rn-ncm-reproduce, rn-reference, rn-semantic-diagnose and rn-source-docs. The 201-row acceptance matrix and integration-readiness gate are complete. Root keeps one continuous focus on the active branch/worktree and sequences ready owners from the saved graph; no independent development branch or worktree launch is implied.

Use available Codex-native execution agents for scoped implementation; root assigns ownership and reviews changes before pushing. Each plan supplies exact ownership, prerequisites, ordered steps, one acceptance checklist, prohibited shortcuts and a stop condition.

1. Reproduce the NCM miss, build the independent 570 comparison runner, correct the source inventory/documentation and diagnose semantic provisioning from the current frontier. The acceptance, integration-readiness and anchor gates are complete.
2. Start the original reference build as soon as the runner is ready. When the runner and contracts are ready, launch three fixture lanes and host fixtures; contracts also release the Native wrapper and fabric work.
3. Release registry, the concrete Native bridge, session delivery and host context as their interfaces become ready.
4. Integrate the Native tests and composition, then run the shared product build/checks with one Cargo owner.
5. Run facts, sessions/LCM, state, Claude, Codex and NCM verification concurrently from the tested artifacts.
6. Two independent reviewers check original fidelity and integration. Root accepts completion only after required findings are resolved.

The reference build overlaps implementation. Manifests and the shared Cargo schedule have one owner. Writers never share a hotspot concurrently.

## Executable plans

| Node | Outcome |
| --- | --- |
| [rn-contract](execution/rn-contract.plan.md) | Make the product interface fit complete original Native |
| [rn-reference](execution/rn-reference.plan.md) | Build the independent original Native comparison runner |
| [rn-source-docs](execution/rn-source-docs.plan.md) | Correct Native identity and architecture documentation |
| [rn-cases-facts](execution/rn-cases-facts.plan.md) | Author original fact behavior comparison cases |
| [rn-cases-sessions](execution/rn-cases-sessions.plan.md) | Author original session and LCM comparison cases |
| [rn-cases-state](execution/rn-cases-state.plan.md) | Author saved-state and owner isolation comparison cases |
| [rn-native-port](execution/rn-native-port.plan.md) | Make the Native wrapper delegate without staged semantics |
| [rn-fabric](execution/rn-fabric.plan.md) | Preserve routing guarantees for the restored Native boundary |
| [rn-registry](execution/rn-registry.plan.md) | Register Native and NCM with truthful independent capabilities |
| [rn-native-bridge](execution/rn-native-bridge.plan.md) | Replace the staged Native backend with original service delegation |
| [rn-session-delivery](execution/rn-session-delivery.plan.md) | Retire Native staged acknowledgements in the host observation journey |
| [rn-host-context](execution/rn-host-context.plan.md) | Deliver original Native context once through its original host path |
| [rn-native-tests](execution/rn-native-tests.plan.md) | Replace substitute-backed Native tests with original behavior checks |
| [rn-host-fixtures](execution/rn-host-fixtures.plan.md) | Prepare real Claude and Codex Native comparison journeys |
| [rn-ncm-tests](execution/rn-ncm-tests.plan.md) | Preserve NCM behavior while Native is restored |
| [rn-composition](execution/rn-composition.plan.md) | Connect the reviewed Native services and remove staged mount assumptions |
| [rn-build](execution/rn-build.plan.md) | Integrate shared manifests and verify both actual runtimes |
| [rn-verify-facts](execution/rn-verify-facts.plan.md) | Verify complete fact behavior against untouched 570 |
| [rn-verify-sessions](execution/rn-verify-sessions.plan.md) | Verify original sessions and LCM against untouched 570 |
| [rn-verify-state](execution/rn-verify-state.plan.md) | Verify cutover leaves original and legacy saved state intact |
| [rn-verify-claude](execution/rn-verify-claude.plan.md) | Verify original Native through real Claude delivery |
| [rn-verify-codex](execution/rn-verify-codex.plan.md) | Verify original Native through real Codex delivery |
| [rn-verify-ncm](execution/rn-verify-ncm.plan.md) | Verify NCM still works with shared canonical services |
| [rn-review-fidelity](execution/rn-review-fidelity.plan.md) | Independently review full original Native fidelity |
| [rn-review-integration](execution/rn-review-integration.plan.md) | Independently review integration, data safety and graph completion |
| [rn-restore-anchor](execution/rn-restore-anchor.plan.md) | Record retrieval-anchor equality against active 570 |
| [rn-close](execution/rn-close.plan.md) | Accept the complete restored Native implementation |
| [rn-build-reference](execution/rn-build-reference.plan.md) | Build untouched original Native while product work runs |

| [rn-acceptance-matrix](execution/rn-acceptance-matrix.plan.md) | Freeze complete operation coverage and acceptance oracles |
| [rn-ncm-reproduce](execution/rn-ncm-reproduce.plan.md) | Identify the first loss behind intermittent recall |
| [rn-ncm-recall-fix](execution/rn-ncm-recall-fix.plan.md) | Repair the demonstrated recall failure |
| [rn-semantic-diagnose](execution/rn-semantic-diagnose.plan.md) | Trace artifact, projection and calibration readiness |
| [rn-semantic-fix](execution/rn-semantic-fix.plan.md) | Repair the demonstrated semantic lifecycle or setup gap |
| [rn-verify-semantic](execution/rn-verify-semantic.plan.md) | Prove strict semantic retrieval and recovery |
| [rn-release-readiness](execution/rn-release-readiness.plan.md) | Require three consecutive integrated passes and pilot instructions |

## Start or resume

Use [ORCHESTRATION.md](ORCHESTRATION.md) for exact graph targeting, sequencing rules, conflict ownership and the scoped handoff template. The saved [handoff bundle](execution-handoff.json) and [selection](execution-selection.json) contain all 89 edges and exact active paths.

From the execution worktree:

```sh
python3 -S .codex/plans/native-original/graph.py validate
python3 -S .codex/plans/native-original/graph.py frontier
```

The helper calls the installed plan-graph CLI with the exact selection; it does not start agents or implementation.

## Required evidence

A source diff alone cannot establish full Native behavior. The original reference and product must use equivalent isolated inputs at matching production boundaries. Preserve scores/order, provenance, effects, receipts and original failure outcomes. Check the entire original operation map, including background responsibilities; unknown or unreachable original coverage cannot be counted as success.

The actual CLI build and isolated daemon/index smoke passed at `276ded8fa`. Complete Native parity remains unverified, intermittent NCM recall remains unresolved, and semantic search reported `calibration_unavailable` with `artifact_unavailable` in the daemon log. The readiness plan requires causal diagnosis and repair plus repeated production-path verification before closure; historical model-oracle and compile evidence does not satisfy these gates.

[Research reports](evidence/) support the decisions. The research graph and plan-review graph are excluded from the runnable execution selection.
