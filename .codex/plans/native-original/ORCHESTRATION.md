# Original Native execution orchestration

## Canonical handoff

Goal: restore complete original Native code and behavior while preserving NCM and shared canonical host services.

- Active original reference: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` (unmodified upstream PR707 tip); candidate metadata: `1fe250fed7ca615f1dfdcd580befeb276328e910`.
- Execution branch/worktree: `feat/pluggable-memory-providers-v2`, `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`.
- Reference checkout: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`; the b3 checkout and audit remain historical.
- Exact selection: `--plans-root /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution --glob '*.plan.md'`.
- Dependency overlay: all 89 exact edges in [execution-selection.json](execution-selection.json); do not omit or paraphrase them.
- Graph ID: `native-original-execution-pr707-20260913`.
- Plan-set hash: `5befc5f407`; selection hash: `bbc92e4d52`.
- Snapshot: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-pr707-20260913/snapshot.json`.
- State directory: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-pr707-20260913`.
- Complete resolved bundle: [execution-handoff.json](execution-handoff.json).
- Active state ledger: the current graph snapshot and generated handoff under `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-pr707-20260913/`; the old 20260911 operator log is historical.

Only `execution/*.plan.md` is selected. README, research, plan-review and other historical project plans are deliberately excluded. Do not add umbrella documents as runnable orphan plans. Revalidate and refresh the full bundle after any edge/selection change.

## Limits and scheduling

Resolved limits: max_threads=50, max_depth=3. Reserve root; use at most 49 subagents, and only ready nodes with actual independent ownership. This graph has 40 nodes and 89 edges; the saved summary/frontier are authoritative for current state. No nested delegation or development branch/worktree fan-out is planned. All execution/test/review agents use `gpt-5.6-luna` with `reasoning_effort=max`.

The current frontier has two pending nodes: rn-acceptance-matrix and rn-integration-readiness. Root keeps one continuous focus on this branch/worktree and may sequence these disjoint read/write scopes as evidence arrives; no independent branch or worktree launch is implied. Source inventory and readiness acceptance release the contract, map, privacy and history gates. The anchor node is already complete from the current 570 blob proof. After contract acceptance, release the Native wrapper and fabric; history-owner validation gates session cases and delivery.

After the Native wrapper is accepted, the graph exposes registry, Native bridge, session-delivery and host-context work. Root sequences those owners within the same branch/worktree; Native test edits depend on the completed bridge. Composition waits for accepted bridge, registry, host, observation and test outputs. The critical implementation chain is source inventory → contract → wrapper → bridge → Native tests → composition → product build → verification → independent review → root close.

Start rn-build-reference early and reuse the same designated Cargo agent for rn-build. The reference and product worktrees have isolated default targets/data; do not multiply targets or duplicate broker tickets. Fixture authors and implementation agents do not submit Cargo. Six post-build verification nodes consume the same verified artifacts in separately owned process/state roots.

Do not wait for an entire visual wave if an individual successor is ready. A task is ready only when root has reviewed and accepted all predecessor outputs and its exact files are free.

## Conflict map

| Shared surface | Sole writer |
| --- | --- |
| Provider API and its canonical contracts/generator | rn-contract |
| Native wrapper crate and wrapper tests | rn-native-port |
| Concrete Native port and removal of staged implementation | rn-native-bridge |
| Native concrete tests | rn-native-tests, after bridge |
| Fabric | rn-fabric |
| Registry, recall plans and any required pack type use | rn-registry |
| observation_journey.rs | rn-privacy-restore first; then rn-session-delivery |
| cognitive_recall.rs and tool_dispatch.rs | rn-host-context |
| project_composition.rs and retained_owner.rs | rn-composition |
| Existing shared Claude/Codex journey test file | rn-host-fixtures |
| Comparison runner, excluding cases | rn-reference |
| Facts / sessions / state cases | Their three separate case authors |
| Cargo manifests/lockfile and all Cargo scheduling | Designated build owner; rn-build-reference then rn-build |
| Retrieval-anchor current source gate | rn-restore-anchor; read-only 570/candidate blob equality only |
| All other original Native/session/LCM engines and NCM internals | No writer |

The original-source boundary inventory takes precedence over broad path globs. Existing shared-host differences require review; a whole directory is not a blanket safe-edit allowlist. Generated outputs outside a contract owner's named paths require a root-reviewed ownership adjustment before generation. No two writers receive the same file.

## Exact launch handoff

Fill this template from the saved graph for each node; use the entire named plan as its instructions rather than sending only a title.

```text
Task: <node ID and concrete outcome>
Role: execution / verification / read-only reviewer
Model: gpt-5.6-luna
Reasoning: max
fork_turns: none
Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2
Execution HEAD: <actual reviewed current HEAD>
Original reference: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6
Graph: native-original-execution-pr707-20260913
Selection hash: bbc92e4d52
Plan: <absolute execution/<node>.plan.md>
Read first: WORKER-RULES.md and REVIEWED-DECISIONS.md
Accepted predecessor outputs: <paths, exact type/API decisions and root acceptance>
Exclusive ownership: <copy exact owned files from the plan>
Output location: <owned diff and/or isolated node result path>
No original-code edits, peer edits, nested agents or unassigned Cargo.
You are not alone in this codebase; preserve peers' changes.
Return the plan's full output contract and stop at its stop condition.
```

Use a fresh agent for each coherent implementation/review node. Reuse the build execution owner across its two ordered nodes. Reuse an owner for bounded corrections in its own files, with an explicit delta instruction; do not broaden the lane.

## Root's execution loop

1. Validate the exact graph and inspect the frontier. Record the current HEAD, active owners and dependencies in the ledger.
2. Process ready nodes in graph order within the budget and one continuous branch/worktree focus. Do not launch rn-close as an agent; it belongs to root.
3. Implementation nodes complete their reviewed authoring outputs and lightweight checks; deferred Cargo/runtime acceptance belongs to the build and verification nodes. Do not claim runtime success early or create a circular dependency by waiting for the downstream build before accepting a compile-ready predecessor. Reopen affected nodes when later checks expose defects.
4. Review each returned diff, ownership and evidence. Reject hidden scope, fake original providers, empty success, disabled assertions and unrun-test claims.
5. Mark only accepted completed work complete and release its successors. Update the ledger with node, agent, files, blocker, status and next action.
6. If a public contract changes, reopen affected downstream work and rerun required checks before trusting stale outputs. Cross-owner fixes go through root to the correct execution agent.
7. After one avoidable overlap, tighten ownership; after a repeated overlap, serialize that hotspot. Keep unrelated ready lanes active.
8. Before any future push, review the full integrated diff. This plan itself neither runs nor authorizes publishing.

Use Helm for live steering after launch. Root synthesizes findings and reviews; it does not become the fallback product-code writer.

## Completion

All original public operations/background responsibilities need concrete routes and executed evidence. Source equality, full behavior, safe saved-state handling, real host delivery and NCM integrity are separate checks. Preserve unresolved results rather than calling a partial projection full Native. The independent reviewers feed rn-close; root resolves their concrete findings through execution owners.

## Current reassessment gates

The active graph has 40 nodes and 89 edges. rn-privacy-audit records current detector equality against 570 but keeps the typed Claude history seam pending. rn-history-owner-followup validates whether `ResolvedScope` reaches historical ingestion and assigns a bounded owner/assertion if it does not. Both are read-only gates; no parity is fabricated.

The independent rn-map-checker owner repairs confirmed stale source/catalog expectations in the existing Python checker and tests. It feeds rn-build; it never establishes runtime parity. Exact files are isolated from source-docs ownership.

The old 85-path/278-hunk source inventory is historical b3-to-571 evidence. rn-source-docs reopens the current 570-to-candidate inventory without rewriting that audit. The retrieval-anchor equality gate is complete for the active baseline; the privacy seam and map/current inventory remain pending.

The active privacy restore plan is a read-only revalidation gate; it does not repeat the historical detector restoration. It feeds session-delivery and rn-build only after the typed seam review is accepted.

The reference-build plan targets the clean detached 570 checkout. Runner and comparison acceptance remain pending; no draft API, runner or single smoke test releases product build.

## Current readiness orchestration

READINESS-PLAN.md governs the NCM causal repair and semantic readiness work. The live wave uses Codex-native Luna Max agents on the existing branch, with one writer per owned surface and a single Cargo owner; historical model requirements cannot force another agent CLI. The final release-readiness gate follows both independent reviews and precedes close. All forty plan files and the exact 89 dependencies must be carried together.
