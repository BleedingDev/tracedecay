# Original Native execution orchestration

## Canonical handoff

Goal: restore complete original Native code and behavior while preserving NCM and shared canonical host services.

- Execution worktree: `/Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2`.
- Exact selection: `--plans-root /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution --glob '*.plan.md'`.
- Dependency overlay: all 60 exact edges in [execution-selection.json](execution-selection.json); do not omit or paraphrase them.
- Graph ID: `native-original-execution-20260911`.
- Selection hash: `6072fc594a`.
- Snapshot: `/Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-20260911/snapshot.json`.
- State directory: `/Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-20260911`.
- Complete resolved bundle: [execution-handoff.json](execution-handoff.json).
- State ledger: `/Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-20260911/operator-log.md`.

Only `execution/*.plan.md` is selected. README, research, plan-review and other historical project plans are deliberately excluded. Do not add umbrella documents as runnable orphan plans. Revalidate and refresh the full bundle after any edge/selection change.

## Limits and scheduling

Resolved limits: max_threads=50, max_depth=3. Reserve root; use at most 49 subagents, and only ready nodes with actual independent ownership. This graph has a maximum independent set of 12 nodes and a ten-node longest dependency chain. No nested delegation is planned. All execution/test/review agents use `gpt-5.6-luna` with `reasoning_effort=max`.

Start these three pending nodes together: rn-reference, rn-source-docs and rn-ncm-tests. After source inventory and original binary requirements are accepted, immediately start the reference build. Source inventory acceptance releases rn-contract and rn-restore-anchor. After rn-contract is accepted, immediately start the Native wrapper and fabric; the three case authors and host fixtures need both runner and contract outputs.

After the Native wrapper is accepted, launch registry, Native bridge, session-delivery and host-context owners together. Native test edits depend on the completed bridge; read-only preparation can happen earlier. Composition waits for accepted bridge, registry, host, observation and test outputs. The critical implementation chain is source inventory → contract → wrapper → bridge → Native tests → composition → product build → verification → independent review → root close.

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
| Original retrieval-anchor authority sole changed method | rn-restore-anchor; exact b3 restoration only |
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
Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2
Execution HEAD: <actual reviewed current HEAD>
Original reference: b3b43410e47115056f2066449aafa1822bbb6049
Graph: native-original-execution-20260911
Selection hash: <current execution-handoff.json value>
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
2. Launch all ready disjoint nodes within the budget. Do not launch rn-close as an agent; it belongs to root.
3. Implementation nodes complete their reviewed authoring outputs and lightweight checks; deferred Cargo/runtime acceptance belongs to the build and verification nodes. Do not claim runtime success early or create a circular dependency by waiting for the downstream build before accepting a compile-ready predecessor. Reopen affected nodes when later checks expose defects.
4. Review each returned diff, ownership and evidence. Reject hidden scope, fake original providers, empty success, disabled assertions and unrun-test claims.
5. Mark only accepted completed work complete and release its successors. Update the ledger with node, agent, files, blocker, status and next action.
6. If a public contract changes, reopen affected downstream work and rerun required checks before trusting stale outputs. Cross-owner fixes go through root to the correct execution agent.
7. After one avoidable overlap, tighten ownership; after a repeated overlap, serialize that hotspot. Keep unrelated ready lanes active.
8. Before any future push, review the full integrated diff. This plan itself neither runs nor authorizes publishing.

Use Helm for live steering after launch. Root synthesizes findings and reviews; it does not become the fallback product-code writer.

## Completion

All original public operations/background responsibilities need concrete routes and executed evidence. Source equality, full behavior, safe saved-state handling, real host delivery and NCM integrity are separate checks. Preserve unresolved results rather than calling a partial projection full Native. The independent reviewers feed rn-close; root resolves their concrete findings through execution owners.

## Execution finding: original privacy dependency

The current graph has 31 nodes. Root review found a shared original entropy-detector change omitted by the first inventory. rn-privacy-audit runs read-only alongside inventory corrections and feeds rn-build acceptance. It owns only execution-results/privacy-isolation-design.md. Root must assign a concrete restoration/product-boundary correction after review; the prior source-preservation claim is not accepted yet. NCM authoring is accepted, with runtime verification deferred to rn-build.

The independent rn-map-checker owner repairs confirmed stale source/catalog expectations in the existing Python checker and tests. It feeds rn-build; it never establishes runtime parity. Exact files are isolated from source-docs ownership.

Root accepted the corrected85path source inventory as documentation. Contract and exact anchor restoration are now active. The unresolved privacy algorithm difference blocks the candidate build and must receive a concrete implementation owner after the audit; documentation acceptance does not establish source parity.

Privacy audit accepted: rn-privacy-restore owns exact detector restoration and typed product admission; feeds both session-delivery (file serialization) and rn-build. Runtime evidence remains pending.

Root reviewed the b3 CLI manifest and reference-build-requirements.md. Reference compilation now depends on accepted rn-source-docs; runner acceptance still blocks all comparisons. This releases independent compilation without premature runner acceptance.
