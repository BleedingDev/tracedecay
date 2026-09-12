# Original Native execution ledger

Graph: native-original-execution-20260911. Selection: a22a65832e. Exact selection and edges: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution-selection.json.

Planning is complete; implementation has not started. Original reference b3b43410e47115056f2066449aafa1822bbb6049; audited product head 571daf3a9612e5247443e4da3a107b542686c1ef. Root only orchestrates/reviews; all execution/test/review agents use gpt-5.6-luna / max. Revalidate current HEAD before launch.

| Node | Agent | Owner / scope | Dependency | Status | Next action |
| --- | --- | --- | --- | --- | --- |
| rn-contract | Unassigned | crates/tracedecay-memory-provider-api/** (excluding Cargo manifests) | rn-source-docs | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-reference | Unassigned | scripts/product/native-original/** except cases/** | None | pending | Launch after execution begins |
| rn-source-docs | Unassigned | product/upstream/** (metadata/documentation only) | None | pending | Launch after execution begins |
| rn-cases-facts | Unassigned | scripts/product/native-original/cases/facts/** | rn-reference, rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-cases-sessions | Unassigned | scripts/product/native-original/cases/sessions/** | rn-reference, rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-cases-state | Unassigned | scripts/product/native-original/cases/state/** | rn-reference, rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-native-port | Unassigned | crates/tracedecay-memory-provider-native/** except Cargo manifests | rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-fabric | Unassigned | crates/tracedecay-memory-fabric/** except Cargo manifests | rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-registry | Unassigned | crates/tracedecay-memory-provider-registry/** except Cargo manifests | rn-native-port | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-native-bridge | Unassigned | crates/tracedecay/src/daemon/retained_owner/native_provider.rs | rn-native-port | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-session-delivery | Unassigned | crates/tracedecay/src/daemon/retained_owner/observation_journey.rs | rn-native-port | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-host-context | Unassigned | crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs | rn-native-port | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-native-tests | Unassigned | crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs | rn-native-port, rn-native-bridge | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-host-fixtures | Unassigned | crates/tracedecay-cli/tests/product_memory_provider_claude_host_journey.rs | rn-reference, rn-contract | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-ncm-tests | Unassigned | crates/tracedecay-memory-provider-ncm/tests/** | None | pending | Launch after execution begins |
| rn-composition | Unassigned | crates/tracedecay/src/daemon/project_composition.rs | rn-native-bridge, rn-registry, rn-session-delivery, rn-host-context, rn-native-tests | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-build | Unassigned | Cargo.toml and Cargo.lock | rn-composition, rn-fabric, rn-host-fixtures, rn-ncm-tests, rn-reference, rn-build-reference, rn-restore-anchor | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-facts | Unassigned | target/task-scratch/native-original/facts/ (test data/results only) | rn-build, rn-cases-facts | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-sessions | Unassigned | target/task-scratch/native-original/sessions/ (test data/results only) | rn-build, rn-cases-sessions | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-state | Unassigned | target/task-scratch/native-original/state/ (test data/results only) | rn-build, rn-cases-state | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-claude | Unassigned | target/task-scratch/native-original/claude/ (test data/results only) | rn-build | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-codex | Unassigned | target/task-scratch/native-original/codex/ (test data/results only) | rn-build | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-verify-ncm | Unassigned | target/task-scratch/native-original/ncm/ (test data/results only) | rn-build | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-review-fidelity | Unassigned | .codex/plans/native-original/execution-results/fidelity-review.md | rn-verify-facts, rn-verify-sessions, rn-verify-state, rn-verify-claude, rn-verify-codex, rn-verify-ncm, rn-source-docs | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-review-integration | Unassigned | .codex/plans/native-original/execution-results/integration-review.md | rn-verify-facts, rn-verify-sessions, rn-verify-state, rn-verify-claude, rn-verify-codex, rn-verify-ncm | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-close | root | Plan statuses, graph snapshot, operator log and final user-facing result | rn-review-fidelity, rn-review-integration | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-build-reference | Unassigned | A clean detached b3 worktree under the repository .worktrees/ and its repo-local build/test artifacts | rn-reference | pending | Wait for accepted predecessors, then launch exact handoff |
| rn-restore-anchor | Unassigned | crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs (restore the sole b3-to-head method difference only) | rn-source-docs | pending | Wait for accepted predecessors, then launch exact handoff |


## Execution authorized 2026-09-11

HEAD 571daf3a9612e5247443e4da3a107b542686c1ef; exact 28-node/55-edge graph validated, selection a22a65832e. Limits 50 threads/3 depth; all workers Luna Max leaves. Root orchestrates/reviews only.

- rn-reference → /root/rn_exec_reference: running, runner-only ownership. Reference checkout setup deferred to build owner.
- rn-source-docs → /root/rn_exec_source_docs: running, exact metadata/docs/inventory ownership.
- rn-ncm-tests → /root/rn_exec_ncm_tests: running, NCM tests and scoped Replay documentation only.

Next: review individual outputs and release ready successors without wave barriers.

- rn-ncm-tests review: namespace test accepted for authoring; host fixture map received. Root corrected sole Replay docs ownership from nonexistent product/providers/ncm-rust/CONTRACT.md to actual product/ncm/spec/CONTRACT.md, Replay row only, after verifying worker Operation::Replay dispatch. No edge changes or overlapping owner. Bounded correction pending. Cargo checks deferred to rn-build.

- rn-ncm-tests accepted after root diff and host-fixture review. Two exact owned files; runtime checks deferred. Results: execution-results/ncm-tests.md. No successor newly ready (rn-build still waits for implementation).

- rn-reference draft review returned lifecycle/isolation/comparison correctness findings: real data/socket isolation; detached daemon lifecycle; selected composition evidence; actual reopen; output bindings; identity fingerprints; generator status reduction; nonvacuous required observables/operations. Not accepted.
- rn-source-docs initial return rejected as incomplete: 70-path/206-hunk inventory omitted privacy detector, maintenance, retained contracts/root and hook-runtime paths. Followup assigned to same owner. Root confirmed shared looks_high_entropy_token algorithm differs from b3; requires explicit original restoration/product-boundary decision before affected implementation. No original file edits yet. Native map checker stale paths also await exact scoped handoff.

- Expanded exact graph to 29 nodes/56 edges, selection d65807659d; strict validation passed before fresh Luna Max reviewer /root/rn_exec_privacy_audit launch. Owns privacy-isolation-design.md only. rn-privacy-audit -> rn-source-docs final acceptance. Source owner continues disjoint inventory/ADR corrections.

- Added rn-map-checker -> rn-build; exact graph30nodes57edges selection7cf86f3fca validated before /root/rn_exec_map_checker Luna Max worker launch. Owns existing checker and tests only. Source docs remains disjoint.

- rn-map-checker accepted after root diff review; checker and8existingPython tests pass. One exact script changed, tests unchanged. Documentation consistency only; build node still waits on other predecessors.

- Root accepted rn-source-docs revised accurate85path inventory/docs. Privacy algorithm drift remains explicitly unresolved; moved rn-privacy-audit edge from docs to build so unrelated interface/anchor authoring can advance. Any required privacy implementation is mandatory beforebuild and will be separately assigned. Exact graph validated before successor launch; selection 63b441f351.

- Started /root/rn_exec_contract (API/contracts/route map only) and /root/rn_exec_restore_anchor (exact solemethod restoration only), freshLunaMax leaves. Source docs accepted; protectedprivacy difference is a separate build prerequisite. NoCargo or commits.

- rn-restore-anchor accepted: root reviewed sole method diff and independently verified whole-file b3 equality (exit0/empty diff). Exact blobd976e2a23c19441a60412420a1db5e68f7ccc5ca. Runtime checks deferred build, noCargo.

- Root accepted rn-privacy-audit design. Added rn-privacy-restore exact detector/product seam lane and serialization before session-delivery, both required by build. Runtime checks pending.

- Root reviewed exact b3 CLI manifest; released rn-build-reference on accepted source-docs plus fixed production/test-transport build requirements. Replaced runner→reference-build with source-docs→reference-build; comparisons still blocked on runner.
