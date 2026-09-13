# Live operator ledger

## Canonical handoff bundle

- Goal: complete Native parity, repair intermittent NCM recall, repair semantic search, and prove release readiness on `feat/pluggable-memory-providers-v2`.
- Selection: `--plans-root /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution --glob '*.plan.md'`.
- Explicit dependency edges: the 89 `--depends` entries in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution-selection.json`; no extra edge overlay.
- Graph ID: `native-original-execution-pr707-20260913`.
- Selection hash: `bbc92e4d52`.
- Snapshot: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-pr707-20260913/snapshot.json`.
- State directory: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plan-graphs/native-original-execution-pr707-20260913`.
- Limits: `max_threads=50`, `max_depth=3`; wave 1 reserves 13 thread slots for root, follow-up verification, and replacements.
- Non-runnable plans excluded from selection: all wrapper/readme/research documents outside `execution/*.plan.md`.

## Execution shape

- Critical path: `rn-acceptance-matrix` -> `{rn-ncm-reproduce, rn-semantic-diagnose, rn-source-docs, rn-reference}` -> causal repairs and Native integration -> `rn-build` -> verification -> two reviews -> `rn-release-readiness` -> `rn-close`.
- Wave 1: two write owners for the two ready graph nodes plus read-only map/checker lanes that feed those owners and the immediately following diagnostic nodes.
- Wave 2: after root reviews and commits the ready-node artifacts, launch the now-ready NCM reproducer, semantic diagnostic, source inventory, and reference runner with one writer per disjoint surface.
- Wave 3: causal repairs, Native port/integration, focused verification, then aggregate acceptance.
- Merge points: root reviews every write result; commits small checkpoints on the same branch; shared interfaces land before dependent writers; one Cargo owner runs builds.
- Conflict hotspots: `Cargo.toml`/`Cargo.lock`, shared provider traits and registries, daemon project composition, semantic runtime lifecycle, and NCM runtime selection/recovery. These remain single-owner in every wave.
- Scope boundary: no new development branch/worktree, no stable V1 profile/service mutation, no other agent CLI, no product source edits by read-only lanes, and no Cargo from wave-1 scouts.

## Wave 1 lanes

| Lane | Agent | Owner / write scope | Dependency | Status | Next action |
| --- | --- | --- | --- | --- | --- |
| rn-acceptance-matrix | `/root/acceptance_matrix_owner` | `execution-results/readiness-matrix.{md,json}` only | ready | complete | Root accepted 201-row frozen matrix; wave 2 unblocked |
| rn-integration-readiness | `/root/integration_readiness_owner` | `execution-results/pr707-integration-readiness.md` only | ready | complete | Root accepted report; keep rn-build blocked |
| Native facts | `/root/native_facts_scout` | read-only | supports matrix | running | Map routes/oracles |
| Native trust/feedback | `/root/native_trust_scout` | read-only | supports matrix | running | Map routes/oracles |
| Native retrieval telemetry | `/root/native_retrieval_scout` | read-only | supports matrix | running | Map effect distinctions |
| Native sessions | `/root/native_sessions_scout` | read-only | supports matrix | running | Map routes/oracles |
| Native LCM | `/root/native_lcm_scout` | read-only | supports matrix | complete | Fed canonical LCM parity and Hermes integration gap |
| Native persistence | `/root/native_persistence_scout` | read-only | supports matrix | complete | Fed restart/state and staged-store preservation cases |
| Native lifecycle/background | `/root/native_lifecycle_scout` | read-only | supports matrix | complete | Fed lifecycle owners and Native actor shutdown risk |
| Native Codex delivery | `/root/native_codex_host_scout` | read-only | supports matrix | complete | Feed findings to matrix owner |
| Native Claude contract | `/root/native_claude_contract_scout` | read-only | supports matrix | running | Map shipped hook contract |
| Native privacy | `/root/native_privacy_scout` | read-only | supports matrix | running | Map isolation cases |
| Native reference runner | `/root/native_reference_scout` | read-only | supports rn-reference | running | Map immutable runner inputs |
| Native 570 diff | `/root/native_570_diff_scout` | read-only | supports rn-source-docs | complete | Feed staged-substitute map to wave 2 |
| NCM admission/journal | `/root/ncm_admission_journal_scout` | read-only | supports reproduce | running | Trace first pipeline segment |
| NCM worker pipeline | `/root/ncm_worker_pipeline_scout` | read-only | supports reproduce | complete | Fed host-admission evidence and worker-race instrumentation map |
| NCM selection/exclusions | `/root/ncm_selection_scout` | read-only | supports reproduce | running | Trace ranking/filter segment |
| NCM replay/recovery | `/root/ncm_recovery_scout` | read-only | supports reproduce | running | Trace persistence segment |
| NCM scopes | `/root/ncm_scopes_scout` | read-only | supports matrix | running | Map seven bindings |
| NCM cancellation/deletion | `/root/ncm_lifecycle_scout` | read-only | supports matrix | running | Map lifecycle cases |
| NCM tests | `/root/ncm_tests_scout` | read-only | supports matrix | running | Inventory real coverage |
| NCM frozen proposal | `/root/ncm_proposal_scout` | read-only | supports reproduce | running | Audit hypothesis only |
| NCM budgets | `/root/ncm_budgets_scout` | read-only | supports verification | running | Freeze performance gates |
| Semantic query authority | `/root/semantic_authority_scout` | read-only | supports diagnose | running | Trace authority decision |
| Semantic acquisition | `/root/semantic_acquisition_scout` | read-only | supports diagnose | running | Trace artifact acquisition |
| Semantic projection | `/root/semantic_projection_scout` | read-only | supports diagnose | running | Trace generation projection |
| Semantic calibration | `/root/semantic_calibration_scout` | read-only | supports diagnose | running | Trace calibration transition |
| Semantic artifact store | `/root/semantic_artifact_scout` | read-only | supports diagnose | running | Trace storage/recovery |
| Semantic retrieval | `/root/semantic_retrieval_scout` | read-only | supports matrix | running | Map serving evidence |
| Semantic tests | `/root/semantic_tests_scout` | read-only | supports matrix | running | Inventory coverage gaps |
| Semantic setup/config | `/root/semantic_setup_scout` | read-only | supports diagnose | running | Map provisioning contract |
| Build/check map | `/root/build_check_scout` | read-only | supports readiness | running | Freeze commands/features |
| Conflict map checker | `/root/conflict_map_checker` | read-only | supports wave 2 | running | Challenge ownership split |
| Acceptance checker | `/root/acceptance_checker` | read-only | supports matrix | running | Challenge completeness |
| Test isolation | `/root/test_isolation_scout` | read-only | supports verification | running | Map safe profiles/sockets |
| Repeat campaign | `/root/repeat_campaign_scout` | read-only | supports release | complete | Feed sealed-ledger contract to matrix |
| Historical NCM workload | `/root/ncm_history_scout` | read-only | supports reproduce | running | Recover exact 2/4 workload |
| Semantic smoke receipts | `/root/semantic_smoke_receipt_scout` | read-only | supports diagnose | running | Correlate observed timeline |
| Native operation-map checker | `/root/native_operation_map_checker` | read-only | supports matrix | complete | Corrected 570 public/internal routes and missing operations |
| Pilot setup | `/root/pilot_setup_scout` | read-only | supports release | complete | Mapped isolated profile, readiness, and rollback contract |
