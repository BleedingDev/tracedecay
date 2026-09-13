# Live operator ledger

## Canonical handoff bundle

- Goal: complete Native parity, repair intermittent NCM recall, repair semantic search, and prove release readiness on `feat/pluggable-memory-providers-v2`.
- Selection: `--plans-root /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution --glob '*.plan.md'`.
- Explicit dependency edges: the 88 `--depends` entries in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/execution-selection.json`; no extra edge overlay.
- Graph ID: `native-original-execution-pr707-20260913`.
- Selection hash: `1752917f10`.
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
| rn-acceptance-matrix | pending | `execution-results/readiness-matrix.{md,json}` only | ready | launching | Publish frozen matrix |
| rn-integration-readiness | pending | `execution-results/pr707-integration-readiness.md` only | ready | launching | Record current gate |
| Native facts | pending | read-only | supports matrix | launching | Map routes/oracles |
| Native trust/feedback | pending | read-only | supports matrix | launching | Map routes/oracles |
| Native retrieval telemetry | pending | read-only | supports matrix | launching | Map effect distinctions |
| Native sessions | pending | read-only | supports matrix | launching | Map routes/oracles |
| Native LCM | pending | read-only | supports matrix | launching | Map routes/oracles |
| Native persistence | pending | read-only | supports matrix | launching | Map restart/state cases |
| Native lifecycle/background | pending | read-only | supports matrix | launching | Map responsibilities |
| Native Codex delivery | pending | read-only | supports matrix | launching | Map host contract |
| Native Claude contract | pending | read-only | supports matrix | launching | Map shipped hook contract |
| Native privacy | pending | read-only | supports matrix | launching | Map isolation cases |
| Native reference runner | pending | read-only | supports rn-reference | launching | Map immutable runner inputs |
| Native 570 diff | pending | read-only | supports rn-source-docs | launching | Map remaining source delta |
| NCM admission/journal | pending | read-only | supports reproduce | launching | Trace first pipeline segment |
| NCM worker pipeline | pending | read-only | supports reproduce | launching | Trace worker segment |
| NCM selection/exclusions | pending | read-only | supports reproduce | launching | Trace ranking/filter segment |
| NCM replay/recovery | pending | read-only | supports reproduce | launching | Trace persistence segment |
| NCM scopes | pending | read-only | supports matrix | launching | Map seven bindings |
| NCM cancellation/deletion | pending | read-only | supports matrix | launching | Map lifecycle cases |
| NCM tests | pending | read-only | supports matrix | launching | Inventory real coverage |
| NCM frozen proposal | pending | read-only | supports reproduce | launching | Audit hypothesis only |
| NCM budgets | pending | read-only | supports verification | launching | Freeze performance gates |
| Semantic query authority | pending | read-only | supports diagnose | launching | Trace authority decision |
| Semantic acquisition | pending | read-only | supports diagnose | launching | Trace artifact acquisition |
| Semantic projection | pending | read-only | supports diagnose | launching | Trace generation projection |
| Semantic calibration | pending | read-only | supports diagnose | launching | Trace calibration transition |
| Semantic artifact store | pending | read-only | supports diagnose | launching | Trace storage/recovery |
| Semantic retrieval | pending | read-only | supports matrix | launching | Map serving evidence |
| Semantic tests | pending | read-only | supports matrix | launching | Inventory coverage gaps |
| Semantic setup/config | pending | read-only | supports diagnose | launching | Map provisioning contract |
| Build/check map | pending | read-only | supports readiness | launching | Freeze commands/features |
| Conflict map checker | pending | read-only | supports wave 2 | launching | Challenge ownership split |
| Acceptance checker | pending | read-only | supports matrix | launching | Challenge completeness |
| Test isolation | pending | read-only | supports verification | launching | Map safe profiles/sockets |
| Repeat campaign | pending | read-only | supports release | launching | Specify retained evidence |
