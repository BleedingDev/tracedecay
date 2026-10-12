# Native, NCM and semantic execution extension

This document is the execution-local handoff for the exact plan files in this
directory. The parent-level [ORCHESTRATION.md](../ORCHESTRATION.md) remains the
baseline for the Native restoration graph, reference `57006f60cb45bcee8487e73a40d4fad1a12ee2b`,
and one-branch/worktree policy.

## Canonical targeting

- Worktree: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`
- Branch: `feat/pluggable-memory-providers-v2`
- Graph ID: `native-original-execution-pr707-20260913`
- Selection: `*.plan.md` under this directory, with the two historical broad plans excluded by `execution-selection.json`
- Excluded historical plans: `rn-ncm-recall-fix`, `rn-semantic-fix`
- Active plan count after this extension: 47
- Active dependency edge count after this extension: 114
- Current plan-set hash: `38902296e5`
- Current selection hash: `789c92dd6f`
- State snapshots are refreshed only by the root operator after review. Temporary validation must pass `--no-write-state` and an isolated `--state-dir` to `graph.py`.

The excluded plans remain in the checkout as historical context. Their active edges are replaced by explicit edges to the exact NCM and semantic nodes listed below; no plan history is deleted or silently treated as completed.

## Wave structure and critical paths

The first extension wave is released only when its existing predecessors are
accepted:

| Node | Role | Required predecessor | Output that releases the next node |
| --- | --- | --- | --- |
| `rn-ncm-byte-budget-fix` | Core code fix | acceptance matrix; recovered NCM report is an input | failing-before/passing-after UTF-8 budget regression |
| `rn-ncm-stage-trace` | Real-worker diagnosis | acceptance matrix; recovered NCM report is an input | first-loss stage receipt and exact repair seam |
| `rn-provider-semantics` | Provider adapter code/contract gate | acceptance matrix, contract | scoped provider semantic diff and focused test handoff |
| `rn-semantic-runtime-dynamic` | Real runtime diagnosis | acceptance matrix; static semantic diagnosis is an input | unavailable-to-ready transition and disjoint fix routing |

The second wave is intentionally gated:

- `rn-ncm-real-fix` waits for `rn-ncm-stage-trace`.
- `rn-ncm-proof-retry` waits for the acceptance matrix and the serialized
  `observation_journey.rs` handoff from `rn-session-delivery`; it consumes the
  partial NCM reproduction report without waiting on its separate trace todo.
- `rn-semantic-acquisition-fix` and `rn-semantic-serving-fix` both wait for
  `rn-semantic-runtime-dynamic` and own disjoint source families.
- `rn-privacy-trace-id` waits for `rn-host-context` and `rn-registry`; it is a
  hostile-ID acceptance gate with no overlapping production source. It gates
  `rn-privacy-restore`.

The NCM critical path is:

```text
acceptance + reproduce
  ├─> byte-budget-fix ─┐
  ├─> stage-trace ─────┼─> ncm-real-fix ─┐
  └─> proof-retry ─────┘                ├─> ncm-tests ─> build ─> verify-ncm
provider-semantics ─────────────────────┘                 └─────> 100 trials
```

`rn-ncm-proof-retry` also has the explicit serialized delivery edge required by
its hot observation-journey source file. Every NCM code-fix node has an active
edge to focused NCM tests and a path to the candidate build and the 100-trial
`rn-verify-ncm` campaign; a single favorable run cannot release close.

The semantic critical path is:

```text
semantic-diagnose ─> semantic-runtime-dynamic
                     ├─> semantic-acquisition-fix ─┐
                     └─> semantic-serving-fix ─────┼─> build ─> verify-semantic
                                                   └──────────> reviews
```

Dynamic runtime evidence precedes both fixes and semantic verification. The
semantic fixes preserve separate acquisition and serving ownership and each
has direct build and verification edges.

## Ownership and conflict map

| Surface | Exclusive owner or ordered handoff |
| --- | --- |
| NCM core ranked text-byte admission | `rn-ncm-byte-budget-fix` |
| NCM provider common/source binding | `rn-provider-semantics` |
| NCM runtime/worker real-loss candidates | `rn-ncm-real-fix`, narrowed by the accepted stage trace |
| NCM instance-proof cache in `observation_journey.rs` | `rn-session-delivery` first, then exclusive handoff to `rn-ncm-proof-retry` |
| NCM reproduction tests and reproduction report | existing `rn-ncm-reproduce` |
| Broader NCM preservation tests | existing `rn-ncm-tests` after all code-fix predecessors |
| Persisted explain-trace privacy acceptance | `rn-privacy-trace-id` owns only its report/fixtures; source fixes remain with `rn-host-context` and `rn-registry` |
| Semantic acquisition/projection/calibration lifecycle | `rn-semantic-acquisition-fix` |
| Semantic code-index/query serving | `rn-semantic-serving-fix` |
| Semantic dynamic and final verification artifacts | `rn-semantic-runtime-dynamic`, then existing `rn-verify-semantic` |

No two write-capable nodes run concurrently on a shared file. Read-only trace
and dynamic-evidence nodes may inspect a source surface, but they do not edit it.
The build owner remains the sole Cargo submitter; all nodes use the existing
branch/worktree and Codex-native Luna Max agents.

## Root merge protocol

Root reviews each node's exact diff, evidence, ownership and actual execution
counts before marking its todo complete. After each accepted checkpoint, root
updates the active graph state and pushes the same branch. Generated
`execution-dag.json`, `execution-frontier.json`, `execution-summary.json`,
`execution-handoff.json`, `execution-graph.mmd` and validation snapshots are
regenerated only by root after this extension has been reviewed.
