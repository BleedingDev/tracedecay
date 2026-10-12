# Rules for executing the original Native restoration plan

This file is shared context for future implementation assignments. It does not start implementation. Read the assigned `.plan.md`, its exact handoff, and the reviewed decisions before editing.

## Active PR707 reassessment context

The active branch/worktree is `feat/pluggable-memory-providers-v2` at `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`, with unmodified upstream reference `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` and candidate metadata `1fe250fed7ca615f1dfdcd580befeb276328e910`. The b3/571 audit and b3 reference checkout remain historical. Keep one continuous focus on this worktree; do not create a recovery branch or relabel historical evidence.

## Model and roles

Every execution, test and review subagent uses `gpt-5.6-luna` with `reasoning_effort=max` and a fresh, bounded handoff. Root only orchestrates, reviews evidence and diffs, resolves conflicts and updates the graph. Root does not fill implementation gaps by editing product code. A named execution owner performs each approved correction and integration edit. Agents are leaves: do not spawn more agents.

## The result we owe the user

Native means the complete original memory behavior at the reviewed original source revision. NCM is the separate alternative. There is no Native Mod product option in this work. Reusing an unchanged fact crate, keeping old code present, returning matching fixture facts, or giving a changed backend the Native name is not enough.

Preserve original operations at matching boundaries. An original operation that changes retrieval tracking, trust, feedback, state or maintenance must keep those effects. An original operation that is read-only must stay read-only. Do not add effects to all queries or suppress all effects by assumption. Use the reviewed operation map.

The original memory algorithms and their storage/ownership rules are protected. Our adapter and modular core must fit them. A missing common feature is an explicit feature difference, never a new Native implementation, an empty successful reply or silently substituted behavior. Host cancellation and privacy controls must remain correct without inventing different Native internals.

Read SOURCE-BOUNDARY.md. The active rn-restore-anchor node is a completed read-only 570/candidate equality gate; it does not authorize an original-file write. All original implementation code remains protected. Shared host extensions already present at the audited product head must remain intact. The build owner is shared by rn-build-reference and rn-build; both nodes may submit their assigned broker work.

## Before editing

1. Use the exact execution worktree from the handoff as `workdir` on every shell command. The inherited shell directory may be the primary checkout, which is a different branch. Confirm `git rev-parse HEAD` in the assigned worktree before reading or editing relative paths.
2. Read the complete assigned handoff and named evidence. Confirm all dependency outputs exist and root has accepted them. An active upstream agent is not a completed dependency.
3. Check the current working tree and re-read every owned file. You are not alone in the codebase. Do not revert, overwrite, reformat or stage peer work.
4. Confirm the protected original source revision and the allowed integration paths in the reviewed plan. Do not touch original-owned code to satisfy our interface.
5. Use the existing graph and symbols to find helpers. Do not invent a parallel store, registry, parser, scorer, transport or authority when the existing implementation supplies it.
6. If your exact files overlap another active owner, stop before writing and report the specific file and required order. Do not negotiate a shared write behind root's back.

## During work

- Make only the assigned outcome. Do not turn a missing input, test failure, old comment or nearby TODO into new scope.
- Each shared protocol, generated output, central registry, Cargo manifest and lockfile has one owner. Send proposed changes to that owner through root.
- A dependent agent may prepare its own disjoint fixtures or read source early. It must not guess an unfinished interface or edit its callers before the dependency is accepted.
- Keep Native's original source and behavior. Do not tune ranking, retention, scope, learning, timers, thresholds or budgets to make tests pass.
- Keep NCM's model, algorithms, learned state format and namespace identity. This plan does not include tuning its earlier intermittent recall result.
- Do not silently copy added staged observations into original Native facts or erase old memory. Follow the reviewed data cutover exactly. Never inspect or mutate the operator's real databases in tests.
- Keep one canonical contract generator. Change branch-local contracts in place unless the reviewed release/use evidence requires compatibility. Do not hand-edit generated outputs or invent a new protocol version as a shortcut.
- Fix a real integration mismatch at its proper boundary. Do not add silent fallback, fabricated IDs/timestamps, empty successes, disabled assertions, ignored tests, hidden modes, fake providers, unfinished flags or user-facing approval gates.
- No broad formatting, cleanup, dependency updates, branch moves, merges or pushes. Commit only when root assigns the exact coherent owned paths after review.

## Verification and build coordination

Verification must show behavior. Compare original Native with modular Native at the same entry point, with equivalent inputs and separately isolated equivalent state. Include original side effects and failures. Tests against two routes that share the same changed backend cannot by themselves establish original parity. Source preservation checks support the user's unchanged-code constraint; they do not replace behavior tests.

Each lane lists meaningful checks and must report which actually ran. A passing command that executes zero relevant tests is not evidence. Use the repository's existing nonvacuous test helper when running exact filters. Keep existing relevant assertions intact. Do not manufacture a fixed test count or a large matrix without a behavioral reason.

Only the build owner schedules Rust builds and shared Cargo tests. Before Cargo, use `hauler status --session native-original-execution` and attach to a matching in-flight ticket. Run fresh TraceDecay diagnostics before compiler checks, or send captured errors to the diagnose tool. Share resulting tickets with dependent test agents. Do not kill a Cargo PID; only use the broker's named ticket operation when its documented stalled condition applies.

Use the checkout's repo-local `target/` by default. On proven target lock contention follow the user's AGENTS.md fallback under `/fast/cargo-target/<worktree-name>`; test data follows the selected target. Never put Cargo targets under `/tmp` or the home/root disk. Do not create extra target directories merely to multiply builds. Keep CLI commands at repository root and prefer affected packages. Reuse logs and completed artifacts rather than rerunning a broad suite to inspect a failure.

A failure outside your ownership is a handoff: report the exact error, file, command/ticket and owner needed. Continue independent authorized work. Root will assign a bounded fix. Do not weaken a test or patch an unrelated module to get a green result.

## Required return to root

- Assigned node ID and the exact outcome completed.
- Changed paths, with a short reason for each. Call out every generated or shared file.
- Verification commands and broker tickets, actual results, and checks not run.
- Proof that protected original behavior relevant to your node is preserved.
- Remaining uncertainty, failing case or dependency, with a specific next action.
- A reviewable diff. Do not call the whole task complete because your lane passes.

Stop once the assigned outcome and checks are complete. Root reviews scope and evidence before marking the graph node complete or releasing dependent writers.

## Historical privacy scope and active gate

The earlier b3-to-571 detector restoration and typed Claude history admission record remain historical. Under active 570, detector equality is already proven; rn-privacy-audit and rn-privacy-restore review the typed history seam without another detector write. Generic admission, original Native and LCM privacy stay unchanged. `observation_journey.rs` remains ordered behind the active privacy gate before rn-session-delivery.

## Readiness extension and host precedence

Read READINESS-PLAN.md. Its NCM causal-repair and semantic lifecycle/setup nodes are the precise exceptions to older blanket preservation wording. All other protected code remains protected. Current AGENTS.md takes precedence over historical model-routing/host instructions: remain in Codex, use available native models/tools, and never launch another agent CLI without an explicit user request. Plan authoring starts no workers. Later implementation uses one branch, one designated Cargo owner, scoped assignments and reviewed commit/push checkpoints.
