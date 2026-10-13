# Start here — Biomem-based Rust NCM

## Coding-session assignment

Implement a real, independently usable Rust NCM backend inside `BleedingDev/tracedecay`, using Biomem commit `500847ff65b5d9548b3826fa29bf3ccf8d221147` as the executable reference and the two identified Michal Seidl/OpenTechLab publications as research references. Start from product checkpoint `25778c7443cd0cfe257da363da01a56ea1d45d3f` on an isolated worktree/branch, suggested name `feat/ncm-biomem-rust-v1`.

This request supersedes the earlier decision to defer the Rust port. It does **not** authorize bypassing the other session's host-retention, CI, ownership, or acceptance repairs. Preserve the existing provider API, fabric, registry, NCM adapter protections and adversarial test doubles. Replace the absent production implementation with real learned state, real native model inference, durable effects and verified recovery—not a canned provider or Python wrapper.

Read `IMPLEMENTATION_DAG.md`, `DESIGN_CONTRACT.md`, then the current task file under `tasks/`. Use `task_dag.json` as the dependency/ownership interchange format. It is **not** a direct Beads import schema or a second authoritative tracker. The coordinator maps these planning IDs into new Beads records through the existing operation process; retain historical descoped closures.

## What may proceed now

Tasks **001–022** have no dependency on the other session's fixes. They produce a standalone Rust core, runtime/worker, real adapter, conformance/evaluation evidence and backend acceptance. They still depend on their own listed prerequisites and required local model/tooling resources.

Tasks **023–026** join the repaired host and have explicit external gates. They must not be represented as complete based on standalone backend results.

```bash
# Validate this planning package; no repository writes.
python3 validate_dag.py

# Initially only 001 is ready.
python3 validate_dag.py --ready

# Example after the foundation really has been verified:
python3 validate_dag.py \
  --completed ncm-rs-001,ncm-rs-002,ncm-rs-003,ncm-rs-004 \
  --ready
```

The last command should select 005, 008, 012 and 013: projections, terrain, real embeddings and durable storage. Pass `--satisfied-gates` only for externally supplied, reviewed evidence; this script checks graph consistency, not whether evidence is authentic or tests passed.

## Ownership and concurrency

Use a separate branch/worktree, disposable state roots and isolated profiles. Never run against the operator's installed daemon/profile. Do not reset another session's checkout, installed CLI, caches or tests.

The NCM coordinator alone edits **this branch's** `Cargo.toml`, `Cargo.lock`, central `lib.rs` declarations and task import metadata. Each worker edits only its node's owned paths. Consult the repository's current cargo contention policy; use its broker/cache rather than spawning competing unrestricted workspace builds or wiping caches.

Before join task 023, do not change:

- `crates/tracedecay-code-index-retention/**`, `crates/tracedecay-code-index-runtime/**`, `crates/tracedecay/src/daemon/store_maintenance/**`, or the retention fixture.
- Existing architecture checker/tests, current upstream convergence workflow, accepted upstream floor or existing host recall/observation composition.
- Shared `.beads/issues.jsonl` on the product branch or other workers' task status.

The two new packages and existing NCM adapter are independent ownership domains. Root manifest/lock changes are an explicit coordinator-owned exception on the isolated branch; their eventual merge is task 023. Proposed host module paths in 024/025 are **proposals**, not claims about existing layout: discover the concrete mount on the stabilized tree and obtain its owner's approval before editing it.

Parallelize only ready nodes with non-overlapping files. Read-only review may proceed in parallel; code changes to the same files require an ordered handoff. The validator checks the declared path prefixes, not undeclared imports, runtime resource contention or actual diffs.

## Definition of done for every node

A task is done only when its implementation and negative controls execute under its actual declared path. Source-marker checks alone do not establish runtime behavior. A stub compiling is not a working capability.

Attach this receipt:

```json
{
  "task_id": "ncm-rs-NNN",
  "source_commit": "actual commit",
  "source_tree": "actual tree",
  "allowed_path_diff": ["actual changed paths"],
  "implementation_summary": "what now works",
  "algorithm_config_identity": "actual version and digest, or not_applicable",
  "model_projection_identity": "actual identities, or not_applicable",
  "exact_commands": ["commands actually executed"],
  "selected_test_ids": ["actual executed test identifiers"],
  "pass_fail_skip_ignored_counts": {},
  "negative_controls": ["mutants/faults that the tests detect"],
  "binary_identity_when_applicable": "actual hash/features",
  "environment_and_resources": {},
  "limitations": [],
  "reviewer_verdict": "pass | fail | blocked"
}
```

This is a schema example, not a valid receipt with substitute values. No placeholder identity is accepted on completion. Discover test IDs before running filtered tests; assert the complete expected set and fail empty selection. Proposed new test names in this package must be created by their owning task and bound to executable commands. A skipped real-model test cannot make a release gate pass.

A reference discrepancy is resolved by a small red test and an approved entry in `deviations.json`. Do not enlarge float tolerances, weaken privacy requirements, remove contradictory tests, or broaden provider capabilities to make a task appear green.

## Deliverables at the first stopping point

At 022: a real Rust worker and adapter that can observe text, consolidate, restart, recall from LTM and delete a source, plus complete backend evidence. Hand the integration owner the exact branch/tree and narrow shared-file patch. Do not mount it into the product prematurely.

At 026: installed-product evidence for an explicitly opt-in active provider, with the separate scientific-fidelity, runtime-safety, platform and usefulness verdicts. Failure of any required verdict leaves that release blocked; it does not invalidate completed isolated kernel work.

## Evidence boundary of this plan

The planner inspected pinned source excerpts, primary publication records/catalog, a primary supplementary document and the model card. Original paper PDFs could not be retrieved successfully, so their figures, equations and full experimental claims were **not** verified. Reference code and Rust backend tests were **not** run by the planner. Task 001/002 closes those source/reference gaps. The planning DAG itself was programmatically validated.
