---
name: rn-build-reference
overview: "Build untouched original Native while product work runs. Reuse the result for final differential verification."
todos:
  - id: rn-build-reference-done
    content: "Build untouched original Native while product work runs and publish the verified reference artifacts."
    status: pending
isProject: false
---

# Build untouched original Native while product work runs

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md. Worktree for orchestration: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6. Use exact workdir for every command.

Model gpt-5.6-luna, reasoning max, fork_turns=none, no child agents. You are not alone; preserve peer files. Prerequisite: rn-source-docs accepted by root and the root-reviewed reference-build requirements below. Runner completion is not required to compile the independently known original binary. Mode: build owner; original source read-only. Read ../evidence/source-baseline.md and ../evidence/verification.md plus ../execution-results/reference-build-requirements.md.

Reference checkout: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`. Create it only if absent; if it exists, verify its HEAD and clean source rather than resetting or deleting it. Reference process/data roots remain test-owned and separate from operator data.

## Ownership and Constraints

- A clean detached 570 worktree under the repository .worktrees/ and its repo-local build/test artifacts
- target/task-scratch/native-original/reference-build/ (logs and binary inventory only)

Original/product source, manifests and runtime user data are read-only. This node and rn-build share one named Cargo execution owner; no other lane submits builds. Do not patch the original checkout to make it compatible with the comparison. No pushes, merges, installs, global settings or operator database access.

## Steps

1. Act as the same designated Cargo owner later reused for rn-build. Read cargo-hauler instructions and attach to any matching broker ticket before scheduling work.
2. Create or reuse the detached 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 reference checkout without modifying its source, manifests or generated files. Confirm source identity and the runner's exact CLI/MCP/feature needs.
3. Build the actual original runtime with the checkout's repo-local target and test-profile data, while disjoint product implementation and fixture authors continue. Use /fast fallback only for observed target contention, never arbitrary duplicate targets.
4. Use fresh diagnostics before checks or diagnose captured build output. Keep complete ticket/log evidence; stop a genuinely stuck build only by broker ticket, never by PID.
5. Publish original binary digests, source/feature identity, commands and test-owned process prerequisites. Do not run product comparisons or label original help output as behavioral parity.

## Acceptance Checklist

- Reference source and original manifests remain exactly at 570.
- The actual reference binary is built and its identity/features are usable by rn-reference.
- One designated broker owner retains the ticket/artifacts for rn-build without duplicate compilation.

## Operator Guidance

Launch after the original binary requirements are reviewed, concurrently with disjoint implementation/fixture work. Root reviews artifact identity before rn-build reuses it. Return node ID, exact commands/tickets, binary/source/feature identity, actual result and blockers. Root alone updates status.

Stop condition: Return the original build result and inventory. A missing public route or incompatible build environment is a concrete blocker, never replaced with mock output or a modified reference.


## Reviewed build requirements

Read ../execution-results/reference-build-requirements.md. Build package tracedecay-cli, binary tracedecay, default production features plus test-transport. Do not enable product-only memory-provider-host on b3. Do not execute comparisons until rn-reference is accepted. No invented flags or original source modifications.

## Current dependency contract

Prerequisites: rn-source-docs. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
