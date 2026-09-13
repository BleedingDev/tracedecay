---
name: rn-verify-claude
overview: "Verify original Native through real Claude delivery. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-verify-claude-done
    content: "Verify original Native through real Claude delivery and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify original Native through real Claude delivery

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: verification-only.
Prerequisites: rn-build. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: host-delivery.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/claude/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Use accepted real Claude journey fixtures and separately built original/product binaries. Keep host configuration/task/source equivalent and isolate all owned processes/state.
2. Execute actual session hooks/ingestion and one scheduled tracedecay_context action per case. Preserve exact final UTF-8 delivery and typed provenance.
3. Check original Native context is delivered once with original semantics, no staged candidates, no extra Native recall and truthful invocation evidence.
4. Run the scoped refusal/failure and restart cases. Inspect actual installed/rendered host configuration rather than source scaffolds.
5. Repeat a nondeterministic failing case when needed to establish stability; retain every outcome and cleanup evidence. Do not change policy or expectations for a pass.

## Acceptance Checklist

- The model-visible Native contribution matches original behavior at the same real host boundary.
- No duplicated Native delivery or staged result is present.
- Results are reproducible/stable enough for the claim; unknown or intermittent outcomes remain visible.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return captured host evidence and focused failures. Do not install globally, alter real user settings or publish artifacts.

## Current dependency contract

Prerequisites: rn-build. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.

## Current host constraint

Exercise the shipped Claude lifecycle hook handlers and model-visible payload contract using existing production fixtures driven from Codex. Do not launch Claude Code or another agent CLI. This verifies the shipped host contract; label a real external Claude application interaction as separate manual coverage, not as an automated pass. Current AGENTS.md overrides older cross-host execution wording.
