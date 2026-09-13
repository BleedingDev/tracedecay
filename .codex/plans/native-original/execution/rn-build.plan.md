---
name: rn-build
overview: "Integrate shared manifests and verify both actual runtimes. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-build-done
    content: "Integrate shared manifests and verify both actual runtimes and return the required reviewable evidence."
    status: pending
isProject: false
---

# Integrate shared manifests and verify both actual runtimes

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable and build owner.
Prerequisites: rn-privacy-audit, rn-map-checker, rn-composition, rn-fabric, rn-host-fixtures, rn-ncm-tests, rn-reference, rn-build-reference, rn-restore-anchor, rn-integration-readiness, rn-history-owner-followup. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910. Reuse the same designated build execution agent and verified reference artifacts from rn-build-reference; rebuild the reference only if its recorded inputs changed.

Read these reports under ../evidence/: verification.md, source-baseline.md, ncm-boundary.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

Reference checkout: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/native-original-reference-57006f60`. Create it only if absent; if it exists, verify its HEAD and clean source rather than resetting or deleting it. Reference process/data roots remain test-owned and separate from operator data.

## Ownership and Constraints

Write only:

- Cargo.toml and Cargo.lock
- Cargo.toml in the product provider/API/fabric/registry/CLI/application crates only when a reviewed dependency is required
- Test-owned build logs and binary inventory under target/task-scratch/native-original/candidate-build/

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Collect reviewed dependency requests; apply only necessary product manifest/lock changes. Do not change original engine manifests or upgrade dependencies. All other source fixes go back to their named execution owners.
2. Read the cargo-hauler skill. Check hauler status --session native-original-execution before each Cargo submission and attach to matching in-flight work. Use fresh TraceDecay diagnostics before compiler checks; diagnose captured failures. Never kill Cargo by PID.
3. Verify and reuse the clean detached 570 reference worktree and artifacts from rn-build-reference. Build the restored product with the actual required features, repo-local target/test-profile defaults and isolated state. Use /fast fallback only on proven target contention. Do not repeat the original build without changed inputs.
4. Run focused affected package tests, real nonvacuous filters and required repository checks. Preserve full error logs/tickets and actual executed-test evidence. Run the required aggregate check once the candidate is coherent; broaden only for unresolved concerns.
5. Publish tested binary paths/digests, exact features, revisions, tickets and commands for the parallel verification nodes. Reference and product must be independently built; no relabelled same executable.
6. If a correction changes a public seam, root re-releases affected dependent owners and required checks. Do not locally patch their code or weaken assertions.

Required source verification: consume `product/architecture/native-original-source-inventory.md` and ../SOURCE-BOUNDARY.md. Compare the final candidate to 570 across the complete protected surface and to the audited product baseline for retained host extensions. The retrieval-anchor file must equal the active 570 blob exactly. Every remaining hunk needs its reviewed classification; new original algorithm/authority/schema/lifecycle changes fail the check.

## Acceptance Checklist

- Both immutable reference and candidate binaries correspond to the recorded sources and required features.
- Affected and required aggregate checks pass with relevant tests actually executed.
- Protected original code remains unchanged and workspace Cargo paths remain portable.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return tested artifacts and actual results. Failed builds remain incomplete; root assigns bounded fixes to owners. No push, merge, release or live settings/data action.
