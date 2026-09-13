---
name: rn-restore-anchor
overview: "Revalidate retrieval-anchor source equality against the unmodified PR707 baseline; preserve the completed no-op result when the blobs match."
todos:
  - id: rn-restore-anchor-done
    content: "Record current 570-to-candidate diff and blob proof for retrieval-anchor authority; current equality proof is complete."
    status: completed
isProject: false
---

# Revalidate retrieval-anchor equality against upstream 570

## Execution Notes

Read ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md and ../SOURCE-BOUNDARY.md. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Active original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; execution candidate: 1fe250fed7ca615f1dfdcd580befeb276328e910. The b3-to-571 restoration result is historical. This is a read-only gate; no Cargo, source edits or child agents.

## Ownership and Constraints

Own only .codex/plans/native-original/execution-results/restore-anchor-57006f60.md. Read the current crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs blob without changing it. Do not resurrect the old b3 method lane, add an optimization, or overwrite the historical restore-anchor.md report.

## Steps

1. From the active worktree, run git diff --exit-code 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 1fe250fed7ca615f1dfdcd580befeb276328e910 -- crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs and record the actual exit code.
2. Record git rev-parse 57006f60cb45bcee8487e73a40d4fad1a12ee2b6:crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs and the candidate blob. The expected shared blob is d976e2a23c19441a60412420a1db5e68f7ccc5ca.
3. Root has supplied an empty diff and matching blobs. Preserve that current proof in the owned result; if a later candidate differs, stop and return the exact diff and a new owner request to root. Do not edit the source in this gate.
4. Keep ordinary/missing/invalid-history behavior in later external cases; source equality is necessary evidence, not a runtime parity claim.

## Acceptance Checklist

- The active result contains current commands, exit codes and both blob IDs.
- Equality with upstream 570 is recorded and this node is complete for the current candidate.
- No historical b3 acceptance is relabeled as current and no code file is changed.

## Operator Guidance

Root has reviewed the current equality proof; this node is complete for the active 570 baseline. Root retains status ownership and must reopen it only if a later candidate changes the blob.
