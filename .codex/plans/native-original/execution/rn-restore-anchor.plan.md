---
name: rn-restore-anchor
overview: "Restore the pre-existing retrieval-anchor optimization to the exact b3 implementation without changing other original or shared host code."
todos:
  - id: rn-restore-anchor-done
    content: "Restore the original full-history latest-disposition implementation and return exact source-equality evidence."
    status: completed
isProject: false
---

# Restore the original retrieval-anchor authority exactly

## Execution Notes

Read ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md and ../SOURCE-BOUNDARY.md. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2; set it explicitly for every command. Reference: b3b43410e47115056f2066449aafa1822bbb6049. Prerequisite: root has accepted rn-source-docs and its exact source inventory.

Model gpt-5.6-luna, reasoning max, fork_turns=none, no child agents. You are not alone: preserve peer edits. This is a narrowly authorized restoration of original bytes, not permission to improve original code.

## Ownership and Constraints

Own only `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs`, and only its pre-existing b3-to-head method difference. No new helpers, optimized query, tests inside original code, manifests or storage changes. No Cargo; rn-build owns scheduling.

## Steps

1. Re-read the file and compare it to b3. Confirm the sole difference is the current latest-row SQL implementation around the retrieval disposition store method. If additional changes exist, stop and send the exact diff to root.
2. Restore that method to the exact b3 source: call `self.retrieval_anchor_disposition_history(owner, anchor_id)`, await it, select the last returned record and map the original store error. Do not emulate the behavior with another implementation.
3. Verify the entire owned file now matches b3. Preserve original tests; send rn-build the exact restoration and useful existing test filters. Facts fixtures derive their expected anchor behavior independently from b3 and SOURCE-BOUNDARY.md; they do not consume this node's implementation output.
4. Return the exact restoration diff, source-equality command/result and build needs. Leave shared directory interruption, Codex, privacy and cursor improvements untouched.

## Acceptance Checklist

- The owned file is byte-identical to b3 after restoration.
- No original source was changed beyond restoring this identified difference.
- External comparison coverage includes original latest/history outcomes, including no-record and relevant invalid-history behavior where the original boundary exposes them.
- A performance difference is reported honestly, never fixed by reintroducing a different original implementation.

## Operator Guidance

This node can run alongside disjoint contract, fixture and host work after the source inventory is reviewed. The product build depends on its accepted result. Root reviews the restoration before accepting it.

Return node ID, exact diff, source comparison, relevant test filters and any unexpected divergence. Stop when the exact restoration is complete; do not expand to other original files or tune NCM.
