# Retrieval-anchor current-baseline proof

Date: 2026-09-13

Active original reference: `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`.
Candidate reviewed for this gate: `1fe250fed7ca615f1dfdcd580befeb276328e910`.
Active worktree: `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`.

Root supplied the read-only comparison result:

```text
git diff --exit-code 57006f60cb45bcee8487e73a40d4fad1a12ee2b6 1fe250fed7ca615f1dfdcd580befeb276328e910 -- crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs
exit: 0
```

Both revisions resolve `crates/tracedecay-runtime-core/src/db/retrieval_anchor_authority.rs` to blob `d976e2a23c19441a60412420a1db5e68f7ccc5ca`. The active source gate is therefore complete with no source edit. This proves source equality for this file only; it does not establish runtime parity or complete Native restoration. The older b3 restore-anchor report remains historical.
