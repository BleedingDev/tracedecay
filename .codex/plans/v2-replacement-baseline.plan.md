---
name: V2 replacement baseline and PR 707 reconciliation
overview: Reconcile the single pluggable-memory branch with the exact latest reviewed PR #707 tree, preserve the Native and NCM additions that remain product requirements, remove superseded architecture, and restore a buildable V2 baseline before feature work continues.
todos:
  - id: quiesce-current-writers
    content: "Finish, freeze, or explicitly stop every active writer to the shared worktree, account for Cargo Hauler readers and queued tests, record exact file ownership, and refuse cleanup while any change remains unclassified or agent-owned."
    status: pending
  - id: checkpoint-accounted-work
    content: "Create recoverable exact-path commits on feat/pluggable-memory-providers-v2 for every coherent current change, including useful superseded work before its later deletion, review each diff, and push each checkpoint so no merge or cleanup can erase the only copy."
    status: pending
  - id: define-replacement-contract
    content: "Pin the installed V1 version, required platforms and install methods, supported user workflows, intentional removals, and the full latest-#707 product scope including hosts and feedback, dashboard and settings, Work and workflow runtime, public SDKs, memory, and code retrieval; map each item to a current owner and future direct journey."
    status: pending
  - id: define-data-continuity
    content: "Inventory which V1 user data can be reconstructed from ordinary repositories and host transcripts and which V1-only facts, corrections, feedback, configuration, or post-cutover writes cannot cross the fresh-store boundary; make those consequences explicit before integration."
    status: pending
  - id: classify-current-working-tree
    content: "Inventory every tracked and untracked change against HEAD 613d9d4ffd961eca0211903a684f1ba5850242ae and classify it as retain and port, already superseded, or disposable artifact using the latest V2 roadmap as authority."
    status: pending
  - id: checkpoint-retained-work
    content: "Using the recoverable checkpoints, retain and port Native, NCM, provider-contract, persistence, privacy, and justified production-receipt slices, then deliberately delete dense code-search and PR-specific gate-runner machinery that the roadmap rejects; commit and push the resulting clean merge input."
    status: pending
  - id: repin-pr707-head
    content: "Re-query PR #707 immediately before integration, record its exact head, merge base, draft and CI state, and redo the delta classification if it moved from 4f28d6fa95377a4ee305a7c7049c725c84c461f1."
    status: pending
  - id: build-ownership-map
    content: "Map each retained provider capability from the old root layout to the latest owners in tracedecay-project, tracedecay-daemon-service, tracedecay-mcp, tracedecay-mcp-catalog, application, SDK, dashboard, and release packaging; identify every deletion-versus-port decision before resolving conflicts."
    status: pending
  - id: merge-pr707-same-branch
    content: "Merge the pinned PR #707 head into feat/pluggable-memory-providers-v2 without creating another development branch or worktree, resolve conflicts in favor of the current V2 operating model, and preserve NCM only through the mapped provider boundaries."
    status: pending
  - id: restore-integrated-build-floor
    content: "Regenerate lockfiles and generated contracts where required, remove orphaned feature and dependency references, and make default and explicit NCM configurations compile on supported targets through Cargo Hauler."
    status: pending
  - id: publish-reconciliation-checkpoint
    content: "Review the integrated diff for accidental capability loss, run focused direct tests plus the normal build floor, then commit and push the clean reconciliation checkpoint with the exact PR #707 SHA in the commit body."
    status: pending
isProject: false
---

# V2 replacement baseline and PR 707 reconciliation

## Execution Notes

The reviewed upstream snapshot is PR #707 head `4f28d6fa95377a4ee305a7c7049c725c84c461f1`; its merge base with the current branch is `57006f60cb45bcee8487e73a40d4fad1a12ee2b6`. The current pushed branch head is `613d9d4ffd961eca0211903a684f1ba5850242ae`. PR #707 is open, draft, mergeable, and currently unstable, so its failing checks remain explicit blockers until fixed or shown unrelated. The early replacement contract prevents a late audit from discovering that an integrated build silently omitted a required V1 workflow or a flagship V2 surface.

The latest upstream tree retires dense neural code retrieval and moves major ownership boundaries. Conflict resolution must not resurrect `tracedecay-semantic`, FastEmbed/ORT code indexing, semantic activation, vector generations, or their public surfaces. NCM remains a separate memory provider and must be ported without becoming a fourth code-intelligence authority.

## Constraints

- Use only `feat/pluggable-memory-providers-v2`; do not create another development branch or worktree.
- Keep the current work recoverable by committing and pushing each coherent reviewed slice before destructive conflict resolution.
- Preserve superseded coherent changes in this same branch's Git history before deleting them from the final tree.
- Never use broad staging commands in the dirty worktree; stage exact reviewed paths.
- Treat `docs/V2-OPERATING-MODEL.md` and `docs/plans/tracedecay-v2/00-plan-set-index.md` at the pinned #707 head as normative.
- Do not retain PR-specific acceptance snapshots, giant comparison ledgers, local trust roots, signing systems, or attestations as release authority.

## Operator Guidance

This plan is the sole root of the replacement DAG. Complete it before starting the memory or retrieval plans. Its first frontier is writer quiescence and a pushed recovery checkpoint, addressing the earlier worktree loss directly. Use execution-focused subagents only for disjoint file ownership; the lead reviews each diff and pushes each accepted slice. If PR #707 advances during execution, finish and record the current pinned reconciliation decision before adopting the next exact head.
