---
name: rn-privacy-audit
overview: "Revalidate privacy detector equality and the typed history admission boundary against the unmodified PR707 baseline."
todos:
  - id: rn-privacy-audit-done
    content: "Map current privacy callers and record detector equality against 570; return a bounded gate for the privacy restore lane."
    status: pending
isProject: false
---

# Revalidate privacy against upstream 570

## Execution Notes

Read AGENTS.md, ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md and ../SOURCE-BOUNDARY.md. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Active original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; execution candidate: 1fe250fed7ca615f1dfdcd580befeb276328e910. The earlier b3-to-571 detector finding is historical. rn-source-docs and rn-integration-readiness must be reviewed before this gate releases rn-build. This is a read-only review; no child agents, Cargo or source edits.

## Ownership and Constraints

Own only .codex/plans/native-original/execution-results/privacy-audit-57006f60.md. Read current privacy and host callers; do not modify crates/tracedecay-privacy/src/detector_kernel.rs, the history admission seam or any generated contract. Preserve the historical privacy-isolation-design.md report.

## Steps

1. Record the detector blob at 570 and the candidate, expecting 9ce4488a34c5bd121d43c35c33f1daf925ab38ba, plus the exact git diff --exit-code.
2. Recheck the current typed Claude history source-field admission callers and their privacy/provenance invariants. Keep generic admission, original Native and LCM privacy separate.
3. Compare current caller paths and symbols to the historical report; identify stale line/path/assertion references and assign each to the next gate. Do not rewrite the old report.
4. If detector equality fails or a caller relies on an unproved changed algorithm, return the exact mismatch and block rn-privacy-restore; do not invent parity.

## Acceptance Checklist

- Current blob, caller paths, and command results are recorded in the new 570 report.
- Historical b3-to-571 evidence is explicitly separated from the active gate.
- A bounded privacy restore decision is returned without a detector source edit.

## Operator Guidance

Root launches after rn-source-docs and rn-integration-readiness are accepted. Root reviews the gate before releasing the privacy-dependent host lane. Return exact unresolved caller and assertion owners; do not claim product build success.
