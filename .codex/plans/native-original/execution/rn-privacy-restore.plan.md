---
name: rn-privacy-restore
overview: "Revalidate the privacy restoration gate at the PR707 baseline and route any real mismatch to its exact product owner."
todos:
  - id: rn-privacy-restore-done
    content: "Record detector equality and typed history admission revalidation for 570; return a bounded fix handoff only if needed."
    status: pending
isProject: false
---

# Revalidate privacy restoration at upstream 570

## Execution Notes

Workdir: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Active original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; execution candidate: 1fe250fed7ca615f1dfdcd580befeb276328e910. The b3-to-571 privacy result is historical. Read AGENTS.md, ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md, ../SOURCE-BOUNDARY.md and the new 570 privacy audit. This is a read-only gate: no source edit, Cargo, commits, push or child agents. Root owns graph/status.

## Ownership and Constraints

Own only .codex/plans/native-original/execution-results/privacy-restoration-57006f60.md. Read the detector and product history admission callers without changing them. Do not relabel privacy-isolation-design.md as current, restore the detector again, or edit observation_journey.rs.

## Steps

1. Consume the accepted 570 privacy audit and independently verify the detector's 570/candidate blob equality.
2. Check the typed Claude source-field admission against current caller signatures, exact provider/identity validation, strict withholding, malformed/extra-tail rejection and sanitized provenance behavior.
3. If all active claims remain supported, record a no-source-change result and leave the gate pending root review. If a mismatch exists, record the smallest product owner and exact path/signature; do not implement it here.
4. Keep generic admission, original Native and LCM privacy behavior, and NCM behavior distinct from the product-only history seam. Product build and session delivery remain blocked until this gate is accepted.

## Acceptance Checklist

- The active result references 570 and current candidate paths, with actual checks and no fabricated pass.
- Detector equality is current evidence; the historical b3 restoration is not reused as a current completion.
- Any required fix is bounded to a named product owner, with no original detector or Cargo edits in this lane.

## Operator Guidance

Root launches this gate after rn-privacy-audit and rn-integration-readiness. Root reviews status and releases rn-session-delivery only after acceptance. Return exact paths, checks, unresolved questions and next owner. Do not claim product build or Native parity.

## Current dependency contract

Prerequisites: rn-privacy-audit. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
