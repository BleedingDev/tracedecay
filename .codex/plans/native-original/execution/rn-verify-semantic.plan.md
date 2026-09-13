---
name: rn-verify-semantic
overview: "Prove real semantic retrieval without lexical fallback masking failure"
todos:
  - id: verify-real-semantic-results
    content: "Run frozen paraphrase/negative-control queries through actual CLI/MCP and assert real semantic participation and expected relevance."
    status: pending
  - id: verify-semantic-lifecycle
    content: "Verify fresh setup, cold/warm/restart, source update and artifact failure/recovery under bounded deadlines."
    status: pending
  - id: publish-semantic-evidence
    content: "Record exact generations, model/calibration identities, query modes, results and latency; leave any missing case incomplete."
    status: pending
isProject: false
---

# Prove real semantic retrieval without lexical fallback masking failure

## Execution Notes

Work only on `feat/pluggable-memory-providers-v2` in `/Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2`. Read ../READINESS-PLAN.md and ../WORKER-RULES.md. The current request is planning only: launch no implementation now. Later execution of this node begins only after lead assignment and graph validation. During later execution, stay in Codex and use available native agents only. The lead assigns exact files, reviews diffs, and commits/pushes accepted checkpoints. No development branch or worktree fan-out.

Use the unmodified `57006f60cb45bcee8487e73a40d4fad1a12ee2b6` Native reference. Keep stable V1 and operator data separate. Cargo runs belong to the single designated build owner via cargo-hauler; attach to matching tickets. Test profiles live below the active target directory. Capture failures as well as passes. No missing prerequisite, zero-test filter, skipped real-model test, fallback answer, or mock-only result counts as success.

Use rn-acceptance-matrix's frozen corpus and rn-build's real artifacts. Discover the current public strict-semantic query mode; require a paraphrase query whose oracle cannot be passed by exact/name matching and inspect serving/lane evidence. Expected relevant items and top-k thresholds are fixed beforehand; unrelated controls must not be counted as relevant. Verify hybrid fallback separately with deliberate artifact removal or corruption in test-owned state.

Exercise compatible model installation/acquisition, calibration/activation ready, query, source update/reindex, same-checkout HEAD change and restart, then recover from missing or mismatched artifacts. Use the current source-generation compatibility receipts, not only exit code zero. Record cold-start provisioning and warm-query latency separately against existing documented budgets. Repeat critical cold/warm/restart cases at least ten times without dropping failures.

## Constraints

Verification-only: own execution-results/semantic-verification.md and isolated target/test-profile/readiness/semantic results. Reuse existing semantic lifecycle/query tests plus the bounded shipped CLI fixture added by the repair owner. No production source edits, oracle changes, lexical-only pass, placeholder embedding model, skipped artifact check or rewritten baseline.

## Operator Guidance

Depends on rn-semantic-fix and rn-build. Gate both independent reviews and rn-release-readiness. If a query returns correct text but semantic is unavailable, this node fails; code search as a whole returning a fallback is not semantic verification.

## Current dependency contract

Prerequisites: rn-semantic-fix, rn-build. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.
