---
name: rn-verify-ncm
overview: "Verify NCM still works with shared canonical services. Preserve the complete original upstream Native implementation from 570 and its operation boundaries."
todos:
  - id: rn-verify-ncm-done
    content: "Verify NCM still works with shared canonical services and return the required reviewable evidence."
    status: pending
isProject: false
---

# Verify NCM still works with shared canonical services

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/workspace/bleedingdev/projects/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Execution host: Codex. Use an available Codex-native execution/review agent when assigned; adapt unavailable model preferences within Codex. You are a leaf; no child agents or cross-host agent CLI launches. Preserve peers' edits and send cross-scope needs to the lead.

Mode: verification-only.
Prerequisites: rn-build. Every named predecessor must be accepted by root before dependent work starts. Original baseline: 57006f60cb45bcee8487e73a40d4fad1a12ee2b6; audited product head: 1fe250fed7ca615f1dfdcd580befeb276328e910.

Read these reports under ../evidence/: ncm-boundary.md, host-delivery.md, verification.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

## Ownership and Constraints

Write only:

- target/task-scratch/native-original/ncm/ (test data/results only)

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

## Steps

1. Run the accepted current NCM focused suite and real worker smoke using tested artifacts and isolated exact scope.
2. Verify common-profile admission, observations, recall, replay/restore identity, cancellation and Native-selected/NCM-observer coexistence.
3. Verify NCM-selected delivery remains attributed separately from shared canonical Native facts and session/LCM context. Do not call this isolated NCM quality.
4. Preserve all seven scope fields, current namespace/model/state format and source integrity. Record model/executable prerequisites.
5. Retain the earlier intermittent recall outcome and any new failures. Repetition is for diagnosing stability, not selecting a favorable run.

## Acceptance Checklist

- NCM model/state/namespace identities remain compatible, and any source change is the reviewed causal recall repair with preserved contract behavior.
- Real NCM active and observer operation succeeds under preserved registration/host authority.
- Known intermittent recall is not relabelled fixed by a single pass.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return regression/stability evidence separately from Native parity. No algorithm/model tuning or broad comparative-quality campaign.

## Current dependency contract

Prerequisites: rn-build. The exact edges in ../execution-selection.json are authoritative; this section supersedes older prerequisite prose. Read ../READINESS-PLAN.md for the latest NCM repair and semantic scope.

## Required reliability campaign

Run at least 100 ordered trials of the frozen workload: 25 cold-worker, 25 warm-worker, 25 worker/daemon-restart, and 25 concurrent-observation/load trials. Every trial uses admitted required-item expectations defined before execution, with top-k/budget/scope compatibility checked independently. Require zero unexplained misses, duplicate deliveries or cross-scope contamination in those deterministic contract checks. Save every outcome and seed; a failed trial cannot be discarded or replaced by a favorable rerun. The original 2/4 workload must be reproduced and causally repaired, or remain explicitly unresolved regardless of a new corpus passing.

Pin the real worker, model/tokenizer, source revision, feature set and namespace/state schema; provision absolute TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT. Missing binaries/models and filtered-out tests are failures of readiness, not skips. Add cancellation, snapshot/restore, replay and failure-recovery cases from existing rust_backend filters. Preserve the established kernel recall p95 <=25 ms at 4096 centers, warm text recall p95 <=250 ms and durable observe p95 <=500 ms where these workloads apply; measure host delivery separately under existing hook deadlines. Retrieval quality thresholds are independently frozen; not every stored item is inherently relevant to every query.

Use shipped Codex/Claude host-entry fixtures through the built binary; do not launch Claude Code or another agent CLI. Record fixture delivery coverage separately from an actual external application manual test. Write a versioned summary in execution-results/ncm-verification.md and retain raw synthetic receipts under the active target/test-profile tree.
