---
name: rn-reference
overview: "Build the independent original Native comparison runner. Preserve the complete original b3 Native implementation and its operation boundaries."
todos:
  - id: rn-reference-done
    content: "Build the independent original Native comparison runner and return the required reviewable evidence."
    status: in_progress
isProject: false
---

# Build the independent original Native comparison runner

## Execution Notes

Read ../WORKER-RULES.md and ../REVIEWED-DECISIONS.md before this node. The reviewed decisions override provisional recommendations in audit reports. Worktree: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2. Use this exact workdir on every shell call; confirm the actual execution HEAD and peer changes before editing.

Model: gpt-5.6-luna. Reasoning: max. Spawn with fork_turns=none and a bounded handoff. You are a leaf; no subagents. You are not alone in the codebase: preserve peers' edits, never revert/reformat/stage them, and send cross-scope needs to root.

Mode: write-capable.
Prerequisites: None; this node can start in the first launch wave.. Every named predecessor must be accepted by root before dependent work starts. Original baseline: b3b43410e47115056f2066449aafa1822bbb6049; audited product head: 571daf3a9612e5247443e4da3a107b542686c1ef.

Read these reports under ../evidence/: verification.md, source-baseline.md, native-facts.md, native-sessions.md. Also read the exact predecessor outputs supplied by root; do not infer an unfinished API.

Reference checkout: `/Users/satan/side/experiments/tracedecay/.worktrees/native-original-reference-b3`. Create it only if absent; if it exists, verify its HEAD and clean source rather than resetting or deleting it. Reference process/data roots remain test-owned and separate from operator data.

## Ownership and Constraints

Write only:

- scripts/product/native-original/** except cases/**

Everything else is out of scope. Original Native/session/LCM implementations, original storage and contracts, NCM internals, live databases/settings, unrelated docs, manifests and generated files are protected unless explicitly named above. An allowed directory does not permit editing an original file protected by the reviewed decision. Shared generated outputs outside this ownership require an explicit root assignment before generation writes them.

No push, merge, release, global install, runtime user-data action or unassigned cleanup. Only the designated build owner in rn-build-reference and rn-build submits Cargo work. A no-change conclusion is valid when evidence proves the required behavior already holds; it must not hide missing coverage.

Required owned output: `scripts/product/native-original/case-contract.json`, the canonical runner-to-fixture contract, plus its small usage example in the same owned runner directory. Publish it as an accepted predecessor artifact before fixture writers begin. It must specify invocation, case schema, result/error/unknown/unsupported semantics and capture/artifact locations, and support exact original operation routes, separate production-composition invocation and distinct host-extension regression classification.

## Steps

1. Create a small runner that invokes separately built b3 and product binaries through corresponding existing production CLI/MCP entry points. Pin full revisions and refuse a modified reference tree. Reference checkout lives under the repository's .worktrees/; no build or product source edits in this node.
2. Publish the case-input format immediately for facts/session/state fixture authors. Prefer existing retained operation schemas and existing comparison capture helpers; do not invent a parallel provider or copy original algorithms into the harness.
3. Give every run isolated stores, process ownership, logical fixtures and captures. Seed both sides independently through original public commands or ingestion. Map only justified nondeterministic identity/time fields; retain original score/order/receipt and raw response evidence.
4. Capture relevant public outputs and operation-specific semantic state before/after reopen. For original operations absent from CLI use an existing original runtime entry point from an external harness without editing b3; inability to reach one is a visible coverage gap.
5. Implement fail/unknown/unsupported distinctions and nonzero-case execution checks. Raw process capture, cleanup and exact command failures must survive failed comparisons. Do not run builds; publish commands and artifact needs to the build owner.

## Acceptance Checklist

- Runner unit checks exercise comparison mismatch, missing original binary, modified reference source, zero relevant cases and child cleanup.
- A changed Native score/order, lost retrieval receipt or missing session row would fail comparison; shared changed-backend comparisons are rejected.
- Fixture schema and runner invocation are usable by all three case authors; no original source is copied or edited.

## Operator Guidance

Root launches this node from the saved execution graph only when its predecessors are accepted and its exact files are free. Review this diff before releasing dependent writers. Root alone updates plan status.

Return: node ID; exact changed paths and reasons; diff; verification commands and actual results; build tickets if applicable; protected-behavior evidence; unresolved dependencies/failures with exact next owner. Do not claim an unrun check passed.

Stop condition: Return a usable external runner and case contract; reference and product binaries may remain unbuilt until rn-build. Do not replace unavailable original execution with canned output.

