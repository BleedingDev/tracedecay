# Draft review orchestration

Goal: finish a concrete, executable plan for complete original Native preservation. No implementation runs in this review.

Exact selection: --plans-root /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2/.codex/plans/native-original/plan-review --glob '*.plan.md'.
Explicit edges: rnp-draft:rnp-fidelity; rnp-draft:rnp-graph; rnp-fidelity:rnp-close; rnp-graph:rnp-close.
Graph ID: native-original-plan-review-20260911. Strict validation passed with no errors/warnings. The saved handoff is plan-review-handoff.json.

The completed draft fans out to two independent read-only reviewers, then root resolves their findings and finishes the plan. Both agents use gpt-5.6-luna / max, fork_turns=none, no child agents. Current limits are 50 threads / depth 3; this wave uses two leaf agents while root reviews the DAG, writes its overview and verifies artifact links.

rnp-fidelity owns only evidence/plan-fidelity-review.md. It checks original behavior coverage and protected-code scope against reviewed decisions.
rnp-graph owns only evidence/plan-graph-review.md. It checks dependencies, exact file ownership, single-writer boundaries and runnable handoffs.
Root owns plan edits, synthesis and status. Neither reviewer edits product code, plans, peer reports or graph state. Reports should be concrete and bounded; no new source audit or broad repository scan.

