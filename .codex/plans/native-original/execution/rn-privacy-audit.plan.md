---
name: rn-privacy-audit
overview: "Resolve newly discovered original privacy algorithm drift at a product boundary."
todos:
  - id: rn-privacy-audit-done
    content: "Map original Native and product callers and return an exact bounded restoration/isolation design."
    status: completed
isProject: false
---

# Audit original privacy algorithm drift

Root found a previously omitted b3-to-audited-head change in crates/tracedecay-privacy/src/detector_kernel.rs: looks_high_entropy_token now peels one structural SHA-256 suffix before applying the original entropy predicate. This is a shared original algorithm change, not automatically a host-only extension.

Workdir: /Users/satan/side/experiments/tracedecay/.worktrees/pluggable-memory-providers-v2 on every shell call. Original b3b43410e47115056f2066449aafa1822bbb6049; audited HEAD571daf3a9612e5247443e4da3a107b542686c1ef. Read AGENTS.md, ../WORKER-RULES.md, ../REVIEWED-DECISIONS.md, ../SOURCE-BOUNDARY.md. Luna Max, fork none, read-only code reviewer, no child agents/Cargo. You are not alone; preserve peers.

Own ONLY ../execution-results/privacy-isolation-design.md. No source/test/doc edits elsewhere.

1. Trace this public predicate and its sanitization callers to original Native fact add/query/privacy/LCM/session operations and product structural identifier processing. Give exact symbols and paths; source/codegraph evidence, no live databases.
2. Identify precisely which original Native inputs change behavior versus b3 and which product host regression required the suffix handling. Reuse existing tests and typed boundaries; distinguish content from structural identifiers.
3. Propose the smallest concrete external product-boundary isolation that permits restoring the complete detector file to b3 while preserving existing host privacy/provenance behavior. No copied Native algorithm, new protected original helpers or global weakening. If no safe external seam exists, name exact signature/caller blocker. Do not silently accept changed Native semantics.
4. Return exact file/method ownership and test filters for execution owners. Identify shared unchanged callers that constrain the change and whether generated contracts or manifest changes are necessary. Runtime proof deferred to designated build owner.

Stop after a reviewable evidence-backed design; root decides and assigns implementation. No scope beyond this discovered difference.
