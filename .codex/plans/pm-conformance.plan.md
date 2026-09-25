---
name: pm-conformance
overview: Create one discriminating provider compatibility suite whose assertions are identical for Native and NCM and cannot pass through empty or unsupported results.
todos:
  - id: common-profile-fixtures
    content: Extend the existing conformance runner with complete common-profile request, temporal, provenance, source lifecycle, replay and effect-accounting fixtures.
    status: completed
  - id: adversarial-profile-controls
    content: Add mutation and negative controls for dropped exclusions or time, forged origin, unsupported required operations, successful no-ops, scope leaks and deleted-source resurrection.
    status: completed
  - id: provider-agnostic-runner-output
    content: Expose one reusable invocation/report contract for real Native and real NCM adapters, preserving nondeterministic timing separately and counting unresolved assertions explicitly.
    status: completed
isProject: false
---

# pm-conformance

## Execution Notes

Read the brief and common profile. Reuse `tracedecay-memory-conformance`, current corpus types and typed outcomes. Each successful positive fixture must demonstrate nonempty expected source/content and actual durable state change where required. Outcome/effect/readiness/generation validation is shared. Provider-native ranking may differ; required source safety and operation semantics may not.

Discriminating cases include an excluded top hit revealing the next eligible result, t1 observe/t2 correct/t3 revoke across all temporal modes, delete-before-observe, replay with a new key after deletion, old-snapshot restore into live and fresh namespaces, feedback targeting after restart/restore, real v1 migration, corrupt payload with an unchanged claimed digest, and cancel-before-dispatch versus lost reply after durable commit. Required operations must be advertised and actually reached. Source-already-applied replay must preserve original effect evidence without pretending a new delivery key committed the old effect.

## Constraints

Own conformance `src/runner.rs`, `adversarial.rs`, `fixture.rs`, `scenario_corpus.rs`, relevant tests and a new narrowly named compatibility fixture module. Add missing canonical temporal/lifecycle scenario variants here before the comparison runner consumes them. `baseline.rs` changes require explicit root coordination with the comparative harness. Do not import concrete adapters or root persistence into provider-neutral conformance code. No production implementation, schema, evaluator or manifest edits.

## Operator Guidance

Dependency: `pm-contract`. Build the generic harness independently of unfinished adapters using controlled adversarial providers, but completion of adapters is proved later through their real instances. Root runs focused crate tests. Stop when both real factories can consume one unchanged suite; do not add provider-specific exemptions to expected outcomes.
