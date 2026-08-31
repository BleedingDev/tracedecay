# External lesson intake

This contract admits ideas from external implementations without making their
runtime, terminology, or internal representation part of TraceDecay's common
product model. The machine-readable authority is
`external-lesson-intake.json`; `external-lesson-intake.schema.json` closes its
shape, and `scripts/product/check-external-lesson-intake.py` enforces the
cross-file rules.

## Required intake record

Every lesson records:

- one stable HTTPS source repository and one exact lowercase 40-character
  commit, never a moving branch or tag;
- the recorded license identity, a provenance statement, and repository-local
  evidence that contains the source repository, commit, and license identity;
- exact-commit source links and the local audit evidence supporting each claim;
- one generic invariant and one generic target capability or policy;
- each source-specific assumption together with the concrete external-provider
  adapter boundary and a real file inside that adapter;
- neutral regression-test files, an implementation Bead, and the decision
  rationale;
- whether external code was copied and, if so, per-file source, destination,
  and license-notice paths; and
- a substantive rejection rationale when the decision is `rejected`.

An accepted lesson must name at least one real file below `tests/`. Neither the
test path nor its stated proof may contain a source identifier. This keeps the
regression useful for every provider implementing the target contract. A
rejected lesson keeps the evidence and target mapping but cannot copy code and
must say why the mapping would weaken or violate the common contract.

Source identifiers are permitted in source evidence, decision history, and the
concrete adapter. They are rejected from the extracted invariant, target, and
neutral test references. Concrete adapter paths must remain below a
`crates/tracedecay-memory-provider-*` crate other than the common API, Native,
or registry crates.

## Code-use rule

`clean_reimplementation` means the intake transfers only a behavior-level
invariant or operation choice. It must have `external_code_copied=false` and no
copy records. `copied_external_code` is accepted only when every copied source
path has an exact-commit evidence link and every destination and license-notice
path is a real repository file. Recording a license name without that
provenance is insufficient. Rejected lessons use `none_rejected` and cannot
carry copied code.

The intake contract does not create administrative digests, snapshots, or
approval receipts. Its durable evidence is the source link, local audit,
neutral regression test, implementation Bead, and reviewed code change.

## Workflow

1. Audit callable behavior at an immutable source commit and record its license
   provenance locally.
2. Extract a source-neutral invariant. Do not infer a capability from a name,
   package, endpoint, or marketing claim.
3. Map the invariant to an existing generic capability or policy. Keep source
   assumptions and protocol mechanics in the concrete adapter.
4. Add or identify a neutral regression test and the Bead that owns the actual
   implementation. An accepted record may precede implementation, but it may
   not claim completed behavior merely because a source primitive exists.
5. Record copied-code provenance or explicitly record a clean
   reimplementation. If the lesson is rejected, retain its evidence and state
   the semantic reason.
6. Validate from the repository root:

   ```sh
   python3 scripts/product/check-external-lesson-intake.py --repo .
   python3 tests/product_external_lesson_intake_test.py
   ```

## Audited example

The checked-in example uses the existing Biomem/NCM surface audit at commit
`500847ff65b5d9548b3826fa29bf3ccf8d221147`, whose repository-local metadata
records MIT. The accepted lesson extracts the generic rule that provider recall
is a bounded advisory read and maps it to `recall.query.v1`. The source audit
supports `search` as the candidate operation and rejects `retrieve` because the
audited path mutates session/usage state. That operation choice remains inside
`NcmCognitiveSurface`; the accepted regression points to the provider-neutral
recall contract test. The record does not claim that the open NCM integration
Bead is complete and does not copy external code.
