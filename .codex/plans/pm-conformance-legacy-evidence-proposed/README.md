# Draft legacy evidence amendment

UNAPPLIED. Root review and application are required before compilation. No Cargo,
model or real-provider run was performed by this worker.

`apply.patch` changes the shared migration validator, adds a small typed legacy
evidence module with five pure regression tests, and adds the two actual factory
read hooks. `protocol.delta.patch` is the documentation-only projection of that
same patch. Readable copies are under `candidate/`; exact starting bytes are under
`baseline/`. There are no adapter/runtime/storage implementation changes.

The original cc1384 report is unchanged and remains incompatible: 190 planned,
133 passed, 6 failed, 39 unknown, 12 expected degraded; the strict original-receipt
row failed. Its artifacts remain at
`target/test-profile/ncm-common-real-5019-1789046515736999000/`, including
`summary.json` and `actual-report.txt`. This proposal neither rewrites that result
nor converts it into a pass. The amended program explicitly contains 192 actions.

## Actual field evidence

Native's old writer copies exactly the original 24 columns named `LEGACY_COLUMNS`
in `native_common_factory_tests.rs`, checks schema version 1 and exactly 24
columns, and reads the public reference/operation/key/receipt from those rows.
Both public identifiers are `Retained`. The new baseline and both postrestart
point reads use that identical 24-column list in fixed order. Each field is
encoded with its SQLite type and exact value/bytes before SHA-256. The separate
receipt hash covers exact stored receipt text bytes. The production migration in
`native_staged_observations.rs::initialize_schema` only adds and populates v2
columns/tables; it leaves those 24 columns intact. Added original-source,
semantic/projected digest, feedback, validity and global generation state are
excluded from this immutable projection. No representation normalization is
performed: the old numeric source revision remains an integer and never becomes
an invented canonical opaque source revision.

NCM's actual writer evidence is retained at
`target/test-profile/ncm-common-real-9063-1789043864616279000/scenario-826b518564cc4e86fe2832a65c5eeae0ec93b5e6ca948a30df912ee1286261e3-40.legacy-writer-evidence.json`.
It proves the actual public operation/key are absent from the old capsule and
journal. The fixture already checks that absence against actual original
call/reply and worker dispatch. Both public identifiers are `CallerObservedOnly`.
The actual old numeric `record_id` is the stable reference. Its public receipt is
not stored verbatim; the existing verified durable-basis function recomputes it
from the stored original `DurableReceipt.reply.state_generation` and payload,
under the original Observe receipt domain. The observed original digest in that
evidence is `58a7b9e30333b1d4a232dfdd0bd49846a0506ac6b2a6a8fc64b98c2c9515c3ad`.

NCM's immutable projection contains exactly the already-audited old columns:
`capsules.record_id, source_id, key_text, value_text, provenance, status,
commit_seq`, joined to `events.seq, kind, idempotency_key, payload_sha256` through
`events.seq = capsules.commit_seq`. It serializes the named JSON projection in
deterministic field order without parsing or rewriting stored provenance text.
The separate receipt hash covers exact `events.receipt` UTF-8 bytes, including
its original state digest. Mutable recall/model counters, center state and global
generation are excluded. Postrestart reads reparse the actual stored receipt to
recompute its original public digest; they never reuse caller memory. Any changed
capsule provenance (including a synthesized public mapping) changes the record
fingerprint and fails. Missing rows, malformed observed receipts or changed
fields fail; inability to execute a safe read stays unknown.

## Lifecycle and protocol boundaries

Both factories retain only actual stored references as lookup locators. They
point-read their owned namespace after each real restart; there is no stop,
reopen, readiness bypass or record mutation in the audit. Native uses a read-only
connection. NCM reuses the existing bounded `sqlite_json(read_only=true)` helper
and cleanup collector. That root-approved helper uses `PRAGMA query_only=ON`
because actual WAL/header-only databases failed with raw `sqlite3 -readonly`;
it permits normal WAL infrastructure while prohibiting SQL record writes.

The public delivery-receipt selector and four-field item schema are unchanged.
Retained identities still demand exact original items and Success. Known missing
identities demand omitted items plus explicit partial coverage or a bounded
warning, typed Success/Partial, no effect, unchanged generation and forbidden
fallback. Degraded is only reachable after this restart's independent audit.
Unknown identity or missing audit remains Unknown. Synthesized/null-filled
items, mismatched retained evidence and unreported loss fail. Trace, recall,
privacy, replay and original receipt assertions are otherwise unchanged.

Reports have a separate `legacy_delivery_identity` aggregate. Both receipt rows
and both physical audits must complete before it may be Complete or Degraded;
it cannot be inferred from `compatible()`. NCM summary JSON and Native test output
include that field. The NCM observer's existing BufWriter hunk is preserved.

## Validation for root

The draft is syntax-formatted and applies cleanly to its exact reviewed baseline.
Five new pure tests cover strict retained items, known loss and forged items,
actual record/receipt tampering, per-restart audit prerequisites and explicit
coverage, and the declared 192-action program/order. Existing opaque-key/source
attribution tests are adapted to typed identity evidence. Suggested bounded
first validation: `cargo test -p tracedecay-memory-conformance --lib --test compatibility`.
Real factory compilation and runs remain root-owned and unperformed here.
