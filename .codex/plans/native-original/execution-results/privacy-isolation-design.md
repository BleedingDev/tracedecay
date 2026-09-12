# rn-privacy-audit — privacy algorithm drift and product isolation

Audit completed against `b3b43410e47115056f2066449aafa1822bbb6049` (original)
and `571daf3a9612e5247443e4da3a107b542686c1ef` (audited head). This lane is
read-only for implementation code. It changes this design artifact only; the
detector restoration and the product seam belong to their named execution
owners.

## Finding

The only b3-to-head change in the shared detector is
`crates/tracedecay-privacy/src/detector_kernel.rs:247-289`.

At b3, `looks_high_entropy_token` applies the complete predicate to the token
it receives: minimum length 36, the allowed token-byte alphabet, at least one
letter and digit, not an all-hex digest, and the fixed Shannon threshold of
4.2 bits per character. The audited head first removes one suffix only when
`rsplit_once("-sha256-")` has a nonempty prefix and exactly 64 lowercase hex
characters, then applies that predicate to the prefix through
`looks_high_entropy_token_core`.

This is a semantic change to a public shared predicate. It is not a safe
general rule for digest-shaped values:

* A Claude observation source ID such as
  `tracedecay-claude-observation-source-v1-sha256-{64 lowercase hex}` is high
  entropy under the b3 whole-token predicate and is accepted by the audited
  wrapper because the known suffix is removed.
* A genuinely high-entropy prefix followed by the same suffix remains a
  finding. The head peels only one suffix and still checks the prefix.
* Short, uppercase, non-hex, extra-tailed, empty-prefix, or otherwise malformed
  suffixes remain on the whole-token path. The current detector tests at
  `:424-465` document these cases.

The required original action is therefore to restore the complete
`detector_kernel.rs` implementation to b3. The restoration owner must remove
the wrapper, private duplicate core, and wrapper-specific tests rather than
preserve a compatibility branch in the original privacy crate. No caller may
redefine or copy the entropy algorithm to compensate.

## Original Native and common callers affected by restoration

Restoring b3 changes every original path that currently receives a valid raw
Claude observation source ID (and any other token made safe solely by the
wrapper). It also restores the b3 result for all ordinary text that happens to
end in a valid `-sha256-` suffix. The relevant original callers are:

| Original path | Symbol / evidence | Effect of b3 restoration |
| --- | --- | --- |
| Session-memory fact hygiene | `crates/tracedecay-session-memory/src/memory/hygiene.rs:47-63`, `detect_secret_like`; shared import at `:14-17` | Exact fact add/update and curation gates again use the original whole-token predicate. `high-entropy token` findings are not silently accepted. |
| Native fact add/sanitize | `crates/tracedecay-session-memory/src/memory/sanitize.rs:64-105`, `sanitize_add_fact_request`; `sanitize_memory_fact_payload` | Canonical fact payload redaction/quarantine again uses b3. This includes privacy remediation and any existing update path that calls the same sanitization helper. |
| Native exact fact lookup | `crates/tracedecay-session-memory/src/memory/project_memory.rs:170-203`, `find_exact_fact_by_content` | Secret-like content is rejected from the exact-content lookup as before; restoration must not make query operations accept a newly exempt token. |
| Native curation | `crates/tracedecay-session-memory/src/memory/curation.rs:688-700` and its hygiene import | Candidate merge/curation content keeps the original secret gate. |
| Shared privacy text/payload sanitization | `crates/tracedecay-privacy/src/detect.rs:497-554, 750-780` through `redact_sensitive_values`, `redact_text`, and `high_entropy_ranges` | `sanitize_memory_fact_payload` and its JSON walk again redact/quarantine the full token. Do not add an exception to `redact_text` or `high_entropy_ranges`. |
| Provider metadata and source diagnostics | `crates/tracedecay-privacy/src/structured_text.rs:590-598` (`sanitize_provider_metadata_text`) and raw-only paths at `:621-661` | Existing Native/session diagnostics keep b3 scanning and redaction semantics. |
| LCM payloads | `crates/tracedecay-privacy/src/structured_text.rs:680-683` (`sanitize_lcm_payload_text`) and `crates/tracedecay-lcm/src/raw.rs:516-525, 689-719, 940+`, `dag.rs:79-94` | LCM raw, DAG, schema and retention/diagnostic payloads retain whole-token b3 behavior. No LCM route may receive the product exception. |
| Parsed Claude/session source protection | `crates/tracedecay-privacy/src/sanitize.rs:230-347`, `ClaudeRecordSanitizerV1::sanitize_parsed`; `structural_id.rs:24-36` | Structural ID protection remains unchanged. The recognized raw Claude source ID is preserved by the structural-ID protector, but this does not authorize a global detector exemption. |

There are no direct predicate callers outside the shared session-memory
hygiene path and detector tests; the other rows reach it through the privacy
redaction helpers. This is why changing the shared detector has a wider blast
radius than the product observation regression suggests. The protected
`tracedecay-session-memory`, `tracedecay-privacy`, `tracedecay-lcm`, store and
LCM authorities remain unchanged apart from the exact b3 restoration assigned
to the original-file owner.

The existing `privacy.structural-id.v1.{64 lowercase hex}` format is a
separate structural protection format. It is not the discovered
`-sha256-` suffix case and must not be added to a broad allowlist as part of
this correction. Existing structural-ID recognition and hashing stay intact.

## Product regression and why the shared change cannot remain

`crates/tracedecay-memory-hygiene/src/credentials.rs:106-158` calls the
original session-memory `detect_secret_like` and then performs its bounded
supplementary credential probes. `ObservationSanitizer::classify_value` at
`crates/tracedecay-memory-hygiene/src/lib.rs:592-685` recursively classifies
every object key and string in the provider envelope. Its admission pipeline
at `:345-435` calls `sanitize_payload` (`:477-564`), which first classifies,
then calls the unchanged public `sanitize_memory_fact_payload` at `:500-521`.
The canonical privacy redactor therefore sees the same source ID a second
time; changing only `credentials.rs` would still redact or quarantine it.

The host adapter builds the exact provider envelope in
`crates/tracedecay/src/daemon/retained_owner/observation_journey.rs:1091-1184`.
The envelope contains the canonical message payload at `:1144-1148`. For a
history delivery, validated attribution and the history grant are added at
`:1149-1157`. The source ID appears as a value in the preserved source
metadata, including the original source attribution and the corresponding
history-grant source. `provider_history.rs:1451-1465` derives that source
identity from the canonical observation, and `:1760-1788` emits it verbatim.

Claude capture creates this opaque value at
`crates/tracedecay-capture/src/claude/mod.rs:23-27` as
`tracedecay-claude-observation-source-v1-sha256-{canonical_framed_sha256}`;
the session host reuses it at
`crates/tracedecay-sessions/src/runtime/hosts/claude/cursor.rs:12-15` and
`frames.rs:147-162`. It is validated/provenanced host data, not message text.
The existing product fixture at
`crates/tracedecay-memory-hygiene/src/lib.rs:987-1031` demonstrates the
required behavior: an innocent message with this source metadata must be
admitted, the sanitized envelope must remain byte-identical, and the receipt
must bind the full envelope. The neighboring test at `:1034-1071` requires
known credentials to remain withheld even when a credential carries a valid
digest suffix.

The current head satisfies the fixture only because the shared original
detector was weakened. Keeping that weakening would silently change all the
Native and LCM callers above. Removing the fixture would hide a real host
integration requirement. The correction belongs at the product host
boundary.

## Smallest safe product seam

Add a product-only typed structural-field admission seam in
`tracedecay-memory-hygiene`. The strict public methods remain strict:

```text
ObservationSanitizer::admit(payload)
ObservationSanitizer::admit_observation(payload, extensions)
```

continue to classify every byte under restored b3 semantics. In particular,
`AdvisoryTextHardener` in `crates/tracedecay-memory-hygiene/src/recall_text.rs:795-827`
continues using strict admission, so provider-controlled recall text cannot
obtain a structural exemption.

The product crate should add an internal common implementation and one
explicit typed entry point, for example:

```text
TrustedClaudeObservationSourceIdV1::from_validated_source(provider, source_key)
ObservationSanitizer::admit_observation_with_trusted_history_source(
    envelope, extensions, &trusted_source_id)
```

The exact Rust names may follow the crate's conventions, but the invariants
are fixed:

1. The constructor accepts only provider `claude` and the complete exact
   `tracedecay-claude-observation-source-v1-sha256-` prefix followed by 64
   lowercase hex characters. It returns no type for wrong case, malformed,
   empty-prefix, extra-tail, or arbitrary `-sha256-` strings.
2. The trusted entry point is called only by the canonical observation adapter
   after scope/canonical-envelope checks and
   `provider_history::validate_history_record` have succeeded. Non-history
   observations and all other providers use strict admission.
3. Shield only the typed value at the two known history source-field paths:
   `/source_identity/original_source/source/source_key` and each validated
   `/history_grant/sources/*/attribution/source/source_key`. Match both path
   and exact value. Do not exempt every occurrence of a string, substrings,
   embedded prose, arbitrary JSON pointers, or extension bytes. This keeps a
   user/provider payload that merely mentions the source ID under b3 scanning.
4. Before classification and the unchanged canonical privacy call, the
   product sanitizer makes a private projection in which those exact scalar
   fields are replaced by short, collision-free, low-entropy placeholders.
   The original input remains borrowed and untouched. After canonical
   redaction, verify each placeholder survived and restore the original typed
   values before attribution, transient handling, final canonical encoding and
   receipt construction. A missing/changed placeholder or path mismatch fails
   closed; it must not silently restore an unverified value.
5. Compute `source_payload_sha256` from the original envelope as today. Restore
   the source fields before computing `sanitized_payload_sha256`; an otherwise
   innocent history envelope consequently has `Accepted` disposition and both
   receipt digests equal the exact full-envelope bytes. If another field is
   redacted or withheld, retain the existing findings, disposition and
   fail-closed behavior.
6. Apply the same typed field projection to the root envelope only. Do not
   weaken `sanitize_memory_fact_payload`, `redact_text`, the shared detector,
   extension validation, or any Native text sanitizer. The policy revision and
   receipt schema remain unchanged because the host's effective admitted bytes
   and provenance remain unchanged after restoration.

This projection is a host-owned typed identifier boundary, not an entropy
algorithm copy. A generic `allowlisted_strings: &[&str]` parameter or a
global exact-value skip would be too broad: it could exempt a matching value
inside canonical message content and would make every future caller decide
privacy policy ad hoc. If the implementation owner cannot provide path-bound
typed fields, the exact blocker is that no safe product seam exists in the
current generic sanitizer; in that case the host adapter must remain strict and
the regression cannot be accepted by changing Native privacy.

### Ownership and call-site changes

* **Original restoration owner:** restore
  `crates/tracedecay-privacy/src/detector_kernel.rs` exactly to b3. No other
  original privacy, session-memory or LCM file changes are authorized.
* **Product hygiene owner:** implement the typed source-ID constructor,
  placeholder projection/restore, and shared admission implementation in
  `crates/tracedecay-memory-hygiene/src/lib.rs` (a private module is fine).
  Keep `credentials.rs` and its shared detector call unchanged unless the
  projection plumbing requires an explicit private context parameter; any such
  parameter must be used only by the new typed entry point.
* **Canonical host owner:** in
  `crates/tracedecay/src/daemon/retained_owner/observation_journey.rs:1149-1168`,
  construct the typed value from the already validated Claude history source
  and call the new seam. Preserve the exact envelope, history metadata,
  `source_identity`, forget-source key, settlement and receipt checks at
  `:1186-1290`. The sanitizer is mounted once at `:4820-4861`; no NCM or
  original service wiring changes are needed.
* **Test owners:** update the product unit fixture and add focused adapter
  assertions without weakening existing credential/provenance checks. Do not
  add a generated contract, protocol revision, manifest dependency or
  persisted-data migration for this Rust-only boundary.

## NCM and common-provider impact

NCM does not own this detector. `crates/tracedecay-memory-provider-ncm/src/lib.rs`
validates the attached host admission in `invoke` at roughly `:1207-1349`,
then calls `surface_payload` (`:463-520`). Its observation source projection
at `:1779-1889` hashes the already admitted original source into the NCM
opaque namespace. The NCM model/store/worker implementations and Rust backend
never call `looks_high_entropy_token` or `sanitize_memory_fact_payload`.

The shared host journey is nevertheless an NCM input boundary. The two real
NCM host fixtures
`product_memory_provider_claude_host_journey::{real_ncm_observer_receives_shipped_claude_hooks_while_native_answers_context,real_ncm_active_recalls_shipped_claude_session_history_with_native_disabled}`
deliver canonical Claude observations through the same adapter. In
particular, active history delivery carries the raw source identity and grant.
Restoring b3 without the host seam would withhold that record before NCM's
existing source projection, changing NCM delivery and provenance behavior.
With the seam, the host receipt still binds the exact original envelope; NCM
then performs its existing `common::project_source_id`/opaque projection. No
NCM source, model, worker, namespace, manifest or contract change is allowed.

Retain the existing common-provider source/provenance checks and tests,
including `ncm_adapter.rs:2690-2734` (restart-stable source alias),
`rust_backend.rs:599-700, 758+` (canonical observation/recall provenance),
`:1368+` (correction/history), and `:1905+` (same-source message/delete).
These tests constrain the seam to preserve the raw source until NCM's current
projection; they do not authorize a second sanitizer or a NCM-local detector.

## Required verification filters

The build/verification owners should run the following focused checks after
the two implementation owners' diffs are reviewed. This audit ran no Cargo
commands.

1. **Original detector/privacy:** exact b3 entropy and malformed-suffix tests;
   session-memory hygiene tests for fact add/exact lookup/curation; privacy
   `sanitize_memory_fact_payload` and structured-text tests; LCM raw, DAG,
   schema and retention/diagnostic filters. Confirm the raw Claude source ID
   is high entropy under strict b3 paths and malformed/uppercase/extra-tail
   suffixes are never exempt.
2. **Product sanitizer:** keep the existing full-envelope receipt test, but
   exercise it through the typed history seam; add a strict-admission test that
   withholds the same raw ID; verify exact source and sanitized digests equal
   the original envelope after the seam. Add wrong provider, wrong case,
   short/non-hex/extra-tail, embedded-content, high-entropy-prefix-plus-valid
   suffix, and duplicate-history-field cases. Keep the existing
   `structural_digest_suffixes_do_not_hide_known_credentials_or_payload_secrets`
   test: credentials in source fields or canonical content remain withheld,
   including credential-plus-suffix values.
3. **Strict recall text:** `AdvisoryTextHardener` with a raw Claude source ID
   remains withheld; only the canonical history adapter may pass the typed
   source field.
4. **Host/NCM:** run the two real Claude host journey filters named above and
   verify NCM receives the same source/provenance bytes before its existing
   opaque projection. Run the NCM source alias/restart, canonical provenance,
   cross-source history and delete/message filters listed above.
5. **Receipt/provenance regression:** for a history record with the source ID
   duplicated in `source_identity` and `history_grant`, verify canonical source
   evidence is untouched, the delivered envelope retains both values, the
   full-envelope receipt validates, and a replay remains idempotent.

No generated output or Cargo manifest change is indicated. The remaining
uncertainty is implementation detail only: the product owner must choose the
private placeholder/path representation while preserving the typed and
path-bound invariants above. If those invariants cannot be met, stop the
product lane and report that exact seam blocker rather than weakening Native
privacy.
