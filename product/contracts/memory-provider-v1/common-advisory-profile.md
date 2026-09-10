# Common advisory profile

`memory.advisory_common.v1` is an explicit registration/handshake capability
requiring the operations listed in `common_advisory_profile.required_capabilities`
in the registry contract. A descriptor claiming it must also declare every
required capability, including temporal recall. Each required operation must
execute on populated state; an unsupported response does not satisfy this profile.
Legacy registration still requires only health, observe and recall. The profile
does not enable a provider or change default-off configuration.

The shared observation kinds are committed session messages and settled source
edits, test executions and feedback outcomes. The other canonical kinds and
associative recall, explicit-fact projection and provider explainability remain
declared extensions. Canonical facts remain a separate host contribution, and
current facts cannot be presented as historical evidence.

## Canonical JSON locations

The existing operation envelope and `MemoryProvider`/`ProviderCall` interface
remain authoritative. The schema definitions below are shared by serializers;
the owned API values are validated runtime projections and do not define a second
wire protocol. All timestamps on this wire are UTC RFC3339 strings. Runtime API
timestamps are explicitly named UTC nanoseconds and match the host parser’s i64
nanosecond range. Application requests retain domain UTC microseconds; host mapping
uses checked multiplication by 1,000 and never silently rounds wire evidence.

| Location | Definition |
| --- | --- |
| Observation `source_identity.original_source` | `provider-observation-contract.schema.json#/$defs/sourceAttribution` |
| Observation or recall request `history_grant` | `provider-observation-contract.schema.json#/$defs/historyGrant` |
| Common replay request `history_grant` | The same `historyGrant`; required for replay |
| Recall candidate `provenance.original_sources` | One to 64 `sourceAttribution` values |
| Feedback/correction `target` | `provider-lifecycle-contract.schema.json#/$defs/lifecycleTarget` |
| Snapshot restore `disposition_checkpoint` | `provider-observation-contract.schema.json#/$defs/restoreDispositionCheckpoint` |

`sourceAttribution` has `source`, `origin_scope`, `source_sequence`, `occurred_at`,
`ingested_at`, and `validity`. `source` preserves `canonical_provider_id`,
`canonical_session_id`, `source_key`, nullable `stable_record_id`, `observation_id`,
nullable `source_revision`, and `content_sha256`. Revision is an opaque canonical
string, such as `cache_policy_r2`, not a counter derived from the envelope version.
Unknown revision is explicit `null`; omitting a required nullable property is
invalid. Revision and assertion validity are independent: an unknown revision
does not erase retained validity evidence. Admission reports the unknown revision
as degraded coverage. Older numeric envelope revisions do not prove content
revision and cannot be substituted into `original_source.source.source_revision`.

`original_source.source.content_sha256` is the lowercase SHA-256 of the canonical
JSON bytes of the original host-retained canonical source payload. It does not
hash projected message text and need not match the payload after hygiene
sanitization. The delivered canonical envelope has its own payload digest;
candidate and trace content digests identify their exact emitted UTF-8 bytes.
Providers preserve the host-validated original attribution digest because they
cannot reconstruct it from a sanitized projection.

Recorded `origin_scope` is `{ "state": "recorded", "exact_scope_identity": …,
"authority_ref": … }`. Legacy or delayed-import records use
`{ "state": "unavailable" }` or `{ "state": "ingestion_only" }`. A history grant
requires recorded original scope; an ingestion-time checkout is insufficient.
Legacy retained provider controls may carry unavailable original scope when the
host can independently authorize their delivery scope/source target. They must
not fabricate origin to delete a retained effect.

`historyGrant` contains `authorization_ref`, `policy_revision`, exact
`destination_scope`, `relation` (`exact_scope` or `same_checkout`), `sources`, and
`disposition_checkpoint`. Each source contains `attribution` plus
`current_disposition`. The checkpoint carries `exact_scope`, `authority_ref`,
nullable authority-local `authority_revision`, and `checked_at`. Constructing,
serializing, validating, or hashing these claims proves no authority. The host
validates the source/daemon identity bridge, original observation evidence and
current dispositions through its existing marker, observation, disposition and
deletion journal ports. Missing `history_grant` grants no cross-session reuse.

`lifecycleTarget` pins `provider_id`, `registration_revision`, `original_scope`,
`delivery_scope`, original `source`, and `reference`. The reference is
`{ "kind": "stable_memory_ref" | "recall_trace_ref" | "context_pack_item_ref",
"reference": … }`. A request candidate ID alone cannot authorize a mutation.
Correction compares the exact nonempty expected revision string; unknown revision
cannot satisfy a correction comparison. Switching active providers cannot redirect
feedback or correction to a different producing provider.

For `supersede` and `replace_content`, `correction.replacement` is the full admitted
canonical observation envelope, including `source_identity.original_source`. Its
opaque revision must differ from the target, and retained `validity.valid_from`
is the transition instant closing the prior revision. It is never an arbitrary
JSON value converted into text. Metadata replacements are narrow objects:
`change_validity` carries `valid_from` and nullable `valid_until`, `mark_incorrect`
carries `revoked_at`, and `restrict_scope` carries `exact_scope_identity`. Missing
required evidence, an unadmitted transition, or a scope restriction that does not
actually narrow the already admitted scope returns `invalid_request`.

Common replay retains `observation_batch_refs` and supplies
`resolved_observations: [{ "receipt_ref": …, "observation": … }]`. Every reference
must belong to the admitted batch refs, the host resolves and validates the
original canonical envelope, the history grant covers every source, and the
source sequence/order matches the admitted replay bounds. The provider never
looks up the host's canonical database to resolve these receipts.


Replay fixtures respect the existing contiguous sequence and acknowledged-cursor
contract. An ordinary Observe or SnapshotRestore does not itself establish a
replay acknowledgement. A page that changes no observation, effect or fence
leaves acknowledgement and generation unchanged. Successor pages therefore
include the full contiguous source range from the actual replay cursor, including
sources currently deleted by the host. Each grant entry carries that source's
actual current disposition; privacy checks still reject deleted sources. A fresh
key repeating a completed page binds `expected_previous_acknowledged_sequence`
to the actual prior Replay reply's `acknowledged_sequence`, while source-already-
applied and rejected counts retain their separate meanings. No fixture assumes
an acknowledgement from an unrelated Observe or restore generation.

Snapshot export returns `snapshot: { "identity": …, "bytes": [0..255],
"sources": […] }`. Identity uses the existing snapshot identity fields; its
length and hash bind the actual bytes. Restore receives the same `snapshot`, a
current `disposition_checkpoint`, and `source_dispositions: [{ "source": …,
"current_disposition": … }]`. The host supplies current effective disposition
including canonical privacy state and the destination provider's deletion fence.
Every declared source must be covered exactly once, and the provider checks that
the inventory equals the sources in its actual internal snapshot. Missing,
duplicate, partial or unknown state prevents ready recall. A checkpoint alone
does not authorize restoration. Both negotiated snapshot and total response-byte
limits apply.

The common `source_influence` inspection item contains `target`, original
`source`, `active`, `disposition`, `settled_feedback` counts for all five canonical
signals, nullable `last_feedback_receipt`, and a bounded
`provider_local_effect_summary`. This makes persisted feedback and the real
provider-local effect inspectable without pretending provider scores are
comparable. Neutral ignored feedback may truthfully report no ranking change.

Common `delivery_receipt` inspection uses exactly
`selector: { "idempotency_key": "<original delivery key>" }`. Each item has exactly
`operation_id`, `idempotency_key`, `provider_receipt_digest`, and
`stable_memory_ref`, copied from the retained original committing effect. The
operation and key remain bounded opaque strings so legacy identities survive
migration unchanged; the receipt is lowercase SHA-256. Multiple original targets
may yield separate items sharing the same original operation, key and receipt.
Neither a later replay key nor the inspection operation replaces that evidence.
Missing original evidence produces no synthesized item and reports partial
coverage or a warning. Current exact scope, byte/item bounds, redaction policy and
scope-bound cursor rules continue to apply.


The common migration fixture distinguishes each original public operation and
idempotency key as `retained`, `caller_observed_only`, or `unverified`. A retained
value must be read from the actual old durable representation. Caller-only
status requires both the actual original dispatch evidence and proof that the
old representation did not retain that public identifier. A caller-only key may
select an inspection; it must not be synthesized into a retained receipt item.
Unverified identity remains unresolved.

After each of the two migration restarts, a trusted fixture action independently
point-reads the actual owned records and original receipts while the restarted
provider remains live. It compares the original public reference, a declared
immutable old-record projection, exact stored receipt bytes, the original public
receipt digest or its verified durable basis, and every retained public
operation/key. Fingerprints exclude legitimate mutable runtime counters and
fields newly added by migration. Missing audit capability remains unknown;
changed retained evidence fails. The amended common program has 192 actions,
including both added physical audits; all existing actions remain required.

When both identifiers were retained, receipt inspection still requires the
exact original four-field item and successful terminal. When an identifier was
proved caller-only, the incomplete item must be omitted and the provider must
report partial coverage or a bounded nonempty warning. This outcome is degraded
only after the independent durable audit succeeds. Both paths require no effect,
unchanged generation and forbidden fallback. Invented or changed items, silent
loss, and changed durable records or receipts fail. Mixed evidence preserves
complete items and omits only known incomplete items. Reports separately expose
`legacy_delivery_identity` as `complete`, `degraded`, `unknown`, or `failed`;
overall compatibility with expected degradation does not claim complete
preservation of original delivery identifiers.

Common `trace` inspection uses exactly
`selector: { "stable_memory_ref": "<retained public target>" }`. Each item has
exactly `stable_memory_ref`, `content`, `content_sha256`, and `original_source`.
`content` is bounded admitted provider text; `content_sha256` hashes the exact
emitted UTF-8 bytes after any truncation. Truncation reports partial coverage and
does not change the full source digest in retained `original_source` attribution.
`original_source` is that retained `SourceAttribution`, or `null` when unavailable;
missing legacy origin, revision or validity is never inferred from the current
checkout or inspection clock. Privacy or content withholding sets `content`,
`content_sha256`, and `original_source` to `null` together and reports the
withholding through existing coverage/redactions/warnings. The requested stable
reference remains the item identity. These views are read projections, including
for migrated state; they grant no recall, replay or canonical source authority.

## Evidence projection and temporal semantics

Committed messages use the existing canonical message projection. Structured
observations retain their canonical payload and project only the nonempty string
fields, in the order specified by `common_source_attribution.structured_evidence_projection`:

- Source edits: `claim`, `status`, `assertion`, `reason`.
- Test executions: `assertion`, `status`, `approach`, `outcome`, `reason`.
- Feedback outcomes: `approach`, `outcome`, `signal`, `reason`, `claim`, `status`.

The text joins `field: value` lines. Each field is bounded to 8,192 UTF-8 bytes and
the complete projection to 32,768 bytes. A selected non-string field or a payload
without supported evidence is rejected. Unknown fields remain in canonical
evidence but never become a recalled message through raw JSON serialization.

Validity is start-inclusive and end-exclusive. Occurrence, source order and
ingestion remain separate from validity. Current and as-of claims describe the
requested instant; a later supersession or ordinary revocation cannot make an
earlier valid instant revoked. As-of cannot exceed evaluation time. An interval
requires start before end and may extend past evaluation time; it admits overlap
with retained validity, clipped at disallowed lifecycle events. Interval claims
may describe either the query-relevant state or the state at evaluation time when
the retained timestamps support that claim. History admits retained expired
records and excludes future-start records. Explicit `valid_until` still bounds
point and interval queries when inclusion flags are enabled.

Supersession time and replacement identity are paired. Unknown validity remains
explicit; `exclude` withholds it, while `degrade` and `allow_with_warning` admit it
with warnings and degraded/stale coverage under the host policy. A retained end or
disallowed lifecycle event still excludes a request wholly after that bound when
the start is unknown. Missing event time cannot support a claimed superseded or
revoked history state. Unknown fields or modes do not silently select current.
Every exclusion class applies before candidate limits. A candidate's
`content_sha256` hashes the exact emitted inline content bytes, including any
snippet truncation. Each `original_sources` entry retains the full original-source
content digest unchanged across truncation; provenance preserves that identity.
Source-content exclusions may compare those full original-source digests separately.

Canonical deletion, redaction and retention expiry, and provider-specific privacy
tombstones override every temporal mode and inclusion flag. Ordinary revocation
metadata is separate from those privacy authorities. Snapshot restore revalidates
current host dispositions before readiness, including on a fresh destination.
An unchanged authority checkpoint can be valid after a fresh read; source
disposition revision, snapshot generation and live provider generation are
independent orderings. Restore keeps live generation monotone and cannot alias
discarded public targets. Provider switching replays canonical history and never
loads a different provider's internal snapshot.

Same-delivery retries return the original committing operation/receipt.
Source-already-applied replay under a new delivery key has its own accounting
category. Every attempted item is applied, a delivery duplicate, source already
applied, rejected, or effect unknown. Unknown effects require reconciliation.
The flat replay counters are `applied_observations`, `duplicate_observations`,
`sources_already_applied`, `rejected_observations`, and
`effect_unknown_observations`; their sum equals `resolved_observations.length`.
`duplicate_observations` counts only repeated delivery keys. Existing delivery
receipt inspection supplies original effect evidence for source-already-applied
replay; its counter cannot claim that a new delivery performed the old mutation.
Provider-local deletion records a host journal fence before dispatch; offline
intent does not prove provider erasure. Only an explicit source-revision admission
permitted by deletion policy can authorize reappearance. Health, inspection,
export and recall are reads; pending privacy repair completes through a lifecycle
transition before they become ready.
