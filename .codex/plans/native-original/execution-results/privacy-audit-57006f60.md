# Current 570 privacy audit gate

Date: 2026-09-13

Audit mode: read-only source review. No Cargo command, product/source edit,
database action, commit, push, or runtime fixture was performed.

## Bounded decision

The bounded **source privacy gate is pending**. The active 570 detector and the
typed Claude history admission seam pass their independently bounded checks,
but the prior strict explain-trace shielding pass is withdrawn. The current
recall path can retain raw provider candidate IDs in a persisted explain trace
before and around `harden_candidate_identity`. This node stays pending until a
production fix and hostile-ID negative tests cover the persisted trace; it must
not be marked completed from the detector and history results alone.

This is a read-only source audit, not a product-readiness or Native parity
claim. The readiness matrix still has no production attempt and remains
blocked. rn-privacy-restore must independently recheck the detector/history
subchecks and the repaired trace boundary, with root review, before
rn-session-delivery is released; the real host/NCM cases remain with their
verification owners.

The audit found no detector mismatch and no unproved changed algorithm that
requires a detector or history-admission source correction. The accepted
explain-trace identity leak is a source-level blocker owned by the host and
registry lanes below. No detector, history-admission, Native, LCM, NCM, or
generated source was edited.

## Active identity and detector proof

The reviewed identity is:

| Identity | Value | Meaning |
| --- | --- | --- |
| Active original | 57006f60cb45bcee8487e73a40d4fad1a12ee2b | Unmodified upstream PR707 reference |
| Plan candidate metadata | 1fe250fed7ca615f1dfdcd580befeb276328e910 | Candidate pin in WORKER-RULES.md and the execution plan |
| Current execution HEAD | 7258fb62f7c11a2cfe00b57d902100b5f24e5ed2 | Worktree state observed for this audit |
| Readiness-matrix candidate | d17f115f4bf455bf205eb39ee1de0b17fb9eaf6b | Stale matrix inventory value; not used as current gate evidence |

The exact detector command was:

    file='crates/tracedecay-privacy/src/detector_kernel.rs'
    printf 'HEAD: '; git rev-parse HEAD
    printf 'HEAD subject: '; git show -s --format='%h %s' HEAD
    printf '570 blob: '; git rev-parse 57006f60cb45bcee8487e73a40d4fad1a12ee2b:"$file"
    printf 'current blob: '; git hash-object "$file"
    printf '570 sha256: '; git show 57006f60cb45bcee8487e73a40d4fad1a12ee2b:"$file" | shasum -a 256
    printf 'current sha256: '; shasum -a 256 "$file"
    git diff --no-ext-diff --exit-code 57006f60cb45bcee8487e73a40d4fad1a12ee2b -- "$file"
    rc=$?
    printf 'scoped diff exit: %s\n' "$rc"
    exit 0

Observed output:

    HEAD: 7258fb62f7c11a2cfe00b57d902100b5f24e5ed2
    HEAD subject: 7258fb62f docs(semantic): provision pinned Jina fixture
    570 blob: 9ce4488a34c5bd121d43c35c33f1daf925ab38ba
    current blob: 9ce4488a34c5bd121d43c35c33f1daf925ab38ba
    570 sha256: 5a101feaaecacd677f01685301c01229a85f5f6f279c5b510ae9509c07f13bfa  -
    current sha256: 5a101feaaecacd677f01685301c01229a85f5f6f279c5b510ae9509c07f13bfa crates/tracedecay-privacy/src/detector_kernel.rs
    scoped diff exit: 0

The plan candidate has the same blob as well. This exact comparison was run
without changing the worktree:

    file='crates/tracedecay-privacy/src/detector_kernel.rs'
    git rev-parse 57006f60cb45bcee8487e73a40d4fad1a12ee2b:"$file"
    git rev-parse 1fe250fed7ca615f1dfdcd580befeb276328e910:"$file"
    git rev-parse 7258fb62f7c11a2cfe00b57d902100b5f24e5ed2:"$file"
    git diff --no-ext-diff --exit-code \
      57006f60cb45bcee8487e73a40d4fad1a12ee2b \
      1fe250fed7ca615f1dfdcd580befeb276328e910 -- "$file"

It returned three times
9ce4488a34c5bd121d43c35c33f1daf925ab38ba, followed by exit 0.

The readiness matrix at
execution-results/readiness-matrix.md:9,33-34 and its JSON equivalent still
names candidate d17f115f4bf455bf205eb39ee1de0b17fb9eaf6b and says binary/run
evidence is pending. Its top-level status is blocked. That stale candidate
identity is recorded for reconciliation; it does not weaken or replace this
file-level 570/current proof.

The accepted source-boundary and reviewed-decision inputs at
SOURCE-BOUNDARY.md:55-57, REVIEWED-DECISIONS.md:58-60, and
WORKER-RULES.md:67-69 require this exact split: keep the historical b3-to-571
privacy record, prove the active 570 detector equality, review the typed
history seam, and keep generic admission plus original Native/LCM privacy
unchanged. The readiness matrix's privacy owner row and negative controls
(readiness-matrix.md:262-283,300) likewise require strict detector shielding,
scope isolation, no empty success, and a typed result rather than an inferred
runtime pass. This report follows those constraints.

## Historical b3 to 571 evidence

The previous report execution-results/privacy-isolation-design.md remains
historical and was not rewritten. Its detector finding is a separate b3-to-571
comparison:

    b3 detector blob:  9ce4488a34c5bd121d43c35c33f1daf925ab38ba
    571 detector blob: 820acd6a51b437647d9b27f684baa130878fc1e8
    historical diff:   61 insertions in detector_kernel.rs

The 571 hunk introduced rsplit_once("-sha256-") and the private
looks_high_entropy_token_core wrapper. It peeled one exact lowercase 64-hex
suffix before applying the predicate. Active 570/current code has the
whole-token predicate at detector_kernel.rs:179-272; it does not have that
wrapper. The current active tests are at :389-430, while the historical
report's :424-465 references the old wrapper tests.

Thus, b3-to-571 evidence explains why the product seam was needed historically,
but it is not an active detector restoration task. The active gate proves that
570 and the worktree retain the original whole-token algorithm. No caller may
recreate or broaden the historical suffix rule.

## Typed Claude history caller map

There is one production caller of the typed history exception. The other
matches below are the type implementation and focused tests.

1. Claude capture creates the opaque source value with the exact
   tracedecay-claude-observation-source-v1-sha256- prefix and a framed SHA-256
   digest at crates/tracedecay-capture/src/claude/mod.rs:7-27. Claude
   cursor/source identity reuses it at
   crates/tracedecay-sessions/src/runtime/hosts/claude/cursor.rs:8-15.

2. Claude source identification protects a sensitive transcript stem as a
   structural ID while leaving the already-opaque observation source digest
   unchanged at
   crates/tracedecay-sessions/src/runtime/hosts/claude/frames.rs:147-162.
   Strict JSONL framing, malformed/oversized handling, source ranges, and
   deferred coverage are handled at :181-320. The legacy parser verifies the
   cursor key, defers incomplete/backlogged input, refuses non-durable frames,
   and sends only sanitizer-issued records onward at
   crates/tracedecay-sessions/src/runtime/hosts/claude/parser.rs:188-308.

3. The canonical product caller is
   crates/tracedecay/src/daemon/retained_owner/observation_journey.rs:1092-1178.
   It first checks the canonical project scope (:1104-1112), builds the full
   provider envelope (:1139-1149), and, only for a supplied history grant,
   runs provider_history::validate_history_record before serializing the
   original source and grant (:1150-1158). It then constructs
   TrustedClaudeObservationSourceIdV1 from the validated provider/source key
   (:1159-1162) and chooses the typed admission method (:1173-1177). Without
   history, it uses strict admit_observation.

4. The type constructor at
   crates/tracedecay-memory-hygiene/src/lib.rs:261-296 accepts only provider
   claude, the complete exact prefix, and exactly 64 lowercase hexadecimal
   characters. It is syntax validation only; it carries no authorization.
   Authorization is supplied by the canonical caller's validated grant.

5. The typed sanitizer entry point is
   crates/tracedecay-memory-hygiene/src/lib.rs:558-573. Its projection
   recognizes only
   /source_identity/original_source/source/source_key and each matching
   /history_grant/sources/*/attribution/source/source_key at :335-400.
   The rg caller inventory confirmed no second product caller:

       observation_journey.rs:1159  TrustedClaudeObservationSourceIdV1::from_validated_source
       observation_journey.rs:1176  admit_observation_with_trusted_history_source
       lib.rs tests:1263-1491       constructor/projection/strictness tests only

native_provider.rs:3884-3975 has history source JSON extraction for its
provider-local candidate projection, but it does not construct the trusted
type or bypass canonical admission. The trusted exception therefore remains
owned by the canonical observation adapter.

## Admission privacy and provenance invariants

The current code preserves the following bounded rules.

* Generic ObservationSanitizer::admit and admit_observation remain strict
  (lib.rs:532-555). They classify the whole payload and all optional opaque
  extensions. Required extensions fail closed; optional extension mutation or
  withholding withholds the observation (:611-679). No global allowlist,
  exact-value skip, detector fork, or extension exemption exists.

* The typed path validates both the provider and exact scalar source value at
  each recognized path (lib.rs:425-464), allocates collision-free low-entropy
  placeholders (:466-478), and restores only a placeholder that is still at
  the expected path and unchanged (:407-421). Missing paths, changed values,
  wrong provider, duplicate matching grants, absent matching grant fields, and
  non-scalar source keys fail closed.

* sanitize_payload_inner computes the source digest from the untouched full
  envelope (lib.rs:737-750), scans the projected copy, invokes the unchanged
  canonical sanitize_memory_fact_payload (:768-789), restores the typed source
  fields (:790-797), applies transient redaction, and computes the sanitized
  digest from the restored envelope (:798-835). A source ID in canonical message
  content, an unrelated object field, or an extension is still scanned
  strictly.

* The canonical adapter rejects an envelope whose shape changed and checks that
  history metadata is byte/value equal to the original envelope after hygiene
  (observation_journey.rs:1268-1285). It binds the resulting payload and
  sanitization receipt to the exact source, settlement, scope, forget key,
  provenance digest, and idempotency key (:1286-1387).

* provider_history::validate_history_record requires the grant source to match
  the stored observation ID, sequence, provider, canonical session, source key,
  and canonical-payload content hash (provider_history.rs:1921-1953). The
  history reader also rejects original source metadata without a grant,
  requires exactly one grant source, and revalidates destination scope
  (:730-805). Source-fence digests bind origin profile/project/provider/session/
  source (:1673-1697), while serialized attribution retains source revision,
  observation/content hash, sequence, times, validity, origin scope, and
  authority reference (:1760-1788).

* A settled record that only exceeds hygiene shape ceilings becomes a typed
  UnclassifiablePayload withholding so the replay cursor can advance without
  writing provider bytes; detector/corpus faults, extension faults, and
  encoding faults stay errors and fail closed. The conversion is explicit at
  lib.rs:682-720 and the canonical caller at observation_journey.rs:1166-1195.
  Canonical evidence is never deleted or rewritten by admission.

The focused source tests document the intended hostile cases, although this
read-only node did not execute them: lib.rs:1292-1313 checks full-envelope
receipt equality; :1317-1335 checks strict raw-ID withholding; :1338-1357
rejects malformed, uppercase, short, non-hex, and extra-tail IDs; :1361-1385
checks collision-free restoration; :1389-1412 keeps matching content and
high-entropy suffixes strict; :1416-1452 rejects wrong provider and duplicate
grants; and :1456-1491 keeps credentials and payload secrets withheld.

## Claude and Codex host boundaries

Claude's source path is a privacy-preserving parsed transcript path. It derives
an opaque observation source from the transcript identity, protects the
session stem, strictly frames JSONL, and hands sanitizer-issued records to the
canonical journey. The typed exception is reachable only after a canonical
history grant is revalidated. It applies to source metadata in a history
envelope, never to Claude message text.

Codex remains a separate live locator and JSONL capture path. Its admission enum
distinguishes project and profile scope and optional session filtering at
crates/tracedecay-sessions/src/runtime/hosts/codex/observation.rs:404-448.
The source identity is provider codex plus a separate replay digest at :520-531;
no Claude trusted type can be constructed for it.

The no-follow/strict live locator is at
crates/tracedecay-sessions/src/runtime/hosts/codex.rs:1043-1080,1267-1429:

* both configured roots must be completely and stably enumerated under the
  deadline, with bounded entries/path bytes/depth;
* links, ambiguous candidates, foreign paths, invalid headers, and incomplete
  coverage return no locator;
* the candidate is opened with open_regular_read_no_follow, and native file
  identity, corpus identity, length/time identity, and canonical path are
  checked again;
* CodexLiveSessionTranscript explicitly grants no history authority; the
  caller must revalidate it after capture and before registration.

The retained Stop path uses the sealed source boundary at
codex/observation.rs:692-715. Bounded admission at :718-797 rejects wrong
sessions, checks ordinary/canonical cursor generations and positions, and only
enters current-message replay when the legacy cursor/file match is still true.
The request carries PersistedCursorUpdate::Replace, lazy bounded frame
preparation, cancellation, required replay start, end offset, and the sealed
witness at :799-846. The shared JSONL admission validates the no-follow page
and sealed frontier (jsonl_observation_admission.rs:126-155,2349-2477);
retryable cursor-CAS losses remain retryable, while a durable frame outcome
advances the cursor only after the admission result
(:1980-2020,2058-2090). A mismatched or cancelled source is deferred and does
not produce success.

The two host paths therefore share canonical scope/admission/journal
boundaries while retaining distinct source IDs, parsing, replay, and live
authority rules. Only Claude history reaches the typed source-field seam.

## Strict recall shielding

Recall has no path to the typed history exception. AdvisoryTextHardener is
constructed from a fresh strict ObservationSanitizer at
crates/tracedecay-memory-hygiene/src/recall_text.rs:591-613. It checks trust,
size, and the provider's original bytes through strict sanitizer.admit before
neutralizing markup and hidden/control characters (:630-710). Metadata labels
use the same strict scanner and containment check (:712-766); the raw string
scanner calls only admit, never the typed history method (:794-820).

cognitive_recall.rs:2684-2701 opens this gate and turns gate construction
failure into typed unavailable. The final provider candidate loop gates
explanation, provenance source/reason, candidate identity, and candidate
content at :2702-2743 and :2819-2889. Gate faults terminate the whole advisory
lane as unavailable (:2850-2867,2971-2987); refused identities become
host-minted digest-only
stand-ins (:2990-3024). Those checks still support the rendered content,
metadata, provenance, and final candidate-identity boundary. They do not prove
strict explain-trace shielding, because trace identity ledgers are assembled
from earlier raw values and are retained separately from the rendered pack.

### Accepted explain-trace blocker

The earlier sentence claiming that a provider cannot print a withheld source ID
through an explain trace is withdrawn. The exact raw-ID flow is:

* `cognitive_recall.rs:2563-2583` clones the admission, normalization, and
  selection receipts and constructs `host_withheld` from
  `outcome.unhydrated_reference_candidate_ids`, copying each provider
  `candidate_id` before any identity hardening.
* `cognitive_recall.rs:2707-2743` builds `explanations` keyed by each raw
  normalized candidate ID. The per-candidate loop at `:2786-2817` can append
  another raw ID to `host_withheld` when hydration excludes a candidate.
* `cognitive_recall.rs:2834-2848` calls `harden_candidate_identity` only after
  those ledgers exist. The alias map intentionally has the raw provider ID as
  its key, while the final `AdvisoryMemoryCandidateV1` at `:2883-2889` carries
  the hardened value. `control_bindings` also remains keyed by the raw ID at
  `:2875-2881`.
* `cognitive_recall.rs:2904-2919` stores the raw-bearing report,
  normalization, selection, withholding list, alias map, and explanation map
  in `AdvisoryRecallExplainV1`; its fields are explicit at `:3639-3661`.
  `prepare_trace` at `:3756-3782` adds selected raw IDs for final withholding
  and passes all of these values to the registry builder.
* `recall_explain_trace.rs:485-562` defines trace rows and host-withholding
  rows with `candidate_id`; `:655-688` accepts the raw stage ledgers. `place`
  at `:701-746` copies its raw argument into `RecallExplainItemV1`. The
  builder then copies raw IDs from admission denials at `:807-819`, selection
  deduplication and budget ledgers at `:822-846`, host withholding at
  `:849-862`, selected rows at `:864-888`, and unplaced admission rows at
  `:891-922`. The deduplicated host decision also carries raw
  `duplicate_of_candidate_id` at `:827-830`.
* `RecallExplanationRedactorV1` only redacts provider explanation text:
  `cognitive_recall.rs:3729-3741` and `recall_explain_trace.rs:748-760` do not
  sanitize candidate IDs, report identities, selection identities, decision
  identities, or alias-map keys.
* `RecallAdmissionLedgerV1::retain_explain_trace_with_control` serializes the
  complete trace at `cognitive_recall.rs:577-588` and writes each raw
  `item.candidate_id` plus serialized host decisions at `:653-681`. The
  persisted trace therefore remains a raw-ID surface even when the final
  rendered candidate used a host-minted stand-in.

This is the accepted blocker described as **admission/stage/dedup/host_withheld
raw provider candidate IDs surviving before `harden_candidate_identity`**. It
does not establish that provider content escapes the rendered recall pack; it
establishes that the separate persisted explain/audit trace is not yet shielded.

The production fix must be owned by `rn-host-context` for
`crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs` and
coordinated with `rn-registry` for
`crates/tracedecay-memory-provider-registry/src/recall_explain_trace.rs`:
every identity-bearing trace field and decision must be projected to a
host-safe stand-in before serialization, while reconciliation remains possible
without retaining raw provider IDs. Completion also requires negative tests
that exercise hostile IDs through admission denial, deduplication (including
`duplicate_of_candidate_id`), budget exclusion, pre-hardening host withholding,
and final selected withholding, then inspect both serialized trace JSON and a
reopened `RecallAdmissionLedgerV1` row for absence of the raw value. Those
production changes and tests have not been run in this read-only node.

## Exact scope and two-profile isolation

OwnedExactScope requires all seven fields and includes all seven in its
canonical digest at
crates/tracedecay-memory-provider-api/src/lib.rs:525-625:

profile_id, project_id, repository_identity, worktree_identity, branch_identity,
agent_session_id, and resolved_scope_digest.

The canonical journey copies the authoritative resolved scope and binds the
profile plus canonical session at observation_journey.rs:874-900. History
destination validation compares profile, project, repository, worktree, branch,
and resolved-scope digest at provider_history.rs:477-493;
validate_history_mount additionally checks provider identity/revision and the
mounted profile/scope at observation_journey.rs:3282-3308.

Static focused assertions include:

* observation_journey/tests/provider_history.rs:222-296,
  only_proven_original_sessions_enter_history_and_wire_cannot_change_them:
  only a proven source enters the grant, wire edits fail revalidation, and
  authority loss withholds the page;
* provider_history.rs:298-377,
  branch_and_profile_changes_never_relabel_original_sources: a foreign profile
  or changed branch is rejected, and a new bridge receives no grant;
* product_memory_provider_claude_host_journey.rs:2547-2567: a second
  destination preserves origin checkout fields but receives a distinct agent
  session rather than relabeling source A.

NCM receives the host-admitted observation only after this exact scope is
validated. NcmNamespace::from_exact_scope hashes all seven values and exposes
no raw profile/project/repository/worktree/branch/session identifiers
(crates/tracedecay-memory-provider-ncm/src/lib.rs:98-123). Its surface_payload
path removes exact-scope identity, rewrites caller IDs, checks recursively and
on serialized bytes for raw scope/caller components, and hashes the projected
payload (:463-515,1966-2075). This is a second provider boundary, not an
alternative to host admission.

## Staged, Native, and NCM separation

The product staged store is explicitly derivative provider state. Its module
documentation at
crates/tracedecay/src/daemon/retained_owner/native_staged_observations.rs:1-38
states that rows are copies of already admitted/settled observations, are not
canonical memory facts, and are promoted separately. Recall filters all seven
stored scope fields, source fences, history state, and exact-scope/grant
eligibility (:805-915). The Native provider opens this store as a provider
local adapter and stages admitted bytes without writing canonical facts
(native_provider.rs:294-360). Its staged recall is restricted to the call's
exact scope (:2142-2166), and staged candidates remain provider-attested
advisory text that the host hardens downstream (:2710-2720).

The original Native facts/session/LCM services and their privacy helpers remain
separate protected routes. A staged candidate cannot be counted as original
Native parity, cannot bypass the canonical host privacy gate, and cannot be
promoted implicitly by this audit.

NCM performs its existing handshake/readiness/admission at
memory-provider-ncm/src/lib.rs:1207-1375, then projects an already host-admitted
observation. For an Observe payload, project_observation_sources retains only
canonical message text and converts source identity to a namespace-bound opaque
key (:1779-1890); common attribution applies its own cross-scope/history checks
at common.rs:370-425. NCM does not call the shared detector or
sanitize_memory_fact_payload; it receives the host's admitted bytes and then
applies its own opaque projection. No NCM source, worker, model, namespace,
manifest, or contract change is part of this gate.

The real NCM host fixture names are present but ignored/manual-runtime scoped:

* real_ncm_observer_receives_shipped_claude_hooks_while_native_answers_context
* real_ncm_active_recalls_shipped_claude_session_history_with_native_disabled

Their presence is source coverage only. It is not a runtime pass.

## Stale references and unresolved owners

The old b3-to-571 report is intentionally untouched. Its detector lines,
product sanitizer lines, and host line ranges describe the historical head;
the current anchors for this audit are the paths and ranges cited above. In
particular, the old report's proposed seam is now implemented at
memory-hygiene/src/lib.rs:261-835 and its sole production caller is the
current canonical adapter at observation_journey.rs:1150-1177.

The following work remains explicitly outside this pending source-audit node:

| Owner | Unresolved work | Required evidence |
| --- | --- | --- |
| rn-host-context | Remove raw IDs from the mounted recall explain inputs and final-withholding additions before they reach the trace builder; preserve exact reconciliation and host-safe aliases | Production fix in `cognitive_recall.rs`, plus hostile-ID negative coverage for unhydrated, hydration-excluded, and final-withheld candidates; serialized and reopened trace contains no raw provider ID |
| rn-registry | Enforce the host-safe identity contract at `build_recall_explain_trace`, including report, selection, dedup, host-withheld, and decision fields | Builder/ledger negative tests prove raw IDs cannot enter `RecallExplainTraceV1`, decision JSON, or retained SQLite rows; no raw-ID fallback remains |
| rn-privacy-restore | Recheck 570/current detector and typed history seam after the explain-trace fix; root review of the no-source-change result and the repaired strict trace boundary | Repeat the file proof, hostile source-field cases, strict recall and persisted-trace cases, and provenance assertions without Cargo claims in this report |
| rn-host-fixtures | Keep real Claude/Codex journey inputs aligned with the accepted runner and current composition | Actual shipped host entry points, exact source attribution, cleanup, and refusal/restart captures |
| rn-verify-claude | Run the accepted production Claude comparison after rn-build | Separate 570/candidate binaries/profiles, exact final delivery, no duplicate/staged substitution, and privacy-negative controls |
| rn-verify-codex | Run the Codex no-follow locator/sealed JSONL comparison after rn-build | Stable/mismatch/rollback/cancel evidence, cursor/write atomicity, exact session identity, and no empty success |
| rn-session-delivery | Release only after privacy restore, history owner, and Native-port prerequisites are accepted | Canonical delivery with no staged Native acknowledgement and unchanged NCM receipts |
| Root/readiness owners (rn-reference, rn-integration-readiness) | Reconcile the stale d17f115... matrix candidate and execute blocked production rows | Independent binary/process/store hashes and nonzero real case counts; matrix status must remain blocked until then |

The detector and typed history subchecks have no source mismatch, but the
accepted persisted explain-trace blocker is still source-level. The privacy
todo remains pending; no runtime owner may treat this report as product build
success or as strict explain-trace approval.

## Verification record

Commands run in the designated worktree:

    git rev-parse HEAD
    git show -s --format='%h %s' HEAD
    git rev-parse 57006f60cb45bcee8487e73a40d4fad1a12ee2b:crates/tracedecay-privacy/src/detector_kernel.rs
    git hash-object crates/tracedecay-privacy/src/detector_kernel.rs
    git show 57006f60cb45bcee8487e73a40d4fad1a12ee2b:crates/tracedecay-privacy/src/detector_kernel.rs | shasum -a 256
    shasum -a 256 crates/tracedecay-privacy/src/detector_kernel.rs
    git diff --no-ext-diff --exit-code 57006f60cb45bcee8487e73a40d4fad1a12ee2b -- crates/tracedecay-privacy/src/detector_kernel.rs
    git diff --stat b3b43410e47115056f2066449aafa1822bbb6049 571daf3a9612e5247443e4da3a107b542686c1ef -- crates/tracedecay-privacy/src/detector_kernel.rs
    rg -n "TrustedClaudeObservationSourceIdV1|admit_observation_with_trusted_history_source" crates
    git grep -n -I -E "RecallExplainTrace|candidate_id|harden_candidate_identity" -- crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs crates/tracedecay-memory-provider-registry/src/recall_explain_trace.rs crates/tracedecay-memory-provider-registry/src/recall_admission.rs crates/tracedecay-memory-provider-registry/src/recall_selection.rs

The detector commands returned the hashes and exit codes recorded above; the
historical diff was one file with 61 added lines; the caller search found only
the canonical adapter plus hygiene implementation/tests. The source inventory
above exposed the accepted raw-ID trace path. No Cargo/test command was run
because this node's worker rules explicitly make build/runtime work a separate
owner responsibility; the production fix and negative persisted-trace tests
remain unresolved with rn-host-context/rn-registry.
