# Source command receipt integration draft

Status: APPLIED after root review, together with the interruptible-directory prerequisite. Preferred artifact: `pm-source-command-receipt.integration.patch`, a single new-file patch for `crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs`. It replaces the feedback-only draft. `pm-source-command-receipt.delta.patch` is only for applying after the current feedback-only patch, not in addition to the combined full patch. `pm-source-command-receipt.draft.rs` is the readable combined draft. The applied file matches that draft byte for byte; no delta was additionally applied.

First-use directory creation now requires the separately reviewed `pm-interruptible-directory.integration.patch` for `PrivateStoreIo::create_dir_all_durable_interruptible`. The full receipt patch already includes its `prepare_root` hook and tests. `pm-source-command-receipt.interruptible-root.delta.patch` contains only that hook and the first-use tests for a checkout that already applied the prior combined receipt draft; do not apply it after the refreshed full receipt patch. The private filesystem strict-sync fix remains a separate, already-live root-approved change.

## Closed scope and final APIs

Root approved exactly two immutable accepted command variants, `FeedbackAssertion` and `DeletionCommand`, under the existing `RecallAdmissionLedgerV1`. The root is fixed at `<ledger parent>/recall-source-commands-v1`, envelope magic `TDSCMD01`, schema version 1. No new database/table/service or canonical fact mutation is introduced. Both reuse the reviewed private immutable frame, original control, one root admission lock, no-replace durable publication, strict decode, retry comparison, and finite quota. The feedback-only design details remain in `pm-feedback-receipt.integration-notes.md` where not superseded here.

The existing ledger exposes:

- `accept_feedback_assertion(&RequestContext, &ProviderFeedbackRequestV1, &LifecycleTarget, &SourceAttribution, &OperationControl) -> Result<AcceptedFeedbackAssertionV1, FeedbackAssertionErrorV1>`.
- `retained_feedback_assertion(&RequestContext, &OperationControl) -> Result<AcceptedFeedbackAssertionV1, HostSourceCommandErrorV1>`.
- `accept_deletion_command(&RequestContext, &ProviderDeleteBySourceRequestV1, &LifecycleTarget, &SourceAttribution, &OperationControl) -> Result<AcceptedDeletionCommandV1, HostSourceCommandErrorV1>`.
- `retained_deletion_command(&RequestContext, &OperationControl) -> Result<AcceptedDeletionCommandV1, HostSourceCommandErrorV1>`.

Both opaque values provide `operation_id() -> &str`, `idempotency_key() -> &str`, `accepted_at() -> UtcMicros`, and `matches(context, public_request, resolved_target, full_original_attribution) -> Result<bool, HostSourceCommandErrorV1>`. Feedback alone provides `canonical_outcome_receipt() -> &str`; deletion records omit that field entirely and cannot be converted into the feedback capability. `FeedbackAssertionErrorV1` remains an alias for `HostSourceCommandErrorV1` so the coordinated feedback API is stable.

Error variants: `Invalid(&'static str)`, `Missing`, `Conflict`, `Corrupt(&'static str)`, `Control(TerminalCode)`, `CapacityExceeded`, `Storage { publication: FeedbackAssertionPublicationV1::{NotAttempted, DurabilityUnknown}, source: io::Error }`. A storage uncertainty concerns HOST command persistence and does not imply provider contact or provider effect. No provider dispatch follows any receipt error. A completed durability barrier returns the persisted accepted command even if cancellation races afterward; the controller separately checks live control before provider contact and preserves truthful accepted-host/no-provider-effect reporting.

## Frozen operation identity spelling

Request lookup key and provider idempotency key are the same bare 64-character lowercase SHA-256 digest:

`sha256_hex(canonical_json_bytes(&("tracedecay.host-source-command.request.v1", authenticated_actor, full_resolved_host_scope, operation_wire, caller_request_id)))`.

`operation_wire` is exactly `feedback` or `delete_by_source`. The complete host `ResolvedScope`, including its reference and scope digest, is serialized. The body target, original attribution, signal and deletion fence revision are excluded from the lookup key so a changed bound body produces a typed conflict. No prefix is added or stripped from the idempotency key; the frozen public result contract requires bare 64-character hexadecimal text.

The operation UUID is lowercase UUIDv7. Its time bits are the original real host acceptance time in milliseconds; remaining bits come from SHA-256 of canonical `("tracedecay.host-source-command.operation.v1", request_lookup_key)` with version/variant bits set. UUID, key, accepted timestamp, exact record bytes, and feedback receipt reference are reused from durable storage on identical retries. The feedback-only canonical receipt reference is `host-feedback-assertion-v1:<request_lookup_key>`.

## Required deletion sequencing

Acceptance binds the complete ORIGINAL public deletion request (`source`, `mode`, `expected_fence_revision`, `include_snapshots`) and the exact host-resolved stable target/full source attribution. It is an accepted command identity, not the deletion fence or a successful erasure claim.

On retry the controller reuses the accepted operation UUID and original body when looking up the host fence intent on the original observation journal, including when the producing provider is disabled. It must not replace the original expected fence revision with the latest fence or generate a fresh operation UUID. The existing host fence transaction and later provider verification remain separate owned effects. Root/source_controls owns their sequencing and real application outcome assembly; this module never manufactures those effects.

Feedback evidence references are bounded caller claims retained unchanged. This producer performs no independent reference resolution and establishes neither objective usefulness nor authority from them. The controller still authorizes the actual source/target/current disposition through existing host owners before admission and at use time as appropriate.

## Small shared filesystem helper surface

Root approved sibling `provider_control::portability` directly importing only these `pub(super)` filesystem helpers from `provider_control::feedback_receipt`:

- `prepare_root(&Path, &OperationControl) -> Result<()>`.
- `acquire_admission_lock(&Path, &OperationControl) -> Result<File>`.
- `ArtifactQuota { maximum_files: usize, maximum_bytes: u64, maximum_record_bytes: usize }` (fields visible to sibling modules).
- `available_artifact_bytes(&Path, ArtifactQuota, &OperationControl) -> Result<usize>`.
- `check_quota(&Path, incoming_bytes: usize, ArtifactQuota, &OperationControl) -> Result<()>`.
- `publish_private_artifact(&Path, &[u8], &OperationControl, maximum_record_bytes: usize) -> Result<()>`.
- `confirm_existing_durability(&Path, &File, &OperationControl) -> Result<()>`.

`available_artifact_bytes` is the single bounded quota loop; `check_quota` delegates to it and compares the complete incoming encoded length. It reserves the fixed root lock and the incoming file overhead before returning the remaining bytes capped by `maximum_record_bytes`. The result bounds the COMPLETE ENCODED carrier, not its raw provider payload. Portability must conservatively subtract frame/checksum/metadata bytes and account for encoding expansion when deriving the minimum of request, negotiated, and available raw payload budgets. Hold the root admission lock through calculation/publication, or reacquire it and recheck the actual final encoded bytes immediately before publication; the returned value is not a reservation.

`prepare_root` passes the original `OperationControl` into the interruptible private hierarchy creator. The callback captures the exact terminal code before returning an I/O interruption; the host maps that captured code back to `Control`, preserving cancellation versus deadline expiry without inspecting an I/O message. First-use directory sidecar contention is checked within the actual nonblocking acquisition loop. Individual filesystem calls and started durability barriers are not preempted. Root preparation may leave already-published ancestors after interruption, but publishes no accepted command receipt.

Their error is `HostSourceCommandErrorV1`, and callers map it to their own typed operation error. Callers provide only host-fixed roots and host-owned bounds, prepare/validate their root, hold its admission lock over lookup+quota+publication, and own all typed artifact encoding, read bounds and validation. This is direct primitive reuse, not a generic artifact service. Portability has its own sibling root/quota and larger record bound. Command admission remains pinned to 16,384 files, 256 MiB accounted bytes, and 256 KiB per command receipt. Its publication wrapper and strict decoder still reject larger command frames. The shared scan charges 4 KiB root reserve plus full existing/new bytes and 4 KiB per file, including crash staging, and never deletes accepted retries or orphan staging.

## Validation and remaining actual journeys

The full draft passes `rustfmt --edition 2024 --check`; the full new-file patch passes `git apply --check`. No Cargo/build/test execution occurred. Embedded tests are drafted, not claimed passing.

The combined suite retains the feedback tests and adds deletion restart/retry preservation of expected fence revision, UUID/key/time, no feedback receipt, correct kind separation, conflicts for body/fence/target changes, concurrent identical deletion command publication, and a check that larger portability helper bounds do not widen the command cap. The feedback idempotency test now asserts the frozen bare 64-character spelling. A quota test checks available-byte accounting for existing orphan bytes, incoming/existing file overhead, per-record and file-count caps, final encoded-length recheck, and original live cancellation.

Root must run compiler/appropriate checks after wiring the module. Real retained-route tests must prove actual provider invocation count zero for receipt corruption, held-lock cancellation, exhausted quota and prepublish/storage uncertainty until durable acceptance is reverified. Local gate tests explicitly do not establish that route integration. Subprocess restart/concurrent-process tests must reuse this ledger-owned root and original UUID/key. The actual deletion journey must verify the same accepted operation UUID opens the original host fence receipt on retry, and that accepted-command persistence never masquerades as deletion completion. Provider-local result receipts and `ApplicationOutcome` construction remain the outcome owner's scope.
