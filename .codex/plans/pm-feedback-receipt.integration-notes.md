# Feedback assertion receipt integration draft

Status: UNAPPLIED. Only `crates/tracedecay/src/daemon/retained_owner/provider_control/feedback_receipt.rs` is represented by `pm-feedback-receipt.integration.patch`; `pm-feedback-receipt.draft.rs` is the readable draft. No live source, Cargo, manifest, DB, service, configuration, commit or push changes were made.

## Reviewed ownership and contract

The existing `RecallAdmissionLedgerV1` owns two methods: `accept_feedback_assertion(context, request, target, original_attribution, control)` and `retained_feedback_assertion(context, control)`. Both return opaque `AcceptedFeedbackAssertionV1` only after durable verification. Its getters are `canonical_outcome_receipt()`, `operation_id()`, `idempotency_key()`, `accepted_at()`, and `matches(context, request, target, original_attribution)`.

`RequestContext` supplies the actual authenticated actor, request ID and full resolved host scope. The complete public request, producing provider/revision/exact delivery scope, resolved stable memory target and complete original source identity/scope/sequence/occurrence/ingestion/validity are bound. Target/source and original-scope equality is checked before constructing the private record. `matches` checks all bound fields. Evidence references remain bounded caller claims; the receipt neither resolves nor promotes them to objective evidence. The producer accepts only host-resolved `StableMemoryRef` targets and recorded original scope; missing authority never creates a receipt.

The receipt accepts an explicit user assertion. It does not establish usefulness, provider completion, or canonical fact authority. Public request DTOs cannot supply `canonical_outcome_receipt`. No provider dispatch is allowed before acceptance succeeds.

## Storage and retention

The feedback-only draft uses `<existing ledger parent>/recall-feedback-v1/`, never caller paths. File name is a SHA-256 request key, and one fixed `.admission.lock` serializes immutable retry lookup, admission quota and publication. The key includes domain/version, authenticated actor, full resolved host scope, operation feedback and request ID. Producing provider, target, signal and all other body values are excluded from the key so changed bodies conflict.

Each immutable artifact is an 8-byte magic/version, big-endian u32 payload length, canonical strict typed JSON and trailing SHA-256 over exact framed bytes. Reads use the one validated private file handle, maximum 256 KiB, chunked original-control checks, exact length/checksum/schema/canonical-byte checks and semantic revalidation. Publication uses existing `PrivateStoreIo::create_dir_all_durable`, `create_private_file`/`open_private_file`, fs2 try-lock, and `framed_log::with_owned_temp_publish` plus `rename_noreplace` and strict parent-directory fsync. No new dependencies are required.

New artifacts are admitted only below 16,384 charged regular files and 256 MiB accounted bytes. Accounting reserves 4 KiB for the fixed root lock and 4 KiB per retained/staging file, and includes the full new file. Directory enumeration stops at the count cap and checks original live control per entry. Orphan staging files are charged, symlinks/nonregular entries refuse admission, and no sweeper/cache/eviction exists. Identical retries verify and resync before quota checks, so saturation does not prevent retry reconciliation. Records share the ledger owner's lifetime; silently deleting a live retry record would destroy conflict detection.

UUIDs remain canonical lowercase UUIDv7: the original host acceptance timestamp supplies the millisecond time field, and a domain-separated request digest supplies the remaining bits. UUID, idempotency key, receipt ref, timestamp and bytes are reused exactly after restart. Existing receipt validation reestablishes file/directory durability before returning an accepted capability, including complete records left by uncertain earlier publication.

All lock polling, framing and directory/file iteration use the original `OperationControl`. Individual kernel open/read/write/fsync calls are not interruptible; the draft bounds bytes/iterations and runs on the controller's existing blocking pool rather than claiming a hard bound on an OS call. Successful final durability is never overwritten by a late cancellation.

## Exact proposed hooks (not applied)

1. In `provider_control.rs`, declare `mod feedback_receipt;` under the existing provider-host feature boundary.
2. Resolve and authorize the retained source, producing namespace, current source disposition through existing host owners. Preserve bounded evidence references as caller claims without requiring a separate evidence resolver. Supply the resulting `LifecycleTarget` and full `SourceAttribution`; receipt structural validation grants no authority.
3. On the controller's existing blocking pool call `ledger.accept_feedback_assertion(...)` using the original request context and original operation control. Any error prevents provider dispatch. `Conflict`, `CapacityExceeded`, corrupt/missing storage and stopped control stay typed. `Storage { publication: DurabilityUnknown }` means the HOST ASSERTION may have persisted; it never implies that the provider was called or committed.
4. After acceptance, preserve that accepted value even if live control has stopped. Report durable host assertion acceptance separately from no provider effect. Do not relabel the persisted receipt cancelled or erase it.
5. Build readiness/admission and the actual provider feedback call with the original accepted operation UUID and idempotency key. Populate wire `canonical_outcome_receipt` only from the opaque getter. Revalidate current admission/disposition at use time, and check the same original live control before dispatch.
6. Pass the accepted value into the projection owner. The mapper calls `matches(...)` and requires receipt/op/key equality with the actual admitted call and reply. The receipt is not an application provider-effect receipt; common `ApplicationOutcome` assembly remains root/executor scope.

## Validation evidence and remaining integration checks

`rustfmt --edition 2024 --check .codex/plans/pm-feedback-receipt.draft.rs` passed. `git apply --check .codex/plans/pm-feedback-receipt.integration.patch` passed. No Cargo or tests were run; Rust tests are drafted, not measured passing.

The embedded tests cover disk reopen identical and conflicting retries; actual concurrent thread locks with identical/conflicting bodies; authentication/request scope key partitioning; full source attribution matches; tamper/missing/oversize/unknown-field/overflow rejection; original-token lock cancellation/deadline; prepublish refusal, retained postpublish uncertainty and retry reconciliation; late cancellation after successful publication; quotas, reserves and charged orphans without eviction; immutable no-replace and symlink/nonregular refusal.

Required controller-level tests remain outside this file ownership: inject receipt-root refusal, held-lock cancellation, saturation, checksum corruption and postpublish acknowledgement loss through the real retained feedback route; assert the actual mounted provider invocation count remains zero until a durable receipt is verified. The local gate test explicitly does not claim to prove real route wiring. Also run a subprocess restart/concurrent-process retry fixture against the same ledger-owned root, then prove exactly one receipt and unchanged UUID/key across the real provider feedback/restart journey. Projection should reject forged accepted metadata and each actual call/op/key mismatch. Root owns Cargo and aggregate verification.

## Superseded by the combined narrow deletion extension

Root requested reusing the same immutable record/lock/quota for a closed FeedbackAssertion|DeletionCommand accepted-command enum. `AcceptedDeletionCommandV1` will preserve operation UUID, idempotency key and acceptance time without exposing a feedback outcome receipt or claiming a deletion fence/effect. Suggested final root `recall-source-commands-v1`, magic `TDSCMD01`, no generic operation/storage framework. Original public delete expected_fence_revision/mode/include_snapshots and full resolved source bind the immutable command. Current host-fence state must not alter its retry body or operation identity. This extension is now available in `pm-source-command-receipt.integration.patch` (full replacement) and `pm-source-command-receipt.delta.patch` (delta after the feedback-only patch). Prefer the full combined patch. See `pm-source-command-receipt.integration-notes.md` for final API and shared helper details.
