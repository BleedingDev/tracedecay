//! Explicit retention and privacy deletion rules.
//!
//! The rules are API surface, not comments:
//!
//! * **Age.** A row's effective expiry is
//!   `min(privacy.expires_at, admitted_at + max_age_for(retention_class))`.
//!   `provider_may_extend_expiry` is false and is enforced by construction:
//!   nothing here ever reads an expiry out of a receipt.
//! * **Order.** A non-terminal delivery is never deleted. It is first moved to
//!   `Expired` *and* given a terminal receipt, then its content is purged.
//!   Nothing is silently dropped (ADR-0005 invariant 7).
//! * **Content vs. audit.** Purge and forget null the payload bytes, the
//!   extension bytes, *and the whole hygiene binding*, then stamp
//!   `content_forgotten_at`. The binding is content-derived evidence — its
//!   receipt JSON restates the pre-sanitization digest — so it goes when the
//!   content goes, all four columns together, never leaving a partially
//!   cleared combination no reader can decode. Payload digests and delivery
//!   receipts survive; they hold no content. Rows are fully deleted only once
//!   every delivery for them is terminal and older than
//!   `receipt_retention_micros`.
//! * **Nothing is left undeliverable.** Content-forgotten rows can never be
//!   delivered, so forget terminalizes their deliveries and the sweep
//!   terminalizes any it still finds non-terminal. A stranded pending row is
//!   never left for a dispatcher to discover.
//! * **Withheld records age out too.** A withheld audit row names a
//!   `forget_source_key` and carries a `withheld_at` instant, so deletion
//!   reaches it and `receipt_retention_micros` bounds it.
//! * **Postcondition.** [`ObservationRetentionPortV1::verify_forgotten`] is a
//!   real re-query returning a boolean (ADR-0005 invariant 8), and it fails
//!   closed unless the write-ahead log has actually been truncated — otherwise
//!   the purged bytes would still be readable in the `-wal` sidecar.
//! * **Bounded.** Every sweep does at most `sweep_batch_rows` and reports what
//!   remains — counting *every* class of remaining work, not only the classes
//!   it happened to purge — so a large backlog converges over calls.
//!
//! [`ObservationRetentionPortV1::verify_forgotten`]: crate::ObservationRetentionPortV1::verify_forgotten

use crate::envelope::RetentionClassV1;
use crate::error::ObservationJournalError;
use crate::identity::{ForgetSourceKeyV1, SOURCE_EVENT_ID_MAX_BYTES, require_bounded};

/// Bounds on age, queue size, retries, and sweep work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionPolicyV1 {
    /// Maximum age of `ephemeral` content.
    pub ephemeral_max_age_micros: i64,
    /// Maximum age of `session` content.
    pub session_max_age_micros: i64,
    /// Maximum age of `project` content.
    pub project_max_age_micros: i64,
    /// Maximum age of `profile` content.
    pub profile_max_age_micros: i64,
    /// How long a fully terminal row's audit trail is kept after its content
    /// was purged.
    pub receipt_retention_micros: i64,
    /// Maximum non-terminal rows per provider instance.
    pub max_queue_items: u64,
    /// Maximum non-terminal queue bytes per provider instance.
    pub max_queue_bytes: u64,
    /// Maximum delivery attempts before `Exhausted`.
    pub max_attempts: u32,
    /// First retry delay.
    pub backoff_base_micros: i64,
    /// Retry delay ceiling.
    pub backoff_max_micros: i64,
    /// Maximum rows one sweep call may touch.
    pub sweep_batch_rows: u32,
}

impl RetentionPolicyV1 {
    /// Rejects a policy that cannot bound anything.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        let invalid =
            |field: &'static str| ObservationJournalError::InvalidRetentionPolicy { field };
        for (value, field) in [
            (self.ephemeral_max_age_micros, "ephemeral_max_age_micros"),
            (self.session_max_age_micros, "session_max_age_micros"),
            (self.project_max_age_micros, "project_max_age_micros"),
            (self.profile_max_age_micros, "profile_max_age_micros"),
            (self.receipt_retention_micros, "receipt_retention_micros"),
            (self.backoff_base_micros, "backoff_base_micros"),
            (self.backoff_max_micros, "backoff_max_micros"),
        ] {
            if value <= 0 {
                return Err(invalid(field));
            }
        }
        if self.backoff_max_micros < self.backoff_base_micros {
            return Err(invalid("backoff_max_micros"));
        }
        if self.max_queue_items == 0 {
            return Err(invalid("max_queue_items"));
        }
        if self.max_queue_bytes == 0 {
            return Err(invalid("max_queue_bytes"));
        }
        if self.max_attempts == 0 {
            return Err(invalid("max_attempts"));
        }
        if self.sweep_batch_rows == 0 {
            return Err(invalid("sweep_batch_rows"));
        }
        Ok(())
    }

    /// Maximum age admitted content of one retention class may reach.
    #[must_use]
    pub const fn max_age_for(&self, class: RetentionClassV1) -> i64 {
        match class {
            RetentionClassV1::Ephemeral => self.ephemeral_max_age_micros,
            RetentionClassV1::Session => self.session_max_age_micros,
            RetentionClassV1::Project => self.project_max_age_micros,
            RetentionClassV1::Profile => self.profile_max_age_micros,
        }
    }

    /// Capped exponential backoff for the given one-based attempt number.
    #[must_use]
    pub fn next_attempt_delay(&self, attempt_number: u32) -> i64 {
        let shift = attempt_number.saturating_sub(1).min(31);
        let multiplier = 1i64.checked_shl(shift).unwrap_or(i64::MAX);
        self.backoff_base_micros
            .checked_mul(multiplier)
            .unwrap_or(i64::MAX)
            .min(self.backoff_max_micros)
    }
}

/// What one bounded retention sweep did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetentionSweepReceiptV1 {
    /// Rows whose content was purged.
    pub payloads_purged: u32,
    /// Non-terminal deliveries moved to `Expired`, each with a terminal receipt.
    pub deliveries_expired: u32,
    /// Deliveries of already content-forgotten rows moved to `Forgotten`, each
    /// with a terminal receipt. These can never be delivered — their content is
    /// gone — so the sweep terminalizes them rather than leaving them queued.
    pub deliveries_forgotten: u32,
    /// Fully aged-out journal rows deleted.
    pub journal_rows_deleted: u32,
    /// Receipts deleted with those rows.
    pub receipts_deleted: u32,
    /// Withheld audit rows aged past `receipt_retention_micros` and deleted.
    pub withheld_rows_deleted: u32,
    /// Whether the write-ahead log was checkpointed and truncated after the
    /// sweep committed, so no purged page image survives in the `-wal` sidecar.
    pub wal_truncated: bool,
    /// Candidates the batch bound left behind, across *every* class of sweep
    /// work. Non-zero means "call again".
    pub remaining_candidates: u64,
}

/// A privacy deletion request against one forget-source key.
///
/// Deletion has no knobs. Content, the hygiene binding that describes it, and
/// the withheld records that name the same key all go together, and every
/// delivery that has not settled is terminalized — an undelivered row pointing
/// at content that no longer exists is not a state this store will leave
/// behind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgetSourceRequestV1 {
    /// Key to forget.
    pub forget_source_key: ForgetSourceKeyV1,
    /// Operator-supplied reason, recorded on the terminal receipts.
    pub reason: String,
    /// Instant the request was made.
    pub requested_at_unix_micros: i64,
}

impl ForgetSourceRequestV1 {
    /// Revalidates the deletion request.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        require_bounded(
            self.forget_source_key.as_str(),
            "forget_source_key",
            SOURCE_EVENT_ID_MAX_BYTES,
        )?;
        require_bounded(&self.reason, "reason", SOURCE_EVENT_ID_MAX_BYTES)
    }
}

/// What one privacy deletion did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgetReceiptV1 {
    /// Key that was forgotten.
    pub forget_source_key: ForgetSourceKeyV1,
    /// Journal rows the key matched.
    pub journal_rows_matched: u64,
    /// Rows whose content bytes were zeroed.
    pub payloads_zeroed: u64,
    /// Rows whose hygiene binding was cleared with the content.
    pub sanitization_bindings_cleared: u64,
    /// Deliveries moved to `Forgotten`.
    pub deliveries_forgotten: u64,
    /// Withheld audit rows the key matched and that were deleted.
    pub withheld_rows_deleted: u64,
    /// Receipts kept as digest-only audit.
    pub receipts_retained: u64,
    /// Whether the write-ahead log was checkpointed and truncated after the
    /// purge committed. `false` means a concurrent reader held the log open and
    /// the purged pages may still be readable in the `-wal` sidecar: the
    /// deletion is incomplete until a later call truncates it.
    pub wal_truncated: bool,
    /// Instant the deletion completed.
    pub completed_at_unix_micros: i64,
}

/// The re-queried postcondition of a privacy deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgetVerificationV1 {
    /// Key that was verified.
    pub forget_source_key: ForgetSourceKeyV1,
    /// Journal rows still matching the key.
    pub journal_rows_matching: u64,
    /// Matching rows that still hold content bytes.
    pub rows_with_content_remaining: u64,
    /// Matching rows that still hold a hygiene binding.
    pub rows_with_binding_remaining: u64,
    /// Matching deliveries still awaiting a provider.
    pub undelivered_remaining: u64,
    /// Withheld audit rows still matching the key.
    pub withheld_rows_remaining: u64,
    /// Whether the write-ahead log is truncated, so no purged page image
    /// survives in the `-wal` sidecar.
    pub wal_truncated: bool,
    /// True only when no content, no binding, no undelivered work, and no
    /// withheld record remain — *and* the write-ahead log was truncated. A busy
    /// log fails closed rather than reporting a deletion that is not yet
    /// complete on disk.
    pub verified: bool,
}
