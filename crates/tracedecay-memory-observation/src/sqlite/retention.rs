//! Bounded retention sweeps and verifiable privacy deletion.

use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use tracedecay_memory_provider_api::contract::DeletionMode;

use crate::error::ObservationJournalError;
use crate::identity::ForgetSourceKeyV1;
use crate::port::ObservationRetentionPortV1;
use crate::receipt::ObservationOutcomeV1;
use crate::retention::{
    ForgetReceiptV1, ForgetSourceRequestV1, ForgetVerificationV1, RetentionPolicyV1,
    RetentionSweepReceiptV1,
};
use crate::state::DeliveryStateV1;

use super::SqliteObservationJournal;
use super::dispatch::{ExpireDeliveryRequest, terminalize_deliveries};
use super::row::{read_u32, read_u64};

/// Provider-local privacy intent retained independently of provider availability.
/// This receipt never asserts that the provider erased its own state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSourceFenceV1 {
    /// Provider that owns the affected advisory effects.
    pub provider_id: String,
    /// Host-derived original source identity, invariant under destination copies.
    pub original_source_sha256: String,
    /// Monotone revision local to this source fence, unrelated to provider generation.
    pub revision: u64,
    /// Canonical deletion mode admitted by the host.
    pub mode: String,
    /// Source revision named by deletion, if retained.
    pub deleted_source_revision: Option<String>,
    /// Only this explicitly admitted revision may reappear after remove-influence.
    pub admitted_source_revision: Option<String>,
    /// Existing host evidence authorizing the latest state transition.
    pub authority_ref: String,
    /// Actual time the host accepted the intent or revision transition.
    pub accepted_at_utc_micros: i64,
}

impl ProviderSourceFenceV1 {
    /// New bytes, observation IDs and delivery keys never bypass this fence.
    pub fn blocks_revision(&self, revision: Option<&str>) -> bool {
        self.mode != "remove_influence"
            || self.admitted_source_revision.as_deref().is_none()
            || self.admitted_source_revision.as_deref() != revision
    }
}

/// Durable acceptance of host intent, explicitly separate from provider erasure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSourceIntentReceiptV1 {
    /// Stable operation key; retries return this original receipt.
    pub operation_id: String,
    /// Fence committed by this operation, even if the current fence moved later.
    pub fence: ProviderSourceFenceV1,
    /// Always false: journal acceptance cannot verify a provider's erasure.
    pub provider_erasure_verified: bool,
}

/// An already authorized provider-local deletion, recorded before dispatch.
pub struct ProviderSourceDeletionIntentV1<'a> {
    /// Host operation idempotency key.
    pub operation_id: &'a str,
    /// Producing provider, independent of active selection.
    pub provider_id: &'a str,
    /// Original source digest derived by the host, unchanged across copies.
    pub original_source_sha256: &'a str,
    /// Compare-and-set fence revision; zero means no prior fence.
    pub expected_fence_revision: u64,
    /// Canonical deletion policy.
    pub mode: DeletionMode,
    /// Opaque canonical source revision, never an envelope counter.
    pub source_revision: Option<&'a str>,
    /// Retained host authority evidence for the control.
    pub authority_ref: &'a str,
    /// Clock at acceptance; deliberately excluded from retry identity.
    pub accepted_at_utc_micros: i64,
}

/// Explicit new canonical revision admission, allowed only by remove-influence.
pub struct ProviderSourceRevisionAdmissionV1<'a> {
    /// Host operation idempotency key.
    pub operation_id: &'a str,
    /// Original producing provider.
    pub provider_id: &'a str,
    /// Original source digest retained across destination copies.
    pub original_source_sha256: &'a str,
    /// Exact current fence revision; stale admission fails closed.
    pub expected_fence_revision: u64,
    /// Nonempty source revision supported by current canonical authority.
    pub source_revision: &'a str,
    /// Host evidence for this exact revision transition.
    pub authority_ref: &'a str,
    /// Clock at acceptance; excluded from retry identity.
    pub accepted_at_utc_micros: i64,
}

pub(super) fn initialize_provider_source_fences(
    transaction: &Transaction<'_>,
) -> Result<(), ObservationJournalError> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS tdmem_provider_source_fence_v1 (
            provider_id TEXT NOT NULL,
            original_source_sha256 TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK(revision > 0),
            fence_json TEXT NOT NULL,
            PRIMARY KEY(provider_id, original_source_sha256)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS tdmem_provider_source_intent_receipt_v1 (
            provider_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            request_json TEXT NOT NULL,
            receipt_json TEXT NOT NULL,
            PRIMARY KEY(provider_id, operation_id)
         ) WITHOUT ROWID;",
    )?;
    Ok(())
}

fn fence_conflict(field: &'static str) -> ObservationJournalError {
    ObservationJournalError::UnsettledSource { field }
}

fn validate_fence_label(value: &str, field: &'static str) -> Result<(), ObservationJournalError> {
    crate::identity::require_bounded(value, field, 1024)?;
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(fence_conflict(field));
    }
    Ok(())
}

fn read_source_fence(
    connection: &rusqlite::Connection,
    provider_id: &str,
    source: &str,
) -> Result<Option<ProviderSourceFenceV1>, ObservationJournalError> {
    let row: Option<(i64, Vec<u8>)> = connection
        .query_row(
            "SELECT revision, substr(CAST(fence_json AS BLOB), 1, 16385) FROM tdmem_provider_source_fence_v1
         WHERE provider_id = ?1 AND original_source_sha256 = ?2",
            params![provider_id, source],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(revision, json)| {
        if json.len() > 16_384 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "provider_source_fence_json",
            });
        }
        let fence: ProviderSourceFenceV1 = serde_json::from_slice(&json).map_err(|error| {
            ObservationJournalError::serialization("provider_source_fence", &error)
        })?;
        if fence.provider_id != provider_id
            || fence.original_source_sha256 != source
            || fence.revision != read_u64(revision, "fence_revision")?
            || !matches!(
                fence.mode.as_str(),
                "remove_influence" | "hard_delete" | "anonymize"
            )
            || fence.revision == 0
            || (fence.mode != "remove_influence" && fence.admitted_source_revision.is_some())
        {
            return Err(fence_conflict("provider_source_fence_binding"));
        }
        validate_fence_label(&fence.authority_ref, "fence_authority_ref")?;
        for value in [
            &fence.deleted_source_revision,
            &fence.admitted_source_revision,
        ]
        .into_iter()
        .flatten()
        {
            validate_fence_label(value, "source_revision")?;
        }
        Ok(fence)
    })
    .transpose()
}

/// Validated retry identity shared by legacy and controlled fence transitions.
struct PreparedSourceFenceTransition<'a> {
    operation_id: &'a str,
    provider_id: &'a str,
    source: &'a str,
    expected: u64,
    mode: &'a str,
    revision: Option<&'a str>,
    authority_ref: &'a str,
    accepted_at: i64,
    admit_revision: bool,
    request_json: String,
}

impl<'a> PreparedSourceFenceTransition<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        operation_id: &'a str,
        provider_id: &'a str,
        source: &'a str,
        expected: u64,
        mode: &'a str,
        revision: Option<&'a str>,
        authority_ref: &'a str,
        accepted_at: i64,
        admit_revision: bool,
    ) -> Result<Self, ObservationJournalError> {
        for (value, field) in [
            (operation_id, "operation_id"),
            (provider_id, "provider_id"),
            (authority_ref, "authority_ref"),
        ] {
            validate_fence_label(value, field)?;
        }
        if let Some(revision) = revision {
            validate_fence_label(revision, "source_revision")?;
        }
        crate::identity::require_sha256(source, "original_source_sha256")?;
        let request_json = serde_json::to_string(&(
            source,
            expected,
            mode,
            revision,
            authority_ref,
            admit_revision,
        ))
        .map_err(|error| ObservationJournalError::serialization("source_intent_request", &error))?;
        Ok(Self {
            operation_id,
            provider_id,
            source,
            expected,
            mode,
            revision,
            authority_ref,
            accepted_at,
            admit_revision,
            request_json,
        })
    }

    fn deletion(
        request: &'a ProviderSourceDeletionIntentV1<'a>,
    ) -> Result<Self, ObservationJournalError> {
        Self::new(
            request.operation_id,
            request.provider_id,
            request.original_source_sha256,
            request.expected_fence_revision,
            request.mode.as_wire(),
            request.source_revision,
            request.authority_ref,
            request.accepted_at_utc_micros,
            false,
        )
    }

    /// Exact operation key plus the complete immutable request identity.
    fn read_receipt(
        &self,
        connection: &rusqlite::Connection,
    ) -> Result<Option<ProviderSourceIntentReceiptV1>, ObservationJournalError> {
        const MAX_JSON_BYTES: usize = 16_384;
        let row: Option<(Vec<u8>, Vec<u8>)> = connection
            .query_row(
                "SELECT substr(CAST(request_json AS BLOB), 1, 16385),
                    substr(CAST(receipt_json AS BLOB), 1, 16385)
             FROM tdmem_provider_source_intent_receipt_v1
             WHERE provider_id = ?1 AND operation_id = ?2",
                params![self.provider_id, self.operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(request, receipt)| {
            if request.len() > MAX_JSON_BYTES || receipt.len() > MAX_JSON_BYTES {
                return Err(fence_conflict("source_intent_receipt_size"));
            }
            if request != self.request_json.as_bytes() {
                return Err(fence_conflict("source_intent_idempotency_conflict"));
            }
            let receipt: ProviderSourceIntentReceiptV1 =
                serde_json::from_slice(&receipt).map_err(|error| {
                    ObservationJournalError::serialization("source_intent_receipt", &error)
                })?;
            let fence = &receipt.fence;
            let expected_revision = self
                .expected
                .checked_add(1)
                .filter(|value| *value <= i64::MAX as u64)
                .ok_or(ObservationJournalError::ValueOutOfRange {
                    field: "fence_revision",
                })?;
            if receipt.operation_id != self.operation_id
                || fence.provider_id != self.provider_id
                || fence.original_source_sha256 != self.source
                || receipt.provider_erasure_verified
                || fence.revision != expected_revision
                || fence.authority_ref != self.authority_ref
                || !matches!(
                    fence.mode.as_str(),
                    "remove_influence" | "anonymize" | "hard_delete"
                )
                || (fence.mode != "remove_influence" && fence.admitted_source_revision.is_some())
                || (self.admit_revision
                    && (fence.mode != "remove_influence"
                        || fence.admitted_source_revision.as_deref() != self.revision))
                || (!self.admit_revision
                    && (fence.deleted_source_revision.as_deref() != self.revision
                        || fence.admitted_source_revision.is_some()
                        || (self.mode == "hard_delete" && fence.mode != "hard_delete")
                        || (self.mode == "anonymize" && fence.mode == "remove_influence")))
            {
                return Err(fence_conflict("source_intent_receipt_binding"));
            }
            validate_fence_label(&fence.authority_ref, "fence_authority_ref")?;
            for revision in [
                &fence.deleted_source_revision,
                &fence.admitted_source_revision,
            ]
            .into_iter()
            .flatten()
            {
                validate_fence_label(revision, "source_revision")?;
            }
            Ok(receipt)
        })
        .transpose()
    }

    fn apply(
        &self,
        transaction: &Transaction<'_>,
        check: &impl Fn() -> Result<(), ObservationJournalError>,
    ) -> Result<ProviderSourceIntentReceiptV1, ObservationJournalError> {
        let operation_id = self.operation_id;
        let provider_id = self.provider_id;
        let source = self.source;
        let expected = self.expected;
        let mode = self.mode;
        let revision = self.revision;
        let authority_ref = self.authority_ref;
        let accepted_at = self.accepted_at;
        let admit_revision = self.admit_revision;
        let request_json = &self.request_json;
        check()?;
        // A late retry returns its immutable receipt before consulting current CAS.
        if let Some(receipt) = self.read_receipt(transaction)? {
            return Ok(receipt);
        }
        let current = read_source_fence(transaction, provider_id, source)?;
        if current.as_ref().map_or(0, |fence| fence.revision) != expected {
            return Err(fence_conflict("source_fence_revision_conflict"));
        }
        let next = expected
            .checked_add(1)
            .filter(|next| *next <= i64::MAX as u64)
            .ok_or(ObservationJournalError::ValueOutOfRange {
                field: "fence_revision",
            })?;
        let mut fence = if admit_revision {
            let mut fence = current.ok_or_else(|| fence_conflict("source_fence_missing"))?;
            if fence.mode != "remove_influence"
                || revision.is_none()
                || revision == fence.deleted_source_revision.as_deref()
                || revision == fence.admitted_source_revision.as_deref()
            {
                return Err(fence_conflict("source_revision_not_admitted"));
            }
            fence.admitted_source_revision = revision.map(str::to_owned);
            fence
        } else {
            // Strong privacy modes cannot be weakened by a later intent.
            let previous_mode = current.as_ref().map(|fence| fence.mode.as_str());
            let mode = if mode == "hard_delete" || previous_mode == Some("hard_delete") {
                "hard_delete"
            } else if mode == "anonymize" || previous_mode == Some("anonymize") {
                "anonymize"
            } else {
                "remove_influence"
            };
            ProviderSourceFenceV1 {
                provider_id: provider_id.to_owned(),
                original_source_sha256: source.to_owned(),
                revision: next,
                mode: mode.to_owned(),
                deleted_source_revision: revision.map(str::to_owned),
                admitted_source_revision: None,
                authority_ref: authority_ref.to_owned(),
                accepted_at_utc_micros: accepted_at,
            }
        };
        fence.revision = next;
        fence.authority_ref = authority_ref.to_owned();
        fence.accepted_at_utc_micros = accepted_at;
        let fence_json = serde_json::to_string(&fence)
            .map_err(|error| ObservationJournalError::serialization("source_fence", &error))?;
        check()?;
        transaction.execute(
                "INSERT INTO tdmem_provider_source_fence_v1 (provider_id, original_source_sha256, revision, fence_json)
                 VALUES (?1, ?2, ?3, ?4) ON CONFLICT(provider_id, original_source_sha256)
                 DO UPDATE SET revision=excluded.revision, fence_json=excluded.fence_json",
                params![provider_id, source, next as i64, fence_json],
            )?;
        let receipt = ProviderSourceIntentReceiptV1 {
            operation_id: operation_id.to_owned(),
            fence,
            provider_erasure_verified: false,
        };
        let json = serde_json::to_string(&receipt).map_err(|error| {
            ObservationJournalError::serialization("source_intent_receipt", &error)
        })?;
        check()?;
        transaction.execute(
                "INSERT INTO tdmem_provider_source_intent_receipt_v1 (provider_id, operation_id, request_json, receipt_json)
                 VALUES (?1, ?2, ?3, ?4)", params![provider_id, operation_id, request_json, json],
            )?;
        Ok(receipt)
    }
}

fn source_intent_control_check(
    started: std::time::Instant,
    budget: crate::RecoveryTimeBudgetV1,
    cancellation: &tracedecay_memory_provider_api::CancellationToken,
    operation: &'static str,
) -> Result<(), ObservationJournalError> {
    if cancellation.is_cancelled() {
        return Err(ObservationJournalError::OperationCancelled { operation });
    }
    if source_intent_remaining(started, budget).is_spent() {
        return Err(ObservationJournalError::BudgetExhausted { operation });
    }
    Ok(())
}

fn source_intent_remaining(
    started: std::time::Instant,
    budget: crate::RecoveryTimeBudgetV1,
) -> crate::RecoveryTimeBudgetV1 {
    crate::RecoveryTimeBudgetV1 {
        remaining_micros: budget
            .remaining_micros
            .saturating_sub(i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX)),
    }
}

impl SqliteObservationJournal {
    /// Existing callers keep the established five-second SQLite allowance;
    /// callers with an operation control must use the explicitly bounded method.
    pub fn read_provider_source_fence(
        &self,
        provider_id: &str,
        original_source_sha256: &str,
    ) -> Result<Option<ProviderSourceFenceV1>, ObservationJournalError> {
        self.read_provider_source_fence_bounded(
            provider_id,
            original_source_sha256,
            crate::RecoveryTimeBudgetV1 {
                remaining_micros: i64::try_from(super::schema::BUSY_TIMEOUT_MILLIS)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1_000),
            },
            &tracedecay_memory_provider_api::CancellationToken::new(),
        )
    }

    /// One exact fence read under the caller's original remaining budget.
    /// This never migrates a schema, repairs a journal, or calls a provider.
    pub fn read_provider_source_fence_bounded(
        &self,
        provider_id: &str,
        original_source_sha256: &str,
        budget: crate::RecoveryTimeBudgetV1,
        cancellation: &tracedecay_memory_provider_api::CancellationToken,
    ) -> Result<Option<ProviderSourceFenceV1>, ObservationJournalError> {
        const OPERATION: &str = "read_provider_source_fence";
        let started = std::time::Instant::now();
        let check = || {
            if cancellation.is_cancelled() {
                return Err(ObservationJournalError::OperationCancelled {
                    operation: OPERATION,
                });
            }
            if budget.remaining_micros
                <= i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX)
            {
                return Err(ObservationJournalError::BudgetExhausted {
                    operation: OPERATION,
                });
            }
            Ok(())
        };
        check()?;
        validate_fence_label(provider_id, "provider_id")?;
        crate::identity::require_sha256(original_source_sha256, "original_source_sha256")?;
        let remaining = crate::RecoveryTimeBudgetV1 {
            remaining_micros: budget
                .remaining_micros
                .saturating_sub(i64::try_from(started.elapsed().as_micros()).unwrap_or(i64::MAX)),
        };
        let result = self.with_cancellable_bounded_connection(
            OPERATION,
            remaining,
            cancellation,
            |connection| {
                check()?;
                let fence = read_source_fence(connection, provider_id, original_source_sha256)?;
                check()?;
                Ok(fence)
            },
        );
        check()?;
        result
    }

    /// Records provider-specific deletion intent even while that provider is offline.
    /// Canonical observations and facts are never changed by this operation.
    pub fn record_provider_source_deletion_intent(
        &self,
        request: &ProviderSourceDeletionIntentV1<'_>,
    ) -> Result<ProviderSourceIntentReceiptV1, ObservationJournalError> {
        let mode = match request.mode {
            DeletionMode::RemoveInfluence => "remove_influence",
            DeletionMode::HardDelete => "hard_delete",
            DeletionMode::Anonymize => "anonymize",
        };
        self.transition_provider_source_fence(
            request.operation_id,
            request.provider_id,
            request.original_source_sha256,
            request.expected_fence_revision,
            mode,
            request.source_revision,
            request.authority_ref,
            request.accepted_at_utc_micros,
            false,
        )
    }

    /// One cancellable host intent transaction using the caller's remaining budget.
    /// Successful COMMIT always returns the durable receipt, including cancellation
    /// racing with the commit. This receipt does not establish provider erasure.
    pub fn record_provider_source_deletion_intent_bounded(
        &self,
        request: &ProviderSourceDeletionIntentV1<'_>,
        budget: crate::RecoveryTimeBudgetV1,
        cancellation: &tracedecay_memory_provider_api::CancellationToken,
    ) -> Result<ProviderSourceIntentReceiptV1, ObservationJournalError> {
        const OPERATION: &str = "record_provider_source_deletion_intent";
        let started = std::time::Instant::now();
        let check = || source_intent_control_check(started, budget, cancellation, OPERATION);
        check()?;
        let prepared = PreparedSourceFenceTransition::deletion(request)?;
        let mut guard = self.lock_within_cancellable(
            OPERATION,
            source_intent_remaining(started, budget),
            Some(cancellation),
        )?;
        check()?;
        // One short SQLite wait; no retry or fresh operation allowance is minted.
        let sqlite_budget = crate::RecoveryTimeBudgetV1 {
            remaining_micros: source_intent_remaining(started, budget)
                .remaining_micros
                .min(10_000),
        };
        let mut committed = None;
        let outcome = super::with_busy_budget(
            &mut guard,
            OPERATION,
            sqlite_budget,
            std::time::Instant::now(),
            |connection| {
                check()?;
                let transaction = connection
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let receipt = prepared.apply(&transaction, &check)?;
                check()?;
                transaction.commit()?;
                // Preserve commit truth even if the busy-timeout restoration fails.
                committed = Some(receipt.clone());
                Ok(receipt)
            },
        );
        if let Some(receipt) = committed {
            return Ok(receipt);
        }
        check()?;
        outcome
    }

    /// Reads one original immutable receipt without performing a transition.
    /// The full request identity is required; acceptance time is not retry identity.
    pub fn read_provider_source_deletion_intent_receipt_bounded(
        &self,
        request: &ProviderSourceDeletionIntentV1<'_>,
        budget: crate::RecoveryTimeBudgetV1,
        cancellation: &tracedecay_memory_provider_api::CancellationToken,
    ) -> Result<Option<ProviderSourceIntentReceiptV1>, ObservationJournalError> {
        const OPERATION: &str = "read_provider_source_deletion_intent_receipt";
        let started = std::time::Instant::now();
        let check = || source_intent_control_check(started, budget, cancellation, OPERATION);
        check()?;
        let prepared = PreparedSourceFenceTransition::deletion(request)?;
        let mut guard = self.lock_within_cancellable(
            OPERATION,
            source_intent_remaining(started, budget),
            Some(cancellation),
        )?;
        check()?;
        let sqlite_budget = crate::RecoveryTimeBudgetV1 {
            remaining_micros: source_intent_remaining(started, budget)
                .remaining_micros
                .min(10_000),
        };
        let outcome = super::with_busy_budget(
            &mut guard,
            OPERATION,
            sqlite_budget,
            std::time::Instant::now(),
            |connection| {
                check()?;
                let receipt = prepared.read_receipt(connection)?;
                check()?;
                Ok(receipt)
            },
        );
        check()?;
        outcome
    }

    /// Admits one exact new revision under a current host-authorized transition.
    /// This is a lifecycle write, never an automatic effect of read or replay.
    pub fn admit_provider_source_revision(
        &self,
        request: &ProviderSourceRevisionAdmissionV1<'_>,
    ) -> Result<ProviderSourceIntentReceiptV1, ObservationJournalError> {
        self.transition_provider_source_fence(
            request.operation_id,
            request.provider_id,
            request.original_source_sha256,
            request.expected_fence_revision,
            "remove_influence",
            Some(request.source_revision),
            request.authority_ref,
            request.accepted_at_utc_micros,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_provider_source_fence(
        &self,
        operation_id: &str,
        provider_id: &str,
        source: &str,
        expected: u64,
        mode: &str,
        revision: Option<&str>,
        authority_ref: &str,
        accepted_at: i64,
        admit_revision: bool,
    ) -> Result<ProviderSourceIntentReceiptV1, ObservationJournalError> {
        let prepared = PreparedSourceFenceTransition::new(
            operation_id,
            provider_id,
            source,
            expected,
            mode,
            revision,
            authority_ref,
            accepted_at,
            admit_revision,
        )?;
        self.with_transaction(|transaction| prepared.apply(transaction, &|| Ok(())))
    }
}

/// Effective expiry: `min(admitted privacy expiry, admitted_at + class age)`.
/// A provider can never widen it — no receipt column feeds this expression.
const EFFECTIVE_EXPIRY: &str = "MIN(j.expires_at_micros, j.admitted_at_micros + CASE j.retention_class \
    WHEN 'ephemeral' THEN ?1 WHEN 'session' THEN ?2 WHEN 'project' THEN ?3 ELSE ?4 END)";

/// The delivery states that still expect a provider to answer.
const UNSETTLED_STATES: &str = "('pending', 'leased', 'effect_unknown')";

/// The delivery states that will never be leased again.
const SETTLED_STATES: &str = "('acknowledged', 'duplicate_acknowledged', 'rejected', \
    'cancelled', 'expired', 'exhausted', 'forgotten')";

/// Content and the hygiene evidence that describes it are purged together.
///
/// The binding is not audit: its receipt JSON restates the pre-sanitization
/// digest of the very bytes being deleted, so leaving it behind would leave a
/// digest of forgotten content at rest — and clearing only part of it would
/// leave a column combination the schema forbids and no reader can decode.
const PURGE_COLUMNS: &str = "payload_bytes = NULL, extensions_json = NULL, \
    sanitization_receipt_id = NULL, sanitizer_revision = NULL, \
    source_payload_sha256 = NULL, sanitization_receipt_json = NULL";

impl SqliteObservationJournal {
    fn purge_content(
        transaction: &Transaction<'_>,
        idempotency_key: &str,
        now_unix_micros: i64,
    ) -> Result<(), ObservationJournalError> {
        transaction.execute(
            &format!(
                "UPDATE tdmem_observation_journal_v1 SET {PURGE_COLUMNS}, \
                 content_forgotten_at_micros = COALESCE(content_forgotten_at_micros, ?1) \
                 WHERE idempotency_key = ?2"
            ),
            params![now_unix_micros, idempotency_key],
        )?;
        Ok(())
    }
}

impl ObservationRetentionPortV1 for SqliteObservationJournal {
    fn retention_policy(&self) -> &RetentionPolicyV1 {
        self.policy()
    }

    fn sweep_expired(
        &self,
        now_unix_micros: i64,
        budget: u32,
    ) -> Result<RetentionSweepReceiptV1, ObservationJournalError> {
        let policy = *self.policy();
        let batch = i64::from(budget.min(policy.sweep_batch_rows).max(1));
        let deletable_cutoff = now_unix_micros.saturating_sub(policy.receipt_retention_micros);
        let receipt = self.with_transaction(|transaction| {
            let mut receipt = RetentionSweepReceiptV1::default();

            // (1) Content whose effective expiry has passed and that still
            // holds bytes. Non-terminal deliveries are expired with a terminal
            // receipt first; only then is content purged.
            let select_candidates = format!(
                "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                 FROM tdmem_observation_journal_v1 j \
                 JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                 WHERE j.payload_bytes IS NOT NULL AND {EFFECTIVE_EXPIRY} <= ?5 \
                 ORDER BY j.admitted_at_micros, j.idempotency_key LIMIT ?6"
            );
            let mut candidates: Vec<(String, String, u32, DeliveryStateV1)> = Vec::new();
            {
                let mut statement = transaction.prepare(&select_candidates)?;
                let mut rows = statement.query(params![
                    policy.ephemeral_max_age_micros,
                    policy.session_max_age_micros,
                    policy.project_max_age_micros,
                    policy.profile_max_age_micros,
                    now_unix_micros,
                    batch,
                ])?;
                while let Some(row) = rows.next()? {
                    candidates.push((
                        row.get(0)?,
                        row.get(1)?,
                        read_u32(row.get::<_, i64>(2)?, "attempt_number")?,
                        DeliveryStateV1::from_wire(&row.get::<_, String>(3)?)?,
                    ));
                }
            }
            for (key, observation_id, attempts, state) in candidates {
                if !state.is_terminal() {
                    Self::expire_delivery(
                        transaction,
                        ExpireDeliveryRequest {
                            observation_id: &observation_id,
                            idempotency_key: &key,
                            attempt_number: attempts,
                            current_state: state,
                            next_state: DeliveryStateV1::Expired,
                            outcome: ObservationOutcomeV1::DeadlineExceeded,
                            reason: "retention_expiry",
                            now_unix_micros,
                        },
                    )?;
                    receipt.deliveries_expired = receipt.deliveries_expired.saturating_add(1);
                }
                Self::purge_content(transaction, &key, now_unix_micros)?;
                receipt.payloads_purged = receipt.payloads_purged.saturating_add(1);
            }

            // (2) Rows whose content is already gone but whose delivery never
            // settled. They can never be delivered — there is nothing left to
            // deliver — and they are not deletable while non-terminal, so
            // without this they would sit in the queue forever and the sweep
            // would keep reporting nothing left to do. Terminalizing them here
            // is what makes convergence real rather than dependent on some
            // dispatcher calling `lease_pending` and noticing.
            receipt.deliveries_forgotten = terminalize_deliveries(
                transaction,
                &format!(
                    "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                     FROM tdmem_observation_journal_v1 j \
                     JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                     WHERE j.payload_bytes IS NULL AND d.state IN {UNSETTLED_STATES} \
                     ORDER BY j.admitted_at_micros, j.idempotency_key LIMIT ?1"
                ),
                params![batch],
                DeliveryStateV1::Forgotten,
                ObservationOutcomeV1::Cancelled,
                "content_forgotten",
                now_unix_micros,
            )?;

            // (3) Rows whose delivery is terminal and whose audit window has
            // closed are removed entirely, receipts included.
            let mut deletable: Vec<(String, String)> = Vec::new();
            {
                let mut statement = transaction.prepare(&format!(
                    "SELECT j.idempotency_key, j.observation_id \
                     FROM tdmem_observation_journal_v1 j \
                     JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
                     WHERE j.content_forgotten_at_micros IS NOT NULL \
                       AND j.content_forgotten_at_micros <= ?1 \
                       AND d.state IN {SETTLED_STATES} \
                     ORDER BY j.content_forgotten_at_micros, j.idempotency_key LIMIT ?2"
                ))?;
                let mut rows = statement.query(params![deletable_cutoff, batch])?;
                while let Some(row) = rows.next()? {
                    deletable.push((row.get(0)?, row.get(1)?));
                }
            }
            for (key, observation_id) in deletable {
                let receipts = transaction.execute(
                    "DELETE FROM tdmem_observation_receipt_v1 WHERE observation_id = ?1",
                    params![&observation_id],
                )?;
                receipt.receipts_deleted = receipt.receipts_deleted.saturating_add(read_u32(
                    i64::try_from(receipts).unwrap_or(i64::MAX),
                    "receipts_deleted",
                )?);
                // The refused-terminal audit ages out with the attempt history
                // it belongs to. It is keyed by observation, not by the journal
                // row, so nothing cascades it: without this it would outlive
                // every row it describes and grow without bound.
                transaction.execute(
                    "DELETE FROM tdmem_observation_attempt_refusal_v1 WHERE observation_id = ?1",
                    params![&observation_id],
                )?;
                // The delivery row cascades with the journal row.
                let deleted = transaction.execute(
                    "DELETE FROM tdmem_observation_journal_v1 WHERE idempotency_key = ?1",
                    params![&key],
                )?;
                receipt.journal_rows_deleted =
                    receipt.journal_rows_deleted.saturating_add(read_u32(
                        i64::try_from(deleted).unwrap_or(i64::MAX),
                        "journal_rows_deleted",
                    )?);
            }

            // (4) Withheld audit rows age out on the same audit window. They
            // hold digests of refused — often secret-bearing — content, so
            // "never swept" would mean "kept forever".
            let withheld =
                transaction.execute(DELETE_AGED_WITHHELD, params![deletable_cutoff, batch])?;
            receipt.withheld_rows_deleted = read_u32(
                i64::try_from(withheld).unwrap_or(i64::MAX),
                "withheld_rows_deleted",
            )?;

            receipt.remaining_candidates =
                remaining_candidates(transaction, &policy, now_unix_micros, deletable_cutoff)?;
            Ok(receipt)
        })?;

        // Purged pages live on in the write-ahead log until it is checkpointed,
        // so a sweep that freed content is not done on disk until this lands.
        let mut receipt = receipt;
        receipt.wal_truncated = self.checkpoint_truncate()?;
        Ok(receipt)
    }

    fn forget_source(
        &self,
        request: &ForgetSourceRequestV1,
    ) -> Result<ForgetReceiptV1, ObservationJournalError> {
        request.validate()?;
        let mut receipt = self.with_transaction(|transaction| {
            let key = request.forget_source_key.as_str();
            let matched: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key],
                |row| row.get(0),
            )?;
            let (with_content, with_binding): (i64, i64) = transaction.query_row(
                "SELECT COALESCE(SUM(payload_bytes IS NOT NULL), 0), \
                        COALESCE(SUM(sanitization_receipt_id IS NOT NULL), 0) \
                 FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;

            // Every unsettled delivery is terminalized, without a flag to opt
            // out of it: once the content is gone the row can never be
            // delivered, and leaving it queued would leave a dispatcher
            // repeatedly discovering an observation whose bytes no longer
            // exist.
            let deliveries_forgotten = terminalize_deliveries(
                transaction,
                &format!(
                    "SELECT d.idempotency_key, d.observation_id, d.attempt_number, d.state \
                     FROM tdmem_observation_delivery_v1 d \
                     JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
                     WHERE j.forget_source_key = ?1 AND d.state IN {UNSETTLED_STATES} \
                     ORDER BY d.idempotency_key"
                ),
                params![key],
                DeliveryStateV1::Forgotten,
                ObservationOutcomeV1::Cancelled,
                &request.reason,
                request.requested_at_unix_micros,
            )?;

            transaction.execute(
                &format!(
                    "UPDATE tdmem_observation_journal_v1 SET {PURGE_COLUMNS}, \
                     content_forgotten_at_micros = COALESCE(content_forgotten_at_micros, ?1) \
                     WHERE forget_source_key = ?2"
                ),
                params![request.requested_at_unix_micros, key],
            )?;

            // The withheld audit answers to the same key: a refused event's
            // digests are exactly as much a record of the subject as an
            // admitted one's.
            let withheld_deleted = transaction.execute(
                "DELETE FROM tdmem_observation_withheld_v2 WHERE forget_source_key = ?1",
                params![key],
            )?;

            let retained: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_receipt_v1 r \
                 JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = r.idempotency_key \
                 WHERE j.forget_source_key = ?1",
                params![key],
                |row| row.get(0),
            )?;

            Ok(ForgetReceiptV1 {
                forget_source_key: request.forget_source_key.clone(),
                journal_rows_matched: read_u64(matched, "journal_rows_matched")?,
                payloads_zeroed: read_u64(with_content, "payloads_zeroed")?,
                sanitization_bindings_cleared: read_u64(
                    with_binding,
                    "sanitization_bindings_cleared",
                )?,
                deliveries_forgotten: u64::from(deliveries_forgotten),
                withheld_rows_deleted: read_u64(
                    i64::try_from(withheld_deleted).unwrap_or(i64::MAX),
                    "withheld_rows_deleted",
                )?,
                receipts_retained: read_u64(retained, "receipts_retained")?,
                wal_truncated: false,
                completed_at_unix_micros: request.requested_at_unix_micros,
            })
        })?;

        // The rows are purged, but the pre-purge page images are still in the
        // write-ahead log until it is checkpointed and truncated. Until that
        // lands the deletion is not complete on disk, and the receipt says so.
        receipt.wal_truncated = self.checkpoint_truncate()?;
        Ok(receipt)
    }

    fn verify_forgotten(
        &self,
        key: &ForgetSourceKeyV1,
    ) -> Result<ForgetVerificationV1, ObservationJournalError> {
        // Truncate first: verification must describe the state of the store
        // after every purged page has actually left the log, not before.
        let wal_truncated = self.checkpoint_truncate()?;
        self.with_connection(|connection| {
            let (matching, with_content, with_binding): (i64, i64, i64) = connection.query_row(
                "SELECT COUNT(*), COALESCE(SUM(payload_bytes IS NOT NULL), 0), \
                        COALESCE(SUM(sanitization_receipt_id IS NOT NULL), 0) \
                 FROM tdmem_observation_journal_v1 WHERE forget_source_key = ?1",
                params![key.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let undelivered: i64 = connection.query_row(
                &format!(
                    "SELECT COUNT(*) FROM tdmem_observation_delivery_v1 d \
                     JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
                     WHERE j.forget_source_key = ?1 AND d.state IN {UNSETTLED_STATES}"
                ),
                params![key.as_str()],
                |row| row.get(0),
            )?;
            let withheld: i64 = connection.query_row(
                "SELECT COUNT(*) FROM tdmem_observation_withheld_v2 WHERE forget_source_key = ?1",
                params![key.as_str()],
                |row| row.get(0),
            )?;
            let rows_with_content_remaining =
                read_u64(with_content, "rows_with_content_remaining")?;
            let rows_with_binding_remaining =
                read_u64(with_binding, "rows_with_binding_remaining")?;
            let undelivered_remaining = read_u64(undelivered, "undelivered_remaining")?;
            let withheld_rows_remaining = read_u64(withheld, "withheld_rows_remaining")?;
            Ok(ForgetVerificationV1 {
                forget_source_key: key.clone(),
                journal_rows_matching: read_u64(matching, "journal_rows_matching")?,
                rows_with_content_remaining,
                rows_with_binding_remaining,
                undelivered_remaining,
                withheld_rows_remaining,
                wal_truncated,
                verified: rows_with_content_remaining == 0
                    && rows_with_binding_remaining == 0
                    && undelivered_remaining == 0
                    && withheld_rows_remaining == 0
                    && wal_truncated,
            })
        })
    }
}

/// Bounded deletion of aged withheld audit rows.
///
/// `DELETE ... LIMIT` needs a compile option the bundled library does not carry,
/// so the bound is applied by selecting the primary key of one batch.
const DELETE_AGED_WITHHELD: &str = r#"
DELETE FROM tdmem_observation_withheld_v2
WHERE (source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id) IN (
    SELECT source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id
    FROM tdmem_observation_withheld_v2
    WHERE withheld_at_micros <= ?1
    ORDER BY withheld_at_micros, source_sequence, receipt_id
    LIMIT ?2)
"#;

/// Every class of work a further sweep would still do.
///
/// Counting only what this sweep happened to purge would report zero while
/// stranded rows sat in the store forever, so each phase contributes its own
/// remainder: content still to purge, deliveries still to terminalize, rows
/// still to delete, and withheld records still to age out.
fn remaining_candidates(
    transaction: &Transaction<'_>,
    policy: &RetentionPolicyV1,
    now_unix_micros: i64,
    deletable_cutoff: i64,
) -> Result<u64, ObservationJournalError> {
    let purge_query = format!(
        "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
         WHERE j.payload_bytes IS NOT NULL AND {EFFECTIVE_EXPIRY} <= ?5"
    );
    let purgeable: i64 = transaction.query_row(
        &purge_query,
        params![
            policy.ephemeral_max_age_micros,
            policy.session_max_age_micros,
            policy.project_max_age_micros,
            policy.profile_max_age_micros,
            now_unix_micros,
        ],
        |row| row.get(0),
    )?;
    let stranded: i64 = transaction.query_row(
        &format!(
            "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
             JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
             WHERE j.payload_bytes IS NULL AND d.state IN {UNSETTLED_STATES}"
        ),
        [],
        |row| row.get(0),
    )?;
    let deletable: i64 = transaction.query_row(
        &format!(
            "SELECT COUNT(*) FROM tdmem_observation_journal_v1 j \
             JOIN tdmem_observation_delivery_v1 d ON d.idempotency_key = j.idempotency_key \
             WHERE j.content_forgotten_at_micros IS NOT NULL \
               AND j.content_forgotten_at_micros <= ?1 \
               AND d.state IN {SETTLED_STATES}"
        ),
        params![deletable_cutoff],
        |row| row.get(0),
    )?;
    let withheld: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_withheld_v2 WHERE withheld_at_micros <= ?1",
        params![deletable_cutoff],
        |row| row.get(0),
    )?;
    Ok(read_u64(purgeable, "purgeable")?
        .saturating_add(read_u64(stranded, "stranded")?)
        .saturating_add(read_u64(deletable, "deletable")?)
        .saturating_add(read_u64(withheld, "withheld")?))
}

#[cfg(test)]
mod bounded_source_fence_tests {
    use super::*;
    use crate::RecoveryTimeBudgetV1;
    use std::time::{Duration, Instant};
    use tracedecay_memory_provider_api::CancellationToken;

    #[test]
    fn provider_source_fence_read_respects_mutex_deadline_and_cancellation()
    -> Result<(), ObservationJournalError> {
        let journal = SqliteObservationJournal::open_in_memory(RetentionPolicyV1 {
            ephemeral_max_age_micros: 1_000_000,
            session_max_age_micros: 1_000_000,
            project_max_age_micros: 1_000_000,
            profile_max_age_micros: 1_000_000,
            receipt_retention_micros: 1_000_000,
            max_queue_items: 256,
            max_queue_bytes: 1_000_000,
            max_attempts: 3,
            backoff_base_micros: 1_000,
            backoff_max_micros: 10_000,
            sweep_batch_rows: 256,
        })?;
        let source = "a".repeat(64);
        let token = CancellationToken::new();
        assert!(
            journal
                .read_provider_source_fence_bounded(
                    "ncm",
                    &source,
                    RecoveryTimeBudgetV1 {
                        remaining_micros: 1_000_000
                    },
                    &token,
                )?
                .is_none()
        );
        let held = journal
            .connection
            .lock()
            .map_err(|_| ObservationJournalError::LockPoisoned)?;
        let started = Instant::now();
        assert!(matches!(
            journal.read_provider_source_fence_bounded(
                "ncm",
                &source,
                RecoveryTimeBudgetV1 {
                    remaining_micros: 10_000
                },
                &token,
            ),
            Err(ObservationJournalError::BudgetExhausted { .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        std::thread::scope(|threads| {
            threads.spawn(|| {
                std::thread::sleep(Duration::from_millis(5));
                token.cancel();
            });
            let started = Instant::now();
            assert!(matches!(
                journal.read_provider_source_fence_bounded(
                    "ncm",
                    &source,
                    RecoveryTimeBudgetV1 {
                        remaining_micros: 5_000_000
                    },
                    &token,
                ),
                Err(ObservationJournalError::OperationCancelled { .. })
            ));
            assert!(started.elapsed() < Duration::from_secs(1));
        });
        drop(held);
        assert!(matches!(
            journal.read_provider_source_fence_bounded(
                "ncm",
                &source,
                RecoveryTimeBudgetV1 {
                    remaining_micros: 1_000_000
                },
                &token,
            ),
            Err(ObservationJournalError::OperationCancelled { .. })
        ));
        Ok(())
    }
}
