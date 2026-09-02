//! Typed failures for the provider observation journal.

use thiserror::Error;
use tracedecay_memory_provider_api::ApiError;

/// Every way the observation journal can refuse or fail an operation.
///
/// No variant is a silent drop: capacity, deadline, sequence, duplicate, and
/// corruption outcomes are all typed and inspectable, per ADR-0005 invariant 7.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ObservationJournalError {
    /// A reused provider-API value failed its own validator.
    #[error("provider API value is invalid: {0}")]
    Api(#[from] ApiError),
    /// The underlying SQLite store failed.
    #[error("observation journal storage failure: {0}")]
    Storage(#[from] rusqlite::Error),
    /// A JSON column could not be encoded or decoded.
    #[error("could not serialize observation journal field {field}: {detail}")]
    Serialization {
        /// Logical column or field name.
        field: &'static str,
        /// Serializer detail.
        detail: String,
    },
    /// A field that must be a lowercase 64-character SHA-256 was not.
    #[error("observation journal field {field} is not a lowercase sha-256 digest")]
    InvalidDigest {
        /// Logical field name.
        field: &'static str,
    },
    /// A required identity or text field was empty.
    #[error("observation journal field {field} must not be empty")]
    EmptyField {
        /// Logical field name.
        field: &'static str,
    },
    /// A required text field exceeded its contract bound.
    #[error("observation journal field {field} exceeds {maximum_bytes} bytes")]
    FieldTooLarge {
        /// Logical field name.
        field: &'static str,
        /// Contract maximum in UTF-8 bytes.
        maximum_bytes: usize,
    },
    /// The declared idempotency key is not the key the contract derivation
    /// produces from the envelope's own inputs.
    #[error("idempotency key {provided} does not match derived key {expected}")]
    IdempotencyKeyMismatch {
        /// Key derived from the envelope.
        expected: String,
        /// Key the caller declared.
        provided: String,
    },
    /// The declared extensions digest does not match the carried extensions.
    #[error("extensions digest does not match the carried extensions")]
    ExtensionsDigestMismatch,
    /// The declared envelope digest does not match the admitted envelope.
    #[error("envelope digest does not match the admitted envelope")]
    EnvelopeDigestMismatch,
    /// The canonical settlement receipt is missing or incomplete, so the source
    /// event is not proven settled.
    #[error("source is not canonically settled: {field} is missing or invalid")]
    UnsettledSource {
        /// Logical settlement field name.
        field: &'static str,
    },
    /// A persisted or supplied wire value is outside its closed enumeration.
    #[error("unknown {field} wire value {value}")]
    UnknownWireValue {
        /// Logical field name.
        field: &'static str,
        /// Offending wire value.
        value: String,
    },
    /// The provider terminal cannot be expressed as an observation outcome.
    #[error("terminal code {terminal_code} is not admissible for an observation delivery")]
    TerminalNotAdmissible {
        /// Canonical terminal wire value.
        terminal_code: &'static str,
    },
    /// A success-shaped outcome carried no provider receipt digest, which the
    /// contract forbids (`success_without_provider_acknowledgement_allowed`).
    #[error("outcome {outcome} requires a provider receipt digest")]
    AcknowledgementWithoutProviderReceipt {
        /// Canonical outcome wire value.
        outcome: &'static str,
    },
    /// A provider claimed a duplicate acknowledgement without naming the exact
    /// mutation it deduplicated, so nothing ties the claim to this observation.
    #[error("duplicate acknowledgement is missing {field}")]
    UnboundDuplicateAcknowledgement {
        /// Logical committed-effect field the claim omitted.
        field: &'static str,
    },
    /// A provider claimed a duplicate acknowledgement for a different
    /// idempotency key than the observation being delivered.
    #[error(
        "duplicate acknowledgement names a different idempotency key than the delivered observation"
    )]
    DuplicateAcknowledgementKeyMismatch,
    /// A receipt's acceptance outcome and the committed effect it carries
    /// disagree about whether the delivery was a duplicate.
    ///
    /// Both halves are read from the same provider evidence when a receipt is
    /// minted, so a receipt — or a persisted row revalidated on read — where
    /// they diverge is a forged or corrupt claim rather than something a
    /// provider can report. Refusing it here keeps `duplicate_acknowledged`
    /// from ever becoming a bare label with no duplicate effect behind it.
    #[error(
        "receipt outcome {outcome} disagrees with committed effect {committed_effect} about duplication"
    )]
    DuplicateAcknowledgementIncoherent {
        /// Canonical outcome wire value.
        outcome: &'static str,
        /// Canonical committed-effect wire value.
        committed_effect: &'static str,
    },
    /// A receipt described content that the journal row does not hold.
    #[error("delivery receipt {field} does not match the journalled observation")]
    ReceiptDigestMismatch {
        /// Logical field name.
        field: &'static str,
    },
    /// The retention policy is not internally consistent.
    #[error("retention policy field {field} is invalid")]
    InvalidRetentionPolicy {
        /// Logical policy field name.
        field: &'static str,
    },
    /// The retention sweep schedule cannot bound the sweep cadence.
    #[error("retention sweep schedule field {field} is invalid")]
    InvalidSweepSchedule {
        /// Logical schedule field name.
        field: &'static str,
    },
    /// The dispatch policy cannot bound a delivery round, or exceeds the
    /// retention policy it must stay within.
    #[error("dispatch policy field {field} is invalid")]
    InvalidDispatchPolicy {
        /// Logical policy field name.
        field: &'static str,
    },
    /// The backpressure policy cannot bound the lane, or reserves no headroom
    /// between shedding optional work and refusing every class.
    #[error("backpressure policy field {field} is invalid")]
    InvalidBackpressurePolicy {
        /// Logical policy field name.
        field: &'static str,
    },
    /// An observation identity was not a lowercase UUIDv7.
    #[error("observation id is not a lowercase uuid v7: {detail}")]
    InvalidObservationId {
        /// Parse detail.
        detail: String,
    },
    /// The request deadline is not after its admission instant.
    #[error("deadline {deadline_unix_micros} is not after admission {admitted_at_unix_micros}")]
    DeadlineBeforeAdmission {
        /// Declared deadline.
        deadline_unix_micros: i64,
        /// Declared admission instant.
        admitted_at_unix_micros: i64,
    },
    /// A receipt named an observation the journal does not hold.
    #[error("observation {observation_id} is not present in the journal")]
    UnknownObservation {
        /// Missing observation identity.
        observation_id: String,
    },
    /// A lease identity is not held by any delivery row.
    #[error("dispatch lease {lease_id} is not held by any delivery row")]
    UnknownLease {
        /// Missing lease identity.
        lease_id: String,
    },
    /// The store was written by a newer schema than this build supports.
    #[error("observation journal schema {found} is ahead of supported schema {supported}")]
    SchemaAhead {
        /// Version found in the store.
        found: i64,
        /// Highest version this build understands.
        supported: i64,
    },
    /// A legacy withheld row predates rederivable hygiene evidence and cannot
    /// be migrated without inventing or deleting audit truth.
    #[error("observation journal schema migration cannot preserve {rows} legacy withheld rows")]
    LegacyWithheldEvidenceUnmigratable {
        /// Number of legacy rows whose missing evidence cannot be reconstructed.
        rows: u64,
    },
    /// A persisted row could not be revalidated on read.
    #[error("observation journal row in {table}.{field} is corrupt")]
    Corrupt {
        /// Physical table name.
        table: &'static str,
        /// Physical column name.
        field: &'static str,
    },
    /// A numeric value did not fit its SQLite representation.
    #[error("observation journal field {field} is out of range")]
    ValueOutOfRange {
        /// Logical field name.
        field: &'static str,
    },
    /// The journal mutex was poisoned by a panicking writer.
    #[error("observation journal connection lock is poisoned")]
    LockPoisoned,
    /// The caller's remaining budget ran out while the operation was waiting
    /// for the journal connection or for SQLite itself. Nothing was written.
    #[error("observation journal operation {operation} exhausted its remaining budget")]
    BudgetExhausted {
        /// Logical operation that ran out of budget.
        operation: &'static str,
    },
}

impl ObservationJournalError {
    pub(crate) fn serialization(field: &'static str, error: &serde_json::Error) -> Self {
        Self::Serialization {
            field,
            detail: error.to_string(),
        }
    }
}
