//! Encode on write, decode *and revalidate* on read.
//!
//! Every read path rebuilds a validated domain value from stored columns. A row
//! that has drifted - a corrupted settlement receipt, a payload whose bytes no
//! longer match their digest - becomes a typed error, never a silently wrong
//! delivery.

use rusqlite::{Connection, Row};
use serde::{Deserialize, Serialize};
use tracedecay_memory_provider_api::{
    CanonicalPayload, OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
};

use crate::envelope::{
    ObservationPrivacyV1, PrivacyClassificationV1, ProvenanceOriginV1, ProviderTargetV1,
    RetentionClassV1, SanitizationBindingV1, WithheldAdmissionV1,
};
use crate::error::ObservationJournalError;
use crate::identity::{
    DispatchLeaseIdV1, ForgetSourceKeyV1, ObservationIdV1, ObservationIdempotencyKeyV1,
    SourceSequenceV1, lowercase_hex,
};
use crate::lease::LeasedObservationV1;
use crate::receipt::{
    ObservationCommittedEffectV1, ObservationDeliveryReceiptV1, ObservationOutcomeV1,
    ProviderEffectSummaryV1,
};

/// Persisted form of one exact coding scope.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredExactScopeV1 {
    pub(crate) profile_id: String,
    pub(crate) project_id: String,
    pub(crate) repository_identity: String,
    pub(crate) worktree_identity: String,
    pub(crate) branch_identity: String,
    pub(crate) agent_session_id: String,
    pub(crate) resolved_scope_digest: String,
}

impl StoredExactScopeV1 {
    pub(crate) fn from_scope(scope: &OwnedExactScope) -> Self {
        Self {
            profile_id: scope.profile_id.clone(),
            project_id: scope.project_id.clone(),
            repository_identity: scope.repository_identity.clone(),
            worktree_identity: scope.worktree_identity.clone(),
            branch_identity: scope.branch_identity.clone(),
            agent_session_id: scope.agent_session_id.clone(),
            resolved_scope_digest: scope.resolved_scope_digest.clone(),
        }
    }

    pub(crate) fn into_scope(self) -> Result<OwnedExactScope, ObservationJournalError> {
        Ok(OwnedExactScope::new(
            self.profile_id,
            self.project_id,
            self.repository_identity,
            self.worktree_identity,
            self.branch_identity,
            self.agent_session_id,
            self.resolved_scope_digest,
        )?)
    }
}

/// Persisted form of one opaque extension. Canonical bytes are hex-encoded so
/// the whole set round-trips through one JSON column without a base64 crate.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredExtensionV1 {
    pub(crate) extension_id: String,
    pub(crate) extension_version: u32,
    pub(crate) required: bool,
    pub(crate) payload_sha256: String,
    pub(crate) canonical_payload_hex: String,
}

impl StoredExtensionV1 {
    pub(crate) fn from_extension(extension: &OwnedOpaqueExtension) -> Self {
        Self {
            extension_id: extension.extension_id.as_str().to_owned(),
            extension_version: extension.extension_version,
            required: extension.required,
            payload_sha256: extension.payload_sha256.clone(),
            canonical_payload_hex: lowercase_hex(&extension.canonical_payload),
        }
    }

    pub(crate) fn into_extension(self) -> Result<OwnedOpaqueExtension, ObservationJournalError> {
        let bytes =
            decode_hex(&self.canonical_payload_hex).ok_or(ObservationJournalError::Corrupt {
                table: "tdmem_observation_journal_v1",
                field: "extensions_json",
            })?;
        Ok(OwnedOpaqueExtension::new(
            OwnedVersionedId::new(self.extension_id)?,
            self.extension_version,
            self.required,
            self.payload_sha256,
            bytes,
        )?)
    }
}

pub(crate) fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(value.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = nibble(pair[0])?;
        let low = nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Some(output)
}

const fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub(crate) fn encode_json<T: Serialize>(
    value: &T,
    field: &'static str,
) -> Result<String, ObservationJournalError> {
    serde_json::to_string(value)
        .map_err(|error| ObservationJournalError::serialization(field, &error))
}

pub(crate) fn decode_json<T: for<'de> Deserialize<'de>>(
    value: &str,
    field: &'static str,
) -> Result<T, ObservationJournalError> {
    serde_json::from_str(value)
        .map_err(|error| ObservationJournalError::serialization(field, &error))
}

pub(crate) fn sql_i64(value: u64, field: &'static str) -> Result<i64, ObservationJournalError> {
    i64::try_from(value).map_err(|_| ObservationJournalError::ValueOutOfRange { field })
}

pub(crate) fn read_u64(value: i64, field: &'static str) -> Result<u64, ObservationJournalError> {
    u64::try_from(value).map_err(|_| ObservationJournalError::ValueOutOfRange { field })
}

pub(crate) fn read_u32(value: i64, field: &'static str) -> Result<u32, ObservationJournalError> {
    u32::try_from(value).map_err(|_| ObservationJournalError::ValueOutOfRange { field })
}

const WITHHELD_SELECT_COLUMNS: &str = "\
source_authority, exact_scope_sha256, source_stream, source_sequence, source_event_id, \
source_event_revision, receipt_id, reason, source_payload_sha256, extensions_digest, \
sanitizer_revision, finding_count, findings_digest, forget_source_key";

/// What one resumable withheld-audit pass did.
///
/// This is the operational surface of the bounded audit: a caller drives it
/// until `complete` is true, and the two fields together say both that
/// progress is being made and when there is nothing left to make.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WithheldAuditProgressV1 {
    /// Withheld receipts revalidated by this pass.
    pub rows_validated: u32,
    /// Whether every row the store held at open has now been revalidated.
    pub complete: bool,
}

/// The primary key of the last withheld row one bounded audit page validated.
///
/// It is a *position*, not an identity: the audit walks the table in its own
/// clustered key order and resumes strictly after this tuple, so the work is
/// paid for once however many pages it takes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WithheldAuditCursorV1 {
    pub(crate) source_authority: String,
    pub(crate) exact_scope_sha256: String,
    pub(crate) source_stream: String,
    pub(crate) source_sequence: i64,
    pub(crate) receipt_id: String,
}

/// What one bounded withheld-audit page did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WithheldAuditPageV1 {
    /// Rows revalidated by this page.
    pub(crate) rows_validated: u32,
    /// Where the next page resumes, or `None` when the table is exhausted.
    ///
    /// A short page is exhaustion: the statement is bounded by `LIMIT`, so
    /// fewer rows than the limit means the walk reached the end of the table.
    pub(crate) resume_after: Option<WithheldAuditCursorV1>,
}

/// The clustered key order the audit walks and resumes in. It is the table's
/// own `WITHOUT ROWID` primary key, so a page is an index range scan rather
/// than a sort.
const WITHHELD_KEY_ORDER: &str =
    "source_authority, exact_scope_sha256, source_stream, source_sequence, receipt_id";

/// Revalidates at most `limit` persisted withheld receipts, resuming strictly
/// after `after`.
///
/// The table carries no payload bytes, so reconstruction is bounded in memory —
/// but it is *not* bounded in rows, and a full-table scan on every open made
/// project open cost grow with the size of the audit. `LIMIT` plus a resume
/// cursor makes each pass cost what the caller asked for and nothing more.
///
/// The check itself is unchanged and still fail-closed: an audit row whose
/// receipt identity no longer matches the evidence stored beside it is
/// corruption, and the pass that meets it refuses rather than blessing it.
pub(crate) fn validate_withheld_page(
    connection: &Connection,
    after: Option<&WithheldAuditCursorV1>,
    limit: u32,
) -> Result<WithheldAuditPageV1, ObservationJournalError> {
    if limit == 0 {
        return Err(ObservationJournalError::ValueOutOfRange {
            field: "withheld_audit_page_limit",
        });
    }
    let bound = i64::from(limit);
    let mut page = WithheldAuditPageV1 {
        rows_validated: 0,
        resume_after: None,
    };
    let mut last: Option<WithheldAuditCursorV1> = None;
    {
        // Row-value comparison against the primary key, so the resume is a
        // seek into the clustered index rather than a scan that skips rows it
        // has already paid to read.
        let mut statement = connection.prepare(&match after {
            Some(_) => format!(
                "SELECT {WITHHELD_SELECT_COLUMNS} FROM tdmem_observation_withheld_v2 \
                 WHERE ({WITHHELD_KEY_ORDER}) > (?1, ?2, ?3, ?4, ?5) \
                 ORDER BY {WITHHELD_KEY_ORDER} LIMIT ?6"
            ),
            None => format!(
                "SELECT {WITHHELD_SELECT_COLUMNS} FROM tdmem_observation_withheld_v2 \
                 ORDER BY {WITHHELD_KEY_ORDER} LIMIT ?1"
            ),
        })?;
        let mut rows = match after {
            Some(cursor) => statement.query(rusqlite::params![
                cursor.source_authority,
                cursor.exact_scope_sha256,
                cursor.source_stream,
                cursor.source_sequence,
                cursor.receipt_id,
                bound,
            ])?,
            None => statement.query(rusqlite::params![bound])?,
        };
        while let Some(row) = rows.next()? {
            let withheld = decode_withheld(row).map_err(|_| ObservationJournalError::Corrupt {
                table: "tdmem_observation_withheld_v2",
                field: "receipt_id",
            })?;
            last = Some(WithheldAuditCursorV1 {
                source_sequence: sql_i64(withheld.source_sequence, "source_sequence")?,
                source_authority: withheld.source_authority,
                exact_scope_sha256: withheld.exact_scope_sha256,
                source_stream: withheld.source_stream,
                receipt_id: withheld.receipt_id,
            });
            page.rows_validated = page.rows_validated.saturating_add(1);
        }
    }
    // A full page means there may be more; a short one means the walk reached
    // the end. Claiming exhaustion on a full page would leave the tail of the
    // table permanently unvalidated.
    if page.rows_validated == limit {
        page.resume_after = last;
    }
    Ok(page)
}

fn decode_withheld(row: &Row<'_>) -> Result<WithheldAdmissionV1, ObservationJournalError> {
    let withheld = WithheldAdmissionV1 {
        source_authority: row.get(0)?,
        exact_scope_sha256: row.get(1)?,
        source_stream: row.get(2)?,
        source_sequence: read_u64(row.get(3)?, "source_sequence")?,
        source_event_id: row.get(4)?,
        source_event_revision: row.get(5)?,
        receipt_id: row.get(6)?,
        reason: row.get(7)?,
        source_payload_sha256: row.get(8)?,
        extensions_digest: row.get(9)?,
        sanitizer_revision: row.get(10)?,
        finding_count: read_u32(row.get(11)?, "finding_count")?,
        findings_digest: row.get(12)?,
        forget_source_key: ForgetSourceKeyV1::new(row.get::<_, String>(13)?)?,
    };
    withheld.validate()?;
    Ok(withheld)
}

/// Columns `LEASE_SELECT_COLUMNS` produces, in order.
pub(crate) const LEASE_SELECT_COLUMNS: &str = "\
j.idempotency_key, j.observation_id, j.provider_id, j.provider_instance_id, \
j.registration_revision, j.ready_receipt_digest, j.exact_scope_sha256, j.observation_kind, \
j.payload_contract, j.payload_sha256, j.payload_bytes, j.extensions_digest, j.extensions_json, \
j.provenance_origin, j.privacy_classification, j.retention_class, j.redaction_revision, \
j.content_policy_revision, j.forget_source_key, j.expires_at_micros, j.deadline_micros, \
j.source_sequence, j.sanitization_receipt_id, j.sanitizer_revision, j.source_payload_sha256, \
j.sanitization_receipt_json, d.attempt_number, j.exact_scope_json, j.settlement_receipt_json";

/// Rebuilds one leased observation from a joined journal/delivery row and
/// derives the lease identity the claim will use.
///
/// `claiming_instance_id` — not the instance recorded at admission — becomes the
/// target's instance. The registration is what the row was addressed to and is
/// immutable; the instance is whichever one turned up to deliver it, and the
/// receipt this lease produces must name that one for its evidence to be true.
pub(crate) fn decode_leased(
    row: &Row<'_>,
    claiming_instance_id: &str,
    lease_owner: &str,
    leased_at_unix_micros: i64,
    lease_expires_at_unix_micros: i64,
) -> Result<LeasedObservationV1, ObservationJournalError> {
    const TABLE: &str = "tdmem_observation_journal_v1";
    let idempotency_key = ObservationIdempotencyKeyV1::parse(&row.get::<_, String>(0)?)?;
    let observation_id = ObservationIdV1::parse(&row.get::<_, String>(1)?)?;
    let _admitted_instance_id: String = row.get(3)?;
    let target = ProviderTargetV1 {
        provider_id: OwnedProviderId::new(row.get::<_, String>(2)?)?,
        provider_instance_id: claiming_instance_id.to_owned(),
        registration_revision: read_u64(row.get::<_, i64>(4)?, "registration_revision")?,
        ready_receipt_digest: row.get::<_, String>(5)?,
    };
    target.validate()?;
    let exact_scope_sha256: String = row.get(6)?;
    let observation_kind = OwnedVersionedId::new(row.get::<_, String>(7)?)?;
    let payload_contract = OwnedVersionedId::new(row.get::<_, String>(8)?)?;
    let payload_sha256: String = row.get(9)?;
    let payload_bytes: Vec<u8> =
        row.get::<_, Option<Vec<u8>>>(10)?
            .ok_or(ObservationJournalError::Corrupt {
                table: TABLE,
                field: "payload_bytes",
            })?;
    let payload = CanonicalPayload::new(payload_contract, payload_bytes, payload_sha256.clone())?;
    let extensions_digest: String = row.get(11)?;
    let extensions = decode_extensions(row.get::<_, Option<String>>(12)?.as_deref())?;
    let provenance_origin = ProvenanceOriginV1::from_wire(&row.get::<_, String>(13)?)?;
    let privacy = ObservationPrivacyV1 {
        classification: PrivacyClassificationV1::from_wire(&row.get::<_, String>(14)?)?,
        retention_class: RetentionClassV1::from_wire(&row.get::<_, String>(15)?)?,
        redaction_revision: read_u32(row.get::<_, i64>(16)?, "redaction_revision")?,
        content_policy_revision: read_u32(row.get::<_, i64>(17)?, "content_policy_revision")?,
        forget_source_key: ForgetSourceKeyV1::new(row.get::<_, String>(18)?)?,
        expires_at_unix_micros: row.get(19)?,
    };
    let deadline_unix_micros: i64 = row.get(20)?;
    let source_sequence = SourceSequenceV1(read_u64(row.get::<_, i64>(21)?, "source_sequence")?);
    let sanitization = decode_sanitization(row, 22, &payload_sha256, &extensions_digest)?;
    let stored_attempts = read_u32(row.get::<_, i64>(26)?, "attempt_number")?;

    // Revalidate the two immutable JSON columns against what they must agree
    // with. A row that has drifted is an error, never a wrong delivery.
    let scope = decode_json::<StoredExactScopeV1>(&row.get::<_, String>(27)?, "exact_scope_json")?
        .into_scope()?;
    if scope.exact_scope_sha256() != exact_scope_sha256 {
        return Err(ObservationJournalError::Corrupt {
            table: TABLE,
            field: "exact_scope_json",
        });
    }
    let settlement = decode_json::<crate::settlement::CanonicalSettlementReceiptV1>(
        &row.get::<_, String>(28)?,
        "settlement_receipt_json",
    )?;
    settlement
        .validate()
        .map_err(|_| ObservationJournalError::Corrupt {
            table: TABLE,
            field: "settlement_receipt_json",
        })?;

    let attempt_number = stored_attempts.saturating_add(1);
    let lease_id = DispatchLeaseIdV1::derive(
        &idempotency_key,
        lease_owner,
        leased_at_unix_micros,
        attempt_number,
    );

    Ok(LeasedObservationV1 {
        lease_id,
        lease_expires_at_unix_micros,
        observation_id,
        idempotency_key,
        target,
        exact_scope: scope,
        exact_scope_sha256,
        observation_kind,
        payload,
        extensions,
        extensions_digest,
        privacy,
        provenance_origin,
        sanitization,
        attempt_number,
        deadline_unix_micros,
        source_sequence,
    })
}

pub(crate) fn decode_extensions(
    value: Option<&str>,
) -> Result<Vec<OwnedOpaqueExtension>, ObservationJournalError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let stored: Vec<StoredExtensionV1> = decode_json(value, "extensions_json")?;
    stored
        .into_iter()
        .map(StoredExtensionV1::into_extension)
        .collect()
}

pub(crate) fn encode_extensions(
    extensions: &[OwnedOpaqueExtension],
) -> Result<Option<String>, ObservationJournalError> {
    if extensions.is_empty() {
        return Ok(None);
    }
    let stored: Vec<StoredExtensionV1> = extensions
        .iter()
        .map(StoredExtensionV1::from_extension)
        .collect();
    Ok(Some(encode_json(&stored, "extensions_json")?))
}

/// Rebuilds the hygiene binding of a row that still holds content.
///
/// A row only reaches here through the lease path, which requires
/// `payload_bytes IS NOT NULL`, and the schema forbids content without a
/// binding — so a missing or half-cleared binding is a corrupt row, never an
/// "unsanitized" delivery. The binding is revalidated against the payload
/// digest before it can be handed to a dispatcher.
fn decode_sanitization(
    row: &Row<'_>,
    offset: usize,
    payload_sha256: &str,
    extensions_digest: &str,
) -> Result<SanitizationBindingV1, ObservationJournalError> {
    let corrupt = || ObservationJournalError::Corrupt {
        table: "tdmem_observation_journal_v1",
        field: "sanitization_receipt_id",
    };
    let receipt_id: Option<String> = row.get(offset)?;
    let sanitizer_revision: Option<String> = row.get(offset + 1)?;
    let source_payload_sha256: Option<String> = row.get(offset + 2)?;
    let receipt_json: Option<String> = row.get(offset + 3)?;
    let binding = SanitizationBindingV1 {
        receipt_id: receipt_id.ok_or_else(corrupt)?,
        sanitizer_revision: sanitizer_revision.ok_or_else(corrupt)?,
        source_payload_sha256: source_payload_sha256.ok_or_else(corrupt)?,
        receipt_json: receipt_json.ok_or_else(corrupt)?,
    };
    binding
        .validate(payload_sha256, extensions_digest)
        .map_err(|_| ObservationJournalError::Corrupt {
            table: "tdmem_observation_journal_v1",
            field: "sanitization_receipt_json",
        })?;
    Ok(binding)
}

/// Columns `RECEIPT_SELECT_COLUMNS` produces, in order.
pub(crate) const RECEIPT_SELECT_COLUMNS: &str = "\
observation_id, attempt_number, receipt_id, idempotency_key, payload_sha256, extensions_digest, \
provider_id, provider_instance_id, registration_revision, state_generation_before, \
state_generation_after, outcome, committed_effect, provider_effect_summary_json, \
provider_receipt_digest, started_at_micros, finished_at_micros, warnings_json";

/// Rebuilds one immutable delivery receipt from its stored row.
pub(crate) fn decode_receipt(
    row: &Row<'_>,
) -> Result<ObservationDeliveryReceiptV1, ObservationJournalError> {
    let receipt = ObservationDeliveryReceiptV1 {
        observation_id: ObservationIdV1::parse(&row.get::<_, String>(0)?)?,
        attempt_number: read_u32(row.get::<_, i64>(1)?, "attempt_number")?,
        receipt_id: crate::identity::DeliveryReceiptIdV1::parse(&row.get::<_, String>(2)?)?,
        idempotency_key: ObservationIdempotencyKeyV1::parse(&row.get::<_, String>(3)?)?,
        payload_sha256: row.get(4)?,
        extensions_digest: row.get(5)?,
        provider_id: OwnedProviderId::new(row.get::<_, String>(6)?)?,
        provider_instance_id: row.get(7)?,
        registration_revision: read_u64(row.get::<_, i64>(8)?, "registration_revision")?,
        state_generation_before: row
            .get::<_, Option<i64>>(9)?
            .map(|value| read_u64(value, "state_generation_before"))
            .transpose()?,
        state_generation_after: row
            .get::<_, Option<i64>>(10)?
            .map(|value| read_u64(value, "state_generation_after"))
            .transpose()?,
        outcome: ObservationOutcomeV1::from_wire(&row.get::<_, String>(11)?)?,
        committed_effect: ObservationCommittedEffectV1::from_wire(&row.get::<_, String>(12)?)?,
        provider_effect_summary: decode_json::<ProviderEffectSummaryV1>(
            &row.get::<_, String>(13)?,
            "provider_effect_summary_json",
        )?,
        provider_receipt_digest: row.get(14)?,
        started_at_unix_micros: row.get(15)?,
        finished_at_unix_micros: row.get(16)?,
        warnings: decode_json::<Vec<String>>(&row.get::<_, String>(17)?, "warnings_json")?,
    };
    receipt.validate()?;
    Ok(receipt)
}
