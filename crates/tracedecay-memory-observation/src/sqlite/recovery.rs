//! The durable recovery record: acknowledged watermark, accepted provider
//! state identity, and the bounded repair counter.
//!
//! One row per `(provider registration, source authority, exact scope, source
//! stream)`. The acknowledged watermark is written by the acknowledging
//! receipt, in that receipt's own transaction, and the only statement that
//! touches it carries `WHERE excluded.acknowledged_sequence >
//! tdmem_observation_recovery_v1.acknowledged_sequence`. There is no `UPDATE`
//! anywhere in this crate that can lower it, which is how monotonicity is
//! structural rather than a convention — a retention sweep or a privacy
//! deletion that removes an acknowledged row leaves the watermark exactly where
//! it was, so already-delivered work is never re-proposed.

use rusqlite::{OptionalExtension, Transaction, params};

use crate::error::ObservationJournalError;
use crate::identity::SourceSequenceV1;
use crate::recovery::{
    AcknowledgedPositionV1, HostRecoveryStateV1, ObservationRecoveryPortV1, ProviderCheckpointV1,
    RecoveryRefusalWriteV1, RecoveryTargetKeyV1, RecoveryTimeBudgetV1, UnacknowledgedFrontierV1,
};

use super::SqliteObservationJournal;
use super::row::{read_u32, read_u64, sql_i64};

/// Delivery states a dispatcher may still act on. Everything else has either
/// been acknowledged or ended without acknowledgement and will never be
/// delivered again.
const DELIVERABLE_STATES: &str = "'pending', 'leased', 'effect_unknown'";

const SELECT_CONTIGUOUS_ACKNOWLEDGED: &str = r#"
SELECT candidate.source_sequence, candidate.observation_id,
       MAX(candidate_receipt.finished_at_micros)
FROM tdmem_observation_journal_v1 candidate
JOIN tdmem_observation_delivery_v1 candidate_delivery
  ON candidate_delivery.idempotency_key = candidate.idempotency_key
JOIN tdmem_observation_receipt_v1 candidate_receipt
  ON candidate_receipt.observation_id = candidate.observation_id
 AND candidate_receipt.outcome IN ('applied', 'partial_effect', 'duplicate_acknowledged')
WHERE candidate_delivery.provider_id = ?1
  AND candidate_delivery.registration_revision = ?2
  AND candidate.source_authority = ?3
  AND candidate.exact_scope_sha256 = ?4
  AND candidate.source_stream = ?5
  AND NOT EXISTS (
      SELECT 1
      FROM tdmem_observation_journal_v1 prior
      JOIN tdmem_observation_delivery_v1 prior_delivery
        ON prior_delivery.idempotency_key = prior.idempotency_key
      WHERE prior_delivery.provider_id = ?1
        AND prior_delivery.registration_revision = ?2
        AND prior.source_authority = ?3
        AND prior.exact_scope_sha256 = ?4
        AND prior.source_stream = ?5
        AND prior.source_sequence < candidate.source_sequence
        AND NOT EXISTS (
            SELECT 1
            FROM tdmem_observation_receipt_v1 prior_receipt
            WHERE prior_receipt.observation_id = prior.observation_id
              AND prior_receipt.outcome IN
                  ('applied', 'partial_effect', 'duplicate_acknowledged')
        )
  )
GROUP BY candidate.source_sequence, candidate.observation_id
ORDER BY candidate.source_sequence DESC
LIMIT 1
"#;

const ADVANCE_ACKNOWLEDGED: &str = r#"
INSERT INTO tdmem_observation_recovery_v1 (
    provider_id, registration_revision, source_authority, exact_scope_sha256, source_stream,
    acknowledged_sequence, acknowledged_observation_id, acknowledged_at_micros,
    automatic_repair_attempts, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?8)
ON CONFLICT (provider_id, registration_revision, source_authority, exact_scope_sha256,
             source_stream)
DO UPDATE SET
    acknowledged_sequence = excluded.acknowledged_sequence,
    acknowledged_observation_id = excluded.acknowledged_observation_id,
    acknowledged_at_micros = excluded.acknowledged_at_micros,
    updated_at_micros = excluded.updated_at_micros
"#;

const CLEAR_ACKNOWLEDGED: &str = r#"
UPDATE tdmem_observation_recovery_v1
SET acknowledged_sequence = NULL,
    acknowledged_observation_id = NULL,
    acknowledged_at_micros = NULL,
    updated_at_micros = ?6
WHERE provider_id = ?1 AND registration_revision = ?2 AND source_authority = ?3
  AND exact_scope_sha256 = ?4 AND source_stream = ?5
"#;

const SELECT_RECOVERY_STATE: &str = r#"
SELECT acknowledged_sequence, acknowledged_at_micros, implementation_identity_sha256,
       state_schema_version, state_generation, replay_position_retained,
       automatic_repair_attempts, last_defect, last_assessment_id, updated_at_micros
FROM tdmem_observation_recovery_v1
WHERE provider_id = ?1 AND registration_revision = ?2 AND source_authority = ?3
  AND exact_scope_sha256 = ?4 AND source_stream = ?5
"#;

const ACCEPT_CHECKPOINT: &str = r#"
INSERT INTO tdmem_observation_recovery_v1 (
    provider_id, registration_revision, source_authority, exact_scope_sha256, source_stream,
    implementation_identity_sha256, state_schema_version, state_generation,
    replay_position_retained, automatic_repair_attempts, last_defect, last_assessment_id,
    updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, NULL, NULL, ?10)
ON CONFLICT (provider_id, registration_revision, source_authority, exact_scope_sha256,
             source_stream)
DO UPDATE SET
    implementation_identity_sha256 = excluded.implementation_identity_sha256,
    state_schema_version = excluded.state_schema_version,
    state_generation = excluded.state_generation,
    replay_position_retained = excluded.replay_position_retained,
    automatic_repair_attempts = 0,
    last_defect = NULL,
    last_assessment_id = NULL,
    updated_at_micros = excluded.updated_at_micros
"#;

/// Records one refusal *for one assessment identity*.
///
/// Two structural properties live in this statement rather than in a caller:
///
/// * re-recording an identity the row already carries leaves the counter where
///   it is, so an ambiguous result that is retried, a crash between the write
///   and the plan, and two dispatchers racing one incarnation all consume one
///   attempt in total rather than one each;
/// * the counter never rises above the caller's ceiling, so a target that has
///   already escalated to an operator cannot be driven past it by continued
///   assessment.
const RECORD_REFUSAL: &str = r#"
INSERT INTO tdmem_observation_recovery_v1 (
    provider_id, registration_revision, source_authority, exact_scope_sha256, source_stream,
    automatic_repair_attempts, last_defect, last_assessment_id, updated_at_micros
) VALUES (?1, ?2, ?3, ?4, ?5, MIN(1, ?8), ?6, ?7, ?9)
ON CONFLICT (provider_id, registration_revision, source_authority, exact_scope_sha256,
             source_stream)
DO UPDATE SET
    automatic_repair_attempts = CASE
        WHEN tdmem_observation_recovery_v1.last_assessment_id IS excluded.last_assessment_id
            THEN tdmem_observation_recovery_v1.automatic_repair_attempts
        WHEN tdmem_observation_recovery_v1.automatic_repair_attempts >= ?8
            THEN tdmem_observation_recovery_v1.automatic_repair_attempts
        ELSE tdmem_observation_recovery_v1.automatic_repair_attempts + 1
    END,
    last_defect = excluded.last_defect,
    last_assessment_id = excluded.last_assessment_id,
    updated_at_micros = excluded.updated_at_micros
"#;

const SELECT_REFUSAL_ATTEMPTS: &str = r#"
SELECT automatic_repair_attempts FROM tdmem_observation_recovery_v1
WHERE provider_id = ?1 AND registration_revision = ?2 AND source_authority = ?3
  AND exact_scope_sha256 = ?4 AND source_stream = ?5
"#;

/// One acknowledgement, as the watermark statement needs it.
pub(super) struct AcknowledgedWriteV1<'a> {
    /// Provider the acknowledgement came from.
    pub(super) provider_id: &'a str,
    /// Pinned registration the delivery was addressed to.
    pub(super) registration_revision: i64,
    /// Canonical source authority of the acknowledged row.
    pub(super) source_authority: &'a str,
    /// Digest of the exact coding scope.
    pub(super) exact_scope_sha256: &'a str,
    /// Stream the sequence is ordered in.
    pub(super) source_stream: &'a str,
    /// Instant the acknowledging attempt finished.
    pub(super) acknowledged_at_unix_micros: i64,
}

/// Recomputes the acknowledged watermark after one receipt settles.
///
/// Called inside the acknowledging receipt's own transaction, so a crash cannot
/// leave a receipt without its watermark or a watermark without its receipt. A
/// later row may answer before an earlier one; only the highest row whose every
/// journalled predecessor also carries an acknowledging receipt is published.
pub(super) fn advance_acknowledged_watermark(
    transaction: &Transaction<'_>,
    write: &AcknowledgedWriteV1<'_>,
) -> Result<(), ObservationJournalError> {
    let target = params![
        write.provider_id,
        write.registration_revision,
        write.source_authority,
        write.exact_scope_sha256,
        write.source_stream,
    ];
    let acknowledged = transaction
        .query_row(SELECT_CONTIGUOUS_ACKNOWLEDGED, target, |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .optional()?;
    if let Some((sequence, observation_id, acknowledged_at)) = acknowledged {
        transaction.execute(
            ADVANCE_ACKNOWLEDGED,
            params![
                write.provider_id,
                write.registration_revision,
                write.source_authority,
                write.exact_scope_sha256,
                write.source_stream,
                sequence,
                observation_id,
                acknowledged_at,
            ],
        )?;
    } else {
        transaction.execute(
            CLEAR_ACKNOWLEDGED,
            params![
                write.provider_id,
                write.registration_revision,
                write.source_authority,
                write.exact_scope_sha256,
                write.source_stream,
                write.acknowledged_at_unix_micros,
            ],
        )?;
    }
    Ok(())
}

fn sequence(value: i64, field: &'static str) -> Result<SourceSequenceV1, ObservationJournalError> {
    read_u64(value, field).map(SourceSequenceV1)
}

impl ObservationRecoveryPortV1 for SqliteObservationJournal {
    fn recovery_state(
        &self,
        target: &RecoveryTargetKeyV1,
        budget: RecoveryTimeBudgetV1,
    ) -> Result<Option<HostRecoveryStateV1>, ObservationJournalError> {
        target.validate()?;
        let revision = sql_i64(target.registration_revision, "registration_revision")?;
        self.with_bounded_connection("recovery_state", budget, |connection| {
            let row = connection
                .query_row(
                    SELECT_RECOVERY_STATE,
                    params![
                        target.provider_id.as_str(),
                        revision,
                        target.stream.source_authority.as_wire(),
                        target.stream.exact_scope_sha256.as_str(),
                        target.stream.source_stream.as_str(),
                    ],
                    |row| {
                        Ok((
                            row.get::<_, Option<i64>>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<i64>>(5)?,
                            row.get::<_, i64>(6)?,
                            row.get::<_, Option<String>>(7)?,
                            row.get::<_, Option<String>>(8)?,
                            row.get::<_, i64>(9)?,
                        ))
                    },
                )
                .optional()?;
            let Some((
                acknowledged_sequence,
                acknowledged_at_micros,
                implementation_identity_sha256,
                state_schema_version,
                state_generation,
                replay_position_retained,
                automatic_repair_attempts,
                last_defect,
                last_assessment_id,
                updated_at_micros,
            )) = row
            else {
                return Ok(None);
            };
            // The two acknowledgement columns are written together and are
            // meaningless apart, so a row carrying one without the other is
            // corruption rather than an absent watermark.
            let acknowledged = match (acknowledged_sequence, acknowledged_at_micros) {
                (Some(value), Some(instant)) => Some(AcknowledgedPositionV1 {
                    sequence: sequence(value, "acknowledged_sequence")?,
                    acknowledged_at_unix_micros: instant,
                }),
                (None, None) => None,
                _ => {
                    return Err(ObservationJournalError::Corrupt {
                        table: "tdmem_observation_recovery_v1",
                        field: "acknowledged_sequence",
                    });
                }
            };
            Ok(Some(HostRecoveryStateV1 {
                acknowledged,
                implementation_identity_sha256,
                state_schema_version,
                state_generation: state_generation
                    .map(|value| read_u64(value, "state_generation"))
                    .transpose()?,
                replay_position_retained: match replay_position_retained {
                    None => None,
                    Some(0) => Some(false),
                    Some(1) => Some(true),
                    Some(_) => {
                        return Err(ObservationJournalError::Corrupt {
                            table: "tdmem_observation_recovery_v1",
                            field: "replay_position_retained",
                        });
                    }
                },
                automatic_repair_attempts: read_u32(
                    automatic_repair_attempts,
                    "automatic_repair_attempts",
                )?,
                last_defect,
                last_assessment_id,
                updated_at_unix_micros: updated_at_micros,
            }))
        })
    }

    fn unacknowledged_frontier(
        &self,
        target: &RecoveryTargetKeyV1,
        budget: RecoveryTimeBudgetV1,
    ) -> Result<UnacknowledgedFrontierV1, ObservationJournalError> {
        target.validate()?;
        let revision = sql_i64(target.registration_revision, "registration_revision")?;
        let statement = format!(
            "SELECT \
             COALESCE(SUM(CASE WHEN d.state IN ({DELIVERABLE_STATES}) THEN 1 ELSE 0 END), 0), \
             MIN(CASE WHEN d.state IN ({DELIVERABLE_STATES}) THEN j.source_sequence END), \
             MAX(j.source_sequence) \
             FROM tdmem_observation_delivery_v1 d \
             JOIN tdmem_observation_journal_v1 j ON j.idempotency_key = d.idempotency_key \
             WHERE d.provider_id = ?1 AND d.registration_revision = ?2 \
             AND j.source_authority = ?3 AND j.exact_scope_sha256 = ?4 \
             AND j.source_stream = ?5"
        );
        self.with_bounded_connection("unacknowledged_frontier", budget, |connection| {
            let (items, first, last) = connection.query_row(
                &statement,
                params![
                    target.provider_id.as_str(),
                    revision,
                    target.stream.source_authority.as_wire(),
                    target.stream.exact_scope_sha256.as_str(),
                    target.stream.source_stream.as_str(),
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )?;
            Ok(UnacknowledgedFrontierV1 {
                unacknowledged_items: read_u64(items, "unacknowledged_items")?,
                first_unacknowledged_sequence: first
                    .map(|value| sequence(value, "first_unacknowledged_sequence"))
                    .transpose()?,
                last_journalled_sequence: last
                    .map(|value| sequence(value, "last_journalled_sequence"))
                    .transpose()?,
            })
        })
    }

    fn accept_checkpoint(
        &self,
        checkpoint: &ProviderCheckpointV1,
        now_unix_micros: i64,
        budget: RecoveryTimeBudgetV1,
    ) -> Result<(), ObservationJournalError> {
        checkpoint.validate()?;
        let target = &checkpoint.target;
        let revision = sql_i64(target.registration_revision, "registration_revision")?;
        let generation = sql_i64(checkpoint.state_generation, "state_generation")?;
        let retained = i64::from(checkpoint.replay_position.retains_position());
        self.with_bounded_transaction("accept_checkpoint", budget, |transaction| {
            transaction.execute(
                ACCEPT_CHECKPOINT,
                params![
                    target.provider_id.as_str(),
                    revision,
                    target.stream.source_authority.as_wire(),
                    target.stream.exact_scope_sha256.as_str(),
                    target.stream.source_stream.as_str(),
                    checkpoint.implementation_identity_sha256.as_str(),
                    checkpoint.state_schema_version.as_str(),
                    generation,
                    retained,
                    now_unix_micros,
                ],
            )?;
            Ok(())
        })
    }

    fn record_recovery_refusal(
        &self,
        refusal: &RecoveryRefusalWriteV1<'_>,
        budget: RecoveryTimeBudgetV1,
    ) -> Result<u32, ObservationJournalError> {
        let target = refusal.target;
        target.validate()?;
        if refusal.max_automatic_attempts == 0 {
            return Err(ObservationJournalError::ValueOutOfRange {
                field: "max_automatic_attempts",
            });
        }
        let revision = sql_i64(target.registration_revision, "registration_revision")?;
        let ceiling = i64::from(refusal.max_automatic_attempts);
        self.with_bounded_transaction("record_recovery_refusal", budget, |transaction| {
            transaction.execute(
                RECORD_REFUSAL,
                params![
                    target.provider_id.as_str(),
                    revision,
                    target.stream.source_authority.as_wire(),
                    target.stream.exact_scope_sha256.as_str(),
                    target.stream.source_stream.as_str(),
                    refusal.defect,
                    refusal.assessment.as_str(),
                    ceiling,
                    refusal.now_unix_micros,
                ],
            )?;
            let attempts: i64 = transaction.query_row(
                SELECT_REFUSAL_ATTEMPTS,
                params![
                    target.provider_id.as_str(),
                    revision,
                    target.stream.source_authority.as_wire(),
                    target.stream.exact_scope_sha256.as_str(),
                    target.stream.source_stream.as_str(),
                ],
                |row| row.get(0),
            )?;
            read_u32(attempts, "automatic_repair_attempts")
        })
    }
}
