//! Append-only audit encoding, sealing, and event projections.

use super::codec::{
    CONFIGURATION_AUDIT_PAYLOAD_SCHEMA_VERSION, CONFIGURATION_SEALED_AUDIT_TARGET_SCHEMA_VERSION,
    SealedAuditTargetReferenceV1, StoredConfigurationAuditPayloadV1, invalid_store_data,
    unavailable_store,
};
use super::{
    ConfigurationAuditEvent, ConfigurationAuditEventId, ConfigurationAuditEventKindV1,
    ConfigurationIdempotencyKey, ConfigurationProtectedOperationV1,
    ConfigurationProtectedPlanRecordV1, ConfigurationRevisionId, ConfigurationStoreError,
    ConfigurationStoreResult, Executor, ManifestDigest, ProtectedChangePlan, QueryExecutor, Row,
    UtcMicros, canonical_sha256, derived_identifier, params,
};
use hmac::{Hmac, KeyInit, Mac};
use serde::Serialize;
use sha2::Sha256;
use zeroize::Zeroizing;

pub(super) fn encode_audit_payload(
    event: &ConfigurationAuditEvent,
) -> ConfigurationStoreResult<String> {
    serde_json::to_string(&StoredConfigurationAuditPayloadV1 {
        schema_version: CONFIGURATION_AUDIT_PAYLOAD_SCHEMA_VERSION,
        event: event.clone(),
    })
    .map_err(|error| invalid_store_data(format!("encode configuration audit payload: {error}")))
}

pub(super) const CONFIGURATION_AUDIT_REDACTION_KEY_BYTES: usize = 32;
const RECALL_LOCATOR_SUBKEY_DOMAIN: &[u8] = b"tracedecay.recall-locator-redaction-subkey.v1";

pub(super) async fn read_audit_redaction_key(
    transaction: &impl QueryExecutor,
) -> ConfigurationStoreResult<Option<Zeroizing<Vec<u8>>>> {
    let mut rows = transaction
        .query(
            "SELECT key_material FROM configuration_audit_redaction_keys WHERE singleton = 1",
            (),
        )
        .await
        .map_err(unavailable_store)?;
    let Some(row) = rows.next().await.map_err(unavailable_store)? else {
        return Ok(None);
    };
    let material = Zeroizing::new(row.get::<Vec<u8>>(0).map_err(|error| {
        invalid_store_data(format!("read configuration audit redaction key: {error}"))
    })?);
    if material.len() != CONFIGURATION_AUDIT_REDACTION_KEY_BYTES
        || rows.next().await.map_err(unavailable_store)?.is_some()
    {
        return Err(invalid_store_data(
            "configuration audit redaction key is not canonical",
        ));
    }
    Ok(Some(material))
}

pub(super) async fn ensure_audit_redaction_key(
    transaction: &impl Executor,
    created_at: UtcMicros,
) -> ConfigurationStoreResult<Zeroizing<Vec<u8>>> {
    if let Some(material) = read_audit_redaction_key(transaction).await? {
        return Ok(material);
    }
    let mut material = Zeroizing::new(vec![0_u8; CONFIGURATION_AUDIT_REDACTION_KEY_BYTES]);
    getrandom::getrandom(material.as_mut_slice()).map_err(ConfigurationStoreError::unavailable)?;
    transaction
        .execute(
            "INSERT INTO configuration_audit_redaction_keys (singleton, key_material, created_at)
             VALUES (1, ?1, ?2)",
            params![material.as_slice(), created_at.0],
        )
        .await
        .map_err(unavailable_store)?;
    Ok(material)
}

/// Derives an independent recall projection key from the durable audit key.
/// The audit key itself remains reserved for configuration audit commitments;
/// a domain-separated HMAC prevents cross-subsystem key reuse.
pub(super) fn derive_recall_locator_key(
    audit_key: &[u8],
) -> ConfigurationStoreResult<Zeroizing<Vec<u8>>> {
    if audit_key.len() != CONFIGURATION_AUDIT_REDACTION_KEY_BYTES {
        return Err(invalid_store_data(
            "configuration audit redaction key is not canonical",
        ));
    }
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(audit_key)
        .map_err(|_| invalid_store_data("configuration audit redaction key is invalid"))?;
    mac.update(&(RECALL_LOCATOR_SUBKEY_DOMAIN.len() as u64).to_be_bytes());
    mac.update(RECALL_LOCATOR_SUBKEY_DOMAIN);
    Ok(Zeroizing::new(mac.finalize().into_bytes().to_vec()))
}

pub(super) fn audit_target_commitment(
    key: &[u8],
    event_id: &ConfigurationAuditEventId,
    sealed_target_reference: &[u8],
) -> ConfigurationStoreResult<ManifestDigest> {
    let authenticated = serde_json::to_vec(&(
        "tracedecay.configuration.audit-target-commitment.v1",
        event_id,
        sealed_target_reference,
    ))
    .map_err(|error| invalid_store_data(format!("encode audit target commitment: {error}")))?;
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
        .map_err(|_| invalid_store_data("configuration audit redaction key is invalid"))?;
    mac.update(&authenticated);
    ManifestDigest::from_sha256_bytes(&mac.finalize().into_bytes())
        .map_err(ConfigurationStoreError::from)
}

pub(super) async fn seal_audit_target<T: Serialize>(
    transaction: &impl Executor,
    event_id: &ConfigurationAuditEventId,
    target: &T,
    created_at: UtcMicros,
) -> ConfigurationStoreResult<(Vec<u8>, ManifestDigest)> {
    let sealed = serde_json::to_vec(&SealedAuditTargetReferenceV1 {
        schema_version: CONFIGURATION_SEALED_AUDIT_TARGET_SCHEMA_VERSION,
        target,
    })
    .map_err(|error| invalid_store_data(format!("seal configuration audit target: {error}")))?;
    let key = ensure_audit_redaction_key(transaction, created_at).await?;
    let commitment = audit_target_commitment(&key, event_id, &sealed)?;
    Ok((sealed, commitment))
}

pub(super) async fn validate_sealed_audit_target(
    transaction: &impl QueryExecutor,
    event: &ConfigurationAuditEvent,
    sealed_target_reference: Option<&[u8]>,
) -> ConfigurationStoreResult<()> {
    let Some(sealed_target_reference) = sealed_target_reference else {
        return Ok(());
    };
    let key = read_audit_redaction_key(transaction)
        .await?
        .ok_or_else(|| invalid_store_data("configuration audit redaction key is missing"))?;
    let expected = audit_target_commitment(&key, &event.event_id, sealed_target_reference)?;
    if event.target_commitment != expected {
        return Err(invalid_store_data(
            "configuration audit target commitment does not bind its sealed reference",
        ));
    }
    Ok(())
}

pub(super) async fn insert_dry_run_audit_event(
    transaction: &impl Executor,
    record: &ConfigurationProtectedPlanRecordV1,
) -> ConfigurationStoreResult<()> {
    let event_kind = match &record.operation {
        ConfigurationProtectedOperationV1::Change(_) => {
            ConfigurationAuditEventKindV1::DryRunCreated
        }
        ConfigurationProtectedOperationV1::Rollback { .. } => {
            ConfigurationAuditEventKindV1::RollbackDryRunCreated
        }
    };
    let event_id = derived_identifier(
        "configuration.audit.v1",
        &canonical_sha256(&(
            "tracedecay.configuration.dry-run-audit-event.v1",
            &record.plan.plan_id,
            event_kind,
        ))
        .map_err(ConfigurationStoreError::from)?,
        "configuration audit event id",
    )
    .map_err(|error| invalid_store_data(error.to_string()))?;
    let (sealed_target_reference, target_commitment) = seal_audit_target(
        transaction,
        &event_id,
        &record.plan.redacted_changes,
        record.plan.created_at,
    )
    .await?;
    let event = ConfigurationAuditEvent {
        event_id,
        event_kind,
        actor_id: record.plan.actor_id.clone(),
        idempotency_key: None,
        base_revision_id: record.plan.base_revision_id.clone(),
        result_revision_id: None,
        operation_digest: record.plan.operation_digest.clone(),
        target_commitment,
        receipt_id: None,
        safe_reason_code: None,
        occurred_at: record.plan.created_at,
    };
    insert_audit_event_with_receipt_digest(
        transaction,
        &event,
        None,
        Some(&sealed_target_reference),
    )
    .await
}

pub(super) fn decode_audit_row(
    row: &Row,
) -> ConfigurationStoreResult<(ConfigurationAuditEvent, Option<Vec<u8>>)> {
    let stored_event_id = row.get::<String>(0).map_err(|error| {
        invalid_store_data(format!("read configuration audit event id: {error}"))
    })?;
    let stored_actor_id = row.get::<String>(1).map_err(|error| {
        invalid_store_data(format!("read configuration audit actor id: {error}"))
    })?;
    let stored_idempotency_key = row.get::<Option<String>>(2).map_err(|error| {
        invalid_store_data(format!("read configuration audit idempotency key: {error}"))
    })?;
    let encoded_payload = row.get::<String>(3).map_err(|error| {
        invalid_store_data(format!(
            "read configuration audit operation payload: {error}"
        ))
    })?;
    let stored_base_revision_id = row.get::<String>(4).map_err(|error| {
        invalid_store_data(format!(
            "read configuration audit base revision id: {error}"
        ))
    })?;
    let stored_result_revision_id = row.get::<Option<String>>(5).map_err(|error| {
        invalid_store_data(format!(
            "read configuration audit result revision id: {error}"
        ))
    })?;
    let sealed_target_reference = row.get::<Option<Vec<u8>>>(6).map_err(|error| {
        invalid_store_data(format!(
            "read configuration audit sealed target reference: {error}"
        ))
    })?;
    let stored_target_commitment = row.get::<String>(7).map_err(|error| {
        invalid_store_data(format!(
            "read configuration audit target commitment: {error}"
        ))
    })?;
    let stored_receipt_digest = row.get::<Option<String>>(8).map_err(|error| {
        invalid_store_data(format!("read configuration audit receipt digest: {error}"))
    })?;
    let stored_safe_reason_code = row.get::<Option<String>>(9).map_err(|error| {
        invalid_store_data(format!("read configuration audit safe reason: {error}"))
    })?;
    let stored_occurred_at = row
        .get::<i64>(10)
        .map_err(|error| invalid_store_data(format!("read configuration audit time: {error}")))?;

    let payload = serde_json::from_str::<StoredConfigurationAuditPayloadV1>(&encoded_payload)
        .map_err(|error| {
            invalid_store_data(format!("decode configuration audit payload: {error}"))
        })?;
    if payload.schema_version != CONFIGURATION_AUDIT_PAYLOAD_SCHEMA_VERSION {
        return Err(invalid_store_data(
            "unsupported configuration audit payload schema version",
        ));
    }
    payload
        .event
        .validate()
        .map_err(ConfigurationStoreError::from)?;
    let event = payload.event;
    if event.event_id.as_str() != stored_event_id
        || event.actor_id.as_str() != stored_actor_id
        || event
            .idempotency_key
            .as_ref()
            .map(ConfigurationIdempotencyKey::as_str)
            != stored_idempotency_key.as_deref()
        || event.base_revision_id.as_str() != stored_base_revision_id
        || event
            .result_revision_id
            .as_ref()
            .map(ConfigurationRevisionId::as_str)
            != stored_result_revision_id.as_deref()
        || event.target_commitment.as_str() != stored_target_commitment
        || event.receipt_id.is_some() != stored_receipt_digest.is_some()
        || event.safe_reason_code.as_deref() != stored_safe_reason_code.as_deref()
        || event.occurred_at.0 != stored_occurred_at
    {
        return Err(invalid_store_data(
            "configuration audit payload does not match immutable projections",
        ));
    }
    Ok((event, sealed_target_reference))
}

pub(super) async fn read_audit_event_from_transaction(
    transaction: &impl QueryExecutor,
    event_id: &ConfigurationAuditEventId,
) -> ConfigurationStoreResult<Option<ConfigurationAuditEvent>> {
    let mut rows = transaction
        .query(
            "SELECT event_id, actor_id, idempotency_key, operation_kind,
                    base_revision_id, result_revision_id, sealed_target_reference,
                    event_scoped_target_commitment, receipt_digest, safe_reason_code, occurred_at
             FROM configuration_audit_events
             WHERE event_id = ?1",
            params![event_id.as_str()],
        )
        .await
        .map_err(unavailable_store)?;
    let Some(row) = rows.next().await.map_err(unavailable_store)? else {
        return Ok(None);
    };
    let (event, sealed_target_reference) = decode_audit_row(&row)?;
    if rows.next().await.map_err(unavailable_store)?.is_some() {
        return Err(invalid_store_data(
            "configuration audit event id resolved to multiple rows",
        ));
    }
    validate_sealed_audit_target(transaction, &event, sealed_target_reference.as_deref()).await?;
    Ok(Some(event))
}

pub(super) async fn insert_audit_event_with_receipt_digest(
    transaction: &impl Executor,
    event: &ConfigurationAuditEvent,
    receipt_digest: Option<&ManifestDigest>,
    sealed_target_reference: Option<&[u8]>,
) -> ConfigurationStoreResult<()> {
    event.validate().map_err(ConfigurationStoreError::from)?;
    validate_sealed_audit_target(transaction, event, sealed_target_reference).await?;
    let encoded_payload = encode_audit_payload(event)?;
    let idempotency_key = event
        .idempotency_key
        .as_ref()
        .map(|value| value.as_str().to_owned());
    let result_revision_id = event
        .result_revision_id
        .as_ref()
        .map(|value| value.as_str().to_owned());
    let receipt_digest = receipt_digest.map(|value| value.as_str().to_owned());
    transaction
        .execute(
            "INSERT INTO configuration_audit_events (
                event_id, actor_id, idempotency_key, operation_kind,
                base_revision_id, result_revision_id, sealed_target_reference,
                event_scoped_target_commitment, receipt_digest, correlation_id,
                safe_reason_code, occurred_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11)",
            params![
                event.event_id.as_str(),
                event.actor_id.as_str(),
                idempotency_key,
                encoded_payload,
                event.base_revision_id.as_str(),
                result_revision_id,
                sealed_target_reference,
                event.target_commitment.as_str(),
                receipt_digest,
                event.safe_reason_code.clone(),
                event.occurred_at.0,
            ],
        )
        .await
        .map_err(unavailable_store)?;
    Ok(())
}

pub(super) fn terminal_plan_event_kind(
    event_kind: ConfigurationAuditEventKindV1,
) -> Option<&'static str> {
    match event_kind {
        ConfigurationAuditEventKindV1::Applied => Some("applied"),
        ConfigurationAuditEventKindV1::RollbackApplied => Some("rollback_applied"),
        _ => None,
    }
}

pub(super) fn is_terminal_plan_event(event_kind: &str) -> bool {
    matches!(event_kind, "applied" | "rollback_applied")
}

pub(super) async fn append_terminal_plan_event(
    transaction: &impl Executor,
    plan: &ProtectedChangePlan,
    audit_event: &ConfigurationAuditEvent,
) -> ConfigurationStoreResult<()> {
    let Some(terminal_kind) = terminal_plan_event_kind(audit_event.event_kind) else {
        return Err(invalid_store_data(
            "configuration commit with a plan requires an applied terminal audit event",
        ));
    };
    let mut rows = transaction
        .query(
            "SELECT sequence, event_kind
             FROM configuration_change_plan_events
             WHERE plan_id = ?1
             ORDER BY sequence ASC",
            params![plan.plan_id.as_str()],
        )
        .await
        .map_err(unavailable_store)?;
    let mut saw_dry_run = false;
    let mut terminal_count = 0usize;
    let mut last_sequence = None;
    while let Some(row) = rows.next().await.map_err(unavailable_store)? {
        let sequence = row.get::<i64>(0).map_err(|error| {
            invalid_store_data(format!("read configuration plan event sequence: {error}"))
        })?;
        let event_kind = row.get::<String>(1).map_err(|error| {
            invalid_store_data(format!("read configuration plan event kind: {error}"))
        })?;
        if sequence < 0 || last_sequence.is_some_and(|previous| sequence <= previous) {
            return Err(invalid_store_data(
                "configuration plan events are not strictly ordered",
            ));
        }
        if sequence == 0 && event_kind == "dry_run_created" {
            saw_dry_run = true;
        }
        if is_terminal_plan_event(&event_kind) {
            terminal_count += 1;
        }
        last_sequence = Some(sequence);
    }
    drop(rows);
    if !saw_dry_run || terminal_count != 0 {
        return Err(ConfigurationStoreError::PlanStale);
    }
    let sequence = last_sequence
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid_store_data("configuration plan event sequence overflow"))?;
    transaction
        .execute(
            "INSERT INTO configuration_change_plan_events (
                plan_id, sequence, event_kind, safe_reason_code, occurred_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                plan.plan_id.as_str(),
                sequence,
                terminal_kind,
                audit_event.safe_reason_code.clone(),
                audit_event.occurred_at.0,
            ],
        )
        .await
        .map_err(unavailable_store)?;
    Ok(())
}

pub(super) async fn has_matching_terminal_plan_event(
    transaction: &impl QueryExecutor,
    plan: &ProtectedChangePlan,
    audit_event: &ConfigurationAuditEvent,
) -> ConfigurationStoreResult<bool> {
    let Some(expected_kind) = terminal_plan_event_kind(audit_event.event_kind) else {
        return Ok(false);
    };
    let mut rows = transaction
        .query(
            "SELECT event_kind
             FROM configuration_change_plan_events
             WHERE plan_id = ?1",
            params![plan.plan_id.as_str()],
        )
        .await
        .map_err(unavailable_store)?;
    let mut terminal_count = 0usize;
    let mut matched = false;
    while let Some(row) = rows.next().await.map_err(unavailable_store)? {
        let event_kind = row.get::<String>(0).map_err(|error| {
            invalid_store_data(format!("read configuration plan terminal event: {error}"))
        })?;
        if is_terminal_plan_event(&event_kind) {
            terminal_count += 1;
            matched |= event_kind == expected_kind;
        }
    }
    Ok(terminal_count == 1 && matched)
}

pub(super) async fn audit_from_transaction(
    transaction: &impl QueryExecutor,
    after: Option<&ConfigurationAuditEventId>,
    limit: usize,
) -> ConfigurationStoreResult<Vec<ConfigurationAuditEvent>> {
    if limit == 0 {
        return Err(invalid_store_data(
            "configuration audit limit must be non-zero",
        ));
    }
    let limit = i64::try_from(limit).map_err(|_| {
        invalid_store_data("configuration audit limit exceeds SQLite integer range")
    })?;
    let cursor = if let Some(after) = after {
        after.validate().map_err(ConfigurationStoreError::from)?;
        let mut rows = transaction
            .query(
                "SELECT occurred_at FROM configuration_audit_events WHERE event_id = ?1",
                params![after.as_str()],
            )
            .await
            .map_err(unavailable_store)?;
        let Some(row) = rows.next().await.map_err(unavailable_store)? else {
            return Err(invalid_store_data(
                "configuration audit cursor does not exist",
            ));
        };
        Some((
            row.get::<i64>(0).map_err(|error| {
                invalid_store_data(format!("read configuration audit cursor time: {error}"))
            })?,
            after.as_str().to_owned(),
        ))
    } else {
        None
    };
    let mut rows = match cursor {
        Some((occurred_at, event_id)) => transaction
            .query(
                "SELECT event_id, actor_id, idempotency_key, operation_kind,
                        base_revision_id, result_revision_id, sealed_target_reference,
                        event_scoped_target_commitment, receipt_digest, safe_reason_code, occurred_at
                 FROM configuration_audit_events
                 WHERE occurred_at > ?1 OR (occurred_at = ?1 AND event_id > ?2)
                 ORDER BY occurred_at ASC, event_id ASC
                 LIMIT ?3",
                params![occurred_at, event_id, limit],
            )
            .await
            .map_err(unavailable_store)?,
        None => transaction
            .query(
                "SELECT event_id, actor_id, idempotency_key, operation_kind,
                        base_revision_id, result_revision_id, sealed_target_reference,
                        event_scoped_target_commitment, receipt_digest, safe_reason_code, occurred_at
                 FROM configuration_audit_events
                 ORDER BY occurred_at ASC, event_id ASC
                 LIMIT ?1",
                params![limit],
            )
            .await
            .map_err(unavailable_store)?,
    };
    let mut events = Vec::new();
    while let Some(row) = rows.next().await.map_err(unavailable_store)? {
        let (event, sealed_target_reference) = decode_audit_row(&row)?;
        validate_sealed_audit_target(transaction, &event, sealed_target_reference.as_deref())
            .await?;
        events.push(event);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_TABLE_SQL: &str = "
        CREATE TABLE configuration_audit_redaction_keys (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            key_material BLOB NOT NULL CHECK (length(key_material) = 32),
            created_at INTEGER NOT NULL
        )";

    #[tokio::test]
    async fn audit_redaction_key_persists_and_reopens_stably() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("configuration.db");
        let first = {
            let connection = tracedecay_runtime_core::db::engine::TestConnection::open(&path);
            connection.execute_batch(KEY_TABLE_SQL).await.unwrap();
            ensure_audit_redaction_key(&connection, UtcMicros(11))
                .await
                .unwrap()
                .to_vec()
        };

        let reopened = tracedecay_runtime_core::db::engine::TestConnection::open(&path);
        let second = ensure_audit_redaction_key(&reopened, UtcMicros(22))
            .await
            .unwrap()
            .to_vec();
        assert_eq!(first, second);

        let stored = read_audit_redaction_key(&reopened).await.unwrap().unwrap();
        assert_eq!(stored.as_slice(), first.as_slice());
    }

    #[tokio::test]
    async fn malformed_persisted_audit_redaction_key_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("configuration.db");
        let connection = tracedecay_runtime_core::db::engine::TestConnection::open(&path);
        connection
            .execute_batch(
                "CREATE TABLE configuration_audit_redaction_keys (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    key_material BLOB NOT NULL,
                    created_at INTEGER NOT NULL
                )",
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO configuration_audit_redaction_keys
                 (singleton, key_material, created_at) VALUES (1, ?1, ?2)",
                params![
                    vec![0_u8; CONFIGURATION_AUDIT_REDACTION_KEY_BYTES - 1],
                    11_i64
                ],
            )
            .await
            .unwrap();

        let error = read_audit_redaction_key(&connection).await.unwrap_err();
        assert!(matches!(
            error,
            ConfigurationStoreError::InvalidData(message)
                if message.contains("redaction key is not canonical")
        ));
    }

    #[test]
    fn recall_locator_key_uses_a_domain_separated_hmac() {
        let audit_key = vec![0x42_u8; CONFIGURATION_AUDIT_REDACTION_KEY_BYTES];
        let derived = derive_recall_locator_key(&audit_key).unwrap();

        let mut expected = <Hmac<Sha256> as KeyInit>::new_from_slice(&audit_key).unwrap();
        expected.update(&(RECALL_LOCATOR_SUBKEY_DOMAIN.len() as u64).to_be_bytes());
        expected.update(RECALL_LOCATOR_SUBKEY_DOMAIN);
        let expected = expected.finalize().into_bytes().to_vec();
        assert_eq!(derived.as_slice(), expected.as_slice());

        let mut alternate_domain = <Hmac<Sha256> as KeyInit>::new_from_slice(&audit_key).unwrap();
        let alternate_domain_bytes = b"tracedecay.configuration-audit.v1";
        alternate_domain.update(&(alternate_domain_bytes.len() as u64).to_be_bytes());
        alternate_domain.update(alternate_domain_bytes);
        let alternate = alternate_domain.finalize().into_bytes();
        assert_ne!(derived.as_slice(), alternate.as_slice());
        assert_ne!(derived.as_slice(), audit_key.as_slice());
    }

    #[test]
    fn recall_locator_key_rejects_noncanonical_audit_key_lengths() {
        let error = derive_recall_locator_key(&[0_u8; CONFIGURATION_AUDIT_REDACTION_KEY_BYTES - 1])
            .unwrap_err();
        assert!(matches!(
            error,
            ConfigurationStoreError::InvalidData(message)
                if message.contains("redaction key is not canonical")
        ));
    }
}
