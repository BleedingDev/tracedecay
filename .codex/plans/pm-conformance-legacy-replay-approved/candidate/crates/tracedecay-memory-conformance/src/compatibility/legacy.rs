//! Independent legacy storage evidence and delivery-identity classification.

use super::*;

/// Proven origin of an original public delivery identifier.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "retention", rename_all = "snake_case")]
pub enum LegacyIdentityEvidence {
    /// Read from the actual old durable representation before migration.
    Retained {
        /// Exact old stored value.
        value: String,
    },
    /// Witnessed on the original call/reply and proved absent from old storage.
    CallerObservedOnly {
        /// Exact caller value; usable as a selector, never as retained evidence.
        value: String,
    },
    /// Neither durable retention nor proved absence was established.
    Unverified,
}

impl LegacyIdentityEvidence {
    fn valid(&self) -> bool {
        match self {
            Self::Retained { value } | Self::CallerObservedOnly { value } => {
                bounded_opaque_text(value, 256)
            }
            Self::Unverified => true,
        }
    }

    fn matches_durable(&self, actual: Option<&str>) -> bool {
        match self {
            Self::Retained { value } => actual == Some(value),
            Self::CallerObservedOnly { .. } => actual.is_none(),
            Self::Unverified => true,
        }
    }
}

/// A fresh physical point read after migration, independent of public inspection.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LegacyDurableRecordEvidence {
    /// Actual persisted provider reference.
    pub stable_memory_ref: String,
    /// SHA-256 over the declared immutable old record projection.
    pub immutable_record_sha256: String,
    /// SHA-256 over the exact original stored receipt bytes.
    pub stored_receipt_bytes_sha256: String,
    /// Actual retained original public receipt digest, or its verified durable basis.
    pub original_receipt_sha256: String,
    /// Actual public operation retained in storage; never filled from caller memory.
    pub retained_operation_id: Option<String>,
    /// Actual public key retained in storage; never filled from caller memory.
    pub retained_idempotency_key: Option<String>,
}

/// Delivery-identity preservation is distinct from overall expected compatibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyDeliveryIdentityCoverage {
    /// Both inspections and both physical audits preserved all original identities.
    Complete,
    /// Known original identity loss was reported; actual records/receipts survived.
    Degraded,
    /// Required identity or independent audit evidence is unavailable.
    Unknown,
    /// Observed delivery identity, record or receipt evidence violated an invariant.
    Failed,
}

pub(super) fn record_valid(record: &LegacyRecordEvidence) -> bool {
    bounded_opaque_text(&record.stable_memory_ref, 1024)
        && record.original_operation_id.valid()
        && record.original_idempotency_key.valid()
        && crate::fixture::is_lowercase_sha256(&record.original_receipt_sha256)
        && crate::fixture::is_lowercase_sha256(&record.immutable_record_sha256)
        && crate::fixture::is_lowercase_sha256(&record.stored_receipt_bytes_sha256)
        && legacy_fields_valid(&record.retained_source_fields)
}

pub(super) fn durable_valid(record: &LegacyDurableRecordEvidence) -> bool {
    bounded_opaque_text(&record.stable_memory_ref, 1024)
        && crate::fixture::is_lowercase_sha256(&record.original_receipt_sha256)
        && crate::fixture::is_lowercase_sha256(&record.immutable_record_sha256)
        && crate::fixture::is_lowercase_sha256(&record.stored_receipt_bytes_sha256)
        && record
            .retained_operation_id
            .as_ref()
            .is_none_or(|value| bounded_opaque_text(value, 256))
        && record
            .retained_idempotency_key
            .as_ref()
            .is_none_or(|value| bounded_opaque_text(value, 256))
}

pub(super) fn records(
    values: &BTreeMap<String, Value>,
    step: &str,
) -> Option<Vec<LegacyRecordEvidence>> {
    let records = serde_json::from_value::<Vec<LegacyRecordEvidence>>(
        values.get(step)?.get("records")?.clone(),
    )
    .ok()?;
    (!records.is_empty() && records.len() <= 64 && records.iter().all(record_valid))
        .then_some(records)
}

pub(super) fn durable_errors(
    expected: &[LegacyRecordEvidence],
    actual: &[LegacyDurableRecordEvidence],
) -> Vec<String> {
    let unique: BTreeSet<_> = actual
        .iter()
        .map(|record| &record.stable_memory_ref)
        .collect();
    if expected.is_empty()
        || actual.len() != expected.len()
        || unique.len() != actual.len()
        || actual.iter().any(|record| !durable_valid(record))
    {
        return vec![
            "legacy physical audit record count, uniqueness or evidence shape differs".into(),
        ];
    }
    expected.iter().filter_map(|before| {
        let same = actual.iter().find(|after| after.stable_memory_ref == before.stable_memory_ref).is_some_and(|after| {
            before.immutable_record_sha256 == after.immutable_record_sha256
                && before.stored_receipt_bytes_sha256 == after.stored_receipt_bytes_sha256
                && before.original_receipt_sha256 == after.original_receipt_sha256
                && before.original_operation_id.matches_durable(after.retained_operation_id.as_deref())
                && before.original_idempotency_key.matches_durable(after.retained_idempotency_key.as_deref())
        });
        (!same).then(|| format!("legacy durable reference, immutable record, original receipt or retained identifier changed: {}", before.stable_memory_ref))
    }).collect()
}

pub(super) fn prerequisite(
    values: &BTreeMap<String, Value>,
    installed_step: &str,
    inspected_step: &str,
) -> Option<String> {
    let Some(records) = records(values, installed_step) else {
        return Some("legacy installation evidence unavailable".into());
    };
    if records.iter().any(|record| {
        matches!(
            record.original_operation_id,
            LegacyIdentityEvidence::Unverified
        ) || matches!(
            record.original_idempotency_key,
            LegacyIdentityEvidence::Unverified
        )
    }) {
        return Some("original legacy delivery identity retention is unverified".into());
    }
    if values
        .get(inspected_step)
        .and_then(|value| value.get("verified_installation"))
        .and_then(Value::as_str)
        != Some(installed_step)
    {
        return Some("independent legacy durable-state audit unavailable".into());
    }
    None
}

pub(super) fn receipt_verdict(
    records: &[LegacyRecordEvidence],
    value: &Value,
    reply: &ProviderReply,
) -> CompatibilityVerdict {
    if records.is_empty() || records.iter().any(|record| !record_valid(record)) {
        return CompatibilityVerdict::Unknown;
    }
    if records.iter().any(|record| {
        matches!(
            record.original_operation_id,
            LegacyIdentityEvidence::Unverified
        ) || matches!(
            record.original_idempotency_key,
            LegacyIdentityEvidence::Unverified
        )
    }) {
        return CompatibilityVerdict::Unknown;
    }
    let effect = reply.terminal.committed_effect();
    if effect.state() != CommittedEffectState::None
        || !matches!((effect.state_generation_before(), effect.state_generation_after()), (Some(before), Some(after)) if before == after && after == reply.state_generation)
        || reply.terminal.fallback() != &FallbackDirective::forbidden()
    {
        return CompatibilityVerdict::Failed;
    }
    let expected: Vec<Value> = records
        .iter()
        .filter_map(|record| {
            match (
                &record.original_operation_id,
                &record.original_idempotency_key,
            ) {
                (
                    LegacyIdentityEvidence::Retained { value: operation },
                    LegacyIdentityEvidence::Retained { value: key },
                ) => Some(json!({
                    "operation_id": operation, "idempotency_key": key,
                    "provider_receipt_digest": record.original_receipt_sha256,
                    "stable_memory_ref": record.stable_memory_ref,
                })),
                _ => None,
            }
        })
        .collect();
    let Some(items) = value.get("items").and_then(Value::as_array) else {
        return CompatibilityVerdict::Failed;
    };
    if items.len() != expected.len()
        || expected
            .iter()
            .any(|item| items.iter().filter(|actual| *actual == item).count() != 1)
    {
        return CompatibilityVerdict::Failed;
    }
    if expected.len() == records.len() {
        return if reply.terminal.terminal_code() == TerminalCode::Success {
            CompatibilityVerdict::Passed
        } else {
            CompatibilityVerdict::Failed
        };
    }
    let warnings_reported = |warnings: &[String]| {
        !warnings.is_empty()
            && warnings.len() <= 32
            && warnings
                .iter()
                .all(|warning| bounded_opaque_text(warning, 4096))
            && warnings.iter().map(String::len).sum::<usize>() <= 32 * 1024
    };
    let payload_warnings = value
        .get("warnings")
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<String>>(value).ok());
    let reported = value.get("coverage").and_then(Value::as_str) == Some("partial")
        || warnings_reported(&reply.warnings)
        || payload_warnings.as_deref().is_some_and(warnings_reported);
    if matches!(
        reply.terminal.terminal_code(),
        TerminalCode::Success | TerminalCode::Partial
    ) && reported
    {
        CompatibilityVerdict::Degraded
    } else {
        CompatibilityVerdict::Failed
    }
}

pub(super) fn aggregate(
    complete_program: bool,
    verdicts: &[CompatibilityVerdict],
) -> LegacyDeliveryIdentityCoverage {
    if verdicts.contains(&CompatibilityVerdict::Failed) {
        LegacyDeliveryIdentityCoverage::Failed
    } else if !complete_program
        || verdicts.len() != 4
        || verdicts.contains(&CompatibilityVerdict::Unknown)
    {
        LegacyDeliveryIdentityCoverage::Unknown
    } else if verdicts.contains(&CompatibilityVerdict::Degraded) {
        LegacyDeliveryIdentityCoverage::Degraded
    } else {
        LegacyDeliveryIdentityCoverage::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_memory_provider_api::{
        CommittedEffectEvidence, OwnedProviderId, TerminalRecord,
    };

    fn record() -> LegacyRecordEvidence {
        LegacyRecordEvidence {
            stable_memory_ref: "original-ref".into(),
            original_operation_id: LegacyIdentityEvidence::Retained {
                value: "original-op".into(),
            },
            original_idempotency_key: LegacyIdentityEvidence::Retained {
                value: "original-key".into(),
            },
            original_receipt_sha256: "a".repeat(64),
            immutable_record_sha256: "b".repeat(64),
            stored_receipt_bytes_sha256: "c".repeat(64),
            retained_source_fields: BTreeMap::new(),
        }
    }
    fn durable(record: &LegacyRecordEvidence) -> LegacyDurableRecordEvidence {
        LegacyDurableRecordEvidence {
            stable_memory_ref: record.stable_memory_ref.clone(),
            immutable_record_sha256: record.immutable_record_sha256.clone(),
            stored_receipt_bytes_sha256: record.stored_receipt_bytes_sha256.clone(),
            original_receipt_sha256: record.original_receipt_sha256.clone(),
            retained_operation_id: match &record.original_operation_id {
                LegacyIdentityEvidence::Retained { value } => Some(value.clone()),
                _ => None,
            },
            retained_idempotency_key: match &record.original_idempotency_key {
                LegacyIdentityEvidence::Retained { value } => Some(value.clone()),
                _ => None,
            },
        }
    }
    fn reply(code: TerminalCode) -> Result<ProviderReply, String> {
        Ok(ProviderReply {
            terminal: TerminalRecord::new(
                ProviderOperation::Inspection,
                OwnedProviderId::new("test.legacy").map_err(|e| e.to_string())?,
                code,
                CommittedEffectEvidence::none(Some(7)),
                FallbackDirective::forbidden(),
                "inspection-op",
                "d".repeat(64),
                None,
            )
            .map_err(|e| e.to_string())?,
            payload: None,
            warnings: vec![],
            extensions: vec![],
            state_generation: 7,
        })
    }
    fn item() -> Value {
        json!({"operation_id":"original-op","idempotency_key":"original-key", "provider_receipt_digest":"a".repeat(64),"stable_memory_ref":"original-ref"})
    }
    fn caller_only() -> LegacyRecordEvidence {
        let mut record = record();
        record.original_operation_id = LegacyIdentityEvidence::CallerObservedOnly {
            value: "original-op".into(),
        };
        record.original_idempotency_key = LegacyIdentityEvidence::CallerObservedOnly {
            value: "original-key".into(),
        };
        record
    }

    #[test]
    fn retained_identity_still_requires_exact_original_four_field_item() -> Result<(), String> {
        let record = record();
        let reply = reply(TerminalCode::Success)?;
        assert_eq!(
            receipt_verdict(&[record.clone()], &json!({"items":[item()]}), &reply),
            CompatibilityVerdict::Passed
        );
        assert_eq!(
            receipt_verdict(
                &[record.clone()],
                &json!({"items":[],"coverage":"partial"}),
                &reply
            ),
            CompatibilityVerdict::Failed
        );
        for field in [
            "operation_id",
            "idempotency_key",
            "provider_receipt_digest",
            "stable_memory_ref",
            "extra",
        ] {
            let mut forged = item();
            forged[field] = json!("forged");
            assert_eq!(
                receipt_verdict(&[record.clone()], &json!({"items":[forged]}), &reply),
                CompatibilityVerdict::Failed,
                "{field}"
            );
        }
        assert_eq!(
            receipt_verdict(
                &[record],
                &json!({"items":[item()]}),
                &self::reply(TerminalCode::Partial)?
            ),
            CompatibilityVerdict::Failed
        );
        Ok(())
    }

    #[test]
    fn known_identity_loss_requires_omission_and_reported_degradation() -> Result<(), String> {
        let record = caller_only();
        let mut reply = reply(TerminalCode::Success)?;
        let empty = json!({"items":[],"coverage":"complete","warnings":[]});
        assert_eq!(
            receipt_verdict(&[record.clone()], &empty, &reply),
            CompatibilityVerdict::Failed
        );
        for value in [
            json!({"items":[],"coverage":"partial"}),
            json!({"items":[],"warnings":["original identifiers unavailable"]}),
        ] {
            assert_eq!(
                receipt_verdict(&[record.clone()], &value, &reply),
                CompatibilityVerdict::Degraded
            );
        }
        reply
            .warnings
            .push("original identifiers unavailable".into());
        assert_eq!(
            receipt_verdict(&[record.clone()], &empty, &reply),
            CompatibilityVerdict::Degraded
        );
        for forged in [
            item(),
            json!({"operation_id":null,"idempotency_key":null,"provider_receipt_digest":"a".repeat(64),"stable_memory_ref":"original-ref"}),
        ] {
            assert_eq!(
                receipt_verdict(
                    &[record.clone()],
                    &json!({"items":[forged],"coverage":"partial"}),
                    &reply
                ),
                CompatibilityVerdict::Failed
            );
        }
        let mut missing = record.clone();
        missing.stable_memory_ref = "second-ref".into();
        assert_eq!(
            receipt_verdict(
                &[self::record(), missing],
                &json!({"items":[item()],"coverage":"partial"}),
                &reply
            ),
            CompatibilityVerdict::Degraded
        );
        let mut unknown = record;
        unknown.original_operation_id = LegacyIdentityEvidence::Unverified;
        assert_eq!(
            receipt_verdict(&[unknown], &empty, &reply),
            CompatibilityVerdict::Unknown
        );
        reply.state_generation = 8;
        assert_eq!(
            receipt_verdict(&[caller_only()], &empty, &reply),
            CompatibilityVerdict::Failed
        );
        Ok(())
    }

    #[test]
    fn actual_record_and_receipt_changes_fail_even_when_public_items_are_omitted() {
        for before in [record(), caller_only()] {
            let after = durable(&before);
            assert!(durable_errors(&[before.clone()], &[after.clone()]).is_empty());
            for field in [
                "stable_memory_ref",
                "immutable_record_sha256",
                "stored_receipt_bytes_sha256",
                "original_receipt_sha256",
                "retained_operation_id",
                "retained_idempotency_key",
            ] {
                let mut value = serde_json::to_value(&after).expect("evidence");
                value[field] = json!(if field.contains("sha256") {
                    "e".repeat(64)
                } else {
                    "forged".into()
                });
                let changed = serde_json::from_value(value).expect("typed change");
                assert!(
                    !durable_errors(&[before.clone()], &[changed]).is_empty(),
                    "{field}"
                );
            }
            assert!(!durable_errors(&[before.clone()], &[]).is_empty());
            assert!(!durable_errors(&[before], &[after.clone(), after]).is_empty());
        }
    }

    #[test]
    fn each_restart_requires_its_own_independent_audit_and_coverage_stays_explicit() {
        let mut values = BTreeMap::from([("install".into(), json!({"records":[caller_only()]}))]);
        assert!(prerequisite(&values, "install", "audit_one").is_some());
        values.insert(
            "audit_one".into(),
            json!({"verified_installation":"install"}),
        );
        assert!(prerequisite(&values, "install", "audit_one").is_none());
        assert!(prerequisite(&values, "install", "audit_two").is_some());
        assert_eq!(
            aggregate(true, &[CompatibilityVerdict::Passed; 4]),
            LegacyDeliveryIdentityCoverage::Complete
        );
        assert_eq!(
            aggregate(
                true,
                &[
                    CompatibilityVerdict::Passed,
                    CompatibilityVerdict::Degraded,
                    CompatibilityVerdict::Passed,
                    CompatibilityVerdict::Degraded
                ]
            ),
            LegacyDeliveryIdentityCoverage::Degraded
        );
        assert_eq!(
            aggregate(true, &[CompatibilityVerdict::Unknown]),
            LegacyDeliveryIdentityCoverage::Unknown
        );
        assert_eq!(
            aggregate(false, &[CompatibilityVerdict::Passed; 4]),
            LegacyDeliveryIdentityCoverage::Unknown
        );
        assert_eq!(
            aggregate(true, &[CompatibilityVerdict::Failed]),
            LegacyDeliveryIdentityCoverage::Failed
        );
    }

    #[test]
    fn amended_program_retains_every_old_action_and_adds_two_postrestart_audits()
    -> Result<(), String> {
        let scope = OwnedExactScope::new(
            "profile",
            "project",
            "repository",
            "worktree",
            "branch",
            "session",
            format!("sha256:{}", "1".repeat(64)),
        )
        .map_err(|e| e.to_string())?;
        let scenarios = common_advisory_scenarios(&scope, 1)?;
        assert_eq!(
            scenarios
                .iter()
                .map(|scenario| scenario.steps.len())
                .sum::<usize>(),
            192
        );
        let migration = scenarios
            .iter()
            .find(|scenario| scenario.case_id == "common.real_v1_migration")
            .ok_or("migration absent")?;
        for (restart, audit, receipt) in [
            (
                "migration.restart",
                "migration.audit_durable_state",
                "migration.original_receipt",
            ),
            (
                "migration.restart_again",
                "migration.audit_durable_state_again",
                "migration.original_receipt_after_second_restart",
            ),
        ] {
            let at = migration
                .steps
                .iter()
                .position(|step| step.step_id() == restart)
                .ok_or("restart absent")?;
            assert_eq!(migration.steps[at + 1].step_id(), audit);
            assert_eq!(migration.steps[at + 2].step_id(), receipt);
        }
        Ok(())
    }
}
