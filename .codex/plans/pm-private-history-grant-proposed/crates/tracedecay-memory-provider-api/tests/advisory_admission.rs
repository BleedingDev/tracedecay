//! The authority result must remain bound to one live call and complete inventory.

use std::error::Error;

use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::{HistoryRelation, SourceDisposition, TerminalCode};
use tracedecay_memory_provider_api::{
    AdvisoryAdmissionAuthority, AdvisoryAdmissionError, AdvisoryCallBinding, CancellationToken,
    CanonicalPayload, CurrentAdvisoryAdmission, CurrentRestoreAdmission, CurrentSourceDisposition,
    GrantedHistorySource, HistoryGrant, MAX_ADVISORY_ADMISSION_SOURCES, OperationControl,
    OriginScopeEvidence, OriginalSourceIdentity, OwnedExactScope, OwnedOpaqueExtension,
    OwnedProviderId, OwnedVersionedId, ProviderCall, ProviderCallParts, ProviderOperation,
    RecordedValidity, RestoreDispositionCheckpoint, SourceAttribution,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn scope() -> Result<OwnedExactScope, Box<dyn Error>> {
    Ok(OwnedExactScope::new(
        "profile",
        "project",
        "repository",
        "worktree",
        "main",
        "session",
        format!("sha256:{DIGEST}"),
    )?)
}

fn payload(bytes: &[u8]) -> Result<CanonicalPayload, Box<dyn Error>> {
    Ok(CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.admission-test.v1")?,
        bytes.to_vec(),
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    )?)
}

fn call(operation: ProviderOperation) -> Result<ProviderCall, Box<dyn Error>> {
    Ok(ProviderCall::new(ProviderCallParts {
        operation,
        provider_id: OwnedProviderId::new("test.provider")?,
        registration_revision: 1,
        ready_receipt_sha256: DIGEST.into(),
        exact_scope: scope()?,
        request_id: "request".into(),
        operation_id: "operation".into(),
        expected_state_generation: 7,
        idempotency_key: Some("delivery-key".into()),
        control: OperationControl::new(i64::MAX, 60_000, CancellationToken::default()),
        payload: payload(br#"{"history_grant":{"authorization_ref":"host-grant"}}"#)?,
        required_capabilities: vec![OwnedVersionedId::new(operation.capability_id())?],
        extensions: Vec::new(),
    })?)
}

fn source() -> Result<OriginalSourceIdentity, Box<dyn Error>> {
    Ok(OriginalSourceIdentity {
        canonical_provider_id: OwnedProviderId::new("codex")?,
        canonical_session_id: "original-session".into(),
        source_key: "original-key".into(),
        stable_record_id: Some("original-record".into()),
        observation_id: "original-observation".into(),
        source_revision: Some("source-revision-9".into()),
        content_sha256: DIGEST.into(),
    })
}

fn disposition(state: SourceDisposition) -> CurrentSourceDisposition {
    CurrentSourceDisposition {
        state,
        authority_ref: "existing-disposition-authority".into(),
        authority_revision: Some(3),
        checked_at_utc_nanos: 100,
    }
}

fn restore() -> Result<CurrentRestoreAdmission, Box<dyn Error>> {
    Ok(CurrentRestoreAdmission::new(
        RestoreDispositionCheckpoint {
            exact_scope: scope()?,
            authority_ref: "existing-checkpoint".into(),
            authority_revision: Some(3),
            checked_at_utc_nanos: 100,
        },
        vec![(source()?, disposition(SourceDisposition::Available))],
    )?)
}

fn history_grant() -> Result<HistoryGrant, Box<dyn Error>> {
    let first = GrantedHistorySource {
        attribution: SourceAttribution {
            source: source()?,
            origin_scope: OriginScopeEvidence::Recorded {
                scope: scope()?,
                authority_ref: "original-event-receipt".into(),
            },
            source_sequence: 12,
            occurred_at_utc_nanos: Some(10),
            ingested_at_utc_nanos: 11,
            validity: RecordedValidity {
                valid_from_utc_nanos: Some(10),
                valid_until_utc_nanos: Some(100),
                superseded_at_utc_nanos: Some(90),
                superseded_by: Some("replacement-record".into()),
                revoked_at_utc_nanos: Some(95),
            },
        },
        current_disposition: disposition(SourceDisposition::Available),
    };
    let mut second = first.clone();
    second.attribution.source.observation_id = "second-observation".into();
    second.attribution.source_sequence = 13;
    let grant = HistoryGrant {
        authorization_ref: "untrusted-history-claim".into(),
        policy_revision: 7,
        destination_scope: scope()?,
        relation: HistoryRelation::ExactScope,
        sources: vec![first, second],
        disposition_checkpoint: restore()?.checkpoint,
    };
    grant.validate_structure()?;
    Ok(grant)
}

#[test]
fn private_history_claim_preserves_wire_payload_and_absent_binding() -> Result<(), Box<dyn Error>> {
    let original = call(ProviderOperation::Replay)?;
    assert!(original.history_grant().is_none());
    let absent = AdvisoryCallBinding::from_call(&original)?;
    // Pre-claim binding fixture: adding the optional carrier keeps None identical.
    assert_eq!(
        absent.sha256(),
        "87e437d28775e5346b564ab640f51ff1faaffbbd21465f9a14d0bf6fec22add0"
    );
    absent.verify_for(&original.clone())?;
    let grant = history_grant()?;
    let claimed = original.clone().with_history_grant(grant.clone());
    assert_eq!(claimed.history_grant(), Some(&grant));
    assert_eq!(claimed.payload, original.payload);
    assert_eq!(claimed.extensions, original.extensions);
    assert_eq!(claimed.sanitization(), original.sanitization());
    claimed.validate()?;
    assert_eq!(
        absent.verify_for(&claimed),
        Err(AdvisoryAdmissionError::BindingMismatch)
    );
    let admission = CurrentAdvisoryAdmission::new(&claimed, Vec::new(), None)?;
    admission.verify_for(&claimed.clone())?;
    assert_eq!(
        admission.verify_for(&original),
        Err(AdvisoryAdmissionError::BindingMismatch)
    );
    Ok(())
}

#[test]
fn private_history_claim_swap_invalidates_admission_for_every_nested_field()
-> Result<(), Box<dyn Error>> {
    let grant = history_grant()?;
    let original = call(ProviderOperation::Replay)?.with_history_grant(grant.clone());
    let admission = CurrentAdvisoryAdmission::new(&original, Vec::new(), None)?;
    for field in [
        "authorization",
        "policy",
        "destination",
        "relation",
        "source_count",
        "source_order",
        "provider",
        "session",
        "source_key",
        "record",
        "record_absent",
        "observation",
        "revision",
        "revision_absent",
        "content",
        "origin_scope",
        "origin_authority",
        "origin_ingestion",
        "origin_unavailable",
        "sequence",
        "occurred",
        "occurred_absent",
        "ingested",
        "valid_from",
        "valid_until",
        "superseded_at",
        "superseded_by",
        "revoked_at",
        "disposition",
        "disposition_authority",
        "disposition_revision",
        "disposition_checked",
        "checkpoint_scope",
        "checkpoint_authority",
        "checkpoint_revision",
        "checkpoint_checked",
    ] {
        let mut changed = grant.clone();
        let source = &mut changed.sources[0];
        match field {
            "authorization" => changed.authorization_ref.push('x'),
            "policy" => changed.policy_revision += 1,
            "destination" => changed.destination_scope.project_id.push('x'),
            "relation" => changed.relation = HistoryRelation::SameCheckout,
            "source_count" => {
                changed.sources.pop();
            }
            "source_order" => changed.sources.swap(0, 1),
            "provider" => {
                source.attribution.source.canonical_provider_id = OwnedProviderId::new("other")?
            }
            "session" => source.attribution.source.canonical_session_id.push('x'),
            "source_key" => source.attribution.source.source_key.push('x'),
            "record" => source.attribution.source.stable_record_id = Some("other-record".into()),
            "record_absent" => source.attribution.source.stable_record_id = None,
            "observation" => source.attribution.source.observation_id.push('x'),
            "revision" => source.attribution.source.source_revision = Some("other-revision".into()),
            "revision_absent" => source.attribution.source.source_revision = None,
            "content" => source.attribution.source.content_sha256 = OTHER_DIGEST.into(),
            "origin_scope" => {
                if let OriginScopeEvidence::Recorded { scope, .. } =
                    &mut source.attribution.origin_scope
                {
                    scope.branch_identity.push('x');
                }
            }
            "origin_authority" => {
                if let OriginScopeEvidence::Recorded { authority_ref, .. } =
                    &mut source.attribution.origin_scope
                {
                    authority_ref.push('x');
                }
            }
            "origin_ingestion" => {
                source.attribution.origin_scope = OriginScopeEvidence::IngestionOnly
            }
            "origin_unavailable" => {
                source.attribution.origin_scope = OriginScopeEvidence::Unavailable
            }
            "sequence" => source.attribution.source_sequence += 1,
            "occurred" => source.attribution.occurred_at_utc_nanos = Some(20),
            "occurred_absent" => source.attribution.occurred_at_utc_nanos = None,
            "ingested" => source.attribution.ingested_at_utc_nanos += 1,
            "valid_from" => source.attribution.validity.valid_from_utc_nanos = None,
            "valid_until" => source.attribution.validity.valid_until_utc_nanos = None,
            "superseded_at" => source.attribution.validity.superseded_at_utc_nanos = None,
            "superseded_by" => source.attribution.validity.superseded_by = None,
            "revoked_at" => source.attribution.validity.revoked_at_utc_nanos = None,
            "disposition" => source.current_disposition.state = SourceDisposition::Deleted,
            "disposition_authority" => source.current_disposition.authority_ref.push('x'),
            "disposition_revision" => source.current_disposition.authority_revision = None,
            "disposition_checked" => source.current_disposition.checked_at_utc_nanos += 1,
            "checkpoint_scope" => changed
                .disposition_checkpoint
                .exact_scope
                .worktree_identity
                .push('x'),
            "checkpoint_authority" => changed.disposition_checkpoint.authority_ref.push('x'),
            "checkpoint_revision" => changed.disposition_checkpoint.authority_revision = None,
            "checkpoint_checked" => changed.disposition_checkpoint.checked_at_utc_nanos += 1,
            _ => return Err("unknown private claim mutation".into()),
        }
        // Some substitutions are invalid claims; binding is not authorization.
        // They must still differ even though the canonical wire bytes are identical.
        let changed = original.clone().with_history_grant(changed);
        assert_eq!(changed.payload, original.payload);
        assert_eq!(
            admission.verify_for(&changed),
            Err(AdvisoryAdmissionError::BindingMismatch),
            "changed {field}"
        );
    }
    Ok(())
}

#[test]
fn binding_rejects_each_changed_call_identity_and_rehashed_grant() -> Result<(), Box<dyn Error>> {
    let original = call(ProviderOperation::Replay)?;
    let binding = AdvisoryCallBinding::from_call(&original)?;
    binding.verify_for(&original.clone())?;
    for field in [
        "provider",
        "operation",
        "registration",
        "ready",
        "profile",
        "project",
        "repository",
        "worktree",
        "branch",
        "session",
        "resolved_scope",
        "request",
        "operation_id",
        "generation",
        "idempotency",
        "payload",
        "payload_contract",
        "extension",
        "capability",
        "deadline",
        "budget",
        "control_restart",
    ] {
        let mut changed = original.clone();
        match field {
            "provider" => changed.provider_id = OwnedProviderId::new("other.provider")?,
            "operation" => {
                changed.operation = ProviderOperation::Maintenance;
                changed
                    .required_capabilities
                    .insert(OwnedVersionedId::new(changed.operation.capability_id())?);
            }
            "registration" => changed.registration_revision += 1,
            "ready" => changed.ready_receipt_sha256 = OTHER_DIGEST.into(),
            "profile" => changed.exact_scope.profile_id.push('x'),
            "project" => changed.exact_scope.project_id.push('x'),
            "repository" => changed.exact_scope.repository_identity.push('x'),
            "worktree" => changed.exact_scope.worktree_identity.push('x'),
            "branch" => changed.exact_scope.branch_identity.push('x'),
            "session" => changed.exact_scope.agent_session_id.push('x'),
            "resolved_scope" => {
                changed.exact_scope.resolved_scope_digest = format!("sha256:{OTHER_DIGEST}")
            }
            "request" => changed.request_id.push('x'),
            "operation_id" => changed.operation_id.push('x'),
            "generation" => changed.expected_state_generation += 1,
            "idempotency" => changed.idempotency_key = Some("other-delivery".into()),
            "payload" => {
                changed.payload =
                    payload(br#"{"history_grant":{"authorization_ref":"forged-grant"}}"#)?
            }
            "payload_contract" => {
                changed.payload.contract_id =
                    OwnedVersionedId::new("tracedecay.memory.other-test.v1")?
            }
            "extension" => {
                let extension = payload(b"{}")?;
                changed.extensions.push(OwnedOpaqueExtension::new(
                    OwnedVersionedId::new("vendor.extension.v1")?,
                    1,
                    false,
                    extension.sha256,
                    extension.bytes,
                )?);
            }
            "capability" => {
                changed
                    .required_capabilities
                    .insert(OwnedVersionedId::new("provider.health.v1")?);
            }
            "deadline" => {
                changed.control =
                    OperationControl::new(i64::MAX - 1, 60_000, CancellationToken::default())
            }
            "budget" => {
                changed.control =
                    OperationControl::new(i64::MAX, 59_000, CancellationToken::default())
            }
            "control_restart" => {
                changed.control =
                    OperationControl::new(i64::MAX, 60_000, CancellationToken::default());
            }
            _ => return Err("unknown test mutation".into()),
        }
        changed.validate()?;
        assert_eq!(
            binding.verify_for(&changed),
            Err(AdvisoryAdmissionError::BindingMismatch),
            "changed {field}"
        );
    }
    let mut corrupted = original.clone();
    corrupted.payload.bytes.push(b' ');
    assert!(matches!(
        binding.verify_for(&corrupted),
        Err(AdvisoryAdmissionError::Boundary(_))
    ));
    Ok(())
}

#[test]
fn binding_frames_adjacent_identifiers() -> Result<(), Box<dyn Error>> {
    let mut original = call(ProviderOperation::Replay)?;
    original.request_id = "ab".into();
    original.operation_id = "c".into();
    let binding = AdvisoryCallBinding::from_call(&original)?;
    original.request_id = "a".into();
    original.operation_id = "bc".into();
    assert_eq!(
        binding.verify_for(&original),
        Err(AdvisoryAdmissionError::BindingMismatch)
    );
    Ok(())
}

#[test]
fn admission_rechecks_live_cancellation_and_expired_control() -> Result<(), Box<dyn Error>> {
    let original = call(ProviderOperation::Replay)?;
    let admission = CurrentAdvisoryAdmission::new(&original, Vec::new(), None)?;
    original.control.cancellation().cancel();
    assert_eq!(
        admission.verify_for(&original),
        Err(AdvisoryAdmissionError::Control(TerminalCode::Cancelled))
    );
    let mut replaced_control = original.clone();
    replaced_control.control =
        OperationControl::new(i64::MAX, 60_000, CancellationToken::default());
    assert_eq!(
        admission.verify_for(&replaced_control),
        Err(AdvisoryAdmissionError::Control(TerminalCode::Cancelled))
    );
    let mut expired = call(ProviderOperation::Replay)?;
    expired.control = OperationControl::new(0, 60_000, CancellationToken::default());
    assert_eq!(
        AdvisoryCallBinding::from_call(&expired),
        Err(AdvisoryAdmissionError::Control(
            TerminalCode::DeadlineExceeded
        ))
    );
    Ok(())
}

#[test]
fn restore_requires_known_unique_complete_actual_inventory() -> Result<(), Box<dyn Error>> {
    let original = restore()?;
    original.verify_inventory(&[source()?])?;
    assert!(original.verify_inventory(&[]).is_err());
    assert!(original.verify_inventory(&[source()?, source()?]).is_err());
    let mut changed_source = source()?;
    changed_source.content_sha256 = OTHER_DIGEST.into();
    assert!(
        original
            .verify_inventory(&[changed_source.clone()])
            .is_err()
    );
    let mut duplicate = original.clone();
    duplicate
        .sources
        .push((changed_source, disposition(SourceDisposition::Deleted)));
    assert_eq!(
        duplicate.validate_for(&scope()?),
        Err(AdvisoryAdmissionError::Invalid("duplicate restore source"))
    );
    let mut malformed = original.clone();
    malformed.sources[0].0.content_sha256 = "malformed".into();
    assert!(malformed.validate_for(&scope()?).is_err());
    let mut unknown = original.clone();
    unknown.sources[0].1.state = SourceDisposition::Unknown;
    assert_eq!(
        unknown.validate_for(&scope()?),
        Err(AdvisoryAdmissionError::Invalid(
            "unknown restore disposition"
        ))
    );
    let mut missing_authority = original.clone();
    missing_authority.sources[0].1.authority_ref.clear();
    assert!(missing_authority.validate_for(&scope()?).is_err());
    let mut oversized = original.clone();
    oversized.sources = vec![original.sources[0].clone(); MAX_ADVISORY_ADMISSION_SOURCES + 1];
    assert_eq!(
        oversized.validate_for(&scope()?),
        Err(AdvisoryAdmissionError::Invalid("restore source bound"))
    );
    let mut other_scope = scope()?;
    other_scope.agent_session_id.push('x');
    assert!(original.validate_for(&other_scope).is_err());
    for state in [
        SourceDisposition::Deleted,
        SourceDisposition::Redacted,
        SourceDisposition::Expired,
    ] {
        let mut fenced = original.clone();
        fenced.sources[0].1.state = state;
        fenced.verify_inventory(&[source()?])?;
    }
    Ok(())
}

#[test]
fn restore_evidence_cannot_be_reused_for_another_operation() -> Result<(), Box<dyn Error>> {
    let restore_call = call(ProviderOperation::SnapshotRestore)?;
    let restore_evidence = restore()?;
    // Current checkpoint revision 3 is independent of provider generation 7.
    CurrentAdvisoryAdmission::new(&restore_call, Vec::new(), Some(restore_evidence.clone()))?
        .verify_for(&restore_call)?;
    assert!(CurrentAdvisoryAdmission::new(&restore_call, Vec::new(), None).is_err());
    assert!(
        CurrentAdvisoryAdmission::new(
            &call(ProviderOperation::Replay)?,
            Vec::new(),
            Some(restore_evidence)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn fresh_supersession_keeps_frozen_source_attribution() -> Result<(), Box<dyn Error>> {
    let original = call(ProviderOperation::Replay)?;
    let attribution = SourceAttribution {
        source: source()?,
        origin_scope: OriginScopeEvidence::Recorded {
            scope: scope()?,
            authority_ref: "original-event-receipt".into(),
        },
        source_sequence: 12,
        occurred_at_utc_nanos: Some(10),
        ingested_at_utc_nanos: 11,
        validity: RecordedValidity {
            valid_from_utc_nanos: Some(10),
            ..RecordedValidity::default()
        },
    };
    let refreshed = GrantedHistorySource {
        attribution: attribution.clone(),
        current_disposition: disposition(SourceDisposition::Superseded),
    };
    let admission = CurrentAdvisoryAdmission::new(&original, vec![refreshed.clone()], None)?;
    assert_eq!(admission.history_sources[0].attribution, attribution);
    assert_eq!(
        admission.history_sources[0].current_disposition.state,
        SourceDisposition::Superseded
    );
    assert!(
        CurrentAdvisoryAdmission::new(&original, vec![refreshed.clone(), refreshed], None).is_err()
    );
    Ok(())
}

#[test]
fn restore_supports_full_inventory_with_exact_attribution_subset() -> Result<(), Box<dyn Error>> {
    let restore_call = call(ProviderOperation::SnapshotRestore)?;
    let seed = GrantedHistorySource {
        attribution: SourceAttribution {
            source: source()?,
            origin_scope: OriginScopeEvidence::Recorded {
                scope: scope()?,
                authority_ref: "original-event-receipt".into(),
            },
            source_sequence: 0,
            occurred_at_utc_nanos: Some(10),
            ingested_at_utc_nanos: 11,
            validity: RecordedValidity::default(),
        },
        current_disposition: disposition(SourceDisposition::Available),
    };
    let mut history_sources = Vec::with_capacity(MAX_ADVISORY_ADMISSION_SOURCES);
    let mut inventory = Vec::with_capacity(MAX_ADVISORY_ADMISSION_SOURCES);
    for index in 0..MAX_ADVISORY_ADMISSION_SOURCES {
        let mut granted = seed.clone();
        granted.attribution.source.source_key = format!("source-{index}");
        granted.attribution.source.observation_id = format!("observation-{index}");
        granted.attribution.source.stable_record_id = Some(format!("record-{index}"));
        granted.attribution.source_sequence = u64::try_from(index)?;
        inventory.push((
            granted.attribution.source.clone(),
            granted.current_disposition.clone(),
        ));
        history_sources.push(granted);
    }
    let admission = CurrentAdvisoryAdmission::new(
        &restore_call,
        history_sources,
        Some(CurrentRestoreAdmission::new(
            restore()?.checkpoint,
            inventory,
        )?),
    )?;
    admission.verify_for(&restore_call)?;

    let mut subset = admission.clone();
    subset.history_sources.truncate(1);
    subset.verify_for(&restore_call)?;
    let mut extra = subset.clone();
    extra.history_sources.push(seed.clone());
    assert_eq!(
        extra.verify_for(&restore_call),
        Err(AdvisoryAdmissionError::Invalid(
            "restore history source binding"
        ))
    );
    for mismatch in [
        "source_digest",
        "stable_record",
        "disposition",
        "disposition_authority",
    ] {
        let mut changed = subset.clone();
        let source = &mut changed.history_sources[0];
        match mismatch {
            "source_digest" => source.attribution.source.content_sha256 = OTHER_DIGEST.into(),
            "stable_record" => {
                source.attribution.source.stable_record_id = Some("other-record".into())
            }
            "disposition" => source.current_disposition.state = SourceDisposition::Deleted,
            "disposition_authority" => {
                source.current_disposition.authority_ref = "different-authority".into()
            }
            _ => return Err("unknown restore mismatch".into()),
        }
        assert_eq!(
            changed.verify_for(&restore_call),
            Err(AdvisoryAdmissionError::Invalid(
                "restore history source binding"
            )),
            "mismatched {mismatch}"
        );
    }

    let mut oversized = admission.history_sources;
    oversized.push(seed);
    assert_eq!(
        CurrentAdvisoryAdmission::new(&call(ProviderOperation::Replay)?, oversized, None),
        Err(AdvisoryAdmissionError::Invalid("history source bound"))
    );
    Ok(())
}

#[test]
fn constructing_a_call_or_binding_does_not_satisfy_the_installed_authority()
-> Result<(), Box<dyn Error>> {
    struct DeniedAuthority;
    impl AdvisoryAdmissionAuthority for DeniedAuthority {
        fn admit(
            &self,
            _call: &ProviderCall,
        ) -> Result<CurrentAdvisoryAdmission, AdvisoryAdmissionError> {
            Err(AdvisoryAdmissionError::Denied("unknown original receipt"))
        }
    }
    // Legacy ProviderCall construction remains independent of host admission.
    let original = call(ProviderOperation::Replay)?;
    original.validate()?;
    let constructed = CurrentAdvisoryAdmission::new(&original, Vec::new(), None)?;
    constructed.verify_for(&original)?;
    let claimed = original.clone().with_history_grant(history_grant()?);
    let bound_claim = CurrentAdvisoryAdmission::new(&claimed, Vec::new(), None)?;
    bound_claim.verify_for(&claimed)?;
    let installed: &dyn AdvisoryAdmissionAuthority = &DeniedAuthority;
    assert_eq!(
        installed.admit(&original),
        Err(AdvisoryAdmissionError::Denied("unknown original receipt"))
    );
    assert_eq!(
        installed.admit(&claimed),
        Err(AdvisoryAdmissionError::Denied("unknown original receipt"))
    );
    Ok(())
}
