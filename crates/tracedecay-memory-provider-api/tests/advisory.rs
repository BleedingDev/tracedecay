//! Shared semantic examples for the common advisory profile.

use std::error::Error;

use tracedecay_memory_provider_api::contract::{
    COMMON_ADVISORY_PROFILE_ID, COMMON_ADVISORY_REQUIRED_CAPABILITIES, HistoryRelation,
    SourceDisposition, TemporalMode, UnknownValidityPolicy,
};
use tracedecay_memory_provider_api::{
    AdvisoryContractError, CurrentSourceDisposition, GrantedHistorySource, HistoryGrant,
    LifecycleTarget, LifecycleTargetReference, OriginScopeEvidence, OriginalSourceIdentity,
    OwnedExactScope, OwnedProviderId, OwnedRecallExclusions, OwnedTemporalQuery, OwnedVersionedId,
    ProviderDescriptor, ProviderLimits, RecordedValidity, ReplayAccounting,
    RestoreDispositionCheckpoint, SourceAttribution, TemporalEligibility,
};

const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SCOPE_DIGEST: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn scope(session: &str) -> Result<OwnedExactScope, Box<dyn Error>> {
    Ok(OwnedExactScope::new(
        "profile",
        "project",
        "repo",
        "worktree",
        "main",
        session,
        SCOPE_DIGEST,
    )?)
}

fn source() -> Result<OriginalSourceIdentity, Box<dyn Error>> {
    Ok(OriginalSourceIdentity {
        canonical_provider_id: OwnedProviderId::new("codex")?,
        canonical_session_id: "session-a".into(),
        source_key: "original-source-key".into(),
        stable_record_id: Some("message-12".into()),
        observation_id: "observation-12".into(),
        source_revision: Some("git:cache_policy_r7".into()),
        content_sha256: DIGEST.into(),
    })
}

#[test]
fn legacy_descriptor_stays_valid_but_profile_requires_every_operation() -> Result<(), Box<dyn Error>>
{
    let mut descriptor = ProviderDescriptor::new(
        OwnedProviderId::new("fixture.provider")?,
        DIGEST,
        "state.v1",
        4,
        [
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1",
        ]
        .into_iter()
        .map(OwnedVersionedId::new)
        .collect::<Result<Vec<_>, _>>()?,
        ProviderLimits {
            request_bytes: 1024,
            response_bytes: 2048,
            observation_batch_items: 8,
            recall_candidates: 8,
            concurrent_operations: 1,
            operation_millis: 500,
            snapshot_bytes: 4096,
            inspection_items: 8,
        },
    )?;
    descriptor.validate()?;
    assert!(descriptor.validate_common_advisory_profile().is_err());
    descriptor
        .capabilities
        .insert(OwnedVersionedId::new(COMMON_ADVISORY_PROFILE_ID)?);
    assert!(descriptor.validate().is_err());
    for capability in COMMON_ADVISORY_REQUIRED_CAPABILITIES {
        descriptor
            .capabilities
            .insert(OwnedVersionedId::new(*capability)?);
    }
    descriptor.validate()?;
    assert!(!descriptor.supports("facts.explicit.v1"));
    for required in COMMON_ADVISORY_REQUIRED_CAPABILITIES {
        let mut incomplete = descriptor.clone();
        incomplete
            .capabilities
            .remove(&OwnedVersionedId::new(*required)?);
        assert!(
            incomplete.validate_common_advisory_profile().is_err(),
            "missing {required}"
        );
    }
    Ok(())
}

#[test]
fn time_modes_use_recorded_intervals_and_revision_lineage() -> Result<(), Box<dyn Error>> {
    let validity = RecordedValidity {
        valid_from_utc_nanos: Some(10),
        valid_until_utc_nanos: Some(20),
        superseded_at_utc_nanos: Some(20),
        superseded_by: Some("replacement-8".into()),
        revoked_at_utc_nanos: None,
    };
    let mut query = OwnedTemporalQuery::current(30);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Excluded
    );
    query.mode = TemporalMode::AsOf;
    query.as_of_utc_nanos = Some(10);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Eligible
    );
    query.as_of_utc_nanos = Some(20);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Excluded
    );
    query.mode = TemporalMode::Interval;
    query.as_of_utc_nanos = None;
    query.interval_start_utc_nanos = Some(19);
    query.interval_end_utc_nanos = Some(21);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Eligible
    );
    query.interval_start_utc_nanos = Some(20);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Excluded
    );
    query.mode = TemporalMode::History;
    query.interval_start_utc_nanos = None;
    query.interval_end_utc_nanos = None;
    query.include_superseded = true;
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Superseded, false)?,
        TemporalEligibility::Eligible
    );
    Ok(())
}

#[test]
fn privacy_disposition_dominates_history_and_unknown_validity_policy() -> Result<(), Box<dyn Error>>
{
    let mut query = OwnedTemporalQuery::current(30);
    query.mode = TemporalMode::History;
    query.include_superseded = true;
    query.include_revoked = true;
    query.unknown_validity_policy = UnknownValidityPolicy::AllowWithWarning;
    let validity = RecordedValidity::default();
    query.unknown_validity_policy = UnknownValidityPolicy::Degrade;
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::IncludedUnknown
    );
    query.unknown_validity_policy = UnknownValidityPolicy::AllowWithWarning;
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::IncludedUnknown
    );
    for disposition in [
        SourceDisposition::Deleted,
        SourceDisposition::Redacted,
        SourceDisposition::Expired,
    ] {
        assert_eq!(
            validity.eligibility(&query, disposition, false)?,
            TemporalEligibility::Excluded
        );
    }
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Available, true)?,
        TemporalEligibility::Excluded
    );
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Unknown, false)?,
        TemporalEligibility::WithheldUnknown
    );
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Revoked, false)?,
        TemporalEligibility::WithheldUnknown
    );
    Ok(())
}

#[test]
fn missing_validity_does_not_override_known_revocation_or_invent_past_truth()
-> Result<(), Box<dyn Error>> {
    let mut query = OwnedTemporalQuery::current(30);
    query.unknown_validity_policy = UnknownValidityPolicy::AllowWithWarning;
    let validity = RecordedValidity {
        revoked_at_utc_nanos: Some(20),
        ..RecordedValidity::default()
    };
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Revoked, false)?,
        TemporalEligibility::Excluded
    );
    query.mode = TemporalMode::AsOf;
    query.as_of_utc_nanos = Some(10);
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Revoked, false)?,
        TemporalEligibility::IncludedUnknown
    );
    query.unknown_validity_policy = UnknownValidityPolicy::Exclude;
    assert_eq!(
        validity.eligibility(&query, SourceDisposition::Revoked, false)?,
        TemporalEligibility::WithheldUnknown
    );
    Ok(())
}

#[test]
fn history_and_mixed_known_bounds_match_host_temporal_admission() -> Result<(), Box<dyn Error>> {
    let mut query = OwnedTemporalQuery::current(30);
    query.mode = TemporalMode::History;
    let future = RecordedValidity {
        valid_from_utc_nanos: Some(40),
        ..Default::default()
    };
    let expired = RecordedValidity {
        valid_from_utc_nanos: Some(10),
        valid_until_utc_nanos: Some(20),
        ..Default::default()
    };
    assert_eq!(
        future.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::Excluded
    );
    assert_eq!(
        expired.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::Eligible
    );
    let unknown_start = RecordedValidity {
        valid_until_utc_nanos: Some(20),
        ..Default::default()
    };
    query.mode = TemporalMode::AsOf;
    query.as_of_utc_nanos = Some(19);
    query.unknown_validity_policy = UnknownValidityPolicy::AllowWithWarning;
    assert_eq!(
        unknown_start.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::IncludedUnknown
    );
    query.as_of_utc_nanos = Some(20);
    assert_eq!(
        unknown_start.eligibility(&query, SourceDisposition::Available, false)?,
        TemporalEligibility::Excluded
    );
    Ok(())
}

#[test]
fn invalid_temporal_bounds_and_exclusions_are_rejected() -> Result<(), Box<dyn Error>> {
    assert_eq!(TemporalMode::from_wire("latest_or_anything"), None);
    assert_eq!(UnknownValidityPolicy::from_wire("silently_allow"), None);
    let mut query = OwnedTemporalQuery::current(10);
    assert!(query.validate_at(9).is_err());
    query.mode = TemporalMode::AsOf;
    query.as_of_utc_nanos = Some(11);
    assert!(query.validate().is_err());
    query.as_of_utc_nanos = None;
    query.mode = TemporalMode::Interval;
    query.interval_start_utc_nanos = Some(5);
    query.interval_end_utc_nanos = Some(5);
    assert!(query.validate().is_err());
    let mut exclusions = OwnedRecallExclusions {
        source_refs: vec!["original-source-key".into()],
        content_sha256: vec![DIGEST.into()],
        ..Default::default()
    };
    exclusions.validate()?;
    exclusions.source_refs.push("original-source-key".into());
    assert!(exclusions.validate().is_err());
    exclusions.source_refs.pop();
    exclusions.content_sha256[0] = DIGEST.to_uppercase();
    assert!(exclusions.validate().is_err());
    Ok(())
}

fn grant() -> Result<HistoryGrant, Box<dyn Error>> {
    Ok(HistoryGrant {
        authorization_ref: "host-admission-12".into(),
        policy_revision: 2,
        destination_scope: scope("session-b")?,
        relation: HistoryRelation::SameCheckout,
        sources: vec![GrantedHistorySource {
            attribution: SourceAttribution {
                source: source()?,
                origin_scope: OriginScopeEvidence::Recorded {
                    scope: scope("session-a")?,
                    authority_ref: "observation-scope-proof-12".into(),
                },
                source_sequence: 12,
                occurred_at_utc_nanos: Some(10),
                ingested_at_utc_nanos: 50,
                validity: RecordedValidity::default(),
            },
            current_disposition: CurrentSourceDisposition {
                state: SourceDisposition::Available,
                authority_ref: "canonical-disposition-12".into(),
                authority_revision: Some(7),
                checked_at_utc_nanos: 60,
            },
        }],
        disposition_checkpoint: RestoreDispositionCheckpoint {
            exact_scope: scope("session-b")?,
            authority_ref: "current-host-checkpoint".into(),
            authority_revision: Some(7),
            checked_at_utc_nanos: 60,
        },
    })
}

#[test]
fn history_retains_origin_and_refuses_unavailable_or_ingestion_only_evidence()
-> Result<(), Box<dyn Error>> {
    let mut grant = grant()?;
    grant.validate_structure()?;
    assert_eq!(
        grant.sources[0].attribution.source.canonical_session_id,
        "session-a"
    );
    assert_eq!(grant.destination_scope.agent_session_id, "session-b");
    assert_eq!(
        grant.sources[0].attribution.validity.valid_from_utc_nanos,
        None
    );
    grant.relation = HistoryRelation::ExactScope;
    assert!(grant.validate_structure().is_err());
    grant.relation = HistoryRelation::SameCheckout;
    for origin in [
        OriginScopeEvidence::Unavailable,
        OriginScopeEvidence::IngestionOnly,
    ] {
        grant.sources[0].attribution.origin_scope = origin;
        assert_eq!(
            grant.validate_structure(),
            Err(AdvisoryContractError::OriginUnavailable)
        );
    }
    Ok(())
}

#[test]
fn current_disposition_checkpoint_is_scope_bound_without_provider_generation_ordering()
-> Result<(), Box<dyn Error>> {
    let grant = grant()?;
    let checkpoint = &grant.disposition_checkpoint;
    checkpoint.validate_for(&scope("session-b")?)?;
    // Re-reading an unchanged authoritative revision is allowed. This helper
    // proves only shape; host current-read/revalidation supplies freshness.
    let mut reread = checkpoint.clone();
    reread.checked_at_utc_nanos = 90;
    reread.validate_for(&scope("session-b")?)?;
    assert_eq!(reread.authority_revision, checkpoint.authority_revision);
    assert!(reread.validate_for(&scope("session-c")?).is_err());
    Ok(())
}

#[test]
fn lifecycle_targets_pin_producer_delivery_scope_and_actual_revision() -> Result<(), Box<dyn Error>>
{
    let mut target = LifecycleTarget {
        provider_id: OwnedProviderId::new("tracedecay.native")?,
        registration_revision: 2,
        original_scope: OriginScopeEvidence::Recorded {
            scope: scope("session-a")?,
            authority_ref: "original-scope-proof".into(),
        },
        delivery_scope: scope("session-b")?,
        source: source()?,
        reference: LifecycleTargetReference::StableMemoryRef("retained-reference-12".into()),
    };
    target.validate_for(
        &OwnedProviderId::new("tracedecay.native")?,
        &scope("session-b")?,
    )?;
    assert!(
        target
            .validate_for(&OwnedProviderId::new("ncm")?, &scope("session-b")?)
            .is_err()
    );
    assert!(
        target
            .validate_for(
                &OwnedProviderId::new("tracedecay.native")?,
                &scope("session-c")?
            )
            .is_err()
    );
    target.validate_expected_revision("git:cache_policy_r7")?;
    assert_eq!(
        target.validate_expected_revision("git:cache_policy_r8"),
        Err(AdvisoryContractError::RevisionConflict)
    );
    target.original_scope = OriginScopeEvidence::Unavailable;
    target.validate()?;
    target.source.source_revision = None;
    assert_eq!(
        target.validate_expected_revision("0"),
        Err(AdvisoryContractError::RevisionConflict)
    );
    Ok(())
}

#[test]
fn replay_accounts_source_reuse_separately_from_delivery_duplicates() -> Result<(), Box<dyn Error>>
{
    let mut accounting = ReplayAccounting {
        attempted: 5,
        applied: 1,
        delivery_duplicates: 1,
        sources_already_applied: 1,
        rejected: 1,
        effect_unknown: 1,
    };
    accounting.validate()?;
    accounting.effect_unknown = 0;
    assert!(accounting.validate().is_err());
    accounting.effect_unknown = u64::MAX;
    assert!(accounting.validate().is_err());
    Ok(())
}
