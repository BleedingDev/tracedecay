//! Real provider-control journeys over the Claude/Codex host fixture.
//!
//! Every selector below is taken from the preceding production recall result.
//! Requests cross the same authenticated daemon RPC used by the comparison
//! fixture, and every refusal is asserted from the typed application outcome.

use super::*;
use std::fs;
use std::path::PathBuf;

use tracedecay_contracts::result::ApplicationProblem;
use tracedecay_contracts::retained_surfaces::{
    ProviderControlCorrectionV1, ProviderControlDeletionModeV1,
    ProviderControlDeletionVerificationV1, ProviderControlEffectStateV1, ProviderControlErasureV1,
    ProviderControlFeedbackSignalV1, ProviderControlHealthCheckV1,
    ProviderControlMaintenanceTaskV1, ProviderControlOperationResultV1, ProviderControlRequestV1,
    ProviderControlResultV1, ProviderControlSourceSelectorV1, ProviderControlStateSelectorV1,
    ProviderControlTerminalV1, ProviderCorrectionRequestV1, ProviderDeleteBySourceRequestV1,
    ProviderFeedbackRequestV1, ProviderHealthRequestV1, ProviderMaintenanceRequestV1,
    RetainedSurfaceResultV1,
};
use tracedecay_contracts::{
    ApplicationOutcome, CancellationSignal, Deadline, RequestId, now_micros,
};
use tracedecay_daemon_protocol::{
    DaemonInvocationError, DaemonInvocationOutcome, DaemonInvocationResponse,
};
use tracedecay_domain::UtcMicros;

const CONTROL_DEADLINE: Duration = Duration::from_secs(60);
const FAULTY_NATIVE_JOURNAL: &str = JOURNAL_FILE_NAME;
const CORRECTION_UNAVAILABLE_REVISION: &str = "revision.unavailable";
const STALE_SOURCE_REVISION: &str = "stale-source-revision";

#[derive(Clone, Debug)]
struct RecalledSource {
    selector: ProviderControlSourceSelectorV1,
    source_revision: Option<String>,
}

/// Runs after the normal host journey has proved the selected provider can
/// recall all four canonical messages across one restart.
pub(super) fn assert_provider_control_journeys(
    journey: &mut ClaudeHostJourney,
    session_id: &str,
    recalled_stdout: &[u8],
    cross_scope_stdout: &[u8],
) {
    let (source, state) = source_and_state(journey, recalled_stdout, session_id);
    let (cross_scope_source, _) =
        source_and_state(journey, cross_scope_stdout, journey.session_id());

    assert_health(journey, &state, "health.before-controls");
    assert_feedback_idempotency(journey, &source.selector);
    let generation_before_maintenance = assert_health(journey, &state, "health.before-maintenance");
    assert_maintenance(journey, &state, generation_before_maintenance);

    // Maintenance is a durable provider operation. A fresh daemon and a real
    // SessionStart must leave the retained source recallable.
    let after_maintenance = restart_and_recall(journey, session_id);
    assert_answered_lane(&after_maintenance, journey.active_provider.id());
    assert_lane_contains_source(&after_maintenance, &source.selector);

    assert_correction_revision_refusal(journey, &source);
    assert_wrong_selector_is_hidden_as_missing_grant(
        journey,
        &source.selector,
        &cross_scope_source.selector,
        &state,
    );

    // Exercise the typed unavailable advisory lane through the real Native
    // mount fault seam, then restore the same journal and prove recovery.
    assert_provider_unavailable_fallback_and_recovery(journey, session_id, &source.selector);

    // Deletion is last because the final restart/recall assertion must prove
    // that this exact source cannot resurrect after a provider restart.
    assert_delete_by_source_non_resurrection(journey, session_id, &source.selector);
}

fn source_and_state(
    journey: &ClaudeHostJourney,
    recalled_stdout: &[u8],
    session_id: &str,
) -> (RecalledSource, ProviderControlStateSelectorV1) {
    let outer: Value =
        serde_json::from_slice(recalled_stdout).expect("real context stdout must be JSON");
    let answer = super::join_content_text(&outer)
        .map(|text| serde_json::from_str::<Value>(&text).expect("context content JSON"))
        .unwrap_or(outer);
    let lane = super::advisory_lane(&answer).expect("real recall must carry provider lane");
    let provider_id = lane["provider_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .expect("recall lane provider id")
        .to_owned();
    let registration_revision = lane["registration_revision"]
        .as_u64()
        .filter(|value| *value > 0)
        .expect("recall lane registration revision");
    let canonical_provider_id = if journey.codex { "codex" } else { "claude" };
    let trace_ref = lane["recall_trace"]["trace_ref"]
        .as_str()
        .filter(|value| !value.is_empty())
        .expect("recall trace selector")
        .to_owned();
    let candidate = lane["candidates"]
        .as_array()
        .and_then(|candidates| {
            candidates.iter().find(|candidate| {
                candidate["provenance_evidence"]["sources"]
                    .as_array()
                    .is_some_and(|sources| !sources.is_empty())
            })
        })
        .expect("recall must expose a canonical source candidate");
    let evidence = &candidate["provenance_evidence"];
    assert_eq!(evidence["recall"]["trace_ref"], trace_ref);
    let item_ref = evidence["recall"]["item_ref"]
        .as_str()
        .filter(|value| !value.is_empty())
        .expect("recall item selector")
        .to_owned();
    let source = evidence["sources"]
        .as_array()
        .and_then(|sources| sources.first())
        .expect("canonical source evidence");
    let observation_id = source["source"]["observation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .expect("observation selector")
        .to_owned();
    let source_revision = source["source"]["source_revision"]
        .as_str()
        .map(str::to_owned);
    let selector = ProviderControlSourceSelectorV1 {
        trace_ref,
        item_ref,
        observation_id,
    };
    let state = ProviderControlStateSelectorV1::CanonicalSession {
        provider_id,
        registration_revision,
        canonical_provider_id: canonical_provider_id.to_owned(),
        session_id: session_id.to_owned(),
    };
    (
        RecalledSource {
            selector,
            source_revision,
        },
        state,
    )
}

fn invoke(
    journey: &ClaudeHostJourney,
    request_id: &str,
    request: ProviderControlRequestV1,
) -> Result<DaemonInvocationResponse, DaemonInvocationError> {
    let authority: DaemonAuthorityRecord = serde_json::from_slice(
        &fs::read(super::daemon_authority_path(&journey.profile))
            .expect("current daemon authority"),
    )
    .expect("canonical daemon authority JSON");
    let observed_at = now_micros();
    let deadline =
        Deadline::new(UtcMicros(observed_at.0.saturating_add(
            CONTROL_DEADLINE.as_micros().try_into().unwrap_or(i64::MAX),
        )))
        .expect("provider control deadline");
    let request_id = RequestId::new(request_id.to_owned()).expect("provider control request id");
    let cancellation = CancellationSignal::active(format!("{}.cancel", request_id.as_str()))
        .expect("provider control cancellation");
    super::comparison_fixture::controlled_rpc::invoke_provider_control(
        &authority,
        &journey.project,
        &journey.profile,
        request_id,
        request,
        observed_at,
        deadline,
        cancellation,
    )
    .expect("create provider control RPC runtime")
}

fn typed_result(response: &DaemonInvocationResponse) -> ProviderControlResultV1 {
    let payload = match &response.outcome {
        DaemonInvocationOutcome::RetainedApplication { outcome, .. } => match outcome {
            ApplicationOutcome::Evidence(packet) => packet.payload.as_ref(),
            ApplicationOutcome::Effect(effect) => effect.payload.as_ref(),
            ApplicationOutcome::Preview(_) => None,
        },
        DaemonInvocationOutcome::RetainedApplicationProblem { problem, .. } => {
            panic!(
                "provider control unexpectedly returned an outer application problem: {problem:?}"
            )
        }
        other => panic!("provider control returned the wrong daemon outcome: {other:?}"),
    };
    match payload {
        Some(RetainedSurfaceResultV1::ProviderControl(result)) => result.clone(),
        other => panic!("provider control omitted its typed result payload: {other:?}"),
    }
}

fn assert_outer_problem(response: &DaemonInvocationResponse, expected: &str) {
    match &response.outcome {
        DaemonInvocationOutcome::RetainedApplicationProblem { problem, .. } => match expected {
            "missing-grant" => assert!(
                matches!(problem, ApplicationProblem::NotFoundOrNotAuthorized { .. }),
                "wrong selector must be hidden as missing grant: {problem:?}"
            ),
            "conflict" => assert!(
                matches!(
                    problem,
                    ApplicationProblem::Conflict { .. } | ApplicationProblem::Stale { .. }
                ),
                "stale correction must be a typed conflict/refusal: {problem:?}"
            ),
            other => panic!("unknown expected problem {other}"),
        },
        DaemonInvocationOutcome::RetainedApplication { .. } if expected == "conflict" => {
            let result = typed_result(response);
            assert_eq!(
                result.terminal,
                ProviderControlTerminalV1::Conflict,
                "stale correction must retain typed conflict: {result:?}"
            );
        }
        other => panic!("expected typed application refusal {expected}, got {other:?}"),
    }
}

fn assert_success(result: &ProviderControlResultV1, operation: &str) {
    assert!(
        matches!(result.terminal, ProviderControlTerminalV1::Success),
        "{operation} must settle successfully: {result:?}"
    );
    assert_ne!(
        result.effect.state,
        ProviderControlEffectStateV1::Unknown,
        "{operation} cannot claim unknown effect after successful settlement"
    );
}

fn assert_health(
    journey: &ClaudeHostJourney,
    state: &ProviderControlStateSelectorV1,
    label: &str,
) -> u64 {
    let response = invoke(
        journey,
        &format!("host-provider-control.{label}"),
        ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
            state: state.clone(),
            requested_checks: vec![
                ProviderControlHealthCheckV1::Protocol,
                ProviderControlHealthCheckV1::State,
                ProviderControlHealthCheckV1::Scope,
                ProviderControlHealthCheckV1::Capacity,
                ProviderControlHealthCheckV1::Persistence,
                ProviderControlHealthCheckV1::Recovery,
                ProviderControlHealthCheckV1::Privacy,
            ],
        }),
    )
    .expect("provider Health RPC transport");
    let result = typed_result(&response);
    assert_success(&result, label);
    let ProviderControlOperationResultV1::Health(Some(health)) = &result.result else {
        panic!("{label} must include typed health evidence: {result:?}");
    };
    assert_eq!(
        health.readiness,
        tracedecay_contracts::retained_surfaces::ProviderControlReadinessV1::Ready,
        "{label} must report a ready provider: {health:?}"
    );
    health.state_generation
}

fn assert_maintenance(
    journey: &ClaudeHostJourney,
    state: &ProviderControlStateSelectorV1,
    generation_before: u64,
) {
    // Native's staged provider recalculates feedback bias; NCM's real worker
    // consolidates the admitted STM records. Both are mutations with a
    // provider-reported kernel change, but their task names are intentionally
    // provider-specific capabilities.
    let task = if journey.active_provider.is_ncm() {
        ProviderControlMaintenanceTaskV1::Consolidate
    } else {
        ProviderControlMaintenanceTaskV1::Decay
    };
    let response = invoke(
        journey,
        "host-provider-control.maintenance",
        ProviderControlRequestV1::Maintenance(ProviderMaintenanceRequestV1 {
            state: state.clone(),
            task,
            maximum_items: 100,
            maximum_bytes: 65_536,
            maximum_duration_millis: 5_000,
            dry_run: false,
            resume_cursor: None,
        }),
    )
    .expect("provider maintenance RPC transport");
    let result = typed_result(&response);
    assert_success(&result, "maintenance");
    let ProviderControlOperationResultV1::Maintenance(Some(maintenance)) = &result.result else {
        panic!("maintenance must include typed maintenance evidence: {result:?}");
    };
    assert_eq!(maintenance.task, task);
    assert!(!maintenance.dry_run);
    assert!(
        !maintenance.partial,
        "maintenance must finish its bounded scan"
    );
    assert!(
        maintenance.scanned_items > 0,
        "maintenance must inspect the admitted provider state: {maintenance:?}"
    );
    if journey.active_provider.is_ncm() {
        assert_eq!(
            maintenance.state_changed,
            Some(true),
            "NCM maintenance must report its kernel recalculation: {maintenance:?}"
        );
    } else {
        assert!(
            maintenance.state_changed.is_none_or(|changed| changed),
            "Native maintenance cannot report a false state change: {maintenance:?}"
        );
    }
    assert!(
        maintenance.changed_items > 0
            || maintenance.removed_items > 0
            || maintenance.proposed_changes.is_some_and(|count| count > 0),
        "maintenance must report an actual recalculation proposal or changed item: {maintenance:?}"
    );
    assert!(
        maintenance.receipt.state_generation_after > maintenance.receipt.state_generation_before,
        "maintenance must advance durable provider state: {:?}",
        maintenance.receipt
    );
    assert_eq!(
        maintenance.receipt.state_generation_before, generation_before,
        "maintenance must start from the generation observed by the preceding Health RPC"
    );
    assert_eq!(
        result.effect.state,
        ProviderControlEffectStateV1::Committed,
        "maintenance must retain a committed provider effect: {result:?}"
    );
    assert_eq!(
        result.effect.state_generation_before,
        Some(maintenance.receipt.state_generation_before)
    );
    assert_eq!(
        result.effect.state_generation_after,
        Some(maintenance.receipt.state_generation_after)
    );
    assert_eq!(
        result.effect.provider_receipt_digest.as_deref(),
        Some(maintenance.receipt.provider_receipt_digest.as_str())
    );
    assert_eq!(
        assert_health(journey, state, "health.after-maintenance"),
        maintenance.receipt.state_generation_after,
        "maintenance receipt must match the provider's post-maintenance health generation"
    );
}

fn assert_feedback_idempotency(
    journey: &mut ClaudeHostJourney,
    selector: &ProviderControlSourceSelectorV1,
) {
    let request = ProviderControlRequestV1::Feedback(ProviderFeedbackRequestV1 {
        source: selector.clone(),
        signal: ProviderControlFeedbackSignalV1::Helpful,
        weight: "1".to_owned(),
        evidence_refs: Vec::new(),
        occurred_at: now_micros(),
    });
    let request_id = "host-provider-control.feedback.idempotent";
    let first_response =
        invoke(journey, request_id, request.clone()).expect("feedback RPC transport");
    let first = typed_result(&first_response);
    assert_success(&first, "feedback");
    let ProviderControlOperationResultV1::Feedback(Some(first_feedback)) = &first.result else {
        panic!("feedback must include typed provider evidence: {first:?}");
    };
    assert_eq!(&first_feedback.source, selector);
    assert_eq!(
        &first_feedback.target.source.observation_id,
        &selector.observation_id
    );
    assert_eq!(
        first.effect.state,
        ProviderControlEffectStateV1::Committed,
        "first feedback attempt must commit its provider effect: {first:?}"
    );
    assert!(
        first_feedback.receipt.state_generation_after
            > first_feedback.receipt.state_generation_before,
        "first feedback must advance provider state: {:?}",
        first_feedback.receipt
    );
    assert_eq!(
        first.effect.state_generation_before,
        Some(first_feedback.receipt.state_generation_before)
    );
    assert_eq!(
        first.effect.state_generation_after,
        Some(first_feedback.receipt.state_generation_after)
    );
    assert_eq!(
        first.effect.provider_receipt_digest.as_deref(),
        Some(first_feedback.receipt.provider_receipt_digest.as_str())
    );
    assert!(
        first.effect.verification_digest.is_some(),
        "first feedback must retain verification evidence: {first:?}"
    );

    // A new daemon process must recover the same provider operation before the
    // exact same request is replayed. This is the persistence half of the
    // exactly-once assertion; an in-memory duplicate would not be enough.
    journey.stop_daemon();
    journey.start_daemon();
    journey.await_startup_history();

    let duplicate = invoke(journey, request_id, request).expect("duplicate feedback RPC transport");
    let duplicate = typed_result(&duplicate);
    assert_success(&duplicate, "duplicate feedback");
    let ProviderControlOperationResultV1::Feedback(Some(duplicate_feedback)) = &duplicate.result
    else {
        panic!("duplicate feedback must retain typed provider evidence: {duplicate:?}");
    };
    assert_eq!(
        duplicate.effect.state,
        ProviderControlEffectStateV1::Duplicate,
        "same feedback identity must be reported as a duplicate: {duplicate:?}"
    );
    assert_eq!(
        duplicate.effect.duplicate_of_idempotency_key, first.idempotency_key,
        "duplicate feedback must identify the original idempotency key"
    );
    assert_eq!(
        duplicate_feedback.receipt, first_feedback.receipt,
        "duplicate feedback must retain the original committing receipt"
    );
    assert_eq!(
        duplicate.effect.state_generation_before, duplicate.effect.state_generation_after,
        "duplicate feedback cannot advance provider state"
    );
    assert_eq!(
        duplicate.effect.state_generation_before, first.effect.state_generation_after,
        "duplicate feedback must observe the persisted post-commit generation"
    );
    assert_eq!(
        duplicate.effect.provider_receipt_digest, first.effect.provider_receipt_digest,
        "duplicate feedback must retain the original provider receipt digest"
    );
    assert!(
        duplicate.effect.committed_item_refs.is_empty()
            && duplicate.effect.uncommitted_item_refs.is_empty(),
        "duplicate feedback must report no second item commit: {duplicate:?}"
    );
}

fn assert_correction_revision_refusal(journey: &ClaudeHostJourney, source: &RecalledSource) {
    let expected = source
        .source_revision
        .clone()
        .unwrap_or_else(|| CORRECTION_UNAVAILABLE_REVISION.to_owned());
    let current_request = ProviderControlRequestV1::Correction(ProviderCorrectionRequestV1 {
        source: source.selector.clone(),
        expected_source_revision: expected.clone(),
        correction: ProviderControlCorrectionV1::ChangeValidity {
            valid_from: now_micros(),
            valid_until: None,
        },
        reason: "real host correction revision check".to_owned(),
        evidence_refs: Vec::new(),
    });
    let response = invoke(
        journey,
        "host-provider-control.correction.current",
        current_request,
    )
    .expect("current correction RPC transport");
    if source.source_revision.is_none() {
        // Claude and Codex canonical file-byte evidence currently has no
        // source revision. Keep this assertion truthful: the host must refuse
        // current correction rather than inventing one. The stale attempt
        // below still runs so both revision refusal identities are exercised.
        assert_outer_problem(&response, "conflict");
    } else {
        let current = typed_result(&response);
        assert_success(&current, "current correction");
        let ProviderControlOperationResultV1::Correction(Some(correction)) = &current.result else {
            panic!("current correction must include typed provider evidence: {current:?}");
        };
        assert_eq!(&correction.source, &source.selector);
        assert_eq!(
            correction.target.source.source_revision.as_deref(),
            source.source_revision.as_deref()
        );
        assert_eq!(
            correction.receipt.state_generation_before,
            current
                .effect
                .state_generation_before
                .expect("correction generation")
        );
        assert!(
            correction.receipt.state_generation_after > correction.receipt.state_generation_before,
            "current correction must advance provider state: {:?}",
            correction.receipt
        );
        assert_eq!(
            current.effect.state,
            ProviderControlEffectStateV1::Committed,
            "current correction must commit a provider effect: {current:?}"
        );
    }

    let stale = ProviderControlRequestV1::Correction(ProviderCorrectionRequestV1 {
        source: source.selector.clone(),
        expected_source_revision: STALE_SOURCE_REVISION.to_owned(),
        correction: ProviderControlCorrectionV1::ChangeValidity {
            valid_from: now_micros(),
            valid_until: None,
        },
        reason: "stale host correction must refuse".to_owned(),
        evidence_refs: Vec::new(),
    });
    let response = invoke(journey, "host-provider-control.correction.stale", stale)
        .expect("stale correction RPC transport");
    assert_outer_problem(&response, "conflict");
}

fn assert_wrong_selector_is_hidden_as_missing_grant(
    journey: &ClaudeHostJourney,
    selector: &ProviderControlSourceSelectorV1,
    cross_scope_selector: &ProviderControlSourceSelectorV1,
    state: &ProviderControlStateSelectorV1,
) {
    assert_ne!(
        selector.trace_ref, cross_scope_selector.trace_ref,
        "cross-scope selector must come from the distinct origin recall trace"
    );
    // This selector is a real source from the originating session's recall,
    // with a valid item and observation identity. The active route is the
    // destination session, so source authorization must deny the cross-scope
    // grant rather than taking a malformed or nonexistent-item path.
    let response = invoke(
        journey,
        "host-provider-control.feedback.wrong-selector",
        ProviderControlRequestV1::Feedback(ProviderFeedbackRequestV1 {
            source: cross_scope_selector.clone(),
            signal: ProviderControlFeedbackSignalV1::Ignored,
            weight: "0".to_owned(),
            evidence_refs: Vec::new(),
            occurred_at: now_micros(),
        }),
    )
    .expect("wrong selector RPC transport");
    assert_outer_problem(&response, "missing-grant");

    // A registered canonical session under the other host identity is a
    // well-formed cross-scope selector, but it has no grant in this journey's
    // canonical session table. The host must collapse that distinction into
    // its typed not-found/not-authorized result.
    let (other_provider, other_session) = if journey.codex {
        ("claude", CLAUDE_SESSION)
    } else {
        ("codex", CODEX_SESSION)
    };
    let ProviderControlStateSelectorV1::CanonicalSession {
        provider_id,
        registration_revision,
        ..
    } = state
    else {
        panic!("host journey uses canonical-session state");
    };
    let response = invoke(
        journey,
        "host-provider-control.health.cross-scope",
        ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
            state: ProviderControlStateSelectorV1::CanonicalSession {
                provider_id: provider_id.clone(),
                registration_revision: *registration_revision,
                canonical_provider_id: other_provider.to_owned(),
                session_id: other_session.to_owned(),
            },
            requested_checks: vec![ProviderControlHealthCheckV1::Protocol],
        }),
    )
    .expect("cross-scope selector RPC transport");
    assert_outer_problem(&response, "missing-grant");

    // The canonical row is real, but the provider registration revision is
    // stale. The host keeps the failure typed and effect-free instead of
    // dispatching against the current provider owner.
    let mut stale_state = state.clone();
    let ProviderControlStateSelectorV1::CanonicalSession {
        registration_revision,
        ..
    } = &mut stale_state
    else {
        panic!("host journey uses canonical-session state");
    };
    let stale_revision = (*registration_revision)
        .checked_add(1)
        .expect("stale registration revision");
    *registration_revision = stale_revision;
    let response = invoke(
        journey,
        "host-provider-control.health.stale-registration",
        ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
            state: stale_state,
            requested_checks: vec![ProviderControlHealthCheckV1::Protocol],
        }),
    )
    .expect("stale registration RPC transport");
    let stale = typed_result(&response);
    assert_eq!(
        stale.terminal,
        ProviderControlTerminalV1::ProviderUnavailable,
        "stale registration must refuse before provider dispatch: {stale:?}"
    );
    assert_eq!(
        stale.effect.state,
        ProviderControlEffectStateV1::None,
        "stale registration must have no provider effect: {stale:?}"
    );
    assert!(matches!(
        stale.result,
        ProviderControlOperationResultV1::Health(None)
    ));
}

fn restart_and_recall(journey: &mut ClaudeHostJourney, session_id: &str) -> Value {
    journey.stop_daemon();
    journey.start_daemon();
    let transcript = journey.transcript_path_for_session(session_id);
    journey.initialize_session_transcript(session_id);
    let started = journey.run_session_start_event(session_id, &transcript);
    assert!(
        started.status.success(),
        "restart SessionStart must succeed: {}",
        String::from_utf8_lossy(&started.stderr)
    );
    journey.await_startup_history();
    journey.tool(
        "tracedecay_context",
        &json!({
            "task": format!("what does the {JOURNEY_TERM} transport probe record?"),
            "format": "json",
            "_meta": { "session_id": session_id },
        }),
    )
}

fn assert_answered_lane(answer: &Value, provider_id: &str) -> Value {
    let lane = super::advisory_lane(answer).expect("context must carry advisory lane");
    assert_eq!(lane["state"], "answered", "healthy lane: {lane}");
    assert_eq!(lane["provider_id"], provider_id, "healthy lane: {lane}");
    lane
}

fn assert_lane_contains_source(answer: &Value, selector: &ProviderControlSourceSelectorV1) {
    let lane = super::advisory_lane(answer).expect("context advisory lane");
    let found = lane["candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|candidate| {
            candidate["provenance_evidence"]["recall"]["trace_ref"] == selector.trace_ref
                && candidate["provenance_evidence"]["recall"]["item_ref"] == selector.item_ref
                && candidate["provenance_evidence"]["sources"]
                    .as_array()
                    .is_some_and(|sources| {
                        sources.iter().any(|source| {
                            source["source"]["observation_id"] == selector.observation_id
                        })
                    })
        });
    assert!(
        found,
        "maintenance restart recall lost the selected source: {lane}"
    );
}

fn assert_provider_unavailable_fallback_and_recovery(
    journey: &mut ClaudeHostJourney,
    session_id: &str,
    selector: &ProviderControlSourceSelectorV1,
) -> Value {
    if journey.active_provider.is_ncm() {
        return assert_ncm_unavailable_fallback_and_recovery(journey, session_id, selector);
    }
    // The provider journal is the required Native observation mount. Renaming
    // its main file while the daemon is stopped makes only the next full
    // provider mount fail; the published core route still owns the cognitive
    // recall lane, which must report MountRefused as typed advisory fallback.
    let journal = super::find_file(&journey.profile, FAULTY_NATIVE_JOURNAL)
        .expect("Native provider journal path");
    let backup = journal.with_extension("sqlite3.host-journey-backup");
    assert!(!backup.exists(), "fault backup must be fresh");
    journey.stop_daemon();
    fs::rename(&journal, &backup).expect("move Native journal behind fault");
    fs::write(
        &journal,
        b"host journey deliberately invalidates this mount\n",
    )
    .expect("write invalid Native journal");
    journey.start_daemon();

    let unavailable = journey.tool(
        "tracedecay_context",
        &json!({
            "task": format!("what does the {JOURNEY_TERM} transport probe record?"),
            "format": "json",
            "_meta": { "session_id": session_id },
        }),
    );
    let lane = super::advisory_lane(&unavailable)
        .expect("provider mount failure must retain advisory lane");
    assert_eq!(
        lane["state"], "unavailable",
        "typed unavailable lane: {lane}"
    );
    assert_eq!(lane["provider_id"], journey.active_provider.id());
    assert_eq!(lane["registration_revision"], 1);

    journey.stop_daemon();
    fs::remove_file(&journal).expect("remove invalid Native journal");
    fs::rename(&backup, &journal).expect("restore Native journal after fault");
    let recovered = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&recovered, journey.active_provider.id());
    assert_eq!(lane["state"], "answered");
    assert_lane_contains_source(&recovered, selector);
    recovered
}

fn assert_ncm_unavailable_fallback_and_recovery(
    journey: &mut ClaudeHostJourney,
    session_id: &str,
    selector: &ProviderControlSourceSelectorV1,
) -> Value {
    let worker = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_WORKER").expect("real NCM worker binary is required"),
    )
    .canonicalize()
    .expect("canonical NCM worker binary");
    let daemon_pid = journey.daemon.as_ref().expect("running NCM daemon").id();
    let listing = Command::new("ps")
        .args(["-axo", "pid=,ppid=,command="])
        .output()
        .expect("ps must enumerate the real NCM worker");
    let worker_pid = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line
                .splitn(3, char::is_whitespace)
                .filter(|field| !field.is_empty());
            let pid = fields.next()?.parse::<u32>().ok()?;
            let ppid = fields.next()?.parse::<u32>().ok()?;
            let command = fields.next()?.trim();
            (ppid == daemon_pid && command.starts_with(worker.to_string_lossy().as_ref()))
                .then_some(pid)
        })
        .find(|pid| *pid != std::process::id())
        .expect("the daemon must own the real NCM worker child");
    let status = Command::new("kill")
        .args(["-TERM", &worker_pid.to_string()])
        .status()
        .expect("kill must signal only the discovered NCM worker");
    assert!(status.success(), "NCM worker termination must be delivered");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let probe = Command::new("kill")
            .args(["-0", &worker_pid.to_string()])
            .status()
            .expect("kill -0 must probe worker state");
        if !probe.success() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let unavailable = journey.tool(
        "tracedecay_context",
        &json!({
            "task": format!("what does the {JOURNEY_TERM} transport probe record?"),
            "format": "json",
            "_meta": { "session_id": session_id },
        }),
    );
    let lane =
        super::advisory_lane(&unavailable).expect("NCM worker loss must retain advisory lane");
    assert_eq!(
        lane["state"], "unavailable",
        "typed NCM unavailable lane: {lane}"
    );
    assert_eq!(lane["provider_id"], journey.active_provider.id());
    assert_eq!(lane["registration_revision"], 1);

    let recovered = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&recovered, journey.active_provider.id());
    assert_eq!(lane["state"], "answered");
    assert_lane_contains_source(&recovered, selector);
    recovered
}

fn assert_delete_by_source_non_resurrection(
    journey: &mut ClaudeHostJourney,
    session_id: &str,
    selector: &ProviderControlSourceSelectorV1,
) {
    let response = invoke(
        journey,
        "host-provider-control.delete-by-source",
        ProviderControlRequestV1::DeleteBySource(ProviderDeleteBySourceRequestV1 {
            source: selector.clone(),
            mode: ProviderControlDeletionModeV1::RemoveInfluence,
            expected_fence_revision: 0,
            include_snapshots: true,
        }),
    )
    .expect("delete-by-source RPC transport");
    let result = typed_result(&response);
    assert_success(&result, "delete-by-source");
    assert_eq!(
        result.terminal,
        ProviderControlTerminalV1::Success,
        "deleting a recalled source must report a committed success, not an empty result"
    );
    let ProviderControlOperationResultV1::DeleteBySource(Some(deletion)) = &result.result else {
        panic!("delete-by-source must include typed deletion evidence: {result:?}");
    };
    assert_eq!(&deletion.source, selector);
    assert_eq!(
        deletion.mode,
        ProviderControlDeletionModeV1::RemoveInfluence
    );
    assert!(deletion.include_snapshots);
    assert_eq!(deletion.intent.fence_revision_before, 0);
    assert_eq!(deletion.intent.fence_revision_after, 1);
    assert_eq!(
        deletion.host_snapshot_cleanup.state,
        tracedecay_contracts::retained_surfaces::ProviderControlHostSnapshotCleanupStateV1::Complete,
        "snapshot cleanup must finish before a successful source deletion is reported"
    );
    let ProviderControlErasureV1::Verified {
        postcondition,
        receipt,
    } = &deletion.erasure
    else {
        panic!(
            "deleting a recalled source must verify provider erasure: {:?}",
            deletion.erasure
        );
    };
    assert!(postcondition.matched_effects > 0);
    assert!(postcondition.removed_effects > 0);
    assert_eq!(postcondition.remaining_influence_count, 0);
    assert_eq!(
        postcondition.verification_state,
        ProviderControlDeletionVerificationV1::VerifiedAbsent
    );
    assert!(
        receipt.state_generation_after > receipt.state_generation_before,
        "delete-by-source must advance provider state: {receipt:?}"
    );
    assert_eq!(
        result.effect.state,
        ProviderControlEffectStateV1::Committed,
        "delete-by-source must retain its provider commit: {result:?}"
    );
    assert_eq!(
        result.effect.state_generation_before,
        Some(receipt.state_generation_before)
    );
    assert_eq!(
        result.effect.state_generation_after,
        Some(receipt.state_generation_after)
    );

    let after_delete = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&after_delete, journey.active_provider.id());
    let candidates = lane["candidates"]
        .as_array()
        .expect("post-delete candidates");
    assert!(
        !candidates.is_empty(),
        "post-delete recall must retain unrelated source candidates"
    );
    assert!(
        candidates.iter().all(|candidate| {
            candidate["provenance_evidence"]["sources"]
                .as_array()
                .is_some_and(|sources| {
                    sources
                        .iter()
                        .all(|source| source["source"]["observation_id"] != selector.observation_id)
                })
        }),
        "deleted source must leave no candidate after restart: {lane}"
    );

    let after_second_restart = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&after_second_restart, journey.active_provider.id());
    let candidates = lane["candidates"]
        .as_array()
        .expect("second post-delete candidates");
    assert!(
        !candidates.is_empty(),
        "second post-delete recall must retain unrelated source candidates"
    );
    assert!(
        candidates.iter().all(|candidate| {
            candidate["provenance_evidence"]["sources"]
                .as_array()
                .is_some_and(|sources| {
                    sources
                        .iter()
                        .all(|source| source["source"]["observation_id"] != selector.observation_id)
                })
        }),
        "deleted source must remain absent after the second restart: {lane}"
    );

    // The selector remains well-formed after deletion, but the fresh source
    // grant must deny it instead of letting a stale caller mutate influence.
    let response = invoke(
        journey,
        "host-provider-control.feedback.after-delete",
        ProviderControlRequestV1::Feedback(ProviderFeedbackRequestV1 {
            source: selector.clone(),
            signal: ProviderControlFeedbackSignalV1::Helpful,
            weight: "1".to_owned(),
            evidence_refs: Vec::new(),
            occurred_at: now_micros(),
        }),
    )
    .expect("post-delete stale selector RPC transport");
    assert_outer_problem(&response, "missing-grant");
}
