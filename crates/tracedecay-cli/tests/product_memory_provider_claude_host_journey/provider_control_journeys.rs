//! Real provider-control journeys over the Claude/Codex host fixture.
//!
//! Every selector below is taken from the preceding production recall result.
//! Requests cross the same authenticated daemon RPC used by the comparison
//! fixture, and every refusal is asserted from the typed application outcome.

use super::*;
use std::fs;

use tracedecay_contracts::result::ApplicationProblem;
use tracedecay_contracts::retained_surfaces::{
    ProviderControlCorrectionV1, ProviderControlDeletionModeV1, ProviderControlEffectStateV1,
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
) {
    let (source, state) = source_and_state(journey, recalled_stdout, session_id);

    assert_health(journey, &state, "health.before-maintenance");
    assert_maintenance(journey, &state);

    // Maintenance is a durable provider operation. A fresh daemon and a real
    // SessionStart must leave the retained source recallable.
    let after_maintenance = restart_and_recall(journey, session_id);
    assert_answered_lane(&after_maintenance, journey.active_provider.id());
    assert_lane_contains_source(&after_maintenance, &source.selector);

    assert_feedback_idempotency(journey, &source.selector);
    assert_correction_revision_refusal(journey, &source);
    assert_wrong_selector_is_hidden_as_missing_grant(journey, &source.selector);

    // Exercise the typed unavailable advisory lane through the real Native
    // mount fault seam, then restore the same journal and prove recovery.
    assert_provider_unavailable_fallback_and_recovery(journey, session_id);

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
        matches!(
            result.terminal,
            ProviderControlTerminalV1::Success | ProviderControlTerminalV1::SuccessZeroResults
        ),
        "{operation} must settle successfully: {result:?}"
    );
    assert_ne!(
        result.effect.state,
        ProviderControlEffectStateV1::Unknown,
        "{operation} cannot claim unknown effect after successful settlement"
    );
}

fn assert_health(journey: &ClaudeHostJourney, state: &ProviderControlStateSelectorV1, label: &str) {
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
    let ProviderControlOperationResultV1::Health(Some(health)) = result.result else {
        panic!("{label} must include typed health evidence: {result:?}");
    };
    assert_eq!(
        health.readiness,
        tracedecay_contracts::retained_surfaces::ProviderControlReadinessV1::Ready,
        "{label} must report a ready provider: {health:?}"
    );
}

fn assert_maintenance(journey: &ClaudeHostJourney, state: &ProviderControlStateSelectorV1) {
    let response = invoke(
        journey,
        "host-provider-control.maintenance",
        ProviderControlRequestV1::Maintenance(ProviderMaintenanceRequestV1 {
            state: state.clone(),
            task: ProviderControlMaintenanceTaskV1::Consolidate,
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
    let ProviderControlOperationResultV1::Maintenance(Some(maintenance)) = result.result else {
        panic!("maintenance must include typed maintenance evidence: {result:?}");
    };
    assert_eq!(
        maintenance.task,
        ProviderControlMaintenanceTaskV1::Consolidate
    );
    assert!(!maintenance.dry_run);
}

fn assert_feedback_idempotency(
    journey: &ClaudeHostJourney,
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
    let first = invoke(journey, request_id, request.clone())
        .expect("feedback RPC transport")
        .clone();
    let first = typed_result(&first);
    assert_success(&first, "feedback");
    assert!(matches!(
        first.result,
        ProviderControlOperationResultV1::Feedback(Some(_))
    ));
    assert_eq!(
        first.effect.state,
        ProviderControlEffectStateV1::Committed,
        "first feedback attempt must commit its provider effect: {first:?}"
    );
    let duplicate = invoke(journey, request_id, request)
        .expect("duplicate feedback RPC transport")
        .clone();
    let duplicate = typed_result(&duplicate);
    assert_success(&duplicate, "duplicate feedback");
    assert!(matches!(
        duplicate.result,
        ProviderControlOperationResultV1::Feedback(Some(_))
    ));
    assert_eq!(
        duplicate.effect.state,
        ProviderControlEffectStateV1::Duplicate,
        "same feedback identity must be reported as a duplicate: {duplicate:?}"
    );
    assert_eq!(
        duplicate.effect.duplicate_of_idempotency_key, first.idempotency_key,
        "duplicate feedback must identify the original idempotency key"
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
        // current correction rather than inventing one. Once the producer
        // supplies a revision, the same fixture automatically exercises the
        // success-then-stale branch below.
        assert_outer_problem(&response, "conflict");
        return;
    }
    let current = typed_result(&response);
    assert_success(&current, "current correction");
    assert!(matches!(
        current.result,
        ProviderControlOperationResultV1::Correction(Some(_))
    ));

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
) {
    let mut wrong = selector.clone();
    wrong.item_ref = format!("{}-missing", wrong.item_ref);
    let response = invoke(
        journey,
        "host-provider-control.feedback.wrong-selector",
        ProviderControlRequestV1::Feedback(ProviderFeedbackRequestV1 {
            source: wrong,
            signal: ProviderControlFeedbackSignalV1::Ignored,
            weight: "0".to_owned(),
            evidence_refs: Vec::new(),
            occurred_at: now_micros(),
        }),
    )
    .expect("wrong selector RPC transport");
    assert_outer_problem(&response, "missing-grant");
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
) {
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
    assert_eq!(lane["provider_id"], CONFIGURED_PROVIDER_ID);
    assert_eq!(lane["registration_revision"], 1);

    journey.stop_daemon();
    fs::remove_file(&journal).expect("remove invalid Native journal");
    fs::rename(&backup, &journal).expect("restore Native journal after fault");
    let recovered = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&recovered, CONFIGURED_PROVIDER_ID);
    assert_eq!(lane["state"], "answered");
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
    let ProviderControlOperationResultV1::DeleteBySource(Some(deletion)) = result.result else {
        panic!("delete-by-source must include typed deletion evidence: {result:?}");
    };
    assert_eq!(deletion.source, *selector);

    let after_delete = restart_and_recall(journey, session_id);
    let lane = assert_answered_lane(&after_delete, CONFIGURED_PROVIDER_ID);
    let candidates = lane["candidates"]
        .as_array()
        .expect("post-delete candidates");
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
    let lane = assert_answered_lane(&after_second_restart, CONFIGURED_PROVIDER_ID);
    let candidates = lane["candidates"]
        .as_array()
        .expect("second post-delete candidates");
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
}
