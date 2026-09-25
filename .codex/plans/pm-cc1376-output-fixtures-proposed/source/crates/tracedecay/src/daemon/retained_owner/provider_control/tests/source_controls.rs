//! Offline source deletion through real canonical admission and retained authority.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tracedecay_contracts::{
    ApplicationOutcome, CancellationContext, CancellationSignal, CapabilityGrantId,
    CapabilityGrantSnapshot, Deadline, DisclosureClass, EffectTermination, ReconciliationState,
    RequestContext, RequestId, ResolvedScope, RetainedProviderControlExecutionPortV1,
    RetainedSurfaceExecutionContextV1,
};
use tracedecay_domain::{
    ActorId, ManifestDigest, ObservationScopeV1, ObservationSourceIdentityV1, ProjectId,
    ProviderId, SessionId,
};
use tracedecay_hooks::admission_ledger::{
    HookAdmissionLedgerLimitsV1, HookAdmissionLedgerV1, HookLiveOriginOutcomeV1,
};
use tracedecay_hooks::{
    HOOK_EVENT_SCHEMA_VERSION, HookBoundaryV1, HookEventEnvelopeV2, HookEventV2, HookHostV1,
    HookOrderingV1,
};
use tracedecay_host_admission::{HostAdmissionAuthorities, HostAdmissionFacade};
use tracedecay_memory_observation::{RecoveryTimeBudgetV1, SqliteObservationJournal};
use tracedecay_memory_provider_registry::recall_admission::source_attribution::RecallSourceAttributionV1;
use tracedecay_memory_provider_registry::{
    AdvisoryAdmissionAuthority, AdvisoryAdmissionError, CancellationToken,
    CurrentAdvisoryAdmission, OperationControl, OwnedProviderId, OwnedVersionedId,
    ProjectMemoryProviderComposition, ProviderCall, ProviderCallParts, ProviderOperation,
    RecallExplainHostDecisionV1, RecallExplainItemV1, RecallExplainProviderExplanationV1,
    RecallExplainTraceV1,
};
use tracedecay_sessions::admission::HostAdmissionScope;
use tracedecay_sessions::observation::ObservationCancellation;
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_sessions::runtime::claude::{ClaudeSource, identify_claude_source};
use tracedecay_sessions::runtime::claude_observation::ingest_source_with_observations_with_admission;

use crate::daemon::retained_owner::cognitive_recall::{
    RecallAdmissionLedgerV1,
    control_attribution::{PreparedRecallControlMetadataV1, RetainedRecallControlBindingV1},
};
use crate::daemon::retained_owner::observation_journey::ObservationJourneyPolicyV1;
use crate::daemon::retained_owner::provider_control::ProviderControlMountInputsV1;
use crate::daemon::retained_owner::provider_control::authority::{
    ProviderControlAuthorityInputsV1, ProviderControlAuthorityV1,
};
use crate::daemon::retained_owner::provider_history::{
    HistoryIdentityBridgeV1, HookOriginReaderV1, MountedOriginalObservationAuthorityV1,
    ProviderHistoryAuthorityV1, ProviderHistoryReaderV1, history_grant_json,
    source_attribution_json,
};
use crate::host_admission::HostAdmissionTestRuntimeV1;
use crate::mcp::tools::handlers::hook_runtime::capture_live_origin_for_control_test;

const SESSION: &str = "offline-source-control-session";
const ORIGINAL_CONTENT: &str =
    "Retain canonical source material while deleting only provider influence.";

struct OfflineSourceFixture {
    _temporary: TempDir,
    _runtime: HostAdmissionTestRuntimeV1,
    port: ProjectProviderControlPortV1,
    ledger: Arc<RecallAdmissionLedgerV1>,
    journal: Arc<SqliteObservationJournal>,
    history_authority: Arc<dyn AdvisoryAdmissionAuthority>,
    selector: ProviderControlSourceSelectorV1,
    attribution: SourceAttribution,
    scope: ResolvedScope,
}

fn operation_control() -> OperationControl {
    OperationControl::new(
        tracedecay_contracts::now_micros().0 + 30_000_000,
        30_000,
        CancellationToken::new(),
    )
}

fn git(root: &Path, args: &[&str]) {
    let output =
        std::process::Command::new(tracedecay_runtime_core::git::try_git_program().unwrap())
            .args([
                "-c",
                "user.name=Source Control Fixture",
                "-c",
                "user.email=source-control@example.invalid",
            ])
            .args(args)
            .current_dir(root)
            .output()
            .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn transcript_line(root: &Path, message: &str, content: &str) -> String {
    let now = tracedecay_contracts::now_micros();
    format!(
        "{}\n",
        json!({
            "type":"user", "sessionId":SESSION, "uuid":message,
            "timestamp":rfc3339_utc_micros(now.0).unwrap(), "cwd":root,
            "message":{"role":"user","content":content},
        })
    )
}

fn hook_envelope(event: u8, now: UtcMicros) -> HookEventEnvelopeV2 {
    HookEventEnvelopeV2 {
        schema_version: HOOK_EVENT_SCHEMA_VERSION,
        event_id: [event; 16],
        producer: HookHostV1::ClaudeCode,
        protected_session_id: tracedecay_agent_hosts::hooks::protected_native_session_id(SESSION),
        project_id: [1; 16],
        repository_id: [2; 16],
        worktree_id: [3; 16],
        worktree_epoch: 1,
        binding_token: [4; 32],
        ordering: HookOrderingV1::Unknown,
        observed_at: now,
        event: HookEventV2::SessionBoundary {
            boundary: HookBoundaryV1::TurnComplete,
        },
    }
}

impl OfflineSourceFixture {
    async fn new() -> Self {
        Self::for_delivery_session(SESSION).await
    }

    async fn for_delivery_session(delivery_session: &str) -> Self {
        let temporary = TempDir::new().unwrap();
        let project_root = temporary.path().join("repository");
        std::fs::create_dir_all(&project_root).unwrap();
        git(&project_root, &["init", "-q", "-b", "main"]);
        git(
            &project_root,
            &["commit", "--allow-empty", "-q", "-m", "initial"],
        );
        let project_root = std::fs::canonicalize(project_root).unwrap();
        let project = ProjectId::new("project.offline-source-control").unwrap();
        let runtime = HostAdmissionTestRuntimeV1::project(
            temporary.path().join("profile"),
            &project_root,
            project.clone(),
        )
        .await
        .unwrap();
        tracedecay_runtime_core::storage::write_repository_identity_marker(
            &project_root,
            project.as_str(),
        )
        .unwrap();
        let registered = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .unwrap();
        let brain = registered.binding().shard_id.brain_id.clone();
        let profile = registered.binding().shard_id.profile_id.clone();
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&project_root, &project)
                .unwrap();
        let data_root = temporary.path().join("provider-data");
        std::fs::create_dir_all(&data_root).unwrap();
        let hook_root = data_root
            .join("hook-v2-admissions")
            .join(HookHostV1::ClaudeCode.hook_key());
        let mut hook_ledger = HookAdmissionLedgerV1::open(
            &hook_root,
            HookHostV1::ClaudeCode,
            HookAdmissionLedgerLimitsV1::stock(),
            tracedecay_contracts::now_micros(),
        )
        .unwrap()
        .0;
        let origin_reader = Arc::new(HookOriginReaderV1::new(
            data_root.clone(),
            brain.clone(),
            profile.clone(),
        ));
        let marker =
            tracedecay_runtime_core::storage::read_repository_identity_marker(&project_root)
                .unwrap()
                .unwrap();
        let provenance = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
            &project_root,
            &project,
            &marker,
        )
        .unwrap()
        .with_original_provenance_resolver(origin_reader.clone());
        let facade = HostAdmissionFacade::new(
            HostAdmissionAuthorities::for_project(
                brain.clone(),
                profile.clone(),
                project.clone(),
                registered.as_ref(),
            )
            .with_repository_provenance(provenance)
            .with_background_cpu(runtime.background_cpu()),
        );
        let home = temporary.path().join("host");
        let transcript = home
            .join(".claude/projects/control-fixture")
            .join(format!("{SESSION}.jsonl"));
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        std::fs::write(
            &transcript,
            transcript_line(
                &project_root,
                "historical-message",
                "Historical material before the live hook boundary.",
            ),
        )
        .unwrap();
        let claude = ClaudeSource::with_home(&home);
        let initial = ingest_source_with_observations_with_admission(
            &claude,
            &project_root,
            ObservationScopeV1::Project {
                project_id: project.clone(),
            },
            &facade,
            Some(1_048_576),
            ObservationCancellation::default(),
        )
        .await
        .unwrap();
        assert_eq!(initial.observations_committed, 1);
        let session = registered
            .get_session_result("claude", SESSION)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.session_id, SESSION);
        assert_eq!(session.provider, "claude");
        let identity = identify_claude_source(&transcript).unwrap();
        assert_eq!(identity.provider, session.provider);
        assert_eq!(identity.session_id, session.session_id);
        assert_eq!(
            std::fs::canonicalize(&identity.source_path).unwrap(),
            std::fs::canonicalize(&transcript).unwrap()
        );
        let source = ObservationSourceIdentityV1::for_provider_source(
            ProviderId::new("claude").unwrap(),
            SessionId::new(identity.session_id).unwrap(),
            SessionId::new(identity.source_id).unwrap(),
        )
        .unwrap();
        let baseline_now = tracedecay_contracts::now_micros();
        let baseline = capture_live_origin_for_control_test(
            project_root.clone(),
            project.clone(),
            brain.clone(),
            profile.clone(),
            transcript.clone(),
            source.clone(),
            None,
            baseline_now,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        let envelope = hook_envelope(10, baseline_now);
        let receipt = hook_ledger
            .admit_with_receipt(&envelope, baseline_now)
            .unwrap();
        assert_eq!(
            hook_ledger
                .record_live_origin(&envelope, receipt, Some(baseline), baseline_now)
                .unwrap(),
            HookLiveOriginOutcomeV1::Baseline
        );
        let previous = hook_ledger
            .live_origin_baseline(envelope.protected_session_id, baseline_now)
            .unwrap();
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            transcript_line(&project_root, "original-message", ORIGINAL_CONTENT).as_bytes(),
        )
        .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let appended_now = tracedecay_contracts::now_micros();
        let appended = capture_live_origin_for_control_test(
            project_root.clone(),
            project.clone(),
            brain,
            profile.clone(),
            transcript.clone(),
            source,
            Some(&previous),
            appended_now,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(appended.frames.len(), 1);
        let envelope = hook_envelope(11, appended_now);
        let receipt = hook_ledger
            .admit_with_receipt(&envelope, appended_now)
            .unwrap();
        assert_eq!(
            hook_ledger
                .record_live_origin(&envelope, receipt, Some(appended), appended_now)
                .unwrap(),
            HookLiveOriginOutcomeV1::Sealed
        );
        drop(hook_ledger);
        let appended = ingest_source_with_observations_with_admission(
            &claude,
            &project_root,
            ObservationScopeV1::Project {
                project_id: project.clone(),
            },
            &facade,
            Some(1_048_576),
            ObservationCancellation::default(),
        )
        .await
        .unwrap();
        assert_eq!(appended.observations_committed, 1);
        assert_eq!(appended.projections_completed, 1);
        drop(facade);

        let dispositions = runtime
            .session_registry_for_test()
            .project_memory(project.clone(), [project_root.clone()])
            .await
            .unwrap();
        let journal = Arc::new(
            SqliteObservationJournal::open(
                data_root.join("memory-observation-journal-v1.sqlite3"),
                ObservationJourneyPolicyV1::project_default().retention,
            )
            .unwrap(),
        );
        let provider = OwnedProviderId::new("tracedecay.native").unwrap();
        let bridge = Arc::new(
            HistoryIdentityBridgeV1::admit(
                &project_root,
                &profile,
                &scope,
                &registered.binding().shard_id,
            )
            .unwrap(),
        );
        let original = Arc::new(MountedOriginalObservationAuthorityV1 {
            reader: origin_reader.clone(),
            bridge: bridge.clone(),
        });
        let observations = registered.observation_store();
        let reader = ProviderHistoryReaderV1 {
            bridge: bridge.as_ref(),
            observations: &observations,
            dispositions: dispositions.as_ref(),
            original_authority: original.as_ref(),
            journal: journal.as_ref(),
            provider_id: &provider,
            policy_revision: 1,
        };
        let delivery = bridge.destination(delivery_session).unwrap();
        let page = reader
            .select_recent_page(&delivery, 256, &operation_control())
            .await
            .unwrap();
        let grant = page.grant.unwrap();
        assert_eq!(
            grant.sources.len(),
            1,
            "only the actual post-boundary frame is original history"
        );
        let attribution = grant.sources[0].attribution.clone();
        assert_eq!(attribution.source.canonical_session_id, SESSION);
        assert_eq!(
            attribution.source.stable_record_id.as_deref(),
            Some("original-message")
        );
        let wire: RecallSourceAttributionV1 =
            serde_json::from_value(source_attribution_json(&attribution).unwrap()).unwrap();
        let trace = RecallExplainTraceV1 {
            trace_id: "a".repeat(64),
            request_id: "request.retained-source".to_owned(),
            provider_id: provider.as_str().to_owned(),
            registration_revision: 1,
            requested_count: 1,
            degraded: false,
            token_summary: None,
            items: vec![RecallExplainItemV1 {
                candidate_id: "candidate.original-source".to_owned(),
                provider_rank: 0,
                stage: RecallExplainHostDecisionV1::Selected.stage(),
                host_decision: RecallExplainHostDecisionV1::Selected,
                host_reason_code: "selected".to_owned(),
                host_reason_detail: None,
                provider_explanation: RecallExplainProviderExplanationV1::NotProvided,
                section: None,
                tokens: None,
            }],
        };
        let metadata = PreparedRecallControlMetadataV1::prepare(
            &trace,
            &delivery,
            &BTreeMap::from([(
                "candidate.original-source".to_owned(),
                RetainedRecallControlBindingV1 {
                    stable_memory_ref: format!("observation:{}", attribution.source.observation_id),
                    original_sources: vec![wire],
                },
            )]),
        )
        .unwrap();
        let ledger = Arc::new(RecallAdmissionLedgerV1::open_for_control_test(&data_root).unwrap());
        ledger
            .retain_explain_trace_with_control(
                &delivery.exact_scope_sha256(),
                &trace,
                Some(&metadata),
            )
            .unwrap();
        let selector = ProviderControlSourceSelectorV1 {
            trace_ref: metadata.trace_ref().as_str().to_owned(),
            item_ref: metadata.item_ref(0).unwrap().as_str().to_owned(),
            observation_id: attribution.source.observation_id.clone(),
        };
        let history_authority: Arc<dyn AdvisoryAdmissionAuthority> =
            Arc::new(ProviderHistoryAuthorityV1 {
                mounted_scope: scope.clone(),
                profile_id: profile.clone(),
                registered_shard: registered.binding().shard_id.clone(),
                observations: Arc::new(observations),
                dispositions: dispositions.clone(),
                original_authority: Some(original),
                journal: journal.clone(),
                provider_id: provider.clone(),
                policy_revision: 1,
                runtime: tokio::runtime::Handle::current(),
            });
        let authority = Arc::new(
            ProviderControlAuthorityV1::from_mounted_data(
                ProviderControlAuthorityInputsV1 {
                    canonical_project_path: project_root.clone(),
                    profile_id: profile.clone(),
                    mounted_scope: scope.clone(),
                    session_db: registered.clone(),
                    dispositions: dispositions.clone(),
                    hook_origin_reader: origin_reader,
                    store_data_root: data_root,
                    live_ledger: Some(ledger.clone()),
                    live_journals: vec![(provider, journal.clone())],
                    runtime: tokio::runtime::Handle::current(),
                },
                &operation_control(),
            )
            .unwrap(),
        );
        let port = ProjectProviderControlPortV1 {
            inputs: ProviderControlMountInputsV1 {
                authority: Some(authority),
                composition: Arc::new(ProjectMemoryProviderComposition::Disabled),
                journeys: Vec::new(),
                profile_id: profile,
                mounted_scope: scope.clone(),
                authoritative_project_id: project,
                project_root,
                configuration_digest: ManifestDigest::new(format!("sha256:{}", "d".repeat(64)))
                    .unwrap(),
                canonical_session_db: registered,
                canonical_dispositions: dispositions,
            },
        };
        Self {
            _temporary: temporary,
            _runtime: runtime,
            port,
            ledger,
            journal,
            history_authority,
            selector,
            attribution,
            scope,
        }
    }

    fn request(&self) -> ProviderControlRequestV1 {
        ProviderControlRequestV1::DeleteBySource(ProviderDeleteBySourceRequestV1 {
            source: self.selector.clone(),
            expected_fence_revision: 0,
            mode: ProviderControlDeletionModeV1::RemoveInfluence,
            include_snapshots: false,
        })
    }

    fn context(
        &self,
        request: &ProviderControlRequestV1,
        request_id: &str,
    ) -> (RequestContext, CancellationSignal) {
        let now = tracedecay_contracts::now_micros();
        let expiry = UtcMicros(now.0 + 30_000_000);
        let operation =
            tracedecay_contracts::retained_surface_application_operation(request.operation())
                .unwrap();
        let grant = CapabilityGrantSnapshot::new(
            CapabilityGrantId::new("grant.offline-source").unwrap(),
            1,
            ManifestDigest::new(format!("sha256:{}", "e".repeat(64))).unwrap(),
            ActorId::new("actor.issuer").unwrap(),
            UtcMicros(now.0 - 1),
            expiry,
            self.scope.clone(),
            BTreeSet::from([operation.capability_id().clone()]),
            BTreeSet::from([operation.use_case_id().clone()]),
            DisclosureClass::Evidence,
        )
        .unwrap();
        (
            RequestContext::new(
                ActorId::new("actor.offline-source").unwrap(),
                self.scope.clone(),
                grant,
                RequestId::new(request_id).unwrap(),
                Deadline::new(expiry).unwrap(),
                CancellationContext::active("cancel.offline-source").unwrap(),
            )
            .unwrap(),
            CancellationSignal::active("cancel.offline-source").unwrap(),
        )
    }

    async fn execute(
        &self,
        request: &ProviderControlRequestV1,
        request_context: &RequestContext,
        cancellation_signal: &CancellationSignal,
    ) -> Result<ApplicationOutcome<RetainedSurfaceResultV1>, RetainedSurfaceExecutionErrorV1> {
        let operation =
            tracedecay_contracts::retained_surface_application_operation(request.operation())
                .unwrap();
        self.port
            .execute_provider_control(
                RetainedSurfaceExecutionContextV1 {
                    request_context,
                    cancellation_signal,
                    operation: &operation,
                    observed_at: tracedecay_contracts::now_micros(),
                },
                request,
            )
            .await
    }

    async fn admit_source_call(
        &self,
        call: ProviderCall,
    ) -> Result<CurrentAdvisoryAdmission, AdvisoryAdmissionError> {
        let authority = self.history_authority.clone();
        tokio::task::spawn_blocking(move || {
            // The installed synchronous authority requires the same absence of
            // an entered Tokio context as its production dispatch thread.
            std::thread::spawn(move || authority.admit(&call))
                .join()
                .unwrap()
        })
        .await
        .unwrap()
    }

    fn deletion_authority_call(
        &self,
        request: &ProviderDeleteBySourceRequestV1,
        context: &RequestContext,
        accepted: &AcceptedDeletionCommandV1,
        grant: HistoryGrant,
    ) -> ProviderCall {
        let control = operation_control();
        let snapshot = control.snapshot().unwrap();
        // This is an untrusted authority-admission request, never dispatched.
        // The readiness digest is an input claim, not a fabricated ready receipt.
        let readiness_claim = "a".repeat(64);
        let mut payload = deletion_body(request, &self.attribution);
        payload["common_request"] = json!({
            "provider_id": "tracedecay.native",
            "registration_revision": 1,
            "ready_receipt_digest": readiness_claim,
            "exact_scope_identity": projection::scope(&grant.destination_scope),
            "operation_id": accepted.operation_id(),
            "idempotency_key": accepted.idempotency_key(),
            "expected_state_generation": 1,
            "request_identity": context.request_id().as_str(),
            "policy_revision": 1,
            "deadline": {"deadline_utc_micros": control.deadline_utc_micros(), "remaining_millis":snapshot.remaining_millis},
            "cancellation": "live", "extensions": [],
        });
        ProviderCall::new(ProviderCallParts {
            operation: ProviderOperation::DeleteBySource,
            provider_id: OwnedProviderId::new("tracedecay.native").unwrap(),
            registration_revision: 1,
            ready_receipt_sha256: readiness_claim,
            exact_scope: grant.destination_scope.clone(),
            request_id: context.request_id().as_str().to_owned(),
            operation_id: accepted.operation_id().to_owned(),
            expected_state_generation: 1,
            idempotency_key: Some(accepted.idempotency_key().to_owned()),
            control,
            payload: authority_payload(&payload),
            required_capabilities: vec![
                OwnedVersionedId::new(ProviderOperation::DeleteBySource.capability_id()).unwrap(),
            ],
            extensions: Vec::new(),
        })
        .unwrap()
        .with_history_grant(grant)
    }

    fn durable_intent(
        &self,
        accepted: &AcceptedDeletionCommandV1,
    ) -> ProviderSourceIntentReceiptV1 {
        let digest = original_source_fence_digest(&self.attribution).unwrap();
        self.journal
            .read_provider_source_deletion_intent_receipt_bounded(
                &ProviderSourceDeletionIntentV1 {
                    operation_id: accepted.operation_id(),
                    provider_id: "tracedecay.native",
                    original_source_sha256: &digest,
                    expected_fence_revision: 0,
                    mode: DeletionMode::RemoveInfluence,
                    source_revision: self.attribution.source.source_revision.as_deref(),
                    authority_ref: accepted.operation_id(),
                    accepted_at_utc_micros: accepted.accepted_at().0,
                },
                RecoveryTimeBudgetV1 {
                    remaining_micros: 1_000_000,
                },
                &CancellationToken::new(),
            )
            .unwrap()
            .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_deletion_retains_real_intent_retries_identically_and_refuses_changed_command() {
    let fixture = OfflineSourceFixture::new().await;
    let request = fixture.request();
    let (context, cancellation) = fixture.context(&request, "request.offline-delete");
    let ApplicationOutcome::Effect(first) = fixture
        .execute(&request, &context, &cancellation)
        .await
        .unwrap()
    else {
        panic!("host deletion is an effect");
    };
    assert_eq!(first.receipt.outcome, EffectTermination::Completed);
    assert_eq!(first.reconciliation, ReconciliationState::Reconciled);
    assert!(first.receipt.committed_state.is_some());
    let Some(RetainedSurfaceResultV1::ProviderControl(result)) = &first.payload else {
        panic!("typed provider result");
    };
    assert_eq!(
        result.terminal,
        ProviderControlTerminalV1::ProviderUnavailable
    );
    assert_eq!(result.effect.state, ProviderControlEffectStateV1::None);
    assert!(result.effect.provider_receipt_digest.is_none());
    let ProviderControlOperationResultV1::DeleteBySource(Some(deletion)) = &result.result else {
        panic!("durable host intent must survive offline dispatch");
    };
    assert!(matches!(
        deletion.erasure,
        ProviderControlErasureV1::Pending { .. }
    ));
    assert_eq!(deletion.intent.host_receipt, first.receipt);
    assert_eq!(
        deletion.host_snapshot_cleanup.state,
        ProviderControlHostSnapshotCleanupStateV1::NotRequested
    );
    assert!(
        deletion
            .host_snapshot_cleanup
            .removed_snapshot_refs
            .is_empty()
    );
    assert_eq!(
        (
            deletion.intent.fence_revision_before,
            deletion.intent.fence_revision_after
        ),
        (0, 1)
    );
    let accepted = fixture
        .ledger
        .retained_deletion_command(&context, &operation_control())
        .unwrap();
    assert_eq!(accepted.operation_id(), result.operation_id);
    assert_eq!(accepted.idempotency_key(), first.idempotency_key.as_str());
    let actual = fixture.durable_intent(&accepted);
    assert_eq!(actual.operation_id, accepted.operation_id());
    assert_eq!(actual.fence.revision, 1);
    assert!(!actual.provider_erasure_verified);

    let ApplicationOutcome::Effect(retry) = fixture
        .execute(&request, &context, &cancellation)
        .await
        .unwrap()
    else {
        panic!("retry retains effect outcome");
    };
    assert_eq!(retry.receipt, first.receipt);
    assert_eq!(retry.idempotency_key, first.idempotency_key);
    let repeated = fixture
        .ledger
        .retained_deletion_command(&context, &operation_control())
        .unwrap();
    assert_eq!(repeated.operation_id(), accepted.operation_id());
    assert_eq!(repeated.accepted_at(), accepted.accepted_at());
    assert_eq!(fixture.durable_intent(&repeated), actual);
    let current_fence = fixture
        .journal
        .read_provider_source_fence(
            "tracedecay.native",
            &original_source_fence_digest(&fixture.attribution).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(current_fence.revision, 1);

    let mut changed = request.clone();
    let ProviderControlRequestV1::DeleteBySource(changed_body) = &mut changed else {
        unreachable!()
    };
    changed_body.mode = ProviderControlDeletionModeV1::HardDelete;
    assert!(matches!(
        fixture.execute(&changed, &context, &cancellation).await,
        Err(RetainedSurfaceExecutionErrorV1::Conflict)
    ));
    assert_eq!(fixture.durable_intent(&accepted), actual);
    let after = fixture
        .ledger
        .retained_deletion_command(&context, &operation_control())
        .unwrap();
    assert_eq!(after.operation_id(), accepted.operation_id());
    assert_eq!(after.idempotency_key(), accepted.idempotency_key());
    let session = fixture
        .port
        .inputs
        .canonical_session_db
        .get_session_result("claude", SESSION)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.session_id, SESSION);
    let selected = fixture
        .port
        .resolve_source(&fixture.selector, true, &operation_control())
        .await
        .unwrap();
    assert_eq!(
        selected.granted_source().unwrap().attribution,
        fixture.attribution
    );
    assert_eq!(
        selected.granted_source().unwrap().current_disposition.state,
        tracedecay_memory_provider_registry::SourceDisposition::Deleted
    );
}

fn snapshot_carrier(
    scope: &RetainedRecallControlScopeV1,
    sources: &[tracedecay_memory_provider_registry::GrantedHistorySource],
    name: &str,
) -> Value {
    use sha2::{Digest, Sha256};
    let bytes = format!("fixture snapshot state: {name}").into_bytes();
    json!({
        "identity": {
            "snapshot_id":name,"provider_id":scope.provider_id.as_str(),
            "implementation_identity_digest":"b".repeat(64), "state_schema_version":"native-staged-v2",
            "exact_scope_digest":scope.delivery_scope.exact_scope_sha256(), "state_generation":1,
            "observation_sequence":sources.iter().map(|item|item.attribution.source_sequence).max().unwrap_or(0),
            "parent_snapshot_id":null,"content_sha256":hex::encode(Sha256::digest(&bytes)),
            "byte_length":bytes.len(),"created_at":rfc3339_utc_micros(tracedecay_contracts::now_micros().0).unwrap(),
        },
        "bytes":bytes,
        "sources":sources.iter().map(|item|source_attribution_json(&item.attribution).unwrap()["source"].clone()).collect::<Vec<_>>(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_snapshot_cleanup_keeps_actual_removal_proof_when_an_unrelated_file_is_corrupt() {
    use crate::daemon::retained_owner::provider_control::portability::{
        read_snapshot_artifact, seed_snapshot_artifact_for_test,
    };
    let fixture = OfflineSourceFixture::new().await;
    let authorized = fixture
        .port
        .resolve_source(&fixture.selector, false, &operation_control())
        .await
        .unwrap();
    let state = &authorized.authorized.retained.scope;
    let matching = seed_snapshot_artifact_for_test(
        &fixture.ledger,
        state,
        &authorized.authorized.grant.sources,
        &snapshot_carrier(
            state,
            &authorized.authorized.grant.sources,
            "snapshot.containing.original",
        ),
        &operation_control(),
    )
    .unwrap();
    // A valid snapshot without original sources makes no provenance claim. It
    // must survive deletion of this source's independent advisory influence.
    let unrelated = seed_snapshot_artifact_for_test(
        &fixture.ledger,
        state,
        &[],
        &snapshot_carrier(state, &[], "snapshot.without.original.sources"),
        &operation_control(),
    )
    .unwrap();
    assert!(
        read_snapshot_artifact(&fixture.ledger, state, &matching, &operation_control()).is_ok()
    );
    assert!(
        read_snapshot_artifact(&fixture.ledger, state, &unrelated, &operation_control()).is_ok()
    );
    let corrupt_path = fixture
        .ledger
        .path()
        .parent()
        .unwrap()
        .join("recall-portability-v1")
        .join("unknown-corrupt.snapshot");
    let mut corrupt = tracedecay_private_fs::create_private_file(&corrupt_path).unwrap();
    corrupt.write_all(b"incomplete artifact header").unwrap();
    corrupt.sync_all().unwrap();
    drop(corrupt);
    let mut request = fixture.request();
    let ProviderControlRequestV1::DeleteBySource(body) = &mut request else {
        unreachable!()
    };
    body.include_snapshots = true;
    let (context, cancellation) = fixture.context(&request, "request.offline-snapshot-delete");
    let ApplicationOutcome::Effect(output) = fixture
        .execute(&request, &context, &cancellation)
        .await
        .unwrap()
    else {
        panic!("durable host intent");
    };
    assert_eq!(output.receipt.outcome, EffectTermination::Completed);
    let Some(RetainedSurfaceResultV1::ProviderControl(result)) = output.payload else {
        panic!("typed provider outcome");
    };
    assert_eq!(
        result.terminal,
        ProviderControlTerminalV1::ProviderUnavailable
    );
    assert_eq!(result.effect.state, ProviderControlEffectStateV1::None);
    let ProviderControlOperationResultV1::DeleteBySource(Some(deletion)) = result.result else {
        panic!("host deletion data");
    };
    assert_eq!(deletion.intent.host_receipt, output.receipt);
    assert!(matches!(
        deletion.erasure,
        ProviderControlErasureV1::Pending { .. }
    ));
    assert_eq!(
        deletion.host_snapshot_cleanup.state,
        ProviderControlHostSnapshotCleanupStateV1::Partial
    );
    assert_eq!(
        deletion.host_snapshot_cleanup.removed_snapshot_refs,
        vec![matching.clone()]
    );
    assert_eq!(deletion.host_snapshot_cleanup.matched_count, 1);
    assert_eq!(deletion.host_snapshot_cleanup.unverifiable_count, 1);
    assert!(
        read_snapshot_artifact(&fixture.ledger, state, &matching, &operation_control()).is_err()
    );
    assert!(
        read_snapshot_artifact(&fixture.ledger, state, &unrelated, &operation_control()).is_ok()
    );
    assert!(corrupt_path.exists());
    let accepted = fixture
        .ledger
        .retained_deletion_command(&context, &operation_control())
        .unwrap();
    let actual = fixture.durable_intent(&accepted);
    assert!(!actual.provider_erasure_verified);
    assert_eq!(actual.fence.revision, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_controls_refuse_foreign_scope_provider_and_reused_replacement_before_mutation() {
    let mut fixture = OfflineSourceFixture::new().await;
    let original_scope = fixture.scope.clone();
    fixture.scope = ResolvedScope::new(
        original_scope.project_id.clone(),
        original_scope.repository_id.clone(),
        tracedecay_domain::WorktreeId::new("worktree.foreign-control").unwrap(),
        original_scope.reference.clone(),
    )
    .unwrap();
    let deletion = fixture.request();
    let (context, cancellation) = fixture.context(&deletion, "request.foreign-scope");
    assert!(matches!(
        fixture.execute(&deletion, &context, &cancellation).await,
        Err(RetainedSurfaceExecutionErrorV1::NotFoundOrNotAuthorized)
    ));
    fixture.scope = original_scope;

    // The actual source cannot replace itself: it has no new canonical revision.
    assert!(validate_replacement_lineage(&fixture.attribution, &fixture.attribution).is_err());
    let expected_source_revision = fixture
        .attribution
        .source
        .source_revision
        .clone()
        .unwrap_or_else(|| "unknown-original-revision".to_owned());
    let correction = ProviderCorrectionRequestV1 {
        source: fixture.selector.clone(),
        expected_source_revision,
        correction: ProviderControlCorrectionV1::ReplaceContent {
            replacement_source: fixture.selector.clone(),
        },
        reason: "A replacement must be independently admitted as a new revision.".to_owned(),
        evidence_refs: Vec::new(),
    };
    let replacement = ProviderControlRequestV1::Correction(correction.clone());
    let (context, cancellation) = fixture.context(&replacement, "request.reused-replacement");
    assert!(matches!(
        fixture.execute(&replacement, &context, &cancellation).await,
        Err(RetainedSurfaceExecutionErrorV1::NotFoundOrNotAuthorized
            | RetainedSurfaceExecutionErrorV1::Conflict)
    ));

    let foreign = ProviderControlRequestV1::Health(ProviderHealthRequestV1 {
        state: ProviderControlStateSelectorV1::CanonicalSession {
            provider_id: "provider.foreign-control".to_owned(),
            registration_revision: 1,
            canonical_provider_id: "claude".to_owned(),
            session_id: SESSION.to_owned(),
        },
        requested_checks: vec![ProviderControlHealthCheckV1::Protocol],
    });
    let (context, cancellation) = fixture.context(&foreign, "request.foreign-provider");
    assert!(
        fixture
            .execute(&foreign, &context, &cancellation)
            .await
            .is_err()
    );
    assert!(
        fixture
            .journal
            .read_provider_source_fence(
                "tracedecay.native",
                &original_source_fence_digest(&fixture.attribution).unwrap(),
            )
            .unwrap()
            .is_none()
    );
    let source = fixture
        .port
        .resolve_source(&fixture.selector, false, &operation_control())
        .await
        .unwrap();
    assert_eq!(
        source.granted_source().unwrap().attribution,
        fixture.attribution
    );
    // Use the real resolved target to verify the exact deletion wire query.
    let ProviderControlRequestV1::DeleteBySource(deletion) = &deletion else {
        panic!("delete")
    };
    let body = deletion_body(deletion, &fixture.attribution);
    assert_eq!(
        body["verification_query"].as_str(),
        Some(fixture.attribution.source.source_key.as_str())
    );
    assert_eq!(
        body["forget_source_keys"],
        json!([fixture.attribution.source.source_key])
    );
    assert!(body.get("retention_lock_blocked").is_none());
}

fn authority_payload(value: &Value) -> CanonicalPayload {
    let bytes = tracedecay_domain::canonical_json_bytes(value).unwrap();
    let digest = tracedecay_domain::canonical_text::sha256_hex(&bytes);
    CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.provider.deletion-by-source.v1").unwrap(),
        bytes,
        digest,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_authority_revalidates_cross_session_private_sources_and_post_fence_deletion() {
    let fixture = OfflineSourceFixture::for_delivery_session("different-delivery-session").await;
    let request = fixture.request();
    let (context, cancellation) = fixture.context(&request, "request.private-source-authority");
    let ProviderControlRequestV1::DeleteBySource(deletion) = &request else {
        unreachable!()
    };
    let source = fixture
        .port
        .resolve_source(&fixture.selector, false, &operation_control())
        .await
        .unwrap();
    let claim = source.authorized.grant.clone();
    assert_eq!(
        claim.relation,
        tracedecay_memory_provider_registry::HistoryRelation::SameCheckout
    );
    assert_ne!(
        claim.destination_scope.agent_session_id,
        fixture
            .attribution
            .origin_scope
            .recorded_scope()
            .unwrap()
            .agent_session_id
    );
    assert!(
        fixture.attribution.source.source_revision.is_none(),
        "the real Claude producer provides no canonical revision"
    );
    let accepted = fixture
        .ledger
        .accept_deletion_command(
            &context,
            deletion,
            &source.target,
            &fixture.attribution,
            &operation_control(),
        )
        .unwrap();
    let call = fixture.deletion_authority_call(deletion, &context, &accepted, claim.clone());
    let admitted = fixture.admit_source_call(call.clone()).await.unwrap();
    admitted.verify_for(&call).unwrap();
    assert_eq!(admitted.history_sources.len(), 1);
    assert_eq!(admitted.history_sources[0].attribution, fixture.attribution);
    assert_eq!(
        admitted.history_sources[0].current_disposition.state,
        tracedecay_memory_provider_registry::SourceDisposition::Available
    );

    let mut false_attribution = claim.clone();
    false_attribution.sources[0]
        .attribution
        .source
        .content_sha256 = "f".repeat(64);
    assert!(
        fixture
            .admit_source_call(call.clone().with_history_grant(false_attribution))
            .await
            .is_err()
    );
    let mut unequal_wire = claim.clone();
    unequal_wire.authorization_ref.push_str(".different-claim");
    let mut payload: Value = serde_json::from_slice(&call.payload.bytes).unwrap();
    payload["history_grant"] = history_grant_json(&unequal_wire).unwrap();
    let mut conflicting = call.clone();
    conflicting.payload = authority_payload(&payload);
    assert!(matches!(
        fixture.admit_source_call(conflicting).await,
        Err(AdvisoryAdmissionError::Denied("conflicting history grants"))
    ));

    let outcome = fixture
        .execute(&request, &context, &cancellation)
        .await
        .unwrap();
    let ApplicationOutcome::Effect(effect) = outcome else {
        panic!("durable host intent")
    };
    assert_eq!(effect.receipt.outcome, EffectTermination::Completed);
    assert!(!fixture.durable_intent(&accepted).provider_erasure_verified);
    assert_eq!(
        claim.sources[0].current_disposition.state,
        tracedecay_memory_provider_registry::SourceDisposition::Available
    );
    let after = fixture.admit_source_call(call.clone()).await.unwrap();
    after.verify_for(&call).unwrap();
    assert_eq!(after.history_sources.len(), 1);
    assert_eq!(after.history_sources[0].attribution, fixture.attribution);
    assert_eq!(
        after.history_sources[0].current_disposition.state,
        tracedecay_memory_provider_registry::SourceDisposition::Deleted
    );
}
