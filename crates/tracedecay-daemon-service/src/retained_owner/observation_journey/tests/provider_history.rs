//! Current canonical evidence remains distinct from original live-event proof.
use super::*;
use crate::retained_owner::provider_history::{
    HistoryIdentityBridgeV1, HookOriginReaderV1, MountedOriginalObservationAuthorityV1,
    ORIGINAL_PROVENANCE_UNAVAILABLE, ProviderHistoryAuthorityV1, ProviderHistoryErrorV1,
    ProviderHistoryReaderV1,
};
use tracedecay_domain::{BrainId, FactOwnerV1, RetrievalAnchorId};
use tracedecay_global_db::GlobalDbObservationStore;
use tracedecay_host_admission::{HostAdmissionAuthorities, HostAdmissionFacade};
use tracedecay_memory_provider_registry::{
    AdmittedTemporalQuery, AdvisoryAdmissionAuthority, AdvisoryAdmissionError, RecallBudgetsV1,
    RecallRequestParts, build_recall_request_payload, rfc3339_utc_micros,
};
use tracedecay_runtime_core::db::Database;
use tracedecay_sessions::observation::ObservationCancellation;
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_sessions::runtime::hosts::claude::ClaudeSource;
use tracedecay_sessions::runtime::hosts::claude_observation::ingest_source_with_observations_with_admission;
use tracedecay_store::{
    AnchorDispositionAppendOutcomeV1, AnchorDispositionReasonClassV1, AnchorDispositionStateV1,
    RetrievalAnchorDerivativeV1, RetrievalAnchorDispositionRecordV1,
    RetrievalAnchorDispositionStore, RetrievalAnchorStoreResult, RetrievalAnchorTombstoneV1,
    StoreShardIdV1,
};

struct RepositoryFixture {
    temp: TempDir,
    project: ProjectId,
    profile: UserProfileId,
    scope: ResolvedScope,
    context: RepositoryProvenanceAdmissionContext,
}
impl RepositoryFixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        git(temp.path(), &["init", "-q", "-b", "main"]);
        git(temp.path(), &["config", "user.name", "History Test"]);
        git(
            temp.path(),
            &["config", "user.email", "history@example.invalid"],
        );
        std::fs::write(temp.path().join("tracked"), "original").unwrap();
        git(temp.path(), &["add", "tracked"]);
        git(temp.path(), &["commit", "-q", "-m", "original"]);
        let project = ProjectId::new("project.authorized-history").unwrap();
        let profile = UserProfileId::new("profile.authorized-history").unwrap();
        tracedecay_runtime_core::storage::write_repository_identity_marker(
            temp.path(),
            project.as_str(),
        )
        .unwrap();
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(temp.path(), &project)
                .unwrap();
        let marker = tracedecay_runtime_core::storage::read_repository_identity_marker(temp.path())
            .unwrap()
            .unwrap();
        let context = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
            temp.path(),
            &project,
            &marker,
        )
        .unwrap();
        Self {
            temp,
            project,
            profile,
            scope,
            context,
        }
    }
    fn bridge(&self) -> HistoryIdentityBridgeV1 {
        HistoryIdentityBridgeV1::admit(
            self.temp.path(),
            &self.profile,
            &self.scope,
            &StoreShardIdV1::project_sessions(
                BrainId::new("brain.history").unwrap(),
                self.profile.clone(),
                self.project.clone(),
            ),
        )
        .unwrap()
    }
    fn record(&self, session: &str, sequence: u64) -> StoredObservation {
        let canonical = self.context.admitted_identity().unwrap().0;
        let observation = canonical_observation_at(
            &canonical,
            &SessionId::new(session).unwrap(),
            "source history",
            sequence,
        );
        let record = settled_record(sequence, observation);
        let authorization = build_observation_resolution_authorization_v1(
            record.observation(),
            "observation-journey-test",
        )
        .unwrap();
        let prepared = self.context.capture_after_sanitization(
            record.observation(),
            record.projection_generation(),
            record.retrieval_anchor().ingested_at(),
            authorization,
        );
        let attachment = tracedecay_store::RepositoryProvenanceAttachmentV1::new(
            prepared.availability().clone(),
            prepared.anchor().cloned(),
        )
        .unwrap();
        assert!(attachment.availability().value().is_some());
        let receipt = record
            .commit_receipt()
            .clone()
            .with_repository_provenance_attachment(attachment)
            .unwrap();
        StoredObservation::from_commit_receipt(receipt, record.projection_status())
    }
    fn journal(&self) -> SqliteObservationJournal {
        SqliteObservationJournal::open(
            &self.temp.path().join("history.sqlite"),
            ObservationJourneyPolicyV1::project_default().retention,
        )
        .unwrap()
    }
}
fn git(root: &Path, args: &[&str]) {
    let output =
        std::process::Command::new(tracedecay_runtime_core::git::try_git_program().unwrap())
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
fn control() -> OperationControl {
    OperationControl::new(
        tracedecay_contracts::now_micros().0 + 30_000_000,
        30_000,
        CancellationToken::new(),
    )
}

const CURRENT_SESSION: &str = "current-observation-session";
const CURRENT_CONTENT: &str =
    "Confirm current canonical content without claiming original history.";

struct CurrentObservationFixture {
    _temp: TempDir,
    _runtime: HostAdmissionTestRuntimeV1,
    project_root: PathBuf,
    stored: StoredObservation,
    authority: Arc<ProviderHistoryAuthorityV1<GlobalDbObservationStore, Database>>,
}

impl CurrentObservationFixture {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("repository");
        std::fs::create_dir_all(&root).unwrap();
        git(&root, &["init", "-q", "-b", "main"]);
        git(&root, &["config", "user.name", "Current Observation Test"]);
        git(
            &root,
            &[
                "config",
                "user.email",
                "current-observation@example.invalid",
            ],
        );
        git(&root, &["commit", "--allow-empty", "-q", "-m", "initial"]);
        let root = std::fs::canonicalize(root).unwrap();
        let project = ProjectId::new("project.current-observation").unwrap();
        let runtime = HostAdmissionTestRuntimeV1::project(
            temp.path().join("profile"),
            &root,
            project.clone(),
        )
        .await
        .unwrap();
        tracedecay_runtime_core::storage::write_repository_identity_marker(&root, project.as_str())
            .unwrap();
        let registered = runtime
            .registered_database_arc(HostAdmissionScope::Project)
            .unwrap();
        let shard = registered.binding().shard_id.clone();
        let profile = shard.profile_id.clone();
        let marker = tracedecay_runtime_core::storage::read_repository_identity_marker(&root)
            .unwrap()
            .unwrap();
        let provenance = RepositoryProvenanceAdmissionContext::from_authoritative_project_marker(
            &root, &project, &marker,
        )
        .unwrap();
        let facade = HostAdmissionFacade::new(
            HostAdmissionAuthorities::for_project(
                shard.brain_id.clone(),
                profile.clone(),
                project.clone(),
                registered.as_ref(),
            )
            .with_repository_provenance(provenance)
            .with_background_cpu(runtime.background_cpu()),
        );
        let home = temp.path().join("host");
        let transcript = home
            .join(".claude/projects/current-fixture")
            .join(format!("{CURRENT_SESSION}.jsonl"));
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let now = tracedecay_contracts::now_micros();
        let line = json!({
            "type": "user", "sessionId": CURRENT_SESSION, "uuid": "message.current-observation",
            "timestamp": rfc3339_utc_micros(now.0).unwrap(), "cwd": root,
            "message": {"role": "user", "content": CURRENT_CONTENT},
        });
        std::fs::write(&transcript, format!("{line}\n")).unwrap();
        let ingest = ingest_source_with_observations_with_admission(
            &ClaudeSource::with_home(&home),
            &root,
            ObservationScopeV1::Project {
                project_id: project.clone(),
            },
            &facade,
            Some(1_048_576),
            ObservationCancellation::default(),
        )
        .await
        .unwrap();
        assert_eq!(ingest.observations_committed, 1);
        assert_eq!(ingest.projections_completed, 1);
        let observations = registered.observation_store();
        let mut records = observations
            .replay_observations(ObservationReplayRequest::new(0, 2).unwrap())
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        let stored = records.pop().unwrap();
        let session = registered
            .get_session("claude", CURRENT_SESSION)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(session.project_key, project.as_str());
        let scope =
            tracedecay_code_index_runtime::resolved_scope_for_project(&root, &project).unwrap();
        let data_root = temp.path().join("provider-data");
        std::fs::create_dir_all(&data_root).unwrap();
        let mounted_reader = Arc::new(HookOriginReaderV1::new(
            data_root.clone(),
            shard.brain_id.clone(),
            profile.clone(),
        ));
        mounted_reader.bind_data_root(&data_root).unwrap();
        let mounted_bridge =
            Arc::new(HistoryIdentityBridgeV1::admit(&root, &profile, &scope, &shard).unwrap());
        let dispositions = runtime
            .session_registry_for_test()
            .project_memory(project, [root.clone()])
            .await
            .unwrap();
        let authority = Arc::new(ProviderHistoryAuthorityV1 {
            mounted_scope: scope,
            profile_id: profile,
            registered_shard: shard,
            session_db: registered,
            mounted_reader,
            mounted_bridge: Some(mounted_bridge),
            observations: Arc::new(observations),
            dispositions,
            original_authority: None,
            journal: Arc::new(
                SqliteObservationJournal::open(
                    data_root.join("current-observation.sqlite"),
                    ObservationJourneyPolicyV1::project_default().retention,
                )
                .unwrap(),
            ),
            provider_id: OwnedProviderId::new("tracedecay.native").unwrap(),
            policy_revision: 1,
            runtime: tokio::runtime::Handle::current(),
        });
        authority.validate_mount().unwrap();
        Self {
            _temp: temp,
            _runtime: runtime,
            project_root: root,
            stored,
            authority,
        }
    }

    fn destination(&self, session: &str) -> OwnedExactScope {
        self.authority
            .mounted_bridge
            .as_ref()
            .unwrap()
            .destination(session)
            .unwrap()
    }

    async fn current(
        &self,
        destination: &OwnedExactScope,
        control: &OperationControl,
    ) -> Result<StoredObservation, ProviderHistoryErrorV1> {
        self.authority
            .resolve_current_session_observation(
                &self.authority.provider_id,
                destination,
                self.stored.observation().observation_id(),
                control,
            )
            .await
    }
}

struct ActiveDispositions;
impl RetrievalAnchorDispositionStore for ActiveDispositions {
    async fn append_disposition(
        &self,
        _: RetrievalAnchorDispositionRecordV1,
    ) -> RetrievalAnchorStoreResult<AnchorDispositionAppendOutcomeV1> {
        panic!("read-only fixture")
    }
    async fn publish_derivative(
        &self,
        _: RetrievalAnchorDerivativeV1,
    ) -> RetrievalAnchorStoreResult<AnchorDispositionAppendOutcomeV1> {
        panic!("read-only fixture")
    }
    async fn current_disposition(
        &self,
        _: &RetrievalAnchorId,
        _: &FactOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorDispositionRecordV1>> {
        Ok(None)
    }
    async fn tombstone(
        &self,
        _: &RetrievalAnchorId,
        _: &FactOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorTombstoneV1>> {
        panic!("read-only fixture")
    }
    async fn derivatives(
        &self,
        _: &RetrievalAnchorId,
        _: &FactOwnerV1,
    ) -> RetrievalAnchorStoreResult<Vec<RetrievalAnchorDerivativeV1>> {
        panic!("read-only fixture")
    }
}

#[tokio::test]
async fn ingestion_capture_cannot_authorize_recent_or_replayed_original_history() {
    let fixture = RepositoryFixture::new();
    let bridge = Arc::new(fixture.bridge());
    let identity = Arc::new(HookOriginReaderV1::new(
        fixture.temp.path().to_path_buf(),
        BrainId::new("brain.history").unwrap(),
        fixture.profile.clone(),
    ));
    identity.bind_data_root(fixture.temp.path()).unwrap();
    let authority = MountedOriginalObservationAuthorityV1 {
        reader: identity,
        bridge: Arc::clone(&bridge),
    };
    let stored = fixture.record("session.source", 1);
    let canonical_payload = stored.observation().payload().clone();
    let records = SettledRecordsPort::single(stored);
    let journal = fixture.journal();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let reader = ProviderHistoryReaderV1 {
        bridge: bridge.as_ref(),
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let destination = bridge.destination("session.destination").unwrap();
    assert!(matches!(
        reader.select_recent_page(&destination, 1, &control()).await,
        Err(ProviderHistoryErrorV1::Unavailable(
            ORIGINAL_PROVENANCE_UNAVAILABLE
        ))
    ));
    assert!(matches!(
        reader.select_page(&destination, 0, 1, &control()).await,
        Err(ProviderHistoryErrorV1::Unavailable(
            ORIGINAL_PROVENANCE_UNAVAILABLE
        ))
    ));
    assert_eq!(
        records.records[0].observation().payload(),
        &canonical_payload
    );
    assert!(
        journal
            .inspect(&JournalInspectionFilterV1::default())
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn absent_original_authority_refuses_history_before_canonical_reads() {
    let fixture = CurrentObservationFixture::new().await;
    let authority = Arc::clone(&fixture.authority);
    authority.validate_mount().unwrap();
    let operation = control();
    let destination = authority
        .mounted_bridge
        .as_ref()
        .unwrap()
        .destination("session.destination")
        .unwrap();
    let request = RecallRequestParts {
        provider_id: authority.provider_id.clone(),
        registration_revision: 1,
        ready_receipt_sha256: READY_RECEIPT.to_owned(),
        exact_scope: destination.clone(),
        request_id: "request.same-session".to_owned(),
        objective: "search".to_owned(),
        query: "current session".to_owned(),
        temporal: AdmittedTemporalQuery::current(
            &rfc3339_utc_micros(tracedecay_contracts::now_micros().0).unwrap(),
        )
        .unwrap(),
        budgets: RecallBudgetsV1 {
            maximum_candidates: 1,
            maximum_candidate_content_bytes: 1024,
            maximum_total_content_bytes: 1024,
            maximum_source_refs_per_candidate: 1,
            maximum_trace_refs_per_candidate: 1,
            maximum_warnings: 1,
            maximum_extensions_per_candidate: 1,
        },
        policy_revision: 1,
        deadline_utc_micros: operation.deadline_utc_micros(),
        remaining_millis: operation.remaining_millis(),
    };
    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: authority.provider_id.clone(),
        registration_revision: 1,
        ready_receipt_sha256: READY_RECEIPT.to_owned(),
        exact_scope: destination,
        request_id: request.request_id.clone(),
        operation_id: "recall.same-session".to_owned(),
        expected_state_generation: 1,
        idempotency_key: None,
        control: operation,
        payload: build_recall_request_payload(&request).unwrap(),
        required_capabilities: vec![
            OwnedVersionedId::new(ProviderOperation::Recall.capability_id()).unwrap(),
        ],
        extensions: Vec::new(),
    })
    .unwrap();
    let current = Arc::clone(&authority);
    let current_call = call.clone();
    // Tokio's blocking pool retains its runtime context. The synchronous
    // provider boundary requires an actual dispatch thread outside that context.
    let admitted = tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || current.admit(&current_call))
            .join()
            .unwrap()
    })
    .await
    .unwrap()
    .unwrap();
    admitted.verify_for(&call).unwrap();
    assert!(admitted.history_sources.is_empty());
    assert!(matches!(
        authority.reader(),
        Err(ProviderHistoryErrorV1::Unavailable(
            "original repository provenance"
        ))
    ));
    git(&fixture.project_root, &["checkout", "-q", "-b", "changed"]);
    assert!(matches!(
        authority.validate_mount(),
        Err(ProviderHistoryErrorV1::Ineligible("mounted scope changed"))
    ));
    assert!(matches!(
        authority.reader(),
        Err(ProviderHistoryErrorV1::Ineligible("mounted scope changed"))
    ));
    let stale = Arc::clone(&authority);
    assert!(matches!(
        tokio::task::spawn_blocking(move || {
            std::thread::spawn(move || stale.admit(&call))
                .join()
                .unwrap()
        })
        .await
        .unwrap(),
        Err(AdvisoryAdmissionError::Denied("mounted scope changed"))
    ));
}

#[tokio::test]
async fn current_canonical_observation_resolves_without_original_history_grant() {
    let fixture = CurrentObservationFixture::new().await;
    let resolved = fixture
        .current(&fixture.destination(CURRENT_SESSION), &control())
        .await
        .unwrap();
    assert_eq!(resolved.observation(), fixture.stored.observation());
    assert_eq!(
        resolved.retrieval_anchor_id(),
        fixture.stored.retrieval_anchor_id()
    );
    assert_eq!(
        resolved.observation().source().provider().as_str(),
        "claude"
    );
    assert_eq!(
        resolved.observation().source().session_id().as_str(),
        CURRENT_SESSION
    );
    let projection = tracedecay_store::derive_canonical_projection(resolved.observation()).unwrap();
    assert_eq!(
        projection.message().unwrap().message().text,
        CURRENT_CONTENT
    );
    assert!(fixture.authority.original_authority.is_none());
    assert!(matches!(
        fixture.authority.reader(),
        Err(ProviderHistoryErrorV1::Unavailable(
            "original repository provenance"
        ))
    ));
}

#[tokio::test]
async fn current_canonical_observation_denies_foreign_provider_session_and_checkout() {
    let fixture = CurrentObservationFixture::new().await;
    let destination = fixture.destination(CURRENT_SESSION);
    let foreign_provider = OwnedProviderId::new("ncm").unwrap();
    assert!(matches!(
        fixture
            .authority
            .resolve_current_session_observation(
                &foreign_provider,
                &destination,
                fixture.stored.observation().observation_id(),
                &control(),
            )
            .await,
        Err(ProviderHistoryErrorV1::ClaimMismatch("mounted provider"))
    ));
    assert!(matches!(
        fixture
            .current(&fixture.destination("foreign-session"), &control())
            .await,
        Err(ProviderHistoryErrorV1::Ineligible(
            "canonical session namespace"
        ))
    ));
    let mounted = &fixture.authority.mounted_scope;
    let foreign_project = ResolvedScope::new(
        ProjectId::new("project.foreign-current-observation").unwrap(),
        mounted.repository_id.clone(),
        mounted.worktree_id.clone(),
        mounted.reference.clone(),
    )
    .unwrap();
    let foreign_checkout = exact_scope_for_session(
        &fixture.authority.profile_id,
        &foreign_project,
        CURRENT_SESSION,
    )
    .unwrap();
    assert!(matches!(
        fixture.current(&foreign_checkout, &control()).await,
        Err(ProviderHistoryErrorV1::Ineligible("destination checkout"))
    ));
}

#[tokio::test]
async fn current_canonical_observation_denies_stale_session_owner_and_git_mount() {
    let fixture = CurrentObservationFixture::new().await;
    let destination = fixture.destination(CURRENT_SESSION);
    let registered = &fixture.authority.session_db;
    let mut session = registered
        .get_session("claude", CURRENT_SESSION)
        .await
        .unwrap()
        .unwrap();
    let current_project = session.project_key.clone();
    session.project_key = "project.earlier-current-observation".to_owned();
    assert!(registered.upsert_session(&session).await);
    assert!(matches!(
        fixture.current(&destination, &control()).await,
        Err(ProviderHistoryErrorV1::Ineligible(
            "canonical session project"
        ))
    ));
    session.project_key = current_project;
    assert!(registered.upsert_session(&session).await);
    assert!(fixture.current(&destination, &control()).await.is_ok());
    git(
        &fixture.project_root,
        &["checkout", "-q", "-b", "changed-current-observation"],
    );
    assert!(matches!(
        fixture.current(&destination, &control()).await,
        Err(ProviderHistoryErrorV1::Ineligible("mounted scope changed"))
    ));
}

#[tokio::test]
async fn current_canonical_observation_denies_cancelled_and_expired_reads() {
    let fixture = CurrentObservationFixture::new().await;
    let destination = fixture.destination(CURRENT_SESSION);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let stopped = OperationControl::new(
        tracedecay_contracts::now_micros().0 + 30_000_000,
        30_000,
        cancellation,
    );
    assert!(matches!(
        fixture.current(&destination, &stopped).await,
        Err(ProviderHistoryErrorV1::Control(TerminalCode::Cancelled))
    ));
    let expired = OperationControl::new(
        tracedecay_contracts::now_micros().0 - 1,
        1_000,
        CancellationToken::new(),
    );
    assert!(matches!(
        fixture.current(&destination, &expired).await,
        Err(ProviderHistoryErrorV1::Control(
            TerminalCode::DeadlineExceeded
        ))
    ));
    assert!(fixture.current(&destination, &control()).await.is_ok());
}

#[tokio::test]
async fn current_canonical_observation_denies_redaction_in_owning_session_store() {
    let fixture = CurrentObservationFixture::new().await;
    let destination = fixture.destination(CURRENT_SESSION);
    assert!(fixture.current(&destination, &control()).await.is_ok());
    let disposition = RetrievalAnchorDispositionRecordV1::new(
        "disposition.current-observation.redacted",
        fixture.stored.retrieval_anchor_id().clone(),
        FactOwnerV1::from(fixture.stored.retrieval_anchor().owner().clone()),
        AnchorDispositionStateV1::Redacted,
        None,
        AnchorDispositionReasonClassV1::Redaction,
        UtcMicros(tracedecay_contracts::now_micros().0),
    )
    .unwrap();
    let transaction = fixture
        .authority
        .session_db
        .begin_write_transaction()
        .await
        .unwrap();
    assert_eq!(
        tracedecay_runtime_core::db::append_retrieval_anchor_disposition_on(
            &transaction,
            &disposition,
        )
        .await
        .unwrap(),
        AnchorDispositionAppendOutcomeV1::Appended,
    );
    transaction.commit().await.unwrap();
    assert!(matches!(
        fixture.current(&destination, &control()).await,
        Err(ProviderHistoryErrorV1::Ineligible(
            "current observation disposition"
        ))
    ));
    let retained = fixture
        .authority
        .observations
        .get_observation(fixture.stored.observation().observation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.observation().payload(),
        fixture.stored.observation().payload()
    );
}
