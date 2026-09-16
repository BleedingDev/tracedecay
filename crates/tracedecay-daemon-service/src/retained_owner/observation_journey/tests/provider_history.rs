//! Original-source history selection and the existing durable delivery journey.
use super::*;
use crate::retained_owner::cognitive_recall::control_attribution::{
    RecallControlTraceRefV1, RecallLocatorKeyV1, redact_retained_source_attribution,
};
use crate::retained_owner::provider_history::{
    HistoryGrantRevalidationV1, HistoryIdentityBridgeV1, OriginalObservationAuthorityV1,
    ProviderHistoryErrorV1, ProviderHistoryReaderV1, history_grant_from_json, history_grant_json,
    original_source_fence_digest, source_attribution_json,
};
use tracedecay_domain::{BrainId, RetrievalAnchorId};
use tracedecay_memory_provider_ncm::NCM_PROVIDER_ID;
use tracedecay_memory_provider_registry::{
    COMMON_ADVISORY_PROFILE_ID, COMMON_ADVISORY_REQUIRED_CAPABILITIES, HistoryGrant,
    MemoryProviderV1, ProviderExecutionShapeV1, ProviderLifecycleOwnershipV1,
    ProviderRegistrationV1, RecallScopeBindingsV1, SelectedProviderActivationV1,
    recall_admission::source_attribution::RecallSourceAttributionV1,
};
use tracedecay_sessions::repository_provenance::RepositoryProvenanceAdmissionContext;
use tracedecay_store::{
    AnchorDispositionAppendOutcomeV1, RetrievalAnchorDerivativeV1,
    RetrievalAnchorDispositionRecordV1, RetrievalAnchorDispositionStore, RetrievalAnchorOwnerV1,
    RetrievalAnchorStoreResult, RetrievalAnchorTombstoneV1, StoreShardIdV1,
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
    fn record(&self, session: &str, sequence: u64, original: bool) -> StoredObservation {
        let canonical = self.context.admitted_identity().unwrap().0;
        let observation = canonical_observation_at(
            &canonical,
            &SessionId::new(session).unwrap(),
            "source history",
            sequence,
        );
        let record = settled_record(sequence, observation);
        if !original {
            return record;
        }
        let token = self
            .context
            .capture_original_observation(
                record.observation().identity().clone(),
                format!("test-live-receipt:{session}:{sequence}"),
                UtcMicros(1_740_000_000_000_000),
            )
            .unwrap();
        let authorization = build_observation_resolution_authorization_v1(
            record.observation(),
            "observation-journey-test",
        )
        .unwrap();
        let attachment = token
            .bind_after_sanitization(
                record.observation(),
                record.projection_generation(),
                record.retrieval_anchor().ingested_at(),
                authorization,
            )
            .unwrap();
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
struct OriginalReceipts {
    sessions: BTreeSet<String>,
    active: AtomicBool,
}
impl OriginalReceipts {
    fn new() -> Self {
        Self {
            sessions: [
                "session.source",
                "session.destination",
                "session.other-destination",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            active: AtomicBool::new(true),
        }
    }
}
impl OriginalObservationAuthorityV1 for OriginalReceipts {
    fn validate_original_event(
        &self,
        authority: &str,
        stored: &StoredObservation,
    ) -> Result<bool, ProviderHistoryErrorV1> {
        Ok(self.active.load(Ordering::SeqCst)
            && authority
                == format!(
                    "test-live-receipt:{}:{}",
                    stored.observation().source().session_id().as_str(),
                    stored.sequence()
                ))
    }
    fn authorizes_session(
        &self,
        source: &str,
        _destination: &OwnedExactScope,
    ) -> Result<bool, ProviderHistoryErrorV1> {
        Ok(self.sessions.contains(source))
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
        _: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorDispositionRecordV1>> {
        Ok(None)
    }
    async fn tombstone(
        &self,
        _: &RetrievalAnchorId,
        _: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorTombstoneV1>> {
        panic!("read-only fixture")
    }
    async fn derivatives(
        &self,
        _: &RetrievalAnchorId,
        _: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Vec<RetrievalAnchorDerivativeV1>> {
        panic!("read-only fixture")
    }
}

#[tokio::test]
async fn only_proven_original_sessions_enter_history_and_wire_cannot_change_them() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let canonical = fixture.context.admitted_identity().unwrap();
    // Canonical IDs deliberately differ from the independently resolved daemon namespace.
    assert_ne!(canonical.1, fixture.scope.repository_id);
    let records = SettledRecordsPort {
        records: vec![
            fixture.record("session.source", 1, true),
            fixture.record("session.legacy", 2, false),
            fixture.record("session.ungranted", 3, true),
        ],
    };
    let journal = fixture.journal();
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let destination = bridge.destination("session.destination").unwrap();
    let page = reader
        .select_page(&destination, 0, 3, &control())
        .await
        .unwrap();
    assert_eq!(
        (
            page.scanned,
            page.withheld,
            page.unknown_revision,
            page.last_scanned_sequence,
            page.has_more
        ),
        (3, 2, 1, 3, true)
    );
    let grant = page.grant.unwrap();
    assert_eq!(grant.sources.len(), 1);
    let original = &grant.sources[0].attribution;
    assert_eq!(original.source.canonical_session_id, "session.source");
    assert_ne!(
        original
            .origin_scope
            .recorded_scope()
            .unwrap()
            .agent_session_id,
        destination.agent_session_id
    );
    assert_eq!(original.source.source_revision, None);
    assert_eq!(original.source_sequence, 1);
    assert_eq!(original.ingested_at_utc_nanos, 1_750_000_000_000_000_000);
    assert_eq!(original.validity, Default::default());
    assert_eq!(
        history_grant_from_json(&history_grant_json(&grant).unwrap()).unwrap(),
        grant
    );
    let mut changed = grant.clone();
    changed.sources[0].attribution.source.content_sha256 = "f".repeat(64);
    assert!(reader.revalidate_grant(&changed, &control()).await.is_err());
    authority.active.store(false, Ordering::SeqCst);
    assert!(reader.revalidate_grant(&grant, &control()).await.is_err());
    assert_eq!(
        reader
            .select_page(&destination, 0, 3, &control())
            .await
            .unwrap()
            .withheld,
        3
    );
}

#[tokio::test]
async fn opaque_retained_locator_reopens_and_reauthorizes_by_exact_source_sequence() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort {
        records: vec![
            fixture.record("session.source", 1, true),
            fixture.record("session.source", 2, true),
        ],
    };
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    let trace = RecallControlTraceRefV1::parse(&format!(
        "recall-trace-v1:{}:{}",
        destination.exact_scope_sha256(),
        "a".repeat(64)
    ))
    .unwrap();
    let candidate_id = format!("advisory.retained-identity-v1.{}", "b".repeat(64));
    let journal = fixture.journal();
    let opaque = {
        let reader = ProviderHistoryReaderV1 {
            bridge: &bridge,
            observations: &records,
            dispositions: &ActiveDispositions,
            original_authority: &authority,
            journal: &journal,
            provider_id: &provider,
            policy_revision: 1,
        };
        let page = reader
            .select_page(&destination, 0, 1, &control())
            .await
            .unwrap();
        let attribution = page.grant.unwrap().sources[0].attribution.clone();
        let wire: RecallSourceAttributionV1 =
            serde_json::from_value(source_attribution_json(&attribution).unwrap()).unwrap();
        redact_retained_source_attribution(&trace, 7, 0, &candidate_id, &wire).unwrap()
    };
    drop(journal);

    // Reopen the provider journal to model a daemon restart. The persisted
    // opaque locator is the only source identity carried into this reader.
    let reopened_journal = fixture.journal();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &reopened_journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let grant = reader
        .authorize_retained_source_locator(
            &destination,
            &trace,
            7,
            0,
            &candidate_id,
            &opaque,
            &control(),
            false,
            &RecallLocatorKeyV1::for_test(),
        )
        .await
        .unwrap();
    assert_eq!(grant.sources.len(), 1);
    assert_eq!(grant.sources[0].attribution.source_sequence, 1);
    assert_eq!(
        grant.sources[0].attribution.source.canonical_session_id,
        "session.source"
    );

    // The persisted handle is bound to the durable host secret. A restarted
    // reader with a different key must fail closed rather than treating an
    // opaque locator as a provider-controlled passthrough.
    let wrong_key = RecallLocatorKeyV1::from_material(vec![0x5A; 32]).unwrap();
    assert!(
        reader
            .authorize_retained_source_locator(
                &destination,
                &trace,
                7,
                0,
                &candidate_id,
                &opaque,
                &control(),
                false,
                &wrong_key,
            )
            .await
            .is_err()
    );

    let mut tampered = opaque.clone();
    tampered.source.content_sha256 = "e".repeat(64);
    assert!(
        reader
            .authorize_retained_source_locator(
                &destination,
                &trace,
                7,
                0,
                &candidate_id,
                &tampered,
                &control(),
                false,
                &RecallLocatorKeyV1::for_test(),
            )
            .await
            .is_err()
    );

    let mut missing = opaque.clone();
    missing.source_sequence = 99;
    assert!(
        reader
            .authorize_retained_source_locator(
                &destination,
                &trace,
                7,
                0,
                &candidate_id,
                &missing,
                &control(),
                false,
                &RecallLocatorKeyV1::for_test(),
            )
            .await
            .is_err()
    );

    let wrong_context = RecallControlTraceRefV1::parse(&format!(
        "recall-trace-v1:{}:{}",
        destination.exact_scope_sha256(),
        "c".repeat(64)
    ))
    .unwrap();
    assert!(
        reader
            .authorize_retained_source_locator(
                &destination,
                &wrong_context,
                7,
                0,
                &candidate_id,
                &opaque,
                &control(),
                false,
                &RecallLocatorKeyV1::for_test(),
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn branch_and_profile_changes_never_relabel_original_sources() {
    let fixture = RepositoryFixture::new();
    let bridge_a = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 1, true));
    let journal = fixture.journal();
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination_a = bridge_a.destination("session.destination").unwrap();
    let reader_a = ProviderHistoryReaderV1 {
        bridge: &bridge_a,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let mut wrong = destination_a.clone();
    wrong.profile_id = "profile.other".to_owned();
    assert!(
        reader_a
            .select_page(&wrong, 0, 1, &control())
            .await
            .is_err()
    );
    git(fixture.temp.path(), &["checkout", "-q", "-b", "ingestion"]);
    assert!(
        reader_a
            .select_page(&destination_a, 0, 1, &control())
            .await
            .is_err()
    );
    let scope_b = tracedecay_code_index_runtime::resolved_scope_for_project(
        fixture.temp.path(),
        &fixture.project,
    )
    .unwrap();
    let bridge_b = HistoryIdentityBridgeV1::admit(
        fixture.temp.path(),
        &fixture.profile,
        &scope_b,
        &StoreShardIdV1::project_sessions(
            BrainId::new("brain.history").unwrap(),
            fixture.profile.clone(),
            fixture.project.clone(),
        ),
    )
    .unwrap();
    let reader_b = ProviderHistoryReaderV1 {
        bridge: &bridge_b,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let page = reader_b
        .select_page(
            &bridge_b.destination("session.destination").unwrap(),
            0,
            1,
            &control(),
        )
        .await
        .unwrap();
    assert!(page.grant.is_none());
    assert_eq!(page.withheld, 1);
    let frozen = records.records[0]
        .repository_provenance_attachment()
        .availability()
        .value()
        .unwrap()
        .capture();
    assert_eq!(
        frozen.evidence().attached_ref().value().unwrap().as_str(),
        "refs/heads/main"
    );
}

#[tokio::test]
async fn offline_source_fence_blocks_every_destination_across_three_journal_lifetimes() {
    use tracedecay_memory_observation::ProviderSourceDeletionIntentV1;
    use tracedecay_memory_provider_registry::DeletionMode;
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 1, true));
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    let other = bridge.destination("session.other-destination").unwrap();
    let original;
    let receipt;
    {
        let journal = fixture.journal();
        let reader = ProviderHistoryReaderV1 {
            bridge: &bridge,
            observations: &records,
            dispositions: &ActiveDispositions,
            original_authority: &authority,
            journal: &journal,
            provider_id: &provider,
            policy_revision: 1,
        };
        original = reader
            .select_page(&destination, 0, 1, &control())
            .await
            .unwrap()
            .grant
            .unwrap();
        let next = reader
            .select_page(&other, 0, 1, &control())
            .await
            .unwrap()
            .grant
            .unwrap();
        let digest = original_source_fence_digest(&original.sources[0].attribution).unwrap();
        assert_eq!(
            digest,
            original_source_fence_digest(&next.sources[0].attribution).unwrap()
        );
        receipt = journal
            .record_provider_source_deletion_intent(&ProviderSourceDeletionIntentV1 {
                operation_id: "forget.original",
                provider_id: provider.as_str(),
                original_source_sha256: &digest,
                expected_fence_revision: 0,
                mode: DeletionMode::RemoveInfluence,
                source_revision: None,
                authority_ref: "host.operator.intent",
                accepted_at_utc_micros: tracedecay_contracts::now_micros().0,
            })
            .unwrap();
        assert!(!receipt.provider_erasure_verified);
    }
    for _lifetime in 2..=3 {
        let journal = fixture.journal();
        let reader = ProviderHistoryReaderV1 {
            bridge: &bridge,
            observations: &records,
            dispositions: &ActiveDispositions,
            original_authority: &authority,
            journal: &journal,
            provider_id: &provider,
            policy_revision: 1,
        };
        for target in [&destination, &other] {
            let page = reader.select_page(target, 0, 1, &control()).await.unwrap();
            assert_eq!(page.withheld, 1);
            assert!(page.grant.is_none());
        }
        assert!(
            reader
                .revalidate_grant(&original, &control())
                .await
                .is_err()
        );
        let digest = original_source_fence_digest(&original.sources[0].attribution).unwrap();
        assert_eq!(
            journal
                .read_provider_source_fence(provider.as_str(), &digest)
                .unwrap()
                .unwrap(),
            receipt.fence
        );
    }
}

struct BoundPageAuthority {
    grant: HistoryGrant,
}

struct RevocablePageAuthority {
    grant: HistoryGrant,
    active: AtomicBool,
    checks: AtomicUsize,
}

impl HistoryGrantRevalidationV1 for RevocablePageAuthority {
    fn revalidate(
        &self,
        provider: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &OperationControl,
    ) -> Result<(), ProviderHistoryErrorV1> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        if !self.active.load(Ordering::SeqCst) {
            return Err(ProviderHistoryErrorV1::Ineligible("revoked source"));
        }
        BoundPageAuthority {
            grant: self.grant.clone(),
        }
        .revalidate(provider, grant, control)
    }
    fn revalidate_async<'a>(
        &'a self,
        provider: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a OperationControl,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ProviderHistoryErrorV1>> + Send + 'a>,
    > {
        Box::pin(async move { self.revalidate(provider, grant, control) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_delivery_wait_requires_receipts_and_rechecks_live_authority() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 7, true));
    let source_authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    let journal_root = fixture.temp.path().join("wait-delivery");
    std::fs::create_dir_all(&journal_root).unwrap();
    // Admission is live but this fixture has deliberately not activated its
    // existing delivery worker, so pending cannot race into acknowledgement.
    let journey = construct_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: composition(Arc::new(JourneyNativePort::with_state_generation(1))),
        profile_id: fixture.profile.clone(),
        scope: fixture.scope.clone(),
        authoritative_project_id: fixture.project.clone(),
        provider: crate::retained_owner::native_observation_mount(&journal_root, 1)
            .unwrap(),
        store_data_root: journal_root,
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .unwrap();
    let journal = journey.history_journal();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &source_authority,
        journal: journal.as_ref(),
        provider_id: &provider,
        policy_revision: 1,
    };
    let page = reader
        .select_recent_page(&destination, 1, &control())
        .await
        .unwrap();
    let grant = page.grant.clone().unwrap();
    let authority = Arc::new(RevocablePageAuthority {
        grant: grant.clone(),
        active: AtomicBool::new(true),
        checks: AtomicUsize::new(0),
    });
    journey.bind_history_authority(authority.clone()).unwrap();
    assert!(matches!(
        journey.await_history_delivery(&grant, open_bounds()).await,
        Err(ObservationJourneyError::History(
            ProviderHistoryErrorV1::Ineligible("history source not admitted")
        ))
    ));
    journey
        .replay_authorized_history_page(page, destination, 1, open_bounds())
        .await
        .unwrap();
    let token = HostCancellationToken::new();
    let short = ReplayBoundsV1 {
        cancellation: &token,
        deadline: tokio::time::Instant::now() + Duration::from_millis(30),
    };
    assert!(matches!(
        journey.await_history_delivery(&grant, short).await,
        Err(ObservationJourneyError::DeadlineExceeded { admitted: 0 })
    ));
    let cancelled = HostCancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        journey
            .await_history_delivery(
                &grant,
                ReplayBoundsV1 {
                    cancellation: &cancelled,
                    deadline: tokio::time::Instant::now() + Duration::from_secs(5)
                }
            )
            .await,
        Err(ObservationJourneyError::Cancelled { admitted: 0 })
    ));
    // Start delivery while the wait is active. Receipt persistence publishes
    // the existing transition even if it happens before the waiter awaits it.
    let (result, ()) = tokio::join!(
        journey.await_history_delivery(&grant, open_bounds()),
        async {
            tokio::task::yield_now().await;
            journey.start_delivery_worker().unwrap();
        },
    );
    result.unwrap();
    let before = authority.checks.load(Ordering::SeqCst);
    journey
        .await_history_delivery(&grant, open_bounds())
        .await
        .unwrap();
    assert_eq!(authority.checks.load(Ordering::SeqCst), before + 2);
    authority.active.store(false, Ordering::SeqCst);
    assert!(matches!(
        journey.await_history_delivery(&grant, open_bounds()).await,
        Err(ObservationJourneyError::History(
            ProviderHistoryErrorV1::Ineligible("revoked source")
        ))
    ));
    assert!(
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_empty()
    );
}
impl HistoryGrantRevalidationV1 for BoundPageAuthority {
    fn revalidate(
        &self,
        _: &OwnedProviderId,
        grant: &HistoryGrant,
        control: &OperationControl,
    ) -> Result<(), ProviderHistoryErrorV1> {
        control
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        if grant == &self.grant {
            Ok(())
        } else {
            Err(ProviderHistoryErrorV1::ClaimMismatch("test page authority"))
        }
    }
    fn revalidate_async<'a>(
        &'a self,
        provider: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        control: &'a OperationControl,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ProviderHistoryErrorV1>> + Send + 'a>,
    > {
        Box::pin(async move { self.revalidate(provider, grant, control) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destination_history_uses_existing_journal_and_resumes_without_recopying() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 1, true));
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    let other = bridge.destination("session.other-destination").unwrap();
    let journal_root = fixture.temp.path().join("delivery");
    std::fs::create_dir_all(&journal_root).unwrap();
    let mut saved_grant = None;
    for lifetime in 1..=3 {
        let port = Arc::new(JourneyNativePort::with_state_generation(1));
        let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
            composition: composition(port),
            profile_id: fixture.profile.clone(),
            scope: fixture.scope.clone(),
            authoritative_project_id: fixture.project.clone(),
            provider: crate::retained_owner::native_observation_mount(
                &journal_root,
                1,
            )
            .unwrap(),
            store_data_root: journal_root.clone(),
            policy: ObservationJourneyPolicyV1::project_default(),
        })
        .unwrap();
        let journal = journey.history_journal();
        let reader = ProviderHistoryReaderV1 {
            bridge: &bridge,
            observations: &records,
            dispositions: &ActiveDispositions,
            original_authority: &authority,
            journal: journal.as_ref(),
            provider_id: &provider,
            policy_revision: 1,
        };
        let page = reader
            .select_page(&destination, 0, 1, &control())
            .await
            .unwrap();
        let grant = page.grant.clone().unwrap();
        if lifetime == 1 {
            saved_grant = Some(grant.clone());
        }
        journey
            .bind_history_authority(Arc::new(BoundPageAuthority {
                grant: grant.clone(),
            }))
            .unwrap();
        let cancel = HostCancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            journey
                .replay_authorized_history_page(
                    reader
                        .select_page(&destination, 0, 1, &control())
                        .await
                        .unwrap(),
                    destination.clone(),
                    1,
                    ReplayBoundsV1 {
                        cancellation: &cancel,
                        deadline: tokio::time::Instant::now() + Duration::from_secs(5)
                    }
                )
                .await,
            Err(ObservationJourneyError::Cancelled { admitted: 0 })
        ));
        assert_eq!(
            journey
                .history_replay_watermark(&destination, 1)
                .await
                .unwrap(),
            u64::from(lifetime > 1)
        );
        let pass = journey
            .replay_authorized_history_page(page, destination.clone(), 1, open_bounds())
            .await
            .unwrap();
        assert_eq!(pass.admitted, u64::from(lifetime == 1));
        assert!(pass.halted.is_none());
        journey
            .await_history_delivery(&grant, open_bounds())
            .await
            .unwrap();
        assert_eq!(
            journey
                .history_replay_watermark(&destination, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            journey.history_replay_watermark(&other, 1).await.unwrap(),
            0
        );
        assert_eq!(
            journey
                .history_replay_watermark(&destination, 2)
                .await
                .unwrap(),
            0
        );
        let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM tdmem_observation_delivery_v1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        let payload: Vec<u8> = connection
            .query_row(
                "SELECT payload_bytes FROM tdmem_observation_journal_v1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let payload: Value = serde_json::from_slice(&payload).unwrap();
        let attribution = &saved_grant.as_ref().unwrap().sources[0].attribution;
        assert_eq!(
            payload
                .pointer("/source_identity/original_source/source/canonical_session_id")
                .and_then(Value::as_str),
            Some("session.source")
        );
        assert_eq!(
            payload
                .pointer("/source_identity/original_source/source/observation_id")
                .and_then(Value::as_str),
            Some(attribution.source.observation_id.as_str())
        );
        let failures = journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        assert!(failures.is_empty(), "{failures:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_admission_without_git_keeps_exact_journal_binding_and_refuses_history() {
    use crate::retained_owner::provider_history::ProviderHistoryAuthorityV1;
    use tracedecay_memory_observation::ObservationIdempotencyKeyV1;
    use tracedecay_memory_provider_registry::{AdvisoryAdmissionAuthority, AdvisoryAdmissionError};
    let temp = TempDir::new().unwrap();
    assert!(!temp.path().join(".git").exists());
    // The already-admitted coding scope remains required. This test does not
    // derive or invent any original branch evidence from the non-Git directory.
    let project = ProjectId::new("project.ordinary-authority").unwrap();
    let profile = UserProfileId::new("profile.ordinary-authority").unwrap();
    let resolved = scope(project.clone());
    let source = SessionId::new("session.ordinary-authority").unwrap();
    let records = Arc::new(SettledRecordsPort::single(settled_record(
        1,
        canonical_observation(&project, &source, "ordinary host message"),
    )));
    let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: composition(Arc::new(JourneyNativePort::with_state_generation(1))),
        profile_id: profile.clone(),
        scope: resolved.clone(),
        authoritative_project_id: project.clone(),
        provider: crate::retained_owner::native_observation_mount(temp.path(), 1)
            .unwrap(),
        store_data_root: temp.path().to_path_buf(),
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .unwrap();
    let pass = journey
        .replay_canonical_observations(records.as_ref(), 1, open_bounds())
        .await
        .unwrap();
    assert_eq!(pass.admitted, 1);
    let journal = journey.history_journal();
    let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
    let key: String = connection
        .query_row(
            "SELECT idempotency_key FROM tdmem_observation_journal_v1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let admitted = journal
        .read_admitted_observation_by_idempotency(
            &ObservationIdempotencyKeyV1::parse(&key).unwrap(),
        )
        .unwrap()
        .unwrap();
    let authority = Arc::new(ProviderHistoryAuthorityV1 {
        mounted_scope: resolved.clone(),
        profile_id: profile.clone(),
        registered_shard: StoreShardIdV1::project_sessions(
            BrainId::new("brain.ordinary-authority").unwrap(),
            profile,
            project,
        ),
        observations: records,
        dispositions: Arc::new(ActiveDispositions),
        original_authority: None,
        journal,
        provider_id: admitted.target.provider_id.clone(),
        policy_revision: 1,
        runtime: tokio::runtime::Handle::current(),
    });
    authority.validate_mount().unwrap();
    assert!(matches!(
        authority.reader(),
        Err(ProviderHistoryErrorV1::Unavailable(
            "original repository provenance"
        ))
    ));
    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Observe,
        provider_id: admitted.target.provider_id.clone(),
        registration_revision: admitted.target.registration_revision,
        ready_receipt_sha256: admitted.target.ready_receipt_digest.clone(),
        exact_scope: admitted.exact_scope.clone(),
        request_id: admitted.observation_id.as_str().to_owned(),
        operation_id: "ordinary.dispatch.1".to_owned(),
        expected_state_generation: 1,
        idempotency_key: Some(admitted.idempotency_key.as_str().to_owned()),
        control: control(),
        payload: admitted.payload.clone(),
        required_capabilities: vec![OwnedVersionedId::new("observation.accept.v1").unwrap()],
        extensions: admitted.extensions.clone(),
    })
    .unwrap()
    .with_sanitization(
        PayloadSanitizationReceipt::from_json(&admitted.sanitization.receipt_json).unwrap(),
    );
    let valid_authority = Arc::clone(&authority);
    let valid_call = call.clone();
    let result = std::thread::spawn(move || valid_authority.admit(&valid_call))
        .join()
        .unwrap()
        .unwrap();
    result.verify_for(&call).unwrap();
    assert!(result.history_sources.is_empty());
    for mutation in [0, 1, 2] {
        let mut changed = call.clone();
        match mutation {
            0 => changed.idempotency_key = Some("f".repeat(64)),
            1 => changed.request_id.push_str(".other"),
            _ => changed.exact_scope.agent_session_id.push_str(".other"),
        }
        let authority = Arc::clone(&authority);
        assert!(
            std::thread::spawn(move || authority.admit(&changed))
                .join()
                .unwrap()
                .is_err()
        );
    }
    let mut restore = call;
    restore.operation = ProviderOperation::SnapshotRestore;
    restore
        .required_capabilities
        .insert(OwnedVersionedId::new("snapshot.restore.v1").unwrap());
    restore.payload = CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.provider.snapshot-restore.v1").unwrap(),
        b"{}".to_vec(),
        tracedecay_domain::canonical_text::sha256_hex(b"{}"),
    )
    .unwrap();
    let result = std::thread::spawn(move || authority.admit(&restore))
        .join()
        .unwrap();
    assert!(matches!(
        result,
        Err(AdvisoryAdmissionError::Unavailable(
            "original repository provenance"
        ))
    ));
    assert!(
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_empty()
    );
}

struct CurrentDisposition(std::sync::RwLock<Option<RetrievalAnchorDispositionRecordV1>>);
impl RetrievalAnchorDispositionStore for CurrentDisposition {
    async fn append_disposition(
        &self,
        record: RetrievalAnchorDispositionRecordV1,
    ) -> RetrievalAnchorStoreResult<AnchorDispositionAppendOutcomeV1> {
        ActiveDispositions.append_disposition(record).await
    }
    async fn publish_derivative(
        &self,
        derivative: RetrievalAnchorDerivativeV1,
    ) -> RetrievalAnchorStoreResult<AnchorDispositionAppendOutcomeV1> {
        ActiveDispositions.publish_derivative(derivative).await
    }
    async fn current_disposition(
        &self,
        anchor: &RetrievalAnchorId,
        owner: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorDispositionRecordV1>> {
        let current = self.0.read().unwrap().clone();
        if let Some(record) = &current {
            assert_eq!(record.anchor_id(), anchor);
            assert_eq!(record.owner(), owner);
        }
        Ok(current)
    }
    async fn tombstone(
        &self,
        anchor: &RetrievalAnchorId,
        owner: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Option<RetrievalAnchorTombstoneV1>> {
        ActiveDispositions.tombstone(anchor, owner).await
    }
    async fn derivatives(
        &self,
        anchor: &RetrievalAnchorId,
        owner: &RetrievalAnchorOwnerV1,
    ) -> RetrievalAnchorStoreResult<Vec<RetrievalAnchorDerivativeV1>> {
        ActiveDispositions.derivatives(anchor, owner).await
    }
}

#[tokio::test]
async fn retained_superseded_source_passes_selection_and_refresh_until_privacy_blocks_it() {
    use tracedecay_domain::FactOwnerV1;
    use tracedecay_memory_provider_registry::{RecordedValidity, SourceDisposition};
    use tracedecay_store::{AnchorDispositionReasonClassV1, AnchorDispositionStateV1};
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let original = fixture.record("session.source", 1, true);
    let successor = fixture.record("session.source", 2, true);
    let dispositions = CurrentDisposition(std::sync::RwLock::new(None));
    let records = SettledRecordsPort::single(original.clone());
    let journal = fixture.journal();
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &dispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let destination = bridge.destination("session.destination").unwrap();
    let available = reader
        .select_page(&destination, 0, 1, &control())
        .await
        .unwrap()
        .grant
        .unwrap();
    let owner = RetrievalAnchorOwnerV1::V2(FactOwnerV1::Project {
        project_id: fixture.project.clone(),
    });
    *dispositions.0.write().unwrap() = Some(
        RetrievalAnchorDispositionRecordV1::new(
            "disposition.corrected",
            original.retrieval_anchor_id().clone(),
            owner.clone(),
            AnchorDispositionStateV1::Superseded,
            Some(successor.retrieval_anchor_id().clone()),
            AnchorDispositionReasonClassV1::Correction,
            UtcMicros(1_760_000_000_000_000),
        )
        .unwrap(),
    );
    let page = reader
        .select_page(&destination, 0, 1, &control())
        .await
        .unwrap();
    assert_eq!(page.withheld, 0);
    let superseded = page.grant.unwrap();
    assert_eq!(
        superseded.sources[0].current_disposition.state,
        SourceDisposition::Superseded
    );
    assert_eq!(
        superseded.sources[0].attribution,
        available.sources[0].attribution
    );
    // The current lifecycle state does not become an invented validity event.
    assert_eq!(
        superseded.sources[0].attribution.validity,
        RecordedValidity::default()
    );
    let refreshed = reader
        .revalidate_grant(&available, &control())
        .await
        .unwrap();
    assert_eq!(
        refreshed.sources[0].current_disposition.state,
        SourceDisposition::Superseded
    );
    assert_eq!(
        refreshed.sources[0].attribution,
        available.sources[0].attribution
    );
    for state in [
        AnchorDispositionStateV1::Deleted,
        AnchorDispositionStateV1::Redacted,
        AnchorDispositionStateV1::Expired,
        AnchorDispositionStateV1::Unavailable,
    ] {
        *dispositions.0.write().unwrap() = Some(
            RetrievalAnchorDispositionRecordV1::new(
                format!("disposition.{}", state.as_str()),
                original.retrieval_anchor_id().clone(),
                owner.clone(),
                state,
                None,
                AnchorDispositionReasonClassV1::UserRequest,
                UtcMicros(1_770_000_000_000_000),
            )
            .unwrap(),
        );
        let page = reader
            .select_page(&destination, 0, 1, &control())
            .await
            .unwrap();
        assert_eq!(page.withheld, 1, "{state:?}");
        assert!(page.grant.is_none(), "{state:?}");
        assert!(
            matches!(
                reader.revalidate_grant(&superseded, &control()).await,
                Err(ProviderHistoryErrorV1::Ineligible(
                    "current source disposition"
                ))
            ),
            "{state:?}"
        );
    }
}

#[tokio::test]
async fn recent_history_uses_newest_canonical_rows_without_changing_startup_replay() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort {
        records: vec![
            fixture.record("session.source", 1, true),
            fixture.record("session.source", 7, true),
            fixture.record("session.legacy", 90, false),
            fixture.record("session.source", 300, true),
        ],
    };
    let journal = fixture.journal();
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &authority,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let destination = bridge.destination("session.destination").unwrap();
    let startup = reader
        .select_page(&destination, 0, 2, &control())
        .await
        .unwrap();
    assert_eq!(
        startup
            .records
            .iter()
            .map(StoredObservation::sequence)
            .collect::<Vec<_>>(),
        vec![1, 7]
    );
    assert!(startup.has_more);
    let recent = reader
        .select_recent_page(&destination, 2, &control())
        .await
        .unwrap();
    assert_eq!(
        recent
            .records
            .iter()
            .map(StoredObservation::sequence)
            .collect::<Vec<_>>(),
        vec![90, 300]
    );
    assert_eq!(
        (
            recent.scanned,
            recent.withheld,
            recent.unknown_revision,
            recent.has_older,
            recent.has_more
        ),
        (2, 1, 1, true, false)
    );
    let grant = recent.grant.unwrap();
    assert_eq!(grant.sources.len(), 1);
    assert_eq!(grant.sources[0].attribution.source_sequence, 300);
    assert!(
        reader
            .select_recent_page(&destination, 257, &control())
            .await
            .is_err()
    );
}

struct AppendingWindowPort {
    records: Mutex<Vec<StoredObservation>>,
    append_after_window: Mutex<Option<StoredObservation>>,
}
impl ObservationAdmissionPort for AppendingWindowPort {
    async fn read_admitted_observation(
        &self,
        id: &CanonicalObservationIdV1,
    ) -> Result<Option<StoredObservation>, ObservationStoreError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|stored| stored.observation().observation_id() == id)
            .cloned())
    }
    async fn replay_admitted_observations(
        &self,
        request: ObservationReplayRequest,
    ) -> Result<Vec<StoredObservation>, ObservationStoreError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|stored| stored.sequence() > request.after_sequence())
            .take(request.limit())
            .cloned()
            .collect())
    }
    async fn recent_admitted_observation_window(
        &self,
        request: tracedecay_store::ObservationRecentWindowRequest,
    ) -> Result<Option<tracedecay_store::ObservationRecentWindowV1>, ObservationStoreError> {
        let window = recent_record_window(&self.records.lock().unwrap(), request)?;
        if let Some(inserted) = self.append_after_window.lock().unwrap().take() {
            self.records.lock().unwrap().push(inserted);
        }
        Ok(window)
    }
}

#[tokio::test]
async fn recent_history_excludes_inserts_above_the_frozen_frontier_before_counting_or_granting() {
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let journal = fixture.journal();
    let authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    for initially_empty in [false, true] {
        let records = AppendingWindowPort {
            records: Mutex::new(if initially_empty {
                Vec::new()
            } else {
                vec![
                    fixture.record("session.source", 7, true),
                    fixture.record("session.source", 90, true),
                ]
            }),
            append_after_window: Mutex::new(Some(fixture.record("session.legacy-new", 999, false))),
        };
        let reader = ProviderHistoryReaderV1 {
            bridge: &bridge,
            observations: &records,
            dispositions: &ActiveDispositions,
            original_authority: &authority,
            journal: &journal,
            provider_id: &provider,
            policy_revision: 1,
        };
        let recent = reader
            .select_recent_page(&destination, 3, &control())
            .await
            .unwrap();
        assert_eq!(recent.scanned, if initially_empty { 0 } else { 2 });
        assert_eq!(recent.withheld, 0);
        assert_eq!(recent.records.len(), recent.scanned);
        assert!(recent.records.iter().all(|stored| stored.sequence() <= 90));
        assert!(recent.grant.as_ref().is_none_or(|grant| {
            grant
                .sources
                .iter()
                .all(|source| source.attribution.source_sequence <= 90)
        }));
        assert!(!recent.has_more);
        assert!(!recent.has_older);
        // A new request captures a new canonical frontier and truthfully counts
        // the newly admitted legacy source as unavailable original provenance.
        let next = reader
            .select_recent_page(&destination, 3, &control())
            .await
            .unwrap();
        assert_eq!(next.scanned, if initially_empty { 1 } else { 3 });
        assert_eq!(next.withheld, 1);
        assert_eq!(next.last_scanned_sequence, 999);
    }
}

struct FreshSelectedPagesAuthority {
    provider: OwnedProviderId,
    grants: Mutex<Vec<HistoryGrant>>,
}

impl HistoryGrantRevalidationV1 for FreshSelectedPagesAuthority {
    fn revalidate(
        &self,
        provider: &OwnedProviderId,
        grant: &HistoryGrant,
        operation: &OperationControl,
    ) -> Result<(), ProviderHistoryErrorV1> {
        operation
            .snapshot()
            .map_err(ProviderHistoryErrorV1::Control)?;
        if provider == &self.provider && self.grants.lock().unwrap().contains(grant) {
            Ok(())
        } else {
            Err(ProviderHistoryErrorV1::ClaimMismatch(
                "fresh selected test page",
            ))
        }
    }

    fn revalidate_async<'a>(
        &'a self,
        provider: &'a OwnedProviderId,
        grant: &'a HistoryGrant,
        operation: &'a OperationControl,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), ProviderHistoryErrorV1>> + Send + 'a>,
    > {
        Box::pin(async move { self.revalidate(provider, grant, operation) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independently_selected_fresh_grants_preserve_durable_delivery_identity() {
    use tracedecay_memory_observation::{
        ExpectedSourceDeliveryV1, RecoveryTimeBudgetV1, SourceDeliveryEvidenceV1,
    };
    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 1, true));
    let original_authority = OriginalReceipts::new();
    let provider = OwnedProviderId::new("tracedecay.native").unwrap();
    let destination = bridge.destination("session.destination").unwrap();
    let journal_root = fixture.temp.path().join("fresh-delivery");
    std::fs::create_dir_all(&journal_root).unwrap();
    let journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: composition(Arc::new(JourneyNativePort::with_state_generation(1))),
        profile_id: fixture.profile.clone(),
        scope: fixture.scope.clone(),
        authoritative_project_id: fixture.project.clone(),
        provider: crate::retained_owner::native_observation_mount(&journal_root, 1)
            .unwrap(),
        store_data_root: journal_root,
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .unwrap();
    let journal = journey.history_journal();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &original_authority,
        journal: journal.as_ref(),
        provider_id: &provider,
        policy_revision: 1,
    };
    let first = reader
        .select_recent_page(&destination, 1, &control())
        .await
        .unwrap();
    let first_grant = first.grant.clone().unwrap();
    let selected_authority = Arc::new(FreshSelectedPagesAuthority {
        provider: provider.clone(),
        grants: Mutex::new(vec![first_grant.clone()]),
    });
    journey
        .bind_history_authority(selected_authority.clone())
        .unwrap();
    assert_eq!(
        journey
            .replay_authorized_history_page(first, destination.clone(), 1, open_bounds())
            .await
            .unwrap()
            .admitted,
        1
    );
    journey
        .await_history_delivery(&first_grant, open_bounds())
        .await
        .unwrap();

    let source = &first_grant.sources[0].attribution;
    let lane = journey.adapter.context.provider_lane.clone();
    let stream = SourceStreamKeyV1 {
        source_authority: SourceAuthorityV1::HostSession,
        exact_scope_sha256: destination.exact_scope_sha256(),
        source_stream: history_source_stream(&destination, 1).unwrap(),
    };
    let expected = [ExpectedSourceDeliveryV1 {
        source_sequence: SourceSequenceV1(source.source_sequence),
        source_event_id: source.source.observation_id.clone(),
    }];
    let snapshot = || {
        journal
            .read_source_deliveries(
                &lane,
                &destination,
                &stream,
                &expected,
                RecoveryTimeBudgetV1 {
                    remaining_micros: 1_000_000,
                },
                &CancellationToken::new(),
            )
            .unwrap()
    };
    let before = snapshot();
    let SourceDeliveryEvidenceV1::Retained {
        admitted,
        receipt: Some(receipt),
        ..
    } = &before[0]
    else {
        panic!("history wait returned without a durable admitted delivery receipt");
    };
    assert_eq!(receipt.observation_id, admitted.observation_id);
    assert_eq!(receipt.idempotency_key, admitted.idempotency_key);
    assert!(!admitted.payload.bytes.is_empty());

    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = reader
        .select_recent_page(&destination, 1, &control())
        .await
        .unwrap();
    let fresh_grant = second.grant.clone().unwrap();
    assert_ne!(
        first_grant.disposition_checkpoint.checked_at_utc_nanos,
        fresh_grant.disposition_checkpoint.checked_at_utc_nanos
    );
    assert_ne!(
        first_grant.sources[0]
            .current_disposition
            .checked_at_utc_nanos,
        fresh_grant.sources[0]
            .current_disposition
            .checked_at_utc_nanos
    );
    assert_eq!(
        first_grant.sources[0].attribution,
        fresh_grant.sources[0].attribution
    );
    assert_ne!(
        history_grant_json(&first_grant).unwrap(),
        history_grant_json(&fresh_grant).unwrap()
    );
    selected_authority
        .grants
        .lock()
        .unwrap()
        .push(fresh_grant.clone());
    let replay = journey
        .replay_authorized_history_page(second, destination.clone(), 1, open_bounds())
        .await
        .unwrap();
    assert_eq!(replay.admitted, 0);
    assert!(replay.halted.is_none());
    assert!(replay.shed.is_none());
    journey
        .await_history_delivery(&fresh_grant, open_bounds())
        .await
        .unwrap();

    // The supported strict journal API returns every admitted field and the full
    // provider receipt: equality includes IDs, keys, payload bytes and attempts.
    assert_eq!(snapshot(), before);
    assert!(
        journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn keyed_control_authorization_rechecks_source_and_allows_only_fresh_cleanup() {
    use tracedecay_memory_observation::ProviderSourceDeletionIntentV1;
    use tracedecay_memory_provider_registry::{DeletionMode, SourceDisposition};

    let fixture = RepositoryFixture::new();
    let bridge = fixture.bridge();
    let records = SettledRecordsPort::single(fixture.record("session.source", 1, true));
    let journal = fixture.journal();
    let original = OriginalReceipts::new();
    // No NCM provider, registration, readiness owner or delivery worker exists.
    let provider = OwnedProviderId::new("ncm").unwrap();
    let reader = ProviderHistoryReaderV1 {
        bridge: &bridge,
        observations: &records,
        dispositions: &ActiveDispositions,
        original_authority: &original,
        journal: &journal,
        provider_id: &provider,
        policy_revision: 1,
    };
    let destination = bridge.destination("session.destination").unwrap();
    let selected = reader
        .select_recent_page(&destination, 1, &control())
        .await
        .unwrap();
    let expected = selected.grant.unwrap().sources.remove(0).attribution;
    let current = reader
        .authorize_retained_source(&destination, &expected, &control(), false)
        .await
        .unwrap();
    assert_eq!(current.sources.len(), 1);
    assert_eq!(current.sources[0].attribution, expected);
    assert_eq!(
        current.sources[0].current_disposition.state,
        SourceDisposition::Available
    );

    let key = original_source_fence_digest(&expected).unwrap();
    journal
        .record_provider_source_deletion_intent(&ProviderSourceDeletionIntentV1 {
            operation_id: "test.offline-existing-fence",
            provider_id: provider.as_str(),
            original_source_sha256: &key,
            expected_fence_revision: 0,
            mode: DeletionMode::HardDelete,
            source_revision: expected.source.source_revision.as_deref(),
            authority_ref: "test.original-host-intent",
            accepted_at_utc_micros: tracedecay_contracts::now_micros().0,
        })
        .unwrap();
    assert!(matches!(
        reader
            .authorize_retained_source(&destination, &expected, &control(), false)
            .await,
        Err(ProviderHistoryErrorV1::Ineligible(
            "current source disposition"
        ))
    ));
    let cleanup = reader
        .authorize_retained_source(&destination, &expected, &control(), true)
        .await
        .unwrap();
    assert_eq!(
        cleanup.sources[0].current_disposition.state,
        SourceDisposition::Deleted
    );
    assert_eq!(cleanup.sources[0].attribution, expected);
    assert_eq!(cleanup.destination_scope, destination);

    let mut wrong = expected.clone();
    wrong.source.content_sha256 = "f".repeat(64);
    assert!(matches!(
        reader
            .authorize_retained_source(&destination, &wrong, &control(), true)
            .await,
        Err(ProviderHistoryErrorV1::ClaimMismatch(
            "original source attribution"
        ))
    ));
    wrong = expected.clone();
    wrong.source.observation_id = format!("sha256:{}", "f".repeat(64));
    assert_ne!(wrong.source.observation_id, expected.source.observation_id);
    assert!(matches!(
        reader
            .authorize_retained_source(&destination, &wrong, &control(), true)
            .await,
        Err(ProviderHistoryErrorV1::Ineligible(
            "canonical observation missing"
        ))
    ));
    for field in 0..6 {
        let mut changed = destination.clone();
        match field {
            0 => changed.profile_id = "profile.other".to_owned(),
            1 => changed.project_id = "project.other".to_owned(),
            2 => changed.repository_identity = "repository.other".to_owned(),
            3 => changed.worktree_identity = "worktree.other".to_owned(),
            4 => changed.branch_identity = "branch.other".to_owned(),
            5 => changed.resolved_scope_digest = format!("sha256:{}", "f".repeat(64)),
            _ => unreachable!(),
        }
        assert!(
            bridge
                .authorize_control_scope(&changed, &control())
                .is_err(),
            "{field}"
        );
    }
    original.active.store(false, Ordering::SeqCst);
    assert!(
        reader
            .authorize_retained_source(&destination, &expected, &control(), true)
            .await
            .is_err()
    );
}

const SWITCH_NATIVE_JOURNAL: &str = "memory-observation-switch-native-v1.sqlite3";
const SWITCH_NCM_JOURNAL: &str = "memory-observation-switch-ncm-v2.sqlite3";
const SWITCH_NATIVE_ROLLBACK_JOURNAL: &str = "memory-observation-switch-native-rollback-v3.sqlite3";

#[derive(Clone, Debug, Eq, PartialEq)]
struct SwitchObservedCall {
    idempotency_key: String,
    exact_scope_sha256: String,
    payload_sha256: String,
}

/// A deterministic in-process NCM-shaped adapter for the product journey.
///
/// The real NCM acceptance path is intentionally ignored because it requires a
/// model runtime. This double still goes through the same injected active
/// registration, readiness handshake, observation journal, and provider
/// dispatch route as that path, so the normal suite can prove the transition
/// protocol without depending on a model or a worker process.
struct SwitchNcmProvider {
    descriptor: ProviderDescriptor,
    handshakes: AtomicUsize,
    observations: Mutex<Vec<SwitchObservedCall>>,
}

impl SwitchNcmProvider {
    fn new() -> Arc<Self> {
        let capabilities = std::iter::once(COMMON_ADVISORY_PROFILE_ID)
            .chain(COMMON_ADVISORY_REQUIRED_CAPABILITIES.iter().copied())
            .map(|capability| OwnedVersionedId::new(capability).expect("capability"));
        let descriptor = ProviderDescriptor::new(
            OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider identity"),
            "4".repeat(64),
            "switch-ncm-test-v1",
            0,
            capabilities,
            crate::retained_owner::native_provider::native_provider_limits(),
        )
        .expect("NCM test descriptor");
        Arc::new(Self {
            descriptor,
            handshakes: AtomicUsize::new(0),
            observations: Mutex::new(Vec::new()),
        })
    }

    fn observation_calls(&self) -> Vec<SwitchObservedCall> {
        self.observations.lock().unwrap().clone()
    }
}

impl MemoryProviderV1 for SwitchNcmProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    fn handshake(&self, request: &HandshakeRequest) -> HandshakeResponse {
        self.handshakes.fetch_add(1, Ordering::AcqRel);
        HandshakeResponse {
            terminal: TerminalRecord::new(
                ProviderOperation::Handshake,
                self.descriptor.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::none(Some(self.descriptor.state_generation)),
                FallbackDirective::forbidden(),
                request.request_id.clone(),
                request.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("NCM handshake terminal"),
            descriptor: Some(self.descriptor.clone()),
            provider_instance_id: Some("ncm.switch-test".to_owned()),
            state_namespace: Some(NCM_PROVIDER_ID.to_owned()),
            accepted_scope: Some(request.exact_scope.clone()),
            effective_limits: Some(request.host_limits.minimum(self.descriptor.limits)),
            ready_receipt_sha256: Some(READY_RECEIPT.to_owned()),
            warnings: Vec::new(),
        }
    }

    fn invoke(&self, call: &ProviderCall) -> ProviderReply {
        if call.operation == ProviderOperation::Observe {
            self.observations.lock().unwrap().push(SwitchObservedCall {
                idempotency_key: call
                    .idempotency_key
                    .clone()
                    .expect("observation idempotency key"),
                exact_scope_sha256: call.exact_scope.exact_scope_sha256(),
                payload_sha256: call.payload.sha256.clone(),
            });
        }
        let terminal = if call.operation == ProviderOperation::Observe {
            TerminalRecord::new(
                ProviderOperation::Observe,
                call.provider_id.clone(),
                TerminalCode::Success,
                CommittedEffectEvidence::committed(
                    call.expected_state_generation,
                    call.expected_state_generation,
                    vec!["observation:switch-ncm-test".to_owned()],
                    PROVIDER_RECEIPT,
                    EFFECT_DIGEST,
                )
                .expect("NCM committed effect"),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("NCM observation terminal")
        } else {
            TerminalRecord::new(
                call.operation,
                call.provider_id.clone(),
                TerminalCode::SuccessZeroResults,
                CommittedEffectEvidence::none(Some(call.expected_state_generation)),
                FallbackDirective::forbidden(),
                call.operation_id.clone(),
                call.exact_scope.exact_scope_sha256(),
                None,
            )
            .expect("NCM control terminal")
        };
        ProviderReply {
            terminal,
            payload: (call.operation == ProviderOperation::Observe).then(|| call.payload.clone()),
            warnings: Vec::new(),
            extensions: call.extensions.clone(),
            state_generation: call.expected_state_generation,
        }
    }
}

fn switch_fabric_config() -> FabricConfig {
    FabricConfig {
        max_registered_providers: 1,
        max_in_flight: 1,
    }
}

fn switch_native_composition(
    port: Arc<dyn NativeMemoryApplicationPort>,
    registration_revision: u64,
) -> Arc<ProjectMemoryProviderComposition> {
    Arc::new(
        ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
            fabric_config: switch_fabric_config(),
            port,
            registration_revision,
            mode: EnabledProviderMode::Active,
        })
        .expect("Native active composition"),
    )
}

fn switch_ncm_composition(
    provider: Arc<SwitchNcmProvider>,
    registration_revision: u64,
) -> Arc<ProjectMemoryProviderComposition> {
    let provider_id = OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider identity");
    let registration = ProviderRegistrationV1 {
        provider_id,
        provider,
        registration_revision,
        mode: EnabledProviderMode::Active,
        execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
        recall_scope_bindings: RecallScopeBindingsV1::from_wire(["exact_coding_scope"])
            .expect("NCM recall binding"),
        lifecycle: ProviderLifecycleOwnershipV1::CompositionBound,
    };
    Arc::new(
        ProjectMemoryProviderComposition::compose_registered(
            SelectedProviderActivationV1::Injected {
                fabric_config: switch_fabric_config(),
                registration,
            },
            Vec::new(),
        )
        .expect("NCM active composition"),
    )
}

fn switch_native_mount(
    data_root: &Path,
    registration_revision: u64,
    journal_file_name: &'static str,
) -> ObservationProviderMountV1 {
    ObservationProviderMountV1 {
        provider_id: OwnedProviderId::new(tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID)
            .expect("Native provider identity"),
        registration_revision,
        provider_instance_id: Some(
            crate::retained_owner::native_provider::PROVIDER_INSTANCE_ID.to_owned(),
        ),
        instance_proof: None,
        host_limits: crate::retained_owner::native_provider::native_provider_limits(),
        state_root: data_root
            .join(crate::retained_owner::observation_journey::PROVIDER_STATE_DIR_NAME)
            .join("native"),
        journal_file_name,
        state_namespace_policy: ObservationStateNamespacePolicyV1::Prefix(
            tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID.to_owned(),
        ),
    }
}

fn switch_ncm_mount(data_root: &Path, registration_revision: u64) -> ObservationProviderMountV1 {
    ObservationProviderMountV1 {
        provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).expect("NCM provider identity"),
        registration_revision,
        provider_instance_id: Some("ncm.switch-test".to_owned()),
        instance_proof: None,
        host_limits: crate::retained_owner::native_provider::native_provider_limits(),
        state_root: data_root
            .join(crate::retained_owner::observation_journey::PROVIDER_STATE_DIR_NAME)
            .join(NCM_PROVIDER_ID),
        journal_file_name: SWITCH_NCM_JOURNAL,
        state_namespace_policy: ObservationStateNamespacePolicyV1::Prefix(
            NCM_PROVIDER_ID.to_owned(),
        ),
    }
}

fn switch_assert_active(
    composition: &ProjectMemoryProviderComposition,
    provider_id: &str,
    registration_revision: u64,
) {
    let registration = composition
        .registry()
        .expect("enabled provider composition")
        .selected_registration()
        .expect("selected active registration");
    assert_eq!(registration.provider_id.as_str(), provider_id);
    assert_eq!(registration.registration_revision, registration_revision);
    assert_eq!(registration.mode, EnabledProviderMode::Active);
}

async fn switch_persist_position(
    store: &impl ObservationStore,
    observation: DurableObservationV1,
    position: u64,
) {
    let anchored = anchored_write(observation);
    let expected_cursor = store
        .get_source_cursor(
            anchored.observation().source(),
            anchored.observation().scope(),
        )
        .await
        .expect("read canonical source cursor");
    assert_eq!(
        expected_cursor
            .as_ref()
            .map(ObservationSourceCursorV1::position),
        (position > 0).then_some(position),
        "canonical source cursor must advance exactly once per committed event",
    );
    let write = ObservationWrite::new(
        anchored.observation().clone(),
        expected_cursor,
        anchored.next_cursor().clone(),
    )
    .expect("source cursor transition");
    let write = AnchoredObservationWrite::new(
        write,
        anchored.retrieval_anchor().clone(),
        anchored.projection_generation().clone(),
    )
    .expect("anchored canonical observation");
    store
        .persist_observation(write)
        .await
        .expect("persist canonical observation");
}

fn switch_git_state(root: &Path) -> (String, String) {
    let git = tracedecay_runtime_core::git::try_git_program().expect("git");
    let branch = std::process::Command::new(&git)
        .args(["symbolic-ref", "--short", "HEAD"])
        .current_dir(root)
        .output()
        .expect("read git branch");
    assert!(branch.status.success(), "git branch read failed");
    let head = std::process::Command::new(&git)
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .expect("read git head");
    assert!(head.status.success(), "git head read failed");
    (
        String::from_utf8(branch.stdout)
            .expect("branch UTF-8")
            .trim()
            .to_owned(),
        String::from_utf8(head.stdout)
            .expect("head UTF-8")
            .trim()
            .to_owned(),
    )
}

fn switch_journal_delivery_rows(path: &Path) -> (String, i64, Vec<String>) {
    let connection = rusqlite::Connection::open(path).expect("switch journal");
    let (provider_id, registration_revision) = connection
        .query_row(
            "SELECT provider_id, registration_revision \
             FROM tdmem_observation_delivery_v1 LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .expect("provider lane row");
    let mut statement = connection
        .prepare(
            "SELECT idempotency_key FROM tdmem_observation_delivery_v1 \
             ORDER BY idempotency_key",
        )
        .expect("delivery keys");
    let keys = statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("delivery key rows")
        .flatten()
        .collect();
    (provider_id, registration_revision, keys)
}

async fn switch_replay_and_assert(
    journey: &ProjectObservationJourneyV1,
    store: &GlobalDbObservationStore,
    expected_source_event_ids: &[String],
    expected_admitted: u64,
) {
    let pass = run_startup_replay(journey, store, &HostCancellationToken::new())
        .await
        .expect("startup canonical replay");
    assert_eq!(pass.admitted, expected_admitted);
    journey.wake_delivery();
    let rows = wait_for_deliveries(journey.journal_path(), expected_source_event_ids.len()).await;
    let actual_source_event_ids = rows
        .iter()
        .map(|(source_event_id, _, _)| source_event_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(actual_source_event_ids, expected_source_event_ids);
    assert!(
        rows.iter()
            .all(|(_, state, attempts)| state == "acknowledged" && *attempts == 1)
    );
}

fn switch_sorted_ids(observations: &[DurableObservationV1]) -> Vec<String> {
    let mut ids = observations
        .iter()
        .map(|observation| observation.observation_id().as_str().to_owned())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_ncm_native_active_rollback_preserves_canonical_history_and_source_control() {
    let temp = TempDir::new().expect("temporary provider-switch root");
    let profile_root = temp.path().join("profile");
    let project_root = temp.path().join("project");
    std::fs::create_dir_all(&project_root).expect("project root");
    git(&project_root, &["init", "-q", "-b", "observation-journey"]);
    git(
        &project_root,
        &["config", "user.name", "Provider Switch Test"],
    );
    git(
        &project_root,
        &["config", "user.email", "provider-switch@example.invalid"],
    );
    std::fs::write(project_root.join("tracked"), "source-control-continuity").expect("tracked");
    git(&project_root, &["add", "tracked"]);
    git(
        &project_root,
        &["commit", "-q", "-m", "provider switch baseline"],
    );
    let source_control_before = switch_git_state(&project_root);

    let project_id = ProjectId::new("project.native-ncm-native-rollback").expect("project id");
    let runtime =
        HostAdmissionTestRuntimeV1::project(&profile_root, &project_root, project_id.clone())
            .await
            .expect("registered project database");
    let store = runtime
        .registered_database_arc(HostAdmissionScope::Project)
        .expect("project database")
        .observation_store();
    let profile_id = UserProfileId::new("profile.native-ncm-native-rollback").expect("profile");
    let resolved_scope = scope(project_id.clone());
    let journey_root = temp.path().join("journey");
    std::fs::create_dir_all(&journey_root).expect("journey root");
    let session_id = SessionId::new("session.native-ncm-native-rollback").expect("session");
    let observations = (0..=2)
        .map(|position| {
            canonical_observation_at(
                &project_id,
                &session_id,
                match position {
                    0 => "native active history",
                    1 => "NCM active history",
                    _ => "native rollback history",
                },
                position,
            )
        })
        .collect::<Vec<_>>();
    let canonical_source_event_ids = observations
        .iter()
        .map(|observation| observation.observation_id().as_str().to_owned())
        .collect::<Vec<_>>();
    let expected_native_history = switch_sorted_ids(&observations[..1]);
    let expected_ncm_history = switch_sorted_ids(&observations[..2]);
    let expected_source_event_ids = switch_sorted_ids(&observations);

    // Native active revision 1 receives the first canonical event, then the
    // same mounted journey receives a second event before the first restart.
    switch_persist_position(&store, observations[0].clone(), 0).await;
    let native_first_port = Arc::new(JourneyNativePort::new());
    let native_first_composition = switch_native_composition(
        Arc::clone(&native_first_port) as Arc<dyn NativeMemoryApplicationPort>,
        1,
    );
    switch_assert_active(native_first_composition.as_ref(), "tracedecay.native", 1);
    let native_first_journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: Arc::clone(&native_first_composition),
        profile_id: profile_id.clone(),
        scope: resolved_scope.clone(),
        authoritative_project_id: project_id.clone(),
        store_data_root: journey_root.clone(),
        provider: switch_native_mount(&journey_root, 1, SWITCH_NATIVE_JOURNAL),
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .expect("Native active journey");
    switch_replay_and_assert(
        native_first_journey.as_ref(),
        &store,
        &expected_native_history,
        1,
    )
    .await;
    assert_eq!(native_first_port.observe_calls.load(Ordering::Acquire), 1);
    assert_eq!(switch_git_state(&project_root), source_control_before);

    switch_persist_position(&store, observations[1].clone(), 1).await;
    let second_pass = native_first_journey
        .replay_canonical_observations(&store, REPLAY_LIVE_PAGES, open_bounds())
        .await
        .expect("Native live replay");
    assert_eq!(second_pass.admitted, 1);
    native_first_journey.wake_delivery();
    let native_rows = wait_for_deliveries(native_first_journey.journal_path(), 2).await;
    assert_eq!(
        native_rows
            .iter()
            .map(|(source_event_id, _, _)| source_event_id.clone())
            .collect::<Vec<_>>(),
        expected_ncm_history
    );
    assert_eq!(native_first_port.observe_calls.load(Ordering::Acquire), 2);
    assert_eq!(switch_git_state(&project_root), source_control_before);
    assert!(
        native_first_journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .is_empty()
    );
    drop(native_first_journey);
    drop(native_first_composition);

    // NCM active revision 2 starts from the canonical history and receives a
    // new event while active. Its second mount below is the restart boundary
    // that must not re-deliver any of the three already settled events.
    let ncm_provider = SwitchNcmProvider::new();
    let ncm_composition = switch_ncm_composition(Arc::clone(&ncm_provider), 2);
    switch_assert_active(ncm_composition.as_ref(), NCM_PROVIDER_ID, 2);
    let ncm_journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: Arc::clone(&ncm_composition),
        profile_id: profile_id.clone(),
        scope: resolved_scope.clone(),
        authoritative_project_id: project_id.clone(),
        store_data_root: journey_root.clone(),
        provider: switch_ncm_mount(&journey_root, 2),
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .expect("NCM active journey");
    switch_replay_and_assert(ncm_journey.as_ref(), &store, &expected_ncm_history, 2).await;
    switch_persist_position(&store, observations[2].clone(), 2).await;
    let ncm_tail = ncm_journey
        .replay_canonical_observations(&store, REPLAY_LIVE_PAGES, open_bounds())
        .await
        .expect("NCM live replay");
    assert_eq!(ncm_tail.admitted, 1);
    ncm_journey.wake_delivery();
    let ncm_rows = wait_for_deliveries(ncm_journey.journal_path(), 3).await;
    assert_eq!(
        ncm_rows
            .iter()
            .map(|(source_event_id, _, _)| source_event_id.clone())
            .collect::<Vec<_>>(),
        expected_source_event_ids
    );
    let ncm_calls_before_restart = ncm_provider.observation_calls();
    assert_eq!(ncm_calls_before_restart.len(), 3);
    let ncm_handshakes_before_restart = ncm_provider.handshakes.load(Ordering::Acquire);
    assert!(ncm_handshakes_before_restart > 0);
    let expected_exact_scope =
        exact_scope_for_session(&profile_id, &resolved_scope, session_id.as_str())
            .expect("canonical exact scope");
    assert_eq!(
        ncm_calls_before_restart
            .iter()
            .map(|call| call.exact_scope_sha256.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([expected_exact_scope.exact_scope_sha256()]),
        "the active NCM route must retain the canonical exact coding scope"
    );
    assert!(
        ncm_calls_before_restart
            .iter()
            .all(|call| call.payload_sha256.len() == 64)
    );
    assert_eq!(switch_git_state(&project_root), source_control_before);
    assert!(
        ncm_journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .is_empty()
    );
    drop(ncm_journey);
    drop(ncm_composition);

    // Restart NCM over its same durable lane. Startup replay must resume at
    // the NCM watermark, leaving both the provider call census and receipts
    // unchanged before Native is selected again.
    let ncm_restart_provider = SwitchNcmProvider::new();
    let ncm_restart_composition = switch_ncm_composition(Arc::clone(&ncm_restart_provider), 2);
    switch_assert_active(ncm_restart_composition.as_ref(), NCM_PROVIDER_ID, 2);
    let ncm_restart_journey = mount_project_observation_journey(ObservationJourneyMountInputsV1 {
        composition: Arc::clone(&ncm_restart_composition),
        profile_id: profile_id.clone(),
        scope: resolved_scope.clone(),
        authoritative_project_id: project_id.clone(),
        store_data_root: journey_root.clone(),
        provider: switch_ncm_mount(&journey_root, 2),
        policy: ObservationJourneyPolicyV1::project_default(),
    })
    .expect("restarted NCM active journey");
    switch_replay_and_assert(
        ncm_restart_journey.as_ref(),
        &store,
        &expected_source_event_ids,
        0,
    )
    .await;
    assert!(ncm_restart_provider.observation_calls().is_empty());
    assert!(ncm_restart_provider.handshakes.load(Ordering::Acquire) > 0);
    assert_eq!(ncm_provider.observation_calls(), ncm_calls_before_restart);
    assert!(ncm_provider.handshakes.load(Ordering::Acquire) >= ncm_handshakes_before_restart);
    assert_eq!(switch_git_state(&project_root), source_control_before);
    assert!(
        ncm_restart_journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .is_empty()
    );
    drop(ncm_restart_journey);
    drop(ncm_restart_composition);

    // Native is selected again under a fresh product revision. A fresh
    // incarnation receives the complete canonical history exactly once,
    // proving rollback is a route change over the store rather than a rewind
    // or loss of canonical source events.
    let native_rollback_port = Arc::new(JourneyNativePort::new());
    let native_rollback_composition = switch_native_composition(
        Arc::clone(&native_rollback_port) as Arc<dyn NativeMemoryApplicationPort>,
        3,
    );
    switch_assert_active(native_rollback_composition.as_ref(), "tracedecay.native", 3);
    let native_rollback_journey =
        mount_project_observation_journey(ObservationJourneyMountInputsV1 {
            composition: Arc::clone(&native_rollback_composition),
            profile_id: profile_id.clone(),
            scope: resolved_scope.clone(),
            authoritative_project_id: project_id.clone(),
            store_data_root: journey_root.clone(),
            provider: switch_native_mount(&journey_root, 3, SWITCH_NATIVE_ROLLBACK_JOURNAL),
            policy: ObservationJourneyPolicyV1::project_default(),
        })
        .expect("Native rollback journey");
    switch_replay_and_assert(
        native_rollback_journey.as_ref(),
        &store,
        &expected_source_event_ids,
        3,
    )
    .await;
    assert_eq!(
        native_rollback_port.observe_calls.load(Ordering::Acquire),
        3
    );
    assert_eq!(
        native_rollback_port.delivered.lock().unwrap().len(),
        expected_source_event_ids.len(),
        "Native rollback must see every canonical event exactly once"
    );
    assert!(
        native_rollback_port
            .delivered
            .lock()
            .unwrap()
            .iter()
            .all(|delivery| delivery.exact_scope == expected_exact_scope)
    );
    assert_eq!(switch_git_state(&project_root), source_control_before);

    let canonical_rows = store
        .replay_admitted_observations(
            ObservationReplayRequest::new(0, 16).expect("canonical replay request"),
        )
        .await
        .expect("canonical history remains readable");
    assert_eq!(
        canonical_rows
            .iter()
            .map(|record| record.observation().observation_id().as_str().to_owned())
            .collect::<Vec<_>>(),
        canonical_source_event_ids
    );

    let (native_id, native_revision, native_keys) =
        switch_journal_delivery_rows(&journey_root.join(SWITCH_NATIVE_JOURNAL));
    assert_eq!(
        (native_id.as_str(), native_revision),
        ("tracedecay.native", 1)
    );
    assert_eq!(native_keys.len(), 2);
    let (ncm_id, ncm_revision, ncm_keys) =
        switch_journal_delivery_rows(&journey_root.join(SWITCH_NCM_JOURNAL));
    assert_eq!((ncm_id.as_str(), ncm_revision), (NCM_PROVIDER_ID, 2));
    assert_eq!(ncm_keys.len(), 3);
    assert_eq!(
        ncm_provider
            .observation_calls()
            .into_iter()
            .map(|call| call.idempotency_key)
            .collect::<BTreeSet<_>>(),
        ncm_keys.into_iter().collect::<BTreeSet<_>>(),
        "NCM must receive each canonical delivery key once"
    );
    let (rollback_id, rollback_revision, rollback_keys) =
        switch_journal_delivery_rows(&journey_root.join(SWITCH_NATIVE_ROLLBACK_JOURNAL));
    assert_eq!(
        (rollback_id.as_str(), rollback_revision),
        ("tracedecay.native", 3)
    );
    assert_eq!(rollback_keys.len(), expected_source_event_ids.len());
    assert!(
        native_rollback_journey
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .is_empty()
    );
}
