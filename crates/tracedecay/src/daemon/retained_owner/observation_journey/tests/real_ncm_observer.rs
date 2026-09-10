//! Real offline worker on the canonical commit, late-enable, and restart path.

use super::*;
use crate::daemon::project_composition::{NcmWorkerOwnerSlot, construct_ncm_observer};
use tracedecay_memory_provider_registry::ObservationProviderMountV1;

/// Requires the actual production worker and a canonically installed model.
/// The fixture shares immutable model artifacts only; every mutable namespace,
/// canonical database, journal, and receipt lives inside its own temporary root.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires TRACEDECAY_NCM_WORKER and TRACEDECAY_NCM_REAL_MODEL_ROOT with pinned offline model"]
async fn real_ncm_observer_replays_independently_after_native_restart() {
    let worker =
        PathBuf::from(std::env::var_os("TRACEDECAY_NCM_WORKER").expect("real worker fixture"));
    let installed = PathBuf::from(
        std::env::var_os("TRACEDECAY_NCM_REAL_MODEL_ROOT").expect("installed model fixture"),
    );
    assert!(worker.is_absolute() && installed.is_absolute());
    let temp = TempDir::new().unwrap();
    let ncm_root = temp.path().join("ncm");
    std::fs::create_dir_all(&ncm_root).unwrap();
    std::os::unix::fs::symlink(installed.join("models"), ncm_root.join("models")).unwrap();
    let project_id = ProjectId::new("project.real-ncm-observer").unwrap();
    let profile_id = UserProfileId::new("profile.real-ncm-observer").unwrap();
    let resolved_scope = scope(project_id.clone());
    let runtime = HostAdmissionTestRuntimeV1::project(
        &temp.path().join("profile"),
        &temp.path().join("project"),
        project_id.clone(),
    )
    .await
    .unwrap();
    let store = runtime
        .registered_database_arc(HostAdmissionScope::Project)
        .unwrap()
        .observation_store();
    let journal_root = temp.path().join("journey");
    std::fs::create_dir_all(&journal_root).unwrap();
    let port = Arc::new(JourneyNativePort::new());
    let inputs = |composition, provider| ObservationJourneyMountInputsV1 {
        composition,
        profile_id: profile_id.clone(),
        scope: resolved_scope.clone(),
        authoritative_project_id: project_id.clone(),
        store_data_root: journal_root.clone(),
        provider,
        policy: ObservationJourneyPolicyV1::project_default(),
    };
    let native_metadata =
        crate::daemon::project_composition::native_observation_mount(&journal_root, 1).unwrap();
    let native = mount_project_observation_journey(inputs(
        composition(port.clone()),
        native_metadata.clone(),
    ))
    .unwrap();
    let session_id = SessionId::new("session.real-ncm-observer").unwrap();
    persist_position(
        &store,
        &project_id,
        &session_id,
        "Database transactions commit atomically after validation.",
        0,
    )
    .await;
    let cancellation = HostCancellationToken::new();
    assert_eq!(
        run_startup_replay(&native, &store, &cancellation)
            .await
            .unwrap()
            .admitted,
        1
    );
    assert_eq!(
        wait_for_settlement(native.journal_path()).await.0,
        "acknowledged"
    );
    assert!(
        native
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .is_empty()
    );
    drop(native);

    // NCM is enabled only after Native has already acknowledged the source.
    // Each open reconstructs the real worker over the same durable root.
    // Open 1 has a same-session commit waiting at startup; open 2 starts
    // caught up and receives its next same-session commit on the live edge.
    for open in 0..3 {
        if open == 1 {
            persist_position(
                &store,
                &project_id,
                &session_id,
                "A pending transaction resumes after the worker restarts.",
                1,
            )
            .await;
        }
        let worker = worker.clone();
        let ncm_root = ncm_root.clone();
        let owners = Arc::new(NcmWorkerOwnerSlot::default());
        let construction_owners = owners.clone();
        let observer_profile = profile_id.clone();
        let (observer, ncm_metadata) = tokio::task::spawn_blocking(move || {
            construct_ncm_observer(&construction_owners, &observer_profile, worker, ncm_root, 1)
        })
        .await
        .unwrap()
        .unwrap();
        let composition = Arc::new(
            ProjectMemoryProviderComposition::compose_with_observers(
                NativeProviderActivation::Enabled {
                    fabric_config: FabricConfig {
                        max_registered_providers: 2,
                        max_in_flight: 1,
                    },
                    port: port.clone(),
                    registration_revision: 1,
                    mode: EnabledProviderMode::Observer,
                },
                vec![observer],
            )
            .unwrap(),
        );
        let native = mount_and_replay(
            inputs(composition.clone(), native_metadata.clone()),
            store.clone(),
            &cancellation,
        )
        .await
        .expect("Native mounts through the production startup/live seam");
        let ncm = mount_and_replay(
            inputs(composition.clone(), ncm_metadata.clone()),
            store.clone(),
            &cancellation,
        )
        .await
        .expect("NCM mounts even when persisted namespace readiness needs a refresh");
        assert_ne!(native.journal_path(), ncm.journal_path());
        if open == 2 {
            wait_for_all_ncm_settlements(&ncm, 2).await;
            persist_position(
                &store,
                &project_id,
                &session_id,
                "A live transaction arrives after an empty restart replay.",
                2,
            )
            .await;
        }
        let expected_count = open + 1;
        wait_for_all_ncm_settlements(&native, expected_count).await;
        wait_for_all_ncm_settlements(&ncm, expected_count).await;
        for journey in [&native, &ncm] {
            let connection = rusqlite::Connection::open(journey.journal_path()).unwrap();
            let (count, applied): (i64, String) = connection
                .query_row(
                    "SELECT COUNT(*), MIN(committed_effect) FROM tdmem_observation_receipt_v1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(
                (count, applied.as_str()),
                (i64::try_from(expected_count).unwrap(), "applied")
            );
        }
        assert_eq!(port.observe_calls.load(Ordering::Relaxed), expected_count);
        assert_observer_has_no_active_output(
            &composition,
            &ncm_metadata,
            &profile_id,
            &resolved_scope,
            session_id.as_str(),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        for journey in [&native, &ncm] {
            assert!(journey.shutdown(deadline).await.is_empty());
        }
        drop(native);
        drop(ncm);
        drop(composition);
        // The last strong daemon owner must close before a fresh worker opens.
        tokio::task::spawn_blocking(move || drop(owners))
            .await
            .unwrap();
    }
}

/// Append at the canonical source frontier without bypassing the store's cursor CAS.
#[cfg(unix)]
async fn persist_position(
    store: &impl ObservationStore,
    project_id: &ProjectId,
    session_id: &SessionId,
    text: &str,
    position: u64,
) {
    let anchored = anchored_write(canonical_observation_at(
        project_id, session_id, text, position,
    ));
    let observation = anchored.observation();
    let expected_cursor = store
        .get_source_cursor(observation.source(), observation.scope())
        .await
        .expect("read the canonical source cursor");
    assert_eq!(
        expected_cursor
            .as_ref()
            .map(ObservationSourceCursorV1::position),
        (position > 0).then_some(position),
        "each append starts at the preceding commit's source frontier",
    );
    let write = ObservationWrite::new(
        observation.clone(),
        expected_cursor,
        anchored.next_cursor().clone(),
    )
    .expect("observation covers the expected-to-next cursor transition");
    let write = AnchoredObservationWrite::new(
        write,
        anchored.retrieval_anchor().clone(),
        anchored.projection_generation().clone(),
    )
    .expect("retain the canonical retrieval anchor");
    store
        .persist_observation(write)
        .await
        .expect("append canonical observation");
}

/// Require every expected row and its own receipt, including commits appended
/// after restart. Looking at the first acknowledged row would miss a stuck tail.
#[cfg(unix)]
async fn wait_for_all_ncm_settlements(journey: &ProjectObservationJourneyV1, expected: usize) {
    // Live replay's existing error backoff is five seconds, so a recovered
    // handshake needs a budget longer than one such pass.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let page = journey
            .journal
            .inspect(&JournalInspectionFilterV1 {
                limit: 8,
                ..JournalInspectionFilterV1::default()
            })
            .unwrap();
        let all_settled = page.rows.len() == expected
            && page.next_cursor.is_none()
            && page.rows.iter().all(|row| {
                row.state.as_wire() == "acknowledged"
                    && journey
                        .journal
                        .receipts_for(&row.observation_id)
                        .unwrap()
                        .len()
                        == 1
            });
        if all_settled && journey.stalled_on().is_none() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {expected} acknowledged rows with receipts and no standing stall: {}",
            ncm_delivery_diagnostics(&journey.journal)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(unix)]
fn assert_observer_has_no_active_output(
    composition: &ProjectMemoryProviderComposition,
    provider: &ObservationProviderMountV1,
    profile_id: &UserProfileId,
    scope: &ResolvedScope,
    session: &str,
) {
    let bytes = b"{}".to_vec();
    let call = ProviderCall::new(ProviderCallParts {
        operation: ProviderOperation::Recall,
        provider_id: provider.provider_id.clone(),
        registration_revision: provider.registration_revision,
        ready_receipt_sha256: "0".repeat(64),
        exact_scope: exact_scope_for_session(profile_id, scope, session).unwrap(),
        request_id: "observer-recall-denied".to_owned(),
        operation_id: "observer-recall-denied".to_owned(),
        expected_state_generation: 0,
        idempotency_key: None,
        control: OperationControl::new(i64::MAX, 1000, CancellationToken::new()),
        payload: CanonicalPayload::new(
            OwnedVersionedId::new("tracedecay.memory.provider.recall.v1").unwrap(),
            bytes.clone(),
            hex::encode(Sha256::digest(&bytes)),
        )
        .unwrap(),
        required_capabilities: vec![OwnedVersionedId::new("recall.query.v1").unwrap()],
        extensions: Vec::new(),
    })
    .unwrap();
    assert!(
        matches!(composition.registry().unwrap().invoke_active(&call),
        Err(FabricError::ProviderObserverOnly(id)) if id == provider.provider_id.as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_ncm_observer_preserves_native_canonical_acknowledgement() {
    let temp = TempDir::new().unwrap();
    let project_id = ProjectId::new("project.unavailable-ncm-observer").unwrap();
    let runtime = HostAdmissionTestRuntimeV1::project(
        &temp.path().join("profile"),
        &temp.path().join("project"),
        project_id.clone(),
    )
    .await
    .unwrap();
    let store = runtime
        .registered_database_arc(HostAdmissionScope::Project)
        .unwrap()
        .observation_store();
    let root = temp.path().join("journey");
    std::fs::create_dir_all(&root).unwrap();
    let worker = temp.path().join("absent-worker");
    let state_root = temp.path().join("ncm");
    let owners = NcmWorkerOwnerSlot::default();
    let observer_profile = UserProfileId::new("profile.unavailable-ncm-observer").unwrap();
    let (observer, metadata) = tokio::task::spawn_blocking(move || {
        construct_ncm_observer(&owners, &observer_profile, worker, state_root, 1)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(metadata.provider_instance_id.is_none());
    let port = Arc::new(JourneyNativePort::new());
    let composition = Arc::new(
        ProjectMemoryProviderComposition::compose_with_observers(
            NativeProviderActivation::Enabled {
                fabric_config: FabricConfig {
                    max_registered_providers: 2,
                    max_in_flight: 1,
                },
                port: port.clone(),
                registration_revision: 1,
                mode: EnabledProviderMode::Observer,
            },
            vec![observer],
        )
        .unwrap(),
    );
    let inputs = |provider| ObservationJourneyMountInputsV1 {
        composition: composition.clone(),
        profile_id: UserProfileId::new("profile.unavailable-ncm-observer").unwrap(),
        scope: scope(project_id.clone()),
        authoritative_project_id: project_id.clone(),
        store_data_root: root.clone(),
        provider,
        policy: ObservationJourneyPolicyV1::project_default(),
    };
    let native = mount_project_observation_journey(inputs(
        crate::daemon::project_composition::native_observation_mount(&root, 1).unwrap(),
    ))
    .unwrap();
    let ncm = mount_project_observation_journey(inputs(metadata)).unwrap();
    let session = SessionId::new("session.unavailable-ncm-observer").unwrap();
    store
        .persist_observation(anchored_write(canonical_observation(
            &project_id,
            &session,
            "Native still acknowledges this commit.",
        )))
        .await
        .unwrap();
    let cancellation = HostCancellationToken::new();
    let refused = run_startup_replay(&ncm, &store, &cancellation)
        .await
        .expect_err("unavailable worker must refuse readiness before admission");
    let ObservationJourneyError::Ingress(ObservationRuntimeError::Admission { cause, .. }) =
        refused
    else {
        panic!("unavailable observer must fail at the typed admission boundary");
    };
    let Some(AdmissionAdapterError::Readiness {
        source:
            ObservationJourneyError::SupervisedReadiness(SupervisedReadinessError::Unavailable {
                kind,
                terminal_code,
                detail,
                ..
            }),
        ..
    }) = cause.cause().downcast_ref::<AdmissionAdapterError>()
    else {
        panic!("admission must retain the supervised readiness refusal");
    };
    assert_eq!(
        *kind,
        tracedecay_memory_provider_registry::DegradationKindV1::HandshakeRefused
    );
    assert_eq!(*terminal_code, Some(TerminalCode::ProviderUnavailable));
    assert_eq!(
        detail,
        &format!(
            "provider handshake terminated with {}",
            TerminalCode::ProviderUnavailable.as_wire()
        )
    );
    assert_eq!(
        run_startup_replay(&native, &store, &cancellation)
            .await
            .unwrap()
            .admitted,
        1
    );
    assert_eq!(
        wait_for_settlement(native.journal_path()).await.0,
        "acknowledged"
    );
    assert_eq!(port.observe_calls.load(Ordering::Relaxed), 1);
    let connection = rusqlite::Connection::open(ncm.journal_path()).unwrap();
    let receipts: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tdmem_observation_receipt_v1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        receipts, 0,
        "unavailable observer cannot claim successful delivery"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for journey in [&native, &ncm] {
        assert!(journey.shutdown(deadline).await.is_empty());
    }
}

/// Bounded, content-free delivery evidence for a failed real-worker assertion.
/// Receipts expose outcomes/effects; refused terminals also retain their code.
#[cfg(unix)]
fn ncm_delivery_diagnostics(journal: &SqliteObservationJournal) -> serde_json::Value {
    let page = journal
        .inspect(&JournalInspectionFilterV1 {
            limit: 8,
            ..JournalInspectionFilterV1::default()
        })
        .expect("inspect NCM delivery metadata");
    let rows = page.rows.iter().map(|row| {
        let receipts = journal.receipts_for(&row.observation_id).expect("inspect NCM receipts");
        let refusals = journal.attempt_refusals_for(&row.observation_id).expect("inspect NCM terminal refusals");
        json!({
            "state": row.state.as_wire(),
            "attempt": row.attempt_number,
            "source_sequence": row.source_sequence.0,
            "receipts": receipts.iter().map(|receipt| json!({
                "attempt": receipt.attempt_number,
                "outcome": receipt.outcome.as_wire(),
                "committed_effect": receipt.committed_effect.as_wire(),
                "provider_receipt_present": receipt.provider_receipt_digest.is_some(),
            })).collect::<Vec<_>>(),
            "refused_terminal_codes": refusals.iter().map(|refusal|
                TerminalCode::from_wire(&refusal.terminal_code).map(TerminalCode::as_wire).unwrap_or("unrecognized")
            ).collect::<Vec<_>>(),
        })
    }).collect::<Vec<_>>();
    json!({ "total_rows": page.total_rows, "page_truncated": page.next_cursor.is_some(), "rows": rows })
}
