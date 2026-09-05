use rusqlite::Connection;
use serde_json::json;
use tracedecay_domain::{
    AnchorDurabilityClass, AnchorSourceGenerationV2, CommitId, ComponentVersion, CoverageReportV1,
    EvidenceAvailabilityV1, EvidenceClass, GenerationBoundRepositoryProvenanceV1, ObservationId,
    ObservationIdentityMaterialV1, ObservationOrderingDomainV1, ObservationScopeV1,
    ObservationSourceCursorV1, ObservationSourceGenerationV1, ObservationSourceIdentityV1,
    ObservationSourceRangeV1, PayloadAccessState, PayloadReferenceV1,
    PrivacyDomainBoundLocatorDigest, ProjectId, ProjectionGenerationId, ProviderId, RefId,
    RepositoryEvidenceV1, RepositoryId, RepositoryProvenanceV1, RepositoryRemoteIdentityV1,
    RetentionClass, RetrievalAnchorRecordV2, RetrievalAnchorRecordV2Parts, RetrievalAnchorTargetV2,
    SanitizationReceiptId, SanitizationReceiptRefV1, SanitizationReceiptV1, SanitizerDispositionV1,
    SensitivityV1, SessionId, UtcMicros, VectorWatermark, WorktreeId,
};
use tracedecay_store::observation::ObservationProvenanceDispositionV1;
use tracedecay_store::{
    AnchoredObservationWrite, CursorAdvanceLedgerReasonV1, CursorAdvanceLedgerReceiptIdV1,
    ObservationCoverageReason, ObservationCursorAdvance, ObservationReadOperationV1,
    ObservationReadResultV1, ObservationWrite, SESSION_MESSAGE_PROJECTOR_VERSION,
    StorageRuntimeErrorV1, build_observation_resolution_authorization_v1,
    build_observation_retrieval_anchor_v2,
};

use crate::operation::StorageOperationError;

use super::ObservationExecutor;

fn observation_write(body: &str, receipt_id: &str) -> ObservationWrite {
    observation_write_at(body, receipt_id, 1, 0, 1, None)
}

fn observation_write_at(
    body: &str,
    receipt_id: &str,
    generation: u64,
    start: u64,
    end: u64,
    expected_cursor: Option<ObservationSourceCursorV1>,
) -> ObservationWrite {
    let source = ObservationSourceIdentityV1::for_provider(
        ProviderId::new("provider.fixture").unwrap(),
        SessionId::new("session.fixture").unwrap(),
    )
    .unwrap();
    let scope = ObservationScopeV1::Project {
        project_id: ProjectId::new("project.fixture").unwrap(),
    };
    let generation = ObservationSourceGenerationV1::new(generation).unwrap();
    let range = ObservationSourceRangeV1::new(start, end).unwrap();
    let payload = json!({"kind": "assistant_message", "body": body});
    let payload_reference = PayloadReferenceV1::for_payload(&payload).unwrap();
    let receipt = SanitizationReceiptV1::new(
        SanitizationReceiptRefV1::new(
            SanitizationReceiptId::new(receipt_id).unwrap(),
            ComponentVersion::new("sanitizer.fixture.v1").unwrap(),
        )
        .unwrap(),
        SanitizerDispositionV1::Accepted,
        SensitivityV1::NonSensitive,
        Some(payload_reference),
    )
    .unwrap();
    let observation = tracedecay_domain::DurableObservationV1::new(
        ObservationIdentityMaterialV1::for_native_record(
            source.clone(),
            scope.clone(),
            generation,
            range,
            ObservationOrderingDomainV1::SqliteRowId,
            ObservationId::new("record.fixture").unwrap(),
        )
        .unwrap(),
        receipt,
        RetentionClass::new("retention.fixture").unwrap(),
        payload,
    )
    .unwrap();
    let next_cursor = ObservationSourceCursorV1::for_ordering(
        source,
        scope,
        generation,
        ObservationOrderingDomainV1::SqliteRowId,
        range.end(),
    )
    .unwrap();
    ObservationWrite::new(observation, expected_cursor, next_cursor).unwrap()
}

fn anchored_observation_write(body: &str, receipt_id: &str) -> AnchoredObservationWrite {
    let write = observation_write(body, receipt_id);
    anchored(write)
}

fn anchored(write: ObservationWrite) -> AnchoredObservationWrite {
    anchored_at(write, UtcMicros(1))
}

fn anchored_at(write: ObservationWrite, ingested_at: UtcMicros) -> AnchoredObservationWrite {
    let projection_generation = ProjectionGenerationId::new("projection.fixture.v1").unwrap();
    let authorization =
        build_observation_resolution_authorization_v1(write.observation(), "runtime.fixture.v1")
            .unwrap();
    let anchor = build_observation_retrieval_anchor_v2(
        write.observation(),
        projection_generation.clone(),
        ingested_at,
        authorization,
    )
    .unwrap();
    AnchoredObservationWrite::new(write, anchor, projection_generation).unwrap()
}

#[test]
fn semantic_anchor_replay_ignores_local_ingest_clock() {
    let mut connection = connection();
    let first = anchored_at(
        observation_write("semantic replay", "receipt.semantic-replay"),
        UtcMicros(1),
    );
    let replay = anchored_at(
        ObservationWrite::new(
            first.observation().clone(),
            None,
            first.next_cursor().clone(),
        )
        .unwrap(),
        UtcMicros(2),
    );

    execute(&mut connection, &first).unwrap();
    execute(&mut connection, &replay)
        .expect("local ingest clocks must not make immutable anchor replay collide");

    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM retrieval_anchors", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

fn with_repository_capture_at(
    write: AnchoredObservationWrite,
    captured_at: UtcMicros,
) -> AnchoredObservationWrite {
    let observation = write.observation();
    let ObservationScopeV1::Project { project_id } = observation.scope() else {
        panic!("repository capture requires project scope");
    };
    let digest =
        PrivacyDomainBoundLocatorDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap();
    let capture = RepositoryProvenanceV1::new(
        RepositoryId::new("repository.fixture").unwrap(),
        Some(project_id.clone()),
        Some(WorktreeId::new("worktree.fixture").unwrap()),
        digest.clone(),
        RepositoryEvidenceV1::new(
            EvidenceAvailabilityV1::Known(RefId::new("refs/heads/main").unwrap()),
            EvidenceAvailabilityV1::Known(
                CommitId::new("0123456789abcdef0123456789abcdef01234567").unwrap(),
            ),
            EvidenceAvailabilityV1::Unknown,
            EvidenceAvailabilityV1::Known(digest),
            RepositoryRemoteIdentityV1::Missing,
            EvidenceAvailabilityV1::Unknown,
        )
        .unwrap(),
        captured_at,
    )
    .unwrap();
    let provenance = GenerationBoundRepositoryProvenanceV1::new(
        write.projection_generation().clone(),
        capture,
        Some(observation.observation_id().clone()),
    )
    .unwrap();
    let anchor = RetrievalAnchorRecordV2::new(RetrievalAnchorRecordV2Parts {
        target: RetrievalAnchorTargetV2::RepositoryCapture {
            repository_id: provenance.capture().repository_id().clone(),
            capture_id: provenance.capture_id().clone(),
            receipt: observation.receipt().receipt().clone(),
        },
        owner: observation.scope().clone(),
        aliases: vec![],
        occurred_at: None,
        ingested_at: write.retrieval_anchor().ingested_at(),
        evidence_class: EvidenceClass::Observed,
        source_generation: AnchorSourceGenerationV2::RepositoryCapture(
            provenance.capture_id().clone(),
        ),
        projection_generation: write.projection_generation().clone(),
        projection_watermark: VectorWatermark::default(),
        coverage: CoverageReportV1::default(),
        source_observations: vec![observation.observation_id().clone()],
        source_anchors: vec![],
        authorization: write.retrieval_anchor().authorization().clone(),
        payload_access: PayloadAccessState::Eligible,
        retention_class: observation.retention_class().clone(),
        durability: AnchorDurabilityClass::DurableEvidence,
    })
    .unwrap();
    write
        .with_repository_provenance_attachment(
            EvidenceAvailabilityV1::Known(provenance),
            Some(anchor),
        )
        .unwrap()
}

#[test]
fn repository_recapture_replay_retains_original_provenance() {
    let mut connection = connection();
    let observation = anchored_observation_write("recapture replay", "receipt.recapture-replay");
    let first = with_repository_capture_at(observation.clone(), UtcMicros(100));
    let replay = with_repository_capture_at(observation, UtcMicros(200))
        .with_provenance_disposition(ObservationProvenanceDispositionV1::RetainCommittedOnReplay);
    let first_capture = first
        .repository_provenance_attachment()
        .provenance()
        .unwrap()
        .capture();
    let replay_capture = replay
        .repository_provenance_attachment()
        .provenance()
        .unwrap()
        .capture();

    // Independent admissions observe identical repository state at different
    // clocks. These remain distinct capture events, but the observation replay
    // must retain its first attachment rather than replacing immutable evidence.
    assert_eq!(first.observation(), replay.observation());
    assert_eq!(first.retrieval_anchor(), replay.retrieval_anchor());
    assert_eq!(
        first_capture.repository_id(),
        replay_capture.repository_id()
    );
    assert_eq!(first_capture.project_id(), replay_capture.project_id());
    assert_eq!(first_capture.worktree_id(), replay_capture.worktree_id());
    assert_eq!(
        first_capture.canonical_root_digest(),
        replay_capture.canonical_root_digest()
    );
    assert_eq!(first_capture.evidence(), replay_capture.evidence());
    assert_eq!(first_capture.captured_at(), UtcMicros(100));
    assert_eq!(replay_capture.captured_at(), UtcMicros(200));
    assert_ne!(first_capture.capture_id(), replay_capture.capture_id());
    assert_ne!(
        first
            .repository_provenance_attachment()
            .anchor()
            .unwrap()
            .anchor_id(),
        replay
            .repository_provenance_attachment()
            .anchor()
            .unwrap()
            .anchor_id(),
    );

    execute(&mut connection, &first).unwrap();
    let read_operation = ObservationReadOperationV1::Observation {
        observation_id: first.observation().observation_id().clone(),
    };
    let retained = read(&mut connection, &read_operation).unwrap();
    // Bypass adapter preflight deliberately: both racing submissions may have
    // observed an empty snapshot before the first writer committed.
    execute(&mut connection, &replay)
        .expect("a recapture of unchanged state must replay the original observation authority");
    assert_eq!(read(&mut connection, &read_operation).unwrap(), retained);
    for (table, expected) in [
        ("observations", 1),
        ("sanitization_receipts", 1),
        ("retrieval_anchors", 2),
        ("observation_retrieval_anchors", 1),
        ("observation_repository_provenance", 1),
        ("source_cursors", 1),
        ("projection_queue", 1),
    ] {
        assert_eq!(
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            expected,
            "{table} changed during observation replay",
        );
    }
}

#[test]
fn strict_repository_replay_rejects_changed_provenance() {
    let mut connection = connection();
    let observation = anchored_observation_write("strict replay", "receipt.strict-replay");
    let first = with_repository_capture_at(observation.clone(), UtcMicros(100));
    let recapture = with_repository_capture_at(observation.clone(), UtcMicros(200));
    execute(&mut connection, &first).unwrap();
    let operation = ObservationReadOperationV1::Observation {
        observation_id: first.observation().observation_id().clone(),
    };
    let retained = read(&mut connection, &operation).unwrap();
    for replay in [recapture, observation] {
        assert_eq!(
            replay.provenance_disposition(),
            ObservationProvenanceDispositionV1::RequireExact
        );
        let changes = connection.total_changes();
        let error = execute(&mut connection, &replay).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("observation repository provenance collision")
        );
        assert_eq!(connection.total_changes(), changes);
        assert_eq!(read(&mut connection, &operation).unwrap(), retained);
    }
    execute(&mut connection, &first).expect("strict exact attachment still replays");
}

#[test]
fn fresh_repository_replay_rejects_corrupt_committed_authority() {
    for corruption in [
        "DELETE FROM observation_repository_provenance",
        "UPDATE observation_repository_provenance SET capture_json = 'null'",
        "UPDATE observation_repository_provenance SET owner_json = 'null'",
        "UPDATE retrieval_anchors SET owner_json = 'null' WHERE anchor_id IN
             (SELECT retrieval_anchor_id FROM observation_repository_provenance)",
        "UPDATE retrieval_anchors SET projection_generation = 'projection.corrupt' WHERE anchor_id IN
             (SELECT retrieval_anchor_id FROM observation_repository_provenance)",
        "INSERT INTO retrieval_anchor_aliases (owner_json, alias_kind, locator_digest, anchor_id)
             SELECT owner_json, 'corrupt-extra', 'corrupt-digest', retrieval_anchor_id
             FROM observation_repository_provenance",
        "DELETE FROM retrieval_anchor_aliases",
        "UPDATE sanitization_receipts SET receipt_json = 'null'",
    ] {
        let mut connection = connection();
        let observation = anchored_observation_write("corrupt replay", "receipt.corrupt-replay");
        let first = with_repository_capture_at(observation.clone(), UtcMicros(100));
        let replay = with_repository_capture_at(observation, UtcMicros(200))
            .with_provenance_disposition(ObservationProvenanceDispositionV1::RetainCommittedOnReplay);
        execute(&mut connection, &first).unwrap();
        connection.execute(corruption, []).unwrap();
        let changes = connection.total_changes();
        assert!(execute(&mut connection, &replay).is_err(), "accepted {corruption}");
        assert_eq!(connection.total_changes(), changes, "repaired {corruption}");
    }
}

#[test]
fn fresh_repository_replay_rejects_changed_observation_and_receipt() {
    let mut connection = connection();
    let first = with_repository_capture_at(
        anchored_observation_write("exact evidence", "receipt.exact-evidence"),
        UtcMicros(100),
    );
    execute(&mut connection, &first).unwrap();
    for candidate in [
        anchored_observation_write("changed evidence", "receipt.changed-evidence"),
        anchored_observation_write("exact evidence", "receipt.changed-receipt"),
    ] {
        let candidate = with_repository_capture_at(candidate, UtcMicros(200))
            .with_provenance_disposition(
                ObservationProvenanceDispositionV1::RetainCommittedOnReplay,
            );
        assert_eq!(
            candidate.observation().observation_id(),
            first.observation().observation_id()
        );
        let changes = connection.total_changes();
        let error = execute(&mut connection, &candidate).unwrap_err();
        assert!(error.to_string().contains("observation identity collision"));
        assert_eq!(connection.total_changes(), changes);
    }
}

fn connection() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE sanitization_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    sanitizer_version TEXT NOT NULL,
                    payload_digest TEXT NOT NULL,
                    receipt_json TEXT NOT NULL
                 );
                 CREATE TABLE observations (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL UNIQUE,
                    payload_digest TEXT NOT NULL,
                    receipt_id TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    committed_cursor_json TEXT NOT NULL
                 );
                 CREATE TABLE source_cursors (
                    source_json TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    cursor_json TEXT NOT NULL,
                    PRIMARY KEY (source_json, scope_json)
                 );
                 CREATE TABLE source_cursor_advances (
                    source_json TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    coverage_json TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    receipt_id TEXT,
                    PRIMARY KEY(source_json, scope_json, coverage_json)
                 );
                 CREATE TABLE projection_queue (
                   observation_id TEXT PRIMARY KEY,
                   observation_sequence INTEGER NOT NULL UNIQUE,
                   attempt_count INTEGER NOT NULL DEFAULT 0,
                   next_retry_at_micros INTEGER NOT NULL DEFAULT 0,
                   last_error TEXT
                 );
                 CREATE TABLE retrieval_anchors (
                    anchor_id TEXT PRIMARY KEY,
                    anchor_json TEXT NOT NULL,
                    owner_json TEXT NOT NULL,
                    projection_generation TEXT NOT NULL,
                    UNIQUE(anchor_id, owner_json)
                 );
                 CREATE TABLE retrieval_anchor_aliases (
                    owner_json TEXT NOT NULL,
                    alias_kind TEXT NOT NULL,
                    locator_digest TEXT NOT NULL,
                    anchor_id TEXT NOT NULL,
                    PRIMARY KEY(owner_json, alias_kind, locator_digest)
                 );
                 CREATE TABLE observation_retrieval_anchors (
                    observation_id TEXT PRIMARY KEY,
                    anchor_id TEXT NOT NULL UNIQUE
                 );
                 CREATE TABLE observation_repository_provenance (
                    observation_id TEXT PRIMARY KEY,
                    availability_json TEXT NOT NULL,
                    capture_json TEXT,
                    retrieval_anchor_id TEXT UNIQUE,
                    owner_json TEXT
                 );
                 CREATE TABLE observation_projection_checkpoints (
                    projector_version TEXT PRIMARY KEY,
                    last_sequence INTEGER NOT NULL
                 );
                 CREATE TABLE observation_projection_rebuilds (
                    projector_version TEXT PRIMARY KEY,
                    generation TEXT NOT NULL,
                    frontier_sequence INTEGER NOT NULL,
                    aliases_staged_through INTEGER NOT NULL,
                    staged_through INTEGER NOT NULL,
                    projected_rows INTEGER NOT NULL,
                    skipped_observations INTEGER NOT NULL,
                    state TEXT NOT NULL
                 );",
        )
        .unwrap();
    connection
}

fn execute(
    connection: &mut Connection,
    write: &AnchoredObservationWrite,
) -> Result<(), StorageOperationError> {
    let mut transaction = connection.transaction()?;
    let savepoint = transaction.savepoint()?;
    ObservationExecutor.execute_write(&savepoint, write)?;
    savepoint.commit()?;
    transaction.commit()?;
    Ok(())
}

fn read(
    connection: &mut Connection,
    operation: &ObservationReadOperationV1,
) -> rusqlite::Result<ObservationReadResultV1> {
    let transaction = connection.transaction()?;
    ObservationExecutor.execute_read(&transaction, operation)
}

fn execute_cursor_advance(
    connection: &mut Connection,
    advance: &ObservationCursorAdvance,
) -> Result<(), StorageOperationError> {
    let mut transaction = connection.transaction()?;
    let savepoint = transaction.savepoint()?;
    ObservationExecutor.execute_cursor_advance(&savepoint, advance)?;
    savepoint.commit()?;
    transaction.commit()?;
    Ok(())
}

#[test]
fn anchored_write_persists_all_authority_rows_atomically() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");

    execute(&mut connection, &write).unwrap();

    for table in [
        "observations",
        "sanitization_receipts",
        "retrieval_anchors",
        "retrieval_anchor_aliases",
        "observation_retrieval_anchors",
        "observation_repository_provenance",
        "source_cursors",
        "projection_queue",
    ] {
        let count = connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        assert!(count > 0, "{table} was not persisted");
    }
}

#[test]
fn relocated_native_duplicate_advances_coverage_without_reinserting() {
    let mut connection = connection();
    let original = anchored(observation_write_at(
        "stable payload",
        "receipt.original",
        1,
        41,
        42,
        None,
    ));
    execute(&mut connection, &original).unwrap();
    let relocated = anchored(observation_write_at(
        "stable payload",
        "receipt.relocated",
        2,
        71,
        72,
        Some(original.next_cursor().clone()),
    ));
    assert_eq!(
        original.observation().observation_id(),
        relocated.observation().observation_id()
    );

    execute(&mut connection, &relocated).unwrap();
    execute(&mut connection, &relocated).unwrap();

    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM observations", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM source_cursor_advances", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM sanitization_receipts", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        2
    );
    let source_json = super::encode(relocated.observation().source()).unwrap();
    let scope_json = super::encode(relocated.observation().scope()).unwrap();
    assert_eq!(
        super::read_cursor(&connection, &source_json, &scope_json).unwrap(),
        Some(relocated.next_cursor().clone())
    );
}

#[test]
fn exact_replay_is_a_no_op_after_the_source_cursor_advanced() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    let replay_write = ObservationWrite::new(
        write.observation().clone(),
        None,
        write.next_cursor().clone().with_resume_checkpoint(7, 11),
    )
    .unwrap();
    let replay = AnchoredObservationWrite::new(
        replay_write,
        write.retrieval_anchor().clone(),
        write.projection_generation().clone(),
    )
    .unwrap();

    execute(&mut connection, &write).unwrap();
    execute(&mut connection, &replay).unwrap();

    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM projection_queue", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn replay_with_different_anchor_fails_without_mutating_authority_rows() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let conflicting_generation = ProjectionGenerationId::new("projection.conflicting.v1").unwrap();
    let authorization =
        build_observation_resolution_authorization_v1(write.observation(), "runtime.fixture.v1")
            .unwrap();
    let conflicting_anchor = build_observation_retrieval_anchor_v2(
        write.observation(),
        conflicting_generation.clone(),
        UtcMicros(1),
        authorization,
    )
    .unwrap();
    let conflicting = AnchoredObservationWrite::new(
        ObservationWrite::new(
            write.observation().clone(),
            None,
            write.next_cursor().clone(),
        )
        .unwrap(),
        conflicting_anchor,
        conflicting_generation,
    )
    .unwrap();

    for disposition in [
        ObservationProvenanceDispositionV1::RequireExact,
        ObservationProvenanceDispositionV1::RetainCommittedOnReplay,
    ] {
        let candidate = conflicting.clone().with_provenance_disposition(disposition);
        let error = execute(&mut connection, &candidate).unwrap_err();
        assert!(error.to_string().contains("retrieval anchor"));
    }
    for table in [
        "observations",
        "retrieval_anchors",
        "observation_retrieval_anchors",
        "observation_repository_provenance",
        "source_cursors",
        "projection_queue",
    ] {
        assert_eq!(
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1,
            "{table} changed after rejected replay"
        );
    }
}

#[test]
fn replay_does_not_repair_missing_anchor_authority() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    connection
        .execute("DELETE FROM retrieval_anchor_aliases", [])
        .unwrap();

    let error = execute(&mut connection, &write).unwrap_err();

    assert!(error.to_string().contains("retrieval anchor alias"));
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM retrieval_anchor_aliases", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
}

#[test]
fn replay_rejects_extra_anchor_alias_authority() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    connection
        .execute(
            "INSERT INTO retrieval_anchor_aliases (
                    owner_json, alias_kind, locator_digest, anchor_id
                 )
                 SELECT owner_json, 'corrupt-extra', locator_digest, anchor_id
                 FROM retrieval_anchor_aliases LIMIT 1",
            [],
        )
        .unwrap();

    let error = execute(&mut connection, &write).unwrap_err();

    assert!(error.to_string().contains("retrieval anchor alias"));
}

#[test]
fn identity_collision_fails_without_advancing_the_source_cursor() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let cursor_before: String = connection
        .query_row("SELECT cursor_json FROM source_cursors", [], |row| {
            row.get(0)
        })
        .unwrap();

    let error = execute(
        &mut connection,
        &anchored_observation_write("conflicting", "receipt.conflicting"),
    )
    .unwrap_err();

    assert!(error.to_string().contains("observation identity collision"));
    assert_eq!(
        connection
            .query_row("SELECT cursor_json FROM source_cursors", [], |row| row
                .get::<_, String>(
                0
            ))
            .unwrap(),
        cursor_before
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM observations", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM projection_queue", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn source_cursor_advance_replays_exactly_and_reports_ledger_disagreement() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let advance = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::BlankFrame,
    )
    .unwrap();

    execute_cursor_advance(&mut connection, &advance).unwrap();
    execute_cursor_advance(&mut connection, &advance).unwrap();

    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM source_cursor_advances", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    let conflicting = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::OutOfScope,
    )
    .unwrap();
    let error = execute_cursor_advance(&mut connection, &conflicting).unwrap_err();
    let StorageOperationError::CursorAdvanceLedgerDisagreement { disagreement } = error else {
        panic!("expected structured immutable ledger disagreement");
    };
    assert_eq!(disagreement.source(), write.observation().source());
    assert_eq!(disagreement.scope(), write.observation().scope());
    assert_eq!(disagreement.coverage(), conflicting.coverage());
    assert!(matches!(
        disagreement.stored().reason(),
        CursorAdvanceLedgerReasonV1::Known(ObservationCoverageReason::BlankFrame)
    ));
    assert!(matches!(
        disagreement.stored().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Absent
    ));
    assert!(matches!(
        disagreement.candidate().reason(),
        CursorAdvanceLedgerReasonV1::Known(ObservationCoverageReason::OutOfScope)
    ));
    assert!(matches!(
        disagreement.candidate().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Absent
    ));
}

#[test]
fn canonical_cursor_advance_receipt_remains_typed_after_authority_lookup() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let advance = ObservationCursorAdvance::for_ordering_with_sanitization_receipt(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::DuplicateObservation,
        write.observation().receipt().clone(),
    )
    .unwrap();
    execute_cursor_advance(&mut connection, &advance).unwrap();

    let conflicting = ObservationCursorAdvance::for_ordering_with_sanitization_receipt(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::CanonicalPayloadRevision,
        write.observation().receipt().clone(),
    )
    .unwrap();

    let error = execute_cursor_advance(&mut connection, &conflicting).unwrap_err();
    let StorageOperationError::CursorAdvanceLedgerDisagreement { disagreement } = error else {
        panic!("expected structured immutable ledger disagreement");
    };
    assert!(matches!(
        disagreement.stored().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Known(receipt_id)
            if receipt_id.as_str() == "receipt.fixture"
    ));
    assert!(matches!(
        disagreement.candidate().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Known(receipt_id)
            if receipt_id.as_str() == "receipt.fixture"
    ));
}

#[test]
fn corrupt_cursor_advance_ledger_values_are_opaque_and_content_free() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let advance = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::BlankFrame,
    )
    .unwrap();
    execute_cursor_advance(&mut connection, &advance).unwrap();

    let private_ledger_value = format!("provider-private-transcript:{}", "x".repeat(16_384));
    connection
        .execute(
            "UPDATE source_cursor_advances SET reason = ?1, receipt_id = ?2",
            rusqlite::params![private_ledger_value, private_ledger_value],
        )
        .unwrap();
    let conflicting = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::OutOfScope,
    )
    .unwrap();

    let error = execute_cursor_advance(&mut connection, &conflicting).unwrap_err();
    let StorageOperationError::CursorAdvanceLedgerDisagreement { disagreement } = error else {
        panic!("expected structured immutable ledger disagreement");
    };
    assert!(matches!(
        disagreement.stored().reason(),
        CursorAdvanceLedgerReasonV1::Opaque { fingerprint }
            if fingerprint.as_str().starts_with("sha256:")
    ));
    assert!(matches!(
        disagreement.stored().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Opaque { fingerprint }
            if fingerprint.as_str().starts_with("sha256:")
    ));
    assert!(matches!(
        disagreement.candidate().reason(),
        CursorAdvanceLedgerReasonV1::Known(ObservationCoverageReason::OutOfScope)
    ));
    assert!(matches!(
        disagreement.candidate().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Absent
    ));
    let rendered = serde_json::to_string(&disagreement).unwrap();
    assert!(rendered.len() < 4_096, "diagnostic must stay bounded");
    assert!(!rendered.contains("provider-private-transcript"));
}

#[test]
fn short_corrupt_ledger_receipt_stays_opaque_across_runtime_boundary() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    let advance = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::BlankFrame,
    )
    .unwrap();
    execute_cursor_advance(&mut connection, &advance).unwrap();

    let secret = "provider-private-transcript-secret";
    assert!(SanitizationReceiptId::new(secret).is_ok());
    connection
        .execute(
            "UPDATE source_cursor_advances SET receipt_id = ?1",
            rusqlite::params![secret],
        )
        .unwrap();
    let conflicting = ObservationCursorAdvance::for_ordering(
        write.observation().source().clone(),
        write.observation().scope().clone(),
        write.observation().identity().generation(),
        write.observation().identity().ordering_domain(),
        Some(write.next_cursor().clone()),
        ObservationSourceRangeV1::new(1, 2).unwrap(),
        ObservationCoverageReason::OutOfScope,
    )
    .unwrap();

    let error = execute_cursor_advance(&mut connection, &conflicting).unwrap_err();
    let StorageOperationError::CursorAdvanceLedgerDisagreement { disagreement } = error else {
        panic!("expected structured immutable ledger disagreement");
    };
    assert!(matches!(
        disagreement.stored().receipt_id(),
        CursorAdvanceLedgerReceiptIdV1::Opaque { fingerprint }
            if fingerprint.as_str().starts_with("sha256:")
    ));
    let disagreement_json = serde_json::to_string(&disagreement).unwrap();
    assert!(!disagreement_json.contains(secret));
    let runtime_error =
        StorageRuntimeErrorV1::ObservationCursorAdvanceLedgerDisagreement { disagreement };
    let runtime_error_json = serde_json::to_string(&runtime_error).unwrap();
    assert!(!runtime_error_json.contains(secret));
}

#[test]
fn retrieval_anchor_alias_reads_are_owner_bound() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    let alias = write.retrieval_anchor().aliases()[0].clone();
    execute(&mut connection, &write).unwrap();

    let resolved = read(
        &mut connection,
        &ObservationReadOperationV1::RetrievalAnchorByAlias {
            scope: write.observation().scope().clone(),
            alias: alias.clone(),
        },
    )
    .unwrap();
    assert_eq!(
        resolved,
        ObservationReadResultV1::RetrievalAnchorByAlias(Some(write.retrieval_anchor_id().clone()))
    );

    let foreign = read(
        &mut connection,
        &ObservationReadOperationV1::RetrievalAnchorByAlias {
            scope: ObservationScopeV1::Profile,
            alias,
        },
    )
    .unwrap();
    assert_eq!(
        foreign,
        ObservationReadResultV1::RetrievalAnchorByAlias(None)
    );
}

#[test]
fn replay_queue_and_checkpoint_reads_preserve_projection_ordering() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();

    let point = read(
        &mut connection,
        &ObservationReadOperationV1::Observation {
            observation_id: write.observation().observation_id().clone(),
        },
    )
    .unwrap();
    let ObservationReadResultV1::Observation(point) = point else {
        panic!("unexpected point-read result");
    };
    let point = point.expect("persisted observation must be readable");
    assert_eq!(point.observation, *write.observation());
    assert_eq!(point.committed_cursor, *write.next_cursor());
    assert_eq!(point.retrieval_anchor, *write.retrieval_anchor());
    assert_eq!(point.projection_generation, *write.projection_generation());
    assert_eq!(
        point.repository_provenance,
        *write.repository_provenance_attachment()
    );
    assert!(point.projection_queued);

    let replay = read(
        &mut connection,
        &ObservationReadOperationV1::Replay {
            after_sequence: 0,
            limit: 10,
        },
    )
    .unwrap();
    let ObservationReadResultV1::Replay(rows) = replay else {
        panic!("unexpected replay result");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].observation.observation_id(),
        write.observation().observation_id()
    );
    assert_eq!(&rows[0].retrieval_anchor, write.retrieval_anchor());
    assert_eq!(
        &rows[0].projection_generation,
        write.projection_generation()
    );
    assert_eq!(
        &rows[0].repository_provenance,
        write.repository_provenance_attachment()
    );
    assert!(rows[0].projection_queued);

    assert_eq!(
        read(
            &mut connection,
            &ObservationReadOperationV1::NextQueuedProjection {
                now_micros: i64::MAX,
            },
        )
        .unwrap(),
        ObservationReadResultV1::NextQueuedProjection(Some(
            write.observation().observation_id().clone()
        ))
    );
    assert_eq!(
        read(
            &mut connection,
            &ObservationReadOperationV1::ProjectionCheckpoint,
        )
        .unwrap(),
        ObservationReadResultV1::ProjectionCheckpoint(0)
    );

    connection
        .execute(
            "INSERT INTO observation_projection_checkpoints
                    (projector_version, last_sequence) VALUES (?1, 1)",
            [SESSION_MESSAGE_PROJECTOR_VERSION],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO observation_projection_rebuilds (
                    projector_version, generation, frontier_sequence, aliases_staged_through,
                    staged_through, projected_rows, skipped_observations, state
                 ) VALUES (?1, ?2, 1, 1, 1, 1, 0, 'ready')",
            [SESSION_MESSAGE_PROJECTOR_VERSION, "projection.fixture.v1"],
        )
        .unwrap();
    assert_eq!(
        read(
            &mut connection,
            &ObservationReadOperationV1::NextQueuedProjection {
                now_micros: i64::MAX,
            },
        )
        .unwrap(),
        ObservationReadResultV1::NextQueuedProjection(None)
    );
    assert_eq!(
        read(
            &mut connection,
            &ObservationReadOperationV1::ProjectionCheckpoint,
        )
        .unwrap(),
        ObservationReadResultV1::ProjectionCheckpoint(1)
    );
    let progress = read(
        &mut connection,
        &ObservationReadOperationV1::ProjectionRebuildProgress,
    )
    .unwrap();
    let ObservationReadResultV1::ProjectionRebuildProgress(Some(progress)) = progress else {
        panic!("unexpected projection rebuild progress result");
    };
    assert_eq!(
        progress.generation,
        ProjectionGenerationId::new("projection.fixture.v1").unwrap()
    );
    assert_eq!(progress.frontier_sequence, 1);
    assert_eq!(progress.staged_through, 1);
    assert_eq!(progress.projected_rows, 1);
}

#[test]
fn point_and_replay_reads_reject_incomplete_observation_authority() {
    let mut connection = connection();
    let write = anchored_observation_write("fixture", "receipt.fixture");
    execute(&mut connection, &write).unwrap();
    connection
        .execute("DELETE FROM observation_retrieval_anchors", [])
        .unwrap();

    for operation in [
        ObservationReadOperationV1::Observation {
            observation_id: write.observation().observation_id().clone(),
        },
        ObservationReadOperationV1::Replay {
            after_sequence: 0,
            limit: 10,
        },
    ] {
        let error = read(&mut connection, &operation).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("observation retrieval anchor is missing")
        );
    }
}
