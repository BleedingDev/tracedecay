//! AC1 (causal binding to a committed host action) and AC2 (stable idempotency
//! and exact source sequence).

mod support;

use support::{Builder, INSTANCE, PROVIDER, T0, TestResult, journal, seal, stream_key};

use tracedecay_memory_observation::{
    AppendOutcomeV1, IdempotencyInputV1, OBSERVATION_CONTRACT_ID, ObservationDispatchPortV1,
    ObservationIdempotencyKeyV1, ObservationJournalError, SCHEMA_VERSION, SourceAuthorityV1,
    SourceSequenceV1, SqliteObservationJournal,
};

fn contract_input<'a>(
    payload_sha256: &'a str,
    extensions_digest: &'a str,
) -> IdempotencyInputV1<'a> {
    IdempotencyInputV1 {
        contract_id: OBSERVATION_CONTRACT_ID,
        provider_id: PROVIDER,
        registration_revision: 4,
        exact_scope_sha256: "aa2f1ac9c33a448fb824abf783a6d40ab52050d91bcc580d907e6b0a3303938e",
        source_authority: SourceAuthorityV1::HostSession,
        source_event_id: "event-1",
        source_event_revision: 0,
        observation_kind: "session.message_committed.v1",
        payload_contract: "tracedecay.memory.observation.session-message.v1",
        payload_sha256,
        extensions_digest,
    }
}

const PAYLOAD_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const EXTENSIONS_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";

#[test]
fn idempotency_key_is_pinned_to_the_contract_derivation() -> TestResult {
    let key =
        ObservationIdempotencyKeyV1::derive(&contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST));
    // Golden vector. A change here means the derivation drifted, and every
    // already-delivered observation would re-deliver as a new key.
    assert_eq!(
        key.as_str(),
        "08b3246c2386c00fc567367a635c6fd8419b28e3d90768fba2ff6e8684f86bcc"
    );
    Ok(())
}

#[test]
fn idempotency_key_ignores_clock_and_row_identity() -> TestResult {
    let mut early = Builder::at_sequence(7).build()?;
    let mut late = Builder {
        admitted_at: T0 + 999_999,
        entropy: [9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
        ..Builder::at_sequence(7)
    }
    .build()?;
    seal(&mut early);
    seal(&mut late);
    assert_ne!(early.observation_id, late.observation_id);
    assert_ne!(early.occurred_at_unix_micros, late.occurred_at_unix_micros);
    assert_eq!(early.idempotency_key, late.idempotency_key);
    Ok(())
}

#[test]
fn idempotency_key_changes_with_every_contract_input() -> TestResult {
    let base =
        ObservationIdempotencyKeyV1::derive(&contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST));
    let mutations: Vec<IdempotencyInputV1<'_>> = vec![
        IdempotencyInputV1 {
            provider_id: "ncm.external",
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            registration_revision: 5,
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            exact_scope_sha256: EXTENSIONS_DIGEST,
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            source_authority: SourceAuthorityV1::ToolExecution,
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            source_event_id: "event-2",
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            source_event_revision: 1,
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            observation_kind: "tool.execution_settled.v1",
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        IdempotencyInputV1 {
            payload_contract: "tracedecay.memory.observation.tool-execution.v1",
            ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
        },
        contract_input(EXTENSIONS_DIGEST, EXTENSIONS_DIGEST),
        contract_input(PAYLOAD_DIGEST, PAYLOAD_DIGEST),
    ];
    for mutation in &mutations {
        assert_ne!(
            base,
            ObservationIdempotencyKeyV1::derive(mutation),
            "a contract input did not change the key"
        );
    }
    // Field-boundary collision guard: moving a character across two adjacent
    // fields must still change the key, which length framing guarantees.
    let shifted = IdempotencyInputV1 {
        source_event_id: "event-",
        observation_kind: "1session.message_committed.v1",
        ..contract_input(PAYLOAD_DIGEST, EXTENSIONS_DIGEST)
    };
    assert_ne!(base, ObservationIdempotencyKeyV1::derive(&shifted));
    Ok(())
}

#[test]
fn a_forged_idempotency_key_is_refused() -> TestResult {
    let mut admitted = Builder::at_sequence(3).build()?;
    admitted.idempotency_key = ObservationIdempotencyKeyV1::parse(PAYLOAD_DIGEST)?;
    let error = admitted.validate().err().ok_or("forged key was accepted")?;
    assert!(matches!(
        error,
        ObservationJournalError::IdempotencyKeyMismatch { .. }
    ));
    Ok(())
}

#[test]
fn an_unsettled_source_is_unrepresentable_and_an_empty_proof_is_refused() -> TestResult {
    let mut admitted = Builder::at_sequence(3).build()?;
    admitted.source.settlement_proof_sha256 = String::new();
    seal(&mut admitted);
    let error = admitted
        .validate()
        .err()
        .ok_or("unsettled source was accepted")?;
    assert!(matches!(
        error,
        ObservationJournalError::UnsettledSource {
            field: "settlement_proof_sha256"
        }
    ));

    let mut admitted = Builder::at_sequence(3).build()?;
    admitted.source.source_event_id = String::new();
    seal(&mut admitted);
    assert!(matches!(
        admitted.validate(),
        Err(ObservationJournalError::UnsettledSource {
            field: "source_event_id"
        })
    ));
    Ok(())
}

#[test]
fn same_key_same_payload_is_duplicate_and_different_payload_is_conflict() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(5).build()?;
    assert!(matches!(
        store.append_admitted(&admitted)?,
        AppendOutcomeV1::Appended { .. }
    ));
    assert!(matches!(
        store.append_admitted(&admitted)?,
        AppendOutcomeV1::DuplicateIdempotencyKey { .. }
    ));

    // Same key with different canonical content: the key is content-derived, so
    // the only way to reach this is to declare one and carry the other.
    let mut conflicting = Builder {
        body: "{\"message\":\"different\"}".to_owned(),
        ..Builder::at_sequence(5)
    }
    .build()?;
    conflicting.idempotency_key = admitted.idempotency_key.clone();
    conflicting.envelope_sha256 = conflicting.expected_envelope_sha256();
    assert!(matches!(
        conflicting.validate(),
        Err(ObservationJournalError::IdempotencyKeyMismatch { .. })
    ));
    Ok(())
}

#[test]
fn a_re_sanitized_replay_at_the_same_sequence_is_a_duplicate_not_a_second_row() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let first = Builder::at_sequence(11).build()?;
    assert!(matches!(
        store.append_admitted(&first)?,
        AppendOutcomeV1::Appended { .. }
    ));

    // The sanitizer corpus moved between admission and replay, so the same
    // settled event now canonicalizes to different sanitized bytes and derives
    // a different key. It is still the same event and must not double-insert.
    let replayed = Builder {
        body: "{\"message\":\"hello-11\",\"redacted\":true}".to_owned(),
        ..Builder::at_sequence(11)
    }
    .build()?;
    assert_ne!(first.idempotency_key, replayed.idempotency_key);
    assert!(matches!(
        store.append_admitted(&replayed)?,
        AppendOutcomeV1::DuplicateSourceEvent { .. }
    ));
    assert_eq!(store.queue_pressure(&support::target()?)?.queue_items, 1);
    Ok(())
}

#[test]
fn a_different_event_at_the_same_sequence_is_an_explicit_conflict() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(4).build()?)?;
    let impostor = Builder {
        source_event_id: "event-impostor".to_owned(),
        ..Builder::at_sequence(4)
    }
    .build()?;
    assert!(matches!(
        store.append_admitted(&impostor)?,
        AppendOutcomeV1::SourceSequenceConflict { .. }
    ));
    Ok(())
}

#[test]
fn source_sequence_regression_is_rejected_but_fan_out_is_not() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(9).build()?)?;

    let regressed = Builder::at_sequence(7).build()?;
    assert_eq!(
        store.append_admitted(&regressed)?,
        AppendOutcomeV1::RejectedSourceSequenceRegression {
            last_admitted: SourceSequenceV1(9)
        }
    );

    // The same settled event fanning out to a second provider registration
    // reuses its sequence and must be admitted.
    let fanned = Builder {
        provider_id: "ncm.external".to_owned(),
        ..Builder::at_sequence(9)
    }
    .build()?;
    assert!(matches!(
        store.append_admitted(&fanned)?,
        AppendOutcomeV1::Appended { .. }
    ));

    // The idempotency key is derived over the provider *registration*, not the
    // instance, so a second instance of the same registration is a duplicate.
    let same_registration = Builder {
        provider_instance_id: "instance-2".to_owned(),
        ..Builder::at_sequence(9)
    }
    .build()?;
    assert!(matches!(
        store.append_admitted(&same_registration)?,
        AppendOutcomeV1::DuplicateIdempotencyKey { .. }
    ));
    Ok(())
}

#[test]
fn source_sequence_is_scoped_per_authority_scope_and_stream() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(9).build()?)?;
    let other_stream = Builder {
        source_stream: "session-2".to_owned(),
        ..Builder::at_sequence(2)
    }
    .build()?;
    assert!(matches!(
        store.append_admitted(&other_stream)?,
        AppendOutcomeV1::Appended { .. }
    ));
    assert_eq!(
        store
            .replay_cursor(&stream_key("session-1")?)?
            .ok_or("cursor missing")?
            .last_admitted_sequence,
        SourceSequenceV1(9)
    );
    assert_eq!(
        store
            .replay_cursor(&stream_key("session-2")?)?
            .ok_or("cursor missing")?
            .last_admitted_sequence,
        SourceSequenceV1(2)
    );
    Ok(())
}

#[test]
fn a_new_source_event_revision_produces_a_new_key() -> TestResult {
    let first = Builder::at_sequence(6).build()?;
    let revised = Builder {
        source_event_revision: 1,
        ..Builder::at_sequence(6)
    }
    .build()?;
    assert_ne!(first.idempotency_key, revised.idempotency_key);
    Ok(())
}

#[test]
fn an_expired_deadline_is_refused_before_the_journal_append() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(2).build()?;
    let outcome = store.append_admitted_at(&admitted, admitted.deadline_unix_micros)?;
    assert_eq!(
        outcome,
        AppendOutcomeV1::RejectedDeadlineExpired {
            deadline_unix_micros: admitted.deadline_unix_micros
        }
    );
    assert!(store.replay_cursor(&stream_key("session-1")?)?.is_none());
    Ok(())
}

#[test]
fn append_rolls_back_entirely_when_the_callers_transaction_fails() -> TestResult {
    // The co-located caller path: a caller that owns the transaction gets true
    // atomicity across journal, delivery, and cursor.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(8).build()?;

    let mut connection = rusqlite::Connection::open(&path)?;
    {
        let transaction = connection.transaction()?;
        let outcome = store.append_admitted_in_transaction(&transaction, &admitted, T0)?;
        assert!(matches!(outcome, AppendOutcomeV1::Appended { .. }));
        transaction.rollback()?;
    }

    let journal_rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_journal_v1",
        [],
        |row| row.get(0),
    )?;
    let delivery_rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_delivery_v1",
        [],
        |row| row.get(0),
    )?;
    let cursor_rows: i64 = connection.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_replay_cursor_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!((journal_rows, delivery_rows, cursor_rows), (0, 0, 0));
    Ok(())
}

#[test]
fn capacity_exhaustion_returns_a_typed_outcome_and_never_a_silent_drop() -> TestResult {
    let mut policy = support::policy();
    policy.max_queue_items = 2;
    let directory = tempfile::tempdir()?;
    let store = SqliteObservationJournal::open(directory.path().join("journal.sqlite3"), policy)?;
    for sequence in 1..=2 {
        assert!(matches!(
            store.append_admitted(&Builder::at_sequence(sequence).build()?)?,
            AppendOutcomeV1::Appended { .. }
        ));
    }
    let outcome = store.append_admitted(&Builder::at_sequence(3).build()?)?;
    assert!(matches!(
        outcome,
        AppendOutcomeV1::RejectedCapacity { queue_items: 2, .. }
    ));
    let pressure = store.queue_pressure(&support::target()?)?;
    assert_eq!(pressure.queue_items, 2);
    assert_eq!(pressure.max_queue_items, 2);
    Ok(())
}

#[test]
fn opening_a_store_with_a_newer_schema_version_fails_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    drop(journal(&path)?);
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch("PRAGMA user_version = 99")?;
    drop(connection);

    let error = SqliteObservationJournal::open(&path, support::policy())
        .err()
        .ok_or("a newer schema was silently downgraded")?;
    assert!(matches!(
        error,
        ObservationJournalError::SchemaAhead {
            found: 99,
            supported: SCHEMA_VERSION
        }
    ));
    Ok(())
}

#[test]
fn instance_identity_is_carried_into_the_provider_target() -> TestResult {
    let admitted = Builder::at_sequence(1).build()?;
    assert_eq!(admitted.target.provider_instance_id, INSTANCE);
    Ok(())
}
