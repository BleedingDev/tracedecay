//! The outbox must not shadow Native facts.
//!
//! ADR-0005: "TraceDecay's outbox never becomes a second Native fact store."
//! These tests pin the structural properties that keep that true.

mod support;

use support::{Builder, SECOND, T0, TestResult, journal, lease_request};

use tracedecay_memory_observation::{
    ForgetSourceKeyV1, ForgetSourceRequestV1, JournalInspectionFilterV1, ObservationDispatchPortV1,
    ObservationJournalReaderV1, ObservationRetentionPortV1, SourceAuthorityV1,
};

#[test]
fn a_native_fact_promotion_observation_is_delivery_state_only() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder {
        source_authority: SourceAuthorityV1::NativeFactPromotion,
        observation_kind: "native.fact_promoted.v1".to_owned(),
        forget_source_key: "forget:fact-1".to_owned(),
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;

    let key = ForgetSourceKeyV1::new("forget:fact-1")?;
    store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "fact_retracted".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;

    // The journal forgets the content completely, which it could not do if it
    // were the record of the fact.
    assert!(store.verify_forgotten(&key)?.verified);
    assert_eq!(store.receipts_for(&admitted.observation_id)?.len(), 1);
    Ok(())
}

#[test]
fn inspection_offers_no_content_query_path() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    store.append_admitted(
        &Builder {
            source_authority: SourceAuthorityV1::ToolExecution,
            observation_kind: "tool.execution_settled.v1".to_owned(),
            source_stream: "tools-1".to_owned(),
            ..Builder::at_sequence(2)
        }
        .build()?,
    )?;

    // Every filter dimension is operational metadata.
    let filtered = store.inspect(&JournalInspectionFilterV1 {
        source_authority: Some(SourceAuthorityV1::ToolExecution),
        ..Default::default()
    })?;
    assert_eq!(filtered.total_rows, 1);
    assert_eq!(
        filtered.rows[0].observation_kind,
        "tool.execution_settled.v1"
    );

    // Rows carry digests, never bytes: the row type has no payload field at
    // all, so a recall path cannot be built on top of inspection.
    let all = store.inspect(&Default::default())?;
    assert_eq!(all.total_rows, 2);
    for row in &all.rows {
        assert_eq!(row.payload_sha256.len(), 64);
        assert!(row.content_present);
    }
    Ok(())
}

#[test]
fn the_schema_holds_no_content_index() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    drop(journal(&path)?);
    let connection = rusqlite::Connection::open(&path)?;

    let full_text: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master \
         WHERE sql LIKE '%fts%' OR sql LIKE '%VIRTUAL TABLE%'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(full_text, 0, "the journal must hold no content index");

    let content_indexes: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' \
         AND (sql LIKE '%payload_bytes%' OR sql LIKE '%observation_kind%' \
              OR sql LIKE '%extensions_json%')",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(
        content_indexes, 0,
        "no index may make observation content queryable"
    );
    Ok(())
}

#[test]
fn leasing_returns_content_only_for_delivery_and_never_after_forgetting() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    assert_eq!(store.lease_pending(&lease_request(T0, 4))?.len(), 1);

    store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: ForgetSourceKeyV1::new("forget:session-1")?,
        reason: "privacy_deletion".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;
    assert!(
        store
            .lease_pending(&lease_request(T0 + 2 * SECOND, 4))?
            .is_empty(),
        "forgotten content must never be delivered"
    );
    Ok(())
}
