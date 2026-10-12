//! Regressions for the journal-correctness defects found in tdmem-0502.
//!
//! Each test names the invariant it guards and fails if the original defect
//! returns. Several reach past the public API with raw SQL — deliberately: an
//! invariant that only holds because no public call can break it is worth
//! proving against a store that has been broken on purpose.

mod support;

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};

use support::{
    Builder, DAY, INSTANCE, LEASE, PROVIDER, PROVIDER_RECEIPT_DIGEST, SECOND, T0, TestResult,
    applied_receipt, digest_hex, journal, lease_request, lease_request_for, policy, stream_key,
    unavailable_receipt, withheld_at,
};

use tracedecay_memory_observation::{
    AppendOutcomeV1, AttemptOutcomeV1, DeliveryReceiptIdV1, DeliveryStateV1, ForgetSourceKeyV1,
    ForgetSourceRequestV1, JournalInspectionFilterV1, ObservationCommittedEffectV1,
    ObservationDispatchPortV1, ObservationJournalError, ObservationJournalReaderV1,
    ObservationOutcomeV1, ObservationRetentionPortV1, ProviderTargetV1, ReplayDispositionV1,
    SourceSequenceV1,
};
use tracedecay_memory_provider_api::{OwnedProviderId, WithheldReason};

const MIRROR: &str = "tracedecay.mirror";

// ---------------------------------------------------------------------------
// Finding 1 — the attempt number is consumed by the claim.
// ---------------------------------------------------------------------------

#[test]
fn a_reaped_lease_never_hands_its_attempt_number_back() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let first = store.lease_pending(&lease_request(T0, 4))?;
    assert_eq!(first[0].attempt_number, 1);

    // The dispatcher dies without recording anything and the lease lapses.
    let after_lapse = T0 + LEASE + SECOND;
    assert_eq!(store.reap_expired_leases(after_lapse, 8)?, 1);

    let second = store.lease_pending(&lease_request(after_lapse, 4))?;
    assert_eq!(
        second[0].attempt_number, 2,
        "a reaped lease handed its attempt number back, so two dispatchers can \
         derive one receipt id"
    );
    assert_ne!(second[0].lease_id, first[0].lease_id);

    // Both attempts address distinct receipt slots, so the second can be
    // recorded even though the first slot was never used.
    assert!(matches!(
        store.record_attempt(&applied_receipt(&second[0], after_lapse))?,
        AttemptOutcomeV1::Recorded {
            state: DeliveryStateV1::Acknowledged,
            ..
        }
    ));
    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].attempt_number, 2);
    Ok(())
}

#[test]
fn a_lease_that_keeps_lapsing_still_converges_on_exhausted() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    // Every lease lapses before its dispatcher records anything. Without the
    // claim consuming an attempt this loop runs forever.
    let mut now = T0;
    for expected in 1..=policy().max_attempts {
        let leased = store.lease_pending(&lease_request(now, 4))?;
        assert_eq!(leased.len(), 1, "attempt {expected} could not be claimed");
        assert_eq!(leased[0].attempt_number, expected);
        now += LEASE + SECOND;
        assert_eq!(store.reap_expired_leases(now, 8)?, 1);
    }

    assert!(
        store.lease_pending(&lease_request(now, 4))?.is_empty(),
        "a row that consumed every attempt was leased again"
    );
    let row = &store.inspect(&Default::default())?.rows[0];
    assert_eq!(row.state, DeliveryStateV1::Exhausted);

    // Nothing was silently dropped: the journal wrote the terminal receipt no
    // provider ever produced, in a slot no attempt had taken.
    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].attempt_number, policy().max_attempts + 1);
    assert_eq!(
        receipts[0].outcome,
        ObservationOutcomeV1::ProviderUnavailable
    );
    assert_eq!(
        receipts[0]
            .provider_effect_summary
            .no_effect_reason
            .as_deref(),
        Some("max_delivery_attempts_consumed")
    );
    // No instance made this attempt, so none is claimed.
    assert_eq!(receipts[0].provider_instance_id, None);
    Ok(())
}

#[test]
fn a_stale_dispatchers_receipt_is_kept_without_disturbing_the_live_lease() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let stale = store.lease_pending(&lease_request(T0, 4))?[0].clone();
    let after_lapse = T0 + LEASE + SECOND;
    store.reap_expired_leases(after_lapse, 8)?;
    let live = store.lease_pending(&lease_request(after_lapse, 4))?[0].clone();

    // The stale dispatcher finally answers, long after its lease was reaped.
    assert!(matches!(
        store.record_attempt(&applied_receipt(&stale, after_lapse))?,
        AttemptOutcomeV1::LeaseLost { .. }
    ));
    let row = &store.inspect(&Default::default())?.rows[0];
    assert_eq!(
        row.state,
        DeliveryStateV1::Leased,
        "a stale receipt settled a row that belongs to a later attempt"
    );
    assert_eq!(row.attempt_number, 2);

    // The live attempt still settles the row, and both receipts stand.
    assert!(matches!(
        store.record_attempt(&applied_receipt(&live, after_lapse + SECOND))?,
        AttemptOutcomeV1::Recorded {
            state: DeliveryStateV1::Acknowledged,
            ..
        }
    ));
    assert_eq!(store.receipts_for(&admitted.observation_id)?.len(), 2);
    Ok(())
}

#[test]
fn a_duplicate_receipt_settles_a_row_that_is_still_leased() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let admitted = Builder::at_sequence(1).build()?;
    let leased = {
        let store = journal(&path)?;
        store.append_admitted(&admitted)?;
        let leased = store.lease_pending(&lease_request(T0, 4))?[0].clone();
        store.record_attempt(&applied_receipt(&leased, T0))?;
        leased
    };

    // A crash between the receipt insert and the delivery advance would leave
    // exactly this: an immutable receipt standing for the attempt the row is
    // still leased against. Recreate it, because it is the state that used to
    // deadlock — the resubmitted receipt returned early and the row stayed
    // leased until the end of time.
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "UPDATE tdmem_observation_delivery_v1 \
             SET state = 'leased', lease_id = ?1, lease_owner = ?2, \
                 lease_expires_at_micros = ?3, last_outcome = NULL, \
                 last_committed_effect = NULL, last_receipt_id = NULL",
            params![leased.lease_id.as_str(), "dispatcher-1", T0 + LEASE,],
        )?;
    }

    let store = journal(&path)?;
    assert_eq!(
        store.record_attempt(&applied_receipt(&leased, T0))?,
        AttemptOutcomeV1::DuplicateReceipt {
            state: DeliveryStateV1::Acknowledged
        }
    );
    let row = &store.inspect(&Default::default())?.rows[0];
    assert_eq!(
        row.state,
        DeliveryStateV1::Acknowledged,
        "a duplicate receipt left the row leased against a finished attempt"
    );
    // The standing receipt is the authority and was not rewritten.
    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].outcome, ObservationOutcomeV1::Applied);
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 2 — a terminal receipt never lands on an occupied slot.
// ---------------------------------------------------------------------------

#[test]
fn expiry_takes_a_fresh_slot_and_never_overwrites_a_standing_receipt() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let admitted = Builder::at_sequence(1).build()?;
    {
        let store = journal(&path)?;
        store.append_admitted(&admitted)?;
        let leased = store.lease_pending(&lease_request(T0, 4))?[0].clone();
        store.record_attempt(&unavailable_receipt(&leased, T0))?;
    }

    // A dispatcher whose lease had already lapsed lands an `Applied` receipt in
    // the slot the delivery row knows nothing about. The row still says one
    // attempt was consumed, so a naive `attempt_number + 1` collides here.
    let occupied = DeliveryReceiptIdV1::derive(&admitted.observation_id, 2);
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "INSERT INTO tdmem_observation_receipt_v1 ( \
                 observation_id, attempt_number, receipt_id, idempotency_key, payload_sha256, \
                 extensions_digest, provider_id, provider_instance_id, registration_revision, \
                 state_generation_before, state_generation_after, outcome, committed_effect, \
                 provider_effect_summary_json, provider_receipt_digest, started_at_micros, \
                 finished_at_micros, warnings_json) \
             SELECT observation_id, 2, ?1, idempotency_key, payload_sha256, extensions_digest, \
                    provider_id, provider_instance_id, registration_revision, \
                    state_generation_before, state_generation_after, 'applied', 'applied', \
                    provider_effect_summary_json, ?2, started_at_micros, finished_at_micros, \
                    warnings_json \
             FROM tdmem_observation_receipt_v1 WHERE attempt_number = 1",
            params![occupied.as_str(), PROVIDER_RECEIPT_DIGEST],
        )?;
    }

    let store = journal(&path)?;
    // Leasing past the deadline is what drives the journal's own expiry.
    let past_deadline = admitted.deadline_unix_micros + SECOND;
    assert!(
        store
            .lease_pending(&lease_request(past_deadline, 4))?
            .is_empty()
    );

    let receipts = store.receipts_for(&admitted.observation_id)?;
    assert_eq!(receipts.len(), 3, "expiry reused an occupied receipt slot");
    // The standing Applied receipt is untouched: its outcome, its effect, and
    // its provider proof all survive.
    assert_eq!(receipts[1].attempt_number, 2);
    assert_eq!(receipts[1].outcome, ObservationOutcomeV1::Applied);
    assert_eq!(
        receipts[1].committed_effect,
        ObservationCommittedEffectV1::Applied
    );
    assert_eq!(
        receipts[1].provider_receipt_digest.as_deref(),
        Some(PROVIDER_RECEIPT_DIGEST)
    );
    assert_eq!(receipts[1].receipt_id.as_str(), occupied.as_str());
    // The terminal receipt took the next free slot.
    assert_eq!(receipts[2].attempt_number, 3);
    assert_eq!(receipts[2].outcome, ObservationOutcomeV1::DeadlineExceeded);

    // And the delivery points at its own terminal receipt, not at a foreign one.
    let connection = Connection::open(&path)?;
    let last: String = connection.query_row(
        "SELECT last_receipt_id FROM tdmem_observation_delivery_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(
        last,
        DeliveryReceiptIdV1::derive(&admitted.observation_id, 3)
            .as_str()
            .to_owned()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 3 — delivery is addressed by registration, not by instance.
// ---------------------------------------------------------------------------

#[test]
fn a_restarted_provider_instance_can_drain_the_queue_it_inherited() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let first = Builder::at_sequence(1).build()?;
    let second = Builder::at_sequence(2).build()?;
    {
        let store = journal(&path)?;
        store.append_admitted(&first)?;
        store.append_admitted(&second)?;
    }

    // The provider process dies and comes back as instance-2 under the same
    // registration. So does the journal.
    let store = journal(&path)?;
    let target = ProviderTargetV1 {
        provider_id: OwnedProviderId::new(PROVIDER)?,
        provider_instance_id: "instance-2".to_owned(),
        registration_revision: 4,
        ready_receipt_digest: support::READY_DIGEST.to_owned(),
    };
    assert_eq!(
        store.queue_pressure(&target)?.queue_items,
        2,
        "capacity is addressed by instance, so a restart sees an empty queue"
    );

    let leased = store.lease_pending(&lease_request_for("instance-2", T0, 8))?;
    assert_eq!(
        leased.len(),
        2,
        "work admitted for instance-1 was stranded by the restart"
    );
    assert_eq!(leased[0].target.provider_instance_id, "instance-2");
    assert_eq!(leased[0].source_sequence, SourceSequenceV1(1));
    assert_eq!(leased[1].source_sequence, SourceSequenceV1(2));

    store.record_attempt(&applied_receipt(&leased[0], T0))?;
    // The receipt records the instance that actually made the attempt.
    let receipts = store.receipts_for(&first.observation_id)?;
    assert_eq!(
        receipts[0].provider_instance_id.as_deref(),
        Some("instance-2")
    );

    // Admission evidence and per-attempt evidence are both kept, and they
    // legitimately differ.
    let row = store
        .inspect(&JournalInspectionFilterV1 {
            provider_instance_id: Some(INSTANCE.to_owned()),
            ..Default::default()
        })?
        .rows
        .into_iter()
        .find(|row| row.observation_id == first.observation_id)
        .ok_or("the admitted instance no longer identifies the row")?;
    assert_eq!(row.provider_instance_id, INSTANCE);
    assert_eq!(row.registration_revision, 4);
    assert_eq!(row.last_provider_instance_id.as_deref(), Some("instance-2"));
    Ok(())
}

#[test]
fn an_expired_deadline_is_reaped_by_whichever_instance_is_running() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;

    let past_deadline = admitted.deadline_unix_micros + SECOND;
    assert!(
        store
            .lease_pending(&lease_request_for("instance-7", past_deadline, 4))?
            .is_empty()
    );
    assert_eq!(
        store.inspect(&Default::default())?.rows[0].state,
        DeliveryStateV1::Expired,
        "expiry is addressed by instance, so a restart leaves rows past their \
         deadline queued forever"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 4 — withheld receipt identities are independently rederivable.
// ---------------------------------------------------------------------------

#[test]
fn withheld_receipt_evidence_rejects_every_identity_perturbation() -> TestResult {
    let valid = withheld_at(7, "forget:session-1")?;
    valid.validate()?;

    let mut variants = Vec::new();
    let mut changed = valid.clone();
    changed.sanitizer_revision.push_str(".other");
    variants.push(changed);
    let mut changed = valid.clone();
    changed.source_payload_sha256 = digest_hex(b"other-source");
    variants.push(changed);
    let mut changed = valid.clone();
    changed.extensions_digest = digest_hex(b"other-extensions");
    variants.push(changed);
    let mut changed = valid.clone();
    changed.reason = WithheldReason::Quarantined.as_str().to_owned();
    variants.push(changed);
    let mut changed = valid.clone();
    changed.finding_count = changed.finding_count.saturating_add(1);
    variants.push(changed);
    let mut changed = valid.clone();
    changed.findings_digest = digest_hex(b"other-findings");
    variants.push(changed);
    let mut changed = valid;
    changed.receipt_id.push('0');
    variants.push(changed);

    for variant in variants {
        assert!(matches!(
            variant.validate(),
            Err(ObservationJournalError::ReceiptDigestMismatch {
                field: "withheld_receipt_id"
            })
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 5 — a withheld record never erases an admitted position.
// ---------------------------------------------------------------------------

#[test]
fn a_withheld_record_cannot_overwrite_an_admitted_cursor() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.append_admitted(&Builder::at_sequence(7).build()?)?;
    let admitted_cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("cursor missing")?;
    assert_eq!(
        admitted_cursor.last_disposition,
        ReplayDispositionV1::Admitted
    );
    assert!(admitted_cursor.last_settlement_proof_sha256.is_some());

    // Hygiene refuses something at a position already settled and delivered.
    store.record_withheld_at(&withheld_at(7, "forget:session-1")?, T0 + SECOND)?;
    assert_eq!(
        store
            .replay_cursor(&stream_key("session-1")?)?
            .ok_or("cursor missing")?,
        admitted_cursor,
        "a withheld record erased the settlement proof of a delivered event"
    );

    // A strictly newer position does move it, and repeating that record is a
    // no-op rather than a rewrite.
    store.record_withheld_at(&withheld_at(8, "forget:session-1")?, T0 + 2 * SECOND)?;
    let withheld_cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("cursor missing")?;
    assert_eq!(withheld_cursor.last_admitted_sequence, SourceSequenceV1(8));
    assert_eq!(
        withheld_cursor.last_disposition,
        ReplayDispositionV1::Withheld
    );
    assert!(withheld_cursor.last_settlement_proof_sha256.is_none());

    store.record_withheld_at(&withheld_at(8, "forget:session-1")?, T0 + 3 * SECOND)?;
    assert_eq!(
        store
            .replay_cursor(&stream_key("session-1")?)?
            .ok_or("cursor missing")?,
        withheld_cursor,
        "a repeated withheld record was not idempotent"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 6 — sequence regression is per registration, so fan-out is late-safe.
// ---------------------------------------------------------------------------

#[test]
fn one_event_still_fans_out_to_a_target_that_is_behind() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;

    // Provider A consumes the stream and runs ahead.
    store.append_admitted(&Builder::at_sequence(1).build()?)?;
    store.append_admitted(&Builder::at_sequence(2).build()?)?;

    // Provider B is registered later and starts from the beginning. The
    // stream's ingress cursor is at 2, which says nothing about B.
    let mirror = |sequence: u64| Builder {
        provider_id: MIRROR.to_owned(),
        ..Builder::at_sequence(sequence)
    };
    assert!(matches!(
        store.append_admitted(&mirror(1).build()?)?,
        AppendOutcomeV1::Appended {
            source_sequence: SourceSequenceV1(1),
            ..
        },
    ));
    assert!(matches!(
        store.append_admitted(&mirror(3).build()?)?,
        AppendOutcomeV1::Appended {
            source_sequence: SourceSequenceV1(3),
            ..
        },
    ));

    // Per-target monotonicity still holds: B cannot go backwards to a position
    // it never took, even though A never offered B that position either.
    assert!(matches!(
        store.append_admitted(&mirror(2).build()?)?,
        AppendOutcomeV1::RejectedSourceSequenceRegression {
            last_admitted: SourceSequenceV1(3)
        }
    ));

    // And source order is preserved per target, not merged across them.
    let mirror_rows = store.inspect(&JournalInspectionFilterV1 {
        provider_id: Some(MIRROR.to_owned()),
        ..Default::default()
    })?;
    assert_eq!(mirror_rows.total_rows, 2);
    let mirror_leased: Vec<SourceSequenceV1> = store
        .lease_pending(&{
            let mut request = lease_request(T0, 8);
            request.provider_id = MIRROR.to_owned();
            request
        })?
        .into_iter()
        .map(|item| item.source_sequence)
        .collect();
    assert_eq!(
        mirror_leased,
        vec![SourceSequenceV1(1), SourceSequenceV1(3)],
        "source order was not preserved for this target"
    );

    // The ingress cursor tracks the stream as a whole and never went backwards
    // when the lagging target took sequence 1.
    let cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("cursor missing")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(3));
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 7 — forgetting content forgets the evidence that describes it.
// ---------------------------------------------------------------------------

#[test]
fn forgetting_clears_the_whole_hygiene_binding_and_leaves_nothing_undecodable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let key = admitted.privacy.forget_source_key.clone();

    let receipt = store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "subject_erasure_request".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;
    assert_eq!(receipt.payloads_zeroed, 1);
    assert_eq!(receipt.sanitization_bindings_cleared, 1);
    assert_eq!(receipt.deliveries_forgotten, 1);

    // Raw SQL: every binding column is gone, and no half-cleared combination
    // was left behind.
    let connection = Connection::open(&path)?;
    let (bindings, halves, digests): (i64, i64, i64) = connection.query_row(
        "SELECT COALESCE(SUM(sanitization_receipt_id IS NOT NULL), 0), \
                COALESCE(SUM((sanitization_receipt_id IS NULL) \
                             != (sanitization_receipt_json IS NULL)), 0), \
                COALESCE(SUM(source_payload_sha256 IS NOT NULL), 0) \
         FROM tdmem_observation_journal_v1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(bindings, 0, "the hygiene binding outlived the content");
    assert_eq!(halves, 0, "a partially cleared binding was left behind");
    assert_eq!(
        digests, 0,
        "the pre-sanitization digest of forgotten content is still at rest"
    );
    let json: Option<String> = connection.query_row(
        "SELECT sanitization_receipt_json FROM tdmem_observation_journal_v1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(json, None);

    // Public decode still works: the row is inspectable, its receipts are
    // readable, and it is never delivered.
    let row = &store.inspect(&Default::default())?.rows[0];
    assert!(!row.content_present);
    assert_eq!(row.state, DeliveryStateV1::Forgotten);
    assert_eq!(store.receipts_for(&admitted.observation_id)?.len(), 1);
    assert!(
        store
            .lease_pending(&lease_request(T0 + 2 * SECOND, 4))?
            .is_empty()
    );

    let verification = store.verify_forgotten(&key)?;
    assert_eq!(verification.rows_with_binding_remaining, 0);
    assert_eq!(verification.rows_with_content_remaining, 0);
    assert_eq!(verification.undelivered_remaining, 0);
    assert!(verification.verified);
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 8 — the sweep terminalizes stranded rows and counts honestly.
// ---------------------------------------------------------------------------

#[test]
fn the_sweep_terminalizes_content_forgotten_rows_and_reports_what_is_left() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    {
        let store = journal(&path)?;
        store.append_admitted(&Builder::at_sequence(1).build()?)?;
        store.append_admitted(&Builder::at_sequence(2).build()?)?;
    }
    {
        // Two rows whose content is gone but whose delivery never settled.
        // Nothing can ever deliver them, and while they are non-terminal they
        // are not deletable either: without the sweep terminalizing them they
        // sit in the queue forever while every receipt reports "nothing left".
        let connection = Connection::open(&path)?;
        connection.execute(
            "UPDATE tdmem_observation_journal_v1 \
             SET payload_bytes = NULL, extensions_json = NULL, \
                 sanitization_receipt_id = NULL, sanitizer_revision = NULL, \
                 source_payload_sha256 = NULL, sanitization_receipt_json = NULL, \
                 content_forgotten_at_micros = ?1",
            params![T0],
        )?;
    }

    let store = journal(&path)?;
    let first = store.sweep_expired(T0 + SECOND, 1)?;
    assert_eq!(first.deliveries_forgotten, 1);
    assert_eq!(
        first.remaining_candidates, 1,
        "the sweep reported no remaining work while a stranded row was queued"
    );

    let second = store.sweep_expired(T0 + 2 * SECOND, 1)?;
    assert_eq!(second.deliveries_forgotten, 1);
    assert_eq!(second.remaining_candidates, 0);

    // Both rows are terminal, each with a terminal receipt of its own.
    let page = store.inspect(&Default::default())?;
    assert_eq!(page.rows.len(), 2);
    for row in &page.rows {
        assert_eq!(row.state, DeliveryStateV1::Forgotten);
        let receipts = store.receipts_for(&row.observation_id)?;
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].outcome, ObservationOutcomeV1::Cancelled);
        assert_eq!(
            receipts[0]
                .provider_effect_summary
                .no_effect_reason
                .as_deref(),
            Some("content_forgotten")
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 9 — the withheld audit has a retention and privacy lifecycle.
// ---------------------------------------------------------------------------

#[test]
fn withheld_records_are_deleted_by_forget_key() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let key = ForgetSourceKeyV1::new("forget:session-9")?;
    store.record_withheld_at(&withheld_at(1, key.as_str())?, T0)?;
    store.record_withheld_at(&withheld_at(2, key.as_str())?, T0 + SECOND)?;
    // A record answering to another subject must survive.
    store.record_withheld_at(&withheld_at(3, "forget:session-other")?, T0 + 2 * SECOND)?;

    assert_eq!(store.verify_forgotten(&key)?.withheld_rows_remaining, 2);
    let receipt = store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "subject_erasure_request".to_owned(),
        requested_at_unix_micros: T0 + 3 * SECOND,
    })?;
    assert_eq!(
        receipt.withheld_rows_deleted, 2,
        "the withheld audit was out of reach of privacy deletion"
    );

    let verification = store.verify_forgotten(&key)?;
    assert_eq!(verification.withheld_rows_remaining, 0);
    assert!(verification.verified);
    assert_eq!(
        store
            .verify_forgotten(&ForgetSourceKeyV1::new("forget:session-other")?)?
            .withheld_rows_remaining,
        1,
        "deletion reached another subject's record"
    );
    Ok(())
}

#[test]
fn withheld_records_age_out_under_a_bounded_sweep() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    store.record_withheld_at(&withheld_at(1, "forget:aged")?, T0)?;
    store.record_withheld_at(&withheld_at(2, "forget:aged")?, T0)?;

    // Inside the audit window nothing is touched.
    let early = store.sweep_expired(T0 + DAY, 8)?;
    assert_eq!(early.withheld_rows_deleted, 0);
    assert_eq!(early.remaining_candidates, 0);

    // Past it, deletion happens under the batch bound and the remainder is
    // reported rather than hidden.
    let aged = T0 + policy().receipt_retention_micros + SECOND;
    let first = store.sweep_expired(aged, 1)?;
    assert_eq!(
        first.withheld_rows_deleted, 1,
        "withheld digests of refused content are never swept"
    );
    assert_eq!(first.remaining_candidates, 1);

    let second = store.sweep_expired(aged, 1)?;
    assert_eq!(second.withheld_rows_deleted, 1);
    assert_eq!(second.remaining_candidates, 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 10 — purged bytes leave the write-ahead log too.
// ---------------------------------------------------------------------------

#[test]
fn forgotten_payload_bytes_do_not_survive_in_the_write_ahead_log() -> TestResult {
    const MARKER: &str = "eyebrow-tarragon-9f31-secret";
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let wal = directory.path().join("journal.sqlite3-wal");

    let store = journal(&path)?;
    let admitted = Builder {
        body: format!("{{\"message\":\"{MARKER}\"}}"),
        ..Builder::at_sequence(1)
    }
    .build()?;
    store.append_admitted(&admitted)?;
    assert!(
        std::fs::read(&wal)?
            .windows(MARKER.len())
            .any(|window| window == MARKER.as_bytes()),
        "the fixture never put the payload in the log, so this proves nothing"
    );

    let key = admitted.privacy.forget_source_key.clone();
    let receipt = store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "subject_erasure_request".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;
    assert!(receipt.wal_truncated);
    let verification = store.verify_forgotten(&key)?;
    assert!(verification.wal_truncated);
    assert!(verification.verified);

    assert!(
        !std::fs::read(&wal)?
            .windows(MARKER.len())
            .any(|window| window == MARKER.as_bytes()),
        "the forgotten payload is still readable in the -wal sidecar"
    );
    Ok(())
}

#[test]
fn a_busy_log_fails_closed_rather_than_reporting_a_complete_deletion() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let store = journal(&path)?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    let key = admitted.privacy.forget_source_key.clone();
    store.forget_source(&ForgetSourceRequestV1 {
        forget_source_key: key.clone(),
        reason: "subject_erasure_request".to_owned(),
        requested_at_unix_micros: T0 + SECOND,
    })?;

    // A reader holding a snapshot pins the log, so it cannot be truncated. The
    // purged pages may still be readable there, and verification must say so
    // instead of reporting a deletion that is not complete on disk.
    let reader = Connection::open(&path)?;
    reader.execute_batch("BEGIN DEFERRED")?;
    let _pinned: i64 = reader.query_row(
        "SELECT COUNT(*) FROM tdmem_observation_journal_v1",
        [],
        |row| row.get(0),
    )?;
    store.append_admitted(
        &Builder {
            forget_source_key: "forget:another-subject".to_owned(),
            ..Builder::at_sequence(2)
        }
        .build()?,
    )?;

    let verification = store.verify_forgotten(&key)?;
    assert!(
        !verification.wal_truncated,
        "a pinned log was reported as truncated"
    );
    assert!(
        !verification.verified,
        "a deletion whose pages may still be in the log was reported verified"
    );
    reader.execute_batch("COMMIT")?;

    // Once the reader lets go, the same query verifies.
    assert!(store.verify_forgotten(&key)?.verified);
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 11 — the wall-clock wrapper is on the port, so it gets a test.
// ---------------------------------------------------------------------------

#[test]
fn record_withheld_stamps_the_wall_clock() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let before = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    // The port method, not the clock-injecting one underneath it.
    ObservationDispatchPortV1::record_withheld(&store, &withheld_at(3, "forget:session-1")?)?;
    let after = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;

    let cursor = store
        .replay_cursor(&stream_key("session-1")?)?
        .ok_or("the wall-clock wrapper never advanced the cursor")?;
    assert_eq!(cursor.last_admitted_sequence, SourceSequenceV1(3));
    assert_eq!(cursor.last_disposition, ReplayDispositionV1::Withheld);
    assert!(
        (before..=after).contains(&cursor.updated_at_unix_micros),
        "the withheld record was not stamped with the wall clock: {} not in {before}..={after}",
        cursor.updated_at_unix_micros
    );
    // A stamp that far in the future would let the retention sweep never reach
    // it, so the same instant has to reach the audit row.
    let store_key = ForgetSourceKeyV1::new("forget:session-1")?;
    assert_eq!(
        store.verify_forgotten(&store_key)?.withheld_rows_remaining,
        1
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Finding 12 — a disagreeing stored digest is corruption, not an outcome.
// ---------------------------------------------------------------------------

#[test]
fn a_stored_payload_digest_that_disagrees_with_its_key_fails_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.sqlite3");
    let admitted = Builder::at_sequence(1).build()?;
    {
        let store = journal(&path)?;
        store.append_admitted(&admitted)?;
    }
    {
        // No caller can reach this state — the key is derived over
        // `payload_sha256` and re-derived at admission — so it takes raw SQL to
        // produce the only condition under which the store may hold one key
        // over two payloads.
        let connection = Connection::open(&path)?;
        connection.execute(
            "UPDATE tdmem_observation_journal_v1 SET payload_sha256 = ?1",
            params![digest_hex(b"not-the-admitted-payload")],
        )?;
    }

    let store = journal(&path)?;
    let error = store
        .append_admitted(&admitted)
        .err()
        .ok_or("a corrupt row was reported as a plain duplicate")?;
    assert!(
        matches!(
            error,
            ObservationJournalError::Corrupt {
                table: "tdmem_observation_journal_v1",
                field: "payload_sha256"
            }
        ),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[test]
fn a_clean_duplicate_is_still_an_ordinary_outcome() -> TestResult {
    // The corruption path above must not have turned every re-append into an
    // error: an honest replay is still a duplicate.
    let directory = tempfile::tempdir()?;
    let store = journal(&directory.path().join("journal.sqlite3"))?;
    let admitted = Builder::at_sequence(1).build()?;
    store.append_admitted(&admitted)?;
    assert!(matches!(
        store.append_admitted(&admitted)?,
        AppendOutcomeV1::DuplicateIdempotencyKey {
            state: DeliveryStateV1::Pending,
            ..
        }
    ));
    Ok(())
}
