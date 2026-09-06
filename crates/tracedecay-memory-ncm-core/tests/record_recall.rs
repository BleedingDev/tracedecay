#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Acceptance coverage for stable records, correction lineage, and read-only recall.

use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::centers::write::{WriteInput, WriteOutcome, WriteParams};
use tracedecay_memory_ncm_core::recall::{
    RecallCandidate, RecallConfidence, RecallLayer, RecallOutput, RecallPolicy, recall,
};
use tracedecay_memory_ncm_core::records::{RecordInput, RecordState, RecordTable, Support};
use tracedecay_memory_ncm_core::{
    AffectVector, CenterSlot, CoreError, LogicalTick, NcmConfig, RecordId, SourceId,
};

const STM_DIM: usize = 16;
const LTM_DIM: usize = 64;
const VALUE_DIM: usize = 128;
const CONTEXT_DIM: usize = 16;

fn config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.stm.n_centers = 4;
    config.ltm.n_centers = 4;
    config.max_center_support = 8;
    config
}

fn unit(dimension: usize, axis: usize, sign: f32) -> Vec<f32> {
    let mut vector = vec![0.0; dimension];
    vector[axis] = sign;
    vector
}

fn as_array<const N: usize>(values: &[f32]) -> [f32; N] {
    values.try_into().expect("fixture dimension")
}

fn record_input(source: &str, key: &str, value: &str, axis: usize) -> RecordInput {
    RecordInput {
        source: SourceId(source.to_owned()),
        key_text: key.to_owned(),
        value_text: value.to_owned(),
        created_tick: LogicalTick(7),
        ltm_key: unit(LTM_DIM, axis, 1.0),
        stm_key: unit(STM_DIM, axis, 1.0),
        value_vec: unit(VALUE_DIM, axis, 1.0),
        affect: AffectVector::neutral(),
    }
}

fn write_record(
    centers: &mut MemoryCenters,
    key: &[f32],
    ltm_key: &[f32],
    record: RecordId,
    params: &WriteParams,
    support: &mut Support,
) -> CenterSlot {
    let input = WriteInput {
        key,
        ltm_key: as_array(ltm_key),
        value: [0.25; VALUE_DIM],
        affect: AffectVector::neutral(),
        intensity: 1.0,
        context: as_array(&unit(CONTEXT_DIM, 0, 1.0)),
        terrain: [0.0; 3],
        record,
        age: 0,
    };
    let outcome = centers.write(input, params).expect("center write");
    for index in 0..centers.config.n_centers {
        if centers.active[index] {
            let slot = centers.slot(index).expect("slot");
            support
                .set_support(slot, &centers.support[index])
                .expect("support snapshot");
        }
    }
    match outcome {
        WriteOutcome::Created { slot } | WriteOutcome::Reinforced { slot, .. } => slot,
        WriteOutcome::Ignored | WriteOutcome::CapacityExhausted => {
            panic!("fixture write did not create support")
        }
    }
}

fn candidates(output: RecallOutput) -> (Vec<RecallCandidate>, bool, bool) {
    match output {
        RecallOutput::Candidates {
            candidates,
            truncated,
            margin_satisfied,
        } => (candidates, truncated, margin_satisfied),
        RecallOutput::Empty => panic!("expected recall candidates"),
    }
}

#[test]
fn same_key_distinct_sources_remain_visible_and_correction_keeps_lineage() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let first = records
        .insert(record_input("source-a", "release", "Friday", 0))
        .expect("first record");
    let second = records
        .insert(record_input("source-b", "release", "Monday", 0))
        .expect("second record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 11).expect("STM");
    let mut ltm = MemoryCenters::new(config.ltm.clone(), 12).expect("LTM");
    let mut support = Support::new(&config);
    let stm_params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    let ltm_params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Ltm);
    let stm_key = unit(STM_DIM, 0, 1.0);
    let ltm_key = unit(LTM_DIM, 0, 1.0);
    write_record(
        &mut stm,
        &stm_key,
        &ltm_key,
        first,
        &stm_params,
        &mut support,
    );
    write_record(
        &mut stm,
        &stm_key,
        &ltm_key,
        second,
        &stm_params,
        &mut support,
    );
    write_record(
        &mut ltm,
        &ltm_key,
        &ltm_key,
        first,
        &ltm_params,
        &mut support,
    );
    write_record(
        &mut ltm,
        &ltm_key,
        &ltm_key,
        second,
        &ltm_params,
        &mut support,
    );

    let policy = RecallPolicy {
        min_margin: 0.01,
        ..RecallPolicy::default()
    };
    let (found, truncated, margin_satisfied) = candidates(
        recall(
            &stm_key,
            &ltm_key,
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &policy,
            4,
        )
        .expect("recall"),
    );
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].record_id, first);
    assert_eq!(found[1].record_id, second);
    assert_eq!(found[0].source, SourceId("source-a".to_owned()));
    assert_eq!(found[1].source, SourceId("source-b".to_owned()));
    assert_eq!(found[0].value_text, "Friday");
    assert_eq!(found[1].value_text, "Monday");
    assert_eq!(found[0].layer, RecallLayer::Both);
    assert_eq!(found[1].layer, RecallLayer::Both);
    assert!(!truncated);
    assert!(!margin_satisfied);
    for candidate in &found {
        assert!(candidate.activation > policy.min_activation);
        assert!(!candidate.support_slots.is_empty());
        assert!(candidate.support_slots.iter().any(|slot| {
            support
                .support_for(*slot)
                .expect("returned slot support")
                .contains(&candidate.record_id)
        }));
        assert!(candidate.distance_components.cosine_d2.abs() < 1e-6);
        assert_eq!(candidate.distance_components.minkowski, None);
        let RecallConfidence::Uncalibrated(confidence) = candidate.confidence;
        assert!((0.0..=1.0).contains(&confidence));
    }

    let evidence = "ab".repeat(32);
    records
        .supersede(first, second, evidence.clone())
        .expect("explicit correction");
    let (corrected, _, _) = candidates(
        recall(
            &stm_key,
            &ltm_key,
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &policy,
            4,
        )
        .expect("corrected recall"),
    );
    assert_eq!(
        corrected[0].validity,
        RecordState::Superseded {
            by: second,
            evidence_sha256: evidence,
        }
    );
    assert_eq!(corrected[1].validity, RecordState::Valid);
    assert!(matches!(
        records.supersede(second, first, "cd".repeat(32)),
        Err(CoreError::InvalidState(_))
    ));
}

#[test]
fn unrelated_singleton_is_empty_not_softmax_confident_or_unavailable() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let id = records
        .insert(record_input("source", "known", "fact", 0))
        .expect("record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 21).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 22).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    write_record(
        &mut stm,
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        id,
        &params,
        &mut support,
    );

    let available = recall(
        &unit(STM_DIM, 0, -1.0),
        &unit(LTM_DIM, 0, -1.0),
        &unit(CONTEXT_DIM, 0, -1.0),
        &stm,
        &ltm,
        0.0,
        &records,
        &support,
        &RecallPolicy::default(),
        4,
    );
    assert_eq!(available, Ok(RecallOutput::Empty));
    let unavailable: Result<RecallOutput, CoreError> =
        Err(CoreError::InvalidState("namespace unavailable".to_owned()));
    assert_ne!(available, unavailable);
}

#[test]
fn fabricated_or_unknown_citation_fails_closed() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let id = records
        .insert(record_input("source", "key", "value", 0))
        .expect("record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 31).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 32).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    let slot = write_record(
        &mut stm,
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        id,
        &params,
        &mut support,
    );

    let fabricated = RecordId(999);
    let index = usize::try_from(slot.index).expect("index");
    stm.support[index].push(fabricated);
    support.record(slot, fabricated).expect("mutant support");
    let error = recall(
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        &unit(CONTEXT_DIM, 0, 1.0),
        &stm,
        &ltm,
        0.0,
        &records,
        &support,
        &RecallPolicy::default(),
        4,
    )
    .expect_err("fabricated record must fail closed");
    assert_eq!(error, CoreError::UnknownRecord(fabricated));
}

#[test]
fn slot_reuse_makes_old_support_stale_and_old_record_absent() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let old_id = records
        .insert(record_input("old-source", "old", "old value", 0))
        .expect("old record");
    let new_id = records
        .insert(record_input("new-source", "new", "new value", 1))
        .expect("new record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 41).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 42).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    let old_slot = write_record(
        &mut stm,
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        old_id,
        &params,
        &mut support,
    );
    stm.deactivate_slot(old_slot).expect("prune old slot");
    let new_slot = write_record(
        &mut stm,
        &unit(STM_DIM, 1, 1.0),
        &unit(LTM_DIM, 1, 1.0),
        new_id,
        &params,
        &mut support,
    );
    assert_eq!(new_slot.index, old_slot.index);
    assert!(new_slot.incarnation > old_slot.incarnation);
    assert_eq!(support.support_for(old_slot), Err(CoreError::StaleHandle));

    let (found, _, _) = candidates(
        recall(
            &unit(STM_DIM, 1, 1.0),
            &unit(LTM_DIM, 1, 1.0),
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &RecallPolicy::default(),
            4,
        )
        .expect("new recall"),
    );
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].record_id, new_id);
    assert!(!found.iter().any(|candidate| candidate.record_id == old_id));
}

#[test]
fn merge_support_unions_both_sides_without_dropping_ids() {
    let config = config();
    let mut support = Support::new(&config);
    let kept = CenterSlot {
        layer: tracedecay_memory_ncm_core::types::Layer::Stm,
        index: 0,
        incarnation: 1,
    };
    let removed = CenterSlot {
        layer: tracedecay_memory_ncm_core::types::Layer::Stm,
        index: 1,
        incarnation: 1,
    };
    support.record(kept, RecordId(1)).expect("kept");
    support.record(removed, RecordId(2)).expect("removed");
    support.merge_support(kept, removed).expect("merge");
    assert_eq!(
        support.support_for(kept),
        Ok(&[RecordId(1), RecordId(2)][..])
    );
    assert_eq!(support.support_for(removed), Ok(&[RecordId(2)][..]));
}

#[test]
fn oversize_record_is_rejected_before_identity_or_storage_changes() {
    let mut config = config();
    config.max_record_bytes = 3;
    let mut records = RecordTable::new(&config);
    let error = records
        .insert(record_input("source", "ab", "cd", 0))
        .expect_err("oversize record");
    assert_eq!(error, CoreError::BudgetExceeded("record text"));
    assert!(records.is_empty());
    let id = records
        .insert(record_input("source", "a", "b", 0))
        .expect("valid record");
    assert_eq!(id, RecordId(1));
    assert_eq!(records.len(), 1);
}

#[test]
fn recall_budget_truncates_candidate_prefix_without_truncating_text() {
    let mut config = config();
    config.max_recall_bytes = "keyone".len() + "valueone".len();
    let mut records = RecordTable::new(&config);
    let first = records
        .insert(record_input("source-a", "keyone", "valueone", 0))
        .expect("first");
    let second = records
        .insert(record_input("source-b", "keytwo", "valuetwo", 0))
        .expect("second");
    let mut stm = MemoryCenters::new(config.stm.clone(), 51).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 52).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    let key = unit(STM_DIM, 0, 1.0);
    let ltm_key = unit(LTM_DIM, 0, 1.0);
    write_record(&mut stm, &key, &ltm_key, first, &params, &mut support);
    write_record(&mut stm, &key, &ltm_key, second, &params, &mut support);

    let (found, truncated, _) = candidates(
        recall(
            &key,
            &ltm_key,
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &RecallPolicy::default(),
            4,
        )
        .expect("recall"),
    );
    assert!(truncated);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].record_id, first);
    assert_eq!(found[0].key_text, "keyone");
    assert_eq!(found[0].value_text, "valueone");
    assert_eq!(
        records.get(second).expect("second record").value_text,
        "valuetwo"
    );
}

#[test]
fn deleted_records_are_never_returned() {
    let config = config();
    let source = SourceId("source".to_owned());
    let mut records = RecordTable::new(&config);
    let id = records
        .insert(record_input(&source.0, "key", "value", 0))
        .expect("record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 61).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 62).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    write_record(
        &mut stm,
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        id,
        &params,
        &mut support,
    );
    assert_eq!(records.mark_deleted(&source, 9), 1);
    assert_eq!(
        recall(
            &unit(STM_DIM, 0, 1.0),
            &unit(LTM_DIM, 0, 1.0),
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &RecallPolicy::default(),
            4,
        ),
        Ok(RecallOutput::Empty)
    );
}

#[test]
fn one_hundred_recalls_leave_center_digests_unchanged() {
    let config = config();
    let mut records = RecordTable::new(&config);
    let id = records
        .insert(record_input("source", "key", "value", 0))
        .expect("record");
    let mut stm = MemoryCenters::new(config.stm.clone(), 71).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 72).expect("LTM");
    let mut support = Support::new(&config);
    let params = WriteParams::for_layer(&config, tracedecay_memory_ncm_core::types::Layer::Stm);
    write_record(
        &mut stm,
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        id,
        &params,
        &mut support,
    );
    let before = serde_json::to_vec(&(&stm, &ltm)).expect("digest serialization");
    for _ in 0..100 {
        let output = recall(
            &unit(STM_DIM, 0, 1.0),
            &unit(LTM_DIM, 0, 1.0),
            &unit(CONTEXT_DIM, 0, 1.0),
            &stm,
            &ltm,
            0.0,
            &records,
            &support,
            &RecallPolicy::default(),
            4,
        )
        .expect("read-only recall");
        assert!(matches!(output, RecallOutput::Candidates { .. }));
    }
    let after = serde_json::to_vec(&(&stm, &ltm)).expect("digest serialization");
    assert_eq!(before, after);
}

#[test]
fn operation_bounds_reject_more_than_sixteen_centers_or_candidates() {
    let config = config();
    let records = RecordTable::new(&config);
    let stm = MemoryCenters::new(config.stm.clone(), 81).expect("STM");
    let ltm = MemoryCenters::new(config.ltm.clone(), 82).expect("LTM");
    let support = Support::new(&config);
    let error = recall(
        &unit(STM_DIM, 0, 1.0),
        &unit(LTM_DIM, 0, 1.0),
        &unit(CONTEXT_DIM, 0, 1.0),
        &stm,
        &ltm,
        0.0,
        &records,
        &support,
        &RecallPolicy::default(),
        17,
    )
    .expect_err("top-k bound");
    assert_eq!(error, CoreError::BudgetExceeded("recall candidates"));
}
