use crate::common;

use tracedecay_contracts::{
    ApplicationContractError, EffectId, EffectReceipt, EffectResult, EffectTermination,
    IdempotencyKey, OperationReceipt, OperationTermination, ReconciliationState,
};
use tracedecay_domain::{RetrievalAnchorId, UtcMicros};
use tracedecay_tool_catalog::EffectClass;

fn effect_receipt(outcome: EffectTermination) -> EffectReceipt {
    let operation = common::operation();
    let context = common::context(&operation);
    EffectReceipt {
        operation: operation.use_case_id().clone(),
        request_id: context.request_id().clone(),
        actor: context.actor().clone(),
        scope: context.scope().clone(),
        effect_class: EffectClass::SourceEdit,
        idempotency_key: IdempotencyKey::new("idempotency.fixture").unwrap(),
        input_digest: common::digest(common::SHA256_A),
        expected_state: common::digest(common::SHA256_A),
        policy_digest: common::digest(common::SHA256_B),
        configuration_digest: common::digest(common::SHA256_A),
        catalog_digest: common::digest(common::SHA256_B),
        privacy_digest: common::digest(common::SHA256_A),
        outcome,
        committed_state: None,
        external_proof: None,
    }
}

#[test]
fn effect_unknown_stays_in_an_admitted_effect_receipt() {
    let operation = common::operation();
    let context = common::context(&operation);
    let execution = OperationReceipt {
        started_at: UtcMicros(2),
        ended_at: UtcMicros(3),
        effective_deadline: context.deadline().clone(),
        cancellation: None,
        budget: Default::default(),
        termination: OperationTermination::EffectUnknown,
    };
    let receipt = effect_receipt(EffectTermination::EffectUnknown);
    assert!(
        EffectResult::new(
            EffectId::new("effect.mismatched-state.fixture").unwrap(),
            EffectClass::SourceEdit,
            IdempotencyKey::new("idempotency.fixture").unwrap(),
            common::authority(&context),
            common::digest(common::SHA256_B),
            execution.clone(),
            ReconciliationState::Pending,
            receipt.clone(),
            None::<()>,
        )
        .is_err()
    );

    let effect = EffectResult::new(
        EffectId::new("effect.fixture").unwrap(),
        EffectClass::SourceEdit,
        IdempotencyKey::new("idempotency.fixture").unwrap(),
        common::authority(&context),
        common::digest(common::SHA256_A),
        execution,
        ReconciliationState::Pending,
        receipt,
        None::<()>,
    )
    .unwrap();

    assert_eq!(effect.receipt.outcome, EffectTermination::EffectUnknown);
    assert_eq!(effect.reconciliation, ReconciliationState::Pending);
    assert!(effect.payload.is_none());
}

#[test]
fn no_change_is_a_completed_admitted_result_without_commit_proof() {
    let operation = common::operation();
    let context = common::context(&operation);
    let receipt = effect_receipt(EffectTermination::NoChange);
    receipt.validate().expect("verified no-change receipt");

    let encoded = serde_json::to_value(&receipt).expect("encode no-change receipt");
    assert_eq!(encoded["outcome"], "no_change");
    assert!(encoded["committed_state"].is_null());
    assert!(encoded["external_proof"].is_null());
    let decoded: EffectReceipt = serde_json::from_value(encoded).expect("decode no-change receipt");
    assert_eq!(decoded, receipt);
    decoded.validate().expect("decoded no-change receipt");

    let execution = OperationReceipt::completed(
        UtcMicros(2),
        UtcMicros(3),
        context.deadline().clone(),
        Default::default(),
    )
    .unwrap();
    let effect_id = EffectId::new("effect.no-change.fixture").unwrap();
    let authority = common::authority(&context);
    let effect = EffectResult::new(
        effect_id.clone(),
        receipt.effect_class,
        receipt.idempotency_key.clone(),
        authority.clone(),
        receipt.expected_state.clone(),
        execution.clone(),
        ReconciliationState::Reconciled,
        decoded,
        None::<()>,
    )
    .expect("no-change result has completed execution");

    assert_eq!(effect.effect_id, effect_id);
    assert_eq!(effect.authority, authority);
    assert_eq!(effect.idempotency_key, receipt.idempotency_key);
    assert_eq!(effect.expected_state, receipt.expected_state);
    assert_eq!(effect.receipt, receipt);
    assert_eq!(effect.execution, execution);
    assert_eq!(effect.reconciliation, ReconciliationState::Reconciled);
}

#[test]
fn no_change_rejects_committed_state_or_external_proof() {
    for (committed, external) in [(true, false), (false, true), (true, true)] {
        let mut receipt = effect_receipt(EffectTermination::NoChange);
        receipt.committed_state = committed.then(|| common::digest(common::SHA256_B));
        receipt.external_proof =
            external.then(|| RetrievalAnchorId::new("anchor.fixture").unwrap());

        assert!(matches!(
            receipt.validate(),
            Err(ApplicationContractError::Inconsistent {
                field: "no-change effect receipt proof",
            })
        ));
    }
}

#[test]
fn completed_still_requires_committed_state_or_external_proof() {
    let receipt = effect_receipt(EffectTermination::Completed);
    assert!(matches!(
        receipt.validate(),
        Err(ApplicationContractError::Inconsistent {
            field: "completed effect receipt proof",
        })
    ));

    for (committed, external) in [(true, false), (false, true), (true, true)] {
        let mut receipt = receipt.clone();
        receipt.committed_state = committed.then(|| common::digest(common::SHA256_B));
        receipt.external_proof =
            external.then(|| RetrievalAnchorId::new("anchor.fixture").unwrap());
        receipt.validate().expect("completed receipt retains proof");
    }
}
