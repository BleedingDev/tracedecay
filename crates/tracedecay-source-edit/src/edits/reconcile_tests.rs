use std::fs;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

use tracedecay_contracts::{
    CancellationSignal, CancellationStage, Deadline, EffectTermination, IdempotencyKey,
    SourceEditReconciliationDispositionV1, SourceEditReconciliationRequestV1,
    SourceEditRollbackRequestV1, source_edit_operation, source_edit_rollback_operation,
};

use tracedecay_domain::UtcMicros;
use tracedecay_domain::errors::{Result, TraceDecayError};

use super::test_support::{EffectUnknownFixture, effect_unknown_fixture};
use crate::journal::{SourceEditDurability, SourceEditJournalStateV1};
use crate::outcome::SourceEditDurableOutcomeV1;
use crate::{
    SourceEditApplicationResult, SourceEditEffectControlV1, SourceEditOutcome, execute_source_edit,
    execute_source_edit_rollback, reconcile_source_edit_effect_unknown_with_control,
};

#[cfg(windows)]
const FILE_SHARE_READ: u32 = 0x0000_0001;

async fn recover_fixture(fixture: &EffectUnknownFixture) -> Result<SourceEditApplicationResult> {
    let operation = source_edit_operation(fixture.request.edit.kind()).unwrap();
    execute_source_edit(
        &fixture.graph,
        &fixture.code_graph,
        &operation,
        fixture.request.clone(),
        &fixture.authorization,
    )
    .await
}

fn rollback_request(
    fixture: &EffectUnknownFixture,
    recovered: &SourceEditApplicationResult,
    key: &str,
) -> SourceEditRollbackRequestV1 {
    let effect = recovered.effect.as_ref().unwrap();
    SourceEditRollbackRequestV1 {
        context: fixture.request.context.clone(),
        authority: fixture.request.authority.clone(),
        effect_id: effect.effect_id.clone(),
        original_idempotency_key: fixture.request.idempotency_key.clone(),
        idempotency_key: IdempotencyKey::new(key).unwrap(),
        original_input_digest: effect.receipt.input_digest.clone(),
        expected_state: effect.receipt.committed_state.clone().unwrap(),
        proof: fixture.request.proof.clone(),
        observed_at: fixture.request.observed_at,
    }
}

async fn assert_rollback_replays(
    fixture: &EffectUnknownFixture,
    recovered: &SourceEditApplicationResult,
    key: &str,
) {
    let request = rollback_request(fixture, recovered, key);
    let operation = source_edit_rollback_operation().unwrap();
    let rolled_back = execute_source_edit_rollback(
        &fixture.graph,
        &operation,
        request.clone(),
        &fixture.authorization,
    )
    .await
    .unwrap();
    assert_eq!(
        rolled_back.effect.as_ref().unwrap().receipt.outcome,
        EffectTermination::Completed
    );
    fixture.assert_preimages();
    let replay =
        execute_source_edit_rollback(&fixture.graph, &operation, request, &fixture.authorization)
            .await
            .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.effect, rolled_back.effect);
    fixture.assert_preimages();
}

#[tokio::test]
async fn prepared_restart_with_preimages_restores_partial_bytes_before_another_edit() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_partial_postimage();

    let operation = source_edit_operation(fixture.request.edit.kind()).unwrap();
    let result = execute_source_edit(
        &fixture.graph,
        &fixture.code_graph,
        &operation,
        fixture.request.clone(),
        &fixture.authorization,
    )
    .await
    .unwrap();

    assert!(result.replayed);
    assert_eq!(
        result.effect.unwrap().receipt.outcome,
        EffectTermination::Failed
    );
    fixture.assert_preimages();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(fixture.permission_preserving_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }
}

#[tokio::test]
async fn prepared_restart_with_completed_edit_rolls_forward_and_preserves_bytes() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_all_postimages();

    let result = recover_fixture(&fixture).await.unwrap();

    assert!(result.replayed);
    let effect = result.effect.as_ref().unwrap();
    assert_eq!(effect.receipt.outcome, EffectTermination::Completed);
    fixture.assert_postimages();

    let replay = recover_fixture(&fixture).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.effect.as_ref().unwrap(), effect);
    fixture.assert_postimages();

    let rollback = rollback_request(&fixture, &result, "source-edit.prepared-recovery.foreign");
    let rollback_operation = source_edit_rollback_operation().unwrap();
    let foreign_path = fixture.project.path().join("src/b.rs");
    fs::write(&foreign_path, b"foreign bytes\n").unwrap();
    let refused = execute_source_edit_rollback(
        &fixture.graph,
        &rollback_operation,
        rollback,
        &fixture.authorization,
    )
    .await
    .unwrap();
    assert_eq!(
        refused.effect.as_ref().unwrap().receipt.outcome,
        EffectTermination::Failed
    );
    assert_eq!(
        refused.outcome.message(),
        "source edit rollback refused stale or foreign workspace bytes"
    );
    assert_eq!(fs::read(&foreign_path).unwrap(), b"foreign bytes\n");
    fixture.write_all_postimages();
    assert_rollback_replays(&fixture, &result, "source-edit.prepared-recovery.rollback").await;
}

#[tokio::test]
async fn prepared_roll_forward_keeps_the_journal_when_inverse_retention_fails() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_all_postimages();
    let durability = SourceEditDurability::for_graph(&fixture.graph);
    let journal_path = durability.journal_path();
    let before = fs::read(&journal_path).unwrap();
    let receipt_path = durability
        .receipt_path(&fixture.request.idempotency_key)
        .unwrap();
    let receipt_before = fs::read(&receipt_path).unwrap();
    let blocker = journal_path.parent().unwrap().join("rollback-records");
    fs::write(&blocker, b"block the inverse directory").unwrap();

    let error = recover_fixture(&fixture)
        .await
        .err()
        .expect("inverse retention must fail");
    assert!(matches!(error, TraceDecayError::Config { .. }));
    assert_eq!(fs::read(&journal_path).unwrap(), before);
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    fixture.assert_postimages();
    fs::remove_file(blocker).unwrap();

    let recovered = recover_fixture(&fixture).await.unwrap();
    assert!(!journal_path.exists());
    assert_rollback_replays(&fixture, &recovered, "source-edit.inverse-failure.rollback").await;
}

#[tokio::test]
async fn prepared_roll_forward_keeps_the_inverse_when_receipt_publication_fails() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_all_postimages();
    let durability = SourceEditDurability::for_graph(&fixture.graph);
    let journal_path = durability.journal_path();
    let before = fs::read(&journal_path).unwrap();
    let receipt_path = durability
        .receipt_path(&fixture.request.idempotency_key)
        .unwrap();
    let receipt_before = fs::read(&receipt_path).unwrap();
    #[cfg(not(windows))]
    let permissions = fs::metadata(receipt_path.parent().unwrap())
        .unwrap()
        .permissions();
    #[cfg(not(windows))]
    {
        let mut denied = permissions.clone();
        denied.set_readonly(true);
        fs::set_permissions(receipt_path.parent().unwrap(), denied).unwrap();
    }
    #[cfg(windows)]
    let held_receipt = {
        fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .open(&receipt_path)
            .unwrap()
    };
    let outcome = recover_fixture(&fixture).await;
    #[cfg(not(windows))]
    fs::set_permissions(receipt_path.parent().unwrap(), permissions).unwrap();
    #[cfg(windows)]
    drop(held_receipt);

    let error = outcome.err().expect("receipt publication must fail");
    assert!(matches!(error, TraceDecayError::Config { .. }));
    assert_eq!(fs::read(&journal_path).unwrap(), before);
    assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    fixture.assert_postimages();
    let journal = durability.load_journal().unwrap().unwrap();
    assert!(
        durability
            .load_rollback_record(&journal.effect_id)
            .unwrap()
            .is_some()
    );

    let recovered = recover_fixture(&fixture).await.unwrap();
    assert!(!journal_path.exists());
    assert_rollback_replays(&fixture, &recovered, "source-edit.receipt-failure.rollback").await;
}

#[tokio::test]
async fn prepared_roll_forward_keeps_the_inverse_when_journal_cleanup_fails() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_all_postimages();
    let durability = SourceEditDurability::for_graph(&fixture.graph);
    let journal_path = durability.journal_path();
    let before = fs::read(&journal_path).unwrap();
    let root = journal_path.parent().unwrap();
    fs::create_dir_all(root.join("rollback-records")).unwrap();
    #[cfg(not(windows))]
    let permissions = fs::metadata(root).unwrap().permissions();
    #[cfg(not(windows))]
    {
        let mut denied = permissions.clone();
        denied.set_readonly(true);
        fs::set_permissions(root, denied).unwrap();
    }
    #[cfg(windows)]
    let held_journal = {
        fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .open(&journal_path)
            .unwrap()
    };
    let outcome = recover_fixture(&fixture).await;
    #[cfg(not(windows))]
    fs::set_permissions(root, permissions).unwrap();
    #[cfg(windows)]
    drop(held_journal);

    let error = outcome.err().expect("journal cleanup must fail");
    assert!(
        matches!(error, TraceDecayError::Config { message } if message.starts_with("remove source edit journal:"))
    );
    assert_eq!(fs::read(&journal_path).unwrap(), before);
    fixture.assert_postimages();
    let journal = durability.load_journal().unwrap().unwrap();
    assert!(
        durability
            .load_rollback_record(&journal.effect_id)
            .unwrap()
            .is_some()
    );
    let receipt = durability
        .load_receipt(&fixture.request.idempotency_key)
        .unwrap()
        .unwrap();
    assert_eq!(receipt.effect.receipt.outcome, EffectTermination::Completed);

    let recovered = recover_fixture(&fixture).await.unwrap();
    assert!(!journal_path.exists());
    assert_rollback_replays(&fixture, &recovered, "source-edit.cleanup-failure.rollback").await;
}

#[tokio::test]
async fn applied_restart_retains_the_exact_move_inverse() {
    let fixture = effect_unknown_fixture().await;
    fixture.write_all_postimages();
    let durability = SourceEditDurability::for_graph(&fixture.graph);
    let mut journal = durability.load_journal().unwrap().unwrap();
    let outcome = SourceEditOutcome::Reconciled {
        success: true,
        message: "completed move awaiting durable receipt".to_owned(),
    };
    journal.state = SourceEditJournalStateV1::Applied {
        outcome: SourceEditDurableOutcomeV1::from_live(&journal.request.operation, &outcome),
        committed_state: journal.predicted_state.clone().unwrap(),
        ended_at: UtcMicros(4),
        control_observation: None,
        verification_state: None,
    };
    durability.persist_journal(&journal).unwrap();

    let recovered = recover_fixture(&fixture).await.unwrap();
    assert_eq!(
        recovered.effect.as_ref().unwrap().receipt.outcome,
        EffectTermination::Completed
    );
    fixture.assert_postimages();
    assert_rollback_replays(
        &fixture,
        &recovered,
        "source-edit.applied-recovery.rollback",
    )
    .await;
}

/// The write never landed: after the fixture the worktree still holds every
/// preimage. Recovery rolls cleanly back (a no-op restore) and records the
/// failure without disturbing any byte.
#[tokio::test]
async fn prepared_restart_with_untouched_preimages_rolls_back_cleanly() {
    let fixture = effect_unknown_fixture().await;

    let operation = source_edit_operation(fixture.request.edit.kind()).unwrap();
    let result = execute_source_edit(
        &fixture.graph,
        &fixture.code_graph,
        &operation,
        fixture.request.clone(),
        &fixture.authorization,
    )
    .await
    .unwrap();

    assert!(result.replayed);
    assert_eq!(
        result.effect.unwrap().receipt.outcome,
        EffectTermination::Failed
    );
    fixture.assert_preimages();
}

#[tokio::test]
async fn reconciliation_before_admission_cancellation_is_durable_and_replayable() {
    let fixture = effect_unknown_fixture().await;
    let effect = fixture.result.effect.as_ref().unwrap();
    let reconciliation = SourceEditReconciliationRequestV1 {
        context: fixture.request.context.clone(),
        authority: fixture.request.authority.clone(),
        kind: fixture.request.edit.kind(),
        effect_id: effect.effect_id.clone(),
        idempotency_key: fixture.request.idempotency_key.clone(),
        attempt_idempotency_key: IdempotencyKey::new("source-edit-reconciliation-attempt.fixture")
            .unwrap(),
        input_digest: effect.receipt.input_digest.clone(),
        disposition: SourceEditReconciliationDispositionV1::ConfirmRolledBack,
        proof: fixture.request.proof.clone(),
        observed_at: UtcMicros(4),
    };
    let cancellation = CancellationSignal::active("cancel.reconcile.before-admission").unwrap();
    assert!(cancellation.cancel(UtcMicros(5)));
    let control =
        SourceEditEffectControlV1::new(Deadline::new(UtcMicros(i64::MAX)).unwrap(), cancellation);

    let result = reconcile_source_edit_effect_unknown_with_control(
        &fixture.graph,
        reconciliation.clone(),
        &fixture.authorization,
        &control,
    )
    .await
    .unwrap();
    assert_eq!(
        result.effect.as_ref().unwrap().receipt.outcome,
        EffectTermination::Cancelled
    );
    assert_eq!(
        result
            .effect
            .as_ref()
            .unwrap()
            .execution
            .cancellation
            .as_ref()
            .unwrap()
            .stage,
        CancellationStage::BeforeAdmission
    );
    fixture.assert_preimages();

    let replay = reconcile_source_edit_effect_unknown_with_control(
        &fixture.graph,
        reconciliation,
        &fixture.authorization,
        &control,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.effect.unwrap().receipt.outcome,
        EffectTermination::Cancelled
    );
}
