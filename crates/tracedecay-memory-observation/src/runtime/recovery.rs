//! Recovery: decide, once per provider incarnation, whether the journal may
//! deliver into the provider state that came back.
//!
//! The decision is a pure function of two durable facts and one piece of
//! provider evidence: the host's persisted recovery record, the journal's
//! unacknowledged frontier, and the checkpoint the readiness handshake proved.
//! The runtime owns no store, no clock, and no transport — the port is the
//! journal, and the instant is the caller's.
//!
//! # Why the refusal is written before it is reported
//!
//! An incompatible provider state is recorded *through the port* before the
//! plan is returned, so a host that crashes between assessing and acting still
//! finds the consumed attempt on the next pass. Without that, a crash loop
//! would reset the bounded repair path on every restart and the escalation to
//! an operator would never arrive. The write is insert-once for the
//! assessment's own identity, so re-submitting the same logical assessment —
//! after an ambiguous result, a crash, or a racing dispatcher — does not spend
//! a second attempt.
//!
//! # Why the caller's bound travels with it
//!
//! Recovery runs inside a delivery attempt, against the same journal a
//! dispatcher is writing to, so both the journal mutex and SQLite can block it.
//! Cancellation and the deadline are therefore checked before the reads and
//! again before the single mutating write, and every port call is handed the
//! budget that is actually left. An assessment that runs out stops with a typed
//! terminal and consumes nothing.

use crate::recovery::{
    HostRecoveryStateV1, ObservationRecoveryPortV1, ProviderCheckpointV1, ProviderReplayPositionV1,
    RecoveryAssessmentIdV1, RecoveryBudgetV1, RecoveryControlV1, RecoveryPlanV1,
    RecoveryRefusalWriteV1, RecoveryTargetKeyV1, StateIncompatibilityV1, UnacknowledgedFrontierV1,
};

use super::error::ObservationRuntimeError;

/// Assesses one provider incarnation against the journal.
#[derive(Debug)]
pub struct RecoveryRuntimeV1<'a, P: ?Sized> {
    port: &'a P,
    budget: RecoveryBudgetV1,
}

impl<'a, P> RecoveryRuntimeV1<'a, P>
where
    P: ObservationRecoveryPortV1 + ?Sized,
{
    /// Binds one recovery port to one validated bounded repair budget.
    pub fn new(port: &'a P, budget: RecoveryBudgetV1) -> Result<Self, ObservationRuntimeError> {
        budget.validate()?;
        Ok(Self { port, budget })
    }

    /// Decides what recovery this provider incarnation needs.
    ///
    /// A converged or replayable outcome re-pins the accepted provider state
    /// identity and clears the bounded repair counter. An incompatible outcome
    /// consumes at most one attempt from the budget — the same assessment
    /// submitted twice consumes one in total — and proposes no delivery at all.
    pub fn assess(
        &self,
        checkpoint: &ProviderCheckpointV1,
        control: &RecoveryControlV1,
        now_unix_micros: i64,
    ) -> Result<RecoveryPlanV1, ObservationRuntimeError> {
        checkpoint.validate()?;
        let target = &checkpoint.target;
        check_control(control, now_unix_micros, "journal read")?;
        let recorded = self
            .port
            .recovery_state(target, control.remaining(now_unix_micros))?;
        check_control(control, now_unix_micros, "frontier read")?;
        let frontier = self
            .port
            .unacknowledged_frontier(target, control.remaining(now_unix_micros))?;

        if let Some(defect) = detect_incompatibility(recorded.as_ref(), checkpoint, &frontier) {
            // The refusal is the only mutation on this path, so the bound is
            // re-checked immediately in front of it rather than only at entry.
            check_control(control, now_unix_micros, "refusal write")?;
            let assessment = RecoveryAssessmentIdV1::for_checkpoint(checkpoint);
            let attempts = self.port.record_recovery_refusal(
                &RecoveryRefusalWriteV1 {
                    target,
                    assessment: &assessment,
                    defect: defect.as_wire(),
                    max_automatic_attempts: self.budget.max_automatic_attempts,
                    now_unix_micros,
                },
                control.remaining(now_unix_micros),
            )?;
            let repair = defect.repair_action();
            if attempts >= self.budget.max_automatic_attempts {
                return Ok(RecoveryPlanV1::OperatorRepairRequired {
                    defect,
                    repair,
                    automatic_repair_attempts: attempts,
                    max_automatic_attempts: self.budget.max_automatic_attempts,
                });
            }
            return Ok(RecoveryPlanV1::StateIncompatible {
                defect,
                repair,
                automatic_attempts_remaining: self
                    .budget
                    .max_automatic_attempts
                    .saturating_sub(attempts),
            });
        }

        check_control(control, now_unix_micros, "checkpoint write")?;
        self.port.accept_checkpoint(
            checkpoint,
            now_unix_micros,
            control.remaining(now_unix_micros),
        )?;
        let acknowledged_through = recorded
            .as_ref()
            .and_then(|state| state.acknowledged)
            .map(|position| position.sequence);
        match (
            frontier.first_unacknowledged_sequence,
            frontier.last_journalled_sequence,
        ) {
            (Some(first), Some(last)) => Ok(RecoveryPlanV1::ReplayUnacknowledged {
                first_unacknowledged_sequence: first,
                last_journalled_sequence: last,
                unacknowledged_items: frontier.unacknowledged_items,
                expected_state_generation: checkpoint.state_generation,
            }),
            // A deliverable row always has a journalled sequence, so the
            // remaining shapes are "nothing outstanding" — the only case that
            // may report convergence.
            _ => Ok(RecoveryPlanV1::Converged {
                acknowledged_through,
                expected_state_generation: checkpoint.state_generation,
            }),
        }
    }

    /// The bounded repair budget this runtime enforces.
    #[must_use]
    pub const fn budget(&self) -> RecoveryBudgetV1 {
        self.budget
    }

    /// The target key this checkpoint is bound to, revalidated.
    ///
    /// Exposed so a caller can prove the key it built matches the checkpoint it
    /// is about to assess without duplicating the validation rules.
    pub fn validate_target(target: &RecoveryTargetKeyV1) -> Result<(), ObservationRuntimeError> {
        target.validate().map_err(ObservationRuntimeError::from)
    }
}

/// Stops the assessment in front of `stage` when the caller's bound is spent.
fn check_control(
    control: &RecoveryControlV1,
    now_unix_micros: i64,
    stage: &'static str,
) -> Result<(), ObservationRuntimeError> {
    if control.is_cancelled() {
        return Err(ObservationRuntimeError::RecoveryCancelled { stage });
    }
    if control.remaining(now_unix_micros).is_spent() {
        return Err(ObservationRuntimeError::RecoveryDeadlineExceeded { stage });
    }
    Ok(())
}

/// Returns the first defect that forbids delivery, if any.
///
/// Order matters: a state schema change explains everything downstream of it,
/// and reporting a sequence regression caused by a schema migration would send
/// an operator to the wrong repair. Implementation identity comes next for the
/// same reason — a swapped binary explains a moved generation, but a moved
/// generation says nothing about a swapped binary.
fn detect_incompatibility(
    recorded: Option<&HostRecoveryStateV1>,
    checkpoint: &ProviderCheckpointV1,
    frontier: &UnacknowledgedFrontierV1,
) -> Option<StateIncompatibilityV1> {
    // Nothing may be delivered into a position the host was told exists and
    // could not read, whatever the record says.
    if checkpoint.replay_position == ProviderReplayPositionV1::Unreadable {
        return Some(StateIncompatibilityV1::ReplayPositionUnreadable);
    }
    if let Some(state) = recorded {
        if let Some(expected) = &state.state_schema_version
            && expected != &checkpoint.state_schema_version
        {
            return Some(StateIncompatibilityV1::StateSchemaChanged {
                expected: expected.clone(),
                reported: checkpoint.state_schema_version.clone(),
            });
        }
        // The registration revision is part of the target key, so a record and
        // a checkpoint that disagree here are the *same* pinned registration
        // reporting two different binaries. Accepting that silently is how a
        // swapped provider inherits another implementation's acknowledged
        // history.
        if let Some(expected) = &state.implementation_identity_sha256
            && expected != &checkpoint.implementation_identity_sha256
        {
            return Some(StateIncompatibilityV1::ImplementationIdentityChanged {
                expected: expected.clone(),
                reported: checkpoint.implementation_identity_sha256.clone(),
            });
        }
        if let Some(expected) = state.state_generation
            && checkpoint.state_generation < expected
        {
            return Some(StateIncompatibilityV1::StateGenerationRegressed {
                expected_at_least: expected,
                reported: checkpoint.state_generation,
            });
        }
        // A provider that kept its own replay position and now keeps none lost
        // the bookkeeping the host verified acknowledged effects against.
        if state.replay_position_retained == Some(true)
            && !checkpoint.replay_position.retains_position()
        {
            return Some(StateIncompatibilityV1::ReplayPositionAbandoned {
                host_acknowledged: state.acknowledged.map(|position| position.sequence),
            });
        }
        // A provider that keeps a position is compared against the host's own
        // durable watermark, including when it reports that it holds nothing:
        // "I have applied nothing" under a watermark of 7 is seven lost
        // effects, not missing evidence.
        if let Some(position) = state.acknowledged
            && checkpoint.replay_position.retains_position()
            && checkpoint
                .replay_position
                .reported()
                .is_none_or(|provider| provider < position.sequence)
        {
            return Some(StateIncompatibilityV1::AcknowledgedSequenceRegressed {
                host_acknowledged: position.sequence,
                provider_acknowledged: checkpoint.replay_position.reported(),
            });
        }
    }
    if let Some(provider) = checkpoint.replay_position.reported()
        && frontier
            .last_journalled_sequence
            .is_none_or(|journalled| provider > journalled)
    {
        return Some(StateIncompatibilityV1::ProviderAheadOfJournal {
            provider_acknowledged: provider,
            journal_highest: frontier.last_journalled_sequence,
        });
    }
    None
}
