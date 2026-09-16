use super::*;

struct TestControl {
    cancelled: bool,
    deadline_exceeded: bool,
}

impl CodeIndexExecutionControlV1 for TestControl {
    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn is_deadline_exceeded(&self) -> bool {
        self.deadline_exceeded
    }
}

#[test]
fn near_serving_checks_cancellation_before_deadline() {
    let cancelled = TestControl {
        cancelled: true,
        deadline_exceeded: true,
    };
    assert_eq!(
        check_similar_control(&cancelled),
        Err(RetrievalPortError::Cancelled)
    );
}

#[test]
fn near_serving_maps_deadline_to_bounded_work_failure() {
    let deadline = TestControl {
        cancelled: false,
        deadline_exceeded: true,
    };
    assert_eq!(
        check_similar_control(&deadline),
        Err(RetrievalPortError::BudgetExceeded)
    );
}

#[test]
fn exhausted_near_page_reports_capped_verification_coverage() {
    let coverage = similar_near_partial_coverage();

    assert_eq!(coverage.capped, 1);
    assert_eq!(coverage.examined, 0);
    assert_eq!(
        similar_near_budget_reason(),
        vec![CloneFingerprintPartialReasonV1::VerificationWorkBudget]
    );
}
