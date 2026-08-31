use std::convert::Infallible;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use tracedecay_application::memory::{
    CognitiveRecallCandidate, CognitiveRecallDegradation, CognitiveRecallPort,
    CognitiveRecallProvenance, CognitiveRecallRequest, CognitiveRecallResult,
    MAX_COGNITIVE_RECALL_CANDIDATE_BYTES, MAX_COGNITIVE_RECALL_CANDIDATES,
};
use tracedecay_application::{CancellationContext, Deadline, RequestId, ResolvedScope};
use tracedecay_domain::{ProjectId, RepositoryId, UtcMicros, WorktreeId};

fn scope(project: &str) -> ResolvedScope {
    ResolvedScope::new(
        ProjectId::new(project).unwrap(),
        RepositoryId::new("repo.cognitive-recall").unwrap(),
        WorktreeId::new("worktree.cognitive-recall").unwrap(),
        None,
    )
    .unwrap()
}

fn request(scope: ResolvedScope) -> CognitiveRecallRequest {
    CognitiveRecallRequest::new(
        scope,
        RequestId::new("request.cognitive-recall").unwrap(),
        Deadline::new(UtcMicros(100)).unwrap(),
        CancellationContext::active("cancel.cognitive-recall").unwrap(),
        "where is the recall boundary?",
        2,
    )
    .unwrap()
}

fn candidate(id: &str) -> CognitiveRecallCandidate {
    CognitiveRecallCandidate::new(
        id,
        "Recall is advisory and does not own final context assembly.",
        CognitiveRecallProvenance::available("fixture.observation").unwrap(),
    )
    .unwrap()
}

struct EchoRecallPort {
    seen: Arc<Mutex<Option<CognitiveRecallRequest>>>,
    candidate: CognitiveRecallCandidate,
}

impl CognitiveRecallPort for EchoRecallPort {
    type Error = Infallible;

    async fn recall(
        &self,
        request: CognitiveRecallRequest,
    ) -> Result<CognitiveRecallResult, Self::Error> {
        *self.seen.lock().unwrap() = Some(request.clone());
        Ok(CognitiveRecallResult::complete(
            request.scope().clone(),
            request.request_id().clone(),
            vec![self.candidate.clone()],
        )
        .unwrap())
    }
}

#[test]
fn recall_port_propagates_exact_scope_identity_deadline_and_cancellation() {
    let request = request(scope("project.cognitive-recall"));
    let seen = Arc::new(Mutex::new(None));
    let port = EchoRecallPort {
        seen: Arc::clone(&seen),
        candidate: candidate("candidate-1"),
    };

    // The port contract requires an implementation future that is Send.
    assert_send(port.recall(request.clone()));
    let result = block_on(port.recall(request.clone())).unwrap();
    result.validate_for(&request).unwrap();

    let captured = seen.lock().unwrap().clone().unwrap();
    assert_eq!(captured.scope(), request.scope());
    assert_eq!(captured.request_id(), request.request_id());
    assert_eq!(captured.deadline(), request.deadline());
    assert_eq!(captured.cancellation(), request.cancellation());
    assert_eq!(captured.query(), request.query());
    assert_eq!(result.candidates().len(), 1);
}

#[test]
fn zero_results_and_degradation_are_distinct_typed_states() {
    let request = request(scope("project.cognitive-recall"));
    let zero = CognitiveRecallResult::complete(
        request.scope().clone(),
        request.request_id().clone(),
        Vec::new(),
    )
    .unwrap();
    assert!(zero.is_complete());
    assert_eq!(zero.degradation(), None);
    zero.validate_for(&request).unwrap();

    let degraded = CognitiveRecallResult::degraded(
        request.scope().clone(),
        request.request_id().clone(),
        vec![candidate("candidate-1")],
        CognitiveRecallDegradation::Unavailable,
    )
    .unwrap();
    assert!(!degraded.is_complete());
    assert_eq!(
        degraded.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    degraded.validate_for(&request).unwrap();
}

#[test]
fn result_validation_keeps_scope_and_candidate_budget_boundaries() {
    let request = request(scope("project.cognitive-recall"));
    let wrong_scope = scope("project.other");
    let wrong_scope_result = CognitiveRecallResult::complete(
        wrong_scope,
        request.request_id().clone(),
        vec![candidate("candidate-1")],
    )
    .unwrap();
    assert!(wrong_scope_result.validate_for(&request).is_err());

    let over_request_budget = CognitiveRecallResult::complete(
        request.scope().clone(),
        request.request_id().clone(),
        vec![
            candidate("candidate-1"),
            candidate("candidate-2"),
            candidate("candidate-3"),
        ],
    )
    .unwrap();
    assert!(over_request_budget.validate_for(&request).is_err());

    let duplicate = CognitiveRecallResult::complete(
        request.scope().clone(),
        request.request_id().clone(),
        vec![candidate("duplicate"), candidate("duplicate")],
    );
    assert!(duplicate.is_err());
}

#[test]
fn candidate_and_request_bounds_fail_closed() {
    let oversized_content = "x".repeat(MAX_COGNITIVE_RECALL_CANDIDATE_BYTES + 1);
    assert!(
        CognitiveRecallCandidate::new(
            "candidate-too-large",
            oversized_content,
            CognitiveRecallProvenance::unavailable(),
        )
        .is_err()
    );

    let base_scope = scope("project.cognitive-recall");
    assert!(
        CognitiveRecallRequest::new(
            base_scope.clone(),
            RequestId::new("request.zero-limit").unwrap(),
            Deadline::new(UtcMicros(100)).unwrap(),
            CancellationContext::active("cancel.zero-limit").unwrap(),
            "query",
            0,
        )
        .is_err()
    );
    assert!(
        CognitiveRecallRequest::new(
            base_scope,
            RequestId::new("request.too-many").unwrap(),
            Deadline::new(UtcMicros(100)).unwrap(),
            CancellationContext::active("cancel.too-many").unwrap(),
            "query",
            MAX_COGNITIVE_RECALL_CANDIDATES + 1,
        )
        .is_err()
    );
}

fn assert_send<T: Send>(_: T) {}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
