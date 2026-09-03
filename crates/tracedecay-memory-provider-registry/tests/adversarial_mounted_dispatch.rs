//! Adversarial provider harness against the *mounted* dispatch and recall
//! paths (`tdmem-sz9`).
//!
//! Every test here registers one provider double that misbehaves on demand
//! and then drives it through production host code — the mounted
//! [`ProjectCognitiveRecallPortV1`] for recall, and the registry's own
//! observation-delivery route for dispatch — never through a bespoke stub or
//! a test-only seam. What is asserted is always three things together:
//!
//! * the *typed* terminal the host produced (a `FabricError` variant, a
//!   `RecallAdmissionError` variant, or a `CognitiveRecallDegradation`), not
//!   merely that "an error happened";
//! * that nothing the misbehaving provider offered reached product output —
//!   for the leak cases, by searching the delivered candidate text for a
//!   sentinel the provider tried to smuggle;
//! * that no provider work outlived the host's answer
//!   ([`AdversarialProviderV1::in_flight`]), and that the lane still answers
//!   afterwards.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

mod adversarial_fixture;

use std::sync::Arc;
use std::time::Duration;

use adversarial_fixture::*;
use tracedecay_application::memory::CognitiveRecallDegradation;
use tracedecay_application::now_micros;
use tracedecay_memory_conformance::{
    AdversarialProviderV1, AdversarialScriptV1, HandshakeMisbehaviourV1, MisbehaviourV1,
    ReleaseLatchV1,
};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{ApiError, ProviderOperation};
use tracedecay_memory_provider_native::NATIVE_PROVIDER_ID;
use tracedecay_memory_provider_registry::{
    CognitiveRecallPortError, FabricError, ProviderInvocationBoundaryV1, ProviderWorkerCensusV1,
    RecallAdmissionError, RecallDenialReason,
};

/// Convenience: one script that exhibits `misbehaviour` on every call.
fn always(misbehaviour: MisbehaviourV1) -> AdversarialScriptV1<MisbehaviourV1> {
    AdversarialScriptV1::always(misbehaviour)
}

/// A generous deadline: no test that is not about deadlines should be able to
/// pass or fail because of one.
const AMPLE_DEADLINE_MICROS: i64 = 60_000_000;

/// A deadline long enough that dispatch always happens inside it — so the
/// pre-contact guard never fires and the provider really is contacted — and
/// short enough that the scripted block outlives it.
const DEADLINE_THAT_EXPIRES_MID_CALL_MICROS: i64 = 600_000;

/// Longest a test waits for the double to be demonstrably inside the call it
/// is about to cancel. Cancellation is fired on that *event*, never on a
/// timer: a fixed delay only guesses that the provider has been entered, and
/// under a loaded scheduler a cancellation that lands before entry proves
/// nothing about reaching work already in flight.
const ENTRY_BUDGET: Duration = Duration::from_secs(5);

/// Longest a caller may still be waiting after its cancellation fired. The
/// host boundary races cancellation against the worker, so a caller that
/// waits longer than this was waiting out the provider instead
/// (`tdmem-sz9` acceptance: cancellation returns within 100ms).
const CANCELLATION_RELEASE_CEILING: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// Readiness: a provider that lies about what it is
// ---------------------------------------------------------------------------

/// A provider whose *handshake* grows a capability its registered descriptor
/// never declared is refused before one recall is dispatched. Capability
/// claims are settled against the registration, not against whatever the
/// provider says once it is running.
#[tokio::test]
async fn a_provider_that_lies_about_its_capabilities_never_answers_a_recall() {
    let provider = double_with_handshake(
        AdversarialScriptV1::always(HandshakeMisbehaviourV1::DeclaresUnregisteredCapability(
            "adversarial.extra.v1".to_owned(),
        )),
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("the host must refuse a provider that redeclares its capabilities");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::SuccessfulHandshakeDescriptorMismatch)
        ),
        "{error:?}"
    );
    assert_eq!(
        provider.invocation_count(),
        0,
        "no recall may be dispatched to a provider whose readiness was refused"
    );
    assert_eq!(provider.in_flight(), 0);
    assert!(observer.reports().is_empty());
}

/// A provider that answers readiness for a different checkout is refused: the
/// host, not the provider, owns which scope a call is about.
#[tokio::test]
async fn a_handshake_that_accepts_a_foreign_scope_never_reaches_a_recall() {
    let provider = double_with_handshake(
        AdversarialScriptV1::always(HandshakeMisbehaviourV1::AcceptsForeignScope),
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("the host must refuse readiness for a foreign scope");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::SuccessfulHandshakeScopeMismatch)
        ),
        "{error:?}"
    );
    assert_eq!(provider.invocation_count(), 0);
}

// ---------------------------------------------------------------------------
// Protocol violations on the mounted recall route
// ---------------------------------------------------------------------------

/// A reply attributed to another operation kind is refused, and no candidate
/// it carried is admitted.
#[tokio::test]
async fn a_recall_terminal_for_another_operation_is_refused() {
    let provider = double(
        always(MisbehaviourV1::TerminalForAnotherOperation(
            ProviderOperation::Health,
        )),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a terminal for another operation kind must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::ResponseOperationKindMismatch {
                expected: ProviderOperation::Recall,
                returned: ProviderOperation::Health,
            })
        ),
        "{error:?}"
    );
    assert_eq!(provider.invocation_count(), 1);
    assert_eq!(provider.in_flight(), 0);
    assert!(observer.reports().is_empty());
}

/// A reply that names an operation the host never dispatched is refused, so a
/// provider cannot settle one call with another call's answer.
#[tokio::test]
async fn a_recall_reply_for_a_foreign_operation_id_is_refused() {
    let provider = double(
        always(MisbehaviourV1::ReplyForForeignOperation),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a reply for a foreign operation must be refused");

    match error {
        CognitiveRecallPortError::Fabric(FabricError::ResponseOperationMismatch {
            returned,
            ..
        }) => assert_eq!(returned, "adversarial.foreign-operation.v1"),
        other => panic!("expected an operation-identity mismatch, got {other:?}"),
    }
}

/// A reply bound to another exact scope is refused even though every other
/// field is well formed.
#[tokio::test]
async fn a_recall_terminal_bound_to_a_foreign_scope_is_refused() {
    let provider = double(
        always(MisbehaviourV1::TerminalForForeignScope),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a terminal bound to a foreign scope must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::ResponseScopeMismatch { .. })
        ),
        "{error:?}"
    );
}

/// A failing terminal may not smuggle a result payload past the host: the
/// reply is refused as a whole, not stripped and admitted.
#[tokio::test]
async fn a_payload_on_a_failing_recall_terminal_is_refused() {
    let provider = double(
        always(MisbehaviourV1::PayloadOnFailureTerminal(
            TerminalCode::CapacityExceeded,
        )),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a payload on a failing terminal must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::Api(
                ApiError::PayloadForbiddenForTerminal {
                    terminal_code: TerminalCode::CapacityExceeded,
                }
            ))
        ),
        "{error:?}"
    );
}

/// A provider whose state generation moved backwards under the host — a
/// restore or a wipe — is refused rather than believed.
#[tokio::test]
async fn a_recall_state_generation_that_moved_backwards_is_refused() {
    let provider = double(
        always(MisbehaviourV1::StateGenerationBackwards),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a regressed state generation must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::ResponseStateGenerationMismatch { .. })
        ),
        "{error:?}"
    );
}

/// A reply past the effective response ceiling negotiated at handshake is
/// refused before anything is decoded.
#[tokio::test]
async fn an_oversized_recall_reply_is_refused_by_the_effective_ceiling() {
    let provider = double(
        always(MisbehaviourV1::OversizedReply {
            padding_bytes: 200_000,
        }),
        RecallOutcomeShapeV1::WellFormed { count: 1 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("an oversized reply must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::Api(ApiError::BoundaryBytesExceeded {
                field: "response",
                ..
            }))
        ),
        "{error:?}"
    );
}

/// A success whose payload digest does not describe its bytes is corrupted
/// effect evidence; it is refused rather than decoded on trust.
#[tokio::test]
async fn a_recall_payload_whose_digest_is_forged_is_refused() {
    let provider = double(
        always(MisbehaviourV1::CorruptedPayloadDigest),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a forged payload digest must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Fabric(FabricError::Api(ApiError::ContentDigestMismatch(
                "payload_sha256"
            )))
        ),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Candidate-level misbehaviour: admission, selection, and leak containment
// ---------------------------------------------------------------------------

/// Bytes that are not a canonical recall outcome are a typed admission
/// failure, never a silently empty result.
#[tokio::test]
async fn undecodable_recall_bytes_are_a_typed_admission_error() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::Undecodable,
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("undecodable outcome bytes must be a typed failure");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Admission(RecallAdmissionError::PayloadDecode { .. })
        ),
        "{error:?}"
    );
    assert!(
        observer.reports().is_empty(),
        "no admission ledger exists for an outcome that never decoded"
    );
}

/// An outcome envelope that names another request is refused: a provider may
/// not answer this recall with the result of a different one.
#[tokio::test]
async fn a_recall_outcome_naming_another_request_is_refused() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::ForgesRequestIdentity,
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("an outcome for another request must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Admission(RecallAdmissionError::OutcomeBinding {
                field: "request_identity"
            })
        ),
        "{error:?}"
    );
}

/// Candidates forged onto another worktree, another repository, or a
/// superseded resolution of this checkout are denied with typed reasons, and
/// the content they tried to smuggle never appears in product output.
#[tokio::test]
async fn forged_scope_candidates_are_denied_and_never_reach_product_output() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::ForgesScope,
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let outcome = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("the honest candidate still resolves");

    let delivered = delivered_content(&outcome.result);
    assert!(
        !delivered.contains(SECRET_CONTENT),
        "forged-scope content reached the application result: {delivered}"
    );
    assert_eq!(
        outcome
            .result
            .candidates()
            .iter()
            .map(|candidate| candidate.candidate_id())
            .collect::<Vec<_>>(),
        vec!["honest"]
    );

    let report = outcome.report.expect("admission report");
    assert_eq!(report.received_count, 4);
    assert_eq!(report.admitted_count, 1);
    let denied: Vec<(&str, &str)> = report
        .denied
        .iter()
        .map(|denial| (denial.candidate_id.as_str(), denial.reason.label()))
        .collect();
    assert_eq!(
        denied,
        vec![
            ("cross-worktree", "scope_mismatch"),
            ("cross-repository", "scope_mismatch"),
            ("superseded-resolution", "stale_identity"),
        ]
    );
    assert!(
        observer.reports().len() == 1,
        "the denial ledger must reach the audit sink"
    );
}

/// A candidate whose declared content digest does not describe its content is
/// denied: content integrity is checked, not assumed.
#[tokio::test]
async fn a_forged_candidate_content_digest_is_denied() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::ForgesContentDigest,
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let outcome = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("the honest candidate still resolves");

    assert!(!delivered_content(&outcome.result).contains(SECRET_CONTENT));
    let report = outcome.report.expect("admission report");
    assert_eq!(report.denied.len(), 1);
    assert_eq!(report.denied[0].candidate_id, "forged-digest");
    assert!(
        matches!(
            report.denied[0].reason,
            RecallDenialReason::ContentDigestMismatch
        ),
        "{:?}",
        report.denied[0].reason
    );
}

/// A provider that replays one memory under many identities cannot spend the
/// advisory-context budget on it more than once, and the selection receipt
/// says why each copy was dropped.
#[tokio::test]
async fn a_replayed_memory_cannot_consume_the_advisory_budget_twice() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::ReplayedContent { copies: 6 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let outcome = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("a replayed stream still resolves");

    assert_eq!(
        outcome.result.candidates().len(),
        1,
        "one memory may occupy one slot, however many identities it is given"
    );
    let selection = outcome.selection.expect("selection receipt");
    assert_eq!(selection.selected.len(), 1);
    assert_eq!(
        selection.deduplicated.len(),
        5,
        "every replayed copy is accounted for in the receipt as a duplicate"
    );
}

/// Two candidates sharing one request-scoped identity are a typed admission
/// failure: the contract forbids the shape outright.
#[tokio::test]
async fn a_repeated_candidate_id_is_a_typed_admission_error() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::RepeatedCandidateId,
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a repeated candidate id must be refused");

    match error {
        CognitiveRecallPortError::Admission(RecallAdmissionError::DuplicateCandidateId(id)) => {
            assert_eq!(id, "repeated");
        }
        other => panic!("expected a duplicate-candidate-id refusal, got {other:?}"),
    }
}

/// A provider that floods the host with far more candidates than the budget
/// the host dispatched is refused, not truncated on trust.
#[tokio::test]
async fn a_candidate_flood_beyond_the_dispatched_budget_is_refused() {
    let provider = double(
        always(MisbehaviourV1::Compliant),
        RecallOutcomeShapeV1::Floods { count: 24 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    // The request asks for three; the host budget allows eight. The
    // dispatched budget is the smaller of the two, and that is what is
    // enforced.
    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 3), &live_signal())
        .await
        .expect_err("a candidate flood must be refused");

    assert!(
        matches!(
            error,
            CognitiveRecallPortError::Admission(RecallAdmissionError::CandidateBudgetExceeded {
                returned: 24,
                maximum: 3,
            })
        ),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Deadlines and cancellation
// ---------------------------------------------------------------------------

/// Blocks the test until the host's own worker accounting reaches `predicate`,
/// so a reclamation assertion waits on the boundary's own published
/// transition rather than on a sleep.
async fn settle_workers(
    boundary: &ProviderInvocationBoundaryV1,
    ceiling: Duration,
    predicate: impl Fn(ProviderWorkerCensusV1) -> bool,
) -> ProviderWorkerCensusV1 {
    match boundary
        .await_worker_census(NATIVE_PROVIDER_ID, ceiling, predicate)
        .await
    {
        Ok(census) => census,
        Err(census) => {
            panic!("the host never reached the expected worker census; last saw {census:?}")
        }
    }
}

/// Blocks the test until the double has finished every call it is inside, so
/// an assertion about what the provider *did* waits on the provider's own exit
/// rather than on a poll.
///
/// The wait itself runs on a blocking worker: the double signals on a
/// condition variable from the host-owned thread it is parked on, and the test
/// runtime must stay free to drive everything else while that happens.
async fn settle_provider(provider: &Arc<AdversarialProviderV1>, ceiling: Duration) {
    let waited = {
        let provider = Arc::clone(provider);
        tokio::task::spawn_blocking(move || provider.wait_until_idle(ceiling))
            .await
            .expect("provider settlement waiter")
    };
    assert!(
        waited,
        "the provider never finished the call the host stopped waiting for"
    );
}

/// Blocks the test until at least `calls` invocations are inside the double.
///
/// This is the entry proof every cancellation test below fires on: the
/// provider is demonstrably in the call before consent is withdrawn.
async fn await_provider_entry(provider: &Arc<AdversarialProviderV1>, calls: u64) {
    let entered = {
        let provider = Arc::clone(provider);
        tokio::task::spawn_blocking(move || provider.wait_until_in_flight(calls, ENTRY_BUDGET))
            .await
            .expect("provider entry waiter")
    };
    assert!(
        entered,
        "the provider was never entered inside {ENTRY_BUDGET:?}, so cancelling now would \
         prove nothing about reaching work already in flight"
    );
}

/// A provider that blocks past the recall's own deadline and then answers a
/// perfectly formed success must not have that answer published: the caller's
/// budget is gone, and content admitted after it is content nobody asked for
/// any more.
///
/// The host answers *at* its deadline rather than waiting the provider out --
/// a provider that never returns must not be able to hold the caller -- so the
/// provider is still working when the caller is answered. What the host owes
/// then is honesty and bounds, and this test pins all three: the still-running
/// invocation stays counted against its provider, the provider is refused
/// before contact while it is wedged rather than queued behind, and the lane
/// answers in full again as soon as the invocation returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_blocks_past_the_deadline_is_answered_with_a_timeout() {
    let provider = double(
        AdversarialScriptV1::then(
            vec![MisbehaviourV1::BlocksPastDeadline {
                block_millis: 1_500,
            }],
            MisbehaviourV1::Compliant,
        ),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let boundary = test_invocation_boundary();
    let port = mount_with_boundary(
        compose_active(Arc::clone(&provider)),
        Arc::clone(&observer),
        Arc::clone(&boundary),
    )
    .expect("mounted port");

    let started = std::time::Instant::now();
    let outcome = port
        .recall_admitted(
            request(DEADLINE_THAT_EXPIRES_MID_CALL_MICROS, 8),
            &live_signal(),
        )
        .await
        .expect("a blown deadline is a degradation, not an error");
    let elapsed = started.elapsed();

    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::TimedOut),
        "a recall answered after its deadline must be reported as timed out"
    );
    assert!(outcome.result.candidates().is_empty());
    assert!(outcome.report.is_none());
    assert!(
        elapsed < Duration::from_millis(1_400),
        "the host waited {elapsed:?}, so it answered only once the provider chose to stop"
    );
    assert_eq!(
        provider.in_flight(),
        1,
        "the provider work the host stopped waiting for is still running"
    );
    assert_eq!(
        settle_workers(&boundary, Duration::from_millis(500), |census| {
            census.stranded == 1
        })
        .await,
        ProviderWorkerCensusV1 {
            live: 0,
            stranded: 1,
            terminated: 0,
        },
        "the host must keep the invocation it stopped waiting for counted"
    );

    // While the provider is wedged it is refused before contact, not queued
    // behind the invocation it is already inside.
    let refused = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("a provider with a stranded invocation is a degradation, not an error");
    assert_eq!(
        refused.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    assert!(refused.result.candidates().is_empty());

    // The lane still answers: the blocked call did not wedge the port. Once
    // the stranded invocation returns, everything it held comes back.
    assert_eq!(
        settle_workers(&boundary, Duration::from_secs(10), |census| {
            census.occupied() == 0
        })
        .await,
        ProviderWorkerCensusV1::default()
    );
    assert_eq!(provider.in_flight(), 0);
    assert_eq!(
        provider
            .ledger()
            .contacts_for(ProviderOperation::Recall)
            .len(),
        1,
        "the refusal happened before contact: the wedged provider was entered exactly once"
    );
    let second = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("the mounted lane stays responsive after a blown deadline");
    assert_eq!(second.result.candidates().len(), 2);
    assert_eq!(second.result.degradation(), None);
}

/// The provider that does not come back at all: its recall stays inside it
/// until this test releases it, on no timer and through no cancellation.
///
/// A timed block lets a host pass a deadline test by outlasting the provider;
/// this one cannot be outlasted, so the lane can only keep working by walking
/// away from its own invocation. Everything the host owes then is asserted
/// against a call that has provably not returned: the caller is answered as
/// timed out with nothing published, the still-running worker stays counted
/// against its provider, the wedged provider is refused before contact rather
/// than queued behind itself, and the whole worker census comes back to its
/// baseline once the provider is finally let go — which this test does itself,
/// as cleanup, after the containment assertions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_never_returns_is_abandoned_and_reclaimed_when_it_finally_does() {
    let latch = ReleaseLatchV1::new();
    let provider = double(
        AdversarialScriptV1::then(
            vec![MisbehaviourV1::NeverRepliesUntilReleased(latch.clone())],
            MisbehaviourV1::Compliant,
        ),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let boundary = test_invocation_boundary();
    let port = mount_with_boundary(
        compose_active(Arc::clone(&provider)),
        Arc::clone(&observer),
        Arc::clone(&boundary),
    )
    .expect("mounted port");
    assert_eq!(
        boundary.worker_census(NATIVE_PROVIDER_ID),
        ProviderWorkerCensusV1::default(),
        "a freshly mounted port must own no workers"
    );

    let outcome = port
        .recall_admitted(
            request(DEADLINE_THAT_EXPIRES_MID_CALL_MICROS, 8),
            &live_signal(),
        )
        .await
        .expect("a blown deadline is a degradation, not an error");

    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::TimedOut),
        "a recall the provider never answered must be reported as timed out"
    );
    assert!(outcome.result.candidates().is_empty());
    assert!(outcome.report.is_none());
    assert!(
        !latch.is_released() && latch.parked() == 1,
        "the provider answered on its own, so nothing here was abandoned"
    );
    assert_eq!(
        settle_workers(&boundary, Duration::from_millis(500), |census| {
            census.stranded == 1
        })
        .await,
        ProviderWorkerCensusV1 {
            live: 0,
            stranded: 1,
            terminated: 0,
        },
        "a worker still inside a provider that never returns must stay counted"
    );

    // While the provider is wedged it is refused before contact.
    let refused = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("a provider with a stranded invocation is a degradation, not an error");
    assert_eq!(
        refused.result.degradation(),
        Some(CognitiveRecallDegradation::Unavailable)
    );
    assert!(refused.result.candidates().is_empty());
    // Entered once and still inside: the refusal happened before contact
    // rather than being queued behind the invocation the provider is wedged
    // in. The ledger cannot say this yet — a call records its contact when it
    // leaves — so the counters that move on entry are what prove it.
    assert_eq!(provider.invocation_count(), 1);
    assert_eq!(provider.in_flight(), 1);
    assert!(observer.reports().is_empty());

    // Cleanup only: let the double go, and prove the host gives every worker
    // back and answers in full again.
    latch.release();
    assert_eq!(
        settle_workers(&boundary, Duration::from_secs(10), |census| {
            census.occupied() == 0
        })
        .await,
        ProviderWorkerCensusV1::default(),
        "the host never reclaimed the worker it had abandoned"
    );
    assert_eq!(provider.in_flight(), 0);
    assert_eq!(latch.parked(), 0);
    let second = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect("the mounted lane stays responsive once the stranded worker returns");
    assert_eq!(second.result.candidates().len(), 2);
    assert_eq!(second.result.degradation(), None);
    assert_eq!(
        provider
            .ledger()
            .contacts_for(ProviderOperation::Recall)
            .len(),
        2,
        "only the abandoned call and the compliant one reached the provider; the refusal \
         while it was wedged never did"
    );
}

/// A provider that keeps working after the caller's cancellation fires, and
/// then answers success, must not have that answer published either. The
/// ledger proves the provider really did ignore a live cancellation rather
/// than the host merely pre-empting it.
///
/// Cancellation fires on the double's own entry event, never after a fixed
/// delay: a cancellation that landed before the provider was entered would
/// prove only that the pre-contact guard works, and a delay is only a guess
/// that entry has happened. The release latency is measured from that exact
/// instant, which is what makes "the caller is released by its own
/// cancellation, not by the provider finally stopping" an assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_that_ignores_cancellation_cannot_publish_its_late_result() {
    const PROVIDER_BLOCKS_FOR: Duration = Duration::from_millis(1_500);
    let provider = double(
        always(MisbehaviourV1::BlocksPastDeadline {
            block_millis: u64::try_from(PROVIDER_BLOCKS_FOR.as_millis()).unwrap(),
        }),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let signal = live_signal();
    let cancelling = {
        let signal = signal.clone();
        let provider = Arc::clone(&provider);
        tokio::spawn(async move {
            await_provider_entry(&provider, 1).await;
            let at = std::time::Instant::now();
            signal.cancel(now_micros());
            at
        })
    };

    let outcome = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &signal)
        .await
        .expect("a cancelled recall is a degradation, not an error");
    let released_at = std::time::Instant::now();
    let cancelled_at = cancelling.await.expect("cancelling task");

    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::Cancelled),
        "a recall whose caller walked away must be reported as cancelled"
    );
    assert!(outcome.result.candidates().is_empty());
    let released_in = released_at.saturating_duration_since(cancelled_at);
    assert!(
        released_in <= CANCELLATION_RELEASE_CEILING,
        "the caller was released {released_in:?} after it withdrew: it waited out the \
         provider's {PROVIDER_BLOCKS_FOR:?} of blocking work instead of its own cancellation"
    );
    // The caller is released when it withdraws, not when this provider decides
    // to stop: the host boundary races cancellation against the worker. So the
    // provider is still working here, and the claim this test exists to make --
    // that it really did answer *after* cancellation, and that the answer went
    // nowhere -- is checked once it has finished.
    settle_provider(&provider, Duration::from_secs(10)).await;
    assert!(
        provider.ledger().answered_after_cancellation(),
        "the harness must have genuinely answered after cancellation, or this \
         test proves nothing about ignoring it"
    );
    assert_eq!(provider.in_flight(), 0);
    assert!(
        observer.reports().is_empty(),
        "a result produced after the caller withdrew must reach no product output"
    );
}

/// The well-behaved counterpart: a provider that *watches* the token sees the
/// caller's cancellation reach it while it is working and answers `cancelled`
/// itself. This is what proves the host bridges one live token into the call
/// rather than handing the provider a token nothing ever fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hosts_live_cancellation_reaches_a_provider_that_is_already_working() {
    const PROVIDER_CEILING: Duration = Duration::from_millis(3_000);
    let provider = double(
        always(MisbehaviourV1::BlocksUntilCancelled {
            ceiling_millis: u64::try_from(PROVIDER_CEILING.as_millis()).unwrap(),
        }),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let port = mount(
        compose_active(Arc::clone(&provider)),
        Arc::new(LedgerObserver::default()),
    )
    .expect("mounted port");

    let signal = live_signal();
    let cancelling = {
        let signal = signal.clone();
        let provider = Arc::clone(&provider);
        tokio::spawn(async move {
            await_provider_entry(&provider, 1).await;
            let at = std::time::Instant::now();
            signal.cancel(now_micros());
            at
        })
    };

    let outcome = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &signal)
        .await
        .expect("a provider-reported cancellation is a degradation");
    let released_at = std::time::Instant::now();
    let cancelled_at = cancelling.await.expect("cancelling task");

    assert_eq!(
        outcome.result.degradation(),
        Some(CognitiveRecallDegradation::Cancelled)
    );
    let released_in = released_at.saturating_duration_since(cancelled_at);
    assert!(
        released_in <= CANCELLATION_RELEASE_CEILING,
        "the caller was released {released_in:?} after it withdrew: it was released by the \
         provider's {PROVIDER_CEILING:?} ceiling rather than by its own cancellation"
    );
    // The caller is answered on cancellation without waiting for the provider,
    // so the provider's own record of having seen the token is read once it
    // has finished with it.
    settle_provider(&provider, Duration::from_secs(10)).await;
    let contacts = provider.ledger().contacts_for(ProviderOperation::Recall);
    assert_eq!(contacts.len(), 1);
    assert!(
        contacts[0].held_millis < u64::try_from(PROVIDER_CEILING.as_millis()).unwrap(),
        "the provider must have stopped on the token, not on its own ceiling: {:?}",
        contacts[0]
    );
    assert_eq!(provider.in_flight(), 0);
}

/// A provider that crashes mid-recall is contained by the host that dispatched
/// it. The panic never unwinds the caller's own task, the lane answers a typed
/// invocation failure rather than a candidate, nothing the crashed call was
/// carrying is admitted, and the crashed provider's route stays typed and
/// refused afterwards instead of crashing a second caller.
#[tokio::test]
async fn a_provider_that_crashes_mid_recall_is_contained_as_a_typed_failure() {
    let provider = double(
        AdversarialScriptV1::then(
            vec![MisbehaviourV1::PanicsMidDispatch],
            MisbehaviourV1::Compliant,
        ),
        RecallOutcomeShapeV1::WellFormed { count: 2 },
    );
    let observer = Arc::new(LedgerObserver::default());
    let port =
        mount(compose_active(Arc::clone(&provider)), Arc::clone(&observer)).expect("mounted port");

    let error = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await
        .expect_err("a provider that crashed must not answer a recall");
    assert!(
        matches!(error, CognitiveRecallPortError::Invocation(_)),
        "{error:?}"
    );
    assert_eq!(
        provider.in_flight(),
        0,
        "a crashed call must not be left counted as in flight"
    );
    assert!(
        observer.reports().is_empty(),
        "a call that never returned may admit nothing"
    );

    // The second recall is answered — typed — rather than crashing the next
    // caller in turn: the crashed provider's own dispatch gate is poisoned.
    let after = port
        .recall_admitted(request(AMPLE_DEADLINE_MICROS, 8), &live_signal())
        .await;
    match after {
        Err(CognitiveRecallPortError::Fabric(FabricError::ProviderGatePoisoned)) => {}
        other => panic!("the crashed provider's route must stay typed and refused: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Mounted observation delivery
// ---------------------------------------------------------------------------

/// A duplicate acknowledgement has to name the mutation it answers. An
/// observer that claims to have already committed somebody else's work
/// produces no receipt, so the delivery stays unsettled and redeliverable.
#[test]
fn an_observer_that_acknowledges_another_mutation_settles_no_receipt() {
    let observer = observer_double(always(
        MisbehaviourV1::DuplicateAcknowledgingAnotherMutation,
    ));
    let composition = compose_with_observers(vec![Arc::clone(&observer)]);
    let registry = composition.registry().expect("enabled composition");
    let ready = registry
        .handshake(&observer_handshake(OBSERVER_PROVIDER_ID))
        .expect("observer readiness");
    let receipt_digest = ready.ready_receipt_sha256.expect("ready receipt");

    let error = registry
        .deliver_observation(&observation_call(
            OBSERVER_PROVIDER_ID,
            "observation-1",
            receipt_digest,
        ))
        .expect_err("a duplicate naming another mutation must be refused");

    assert!(
        matches!(
            error,
            FabricError::Api(ApiError::DuplicateEffectKeyMismatch)
        ),
        "{error:?}"
    );
    assert_eq!(observer.invocation_count(), 1);
    assert_eq!(observer.in_flight(), 0);
}

/// An observation reply past the negotiated response ceiling is refused and
/// leaves no receipt behind.
#[test]
fn an_oversized_observation_reply_is_refused() {
    let observer = observer_double(always(MisbehaviourV1::OversizedReply {
        padding_bytes: 200_000,
    }));
    let composition = compose_with_observers(vec![Arc::clone(&observer)]);
    let registry = composition.registry().expect("enabled composition");
    let ready = registry
        .handshake(&observer_handshake(OBSERVER_PROVIDER_ID))
        .expect("observer readiness");
    let receipt_digest = ready.ready_receipt_sha256.expect("ready receipt");

    let error = registry
        .deliver_observation(&observation_call(
            OBSERVER_PROVIDER_ID,
            "observation-1",
            receipt_digest,
        ))
        .expect_err("an oversized observation reply must be refused");

    assert!(
        matches!(
            error,
            FabricError::Api(ApiError::BoundaryBytesExceeded {
                field: "response",
                ..
            })
        ),
        "{error:?}"
    );
}

/// An observation reply attributed to another operation kind is refused, and
/// the refusal invalidates the readiness that admitted it: the next delivery
/// has to prove readiness again rather than reusing a receipt the provider
/// has already contradicted.
#[test]
fn an_observation_terminal_for_another_operation_invalidates_readiness() {
    let observer = observer_double(always(MisbehaviourV1::TerminalForAnotherOperation(
        ProviderOperation::Recall,
    )));
    let composition = compose_with_observers(vec![Arc::clone(&observer)]);
    let registry = composition.registry().expect("enabled composition");
    let ready = registry
        .handshake(&observer_handshake(OBSERVER_PROVIDER_ID))
        .expect("observer readiness");
    let receipt_digest = ready.ready_receipt_sha256.expect("ready receipt");

    let error = registry
        .deliver_observation(&observation_call(
            OBSERVER_PROVIDER_ID,
            "observation-1",
            receipt_digest.clone(),
        ))
        .expect_err("a terminal for another operation kind must be refused");
    assert!(
        matches!(
            error,
            FabricError::ResponseOperationKindMismatch {
                expected: ProviderOperation::Observe,
                returned: ProviderOperation::Recall,
            }
        ),
        "{error:?}"
    );

    let after = registry
        .deliver_observation(&observation_call(
            OBSERVER_PROVIDER_ID,
            "observation-2",
            receipt_digest,
        ))
        .expect_err("readiness must not survive the refusal");
    assert!(
        matches!(after, FabricError::ProviderNotReady(_)),
        "{after:?}"
    );
    assert_eq!(
        observer.invocation_count(),
        1,
        "the second delivery must be refused before the provider is contacted again"
    );
}

/// A provider that crashes mid-dispatch is contained: the panic does not take
/// the host down, the crashed provider's own route is typed and refused from
/// then on, and a second registered observer keeps delivering. That last
/// clause is what "the daemon stays responsive" means here.
#[test]
fn a_crashing_observer_is_contained_and_the_other_observer_keeps_delivering() {
    let crashing = observer_double(always(MisbehaviourV1::PanicsMidDispatch));
    let healthy = second_observer_double(always(MisbehaviourV1::Compliant));
    let composition = compose_with_observers(vec![Arc::clone(&crashing), Arc::clone(&healthy)]);
    let registry = composition.registry().expect("enabled composition");

    let crashing_receipt = registry
        .handshake(&observer_handshake(OBSERVER_PROVIDER_ID))
        .expect("crashing observer readiness")
        .ready_receipt_sha256
        .expect("ready receipt");
    let healthy_receipt = registry
        .handshake(&observer_handshake(SECOND_OBSERVER_PROVIDER_ID))
        .expect("healthy observer readiness")
        .ready_receipt_sha256
        .expect("ready receipt");

    let call = observation_call(
        OBSERVER_PROVIDER_ID,
        "observation-1",
        crashing_receipt.clone(),
    );
    let crashed = without_panic_noise(|| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            registry.deliver_observation(&call)
        }))
    });
    assert!(crashed.is_err(), "the scripted crash must actually unwind");
    assert_eq!(
        crashing.in_flight(),
        0,
        "a crashed call must not be left counted as in flight"
    );

    // The crashed provider's own dispatch gate is poisoned, which is a typed
    // refusal rather than a second panic or a silent success.
    let after = registry
        .deliver_observation(&observation_call(
            OBSERVER_PROVIDER_ID,
            "observation-2",
            crashing_receipt,
        ))
        .expect_err("the crashed provider's route must stay refused");
    assert!(
        matches!(after, FabricError::ProviderGatePoisoned),
        "{after:?}"
    );

    // The other observer is untouched.
    let receipt = registry
        .deliver_observation(&observation_call(
            SECOND_OBSERVER_PROVIDER_ID,
            "observation-3",
            healthy_receipt,
        ))
        .expect("an unrelated observer keeps delivering after a peer crashed");
    assert_eq!(receipt.provider_id.as_str(), SECOND_OBSERVER_PROVIDER_ID);
    assert_eq!(receipt.terminal.terminal_code(), TerminalCode::Success);
    assert_eq!(healthy.invocation_count(), 1);
}
