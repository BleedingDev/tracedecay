//! The host execution boundary for synchronous provider work.
//!
//! Provider ports are synchronous and cannot be interrupted from inside the
//! process, so the boundary's whole job is to make the *execution capability*
//! explicit and then hold every provider to what that capability can honestly
//! deliver. These tests judge exactly that: which shapes are admitted at all,
//! how quickly the caller is released when it withdraws, whether the host
//! actually stopped the work or merely stopped waiting for it, what capacity
//! comes back either way, and whether the route survives.
//!
//! One scripted crash is a deliberate `panic!`: a provider that unwinds is one
//! of the outcomes the boundary exists to contain, and there is no other way
//! to produce it.
#![allow(clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tracedecay_memory_provider_registry::{
    ProviderExecutionShapeV1, ProviderInvocationBoundaryV1, ProviderInvocationFaultV1,
    ProviderInvocationLimitsV1, ProviderInvocationRequestV1, ProviderWorkV1,
    ProviderWorkerCensusV1, ProviderWorkerHandleV1, ProviderWorkerIsolationV1,
    ProviderWorkerSpawnErrorV1, ProviderWorkerSpawnV1, ProviderWorkerTerminationV1,
    WorkerDispositionV1,
};

const PROVIDER: &str = "provider.native";

/// A latch a worker blocks on until the test releases it.
#[derive(Default)]
struct ReleaseGate {
    state: std::sync::Mutex<GateState>,
    changed: std::sync::Condvar,
}

#[derive(Default)]
struct GateState {
    released: bool,
}

impl ReleaseGate {
    fn enter(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        self.changed.notify_all();
        while !state.released {
            match self.changed.wait(state) {
                Ok(next) => state = next,
                Err(_) => return,
            }
        }
    }

    fn release(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.released = true;
            self.changed.notify_all();
        }
    }
}

/// A latch that is **never** released, for the provider shapes whose whole
/// point is that nothing outside the host can make them return.
#[derive(Default)]
struct NeverReturns {
    state: std::sync::Mutex<usize>,
    never: std::sync::Condvar,
}

impl NeverReturns {
    fn enter(&self) {
        let Ok(mut entered) = self.state.lock() else {
            return;
        };
        *entered = entered.saturating_add(1);
        self.never.notify_all();
        loop {
            match self.never.wait(entered) {
                Ok(next) => entered = next,
                Err(_) => return,
            }
        }
    }

    fn entered(&self) -> usize {
        self.state.lock().map_or(0, |entered| *entered)
    }
}

/// The host execution capability the daemon supplies in production: a detached
/// in-process thread it has no sound way to stop.
struct HostWorkers;

struct HostThreadHandle;

impl ProviderWorkerHandleV1 for HostThreadHandle {
    fn terminate(&self) -> ProviderWorkerTerminationV1 {
        ProviderWorkerTerminationV1::NotTerminable
    }
}

impl ProviderWorkerSpawnV1 for HostWorkers {
    fn isolation(&self) -> ProviderWorkerIsolationV1 {
        ProviderWorkerIsolationV1::CooperativeOnly
    }

    fn spawn_detached(
        &self,
        name: &str,
        work: ProviderWorkV1,
    ) -> Result<Box<dyn ProviderWorkerHandleV1>, ProviderWorkerSpawnErrorV1> {
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(work)
            .map(|_joinable| -> Box<dyn ProviderWorkerHandleV1> { Box::new(HostThreadHandle) })
            .map_err(|error| ProviderWorkerSpawnErrorV1::new(error.to_string()))
    }
}

/// A host that refuses to start any worker at all.
struct RefusingWorkers;

impl ProviderWorkerSpawnV1 for RefusingWorkers {
    fn isolation(&self) -> ProviderWorkerIsolationV1 {
        ProviderWorkerIsolationV1::CooperativeOnly
    }

    fn spawn_detached(
        &self,
        _name: &str,
        _work: ProviderWorkV1,
    ) -> Result<Box<dyn ProviderWorkerHandleV1>, ProviderWorkerSpawnErrorV1> {
        Err(ProviderWorkerSpawnErrorV1::new("no worker available"))
    }
}

fn boundary() -> Arc<ProviderInvocationBoundaryV1> {
    Arc::new(ProviderInvocationBoundaryV1::new(
        ProviderInvocationLimitsV1::for_in_flight(2),
        Arc::new(HostWorkers),
    ))
}

/// A request from a caller that never withdraws.
fn uncancelled(budget: Duration) -> ProviderInvocationRequestV1<'static> {
    ProviderInvocationRequestV1 {
        provider_id: PROVIDER,
        execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
        budget,
        cancelled: Box::pin(std::future::pending()),
    }
}

/// A request whose caller withdraws after `after`.
fn cancelled_after(budget: Duration, after: Duration) -> ProviderInvocationRequestV1<'static> {
    ProviderInvocationRequestV1 {
        provider_id: PROVIDER,
        execution_shape: ProviderExecutionShapeV1::HostAuthoredInProcess,
        budget,
        cancelled: Box::pin(tokio::time::sleep(after)),
    }
}

async fn settle_until(
    boundary: &ProviderInvocationBoundaryV1,
    ceiling: Duration,
    predicate: impl Fn(ProviderWorkerCensusV1) -> bool,
) -> ProviderWorkerCensusV1 {
    let started = Instant::now();
    loop {
        let census = boundary.worker_census(PROVIDER);
        if predicate(census) {
            return census;
        }
        assert!(
            started.elapsed() < ceiling,
            "boundary never reached the expected census; last saw {census:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A worker that returns inside its budget delivers its outcome and leaves the
/// provider owning nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_answers_in_time_leaves_nothing_behind() {
    let boundary = boundary();
    let answer = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || 41_usize + 1)
        .await;

    assert_eq!(answer.ok(), Some(42));
    assert_eq!(
        boundary.worker_census(PROVIDER),
        ProviderWorkerCensusV1::default()
    );
}

/// Foreign provider code offered to a host with no kill primitive is refused
/// **before contact**, and the refusal names the provider.
///
/// Real defect this catches: accepting an execution shape the host cannot
/// contain and then trying to make a deadline stand in for isolation — which
/// is how a third-party provider ends up able to strand host threads at will.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_code_is_refused_by_a_host_that_cannot_terminate_a_worker() {
    let boundary = boundary();
    let ran = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&ran);

    let refused = boundary
        .invoke(
            ProviderInvocationRequestV1 {
                provider_id: PROVIDER,
                execution_shape: ProviderExecutionShapeV1::Foreign,
                budget: Duration::from_secs(5),
                cancelled: Box::pin(std::future::pending()),
            },
            move || counter.fetch_add(1, Ordering::AcqRel),
        )
        .await;

    match refused {
        Err(ProviderInvocationFaultV1::ExecutionNotIsolated { provider_id }) => {
            assert_eq!(provider_id, PROVIDER);
        }
        other => panic!("foreign code must be refused before contact: {other:?}"),
    }
    assert_eq!(
        ran.load(Ordering::Acquire),
        0,
        "a refused shape must never run"
    );
    assert_eq!(
        boundary.worker_census(PROVIDER),
        ProviderWorkerCensusV1::default(),
        "a refusal before contact holds no capacity"
    );
}

/// A terminable host is the whole answer to a provider that never returns: the
/// caller is released promptly, the worker is *actually stopped* rather than
/// merely forgotten, the provider's capacity comes back with no cooperation
/// from provider code, and the route is immediately usable again.
///
/// The worker's blocking operation here is a real child process the provider
/// waits on. That is the supervised-process shape (`tdmem-0703`, ADR-0009) in
/// miniature: the provider code neither polls a cancellation token nor is
/// released by this test — it is stuck in a syscall until the *host* kills the
/// process out from under it.
///
/// Real defect this catches: reporting a deadline outcome while the work keeps
/// running and the slot stays committed, which is what turns one hung call
/// into a permanently unavailable route.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_terminable_host_stops_a_never_returning_worker_and_reopens_the_route() {
    use std::collections::VecDeque;
    use std::io::Read;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Child, ChildStdout, Command, Stdio};
    use std::sync::Mutex;

    /// Starts one supervised provider process whose only observable end is
    /// the host killing it. The pipe handed to the provider is what the
    /// provider blocks on; the `Child` stays with the host.
    fn supervised_process() -> (Arc<Mutex<Child>>, ChildStdout) {
        let mut child = Command::new("sleep")
            .arg("600")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("a supervised provider process");
        let pipe = child.stdout.take().expect("supervised process pipe");
        (Arc::new(Mutex::new(child)), pipe)
    }

    /// A host that owns a real kill primitive over each worker's process.
    struct SupervisedProcessWorkers {
        pending: Mutex<VecDeque<Arc<Mutex<Child>>>>,
    }

    struct SupervisedProcessHandle {
        child: Arc<Mutex<Child>>,
    }

    impl ProviderWorkerHandleV1 for SupervisedProcessHandle {
        fn terminate(&self) -> ProviderWorkerTerminationV1 {
            let Ok(mut child) = self.child.lock() else {
                return ProviderWorkerTerminationV1::NotTerminable;
            };
            if child.kill().is_err() {
                return ProviderWorkerTerminationV1::NotTerminable;
            }
            // `Terminated` is a guarantee, so it is only claimed once the
            // killed process has been reaped and is confirmed gone.
            match child.wait() {
                Ok(_) => ProviderWorkerTerminationV1::Terminated,
                Err(_) => ProviderWorkerTerminationV1::NotTerminable,
            }
        }
    }

    impl ProviderWorkerSpawnV1 for SupervisedProcessWorkers {
        fn isolation(&self) -> ProviderWorkerIsolationV1 {
            ProviderWorkerIsolationV1::Terminable
        }

        fn spawn_detached(
            &self,
            name: &str,
            work: ProviderWorkV1,
        ) -> Result<Box<dyn ProviderWorkerHandleV1>, ProviderWorkerSpawnErrorV1> {
            let child = self
                .pending
                .lock()
                .ok()
                .and_then(|mut pending| pending.pop_front())
                .ok_or_else(|| ProviderWorkerSpawnErrorV1::new("no supervised process"))?;
            std::thread::Builder::new()
                .name(name.to_owned())
                .spawn(work)
                .map_err(|error| ProviderWorkerSpawnErrorV1::new(error.to_string()))?;
            Ok(Box::new(SupervisedProcessHandle { child }))
        }
    }

    let (first_child, first_pipe) = supervised_process();
    let (second_child, second_pipe) = supervised_process();
    let boundary = Arc::new(ProviderInvocationBoundaryV1::new(
        ProviderInvocationLimitsV1 {
            max_workers: 1,
            max_stranded_workers: 1,
        },
        Arc::new(SupervisedProcessWorkers {
            pending: Mutex::new(VecDeque::from([
                Arc::clone(&first_child),
                Arc::clone(&second_child),
            ])),
        }),
    ));
    assert_eq!(boundary.isolation(), ProviderWorkerIsolationV1::Terminable);

    // The provider work: block on the supervised process's pipe. It polls no
    // cancellation token, and nothing in this test releases it -- the read
    // returns only when the host kills the process out from under it.
    let blocked_on_process = move || {
        let mut pipe = first_pipe;
        let mut drained = Vec::new();
        let _ = pipe.read_to_end(&mut drained);
        "never produced"
    };

    let started = Instant::now();
    let outcome = boundary
        .invoke(
            cancelled_after(Duration::from_secs(600), Duration::from_millis(150)),
            blocked_on_process,
        )
        .await;
    let waited = started.elapsed();

    match outcome {
        Err(ProviderInvocationFaultV1::Cancelled { disposition, .. }) => assert_eq!(
            disposition,
            WorkerDispositionV1::Terminated,
            "a terminable host must report the worker it actually stopped"
        ),
        other => panic!("cancellation must end the wait: {other:?}"),
    }
    assert!(
        waited < Duration::from_secs(5),
        "the caller waited {waited:?} on a provider that never returns"
    );

    // The work is really over: the process the provider was blocked on was
    // killed and reaped, and the signal is on its exit status.
    let killed = first_child
        .lock()
        .expect("supervised process")
        .try_wait()
        .expect("supervised process status")
        .expect("the supervised process must have exited");
    assert_eq!(
        killed.signal(),
        Some(libc_sigkill()),
        "the host must have killed the supervised process, not waited it out"
    );
    assert_eq!(
        boundary.worker_census(PROVIDER),
        ProviderWorkerCensusV1 {
            live: 0,
            stranded: 0,
            terminated: 1,
        },
        "termination must reclaim capacity, not park it"
    );

    // The route is usable again on the same single-worker budget the hung call
    // had committed, with no cooperation from the provider that hung.
    let second_blocked = move || {
        let mut pipe = second_pipe;
        let mut drained = Vec::new();
        let _ = pipe.read_to_end(&mut drained);
        "never produced"
    };
    let reopened = boundary
        .invoke(
            cancelled_after(Duration::from_secs(600), Duration::from_millis(150)),
            second_blocked,
        )
        .await;
    assert!(
        matches!(reopened, Err(ProviderInvocationFaultV1::Cancelled { .. })),
        "the route must be usable after a terminated worker: {reopened:?}"
    );
    assert_eq!(
        boundary.worker_census(PROVIDER).terminated,
        2,
        "the second worker was stopped by the host too"
    );
    assert!(
        second_child
            .lock()
            .expect("supervised process")
            .try_wait()
            .expect("supervised process status")
            .is_some(),
        "the second supervised process must also be gone"
    );
}

/// `SIGKILL`, without taking a `libc` dependency for one integer.
#[cfg(unix)]
const fn libc_sigkill() -> i32 {
    9
}

/// The non-terminable host's honest contract, against a provider that never
/// returns and is never released: the caller is answered in bounded time, the
/// still-running worker is *published* rather than pretended away, the next
/// call is refused before contact instead of stranding a second thread behind
/// the first, and the route comes back the moment the worker does.
///
/// This is deliberately the weaker half of the pair. It is what an in-process
/// host can honestly promise, and it is why
/// [`a_terminable_host_stops_a_never_returning_worker_and_reopens_the_route`]
/// exists: the way to make a hang survivable is isolation the host can kill,
/// not a bigger allowance for hangs.
///
/// Real defect this catches: reporting the deadline while forgetting the
/// worker, so a runaway provider consumes a thread per call, invisibly, and a
/// wedged route is indistinguishable from a recovered one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_terminable_worker_is_published_and_refuses_the_next_call() {
    let boundary = boundary();
    let wedged = Arc::new(NeverReturns::default());

    let never = Arc::clone(&wedged);
    let started = Instant::now();
    let hung = boundary
        .invoke(uncancelled(Duration::from_millis(80)), move || {
            never.enter();
        })
        .await;
    let waited = started.elapsed();

    match hung {
        Err(ProviderInvocationFaultV1::DeadlineExceeded { disposition, .. }) => assert_eq!(
            disposition,
            WorkerDispositionV1::Stranded,
            "an in-process host must not claim a termination it cannot perform"
        ),
        other => panic!("the deadline must end the wait: {other:?}"),
    }
    assert!(
        waited < Duration::from_secs(2),
        "the caller waited {waited:?} on a worker that never returns"
    );
    let census = settle_until(&boundary, Duration::from_secs(5), |census| {
        census.stranded == 1
    })
    .await;
    assert_eq!(
        census,
        ProviderWorkerCensusV1 {
            live: 0,
            stranded: 1,
            terminated: 0,
        },
        "the still-running worker must stay counted against its provider"
    );
    assert_eq!(wedged.entered(), 1);

    // The next call is refused before contact -- promptly, and without
    // stranding a second thread behind the first.
    let never_run = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&never_run);
    let refused_at = Instant::now();
    let refused = boundary
        .invoke(uncancelled(Duration::from_secs(5)), move || {
            counter.fetch_add(1, Ordering::AcqRel);
        })
        .await;
    match refused {
        Err(ProviderInvocationFaultV1::ProviderStalled {
            provider_id,
            stranded,
        }) => {
            assert_eq!(provider_id, PROVIDER);
            assert_eq!(stranded, 1);
        }
        other => panic!("a provider with a stranded worker must be refused: {other:?}"),
    }
    assert!(
        refused_at.elapsed() < Duration::from_millis(500),
        "the refusal must not wait on the wedged worker"
    );
    assert_eq!(
        never_run.load(Ordering::Acquire),
        0,
        "a refused-before-contact invocation must not run any work"
    );
    assert_eq!(
        wedged.entered(),
        1,
        "the wedged provider was not re-entered"
    );
}

/// A worker whose caller withdrew and which the host cannot stop is a strand,
/// and the strand -- not the caller's patience -- is what the route waits on:
/// the provider is refused before contact until it returns, and answers again
/// as soon as it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_non_terminable_worker_holds_the_route_only_until_it_returns() {
    let boundary = boundary();
    let gate = Arc::new(ReleaseGate::default());

    let held = Arc::clone(&gate);
    let cancelled = boundary
        .invoke(
            cancelled_after(Duration::from_secs(30), Duration::from_millis(100)),
            move || held.enter(),
        )
        .await;
    assert!(
        matches!(
            cancelled,
            Err(ProviderInvocationFaultV1::Cancelled {
                disposition: WorkerDispositionV1::Stranded,
                ..
            })
        ),
        "{cancelled:?}"
    );
    settle_until(&boundary, Duration::from_secs(5), |census| {
        census.stranded == 1
    })
    .await;

    let refused = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || ())
        .await;
    assert!(
        matches!(
            refused,
            Err(ProviderInvocationFaultV1::ProviderStalled { stranded: 1, .. })
        ),
        "{refused:?}"
    );

    gate.release();
    assert_eq!(
        settle_until(&boundary, Duration::from_secs(10), |census| census
            .occupied()
            == 0)
        .await
        .stranded,
        0
    );
    let recovered = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || "answered")
        .await;
    assert_eq!(recovered.ok(), Some("answered"));
}

/// Cancellation ends the caller's wait when it happens, not when the provider
/// finally returns.
///
/// Real defect this catches: handing the cancellation token to provider code
/// and then waiting on the worker anyway, so a provider that ignores the token
/// keeps the caller — and the route above it — blocked for its whole blocking
/// duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_releases_the_caller_without_waiting_out_the_provider() {
    let boundary = boundary();
    const PROVIDER_BLOCKS_FOR: Duration = Duration::from_millis(1_500);
    const CANCEL_AFTER: Duration = Duration::from_millis(150);

    let started = Instant::now();
    let cancelled = boundary
        .invoke(
            cancelled_after(Duration::from_secs(30), CANCEL_AFTER),
            || {
                // Non-cooperative on purpose: it never looks at a token.
                std::thread::sleep(PROVIDER_BLOCKS_FOR);
                "late answer"
            },
        )
        .await;
    let waited = started.elapsed();

    match cancelled {
        Err(ProviderInvocationFaultV1::Cancelled {
            provider_id,
            disposition,
            ..
        }) => {
            assert_eq!(provider_id, PROVIDER);
            assert_eq!(disposition, WorkerDispositionV1::Stranded);
        }
        other => panic!("a withdrawn caller must be told so: {other:?}"),
    }
    assert!(
        waited < PROVIDER_BLOCKS_FOR / 2,
        "cancellation returned after {waited:?}: the caller waited out the provider's \
         {PROVIDER_BLOCKS_FOR:?} of blocking work instead of its own cancellation"
    );
    assert_eq!(
        boundary.worker_census(PROVIDER).live,
        0,
        "cancellation must reclaim the waited-worker capacity immediately"
    );

    // The provider does eventually return, and the strand it held is released
    // then — the route needed neither that return nor any cooperation to stay
    // usable in the meantime.
    let settled = settle_until(&boundary, Duration::from_secs(10), |census| {
        census.stranded == 0
    })
    .await;
    assert_eq!(settled, ProviderWorkerCensusV1::default());
}

/// An armed cancellation must not discard an answer the provider produced
/// first. Racing three events is only correct if completion wins when it
/// actually happened.
///
/// Real defect this catches: settling the invocation on whichever branch the
/// executor happens to poll first, so a provider that answered inside its
/// budget is reported as cancelled and its work thrown away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_armed_cancellation_does_not_discard_an_answer_produced_first() {
    let boundary = boundary();
    let answered = boundary
        .invoke(
            cancelled_after(Duration::from_secs(30), Duration::from_millis(400)),
            || {
                std::thread::sleep(Duration::from_millis(20));
                "produced"
            },
        )
        .await;

    assert_eq!(answered.ok(), Some("produced"));
    assert_eq!(
        settle_until(&boundary, Duration::from_secs(5), |census| census
            .occupied()
            == 0)
        .await,
        ProviderWorkerCensusV1::default()
    );
}

/// A provider that unwinds mid-call is a typed host fault, and its slot is
/// released rather than stranded.
///
/// Real defect this catches: releasing the slot only on the success path, so a
/// panicking provider retires the route after `max_workers` crashes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_that_unwinds_is_a_typed_fault_and_releases_its_slot() {
    let boundary = boundary();
    let panicked = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let crashed = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || -> usize {
            panic!("provider crashed mid-call")
        })
        .await;
    std::panic::set_hook(panicked);

    assert!(
        matches!(crashed, Err(ProviderInvocationFaultV1::WorkerLost { .. })),
        "{crashed:?}"
    );
    let settled = settle_until(&boundary, Duration::from_secs(5), |census| {
        census.occupied() == 0
    })
    .await;
    assert_eq!(settled, ProviderWorkerCensusV1::default());

    // The route is not retired by the crash.
    let after = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || 7_usize)
        .await;
    assert_eq!(after.ok(), Some(7));
}

/// Concurrent live workers are bounded by the provider's worker budget, and a
/// refusal at the bound is typed rather than an unbounded queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_workers_are_bounded_by_the_provider_worker_budget() {
    let boundary = Arc::new(ProviderInvocationBoundaryV1::new(
        ProviderInvocationLimitsV1 {
            max_workers: 1,
            max_stranded_workers: 1,
        },
        Arc::new(HostWorkers),
    ));
    let gate = Arc::new(ReleaseGate::default());

    let occupied = Arc::clone(&gate);
    let holder = Arc::clone(&boundary);
    let holding = tokio::spawn(async move {
        holder
            .invoke(uncancelled(Duration::from_secs(10)), move || {
                occupied.enter();
            })
            .await
    });
    settle_until(&boundary, Duration::from_secs(5), |census| census.live == 1).await;

    let refused = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || ())
        .await;
    assert!(
        matches!(
            refused,
            Err(ProviderInvocationFaultV1::WorkerCapacityExhausted { maximum: 1, .. })
        ),
        "{refused:?}"
    );

    gate.release();
    let held = holding.await;
    assert!(held.is_ok(), "the holding invocation must complete");
    assert_eq!(
        settle_until(&boundary, Duration::from_secs(5), |census| census
            .occupied()
            == 0)
        .await,
        ProviderWorkerCensusV1::default()
    );
}

/// A host that cannot start a worker is a typed fault, not a silent empty
/// answer, and it leaves the provider owning nothing.
///
/// Real defect this catches: counting the reserved slot against the provider
/// when no worker was ever started, which would retire the route after
/// `max_workers` failed spawns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_that_cannot_start_a_worker_reports_it_and_keeps_no_slot() {
    let boundary = ProviderInvocationBoundaryV1::new(
        ProviderInvocationLimitsV1::for_in_flight(1),
        Arc::new(RefusingWorkers),
    );

    let refused = boundary
        .invoke(uncancelled(Duration::from_secs(5)), || ())
        .await;
    assert!(
        matches!(
            refused,
            Err(ProviderInvocationFaultV1::WorkerUnavailable { .. })
        ),
        "{refused:?}"
    );
    assert_eq!(
        boundary.worker_census(PROVIDER),
        ProviderWorkerCensusV1::default(),
        "an unstarted worker must not hold a slot"
    );
}
