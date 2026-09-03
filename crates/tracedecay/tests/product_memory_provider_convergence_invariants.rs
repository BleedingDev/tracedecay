//! Behavioral cover for two convergence invariants whose production code lives
//! in upstream-owned files.
//!
//! `product/upstream/convergence-map.json` states that LSP shutdown consumes
//! only the remaining shared deadline
//! (`crates/tracedecay-daemon-service/src/invocation/lsp.rs` and the lease
//! registry it calls in `.../invocation/types.rs`) and that integration tests
//! run a same-checkout CLI (`crates/tracedecay/tests/common/mod.rs`). The
//! tests live here rather than beside that code because product-owned test
//! files carry no upstream footprint: the invariants stay covered without
//! spending the `daemon_shutdown_deadline` touch point's line budget on test
//! bodies inside Zack-owned files.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use tracedecay_daemon_service::{DaemonInvocationService, LspSessionId};

/// What one blocked lease task hands back to the test: the release channel
/// that unblocks its worker thread, the signal it fires once it resumes, and
/// the signal fired when the runtime drops the task's future.
struct BlockedLeaseTask {
    release: mpsc::Sender<()>,
    resumed: mpsc::Receiver<()>,
    retired: mpsc::Receiver<()>,
}

/// Fires exactly once, when the runtime drops the lease task's future.
///
/// A future is dropped either because the task ran to completion or because it
/// was aborted, and the drop is the last thing that happens to it either way.
/// Observing it is therefore a terminal, positive event: after it arrives the
/// lease body can never execute another statement, so a `side_effect` still at
/// zero at that moment can never be raised afterwards. That is the deterministic
/// replacement for waiting out a fixed number of yields and a sleep, which only
/// ever proved that the effect had not happened *yet*.
struct LeaseRetirementWitness(mpsc::Sender<()>);

impl Drop for LeaseRetirementWitness {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Start one LSP lease task that blocks its worker thread, so the registry's
/// cancellation can never preempt it and the shutdown join cannot finish.
///
/// After the block is released the task signals `resumed` and only then
/// reaches an await point followed by `side_effect`. The signals separate the
/// halves of the guarantee the registry actually provides: `resumed` is
/// in-body work that a cancelled and aborted task still performs, because
/// abort is cooperative and cannot preempt a poll that is already running,
/// while `side_effect` is sequenced after the task's next suspension point and
/// is therefore reachable only by a task that shutdown detached instead of
/// aborting. `retired` fires when the runtime drops the future, which bounds
/// the observation: once it has arrived no further lease statement can run.
async fn blocked_lease_task(
    service: &DaemonInvocationService,
    session_id: &str,
    side_effect: Arc<AtomicUsize>,
) -> BlockedLeaseTask {
    let (release, blocked) = mpsc::channel::<()>();
    let (resumed_tx, resumed) = mpsc::channel::<()>();
    let (retired_tx, retired) = mpsc::channel::<()>();
    let (entered, in_blocking_section) = tokio::sync::oneshot::channel::<()>();
    service
        .lsp_lease_tasks
        .start(
            LspSessionId::new(session_id).expect("lease session id"),
            async move {
                // Owned by the future, so its drop is the runtime retiring
                // this task, whether by abort or by normal completion.
                let _retirement = LeaseRetirementWitness(retired_tx);
                let _ = entered.send(());
                // Blocking inside the poll is the one lease shape cancellation
                // cannot reach: the task never yields, so the join is unbounded
                // unless the caller's deadline bounds it.
                let _ = blocked.recv();
                let _ = resumed_tx.send(());
                tokio::task::yield_now().await;
                side_effect.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await
        .expect("lease task admitted before shutdown");
    in_blocking_section
        .await
        .expect("lease task reached its blocking section");
    BlockedLeaseTask {
        release,
        resumed,
        retired,
    }
}

/// Without the bound, `expire_all_until` joins the blocked lease task forever
/// and daemon teardown outlives its absolute deadline with no report.
///
/// Two tasks are required: a shutdown that abandons its join at the deadline
/// would leave the second task never even cancelled, because the first task's
/// join is what the deadline interrupts.
///
/// The test asserts the bounded guarantee, not a stronger one. Each task is
/// deliberately released *after* shutdown returned and is observed running its
/// remaining in-body work; what must not happen is a lease effect sequenced
/// after the task's next suspension point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lsp_shutdown_returns_unclean_when_a_lease_join_outlives_the_shared_deadline() {
    let service = DaemonInvocationService::default();
    let side_effects = Arc::new(AtomicUsize::new(0));
    let first = blocked_lease_task(&service, "lsp.blocked-lease.first", side_effects.clone()).await;
    let second =
        blocked_lease_task(&service, "lsp.blocked-lease.second", side_effects.clone()).await;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    let expired = tokio::time::timeout(Duration::from_secs(10), service.expire_all_until(deadline))
        .await
        .expect("LSP shutdown must end on the shared deadline instead of joining a blocked lease");

    assert!(
        !expired,
        "a lease join abandoned at the deadline must report incomplete shutdown"
    );
    assert!(
        tokio::time::Instant::now() >= deadline,
        "the bound must consume the caller's remaining budget, not a shorter fresh one"
    );
    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        0,
        "no lease body may run past its blocking section while shutdown is still joining"
    );

    // Release both tasks only after shutdown returned. Each one resumes — a
    // cancelled and aborted task still finishes the poll it was already
    // running — so neither was silently wedged, and each then reaches its
    // first await, which is where the abort takes effect.
    for task in [&first, &second] {
        task.release.send(()).expect("lease task still receiving");
        assert!(
            task.resumed.recv_timeout(Duration::from_secs(10)).is_ok(),
            "cancellation and abort must not be claimed to preempt a lease body \
             that is already executing inside its own poll"
        );
    }

    // Both tasks are past the block and at an await point, so the abort takes
    // effect on their next poll. Wait for the runtime to actually drop each
    // future: that is a positive terminal event, not the absence of one, and
    // after it the lease body provably cannot execute another statement.
    for task in [&first, &second] {
        task.retired
            .recv_timeout(Duration::from_secs(10))
            .expect("shutdown must retire the lease task's future, not detach it");
    }

    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        0,
        "a lease task shutdown abandoned at the deadline must be cancelled and aborted, \
         so it can never resume past its next suspension point; a detached task would \
         have run the effect sequenced after that await before its future was dropped"
    );
    assert_eq!(
        service.lsp_lease_tasks.active_tasks(),
        0,
        "shutdown must leave no lease task registered"
    );
}

/// The bound must not turn a cooperative teardown into a false failure: the
/// same short deadline still reports a clean shutdown when nothing blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lsp_shutdown_still_reports_clean_when_every_lease_task_joins_in_budget() {
    let service = DaemonInvocationService::default();
    service
        .lsp_lease_tasks
        .start(
            LspSessionId::new("lsp.cooperative-lease").expect("lease session id"),
            std::future::pending(),
        )
        .await
        .expect("lease task admitted before shutdown");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let expired = service.expire_all_until(deadline).await;

    assert!(
        expired,
        "a cancellable lease task must still join inside the shared deadline"
    );
    assert!(
        tokio::time::Instant::now() < deadline,
        "a clean teardown must not wait out the deadline"
    );
}

/// The same-checkout guard the upstream harness mounts in `tracedecay_bin()`.
#[path = "product_memory_provider/harness_binary.rs"]
mod harness_binary;

/// The running test binary is the one artifact this checkout's build tree is
/// guaranteed to have produced, so it must be admitted.
#[test]
fn a_binary_from_this_checkouts_build_tree_is_admitted() {
    let executable = std::env::current_exe().expect("test executable path should resolve");
    let executable = executable
        .canonicalize()
        .expect("test executable should canonicalize");

    harness_binary::refuse_foreign_test_bin(&executable, &harness_binary::test_build_tree())
        .expect("the running test executable comes from this checkout's build tree");
}

/// A CLI built in another checkout reports this release too, so location is the
/// only evidence left; the harness must refuse it and name both paths.
#[test]
fn a_binary_from_another_checkouts_build_tree_is_refused() {
    let other_checkout = tempfile::TempDir::new().expect("foreign checkout tempdir");
    let foreign_cli = other_checkout.path().join("target/debug/tracedecay");
    let build_tree = harness_binary::test_build_tree();

    let refusal = harness_binary::refuse_foreign_test_bin(&foreign_cli, &build_tree)
        .expect_err("a CLI outside this checkout's build tree must be refused");

    assert!(
        refusal.contains(&foreign_cli.display().to_string()),
        "the refusal must name the rejected CLI: {refusal}"
    );
    assert!(
        refusal.contains(&build_tree.display().to_string()),
        "the refusal must name the build tree that should have produced it: {refusal}"
    );
}
