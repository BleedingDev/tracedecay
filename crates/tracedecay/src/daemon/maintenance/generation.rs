//! Ordered generation retention for one mounted project.

use super::{MaintenanceContinuation, MaintenanceTickOutcome, StoreTelemetrySamplingRegistry};
use crate::daemon::store_maintenance::CodeGenerationRetentionOutcomeV1;

/// Run the production generation-maintenance journey for one mounted project.
///
/// Vector generations converge before their source code generations can be
/// collected. Scope deletion is admitted only from a complete
/// post-convergence vector census. Code-generation retention runs only in a
/// tick whose vector pass converged; a paging, retiring, or degraded vector
/// pass skips it and its own continuation or retry drives the next tick. The
/// code pass then resolves its own vector protection inventory, deferring
/// destructive collection when the authority is unavailable. Current
/// default-off configuration does not prove historical vector state empty.
/// Complete authoritative inventory (including an empty one) enables bounded
/// collection; an in-progress census waits until its exact pin set completes.
/// A fresh full tick preserves this ordered journey, including independent
/// compaction. A semantic continuation
/// returns after its owning phase, while a code-generation continuation runs
/// the bounded semantic-vector page and the bounded code-generation unit —
/// draining a superseded backlog on the short cadence without re-running
/// scope reconciliation or compaction.
#[hotpath::measure(label = "daemon.maintenance.generation", future = true)]
pub(in crate::daemon) async fn run_project_generation_maintenance(
    graph: &crate::tracedecay::TraceDecay,
    code_index_schedulers: &tracedecay_code_index_runtime::code_index_scheduler::CodeIndexSchedulerRegistryV1,
    maintenance_observations: &StoreTelemetrySamplingRegistry,
    cancellation: &tracedecay_session_memory::context::CancellationToken,
    retention: &crate::config::RetentionConfig,
    continuation: Option<MaintenanceContinuation>,
) -> MaintenanceTickOutcome {
    // Each ordered phase gets its own wall span: the outer generation span is
    // inclusive, so a slow tick is attributed to vector retention, code
    // generation retention, scope reconciliation, or compaction — not guessed.
    let mut outcome = hotpath::measure_block!(
        "daemon.maintenance.vector_retention",
        crate::daemon::store_maintenance::run_semantic_vector_generation_retention(
            graph,
            code_index_schedulers,
            maintenance_observations,
            cancellation,
        )
        .await
    );
    if continuation == Some(MaintenanceContinuation::SemanticVectorRetention) {
        return outcome;
    }
    let semantic_collection_complete = outcome.is_complete();
    // Source-code deletion never runs ahead of vector cleanup: a tick whose
    // semantic-vector pass is still paging, retiring, or degraded skips the
    // code-generation pass entirely and lets that pass's own continuation or
    // retry drive the next tick. Only a converged vector inventory admits a
    // sweep, so the tick that deletes a source is the tick that converged.
    let code_generation = if cancellation.is_cancelled() {
        Some(CodeGenerationRetentionOutcomeV1::Failed)
    } else if semantic_collection_complete {
        Some(hotpath::measure_block!(
            "daemon.maintenance.code_generation_retention",
            crate::daemon::store_maintenance::run_code_generation_retention(
                graph,
                code_index_schedulers,
                maintenance_observations,
                cancellation,
            )
            .await
        ))
    } else {
        None
    };
    match code_generation {
        None
        | Some(CodeGenerationRetentionOutcomeV1::Complete)
        | Some(CodeGenerationRetentionOutcomeV1::SemanticUnseated) => {}
        Some(CodeGenerationRetentionOutcomeV1::MoreWork) => {
            outcome = outcome.combine(MaintenanceTickOutcome::Continue(
                MaintenanceContinuation::CodeGenerationRetention,
            ));
        }
        Some(
            CodeGenerationRetentionOutcomeV1::VectorInventoryUnproven
            | CodeGenerationRetentionOutcomeV1::Failed,
        ) => {
            if !cancellation.is_cancelled() {
                outcome = MaintenanceTickOutcome::Retry;
            }
        }
    }
    if continuation == Some(MaintenanceContinuation::CodeGenerationRetention) {
        return finalize_generation_outcome(outcome, cancellation);
    }
    if semantic_collection_complete
        && code_generation == Some(CodeGenerationRetentionOutcomeV1::Complete)
        && !cancellation.is_cancelled()
        && maintenance_observations.semantic_vector_scope_collection_ready(graph.project_root())
    {
        let scope_reconciled = hotpath::measure_block!(
            "daemon.maintenance.scope_reconciliation",
            crate::daemon::store_maintenance::run_code_index_scope_reconciliation(
                graph,
                code_index_schedulers,
                maintenance_observations,
            )
            .await
        );
        if !scope_reconciled {
            outcome = MaintenanceTickOutcome::Retry;
        }
    }
    if !cancellation.is_cancelled()
        && let Some(compaction) = &retention.compaction
    {
        hotpath::measure_block!("daemon.maintenance.compaction", {
            let project_compacted =
                crate::daemon::store_maintenance::run_project_compaction(graph.db(), compaction)
                    .await;
            if !project_compacted {
                outcome = MaintenanceTickOutcome::Retry;
            }
            if !cancellation.is_cancelled() {
                let branch_compacted =
                    crate::daemon::store_maintenance::run_branch_compaction(graph, compaction)
                        .await;
                if !branch_compacted {
                    outcome = MaintenanceTickOutcome::Retry;
                }
            }
        });
    }
    finalize_generation_outcome(outcome, cancellation)
}

/// Cancelled and degraded ticks are recorded too: a maintenance lane that
/// silently retries forever is exactly the waste being diagnosed.
fn finalize_generation_outcome(
    outcome: MaintenanceTickOutcome,
    cancellation: &tracedecay_session_memory::context::CancellationToken,
) -> MaintenanceTickOutcome {
    if cancellation.is_cancelled() {
        hotpath::gauge!("daemon.maintenance.generation.cancelled_total").inc(1_u64);
        MaintenanceTickOutcome::Retry
    } else {
        if matches!(outcome, MaintenanceTickOutcome::Retry) {
            hotpath::gauge!("daemon.maintenance.generation.retry_total").inc(1_u64);
        }
        outcome
    }
}
