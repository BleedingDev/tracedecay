use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use tracedecay_domain::{CodeGenerationId, ProjectionKeyV1};
use tracedecay_semantic_contracts::SemanticRuntimeScheduleFailureV1;

use crate::embedding_backend::ProductionEmbeddingRuntime;
use crate::model_lifecycle::{
    SemanticModelLifecycleMutationTargetV1, SemanticModelLifecycleOwnerV1,
};
use crate::runtime_query::CurrentSemanticQueryRuntimeV1;
use crate::{PreparedSemanticRuntimeCommitV1, SemanticRuntimeService};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemanticProjectionResumeOutcomeV1 {
    ReplayFromStart,
    CompletedBatches(u64),
    AlreadyPublished,
}

pub(super) type SemanticProjectionResumeFutureV1 = Pin<
    Box<
        dyn Future<
                Output = Result<
                    SemanticProjectionResumeOutcomeV1,
                    SemanticRuntimeScheduleFailureV1,
                >,
            > + Send
            + 'static,
    >,
>;

/// Opens a staged projection before encoder work and distinguishes exact
/// terminal publication from resumable batch progress.
pub(super) type SemanticProjectionResumeV1 =
    Box<dyn FnOnce() -> SemanticProjectionResumeFutureV1 + Send + 'static>;

pub(super) fn completed_batch_offset(
    outcome: SemanticProjectionResumeOutcomeV1,
    batch_count: usize,
) -> Result<Option<usize>, SemanticRuntimeScheduleFailureV1> {
    match outcome {
        SemanticProjectionResumeOutcomeV1::ReplayFromStart => Ok(Some(0)),
        SemanticProjectionResumeOutcomeV1::CompletedBatches(completed) => {
            let completed = usize::try_from(completed)
                .map_err(|_| SemanticRuntimeScheduleFailureV1::Publication)?;
            if completed > batch_count {
                return Err(SemanticRuntimeScheduleFailureV1::Publication);
            }
            Ok(Some(completed))
        }
        SemanticProjectionResumeOutcomeV1::AlreadyPublished => Ok(None),
    }
}

pub(super) fn install_candidate_on_success(
    commit: PreparedSemanticRuntimeCommitV1,
    expected_source: CodeGenerationId,
    expected_projection: ProjectionKeyV1,
    runtime: Arc<RwLock<Option<CurrentSemanticQueryRuntimeV1<ProductionEmbeddingRuntime>>>>,
    candidate: Arc<SemanticRuntimeService<ProductionEmbeddingRuntime>>,
    query_in_flight: Arc<AtomicBool>,
    lifecycle_binding: Option<(
        Arc<SemanticModelLifecycleOwnerV1>,
        SemanticModelLifecycleMutationTargetV1,
    )>,
) -> PreparedSemanticRuntimeCommitV1 {
    commit.on_success(move |pointer| {
        if pointer.source_generation != expected_source
            || pointer.projection_key != expected_projection
        {
            return Err(SemanticRuntimeScheduleFailureV1::Publication);
        }
        let next = CurrentSemanticQueryRuntimeV1::new_with_admission(
            pointer.clone(),
            candidate,
            query_in_flight,
        );
        let committed = match lifecycle_binding {
            None => {
                runtime
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .replace(next);
                Ok(true)
            }
            Some((lifecycle, target)) => {
                let next = std::cell::RefCell::new(Some(next));
                let prior = std::cell::RefCell::new(None);
                let installed = std::cell::Cell::new(false);
                lifecycle
                    .commit_runtime_ready_with_rollback(
                        &target,
                        || {
                            let next = next
                                .borrow_mut()
                                .take()
                                .expect("lifecycle runtime install is committed once");
                            *prior.borrow_mut() = runtime
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .replace(next);
                            installed.set(true);
                            true
                        },
                        || {
                            if installed.replace(false) {
                                *runtime
                                    .write()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                    prior.borrow_mut().take();
                            }
                        },
                    )
                    .map_err(|_| SemanticRuntimeScheduleFailureV1::Publication)
            }
        };
        match committed {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => Err(SemanticRuntimeScheduleFailureV1::Publication),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{SemanticProjectionResumeOutcomeV1, completed_batch_offset};
    use tracedecay_semantic_contracts::SemanticRuntimeScheduleFailureV1;

    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    #[tokio::test]
    async fn failed_ready_persistence_removes_candidate_when_runtime_started_empty() {
        use std::path::{Path, PathBuf};
        use std::sync::{Arc, RwLock};

        use tracedecay_domain::{CodeGenerationId, ManifestDigest, VectorGenerationIdV1};

        use crate::embedding_backend::{
            ProductionEmbeddingRuntime, production_embedding_runtime_factory,
        };
        use crate::fastembed_adapter::lifecycle_test_support::{
            lifecycle_authority_from, lifecycle_install_fixture,
        };
        use crate::model_lifecycle::{ModelMemberSourceV1, SemanticModelLifecycleOwnerV1};
        use crate::runtime_query::CurrentSemanticQueryRuntimeV1;
        use crate::runtime_service::{SemanticRuntimeService, SharedEmbeddingRuntimeFactory};
        use crate::session_pool::test_support::config;
        use crate::{PreparedSemanticRuntimeCommitV1, SemanticGenerationPointerV1};

        struct FixtureSource {
            root: PathBuf,
        }

        impl ModelMemberSourceV1 for FixtureSource {
            fn fetch_member(
                &self,
                _model: &crate::CatalogedFastEmbedModelV1,
                upstream_path: &str,
                destination: &Path,
            ) -> Result<(), crate::ModelLifecycleErrorV1> {
                if let Some(parent) = destination.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|_| crate::ModelLifecycleErrorV1::DownloadFailed)?;
                }
                std::fs::copy(self.root.join(upstream_path), destination)
                    .map(|_| ())
                    .map_err(|_| crate::ModelLifecycleErrorV1::DownloadFailed)
            }
        }

        let fixture = lifecycle_install_fixture(b"model");
        let model_id = fixture.model.model_id.clone();
        let mut model = fixture.model.clone();
        model.source.revision = "0123456789abcdef0123456789abcdef01234567".to_owned();
        let root = tempfile::tempdir().expect("lifecycle root");
        let catalog = crate::FastEmbedModelCatalogV1 {
            schema: crate::FastEmbedModelCatalogV1::production().schema,
            models: vec![model],
        };
        let lifecycle = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(FixtureSource {
                    root: fixture.install.path().to_path_buf(),
                }),
            )
            .expect("lifecycle owner"),
        );
        lifecycle
            .select_model(Some(&model_id), false)
            .expect("select fixture model");
        lifecycle
            .acquire_blocking_for_tests()
            .expect("install fixture model");
        let target = lifecycle
            .lifecycle_mutation_target()
            .expect("installed lifecycle target");

        // Replace the durable file with a directory after the target is
        // admitted. The runtime install therefore succeeds first, then the
        // owner's Ready write deterministically fails and invokes rollback.
        std::fs::remove_file(root.path().join("lifecycle.json")).expect("lifecycle file");
        std::fs::create_dir(root.path().join("lifecycle.json")).expect("persistence failure");

        let authority =
            Arc::new(lifecycle_authority_from(&fixture, 1 << 20).expect("fixture authority"));
        let factory: SharedEmbeddingRuntimeFactory<ProductionEmbeddingRuntime> =
            production_embedding_runtime_factory();
        let candidate = SemanticRuntimeService::new_owned(
            Arc::clone(&authority),
            factory,
            config(1, std::time::Duration::from_mins(1), 1 << 20),
        )
        .expect("candidate runtime");
        let source = CodeGenerationId::new("generation.resume.rollback".to_owned())
            .expect("source generation");
        let pointer = SemanticGenerationPointerV1 {
            generation: VectorGenerationIdV1::new(
                ManifestDigest::new(format!("sha256:{}", "r".repeat(64)))
                    .expect("vector generation"),
            ),
            source_generation: source.clone(),
            projection_key: authority.projection().projection_key().clone(),
        };
        let runtime = Arc::new(RwLock::new(
            None::<CurrentSemanticQueryRuntimeV1<ProductionEmbeddingRuntime>>,
        ));
        let commit_pointer = pointer.clone();
        let prepared = super::install_candidate_on_success(
            PreparedSemanticRuntimeCommitV1::new(move || async move { Ok(commit_pointer) }),
            source,
            authority.projection().projection_key().clone(),
            Arc::clone(&runtime),
            candidate,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some((Arc::clone(&lifecycle), target)),
        );
        let (_, install, _) = prepared.commit().await.expect("prepared commit");
        let install = install.expect("candidate install callback");
        assert_eq!(
            install(&pointer),
            Err(SemanticRuntimeScheduleFailureV1::Publication)
        );
        assert!(
            runtime.read().expect("runtime lock").is_none(),
            "failed lifecycle persistence must remove a candidate installed into an empty slot"
        );
    }

    #[test]
    fn completed_batch_offset_rejects_progress_beyond_the_canonical_plan() {
        assert_eq!(
            completed_batch_offset(SemanticProjectionResumeOutcomeV1::CompletedBatches(2), 1),
            Err(SemanticRuntimeScheduleFailureV1::Publication)
        );
        assert_eq!(
            completed_batch_offset(SemanticProjectionResumeOutcomeV1::AlreadyPublished, 1),
            Ok(None)
        );
    }
}
