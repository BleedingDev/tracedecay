//! Per-project ownership for optional CPU Jina semantic code retrieval.

mod projection;
mod query;
mod runtime;
#[cfg(test)]
mod tests;
mod vector_read;

pub use tracedecay_semantic::{ArtifactImportErrorV1, ModelLifecycleErrorV1};

pub use query::{ProjectSemanticQueryBindingV1, ProjectSemanticQueryExecutionV1};
pub use runtime::{
    ProjectSemanticRuntimeErrorV1, ProjectSemanticRuntimeShutdownReceiptV1,
    ProjectSemanticRuntimeV1, SemanticModelAcquisitionV1,
};
