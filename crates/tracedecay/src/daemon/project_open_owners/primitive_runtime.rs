//! Primitive runtime ownership installed after full project admission.

use std::path::Path;
use std::sync::Arc;

use tracedecay_application::primitives::{
    ProductionPrimitiveCodeAuthoritiesV1, ProductionPrimitiveOpenRequestV1,
};
use tracedecay_application::source_authorization::ProjectSourceAccessSnapshot;

use crate::daemon::DaemonInvocationState;
use tracedecay_daemon_service::{
    DaemonPrimitiveRuntimeRegistrationError, daemon_operation_event_authority,
};
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_graph_query::SourceReadContext;
use tracedecay_session_runtime::session_retrieval::DaemonSessionLookupPrimitiveV1;
use tracedecay_session_runtime::session_retrieval::SessionApplicationRetrievalPortV1;

#[hotpath::measure(label = "daemon.project.owners.primitive", future = true)]
pub(super) async fn open_and_register_project_primitive_runtime(
    invocation: &DaemonInvocationState,
    project_root: &Path,
    source: SourceReadContext,
    code_graph: crate::mcp::server::CodeGraphProjectionReadPort,
    ignored_dependency_admission: crate::mcp::server::CodeIndexIgnoredDependencyAdmissionPort,
    session_db: tracedecay_global_db::RegisteredGlobalDbLeaseV1,
    session_retrieval: Arc<dyn SessionApplicationRetrievalPortV1>,
    access: ProjectSourceAccessSnapshot,
    admitted_root_uri: &str,
) -> Result<()> {
    let temporal = Arc::new(DaemonSessionLookupPrimitiveV1::new(session_retrieval));
    invocation
        .primitive_runtime_registrar()
        .open_and_register(
            project_root.to_path_buf(),
            ProductionPrimitiveOpenRequestV1::new(
                Arc::new(source),
                ProductionPrimitiveCodeAuthoritiesV1 {
                    code_graph,
                    ignored_dependency_admission: Some(ignored_dependency_admission),
                    code_index: Arc::new(invocation.code_index_schedulers.clone()),
                    diagnostic_identity: Arc::new(invocation.code_index_schedulers.clone()),
                    convergence_park: Arc::new(invocation.code_index_schedulers.clone()),
                },
                session_db,
                temporal,
                access,
                admitted_root_uri.to_owned(),
                daemon_operation_event_authority(),
            ),
        )
        .await
        .map_err(primitive_runtime_registration_error)
}

fn primitive_runtime_registration_error(
    error: DaemonPrimitiveRuntimeRegistrationError,
) -> TraceDecayError {
    match error {
        error @ DaemonPrimitiveRuntimeRegistrationError::Open(_) => {
            TraceDecayError::Io(std::io::Error::other(error))
        }
        error => TraceDecayError::Config {
            message: format!("project-open primitive runtime registration failed: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tracedecay_contracts::ApplicationContractError;
    use tracedecay_daemon_service::DaemonPrimitiveRuntimeRegistrationError;

    use super::primitive_runtime_registration_error;

    #[test]
    fn primitive_open_failure_preserves_field_specific_contract_cause() {
        let error = primitive_runtime_registration_error(
            DaemonPrimitiveRuntimeRegistrationError::Open(ApplicationContractError::Inconsistent {
                field: "application primitive session cursor key",
            }),
        );
        let io = error
            .source()
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("typed root error source");
        let registration = io
            .get_ref()
            .and_then(|source| source.downcast_ref::<DaemonPrimitiveRuntimeRegistrationError>())
            .expect("typed primitive registration cause");
        let contract = registration
            .source()
            .and_then(|source| source.downcast_ref::<ApplicationContractError>())
            .expect("typed application contract cause");

        assert!(matches!(
            contract,
            ApplicationContractError::Inconsistent {
                field: "application primitive session cursor key"
            }
        ));
    }
}
