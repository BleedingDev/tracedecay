//! Focused common contract checks for the canonical Native read adapter.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::sync::Arc;

use tracedecay_memory_provider_registry::{MemoryProviderV1, NativeProvider};

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_common_contract_exposes_only_canonical_read_operations() {
    let (_temporary, project_root, graph, _owner, _project_id) =
        super::real_project_fixture().await;
    let port = ProjectNativeMemoryApplicationPort::new(
        Arc::new(tokio::sync::RwLock::new(Arc::clone(&graph))),
        project_root,
    )
    .expect("construct project Native application port");
    let provider = NativeProvider::new(Arc::new(port)).expect("construct Native provider");
    let descriptor = provider.descriptor();

    let capabilities = descriptor
        .capabilities
        .iter()
        .map(|capability| capability.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        capabilities,
        vec![
            "provider.health.v1",
            "observation.accept.v1",
            "recall.query.v1"
        ]
    );
    assert_eq!(descriptor.state_generation, 0);
    assert_eq!(
        descriptor.state_schema_version,
        "native-application-port-v1"
    );
}
