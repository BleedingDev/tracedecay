//! Parity checks for the Native adapter's canonical, read-only boundary.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::native_provider;

#[test]
fn native_parity_contract_has_no_provider_state_generation() {
    let limits = native_provider::native_provider_limits();
    assert!(limits.request_bytes > 0);
    assert!(limits.response_bytes > 0);
    assert_eq!(
        native_provider::STATE_SCHEMA_VERSION,
        "native-application-port-v1"
    );
    assert_eq!(
        native_provider::PROVIDER_INSTANCE_ID,
        "tracedecay.native.project"
    );
}
