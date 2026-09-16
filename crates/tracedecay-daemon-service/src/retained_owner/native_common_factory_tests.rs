//! Focused factory checks for the Native application port.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use super::native_provider;

#[test]
fn native_factory_has_bounded_read_limits_without_private_state() {
    let limits = native_provider::native_provider_limits();
    assert_eq!(limits.concurrent_operations, 4);
    assert_eq!(limits.observation_batch_items, 16);
    assert_eq!(limits.recall_candidates, 32);
}
