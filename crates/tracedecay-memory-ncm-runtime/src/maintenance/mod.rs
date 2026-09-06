//! Bounded maintenance accounting shared by the runtime engine.

use crate::store::StorageUsage;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Instant;

/// Storage and wall-clock measurements attached to every maintenance receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceMeasurement {
    /// Elapsed wall-clock milliseconds measured before the durable commit.
    pub elapsed_ms: u64,
    /// Controlled SQLite bytes observed before the operation.
    pub bytes_before: u64,
    /// Controlled SQLite bytes observed after the bounded work and before journaling its receipt.
    pub bytes_after: u64,
}

impl MaintenanceMeasurement {
    pub(crate) fn new(started: Instant, before: StorageUsage, after: StorageUsage) -> Self {
        Self {
            elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            bytes_before: before.physical_bytes(),
            bytes_after: after.physical_bytes(),
        }
    }
}

pub(crate) fn measured_payload(mut payload: Value, measurement: MaintenanceMeasurement) -> Value {
    let metrics = json!({
        "elapsed_ms": measurement.elapsed_ms,
        "bytes_before": measurement.bytes_before,
        "bytes_after": measurement.bytes_after,
    });
    if let Some(object) = payload.as_object_mut()
        && let Some(metrics_object) = metrics.as_object()
    {
        object.extend(metrics_object.clone());
        payload
    } else {
        json!({"result": payload, "measurement": metrics})
    }
}
