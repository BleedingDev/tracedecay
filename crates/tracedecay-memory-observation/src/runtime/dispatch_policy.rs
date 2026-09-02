//! Bounds on one delivery round: how much is leased, for how long, how long a
//! single attempt may run, and how much lapsed work one reap pass may recover.
//!
//! Every value is a product decision the mounting process supplies; nothing
//! here defaults. The policy is validated against the journal's
//! [`RetentionPolicyV1`] so a batch can never lease more than the queue is
//! allowed to hold, and an attempt can never be promised longer than the lease
//! that protects it.

use crate::error::ObservationJournalError;
use crate::retention::RetentionPolicyV1;

/// Bounds for one delivery round and its recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchPolicyV1 {
    /// How long one delivery attempt may hold its lease.
    pub lease_duration_micros: i64,
    /// Maximum rows one round leases.
    pub batch_max_items: u32,
    /// Maximum queue bytes one round leases.
    pub batch_max_bytes: u64,
    /// How long one provider attempt may run before its control deadline
    /// elapses. Never longer than the lease: an attempt that outlives its lease
    /// would race the reaper.
    pub attempt_budget_micros: i64,
    /// Maximum lapsed leases one reap pass returns to `Pending`.
    pub reap_budget: u32,
}

impl DispatchPolicyV1 {
    /// Rejects a policy that bounds nothing, or that exceeds the retention
    /// policy it must stay within.
    pub fn validate_against(
        &self,
        retention: &RetentionPolicyV1,
    ) -> Result<(), ObservationJournalError> {
        let invalid =
            |field: &'static str| ObservationJournalError::InvalidDispatchPolicy { field };
        if self.lease_duration_micros <= 0 {
            return Err(invalid("lease_duration_micros"));
        }
        if self.batch_max_items == 0 || u64::from(self.batch_max_items) > retention.max_queue_items
        {
            return Err(invalid("batch_max_items"));
        }
        if self.batch_max_bytes == 0 || self.batch_max_bytes > retention.max_queue_bytes {
            return Err(invalid("batch_max_bytes"));
        }
        if self.attempt_budget_micros <= 0
            || self.attempt_budget_micros > self.lease_duration_micros
        {
            return Err(invalid("attempt_budget_micros"));
        }
        if self.reap_budget == 0 {
            return Err(invalid("reap_budget"));
        }
        Ok(())
    }
}
