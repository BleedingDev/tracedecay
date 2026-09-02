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

use super::delivery::DrainBoundsV1;

/// The capped exponential the whole delivery path reschedules with.
///
/// There is one formula, and this is it. The journal applies it when a
/// provider's own retryable terminal is recorded, and the dispatcher applies it
/// when an attempt produced *no* terminal at all — an adapter that failed
/// before the provider answered, or a provider that never answered. Both come
/// from the same numbers, so a provider that is simply down backs off exactly
/// like a provider that answers `provider_unavailable`, instead of being
/// hammered at a flat interval until its attempt ceiling is consumed.
///
/// The two numbers are private and there is exactly one constructor,
/// [`RetryBackoffV1::of`], which reads them off a [`RetentionPolicyV1`]. A
/// caller therefore cannot hand the dispatcher a hand-built curve — a zero
/// base, a ceiling under the base, or a negative delay that would make a
/// failed batch immediately eligible again inside the same drain. Whatever
/// curve reaches the runtime is a curve the journal itself reschedules on, and
/// [`DeliveryRuntimeV1::dispatch_batch`] revalidates it before it leases
/// anything.
///
/// [`DeliveryRuntimeV1::dispatch_batch`]: super::DeliveryRuntimeV1::dispatch_batch
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryBackoffV1 {
    base_micros: i64,
    max_micros: i64,
}

impl RetryBackoffV1 {
    /// Reads the backoff the journal itself enforces, so a dispatcher can
    /// never reschedule on numbers the store does not know about.
    #[must_use]
    pub const fn of(retention: &RetentionPolicyV1) -> Self {
        Self {
            base_micros: retention.backoff_base_micros,
            max_micros: retention.backoff_max_micros,
        }
    }

    /// Delay after the first attempt.
    #[must_use]
    pub const fn base_micros(&self) -> i64 {
        self.base_micros
    }

    /// Ceiling the doubling saturates at.
    #[must_use]
    pub const fn max_micros(&self) -> i64 {
        self.max_micros
    }

    /// Rejects a backoff that cannot delay or cannot converge.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        if self.base_micros <= 0 {
            return Err(ObservationJournalError::InvalidDispatchPolicy {
                field: "backoff_base_micros",
            });
        }
        if self.max_micros < self.base_micros {
            return Err(ObservationJournalError::InvalidDispatchPolicy {
                field: "backoff_max_micros",
            });
        }
        Ok(())
    }

    /// Capped exponential delay for the given one-based attempt number.
    #[must_use]
    pub fn delay_for(&self, attempt_number: u32) -> i64 {
        let shift = attempt_number.saturating_sub(1).min(31);
        let multiplier = 1i64.checked_shl(shift).unwrap_or(i64::MAX);
        self.base_micros
            .checked_mul(multiplier)
            .unwrap_or(i64::MAX)
            .min(self.max_micros)
    }

    /// Instant the attempt after `attempt_number` becomes eligible.
    #[must_use]
    pub fn next_attempt_at(&self, now_unix_micros: i64, attempt_number: u32) -> i64 {
        now_unix_micros.saturating_add(self.delay_for(attempt_number))
    }
}

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
    /// Maximum delivery rounds one drain may run before it hands the loop back.
    ///
    /// A wake signal is one edge, not a count, so a worker that dispatched one
    /// batch per wake drains a backlog at `batch_max_items` per park interval.
    /// This is the bound that lets a drain keep going while a round is still
    /// leasing full batches, without letting it run forever.
    pub max_rounds_per_drain: u32,
    /// Wall budget one drain may consume before it hands the loop back, so
    /// reaping, retention, and shutdown are never starved by a long backlog.
    /// Must fit at least one attempt.
    pub drain_budget_micros: i64,
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
        // A round bound is real only if it is also an upper bound. Rounds past
        // the point where `max_rounds * batch_max_items` exceeds everything the
        // queue is permitted to hold cannot lease anything, so a caller asking
        // for more — `u32::MAX`, say — is asking for a drain the store can
        // never justify, and is refused rather than trusted.
        let reachable =
            u64::from(self.max_rounds_per_drain).saturating_mul(u64::from(self.batch_max_items));
        if self.max_rounds_per_drain == 0 || reachable > retention.max_queue_items {
            return Err(invalid("max_rounds_per_drain"));
        }
        // The wall budget must fit at least one attempt and must not outlast a
        // lease: a drain allowed to run longer than `lease_duration_micros`
        // could still be dispatching while the leases its first round handed
        // out lapse, with no reap pass in between.
        if self.drain_budget_micros < self.attempt_budget_micros
            || self.drain_budget_micros > self.lease_duration_micros
        {
            return Err(invalid("drain_budget_micros"));
        }
        RetryBackoffV1::of(retention).validate()?;
        Ok(())
    }

    /// The bounds one drain starting at `now_unix_micros` runs under.
    ///
    /// This is the *only* way to obtain a [`DrainBoundsV1`]: the type's fields
    /// are private and it has no other constructor. So the bounds a drain runs
    /// under are always this policy's own, and they are always a policy the
    /// retention policy admitted — `retention` is revalidated here rather than
    /// assumed, which is what makes "no call site can widen the validated
    /// bounds" an enforced property instead of a convention. A policy that
    /// bounds nothing is refused before any lease is taken.
    pub fn drain_bounds(
        &self,
        retention: &RetentionPolicyV1,
        now_unix_micros: i64,
    ) -> Result<DrainBoundsV1, ObservationJournalError> {
        self.validate_against(retention)?;
        Ok(DrainBoundsV1::of_policy(
            self.max_rounds_per_drain,
            now_unix_micros.saturating_add(self.drain_budget_micros),
        ))
    }
}
