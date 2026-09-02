//! Backpressure, load shedding, and the no-silent-drop invariant.
//!
//! The journal is bounded (ADR-0005), so a provider that stops draining
//! eventually meets a ceiling. What happens *at* that ceiling is the product
//! decision this module owns, and it has exactly three moving parts:
//!
//! * **Measurement.** [`QueueBacklogV1`] turns one [`QueuePressureV1`] reading
//!   plus a caller-supplied instant into backlog *size* (items, bytes, and the
//!   utilization each of them represents) and backlog *age* (how long the
//!   oldest non-terminal row has waited). Those are the metrics an operator
//!   needs to see a lane filling up before it saturates.
//! * **Classification.** [`ObservationLoadClassV1`] splits admitted work into
//!   the part that may be refused early and the part that may not. The class is
//!   derived from the envelope's own [`RetentionClassV1`] — content the product
//!   keeps only for a session or less is the high-volume advisory stream and is
//!   shed first; project- and profile-lifetime content keeps the headroom
//!   between the shed threshold and the ceiling to itself.
//! * **Decision.** [`BackpressureGateV1::decide`] answers admit or shed, with a
//!   typed reason, before anything is appended.
//!
//! # Shedding is a refusal, never a drop
//!
//! A shed record is **not** thrown away. One source stream has one watermark,
//! so the only way to "skip" a record is to advance the watermark past it,
//! which is a silent drop with extra steps. Ingress therefore stops at a shed
//! exactly as it stops at a typed journal refusal: nothing is appended, the
//! watermark holds at the refused position, the canonical source still holds
//! the record, and the next pass re-presents it once the lane drains. What the
//! load class buys is *when* a stream stops feeding the queue — an optional
//! lane stops at `shed_optional_at_ppm` while a required lane keeps going until
//! the ceiling — which is what reserves the last slice of the queue for work
//! that must not be refused early.
//!
//! # Foreground latency is an input, not just an output
//!
//! A lane can be in trouble while its queue looks empty: if the journal itself
//! has become slow, admission is what is hurting the coding agent, and queue
//! depth will never say so because nothing is getting in. So the gate takes a
//! measured foreground admission latency
//! ([`BackpressureGateV1::observe_foreground`]) and treats a *run* of samples
//! over `foreground_budget_micros` as a shed trigger in its own right. A run,
//! not a single sample: one slow fsync is noise, and a product that shed work
//! over noise would be jumpier than the problem. The budget and the run length
//! are declared by the mounting process; nothing here defaults them.

use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};

use crate::envelope::RetentionClassV1;
use crate::error::ObservationJournalError;
use crate::identity::SourceSequenceV1;
use crate::inspection::QueuePressureV1;

/// Denominator of every utilization figure in this module.
pub const UTILIZATION_SCALE_PPM: u32 = 1_000_000;

/// Whether admitted work may be refused before the queue ceiling is reached.
///
/// This is derived, never declared by a caller: the envelope already says how
/// long the product intends to keep the content, and that is the honest proxy
/// for how much it costs to refuse it now and re-present it later.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObservationLoadClassV1 {
    /// Ephemeral and session-lifetime content: the high-volume advisory stream.
    /// Refused first, so a filling lane stops taking it while headroom remains.
    Optional,
    /// Project- and profile-lifetime content. Refused only when the queue
    /// itself can hold no more.
    Required,
}

impl ObservationLoadClassV1 {
    /// The product rule mapping retention lifetime to shed priority.
    #[must_use]
    pub const fn of(retention_class: RetentionClassV1) -> Self {
        match retention_class {
            RetentionClassV1::Ephemeral | RetentionClassV1::Session => Self::Optional,
            RetentionClassV1::Project | RetentionClassV1::Profile => Self::Required,
        }
    }

    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "optional" => Ok(Self::Optional),
            "required" => Ok(Self::Required),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "observation_load_class",
                value: other.to_owned(),
            }),
        }
    }
}

/// How loaded one provider lane is, as a closed three-step ladder.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackpressureStateV1 {
    /// Below every threshold. Everything is admitted.
    #[default]
    Nominal,
    /// Past a shed threshold. Optional work is refused; required work is not.
    SheddingOptional,
    /// At or past the refusal threshold, or the queue ceiling itself. Nothing
    /// new is admitted; the backlog persists and delivery must drain it.
    Saturated,
}

impl BackpressureStateV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Nominal => "nominal",
            Self::SheddingOptional => "shedding_optional",
            Self::Saturated => "saturated",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "nominal" => Ok(Self::Nominal),
            "shedding_optional" => Ok(Self::SheddingOptional),
            "saturated" => Ok(Self::Saturated),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "backpressure_state",
                value: other.to_owned(),
            }),
        }
    }
}

/// Which measurement moved the lane off [`BackpressureStateV1::Nominal`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackpressureReasonV1 {
    /// Queued items or bytes crossed a declared utilization threshold.
    QueueUtilization,
    /// The oldest non-terminal row has waited longer than the declared bound.
    BacklogAge,
    /// The last measured foreground admission exceeded its declared budget.
    ForegroundBudget,
    /// One more row of this size would exceed the journal's own ceiling.
    QueueCeiling,
}

impl BackpressureReasonV1 {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::QueueUtilization => "queue_utilization",
            Self::BacklogAge => "backlog_age",
            Self::ForegroundBudget => "foreground_budget",
            Self::QueueCeiling => "queue_ceiling",
        }
    }

    /// Decodes one canonical wire value.
    pub fn from_wire(value: &str) -> Result<Self, ObservationJournalError> {
        match value {
            "queue_utilization" => Ok(Self::QueueUtilization),
            "backlog_age" => Ok(Self::BacklogAge),
            "foreground_budget" => Ok(Self::ForegroundBudget),
            "queue_ceiling" => Ok(Self::QueueCeiling),
            other => Err(ObservationJournalError::UnknownWireValue {
                field: "backpressure_reason",
                value: other.to_owned(),
            }),
        }
    }
}

/// The thresholds one mounted lane runs under.
///
/// Every value is a product decision the mounting process supplies; nothing
/// here defaults. The two utilization thresholds are ordered so that the band
/// between them is real reserved headroom for required work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpressurePolicyV1 {
    /// Utilization at or above which optional work is refused.
    pub shed_optional_at_ppm: u32,
    /// Utilization at or above which every class is refused.
    pub refuse_at_ppm: u32,
    /// How long the oldest non-terminal row may wait before the lane starts
    /// shedding optional work, however few rows it holds. A lane whose oldest
    /// row is this old is not draining, and adding to it helps nobody.
    pub max_backlog_age_micros: i64,
    /// Budget one foreground admission may consume.
    pub foreground_budget_micros: i64,
    /// How many *consecutive* admissions must overrun the budget before the
    /// lane starts shedding optional work.
    ///
    /// One slow admission is a slow disk, not a lane in trouble, and refusing
    /// work over it would make the product jumpy for no gain. A run of them is
    /// a lane whose admission path is genuinely not keeping up, and there the
    /// remedy is real: optional traffic stops competing for the journal so the
    /// delivery worker can drain. A single within-budget admission clears the
    /// run, so the lane recovers on its own.
    pub foreground_breach_streak: u32,
}

impl BackpressurePolicyV1 {
    /// Rejects a policy whose thresholds cannot bound anything, or that
    /// reserves no headroom between shedding and refusing.
    pub fn validate(&self) -> Result<(), ObservationJournalError> {
        let invalid =
            |field: &'static str| ObservationJournalError::InvalidBackpressurePolicy { field };
        if self.shed_optional_at_ppm == 0 || self.shed_optional_at_ppm > UTILIZATION_SCALE_PPM {
            return Err(invalid("shed_optional_at_ppm"));
        }
        // Equal thresholds would make the load class meaningless: optional and
        // required work would be refused at exactly the same point, which is
        // the "classification that changes nothing" failure this policy exists
        // to prevent. The band must be non-empty.
        if self.refuse_at_ppm <= self.shed_optional_at_ppm
            || self.refuse_at_ppm > UTILIZATION_SCALE_PPM
        {
            return Err(invalid("refuse_at_ppm"));
        }
        if self.max_backlog_age_micros <= 0 {
            return Err(invalid("max_backlog_age_micros"));
        }
        if self.foreground_budget_micros <= 0 {
            return Err(invalid("foreground_budget_micros"));
        }
        if self.foreground_breach_streak == 0 {
            return Err(invalid("foreground_breach_streak"));
        }
        Ok(())
    }
}

/// What one measured foreground admission did against its declared budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForegroundOutcomeV1 {
    /// The admission finished inside the budget.
    WithinBudget {
        /// Measured wall time.
        elapsed_micros: i64,
        /// Declared budget.
        budget_micros: i64,
    },
    /// The admission overran the budget.
    BudgetExceeded {
        /// Measured wall time.
        elapsed_micros: i64,
        /// Declared budget.
        budget_micros: i64,
        /// Consecutive overruns including this one. Optional work sheds once
        /// this reaches the policy's `foreground_breach_streak`.
        consecutive_breaches: u32,
    },
}

impl ForegroundOutcomeV1 {
    /// Whether the sample was inside the declared budget.
    #[must_use]
    pub const fn within_budget(&self) -> bool {
        matches!(self, Self::WithinBudget { .. })
    }

    /// The measured wall time either way.
    #[must_use]
    pub const fn elapsed_micros(&self) -> i64 {
        match self {
            Self::WithinBudget { elapsed_micros, .. }
            | Self::BudgetExceeded { elapsed_micros, .. } => *elapsed_micros,
        }
    }
}

/// One backlog measurement: size, age, and the state they imply.
///
/// This is the metrics record. It is produced from a real
/// [`QueuePressureV1`] read against the journal and a caller-supplied instant —
/// the runtime mints no clock of its own — so every field here is measured
/// rather than estimated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueBacklogV1 {
    /// Non-terminal rows queued for the lane.
    pub queue_items: u64,
    /// Non-terminal queue bytes held by the lane.
    pub queue_bytes: u64,
    /// Configured item ceiling.
    pub max_queue_items: u64,
    /// Configured byte ceiling.
    pub max_queue_bytes: u64,
    /// Items as parts-per-million of the item ceiling.
    pub items_utilization_ppm: u32,
    /// Bytes as parts-per-million of the byte ceiling.
    pub bytes_utilization_ppm: u32,
    /// The larger of the two, which is what the thresholds compare against: a
    /// lane full by bytes is just as full as a lane full by rows.
    pub utilization_ppm: u32,
    /// How long the oldest non-terminal row has waited. Zero when the lane is
    /// empty or when the reading instant precedes that row's admission.
    pub oldest_backlog_age_micros: i64,
    /// Last measured foreground admission latency, when one has been observed.
    pub foreground_latency_micros: Option<i64>,
    /// Consecutive foreground admissions that overran the budget at the moment
    /// of the reading.
    pub foreground_breaches: u32,
    /// The measurement that moved the lane off nominal, when one did.
    pub trigger: Option<BackpressureReasonV1>,
    /// The state these measurements imply.
    pub state: BackpressureStateV1,
    /// Instant the measurement was taken, supplied by the caller.
    pub observed_at_unix_micros: i64,
}

impl QueueBacklogV1 {
    /// Whether the lane is holding work that has not settled.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.queue_items == 0
    }
}

/// The answer to "may this record be appended right now".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpressureDecisionV1 {
    /// Append it.
    Admit,
    /// Refuse it, with the measurements and reason that refused it. Nothing is
    /// discarded: the caller stops and re-presents the record later.
    Shed(BackpressureRefusalV1),
}

/// The typed refusal a shed carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackpressureRefusalV1 {
    /// Class the refused record was in.
    pub load_class: ObservationLoadClassV1,
    /// Why it was refused.
    pub reason: BackpressureReasonV1,
    /// Lane state at the moment of refusal.
    pub state: BackpressureStateV1,
    /// Queue weight the record would have added.
    pub additional_bytes: u64,
    /// Utilization the lane *would* have reached had this record been
    /// appended. This is what the thresholds are applied to, and it is why the
    /// reserved band is real: a threshold checked against the queue as it was
    /// before the append lets one heavy record jump straight from nominal into
    /// the band — or past the refusal point — in a single step.
    pub projected_utilization_ppm: u32,
    /// The measurements the refusal was taken on.
    pub backlog: QueueBacklogV1,
}

/// A shed positioned in its source stream, so a caller can report exactly which
/// record the lane stopped at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackpressureHaltV1 {
    /// Position the lane refused.
    pub source_sequence: SourceSequenceV1,
    /// Settled event identity at that position.
    pub source_event_id: String,
    /// The typed refusal, carried verbatim.
    pub refusal: BackpressureRefusalV1,
}

/// The mounted gate: one declared policy, the last foreground sample, and the
/// last backlog measurement it produced.
///
/// The two remembered values are what make this a gate rather than a formula.
/// Both are bounded — one `i64` and one fixed-size record — and both are
/// overwritten rather than accumulated, so nothing here grows.
#[derive(Debug)]
pub struct BackpressureGateV1 {
    policy: BackpressurePolicyV1,
    /// Last measured foreground admission latency, or a negative sentinel when
    /// nothing has been measured yet.
    foreground_micros: AtomicI64,
    /// Consecutive admissions that overran the budget. Reset by the first one
    /// that does not, so this counts a run rather than a lifetime total.
    foreground_breaches: AtomicU32,
    last_backlog: Mutex<Option<QueueBacklogV1>>,
}

/// Sentinel meaning "no foreground admission has been measured yet".
const NO_FOREGROUND_SAMPLE: i64 = -1;

impl BackpressureGateV1 {
    /// Binds one validated policy. A policy that cannot bound the lane is
    /// refused here rather than silently ignored at the first saturation.
    pub fn new(policy: BackpressurePolicyV1) -> Result<Self, ObservationJournalError> {
        policy.validate()?;
        Ok(Self {
            policy,
            foreground_micros: AtomicI64::new(NO_FOREGROUND_SAMPLE),
            foreground_breaches: AtomicU32::new(0),
            last_backlog: Mutex::new(None),
        })
    }

    /// The thresholds this gate enforces.
    #[must_use]
    pub const fn policy(&self) -> &BackpressurePolicyV1 {
        &self.policy
    }

    /// Records one measured foreground admission and classifies it.
    ///
    /// A negative measurement is impossible from a monotonic clock and is
    /// clamped to zero rather than being stored as the "no sample" sentinel.
    pub fn observe_foreground(&self, elapsed_micros: i64) -> ForegroundOutcomeV1 {
        let elapsed_micros = elapsed_micros.max(0);
        self.foreground_micros
            .store(elapsed_micros, Ordering::Relaxed);
        let budget_micros = self.policy.foreground_budget_micros;
        if elapsed_micros > budget_micros {
            let breaches = self
                .foreground_breaches
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            ForegroundOutcomeV1::BudgetExceeded {
                elapsed_micros,
                budget_micros,
                consecutive_breaches: breaches,
            }
        } else {
            self.foreground_breaches.store(0, Ordering::Relaxed);
            ForegroundOutcomeV1::WithinBudget {
                elapsed_micros,
                budget_micros,
            }
        }
    }

    /// Consecutive admissions that have overrun the budget without a recovery.
    #[must_use]
    pub fn foreground_breaches(&self) -> u32 {
        self.foreground_breaches.load(Ordering::Relaxed)
    }

    /// The last measured foreground admission latency, when one exists.
    #[must_use]
    pub fn foreground_sample(&self) -> Option<i64> {
        let sample = self.foreground_micros.load(Ordering::Relaxed);
        (sample >= 0).then_some(sample)
    }

    /// Turns one journal pressure reading into a backlog measurement, and
    /// remembers it as this lane's current metrics.
    ///
    /// `now_unix_micros` is the caller's instant — in ingress it is the
    /// admitting authority's own `admitted_at` stamp, so the runtime still
    /// mints no clock.
    pub fn observe(&self, pressure: &QueuePressureV1, now_unix_micros: i64) -> QueueBacklogV1 {
        let items_utilization_ppm = utilization_ppm(pressure.queue_items, pressure.max_queue_items);
        let bytes_utilization_ppm = utilization_ppm(pressure.queue_bytes, pressure.max_queue_bytes);
        let utilization_ppm = items_utilization_ppm.max(bytes_utilization_ppm);
        let oldest_backlog_age_micros = pressure
            .oldest_admitted_at_unix_micros
            .map_or(0, |admitted_at| now_unix_micros.saturating_sub(admitted_at))
            .max(0);
        let foreground_latency_micros = self.foreground_sample();
        let foreground_breaches = self.foreground_breaches();

        // Ordered by severity: a saturated lane is saturated whatever else is
        // also true, and among the shed triggers the one that is reported is
        // the one an operator should act on first.
        let (state, trigger) = if utilization_ppm >= self.policy.refuse_at_ppm {
            (
                BackpressureStateV1::Saturated,
                Some(BackpressureReasonV1::QueueUtilization),
            )
        } else if utilization_ppm >= self.policy.shed_optional_at_ppm {
            (
                BackpressureStateV1::SheddingOptional,
                Some(BackpressureReasonV1::QueueUtilization),
            )
        } else if oldest_backlog_age_micros >= self.policy.max_backlog_age_micros
            && pressure.queue_items > 0
        {
            (
                BackpressureStateV1::SheddingOptional,
                Some(BackpressureReasonV1::BacklogAge),
            )
        } else if foreground_breaches >= self.policy.foreground_breach_streak {
            (
                BackpressureStateV1::SheddingOptional,
                Some(BackpressureReasonV1::ForegroundBudget),
            )
        } else {
            (BackpressureStateV1::Nominal, None)
        };

        let backlog = QueueBacklogV1 {
            queue_items: pressure.queue_items,
            queue_bytes: pressure.queue_bytes,
            max_queue_items: pressure.max_queue_items,
            max_queue_bytes: pressure.max_queue_bytes,
            items_utilization_ppm,
            bytes_utilization_ppm,
            utilization_ppm,
            oldest_backlog_age_micros,
            foreground_latency_micros,
            foreground_breaches,
            trigger,
            state,
            observed_at_unix_micros: now_unix_micros,
        };
        let mut slot = match self.last_backlog.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        *slot = Some(backlog);
        backlog
    }

    /// The most recent backlog measurement, for metrics and inspection.
    #[must_use]
    pub fn metrics(&self) -> Option<QueueBacklogV1> {
        match self.last_backlog.lock() {
            Ok(slot) => *slot,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    /// Utilization the lane would hold with this record in it.
    ///
    /// Every threshold is applied to *this*, never to the backlog as it was
    /// measured. A record is admitted for what it would make the lane, because
    /// the queue the product has to survive is the one that exists after the
    /// append: with payloads allowed into the megabytes, a lane sitting just
    /// under a threshold can be carried past it — and through the whole
    /// reserved band — by one candidate, and a gate that looked only at the
    /// old reading would call that admission nominal.
    #[must_use]
    fn projected_utilization_ppm(backlog: &QueueBacklogV1, additional_bytes: u64) -> u32 {
        let items = utilization_ppm(
            backlog.queue_items.saturating_add(1),
            backlog.max_queue_items,
        );
        let bytes = utilization_ppm(
            backlog.queue_bytes.saturating_add(additional_bytes),
            backlog.max_queue_bytes,
        );
        items.max(bytes)
    }

    /// Answers admit or shed for one record of `additional_bytes` in `class`.
    ///
    /// The ceiling check comes first and is absolute: the journal would refuse
    /// the append with a typed capacity outcome anyway, and paying for the
    /// append transaction to learn that is exactly the foreground cost this
    /// gate exists to avoid.
    ///
    /// Then the declared thresholds run against the *projected* utilization,
    /// so the shed threshold and the refusal threshold bound the lane the
    /// append would produce rather than the lane the last reading described.
    ///
    /// The projected comparison is strict where the measured one is not, and
    /// the pair is what makes a threshold exact: a record that lands the lane
    /// *on* a threshold is the last one that threshold allows, and the very
    /// next record meets a measured reading that is already at it. A
    /// non-strict projection would instead make a threshold of 100 % refuse
    /// the record that fills the final slot the hard ceiling explicitly
    /// permits — a lane of capacity one could then never admit anything.
    ///
    /// The measured state still has the last word for the triggers projection
    /// cannot see — a backlog that has stopped draining, and an admission path
    /// that has overrun its foreground budget — because neither of those gets
    /// better or worse for the size of the next record.
    ///
    /// `additional_bytes` may legitimately be zero: a caller that has not yet
    /// paid for admission does not know the record's weight, and gating on
    /// `queue_items + 1` alone is the conservative half of the same answer.
    #[must_use]
    pub fn decide(
        &self,
        backlog: &QueueBacklogV1,
        class: ObservationLoadClassV1,
        additional_bytes: u64,
    ) -> BackpressureDecisionV1 {
        let projected_utilization_ppm = Self::projected_utilization_ppm(backlog, additional_bytes);
        let shed = |reason, state| {
            BackpressureDecisionV1::Shed(BackpressureRefusalV1 {
                load_class: class,
                reason,
                state,
                additional_bytes,
                projected_utilization_ppm,
                backlog: *backlog,
            })
        };
        if backlog.queue_items >= backlog.max_queue_items
            || backlog.queue_bytes.saturating_add(additional_bytes) > backlog.max_queue_bytes
        {
            return shed(
                BackpressureReasonV1::QueueCeiling,
                BackpressureStateV1::Saturated,
            );
        }
        if projected_utilization_ppm > self.policy.refuse_at_ppm {
            return shed(
                BackpressureReasonV1::QueueUtilization,
                BackpressureStateV1::Saturated,
            );
        }
        if projected_utilization_ppm > self.policy.shed_optional_at_ppm
            && class == ObservationLoadClassV1::Optional
        {
            return shed(
                BackpressureReasonV1::QueueUtilization,
                BackpressureStateV1::SheddingOptional,
            );
        }
        match backlog.state {
            BackpressureStateV1::Nominal => BackpressureDecisionV1::Admit,
            BackpressureStateV1::SheddingOptional => match class {
                ObservationLoadClassV1::Required => BackpressureDecisionV1::Admit,
                ObservationLoadClassV1::Optional => shed(
                    backlog
                        .trigger
                        .unwrap_or(BackpressureReasonV1::QueueUtilization),
                    backlog.state,
                ),
            },
            BackpressureStateV1::Saturated => shed(
                backlog
                    .trigger
                    .unwrap_or(BackpressureReasonV1::QueueUtilization),
                backlog.state,
            ),
        }
    }
}

/// Parts-per-million of `used` against `capacity`, saturating at the scale.
fn utilization_ppm(used: u64, capacity: u64) -> u32 {
    if capacity == 0 {
        return UTILIZATION_SCALE_PPM;
    }
    let scaled = used
        .saturating_mul(u64::from(UTILIZATION_SCALE_PPM))
        .checked_div(capacity)
        .unwrap_or(u64::from(UTILIZATION_SCALE_PPM));
    u32::try_from(scaled).unwrap_or(UTILIZATION_SCALE_PPM)
}
