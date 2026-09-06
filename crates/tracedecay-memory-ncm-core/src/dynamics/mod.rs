//! Explicit logical-time homeostasis and fatigue scheduling.
//!
//! Time in this module is counted only in committed learning or maintenance
//! ticks. It has no calendar-time mapping: the leak coefficients are per tick,
//! not half-lives in days or years.

use crate::centers::MemoryCenters;
use crate::terrain::Terrain3D;
use crate::types::{AFFECT_DIM, CoreError, LayerConfig, LogicalTick, NcmConfig, VALUE_DIM};
use serde::{Deserialize, Serialize};

/// Frozen v1 budget for one explicit maintenance advance (contract D10).
///
/// This is a profile constant because the shared frozen [`NcmConfig`] does not
/// contain a maintenance-budget field.
pub const MAX_ADVANCE_TICKS: u32 = 10_000;

/// Persisted state for deterministic learning and consolidation eligibility.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scheduler {
    /// Current logical learning tick.
    pub tick: LogicalTick,
    /// Current exponentially leaked fatigue.
    pub fatigue: f32,
    /// Ticks since the last completed consolidation.
    pub steps_since_consolidation: u64,
    /// Logical tick of the most recent explicit maintenance advance.
    pub last_maintenance: Option<LogicalTick>,
}

impl Scheduler {
    /// Creates a scheduler at logical tick zero with no fatigue.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            tick: LogicalTick(0),
            fatigue: 0.0,
            steps_since_consolidation: 0,
            last_maintenance: None,
        }
    }
}

/// Outcome of a bounded explicit maintenance advance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvanceReport {
    /// Number of ticks applied.
    pub ticks_applied: u32,
    /// Whether consolidation is due after the applied ticks.
    pub sleep_due: bool,
}

/// Applies one reference-compatible homeostasis step to a center bank.
///
/// Active intensity and values decay toward zero, affect decays toward neutral
/// one, and active ages increment. Usage is intentionally unchanged (D05).
/// The operation order matches `MemoryCenters.homeostasis_step`: intensity,
/// values, affect, ages, then the bank's total-step counter.
pub fn homeostasis_step(centers: &mut MemoryCenters, layer_cfg: &LayerConfig) {
    let intensity_factor = 1.0 - layer_cfg.leak;
    let value_factor = 1.0 - layer_cfg.leak_value;
    let affect_factor = 1.0 - layer_cfg.leak_emotion;

    for index in 0..centers.config.n_centers {
        if !centers.active[index] {
            continue;
        }

        centers.intensity[index] *= intensity_factor;

        let value_start = index * VALUE_DIM;
        for value in &mut centers.values[value_start..value_start + VALUE_DIM] {
            *value *= value_factor;
        }

        let affect_start = index * AFFECT_DIM;
        for affect in &mut centers.affect[affect_start..affect_start + AFFECT_DIM] {
            *affect = affect_factor * *affect + layer_cfg.leak_emotion * 1.0;
        }

        centers.age[index] = centers.age[index].saturating_add(1);
    }

    centers.total_step = centers.total_step.saturating_add(1);
}

/// Applies one fatigue update using the input observation intensity.
///
/// The Biomem `TextMemory.store` path passes `1.0 * intensity`, not the derived
/// write strength omega: `F = (1-lambda_F)F + fatigue_gain*intensity`.
pub fn fatigue_step(scheduler: &mut Scheduler, intensity: f32, config: &NcmConfig) {
    scheduler.fatigue =
        (1.0 - config.fatigue_leak) * scheduler.fatigue + config.fatigue_gain * intensity;
}

/// Returns whether both the strict fatigue threshold and minimum interval hold.
#[must_use]
pub fn sleep_due(scheduler: &Scheduler, config: &NcmConfig) -> bool {
    scheduler.fatigue > config.fatigue_threshold
        && scheduler.steps_since_consolidation >= config.consolidation_min_interval
}

/// Applies bounded explicit maintenance ticks with no observation input.
///
/// Each tick applies center homeostasis to STM and LTM, evolves the terrain,
/// leaks fatigue with zero input intensity, and advances logical counters.
/// Requests over [`MAX_ADVANCE_TICKS`] fail before any state is changed.
pub fn advance(
    centers_stm: &mut MemoryCenters,
    centers_ltm: &mut MemoryCenters,
    terrain: &mut Terrain3D,
    scheduler: &mut Scheduler,
    ticks: u32,
    config: &NcmConfig,
) -> Result<AdvanceReport, CoreError> {
    if ticks > MAX_ADVANCE_TICKS {
        return Err(CoreError::BudgetExceeded("advance ticks"));
    }

    for _ in 0..ticks {
        homeostasis_step(centers_stm, &config.stm);
        homeostasis_step(centers_ltm, &config.ltm);
        terrain.step();
        fatigue_step(scheduler, 0.0, config);
        scheduler.tick.0 = scheduler.tick.0.saturating_add(1);
        scheduler.steps_since_consolidation = scheduler.steps_since_consolidation.saturating_add(1);
    }

    if ticks != 0 {
        scheduler.last_maintenance = Some(scheduler.tick);
    }

    Ok(AdvanceReport {
        ticks_applied: ticks,
        sleep_due: sleep_due(scheduler, config),
    })
}

/// Applies the scheduler portion of one unique committed observation tick.
///
/// Engine callers invoke this exactly once after a committed unique write and
/// terrain splat. Reads and replayed/duplicate observations must not call it.
/// Center and terrain homeostasis for the frozen D10 observation schedule is
/// applied separately by the engine after this scheduler update.
pub fn observe_tick(scheduler: &mut Scheduler, intensity: f32, config: &NcmConfig) {
    scheduler.tick.0 = scheduler.tick.0.saturating_add(1);
    scheduler.steps_since_consolidation = scheduler.steps_since_consolidation.saturating_add(1);
    fatigue_step(scheduler, intensity, config);
}

/// Returns `h0 * (1 - leak)^ticks` using real `f32` arithmetic.
///
/// `ticks` are explicit logical ticks. There is no calendar-time mapping and
/// this helper makes no claim about a half-life in days or years.
#[must_use]
pub fn intensity_after(h0: f32, leak: f32, ticks: u32) -> f32 {
    h0 * decay_factor(leak, ticks)
}

/// Returns `v0 * (1 - leak)^ticks` for one value component.
///
/// The coefficient is per logical tick; no calendar-time mapping is implied.
#[must_use]
pub fn value_after(v0: f32, leak: f32, ticks: u32) -> f32 {
    v0 * decay_factor(leak, ticks)
}

/// Returns `1 + (e0 - 1) * (1 - leak)^ticks` for one affect component.
///
/// Affect approaches the reference neutral value one. The coefficient is per
/// logical tick; no calendar-time mapping is implied.
#[must_use]
pub fn emotion_after(e0: f32, leak: f32, ticks: u32) -> f32 {
    1.0 + (e0 - 1.0) * decay_factor(leak, ticks)
}

fn decay_factor(leak: f32, mut ticks: u32) -> f32 {
    let mut base = 1.0 - leak;
    let mut result = 1.0;
    while ticks != 0 {
        if ticks & 1 == 1 {
            result *= base;
        }
        ticks >>= 1;
        if ticks != 0 {
            base *= base;
        }
    }
    result
}
