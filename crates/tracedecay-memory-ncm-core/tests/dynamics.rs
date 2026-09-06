//! Acceptance tests for explicit logical-time dynamics.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use tracedecay_memory_ncm_core::centers::read::ReadParams;
use tracedecay_memory_ncm_core::centers::MemoryCenters;
use tracedecay_memory_ncm_core::dynamics::{
    advance, emotion_after, homeostasis_step, intensity_after, observe_tick, sleep_due,
    value_after, Scheduler, MAX_ADVANCE_TICKS,
};
use tracedecay_memory_ncm_core::terrain::Terrain3D;
use tracedecay_memory_ncm_core::{CoreError, LogicalTick, NcmConfig};

fn compact_config() -> NcmConfig {
    let mut config = NcmConfig::default();
    config.stm.n_centers = 2;
    config.ltm.n_centers = 2;
    config.terrain_resolution = 3;
    config
}

fn active_centers(
    config: &tracedecay_memory_ncm_core::types::LayerConfig,
    seed: u64,
) -> MemoryCenters {
    let mut centers = MemoryCenters::new(config.clone(), seed).expect("valid center config");
    centers.active[0] = true;
    centers.intensity[0] = 4.0;
    centers.age[0] = 7;
    centers.usage[0] = 11;
    centers.values[..128].fill(0.75);
    centers.affect[..4].copy_from_slice(&[0.25, 0.5, 1.5, 2.0]);
    centers
}

fn seeded_terrain(config: &NcmConfig) -> Terrain3D {
    let mut terrain = Terrain3D::new(
        config.terrain_resolution,
        config.stm.terrain_alpha_h,
        config.stm.terrain_alpha_e,
        config.stm.terrain_lambda,
    )
    .expect("stable terrain config");
    terrain
        .splat([0.2, -0.4, 0.7], 2.0, Some([0.2, 0.8, 1.2, 1.8]), 0.1, 0.02)
        .expect("valid terrain splat");
    terrain
}

fn assert_close(actual: f32, expected: f32, tolerance: f32, label: &str) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{label}: actual={actual:?}, expected={expected:?}, tolerance={tolerance:?}"
    );
}

fn json_digest<T: serde::Serialize>(value: &T) -> u64 {
    serde_json::to_vec(value)
        .expect("serializable state")
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

#[test]
fn explicit_homeostasis_matches_closed_forms_through_one_thousand_ticks() {
    let config = compact_config();
    let mut centers = active_centers(&config.stm, 17);
    let initial_intensity = centers.intensity[0];
    let initial_value = centers.values[0];
    let initial_affect = centers.affect[..4].to_vec();
    let initial_usage = centers.usage[0];

    for ticks in 1..=1_000 {
        homeostasis_step(&mut centers, &config.stm);
        assert_close(
            centers.intensity[0],
            intensity_after(initial_intensity, config.stm.leak, ticks),
            1e-5,
            "intensity closed-form assertion",
        );
        assert_close(
            centers.values[0],
            value_after(initial_value, config.stm.leak_value, ticks),
            1e-5,
            "value closed-form assertion",
        );
        for (channel, initial) in initial_affect.iter().copied().enumerate() {
            assert_close(
                centers.affect[channel],
                emotion_after(initial, config.stm.leak_emotion, ticks),
                1e-5,
                "affect closed-form assertion",
            );
        }
        assert_eq!(centers.age[0], 7 + u64::from(ticks));
        assert_eq!(centers.usage[0], initial_usage);
        assert_eq!(centers.total_step, u64::from(ticks));
    }
}

#[test]
fn intensity_rate_swap_mutant_is_discriminated() {
    let config = compact_config();
    let h0 = 4.0;
    let expected = intensity_after(h0, config.stm.leak, 1);
    let value_rate_mutant = intensity_after(h0, config.stm.leak_value, 1);

    assert!(
        (value_rate_mutant - expected).abs() > 1e-5,
        "the intensity closed-form assertion must reject a mutant using leak_value"
    );

    let mut centers = active_centers(&config.stm, 19);
    homeostasis_step(&mut centers, &config.stm);
    assert_close(
        centers.intensity[0],
        expected,
        1e-5,
        "intensity closed-form assertion",
    );
}

#[test]
fn fatigue_uses_input_intensity_and_observe_tick_advances_once() {
    let mut config = compact_config();
    config.fatigue_leak = 0.2;
    config.fatigue_gain = 0.1;
    let mut scheduler = Scheduler {
        tick: LogicalTick(8),
        fatigue: 1.5,
        steps_since_consolidation: 41,
        last_maintenance: Some(LogicalTick(4)),
    };

    observe_tick(&mut scheduler, 3.0, &config);

    assert_eq!(scheduler.tick, LogicalTick(9));
    assert_eq!(scheduler.steps_since_consolidation, 42);
    assert_eq!(scheduler.last_maintenance, Some(LogicalTick(4)));
    assert_close(
        scheduler.fatigue,
        1.5,
        f32::EPSILON,
        "input intensity fatigue",
    );
}

#[test]
fn sleep_requires_strict_threshold_and_interval_boundary() {
    let config = compact_config();
    for steps in [99, 100, 101] {
        let scheduler = Scheduler {
            tick: LogicalTick(steps),
            fatigue: f32::from_bits(2.5_f32.to_bits() + 1),
            steps_since_consolidation: steps,
            last_maintenance: None,
        };
        assert_eq!(
            sleep_due(&scheduler, &config),
            steps >= 100,
            "steps={steps}"
        );
    }

    let exactly_threshold = Scheduler {
        tick: LogicalTick(101),
        fatigue: 2.5,
        steps_since_consolidation: 101,
        last_maintenance: None,
    };
    assert!(!sleep_due(&exactly_threshold, &config));
}

#[test]
fn exact_fatigue_threshold_crossing_is_strict() {
    let mut config = compact_config();
    config.fatigue_leak = 0.0;
    let mut scheduler = Scheduler {
        steps_since_consolidation: 100,
        ..Scheduler::new()
    };

    observe_tick(&mut scheduler, 25.0, &config);
    assert_eq!(scheduler.fatigue, 2.5);
    assert!(!sleep_due(&scheduler, &config));

    observe_tick(&mut scheduler, f32::EPSILON * 32.0, &config);
    assert!(scheduler.fatigue > 2.5);
    assert!(sleep_due(&scheduler, &config));
}

#[test]
fn split_advance_is_bit_for_bit_equivalent() {
    let config = compact_config();
    let stm = active_centers(&config.stm, 23);
    let ltm = active_centers(&config.ltm, 29);
    let terrain = seeded_terrain(&config);
    let scheduler = Scheduler {
        tick: LogicalTick(12),
        fatigue: 3.25,
        steps_since_consolidation: 17,
        last_maintenance: None,
    };

    let mut whole_stm = stm.clone();
    let mut whole_ltm = ltm.clone();
    let mut whole_terrain = terrain.clone();
    let mut whole_scheduler = scheduler;
    let whole_report = advance(
        &mut whole_stm,
        &mut whole_ltm,
        &mut whole_terrain,
        &mut whole_scheduler,
        1_000,
        &config,
    )
    .expect("bounded advance");

    let mut split_stm = stm;
    let mut split_ltm = ltm;
    let mut split_terrain = terrain;
    let mut split_scheduler = scheduler;
    advance(
        &mut split_stm,
        &mut split_ltm,
        &mut split_terrain,
        &mut split_scheduler,
        400,
        &config,
    )
    .expect("first bounded advance");
    let split_report = advance(
        &mut split_stm,
        &mut split_ltm,
        &mut split_terrain,
        &mut split_scheduler,
        600,
        &config,
    )
    .expect("second bounded advance");

    assert_eq!(
        serde_json::to_vec(&whole_stm).unwrap(),
        serde_json::to_vec(&split_stm).unwrap()
    );
    assert_eq!(
        serde_json::to_vec(&whole_ltm).unwrap(),
        serde_json::to_vec(&split_ltm).unwrap()
    );
    assert_eq!(whole_terrain.digest(), split_terrain.digest());
    assert_eq!(
        serde_json::to_vec(&whole_scheduler).unwrap(),
        serde_json::to_vec(&split_scheduler).unwrap()
    );
    assert_eq!(whole_report.sleep_due, split_report.sleep_due);
    assert_eq!(whole_scheduler.last_maintenance, Some(LogicalTick(1_012)));
}

#[test]
fn over_budget_advance_fails_before_mutation() {
    let config = compact_config();
    let mut stm = active_centers(&config.stm, 31);
    let mut ltm = active_centers(&config.ltm, 37);
    let mut terrain = seeded_terrain(&config);
    let mut scheduler = Scheduler {
        tick: LogicalTick(4),
        fatigue: 1.25,
        steps_since_consolidation: 9,
        last_maintenance: Some(LogicalTick(3)),
    };
    let stm_digest = json_digest(&stm);
    let ltm_digest = json_digest(&ltm);
    let terrain_digest = terrain.digest();
    let scheduler_digest = json_digest(&scheduler);

    let error = advance(
        &mut stm,
        &mut ltm,
        &mut terrain,
        &mut scheduler,
        MAX_ADVANCE_TICKS + 1,
        &config,
    )
    .expect_err("over-budget advance must fail");

    assert_eq!(error, CoreError::BudgetExceeded("advance ticks"));
    assert_eq!(json_digest(&stm), stm_digest);
    assert_eq!(json_digest(&ltm), ltm_digest);
    assert_eq!(terrain.digest(), terrain_digest);
    assert_eq!(json_digest(&scheduler), scheduler_digest);
}

#[test]
fn read_only_recall_changes_neither_centers_nor_scheduler() {
    let config = compact_config();
    let mut centers = active_centers(&config.stm, 41);
    centers.values[0] = 0.5;
    let query = centers.key(0).to_vec();
    let scheduler = Scheduler {
        tick: LogicalTick(77),
        fatigue: 2.25,
        steps_since_consolidation: 98,
        last_maintenance: Some(LogicalTick(60)),
    };
    let centers_digest = json_digest(&centers);
    let scheduler_digest = json_digest(&scheduler);

    let result = centers
        .read(
            &query,
            ReadParams {
                top_k: 1,
                ..ReadParams::default()
            },
        )
        .expect("read-only recall");

    assert_eq!(result.selection.centers.len(), 1);
    assert_eq!(json_digest(&centers), centers_digest);
    assert_eq!(json_digest(&scheduler), scheduler_digest);
}

#[test]
fn scheduler_serde_preserves_persisted_fields() {
    let scheduler = Scheduler {
        tick: LogicalTick(123),
        fatigue: 2.75,
        steps_since_consolidation: 101,
        last_maintenance: Some(LogicalTick(120)),
    };
    let encoded = serde_json::to_vec(&scheduler).expect("serialize scheduler");
    let decoded: Scheduler = serde_json::from_slice(&encoded).expect("deserialize scheduler");
    assert_eq!(decoded, scheduler);
}
