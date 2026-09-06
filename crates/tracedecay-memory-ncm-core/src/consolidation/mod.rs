//! Atomic STM-to-LTM consolidation, center merging, and pruning.
//!
//! Every public mutator stages all affected values in clones and publishes them
//! only after the complete operation succeeds. Callers therefore never observe
//! a partially transferred, normalized, poured, merged, or pruned generation.

use crate::centers::MemoryCenters;
use crate::centers::write::{WriteInput, WriteOutcome, WriteParams};
use crate::dynamics::Scheduler;
use crate::numeric::{cosine, normalize, validate_finite};
use crate::records::{RecordState, RecordTable, Support};
use crate::terrain::Terrain3D;
use crate::types::{
    AFFECT_DIM, AffectVector, CONTEXT_DIM, CenterSlot, CoreError, LTM_KEY_DIM, Layer, NcmConfig,
    RecordId, TERRAIN_DIM, VALUE_DIM,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const PAIR_ROW_PAGE: usize = 256;
const UNPAGED_PAIR_LIMIT: usize = 1024;

/// Statistics for one completed consolidation generation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsolidationReport {
    /// Number of selected STM centers transferred to LTM.
    pub transferred: usize,
    /// Number of latent center pairs merged by the same maintenance request.
    pub merged: usize,
    /// Number of weak latent centers pruned by the same maintenance request.
    pub pruned: usize,
    /// Scheduler fatigue before consolidation.
    pub fatigue_before: f32,
    /// Scheduler fatigue after reference relief (`F *= rho_F`).
    pub fatigue_after: f32,
}

/// Statistics for one merge pass.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeReport {
    /// Number of pairs merged into their lower slot index.
    pub merged: usize,
    /// Valid source-record pairs whose assertions prevented a latent merge.
    pub conflicts: Vec<(RecordId, RecordId)>,
}

/// Statistics for one prune pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneReport {
    /// Number of slots deactivated and scrubbed.
    pub pruned: usize,
}

/// Combined merge/prune statistics for STM and LTM.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergePruneReport {
    /// STM merge result.
    pub stm_merge: MergeReport,
    /// LTM merge result.
    pub ltm_merge: MergeReport,
    /// STM prune result.
    pub stm_prune: PruneReport,
    /// LTM prune result.
    pub ltm_prune: PruneReport,
}

impl MergePruneReport {
    /// Total merged pairs across both layers.
    #[must_use]
    pub const fn merged(&self) -> usize {
        self.stm_merge.merged + self.ltm_merge.merged
    }

    /// Total pruned slots across both layers.
    #[must_use]
    pub const fn pruned(&self) -> usize {
        self.stm_prune.pruned + self.ltm_prune.pruned
    }
}

/// Selects active STM slots above the normalization floor.
///
/// Results are ordered by descending intensity; exact ties use ascending slot
/// index (D13), and at most `consolidation_top_m` entries are returned.
#[must_use]
pub fn select_for_transfer(stm: &MemoryCenters, config: &NcmConfig) -> Vec<usize> {
    let mut selected: Vec<usize> = (0..stm.config.n_centers)
        .filter(|index| {
            stm.active[*index] && stm.intensity[*index] > config.normalization_min_intensity
        })
        .collect();
    selected.sort_by(|left, right| {
        stm.intensity[*right]
            .total_cmp(&stm.intensity[*left])
            .then_with(|| left.cmp(right))
    });
    selected.truncate(config.consolidation_top_m);
    selected
}

/// Transfers selected STM centers into LTM using canonical record LTM keys (D08).
///
/// The function stages both center banks and support map, then publishes all
/// three together. Terrain pour and scheduler relief are performed by
/// [`consolidate`]. The report's fatigue fields are zero because this lower-level
/// operation does not receive a scheduler.
pub fn transfer(
    stm: &mut MemoryCenters,
    ltm: &mut MemoryCenters,
    records: &RecordTable,
    support: &mut Support,
    config: &NcmConfig,
) -> Result<ConsolidationReport, CoreError> {
    let mut staged_stm = stm.clone();
    let mut staged_ltm = ltm.clone();
    let mut staged_support = support.clone();
    let transferred = transfer_staged(
        &mut staged_stm,
        &mut staged_ltm,
        records,
        &mut staged_support,
        config,
    )?;
    *stm = staged_stm;
    *ltm = staged_ltm;
    *support = staged_support;
    Ok(ConsolidationReport {
        transferred,
        merged: 0,
        pruned: 0,
        fatigue_before: 0.0,
        fatigue_after: 0.0,
    })
}

/// Completes an atomic consolidation generation, including real blurred terrain
/// pour (D01), both-layer normalization, scheduler relief, and interval reset.
#[allow(clippy::too_many_arguments)]
pub fn consolidate(
    stm: &mut MemoryCenters,
    ltm: &mut MemoryCenters,
    stm_terrain: &mut Terrain3D,
    ltm_terrain: &mut Terrain3D,
    records: &RecordTable,
    support: &mut Support,
    scheduler: &mut Scheduler,
    config: &NcmConfig,
) -> Result<ConsolidationReport, CoreError> {
    let mut staged_stm = stm.clone();
    let mut staged_ltm = ltm.clone();
    let staged_stm_terrain = stm_terrain.clone();
    let mut staged_ltm_terrain = ltm_terrain.clone();
    let mut staged_support = support.clone();
    let mut staged_scheduler = *scheduler;

    let fatigue_before = staged_scheduler.fatigue;
    let transferred = transfer_staged(
        &mut staged_stm,
        &mut staged_ltm,
        records,
        &mut staged_support,
        config,
    )?;
    staged_ltm_terrain.merge_from(
        &staged_stm_terrain,
        config.consolidation_xi_h,
        config.consolidation_xi_e,
        config.consolidation_blur_sigma,
    )?;
    staged_scheduler.fatigue *= config.normalization_rho_f;
    validate_finite(&[staged_scheduler.fatigue], "consolidation fatigue")?;
    staged_scheduler.steps_since_consolidation = 0;
    let fatigue_after = staged_scheduler.fatigue;

    *stm = staged_stm;
    *ltm = staged_ltm;
    *stm_terrain = staged_stm_terrain;
    *ltm_terrain = staged_ltm_terrain;
    *support = staged_support;
    *scheduler = staged_scheduler;

    Ok(ConsolidationReport {
        transferred,
        merged: 0,
        pruned: 0,
        fatigue_before,
        fatigue_after,
    })
}

/// Greedily merges cosine-similar centers into the lower slot index.
///
/// Pair rows are processed in deterministic pages of 256 once more than 1024
/// active slots exist, bounding temporary pair storage. Valid records with
/// differing assertions are retained as separate centers and reported.
pub fn merge(
    centers: &mut MemoryCenters,
    records: &RecordTable,
    support: &mut Support,
    config: &NcmConfig,
) -> Result<MergeReport, CoreError> {
    let mut staged_centers = centers.clone();
    let mut staged_support = support.clone();
    let report = merge_staged(&mut staged_centers, records, &mut staged_support, config)?;
    *centers = staged_centers;
    *support = staged_support;
    Ok(report)
}

/// Prunes weak, old, unused centers while retaining immutable records.
///
/// The effective threshold is `max(configured, 0.011)`. Scrubbing invalidates
/// the old handle immediately and advances the freed slot incarnation.
pub fn prune(
    centers: &mut MemoryCenters,
    _records: &RecordTable,
    support: &mut Support,
    config: &NcmConfig,
) -> Result<PruneReport, CoreError> {
    let mut staged_centers = centers.clone();
    let mut staged_support = support.clone();
    let effective_threshold = config.prune_intensity_threshold.max(0.011);
    let mut selected = Vec::new();
    for index in 0..staged_centers.config.n_centers {
        if staged_centers.active[index]
            && staged_centers.intensity[index] < effective_threshold
            && staged_centers.age[index] > config.prune_min_age
            && staged_centers.usage[index] < config.prune_usage_ceiling
        {
            selected.push(index);
        }
    }
    for index in &selected {
        deactivate_and_invalidate(&mut staged_centers, &mut staged_support, *index)?;
    }
    *centers = staged_centers;
    *support = staged_support;
    Ok(PruneReport {
        pruned: selected.len(),
    })
}

fn transfer_staged(
    stm: &mut MemoryCenters,
    ltm: &mut MemoryCenters,
    records: &RecordTable,
    support: &mut Support,
    config: &NcmConfig,
) -> Result<usize, CoreError> {
    let selected = select_for_transfer(stm, config);
    let write_params = WriteParams::for_layer(config, Layer::Ltm);
    for index in &selected {
        let source_slot = stm.slot(*index).ok_or_else(|| {
            CoreError::InvalidState("STM slot index does not fit CenterSlot".to_owned())
        })?;
        let source_records = support.support_for(source_slot)?.to_vec();
        if source_records.is_empty() {
            return Err(CoreError::InvalidState(
                "selected STM center has no source support".to_owned(),
            ));
        }
        let canonical_key = canonical_ltm_key(records, &source_records)?;
        let representative = stm.record[*index]
            .filter(|record| source_records.contains(record))
            .unwrap_or(source_records[0]);
        let value = row_array::<VALUE_DIM>(&stm.values, *index, "STM value")?;
        let affect = AffectVector(row_array::<AFFECT_DIM>(&stm.affect, *index, "STM affect")?);
        let context = row_array::<CONTEXT_DIM>(&stm.context, *index, "STM context")?;
        let terrain = row_array::<TERRAIN_DIM>(&stm.terrain, *index, "STM terrain")?;
        let omega = (config.consolidation_kappa * stm.intensity[*index])
            .max(config.consolidation_min_intensity);
        validate_finite(&[omega], "consolidation transfer strength")?;
        let input = WriteInput {
            key: &canonical_key,
            ltm_key: array_from_vec::<LTM_KEY_DIM>(&canonical_key, "canonical LTM key")?,
            value,
            affect,
            intensity: omega,
            context,
            terrain,
            record: representative,
            age: stm.age[*index].saturating_add(1),
        };
        let outcome = ltm.write(input, &write_params)?;
        let destinations = outcome_slots(&outcome)?;
        for destination in destinations {
            propagate_support(
                support,
                ltm,
                destination,
                source_slot,
                &source_records,
                config.max_center_support,
            )?;
        }
        stm.intensity[*index] *= (1.0 - config.consolidation_kappa).max(0.0);
        validate_finite(&[stm.intensity[*index]], "post-transfer STM intensity")?;
    }
    normalize_centers(ltm, config)?;
    normalize_centers(stm, config)?;
    Ok(selected.len())
}

fn outcome_slots(outcome: &WriteOutcome) -> Result<Vec<CenterSlot>, CoreError> {
    match outcome {
        WriteOutcome::Ignored => Err(CoreError::InvalidState(
            "consolidation transfer was unexpectedly ignored".to_owned(),
        )),
        WriteOutcome::CapacityExhausted => Err(CoreError::CapacityExhausted(Layer::Ltm)),
        WriteOutcome::Created { slot } => Ok(vec![*slot]),
        WriteOutcome::Reinforced { updated, .. } => Ok(updated.clone()),
    }
}

fn canonical_ltm_key(
    records: &RecordTable,
    source_records: &[RecordId],
) -> Result<Vec<f32>, CoreError> {
    let mut mean = vec![0.0; LTM_KEY_DIM];
    for record_id in source_records {
        let record = records
            .get(*record_id)
            .ok_or(CoreError::UnknownRecord(*record_id))?;
        for (sum, component) in mean.iter_mut().zip(&record.ltm_key) {
            *sum += *component;
        }
    }
    let common_intensity_weight = 1.0 / source_records.len() as f32;
    for component in &mut mean {
        *component *= common_intensity_weight;
    }
    normalize(&mean)
}

fn propagate_support(
    support: &mut Support,
    centers: &mut MemoryCenters,
    destination: CenterSlot,
    source: CenterSlot,
    source_records: &[RecordId],
    maximum: usize,
) -> Result<(), CoreError> {
    let destination_index = centers.resolve(destination)?;
    let mut destination_records = centers.support[destination_index].clone();
    for record in source_records {
        if !destination_records.contains(record) {
            destination_records.push(*record);
        }
    }
    if destination_records.len() > maximum {
        return Err(CoreError::BudgetExceeded("merged center support"));
    }
    support.set_support(destination, &centers.support[destination_index])?;
    if destination.index != source.index {
        support.merge_support(destination, source)?;
    }
    support.set_support(destination, &destination_records)?;
    centers.support[destination_index] = destination_records;
    Ok(())
}

fn normalize_centers(centers: &mut MemoryCenters, config: &NcmConfig) -> Result<(), CoreError> {
    validate_finite(
        &[config.normalization_c_v, config.normalization_min_intensity],
        "normalization parameters",
    )?;
    if config.normalization_c_v <= 0.0 || config.normalization_min_intensity < 0.0 {
        return Err(CoreError::InvalidState(
            "normalization requires positive c_v and nonnegative floor".to_owned(),
        ));
    }
    for index in 0..centers.config.n_centers {
        if !centers.active[index] {
            continue;
        }
        let intensity = centers.intensity[index].ln_1p();
        centers.intensity[index] = intensity.max(config.normalization_min_intensity);
        let start = index * VALUE_DIM;
        let norm = centers.values[start..start + VALUE_DIM]
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        let denominator = 1.0 + norm / config.normalization_c_v;
        for value in &mut centers.values[start..start + VALUE_DIM] {
            *value /= denominator;
        }
        let affect_start = index * AFFECT_DIM;
        for value in &mut centers.affect[affect_start..affect_start + AFFECT_DIM] {
            *value = value.tanh();
        }
    }
    validate_finite(&centers.intensity, "normalized intensity")?;
    validate_finite(&centers.values, "normalized values")?;
    validate_finite(&centers.affect, "normalized affect")
}

fn merge_staged(
    centers: &mut MemoryCenters,
    records: &RecordTable,
    support: &mut Support,
    config: &NcmConfig,
) -> Result<MergeReport, CoreError> {
    validate_finite(
        &[config.merge_similarity_threshold],
        "merge similarity threshold",
    )?;
    let active: Vec<usize> = (0..centers.config.n_centers)
        .filter(|index| centers.active[*index])
        .collect();
    if active.len() < 2 {
        return Ok(MergeReport::default());
    }
    let page_rows = if active.len() > UNPAGED_PAIR_LIMIT {
        PAIR_ROW_PAGE
    } else {
        active.len()
    };
    let mut blocked = BTreeSet::new();
    let mut conflicts = BTreeSet::new();
    let mut merged = 0;
    for page_start in (0..active.len()).step_by(page_rows.max(1)) {
        let page_end = (page_start + page_rows).min(active.len());
        let mut pairs = Vec::new();
        for left_position in page_start..page_end {
            for right_position in left_position + 1..active.len() {
                let left = active[left_position];
                let right = active[right_position];
                let similarity = cosine(centers.key(left), centers.key(right))?;
                if similarity >= config.merge_similarity_threshold {
                    pairs.push((similarity, left, right));
                }
            }
        }
        pairs.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        for (_, left, right) in pairs {
            if blocked.contains(&left)
                || blocked.contains(&right)
                || !centers.active[left]
                || !centers.active[right]
            {
                continue;
            }
            let left_slot = live_slot(centers, left)?;
            let right_slot = live_slot(centers, right)?;
            let pair_conflicts = conflicting_records(records, support, left_slot, right_slot)?;
            if !pair_conflicts.is_empty() {
                conflicts.extend(pair_conflicts);
                continue;
            }
            match support.merge_support(left_slot, right_slot) {
                Ok(()) => {}
                Err(CoreError::BudgetExceeded(_)) => continue,
                Err(error) => return Err(error),
            }
            merge_pair(centers, left, right)?;
            let merged_support = support.support_for(left_slot)?.to_vec();
            centers.support[left] = merged_support;
            deactivate_and_invalidate(centers, support, right)?;
            blocked.insert(left);
            blocked.insert(right);
            merged += 1;
        }
    }
    Ok(MergeReport {
        merged,
        conflicts: conflicts.into_iter().collect(),
    })
}

fn conflicting_records(
    records: &RecordTable,
    support: &Support,
    left: CenterSlot,
    right: CenterSlot,
) -> Result<Vec<(RecordId, RecordId)>, CoreError> {
    let mut conflicts = Vec::new();
    for left_id in support.support_for(left)? {
        let left_record = records
            .get(*left_id)
            .ok_or(CoreError::UnknownRecord(*left_id))?;
        if left_record.state != RecordState::Valid {
            continue;
        }
        for right_id in support.support_for(right)? {
            let right_record = records
                .get(*right_id)
                .ok_or(CoreError::UnknownRecord(*right_id))?;
            if right_record.state == RecordState::Valid
                && left_record.value_text != right_record.value_text
            {
                let pair = if left_id <= right_id {
                    (*left_id, *right_id)
                } else {
                    (*right_id, *left_id)
                };
                conflicts.push(pair);
            }
        }
    }
    Ok(conflicts)
}

fn merge_pair(centers: &mut MemoryCenters, left: usize, right: usize) -> Result<(), CoreError> {
    let h_left = centers.intensity[left];
    let h_right = centers.intensity[right];
    let total = h_left + h_right + 1e-8;
    validate_finite(&[h_left, h_right, total], "merge intensities")?;
    let key = weighted_row(
        &centers.keys,
        centers.config.d_key,
        left,
        right,
        h_left,
        h_right,
        total,
    );
    let ltm_key = weighted_row(
        &centers.ltm_keys,
        LTM_KEY_DIM,
        left,
        right,
        h_left,
        h_right,
        total,
    );
    let value = weighted_row(
        &centers.values,
        VALUE_DIM,
        left,
        right,
        h_left,
        h_right,
        total,
    );
    let affect = weighted_row(
        &centers.affect,
        AFFECT_DIM,
        left,
        right,
        h_left,
        h_right,
        total,
    );
    let normalized_key = normalize(&key)?;
    let normalized_ltm_key = normalize(&ltm_key)?;
    let key_start = left * centers.config.d_key;
    centers.keys[key_start..key_start + centers.config.d_key].copy_from_slice(&normalized_key);
    let ltm_start = left * LTM_KEY_DIM;
    centers.ltm_keys[ltm_start..ltm_start + LTM_KEY_DIM].copy_from_slice(&normalized_ltm_key);
    let value_start = left * VALUE_DIM;
    centers.values[value_start..value_start + VALUE_DIM].copy_from_slice(&value);
    let affect_start = left * AFFECT_DIM;
    centers.affect[affect_start..affect_start + AFFECT_DIM].copy_from_slice(&affect);
    centers.intensity[left] = total;
    let context_right = row_array::<CONTEXT_DIM>(&centers.context, right, "merge context")?;
    let terrain_right = row_array::<TERRAIN_DIM>(&centers.terrain, right, "merge terrain")?;
    let context_start = left * CONTEXT_DIM;
    centers.context[context_start..context_start + CONTEXT_DIM].copy_from_slice(&context_right);
    let terrain_start = left * TERRAIN_DIM;
    centers.terrain[terrain_start..terrain_start + TERRAIN_DIM].copy_from_slice(&terrain_right);
    if centers.record[left].is_none() {
        centers.record[left] = centers.record[right];
    }
    Ok(())
}

fn weighted_row(
    values: &[f32],
    width: usize,
    left: usize,
    right: usize,
    h_left: f32,
    h_right: f32,
    total: f32,
) -> Vec<f32> {
    let left_start = left * width;
    let right_start = right * width;
    values[left_start..left_start + width]
        .iter()
        .zip(&values[right_start..right_start + width])
        .map(|(left_value, right_value)| (h_left * *left_value + h_right * *right_value) / total)
        .collect()
}

fn deactivate_and_invalidate(
    centers: &mut MemoryCenters,
    support: &mut Support,
    index: usize,
) -> Result<(), CoreError> {
    let slot = live_slot(centers, index)?;
    centers.deactivate_slot(slot)?;
    centers.incarnation[index] = centers.incarnation[index]
        .checked_add(1)
        .ok_or_else(|| CoreError::InvalidState("center incarnation exhausted".to_owned()))?;
    let invalidated = centers.slot(index).ok_or_else(|| {
        CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
    })?;
    support.set_support(invalidated, &[])
}

fn live_slot(centers: &MemoryCenters, index: usize) -> Result<CenterSlot, CoreError> {
    let slot = centers.slot(index).ok_or_else(|| {
        CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
    })?;
    centers.resolve(slot)?;
    Ok(slot)
}

fn row_array<const N: usize>(
    values: &[f32],
    index: usize,
    what: &'static str,
) -> Result<[f32; N], CoreError> {
    let start = index
        .checked_mul(N)
        .ok_or_else(|| CoreError::InvalidState("center row overflow".to_owned()))?;
    let row = values
        .get(start..start + N)
        .ok_or(CoreError::DimensionMismatch {
            what,
            expected: start + N,
            actual: values.len(),
        })?;
    row.try_into().map_err(|_| CoreError::DimensionMismatch {
        what,
        expected: N,
        actual: row.len(),
    })
}

fn array_from_vec<const N: usize>(
    values: &[f32],
    what: &'static str,
) -> Result<[f32; N], CoreError> {
    values.try_into().map_err(|_| CoreError::DimensionMismatch {
        what,
        expected: N,
        actual: values.len(),
    })
}
