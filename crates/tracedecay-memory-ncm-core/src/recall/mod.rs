//! Read-only, record-aware recall with explicit provenance and admission policy.

use crate::centers::MemoryCenters;
use crate::centers::read::{CompoundWeights, ReadResult};
use crate::numeric::{minkowski, validate_finite};
use crate::records::{Record, RecordState, RecordTable, Support};
use crate::types::{CenterSlot, CoreError, RecordId, SourceId};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;

const MAX_RECALL_TOP_K: usize = 16;

/// Layer membership of a deduplicated record candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecallLayer {
    /// Supported only by short-term memory.
    Stm,
    /// Supported only by long-term memory.
    Ltm,
    /// The same stable record is supported by both layers (D04).
    Both,
}

/// Inspectable distance values for the center that supplied peak activation.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct DistanceComponents {
    /// Squared cosine distance, `2 - 2*cosine`.
    pub cosine_d2: f32,
    /// Hybrid Minkowski distance when the selected read path used that rerank.
    pub minkowski: Option<f32>,
}

/// Confidence label that deliberately makes the lack of calibration explicit.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum RecallConfidence {
    /// Bounded activation-to-layer-intensity ratio in `[0, 1]`.
    Uncalibrated(f32),
}

/// One hydrated, source-backed recall candidate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecallCandidate {
    /// Stable source record identity.
    pub record_id: RecordId,
    /// Opaque admitted source identity.
    pub source: SourceId,
    /// Original key text retained by the record table.
    pub key_text: String,
    /// Original value text retained by the record table.
    pub value_text: String,
    /// Layer membership after deduplication by record identity.
    pub layer: RecallLayer,
    /// Peak raw intensity-weighted RBF match across supporting centers/layers.
    pub activation: f32,
    /// Distance components of the peak-activation center.
    pub distance_components: DistanceComponents,
    /// Exact incarnation-bearing center handles supporting this candidate.
    pub support_slots: Vec<CenterSlot>,
    /// Exact current lifecycle metadata, including supersession lineage.
    pub validity: RecordState,
    /// Explicitly uncalibrated bounded confidence.
    pub confidence: RecallConfidence,
}

/// Predeclared relevance and result-bound policy.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecallPolicy {
    /// Minimum raw intensity-weighted activation required for admission.
    pub min_activation: f32,
    /// Minimum top-versus-runner-up activation gap considered unambiguous.
    /// Candidates remain visible when this margin is not met.
    pub min_margin: f32,
    /// Maximum number of hydrated record candidates.
    pub max_candidates: usize,
}

impl Default for RecallPolicy {
    fn default() -> Self {
        Self {
            min_activation: 0.05,
            min_margin: 0.0,
            max_candidates: MAX_RECALL_TOP_K,
        }
    }
}

/// Result of a successful, available core recall.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RecallOutput {
    /// No source-backed candidate met the predeclared activation threshold.
    Empty,
    /// One or more relevant records were found and bounded before hydration.
    Candidates {
        /// Hydrated candidates in deterministic rank order.
        candidates: Vec<RecallCandidate>,
        /// Whether candidate count or text budget removed a ranked suffix.
        truncated: bool,
        /// Whether the top activation exceeds the runner-up by `min_margin`.
        margin_satisfied: bool,
    },
}

#[derive(Clone, Debug)]
struct CandidateAccumulator {
    record_id: RecordId,
    stm_activation: Option<f32>,
    ltm_activation: Option<f32>,
    peak_activation: f32,
    distance_components: DistanceComponents,
    support_slots: Vec<CenterSlot>,
}

impl CandidateAccumulator {
    fn new(record_id: RecordId, activation: f32, distance: DistanceComponents) -> Self {
        Self {
            record_id,
            stm_activation: None,
            ltm_activation: None,
            peak_activation: activation,
            distance_components: distance,
            support_slots: Vec::new(),
        }
    }

    fn layer(&self) -> RecallLayer {
        match (self.stm_activation.is_some(), self.ltm_activation.is_some()) {
            (true, true) => RecallLayer::Both,
            (true, false) => RecallLayer::Stm,
            (false, true) => RecallLayer::Ltm,
            (false, false) => RecallLayer::Stm,
        }
    }

    fn confidence(&self, stm_mass: f32, ltm_mass: f32) -> f32 {
        let stm_ratio = self
            .stm_activation
            .map_or(0.0, |activation| bounded_ratio(activation, stm_mass));
        let ltm_ratio = self
            .ltm_activation
            .map_or(0.0, |activation| bounded_ratio(activation, ltm_mass));
        stm_ratio.max(ltm_ratio)
    }
}

/// Recalls source-backed records from immutable STM and LTM views.
///
/// Candidates are combined only by [`RecordId`] (D04). Activation is the raw
/// center RBF multiplied by center intensity, not the normalized top-k softmax;
/// this prevents an unrelated singleton from receiving confidence `1.0` merely
/// because it was the only selected center. Confidence divides that activation
/// by total active intensity mass in the contributing layer and is explicitly
/// uncalibrated. Text recall passes `terrain_part = 0.0` (D03).
#[allow(clippy::too_many_arguments)]
pub fn recall(
    query_key_stm: &[f32],
    query_key_ltm: &[f32],
    query_ctx: &[f32],
    stm: &MemoryCenters,
    ltm: &MemoryCenters,
    terrain_part: f32,
    records: &RecordTable,
    support: &Support,
    policy: &RecallPolicy,
    top_k: usize,
) -> Result<RecallOutput, CoreError> {
    validate_policy(policy, top_k, terrain_part)?;
    if top_k == 0 {
        return Ok(RecallOutput::Empty);
    }

    let weights = CompoundWeights {
        terrain: terrain_part,
        ..CompoundWeights::default()
    };
    let stm_read = stm.read_compound(query_key_stm, Some(query_ctx), None, weights, top_k)?;
    let ltm_read = ltm.read_compound(query_key_ltm, Some(query_ctx), None, weights, top_k)?;
    let stm_mass = total_intensity_mass(stm)?;
    let ltm_mass = total_intensity_mass(ltm)?;

    let mut accumulated = BTreeMap::new();
    collect_layer(
        &mut accumulated,
        &stm_read,
        query_key_stm,
        stm,
        records,
        support,
        true,
    )?;
    collect_layer(
        &mut accumulated,
        &ltm_read,
        query_key_ltm,
        ltm,
        records,
        support,
        false,
    )?;

    let mut ranked: Vec<CandidateAccumulator> = accumulated
        .into_values()
        .filter(|candidate| candidate.peak_activation >= policy.min_activation)
        .collect();
    ranked.sort_by(|left, right| {
        right
            .peak_activation
            .total_cmp(&left.peak_activation)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });
    if ranked.is_empty() {
        return Ok(RecallOutput::Empty);
    }

    let margin = if ranked.len() > 1 {
        ranked[0].peak_activation - ranked[1].peak_activation
    } else {
        ranked[0].peak_activation
    };
    let margin_satisfied = margin >= policy.min_margin;
    let count_limit = policy.max_candidates.min(ranked.len());
    let mut truncated = count_limit < ranked.len();
    let mut selected = Vec::with_capacity(count_limit);
    let mut text_bytes = 0_usize;
    for candidate in ranked.into_iter().take(count_limit) {
        let record = records
            .get(candidate.record_id)
            .ok_or(CoreError::UnknownRecord(candidate.record_id))?;
        let candidate_text_bytes = record
            .key_text
            .len()
            .checked_add(record.value_text.len())
            .ok_or(CoreError::BudgetExceeded("recall text"))?;
        let next_bytes = text_bytes
            .checked_add(candidate_text_bytes)
            .ok_or(CoreError::BudgetExceeded("recall text"))?;
        if next_bytes > records.max_recall_bytes() {
            truncated = true;
            break;
        }
        text_bytes = next_bytes;
        selected.push(hydrate_candidate(candidate, record, stm_mass, ltm_mass));
    }

    Ok(RecallOutput::Candidates {
        candidates: selected,
        truncated,
        margin_satisfied,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_layer(
    accumulated: &mut BTreeMap<RecordId, CandidateAccumulator>,
    read: &ReadResult,
    query: &[f32],
    centers: &MemoryCenters,
    records: &RecordTable,
    support: &Support,
    is_stm: bool,
) -> Result<(), CoreError> {
    for trace in &read.selection.centers {
        let tracked = support.support_for(trace.slot)?;
        let activation = trace.raw_rbf_weight * trace.intensity;
        validate_finite(&[activation], "recall activation")?;
        let cosine_d2 = 2.0 - 2.0 * trace.cosine;
        let minkowski_distance = if read.selection.hybrid_applied {
            Some(minkowski(query, centers.key(trace.index), 0.5)?)
        } else {
            None
        };
        let distance = DistanceComponents {
            cosine_d2,
            minkowski: minkowski_distance,
        };
        for record_id in tracked {
            if !trace.support.contains(record_id) {
                continue;
            }
            let record = records
                .get(*record_id)
                .ok_or(CoreError::UnknownRecord(*record_id))?;
            if matches!(record.state, RecordState::Deleted { .. }) {
                continue;
            }
            let entry = accumulated
                .entry(*record_id)
                .or_insert_with(|| CandidateAccumulator::new(*record_id, activation, distance));
            let layer_activation = if is_stm {
                &mut entry.stm_activation
            } else {
                &mut entry.ltm_activation
            };
            if layer_activation.is_none_or(|current| activation > current) {
                *layer_activation = Some(activation);
            }
            if activation > entry.peak_activation
                || (activation.total_cmp(&entry.peak_activation) == Ordering::Equal
                    && trace.slot.index
                        < entry
                            .support_slots
                            .first()
                            .map_or(u32::MAX, |slot| slot.index))
            {
                entry.peak_activation = activation;
                entry.distance_components = distance;
            }
            if !entry.support_slots.contains(&trace.slot) {
                entry.support_slots.push(trace.slot);
                entry
                    .support_slots
                    .sort_by_key(|slot| (slot.index, slot.incarnation));
            }
        }
    }
    Ok(())
}

fn hydrate_candidate(
    candidate: CandidateAccumulator,
    record: &Record,
    stm_mass: f32,
    ltm_mass: f32,
) -> RecallCandidate {
    let confidence = candidate.confidence(stm_mass, ltm_mass);
    RecallCandidate {
        record_id: candidate.record_id,
        source: record.source.clone(),
        key_text: record.key_text.clone(),
        value_text: record.value_text.clone(),
        layer: candidate.layer(),
        activation: candidate.peak_activation,
        distance_components: candidate.distance_components,
        support_slots: candidate.support_slots,
        validity: record.state.clone(),
        confidence: RecallConfidence::Uncalibrated(confidence),
    }
}

fn total_intensity_mass(centers: &MemoryCenters) -> Result<f32, CoreError> {
    let mass: f32 = centers
        .active
        .iter()
        .zip(&centers.intensity)
        .filter_map(|(active, intensity)| active.then_some(*intensity))
        .sum();
    validate_finite(&[mass], "recall layer intensity mass")?;
    if mass < 0.0 {
        return Err(CoreError::InvalidState(
            "recall layer intensity mass must be non-negative".to_owned(),
        ));
    }
    Ok(mass)
}

fn bounded_ratio(activation: f32, mass: f32) -> f32 {
    if mass > 0.0 {
        (activation / mass).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn validate_policy(
    policy: &RecallPolicy,
    top_k: usize,
    terrain_part: f32,
) -> Result<(), CoreError> {
    validate_finite(
        &[policy.min_activation, policy.min_margin, terrain_part],
        "recall policy",
    )?;
    if !(0.0..=1.0).contains(&policy.min_activation) || policy.min_margin < 0.0 {
        return Err(CoreError::InvalidState(
            "recall activation and margin thresholds must be bounded".to_owned(),
        ));
    }
    if !(0.0..=1.0).contains(&terrain_part) {
        return Err(CoreError::InvalidState(
            "recall terrain part must be in [0, 1]".to_owned(),
        ));
    }
    if top_k > MAX_RECALL_TOP_K || policy.max_candidates > MAX_RECALL_TOP_K {
        return Err(CoreError::BudgetExceeded("recall candidates"));
    }
    Ok(())
}
