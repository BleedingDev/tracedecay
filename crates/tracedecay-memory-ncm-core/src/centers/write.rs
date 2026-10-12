//! Stateful reference-compatible center writes with bounded record support.

use super::MemoryCenters;
use crate::numeric::{l2_norm, normalize, validate_dimension, validate_finite};
use crate::types::{
    AffectVector, CONTEXT_DIM, CenterSlot, CoreError, LTM_KEY_DIM, Layer, NcmConfig, RecordId,
    TERRAIN_DIM, VALUE_DIM,
};

const ZERO_STRENGTH_EPSILON: f32 = 1e-6;
const LOCAL_WEIGHT_EPSILON: f32 = 1e-8;
const UNIT_NORM_TOLERANCE: f32 = 1e-5;

/// One validated record projected into a center bank.
#[derive(Clone, Debug)]
pub struct WriteInput<'a> {
    /// Unit semantic key in the bank's configured key dimension.
    pub key: &'a [f32],
    /// Canonical 64-D LTM key retained for consolidation (D08).
    pub ltm_key: [f32; LTM_KEY_DIM],
    /// Projected 128-D value.
    pub value: [f32; VALUE_DIM],
    /// Four-channel affect vector.
    pub affect: AffectVector,
    /// Write strength omega.
    pub intensity: f32,
    /// Sixteen-dimensional context fingerprint.
    pub context: [f32; CONTEXT_DIM],
    /// Three-dimensional terrain position.
    pub terrain: [f32; TERRAIN_DIM],
    /// Stable logical record identity.
    pub record: RecordId,
    /// Initial or carried age.
    pub age: u64,
}

/// Per-operation write policy derived from the frozen configuration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WriteParams {
    /// Maximum number of existing centers reinforced by one write.
    pub top_k_write: usize,
    /// Maximum raw RBF weight below which a new center is allocated.
    pub new_center_threshold: f32,
    /// Maximum recent distinct supporting records retained per center.
    pub max_center_support: usize,
}

impl WriteParams {
    /// Derives write parameters for one memory layer.
    #[must_use]
    pub fn for_layer(config: &NcmConfig, layer: Layer) -> Self {
        let layer_config = match layer {
            Layer::Stm => &config.stm,
            Layer::Ltm => &config.ltm,
        };
        Self {
            top_k_write: layer_config.top_k_write,
            new_center_threshold: layer_config.new_center_threshold,
            max_center_support: config.max_center_support,
        }
    }

    /// Alias for [`Self::for_layer`] for callers deriving parameters from config.
    #[must_use]
    pub fn from_config(config: &NcmConfig, layer: Layer) -> Self {
        Self::for_layer(config, layer)
    }
}

impl Default for WriteParams {
    fn default() -> Self {
        Self::for_layer(&NcmConfig::default(), Layer::Stm)
    }
}

/// Explicit effect of one center write.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteOutcome {
    /// Strength was below the executed reference's `1e-6` cutoff.
    Ignored,
    /// A new center was required but the fixed-capacity bank was full.
    CapacityExhausted,
    /// A fresh slot was allocated.
    Created {
        /// Incarnation-bearing handle of the new center.
        slot: CenterSlot,
    },
    /// Existing centers were reinforced.
    Reinforced {
        /// Winning center that received record metadata.
        slot: CenterSlot,
        /// Every center whose numerical state and support were updated.
        updated: Vec<CenterSlot>,
        /// Updated centers whose oldest support record was dropped.
        support_truncated: Vec<CenterSlot>,
    },
}

impl MemoryCenters {
    /// Writes by semantic proximity, using `sigma_read` for candidates (D02).
    pub fn write(
        &mut self,
        input: WriteInput<'_>,
        params: &WriteParams,
    ) -> Result<WriteOutcome, CoreError> {
        self.write_impl(input, params, None)
    }

    /// Reinforces a center carrying `record_id` with weight one, if present.
    ///
    /// If no active center carries the requested record, this follows the
    /// normal proximity/allocation path. Durable request deduplication remains
    /// a runtime concern and is deliberately not inferred from record identity.
    pub fn write_to_record(
        &mut self,
        record_id: RecordId,
        input: WriteInput<'_>,
        params: &WriteParams,
    ) -> Result<WriteOutcome, CoreError> {
        self.write_impl(input, params, Some(record_id))
    }

    fn write_impl(
        &mut self,
        input: WriteInput<'_>,
        params: &WriteParams,
        target_record: Option<RecordId>,
    ) -> Result<WriteOutcome, CoreError> {
        self.validate_write_input(&input, params)?;
        if input.intensity < ZERO_STRENGTH_EPSILON {
            return Ok(WriteOutcome::Ignored);
        }

        let forced_index = target_record.and_then(|record_id| {
            self.active.iter().zip(&self.record).enumerate().find_map(
                |(index, (active, record))| {
                    (*active && *record == Some(record_id)).then_some(index)
                },
            )
        });

        let selected: Vec<(usize, f32)> = if let Some(index) = forced_index {
            vec![(index, 1.0)]
        } else {
            let selection = self.compute_rbf_weights(input.key, params.top_k_write, false, None)?;
            selection
                .centers
                .iter()
                .zip(selection.weights)
                .map(|(center, weight)| (center.index, weight))
                .collect()
        };

        let max_weight = selected.first().map_or(0.0, |(_, weight)| *weight);
        if forced_index.is_none()
            && (selected.is_empty() || max_weight < params.new_center_threshold)
        {
            let Some(index) = self.first_free_slot() else {
                return Ok(WriteOutcome::CapacityExhausted);
            };
            let mut staged = self.clone();
            let slot = staged.activate_slot(index, &input)?;
            *self = staged;
            return Ok(WriteOutcome::Created { slot });
        }

        let total: f32 = selected.iter().map(|(_, weight)| *weight).sum();
        validate_finite(&[total], "center write local weight sum")?;
        let denominator = total + LOCAL_WEIGHT_EPSILON;
        validate_finite(&[denominator], "center write local denominator")?;
        let winner = selected.first().map(|(index, _)| *index).ok_or_else(|| {
            CoreError::InvalidState("center reinforcement has no candidates".to_owned())
        })?;

        let mut staged = self.clone();
        let alpha_value = staged.config.alpha_value;
        let alpha_emotion = staged.config.alpha_emotion;
        let alpha_key = staged.config.alpha_key;
        let mut updated = Vec::with_capacity(selected.len());
        let mut support_truncated = Vec::new();
        for (index, raw_weight) in selected {
            let weight = raw_weight / denominator * input.intensity;
            validate_finite(&[weight], "center write normalized weight")?;

            let new_intensity = staged.intensity[index] + weight;
            validate_finite(&[new_intensity], "center write intensity update")?;
            staged.intensity[index] = new_intensity;

            for (stored, incoming) in staged.value_row_mut(index).iter_mut().zip(input.value) {
                let next = *stored + alpha_value * weight * (incoming - *stored);
                validate_finite(&[next], "center write value update")?;
                *stored = next;
            }
            for (stored, incoming) in staged.affect_row_mut(index).iter_mut().zip(input.affect.0) {
                let next = *stored + alpha_emotion * weight * (incoming - *stored);
                validate_finite(&[next], "center write affect update")?;
                *stored = next;
            }
            if alpha_key > 0.0 {
                let drifted: Vec<f32> = staged
                    .key(index)
                    .iter()
                    .zip(input.key)
                    .map(|(stored, incoming)| *stored + alpha_key * weight * (*incoming - *stored))
                    .collect();
                let unit = normalize(&drifted)?;
                staged.key_row_mut(index).copy_from_slice(&unit);
            }

            let slot = staged.slot(index).ok_or_else(|| {
                CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
            })?;
            if Self::push_bounded_support(
                &mut staged.support[index],
                input.record,
                params.max_center_support,
            ) {
                support_truncated.push(slot);
            }
            updated.push(slot);
        }

        staged
            .context_row_mut(winner)
            .copy_from_slice(&input.context);
        staged
            .terrain_row_mut(winner)
            .copy_from_slice(&input.terrain);
        staged
            .ltm_key_row_mut(winner)
            .copy_from_slice(&input.ltm_key);
        staged.record[winner] = Some(input.record);
        staged.age[winner] = staged.age[winner]
            .checked_add(input.age)
            .ok_or_else(|| CoreError::InvalidState("center age overflow".to_owned()))?;
        let slot = staged.slot(winner).ok_or_else(|| {
            CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
        })?;
        *self = staged;
        Ok(WriteOutcome::Reinforced {
            slot,
            updated,
            support_truncated,
        })
    }

    fn validate_write_input(
        &self,
        input: &WriteInput<'_>,
        params: &WriteParams,
    ) -> Result<(), CoreError> {
        self.validate_storage_layout()?;
        validate_dimension(input.key, self.config.d_key, "center write key")?;
        validate_finite(input.key, "center write key")?;
        validate_finite(&input.ltm_key, "center write LTM key")?;
        validate_finite(&input.value, "center write value")?;
        validate_finite(&input.affect.0, "center write affect")?;
        validate_finite(&[input.intensity], "center write intensity")?;
        validate_finite(&input.context, "center write context")?;
        validate_finite(&input.terrain, "center write terrain")?;
        validate_finite(
            &[
                params.new_center_threshold,
                self.config.alpha_value,
                self.config.alpha_emotion,
                self.config.alpha_key,
                self.config.sigma_read,
            ],
            "center write parameters",
        )?;
        if params.top_k_write == 0 {
            return Err(CoreError::InvalidState(
                "write top-k must be positive".to_owned(),
            ));
        }
        if params.max_center_support == 0 {
            return Err(CoreError::BudgetExceeded("center support"));
        }
        if self.config.sigma_read <= 0.0 {
            return Err(CoreError::InvalidState(
                "write candidate sigma_read must be positive".to_owned(),
            ));
        }
        let norm = l2_norm(input.key)?;
        if (norm - 1.0).abs() > UNIT_NORM_TOLERANCE {
            return Err(CoreError::InvalidState(
                "center write key must have unit norm".to_owned(),
            ));
        }
        Ok(())
    }

    fn push_bounded_support(support: &mut Vec<RecordId>, record: RecordId, maximum: usize) -> bool {
        if let Some(position) = support.iter().position(|existing| *existing == record) {
            support.remove(position);
        }
        let truncated = support.len() >= maximum;
        if truncated {
            support.remove(0);
        }
        support.push(record);
        truncated
    }
}
