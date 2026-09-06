//! Fixed-capacity center allocation and incarnation-safe slot lifecycle.

use super::MemoryCenters;
use super::write::WriteInput;
use crate::numeric::{DeterministicRng, l2_norm, normalize, validate_dimension, validate_finite};
use crate::types::{
    AFFECT_DIM, CONTEXT_DIM, CenterSlot, CoreError, LTM_KEY_DIM, LayerConfig, TERRAIN_DIM,
    VALUE_DIM,
};

impl MemoryCenters {
    /// Creates an empty center bank with deterministic random unit keys.
    ///
    /// Inactive keys match the reference constructor's Gaussian initialization;
    /// all other fields retain [`MemoryCenters::with_layout`] defaults.
    pub fn new(config: LayerConfig, seed: u64) -> Result<Self, CoreError> {
        Self::validate_allocation_config(&config)?;
        let mut centers = Self::with_layout(config);
        let mut rng = DeterministicRng::new(seed);
        for index in 0..centers.config.n_centers {
            let row: Vec<f32> = (0..centers.config.d_key).map(|_| rng.gaussian()).collect();
            let unit = normalize(&row)?;
            centers.key_row_mut(index).copy_from_slice(&unit);
        }
        Ok(centers)
    }

    /// Returns the lowest inactive slot index, matching reference first-free allocation.
    #[must_use]
    pub fn first_free_slot(&self) -> Option<usize> {
        self.active.iter().position(|active| !*active)
    }

    /// Activates an inactive slot from a fully validated write input.
    ///
    /// Every activation increments the incarnation and resets all slot-local
    /// counters, metadata, and support before installing the new record.
    pub fn activate_slot(
        &mut self,
        index: usize,
        input: &WriteInput<'_>,
    ) -> Result<CenterSlot, CoreError> {
        self.validate_storage_layout()?;
        self.validate_activation_input(input)?;
        let active = self.active.get(index).ok_or_else(|| {
            CoreError::InvalidState("center activation index out of range".to_owned())
        })?;
        if *active {
            return Err(CoreError::InvalidState(
                "cannot activate an active center slot".to_owned(),
            ));
        }
        let next_incarnation = self.incarnation[index]
            .checked_add(1)
            .ok_or_else(|| CoreError::InvalidState("center incarnation exhausted".to_owned()))?;

        self.key_row_mut(index).copy_from_slice(input.key);
        self.ltm_key_row_mut(index).copy_from_slice(&input.ltm_key);
        self.value_row_mut(index).copy_from_slice(&input.value);
        self.affect_row_mut(index).copy_from_slice(&input.affect.0);
        self.context_row_mut(index).copy_from_slice(&input.context);
        self.terrain_row_mut(index).copy_from_slice(&input.terrain);
        self.intensity[index] = input.intensity;
        self.usage[index] = 0;
        self.age[index] = input.age;
        self.record[index] = Some(input.record);
        self.support[index].clear();
        self.support[index].push(input.record);
        self.incarnation[index] = next_incarnation;
        self.active[index] = true;

        Ok(CenterSlot {
            layer: self.config.layer,
            index: u32::try_from(index).map_err(|_| {
                CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
            })?,
            incarnation: next_incarnation,
        })
    }

    /// Deactivates and scrubs a slot resolved through its current incarnation.
    ///
    /// Affect is zeroed rather than reset to neutral, matching reference
    /// `scrub_slot`; the next activation installs a fresh affect vector.
    pub fn deactivate_slot(&mut self, slot: CenterSlot) -> Result<(), CoreError> {
        self.validate_storage_layout()?;
        let index = self.resolve(slot)?;
        self.key_row_mut(index).fill(0.0);
        self.ltm_key_row_mut(index).fill(0.0);
        self.value_row_mut(index).fill(0.0);
        self.affect_row_mut(index).fill(0.0);
        self.context_row_mut(index).fill(0.0);
        self.terrain_row_mut(index).fill(0.0);
        self.intensity[index] = 0.0;
        self.usage[index] = 0;
        self.age[index] = 0;
        self.active[index] = false;
        self.record[index] = None;
        self.support[index].clear();
        Ok(())
    }

    /// Resolves a live incarnation-bearing slot handle to its array index.
    pub fn resolve(&self, slot: CenterSlot) -> Result<usize, CoreError> {
        let index = usize::try_from(slot.index).map_err(|_| CoreError::StaleHandle)?;
        match (self.active.get(index), self.incarnation.get(index)) {
            (Some(true), Some(incarnation)) if *incarnation == slot.incarnation => Ok(index),
            _ => Err(CoreError::StaleHandle),
        }
    }

    fn validate_allocation_config(config: &LayerConfig) -> Result<(), CoreError> {
        if config.d_key == 0 {
            return Err(CoreError::InvalidState(
                "center key dimension must be positive".to_owned(),
            ));
        }
        if config.n_centers > u32::MAX as usize {
            return Err(CoreError::InvalidState(
                "center capacity does not fit CenterSlot".to_owned(),
            ));
        }
        validate_finite(
            &[
                config.sigma_read,
                config.sigma_write,
                config.leak,
                config.leak_emotion,
                config.leak_value,
                config.alpha_value,
                config.alpha_emotion,
                config.alpha_key,
                config.new_center_threshold,
                config.terrain_eta,
                config.terrain_lambda,
                config.terrain_alpha_h,
                config.terrain_alpha_e,
            ],
            "center configuration",
        )?;
        if config.sigma_read <= 0.0 || config.sigma_write <= 0.0 {
            return Err(CoreError::InvalidState(
                "center sigma values must be positive".to_owned(),
            ));
        }
        Ok(())
    }

    fn validate_activation_input(&self, input: &WriteInput<'_>) -> Result<(), CoreError> {
        validate_dimension(input.key, self.config.d_key, "center write key")?;
        validate_finite(input.key, "center write key")?;
        let key_norm = l2_norm(input.key)?;
        if (key_norm - 1.0).abs() > 1e-5 {
            return Err(CoreError::InvalidState(
                "center write key must have unit norm".to_owned(),
            ));
        }
        validate_finite(&input.ltm_key, "center write LTM key")?;
        validate_finite(&input.value, "center write value")?;
        validate_finite(&input.affect.0, "center write affect")?;
        validate_finite(&[input.intensity], "center write intensity")?;
        validate_finite(&input.context, "center write context")?;
        validate_finite(&input.terrain, "center write terrain")
    }

    pub(super) fn validate_storage_layout(&self) -> Result<(), CoreError> {
        let n = self.config.n_centers;
        Self::check_len(
            &self.keys,
            n.saturating_mul(self.config.d_key),
            "center keys",
        )?;
        Self::check_len(
            &self.ltm_keys,
            n.saturating_mul(LTM_KEY_DIM),
            "center LTM keys",
        )?;
        Self::check_len(&self.values, n.saturating_mul(VALUE_DIM), "center values")?;
        Self::check_len(&self.intensity, n, "center intensity")?;
        Self::check_len(&self.affect, n.saturating_mul(AFFECT_DIM), "center affect")?;
        Self::check_len(&self.usage, n, "center usage")?;
        Self::check_len(&self.age, n, "center age")?;
        Self::check_len(&self.active, n, "center active mask")?;
        Self::check_len(&self.incarnation, n, "center incarnations")?;
        Self::check_len(
            &self.context,
            n.saturating_mul(CONTEXT_DIM),
            "center context",
        )?;
        Self::check_len(
            &self.terrain,
            n.saturating_mul(TERRAIN_DIM),
            "center terrain",
        )?;
        Self::check_len(&self.record, n, "center records")?;
        Self::check_len(&self.support, n, "center support")
    }

    fn check_len<T>(values: &[T], expected: usize, what: &'static str) -> Result<(), CoreError> {
        if values.len() == expected {
            Ok(())
        } else {
            Err(CoreError::DimensionMismatch {
                what,
                expected,
                actual: values.len(),
            })
        }
    }

    pub(super) fn key_row_mut(&mut self, index: usize) -> &mut [f32] {
        let d_key = self.config.d_key;
        &mut self.keys[index * d_key..(index + 1) * d_key]
    }

    pub(super) fn ltm_key_row_mut(&mut self, index: usize) -> &mut [f32] {
        &mut self.ltm_keys[index * LTM_KEY_DIM..(index + 1) * LTM_KEY_DIM]
    }

    pub(super) fn value_row_mut(&mut self, index: usize) -> &mut [f32] {
        &mut self.values[index * VALUE_DIM..(index + 1) * VALUE_DIM]
    }

    pub(super) fn affect_row_mut(&mut self, index: usize) -> &mut [f32] {
        &mut self.affect[index * AFFECT_DIM..(index + 1) * AFFECT_DIM]
    }

    pub(super) fn context_row_mut(&mut self, index: usize) -> &mut [f32] {
        &mut self.context[index * CONTEXT_DIM..(index + 1) * CONTEXT_DIM]
    }

    pub(super) fn terrain_row_mut(&mut self, index: usize) -> &mut [f32] {
        &mut self.terrain[index * TERRAIN_DIM..(index + 1) * TERRAIN_DIM]
    }
}
