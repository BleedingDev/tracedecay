//! Immutable associative reads over a [`MemoryCenters`] bank.
//!
//! The public API accepts one query at a time (`B = T = 1` in the Biomem
//! reference). The hybrid Minkowski denominator is therefore the maximum over
//! that query's candidate set; this is exactly the reference batch-global
//! denominator for a single-query batch.

use super::MemoryCenters;
use crate::numeric::{
    cosine, log_space_softmax, minkowski, rbf_weight, top_k_largest, top_k_smallest,
    validate_dimension, validate_finite,
};
use crate::types::{
    CenterSlot, CoreError, RecordId, AFFECT_DIM, CONTEXT_DIM, TERRAIN_DIM, VALUE_DIM,
};

/// Hybrid-selection settings and operation budget for a plain center read.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReadParams {
    /// Maximum number of selected centers.
    pub top_k: usize,
    /// Whether to rerank a bounded cosine candidate set with Minkowski distance.
    pub use_hybrid_metric: bool,
    /// Minkowski exponent used by the hybrid rerank.
    pub minkowski_p: f32,
    /// Cosine-distance contribution to the hybrid score.
    pub weight_cosine: f32,
    /// Normalized Minkowski-distance contribution to the hybrid score.
    pub weight_minkowski: f32,
    /// Candidate budget and boundary for activating the hybrid branch.
    pub hybrid_candidates: usize,
}

impl Default for ReadParams {
    fn default() -> Self {
        Self {
            top_k: 32,
            use_hybrid_metric: true,
            minkowski_p: 0.5,
            weight_cosine: 0.7,
            weight_minkowski: 0.3,
            hybrid_candidates: 64,
        }
    }
}

/// Semantic, context, and terrain contributions to a compound read.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompoundWeights {
    /// Semantic-key cosine contribution.
    pub semantic: f32,
    /// Context-key cosine contribution.
    pub context: f32,
    /// Terrain-position cosine contribution.
    pub terrain: f32,
}

impl Default for CompoundWeights {
    fn default() -> Self {
        Self {
            semantic: 0.6,
            context: 0.25,
            terrain: 0.15,
        }
    }
}

/// Inspectable contribution from one selected center.
#[derive(Clone, Debug, PartialEq)]
pub struct CenterReadTrace {
    /// Stable array index of the selected center.
    pub index: usize,
    /// Incarnation-bearing handle for stale-reference detection.
    pub slot: CenterSlot,
    /// Cosine RBF value before intensity normalization.
    pub raw_rbf_weight: f32,
    /// Log-space softmax weight after mixing in center intensity.
    pub normalized_weight: f32,
    /// Semantic/key cosine component.
    pub cosine: f32,
    /// Context cosine component, or zero when context is absent.
    pub context_cosine: f32,
    /// Terrain cosine component, or zero when terrain is absent.
    pub terrain_cosine: f32,
    /// Weighted compound score before conversion to RBF distance.
    pub combined_score: f32,
    /// Center intensity mixed into the normalized weight.
    pub intensity: f32,
    /// Winning metadata record attached to the center.
    pub record: Option<RecordId>,
    /// Record handles that support the center.
    pub support: Vec<RecordId>,
}

/// Selected centers and the weights returned by an RBF operation.
#[derive(Clone, Debug, PartialEq)]
pub struct RbfSelection {
    /// Operation weights in trace order: normalized when requested, raw otherwise.
    pub weights: Vec<f32>,
    /// Per-center score and provenance traces.
    pub centers: Vec<CenterReadTrace>,
    /// Whether the `n_active > hybrid_candidates` rerank branch ran.
    pub hybrid_applied: bool,
}

/// Value, affect, and selected-center trace returned by a read.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadResult {
    /// Intensity-normalized value readout.
    pub r_v: [f32; VALUE_DIM],
    /// Intensity-normalized affect readout.
    pub r_e: [f32; AFFECT_DIM],
    /// Selected centers and their contributions.
    pub selection: RbfSelection,
}

#[derive(Clone, Copy)]
struct ScoreParts {
    cosine: f32,
    context_cosine: f32,
    terrain_cosine: f32,
    combined_score: f32,
}

impl MemoryCenters {
    /// Selects centers with the reference cosine RBF and production hybrid settings.
    ///
    /// `normalize = false` returns raw RBF values in [`RbfSelection::weights`],
    /// while every center trace still includes the corresponding intensity-normalized
    /// weight so callers can inspect both quantities without calling either one a
    /// confidence score.
    pub fn compute_rbf_weights(
        &self,
        query: &[f32],
        top_k: usize,
        normalize: bool,
        sigma: Option<f32>,
    ) -> Result<RbfSelection, CoreError> {
        let params = ReadParams {
            top_k,
            ..ReadParams::default()
        };
        self.compute_rbf_weights_with_params(query, params, normalize, sigma)
    }

    /// Reads a weighted value and affect vector without mutating center state (D05).
    pub fn read(&self, query: &[f32], params: ReadParams) -> Result<ReadResult, CoreError> {
        let selection = self.compute_rbf_weights_with_params(query, params, true, None)?;
        self.finish_read(selection)
    }

    /// Reads with the separate semantic/context/terrain compound-key scoring path.
    ///
    /// Missing context or terrain is represented by an all-zero part, matching the
    /// executed reference. In particular, text recall passes no terrain query (D03).
    pub fn read_compound(
        &self,
        query: &[f32],
        context: Option<&[f32]>,
        terrain: Option<&[f32]>,
        weights: CompoundWeights,
        top_k: usize,
    ) -> Result<ReadResult, CoreError> {
        self.validate_read_layout()?;
        validate_dimension(query, self.config.d_key, "center read query")?;
        validate_finite(query, "center read query")?;
        if let Some(context_query) = context {
            validate_dimension(context_query, CONTEXT_DIM, "center read context")?;
            validate_finite(context_query, "center read context")?;
        }
        if let Some(terrain_query) = terrain {
            validate_dimension(terrain_query, TERRAIN_DIM, "center read terrain")?;
            validate_finite(terrain_query, "center read terrain")?;
        }
        validate_finite(
            &[weights.semantic, weights.context, weights.terrain],
            "compound weights",
        )?;

        let active_indices = self.active_indices();
        if active_indices.is_empty() || top_k == 0 {
            return self.finish_read(Self::empty_selection(false));
        }

        let mut parts = Vec::with_capacity(active_indices.len());
        let mut raw_weights = Vec::with_capacity(active_indices.len());
        for &index in &active_indices {
            let semantic = cosine(query, self.key(index))?;
            let context_cosine = match context {
                Some(context_query) => cosine(context_query, self.context_row(index))?,
                None => 0.0,
            };
            let terrain_cosine = match terrain {
                Some(terrain_query) => cosine(terrain_query, self.terrain_row(index))?,
                None => 0.0,
            };
            let combined_score = weights.semantic * semantic
                + weights.context * context_cosine
                + weights.terrain * terrain_cosine;
            validate_finite(&[combined_score], "compound score")?;
            let distance_squared = 2.0 - 2.0 * combined_score;
            let raw_weight = rbf_weight(distance_squared, self.config.sigma_read)?;
            parts.push(ScoreParts {
                cosine: semantic,
                context_cosine,
                terrain_cosine,
                combined_score,
            });
            raw_weights.push(raw_weight);
        }

        let selected = top_k_largest(&raw_weights, top_k.min(active_indices.len()))?;
        let selected_locals: Vec<usize> = selected.iter().map(|(_, local)| *local).collect();
        let selection = self.build_selection(
            &active_indices,
            &raw_weights,
            &parts,
            &selected_locals,
            true,
            false,
        )?;
        self.finish_read(selection)
    }

    fn compute_rbf_weights_with_params(
        &self,
        query: &[f32],
        params: ReadParams,
        normalize: bool,
        sigma: Option<f32>,
    ) -> Result<RbfSelection, CoreError> {
        self.validate_read_layout()?;
        validate_dimension(query, self.config.d_key, "center read query")?;
        validate_finite(query, "center read query")?;
        validate_finite(
            &[
                params.minkowski_p,
                params.weight_cosine,
                params.weight_minkowski,
            ],
            "hybrid read parameters",
        )?;
        if params.use_hybrid_metric && params.hybrid_candidates == 0 {
            return Err(CoreError::InvalidState(
                "hybrid candidate count must be positive".to_owned(),
            ));
        }

        let active_indices = self.active_indices();
        if active_indices.is_empty() || params.top_k == 0 {
            return Ok(Self::empty_selection(false));
        }

        let sigma = sigma.unwrap_or(self.config.sigma_read);
        let mut parts = Vec::with_capacity(active_indices.len());
        let mut raw_weights = Vec::with_capacity(active_indices.len());
        for &index in &active_indices {
            let similarity = cosine(query, self.key(index))?;
            let distance_squared = 2.0 - 2.0 * similarity;
            raw_weights.push(rbf_weight(distance_squared, sigma)?);
            parts.push(ScoreParts {
                cosine: similarity,
                context_cosine: 0.0,
                terrain_cosine: 0.0,
                combined_score: similarity,
            });
        }

        let hybrid_applied =
            params.use_hybrid_metric && active_indices.len() > params.hybrid_candidates;
        let selected_locals: Vec<usize> = if hybrid_applied {
            let candidates = top_k_largest(&raw_weights, params.hybrid_candidates)?;
            let mut candidate_locals: Vec<usize> =
                candidates.iter().map(|(_, local)| *local).collect();
            candidate_locals.sort_unstable();

            let mut distances = Vec::with_capacity(candidate_locals.len());
            for &local in &candidate_locals {
                distances.push(minkowski(
                    query,
                    self.key(active_indices[local]),
                    params.minkowski_p,
                )?);
            }
            let distance_max = distances.iter().copied().fold(0.0_f32, f32::max);
            let mut combined = Vec::with_capacity(candidate_locals.len());
            for (&local, distance) in candidate_locals.iter().zip(&distances) {
                let distance_normalized = *distance / (distance_max + 1e-8);
                let minkowski_score = 1.0 - distance_normalized;
                combined.push(
                    params.weight_cosine * ((1.0 - parts[local].cosine) / 2.0)
                        + params.weight_minkowski * (1.0 - minkowski_score),
                );
            }
            let selected_in_candidates =
                top_k_smallest(&combined, params.top_k.min(candidate_locals.len()))?;
            selected_in_candidates
                .iter()
                .map(|(_, candidate)| candidate_locals[*candidate])
                .collect()
        } else {
            top_k_largest(&raw_weights, params.top_k.min(active_indices.len()))?
                .iter()
                .map(|(_, local)| *local)
                .collect()
        };

        self.build_selection(
            &active_indices,
            &raw_weights,
            &parts,
            &selected_locals,
            normalize,
            hybrid_applied,
        )
    }

    fn build_selection(
        &self,
        active_indices: &[usize],
        all_raw_weights: &[f32],
        all_parts: &[ScoreParts],
        selected_locals: &[usize],
        normalize: bool,
        hybrid_applied: bool,
    ) -> Result<RbfSelection, CoreError> {
        let raw_weights: Vec<f32> = selected_locals
            .iter()
            .map(|local| all_raw_weights[*local])
            .collect();
        let intensities: Vec<f32> = selected_locals
            .iter()
            .map(|local| self.intensity[active_indices[*local]])
            .collect();
        let normalized_weights = log_space_softmax(&raw_weights, &intensities)?;
        let operation_weights = if normalize {
            normalized_weights.clone()
        } else {
            raw_weights.clone()
        };

        let mut centers = Vec::with_capacity(selected_locals.len());
        for (selection_index, &local) in selected_locals.iter().enumerate() {
            let index = active_indices[local];
            let slot = self.slot(index).ok_or_else(|| {
                CoreError::InvalidState("center index does not fit CenterSlot".to_owned())
            })?;
            let parts = all_parts[local];
            centers.push(CenterReadTrace {
                index,
                slot,
                raw_rbf_weight: raw_weights[selection_index],
                normalized_weight: normalized_weights[selection_index],
                cosine: parts.cosine,
                context_cosine: parts.context_cosine,
                terrain_cosine: parts.terrain_cosine,
                combined_score: parts.combined_score,
                intensity: intensities[selection_index],
                record: self.record[index],
                support: self.support[index].clone(),
            });
        }
        Ok(RbfSelection {
            weights: operation_weights,
            centers,
            hybrid_applied,
        })
    }

    fn finish_read(&self, selection: RbfSelection) -> Result<ReadResult, CoreError> {
        let mut r_v = [0.0; VALUE_DIM];
        let mut r_e = [0.0; AFFECT_DIM];
        for center in &selection.centers {
            let value = self.value_row(center.index);
            let affect = self.affect_row(center.index);
            for (output, selected) in r_v.iter_mut().zip(value) {
                *output += center.normalized_weight * selected;
            }
            for (output, selected) in r_e.iter_mut().zip(affect) {
                *output += center.normalized_weight * selected;
            }
        }
        validate_finite(&r_v, "center value readout")?;
        validate_finite(&r_e, "center affect readout")?;
        Ok(ReadResult {
            r_v,
            r_e,
            selection,
        })
    }

    fn validate_read_layout(&self) -> Result<(), CoreError> {
        let n = self.config.n_centers;
        Self::validate_storage_len(
            &self.keys,
            n.saturating_mul(self.config.d_key),
            "center keys",
        )?;
        Self::validate_storage_len(&self.values, n.saturating_mul(VALUE_DIM), "center values")?;
        Self::validate_storage_len(&self.intensity, n, "center intensity")?;
        Self::validate_storage_len(&self.affect, n.saturating_mul(AFFECT_DIM), "center affect")?;
        Self::validate_storage_len(&self.active, n, "center active mask")?;
        Self::validate_storage_len(&self.incarnation, n, "center incarnations")?;
        Self::validate_storage_len(
            &self.context,
            n.saturating_mul(CONTEXT_DIM),
            "center context",
        )?;
        Self::validate_storage_len(
            &self.terrain,
            n.saturating_mul(TERRAIN_DIM),
            "center terrain",
        )?;
        Self::validate_storage_len(&self.record, n, "center records")?;
        Self::validate_storage_len(&self.support, n, "center support")?;
        Ok(())
    }

    fn validate_storage_len<T>(
        values: &[T],
        expected: usize,
        what: &'static str,
    ) -> Result<(), CoreError> {
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

    fn active_indices(&self) -> Vec<usize> {
        self.active
            .iter()
            .enumerate()
            .filter_map(|(index, active)| active.then_some(index))
            .collect()
    }

    fn context_row(&self, index: usize) -> &[f32] {
        &self.context[index * CONTEXT_DIM..(index + 1) * CONTEXT_DIM]
    }

    fn terrain_row(&self, index: usize) -> &[f32] {
        &self.terrain[index * TERRAIN_DIM..(index + 1) * TERRAIN_DIM]
    }

    fn value_row(&self, index: usize) -> &[f32] {
        &self.values[index * VALUE_DIM..(index + 1) * VALUE_DIM]
    }

    fn affect_row(&self, index: usize) -> &[f32] {
        &self.affect[index * AFFECT_DIM..(index + 1) * AFFECT_DIM]
    }

    fn empty_selection(hybrid_applied: bool) -> RbfSelection {
        RbfSelection {
            weights: Vec::new(),
            centers: Vec::new(),
            hybrid_applied,
        }
    }
}
