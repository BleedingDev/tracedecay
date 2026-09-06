//! Admitted write-strength, novelty, and optional affect heuristic signals.

pub mod affect;

use crate::centers::MemoryCenters;
use crate::numeric::{sigmoid, validate_finite};
use crate::types::{AffectVector, CoreError, NcmConfig};
use serde::{Deserialize, Serialize};

/// Identity of an optional signal-generation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalSource {
    /// Pinned Czech/English substring heuristic from Biomem `EmotionExtractor`.
    KeywordHeuristicV1,
}

/// Computes the frozen write-strength equation.
///
/// `omega = base * 3 * sigmoid(2*novelty + 0.3*surprise +
/// 0.3*salience - 1) * intensity`, with coefficients read from the frozen
/// configuration and `salience = max(abs(affect_i - 1))`.
pub fn write_strength(
    config: &NcmConfig,
    novelty: f32,
    surprise: f32,
    affect: AffectVector,
    intensity: f32,
) -> Result<f32, CoreError> {
    validate_finite(
        &[
            novelty,
            surprise,
            intensity,
            config.write_strength_base,
            config.write_novelty_weight,
            config.write_surprise_weight,
            config.write_emotion_weight,
            config.write_bias,
            config.write_strength_scale,
        ],
        "write-strength inputs",
    )?;
    validate_finite(&affect.0, "write-strength affect")?;
    let salience = affect
        .0
        .iter()
        .map(|channel| (*channel - 1.0).abs())
        .fold(0.0_f32, f32::max);
    let logit = config.write_novelty_weight * novelty
        + config.write_surprise_weight * surprise
        + config.write_emotion_weight * salience
        + config.write_bias;
    let omega =
        config.write_strength_base * config.write_strength_scale * sigmoid(logit)? * intensity;
    validate_finite(&[omega], "write strength")?;
    Ok(omega)
}

/// Computes novelty as one minus the top unnormalized LTM RBF weight.
///
/// An empty LTM bank has novelty `1.0`.
pub fn novelty_from_ltm(ltm: &MemoryCenters, ltm_key: &[f32]) -> Result<f32, CoreError> {
    let selection = ltm.compute_rbf_weights(ltm_key, 1, false, None)?;
    let novelty = selection
        .weights
        .first()
        .map_or(1.0, |weight| 1.0 - *weight);
    validate_finite(&[novelty], "LTM novelty")?;
    Ok(novelty)
}
