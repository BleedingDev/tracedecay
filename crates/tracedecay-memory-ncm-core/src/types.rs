//! Frozen identities, configuration, and error types (contract §2, §4).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Production algorithm profile name. Any corrected behavior change bumps this.
pub const ALGORITHM_PROFILE: &str = "ncm-biomem-rs.v1";

/// Embedding dimensionality of `paraphrase-multilingual-MiniLM-L12-v2`.
pub const EMBEDDING_DIM: usize = 384;
/// LTM key dimensionality.
pub const LTM_KEY_DIM: usize = 64;
/// STM key dimensionality.
pub const STM_KEY_DIM: usize = 16;
/// Value dimensionality.
pub const VALUE_DIM: usize = 128;
/// Context fingerprint dimensionality.
pub const CONTEXT_DIM: usize = 16;
/// Terrain coordinate dimensionality.
pub const TERRAIN_DIM: usize = 3;
/// Affect channels: dopamine, serotonin, cortisol, oxytocin.
pub const AFFECT_DIM: usize = 4;

/// Four-channel affect vector; neutral is `[1.0; 4]`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct AffectVector(pub [f32; AFFECT_DIM]);

impl AffectVector {
    /// Neutral affect `[1,1,1,1]` (reference `_sanitize_emotion(None)`).
    #[must_use]
    pub const fn neutral() -> Self {
        Self([1.0; AFFECT_DIM])
    }

    /// Validates finiteness; non-finite channels are a typed error, never replaced.
    pub fn validated(values: [f32; AFFECT_DIM]) -> Result<Self, CoreError> {
        if values.iter().all(|value| value.is_finite()) {
            Ok(Self(values))
        } else {
            Err(CoreError::NonFinite("affect"))
        }
    }
}

/// Stable per-namespace record identity; never reused, never an array index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordId(pub u64);

/// Opaque host-admitted source identity (deletion key).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

/// Center slot handle with incarnation so stale handles never resolve.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CenterSlot {
    /// Fixed-capacity slot index.
    pub index: u32,
    /// Incremented on every (re)activation of the slot.
    pub incarnation: u32,
}

/// Logical learning tick (contract §4 D10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogicalTick(pub u64);

/// Algorithm identity = profile + canonical config digest (computed by the runtime).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgorithmIdentity {
    /// `ALGORITHM_PROFILE`.
    pub profile: String,
    /// Lowercase sha256 hex of the canonical config JSON.
    pub config_sha256: String,
}

/// Memory layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Layer {
    /// Short-term memory (16-D keys).
    Stm,
    /// Long-term memory (64-D keys).
    Ltm,
}

/// Per-layer center parameters (reference `MemoryCenters` constructor arguments).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LayerConfig {
    /// Fixed center capacity.
    pub n_centers: usize,
    /// Key dimensionality.
    pub d_key: usize,
    /// Read kernel width.
    pub sigma_read: f32,
    /// Configured write width. D02: retained but unused by the executed write path.
    pub sigma_write: f32,
    /// Intensity leak per tick.
    pub leak: f32,
    /// Emotion leak per tick (toward 1.0).
    pub leak_emotion: f32,
    /// Value leak per tick.
    pub leak_value: f32,
    /// Value EMA rate on reinforcement.
    pub alpha_value: f32,
    /// Emotion EMA rate on reinforcement.
    pub alpha_emotion: f32,
    /// Key drift rate (reference default 0.0: keys never move).
    pub alpha_key: f32,
    /// Read top-k.
    pub top_k_read: usize,
    /// Write top-k.
    pub top_k_write: usize,
    /// Max-weight novelty threshold below which a new center is created.
    pub new_center_threshold: f32,
    /// Terrain write strength (eta).
    pub terrain_eta: f32,
    /// Terrain leak (lambda).
    pub terrain_lambda: f32,
    /// Terrain H diffusion coefficient.
    pub terrain_alpha_h: f32,
    /// Terrain E diffusion coefficient.
    pub terrain_alpha_e: f32,
}

/// Frozen production configuration (Biomem `MemoryConfig` defaults).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NcmConfig {
    /// LTM layer.
    pub ltm: LayerConfig,
    /// STM layer.
    pub stm: LayerConfig,
    /// Terrain grid resolution (48³).
    pub terrain_resolution: usize,
    /// Terrain splat sigma in normalized units (reference `sigma=0.1`).
    pub terrain_splat_sigma: f32,
    /// Hybrid metric enabled.
    pub use_hybrid_metric: bool,
    /// Minkowski p for the hybrid rerank.
    pub minkowski_p: f32,
    /// Hybrid cosine weight.
    pub weight_cosine: f32,
    /// Hybrid Minkowski weight.
    pub weight_minkowski: f32,
    /// Hybrid candidate count.
    pub hybrid_candidates: usize,
    /// Compound semantic weight.
    pub weight_semantic: f32,
    /// Compound context weight.
    pub weight_context: f32,
    /// Compound terrain weight.
    pub weight_terrain: f32,
    /// Fatigue leak.
    pub fatigue_leak: f32,
    /// Fatigue accumulation factor (reference `update_fatigue` uses 0.1).
    pub fatigue_gain: f32,
    /// Fatigue threshold for sleep.
    pub fatigue_threshold: f32,
    /// Minimum ticks between automatic consolidations.
    pub consolidation_min_interval: u64,
    /// Top-M STM centers transferred.
    pub consolidation_top_m: usize,
    /// Transfer strength kappa.
    pub consolidation_kappa: f32,
    /// Minimum transfer intensity.
    pub consolidation_min_intensity: f32,
    /// Terrain pour H coefficient.
    pub consolidation_xi_h: f32,
    /// Terrain pour E coefficient.
    pub consolidation_xi_e: f32,
    /// Blur sigma in grid cells for the pour (D01: actually applied).
    pub consolidation_blur_sigma: f32,
    /// Post-sleep fatigue factor rho_F.
    pub normalization_rho_f: f32,
    /// Value saturation constant c_V.
    pub normalization_c_v: f32,
    /// Normalization intensity floor.
    pub normalization_min_intensity: f32,
    /// Merge similarity threshold.
    pub merge_similarity_threshold: f32,
    /// Prune intensity threshold (effective = max(thr, floor*1.1)).
    pub prune_intensity_threshold: f32,
    /// Prune minimum age.
    pub prune_min_age: u64,
    /// Prune usage ceiling (usage < 5 prunable; >= 5 protected).
    pub prune_usage_ceiling: u64,
    /// Write strength base.
    pub write_strength_base: f32,
    /// Novelty coefficient.
    pub write_novelty_weight: f32,
    /// Surprise coefficient.
    pub write_surprise_weight: f32,
    /// Salience coefficient.
    pub write_emotion_weight: f32,
    /// Sigmoid bias.
    pub write_bias: f32,
    /// Write strength sigmoid scale (reference literal 3.0).
    pub write_strength_scale: f32,
    /// Maximum UTF-8 bytes of key+value per record.
    pub max_record_bytes: usize,
    /// Maximum retained sources per center support set.
    pub max_center_support: usize,
}

impl Default for NcmConfig {
    fn default() -> Self {
        Self {
            ltm: LayerConfig {
                n_centers: 4096,
                d_key: LTM_KEY_DIM,
                sigma_read: 0.5,
                sigma_write: 0.15,
                leak: 2.66e-5,
                leak_emotion: 3.5e-5,
                leak_value: 2.1e-5,
                alpha_value: 0.03,
                alpha_emotion: 0.01,
                alpha_key: 0.0,
                top_k_read: 32,
                top_k_write: 16,
                new_center_threshold: 0.78,
                terrain_eta: 0.005,
                terrain_lambda: 3.5e-5,
                terrain_alpha_h: 0.002,
                terrain_alpha_e: 0.001,
            },
            stm: LayerConfig {
                n_centers: 512,
                d_key: STM_KEY_DIM,
                sigma_read: 0.4,
                sigma_write: 0.2,
                leak: 0.0035,
                leak_emotion: 0.0049,
                leak_value: 0.0028,
                alpha_value: 0.1,
                alpha_emotion: 0.08,
                alpha_key: 0.0,
                top_k_read: 16,
                top_k_write: 8,
                new_center_threshold: 0.5,
                terrain_eta: 0.02,
                terrain_lambda: 0.0007,
                terrain_alpha_h: 0.02,
                terrain_alpha_e: 0.01,
            },
            terrain_resolution: 48,
            terrain_splat_sigma: 0.1,
            use_hybrid_metric: true,
            minkowski_p: 0.5,
            weight_cosine: 0.7,
            weight_minkowski: 0.3,
            hybrid_candidates: 64,
            weight_semantic: 0.6,
            weight_context: 0.25,
            weight_terrain: 0.15,
            fatigue_leak: 0.007,
            fatigue_gain: 0.1,
            fatigue_threshold: 2.5,
            consolidation_min_interval: 100,
            consolidation_top_m: 128,
            consolidation_kappa: 0.8,
            consolidation_min_intensity: 0.3,
            consolidation_xi_h: 0.005,
            consolidation_xi_e: 0.003,
            consolidation_blur_sigma: 2.0,
            normalization_rho_f: 0.2,
            normalization_c_v: 2.0,
            normalization_min_intensity: 0.01,
            merge_similarity_threshold: 0.95,
            prune_intensity_threshold: 0.001,
            prune_min_age: 300,
            prune_usage_ceiling: 5,
            write_strength_base: 1.0,
            write_novelty_weight: 2.0,
            write_surprise_weight: 0.3,
            write_emotion_weight: 0.3,
            write_bias: -1.0,
            write_strength_scale: 3.0,
            max_record_bytes: 16 * 1024,
            max_center_support: 32,
        }
    }
}

/// Typed kernel errors. No variant implies partial state was left behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreError {
    /// A vector or matrix had the wrong dimensionality.
    DimensionMismatch {
        /// What was being validated.
        what: &'static str,
        /// Expected length.
        expected: usize,
        /// Actual length.
        actual: usize,
    },
    /// A non-finite value was supplied.
    NonFinite(&'static str),
    /// A record/content budget was exceeded.
    BudgetExceeded(&'static str),
    /// Fixed center capacity is exhausted for this layer.
    CapacityExhausted(Layer),
    /// A handle referred to a slot incarnation that no longer exists.
    StaleHandle,
    /// A record identity was unknown.
    UnknownRecord(RecordId),
    /// Serialized state was malformed or incompatible.
    InvalidState(String),
    /// A requested operation is not part of the v1 profile.
    Unsupported(&'static str),
}

impl fmt::Display for CoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionMismatch { what, expected, actual } => {
                write!(formatter, "{what}: expected {expected} elements, got {actual}")
            }
            Self::NonFinite(what) => write!(formatter, "{what}: non-finite value"),
            Self::BudgetExceeded(what) => write!(formatter, "{what}: budget exceeded"),
            Self::CapacityExhausted(layer) => write!(formatter, "{layer:?}: capacity exhausted"),
            Self::StaleHandle => formatter.write_str("stale center handle"),
            Self::UnknownRecord(id) => write!(formatter, "unknown record {}", id.0),
            Self::InvalidState(reason) => write!(formatter, "invalid state: {reason}"),
            Self::Unsupported(what) => write!(formatter, "unsupported: {what}"),
        }
    }
}

impl std::error::Error for CoreError {}
