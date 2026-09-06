//! Associative memory centers (reference `MemoryCenters`). Read path in `read`,
//! write/allocation in `write`/`allocation`; this file only owns the shared state
//! layout so the two workers compile against one definition.

pub mod allocation;
pub mod read;
pub mod write;

use crate::types::{
    CenterSlot, LayerConfig, RecordId, AFFECT_DIM, CONTEXT_DIM, LTM_KEY_DIM, TERRAIN_DIM, VALUE_DIM,
};
use serde::{Deserialize, Serialize};

/// Fixed-capacity center bank. Vectors are stored row-major and flattened:
/// `keys[i * d_key .. (i + 1) * d_key]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryCenters {
    /// Layer parameters.
    pub config: LayerConfig,
    /// Unit-norm keys `[n, d_key]`.
    pub keys: Vec<f32>,
    /// Canonical LTM-basis key per center `[n, 64]` (D08). For LTM banks this equals `keys`.
    pub ltm_keys: Vec<f32>,
    /// Values `[n, 128]`.
    pub values: Vec<f32>,
    /// Intensity `h[n]`.
    pub intensity: Vec<f32>,
    /// Affect `[n, 4]` (neutral 1.0).
    pub affect: Vec<f32>,
    /// Usage counters (only `Feedback` increments them; D05).
    pub usage: Vec<u64>,
    /// Age in ticks since creation (carried on consolidation).
    pub age: Vec<u64>,
    /// Active mask.
    pub active: Vec<bool>,
    /// Slot incarnation counters.
    pub incarnation: Vec<u32>,
    /// Context fingerprints `[n, 16]`.
    pub context: Vec<f32>,
    /// Terrain positions `[n, 3]` in `[-1, 1]`.
    pub terrain: Vec<f32>,
    /// Winning (metadata) record per center, if any.
    pub record: Vec<Option<RecordId>>,
    /// Every record that reinforced this center (bounded by `max_center_support`).
    pub support: Vec<Vec<RecordId>>,
    /// Homeostasis ticks applied.
    pub total_step: u64,
}

impl MemoryCenters {
    /// Creates an empty bank. Keys are initialized by the owning constructor in
    /// `allocation` from a recorded seed (reference: random unit keys), so this
    /// only lays out storage.
    #[must_use]
    pub fn with_layout(config: LayerConfig) -> Self {
        let n = config.n_centers;
        let d_key = config.d_key;
        Self {
            keys: vec![0.0; n * d_key],
            ltm_keys: vec![0.0; n * LTM_KEY_DIM],
            values: vec![0.0; n * VALUE_DIM],
            intensity: vec![0.0; n],
            affect: vec![1.0; n * AFFECT_DIM],
            usage: vec![0; n],
            age: vec![0; n],
            active: vec![false; n],
            incarnation: vec![0; n],
            context: vec![0.0; n * CONTEXT_DIM],
            terrain: vec![0.0; n * TERRAIN_DIM],
            record: vec![None; n],
            support: vec![Vec::new(); n],
            total_step: 0,
            config,
        }
    }

    /// Number of active centers.
    #[must_use]
    pub fn n_active(&self) -> usize {
        self.active.iter().filter(|active| **active).count()
    }

    /// Handle for a slot at its current incarnation.
    #[must_use]
    pub fn slot(&self, index: usize) -> Option<CenterSlot> {
        let incarnation = *self.incarnation.get(index)?;
        Some(CenterSlot {
            layer: self.config.layer,
            index: u32::try_from(index).ok()?,
            incarnation,
        })
    }

    /// Key row for a center.
    #[must_use]
    pub fn key(&self, index: usize) -> &[f32] {
        let d = self.config.d_key;
        &self.keys[index * d..(index + 1) * d]
    }
}
