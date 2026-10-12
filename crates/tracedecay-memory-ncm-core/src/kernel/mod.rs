//! Serializable, build-then-publish facade for the pure NCM state machine.

mod digest;

use crate::centers::MemoryCenters;
use crate::centers::write::{WriteInput, WriteOutcome, WriteParams};
use crate::consolidation::{self, ConsolidationReport, MergePruneReport};
use crate::dynamics::{self, AdvanceReport, Scheduler};
use crate::projections::ProjectionBundle;
use crate::recall::{RecallOutput, RecallPolicy};
use crate::records::{RecordInput, RecordTable, Support};
use crate::signals::{novelty_from_ltm, write_strength};
use crate::terrain::{Terrain3D, TerrainStats};
use crate::types::{
    AffectVector, CONTEXT_DIM, CenterSlot, CoreError, LTM_KEY_DIM, Layer, LogicalTick, NcmConfig,
    RecordId, SourceId, TERRAIN_DIM, VALUE_DIM,
};
use serde::{Deserialize, Serialize};

/// Projection bundle name used by the kernel facade.
pub type Projections = ProjectionBundle;

/// The independent STM and LTM terrain fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TerrainField {
    /// Short-term terrain.
    pub stm: Terrain3D,
    /// Long-term terrain.
    pub ltm: Terrain3D,
}

/// One source-backed observation admitted to the kernel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewRecord {
    /// Opaque host-admitted deletion identity.
    pub source: SourceId,
    /// Original lookup text.
    pub key_text: String,
    /// Original value text.
    pub value_text: String,
    /// Validated four-channel affect.
    pub affect: AffectVector,
    /// Caller-admitted surprise signal.
    pub surprise: f32,
    /// Caller-admitted observation intensity.
    pub intensity: f32,
}

/// Coarse effect of a write in one layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LayerWriteKind {
    /// Strength fell below the reference write cutoff.
    Ignored,
    /// A new fixed-capacity slot was activated.
    Created,
    /// One or more existing centers were reinforced.
    Reinforced,
}

/// Layer-specific write effects for one observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedOrReinforced {
    /// STM write effect. Observe writes STM only (reference text_memory.py store_record); LTM is populated by consolidation.
    pub stm: LayerWriteKind,
}

/// Result of one atomic observation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObserveReport {
    /// Newly allocated stable record identity.
    pub record_id: RecordId,
    /// Effects in both center layers.
    pub created_or_reinforced: CreatedOrReinforced,
    /// Logical tick after the observation schedule completed.
    pub tick: LogicalTick,
    /// Whether the sleep boundary triggered consolidation.
    pub consolidated: bool,
}

/// Result of explicit usage feedback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedbackReport {
    /// Distinct center slots whose usage counter advanced.
    pub centers_updated: usize,
}

/// Redacted, deterministic kernel inspection summary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KernelStats {
    /// Active STM center count.
    pub stm_active: usize,
    /// Active LTM center count.
    pub ltm_active: usize,
    /// Retained immutable record count.
    pub records: usize,
    /// Current logical tick.
    pub tick: LogicalTick,
    /// Current scheduler fatigue.
    pub fatigue: f32,
    /// Ticks since completed consolidation.
    pub steps_since_consolidation: u64,
    /// STM terrain summary.
    pub stm_terrain: TerrainStats,
    /// LTM terrain summary.
    pub ltm_terrain: TerrainStats,
}

/// Complete serializable pure NCM state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NcmKernel {
    /// Short-term center bank.
    pub stm: MemoryCenters,
    /// Long-term center bank.
    pub ltm: MemoryCenters,
    /// Independent STM/LTM terrain fields.
    pub terrain: TerrainField,
    /// Immutable source records and correction lineage.
    pub records: RecordTable,
    /// Incarnation-safe record support.
    pub support: Support,
    /// Logical-time and fatigue state.
    pub scheduler: Scheduler,
    /// Frozen algorithm configuration.
    pub config: NcmConfig,
    /// Persisted deterministic projection matrices.
    pub projections: Projections,
}

impl NcmKernel {
    /// Builds a fresh deterministic kernel from a projection seed.
    pub fn new(seed: u64, config: NcmConfig) -> Result<Self, CoreError> {
        let projections = ProjectionBundle::generate(seed);
        let stm = MemoryCenters::new(config.stm.clone(), seed ^ 0x5354_4d00_0000_0001)?;
        let ltm = MemoryCenters::new(config.ltm.clone(), seed ^ 0x4c54_4d00_0000_0001)?;
        let terrain = TerrainField {
            stm: Terrain3D::new(
                config.terrain_resolution,
                config.stm.terrain_alpha_h,
                config.stm.terrain_alpha_e,
                config.stm.terrain_lambda,
            )?,
            ltm: Terrain3D::new(
                config.terrain_resolution,
                config.ltm.terrain_alpha_h,
                config.ltm.terrain_alpha_e,
                config.ltm.terrain_lambda,
            )?,
        };
        Ok(Self {
            stm,
            ltm,
            terrain,
            records: RecordTable::new(&config),
            support: Support::new(&config),
            scheduler: Scheduler::new(),
            config,
            projections,
        })
    }

    /// Reconstitutes a kernel from already decoded persisted parts.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        stm: MemoryCenters,
        ltm: MemoryCenters,
        terrain: TerrainField,
        records: RecordTable,
        support: Support,
        scheduler: Scheduler,
        config: NcmConfig,
        projections: Projections,
    ) -> Result<Self, CoreError> {
        if stm.config != config.stm || ltm.config != config.ltm {
            return Err(CoreError::InvalidState(
                "kernel center configuration mismatch".to_owned(),
            ));
        }
        if terrain.stm.resolution != config.terrain_resolution
            || terrain.ltm.resolution != config.terrain_resolution
        {
            return Err(CoreError::InvalidState(
                "kernel terrain resolution mismatch".to_owned(),
            ));
        }
        Ok(Self {
            stm,
            ltm,
            terrain,
            records,
            support,
            scheduler,
            config,
            projections,
        })
    }

    /// Atomically projects, records, writes, splats, advances one D10 tick, and
    /// performs automatic sleep maintenance when due.
    pub fn observe(
        &mut self,
        key_embedding: &[f32],
        value_embedding: &[f32],
        record: NewRecord,
    ) -> Result<ObserveReport, CoreError> {
        let mut staged = self.clone();
        let projected = staged
            .projections
            .project_record(key_embedding, value_embedding)?;
        let novelty = novelty_from_ltm(&staged.ltm, &projected.ltm_key)?;
        let omega = write_strength(
            &staged.config,
            novelty,
            record.surprise,
            record.affect,
            record.intensity,
        )?;
        let record_id = staged.records.insert(RecordInput {
            source: record.source,
            key_text: record.key_text,
            value_text: record.value_text,
            created_tick: staged.scheduler.tick,
            ltm_key: projected.ltm_key.clone(),
            stm_key: projected.stm_key.clone(),
            value_vec: projected.value.clone(),
            affect: record.affect,
        })?;
        let value = array::<VALUE_DIM>(&projected.value, "projected value")?;
        let context = array::<CONTEXT_DIM>(&projected.context, "projected context")?;
        let ltm_key = array::<LTM_KEY_DIM>(&projected.ltm_key, "projected LTM key")?;
        let stm_terrain = array::<TERRAIN_DIM>(&projected.stm_terrain, "STM terrain")?;
        let stm_outcome = staged.stm.write(
            WriteInput {
                key: &projected.stm_key,
                ltm_key,
                value,
                affect: record.affect,
                intensity: omega,
                context,
                terrain: stm_terrain,
                record: record_id,
                age: 0,
            },
            &WriteParams::for_layer(&staged.config, Layer::Stm),
        )?;
        check_capacity(&stm_outcome, Layer::Stm)?;
        sync_write_support(&mut staged.support, &staged.stm, &stm_outcome)?;
        staged.terrain.stm.splat(
            stm_terrain,
            omega,
            Some(record.affect.0),
            staged.config.terrain_splat_sigma,
            staged.config.stm.terrain_eta,
        )?;
        dynamics::observe_tick(&mut staged.scheduler, record.intensity, &staged.config);
        dynamics::homeostasis_step(&mut staged.stm, &staged.config.stm);
        dynamics::homeostasis_step(&mut staged.ltm, &staged.config.ltm);
        staged.terrain.stm.step();
        staged.terrain.ltm.step();

        let consolidated = dynamics::sleep_due(&staged.scheduler, &staged.config);
        if consolidated {
            staged.consolidate()?;
            staged.merge_prune()?;
        }
        let report = ObserveReport {
            record_id,
            created_or_reinforced: CreatedOrReinforced {
                stm: write_kind(&stm_outcome),
            },
            tick: staged.scheduler.tick,
            consolidated,
        };
        *self = staged;
        Ok(report)
    }

    /// Applies explicit use feedback; recall itself remains side-effect free (D05).
    pub fn feedback(&mut self, record_ids: &[RecordId]) -> Result<FeedbackReport, CoreError> {
        for record_id in record_ids {
            if self.records.get(*record_id).is_none() {
                return Err(CoreError::UnknownRecord(*record_id));
            }
        }
        let mut staged = self.clone();
        let mut centers_updated = 0;
        for centers in [&mut staged.stm, &mut staged.ltm] {
            for index in 0..centers.config.n_centers {
                if centers.active[index]
                    && centers.support[index]
                        .iter()
                        .any(|record| record_ids.contains(record))
                {
                    centers.usage[index] = centers.usage[index].saturating_add(1);
                    centers_updated += 1;
                }
            }
        }
        *self = staged;
        Ok(FeedbackReport { centers_updated })
    }

    /// Links an old record to an already admitted replacement with evidence.
    pub fn correction(
        &mut self,
        old: RecordId,
        new: RecordId,
        evidence_sha256: String,
    ) -> Result<(), CoreError> {
        let mut staged = self.clone();
        staged.records.supersede(old, new, evidence_sha256)?;
        *self = staged;
        Ok(())
    }

    /// Applies bounded explicit maintenance ticks atomically.
    pub fn advance(&mut self, ticks: u32) -> Result<AdvanceReport, CoreError> {
        let mut staged = self.clone();
        let report = dynamics::advance(
            &mut staged.stm,
            &mut staged.ltm,
            &mut staged.terrain.stm,
            &mut staged.scheduler,
            ticks,
            &staged.config,
        )?;
        for _ in 0..ticks {
            staged.terrain.ltm.step();
        }
        *self = staged;
        Ok(report)
    }

    /// Runs one explicit atomic STM-to-LTM consolidation generation.
    pub fn consolidate(&mut self) -> Result<ConsolidationReport, CoreError> {
        let mut staged = self.clone();
        let report = consolidation::consolidate(
            &mut staged.stm,
            &mut staged.ltm,
            &mut staged.terrain.stm,
            &mut staged.terrain.ltm,
            &staged.records,
            &mut staged.support,
            &mut staged.scheduler,
            &staged.config,
        )?;
        *self = staged;
        Ok(report)
    }

    /// Runs deterministic merge then prune passes for both layers atomically.
    pub fn merge_prune(&mut self) -> Result<MergePruneReport, CoreError> {
        let mut staged = self.clone();
        let stm_merge = consolidation::merge(
            &mut staged.stm,
            &staged.records,
            &mut staged.support,
            &staged.config,
        )?;
        let ltm_merge = consolidation::merge(
            &mut staged.ltm,
            &staged.records,
            &mut staged.support,
            &staged.config,
        )?;
        let stm_prune = consolidation::prune(
            &mut staged.stm,
            &staged.records,
            &mut staged.support,
            &staged.config,
        )?;
        let ltm_prune = consolidation::prune(
            &mut staged.ltm,
            &staged.records,
            &mut staged.support,
            &staged.config,
        )?;
        *self = staged;
        Ok(MergePruneReport {
            stm_merge,
            ltm_merge,
            stm_prune,
            ltm_prune,
        })
    }

    /// Recalls from immutable state with terrain influence fixed to zero (D03).
    pub fn recall(
        &self,
        query_embedding: &[f32],
        top_k: usize,
        policy: RecallPolicy,
    ) -> Result<RecallOutput, CoreError> {
        let query_stm = self.projections.project_to_stm(query_embedding)?;
        let query_ltm = self.projections.project_to_ltm(query_embedding)?;
        let query_context = self.projections.project_to_context(query_embedding)?;
        crate::recall::recall(
            &query_stm,
            &query_ltm,
            &query_context,
            &self.stm,
            &self.ltm,
            0.0,
            &self.records,
            &self.support,
            &policy,
            top_k,
        )
    }

    /// Returns redacted state statistics without mutation.
    #[must_use]
    pub fn inspect(&self) -> KernelStats {
        KernelStats {
            stm_active: self.stm.n_active(),
            ltm_active: self.ltm.n_active(),
            records: self.records.len(),
            tick: self.scheduler.tick,
            fatigue: self.scheduler.fatigue,
            steps_since_consolidation: self.scheduler.steps_since_consolidation,
            stm_terrain: self.terrain.stm.stats(),
            ltm_terrain: self.terrain.ltm.stats(),
        }
    }

    /// SHA-256 of canonical serde traversal bytes for the complete state.
    #[must_use]
    pub fn state_digest(&self) -> [u8; 32] {
        digest::digest(self)
    }
}

fn check_capacity(outcome: &WriteOutcome, layer: Layer) -> Result<(), CoreError> {
    if *outcome == WriteOutcome::CapacityExhausted {
        Err(CoreError::CapacityExhausted(layer))
    } else {
        Ok(())
    }
}

fn write_kind(outcome: &WriteOutcome) -> LayerWriteKind {
    match outcome {
        WriteOutcome::Ignored => LayerWriteKind::Ignored,
        WriteOutcome::Created { .. } => LayerWriteKind::Created,
        WriteOutcome::Reinforced { .. } => LayerWriteKind::Reinforced,
        WriteOutcome::CapacityExhausted => LayerWriteKind::Ignored,
    }
}

fn sync_write_support(
    support: &mut Support,
    centers: &MemoryCenters,
    outcome: &WriteOutcome,
) -> Result<(), CoreError> {
    let slots: Vec<CenterSlot> = match outcome {
        WriteOutcome::Created { slot } => vec![*slot],
        WriteOutcome::Reinforced { updated, .. } => updated.clone(),
        WriteOutcome::Ignored | WriteOutcome::CapacityExhausted => Vec::new(),
    };
    for slot in slots {
        let index = centers.resolve(slot)?;
        let mut records = match support.support_for(slot) {
            Ok(existing) => existing.to_vec(),
            Err(CoreError::StaleHandle) => Vec::new(),
            Err(error) => return Err(error),
        };
        for record in &centers.support[index] {
            if !records.contains(record) {
                records.push(*record);
            }
        }
        support.set_support(slot, &records)?;
    }
    Ok(())
}

fn array<const N: usize>(values: &[f32], what: &'static str) -> Result<[f32; N], CoreError> {
    values.try_into().map_err(|_| CoreError::DimensionMismatch {
        what,
        expected: N,
        actual: values.len(),
    })
}
