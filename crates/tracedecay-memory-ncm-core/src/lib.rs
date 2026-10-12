//! Pure Rust NCM kernel derived from Biomem (`500847ff…`), profile `ncm-biomem-rs.v1`.
//!
//! No filesystem, network, process, clock, or host-store access lives here. The
//! runtime crate owns durability, inference, and transport. Contract:
//! `product/ncm/spec/CONTRACT.md`.

pub mod centers;
pub mod consolidation;
pub mod dynamics;
pub mod kernel;
pub mod numeric;
pub mod projections;
pub mod recall;
pub mod records;
pub mod signals;
pub mod terrain;
pub mod types;

pub use types::{
    ALGORITHM_PROFILE, AffectVector, AlgorithmIdentity, CenterSlot, CoreError, LogicalTick,
    NcmConfig, RecordId, SourceId,
};
