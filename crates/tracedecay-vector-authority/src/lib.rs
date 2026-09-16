#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

//! An isolated authority for immutable dense vector generations.
//!
//! This crate owns generation planning, resumable staging, content addressed
//! vector bytes, complete only publication, rollback, and a read only exact
//! flat search path.  It deliberately has no inference runtime, query layer,
//! ANN index, or graph database dependency.

pub mod authority;
pub mod types;

pub use authority::*;
pub use types::*;

/// Compatibility module for callers migrating from the historical
/// `vector_generations` owner.  The implementation remains in this isolated
/// crate; this module only keeps the lifecycle vocabulary discoverable.
pub mod vector_generations {
    pub use crate::authority::*;
    pub use crate::types::*;
}

/// Contract-only namespace for integrations that want to import the durable
/// identities without importing the authority implementation module.
pub mod contracts {
    pub use crate::types::*;
}
