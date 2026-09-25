#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

//! The authority for immutable dense vector generations.
//!
//! This crate owns generation planning, resumable staging, content addressed
//! vector bytes, complete only publication, rollback, and a read only exact
//! flat search path.  It deliberately has no inference runtime, query layer,
//! ANN index, or graph database dependency.

pub mod authority;
pub mod types;

pub use authority::*;
pub use types::*;
