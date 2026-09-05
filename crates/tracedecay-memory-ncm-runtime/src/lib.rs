//! NCM runtime for the `ncm-biomem-rs.v1` backend (contract: `product/ncm/spec/CONTRACT.md`).
//!
//! Owns everything the pure core must not: the real text encoder, the durable
//! per-namespace store, transactional engine effects, deletion-by-rebuild,
//! snapshots, and the supervised worker/wire/client transport. Never depends on
//! the provider adapter or the registry.

pub mod client;
pub mod embedding;
pub mod engine;
pub mod maintenance;
pub mod ports;
pub mod privacy;
pub mod snapshot;
pub mod store;
pub mod wire;
pub mod worker;
