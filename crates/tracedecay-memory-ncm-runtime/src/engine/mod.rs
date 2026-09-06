//! Transactional coordinator joining text encoding, the pure NCM kernel, and durable storage.
//!
//! Mutations are built against a cloned kernel, committed with their durable receipt, and only
//! then published to readers. Dropping a store transaction rolls back the complete candidate
//! effect; a commit that cannot be published is reported as [`Outcome::EffectUnknown`] and is
//! reconciled from the journal on the next open.

mod runtime;

use crate::ports::{Deadline, StateRoot, TextEncoder};
use crate::store::{CommitSeq, NamespaceStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, RwLock};
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};

pub(crate) const MAX_RESIDENT_NAMESPACES: usize = 4;
pub(crate) const MAX_CATALOG_NAMESPACES: usize = 32;
pub(crate) const MAX_TOP_K: usize = 16;
pub(crate) const CHECKPOINT_INTERVAL: u64 = 32;
pub(crate) const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// Stable operation outcome used by every engine reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The operation completed and its payload is authoritative.
    Success,
    /// The operation completed but found no materialized state or recall candidate.
    Empty,
    /// The request was rejected before a durable effect.
    Rejected(RejectReason),
    /// The namespace writer is held elsewhere.
    Busy,
    /// The deadline elapsed before commit.
    Cancelled,
    /// A commit may have happened, but publication or acknowledgement did not complete.
    EffectUnknown,
    /// Persisted compatibility identity differs from the configured engine.
    Incompatible,
    /// Persisted state failed integrity or deterministic replay validation.
    Corrupt,
    /// A required local service or artifact is unavailable.
    Unavailable(String),
    /// The operation belongs to a later contract task or is absent from v1.
    Unsupported,
    /// A declared runtime, catalog, or storage budget was exceeded.
    BudgetExceeded,
}

/// Typed reasons for a pre-effect rejection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    /// The same idempotency key was previously bound to another payload digest.
    IdempotencyConflict,
    /// The request was malformed or outside a declared input bound.
    InvalidRequest(String),
    /// A referenced record does not exist.
    UnknownRecord(RecordId),
}

/// Uniform engine response. `state_generation` is the namespace commit sequence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EngineReply {
    /// Typed operation outcome.
    pub outcome: Outcome,
    /// Durable commit sequence represented by this reply.
    pub state_generation: u64,
    /// Operation-specific JSON payload.
    pub payload: Value,
}

impl EngineReply {
    pub(crate) fn new(outcome: Outcome, state_generation: u64, payload: Value) -> Self {
        Self {
            outcome,
            state_generation,
            payload,
        }
    }

    pub(crate) fn rejected(reason: RejectReason, state_generation: u64) -> Self {
        Self::new(Outcome::Rejected(reason), state_generation, Value::Null)
    }
}

/// Observe affect input: explicit channels or a Biomem-compatible preset name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserveAffect {
    /// Explicit dopamine, serotonin, cortisol, and oxytocin channels.
    Values([f32; 4]),
    /// Exact reference preset name; unknown names resolve to neutral.
    Preset(String),
}

/// Text observation admitted for exactly-once learning.
#[derive(Clone, Debug, PartialEq)]
pub struct ObserveRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Canonical payload SHA-256 supplied by the admitted caller.
    pub payload_sha256: String,
    /// Opaque host-admitted deletion identity.
    pub source: SourceId,
    /// Canonical lookup text.
    pub key_text: String,
    /// Canonical supported value text.
    pub value_text: String,
    /// Optional explicit affect or preset. `None` is neutral.
    pub affect: Option<ObserveAffect>,
    /// Caller-admitted surprise signal.
    pub surprise: f32,
    /// Caller-admitted input intensity.
    pub intensity: f32,
    /// JSON provenance retained in the source capsule.
    pub provenance: Value,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

impl ObserveRequest {
    /// Computes the canonical digest over effect-bearing fields, excluding key, digest, and deadline.
    pub fn canonical_payload_sha256(&self) -> Result<String, String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            source: &'a SourceId,
            key_text: &'a str,
            value_text: &'a str,
            affect: &'a Option<ObserveAffect>,
            surprise: f32,
            intensity: f32,
            provenance: &'a Value,
        }
        runtime::canonical_digest(&Payload {
            source: &self.source,
            key_text: &self.key_text,
            value_text: &self.value_text,
            affect: &self.affect,
            surprise: self.surprise,
            intensity: self.intensity,
            provenance: &self.provenance,
        })
    }
}

/// Immutable text recall request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallRequest {
    /// Query text encoded against the current published view.
    pub query_text: String,
    /// Maximum result count, bounded to sixteen.
    pub top_k: usize,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

/// Explicit usage feedback request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedbackRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Records whose supporting center usage should advance.
    pub record_ids: Vec<RecordId>,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

/// Correction-lineage request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorrectionRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Existing record being superseded.
    pub superseded: RecordId,
    /// Existing replacement record.
    pub superseding: RecordId,
    /// Evidence SHA-256 bound into lineage.
    pub evidence: String,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

/// Bounded explicit maintenance operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceKind {
    /// Apply at most 10,000 explicit homeostasis ticks.
    Advance {
        /// Number of ticks to apply.
        ticks: u32,
    },
    /// Transfer selected STM centers to LTM.
    Consolidate,
    /// Merge similar centers, then prune weak centers.
    MergePrune,
    /// Persist the current kernel without changing its mathematical state.
    Checkpoint,
    /// Reclaim SQLite free pages and truncate the WAL within the normal maintenance budget.
    Compact,
}

/// Idempotent maintenance request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceRequest {
    /// Namespace-local idempotency key.
    pub idempotency_key: String,
    /// Requested bounded maintenance effect.
    pub kind: MaintenanceKind,
    /// Remaining operation budget.
    pub deadline: Deadline,
}

/// Deterministic one-shot failure points used by transaction crash tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    /// Abort after candidate persistence calls but before SQLite commit.
    BeforeCommit,
    /// Commit durably, then omit publication to the immutable live view.
    AfterCommitBeforePublish,
    /// Publish the committed view, then lose the acknowledgement.
    AfterPublishBeforeAck,
    /// Abort while preparing a checkpoint, before SQLite commit.
    DuringCheckpoint,
    /// Stop after the durable deletion fence commit, before the sanitized rebuild publishes.
    AfterDeletionFenceCommit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CheckpointEnvelope {
    pub(crate) kernel: NcmKernel,
    pub(crate) state_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DurableReceipt {
    pub(crate) reply: EngineReply,
    pub(crate) operation: DurableOperation,
    pub(crate) state_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurableOperation {
    Observe {
        record_id: RecordId,
    },
    Feedback {
        record_ids: Vec<RecordId>,
    },
    Correction {
        superseded: RecordId,
        superseding: RecordId,
        evidence: String,
    },
    Maintenance {
        kind: MaintenanceKind,
    },
    DeletionFence {
        source: SourceId,
        target_epoch: u64,
        idempotency_key: String,
        payload_sha256: String,
        deleted_records: u64,
    },
    DeleteBySource {
        source: SourceId,
        target_epoch: u64,
        deleted_records: u64,
    },
}

pub(crate) struct NamespaceHandle {
    pub(crate) store: NamespaceStore,
    pub(crate) live: Arc<RwLock<Arc<NcmKernel>>>,
    pub(crate) commit_seq: CommitSeq,
    pub(crate) epoch: u64,
    pub(crate) fenced: bool,
    pub(crate) last_used: u64,
}

/// Transactional NCM engine with bounded resident namespace handles.
pub struct NcmEngine {
    pub(crate) root: StateRoot,
    pub(crate) encoder: Arc<dyn TextEncoder>,
    pub(crate) config: NcmConfig,
    pub(crate) namespaces: Mutex<BTreeMap<String, NamespaceHandle>>,
    pub(crate) use_clock: AtomicU64,
    pub(crate) fault_once: Mutex<Option<FaultPoint>>,
}
