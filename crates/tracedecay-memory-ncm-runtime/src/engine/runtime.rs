mod mutation;
mod observe;
mod recovery;
mod util;

pub(crate) use util::canonical_digest;

use super::*;
use crate::ports::EncoderIdentity;
use crate::store::{NamespaceStore, StoreIdentity, StoreMeta};
use recovery::recover_kernel;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use std::sync::{Arc, MutexGuard, RwLock};
use std::time::Instant;
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::projections::ProjectionBundle;
use tracedecay_memory_ncm_core::recall::{RecallOutput, RecallPolicy};
use tracedecay_memory_ncm_core::types::{ALGORITHM_PROFILE, AlgorithmIdentity};
use util::{
    catalog_count, encoder_payload, encoder_reply, read_live, ready_payload, remaining_deadline,
    store_reply, unavailable_recovery,
};

impl NcmEngine {
    /// Creates an engine. Namespace state remains lazy and no disk path is created.
    #[must_use]
    pub fn new(root: StateRoot, encoder: Arc<dyn TextEncoder>, config: NcmConfig) -> Self {
        Self {
            root,
            encoder,
            config,
            namespaces: Mutex::new(BTreeMap::new()),
            use_clock: AtomicU64::new(0),
            fault_once: Mutex::new(None),
        }
    }

    /// Arms one deterministic fault. It is consumed only when execution reaches that point.
    pub fn inject_fault_once(&self, point: FaultPoint) -> Result<(), String> {
        let mut fault = self
            .fault_once
            .lock()
            .map_err(|_| "engine fault mutex poisoned".to_owned())?;
        *fault = Some(point);
        Ok(())
    }

    /// Checks readiness without materializing an absent namespace.
    pub fn handshake(&self, namespace: &str) -> EngineReply {
        let prepared = match self.prepare_identity(namespace) {
            Ok(prepared) => prepared,
            Err(reply) => return reply,
        };
        if !NamespaceStore::exists(&self.root, namespace) {
            return EngineReply::new(Outcome::Success, 0, ready_payload(&prepared, 0, true));
        }
        let mut namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                return EngineReply::new(Outcome::Empty, 0, ready_payload(&prepared, 0, true));
            }
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        EngineReply::new(
            Outcome::Success,
            handle.commit_seq,
            ready_payload(handle.store.identity(), handle.epoch, false),
        )
    }

    /// Reports encoder identity, resident count, catalog count, and aggregate quota headroom.
    pub fn health(&self) -> EngineReply {
        let namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let mut source_headroom = 0_u64;
        let mut controlled_headroom = 0_u64;
        for handle in namespaces.values() {
            let usage = match handle.store.usage() {
                Ok(usage) => usage,
                Err(error) => return store_reply(error, handle.commit_seq),
            };
            let quota = handle.store.quota();
            source_headroom = source_headroom.saturating_add(
                quota
                    .source_basis_bytes
                    .saturating_sub(usage.source_basis_bytes()),
            );
            controlled_headroom = controlled_headroom.saturating_add(
                quota
                    .controlled_bytes
                    .saturating_sub(usage.physical_bytes()),
            );
        }
        let catalog = match catalog_count(&self.root) {
            Ok(count) => count,
            Err(reply) => return reply,
        };
        let identity: EncoderIdentity = self.encoder.identity();
        EngineReply::new(
            Outcome::Success,
            0,
            json!({
                "encoder": encoder_payload(&identity),
                "resident_namespaces": namespaces.len(),
                "resident_limit": MAX_RESIDENT_NAMESPACES,
                "catalog_namespaces": catalog,
                "catalog_limit": MAX_CATALOG_NAMESPACES,
                "quota_headroom": {
                    "source_basis_bytes": source_headroom,
                    "controlled_bytes": controlled_headroom
                }
            }),
        )
    }

    /// Encodes a query and recalls only from the published immutable view.
    pub fn recall(&self, namespace: &str, request: RecallRequest) -> EngineReply {
        let started = Instant::now();
        if request.deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, 0, Value::Null);
        }
        if request.top_k > MAX_TOP_K {
            return EngineReply::rejected(
                RejectReason::InvalidRequest("top_k exceeds 16".to_owned()),
                0,
            );
        }
        let (live, commit_seq) = {
            let mut namespaces = match self.namespace_lock() {
                Ok(namespaces) => namespaces,
                Err(reply) => return reply,
            };
            let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
                Ok(Some(handle)) => handle,
                Ok(None) => return EngineReply::new(Outcome::Empty, 0, Value::Null),
                Err(reply) => return reply,
            };
            if handle.fenced {
                return unavailable_recovery(handle.commit_seq);
            }
            let live = match read_live(handle) {
                Ok(live) => live,
                Err(reply) => return reply,
            };
            (live, handle.commit_seq)
        };
        let deadline = remaining_deadline(request.deadline, started);
        if deadline.remaining_ms == 0 {
            return EngineReply::new(Outcome::Cancelled, commit_seq, Value::Null);
        }
        let encoded = match self
            .encoder
            .encode(&[request.query_text.as_str()], deadline)
        {
            Ok(mut encoded) if encoded.len() == 1 => encoded.remove(0),
            Ok(_) => return EngineReply::new(Outcome::Corrupt, commit_seq, Value::Null),
            Err(error) => return encoder_reply(error, commit_seq),
        };
        let output = match live.recall(&encoded.0, request.top_k, RecallPolicy::default()) {
            Ok(output) => output,
            Err(error) => return util::core_reply(error, commit_seq),
        };
        match output {
            RecallOutput::Empty => EngineReply::new(Outcome::Empty, commit_seq, Value::Null),
            candidates @ RecallOutput::Candidates { .. } => {
                match serde_json::to_value(candidates) {
                    Ok(payload) => EngineReply::new(Outcome::Success, commit_seq, payload),
                    Err(error) => {
                        util::corrupt_reply(commit_seq, &format!("serialize recall: {error}"))
                    }
                }
            }
        }
    }

    /// Returns redacted counts, scheduler state, generation, epoch, and quota usage.
    pub fn inspection(&self, namespace: &str) -> EngineReply {
        let mut namespaces = match self.namespace_lock() {
            Ok(namespaces) => namespaces,
            Err(reply) => return reply,
        };
        let handle = match self.ensure_handle(&mut namespaces, namespace, false) {
            Ok(Some(handle)) => handle,
            Ok(None) => return EngineReply::new(Outcome::Empty, 0, Value::Null),
            Err(reply) => return reply,
        };
        if handle.fenced {
            return unavailable_recovery(handle.commit_seq);
        }
        let live = match read_live(handle) {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let stats = live.inspect();
        let usage = match handle.store.usage() {
            Ok(usage) => usage,
            Err(error) => return store_reply(error, handle.commit_seq),
        };
        let sources = live
            .records
            .iter()
            .map(|(_, record)| record.source.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        EngineReply::new(
            Outcome::Success,
            handle.commit_seq,
            json!({
                "stm_active": stats.stm_active,
                "ltm_active": stats.ltm_active,
                "records": stats.records,
                "sources": sources,
                "tick": stats.tick.0,
                "fatigue": stats.fatigue,
                "steps_since_consolidation": stats.steps_since_consolidation,
                "epoch": handle.epoch,
                "commit_seq": handle.commit_seq,
                "state_digest": util::sha256_hex(&live.state_digest()),
                "stm_terrain_digest": live.terrain.stm.digest(),
                "ltm_terrain_digest": live.terrain.ltm.digest(),
                "quota_usage": usage
            }),
        )
    }

    /// Deletes one admitted source by fencing and rebuilding the namespace from retained inputs.
    pub fn delete_by_source(
        &self,
        namespace: &str,
        source: &SourceId,
        idempotency_key: &str,
        deadline: Deadline,
    ) -> EngineReply {
        crate::privacy::delete_by_source(
            self,
            namespace,
            crate::privacy::DeleteRequest {
                idempotency_key: idempotency_key.to_owned(),
                source: source.clone(),
                deadline,
            },
        )
    }

    /// Returns the durable source revocation authority for snapshot sanitization.
    pub fn revoked_sources(&self, namespace: &str) -> Result<Vec<SourceId>, EngineReply> {
        crate::privacy::revoked_sources(self, namespace)
    }

    /// Task 016 extension point; snapshot export is deliberately unsupported here.
    pub fn snapshot_export(&self, _namespace: &str, _deadline: Deadline) -> EngineReply {
        match crate::snapshot::export(self, _namespace, _deadline) {
            Ok(snapshot) => {
                let state_generation = snapshot.state_generation();
                EngineReply::new(
                    Outcome::Success,
                    state_generation,
                    json!({"format": "ncm-snapshot.v1", "bytes": snapshot.into_vec()}),
                )
            }
            Err(reply) => reply,
        }
    }

    /// Task 016 extension point; snapshot restore is deliberately unsupported here.
    pub fn snapshot_restore(
        &self,
        _namespace: &str,
        _snapshot: &[u8],
        _idempotency_key: &str,
        _deadline: Deadline,
    ) -> EngineReply {
        crate::snapshot::restore(
            self,
            _namespace,
            crate::snapshot::RestoreRequest {
                idempotency_key: _idempotency_key.to_owned(),
                bytes: _snapshot.to_vec(),
            },
            _deadline,
        )
    }

    /// Contract v1 extension point; provider replay remains unsupported.
    pub fn replay(&self, _namespace: &str, _deadline: Deadline) -> EngineReply {
        EngineReply::new(Outcome::Unsupported, 0, Value::Null)
    }

    pub(crate) fn namespace_lock(
        &self,
    ) -> Result<MutexGuard<'_, BTreeMap<String, NamespaceHandle>>, EngineReply> {
        self.namespaces
            .lock()
            .map_err(|_| util::corrupt_reply(0, "namespace mutex poisoned"))
    }

    pub(crate) fn ensure_handle<'a>(
        &self,
        namespaces: &'a mut BTreeMap<String, NamespaceHandle>,
        namespace: &str,
        materialize: bool,
    ) -> Result<Option<&'a mut NamespaceHandle>, EngineReply> {
        if let Err(reason) = self.root.namespace_dir(namespace) {
            return Err(EngineReply::rejected(
                RejectReason::InvalidRequest(reason),
                0,
            ));
        }
        let use_id = self
            .use_clock
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if namespaces.contains_key(namespace) {
            if let Some(handle) = namespaces.get_mut(namespace) {
                handle.last_used = use_id;
                return Ok(Some(handle));
            }
            return Err(util::corrupt_reply(0, "resident namespace disappeared"));
        }
        let exists = NamespaceStore::exists(&self.root, namespace);
        if !exists && !materialize {
            return Ok(None);
        }
        if !exists && catalog_count(&self.root)? >= MAX_CATALOG_NAMESPACES {
            return Err(EngineReply::new(Outcome::BudgetExceeded, 0, Value::Null));
        }
        if namespaces.len() >= MAX_RESIDENT_NAMESPACES {
            let lru = namespaces
                .iter()
                .min_by_key(|(_, handle)| handle.last_used)
                .map(|(name, _)| name.clone());
            if let Some(lru) = lru {
                namespaces.remove(&lru);
            }
        }
        let prepared = self.prepare_identity(namespace)?;
        let (store, kernel, meta, fenced) = if exists {
            let mut store = NamespaceStore::open(&self.root, namespace, &prepared)
                .map_err(|error| store_reply(error, 0))?;
            if let Some(resumed) = crate::privacy::resume_pending_rebuild(&mut store, &self.config)?
            {
                (store, resumed.kernel, resumed.meta, false)
            } else {
                let fenced = store
                    .fenced()
                    .map_err(|error| store_reply(error, 0))?
                    .is_some();
                let meta = store.meta().map_err(|error| store_reply(error, 0))?;
                let kernel = recover_kernel(&store, &self.config, prepared.seed, &meta)?;
                (store, kernel, meta, fenced)
            }
        } else {
            let kernel = NcmKernel::new(prepared.seed, self.config.clone())
                .map_err(|error| util::core_reply(error, 0))?;
            let store = NamespaceStore::create(&self.root, namespace, prepared)
                .map_err(|error| store_reply(error, 0))?;
            (store, kernel, StoreMeta::default(), false)
        };
        namespaces.insert(
            namespace.to_owned(),
            NamespaceHandle {
                store,
                live: Arc::new(RwLock::new(Arc::new(kernel))),
                commit_seq: meta.commit_seq,
                epoch: meta.epoch,
                fenced,
                last_used: use_id,
            },
        );
        namespaces
            .get_mut(namespace)
            .map(Some)
            .ok_or_else(|| util::corrupt_reply(0, "resident namespace insertion failed"))
    }

    fn prepare_identity(&self, namespace: &str) -> Result<StoreIdentity, EngineReply> {
        if let Err(reason) = self.root.namespace_dir(namespace) {
            return Err(EngineReply::rejected(
                RejectReason::InvalidRequest(reason),
                0,
            ));
        }
        let seed = util::namespace_seed(namespace).ok_or_else(|| {
            EngineReply::rejected(
                RejectReason::InvalidRequest("namespace is not lowercase hexadecimal".to_owned()),
                0,
            )
        })?;
        let config_json = serde_json::to_string(&self.config)
            .map_err(|error| util::corrupt_reply(0, &format!("serialize config: {error}")))?;
        let config_sha256 = util::sha256_hex(config_json.as_bytes());
        let projections = ProjectionBundle::generate(seed);
        let projection_bytes = serde_json::to_vec(&projections)
            .map_err(|error| util::corrupt_reply(0, &format!("serialize projections: {error}")))?;
        let projection_sha256 = util::sha256_hex(&projection_bytes);
        let encoder = self.encoder.identity();
        Ok(StoreIdentity {
            algorithm: AlgorithmIdentity {
                profile: ALGORITHM_PROFILE.to_owned(),
                config_sha256,
            },
            projection_sha256,
            encoder_model: encoder.model,
            encoder_artifact_sha256: encoder.artifact_sha256,
            projection_bytes,
            seed,
            config_json,
        })
    }

    pub(crate) fn consume_fault(&self, point: FaultPoint) -> bool {
        let Ok(mut fault) = self.fault_once.lock() else {
            return false;
        };
        if *fault == Some(point) {
            *fault = None;
            true
        } else {
            false
        }
    }
}
