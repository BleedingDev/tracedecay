//! Request-local admission over retained capsules. No host identity is decoded
//! here: source references and exclusions are namespace-bound opaque hashes.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_ncm_core::kernel::NcmKernel;
use tracedecay_memory_ncm_core::recall::RecallOutput;
use tracedecay_memory_ncm_core::records::RecordState;
use tracedecay_memory_ncm_core::types::{CoreError, RecordId};
use tracedecay_memory_provider_api::contract::{
    SourceDisposition, TemporalMode, UnknownValidityPolicy,
};
use tracedecay_memory_provider_api::{OwnedTemporalQuery, RecordedValidity, TemporalEligibility};

use super::util::validate_common_capsule;
use crate::engine::{EngineReply, Outcome};
use crate::store::{NamespaceStore, StoreError};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selection {
    mode: String,
    evaluation: i64,
    as_of: Option<i64>,
    start: Option<i64>,
    end: Option<i64>,
    include_superseded: bool,
    include_revoked: bool,
    unknown_policy: String,
    exclusions: BTreeMap<String, Vec<String>>,
    request_token: String,
    pub(super) maximum_candidates: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    valid_from: Option<i64>,
    valid_until: Option<i64>,
    superseded_at: Option<i64>,
    superseded_by: Option<String>,
    revoked_at: Option<i64>,
    source_refs: Vec<String>,
    observation_ids: Vec<String>,
    unknown_revision: bool,
    #[serde(default, rename = "source_identity_sha256")]
    _source_identity_sha256: Option<String>,
    #[serde(default, rename = "revision_digest")]
    _revision_digest: Option<String>,
    #[serde(default, rename = "observation_identity")]
    _observation_identity: Option<String>,
}

pub(super) struct Evidence {
    admitted: BTreeSet<RecordId>,
    provenance: BTreeMap<RecordId, Value>,
    stable: BTreeMap<RecordId, String>,
    candidates: BTreeMap<RecordId, String>,
    scanned: u64,
    excluded: u64,
    unknown: u64,
    score_upper_bound: f32,
}

impl Selection {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let result: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if result.maximum_candidates == 0
            || result.maximum_candidates > 16
            || !is_digest(&result.request_token)
        {
            return Err("invalid selected recall bound or request token".to_owned());
        }
        result.temporal()?;
        let names = [
            "stable_memory_refs",
            "candidate_ids",
            "source_refs",
            "trace_refs",
            "observation_ids",
            "content_sha256",
        ];
        if result.exclusions.len() != names.len()
            || names
                .iter()
                .any(|name| !result.exclusions.contains_key(*name))
        {
            return Err("invalid recall exclusion classes".to_owned());
        }
        for values in result.exclusions.values() {
            if values.len() > 1024
                || values.iter().collect::<BTreeSet<_>>().len() != values.len()
                || values.iter().any(|value| !is_digest(value))
            {
                return Err("invalid recall exclusion values".to_owned());
            }
        }
        Ok(result)
    }

    fn temporal(&self) -> Result<OwnedTemporalQuery, String> {
        let query = OwnedTemporalQuery {
            mode: TemporalMode::from_wire(&self.mode).ok_or("invalid temporal mode")?,
            evaluation_time_utc_nanos: self.evaluation,
            as_of_utc_nanos: self.as_of,
            interval_start_utc_nanos: self.start,
            interval_end_utc_nanos: self.end,
            include_superseded: self.include_superseded,
            include_revoked: self.include_revoked,
            unknown_validity_policy: UnknownValidityPolicy::from_wire(&self.unknown_policy)
                .ok_or("invalid unknown validity policy")?,
        };
        query.validate().map_err(|error| error.to_string())?;
        Ok(query)
    }

    pub(super) fn prepare(
        &self,
        namespace: &str,
        kernel: &NcmKernel,
        store: &NamespaceStore,
    ) -> Result<Evidence, StoreError> {
        let query = self.temporal().map_err(StoreError::InvalidInput)?;
        let score_upper_bound = [&kernel.stm, &kernel.ltm]
            .iter()
            .flat_map(|centers| centers.intensity.iter().zip(&centers.active))
            .filter_map(|(intensity, active)| active.then_some(*intensity))
            .fold(0.0_f32, f32::max);
        let mut evidence = Evidence {
            admitted: BTreeSet::new(),
            provenance: BTreeMap::new(),
            stable: BTreeMap::new(),
            candidates: BTreeMap::new(),
            scanned: 0,
            excluded: 0,
            unknown: 0,
            score_upper_bound,
        };
        let revoked = store
            .revocations()?
            .into_iter()
            .map(|revocation| revocation.source_id)
            .collect::<BTreeSet<_>>();
        for (id, record) in kernel.records.iter() {
            evidence.scanned += 1;
            if revoked.contains(&record.source)
                || matches!(record.state, RecordState::Deleted { .. })
            {
                evidence.excluded += 1;
                continue;
            }
            let capsule = store
                .capsule(*id)?
                .ok_or_else(|| StoreError::Corrupt("record capsule missing".to_owned()))?;
            let provenance: Value = serde_json::from_str(&capsule.provenance)
                .map_err(|error| StoreError::Corrupt(error.to_string()))?;
            validate_common_capsule(&provenance)?;
            let source_binding =
                crate::source_binding::read(namespace, &capsule.source_id, &provenance)
                    .map_err(StoreError::Corrupt)?;
            if source_binding.as_ref().is_some_and(|binding| {
                revoked.contains(&binding.legacy_source_id)
                    || binding
                        .full_source_id
                        .as_ref()
                        .is_some_and(|source| revoked.contains(source))
            }) {
                evidence.excluded += 1;
                continue;
            }
            if provenance
                .pointer("/control/feedback/suppressed")
                .and_then(Value::as_bool)
                == Some(true)
                || provenance
                    .pointer("/control/restricted")
                    .and_then(Value::as_bool)
                    == Some(true)
            {
                evidence.excluded += 1;
                continue;
            }
            let Some(metadata) = provenance
                .pointer("/control/selection")
                .or_else(|| provenance.get("selection"))
            else {
                evidence.unknown += 1;
                continue;
            };
            let metadata: Metadata = serde_json::from_value(metadata.clone())
                .map_err(|error| StoreError::Corrupt(error.to_string()))?;
            let Some(capsule_digest) = provenance
                .pointer("/common_capsule/sha256")
                .and_then(Value::as_str)
                .filter(|value| is_digest(value))
            else {
                evidence.unknown += 1;
                continue;
            };
            let validity = RecordedValidity {
                valid_from_utc_nanos: metadata.valid_from,
                valid_until_utc_nanos: metadata.valid_until,
                superseded_at_utc_nanos: metadata.superseded_at,
                superseded_by: metadata.superseded_by.clone(),
                revoked_at_utc_nanos: metadata.revoked_at,
            };
            let disposition = if matches!(record.state, RecordState::Superseded { .. }) {
                SourceDisposition::Superseded
            } else if metadata.revoked_at.is_some() {
                SourceDisposition::Revoked
            } else {
                SourceDisposition::Available
            };
            match validity
                .eligibility(&query, disposition, false)
                .map_err(|error| StoreError::Corrupt(error.to_string()))?
            {
                TemporalEligibility::Excluded => {
                    evidence.excluded += 1;
                    continue;
                }
                TemporalEligibility::WithheldUnknown => {
                    evidence.unknown += 1;
                    continue;
                }
                TemporalEligibility::IncludedUnknown => evidence.unknown += 1,
                TemporalEligibility::Eligible => {}
            }
            if metadata.unknown_revision {
                evidence.unknown += 1;
            }
            let stable = stable_reference(namespace, id.0, capsule_digest);
            let candidate = format!(
                "ncm-candidate:{}",
                digest_parts(&[self.request_token.as_bytes(), stable.as_bytes()])
            );
            let content_digest = hex(&Sha256::digest(record.value_text.as_bytes()));
            let excluded = [
                (
                    "stable_memory_refs",
                    opaque(namespace, b"stable_memory_refs", &stable),
                ),
                ("trace_refs", opaque(namespace, b"trace_refs", &stable)),
                (
                    "candidate_ids",
                    opaque(namespace, b"candidate_ids", &candidate),
                ),
                (
                    "content_sha256",
                    opaque(namespace, b"content_sha256", &content_digest),
                ),
            ]
            .iter()
            .any(|(name, value)| {
                self.exclusions
                    .get(*name)
                    .is_some_and(|items| items.contains(value))
            }) || source_binding.as_ref().is_some_and(|binding| {
                self.exclusions["source_refs"].contains(&binding.source_ref_digest)
            }) || metadata
                .source_refs
                .iter()
                .any(|value| self.exclusions["source_refs"].contains(value))
                || metadata
                    .observation_ids
                    .iter()
                    .any(|value| self.exclusions["observation_ids"].contains(value));
            if excluded {
                evidence.excluded += 1;
                continue;
            }
            evidence.admitted.insert(*id);
            evidence.provenance.insert(*id, provenance);
            evidence.stable.insert(*id, stable);
            evidence.candidates.insert(*id, candidate);
        }
        Ok(evidence)
    }
}

impl Evidence {
    pub(super) fn filtered_view(&self, kernel: &NcmKernel) -> Result<NcmKernel, CoreError> {
        let mut view = kernel.clone();
        for centers in [&kernel.stm, &kernel.ltm] {
            for (index, active) in centers.active.iter().enumerate() {
                if !active {
                    continue;
                }
                let slot = centers.slot(index).ok_or(CoreError::StaleHandle)?;
                let support = kernel
                    .support
                    .support_for(slot)?
                    .iter()
                    .copied()
                    .filter(|id| self.admitted.contains(id))
                    .collect::<Vec<_>>();
                view.support.set_support(slot, &support)?;
            }
        }
        Ok(view)
    }

    pub(super) fn reply(self, output: RecallOutput, generation: u64) -> EngineReply {
        let (candidates, truncated) = match output {
            RecallOutput::Empty => (Vec::new(), false),
            RecallOutput::Candidates {
                candidates,
                truncated,
                ..
            } => (candidates, truncated),
        };
        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let id = candidate.record_id;
            let Ok(mut row) = serde_json::to_value(candidate) else {
                return EngineReply::new(Outcome::Corrupt, generation, Value::Null);
            };
            row["provenance"] = self.provenance.get(&id).cloned().unwrap_or(Value::Null);
            row["stable_memory_ref"] = self
                .stable
                .get(&id)
                .map_or(Value::Null, |value| json!(value));
            row["candidate_id"] = self
                .candidates
                .get(&id)
                .map_or(Value::Null, |value| json!(value));
            rows.push(row);
        }
        EngineReply::new(
            if rows.is_empty() {
                Outcome::Empty
            } else {
                Outcome::Success
            },
            generation,
            json!({"common_recall": {"candidates": rows, "truncated": truncated,
                "scanned_items": self.scanned, "excluded_items": self.excluded, "unknown_items": self.unknown,
                "score_upper_bound": self.score_upper_bound}}),
        )
    }
}

pub(super) fn empty_payload() -> Value {
    json!({"common_recall": {"candidates": [], "truncated": false, "scanned_items": 0, "excluded_items": 0, "unknown_items": 0, "score_upper_bound": 0.0}})
}

pub(super) fn stable_reference(namespace: &str, record: u64, capsule_digest: &str) -> String {
    format!(
        "ncm-memory:{}",
        digest_parts(&[
            b"tracedecay.ncm.memory-reference.v1",
            namespace.as_bytes(),
            &record.to_be_bytes(),
            capsule_digest.as_bytes()
        ])
    )
}

fn opaque(namespace: &str, kind: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tracedecay.ncm.opaque-id.v1\0");
    for field in [namespace.as_bytes(), kind, value.as_bytes()] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    hex(&digest.finalize())
}

fn digest_parts(parts: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    for field in parts {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    hex(&digest.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default, clippy::unwrap_used)]
mod tests {
    use super::super::util::sha256_hex;
    use super::opaque;
    use crate::embedding::doubles::HashEncoder;
    use crate::engine::{NcmEngine, ObserveRequest, Outcome, RecallRequest};
    use crate::ports::{Deadline, StateRoot};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tracedecay_memory_ncm_core::types::{NcmConfig, RecordId, SourceId};

    #[test]
    fn selected_trace_exclusion_returns_next_candidate_before_top_k() {
        let tempdir = TempDir::new().unwrap();
        let namespace = format!("6e{}", "0".repeat(62));
        let deadline = Deadline {
            remaining_ms: u64::MAX,
        };
        let mut config = NcmConfig::default();
        config.terrain_resolution = 3;
        config.stm.n_centers = 8;
        config.stm.top_k_read = 8;
        config.stm.top_k_write = 8;
        config.ltm.n_centers = 8;
        config.ltm.top_k_read = 8;
        config.ltm.top_k_write = 8;
        config.hybrid_candidates = 8;
        let engine = NcmEngine::new(
            StateRoot::new(tempdir.path()).unwrap(),
            Arc::new(HashEncoder::new()),
            config,
        );
        for number in 1..=2 {
            let bytes = serde_json::to_vec(&json!({"retained_fixture":number})).unwrap();
            let mut request = ObserveRequest {
                idempotency_key: format!("trace-observe-{number}"),
                payload_sha256: String::new(),
                source: SourceId(format!("trace-source-{number}")),
                key_text: "trace exclusion retrieval".to_owned(),
                value_text: format!("retained trace answer {number}"),
                affect: None,
                surprise: 0.4,
                intensity: 1.0,
                provenance: json!({
                    "common_capsule":{"version":1,"sha256":sha256_hex(&bytes),"bytes":bytes},
                    "selection":{"valid_from":0,"valid_until":null,"superseded_at":null,
                        "superseded_by":null,"revoked_at":null,"source_refs":[],
                        "observation_ids":[],"unknown_revision":false}
                }),
                deadline,
            };
            request.payload_sha256 = request.canonical_payload_sha256().unwrap();
            let observed = engine.observe(&namespace, request);
            assert_eq!(observed.outcome, Outcome::Success, "{observed:?}");
        }
        let mut selection = json!({
            "mode":"current","evaluation":1,"as_of":null,"start":null,"end":null,
            "include_superseded":false,"include_revoked":false,"unknown_policy":"exclude",
            "maximum_candidates":2,"request_token":"a".repeat(64),
            "exclusions":{"stable_memory_refs":[],"candidate_ids":[],"source_refs":[],
                "trace_refs":[],"observation_ids":[],"content_sha256":[]}
        });
        let recall = |selection| {
            engine.recall_selected(
                &namespace,
                RecallRequest {
                    query_text: "trace exclusion retrieval".to_owned(),
                    top_k: 8,
                    deadline,
                },
                selection,
            )
        };
        let before = engine.inspection(&namespace);
        let ranked = recall(selection.clone());
        assert_eq!(ranked.outcome, Outcome::Success, "{ranked:?}");
        let candidates = ranked.payload["common_recall"]["candidates"]
            .as_array()
            .unwrap();
        assert_eq!(candidates.len(), 2);
        let first_trace = candidates[0]["stable_memory_ref"].as_str().unwrap();
        let expected_next = candidates[1]["stable_memory_ref"].clone();
        let trace = engine.common_control(
            &namespace,
            json!({"action":"inspection","expected_generation":ranked.state_generation,
                "view":"trace","stable_memory_ref":first_trace,
                "maximum_items":8,"maximum_bytes":65536,"after":0}),
            deadline,
        );
        assert_eq!(trace.outcome, Outcome::Success, "{trace:?}");
        assert_eq!(trace.payload["items"].as_array().unwrap().len(), 1);
        assert_eq!(trace.payload["items"][0]["stable_memory_ref"], first_trace);
        assert_eq!(
            trace.payload["items"][0]["content"],
            candidates[0]["value_text"]
        );

        selection["maximum_candidates"] = json!(1);
        selection["exclusions"]["trace_refs"] =
            json!([opaque(&namespace, b"trace_refs", first_trace)]);
        let excluded = recall(selection);
        assert_eq!(excluded.outcome, Outcome::Success, "{excluded:?}");
        assert_eq!(
            excluded.payload["common_recall"]["candidates"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            excluded.payload["common_recall"]["candidates"][0]["stable_memory_ref"],
            expected_next
        );
        assert_eq!(excluded.payload["common_recall"]["excluded_items"], 1);
        assert_eq!(engine.inspection(&namespace), before);
    }

    #[test]
    fn selected_read_rejects_corruption_before_suppression_or_unknown_metadata() {
        for suppressed in [false, true] {
            let tempdir = TempDir::new().unwrap();
            let namespace = format!("5d{}", "0".repeat(62));
            let deadline = Deadline {
                remaining_ms: u64::MAX,
            };
            let mut config = NcmConfig::default();
            config.terrain_resolution = 3;
            config.stm.n_centers = 8;
            config.stm.top_k_read = 8;
            config.stm.top_k_write = 8;
            config.ltm.n_centers = 8;
            config.ltm.top_k_read = 8;
            config.ltm.top_k_write = 8;
            config.hybrid_candidates = 8;
            let engine = NcmEngine::new(
                StateRoot::new(tempdir.path()).unwrap(),
                Arc::new(HashEncoder::new()),
                config,
            );
            let bytes = vec![255_u8, 0, 42];
            let mut request = ObserveRequest {
                idempotency_key: "integrity-observe".to_owned(),
                payload_sha256: String::new(),
                source: SourceId("source-integrity-observe".to_owned()),
                key_text: "integrity key".to_owned(),
                value_text: "retained answer".to_owned(),
                affect: None,
                surprise: 0.4,
                intensity: 1.0,
                provenance: json!({
                    "common_capsule": {
                        "version": 1,
                        "sha256": sha256_hex(&bytes),
                        "bytes": bytes
                    },
                    "control": {"feedback": {"suppressed": suppressed}}
                }),
                deadline,
            };
            request.payload_sha256 = request.canonical_payload_sha256().unwrap();
            let observed = engine.observe(&namespace, request);
            assert_eq!(observed.outcome, Outcome::Success, "{observed:?}");

            let published = {
                let mut namespaces = engine.namespace_lock().unwrap();
                let handle = namespaces.get_mut(&namespace).unwrap();
                let published = handle.live.read().unwrap().clone();
                let capsule = handle.store.capsule(RecordId(1)).unwrap().unwrap();
                let mut provenance: Value = serde_json::from_str(&capsule.provenance).unwrap();
                provenance["common_capsule"]["bytes"][2] = json!(43);
                // The resident store owns the exclusive connection. Corrupt its
                // retained bytes through that connection without republishing.
                let mut mutation = handle.store.begin_mutation().unwrap();
                mutation
                    .corrupt_capsule_provenance_for_test(RecordId(1), &provenance.to_string())
                    .unwrap();
                // Keep the cached generation aligned with this test store edit.
                handle.commit_seq = mutation.commit().unwrap();
                published
            };
            let before = engine.inspection(&namespace);
            assert_eq!(before.outcome, Outcome::Success);
            let reply = engine.recall_selected(
                &namespace,
                RecallRequest {
                    query_text: "integrity key".to_owned(),
                    top_k: 8,
                    deadline,
                },
                json!({
                    "mode": "current",
                    "evaluation": 0,
                    "as_of": null,
                    "start": null,
                    "end": null,
                    "include_superseded": false,
                    "include_revoked": false,
                    "unknown_policy": "exclude",
                    "maximum_candidates": 8,
                    "request_token": "a".repeat(64),
                    "exclusions": {
                        "stable_memory_refs": [],
                        "candidate_ids": [],
                        "source_refs": [],
                        "trace_refs": [],
                        "observation_ids": [],
                        "content_sha256": []
                    }
                }),
            );
            assert_eq!(reply.outcome, Outcome::Corrupt, "{reply:?}");
            assert_eq!(reply.state_generation, before.state_generation);
            assert_eq!(before, engine.inspection(&namespace));
            let namespaces = engine.namespace_lock().unwrap();
            let handle = namespaces.get(&namespace).unwrap();
            assert_eq!(
                handle.store.meta().unwrap().commit_seq,
                before.state_generation
            );
            assert!(Arc::ptr_eq(&published, &handle.live.read().unwrap()));
        }
    }
}
