//! Verified common source aliases used by existing storage and privacy paths.

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tracedecay_memory_ncm_core::types::SourceId;

const OPAQUE_ID_DOMAIN: &[u8] = b"tracedecay.ncm.opaque-id.v1\0";
const MAX_CAPSULE_BYTES: usize = 131_072;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceBinding {
    pub(crate) full_source_id: Option<SourceId>,
    pub(crate) legacy_source_id: SourceId,
    pub(crate) source_ref_digest: String,
    pub(crate) is_legacy: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeletionSourceBinding {
    pub(crate) source_id: SourceId,
    pub(crate) legacy_source_id: SourceId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedBinding {
    version: u64,
    source_id: SourceId,
    legacy_source_id: SourceId,
}

impl DeletionSourceBinding {
    pub(crate) fn validate_set(bindings: &[Self], sources: &[SourceId]) -> Result<(), String> {
        if bindings.is_empty()
            || bindings.len() > 1024
            || bindings.len() != sources.len()
            || bindings.iter().any(|binding| {
                !is_digest(&binding.source_id.0)
                    || !is_digest(&binding.legacy_source_id.0)
                    || binding.source_id == binding.legacy_source_id
            })
            || bindings
                .iter()
                .map(|binding| &binding.source_id)
                .collect::<BTreeSet<_>>()
                != sources.iter().collect::<BTreeSet<_>>()
        {
            return Err("invalid claimed source bindings".to_owned());
        }
        Ok(())
    }
}

/// An absent binding denotes the existing raw lane. Common legacy capsules can
/// still identify their original lineage without changing their stored ID.
pub(crate) fn read(
    namespace: &str,
    actual: &SourceId,
    provenance: &Value,
) -> Result<Option<SourceBinding>, String> {
    let declared = provenance.get("source_binding");
    let Some(capsule) = provenance.get("common_capsule") else {
        return if declared.is_some() {
            Err("source binding lacks retained original evidence".to_owned())
        } else {
            Ok(None)
        };
    };
    let Some(retained) = decode_capsule(capsule)? else {
        return if declared.is_some() {
            Err("source binding lacks decoded original evidence".to_owned())
        } else {
            Ok(None)
        };
    };
    let Some(original) = retained.get("original_source") else {
        return if declared.is_some() {
            Err("source binding lacks original attribution".to_owned())
        } else {
            // Older raw clients used this opaque capsule without attribution.
            Ok(None)
        };
    };
    let source = &original["source"];
    let key = reference(&source["source_key"])?;
    let legacy = SourceId(opaque_id(namespace, b"forget-source-key", key));
    let full = if original["origin_scope"]["state"] == "recorded" {
        let scope = &original["origin_scope"]["exact_scope_identity"];
        let identity = serde_json::to_string(&json!([
            reference(&scope["profile_id"])?,
            reference(&scope["project_id"])?,
            reference(&source["canonical_provider_id"])?,
            reference(&source["canonical_session_id"])?,
            key,
        ]))
        .map_err(|error| error.to_string())?;
        Some(SourceId(opaque_id(
            namespace,
            b"common-source-key-v1",
            &identity,
        )))
    } else {
        None
    };
    if let Some(declared) = declared {
        let declared: RetainedBinding = serde_json::from_value(declared.clone())
            .map_err(|_| "invalid retained source binding".to_owned())?;
        if declared.version != 1
            || Some(&declared.source_id) != full.as_ref()
            || declared.legacy_source_id != legacy
            || &declared.source_id != actual
        {
            return Err("retained source binding differs from original evidence".to_owned());
        }
    } else if actual != &legacy {
        return Err("legacy source binding differs from original evidence".to_owned());
    }
    Ok(Some(SourceBinding {
        full_source_id: full,
        legacy_source_id: legacy,
        source_ref_digest: opaque_id(namespace, b"source_refs", key),
        is_legacy: declared.is_none(),
    }))
}

pub(crate) fn read_text(
    namespace: &str,
    actual: &SourceId,
    provenance: &str,
) -> Result<Option<SourceBinding>, String> {
    let provenance: Value =
        serde_json::from_str(provenance).map_err(|_| "invalid source provenance".to_owned())?;
    read(namespace, actual, &provenance)
}

pub(crate) fn matches_sources(
    namespace: &str,
    actual: &SourceId,
    provenance: &str,
    sources: &BTreeSet<SourceId>,
) -> Result<bool, String> {
    if sources.contains(actual) {
        return Ok(true);
    }
    let provenance: Value =
        serde_json::from_str(provenance).map_err(|_| "invalid source provenance".to_owned())?;
    if provenance.get("source_binding").is_none() {
        return Ok(false);
    }
    let binding = read(namespace, actual, &provenance)?;
    Ok(binding.is_some_and(|binding| sources.contains(&binding.legacy_source_id)))
}

fn decode_capsule(capsule: &Value) -> Result<Option<Value>, String> {
    let invalid = || "common capsule integrity mismatch".to_owned();
    if capsule["version"].as_u64() != Some(1) {
        return Err(invalid());
    }
    let values = capsule["bytes"].as_array().ok_or_else(invalid)?;
    if values.len() > MAX_CAPSULE_BYTES {
        return Err(invalid());
    }
    let bytes = values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .and_then(|byte| u8::try_from(byte).ok())
                .ok_or_else(invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if capsule["sha256"].as_str() != Some(hex(&Sha256::digest(&bytes)).as_str()) {
        return Err(invalid());
    }
    Ok(serde_json::from_slice(&bytes).ok())
}

fn reference(value: &Value) -> Result<&str, String> {
    value
        .as_str()
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 1024
                && value.trim() == *value
                && !value.chars().any(char::is_control)
        })
        .ok_or_else(|| "invalid original source identity".to_owned())
}

pub(crate) fn opaque_id(namespace: &str, kind: &[u8], value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(OPAQUE_ID_DOMAIN);
    for field in [namespace.as_bytes(), kind, value.as_bytes()] {
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
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
