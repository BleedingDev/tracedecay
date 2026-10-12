//! Verified common source aliases used by existing storage and privacy paths.

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tracedecay_memory_ncm_core::types::SourceId;
use tracedecay_memory_provider_api::OwnedExactScope;

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
        if retained.get("source_identity").is_some() {
            return read_current(namespace, actual, declared, &retained).map(Some);
        }
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

/// Current delivery evidence binds the canonical record locator and exact
/// session namespace. Historical source authentication remains unavailable.
fn read_current(
    namespace: &str,
    actual: &SourceId,
    declared: Option<&Value>,
    retained: &Value,
) -> Result<SourceBinding, String> {
    let invalid = || "invalid current canonical source binding".to_owned();
    let identity = retained["source_identity"]
        .as_object()
        .ok_or_else(invalid)?;
    if identity.len() != 1 {
        return Err(invalid());
    }
    let observation_id = reference(&retained["source_identity"]["canonical_observation_id"])?;
    let canonical = &retained["canonical_payload"];
    if canonical["version"].as_u64() != Some(1)
        || retained["projection"]["observation_kind"] != "session.message_committed.v1"
    {
        return Err(invalid());
    }
    reference(&canonical["native_record_kind"])?;
    reference(&canonical["stable_record_id"])?;
    let provider = reference(&canonical["provider"])?;
    let session = reference(&canonical["relations"]["session_id"])?;
    let scope_value = &retained["delivery_scope"];
    if scope_value
        .as_object()
        .is_none_or(|fields| fields.len() != 7)
    {
        return Err(invalid());
    }
    let scope = OwnedExactScope::new(
        reference(&scope_value["profile_id"])?,
        reference(&scope_value["project_id"])?,
        reference(&scope_value["repository_identity"])?,
        reference(&scope_value["worktree_identity"])?,
        reference(&scope_value["branch_identity"])?,
        reference(&scope_value["agent_session_id"])?,
        reference(&scope_value["resolved_scope_digest"])?,
    )
    .map_err(|_| invalid())?;
    let source_ref = format!("record:{observation_id}");
    if retained["source_refs"] != json!([source_ref]) {
        return Err(invalid());
    }
    let key = scope.session_forget_source_key(session);
    let legacy = SourceId(opaque_id(namespace, b"forget-source-key", &key));
    let identity = serde_json::to_string(&json!([
        scope.profile_id,
        scope.project_id,
        provider,
        session,
        key,
    ]))
    .map_err(|error| error.to_string())?;
    let full = SourceId(opaque_id(namespace, b"common-source-key-v1", &identity));
    let declared: RetainedBinding =
        serde_json::from_value(declared.ok_or_else(invalid)?.clone()).map_err(|_| invalid())?;
    if declared.version != 1
        || declared.source_id != full
        || declared.legacy_source_id != legacy
        || actual != &full
    {
        return Err("retained source binding differs from current evidence".to_owned());
    }
    Ok(SourceBinding {
        full_source_id: Some(full),
        legacy_source_id: legacy,
        source_ref_digest: opaque_id(namespace, b"source_refs", &key),
        is_legacy: false,
    })
}

/// Returns the original source object authenticated by the common capsule.
/// Selection metadata lives outside that capsule and therefore must never be
/// treated as proof of the source or observation identity on its own.
pub(crate) fn authenticated_original_source(provenance: &Value) -> Result<Value, String> {
    let capsule = provenance
        .get("common_capsule")
        .ok_or_else(|| "source binding lacks retained original evidence".to_owned())?;
    let retained = decode_capsule(capsule)?
        .ok_or_else(|| "source binding lacks decoded original evidence".to_owned())?;
    retained
        .get("original_source")
        .cloned()
        .ok_or_else(|| "source binding lacks original attribution".to_owned())
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{authenticated_original_source, hex, opaque_id, read};
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tracedecay_memory_ncm_core::types::SourceId;
    use tracedecay_memory_provider_api::OwnedExactScope;

    fn current_delivery() -> (String, SourceId, Value, Value) {
        let namespace = "cd".repeat(32);
        let exact = OwnedExactScope::new(
            "profile",
            "project",
            "repository",
            "worktree",
            "master",
            "admitted-session",
            format!("sha256:{}", "ab".repeat(32)),
        )
        .unwrap();
        let key = exact.session_forget_source_key("canonical-session");
        let lineage = serde_json::to_string(&json!([
            exact.profile_id,
            exact.project_id,
            "claude",
            "canonical-session",
            key,
        ]))
        .unwrap();
        let actual = SourceId(opaque_id(&namespace, b"common-source-key-v1", &lineage));
        let binding = json!({"version":1,"source_id":actual.0,
            "legacy_source_id":opaque_id(&namespace, b"forget-source-key", &key)});
        let observation = format!("sha256:{}", "ef".repeat(32));
        let retained = json!({
            "source_identity":{"canonical_observation_id":observation},
            "canonical_payload":{"version":1,"provider":"claude","native_record_kind":"assistant",
                "stable_record_id":"native-record", "relations":{"session_id":"canonical-session"}},
            "source_refs":[format!("record:{observation}")],
            "delivery_scope":{"profile_id":exact.profile_id,"project_id":exact.project_id,
                "repository_identity":exact.repository_identity,"worktree_identity":exact.worktree_identity,
                "branch_identity":exact.branch_identity,"agent_session_id":exact.agent_session_id,
                "resolved_scope_digest":exact.resolved_scope_digest},
            "projection":{"observation_kind":"session.message_committed.v1","key_text":"assistant","value_text":"sanitized content"}
        });
        (namespace, actual, binding, retained)
    }

    fn capsule(binding: &Value, retained: &Value) -> Value {
        let bytes = serde_json::to_vec(retained).unwrap();
        json!({"source_binding":binding,"common_capsule":{"version":1,
            "sha256":hex(&Sha256::digest(&bytes)),"bytes":bytes}})
    }

    #[test]
    fn current_delivery_capsule_has_session_alias_without_original_authority() {
        let (namespace, actual, binding, retained) = current_delivery();
        let provenance = capsule(&binding, &retained);
        let resolved = read(&namespace, &actual, &provenance).unwrap().unwrap();
        assert_eq!(resolved.full_source_id.as_ref(), Some(&actual));
        assert_eq!(resolved.legacy_source_id.0, binding["legacy_source_id"]);
        assert!(!resolved.is_legacy);
        assert!(authenticated_original_source(&provenance).is_err());
    }

    #[test]
    fn current_delivery_capsule_denies_corruption_foreign_lineage_and_record() {
        let (namespace, actual, binding, retained) = current_delivery();
        let mut corrupt = capsule(&binding, &retained);
        corrupt["common_capsule"]["bytes"][0] = json!(0);
        assert!(read(&namespace, &actual, &corrupt).is_err());
        for (path, value) in [
            ("/canonical_payload/provider", json!("codex")),
            (
                "/canonical_payload/relations/session_id",
                json!("foreign-session"),
            ),
            ("/delivery_scope/profile_id", json!("foreign-profile")),
            ("/delivery_scope/project_id", json!("foreign-project")),
            (
                "/delivery_scope/worktree_identity",
                json!("foreign-worktree"),
            ),
            (
                "/source_identity/canonical_observation_id",
                json!("another-record"),
            ),
        ] {
            let mut changed = retained.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(
                read(&namespace, &actual, &capsule(&binding, &changed)).is_err(),
                "{path}"
            );
        }
        assert!(read(&"aa".repeat(32), &actual, &capsule(&binding, &retained)).is_err());
        let foreign_actual = SourceId("ff".repeat(32));
        assert!(read(&namespace, &foreign_actual, &capsule(&binding, &retained)).is_err());
    }
}
