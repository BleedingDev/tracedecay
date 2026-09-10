//! Shared trusted setup for real-provider conformance factories.
//!
//! This module owns no provider, storage implementation, process or runtime. Its
//! inventory is registered from the unchanged suite before a provider is opened.
//! Caller-supplied grants are checked against that inventory and a fixed fixture
//! policy; they never add sources or overwrite current disposition state.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracedecay_memory_provider_api::contract::SourceDisposition;
use tracedecay_memory_provider_api::{
    AdvisoryAdmissionAuthority, AdvisoryAdmissionError, CurrentAdvisoryAdmission,
    CurrentRestoreAdmission, CurrentSourceDisposition, GrantedHistorySource, OriginScopeEvidence,
    OriginalSourceIdentity, OwnedExactScope, OwnedProviderId, ProviderCall, ProviderOperation,
    RecordedValidity, RestoreDispositionCheckpoint, SourceAttribution,
};

use crate::compatibility::{CompatibilityScenario, CompatibilityStep, FixtureEnvironmentAction};

mod transport;
pub use transport::{
    DescriptorDto, HandshakeRequestDto, HandshakeResponseDto, MAX_TRANSPORT_JSON_BYTES,
    ProviderCallDto, ProviderReplyDto,
};

const MAX_SOURCES: usize = 4096;
const MAX_INVENTORY_BYTES: usize = 16 * 1024 * 1024;
const ADMISSION_REF: &str = "fixture.host.admitted-history";

/// Serializable trusted fixture state for a child restart. Deserialize only from
/// the owning test process; this is never a provider call or a grant payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureAuthoritySnapshot {
    base_scope: Value,
    observations: BTreeMap<String, Value>,
    dispositions: BTreeMap<String, String>,
    available: bool,
    revision: u64,
}

/// One fixture-owned authority shared across physical namespaces and processes.
#[derive(Clone, Debug)]
pub struct FixtureAuthority {
    state: Arc<Mutex<FixtureAuthoritySnapshot>>,
}

impl FixtureAuthority {
    /// Registers original evidence from actual observation bodies in the given
    /// scenario. No assertion, case name or provider identity affects admission.
    pub fn from_scenario(
        scenario: &CompatibilityScenario,
        scope: &OwnedExactScope,
    ) -> Result<Self, String> {
        scope.validate().map_err(err)?;
        let mut state = FixtureAuthoritySnapshot {
            base_scope: crate::compatibility::scope_json(scope),
            observations: BTreeMap::new(),
            dispositions: BTreeMap::new(),
            available: true,
            revision: 1,
        };
        for step in &scenario.steps {
            match step {
                CompatibilityStep::Call { fixture, .. } => {
                    let value: Value =
                        serde_json::from_slice(&fixture.payload.bytes).map_err(err)?;
                    match fixture.operation {
                        ProviderOperation::Observe => register(&mut state, &value)?,
                        ProviderOperation::Correction => {
                            if value["replacement"].get("source_identity").is_some() {
                                register(&mut state, &value["replacement"])?;
                            }
                        }
                        ProviderOperation::Replay => {
                            for row in array(&value["resolved_observations"])? {
                                register(&mut state, &row["observation"])?;
                            }
                        }
                        _ => {}
                    }
                }
                CompatibilityStep::Environment {
                    action: FixtureEnvironmentAction::InstallLegacyV1 { observations },
                    ..
                } => {
                    for observation in observations {
                        register(&mut state, observation)?;
                    }
                }
                _ => {}
            }
        }
        Self::from_snapshot(state)
    }

    /// Reopens a trusted owning-process snapshot without relabeling its sources.
    pub fn from_snapshot(snapshot: FixtureAuthoritySnapshot) -> Result<Self, String> {
        bounded_inventory(&snapshot)?;
        parse_scope(&snapshot.base_scope)?;
        if snapshot.revision == 0
            || snapshot.observations.len() > MAX_SOURCES
            || snapshot.dispositions.len() != snapshot.observations.len()
        {
            return Err("invalid fixture authority inventory bounds".into());
        }
        for (key, observation) in &snapshot.observations {
            let source = parse_attribution(&observation["source_identity"]["original_source"])?;
            verify_observation(observation)?;
            if source.source.source_key != *key {
                return Err("fixture inventory source key differs".into());
            }
            let state = snapshot
                .dispositions
                .get(key)
                .and_then(|s| SourceDisposition::from_wire(s))
                .ok_or("missing fixture disposition")?;
            if state == SourceDisposition::Unknown {
                return Err("unknown fixture disposition".into());
            }
        }
        Ok(Self {
            state: Arc::new(Mutex::new(snapshot)),
        })
    }

    /// Copies the independently owned current state for a real process restart.
    pub fn snapshot(&self) -> Result<FixtureAuthoritySnapshot, String> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| "fixture authority lock poisoned".into())
    }

    /// Changes the actual callback availability, independently of request JSON.
    pub fn set_available(&self, available: bool) -> Result<(), String> {
        self.state
            .lock()
            .map_err(|_| "fixture authority lock poisoned")?
            .available = available;
        Ok(())
    }

    /// Records a known source's actual current disposition and returns the
    /// authority-local journal reference. Unknown sources cannot be registered.
    pub fn record_disposition(
        &self,
        source_key: &str,
        disposition: SourceDisposition,
    ) -> Result<String, String> {
        if disposition == SourceDisposition::Unknown {
            return Err("unknown fixture disposition".into());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| "fixture authority lock poisoned")?;
        if !state.observations.contains_key(source_key) {
            return Err("unregistered fixture source".into());
        }
        state.revision = state
            .revision
            .checked_add(1)
            .ok_or("fixture authority revision overflow")?;
        state
            .dispositions
            .insert(source_key.into(), disposition.as_wire().into());
        Ok(format!(
            "fixture.host.disposition-journal.{}",
            state.revision
        ))
    }
}

impl AdvisoryAdmissionAuthority for FixtureAuthority {
    fn admit(
        &self,
        call: &ProviderCall,
    ) -> Result<CurrentAdvisoryAdmission, AdvisoryAdmissionError> {
        call.control
            .snapshot()
            .map_err(AdvisoryAdmissionError::Control)?;
        call.validate()?;
        let state = self
            .state
            .lock()
            .map_err(|_| AdvisoryAdmissionError::Unavailable("fixture authority lock"))?;
        if !state.available {
            return Err(AdvisoryAdmissionError::Unavailable("fixture authority"));
        }
        let result = resolve(&state, call).map_err(|_| {
            AdvisoryAdmissionError::Denied("fixture source or policy evidence differs")
        })?;
        CurrentAdvisoryAdmission::new(call, result.0, result.1)
    }
}

fn resolve(
    state: &FixtureAuthoritySnapshot,
    call: &ProviderCall,
) -> Result<(Vec<GrantedHistorySource>, Option<CurrentRestoreAdmission>), String> {
    let value: Value = serde_json::from_slice(&call.payload.bytes).map_err(err)?;
    let now = current_nanos()?;
    let mut requested = BTreeSet::new();
    let grant = value.get("history_grant").filter(|value| !value.is_null());
    if let Some(grant) = grant {
        if string(&grant["authorization_ref"])? != ADMISSION_REF
            || grant["policy_revision"].as_u64() != Some(1)
            || parse_scope(&grant["destination_scope"])? != call.exact_scope
        {
            return Err("fixture history policy differs".into());
        }
        let relation = string(&grant["relation"])?;
        let claims = array(&grant["sources"])?;
        if claims.is_empty() || claims.len() > MAX_SOURCES {
            return Err("fixture history source bounds".into());
        }
        for claim in claims {
            let attribution = &claim["attribution"];
            let source = parse_attribution(attribution)?;
            let registered = state
                .observations
                .get(&source.source.source_key)
                .ok_or("unregistered fixture history source")?;
            if registered["source_identity"]["original_source"] != *attribution {
                return Err("fixture attribution differs".into());
            }
            let origin = source.origin_scope.recorded_scope().map_err(err)?;
            if !match relation {
                "exact_scope" => origin == &call.exact_scope,
                "same_checkout" => same_checkout(origin, &call.exact_scope),
                _ => false,
            } {
                return Err("fixture history scope relation denied".into());
            }
            if !requested.insert(source.source.source_key) {
                return Err("duplicate fixture history source".into());
            }
        }
    }
    if call.operation == ProviderOperation::Replay {
        if grant.is_none() {
            return Err("replay has no admitted history relation".into());
        }
        let rows = array(&value["resolved_observations"])?;
        let mut replay_sources = BTreeSet::new();
        let mut receipts = BTreeSet::new();
        for row in rows {
            let observation = &row["observation"];
            let key = observation_key(observation)?;
            let registered = state
                .observations
                .get(key)
                .ok_or("unregistered replay source")?;
            verify_same_observation(registered, observation)?;
            let sequence = registered["source_sequence"]
                .as_u64()
                .ok_or("missing source sequence")?;
            let receipt = format!("fixture.host.observation-receipt.{sequence}");
            if string(&row["receipt_ref"])? != receipt
                || !receipts.insert(receipt)
                || !replay_sources.insert(key.to_owned())
            {
                return Err("fixture retained replay receipt differs".into());
            }
        }
        if replay_sources != requested {
            return Err("fixture replay coverage differs".into());
        }
    } else if call.operation != ProviderOperation::SnapshotRestore {
        for (key, observation) in &state.observations {
            let source = parse_attribution(&observation["source_identity"]["original_source"])?;
            if source.origin_scope.recorded_scope().map_err(err)? == &call.exact_scope {
                requested.insert(key.clone());
            }
        }
    }
    for observation in match call.operation {
        ProviderOperation::Observe => vec![&value],
        ProviderOperation::Correction if value["replacement"].get("source_identity").is_some() => {
            vec![&value["replacement"]]
        }
        _ => vec![],
    } {
        let key = observation_key(observation)?;
        let registered = state
            .observations
            .get(key)
            .ok_or("unregistered dispatched observation")?;
        verify_same_observation(registered, observation)?;
        if !requested.contains(key) {
            return Err("observation source scope denied".into());
        }
    }
    let restore = if call.operation == ProviderOperation::SnapshotRestore {
        let mut sources = Vec::new();
        let mut seen = BTreeSet::new();
        for raw in array(&value["snapshot"]["sources"])? {
            let source = parse_identity(raw)?;
            let registered = state
                .observations
                .get(&source.source_key)
                .ok_or("unregistered snapshot source")?;
            let attribution = parse_attribution(&registered["source_identity"]["original_source"])?;
            if attribution.source != source
                || !same_checkout(
                    attribution.origin_scope.recorded_scope().map_err(err)?,
                    &call.exact_scope,
                )
                || !seen.insert(source.source_key.clone())
            {
                return Err("fixture snapshot source differs".into());
            }
            requested.insert(source.source_key.clone());
            sources.push((
                source.clone(),
                current_disposition(state, &source.source_key, now)?,
            ));
        }
        Some(
            CurrentRestoreAdmission::new(
                RestoreDispositionCheckpoint {
                    exact_scope: call.exact_scope.clone(),
                    authority_ref: "fixture.host.current-disposition-checkpoint".into(),
                    authority_revision: Some(state.revision),
                    checked_at_utc_nanos: now,
                },
                sources,
            )
            .map_err(err)?,
        )
    } else {
        None
    };
    let history = requested
        .into_iter()
        .map(|key| {
            let observation = state
                .observations
                .get(&key)
                .ok_or("missing fixture source")?;
            Ok(GrantedHistorySource {
                attribution: parse_attribution(&observation["source_identity"]["original_source"])?,
                current_disposition: current_disposition(state, &key, now)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((history, restore))
}

fn current_disposition(
    state: &FixtureAuthoritySnapshot,
    key: &str,
    now: i64,
) -> Result<CurrentSourceDisposition, String> {
    Ok(CurrentSourceDisposition {
        state: state
            .dispositions
            .get(key)
            .and_then(|s| SourceDisposition::from_wire(s))
            .ok_or("missing fixture disposition")?,
        authority_ref: "fixture.host.current-source-disposition".into(),
        authority_revision: Some(state.revision),
        checked_at_utc_nanos: now,
    })
}

fn register(state: &mut FixtureAuthoritySnapshot, observation: &Value) -> Result<(), String> {
    verify_observation(observation)?;
    let key = observation_key(observation)?.to_owned();
    if let Some(existing) = state.observations.get(&key) {
        verify_same_observation(existing, observation)?;
    } else {
        if state.observations.len() >= MAX_SOURCES {
            return Err("fixture source inventory bound exceeded".into());
        }
        state.observations.insert(key.clone(), observation.clone());
        state.dispositions.insert(key, "available".into());
    }
    bounded_inventory(state)
}

fn verify_observation(observation: &Value) -> Result<(), String> {
    let original = parse_attribution(&observation["source_identity"]["original_source"])?;
    if original.source.content_sha256
        != crate::canonical_json_sha256(&observation["canonical_payload"]).map_err(err)?
        || observation["payload_sha256"].as_str() != Some(original.source.content_sha256.as_str())
        || observation["observation_id"].as_str() != Some(original.source.observation_id.as_str())
        || observation["source_sequence"].as_u64() != Some(original.source_sequence)
        || observation["source_identity"]["canonical_settlement_receipt"].as_str()
            != Some("fixture.host.settled-message")
    {
        return Err("fixture original observation evidence differs".into());
    }
    original.origin_scope.recorded_scope().map_err(err)?;
    Ok(())
}

fn verify_same_observation(expected: &Value, actual: &Value) -> Result<(), String> {
    for field in [
        "observation_id",
        "observation_kind",
        "payload_contract",
        "canonical_payload",
        "payload_sha256",
        "source_identity",
        "provenance",
        "privacy",
        "occurred_at",
        "admitted_at",
        "source_sequence",
    ] {
        if expected.get(field) != actual.get(field) {
            return Err(format!("fixture original observation {field} differs"));
        }
    }
    Ok(())
}

fn observation_key(observation: &Value) -> Result<&str, String> {
    string(&observation["source_identity"]["original_source"]["source"]["source_key"])
}
fn bounded_inventory(state: &FixtureAuthoritySnapshot) -> Result<(), String> {
    if serde_json::to_vec(state).map_err(err)?.len() > MAX_INVENTORY_BYTES {
        return Err("fixture inventory byte bound exceeded".into());
    }
    Ok(())
}
fn same_checkout(a: &OwnedExactScope, b: &OwnedExactScope) -> bool {
    a.profile_id == b.profile_id
        && a.project_id == b.project_id
        && a.repository_identity == b.repository_identity
        && a.worktree_identity == b.worktree_identity
        && a.branch_identity == b.branch_identity
        && a.resolved_scope_digest == b.resolved_scope_digest
}
fn current_nanos() -> Result<i64, String> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(err)?
            .as_nanos(),
    )
    .map_err(err)
}
fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn string(value: &Value) -> Result<&str, String> {
    value
        .as_str()
        .ok_or_else(|| "fixture text field missing".into())
}
fn array(value: &Value) -> Result<&Vec<Value>, String> {
    value
        .as_array()
        .ok_or_else(|| "fixture array field missing".into())
}
fn optional_string(value: &Value) -> Result<Option<String>, String> {
    if value.is_null() {
        Ok(None)
    } else {
        string(value).map(|s| Some(s.to_owned()))
    }
}
fn instant(value: &Value) -> Result<Option<i64>, String> {
    if value.is_null() {
        Ok(None)
    } else {
        crate::compatibility::utc_nanos(string(value)?)
            .map(Some)
            .ok_or_else(|| "fixture UTC timestamp invalid".into())
    }
}
fn parse_scope(value: &Value) -> Result<OwnedExactScope, String> {
    OwnedExactScope::new(
        string(&value["profile_id"])?,
        string(&value["project_id"])?,
        string(&value["repository_identity"])?,
        string(&value["worktree_identity"])?,
        string(&value["branch_identity"])?,
        string(&value["agent_session_id"])?,
        string(&value["resolved_scope_digest"])?,
    )
    .map_err(err)
}
fn parse_identity(value: &Value) -> Result<OriginalSourceIdentity, String> {
    let source = OriginalSourceIdentity {
        canonical_provider_id: OwnedProviderId::new(string(&value["canonical_provider_id"])?)
            .map_err(err)?,
        canonical_session_id: string(&value["canonical_session_id"])?.into(),
        source_key: string(&value["source_key"])?.into(),
        stable_record_id: optional_string(&value["stable_record_id"])?,
        observation_id: string(&value["observation_id"])?.into(),
        source_revision: optional_string(&value["source_revision"])?,
        content_sha256: string(&value["content_sha256"])?.into(),
    };
    source.validate().map_err(err)?;
    Ok(source)
}
fn parse_attribution(value: &Value) -> Result<SourceAttribution, String> {
    let origin = &value["origin_scope"];
    let validity = &value["validity"];
    let source = SourceAttribution {
        source: parse_identity(&value["source"])?,
        origin_scope: match string(&origin["state"])? {
            "recorded" => OriginScopeEvidence::Recorded {
                scope: parse_scope(&origin["exact_scope_identity"])?,
                authority_ref: string(&origin["authority_ref"])?.into(),
            },
            "ingestion_only" => OriginScopeEvidence::IngestionOnly,
            "unavailable" => OriginScopeEvidence::Unavailable,
            _ => return Err("fixture origin evidence state invalid".into()),
        },
        source_sequence: value["source_sequence"]
            .as_u64()
            .ok_or("fixture source sequence missing")?,
        occurred_at_utc_nanos: instant(&value["occurred_at"])?,
        ingested_at_utc_nanos: instant(&value["ingested_at"])?
            .ok_or("fixture ingestion timestamp missing")?,
        validity: RecordedValidity {
            valid_from_utc_nanos: instant(&validity["valid_from"])?,
            valid_until_utc_nanos: instant(&validity["valid_until"])?,
            superseded_at_utc_nanos: instant(&validity["superseded_at"])?,
            superseded_by: optional_string(&validity["superseded_by"])?,
            revoked_at_utc_nanos: instant(&validity["revoked_at"])?,
        },
    };
    source.validate().map_err(err)?;
    Ok(source)
}

#[cfg(test)]
mod tests;
