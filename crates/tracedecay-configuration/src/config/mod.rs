//! Runtime pin surfaces and control-plane config helpers.
//!
//! Retrieval-profile evaluation stays in `tracedecay-usecases::config::retrieval`.
//! The re-export rows below are surfaces `tracedecay-global-db` and
//! `tracedecay-domain` already own, kept under the `crate::config::…`
//! spelling so call sites share one import path.

pub mod analyzer;
pub mod scope_control;
pub mod topology;
pub mod work_executable_binding;

pub use tracedecay_domain::configuration::{
    MemoryProviderRecallFallbackV1, MemoryProviderRecallRoutingV1, SEMANTIC_RUNTIME_SETTING_KEY,
};
pub use tracedecay_global_db::configuration::{registry, resolver};
#[cfg(test)]
pub use tracedecay_runtime_core::config::PinnedUserDataDir;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use tracedecay_domain::ProjectId;
use tracedecay_domain::configuration::{
    ConfigurationRevisionId, ConfigurationSnapshotV1, ConfigurationValueV1,
    DIAGNOSTICS_PREWARM_SETTING_KEY, INDEX_EXCLUDE_SETTING_KEY,
    INDEX_EXTRACT_DOCSTRINGS_SETTING_KEY, INDEX_GIT_IGNORE_SETTING_KEY, INDEX_INCLUDE_SETTING_KEY,
    INDEX_MAX_FILE_SIZE_SETTING_KEY, INDEX_NATIVE_GRAPH_ACTIVATION_SETTING_KEY,
    INDEX_TRACK_CALL_SITES_SETTING_KEY, MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
    MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY, SYNC_AUTO_TRACK_PR_BRANCHES_SETTING_KEY,
    SYNC_AUTO_TRACK_PR_POLL_SECS_SETTING_KEY, TELEMETRY_TIMINGS_SETTING_KEY,
};
use tracedecay_domain::errors::{Result, TraceDecayError};
use tracedecay_global_db::RegisteredGlobalDbLeaseV1;
use tracedecay_runtime_core::storage::StoreLayout;
use tracedecay_semantic_contracts::SemanticConfig;

#[derive(Debug, Clone, PartialEq)]
pub struct TraceDecayConfig {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub max_file_size: u64,
    pub extract_docstrings: bool,
    pub track_call_sites: bool,
    pub git_ignore: bool,
    pub diagnostics_prewarm: bool,
    pub native_graph_activation: bool,
    pub memory_provider_native_enabled: bool,
    pub memory_provider_recall_routing: MemoryProviderRecallRoutingV1,
    pub semantic: SemanticConfig,
    pub sync: SyncConfig,
    pub telemetry: TelemetryConfig,
}

impl Default for TraceDecayConfig {
    fn default() -> Self {
        Self {
            include: Vec::new(),
            exclude: Vec::new(),
            max_file_size: 1_048_576,
            extract_docstrings: true,
            track_call_sites: true,
            git_ignore: true,
            diagnostics_prewarm: false,
            native_graph_activation: true,
            memory_provider_native_enabled: false,
            memory_provider_recall_routing: MemoryProviderRecallRoutingV1::default(),
            semantic: SemanticConfig::default(),
            sync: SyncConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConfig {
    pub auto_track_pr_branches: bool,
    pub auto_track_pr_poll_secs: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            auto_track_pr_branches: false,
            auto_track_pr_poll_secs: 300,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    pub timings: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self { timings: true }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfigurationTarget {
    pub project_id: ProjectId,
    pub project_root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct PinnedRuntimeConfiguration {
    pub target: RuntimeConfigurationTarget,
    pub revision_id: ConfigurationRevisionId,
    pub snapshot: ConfigurationSnapshotV1,
    pub config: TraceDecayConfig,
}

impl PinnedRuntimeConfiguration {
    pub fn new(
        target: RuntimeConfigurationTarget,
        revision_id: ConfigurationRevisionId,
        snapshot: ConfigurationSnapshotV1,
    ) -> Result<Self> {
        let config = runtime_config_from_snapshot(&snapshot)?;
        Ok(Self {
            target,
            revision_id,
            snapshot,
            config,
        })
    }
}

pub type RuntimeConfigurationFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub struct OpenedRuntimeConfiguration {
    pub(crate) configuration: PinnedRuntimeConfiguration,
    pub(crate) registered_database: RegisteredGlobalDbLeaseV1,
}

impl OpenedRuntimeConfiguration {
    pub fn new(
        configuration: PinnedRuntimeConfiguration,
        registered_database: RegisteredGlobalDbLeaseV1,
    ) -> Self {
        Self {
            configuration,
            registered_database,
        }
    }
}

pub trait RuntimeConfigurationAuthorityPort: Send + Sync {
    fn open<'a>(
        &'a self,
        project_root: &'a Path,
        layout: &'a StoreLayout,
        database: RegisteredGlobalDbLeaseV1,
    ) -> RuntimeConfigurationFuture<'a, OpenedRuntimeConfiguration>;

    fn open_read_only<'a>(
        &'a self,
        project_root: &'a Path,
        layout: &'a StoreLayout,
        database: RegisteredGlobalDbLeaseV1,
    ) -> RuntimeConfigurationFuture<'a, OpenedRuntimeConfiguration>;

    fn resolve<'a>(
        &'a self,
        project_root: &'a Path,
        layout: &'a StoreLayout,
        database: RegisteredGlobalDbLeaseV1,
    ) -> RuntimeConfigurationFuture<'a, PinnedRuntimeConfiguration>;

    fn load_read_only<'a>(
        &'a self,
        project_root: &'a Path,
        layout: &'a StoreLayout,
        database: RegisteredGlobalDbLeaseV1,
    ) -> RuntimeConfigurationFuture<'a, PinnedRuntimeConfiguration>;
}

static RUNTIME_CONFIGURATION_AUTHORITY: OnceLock<Arc<dyn RuntimeConfigurationAuthorityPort>> =
    OnceLock::new();

pub fn install_runtime_configuration_authority(
    authority: Arc<dyn RuntimeConfigurationAuthorityPort>,
) -> Result<()> {
    RUNTIME_CONFIGURATION_AUTHORITY
        .set(authority)
        .map_err(|_| config_error("runtime configuration authority is already installed"))
}

pub async fn open_runtime_configuration_for_registered_database(
    project_root: &Path,
    layout: &StoreLayout,
    database: RegisteredGlobalDbLeaseV1,
) -> Result<OpenedRuntimeConfiguration> {
    runtime_configuration_authority()?
        .open(project_root, layout, database)
        .await
}

pub async fn open_runtime_configuration_for_registered_database_read_only(
    project_root: &Path,
    layout: &StoreLayout,
    database: RegisteredGlobalDbLeaseV1,
) -> Result<OpenedRuntimeConfiguration> {
    runtime_configuration_authority()?
        .open_read_only(project_root, layout, database)
        .await
}

/// Process-wide pin cache used by daemon invocation after project-open
/// publishes a snapshot. Distinct from [`RuntimeConfigurationAuthorityPort`],
/// which opens durable configuration from a registered store.
pub trait PinnedRuntimeConfigurationCachePort: Send + Sync {
    fn publish(&self, configuration: PinnedRuntimeConfiguration) -> Result<()>;

    fn cached_for_root(&self, project_root: &Path) -> Result<PinnedRuntimeConfiguration>;
}

static PINNED_RUNTIME_CONFIGURATION_CACHE: OnceLock<Arc<dyn PinnedRuntimeConfigurationCachePort>> =
    OnceLock::new();

pub fn install_pinned_runtime_configuration_cache(
    cache: Arc<dyn PinnedRuntimeConfigurationCachePort>,
) -> Result<()> {
    PINNED_RUNTIME_CONFIGURATION_CACHE
        .set(cache)
        .map_err(|_| config_error("pinned runtime configuration cache is already installed"))
}

fn pinned_runtime_configuration_cache() -> Result<&'static dyn PinnedRuntimeConfigurationCachePort>
{
    PINNED_RUNTIME_CONFIGURATION_CACHE
        .get()
        .map(Arc::as_ref)
        .ok_or_else(|| config_error("pinned runtime configuration cache is not installed"))
}

pub fn publish_pinned_runtime_configuration(
    configuration: PinnedRuntimeConfiguration,
) -> Result<()> {
    pinned_runtime_configuration_cache()?.publish(configuration)
}

pub fn cached_pinned_runtime_configuration(
    project_root: &Path,
) -> Result<PinnedRuntimeConfiguration> {
    pinned_runtime_configuration_cache()?.cached_for_root(project_root)
}

fn runtime_configuration_authority() -> Result<&'static dyn RuntimeConfigurationAuthorityPort> {
    RUNTIME_CONFIGURATION_AUTHORITY
        .get()
        .map(Arc::as_ref)
        .ok_or_else(|| config_error("runtime configuration authority is not installed"))
}

fn runtime_config_from_snapshot(snapshot: &ConfigurationSnapshotV1) -> Result<TraceDecayConfig> {
    Ok(TraceDecayConfig {
        include: required_string_list(snapshot, INDEX_INCLUDE_SETTING_KEY)?,
        exclude: required_string_list(snapshot, INDEX_EXCLUDE_SETTING_KEY)?,
        max_file_size: required_unsigned(snapshot, INDEX_MAX_FILE_SIZE_SETTING_KEY)?,
        extract_docstrings: required_bool(snapshot, INDEX_EXTRACT_DOCSTRINGS_SETTING_KEY)?,
        track_call_sites: required_bool(snapshot, INDEX_TRACK_CALL_SITES_SETTING_KEY)?,
        git_ignore: required_bool(snapshot, INDEX_GIT_IGNORE_SETTING_KEY)?,
        diagnostics_prewarm: required_bool(snapshot, DIAGNOSTICS_PREWARM_SETTING_KEY)?,
        native_graph_activation: required_bool(
            snapshot,
            INDEX_NATIVE_GRAPH_ACTIVATION_SETTING_KEY,
        )?,
        memory_provider_native_enabled: required_bool(
            snapshot,
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
        )?,
        memory_provider_recall_routing: memory_provider_recall_routing_from_snapshot(snapshot)?,
        semantic: SemanticConfig::default(),
        sync: SyncConfig {
            auto_track_pr_branches: required_bool(
                snapshot,
                SYNC_AUTO_TRACK_PR_BRANCHES_SETTING_KEY,
            )?,
            auto_track_pr_poll_secs: required_unsigned(
                snapshot,
                SYNC_AUTO_TRACK_PR_POLL_SECS_SETTING_KEY,
            )?,
        },
        telemetry: TelemetryConfig {
            timings: required_bool(snapshot, TELEMETRY_TIMINGS_SETTING_KEY)?,
        },
    })
}

fn required_setting<'a>(
    snapshot: &'a ConfigurationSnapshotV1,
    key: &str,
) -> Result<&'a ConfigurationValueV1> {
    let key = tracedecay_domain::configuration::SettingKey::new(key)
        .map_err(|error| config_error(format!("invalid runtime setting key: {error}")))?;
    snapshot.effective_values.get(&key).ok_or_else(|| {
        config_error(format!(
            "resolved configuration is missing '{}'",
            key.as_str()
        ))
    })
}

fn required_bool(snapshot: &ConfigurationSnapshotV1, key: &str) -> Result<bool> {
    match required_setting(snapshot, key)? {
        ConfigurationValueV1::Boolean(value) => Ok(*value),
        _ => Err(config_error(format!(
            "resolved configuration setting '{key}' is not boolean"
        ))),
    }
}

fn required_unsigned(snapshot: &ConfigurationSnapshotV1, key: &str) -> Result<u64> {
    match required_setting(snapshot, key)? {
        ConfigurationValueV1::Unsigned(value) => Ok(*value),
        _ => Err(config_error(format!(
            "resolved configuration setting '{key}' is not unsigned"
        ))),
    }
}

fn memory_provider_recall_routing_from_snapshot(
    snapshot: &ConfigurationSnapshotV1,
) -> Result<MemoryProviderRecallRoutingV1> {
    let routing: MemoryProviderRecallRoutingV1 =
        match required_setting(snapshot, MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY)? {
            ConfigurationValueV1::Text(value) => serde_json::from_str(value).map_err(|error| {
                config_error(format!(
                    "resolved configuration setting '{MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY}' is not a recall routing document: {error}"
                ))
            })?,
            _ => {
                return Err(config_error(format!(
                    "resolved configuration setting '{MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY}' is not text"
                )));
            }
        };
    routing.validate().map_err(|error| {
        config_error(format!(
            "resolved configuration setting '{MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY}' is invalid: {error}"
        ))
    })?;
    Ok(routing)
}

fn required_string_list(snapshot: &ConfigurationSnapshotV1, key: &str) -> Result<Vec<String>> {
    match required_setting(snapshot, key)? {
        ConfigurationValueV1::StringList(value) => Ok(value.clone()),
        _ => Err(config_error(format!(
            "resolved configuration setting '{key}' is not a string list"
        ))),
    }
}

fn config_error(message: impl Into<String>) -> TraceDecayError {
    TraceDecayError::Config {
        message: message.into(),
    }
}

#[cfg(test)]
mod memory_provider_snapshot_tests {
    use std::collections::BTreeMap;

    use tracedecay_domain::configuration::SettingKey;
    use tracedecay_global_db::configuration::registry::ConfigurationRegistry;
    use tracedecay_global_db::configuration::resolver::resolve_configuration;

    use super::*;

    fn setting_key(raw: &str) -> SettingKey {
        SettingKey::new(raw).expect("fixture setting key is canonical")
    }

    /// The snapshot the production resolver publishes with no operator layers.
    fn default_snapshot() -> ConfigurationSnapshotV1 {
        let registry = ConfigurationRegistry::core().expect("core registry");
        resolve_configuration(&registry, &[])
            .expect("default resolution")
            .snapshot
    }

    /// Rebuild the default snapshot with one setting replaced (`Some`) or
    /// removed from both the value and provenance maps (`None`).
    fn snapshot_with(raw_key: &str, value: Option<ConfigurationValueV1>) -> ConfigurationSnapshotV1 {
        let base = default_snapshot();
        let mut effective_values: BTreeMap<_, _> = base.effective_values.clone();
        let mut provenance: BTreeMap<_, _> = base.provenance.clone();
        let key = setting_key(raw_key);
        match value {
            Some(value) => {
                effective_values.insert(key, value);
            }
            None => {
                effective_values.remove(&key);
                provenance.remove(&key);
            }
        }
        ConfigurationSnapshotV1::new(effective_values, provenance).expect("fixture snapshot")
    }

    fn routing_text(routing: &MemoryProviderRecallRoutingV1) -> ConfigurationValueV1 {
        ConfigurationValueV1::Text(serde_json::to_string(routing).expect("routing encodes"))
    }

    #[test]
    fn both_memory_provider_settings_extract_from_the_resolved_snapshot() {
        // Stock configuration composes no provider and routes no recall.
        let stock = runtime_config_from_snapshot(&default_snapshot())
            .expect("default snapshot yields a runtime configuration");
        assert!(!stock.memory_provider_native_enabled);
        assert_eq!(
            stock.memory_provider_recall_routing,
            MemoryProviderRecallRoutingV1::default()
        );

        // An operator-pinned host boolean reaches the runtime configuration.
        let host_on = runtime_config_from_snapshot(&snapshot_with(
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            Some(ConfigurationValueV1::Boolean(true)),
        ))
        .expect("host-enabled snapshot yields a runtime configuration");
        assert!(host_on.memory_provider_native_enabled);

        // A complete, valid routing document reaches it verbatim.
        let routing = MemoryProviderRecallRoutingV1 {
            active_provider: Some("native".to_owned()),
            fallback: Some(MemoryProviderRecallFallbackV1 {
                policy_id: "policy.recall.fallback".to_owned(),
                policy_revision: 3,
                target_provider: "ncm".to_owned(),
            }),
        };
        let routed = runtime_config_from_snapshot(&snapshot_with(
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
            Some(routing_text(&routing)),
        ))
        .expect("routed snapshot yields a runtime configuration");
        assert_eq!(routed.memory_provider_recall_routing, routing);
    }

    #[test]
    fn missing_memory_provider_settings_fail_the_snapshot_read() {
        for key in [
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
        ] {
            let snapshot = snapshot_with(key, None);
            assert!(
                matches!(
                    runtime_config_from_snapshot(&snapshot),
                    Err(TraceDecayError::Config { .. })
                ),
                "missing {key} must fail project open"
            );
        }
    }

    #[test]
    fn wrong_typed_memory_provider_settings_fail_the_snapshot_read() {
        let mistyped_host = snapshot_with(
            MEMORY_PROVIDER_NATIVE_ENABLED_SETTING_KEY,
            Some(ConfigurationValueV1::Text("true".to_owned())),
        );
        assert!(matches!(
            runtime_config_from_snapshot(&mistyped_host),
            Err(TraceDecayError::Config { .. })
        ));

        let mistyped_routing = snapshot_with(
            MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
            Some(ConfigurationValueV1::Boolean(true)),
        );
        assert!(matches!(
            runtime_config_from_snapshot(&mistyped_routing),
            Err(TraceDecayError::Config { .. })
        ));
    }

    #[test]
    fn malformed_recall_routing_documents_fail_the_snapshot_read() {
        for document in [
            "{",
            "\"native\"",
            "{\"active_provider\": \"native\", \"unknown\": true}",
            "{\"active_provider\": 7}",
        ] {
            let snapshot = snapshot_with(
                MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
                Some(ConfigurationValueV1::Text(document.to_owned())),
            );
            assert!(
                matches!(
                    runtime_config_from_snapshot(&snapshot),
                    Err(TraceDecayError::Config { .. })
                ),
                "malformed routing document {document} must fail project open"
            );
        }
    }

    #[test]
    fn invalid_recall_routing_gates_fail_the_snapshot_read() {
        // A well-formed document that the routing gate itself rejects: a
        // fallback without an active provider, a zero fallback revision, a
        // fallback that targets the active provider, and a blank identity.
        let fallback_without_active = MemoryProviderRecallRoutingV1 {
            active_provider: None,
            fallback: Some(MemoryProviderRecallFallbackV1 {
                policy_id: "policy.recall.fallback".to_owned(),
                policy_revision: 1,
                target_provider: "ncm".to_owned(),
            }),
        };
        let zero_revision = MemoryProviderRecallRoutingV1 {
            active_provider: Some("native".to_owned()),
            fallback: Some(MemoryProviderRecallFallbackV1 {
                policy_id: "policy.recall.fallback".to_owned(),
                policy_revision: 0,
                target_provider: "ncm".to_owned(),
            }),
        };
        let fallback_targets_active = MemoryProviderRecallRoutingV1 {
            active_provider: Some("native".to_owned()),
            fallback: Some(MemoryProviderRecallFallbackV1 {
                policy_id: "policy.recall.fallback".to_owned(),
                policy_revision: 2,
                target_provider: "native".to_owned(),
            }),
        };
        let blank_active = MemoryProviderRecallRoutingV1 {
            active_provider: Some(" ".to_owned()),
            fallback: None,
        };

        for routing in [
            fallback_without_active,
            zero_revision,
            fallback_targets_active,
            blank_active,
        ] {
            let snapshot = snapshot_with(
                MEMORY_PROVIDER_RECALL_ROUTING_SETTING_KEY,
                Some(routing_text(&routing)),
            );
            assert!(
                matches!(
                    runtime_config_from_snapshot(&snapshot),
                    Err(TraceDecayError::Config { .. })
                ),
                "invalid routing gate {routing:?} must fail project open"
            );
        }
    }
}
