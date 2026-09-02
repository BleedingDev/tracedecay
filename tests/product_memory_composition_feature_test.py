#!/usr/bin/env python3
"""Focused tests for the dormant-by-default memory-provider host verifier.

Shape note: these fixtures used to model the pre-mount program -- one registry
feature edge, an enabled Native activation reachable only from a
`#[cfg(any(test, feature = "test-transport"))]` match arm, and no root-crate
consumer of the provider boundary.  Production has since mounted the
observation journey and the cognitive-recall route and resolves activation from
the authoritative runtime configuration.  The fixtures now model that shape,
and the negative cases below still pin every invariant the old ones did:
the host feature stays optional and outside the *transitive* `default`
closure, every host-support dependency stays optional and reachable only
through the host feature, default configuration stays dormant, production can
only pass `FromRuntimeConfiguration`, the pinned selector stays test-gated, the
enabled activation is built exactly once inside the resolved arm, and the two
exact mount files may consume the registry but may not compose, enable, name
the concrete Native adapter, or branch on a provider name.

The negative cases below are deliberately written as *bypasses* rather than as
deletions: a check that only notices a missing fragment is satisfied by a
violation that keeps every fragment and adds something.  So the suite moves the
enabled construction instead of deleting it, shadows the resolved activation
instead of removing the resolve call, inserts a resolver arm instead of
dropping one, relocates the boundary refusal into `#[cfg(test)]` instead of
erasing it, and hides an indented production item after a test module.
"""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/product/check-memory-composition-feature.py"


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("memory_composition_checker", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load memory composition checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER = load_checker()

VALID_MANIFEST = '''[features]
default = ["production"]
production = ["token-counting"]
token-counting = []
memory-provider-host = [
    "dep:tracedecay-memory-provider-registry",
    "dep:tracedecay-memory-observation",
    "dep:tracedecay-memory-hygiene",
]

[dependencies]
tracedecay-memory-provider-registry = { path = "../tracedecay-memory-provider-registry", optional = true }
tracedecay-memory-observation = { path = "../tracedecay-memory-observation", optional = true }
tracedecay-memory-hygiene = { path = "../tracedecay-memory-hygiene", optional = true }
'''

VALID_CONFIG = '''pub struct TraceDecayConfig {
    #[serde(default)]
    pub memory_provider_native_enabled: bool,
    #[serde(default)]
    pub memory_provider_recall_routing: MemoryProviderRecallRoutingV1,
}

impl Default for TraceDecayConfig {
    fn default() -> Self {
        Self {
            memory_provider_native_enabled: false,
            memory_provider_recall_routing: MemoryProviderRecallRoutingV1::default(),
        }
    }
}
'''

VALID_ROUTING_GATE = '''#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryProviderRecallRoutingV1 {
    #[serde(default)]
    pub active_provider: Option<String>,
    #[serde(default)]
    pub fallback: Option<MemoryProviderRecallFallbackV1>,
}
'''

VALID_MOUNT = '''#[cfg(feature = "memory-provider-host")]
pub(super) enum ProjectMemoryProviderActivation {
    Disabled,
    NativeObserver,
    NativeActive,
}

pub(super) enum ProjectMemoryProviderActivationSelector {
    FromRuntimeConfiguration,
    /// Pin one activation explicitly. Test and transport builds only.
    #[cfg(any(test, feature = "test-transport"))]
    Pinned(ProjectMemoryProviderActivation),
}

impl ProjectMemoryProviderActivationSelector {
    fn resolve(
        self,
        runtime_configuration: &PinnedRuntimeConfiguration,
    ) -> Result<ProjectMemoryProviderActivation> {
        match self {
            Self::FromRuntimeConfiguration => {
                resolve_memory_provider_activation(&runtime_configuration.config)
            }
            #[cfg(any(test, feature = "test-transport"))]
            Self::Pinned(activation) => Ok(activation),
        }
    }
}

fn resolve_memory_provider_activation(
    config: &TraceDecayConfig,
) -> Result<ProjectMemoryProviderActivation> {
    let routing = &config.memory_provider_recall_routing;
    match (
        config.memory_provider_native_enabled,
        routing.active_provider.as_deref(),
    ) {
        (false, None) => Ok(ProjectMemoryProviderActivation::Disabled),
        (false, Some(provider)) => Err(TraceDecayError::Config {
            message: format!("routing names '{provider}' while the host is disabled"),
        }),
        (true, None) => Ok(ProjectMemoryProviderActivation::NativeObserver),
        (true, Some(provider)) if is_mountable_active_provider(provider) => {
            Ok(ProjectMemoryProviderActivation::NativeActive)
        }
        (true, Some(provider)) => Err(TraceDecayError::Config {
            message: format!("routing names unmountable provider '{provider}'"),
        }),
    }
}

#[cfg(feature = "memory-provider-host")]
fn mount_project_memory_provider_host(
    activation: ProjectMemoryProviderActivation,
) -> Result<crate::mcp::server::MemoryProviderHostMount> {
    let enabled_mode = match activation {
        ProjectMemoryProviderActivation::Disabled => None,
        ProjectMemoryProviderActivation::NativeObserver => Some(EnabledProviderMode::Observer),
        ProjectMemoryProviderActivation::NativeActive => Some(EnabledProviderMode::Active),
    };
    let activation = match enabled_mode {
        None => tracedecay_memory_provider_registry::NativeProviderActivation::Disabled,
        Some(mode) => {
            tracedecay_memory_provider_registry::NativeProviderActivation::Enabled { port, mode }
        }
    };
    let composition =
        tracedecay_memory_provider_registry::ProjectMemoryProviderComposition::compose(activation)
            .map_err(|error| TraceDecayError::Config {
                message: format!("could not compose project memory-provider host: {error}"),
            })?;
    Ok(Arc::new(composition))
}

pub(super) async fn production_project_server() -> Result<()> {
    production_project_server_with_activation(
        ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,
    )
    .await
}

pub(super) async fn production_project_server_with_activation(
    activation: ProjectMemoryProviderActivationSelector,
) -> Result<()> {
    Box::pin(production_project_server_inner(activation)).await
}

async fn production_project_server_inner(
    activation: ProjectMemoryProviderActivationSelector,
) -> Result<()> {
    let memory_provider_activation = activation.resolve(&runtime_configuration)?;
    let memory_provider_host_mount = mount_project_memory_provider_host(
        memory_provider_activation,
    )?;
    let cognitive_recall_mount = match (
        memory_provider_host_mount.registry().is_some(),
        project_recall_routing_policy(memory_provider_activation, &runtime_configuration.config)?,
    ) {
        (true, Some(routing)) => Some(mount_project_cognitive_recall(routing)?),
        _ => None,
    };
    let observation_journey_mount = if memory_provider_host_mount.registry().is_some() {
        Some(mount_and_replay().await?)
    } else {
        None
    };
    Ok(())
}
'''

VALID_ACTIVATION_HARNESS = '''#[cfg(any(test, feature = "test-transport"))]
#[doc(hidden)]
pub async fn open_with_native_provider_for_test() {
    open_with_live_profile_root(
        ProjectMemoryProviderActivationSelector::Pinned(
            ProjectMemoryProviderActivation::NativeActive,
        ),
    );
}
'''

VALID_RETENTION = '''#[cfg(feature = "memory-provider-host")]
pub(crate) type MemoryProviderHostMount =
    Arc<tracedecay_memory_provider_registry::ProjectMemoryProviderComposition>;
'''

VALID_RETAINED_OWNER = '''#[cfg(feature = "memory-provider-host")]
pub(crate) mod cognitive_recall;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_provider;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_provider_parity_tests.rs"]
mod native_provider_parity_tests;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod observation_journey;
'''

VALID_NATIVE_ADAPTER = '''use tracedecay_memory_provider_native::NativeProvider;
use tracedecay_memory_provider_registry::NativeMemoryApplicationPort;
'''

VALID_NATIVE_PROVIDER = (
    VALID_NATIVE_ADAPTER
    + '#[cfg(test)]\n'
    + '#[path = "native_baseline_tests.rs"]\n'
    + "mod baseline_tests;\n"
    + '#[cfg(test)]\n'
    + '#[path = "native_provider_tests.rs"]\n'
    + "mod tests;\n"
)

# The mounted cognitive-recall route: consumes an already-composed registry,
# refuses a disabled composition, and composes nothing itself.  Its enabled
# composition appears only inside the trailing `#[cfg(test)] mod tests`.
VALID_COGNITIVE_RECALL = '''use tracedecay_memory_provider_registry::{
    ActiveRoutingPolicy, ProjectMemoryProviderComposition,
};

pub(crate) fn mount_project_cognitive_recall(
    inputs: CognitiveRecallMountInputsV1,
) -> Result<Arc<ProjectCognitiveRecallMountV1>, CognitiveRecallMountError> {
    inputs
        .composition
        .registry()
        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;
    Ok(Arc::new(ProjectCognitiveRecallMountV1 {
        composition: inputs.composition,
    }))
}

#[cfg(test)]
mod tests {
    use tracedecay_memory_provider_registry::NativeProviderActivation;

    fn composition() -> Arc<ProjectMemoryProviderComposition> {
        Arc::new(
            ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
                port,
            })
            .expect("compose"),
        )
    }
}
'''

# The mounted observation journey: the only root-crate file allowed to name the
# durable journal and the hygiene pipeline.
VALID_OBSERVATION_JOURNEY = '''use tracedecay_memory_hygiene::SanitizationDisposition;
use tracedecay_memory_observation::SqliteObservationJournal;
use tracedecay_memory_provider_registry::{
    NATIVE_PROVIDER_ID, OwnedProviderId, ProjectMemoryProviderComposition,
};

pub(crate) fn mount_project_observation_journey(
    inputs: ObservationJourneyMountInputsV1,
) -> Result<Arc<ProjectObservationJourneyV1>, ObservationJourneyError> {
    inputs
        .composition
        .registry()
        .ok_or(ObservationJourneyError::CompositionDisabled)?;
    let provider_id = OwnedProviderId::new(NATIVE_PROVIDER_ID)
        .map_err(ObservationJourneyError::Contract)?;
    Ok(Arc::new(ProjectObservationJourneyV1 {
        composition: inputs.composition,
        provider_id,
    }))
}

#[cfg(test)]
mod tests {
    use tracedecay_memory_provider_registry::NativeProviderActivation;

    fn composition() -> Arc<ProjectMemoryProviderComposition> {
        Arc::new(
            ProjectMemoryProviderComposition::compose(NativeProviderActivation::Enabled {
                port,
            })
            .expect("compose"),
        )
    }
}
'''

NATIVE_ADAPTER_PATHS = (
    Path("crates/tracedecay/src/daemon/retained_owner/native_provider.rs"),
    Path("crates/tracedecay/src/daemon/retained_owner/native_provider_tests.rs"),
    Path(
        "crates/tracedecay/src/daemon/retained_owner/native_provider_parity_tests.rs"
    ),
    Path("crates/tracedecay/src/daemon/retained_owner/native_baseline_tests.rs"),
)
COGNITIVE_RECALL_PATH = Path(
    "crates/tracedecay/src/daemon/retained_owner/cognitive_recall.rs"
)
OBSERVATION_JOURNEY_PATH = Path(
    "crates/tracedecay/src/daemon/retained_owner/observation_journey.rs"
)
RETAINED_OWNER_PATH = Path("crates/tracedecay/src/daemon/retained_owner.rs")


class MemoryCompositionFeatureTest(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        repo = Path(directory.name)
        manifest = repo / "crates/tracedecay/Cargo.toml"
        mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
        retention = repo / "crates/tracedecay/src/mcp/server/construction.rs"
        harness = repo / "crates/tracedecay/src/daemon/production_harness.rs"
        config = repo / "crates/tracedecay/src/config.rs"
        routing_gate = repo / "crates/tracedecay-domain/src/configuration.rs"
        manifest.parent.mkdir(parents=True)
        mount.parent.mkdir(parents=True)
        retention.parent.mkdir(parents=True)
        routing_gate.parent.mkdir(parents=True)
        manifest.write_text(VALID_MANIFEST, encoding="utf-8")
        mount.write_text(VALID_MOUNT, encoding="utf-8")
        harness.write_text(VALID_ACTIVATION_HARNESS, encoding="utf-8")
        retention.write_text(VALID_RETENTION, encoding="utf-8")
        config.write_text(VALID_CONFIG, encoding="utf-8")
        routing_gate.write_text(VALID_ROUTING_GATE, encoding="utf-8")
        (repo / "crates/tracedecay/src/lib.rs").write_text(
            "pub mod stable;\n", encoding="utf-8"
        )
        return directory, repo

    def write_feature_gated_native_adapters(self, repo: Path) -> None:
        retained_owner = repo / RETAINED_OWNER_PATH
        retained_owner.write_text(VALID_RETAINED_OWNER, encoding="utf-8")
        for relative in NATIVE_ADAPTER_PATHS:
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                VALID_NATIVE_PROVIDER
                if relative == NATIVE_ADAPTER_PATHS[0]
                else VALID_NATIVE_ADAPTER,
                encoding="utf-8",
            )

    def write_provider_boundary_mounts(self, repo: Path) -> None:
        retained_owner = repo / RETAINED_OWNER_PATH
        retained_owner.parent.mkdir(parents=True, exist_ok=True)
        retained_owner.write_text(VALID_RETAINED_OWNER, encoding="utf-8")
        for relative, source in (
            (COGNITIVE_RECALL_PATH, VALID_COGNITIVE_RECALL),
            (OBSERVATION_JOURNEY_PATH, VALID_OBSERVATION_JOURNEY),
        ):
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")

    def test_valid_feature_and_mount_pass(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_default_feature_activation_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'default = ["production"]',
                    'default = ["production", "memory-provider-host"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("default features must remain exactly" in error for error in errors)
            )

    def test_non_optional_registry_dependency_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'tracedecay-memory-provider-registry = { path = '
                    '"../tracedecay-memory-provider-registry", optional = true }',
                    'tracedecay-memory-provider-registry = { path = '
                    '"../tracedecay-memory-provider-registry" }',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "dependency tracedecay-memory-provider-registry must be optional"
                    in error
                    for error in errors
                )
            )

    def test_non_optional_support_dependency_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'tracedecay-memory-observation = { path = '
                    '"../tracedecay-memory-observation", optional = true }',
                    'tracedecay-memory-observation = { path = '
                    '"../tracedecay-memory-observation" }',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "dependency tracedecay-memory-observation must be optional" in error
                    for error in errors
                )
            )

    def test_extra_host_feature_edge_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    '    "dep:tracedecay-memory-hygiene",\n',
                    '    "dep:tracedecay-memory-hygiene",\n    "semantic-fastembed",\n',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "feature memory-provider-host must contain exactly" in error
                    for error in errors
                )
            )

    def test_production_reaching_support_dependency_directly_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'production = ["token-counting"]',
                    'production = ["token-counting", '
                    '"dep:tracedecay-memory-hygiene"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must reach tracedecay-memory-hygiene only through" in error
                    for error in errors
                )
            )

    def test_default_configuration_must_keep_host_dormant(self) -> None:
        directory, repo = self.fixture()
        with directory:
            config = repo / "crates/tracedecay/src/config.rs"
            config.write_text(
                VALID_CONFIG.replace(
                    "memory_provider_native_enabled: false,",
                    "memory_provider_native_enabled: true,",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must default the provider host to dormant" in error
                    for error in errors
                )
            )

    def test_routing_gate_must_default_to_no_active_provider(self) -> None:
        directory, repo = self.fixture()
        with directory:
            routing_gate = repo / "crates/tracedecay-domain/src/configuration.rs"
            routing_gate.write_text(
                VALID_ROUTING_GATE.replace(
                    "    #[serde(default)]\n    pub active_provider: Option<String>,",
                    "    pub active_provider: String,",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must default to no active provider" in error for error in errors
                )
            )

    def test_concrete_adapter_in_mount_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT + "use tracedecay_memory_provider_native::NativeProvider;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("NativeProvider" in error for error in errors))

    def test_activation_seam_does_not_trip_adapter_heuristic(self) -> None:
        directory, repo = self.fixture()
        with directory:
            errors = CHECKER.check_repository(repo)
            self.assertFalse(
                any("concrete Native adapter" in error for error in errors)
            )

    def test_pinned_selector_variant_must_stay_test_gated(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    '    #[cfg(any(test, feature = "test-transport"))]\n'
                    "    Pinned(ProjectMemoryProviderActivation),",
                    "    Pinned(ProjectMemoryProviderActivation),",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "missing exact gating fragment" in error
                    and "Pinned(ProjectMemoryProviderActivation)," in error
                    for error in errors
                )
            )

    def test_production_entry_cannot_pin_an_activation(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,\n"
                    "    )",
                    "ProjectMemoryProviderActivationSelector::Pinned(\n"
                    "            ProjectMemoryProviderActivation::NativeActive,\n"
                    "        ),\n    )",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must not pin an activation selector" in error for error in errors)
            )

    def test_enabled_activation_outside_resolved_arm_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                "fn eager() {\n"
                "    let _ = tracedecay_memory_provider_registry::"
                "NativeProviderActivation::Enabled { port };\n"
                "}\n" + VALID_MOUNT,
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must be constructed exactly once in production" in error
                    for error in errors
                )
            )

    def test_resolver_must_refuse_routing_while_host_disabled(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "        (false, Some(provider)) => Err(TraceDecayError::Config {\n"
                    "            message: format!(\"routing names '{provider}' while "
                    'the host is disabled"),\n'
                    "        }),",
                    "        (false, Some(_provider)) => "
                    "Ok(ProjectMemoryProviderActivation::NativeObserver),",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "missing or duplicated exact arm" in error
                    and "(false, Some(provider)) => Err(TraceDecayError::Config {"
                    in error
                    for error in errors
                )
            )

    def test_resolver_must_not_promote_enabled_host_to_active(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "(true, None) => Ok(ProjectMemoryProviderActivation::NativeObserver),",
                    "(true, None) => Ok(ProjectMemoryProviderActivation::NativeActive),",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "missing or duplicated exact arm" in error
                    and "NativeObserver" in error
                    for error in errors
                )
            )

    def test_native_active_call_outside_gated_harness_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/eager.rs"
            leaked.write_text(
                "fn eager() { activate(ProjectMemoryProviderActivation::NativeActive); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("activation leaked" in error for error in errors))

    def test_pinned_selector_outside_gated_harness_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/eager.rs"
            leaked.write_text(
                "fn eager() { open(ProjectMemoryProviderActivationSelector::Pinned(mode)); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "pinned activation selector leaked outside" in error
                    for error in errors
                )
            )

    def test_feature_gated_native_adapter_allowlist_passes(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_native_adapter_lookalikes_still_fail(self) -> None:
        directory, repo = self.fixture()
        with directory:
            lookalikes = (
                Path(
                    "crates/tracedecay/src/daemon/retained_owner/"
                    "native_provider_copy.rs"
                ),
                Path("crates/tracedecay/src/daemon/foreign_native_provider.rs"),
            )
            for relative in lookalikes:
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(VALID_NATIVE_ADAPTER, encoding="utf-8")
            errors = CHECKER.check_repository(repo)
            for relative in lookalikes:
                self.assertTrue(
                    any(str(relative) in error for error in errors),
                    (relative, errors),
                )

    def test_allowlisted_native_adapter_requires_feature_gate(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            retained_owner = repo / RETAINED_OWNER_PATH
            retained_owner.write_text(
                VALID_RETAINED_OWNER.replace(
                    '#[cfg(feature = "memory-provider-host")]\n'
                    "pub(crate) mod native_provider;",
                    "pub(crate) mod native_provider;",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("must be feature-gated" in error for error in errors))

    def test_enabled_activation_in_allowlisted_adapter_still_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_feature_gated_native_adapters(repo)
            adapter = repo / NATIVE_ADAPTER_PATHS[0]
            adapter.write_text(
                VALID_NATIVE_ADAPTER
                + "fn eager() { activate(NativeProviderActivation::Enabled { port }); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must keep the provider host dormant" in error
                    and str(NATIVE_ADAPTER_PATHS[0]) in error
                    for error in errors
                )
            )

    def test_registry_leak_outside_mount_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            leaked = repo / "crates/tracedecay/src/dashboard.rs"
            leaked.write_text(
                "use tracedecay_memory_provider_registry::ProjectMemoryProviderComposition;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("leaked outside" in error for error in errors))

    def test_retention_mount_must_not_compose(self) -> None:
        directory, repo = self.fixture()
        with directory:
            retention = repo / "crates/tracedecay/src/mcp/server/construction.rs"
            retention.write_text(
                VALID_RETENTION
                + "fn sneak() { let _ = tracedecay_memory_provider_registry::"
                "ProjectMemoryProviderComposition::compose(activation); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must not compose providers" in error for error in errors)
            )

    def test_enabled_activation_in_root_source_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            enabled = repo / "crates/tracedecay/src/eager.rs"
            enabled.write_text(
                "fn eager() { activate(NativeProviderActivation::Enabled { port }); }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must keep the provider host dormant" in error for error in errors)
            )

    def test_provider_boundary_mounts_pass(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_provider_boundary_mount_requires_feature_gate(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            retained_owner = repo / RETAINED_OWNER_PATH
            retained_owner.write_text(
                VALID_RETAINED_OWNER.replace(
                    '#[cfg(feature = "memory-provider-host")]\n'
                    "pub(crate) mod observation_journey;",
                    "pub(crate) mod observation_journey;",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "provider-boundary mount" in error
                    and "must be feature-gated" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                )
            )
            # Losing the gate also drops the allowlist, so the registry
            # reference in that file is a leak again.
            self.assertTrue(
                any(
                    "registry dependency leaked outside" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                )
            )

    def test_provider_boundary_mount_must_refuse_disabled_composition(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            recall.write_text(
                VALID_COGNITIVE_RECALL.replace(
                    "    inputs\n"
                    "        .composition\n"
                    "        .registry()\n"
                    "        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;\n",
                    "",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must open with its registry refusal" in error
                    and str(COGNITIVE_RECALL_PATH) in error
                    for error in errors
                )
            )

    def test_provider_boundary_mount_cannot_compose_providers(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            recall.write_text(
                VALID_COGNITIVE_RECALL.replace(
                    "#[cfg(test)]\nmod tests {",
                    "fn sneak() {\n"
                    "    let _ = ProjectMemoryProviderComposition::compose(activation);\n"
                    "}\n\n#[cfg(test)]\nmod tests {",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not name ProjectMemoryProviderComposition::compose" in error
                    and str(COGNITIVE_RECALL_PATH) in error
                    for error in errors
                )
            )

    def test_provider_boundary_mount_cannot_enable_activation(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            journey = repo / OBSERVATION_JOURNEY_PATH
            journey.write_text(
                VALID_OBSERVATION_JOURNEY.replace(
                    "#[cfg(test)]\nmod tests {",
                    "fn sneak() {\n"
                    "    let _ = NativeProviderActivation::Enabled { port };\n"
                    "}\n\n#[cfg(test)]\nmod tests {",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must keep the provider host dormant" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                )
            )

    def test_provider_boundary_mount_cannot_branch_on_provider_name(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            journey = repo / OBSERVATION_JOURNEY_PATH
            journey.write_text(
                VALID_OBSERVATION_JOURNEY.replace(
                    "#[cfg(test)]\nmod tests {",
                    "fn route(provider: &str) -> bool {\n"
                    '    provider == "tracedecay.native"\n'
                    "}\n\n#[cfg(test)]\nmod tests {",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not branch on a provider identity" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                )
            )

    def test_indented_production_item_after_tests_is_still_scanned(self) -> None:
        # The old gate split the production region on indentation: every line
        # after the test marker starting with whitespace was treated as test
        # code.  Rust allows indented top-level items, so a production
        # function parked after the test module was invisible.  The region is
        # now cut by the test module's balanced braces, so anything after its
        # real closing brace -- indented or not -- is production again.
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            recall.write_text(
                VALID_COGNITIVE_RECALL
                + "    pub(crate) fn after_tests() {\n"
                "        let _ = ProjectMemoryProviderComposition::compose(\n"
                "            NativeProviderActivation::Enabled { port },\n"
                "        );\n"
                "    }\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not name ProjectMemoryProviderComposition::compose" in error
                    and str(COGNITIVE_RECALL_PATH) in error
                    for error in errors
                ),
                errors,
            )
            self.assertTrue(
                any(
                    "must not name NativeProviderActivation::Enabled" in error
                    and str(COGNITIVE_RECALL_PATH) in error
                    for error in errors
                ),
                errors,
            )

    def test_second_named_test_module_is_also_stripped(self) -> None:
        # ...and the same balanced-brace extraction means a differently named
        # test module is stripped too, so a legitimate fixture never trips the
        # production scans just because it is not called `tests`.
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            recall.write_text(
                VALID_COGNITIVE_RECALL
                + "#[cfg(test)]\nmod journey_tests {\n"
                "    use tracedecay_memory_provider_registry::NativeProviderActivation;\n"
                "    fn fixture() {\n"
                "        let _ = ProjectMemoryProviderComposition::compose(\n"
                "            NativeProviderActivation::Enabled { port },\n"
                "        );\n"
                "    }\n}\n",
                encoding="utf-8",
            )
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_prose_about_provider_names_is_not_read_as_branching(self) -> None:
        # The branching scan runs against a code mask, so a doc comment or an
        # assertion string that quotes a provider identity is not a branch.
        # Before this, a `#[cfg(test)]` assertion containing a provider id was
        # enough to fail the live gate -- a false positive that teaches people
        # to loosen the rule.
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            recall.write_text(
                "/// Never write `provider == \"tracedecay.native\"` here: provider\n"
                "/// identity recognition belongs to the registry.\n"
                + VALID_COGNITIVE_RECALL,
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertEqual(
                [error for error in errors if "branch on a provider identity" in error],
                [],
            )

    # -- bypasses the previous gate accepted -------------------------------

    def test_transitive_default_closure_reaching_the_host_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            manifest = repo / "crates/tracedecay/Cargo.toml"
            manifest.write_text(
                VALID_MANIFEST.replace(
                    'production = ["token-counting"]',
                    'production = ["token-counting", "shipped-extras"]\n'
                    'shipped-extras = ["memory-provider-host"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must stay outside the default feature closure" in error
                    for error in errors
                ),
                errors,
            )
            self.assertTrue(
                any(
                    "must stay outside the production feature closure" in error
                    for error in errors
                ),
                errors,
            )

    def test_shadowing_the_resolved_activation_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "    let memory_provider_activation = "
                    "activation.resolve(&runtime_configuration)?;",
                    "    let memory_provider_activation = "
                    "activation.resolve(&runtime_configuration)?;\n"
                    "    let memory_provider_activation = forced_activation();",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("must be bound exactly once and immutably" in error for error in errors),
                errors,
            )

    def test_overwriting_the_resolved_activation_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "    let memory_provider_activation = "
                    "activation.resolve(&runtime_configuration)?;",
                    "    let mut memory_provider_activation = "
                    "activation.resolve(&runtime_configuration)?;\n"
                    "    memory_provider_activation = "
                    "ProjectMemoryProviderActivation::NativeActive;",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must resolve the activation exactly once" in error
                    or "must never be reassigned" in error
                    for error in errors
                ),
                errors,
            )

    def test_mounting_an_activation_the_selector_did_not_resolve_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "    let memory_provider_host_mount = "
                    "mount_project_memory_provider_host(\n"
                    "        memory_provider_activation,\n    )?;",
                    "    let forced = ProjectMemoryProviderActivation::NativeActive;\n"
                    "    let memory_provider_host_mount = "
                    "mount_project_memory_provider_host(\n        forced,\n    )?;",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not construct a ProjectMemoryProviderActivation" in error
                    for error in errors
                ),
                errors,
            )

    def test_moving_the_enabled_construction_to_another_function_fails(self) -> None:
        # A *moved* construction keeps the "exactly one" count intact, which is
        # why the gate has to prove containment in the resolved arm rather than
        # count occurrences and compare offsets.
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "            tracedecay_memory_provider_registry::"
                    "NativeProviderActivation::Enabled { port, mode }\n",
                    "            eager_enabled(port, mode)\n",
                ).replace(
                    '#[cfg(feature = "memory-provider-host")]\n'
                    "fn mount_project_memory_provider_host(",
                    '#[cfg(feature = "memory-provider-host")]\n'
                    "fn eager_enabled(port: Port, mode: Mode) -> Activation {\n"
                    "    tracedecay_memory_provider_registry::"
                    "NativeProviderActivation::Enabled { port, mode }\n"
                    "}\n\n"
                    '#[cfg(feature = "memory-provider-host")]\n'
                    "fn mount_project_memory_provider_host(",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "only inside the resolved" in error and "arm" in error
                    for error in errors
                ),
                errors,
            )

    def test_inserted_resolver_arm_that_shadows_the_refusal_fails(self) -> None:
        # Every required arm is still present here; the violation is the *new*
        # arm in front of the hard error, which downgrades a routed provider to
        # dormant instead of failing project open.
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT.replace(
                    "        (false, Some(provider)) => Err(TraceDecayError::Config {",
                    "        (false, Some(_ignored)) => "
                    "Ok(ProjectMemoryProviderActivation::Disabled),\n"
                    "        (false, Some(provider)) => Err(TraceDecayError::Config {",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must be exactly, and in this order" in error for error in errors
                ),
                errors,
            )

    def test_refusal_relocated_into_the_boundary_test_module_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            recall = repo / COGNITIVE_RECALL_PATH
            refusal = (
                "    inputs\n"
                "        .composition\n"
                "        .registry()\n"
                "        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;\n"
            )
            recall.write_text(
                VALID_COGNITIVE_RECALL.replace(refusal, "", 1).replace(
                    "#[cfg(test)]\nmod tests {",
                    "#[cfg(test)]\nmod tests {\n    fn refusal_shape() {\n"
                    + refusal
                    + "    }\n",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must open with its registry refusal" in error
                    and str(COGNITIVE_RECALL_PATH) in error
                    for error in errors
                ),
                errors,
            )

    def test_side_effect_before_the_boundary_refusal_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            journey = repo / OBSERVATION_JOURNEY_PATH
            journey.write_text(
                VALID_OBSERVATION_JOURNEY.replace(
                    "    inputs\n        .composition\n        .registry()\n",
                    "    let early = SqliteObservationJournal::open(&inputs.root)?;\n"
                    "    inputs\n        .composition\n        .registry()\n",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must open with its registry refusal" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                ),
                errors,
            )

    def test_match_based_provider_dispatch_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT
                + "fn pick(provider: &str) -> bool {\n"
                '    match provider {\n        "provider.ncm-local" => true,\n'
                "        _ => false,\n    }\n}\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("match arm pattern" in error for error in errors), errors
            )

    def test_constant_based_provider_comparison_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT
                + "fn pick(provider: &str) -> bool {\n"
                "    provider == tracedecay_memory_provider_registry::NATIVE_PROVIDER_ID\n"
                "}\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not branch on a provider identity (comparison)" in error
                    for error in errors
                ),
                errors,
            )

    def test_unknown_provider_identity_comparison_fails(self) -> None:
        # The rule cannot be a list of provider names this gate happens to
        # know; an identity it has never heard of must fail just the same.
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT
                + "fn pick(provider: &str) -> bool {\n"
                '    provider == "provider.acme-brain"\n'
                "}\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not branch on a provider identity" in error
                    for error in errors
                ),
                errors,
            )

    def test_matches_macro_provider_dispatch_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            mount = repo / "crates/tracedecay/src/daemon/project_composition.rs"
            mount.write_text(
                VALID_MOUNT
                + "fn pick(provider_id: &str) -> bool {\n"
                '    matches!(provider_id, "tracedecay.native")\n'
                "}\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("matches! macro" in error for error in errors), errors
            )

    def test_helper_mediated_provider_branching_fails(self) -> None:
        # A locally defined recogniser is still name dispatch in this layer:
        # the helper's own body lives in the same production region.
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            journey = repo / OBSERVATION_JOURNEY_PATH
            journey.write_text(
                VALID_OBSERVATION_JOURNEY.replace(
                    "pub(crate) fn mount_project_observation_journey(",
                    "fn is_native(provider_id: &str) -> bool {\n"
                    "    provider_id.eq_ignore_ascii_case(NATIVE_PROVIDER_ID)\n"
                    "}\n\npub(crate) fn mount_project_observation_journey(",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "must not branch on a provider identity" in error
                    and str(OBSERVATION_JOURNEY_PATH) in error
                    for error in errors
                ),
                errors,
            )

    def test_support_crate_leak_outside_observation_journey_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            self.write_provider_boundary_mounts(repo)
            leaked = repo / "crates/tracedecay/src/dashboard.rs"
            leaked.write_text(
                "use tracedecay_memory_hygiene::SanitizationDisposition;\n",
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any(
                    "host-support crate tracedecay_memory_hygiene leaked outside"
                    in error
                    for error in errors
                )
            )


if __name__ == "__main__":
    unittest.main()
