#!/usr/bin/env python3
"""Focused tests for the #707 Native/NCM composition-boundary guard."""

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

ROOT_MANIFEST = """[features]
default = ["production"]
production = []
memory-provider-host = [
    "dep:chrono",
    "dep:tracedecay-memory-provider-registry",
    "dep:tracedecay-memory-provider-ncm",
    "dep:tracedecay-memory-observation",
    "dep:tracedecay-memory-hygiene",
    "tracedecay-daemon-service/memory-provider-host",
]

[dependencies]
tracedecay-daemon-service = { path = "../tracedecay-daemon-service" }
chrono = { version = "0.4", optional = true }
tracedecay-memory-provider-registry = { path = "../tracedecay-memory-provider-registry", optional = true }
tracedecay-memory-provider-ncm = { path = "../tracedecay-memory-provider-ncm", optional = true, features = ["rust-backend"] }
tracedecay-memory-observation = { path = "../tracedecay-memory-observation", optional = true }
tracedecay-memory-hygiene = { path = "../tracedecay-memory-hygiene", optional = true }
"""

SERVICE_MANIFEST = """[features]
test-helpers = ["tracedecay-memory-provider-ncm?/test-helpers"]
memory-provider-host = [
    "dep:chrono",
    "dep:hmac",
    "dep:rusqlite",
    "dep:zeroize",
    "dep:tracedecay-memory-provider-registry",
    "dep:tracedecay-memory-provider-ncm",
    "dep:tracedecay-memory-observation",
    "dep:tracedecay-memory-hygiene",
]

[dependencies]
chrono = { version = "0.4", optional = true }
hmac = { version = "0.13", optional = true }
rusqlite = { version = "0.40", optional = true }
zeroize = { version = "1", optional = true }
tracedecay-memory-provider-registry = { path = "../tracedecay-memory-provider-registry", optional = true }
tracedecay-memory-provider-ncm = { path = "../tracedecay-memory-provider-ncm", optional = true, features = ["rust-backend"] }
tracedecay-memory-observation = { path = "../tracedecay-memory-observation", optional = true }
tracedecay-memory-hygiene = { path = "../tracedecay-memory-hygiene", optional = true }
"""

CONFIG = """use tracedecay_domain::configuration::{
    MemoryProviderNcmObserverV1, MemoryProviderRecallRoutingV1,
};

pub struct TraceDecayConfig {
    pub memory_provider_native_enabled: bool,
    pub memory_provider_ncm_observer: MemoryProviderNcmObserverV1,
    pub memory_provider_recall_routing: MemoryProviderRecallRoutingV1,
}

impl Default for TraceDecayConfig {
    fn default() -> Self {
        Self {
            memory_provider_native_enabled: false,
            memory_provider_ncm_observer: MemoryProviderNcmObserverV1::default(),
            memory_provider_recall_routing: MemoryProviderRecallRoutingV1::default(),
        }
    }
}
"""

ROUTING = """#[derive(Default)]
pub struct MemoryProviderRecallRoutingV1 {
    #[serde(default)]
    pub active_provider: Option<String>,
}
"""

ROOT_COMPOSITION = """#[cfg(feature = "memory-provider-host")]
mod ncm_observer;

#[cfg(feature = "memory-provider-host")]
type ProjectMemoryProviderActivation =
    tracedecay_domain::configuration::MemoryProviderSelectionV1;

#[cfg(feature = "memory-provider-host")]
enum ProjectMemoryProviderActivationSelector {
    FromRuntimeConfiguration,
}

#[cfg(feature = "memory-provider-host")]
impl ProjectMemoryProviderActivationSelector {
    fn resolve(
        self,
        runtime_configuration: &PinnedRuntimeConfiguration,
    ) -> Result<ProjectMemoryProviderActivation> {
        let config = runtime_configuration.config();
        tracedecay_domain::configuration::MemoryProviderSelectionV1::resolve(
            config.memory_provider_native_enabled,
            &config.memory_provider_ncm_observer,
            &config.memory_provider_recall_routing,
        )
        .map_err(|error| TraceDecayError::Config { message: error.to_string() })
    }
}

pub(super) async fn production_project_server(
    runtime: ProductionProjectCompositionRuntime,
) -> Result<()> {
    production_project_server_inner(
        runtime,
        ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration,
    )
    .await
}

async fn production_project_server_inner(
    runtime: ProductionProjectCompositionRuntime,
    activation: ProjectMemoryProviderActivationSelector,
) -> Result<()> {
    compose_core_server(runtime, activation).await
}

async fn compose_core_server(
    runtime: ProductionProjectCompositionRuntime,
    activation: ProjectMemoryProviderActivationSelector,
) -> Result<()> {
    let runtime_configuration = runtime.configuration();
    let memory_provider_activation = self.activation.clone().resolve(runtime_configuration)?;
    let ncm_registration_factory = ncm_registration_factory(
        ncm_observer::construct_ncm_registration_with_authority(/* factory args */),
    );
    let memory_provider_host =
        tracedecay_daemon_service::retained_owner::mount_project_memory_provider_host(
            tracedecay_daemon_service::retained_owner::ProjectMemoryProviderHostInputsV1 {
                activation: memory_provider_activation,
                ncm_registration_factory,
            },
        )
        .await?;
    let full = tracedecay_daemon_service::retained_owner::mount_project_memory_provider_full(
        &memory_provider_host,
    )
    .await?;
    let context = context
        .with_memory_provider_host_mount(memory_provider_host)
        .with_cognitive_recall_mount(full.cognitive_recall_mount())
        .with_observation_journey_mount(full.observation_journey());
    Ok(())
}
"""

NCM_COMPOSITION = """fn construct_ncm_registration_with_authority() {
    let _adapter = tracedecay_memory_provider_ncm::NcmProviderAdapter;
    let _registry = tracedecay_memory_provider_registry::ProviderRegistrationV1;
}
"""

SERVICE_OWNER = """#[cfg(feature = "memory-provider-host")]
pub(crate) mod cognitive_recall;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_provider;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_common_factory_tests.rs"]
mod native_common_factory_tests;
#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "retained_owner/native_provider_parity_tests.rs"]
mod native_provider_parity_tests;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod native_staged_observations;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod observation_journey;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod provider_control;
#[cfg(feature = "memory-provider-host")]
pub(crate) mod provider_history;

#[cfg(feature = "memory-provider-host")]
pub async fn mount_project_memory_provider_host(
    inputs: ProjectMemoryProviderHostInputsV1,
) -> Result<ProjectMemoryProviderHostMountV1, String> {
    if inputs.activation.is_disabled() {
        return Ok(ProjectMemoryProviderComposition::Disabled);
    }
    let mut selected = None;
    let mut observers = Vec::new();
    let mut observation_provider_mounts = Vec::new();
    for (kind, participation) in [
        (MemoryProviderKindV1::Native, inputs.activation.native),
        (MemoryProviderKindV1::Ncm, inputs.activation.ncm),
    ] {
        let mode = match participation {
            MemoryProviderParticipationV1::Disabled => continue,
            MemoryProviderParticipationV1::Observer => EnabledProviderMode::Observer,
            MemoryProviderParticipationV1::Active => EnabledProviderMode::Active,
        };
        let registration = match kind {
            MemoryProviderKindV1::Native => {
                let provider = NativeProvider::new(
                    native_provider::project_native_memory_application_port_off_runtime(),
                )?;
                Ok((provider, NativeObservationMount))
            }
            MemoryProviderKindV1::Ncm => {
                let tracedecay_domain::configuration::MemoryProviderNcmObserverV1::Enabled {
                    worker_binary,
                    state_root,
                } = inputs.ncm_observer
                else {
                    return Err("selected NCM participation is disabled".to_owned());
                };
                let registration = tokio::task::spawn_blocking(move || {
                    inputs.ncm_registration_factory(worker_binary, state_root, mode)
                })
                .await??;
                Ok(registration)
            }
        };
        match registration {
            Ok((registration, mount)) => {
                if mode == EnabledProviderMode::Active {
                    selected = Some(registration);
                } else {
                    observers.push(registration);
                }
                observation_provider_mounts.push((mount, history_mount));
            }
            Err(error) => return Err(error),
        }
    }
    let composition = ProjectMemoryProviderComposition::compose_registered(
        match selected {
            Some(registration) => SelectedProviderActivationV1::Injected { registration },
            None => SelectedProviderActivationV1::ObserversOnly,
        },
        observers,
    )?;
    let cognitive = cognitive_recall::mount_project_cognitive_recall(composition)?;
    Ok(ProjectMemoryProviderHostMountV1 { composition, cognitive })
}

#[cfg(feature = "memory-provider-host")]
pub async fn mount_project_memory_provider_full(
    host: &Arc<ProjectMemoryProviderHostMountV1>,
) -> Result<ProjectMemoryProviderFullMountV1, String> {
    let journey = observation_journey::mount_observer_dormant(host).await?;
    Ok(ProjectMemoryProviderFullMountV1 {
        observation_journeys: vec![journey],
        provider_control_mount: provider_control_mount(),
    })
}
"""

COGNITIVE_RECALL = """pub(crate) fn mount_project_cognitive_recall(
    inputs: CognitiveRecallMountInputsV1,
) -> Result<ProjectCognitiveRecallMountV1, CognitiveRecallMountError> {
    inputs
        .composition
        .registry()
        .ok_or(CognitiveRecallMountError::CompositionDisabled)?;
    let ledger = RecallAdmissionLedgerV1::open(inputs.store_data_root)?;
    Ok(ProjectCognitiveRecallMountV1 { ledger })
}

#[cfg(test)]
mod tests {
    fn fixture() {
        let _ = ProjectMemoryProviderComposition::compose(activation);
    }
}
"""

OBSERVATION_JOURNEY = """pub(crate) fn mount_project_observation_journey(
    inputs: ObservationJourneyMountInputsV1,
) -> Result<ProjectObservationJourneyV1, ObservationJourneyError> {
    let journey = construct_project_observation_journey(inputs)?;
    journey.start_delivery_worker()?;
    Ok(journey)
}

fn construct_project_observation_journey(
    inputs: ObservationJourneyMountInputsV1,
) -> Result<ProjectObservationJourneyV1, ObservationJourneyError> {
    inputs
        .composition
        .registry()
        .ok_or(ObservationJourneyError::CompositionDisabled)?;
    let journal = SqliteObservationJournal::open(inputs.store_data_root)?;
    Ok(ProjectObservationJourneyV1 { journal })
}

#[cfg(all(test, feature = "memory-provider-host"))]
#[path = "claude_host_journey_tests.rs"]
mod claude_host_journey_tests;
"""

NATIVE_PROVIDER = """use tracedecay_memory_provider_registry::NativeProvider;
#[cfg(test)]
#[path = "native_baseline_tests.rs"]
mod baseline_tests;
#[cfg(test)]
#[path = "native_provider_tests.rs"]
mod tests;
"""

NATIVE_PROVIDER_TESTS = """#[path = "native_common_tests.rs"]
mod common_profile;
"""

def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class MemoryCompositionFeatureTest(unittest.TestCase):
    def fixture(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        repo = Path(directory.name)
        write(repo / CHECKER.ROOT_MANIFEST, ROOT_MANIFEST)
        write(repo / CHECKER.SERVICE_MANIFEST, SERVICE_MANIFEST)
        write(
            repo / "crates/tracedecay-configuration/src/config/model.rs",
            CONFIG,
        )
        write(repo / "crates/tracedecay-domain/src/configuration.rs", ROUTING)
        write(repo / CHECKER.ROOT_COMPOSITION, ROOT_COMPOSITION)
        write(repo / CHECKER.NCM_COMPOSITION, NCM_COMPOSITION)
        write(repo / CHECKER.SERVICE_OWNER, SERVICE_OWNER)
        write(
            repo / (CHECKER.SERVICE_SOURCE / "lib.rs"),
            "pub mod retained_owner;\n",
        )
        write(
            repo / (CHECKER.ROOT_SOURCE / "lib.rs"),
            "pub mod daemon;\n",
        )
        return directory, repo

    def write_service_files(self, repo: Path) -> None:
        sources = {
            "native_provider.rs": NATIVE_PROVIDER,
            "native_provider_tests.rs": NATIVE_PROVIDER_TESTS,
            "native_provider_parity_tests.rs": "",
            "native_baseline_tests.rs": "",
            "native_staged_observations.rs": "",
            "native_common_tests.rs": "",
            "native_common_factory_tests.rs": "",
            "claude_host_journey_tests.rs": "",
            "cognitive_recall.rs": COGNITIVE_RECALL,
            "observation_journey.rs": OBSERVATION_JOURNEY,
            "provider_control.rs": "",
            "provider_history.rs": "",
        }
        for name, source in sources.items():
            write(repo / (CHECKER.SERVICE_OWNER_ROOT / name), source)

    def valid_repo(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory, repo = self.fixture()
        self.write_service_files(repo)
        return directory, repo

    def test_valid_native_ncm_service_mount_passes(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            self.assertEqual(CHECKER.check_repository(repo), [])

    def test_missing_native_participation_wiring_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace(
                    "(MemoryProviderKindV1::Native, inputs.activation.native),\n",
                    "",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("missing Native participation wiring" in error for error in errors),
                errors,
            )

    def test_missing_ncm_participation_wiring_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace(
                    "(MemoryProviderKindV1::Ncm, inputs.activation.ncm),",
                    "",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("missing NCM participation wiring" in error for error in errors),
                errors,
            )

    def test_missing_native_constructor_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace("NativeProvider::new(", "NativeProvider::from_port("),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("NativeProvider::new" in error for error in errors), errors)

    def test_missing_ncm_factory_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace("inputs.ncm_registration_factory", "inputs.other_factory"),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("ncm_registration_factory" in error for error in errors), errors)

    def test_root_must_call_service_mount(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.ROOT_COMPOSITION
            path.write_text(
                ROOT_COMPOSITION.replace(
                    "tracedecay_daemon_service::retained_owner::mount_project_memory_provider_host(",
                    "legacy_mount_project_memory_provider_host(",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("mount_project_memory_provider_host" in error for error in errors),
                errors,
            )

    def test_root_must_forward_ncm_factory(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.ROOT_COMPOSITION
            path.write_text(
                ROOT_COMPOSITION.replace(
                    "ncm_observer::construct_ncm_registration_with_authority(",
                    "legacy_ncm_registration(",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("construct_ncm_registration" in error for error in errors), errors)

    def test_root_retained_owner_file_is_stale(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            write(
                repo / "crates/tracedecay/src/daemon/retained_owner.rs",
                "mod stale;\n",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("stale root retained_owner file" in error for error in errors), errors)

    def test_root_retained_owner_reference_is_stale(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            write(
                repo / (CHECKER.ROOT_SOURCE / "stale.rs"),
                "fn stale() { crate::daemon::retained_owner::native_provider(); }\n",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("stale root retained_owner reference" in error for error in errors), errors)

    def test_missing_service_owner_fails(self) -> None:
        directory, repo = self.fixture()
        with directory:
            (repo / CHECKER.SERVICE_OWNER).unlink()
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("daemon-service retained_owner.rs" in error for error in errors), errors)

    def test_service_module_must_be_feature_gated(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace(
                    '#[cfg(feature = "memory-provider-host")]\npub(crate) mod observation_journey;',
                    "pub(crate) mod observation_journey;",
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(
                any("module must be feature-gated" in error and "observation_journey" in error for error in errors),
                errors,
            )

    def test_service_ncm_branch_cannot_be_dropped(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace(
                    "MemoryProviderKindV1::Ncm => {",
                    "MemoryProviderKindV1::Native => {",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("missing its NCM mount arm" in error for error in errors), errors)

    def test_service_native_branch_cannot_be_dropped(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_OWNER
            path.write_text(
                SERVICE_OWNER.replace(
                    "MemoryProviderKindV1::Native => {",
                    "MemoryProviderKindV1::Ncm => {",
                    1,
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("missing its Native mount arm" in error for error in errors), errors)

    def test_default_feature_reaching_host_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.ROOT_MANIFEST
            path.write_text(
                ROOT_MANIFEST.replace(
                    'production = []',
                    'production = ["shipped"]\nshipped = ["memory-provider-host"]',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("outside the production feature closure" in error for error in errors), errors)

    def test_ncm_dependency_must_be_optional(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            path = repo / CHECKER.SERVICE_MANIFEST
            path.write_text(
                SERVICE_MANIFEST.replace(
                    'tracedecay-memory-provider-ncm = { path = "../tracedecay-memory-provider-ncm", optional = true, features = ["rust-backend"] }',
                    'tracedecay-memory-provider-ncm = { path = "../tracedecay-memory-provider-ncm", features = ["rust-backend"] }',
                ),
                encoding="utf-8",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("daemon-service dependency tracedecay-memory-provider-ncm must be optional" in error for error in errors), errors)

    def test_root_direct_registry_leak_fails(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            write(
                repo / (CHECKER.ROOT_SOURCE / "leak.rs"),
                "use tracedecay_memory_provider_registry::ProjectMemoryProviderComposition;\n",
            )
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("registry dependency leaked" in error for error in errors), errors)

    def test_observation_and_recall_mounts_are_service_owned(self) -> None:
        directory, repo = self.valid_repo()
        with directory:
            root = repo / CHECKER.ROOT_SOURCE
            write(root / "daemon" / "retained_owner" / "cognitive_recall.rs", "fn stale() {}\n")
            errors = CHECKER.check_repository(repo)
            self.assertTrue(any("stale root retained_owner directory" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
