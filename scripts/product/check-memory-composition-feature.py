#!/usr/bin/env python3
"""Verify the default-off Native/NCM provider composition boundary.

Upstream PR #707 moved the retained owner from the tracedecay binary into
tracedecay-daemon-service. The daemon binary still owns the project-open state
machine and the NCM worker slot, while the service owns the provider host,
Native adapter, observation journey, and cognitive-recall mount.

This gate checks that boundary directly. It proves that the root forwards the
runtime selection and the real NCM factory, that the service constructs both
Native and NCM registrations, that retained mounts are feature-gated, and that
the retired root retained_owner layout cannot return after a merge.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _rust_region import RustParseError, body_of, code_mask, strip_cfg_test_modules  # noqa: E402

FEATURE = "memory-provider-host"

ROOT_MANIFEST = Path("crates/tracedecay/Cargo.toml")
SERVICE_MANIFEST = Path("crates/tracedecay-daemon-service/Cargo.toml")
ROOT_SOURCE = Path("crates/tracedecay/src")
SERVICE_SOURCE = Path("crates/tracedecay-daemon-service/src")
ROOT_COMPOSITION = Path("crates/tracedecay/src/daemon/project_composition.rs")
NCM_COMPOSITION = Path("crates/tracedecay/src/daemon/project_composition/ncm_observer.rs")
SERVICE_OWNER = Path("crates/tracedecay-daemon-service/src/retained_owner.rs")
SERVICE_OWNER_ROOT = Path("crates/tracedecay-daemon-service/src/retained_owner")

# The binary needs direct provider edges because it owns the concrete NCM
# worker factory. The service feature owns the retained implementation.
ROOT_HOST_FEATURE_EDGES = [
    "dep:chrono",
    "dep:tracedecay-memory-provider-registry",
    "dep:tracedecay-memory-provider-ncm",
    "dep:tracedecay-memory-observation",
    "dep:tracedecay-memory-hygiene",
    "tracedecay-daemon-service/memory-provider-host",
]
SERVICE_HOST_FEATURE_EDGES = [
    "dep:chrono",
    "dep:hmac",
    "dep:rusqlite",
    "dep:zeroize",
    "dep:tracedecay-memory-provider-registry",
    "dep:tracedecay-memory-provider-ncm",
    "dep:tracedecay-memory-observation",
    "dep:tracedecay-memory-hygiene",
]

REGISTRY_PACKAGE = "tracedecay-memory-provider-registry"
NCM_PACKAGE = "tracedecay-memory-provider-ncm"
OBSERVATION_PACKAGE = "tracedecay-memory-observation"
HYGIENE_PACKAGE = "tracedecay-memory-hygiene"
REGISTRY_IDENT = "tracedecay_memory_provider_registry"

ROOT_HOST_DEPENDENCIES = {
    "chrono": None,
    REGISTRY_PACKAGE: "../tracedecay-memory-provider-registry",
    NCM_PACKAGE: "../tracedecay-memory-provider-ncm",
    OBSERVATION_PACKAGE: "../tracedecay-memory-observation",
    HYGIENE_PACKAGE: "../tracedecay-memory-hygiene",
}
SERVICE_HOST_DEPENDENCIES = {
    "chrono": None,
    "hmac": None,
    "rusqlite": None,
    "zeroize": None,
    REGISTRY_PACKAGE: "../tracedecay-memory-provider-registry",
    NCM_PACKAGE: "../tracedecay-memory-provider-ncm",
    OBSERVATION_PACKAGE: "../tracedecay-memory-observation",
    HYGIENE_PACKAGE: "../tracedecay-memory-hygiene",
}
NCM_BACKEND_FEATURES = ["rust-backend"]

# These files may name the opaque service mount or the concrete NCM factory.
# Transport/retention files can retain the opaque result, but cannot compose.
ROOT_PROVIDER_WIRING_FILES = {ROOT_COMPOSITION, NCM_COMPOSITION}
ROOT_RETENTION_FILES = {
    Path("crates/tracedecay/src/mcp/server.rs"),
    Path("crates/tracedecay/src/mcp/server/construction.rs"),
}

SERVICE_PROVIDER_FILES = (
    SERVICE_OWNER_ROOT / "native_provider.rs",
    SERVICE_OWNER_ROOT / "native_provider_tests.rs",
    SERVICE_OWNER_ROOT / "native_provider_parity_tests.rs",
    SERVICE_OWNER_ROOT / "native_baseline_tests.rs",
    SERVICE_OWNER_ROOT / "native_common_tests.rs",
    SERVICE_OWNER_ROOT / "native_common_factory_tests.rs",
    SERVICE_OWNER_ROOT / "claude_host_journey_tests.rs",
    SERVICE_OWNER_ROOT / "cognitive_recall.rs",
    SERVICE_OWNER_ROOT / "observation_journey.rs",
    SERVICE_OWNER_ROOT / "provider_control.rs",
    SERVICE_OWNER_ROOT / "provider_history.rs",
)

SERVICE_MODULE_DECLARATIONS = {
    "cognitive_recall": '#[cfg(feature = "memory-provider-host")]\npub(crate) mod cognitive_recall;',
    "native_provider": '#[cfg(feature = "memory-provider-host")]\npub(crate) mod native_provider;',
    "observation_journey": '#[cfg(feature = "memory-provider-host")]\npub(crate) mod observation_journey;',
    "provider_control": '#[cfg(feature = "memory-provider-host")]\npub(crate) mod provider_control;',
    "provider_history": '#[cfg(feature = "memory-provider-host")]\npub(crate) mod provider_history;',
}
SERVICE_EXTRA_MODULE_DECLARATIONS = (
    '#[cfg(all(test, feature = "memory-provider-host"))]\n'
    '#[path = "retained_owner/native_common_factory_tests.rs"]\n'
    "mod native_common_factory_tests;",
    '#[cfg(all(test, feature = "memory-provider-host"))]\n'
    '#[path = "retained_owner/native_provider_parity_tests.rs"]\n'
    "mod native_provider_parity_tests;",
)
NATIVE_PROVIDER_NESTED_DECLARATIONS = (
    '#[cfg(test)]\n#[path = "native_baseline_tests.rs"]\nmod baseline_tests;',
    '#[cfg(test)]\n#[path = "native_provider_tests.rs"]\nmod tests;',
)
NATIVE_TEST_NESTED_DECLARATION = '#[path = "native_common_tests.rs"]\nmod common_profile;'
CLAUDE_TEST_DECLARATION = (
    '#[cfg(all(test, feature = "memory-provider-host"))]\n'
    '#[path = "claude_host_journey_tests.rs"]\n'
    "mod claude_host_journey_tests;"
)

# A root owner module was removed by #707. Scan code and physical paths so a
# merge cannot silently reintroduce the retired layout.
STALE_ROOT_OWNER_MARKERS = (
    "crate::daemon::retained_owner",
    "super::retained_owner",
    "crate::retained_owner::",
)
STALE_ROOT_OWNER_ATTRIBUTE = re.compile(
    r"#\s*\[path\s*=\s*[\"']retained_owner(?:/|[\"'])"
)
STALE_ROOT_MODULE = re.compile(r"\bmod\s+retained_owner\s*;")


def read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read TOML {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"TOML root must be a table: {path}")
    return value


def feature_closure(features: dict[str, Any], root: str) -> set[str]:
    """Return every local feature and dependency edge reachable from root."""

    reached: set[str] = set()
    pending = [root]
    while pending:
        name = pending.pop()
        if name in reached:
            continue
        reached.add(name)
        entries = features.get(name)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, str):
                continue
            if entry.startswith("dep:") or "/" in entry:
                reached.add(entry)
            else:
                pending.append(entry)
    return reached


def _check_dependencies(
    manifest: dict[str, Any],
    dependencies: dict[str, str | None],
    label: str,
    errors: list[str],
) -> None:
    table = manifest.get("dependencies")
    if not isinstance(table, dict):
        errors.append(f"{label} manifest [dependencies] table is missing")
        return
    for package, expected_path in dependencies.items():
        dependency = table.get(package)
        if not isinstance(dependency, dict):
            errors.append(f"{label} optional dependency {package} is missing")
            continue
        if dependency.get("optional") is not True:
            errors.append(f"{label} dependency {package} must be optional")
        if expected_path is not None and dependency.get("path") != expected_path:
            errors.append(
                f"{label} dependency {package} path must be {expected_path}"
            )
        if package == NCM_PACKAGE:
            if dependency.get("features") != NCM_BACKEND_FEATURES:
                errors.append(
                    f"{label} dependency {package} must enable exactly "
                    f"{NCM_BACKEND_FEATURES}"
                )
        elif package in {
            REGISTRY_PACKAGE,
            OBSERVATION_PACKAGE,
            HYGIENE_PACKAGE,
        }:
            forbidden = sorted(set(dependency) & {"default-features", "features"})
            if forbidden:
                errors.append(
                    f"{label} dependency {package} must not silently enable "
                    f"features: {forbidden}"
                )


def _check_host_feature(
    manifest: dict[str, Any],
    expected_edges: list[str],
    dependencies: dict[str, str | None],
    label: str,
    errors: list[str],
) -> None:
    features = manifest.get("features")
    if not isinstance(features, dict):
        errors.append(f"{label} manifest [features] table is missing")
        return
    if features.get(FEATURE) != expected_edges:
        errors.append(
            f"{label} feature {FEATURE} must contain exactly {expected_edges}, "
            f"found {features.get(FEATURE)!r}"
        )
    _check_dependencies(manifest, dependencies, label, errors)


def check_manifest(
    root_manifest: dict[str, Any],
    service_manifest: dict[str, Any],
    errors: list[str],
) -> None:
    """Check both sides of the #707 feature/dependency boundary."""

    _check_host_feature(
        root_manifest,
        ROOT_HOST_FEATURE_EDGES,
        ROOT_HOST_DEPENDENCIES,
        "root",
        errors,
    )
    _check_host_feature(
        service_manifest,
        SERVICE_HOST_FEATURE_EDGES,
        SERVICE_HOST_DEPENDENCIES,
        "daemon-service",
        errors,
    )

    root_features = root_manifest.get("features")
    if not isinstance(root_features, dict):
        return
    if root_features.get("default") != ["production"]:
        errors.append(
            "root default features must remain exactly ['production'], "
            f"found {root_features.get('default')!r}"
        )
    if not isinstance(root_features.get("production"), list):
        errors.append("root feature production must be an array")

    support_edges = {
        f"dep:{package}"
        for package in ROOT_HOST_DEPENDENCIES
        if package
        in {
            REGISTRY_PACKAGE,
            NCM_PACKAGE,
            OBSERVATION_PACKAGE,
            HYGIENE_PACKAGE,
        }
    }
    for root in ("default", "production"):
        if not isinstance(root_features.get(root), list):
            continue
        closure = feature_closure(root_features, root)
        if FEATURE in closure:
            errors.append(
                f"root feature {FEATURE} must stay outside the {root} feature "
                "closure; Native/NCM hosting is opt-in"
            )
        for edge in support_edges:
            if edge in closure:
                errors.append(
                    f"root support dependency {edge[4:]} must stay outside the "
                    f"{root} feature closure; it may be reached only by "
                    f"explicitly selecting {FEATURE}"
                )

    # No other root feature may create a direct provider edge. Cargo optional
    # NCM test-helper edges use the question-mark syntax and are allowed.
    for name, entries in root_features.items():
        if name == FEATURE or not isinstance(entries, list):
            continue
        for edge in support_edges:
            if edge in entries:
                errors.append(
                    f"root feature {name} must reach {edge[4:]} only through "
                    f"{FEATURE}"
                )

    root_dependencies = root_manifest.get("dependencies")
    if isinstance(root_dependencies, dict):
        service = root_dependencies.get("tracedecay-daemon-service")
        if (
            not isinstance(service, dict)
            or service.get("path") != "../tracedecay-daemon-service"
        ):
            errors.append(
                "root dependency tracedecay-daemon-service must point to "
                "../tracedecay-daemon-service"
            )


def _read_source(repo: Path, relative: Path, errors: list[str]) -> str | None:
    try:
        return (repo / relative).read_text(encoding="utf-8")
    except OSError as error:
        errors.append(f"cannot read {relative}: {error}")
        return None


def check_activation_defaults(repo: Path, errors: list[str]) -> None:
    """Require the extracted runtime configuration to be dormant by default."""

    config_path = Path("crates/tracedecay-configuration/src/config/model.rs")
    config = _read_source(repo, config_path, errors)
    if config is not None:
        required = (
            (
                r"\bpub\s+memory_provider_native_enabled\s*:\s*bool\s*,",
                "Native enablement field",
            ),
            (
                r"\bpub\s+memory_provider_ncm_observer\s*:\s*MemoryProviderNcmObserverV1\s*,",
                "NCM observer field",
            ),
            (
                r"memory_provider_recall_routing\s*:\s*MemoryProviderRecallRoutingV1\s*,",
                "recall routing field",
            ),
            (r"memory_provider_native_enabled\s*:\s*false\s*,", "Native default"),
            (
                r"memory_provider_ncm_observer\s*:\s*MemoryProviderNcmObserverV1::default\(\)\s*,",
                "NCM default",
            ),
            (
                r"memory_provider_recall_routing\s*:\s*MemoryProviderRecallRoutingV1::default\(\)\s*,",
                "recall routing default",
            ),
        )
        for pattern, description in required:
            if re.search(pattern, config) is None:
                errors.append(
                    "runtime configuration must keep the provider host dormant; "
                    f"{config_path} is missing {description}"
                )

    routing_path = Path("crates/tracedecay-domain/src/configuration.rs")
    routing = _read_source(repo, routing_path, errors)
    if routing is None:
        return
    struct_match = re.search(
        r"pub\s+struct\s+MemoryProviderRecallRoutingV1\s*\{(?P<body>.*?)\n\}",
        routing,
        re.DOTALL,
    )
    if struct_match is None or re.search(
        r"#\[serde\(default\)\]\s*pub\s+active_provider\s*:\s*Option<String>",
        struct_match.group("body") if struct_match else "",
    ) is None:
        errors.append(
            "recall routing gate must default to no active provider; "
            f"{routing_path} is missing an optional active_provider"
        )


def _production(text: str, label: str, errors: list[str]) -> str:
    try:
        return strip_cfg_test_modules(text)
    except RustParseError as error:
        errors.append(f"{label} cannot be parsed structurally: {error}")
        return ""


def _body(text: str, marker: str, label: str, errors: list[str]) -> str | None:
    mask = code_mask(text)
    try:
        start, end = body_of(text, mask, marker)
    except RustParseError as error:
        errors.append(f"{label} must expose exactly one {marker!r}: {error}")
        return None
    return text[start:end]


def _require(
    text: str, fragments: tuple[str, ...], label: str, errors: list[str]
) -> None:
    for fragment in fragments:
        if fragment not in text:
            errors.append(f"{label} is missing required mount wiring: {fragment}")


def check_root_composition(text: str, errors: list[str]) -> None:
    """Check the binary-side selector, factory, and opaque service mount."""

    production = _production(text, "root composition", errors)
    if not production:
        return
    mask = code_mask(production)

    _require(
        text,
        (
            '#[cfg(feature = "memory-provider-host")]\nmod ncm_observer;',
            "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration",
            "MemoryProviderSelectionV1::resolve(",
            "tracedecay_daemon_service::retained_owner::mount_project_memory_provider_host(",
            "ProjectMemoryProviderHostInputsV1",
            "ncm_registration_factory",
            "ncm_observer::construct_ncm_registration_with_authority",
            "activation: memory_provider_activation,",
            "mount_project_memory_provider_full(",
            "with_memory_provider_host_mount(",
            "with_cognitive_recall_mount(",
            "with_observation_journey_mount(",
        ),
        "root composition",
        errors,
    )

    entry = _body(
        production,
        "pub(super) async fn production_project_server(",
        "root composition",
        errors,
    )
    if entry is not None:
        if (
            "ProjectMemoryProviderActivationSelector::FromRuntimeConfiguration"
            not in entry
        ):
            errors.append(
                "production_project_server must pass the runtime provider selector"
            )
        if "production_project_server_inner(" not in entry:
            errors.append(
                "production_project_server must delegate to production_project_server_inner"
            )

    assignment = (
        "let memory_provider_activation = "
        "self.activation.clone().resolve(runtime_configuration)?;"
    )
    if production.count(assignment) != 1:
        errors.append(
            "root composition must resolve memory_provider_activation exactly once "
            f"with: {assignment}"
        )
    if re.search(r"\blet\s+mut\s+memory_provider_activation\b", mask):
        errors.append(
            "root composition must keep memory_provider_activation immutable"
        )
    if len(re.findall(r"\blet\s+memory_provider_activation\b", mask)) != 1:
        errors.append(
            "root composition must bind memory_provider_activation exactly once"
        )
    remainder = mask.replace(code_mask(assignment), "", 1)
    if re.search(r"\bmemory_provider_activation\s*=(?!=)", remainder):
        errors.append(
            "root composition must not overwrite memory_provider_activation"
        )

    forbidden = (
        "ProjectMemoryProviderComposition::compose",
        "ProjectMemoryProviderComposition::compose_registered",
        "NativeProvider::new",
        "NcmProviderAdapter",
        "tracedecay_memory_provider_native",
        "ProjectMemoryProviderActivationSelector::Pinned",
        "ProjectMemoryProviderActivation::Native",
        "ProjectMemoryProviderActivation::Ncm",
    )
    for fragment in forbidden:
        if fragment in production:
            errors.append(
                "root composition must delegate provider construction to "
                f"daemon-service; found {fragment}"
            )
    if re.search(r"\b(?:NATIVE_PROVIDER_ID|NCM_PROVIDER_ID)\b", mask):
        errors.append(
            "root composition must not branch on a provider identity; "
            "Native/NCM dispatch belongs to daemon-service"
        )


def check_service_host_composition(text: str, errors: list[str]) -> None:
    """Prove the service host constructs both Native and NCM registrations."""

    production = _production(text, "daemon-service retained owner", errors)
    if not production:
        return
    mask = code_mask(production)

    for name, declaration in SERVICE_MODULE_DECLARATIONS.items():
        if declaration not in text:
            errors.append(
                "daemon-service retained owner module must be feature-gated: "
                f"{name}"
            )
    for declaration in SERVICE_EXTRA_MODULE_DECLARATIONS:
        if declaration not in text:
            errors.append(
                "daemon-service retained owner test module declaration is missing: "
                f"{declaration}"
            )
    for declaration in (
        '#[cfg(feature = "memory-provider-host")]\npub async fn mount_project_memory_provider_host(',
        '#[cfg(feature = "memory-provider-host")]\npub async fn mount_project_memory_provider_full(',
    ):
        if declaration not in text:
            errors.append(
                "daemon-service provider mount must be feature-gated: "
                f"{declaration}"
            )

    host_body = _body(
        production,
        "pub async fn mount_project_memory_provider_host(",
        "daemon-service retained owner",
        errors,
    )
    if host_body is None:
        return
    host_mask = code_mask(host_body)
    required = (
        "if inputs.activation.is_disabled()",
        "ProjectMemoryProviderComposition::Disabled",
        "for (kind, participation) in [",
        "(MemoryProviderKindV1::Native, inputs.activation.native),\n"
        "        (MemoryProviderKindV1::Ncm, inputs.activation.ncm),",
        "MemoryProviderParticipationV1::Disabled => continue",
        "MemoryProviderParticipationV1::Observer => EnabledProviderMode::Observer",
        "MemoryProviderParticipationV1::Active => EnabledProviderMode::Active",
        "MemoryProviderKindV1::Native =>",
        "MemoryProviderKindV1::Ncm =>",
        "NativeProvider::new(",
        "ncm_registration_factory",
        "MemoryProviderNcmObserverV1::Enabled",
        "spawn_blocking(move ||",
        "ProjectMemoryProviderComposition::compose_registered(",
        "SelectedProviderActivationV1::Injected",
        "SelectedProviderActivationV1::ObserversOnly",
        "cognitive_recall::mount_project_cognitive_recall(",
        "observation_provider_mounts.push(",
    )
    _require(host_body, required, "daemon-service provider host", errors)

    disabled = host_body.find("if inputs.activation.is_disabled()")
    loop = host_body.find("for (kind, participation) in [")
    if disabled == -1 or loop == -1 or disabled > loop:
        errors.append(
            "daemon-service provider host must return its disabled composition "
            "before constructing Native or NCM infrastructure"
        )
    if len(re.findall(r"MemoryProviderKindV1::Native\s*=>\s*(?:async\s*)?\{", host_mask)) != 1:
        errors.append("daemon-service provider host is missing its Native mount arm")
    if len(re.findall(r"MemoryProviderKindV1::Ncm\s*=>\s*(?:async\s*)?\{", host_mask)) != 1:
        errors.append("daemon-service provider host is missing its NCM mount arm")
    if host_body.count("(MemoryProviderKindV1::Native, inputs.activation.native)") != 1:
        errors.append("daemon-service provider host is missing Native participation wiring")
    if host_body.count("(MemoryProviderKindV1::Ncm, inputs.activation.ncm)") != 1:
        errors.append("daemon-service provider host is missing NCM participation wiring")
    if "if mode == EnabledProviderMode::Active" not in host_mask:
        errors.append(
            "daemon-service provider host must classify active registrations before "
            "building observer-only composition"
        )

    full = _body(
        production,
        "pub async fn mount_project_memory_provider_full(",
        "daemon-service retained owner",
        errors,
    )
    if full is not None:
        _require(
            full,
            (
                "observation_journey::mount_observer_dormant(",
                "observation_journeys",
                "provider_control_mount",
            ),
            "daemon-service full provider mount",
            errors,
        )


def check_service_boundary_mounts(repo: Path, errors: list[str]) -> None:
    """Check service-owned recall and observation refusal boundaries."""

    recall = SERVICE_OWNER_ROOT / "cognitive_recall.rs"
    observation = SERVICE_OWNER_ROOT / "observation_journey.rs"
    recall_text = _read_source(repo, recall, errors)
    observation_text = _read_source(repo, observation, errors)
    if recall_text is not None:
        recall_production = _production(recall_text, str(recall), errors)
        recall_body = _body(
            recall_production,
            "pub(crate) fn mount_project_cognitive_recall(",
            str(recall),
            errors,
        )
        if recall_body is not None and not recall_body.lstrip().startswith(
            "inputs\n        .composition\n        .registry()"
        ):
            errors.append(
                f"{recall} must refuse a disabled composition before opening its ledger"
            )
        if "ProjectMemoryProviderComposition::compose" in recall_production:
            errors.append(f"{recall} must not compose providers")
    if observation_text is not None:
        observation_production = _production(observation_text, str(observation), errors)
        construct_body = _body(
            observation_production,
            "fn construct_project_observation_journey(",
            str(observation),
            errors,
        )
        if construct_body is not None and not construct_body.lstrip().startswith(
            "inputs\n        .composition\n        .registry()"
        ):
            errors.append(
                f"{observation} must refuse a disabled composition before opening storage"
            )
        mount_body = _body(
            observation_production,
            "pub(crate) fn mount_project_observation_journey(",
            str(observation),
            errors,
        )
        if mount_body is not None:
            _require(
                mount_body,
                (
                    "construct_project_observation_journey(inputs)",
                    "start_delivery_worker()",
                ),
                str(observation),
                errors,
            )


def check_service_files(repo: Path, errors: list[str]) -> None:
    owner = _read_source(repo, SERVICE_OWNER, errors)
    if owner is None and not (repo / SERVICE_OWNER).exists():
        errors.append(f"daemon-service retained_owner.rs is missing: {SERVICE_OWNER}")
    if owner is not None:
        check_service_host_composition(owner, errors)
    for relative in SERVICE_PROVIDER_FILES:
        if not (repo / relative).is_file():
            errors.append(
                f"daemon-service provider boundary file is missing: {relative}"
            )
    native = _read_source(repo, SERVICE_OWNER_ROOT / "native_provider.rs", errors)
    if native is not None:
        for declaration in NATIVE_PROVIDER_NESTED_DECLARATIONS:
            if declaration not in native:
                errors.append(
                    "daemon-service Native provider test module declaration is "
                    f"missing: {declaration}"
                )
    tests = _read_source(repo, SERVICE_OWNER_ROOT / "native_provider_tests.rs", errors)
    if tests is not None and NATIVE_TEST_NESTED_DECLARATION not in tests:
        errors.append(
            "daemon-service Native provider tests must include native_common_tests.rs"
        )
    journey = _read_source(repo, SERVICE_OWNER_ROOT / "observation_journey.rs", errors)
    if journey is not None and CLAUDE_TEST_DECLARATION not in journey:
        errors.append(
            "daemon-service observation journey must feature-gate the Claude host journey"
        )


def _scan_stale_root_reference(relative: Path, text: str, errors: list[str]) -> None:
    mask = code_mask(text)
    for marker in STALE_ROOT_OWNER_MARKERS:
        if marker in mask:
            errors.append(
                "stale root retained_owner reference is forbidden after #707: "
                f"{relative} contains {marker}"
            )
    if STALE_ROOT_MODULE.search(mask):
        errors.append(
            "stale root retained_owner module declaration is forbidden after #707: "
            f"{relative}"
        )
    if STALE_ROOT_OWNER_ATTRIBUTE.search(text):
        errors.append(
            "stale root retained_owner path attribute is forbidden after #707: "
            f"{relative}"
        )


def check_repository(repo: Path) -> list[str]:
    errors: list[str] = []
    try:
        root_manifest = read_toml(repo / ROOT_MANIFEST)
    except ValueError as error:
        return [str(error)]
    try:
        service_manifest = read_toml(repo / SERVICE_MANIFEST)
    except ValueError as error:
        return [str(error)]
    check_manifest(root_manifest, service_manifest, errors)
    check_activation_defaults(repo, errors)

    root_composition = _read_source(repo, ROOT_COMPOSITION, errors)
    if root_composition is not None:
        check_root_composition(root_composition, errors)
    ncm_composition = _read_source(repo, NCM_COMPOSITION, errors)
    if ncm_composition is not None and "construct_ncm_registration_with_authority" not in ncm_composition:
        errors.append(
            f"NCM worker composition must expose the real registration factory: {NCM_COMPOSITION}"
        )

    root_owner_file = Path("crates/tracedecay/src/daemon/retained_owner.rs")
    root_owner_dir = Path("crates/tracedecay/src/daemon/retained_owner")
    if (repo / root_owner_file).exists():
        errors.append(
            "stale root retained_owner file remains after #707: "
            f"{root_owner_file}"
        )
    if (repo / root_owner_dir).is_dir():
        errors.append(
            "stale root retained_owner directory remains after #707: "
            f"{root_owner_dir}"
        )

    check_service_files(repo, errors)
    check_service_boundary_mounts(repo, errors)

    source_root = repo / ROOT_SOURCE
    if source_root.is_dir():
        for path in sorted(source_root.rglob("*.rs")):
            relative = path.relative_to(repo)
            text = path.read_text(encoding="utf-8")
            _scan_stale_root_reference(relative, text, errors)
            if relative in ROOT_PROVIDER_WIRING_FILES:
                continue
            if relative in ROOT_RETENTION_FILES:
                if "ProjectMemoryProviderComposition::compose" in code_mask(text):
                    errors.append(
                        f"retention mount must not compose providers: {relative}"
                    )
                continue
            if REGISTRY_IDENT in code_mask(text):
                errors.append(
                    "registry dependency leaked outside the provider composition "
                    f"boundary: {relative}"
                )
            if re.search(r"\b(?:NativeProvider|NcmProviderAdapter)\b", code_mask(text)):
                errors.append(
                    "concrete provider adapter leaked outside the composition "
                    f"boundary: {relative}"
                )
    return errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=".", help="repository root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    repo = Path(args.repo).resolve()
    try:
        errors = check_repository(repo)
    except ValueError as error:
        print(f"memory composition feature error: {error}", file=sys.stderr)
        return 2
    if errors:
        for error in errors:
            print(f"memory composition feature violation: {error}", file=sys.stderr)
        return 1
    print("memory composition feature verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
