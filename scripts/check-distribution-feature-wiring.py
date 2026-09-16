#!/usr/bin/env python3
"""Validate source and packaged Cargo feature ownership for distribution builds.

The checks are public-contract and layering rules only: the packaged manifest
must carry the source feature set, optional native dependencies must stay
optional and feature-wired, the supported `lang-*` surface must match the
extraction owner, and each language feature must compile in isolation. How a
feature is forwarded through
intermediate crates is Cargo's job; resolved behavior is proven by the
packaged-artifact builds and launch checks in check-distribution-acceptance.sh.

When the source manifest is this checkout's production manifest, the gate also
validates the explicit NCM distribution matrix: arm64 macOS V2 is the only
target allowed to claim the verified worker sidecar and pinned model contract;
all other current release targets must remain Native-only.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tomllib
from typing import Any


REQUIRED_ROOT_FEATURES = {
    "full",
    "hotpath",
    "hotpath-alloc",
    "hotpath-cpu",
    "hotpath-mcp",
    "token-counting",
    "test-transport",
}
REQUIRED_CLI_FEATURE_MEMBERS = {
    "production": {"tracedecay/production"},
    "hotpath": {
        "dep:regex",
        "tracedecay/hotpath",
        "hotpath/hotpath",
        "hotpath/tokio",
        "hotpath/ureq-3",
    },
    "hotpath-alloc": {
        "hotpath",
        "tracedecay/hotpath-alloc",
        "hotpath/hotpath-alloc",
    },
    "hotpath-cpu": {
        "hotpath",
        "tracedecay/hotpath-cpu",
        "hotpath/hotpath-cpu",
    },
    "hotpath-mcp": {"hotpath", "hotpath/hotpath-mcp"},
}

# The NCM worker is deliberately a much smaller distribution surface than the
# native providers. Keep this matrix in the distribution gate so a new release
# target cannot accidentally inherit the host's NCM capability claim.
NCM_SUPPORTED_TARGET = "aarch64-apple-darwin"
NCM_SUPPORTED_RELEASE = "aarch64-macos"
NCM_RUNTIME_POLICY = {
    "provider_package": "tracedecay-memory-provider-ncm",
    "provider_feature": "rust-backend",
    "runtime_package": "tracedecay-memory-ncm-runtime",
    "runtime_feature": "real-encoder",
    "worker_distribution": "separate-sidecar",
    "worker_manifest": "product/ncm/reference/worker-manifest.json",
    "model_manifest": "product/ncm/reference/embedding-manifest.json",
}
NCM_MODEL_CONTRACT = {
    "model": "paraphrase-multilingual-MiniLM-L12-v2",
    "repository": "Xenova/paraphrase-multilingual-MiniLM-L12-v2",
    "revision": "2c4055b12046f11709e9df2c122e59ffbdc2f900",
    "max_length": 128,
    "pooling": "mean",
    "normalize": True,
}
NCM_MODEL_FILES = {
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
}
NCM_RELEASE_WORKFLOW_SNIPPETS = (
    "Build NCM worker sidecar",
    "cargo build -p tracedecay-memory-ncm-runtime --bin tracedecay-ncm-worker",
    "Package NCM worker sidecar",
    'manifest="product/ncm/reference/worker-manifest.json"',
    'test -x "$worker"',
    'test -s "$manifest"',
    "--format tar.gz",
    "--entry-name tracedecay-ncm-worker",
    '--companion "$manifest=worker-manifest.json"',
    "test -x verify-ncm-worker/tracedecay-ncm-worker",
    'cmp "$manifest" verify-ncm-worker/worker-manifest.json',
    'if len(payload) != target["bytes"]',
    "digest = hashlib.sha256(payload).hexdigest()",
    'if digest != target["sha256"]',
    'shasum -a 256 "$archive" > "$archive.sha256"',
)


def load(path: Path) -> dict:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"distribution acceptance: invalid {label}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit(f"distribution acceptance: {label} must be a JSON object")
    return value


def optional_dependencies(manifest: dict) -> set[str]:
    names: set[str] = set()

    def collect(table: object) -> None:
        if not isinstance(table, dict):
            return
        dependencies = table.get("dependencies")
        if isinstance(dependencies, dict):
            for name, spec in dependencies.items():
                if isinstance(spec, dict) and spec.get("optional") is True:
                    names.add(name)

    collect(manifest)
    for target in manifest.get("target", {}).values():
        collect(target)
    return names


def require_matching_features(name: str, source: dict, packaged: dict) -> dict:
    source_features = source.get("features", {})
    packaged_features = packaged.get("features", {})
    if source_features != packaged_features:
        raise SystemExit(
            f"distribution acceptance: packaged {name} feature wiring differs from Cargo.toml"
        )
    return packaged_features


def require_optional_dependencies_wired(name: str, manifest: dict, features: dict) -> None:
    references = {
        item
        for members in features.values()
        for item in members
        if isinstance(item, str)
    }
    unwired = sorted(
        dependency
        for dependency in optional_dependencies(manifest)
        if f"dep:{dependency}" not in references and dependency not in features
    )
    if unwired:
        raise SystemExit(
            f"distribution acceptance: {name} optional dependencies are not feature-wired: "
            + ", ".join(unwired)
        )


def language_feature_names(features: dict) -> set[str]:
    return {name for name in features if name.startswith("lang-")}


def require_language_surface(
    name: str,
    features: dict,
    authority_features: set[str],
) -> None:
    """The public `lang-*` surface must advertise exactly the owner's languages."""
    actual_features = language_feature_names(features)
    if actual_features != authority_features:
        missing = sorted(authority_features - actual_features)
        extra = sorted(actual_features - authority_features)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("extra " + ", ".join(extra))
        raise SystemExit(
            f"distribution acceptance: {name} language features differ from "
            "tracedecay-code-extraction: " + "; ".join(details)
        )


def require_isolated_language_features_compile(
    manifest_path: Path,
    authority_features: set[str],
    cargo_config: Path | None,
    offline: bool,
) -> None:
    manifest = load(manifest_path)
    package_name = manifest.get("package", {}).get("name")
    if not isinstance(package_name, str):
        raise SystemExit(
            "distribution acceptance: extraction build manifest has no package name"
        )

    build_features = language_feature_names(manifest.get("features", {}))
    if build_features != authority_features:
        raise SystemExit(
            "distribution acceptance: extraction build manifest language features "
            "differ from the packaged authority"
        )

    for feature in sorted(authority_features):
        command = [
            "cargo",
            "check",
            "--manifest-path",
            str(manifest_path),
            "--package",
            package_name,
            "--lib",
            "--no-default-features",
            "--features",
            feature,
        ]
        if cargo_config is not None:
            command.extend(["--config", str(cargo_config)])
        if offline:
            command.append("--offline")
        completed = subprocess.run(
            command, check=False, capture_output=True, text=True
        )
        if completed.returncode != 0:
            details = completed.stderr.strip() or completed.stdout.strip()
            raise SystemExit(
                f"distribution acceptance: {feature} does not compile in isolation"
                + (f"\n{details}" if details else "")
            )


def _ncm_failure(message: str) -> None:
    raise SystemExit(f"distribution acceptance: NCM {message}")


def _load_worker_platform_policy_module() -> Any:
    """Load the shared NCM platform-policy validator without package imports."""
    module_path = Path(__file__).with_name("product") / "ncm" / "worker_platform_policy.py"
    spec = importlib.util.spec_from_file_location(
        "tracedecay_distribution_worker_platform_policy", module_path
    )
    if spec is None or spec.loader is None:
        _ncm_failure(f"worker platform policy validator is unavailable: {module_path}")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as error:
        _ncm_failure(f"worker platform policy validator could not load: {error}")
    return module


def _ncm_feature_members(manifest: dict[str, Any], feature: str, label: str) -> set[str]:
    features = manifest.get("features")
    if not isinstance(features, dict):
        _ncm_failure(f"{label} has no feature table")
    members = features.get(feature)
    if not isinstance(members, list) or not all(isinstance(item, str) for item in members):
        _ncm_failure(f"{label} feature {feature!r} must be a list of feature members")
    return set(members)


def _ncm_required_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        _ncm_failure(f"{label} must be a non-empty string")
    return value


def _ncm_required_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _ncm_failure(f"{label} must be a positive integer")
    return value


def _require_ncm_runtime_policy(policy: dict[str, Any]) -> dict[str, Any]:
    runtime_policy = policy.get("runtime_policy")
    if not isinstance(runtime_policy, dict):
        _ncm_failure(
            "supported host capability has no runtime_policy for its encoder and artifacts"
        )
    for key, expected in NCM_RUNTIME_POLICY.items():
        if runtime_policy.get(key) != expected:
            _ncm_failure(
                f"runtime_policy.{key} must be {expected!r} for the verified arm64 macOS sidecar"
            )
    if policy.get("worker_manifest") != runtime_policy["worker_manifest"]:
        _ncm_failure("runtime_policy.worker_manifest differs from worker_manifest")
    packaging = policy.get("packaging")
    if not isinstance(packaging, dict):
        _ncm_failure("packaging must describe the worker artifact")
    if packaging.get("worker_distribution") != runtime_policy["worker_distribution"]:
        _ncm_failure(
            "packaging.worker_distribution must match runtime_policy.worker_distribution"
        )
    return runtime_policy


def _require_ncm_target_matrix(
    policy: dict[str, Any], release_target_manifest: dict[str, Any]
) -> None:
    supported = policy.get("supported_targets")
    if not isinstance(supported, list) or len(supported) != 1:
        _ncm_failure(
            "distribution matrix must have exactly one supported target: arm64 macOS V2"
        )
    supported_entry = supported[0]
    if not isinstance(supported_entry, dict):
        _ncm_failure("supported_targets[0] must be an object")
    expected_supported = {
        "target": NCM_SUPPORTED_TARGET,
        "release_name": NCM_SUPPORTED_RELEASE,
        "status": "supported",
    }
    for key, expected in expected_supported.items():
        if supported_entry.get(key) != expected:
            _ncm_failure(
                f"supported_targets[0].{key} must be {expected!r} for the verified "
                "arm64 macOS sidecar"
            )

    release_entries = policy.get("release_targets")
    if not isinstance(release_entries, list) or not release_entries:
        _ncm_failure("release_targets must be a non-empty list")
    release_by_target: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(release_entries):
        if not isinstance(entry, dict):
            _ncm_failure(f"release_targets[{index}] must be an object")
        target = _ncm_required_string(entry.get("target"), f"release_targets[{index}].target")
        if target in release_by_target:
            _ncm_failure(f"release_targets contains duplicate target {target}")
        release_by_target[target] = entry
        expected_status = (
            "supported" if target == NCM_SUPPORTED_TARGET else "native-only"
        )
        if entry.get("ncm") != expected_status:
            _ncm_failure(
                f"release target {target} must declare NCM {expected_status!r}; "
                "only arm64 macOS V2 has a verified worker sidecar"
            )
    if NCM_SUPPORTED_TARGET not in release_by_target:
        _ncm_failure("the supported arm64 macOS target is missing from release_targets")

    include = release_target_manifest.get("include")
    if not isinstance(include, list) or not include:
        _ncm_failure("release target manifest.include must be a non-empty list")
    current_targets: set[str] = set()
    for index, entry in enumerate(include):
        if not isinstance(entry, dict):
            _ncm_failure(f"release target manifest.include[{index}] must be an object")
        target = _ncm_required_string(
            entry.get("target"), f"release target manifest.include[{index}].target"
        )
        if target in current_targets:
            _ncm_failure(f"release target manifest contains duplicate target {target}")
        current_targets.add(target)
        if target not in release_by_target:
            _ncm_failure(
                f"current release target {target} has no explicit Native/NCM policy row"
            )
    policy_targets = set(release_by_target)
    if policy_targets != current_targets:
        missing = sorted(current_targets - policy_targets)
        extra = sorted(policy_targets - current_targets)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("extra " + ", ".join(extra))
        _ncm_failure(
            "release target policy must cover exactly the current release matrix: "
            + "; ".join(details)
        )


def _require_ncm_worker_artifact(
    policy: dict[str, Any], worker_manifest: dict[str, Any]
) -> None:
    worker_name = policy.get("worker")
    if worker_name != "tracedecay-ncm-worker":
        _ncm_failure("worker policy must name tracedecay-ncm-worker")
    targets = worker_manifest.get("targets")
    if not isinstance(targets, list) or len(targets) != 1:
        _ncm_failure(
            "worker artifact policy must pin exactly one verified arm64 macOS sidecar"
        )
    target = targets[0]
    if not isinstance(target, dict):
        _ncm_failure("worker manifest target must be an object")
    expected = {
        "triple": NCM_SUPPORTED_TARGET,
        "os": "macos",
        "arch": "aarch64",
        "family": "unix",
    }
    for key, value in expected.items():
        if target.get(key) != value:
            _ncm_failure(
                f"worker manifest target {key} must be {value!r} for the verified "
                "arm64 macOS sidecar"
            )
    _ncm_required_positive_int(target.get("bytes"), "worker manifest target bytes")
    digest = _ncm_required_string(target.get("sha256"), "worker manifest target sha256")
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        _ncm_failure("worker manifest target sha256 must be lowercase hexadecimal")


def _require_ncm_release_workflow(path: Path) -> None:
    try:
        workflow = path.read_text(encoding="utf-8")
    except OSError as error:
        _ncm_failure(f"release workflow cannot be read ({path}): {error}")
    missing = [snippet for snippet in NCM_RELEASE_WORKFLOW_SNIPPETS if snippet not in workflow]
    if missing:
        _ncm_failure(
            f"release workflow {path.name} does not implement the verified "
            "sidecar/archive/checksum contract: "
            + "; ".join(missing)
        )
    macos_guards = workflow.count("if: matrix.name == 'aarch64-macos'")
    if macos_guards < 3:
        _ncm_failure(
            f"release workflow {path.name} must guard worker build, packaging, and "
            "upload with the arm64 macOS target"
        )


def _require_ncm_model_contract(
    model_manifest: dict[str, Any], source_manifest: dict[str, Any] | None
) -> None:
    for key, expected in NCM_MODEL_CONTRACT.items():
        if model_manifest.get(key) != expected:
            _ncm_failure(
                f"pinned model contract field {key} must be {expected!r}"
            )
    provenance = model_manifest.get("revision_provenance")
    if not isinstance(provenance, str) or not provenance.startswith("product/ncm/receipts/"):
        _ncm_failure("pinned model contract must include a receipt-backed revision provenance")

    files = model_manifest.get("files")
    if not isinstance(files, list) or not files:
        _ncm_failure("pinned model contract files must be a non-empty list")
    files_by_path: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(files):
        if not isinstance(entry, dict):
            _ncm_failure(f"pinned model contract files[{index}] must be an object")
        path = _ncm_required_string(entry.get("path"), f"pinned model contract files[{index}].path")
        if path in files_by_path:
            _ncm_failure(f"pinned model contract contains duplicate file {path}")
        files_by_path[path] = entry
        _ncm_required_positive_int(
            entry.get("bytes"), f"pinned model contract files[{index}].bytes"
        )
        digest = _ncm_required_string(
            entry.get("sha256"), f"pinned model contract files[{index}].sha256"
        )
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            _ncm_failure(
                f"pinned model contract files[{index}].sha256 must be lowercase hexadecimal"
            )
    if set(files_by_path) != NCM_MODEL_FILES:
        missing = sorted(NCM_MODEL_FILES - set(files_by_path))
        extra = sorted(set(files_by_path) - NCM_MODEL_FILES)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("extra " + ", ".join(extra))
        _ncm_failure("pinned model contract file set drift: " + "; ".join(details))

    if source_manifest is None:
        return
    source_model = source_manifest.get("model")
    if not isinstance(source_model, dict):
        _ncm_failure("source-manifest.json has no model contract")
    source_matches = {
        "rust_repository": model_manifest["repository"],
        "rust_revision": model_manifest["revision"],
        "artifact_sha256": files_by_path["onnx/model.onnx"]["sha256"],
        "max_seq_length": model_manifest["max_length"],
        "pooling": model_manifest["pooling"],
        "normalize": model_manifest["normalize"],
    }
    for key, expected in source_matches.items():
        if source_model.get(key) != expected:
            _ncm_failure(
                f"source-manifest.json model field {key} differs from the pinned model contract"
            )


def _require_ncm_runtime_features(
    root_manifest: dict[str, Any],
    provider_manifest: dict[str, Any],
    runtime_manifest: dict[str, Any],
    runtime_policy: dict[str, Any],
) -> None:
    provider_package = runtime_policy["provider_package"]
    provider_feature = runtime_policy["provider_feature"]
    runtime_package = runtime_policy["runtime_package"]
    runtime_feature = runtime_policy["runtime_feature"]

    root_features = _ncm_feature_members(
        root_manifest, "memory-provider-host", "root manifest"
    )
    if f"dep:{provider_package}" not in root_features:
        _ncm_failure(
            "host capability claims NCM but memory-provider-host does not mount "
            f"dep:{provider_package}"
        )
    root_dependencies = root_manifest.get("dependencies")
    root_dependency = (
        root_dependencies.get(provider_package)
        if isinstance(root_dependencies, dict)
        else None
    )
    if not isinstance(root_dependency, dict) or root_dependency.get("optional") is not True:
        _ncm_failure(
            "host capability claims NCM but its provider dependency is not optional"
        )
    root_provider_features = root_dependency.get("features")
    if (
        not isinstance(root_provider_features, list)
        or provider_feature not in root_provider_features
    ):
        _ncm_failure(
            "host capability claims NCM but the provider dependency does not enable "
            f"{provider_feature!r}"
        )

    if provider_manifest.get("package", {}).get("name") != provider_package:
        _ncm_failure("provider manifest package name differs from runtime_policy.provider_package")
    provider_feature_members = _ncm_feature_members(
        provider_manifest, provider_feature, "NCM provider manifest"
    )
    runtime_edges = {
        f"{runtime_package}/{runtime_feature}",
        f"{runtime_package}?/{runtime_feature}",
    }
    if not provider_feature_members & runtime_edges:
        _ncm_failure(
            "host capability claims NCM but "
            f"{provider_package}/{provider_feature} does not enable "
            f"{runtime_package}/{runtime_feature}"
        )
    provider_dependencies = provider_manifest.get("dependencies")
    runtime_dependency = (
        provider_dependencies.get(runtime_package)
        if isinstance(provider_dependencies, dict)
        else None
    )
    if not isinstance(runtime_dependency, dict):
        _ncm_failure(
            f"NCM provider manifest has no optional {runtime_package} dependency"
        )
    if runtime_dependency.get("optional") is not True:
        _ncm_failure(f"NCM provider dependency {runtime_package} must remain optional")
    if runtime_dependency.get("default-features") is not False:
        _ncm_failure(
            f"NCM provider dependency {runtime_package} must disable default features"
        )

    if runtime_manifest.get("package", {}).get("name") != runtime_package:
        _ncm_failure("runtime manifest package name differs from runtime_policy.runtime_package")
    encoder_members = _ncm_feature_members(
        runtime_manifest, runtime_feature, "NCM runtime manifest"
    )
    if "dep:fastembed" not in encoder_members:
        _ncm_failure(
            "host capability claims NCM but the runtime encoder feature does not "
            "enable dep:fastembed"
        )
    runtime_dependencies = runtime_manifest.get("dependencies")
    fastembed = (
        runtime_dependencies.get("fastembed")
        if isinstance(runtime_dependencies, dict)
        else None
    )
    if not isinstance(fastembed, dict) or fastembed.get("optional") is not True:
        _ncm_failure("NCM runtime encoder dependency fastembed must be optional")
    if fastembed.get("default-features") is not False:
        _ncm_failure("NCM runtime encoder dependency fastembed must disable default features")


def validate_ncm_distribution_matrix(
    policy_path: Path,
    *,
    worker_manifest_path: Path,
    model_manifest_path: Path,
    release_target_manifest_path: Path,
    root_manifest_path: Path,
    provider_manifest_path: Path,
    runtime_manifest_path: Path,
    source_manifest_path: Path | None = None,
    release_workflow_paths: list[Path] | None = None,
) -> None:
    """Validate NCM's explicit sidecar/native-only distribution matrix.

    The platform-policy checker owns the general worker policy rules. This
    gate adds the release-specific V2 contract: exactly arm64 macOS may claim
    the verified sidecar, its model pin must be present, and the host feature
    must reach the real encoder runtime feature.
    """
    policy_module = _load_worker_platform_policy_module()
    try:
        policy_module.validate_worker_platform_policy(
            policy_path,
            worker_manifest_path=worker_manifest_path,
            release_target_manifest_path=release_target_manifest_path,
        )
    except Exception as error:
        _ncm_failure(f"worker platform policy is invalid: {error}")

    policy = load_json(policy_path, "NCM worker platform policy")
    worker_manifest = load_json(worker_manifest_path, "NCM worker manifest")
    model_manifest = load_json(model_manifest_path, "NCM model manifest")
    release_target_manifest = load_json(
        release_target_manifest_path, "release target manifest"
    )
    root_manifest = load(root_manifest_path)
    provider_manifest = load(provider_manifest_path)
    runtime_manifest = load(runtime_manifest_path)
    source_manifest = (
        load_json(source_manifest_path, "NCM source manifest")
        if source_manifest_path is not None
        else None
    )

    runtime_policy = _require_ncm_runtime_policy(policy)
    _require_ncm_target_matrix(policy, release_target_manifest)
    _require_ncm_worker_artifact(policy, worker_manifest)
    _require_ncm_model_contract(model_manifest, source_manifest)
    _require_ncm_runtime_features(
        root_manifest, provider_manifest, runtime_manifest, runtime_policy
    )
    for workflow_path in release_workflow_paths or []:
        _require_ncm_release_workflow(workflow_path)


def validate(
    root_source: dict,
    root_packaged: dict,
    code_index_source: dict,
    code_index_packaged: dict,
    extraction_source: dict,
    extraction_packaged: dict,
    cli_source: dict,
    cli_packaged: dict,
) -> None:
    root_features = require_matching_features("root", root_source, root_packaged)
    missing = sorted(REQUIRED_ROOT_FEATURES - root_features.keys())
    if missing:
        raise SystemExit(
            "distribution acceptance: source manifest is missing required features: "
            + ", ".join(missing)
        )
    require_optional_dependencies_wired("root", root_packaged, root_features)

    require_matching_features(
        "tracedecay-code-index", code_index_source, code_index_packaged
    )
    extraction_features = require_matching_features(
        "tracedecay-code-extraction", extraction_source, extraction_packaged
    )
    require_language_surface(
        "root", root_features, language_feature_names(extraction_features)
    )
    require_optional_dependencies_wired(
        "tracedecay-code-extraction", extraction_packaged, extraction_features
    )

    cli_features = require_matching_features(
        "tracedecay-cli", cli_source, cli_packaged
    )
    missing_cli = sorted(
        REQUIRED_CLI_FEATURE_MEMBERS.keys() - cli_features.keys()
    )
    if missing_cli:
        raise SystemExit(
            "distribution acceptance: tracedecay-cli is missing required features: "
            + ", ".join(missing_cli)
        )
    for feature, expected in REQUIRED_CLI_FEATURE_MEMBERS.items():
        members = cli_features.get(feature)
        # Extra crate passthroughs are allowed; the contract is the required
        # Hotpath/release members, not an exhaustive crate inventory.
        if not isinstance(members, list) or not expected.issubset(members):
            raise SystemExit(
                f"distribution acceptance: tracedecay-cli {feature} must enable "
                + ", ".join(sorted(expected))
            )
    require_optional_dependencies_wired(
        "tracedecay-cli", cli_packaged, cli_features
    )


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    root_manifest = repo / "crates/tracedecay/Cargo.toml"
    code_index_manifest = repo / "crates/tracedecay-code-index/Cargo.toml"
    extraction_manifest = repo / "crates/tracedecay-code-extraction/Cargo.toml"
    cli_manifest = repo / "crates/tracedecay-cli/Cargo.toml"
    ncm_policy = repo / "product/ncm/reference/worker-platforms.json"
    ncm_worker_manifest = repo / "product/ncm/reference/worker-manifest.json"
    ncm_model_manifest = repo / "product/ncm/reference/embedding-manifest.json"
    ncm_release_targets = repo / ".github/release-targets.json"
    ncm_provider_manifest = repo / "crates/tracedecay-memory-provider-ncm/Cargo.toml"
    ncm_runtime_manifest = repo / "crates/tracedecay-memory-ncm-runtime/Cargo.toml"
    ncm_source_manifest = repo / "product/ncm/reference/source-manifest.json"
    ncm_release_workflows = [
        repo / ".github/workflows/release.yml",
        repo / ".github/workflows/release-beta.yml",
    ]
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Source manifests default to this checkout; packaged manifests must be
    # the extracted `.crate` trees, so they have no default — comparing a
    # manifest with itself is not package verification.
    parser.add_argument("--root-source", type=Path, default=root_manifest)
    parser.add_argument("--root-packaged", type=Path, required=True)
    parser.add_argument("--code-index-source", type=Path, default=code_index_manifest)
    parser.add_argument("--code-index-packaged", type=Path, required=True)
    parser.add_argument("--extraction-source", type=Path, default=extraction_manifest)
    parser.add_argument("--extraction-packaged", type=Path, required=True)
    parser.add_argument("--cli-source", type=Path, default=cli_manifest)
    parser.add_argument("--cli-packaged", type=Path, required=True)
    parser.add_argument("--check-extraction-manifest", type=Path)
    parser.add_argument("--cargo-config", type=Path)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--ncm-platform-policy", type=Path)
    parser.add_argument("--ncm-worker-manifest", type=Path)
    parser.add_argument("--ncm-model-manifest", type=Path)
    parser.add_argument("--ncm-release-targets", type=Path)
    parser.add_argument("--ncm-provider-manifest", type=Path)
    parser.add_argument("--ncm-runtime-manifest", type=Path)
    parser.add_argument("--ncm-source-manifest", type=Path)
    parser.add_argument("--ncm-release-workflow", type=Path, action="append")
    arguments = parser.parse_args()
    root_source = load(arguments.root_source)
    extraction_packaged = load(arguments.extraction_packaged)
    validate(
        root_source,
        load(arguments.root_packaged),
        load(arguments.code_index_source),
        load(arguments.code_index_packaged),
        load(arguments.extraction_source),
        extraction_packaged,
        load(arguments.cli_source),
        load(arguments.cli_packaged),
    )
    if arguments.check_extraction_manifest is not None:
        require_isolated_language_features_compile(
            arguments.check_extraction_manifest,
            language_feature_names(extraction_packaged.get("features", {})),
            arguments.cargo_config,
            arguments.offline,
        )
    # The archive acceptance script supplies a source manifest from this
    # checkout. Fixture callers use temporary source manifests and therefore
    # opt out unless they pass an explicit NCM policy path.
    ncm_policy_path = arguments.ncm_platform_policy
    if ncm_policy_path is None and arguments.root_source.resolve() == root_manifest.resolve():
        ncm_policy_path = ncm_policy
    if ncm_policy_path is not None:
        validate_ncm_distribution_matrix(
            ncm_policy_path,
            worker_manifest_path=arguments.ncm_worker_manifest or ncm_worker_manifest,
            model_manifest_path=arguments.ncm_model_manifest or ncm_model_manifest,
            release_target_manifest_path=arguments.ncm_release_targets or ncm_release_targets,
            root_manifest_path=arguments.root_source,
            provider_manifest_path=arguments.ncm_provider_manifest or ncm_provider_manifest,
            runtime_manifest_path=arguments.ncm_runtime_manifest or ncm_runtime_manifest,
            source_manifest_path=(
                arguments.ncm_source_manifest
                if arguments.ncm_source_manifest is not None
                else ncm_source_manifest
                if ncm_policy_path.resolve() == ncm_policy.resolve()
                and ncm_source_manifest.exists()
                else None
            ),
            release_workflow_paths=(
                arguments.ncm_release_workflow
                if arguments.ncm_release_workflow is not None
                else ncm_release_workflows
                if ncm_policy_path.resolve() == ncm_policy.resolve()
                else None
            ),
        )
    print("distribution feature wiring is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
