"""Validation and target lookup for the NCM worker platform policy."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class WorkerPlatformPolicyError(ValueError):
    """The NCM worker platform policy is malformed or inconsistent."""


WORKER_MANIFEST_PATH = "product/ncm/reference/worker-manifest.json"
MODEL_ACQUISITION_MANIFEST_PATH = "product/ncm/release/model-acquisition-manifest.json"
WORKER_MANIFEST_NAME = "worker-manifest.json"
MODEL_ACQUISITION_MANIFEST_NAME = "model-acquisition-manifest.json"
EXPLICIT_INTEL_MAC_TARGET = "x86_64-apple-darwin"


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise WorkerPlatformPolicyError(f"invalid {label}: {error}") from error
    if not isinstance(value, dict):
        raise WorkerPlatformPolicyError(f"{label} must be a JSON object")
    return value


def _required_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise WorkerPlatformPolicyError(f"{label} must be a non-empty string")
    return value


def _required_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WorkerPlatformPolicyError(f"{label} must be a positive integer")
    return value


def _entries(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise WorkerPlatformPolicyError(f"{label} must be a non-empty list")
    entries: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise WorkerPlatformPolicyError(f"{label}[{index}] must be an object")
        entries.append(item)
    return entries


def _unique_targets(
    entries: list[dict[str, Any]], label: str, *, field: str = "target"
) -> set[str]:
    targets: set[str] = set()
    for index, entry in enumerate(entries):
        target = _required_string(entry.get(field), f"{label}[{index}].{field}")
        if target in targets:
            raise WorkerPlatformPolicyError(f"duplicate {label} target: {target}")
        targets.add(target)
    return targets


def _load_release_targets(path: Path) -> list[dict[str, Any]]:
    manifest = _load_object(path, "release target manifest")
    return _entries(manifest.get("include"), "release target manifest.include")


def validate_worker_platform_policy(
    policy_path: Path,
    *,
    worker_manifest_path: Path | None = None,
    release_target_manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Validate the policy against the pinned worker and release matrices.

    The returned object is the parsed policy. Validation intentionally requires
    the supported target set to equal the pinned worker manifest target set;
    adding a worker artifact without updating this policy is therefore a
    release-check failure instead of an accidental capability advertisement.
    """

    policy = _load_object(policy_path, "worker platform policy")
    if policy.get("schema_version") != 1:
        raise WorkerPlatformPolicyError("worker platform policy schema_version must be 1")
    if policy.get("provider_id") != "ncm":
        raise WorkerPlatformPolicyError("worker platform policy provider_id must be 'ncm'")
    worker_name = _required_string(policy.get("worker"), "worker platform policy.worker")
    if policy.get("policy") != "pinned-artifact-only":
        raise WorkerPlatformPolicyError(
            "worker platform policy.policy must be 'pinned-artifact-only'"
        )
    if policy.get("fallback") != "native-only":
        raise WorkerPlatformPolicyError(
            "worker platform policy.fallback must be 'native-only'"
        )
    if policy.get("worker_manifest") != WORKER_MANIFEST_PATH:
        raise WorkerPlatformPolicyError(
            "worker platform policy.worker_manifest must name the trusted worker manifest"
        )
    if policy.get("model_acquisition_manifest") != MODEL_ACQUISITION_MANIFEST_PATH:
        raise WorkerPlatformPolicyError(
            "worker platform policy.model_acquisition_manifest must name the pinned model acquisition manifest"
        )
    runtime_policy = policy.get("runtime_policy")
    if not isinstance(runtime_policy, dict):
        raise WorkerPlatformPolicyError(
            "worker platform policy.runtime_policy must be an object"
        )
    if runtime_policy.get("worker_manifest") != WORKER_MANIFEST_PATH:
        raise WorkerPlatformPolicyError(
            "worker platform policy.runtime_policy.worker_manifest must name the trusted worker manifest"
        )
    if runtime_policy.get("model_manifest") != "product/ncm/reference/embedding-manifest.json":
        raise WorkerPlatformPolicyError(
            "worker platform policy.runtime_policy.model_manifest must name the canonical model manifest"
        )
    if runtime_policy.get("model_acquisition_manifest") != MODEL_ACQUISITION_MANIFEST_PATH:
        raise WorkerPlatformPolicyError(
            "worker platform policy.runtime_policy.model_acquisition_manifest must name the pinned model acquisition manifest"
        )
    packaging = policy.get("packaging")
    if not isinstance(packaging, dict):
        raise WorkerPlatformPolicyError("worker platform policy.packaging must be an object")
    if packaging.get("worker_distribution") != "separate-sidecar":
        raise WorkerPlatformPolicyError(
            "worker platform policy.packaging.worker_distribution must be 'separate-sidecar'"
        )
    if packaging.get("standard_cli_archive_includes_worker") is not False:
        raise WorkerPlatformPolicyError(
            "worker platform policy.packaging.standard_cli_archive_includes_worker must be false"
        )
    if packaging.get("manifest_sidecar_required") is not True:
        raise WorkerPlatformPolicyError(
            "worker platform policy.packaging.manifest_sidecar_required must be true"
        )
    if packaging.get("model_acquisition_manifest_sidecar_required") is not True:
        raise WorkerPlatformPolicyError(
            "worker platform policy.packaging.model_acquisition_manifest_sidecar_required must be true"
        )

    supported = _entries(policy.get("supported_targets"), "supported_targets")
    unsupported = _entries(policy.get("unsupported_targets"), "unsupported_targets")
    supported_targets = _unique_targets(supported, "supported_targets")
    unsupported_targets = _unique_targets(unsupported, "unsupported_targets")
    overlap = sorted(supported_targets & unsupported_targets)
    if overlap:
        raise WorkerPlatformPolicyError(
            "target is both supported and unsupported: " + ", ".join(overlap)
        )

    for index, entry in enumerate(supported):
        release_name = _required_string(
            entry.get("release_name"), f"supported_targets[{index}].release_name"
        )
        if entry.get("status") != "supported":
            raise WorkerPlatformPolicyError(
                f"supported_targets[{index}].status must be 'supported'"
            )
    for index, entry in enumerate(unsupported):
        _required_string(entry.get("reason"), f"unsupported_targets[{index}].reason")
    if EXPLICIT_INTEL_MAC_TARGET not in unsupported_targets:
        raise WorkerPlatformPolicyError(
            "Intel macOS must have an explicit unsupported NCM policy row"
        )

    worker_manifest = _load_object(
        worker_manifest_path
        or policy_path.parent / "worker-manifest.json",
        "worker manifest",
    )
    if worker_manifest.get("worker") != worker_name:
        raise WorkerPlatformPolicyError(
            "worker platform policy.worker does not match worker manifest.worker"
        )
    if worker_manifest.get("schema_version") != 1:
        raise WorkerPlatformPolicyError("worker manifest schema_version must be 1")
    if worker_manifest.get("protocol_version") != 1:
        raise WorkerPlatformPolicyError("worker manifest protocol_version must be 1")
    if worker_manifest.get("protocol_identity") != "tracedecay.ncm.worker.v1":
        raise WorkerPlatformPolicyError(
            "worker manifest protocol_identity must be 'tracedecay.ncm.worker.v1'"
        )
    worker_targets = _entries(worker_manifest.get("targets"), "worker manifest.targets")
    for index, entry in enumerate(worker_targets):
        _required_string(entry.get("triple"), f"worker manifest.targets[{index}].triple")
        _required_string(entry.get("os"), f"worker manifest.targets[{index}].os")
        _required_string(entry.get("arch"), f"worker manifest.targets[{index}].arch")
        _required_string(entry.get("family"), f"worker manifest.targets[{index}].family")
        _required_positive_int(entry.get("bytes"), f"worker manifest.targets[{index}].bytes")
        digest = _required_string(
            entry.get("sha256"), f"worker manifest.targets[{index}].sha256"
        )
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise WorkerPlatformPolicyError(
                f"worker manifest.targets[{index}].sha256 must be lowercase hexadecimal"
            )
    worker_target_triples = _unique_targets(
        worker_targets, "worker manifest.targets", field="triple"
    )
    if worker_target_triples != supported_targets:
        missing = sorted(worker_target_triples - supported_targets)
        extra = sorted(supported_targets - worker_target_triples)
        details: list[str] = []
        if missing:
            details.append("unadvertised pinned targets: " + ", ".join(missing))
        if extra:
            details.append("supported targets without pins: " + ", ".join(extra))
        raise WorkerPlatformPolicyError("worker target policy mismatch: " + "; ".join(details))

    release_entries = _entries(policy.get("release_targets"), "release_targets")
    release_names: set[str] = set()
    release_targets_by_name: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(release_entries):
        name = _required_string(entry.get("name"), f"release_targets[{index}].name")
        if name in release_names:
            raise WorkerPlatformPolicyError(f"duplicate release target: {name}")
        release_names.add(name)
        release_targets_by_name[name] = entry
        target = _required_string(entry.get("target"), f"release_targets[{index}].target")
        status = entry.get("ncm")
        if status not in {"supported", "native-only"}:
            raise WorkerPlatformPolicyError(
                f"release_targets[{index}].ncm must be 'supported' or 'native-only'"
            )
        if status == "supported" and target not in supported_targets:
            raise WorkerPlatformPolicyError(
                f"release target {name} advertises NCM without a pinned worker target"
            )
        if status == "native-only" and target not in unsupported_targets:
            raise WorkerPlatformPolicyError(
                f"release target {name} lacks an explicit NCM unsupported entry"
            )

    release_target_manifest = _load_release_targets(
        release_target_manifest_path
        or policy_path.parents[3] / ".github" / "release-targets.json"
    )
    expected_release_names: set[str] = set()
    for index, entry in enumerate(release_target_manifest):
        name = _required_string(entry.get("name"), f"release target manifest.include[{index}].name")
        target = _required_string(entry.get("target"), f"release target manifest.include[{index}].target")
        expected_release_names.add(name)
        policy_entry = release_targets_by_name.get(name)
        if policy_entry is None:
            raise WorkerPlatformPolicyError(
                f"release target {name} is missing from NCM platform policy"
            )
        if policy_entry["target"] != target:
            raise WorkerPlatformPolicyError(
                f"release target {name} target differs between manifests"
            )
        release_status = entry.get("ncm")
        if release_status != policy_entry.get("ncm"):
            raise WorkerPlatformPolicyError(
                f"release target {name} NCM status differs between manifests"
            )
        sidecar = entry.get("sidecar")
        if policy_entry["ncm"] == "supported":
            if not isinstance(sidecar, dict):
                raise WorkerPlatformPolicyError(
                    f"supported release target {name} must carry sidecar metadata"
                )
            if sidecar.get("worker") != worker_name:
                raise WorkerPlatformPolicyError(
                    f"supported release target {name} sidecar worker differs from policy"
                )
            if sidecar.get("manifest") != WORKER_MANIFEST_NAME:
                raise WorkerPlatformPolicyError(
                    f"supported release target {name} sidecar must carry {WORKER_MANIFEST_NAME}"
                )
            if sidecar.get("model_manifest") != MODEL_ACQUISITION_MANIFEST_NAME:
                raise WorkerPlatformPolicyError(
                    f"supported release target {name} sidecar must carry {MODEL_ACQUISITION_MANIFEST_NAME}"
                )
        elif sidecar is not None:
            raise WorkerPlatformPolicyError(
                f"native-only release target {name} must not carry NCM sidecar metadata"
            )
    if expected_release_names != release_names:
        extra = sorted(release_names - expected_release_names)
        if extra:
            raise WorkerPlatformPolicyError(
                "NCM platform policy names unknown release targets: " + ", ".join(extra)
            )
    supported_release_names = {entry["release_name"] for entry in supported}
    release_supported_names = {
        entry["name"] for entry in release_entries if entry["ncm"] == "supported"
    }
    if supported_release_names != release_supported_names:
        missing = sorted(supported_release_names - release_supported_names)
        extra = sorted(release_supported_names - supported_release_names)
        details: list[str] = []
        if missing:
            details.append(
                "supported targets without supported release entries: " + ", ".join(missing)
            )
        if extra:
            details.append(
                "supported release entries without supported targets: " + ", ".join(extra)
            )
        raise WorkerPlatformPolicyError("NCM supported release mismatch: " + "; ".join(details))
    for entry in supported:
        release_name = entry["release_name"]
        if release_targets_by_name[release_name]["target"] != entry["target"]:
            raise WorkerPlatformPolicyError(
                f"supported target {entry['target']} differs from release target {release_name}"
            )

    return policy


def worker_platform_capability(policy: dict[str, Any], target: str) -> dict[str, str]:
    """Return the published capability for one Rust target triple."""

    _required_string(target, "target")
    supported = {
        entry["target"] for entry in policy["supported_targets"]
    }
    if target in supported:
        return {"target": target, "status": "supported", "fallback": "ncm-worker"}
    return {"target": target, "status": "unsupported", "fallback": "native-only"}
