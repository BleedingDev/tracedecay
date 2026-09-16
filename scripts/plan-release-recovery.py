#!/usr/bin/env python3
"""Plan release work without rebuilding already-published immutable artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


WORKER_POLICY_NAME = "tracedecay-ncm-worker"
WORKER_MANIFEST_NAME = "worker-manifest.json"


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid {label}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit(f"{label} must be a JSON object")
    return value


def _worker_platform_path(manifest: Path, supplied: Path | None) -> Path | None:
    if supplied is not None:
        return supplied
    candidate = manifest.resolve().parent.parent / "product/ncm/reference/worker-platforms.json"
    return candidate if candidate.is_file() else None


def _validate_worker_platforms(
    targets: list[dict[str, Any]],
    manifest: Path,
    supplied: Path | None,
) -> None:
    has_ncm_metadata = any("ncm" in target or "sidecar" in target for target in targets)
    policy_path = _worker_platform_path(manifest, supplied)
    if policy_path is None:
        if supplied is not None or has_ncm_metadata:
            raise SystemExit("NCM worker platform policy is missing")
        return
    policy = _load_json(policy_path, "NCM worker platform policy")
    if policy.get("provider_id") != "ncm":
        raise SystemExit("NCM worker platform policy provider_id must be 'ncm'")
    worker = policy.get("worker")
    if not isinstance(worker, str) or not worker:
        raise SystemExit("NCM worker platform policy.worker must be a non-empty string")
    if worker != WORKER_POLICY_NAME:
        raise SystemExit("NCM worker platform policy names an unsupported worker")
    packaging = policy.get("packaging")
    if not isinstance(packaging, dict):
        raise SystemExit("NCM worker platform policy.packaging must be an object")
    if packaging.get("worker_distribution") != "separate-sidecar":
        raise SystemExit("NCM worker platform policy requires separate-sidecar packaging")
    if packaging.get("standard_cli_archive_includes_worker") is not False:
        raise SystemExit("NCM worker policy must keep the worker out of CLI archives")
    if packaging.get("manifest_sidecar_required") is not True:
        raise SystemExit("NCM worker policy requires a sidecar manifest")
    rows = policy.get("release_targets")
    if not isinstance(rows, list) or not rows:
        raise SystemExit("NCM worker platform policy has no release target rows")
    by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise SystemExit("NCM worker platform policy release target must be an object")
        name = row.get("name")
        target = row.get("target")
        status = row.get("ncm")
        if not isinstance(name, str) or not isinstance(target, str):
            raise SystemExit("NCM worker platform policy release rows need name and target")
        if status not in {"supported", "native-only"}:
            raise SystemExit(f"invalid NCM status for release target {name}: {status!r}")
        if name in by_name:
            raise SystemExit(f"duplicate NCM release target: {name}")
        by_name[name] = row
    matrix_names = {target["name"] for target in targets}
    if matrix_names != set(by_name):
        raise SystemExit("NCM policy/release target mismatch")
    for target in targets:
        name = target["name"]
        row = by_name[name]
        if row["target"] != target["target"]:
            raise SystemExit(f"NCM policy target differs for release target {name}")
        if target.get("ncm") != row["ncm"]:
            raise SystemExit(f"release target {name} NCM status differs from policy")
        sidecar = target.get("sidecar")
        if row["ncm"] == "supported":
            if not isinstance(sidecar, dict) or sidecar.get("worker") != worker:
                raise SystemExit(f"supported NCM target {name} has invalid sidecar metadata")
            policy_manifest = policy.get("worker_manifest")
            if isinstance(policy_manifest, str) and Path(policy_manifest).name != sidecar.get("manifest"):
                raise SystemExit(f"NCM sidecar manifest differs from policy for {name}")
        elif sidecar is not None:
            raise SystemExit(f"native-only target {name} must not publish an NCM sidecar")


def _validate_sidecar_metadata(target: dict[str, Any]) -> None:
    status = target.get("ncm")
    sidecar = target.get("sidecar")
    if status is not None and status not in {"supported", "native-only"}:
        raise SystemExit(f"invalid NCM status for release target {target['name']}")
    if sidecar is None:
        if status == "supported":
            raise SystemExit(f"supported NCM target {target['name']} has no sidecar metadata")
        return
    if not isinstance(sidecar, dict):
        raise SystemExit(f"release target {target['name']} sidecar must be an object")
    required = ("worker", "archive", "manifest", "checksum")
    if any(not isinstance(sidecar.get(field), str) or not sidecar[field] for field in required):
        raise SystemExit(f"release target {target['name']} sidecar is missing metadata")
    if sidecar["archive"] != "tar.gz" or sidecar["checksum"] != "sha256":
        raise SystemExit(f"release target {target['name']} has invalid NCM sidecar format")
    if sidecar["worker"] != WORKER_POLICY_NAME:
        raise SystemExit(f"release target {target['name']} has an unsupported NCM worker")
    if sidecar["manifest"] != WORKER_MANIFEST_NAME:
        raise SystemExit(f"release target {target['name']} has an invalid NCM worker manifest")
    if status != "supported":
        raise SystemExit(f"release target {target['name']} sidecar is not supported by its NCM status")


def load_targets(path: Path, worker_platforms: Path | None = None) -> list[dict[str, Any]]:
    value = _load_json(path, "release target manifest")
    targets = value.get("include")
    if not isinstance(targets, list) or not targets:
        raise SystemExit("release target manifest has no targets")
    names: set[str] = set()
    for target in targets:
        if not isinstance(target, dict):
            raise SystemExit("release target must be an object")
        required = ("name", "runner", "target", "archive")
        if any(not isinstance(target.get(field), str) or not target[field] for field in required):
            raise SystemExit("release target is missing required string fields")
        if target["archive"] not in {"tar.gz", "zip"}:
            raise SystemExit(f"unsupported release archive: {target['archive']}")
        if target["name"] in names:
            raise SystemExit(f"duplicate release target: {target['name']}")
        names.add(target["name"])
        _validate_sidecar_metadata(target)
    _validate_worker_platforms(targets, path, worker_platforms)
    return targets


def sidecar_assets(target: dict[str, Any], tag: str, profile: str) -> tuple[str, ...]:
    sidecar = target.get("sidecar")
    if sidecar is None:
        return ()
    prefix = str(sidecar["worker"])
    if profile == "beta":
        prefix += "-beta"
    archive = f"{prefix}-{tag}-{target['name']}.{sidecar['archive']}"
    return archive, f"{archive}.sha256"


def target_assets(target: dict[str, Any], tag: str, profile: str) -> tuple[str, ...]:
    name = target["name"]
    archive = target["archive"]
    if profile == "beta":
        assets = (
            f"tracedecay-beta-{tag}-{name}.{archive}",
            f"tracedecay-beta-{tag}-{name}.mcpb",
        )
    else:
        assets = (
            f"tracedecay-{tag}-{name}.{archive}",
            f"tracedecay-{tag}-{name}.mcpb",
        )
    return assets + sidecar_assets(target, tag, profile)


def plan(
    targets: list[dict[str, Any]],
    tag: str,
    profile: str,
    existing: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    expected_mutable = {
        asset
        for target in targets
        for asset in target_assets(target, tag, profile)
    }
    fixed = {"SHA256SUMS", "install.sh"} if profile == "stable" else {"SHA256SUMS"}
    unexpected = sorted(existing - expected_mutable - fixed)
    if unexpected:
        raise SystemExit("unexpected existing release assets: " + ", ".join(unexpected))

    missing_targets = [
        target
        for target in targets
        if any(asset not in existing for asset in target_assets(target, tag, profile))
    ]
    finalized = sorted(existing & fixed)
    if finalized and missing_targets:
        raise SystemExit(
            "final release metadata exists before all immutable artifacts "
            f"({', '.join(finalized)}); refusing destructive recovery"
        )
    retained = sorted(existing & expected_mutable)
    return missing_targets, retained


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--worker-platforms", type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--profile", choices=("stable", "beta"), default="stable")
    parser.add_argument("--asset-names", type=Path, required=True)
    parser.add_argument("--retained-output", type=Path, required=True)
    parser.add_argument("--github-output", type=Path, required=True)
    arguments = parser.parse_args()

    targets = load_targets(arguments.manifest, arguments.worker_platforms)
    existing = {
        line.strip()
        for line in arguments.asset_names.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    missing, retained = plan(targets, arguments.tag, arguments.profile, existing)
    arguments.retained_output.write_text(
        "".join(f"{asset}\n" for asset in retained),
        encoding="utf-8",
    )
    matrix = json.dumps({"include": missing}, separators=(",", ":"))
    with arguments.github_output.open("a", encoding="utf-8") as output:
        output.write(f"matrix={matrix}\n")
        output.write(f"build_required={'true' if missing else 'false'}\n")


if __name__ == "__main__":
    main()
