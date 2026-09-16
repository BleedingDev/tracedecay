#!/usr/bin/env python3
"""Validate exact release asset coverage from the release target manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import stat
import tarfile
from typing import Any


REQUIRED_TARGET_FIELDS = ("name", "runner", "target", "archive")
SUPPORTED_ARCHIVES = {"tar.gz", "zip"}
WORKER_POLICY_NAME = "tracedecay-ncm-worker"
WORKER_MANIFEST_NAME = "worker-manifest.json"
WORKER_TARGET_TRIPLE = "aarch64-apple-darwin"


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
    root = manifest.resolve().parent.parent
    candidate = root / "product/ncm/reference/worker-platforms.json"
    return candidate if candidate.is_file() else None


def _validate_worker_platforms(
    targets: list[dict[str, Any]],
    manifest: Path,
    supplied: Path | None,
) -> None:
    """Keep sidecar rows synchronized with the NCM platform policy.

    The release matrix is the source of artifact names, while the NCM policy is
    the source of which target is allowed to carry the worker.  Requiring both
    rows to agree prevents a sidecar from becoming an accidental capability
    advertisement when the platform policy changes.
    """

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
    policy_rows = policy.get("release_targets")
    if not isinstance(policy_rows, list) or not policy_rows:
        raise SystemExit("NCM worker platform policy has no release target rows")
    by_name: dict[str, dict[str, Any]] = {}
    for row in policy_rows:
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
        missing = sorted(matrix_names - set(by_name))
        extra = sorted(set(by_name) - matrix_names)
        details: list[str] = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise SystemExit("NCM policy/release target mismatch: " + "; ".join(details))

    for target in targets:
        name = target["name"]
        policy_row = by_name[name]
        if policy_row["target"] != target["target"]:
            raise SystemExit(f"NCM policy target differs for release target {name}")
        status = target.get("ncm")
        if status != policy_row["ncm"]:
            raise SystemExit(f"release target {name} NCM status differs from policy")
        sidecar = target.get("sidecar")
        if status == "supported":
            if not isinstance(sidecar, dict):
                raise SystemExit(f"supported NCM target {name} has no sidecar metadata")
            if sidecar.get("worker") != worker:
                raise SystemExit(f"NCM sidecar worker differs from policy for {name}")
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
    if sidecar["archive"] != "tar.gz":
        raise SystemExit("NCM worker sidecars must use tar.gz archives")
    if sidecar["worker"] != WORKER_POLICY_NAME:
        raise SystemExit("NCM worker sidecars must package tracedecay-ncm-worker")
    if sidecar["manifest"] != WORKER_MANIFEST_NAME:
        raise SystemExit("NCM worker sidecars must package worker-manifest.json")
    if sidecar["checksum"] != "sha256":
        raise SystemExit("NCM worker sidecars must publish a sha256 checksum")
    if status != "supported":
        raise SystemExit(f"release target {target['name']} sidecar is not supported by its NCM status")


def target_matrix(path: Path, worker_platforms: Path | None = None) -> list[dict[str, Any]]:
    value = _load_json(path, "release target manifest")
    targets = value.get("include")
    if not isinstance(targets, list) or not targets:
        raise SystemExit("release target manifest has no targets")
    names: set[str] = set()
    for target in targets:
        if not isinstance(target, dict):
            raise SystemExit("release target must be an object")
        if any(
            not isinstance(target.get(field), str) or not target[field]
            for field in REQUIRED_TARGET_FIELDS
        ):
            raise SystemExit("release target is missing required string fields")
        if target["archive"] not in SUPPORTED_ARCHIVES:
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
    return (archive, f"{archive}.sha256")


def files(path: Path) -> set[str]:
    if not path.is_dir():
        raise SystemExit(f"release artifact directory is missing: {path}")
    result = {item.name for item in path.iterdir() if item.is_file() and item.stat().st_size}
    empty = sorted(item.name for item in path.iterdir() if item.is_file() and not item.stat().st_size)
    if empty:
        raise SystemExit("empty release artifacts: " + ", ".join(empty))
    return result


def require_exact(kind: str, actual: set[str], expected: set[str]) -> None:
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise SystemExit(f"{kind} coverage mismatch: {'; '.join(details)}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sidecar_checksums(path: Path, archives: set[str]) -> None:
    for archive in sorted(archives):
        checksum_path = path / f"{archive}.sha256"
        try:
            fields = checksum_path.read_text(encoding="utf-8").split()
        except (OSError, UnicodeDecodeError) as error:
            raise SystemExit(f"cannot read sidecar checksum {checksum_path}: {error}") from error
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-fA-F]{64}", fields[0]):
            raise SystemExit(f"invalid sidecar checksum format: {checksum_path.name}")
        checksum_name = fields[1].lstrip("*")
        if checksum_name != archive:
            raise SystemExit(
                f"sidecar checksum names {checksum_name!r}, expected {archive!r}"
            )
        actual = _sha256(path / archive)
        if fields[0].lower() != actual:
            raise SystemExit(
                f"sidecar checksum mismatch for {archive}: {fields[0]} != {actual}"
            )


def _load_worker_manifest(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise SystemExit(f"cannot read worker manifest {path}: {error}") from error
    if not raw:
        raise SystemExit(f"worker manifest is empty: {path}")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid worker manifest {path}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit("worker manifest must be a JSON object")
    if value.get("worker") != WORKER_POLICY_NAME:
        raise SystemExit(
            f"worker manifest names {value.get('worker')!r}, expected {WORKER_POLICY_NAME!r}"
        )
    targets = value.get("targets")
    if not isinstance(targets, list) or not targets:
        raise SystemExit("worker manifest has no target pins")
    seen: set[str] = set()
    for index, target in enumerate(targets):
        if not isinstance(target, dict):
            raise SystemExit(f"worker manifest target {index} must be an object")
        triple = target.get("triple")
        if not isinstance(triple, str) or not triple or triple in seen:
            raise SystemExit(f"worker manifest target {index} has an invalid or duplicate triple")
        seen.add(triple)
        byte_count = target.get("bytes")
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count <= 0:
            raise SystemExit(f"worker manifest target {triple} has an invalid byte count")
        digest = target.get("sha256")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise SystemExit(f"worker manifest target {triple} has an invalid sha256")
    if WORKER_TARGET_TRIPLE not in seen:
        raise SystemExit(f"worker manifest does not pin {WORKER_TARGET_TRIPLE}")
    return raw, value


def verify_sidecar_archives(
    path: Path,
    archives: set[str],
    targets: list[dict[str, Any]],
    worker_manifest: Path,
) -> None:
    """Verify sidecar contents against the checked-in worker trust root."""
    manifest_bytes, manifest = _load_worker_manifest(worker_manifest)
    manifest_targets = {
        target["triple"]: target for target in manifest["targets"]
    }
    expected_archives = {}
    for target in targets:
        sidecar = target.get("sidecar")
        if sidecar is None:
            continue
        matches = [
            asset
            for asset in archives
            if asset.endswith(f"-{target['name']}.{sidecar['archive']}")
        ]
        if len(matches) != 1:
            raise SystemExit(
                f"NCM sidecar archive is missing for release target {target['name']}"
            )
        archive = matches[0]
        expected_archives[archive] = target

    for archive_name, release_target in sorted(expected_archives.items()):
        archive_path = path / archive_name
        if archive_path.is_symlink():
            raise SystemExit(f"NCM sidecar archive must not be a symlink: {archive_name}")
        try:
            with tarfile.open(archive_path, mode="r:gz") as bundle:
                members = bundle.getmembers()
                names = [member.name for member in members]
                expected_names = [WORKER_POLICY_NAME, WORKER_MANIFEST_NAME]
                if names != expected_names:
                    raise SystemExit(
                        f"NCM sidecar {archive_name} must contain exactly "
                        f"{', '.join(expected_names)}; got {', '.join(names)}"
                    )
                by_name = {member.name: member for member in members}
                worker_member = by_name[WORKER_POLICY_NAME]
                manifest_member = by_name[WORKER_MANIFEST_NAME]
                if not worker_member.isreg() or not manifest_member.isreg():
                    raise SystemExit(f"NCM sidecar {archive_name} contains a non-file entry")
                if stat.S_IMODE(worker_member.mode) != 0o755:
                    raise SystemExit(f"NCM worker entry in {archive_name} is not executable")
                if stat.S_IMODE(manifest_member.mode) != 0o644:
                    raise SystemExit(f"NCM worker manifest entry in {archive_name} has wrong mode")
                worker_file = bundle.extractfile(worker_member)
                manifest_file = bundle.extractfile(manifest_member)
                if worker_file is None or manifest_file is None:
                    raise SystemExit(f"NCM sidecar {archive_name} has unreadable entries")
                worker_bytes = worker_file.read()
                packaged_manifest = manifest_file.read()
        except (OSError, tarfile.TarError) as error:
            raise SystemExit(f"invalid NCM sidecar archive {archive_name}: {error}") from error

        if packaged_manifest != manifest_bytes:
            raise SystemExit(
                f"NCM sidecar {archive_name} does not carry the trusted worker manifest"
            )
        pin = manifest_targets.get(release_target["target"])
        if pin is None:
            raise SystemExit(
                f"worker manifest has no pin for release target {release_target['target']}"
            )
        if len(worker_bytes) != pin["bytes"]:
            raise SystemExit(
                f"NCM worker size mismatch for {archive_name}: "
                f"{len(worker_bytes)} != {pin['bytes']}"
            )
        digest = hashlib.sha256(worker_bytes).hexdigest()
        if digest != pin["sha256"]:
            raise SystemExit(
                f"NCM worker digest mismatch for {archive_name}: {digest} != {pin['sha256']}"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--worker-platforms", type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--profile", choices=("stable", "beta"), default="stable")
    parser.add_argument("--mcpbs", type=Path)
    parser.add_argument("--sidecars", type=Path)
    parser.add_argument("--worker-manifest", type=Path)
    arguments = parser.parse_args()
    targets = target_matrix(arguments.manifest, arguments.worker_platforms)
    binary_prefix = "tracedecay-beta" if arguments.profile == "beta" else "tracedecay"

    require_exact(
        "binary",
        files(arguments.binaries),
        {
            f"{binary_prefix}-{arguments.tag}-{target['name']}.{target['archive']}"
            for target in targets
        },
    )
    if arguments.mcpbs is None:
        raise SystemExit("release validation requires an MCPB directory")
    mcpb_prefix = "tracedecay-beta" if arguments.profile == "beta" else "tracedecay"
    require_exact(
        "MCPB",
        files(arguments.mcpbs),
        {
            f"{mcpb_prefix}-{arguments.tag}-{target['name']}.mcpb"
            for target in targets
        },
    )
    expected_sidecars = {
        asset
        for target in targets
        for asset in sidecar_assets(target, arguments.tag, arguments.profile)
    }
    if expected_sidecars and arguments.sidecars is None:
        raise SystemExit("release validation requires an NCM sidecar directory")
    if expected_sidecars and arguments.worker_manifest is None:
        raise SystemExit("release validation requires a trusted worker manifest")
    if not expected_sidecars and arguments.worker_manifest is not None:
        raise SystemExit("a worker manifest is only valid when sidecar assets are expected")
    if arguments.sidecars is not None:
        actual_sidecars = files(arguments.sidecars)
        require_exact("NCM sidecar", actual_sidecars, expected_sidecars)
        if arguments.worker_manifest is None:
            raise SystemExit("release validation requires a trusted worker manifest")
        verify_sidecar_checksums(
            arguments.sidecars,
            {asset for asset in expected_sidecars if not asset.endswith(".sha256")},
        )
        verify_sidecar_archives(
            arguments.sidecars,
            {asset for asset in expected_sidecars if not asset.endswith(".sha256")},
            targets,
            arguments.worker_manifest,
        )
    print("release artifact coverage matches target manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
