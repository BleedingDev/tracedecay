#!/usr/bin/env python3
"""Validate exact release asset coverage from the release target manifest."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tarfile
from typing import Any


REQUIRED_TARGET_FIELDS = ("name", "runner", "target", "archive")
SUPPORTED_ARCHIVES = {"tar.gz", "zip"}
WORKER_POLICY_NAME = "tracedecay-ncm-worker"
WORKER_MANIFEST_NAME = "worker-manifest.json"
MODEL_ACQUISITION_MANIFEST_NAME = "model-acquisition-manifest.json"
WORKER_TARGET_TRIPLE = "aarch64-apple-darwin"
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REPOSITORY = "Xenova/paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REVISION = "2c4055b12046f11709e9df2c122e59ffbdc2f900"
MODEL_REVISION_PROVENANCE = (
    "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
    "#/identities/model/revision"
)
MODEL_BASE_URL = (
    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
    f"{MODEL_REVISION}/"
)
MODEL_FILES = {
    "onnx/model.onnx": (470268510, "185ae63f47e17a7e8d30d0e6a3cde6a6e4b79bc5b81666ecffc279a6856ca113"),
    "tokenizer.json": (17082913, "b60b6b43406a48bf3638526314f3d232d97058bc93472ff2de930d43686fa441"),
    "config.json": (673, "05b570bff786faa5c4604152aa16f19f77ed6dfc31e47dd0f3dd987078693ac7"),
    "special_tokens_map.json": (280, "06e405a36dfe4b9604f484f6a1e619af1a7f7d09e34a8555eb0b77b66318067f"),
    "tokenizer_config.json": (496, "3f5961b9ac86288cccdb97f32fb848d6187c78e1603958c53f3ea1f296b7d8a2"),
}
EMBEDDING_MANIFEST_SHA256 = "40084ced45c8bc429e525f65ffbfec6dd4e9ded4267f1be8d092c499f2dcb328"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


def _open_regular(path: Path, label: str) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SystemExit(f"{label} is missing: {path}") from error
    except OSError as error:
        raise SystemExit(f"cannot inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise SystemExit(f"{label} must not be a symlink: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"{label} must be a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise SystemExit(f"{label} must not be a symlink: {path}") from error
        raise SystemExit(f"cannot read {label} {path}: {error}") from error
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode):
        os.close(descriptor)
        raise SystemExit(f"{label} must be a regular file: {path}")
    return descriptor


def _read_regular(path: Path, label: str, maximum: int = MAX_MANIFEST_BYTES) -> bytes:
    descriptor = _open_regular(path, label)
    chunks: list[bytes] = []
    total = 0
    try:
        while True:
            chunk = os.read(descriptor, CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > maximum:
                raise SystemExit(f"{label} exceeds its byte bound: {path}")
            chunks.append(chunk)
    except OSError as error:
        raise SystemExit(f"cannot read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = _read_regular(path, label)
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
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
    if policy.get("model_acquisition_manifest") != (
        "product/ncm/release/model-acquisition-manifest.json"
    ):
        raise SystemExit(
            "NCM worker platform policy must name the pinned model acquisition manifest"
        )
    if packaging.get("model_acquisition_manifest_sidecar_required") is not True:
        raise SystemExit(
            "NCM worker policy requires a model acquisition manifest sidecar"
        )
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
            policy_model_manifest = policy.get("model_acquisition_manifest")
            if isinstance(policy_model_manifest, str):
                if sidecar.get("model_manifest") != Path(policy_model_manifest).name:
                    raise SystemExit(f"NCM sidecar model manifest differs from policy for {name}")
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
    required = ("worker", "archive", "manifest", "model_manifest", "checksum")
    if any(not isinstance(sidecar.get(field), str) or not sidecar[field] for field in required):
        raise SystemExit(f"release target {target['name']} sidecar is missing metadata")
    if sidecar["archive"] != "tar.gz":
        raise SystemExit("NCM worker sidecars must use tar.gz archives")
    if sidecar["worker"] != WORKER_POLICY_NAME:
        raise SystemExit("NCM worker sidecars must package tracedecay-ncm-worker")
    if sidecar["manifest"] != WORKER_MANIFEST_NAME:
        raise SystemExit("NCM worker sidecars must package worker-manifest.json")
    if sidecar["model_manifest"] != MODEL_ACQUISITION_MANIFEST_NAME:
        raise SystemExit(
            "NCM worker sidecar model_manifest must name model-acquisition-manifest.json"
        )
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
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SystemExit(f"release artifact directory is missing: {path}") from error
    except OSError as error:
        raise SystemExit(f"cannot inspect release artifact directory {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SystemExit(f"release artifact directory is missing: {path}")
    result: set[str] = set()
    empty: list[str] = []
    for item in path.iterdir():
        item_metadata = item.lstat()
        if stat.S_ISLNK(item_metadata.st_mode):
            raise SystemExit(f"release artifact must not be a symlink: {item.name}")
        if not stat.S_ISREG(item_metadata.st_mode):
            raise SystemExit(f"release artifact must be a regular file: {item.name}")
        if item_metadata.st_size == 0:
            empty.append(item.name)
        else:
            result.add(item.name)
    if empty:
        raise SystemExit("empty release artifacts: " + ", ".join(sorted(empty)))
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
    descriptor = _open_regular(path, "release artifact")
    try:
        while True:
            chunk = os.read(descriptor, CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    except OSError as error:
        raise SystemExit(f"cannot hash release artifact {path}: {error}") from error
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def verify_sidecar_checksums(path: Path, archives: set[str]) -> None:
    for archive in sorted(archives):
        checksum_path = path / f"{archive}.sha256"
        try:
            fields = _read_regular(checksum_path, "sidecar checksum", 4096).decode("utf-8").split()
        except UnicodeDecodeError as error:
            raise SystemExit(f"cannot read sidecar checksum {checksum_path}: {error}") from error
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
            raise SystemExit(f"invalid sidecar checksum format: {checksum_path.name}")
        checksum_name = fields[1].lstrip("*")
        if checksum_name != archive:
            raise SystemExit(
                f"sidecar checksum names {checksum_name!r}, expected {archive!r}"
            )
        actual = _sha256(path / archive)
        if fields[0] != actual:
            raise SystemExit(
                f"sidecar checksum mismatch for {archive}: {fields[0]} != {actual}"
            )


def _parse_worker_manifest(raw: bytes, label: str) -> dict[str, Any]:
    if not raw:
        raise SystemExit(f"worker manifest is empty: {label}")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid worker manifest {label}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit("worker manifest must be a JSON object")
    if value.get("worker") != WORKER_POLICY_NAME:
        raise SystemExit(
            f"worker manifest names {value.get('worker')!r}, expected {WORKER_POLICY_NAME!r}"
        )
    targets = value.get("targets")
    if not isinstance(targets, list) or len(targets) != 1:
        raise SystemExit("worker manifest must contain exactly one target pin")
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
        if triple == WORKER_TARGET_TRIPLE and (
            target.get("os") != "macos"
            or target.get("arch") != "aarch64"
            or target.get("family") != "unix"
        ):
            raise SystemExit(f"worker manifest target {triple} has invalid platform metadata")
    if WORKER_TARGET_TRIPLE not in seen:
        raise SystemExit(f"worker manifest does not pin {WORKER_TARGET_TRIPLE}")
    return value


def _load_worker_manifest(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = _read_regular(path, "worker manifest")
    return raw, _parse_worker_manifest(raw, str(path))


def _validate_model_acquisition_manifest(
    raw: bytes, *, release_target: dict[str, Any]
) -> None:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid NCM model acquisition manifest: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit("NCM model acquisition manifest must be a JSON object")
    if value.get("schema_version") != 1 or value.get("manifest_type") != "ncm-model-acquisition":
        raise SystemExit("NCM model acquisition manifest has an unsupported schema")
    if value.get("provider_id") != "ncm" or value.get("worker") != WORKER_POLICY_NAME:
        raise SystemExit("NCM model acquisition manifest has the wrong provider or worker")
    if value.get("target") != release_target["target"]:
        raise SystemExit(
            f"NCM model acquisition manifest target differs for {release_target['name']}"
        )
    if value.get("release_name") != release_target["name"]:
        raise SystemExit(
            f"NCM model acquisition manifest release name differs for {release_target['name']}"
        )
    if value.get("model") != MODEL_NAME or value.get("repository") != MODEL_REPOSITORY:
        raise SystemExit("NCM model acquisition manifest names the wrong model repository")
    if (
        value.get("model_root") != "models"
        or value.get("cache_repository") != "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2"
    ):
        raise SystemExit("NCM model acquisition manifest cache layout drifted")
    if value.get("embedding_manifest") != "product/ncm/reference/embedding-manifest.json":
        raise SystemExit("NCM model acquisition manifest embedding manifest path drifted")
    if value.get("embedding_manifest_sha256") != EMBEDDING_MANIFEST_SHA256:
        raise SystemExit("NCM model acquisition manifest embedding manifest digest drifted")
    if value.get("revision_provenance") != MODEL_REVISION_PROVENANCE:
        raise SystemExit("NCM model acquisition manifest revision provenance drifted")
    if value.get("revision") != MODEL_REVISION:
        raise SystemExit("NCM model acquisition manifest is not pinned to the accepted revision")
    if value.get("max_length") != 128 or value.get("pooling") != "mean" or value.get("normalize") is not True:
        raise SystemExit("NCM model acquisition manifest encoder profile drifted")
    base_url = value.get("base_url")
    if base_url != MODEL_BASE_URL or value.get("transport") != "https":
        raise SystemExit("NCM model acquisition manifest transport or base URL drifted")
    files = value.get("files")
    if not isinstance(files, list) or len(files) != len(MODEL_FILES):
        raise SystemExit("NCM model acquisition manifest does not pin the required file set")
    seen: set[str] = set()
    for entry in files:
        if not isinstance(entry, dict):
            raise SystemExit("NCM model acquisition manifest file entry is not an object")
        path = entry.get("path")
        if path not in MODEL_FILES or path in seen:
            raise SystemExit("NCM model acquisition manifest has an invalid or duplicate file")
        seen.add(path)
        expected_bytes, expected_digest = MODEL_FILES[path]
        if entry.get("bytes") != expected_bytes or entry.get("sha256") != expected_digest:
            raise SystemExit(f"NCM model acquisition manifest digest drifted for {path}")
        if entry.get("url") != f"{base_url}{path}":
            raise SystemExit(f"NCM model acquisition manifest URL drifted for {path}")
    if seen != set(MODEL_FILES):
        raise SystemExit("NCM model acquisition manifest file set differs from the release pin")
    transaction = value.get("transaction")
    if (
        not isinstance(transaction, dict)
        or transaction.get("version") != 1
        or transaction.get("publication") != "atomic-directory-swap"
        or transaction.get("journal") != "ncm-model-acquisition-v1.json"
        or transaction.get("staging_prefix") != ".ncm-model-staging-"
        or transaction.get("backup_prefix") != ".ncm-model-backup-"
    ):
        raise SystemExit("NCM model acquisition manifest does not require atomic publication")
    receipt = value.get("receipt")
    required_fields = {
        "schema_version",
        "operation_id",
        "operation",
        "outcome",
        "target",
        "model",
        "repository",
        "revision",
        "manifest_sha256",
        "files",
        "created_at_unix",
    }
    receipt_fields = receipt.get("required_fields") if isinstance(receipt, dict) else None
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema_version") != 1
        or receipt.get("relative_path") != "receipts/ncm-model-acquisition-v1.json"
        or not isinstance(receipt_fields, list)
        or not all(isinstance(field, str) for field in receipt_fields)
        or not required_fields.issubset(set(receipt_fields))
    ):
        raise SystemExit("NCM model acquisition manifest does not name the required receipt")


def verify_sidecar_archives(
    path: Path,
    archives: set[str],
    targets: list[dict[str, Any]],
    worker_manifest: Path,
    model_acquisition_manifest: Path,
) -> None:
    """Verify sidecar contents against the checked-in worker trust root."""
    manifest_bytes, manifest = _load_worker_manifest(worker_manifest)
    model_manifest_bytes = _read_regular(
        model_acquisition_manifest, "trusted model acquisition manifest"
    )
    _validate_model_acquisition_manifest(
        model_manifest_bytes,
        release_target={
            "name": "aarch64-macos",
            "target": WORKER_TARGET_TRIPLE,
        },
    )
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
        try:
            archive_metadata = archive_path.lstat()
        except FileNotFoundError as error:
            raise SystemExit(f"NCM sidecar archive is missing: {archive_name}") from error
        except OSError as error:
            raise SystemExit(f"cannot inspect NCM sidecar archive {archive_name}: {error}") from error
        if stat.S_ISLNK(archive_metadata.st_mode) or not stat.S_ISREG(archive_metadata.st_mode):
            raise SystemExit(f"NCM sidecar archive must be a regular file: {archive_name}")
        pin = manifest_targets.get(release_target["target"])
        if pin is None:
            raise SystemExit(
                f"worker manifest has no pin for release target {release_target['target']}"
            )
        try:
            with tarfile.open(archive_path, mode="r:gz") as bundle:
                members = bundle.getmembers()
                names = [member.name for member in members]
                model_member_name = release_target.get("sidecar", {}).get(
                    "model_manifest"
                )
                expected_names = [WORKER_POLICY_NAME, WORKER_MANIFEST_NAME]
                if model_member_name:
                    expected_names.append(model_member_name)
                if names != expected_names:
                    raise SystemExit(
                        f"NCM sidecar {archive_name} must contain exactly "
                        f"{', '.join(expected_names)}; got {', '.join(names)}"
                    )
                by_name = {member.name: member for member in members}
                worker_member = by_name[WORKER_POLICY_NAME]
                manifest_member = by_name[WORKER_MANIFEST_NAME]
                model_member = by_name.get(model_member_name) if model_member_name else None
                if not worker_member.isreg() or not manifest_member.isreg():
                    raise SystemExit(f"NCM sidecar {archive_name} contains a non-file entry")
                if model_member_name and (model_member is None or not model_member.isreg()):
                    raise SystemExit(
                        f"NCM sidecar {archive_name} is missing its model acquisition manifest"
                    )
                if stat.S_IMODE(worker_member.mode) != 0o755:
                    raise SystemExit(f"NCM worker entry in {archive_name} is not executable")
                if stat.S_IMODE(manifest_member.mode) != 0o644:
                    raise SystemExit(f"NCM worker manifest entry in {archive_name} has wrong mode")
                if model_member is not None and stat.S_IMODE(model_member.mode) != 0o644:
                    raise SystemExit(
                        f"NCM model acquisition manifest entry in {archive_name} has wrong mode"
                    )
                manifest_file = bundle.extractfile(manifest_member)
                model_file = bundle.extractfile(model_member) if model_member is not None else None
                if manifest_file is None or (model_member is not None and model_file is None):
                    raise SystemExit(f"NCM sidecar {archive_name} has unreadable entries")
                packaged_manifest = manifest_file.read(MAX_MANIFEST_BYTES + 1)
                if len(packaged_manifest) > MAX_MANIFEST_BYTES:
                    raise SystemExit(f"NCM sidecar {archive_name} worker manifest is too large")
                packaged_model_manifest = (
                    model_file.read(MAX_MANIFEST_BYTES + 1)
                    if model_file is not None
                    else None
                )
                if packaged_model_manifest is not None and len(packaged_model_manifest) > MAX_MANIFEST_BYTES:
                    raise SystemExit(
                        f"NCM sidecar {archive_name} model manifest is too large"
                    )
                if worker_member.size != pin["bytes"]:
                    raise SystemExit(
                        f"NCM worker size metadata mismatch for {archive_name}: "
                        f"{worker_member.size} != {pin['bytes']}"
                    )
                worker_file = bundle.extractfile(worker_member)
                if worker_file is None:
                    raise SystemExit(
                        f"NCM sidecar {archive_name} has an unreadable worker entry"
                    )
                worker_bytes = worker_file.read(pin["bytes"] + 1)
                if len(worker_bytes) != pin["bytes"]:
                    raise SystemExit(
                        f"NCM worker size mismatch for {archive_name}: "
                        f"{len(worker_bytes)} != {pin['bytes']}"
                    )
        except (OSError, tarfile.TarError) as error:
            raise SystemExit(f"invalid NCM sidecar archive {archive_name}: {error}") from error

        if packaged_manifest != manifest_bytes:
            raise SystemExit(
                f"NCM sidecar {archive_name} does not carry the trusted worker manifest"
            )
        model_manifest_name = release_target.get("sidecar", {}).get("model_manifest")
        if model_manifest_name:
            if packaged_model_manifest != model_manifest_bytes:
                raise SystemExit(
                    f"NCM sidecar {archive_name} does not carry the trusted model acquisition manifest"
                )
            _validate_model_acquisition_manifest(
                packaged_model_manifest, release_target=release_target
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
    parser.add_argument("--model-acquisition-manifest", type=Path)
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
    expected_model_manifests = {
        target.get("sidecar", {}).get("model_manifest")
        for target in targets
        if isinstance(target.get("sidecar"), dict)
        and target["sidecar"].get("model_manifest")
    }
    if expected_model_manifests and arguments.model_acquisition_manifest is None:
        raise SystemExit("release validation requires a trusted model acquisition manifest")
    if arguments.model_acquisition_manifest is not None and not expected_model_manifests:
        raise SystemExit(
            "a model acquisition manifest is only valid when sidecar metadata expects one"
        )
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
            arguments.model_acquisition_manifest,
        )
    print("release artifact coverage matches target manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
