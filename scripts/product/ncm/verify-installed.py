#!/usr/bin/env python3
"""Verify and exercise the installed NCM release contract.

The worker is distributed as a target-specific sidecar.  This gate verifies
that the CLI and sidecar archives contain only the entries promised by the
release metadata, then optionally exercises the same model transaction used by
an installed release.  Model files are downloaded into a private staging tree,
checked against the immutable acquisition manifest, and published with one
directory swap.  A failed download therefore leaves the previous model tree
and its receipt untouched.

Only the Python standard library is used here.  Python is release tooling; the
shipped runtime remains the Rust implementation.  ``--source-dir`` is an
explicit offline fixture hook for CI and regression tests.  Production
acquisition uses the HTTPS URLs in the checked-in release manifest.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Any, BinaryIO, Iterable
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen
import uuid
import zipfile


SUPPORTED_TARGET = "aarch64-apple-darwin"
SUPPORTED_RELEASE_NAME = "aarch64-macos"
WORKER_NAME = "tracedecay-ncm-worker"
WORKER_MANIFEST_NAME = "worker-manifest.json"
MODEL_ACQUISITION_MANIFEST_NAME = "model-acquisition-manifest.json"
EMBEDDING_MANIFEST_NAME = "ncm-encoder-manifest.json"
MODEL_CACHE_REPOSITORY = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2"
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REPOSITORY = "Xenova/paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REVISION = "2c4055b12046f11709e9df2c122e59ffbdc2f900"
MODEL_BASE_URL = (
    "https://huggingface.co/Xenova/paraphrase-multilingual-MiniLM-L12-v2/resolve/"
    f"{MODEL_REVISION}/"
)
MODEL_REVISION_PROVENANCE = (
    "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
    "#/identities/model/revision"
)
MODEL_REVISION_RECEIPT_PATH = (
    "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
)
MODEL_REQUIRED_FILES = (
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
)
# Keep the lifecycle journal name identical to the Rust model_lifecycle owner.
# The release verifier's acquisition receipt remains a separate Python-owned
# evidence file because the Rust owner intentionally emits no release receipt.
JOURNAL_FILENAME = "ncm-model-lifecycle-v1.json"
RECEIPT_FILENAME = "ncm-model-acquisition-v1.json"
STAGING_PREFIX = ".ncm-model-staging-"
BACKUP_PREFIX = ".ncm-model-backup-"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024 * 1024
CHUNK_BYTES = 1024 * 1024


class VerificationFailure(RuntimeError):
    """A release or model lifecycle assertion failed."""


def canonical_json(value: Any) -> bytes:
    """Encode JSON deterministically for identities and receipts."""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationFailure(message)


def _require_string(value: Any, label: str) -> str:
    _require(isinstance(value, str) and bool(value), f"{label} must be a non-empty string")
    return value


def _require_digest(value: Any, label: str) -> str:
    digest = _require_string(value, label)
    _require(
        len(digest) == 64 and all(character in "0123456789abcdef" for character in digest),
        f"{label} must be lowercase hexadecimal SHA-256",
    )
    return digest


def _require_source_sha(value: Any, label: str) -> str:
    source_sha = _require_string(value, label)
    _require(
        re.fullmatch(r"[0-9a-f]{40}", source_sha) is not None,
        f"{label} must be a lowercase 40-character git commit SHA",
    )
    return source_sha


def _require_release_version(value: Any, label: str) -> str:
    version = _require_string(value, label)
    _require(
        re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?", version)
        is not None,
        f"{label} must be a release SemVer without build metadata",
    )
    return version


def _require_positive_int(value: Any, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{label} must be a positive integer",
    )
    return value


def _safe_relative_path(value: Any, label: str) -> str:
    path = _require_string(value, label)
    candidate = Path(path)
    _require(
        not candidate.is_absolute()
        and "\\" not in path
        and all(part not in {"", ".", ".."} for part in candidate.parts)
        and all(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", part) is not None for part in candidate.parts),
        f"{label} must be a safe relative path",
    )
    return path


def _lstat_regular(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise VerificationFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise VerificationFailure(f"inspect {label} {path}: {error}") from error
    _require(not stat.S_ISLNK(metadata.st_mode), f"{label} must not be a symlink: {path}")
    _require(stat.S_ISREG(metadata.st_mode), f"{label} must be a regular file: {path}")
    return metadata


def _lstat_directory(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise VerificationFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise VerificationFailure(f"inspect {label} {path}: {error}") from error
    _require(not stat.S_ISLNK(metadata.st_mode), f"{label} must not be a symlink: {path}")
    _require(stat.S_ISDIR(metadata.st_mode), f"{label} must be a directory: {path}")
    return metadata


def _open_regular(path: Path, label: str) -> int:
    _lstat_regular(path, label)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise VerificationFailure(f"{label} must not be a symlink: {path}") from error
        raise VerificationFailure(f"read {label} {path}: {error}") from error
    metadata = os.fstat(descriptor)
    _require(stat.S_ISREG(metadata.st_mode), f"{label} must be a regular file: {path}")
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
            _require(total <= maximum, f"{label} exceeds its byte bound: {path}")
            chunks.append(chunk)
    except OSError as error:
        raise VerificationFailure(f"read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _read_prefix(path: Path, label: str, maximum: int) -> bytes:
    """Read at most ``maximum`` bytes without imposing a full-file limit."""
    _require(maximum > 0, "prefix byte bound must be positive")
    descriptor = _open_regular(path, label)
    chunks: list[bytes] = []
    total = 0
    try:
        while total < maximum:
            chunk = os.read(descriptor, min(CHUNK_BYTES, maximum - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    except OSError as error:
        raise VerificationFailure(f"read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _digest_regular(path: Path, label: str) -> tuple[int, str]:
    descriptor = _open_regular(path, label)
    digest = hashlib.sha256()
    total = 0
    try:
        while True:
            chunk = os.read(descriptor, CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            digest.update(chunk)
    except OSError as error:
        raise VerificationFailure(f"read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    return total, digest.hexdigest()


def _ensure_private_directory(path: Path, *, create: bool = False) -> None:
    if create:
        path.mkdir(parents=True, exist_ok=True)
    _lstat_directory(path, "private directory")
    if os.name != "nt":
        os.chmod(path, 0o700)
        metadata = path.lstat()
        _require(
            stat.S_IMODE(metadata.st_mode) == 0o700,
            f"private directory has unsafe permissions: {path}",
        )
        if hasattr(os, "geteuid"):
            _require(metadata.st_uid == os.geteuid(), f"private directory has wrong owner: {path}")


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new_file(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    _ensure_parent_directory(path.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, mode)
    except OSError as error:
        raise VerificationFailure(f"create file {path}: {error}") from error
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            _require(written > 0, f"write made no progress: {path}")
            offset += written
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except OSError as error:
        raise VerificationFailure(f"write file {path}: {error}") from error
    finally:
        os.close(descriptor)


def _ensure_parent_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    # The lifecycle root is checked before this helper is called.  Checking
    # only the directory being created avoids rejecting platform aliases such
    # as macOS's /var -> /private/var while still catching a swapped child.
    _lstat_directory(path, "model directory")


def _write_atomic(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    _ensure_parent_directory(path.parent)
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, mode)
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            _require(written > 0, f"write made no progress: {temporary}")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        temporary = None
        _sync_directory(path.parent)
    except OSError as error:
        raise VerificationFailure(f"atomically write {path}: {error}") from error
    finally:
        if "descriptor" in locals() and descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _load_json(path: Path, label: str, maximum: int = MAX_MANIFEST_BYTES) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular(path, label, maximum)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VerificationFailure(f"invalid {label} {path}: {error}") from error
    _require(isinstance(value, dict), f"{label} must be a JSON object")
    return value, raw


def _model_file_entries(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entries = manifest.get("files")
    _require(isinstance(entries, list) and entries, "model acquisition manifest.files must be a list")
    by_path: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(entries):
        _require(isinstance(entry, dict), f"model acquisition files[{index}] must be an object")
        path = _safe_relative_path(entry.get("path"), f"model acquisition files[{index}].path")
        _require(path not in by_path, f"duplicate model acquisition file: {path}")
        _require(path in MODEL_REQUIRED_FILES, f"unexpected model acquisition file: {path}")
        _require_positive_int(entry.get("bytes"), f"model acquisition files[{index}].bytes")
        _require_digest(entry.get("sha256"), f"model acquisition files[{index}].sha256")
        url = _require_string(entry.get("url"), f"model acquisition files[{index}].url")
        parsed = urlparse(url)
        _require(parsed.scheme in {"https", "file"}, f"unsupported model acquisition URL scheme: {url}")
        by_path[path] = entry
    _require(set(by_path) == set(MODEL_REQUIRED_FILES), "model acquisition file set differs from the pinned model")
    return by_path


def validate_acquisition_manifest(
    manifest: dict[str, Any],
    *,
    target: str = SUPPORTED_TARGET,
    release_name: str = SUPPORTED_RELEASE_NAME,
    embedding_manifest: Path | None = None,
    revision_receipt: Path | None = None,
) -> dict[str, Any]:
    """Validate the release descriptor and its target-bound model identity."""
    _require(manifest.get("schema_version") == 1, "model acquisition schema_version must be 1")
    _require(manifest.get("manifest_type") == "ncm-model-acquisition", "invalid model acquisition manifest type")
    _require(manifest.get("provider_id") == "ncm", "model acquisition provider_id must be ncm")
    _require(manifest.get("worker") == WORKER_NAME, "model acquisition worker is not the NCM worker")
    _require(manifest.get("target") == target, f"model acquisition target is not bound to {target}")
    _require(manifest.get("release_name") == release_name, f"model acquisition release name is not {release_name}")
    _require(manifest.get("embedding_manifest") == "product/ncm/reference/embedding-manifest.json", "model acquisition embedding manifest path drifted")
    _require_digest(manifest.get("embedding_manifest_sha256"), "model acquisition embedding_manifest_sha256")
    _require(manifest.get("model_root") == "models", "model acquisition model_root must be models")
    _require(manifest.get("cache_repository") == MODEL_CACHE_REPOSITORY, "model acquisition cache repository drifted")
    _require(manifest.get("model") == MODEL_NAME, "model acquisition model is not pinned")
    _require(manifest.get("repository") == MODEL_REPOSITORY, "model acquisition repository is not pinned")
    _require(manifest.get("revision") == MODEL_REVISION, "model acquisition revision is not pinned")
    _require(manifest.get("revision_provenance") == MODEL_REVISION_PROVENANCE, "model acquisition revision provenance drifted")
    revision_receipt_digest = _require_digest(
        manifest.get("revision_provenance_sha256"),
        "model acquisition revision_provenance_sha256",
    )
    _require(manifest.get("transport") == "https", "production model acquisition transport must be HTTPS")
    _require_string(manifest.get("base_url"), "model acquisition base_url")
    _require(manifest.get("max_length") == 128, "model acquisition max_length must be 128")
    _require(manifest.get("pooling") == "mean", "model acquisition pooling must be mean")
    _require(manifest.get("normalize") is True, "model acquisition normalize must be true")
    files = _model_file_entries(manifest)
    base_url = manifest["base_url"]
    parsed_base = urlparse(base_url)
    _require(
        parsed_base.scheme == "https" and parsed_base.netloc == "huggingface.co",
        "model acquisition base_url must use the pinned HTTPS model host",
    )
    _require(
        base_url == MODEL_BASE_URL,
        "model acquisition base_url must name the pinned model repository and revision",
    )
    for path, entry in files.items():
        url = entry["url"]
        _require(url.startswith(base_url) and url == f"{base_url}{path}", f"model acquisition URL is not bound to {path}")

    transaction = manifest.get("transaction")
    _require(isinstance(transaction, dict), "model acquisition transaction must be an object")
    _require(transaction.get("version") == 1, "model acquisition transaction version must be 1")
    _require(transaction.get("publication") == "atomic-directory-swap", "model acquisition publication must be atomic-directory-swap")
    _require(transaction.get("journal") == JOURNAL_FILENAME, "model acquisition journal name drifted")
    _require(transaction.get("staging_prefix") == STAGING_PREFIX, "model acquisition staging prefix drifted")
    _require(transaction.get("backup_prefix") == BACKUP_PREFIX, "model acquisition backup prefix drifted")

    receipt = manifest.get("receipt")
    _require(isinstance(receipt, dict), "model acquisition receipt must be an object")
    _require(receipt.get("schema_version") == 1, "model acquisition receipt schema_version must be 1")
    receipt_path = _safe_relative_path(receipt.get("relative_path"), "model acquisition receipt.relative_path")
    _require(receipt_path == "receipts/ncm-model-acquisition-v1.json", "model acquisition receipt path drifted")
    required_fields = receipt.get("required_fields")
    _require(isinstance(required_fields, list) and all(isinstance(item, str) for item in required_fields), "model acquisition receipt.required_fields must be strings")
    required = set(required_fields)
    _require(
        {
            "schema_version",
            "operation_id",
            "operation",
            "outcome",
            "target",
            "model",
            "repository",
            "revision",
            "manifest_sha256",
            "revision_provenance_sha256",
            "files",
            "created_at_unix",
        }
        <= required,
        "model acquisition receipt omits identity fields",
    )

    if embedding_manifest is not None:
        trusted, trusted_bytes = _load_json(embedding_manifest, "trusted embedding manifest")
        _require(sha256_bytes(trusted_bytes) == manifest["embedding_manifest_sha256"], "trusted embedding manifest digest differs from release descriptor")
        _require(trusted.get("model") == MODEL_NAME, "trusted embedding manifest model drifted")
        _require(trusted.get("repository") == MODEL_REPOSITORY, "trusted embedding manifest repository drifted")
        _require(trusted.get("revision") == MODEL_REVISION, "trusted embedding manifest revision drifted")
        _require(trusted.get("revision_provenance") == MODEL_REVISION_PROVENANCE, "trusted embedding manifest provenance drifted")
        _require(trusted.get("max_length") == 128 and trusted.get("pooling") == "mean" and trusted.get("normalize") is True, "trusted embedding manifest profile drifted")
        trusted_files = _model_file_entries_without_url(trusted)
        for path, entry in files.items():
            trusted_entry = trusted_files.get(path)
            _require(trusted_entry is not None, f"trusted embedding manifest omits {path}")
            _require(entry["bytes"] == trusted_entry["bytes"] and entry["sha256"] == trusted_entry["sha256"], f"release model digest differs from trusted embedding manifest for {path}")
    if revision_receipt is not None:
        _validate_revision_receipt(
            revision_receipt,
            manifest=manifest,
            files=files,
            expected_digest=revision_receipt_digest,
        )
    return files


def _validate_revision_receipt(
    path: Path,
    *,
    manifest: dict[str, Any],
    files: dict[str, dict[str, Any]],
    expected_digest: str,
) -> dict[str, Any]:
    """Validate the tracked backend receipt used as model revision evidence."""
    _require(
        path.name == Path(MODEL_REVISION_RECEIPT_PATH).name,
        "trusted model revision receipt path is not the canonical receipt",
    )
    raw = _read_regular(path, "trusted model revision receipt", MAX_RECEIPT_BYTES)
    _require(
        sha256_bytes(raw) == expected_digest,
        "model acquisition revision receipt digest differs from its manifest pin",
    )
    receipt, _ = _load_json(path, "trusted model revision receipt", MAX_RECEIPT_BYTES)
    identities = receipt.get("identities")
    model_identity = identities.get("model") if isinstance(identities, dict) else None
    _require(isinstance(model_identity, dict), "trusted model revision receipt has no model identity")
    _require(
        model_identity.get("model") == manifest["model"],
        "trusted model revision receipt model identity drifted",
    )
    _require(
        model_identity.get("revision") == manifest["revision"],
        "trusted model revision receipt revision identity drifted",
    )
    artifact_digest = _require_digest(
        model_identity.get("artifact_sha256"),
        "trusted model revision receipt artifact_sha256",
    )
    _require(
        artifact_digest == files["onnx/model.onnx"]["sha256"],
        "trusted model revision receipt artifact digest drifted",
    )
    _require(
        model_identity.get("manifest_sha256") == manifest["embedding_manifest_sha256"],
        "trusted model revision receipt manifest digest differs from the canonical model manifest",
    )
    receipt_files = model_identity.get("files")
    _require(
        isinstance(receipt_files, list),
        "trusted model revision receipt files must be a list",
    )
    receipt_by_path: dict[str, dict[str, Any]] = {}
    for index, entry in enumerate(receipt_files):
        _require(
            isinstance(entry, dict),
            f"trusted model revision receipt files[{index}] must be an object",
        )
        relative = _safe_relative_path(
            entry.get("path"),
            f"trusted model revision receipt files[{index}].path",
        )
        _require(
            relative not in receipt_by_path,
            f"trusted model revision receipt repeats {relative}",
        )
        _require_positive_int(
            entry.get("bytes"),
            f"trusted model revision receipt files[{index}].bytes",
        )
        _require_digest(
            entry.get("sha256"),
            f"trusted model revision receipt files[{index}].sha256",
        )
        receipt_by_path[relative] = entry
    _require(
        set(receipt_by_path) == set(files),
        "trusted model revision receipt file set differs from the release pin",
    )
    for relative, entry in files.items():
        receipt_entry = receipt_by_path[relative]
        _require(
            receipt_entry["bytes"] == entry["bytes"]
            and receipt_entry["sha256"] == entry["sha256"],
            f"trusted model revision receipt file identity differs for {relative}",
        )
    return receipt


def _model_file_entries_without_url(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entries = manifest.get("files")
    _require(isinstance(entries, list), "embedding manifest.files must be a list")
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        _require(isinstance(entry, dict), "embedding manifest file must be an object")
        path = _safe_relative_path(entry.get("path"), "embedding manifest file.path")
        _require(path not in result, f"duplicate embedding manifest file: {path}")
        _require_positive_int(entry.get("bytes"), f"embedding manifest {path}.bytes")
        _require_digest(entry.get("sha256"), f"embedding manifest {path}.sha256")
        result[path] = entry
    _require(set(result) == set(MODEL_REQUIRED_FILES), "embedding manifest file set drifted")
    return result


def _worker_manifest(manifest: dict[str, Any], *, target: str) -> dict[str, Any]:
    _require(manifest.get("schema_version") == 1, "worker manifest schema_version must be 1")
    _require(manifest.get("worker") == WORKER_NAME, "worker manifest names an unexpected worker")
    _require(manifest.get("protocol_version") == 1, "worker manifest protocol_version must be 1")
    _require(manifest.get("protocol_identity") == "tracedecay.ncm.worker.v1", "worker manifest protocol identity drifted")
    targets = manifest.get("targets")
    _require(isinstance(targets, list) and len(targets) == 1, "worker manifest must contain exactly one target pin")
    selected: dict[str, Any] | None = None
    seen: set[str] = set()
    for entry in targets:
        _require(isinstance(entry, dict), "worker manifest target must be an object")
        triple = _require_string(entry.get("triple"), "worker manifest target.triple")
        _require(triple not in seen, f"worker manifest repeats target {triple}")
        seen.add(triple)
        _require_positive_int(entry.get("bytes"), f"worker manifest target {triple}.bytes")
        _require_digest(entry.get("sha256"), f"worker manifest target {triple}.sha256")
        if triple == target:
            selected = entry
    _require(selected is not None, f"worker manifest has no target pin for {target}")
    _require(selected.get("os") == "macos" and selected.get("arch") == "aarch64" and selected.get("family") == "unix", "worker manifest target metadata drifted")
    return selected


def _safe_archive_name(name: str) -> str:
    _require(
        name
        and "\x00" not in name
        and not name.startswith(("/", "\\"))
        and "\\" not in name
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", name) is not None
        and re.fullmatch(r"[A-Za-z]:.*", name) is None,
        f"archive entry has unsafe name: {name!r}",
    )
    parts = Path(name).parts
    _require(all(part not in {"", ".", ".."} for part in parts), f"archive entry has unsafe name: {name!r}")
    return name


def _extract_tar(path: Path, destination: Path) -> list[str]:
    names: list[str] = []
    try:
        with tarfile.open(path, "r:gz") as archive:
            for member in archive.getmembers():
                name = _safe_archive_name(member.name)
                _require(member.isreg(), f"NCM sidecar contains a non-file entry: {name}")
                _require(member.linkname == "", f"NCM sidecar entry is linked: {name}")
                names.append(name)
            _require(len(names) == len(set(names)), "NCM sidecar contains duplicate entries")
            archive.extractall(destination)
    except (OSError, tarfile.TarError) as error:
        raise VerificationFailure(f"extract NCM sidecar {path}: {error}") from error
    return names


def _extract_zip(path: Path, destination: Path) -> list[str]:
    names: list[str] = []
    try:
        with zipfile.ZipFile(path) as archive:
            for info in archive.infolist():
                name = _safe_archive_name(info.filename)
                mode = (info.external_attr >> 16) & 0o170000
                _require(mode != stat.S_IFLNK, f"release archive entry is a symlink: {name}")
                _require(not name.endswith("/"), f"release archive contains a directory entry: {name}")
                names.append(name)
            _require(len(names) == len(set(names)), "release archive contains duplicate entries")
            archive.extractall(destination)
    except (OSError, zipfile.BadZipFile) as error:
        raise VerificationFailure(f"extract release archive {path}: {error}") from error
    return names


def _probe_cli_identity(
    binary: Path,
    *,
    expected_version: str,
    expected_source_sha: str,
) -> None:
    """Require the CLI to report the release identity stamped by its build."""
    _require_release_version(expected_version, "expected release version")
    _require_source_sha(expected_source_sha, "expected source SHA")
    expected_line = f"tracedecay {expected_version}+{expected_source_sha}"
    for arguments in (("--version",), ("--help",)):
        completed = subprocess.run(
            [str(binary), *arguments],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        _require(
            completed.returncode == 0,
            f"installed CLI {arguments[0]} failed: {completed.stderr.strip()}",
        )
        if arguments == ("--version",):
            _require(
                completed.stdout.splitlines() == [expected_line],
                "installed CLI --version did not report the trusted release/source identity",
            )


def _verify_executable_format(path: Path, *, target: str) -> None:
    """Reject text scripts and binaries for a different release platform."""
    prefix = _read_prefix(path, "CLI executable", 4096)
    if target.endswith("-apple-darwin"):
        _require(
            len(prefix) >= 8
            and prefix[:4] in {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf"},
            "CLI executable is not a 64-bit Mach-O binary",
        )
        if prefix[:4] == b"\xcf\xfa\xed\xfe":
            cpu_type = int.from_bytes(prefix[4:8], "little", signed=False)
        else:
            cpu_type = int.from_bytes(prefix[4:8], "big", signed=False)
        expected_cpu = 0x0100000C if target == SUPPORTED_TARGET else 0x01000007
        _require(
            cpu_type == expected_cpu,
            f"CLI executable CPU type {cpu_type:#x} is not pinned for {target}",
        )
        return
    if target.endswith("-windows-msvc"):
        _require(len(prefix) >= 0x40 and prefix[:2] == b"MZ", "CLI executable is not a PE binary")
        pe_offset = int.from_bytes(prefix[0x3C:0x40], "little", signed=False)
        _require(
            pe_offset + 6 <= len(prefix) and prefix[pe_offset : pe_offset + 4] == b"PE\0\0",
            "CLI executable has no valid PE header",
        )
        machine = int.from_bytes(prefix[pe_offset + 4 : pe_offset + 6], "little", signed=False)
        architecture = target.split("-", 1)[0]
        expected_machine = {
            "x86_64": 0x8664,
            "aarch64": 0xAA64,
            "i686": 0x014C,
        }.get(architecture)
        _require(
            expected_machine is not None,
            f"CLI executable target architecture is unsupported: {target}",
        )
        _require(
            machine == expected_machine,
            f"CLI executable COFF machine {machine:#x} is not pinned for {target}",
        )
        return
    _require(len(prefix) >= 20 and prefix[:4] == b"\x7fELF", "CLI executable is not an ELF binary")
    architecture = target.split("-", 1)[0]
    expected = {
        "x86_64": (2, 0x003E),
        "aarch64": (2, 0x00B7),
        "i686": (1, 0x0003),
        "armv7": (1, 0x0028),
    }.get(architecture)
    _require(
        expected is not None,
        f"CLI executable target architecture is unsupported: {target}",
    )
    elf_class, expected_machine = expected
    _require(
        prefix[4] == elf_class,
        f"CLI executable ELF class {prefix[4]} is not pinned for {target}",
    )
    _require(
        prefix[5] == 1,
        "CLI executable ELF header has unsupported byte order",
    )
    machine = int.from_bytes(prefix[18:20], "little", signed=False)
    _require(
        machine == expected_machine,
        f"CLI executable ELF machine {machine:#x} is not pinned for {target}",
    )


def _run_cli_raw(
    binary: Path,
    arguments: Iterable[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    timeout: int = 180,
) -> subprocess.CompletedProcess[bytes]:
    """Run one release CLI command with bounded captured output."""
    try:
        return subprocess.run(
            [str(binary), *arguments],
            cwd=cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
        )
    except OSError as error:
        raise VerificationFailure(f"run installed CLI {binary}: {error}") from error
    except subprocess.TimeoutExpired as error:
        raise VerificationFailure(
            f"installed CLI timed out after {timeout}s: {' '.join(arguments)}"
        ) from error


def _run_cli(
    binary: Path,
    arguments: Iterable[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    label: str,
    timeout: int = 180,
) -> subprocess.CompletedProcess[bytes]:
    """Run one CLI command and turn any non-zero exit into a gate failure."""
    completed = _run_cli_raw(
        binary,
        arguments,
        cwd=cwd,
        environment=environment,
        timeout=timeout,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.decode("utf-8", errors="replace").strip()
        stdout = completed.stdout.decode("utf-8", errors="replace").strip()
        details = stderr or stdout or f"exit status {completed.returncode}"
        raise VerificationFailure(f"{label} failed: {details[-4000:]}")
    return completed


def _run_cli_json(
    binary: Path,
    arguments: Iterable[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    label: str,
    timeout: int = 180,
) -> dict[str, Any]:
    """Run a JSON-producing CLI command and require one object result."""
    completed = _run_cli(
        binary,
        arguments,
        cwd=cwd,
        environment=environment,
        label=label,
        timeout=timeout,
    )
    try:
        value = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VerificationFailure(f"{label} did not return valid JSON: {error}") from error
    _require(isinstance(value, dict), f"{label} must return one JSON object")
    return value


def _initialize_e2e_project(project: Path, *, environment: dict[str, str]) -> None:
    """Create the smallest committed project accepted by the installed CLI."""
    project.mkdir(parents=True, exist_ok=False)
    (project / "src").mkdir()
    (project / "Cargo.toml").write_text(
        "[package]\nname = \"ncm-installed-e2e\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        encoding="utf-8",
    )
    (project / "src/lib.rs").write_text(
        "pub fn ncm_installed_e2e_fixture() -> u8 { 7 }\n",
        encoding="utf-8",
    )
    for arguments in (
        ("git", "init", "--quiet", "-b", "main"),
        ("git", "config", "user.email", "ncm-release@example.com"),
        ("git", "config", "user.name", "NCM Release"),
        ("git", "add", "."),
        ("git", "commit", "--quiet", "-m", "initial"),
    ):
        try:
            completed = subprocess.run(
                list(arguments),
                cwd=project,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise VerificationFailure(f"initialize installed CLI project: {error}") from error
        if completed.returncode != 0:
            details = completed.stderr.decode("utf-8", errors="replace").strip()
            raise VerificationFailure(
                f"initialize installed CLI project command {' '.join(arguments)} failed: {details[-2000:]}"
            )


def _authority_paths(profile_root: Path) -> tuple[Path, ...]:
    """Return the platform-specific daemon authority record candidates."""
    return (
        profile_root / "daemon-authority.json",
        profile_root / "daemon-authority" / "daemon-authority.json",
    )


def _read_authority_record(
    paths: tuple[Path, ...],
    *,
    expected_profile_root: Path,
) -> dict[str, Any] | None:
    """Read one valid authority record without accepting a stale malformed file."""
    for path in paths:
        if not path.exists():
            continue
        record, _ = _load_json(path, "daemon authority record", MAX_RECEIPT_BYTES)
        _require(
            isinstance(record.get("pid"), int)
            and not isinstance(record.get("pid"), bool)
            and record["pid"] > 0,
            "daemon authority record pid is invalid",
        )
        _require(
            isinstance(record.get("process_run_id"), str)
            and bool(record["process_run_id"]),
            "daemon authority record process_run_id is invalid",
        )
        _require(
            isinstance(record.get("epoch"), int)
            and not isinstance(record.get("epoch"), bool)
            and record["epoch"] > 0,
            "daemon authority record epoch is invalid",
        )
        _require(
            isinstance(record.get("version"), str) and bool(record["version"]),
            "daemon authority record version is invalid",
        )
        try:
            observed_root = Path(record["profile_root"]).resolve()
        except (OSError, RuntimeError) as error:
            raise VerificationFailure("daemon authority record profile root is invalid") from error
        _require(
            observed_root == expected_profile_root.resolve(),
            "daemon authority record belongs to a different profile",
        )
        return record
    return None


def _wait_for_authority(
    paths: tuple[Path, ...],
    *,
    expected_profile_root: Path,
    previous: dict[str, Any] | None = None,
    timeout: int = 180,
) -> dict[str, Any]:
    """Wait for a fresh daemon authority record after install or restart."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = _read_authority_record(paths, expected_profile_root=expected_profile_root)
        if record is not None and (
            previous is None
            or any(
                record.get(key) != previous.get(key)
                for key in ("pid", "process_run_id", "epoch")
            )
        ):
            return record
        time.sleep(0.25)
    raise VerificationFailure(
        "timed out waiting for the installed daemon authority after "
        f"{timeout}s ({', '.join(str(path) for path in paths)})"
    )


def _worker_handshake(
    worker: Path,
    state_root: Path,
    *,
    expected_artifact_sha256: str,
) -> dict[str, Any]:
    """Launch the production worker and require its real encoder identity."""
    _lstat_regular(worker, "installed NCM worker")
    if os.name != "nt":
        _require(worker.stat().st_mode & 0o111, "installed NCM worker is not executable")
    request = canonical_json(
        {
            "protocol_version": 1,
            "id": 1,
            "deadline_ms": 30_000,
            "op": "handshake",
            "namespace": "0" * 64,
            "payload": {
                "protocol_version": 1,
                "algorithm_profile": "ncm-biomem-rs.v1",
            },
        }
    )
    frame = struct.pack(">I", len(request)) + request
    environment = os.environ.copy()
    # A production worker must not resolve a Python or test-double substitute
    # from PATH. The executable and the pinned model tree are its only inputs.
    environment["PATH"] = ""
    try:
        completed = subprocess.run(
            [str(worker), "--state-root", str(state_root)],
            input=frame,
            capture_output=True,
            check=False,
            timeout=120,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise VerificationFailure(f"launch production NCM worker handshake: {error}") from error
    stderr = completed.stderr.decode("utf-8", errors="replace").strip()
    _require(
        completed.returncode == 0,
        f"production NCM worker handshake process failed: {stderr[-2000:]}",
    )
    _require(len(completed.stdout) >= 4, "production NCM worker returned no framed handshake")
    length = struct.unpack(">I", completed.stdout[:4])[0]
    _require(
        length > 0 and len(completed.stdout) == length + 4,
        "production NCM worker handshake frame is truncated or has trailing bytes",
    )
    try:
        reply = json.loads(completed.stdout[4:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VerificationFailure(f"production NCM worker handshake is invalid JSON: {error}") from error
    _require(isinstance(reply, dict), "production NCM worker handshake must be an object")
    _require(
        str(reply.get("outcome", "")).lower() == "success",
        f"production NCM worker handshake failed: {reply}",
    )
    payload = reply.get("payload")
    _require(isinstance(payload, dict), "production NCM worker handshake has no payload")
    encoder = payload.get("encoder")
    _require(isinstance(encoder, dict), "production NCM worker handshake has no encoder identity")
    _require(
        encoder.get("model") == MODEL_NAME,
        "production NCM worker handshake did not expose the pinned MiniLM model",
    )
    _require(
        encoder.get("artifact_sha256") == expected_artifact_sha256,
        "production NCM worker handshake artifact digest differs from the pinned model",
    )
    return {
        "outcome": "success",
        "model": encoder.get("model"),
        "artifact_sha256": encoder.get("artifact_sha256"),
        "algorithm": payload.get("algorithm"),
        "state_generation": reply.get("state_generation"),
    }


def _status_with_identity(
    binary: Path,
    project: Path,
    *,
    environment: dict[str, str],
    profile_root: Path,
    model_root: Path,
) -> dict[str, Any]:
    """Require status to expose the complete installed sidecar/model binding."""
    status = _run_cli_json(
        binary,
        ("ncm", "status", "--path", str(project), "--json"),
        cwd=project,
        environment=environment,
        label="installed NCM status",
    )
    for key in (
        "enabled",
        "platform_supported",
        "worker_present",
        "manifest_present",
        "model_acquisition_manifest_present",
        "model_manifest_present",
        "encoder_ready",
    ):
        _require(status.get(key) is True, f"installed NCM status does not prove {key}")
    _require(
        Path(_require_string(status.get("profile_root"), "NCM status profile_root")).resolve()
        == profile_root.resolve(),
        "installed NCM status profile root differs from the isolated service profile",
    )
    _require(
        Path(_require_string(status.get("state_root"), "NCM status state_root")).resolve()
        == model_root.resolve(),
        "installed NCM status state root differs from the pinned model root",
    )
    for key in ("worker_path", "worker_manifest_path", "model_acquisition_manifest_path"):
        _require_string(status.get(key), f"NCM status {key}")
    return status


def verify_installed_e2e(
    binary: Path,
    worker_archive: Path,
    model_root: Path,
    manifest: dict[str, Any],
    *,
    acquisition_manifest_bytes: bytes,
    embedding_manifest_path: Path,
    worker_manifest_path: Path,
    revision_receipt: Path,
    target: str,
    release_name: str,
    profile: str,
    expected_version: str | None,
    expected_source_sha: str | None,
    expected_binary_sha256: str | None,
) -> dict[str, Any]:
    """Exercise CLI install, service restart, and a production worker handshake."""
    _require(target == SUPPORTED_TARGET, "installed NCM E2E requires the supported arm64 macOS target")
    _require(release_name == SUPPORTED_RELEASE_NAME, "installed NCM E2E release name is unsupported")
    _require(profile in {"stable", "beta"}, "installed NCM E2E release profile is invalid")
    binary = binary.absolute()
    model_root = model_root.absolute()
    _require_release_version(expected_version, "expected release version")
    _require_source_sha(expected_source_sha, "expected source SHA")
    expected_binary_sha256 = _require_digest(
        expected_binary_sha256,
        "expected installed CLI SHA-256",
    )
    _lstat_regular(binary, "installed CLI binary")
    if os.name != "nt":
        _require(binary.stat().st_mode & 0o111, "installed CLI binary is not executable")
    _verify_executable_format(binary, target=target)
    _probe_cli_identity(
        binary,
        expected_version=expected_version,
        expected_source_sha=expected_source_sha,
    )
    _require(
        _digest_regular(binary, "installed CLI binary")[1] == expected_binary_sha256,
        "installed CLI digest differs from the trusted release identity",
    )
    _lstat_directory(model_root, "installed NCM model root")
    embedding_bytes = _read_regular(embedding_manifest_path, "trusted embedding manifest")
    verified_before = verify_model_tree(
        model_root,
        manifest,
        manifest_bytes=embedding_bytes,
        receipt_required=True,
        revision_receipt=revision_receipt,
    )
    _require(
        not _journal_path(model_root).exists(),
        "installed NCM model root has a pending lifecycle journal before the CLI E2E gate",
    )
    expected_artifact_sha256 = next(
        entry["sha256"] for entry in manifest["files"] if entry["path"] == "onnx/model.onnx"
    )

    with tempfile.TemporaryDirectory(prefix="ncm-installed-e2e-") as directory:
        e2e_root = Path(directory)
        home = e2e_root / "home"
        profile_root = e2e_root / "profile"
        config_root = home / ".config"
        project = e2e_root / "project"
        worker_root = e2e_root / "worker"
        for path in (home, profile_root, config_root, worker_root):
            path.mkdir(parents=True)
        environment = os.environ.copy()
        environment.update(
            {
                "HOME": str(home),
                "USERPROFILE": str(home),
                "XDG_CONFIG_HOME": str(config_root),
                "TRACEDECAY_DATA_DIR": str(profile_root),
                "TRACEDECAY_GLOBAL_DB": str(profile_root / "global.db"),
                "TRACEDECAY_SERVICE_NAMESPACE": (
                    f"ncm-e2e-{os.getpid()}-{uuid.uuid4().hex[:8]}"
                ),
                "TRACEDECAY_TEST_ALLOW_INCOMPLETE_HOLDER_SCAN": "1",
            }
        )
        for key in ("HF_HOME", "HF_ENDPOINT", "FASTEMBED_CACHE_DIR", "TRACEDECAY_NCM_WORKER"):
            environment.pop(key, None)
        _initialize_e2e_project(project, environment=environment)
        names = _extract_tar(worker_archive, worker_root)
        expected_names = [WORKER_NAME, WORKER_MANIFEST_NAME, MODEL_ACQUISITION_MANIFEST_NAME]
        _require(names == expected_names, f"installed NCM E2E sidecar entries differ: {names}")
        worker = worker_root / WORKER_NAME
        worker_manifest = worker_root / WORKER_MANIFEST_NAME
        model_manifest = worker_root / MODEL_ACQUISITION_MANIFEST_NAME
        sidecar_worker_data, _ = _load_json(worker_manifest, "E2E sidecar worker manifest")
        sidecar_worker_pin = _worker_manifest(sidecar_worker_data, target=target)
        _verify_executable_format(worker, target=target)
        sidecar_worker_size, sidecar_worker_digest = _digest_regular(
            worker, "E2E sidecar worker"
        )
        _require(
            sidecar_worker_size == sidecar_worker_pin["bytes"]
            and sidecar_worker_digest == sidecar_worker_pin["sha256"],
            "E2E sidecar worker differs from the trusted target pin",
        )
        _lstat_regular(worker_manifest, "E2E sidecar worker manifest")
        _lstat_regular(model_manifest, "E2E sidecar model acquisition manifest")
        _require(
            _read_regular(worker_manifest, "E2E sidecar worker manifest")
            == _read_regular(worker_manifest_path, "trusted worker manifest"),
            "E2E sidecar worker manifest differs from the canonical worker manifest",
        )
        _require(
            _read_regular(model_manifest, "E2E sidecar model acquisition manifest")
            == acquisition_manifest_bytes,
            "E2E sidecar model acquisition manifest differs from the canonical release descriptor",
        )

        authority_paths = _authority_paths(profile_root)
        service_installed = False
        cleanup_error: str | None = None
        failure: Exception | None = None
        before_status: dict[str, Any] | None = None
        after_status: dict[str, Any] | None = None
        handshake: dict[str, Any] | None = None
        evidence: dict[str, Any] | None = None
        try:
            _run_cli(
                binary,
                ("daemon", "install-service"),
                cwd=project,
                environment=environment,
                label="installed daemon service install",
            )
            service_installed = True
            first_authority = _wait_for_authority(
                authority_paths,
                expected_profile_root=profile_root,
            )
            # `init` can race the daemon's code-index scheduler during a cold
            # service start; retry only that typed warming outcome.
            init_deadline = time.monotonic() + 180
            while True:
                init = _run_cli_raw(
                    binary,
                    ("init",),
                    cwd=project,
                    environment=environment,
                    timeout=180,
                )
                if init.returncode == 0:
                    break
                init_stderr = init.stderr.decode("utf-8", errors="replace")
                if "code_index_scheduler_unavailable" not in init_stderr or time.monotonic() >= init_deadline:
                    _require(
                        False,
                        f"installed CLI init failed: {init_stderr.strip()[-4000:]}",
                    )
                time.sleep(0.25)
            _run_cli(
                binary,
                (
                    "--yes",
                    "ncm",
                    "install",
                    "--path",
                    str(project),
                    "--worker",
                    str(worker),
                    "--state-root",
                    str(model_root),
                    "--json",
                ),
                cwd=project,
                environment=environment,
                label="installed NCM CLI install",
            )
            before_status = _status_with_identity(
                binary,
                project,
                environment=environment,
                profile_root=profile_root,
                model_root=model_root,
            )
            _require(
                Path(before_status["worker_path"]).resolve().is_file(),
                "installed NCM status worker path is missing",
            )
            _run_cli(
                binary,
                ("daemon", "restart"),
                cwd=project,
                environment=environment,
                label="installed daemon service restart",
            )
            _wait_for_authority(
                authority_paths,
                expected_profile_root=profile_root,
                previous=first_authority,
            )
            after_status = _status_with_identity(
                binary,
                project,
                environment=environment,
                profile_root=profile_root,
                model_root=model_root,
            )
            _require(
                after_status["worker_path"] == before_status["worker_path"],
                "daemon restart changed the installed NCM worker binding",
            )
            verified_after = verify_model_tree(
                model_root,
                manifest,
                manifest_bytes=embedding_bytes,
                receipt_required=True,
                revision_receipt=revision_receipt,
            )
            _require(
                not _journal_path(model_root).exists(),
                "installed daemon restart left a pending model lifecycle journal",
            )
            installed_worker = Path(after_status["worker_path"])
            _lstat_regular(installed_worker, "installed staged NCM worker")
            _verify_executable_format(installed_worker, target=target)
            installed_worker_size, installed_worker_digest = _digest_regular(
                installed_worker, "installed staged NCM worker"
            )
            _require(
                installed_worker_size == sidecar_worker_pin["bytes"]
                and installed_worker_digest == sidecar_worker_pin["sha256"],
                "installed staged NCM worker differs from the trusted target pin",
            )
            installed_worker_root = installed_worker.resolve()
            expected_worker_root = (profile_root / "ncm" / "worker").resolve()
            _require(
                installed_worker_root.is_relative_to(expected_worker_root),
                "installed NCM worker escaped the isolated profile worker root",
            )
            installed_manifest = Path(after_status["worker_manifest_path"])
            installed_model_manifest = Path(after_status["model_acquisition_manifest_path"])
            _require(
                _read_regular(installed_manifest, "installed worker manifest")
                == _read_regular(worker_manifest_path, "trusted worker manifest"),
                "installed worker manifest differs from the canonical worker manifest",
            )
            _require(
                _read_regular(installed_model_manifest, "installed model acquisition manifest")
                == acquisition_manifest_bytes,
                "installed model acquisition manifest differs from the canonical release descriptor",
            )
            handshake = _worker_handshake(
                installed_worker,
                model_root,
                expected_artifact_sha256=expected_artifact_sha256,
            )
            evidence = {
                "mode": "installed-cli-service-restart",
                "profile": profile,
                "target": target,
                "version": expected_version,
                "source_sha": expected_source_sha,
                "binary_sha256": expected_binary_sha256,
                "model_tree_before": verified_before,
                "model_tree_after": verified_after,
                "before_status": before_status,
                "after_status": after_status,
                "worker_handshake": handshake,
            }
        except Exception as error:
            failure = error
        finally:
            if service_installed:
                try:
                    cleanup = _run_cli_raw(
                        binary,
                        ("daemon", "uninstall-service"),
                        cwd=project,
                        environment=environment,
                        timeout=120,
                    )
                    if cleanup.returncode != 0:
                        detail = cleanup.stderr.decode("utf-8", errors="replace").strip()
                        cleanup_error = detail[-2000:] or f"exit status {cleanup.returncode}"
                except VerificationFailure as error:
                    cleanup_error = str(error)
        if failure is not None:
            raise failure
        _require(cleanup_error is None, f"installed daemon service cleanup failed: {cleanup_error}")
        _require(
            evidence is not None
            and before_status is not None
            and after_status is not None
            and handshake is not None,
            "installed NCM E2E produced incomplete evidence",
        )
        return evidence


def verify_binary_archive(
    path: Path,
    *,
    target: str,
    profile: str = "stable",
    expected_version: str | None = None,
    expected_source_sha: str | None = None,
    expected_archive_sha256: str | None = None,
    expected_binary_sha256: str | None = None,
) -> dict[str, Any]:
    """Extract a CLI archive and prove its trusted release/source identity."""
    _lstat_regular(path, "CLI release archive")
    expected_version = _require_release_version(expected_version, "expected release version")
    expected_source_sha = _require_source_sha(expected_source_sha, "expected source SHA")
    expected_archive_sha256 = _require_digest(
        expected_archive_sha256,
        "expected CLI archive SHA-256",
    )
    if expected_binary_sha256 is not None:
        expected_binary_sha256 = _require_digest(
            expected_binary_sha256,
            "expected installed CLI SHA-256",
        )
    archive_size, archive_digest = _digest_regular(path, "CLI release archive")
    _require(
        archive_digest == expected_archive_sha256,
        "CLI release archive digest differs from the trusted release identity",
    )
    expected_name = "tracedecay.exe" if path.suffix == ".zip" or target.endswith("windows-msvc") else "tracedecay"
    with tempfile.TemporaryDirectory(prefix="ncm-release-cli-") as directory:
        root = Path(directory)
        names = _extract_zip(path, root) if path.suffix == ".zip" else _extract_tar(path, root)
        _require(names == [expected_name], f"CLI archive must contain exactly {expected_name}; got {names}")
        binary = root / expected_name
        _lstat_regular(binary, "installed CLI binary")
        if os.name != "nt":
            _require(binary.stat().st_mode & 0o111, f"installed CLI binary is not executable: {binary}")
        _verify_executable_format(binary, target=target)
        _probe_cli_identity(
            binary,
            expected_version=expected_version,
            expected_source_sha=expected_source_sha,
        )
        bytes_count, digest = _digest_regular(binary, "installed CLI binary")
        if expected_binary_sha256 is not None:
            _require(
                digest == expected_binary_sha256,
                "extracted CLI binary digest differs from the separately verified installed binary",
            )
        return {
            "path": str(path),
            "entry": expected_name,
            "bytes": bytes_count,
            "sha256": digest,
            "archive_bytes": archive_size,
            "archive_sha256": archive_digest,
            "profile": profile,
            "target": target,
            "version": expected_version,
            "source_sha": expected_source_sha,
        }


def verify_installed_binary(
    path: Path,
    *,
    target: str,
    expected_version: str | None = None,
    expected_source_sha: str | None = None,
    expected_binary_sha256: str | None = None,
) -> dict[str, Any]:
    """Run an installed binary and prove its trusted release/source identity."""
    _lstat_regular(path, "installed CLI binary")
    if os.name != "nt":
        _require(path.stat().st_mode & 0o111, f"installed CLI binary is not executable: {path}")
    expected_version = _require_release_version(expected_version, "expected release version")
    expected_source_sha = _require_source_sha(expected_source_sha, "expected source SHA")
    expected_binary_sha256 = _require_digest(
        expected_binary_sha256,
        "expected installed CLI SHA-256",
    )
    _verify_executable_format(path, target=target)
    _probe_cli_identity(
        path,
        expected_version=expected_version,
        expected_source_sha=expected_source_sha,
    )
    size, digest = _digest_regular(path, "installed CLI binary")
    _require(
        digest == expected_binary_sha256,
        "installed CLI digest differs from the trusted release identity",
    )
    return {
        "path": str(path),
        "bytes": size,
        "sha256": digest,
        "version": expected_version,
        "source_sha": expected_source_sha,
    }


def verify_worker_archive(
    path: Path,
    *,
    target: str = SUPPORTED_TARGET,
    worker_manifest_path: Path | None = None,
    model_manifest_path: Path | None = None,
    revision_receipt_path: Path | None = None,
    checksum_path: Path | None = None,
) -> dict[str, Any]:
    """Verify a target-specific worker sidecar and both trusted manifests."""
    _lstat_regular(path, "NCM worker sidecar archive")
    _require(checksum_path is not None, "NCM worker sidecar checksum is required")
    try:
        fields = _read_regular(checksum_path, "NCM worker sidecar checksum", 4096).decode("utf-8").split()
    except UnicodeDecodeError as error:
        raise VerificationFailure("NCM sidecar checksum is not UTF-8") from error
    _require(len(fields) == 2 and fields[1].lstrip("*") == path.name, "NCM sidecar checksum names the wrong archive")
    _require(_require_digest(fields[0], "NCM sidecar checksum") == _digest_regular(path, "NCM worker sidecar archive")[1], "NCM sidecar checksum does not match archive")
    _require(worker_manifest_path is not None, "trusted worker manifest is required")
    _require(model_manifest_path is not None, "trusted model acquisition manifest is required")
    with tempfile.TemporaryDirectory(prefix="ncm-release-worker-") as directory:
        root = Path(directory)
        names = _extract_tar(path, root)
        expected_names = [WORKER_NAME, WORKER_MANIFEST_NAME, MODEL_ACQUISITION_MANIFEST_NAME]
        _require(names == expected_names, f"NCM sidecar must contain exactly {expected_names}; got {names}")
        worker = root / WORKER_NAME
        worker_manifest = root / WORKER_MANIFEST_NAME
        model_manifest = root / MODEL_ACQUISITION_MANIFEST_NAME
        worker_metadata = _lstat_regular(worker, "sidecar worker")
        worker_manifest_metadata = _lstat_regular(
            worker_manifest, "sidecar worker manifest"
        )
        model_manifest_metadata = _lstat_regular(
            model_manifest, "sidecar model acquisition manifest"
        )
        _require(
            stat.S_IMODE(worker_metadata.st_mode) == 0o755,
            "sidecar worker must have executable mode 0755",
        )
        _require(
            stat.S_IMODE(worker_manifest_metadata.st_mode) == 0o644,
            "sidecar worker manifest must have mode 0644",
        )
        _require(
            stat.S_IMODE(model_manifest_metadata.st_mode) == 0o644,
            "sidecar model acquisition manifest must have mode 0644",
        )
        _verify_executable_format(worker, target=target)
        worker_data, _ = _load_json(worker_manifest, "sidecar worker manifest")
        pin = _worker_manifest(worker_data, target=target)
        worker_size, worker_digest = _digest_regular(worker, "sidecar worker")
        _require(worker_size == pin["bytes"] and worker_digest == pin["sha256"], "sidecar worker bytes or digest differs from target pin")
        acquisition, acquisition_bytes = _load_json(model_manifest, "sidecar model acquisition manifest")
        validate_acquisition_manifest(
            acquisition,
            target=target,
            release_name=SUPPORTED_RELEASE_NAME,
            embedding_manifest=None,
            revision_receipt=revision_receipt_path,
        )
        trusted_bytes = _read_regular(worker_manifest_path, "trusted worker manifest")
        _require(trusted_bytes == _read_regular(worker_manifest, "sidecar worker manifest"), "sidecar worker manifest differs from trusted target manifest")
        trusted_bytes = _read_regular(model_manifest_path, "trusted model acquisition manifest")
        _require(trusted_bytes == acquisition_bytes, "sidecar model acquisition manifest differs from trusted target manifest")
        return {
            "path": str(path),
            "target": target,
            "worker": {"bytes": worker_size, "sha256": worker_digest},
            "worker_manifest_sha256": sha256_bytes(worker_manifest.read_bytes()),
            "model_manifest_sha256": sha256_bytes(acquisition_bytes),
        }


def _download(url: str, destination: Path, expected_bytes: int, *, source_dir: Path | None = None, relative_path: str) -> tuple[int, str]:
    if source_dir is not None:
        source = source_dir / relative_path
        _lstat_regular(source, "offline model source")
        stream: BinaryIO = source.open("rb")
    else:
        parsed = urlparse(url)
        _require(parsed.scheme == "https", "production model acquisition accepts only HTTPS URLs")
        request = Request(url, headers={"Accept": "application/octet-stream", "User-Agent": "tracedecay-ncm-installer/1"})
        try:
            stream = urlopen(request, timeout=120)  # noqa: S310 - URL is pinned in the release manifest.
        except OSError as error:
            raise VerificationFailure(f"download model artifact {relative_path}: {error}") from error
    descriptor: int | None = None
    try:
        _ensure_parent_directory(destination.parent)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(destination, flags, 0o600)
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = stream.read(CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            _require(total <= min(expected_bytes, MAX_DOWNLOAD_BYTES), f"downloaded {relative_path} exceeds its pinned size")
            digest.update(chunk)
            offset = 0
            while offset < len(chunk):
                written = os.write(descriptor, chunk[offset:])
                _require(written > 0, f"download write made no progress for {relative_path}")
                offset += written
        os.fsync(descriptor)
        _require(total == expected_bytes, f"downloaded {relative_path} has {total} bytes; expected {expected_bytes}")
        actual = digest.hexdigest()
        return total, actual
    except OSError as error:
        raise VerificationFailure(f"download model artifact {relative_path}: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        stream.close()


def _tree_digest(root: Path) -> str:
    _lstat_directory(root, "model tree")
    digest = hashlib.sha256()
    entries: list[Path] = []
    for path in root.rglob("*"):
        entries.append(path)
    for path in sorted(entries, key=lambda candidate: candidate.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        metadata = path.lstat()
        _require(not stat.S_ISLNK(metadata.st_mode), f"model tree contains a symlink: {path}")
        _require(stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode), f"model tree contains a non-file entry: {path}")
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(b"d\0" if stat.S_ISDIR(metadata.st_mode) else b"f\0")
        if stat.S_ISREG(metadata.st_mode):
            descriptor = _open_regular(path, f"model tree file {relative}")
            try:
                while True:
                    chunk = os.read(descriptor, CHUNK_BYTES)
                    if not chunk:
                        break
                    digest.update(chunk)
            except OSError as error:
                raise VerificationFailure(
                    f"read model tree file {relative}: {error}"
                ) from error
            finally:
                os.close(descriptor)
    return digest.hexdigest()


def _manifest_runtime_bytes(manifest: dict[str, Any]) -> bytes:
    files = [
        {
            "path": entry["path"],
            "sha256": entry["sha256"],
            "bytes": entry["bytes"],
        }
        for entry in manifest["files"]
    ]
    return json.dumps(
        {
            "model": manifest["model"],
            "repository": manifest["repository"],
            "revision": manifest["revision"],
            "revision_provenance": manifest["revision_provenance"],
            "files": files,
            "max_length": manifest["max_length"],
            "pooling": manifest["pooling"],
            "normalize": manifest["normalize"],
        },
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8") + b"\n"


def _model_tree_paths(root: Path, manifest: dict[str, Any]) -> tuple[Path, Path]:
    models = root / "models"
    repository = models / MODEL_CACHE_REPOSITORY
    snapshot = repository / "snapshots" / MODEL_REVISION
    return models, snapshot


def _require_exact_entries(
    directory: Path,
    *,
    expected_directories: set[str],
    expected_files: set[str],
    label: str,
) -> None:
    _lstat_directory(directory, label)
    actual_directories: set[str] = set()
    actual_files: set[str] = set()
    for entry in directory.iterdir():
        metadata = entry.lstat()
        _require(not stat.S_ISLNK(metadata.st_mode), f"{label} contains a symlink: {entry}")
        if stat.S_ISDIR(metadata.st_mode):
            actual_directories.add(entry.name)
        elif stat.S_ISREG(metadata.st_mode):
            actual_files.add(entry.name)
        else:
            raise VerificationFailure(f"{label} contains a non-regular entry: {entry}")
    _require(
        actual_directories == expected_directories and actual_files == expected_files,
        f"{label} entries differ from the pinned model layout",
    )


def verify_model_tree(
    root: Path,
    manifest: dict[str, Any],
    *,
    manifest_bytes: bytes | None = None,
    receipt_required: bool = False,
    revision_receipt: Path | None = None,
) -> dict[str, Any]:
    """Verify the exact cache layout and every pinned artifact byte."""
    validate_acquisition_manifest(manifest, revision_receipt=revision_receipt)
    models, snapshot = _model_tree_paths(root, manifest)
    _require_exact_entries(
        models,
        expected_directories={MODEL_CACHE_REPOSITORY},
        expected_files={EMBEDDING_MANIFEST_NAME},
        label="installed model directory",
    )
    _require_exact_entries(
        models / MODEL_CACHE_REPOSITORY,
        expected_directories={"refs", "snapshots"},
        expected_files=set(),
        label="installed model repository",
    )
    refs = models / MODEL_CACHE_REPOSITORY / "refs"
    snapshots = models / MODEL_CACHE_REPOSITORY / "snapshots"
    _require_exact_entries(refs, expected_directories=set(), expected_files={"main"}, label="installed model refs")
    _require_exact_entries(snapshots, expected_directories={MODEL_REVISION}, expected_files=set(), label="installed model snapshots")
    ref_bytes = _read_regular(refs / "main", "installed model revision", 256)
    _require(ref_bytes.decode("utf-8").strip() == MODEL_REVISION, "installed model ref is not the pinned revision")
    _require_exact_entries(
        snapshot,
        expected_directories={"onnx"},
        expected_files={entry["path"] for entry in manifest["files"] if "/" not in entry["path"]},
        label="installed model snapshot",
    )
    _require_exact_entries(snapshot / "onnx", expected_directories=set(), expected_files={"model.onnx"}, label="installed model onnx directory")
    expected_files = _model_file_entries(manifest)
    for relative, entry in expected_files.items():
        path = snapshot / relative
        size, digest = _digest_regular(path, f"installed model {relative}")
        _require(size == entry["bytes"] and digest == entry["sha256"], f"installed model {relative} differs from its pinned digest")
    runtime_manifest = models / EMBEDDING_MANIFEST_NAME
    runtime_bytes = _read_regular(runtime_manifest, "installed runtime model manifest")
    expected_runtime = manifest_bytes if manifest_bytes is not None else _manifest_runtime_bytes(manifest)
    _require(runtime_bytes == expected_runtime, "installed runtime model manifest differs from the release pin")
    result = {
        "root": str(root),
        "revision": MODEL_REVISION,
        "manifest_sha256": sha256_bytes(runtime_bytes),
        "files": [{"path": path, "bytes": entry["bytes"], "sha256": entry["sha256"]} for path, entry in expected_files.items()],
        "tree_sha256": _tree_digest(models),
    }
    if receipt_required:
        _require(
            revision_receipt is not None,
            "installed model verification requires the canonical model revision receipt",
        )
        _validate_installed_receipt(
            root,
            manifest,
            result,
            expected_operation_id=None,
            revision_receipt=revision_receipt,
        )
    return result


def _operation_id(operation: str) -> str:
    return f"{int(time.time_ns()):x}-{uuid.uuid4().hex[:16]}-{operation}"


def _journal_path(root: Path) -> Path:
    return root / JOURNAL_FILENAME


def _read_journal(root: Path) -> dict[str, Any] | None:
    path = _journal_path(root)
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    value, _ = _load_json(path, "model lifecycle journal", MAX_RECEIPT_BYTES)
    _require(value.get("schema_version") == 1, "unsupported model lifecycle journal schema")
    for name_key in ("staging_name", "backup_name"):
        name = value.get(name_key)
        if name is not None:
            _require(
                isinstance(name, str)
                and Path(name).name == name
                and Path(name).parent == Path(".")
                and (
                    (name_key == "staging_name" and name.startswith(STAGING_PREFIX))
                    or (name_key == "backup_name" and name.startswith(BACKUP_PREFIX))
                ),
                "model lifecycle journal contains an unsafe path",
            )
    _require(value.get("operation") in {"install", "update"}, "model lifecycle journal operation is invalid")
    _require(value.get("phase") in {"prepared", "staged", "backed_up", "published"}, "model lifecycle journal phase is invalid")
    operation = value["operation"]
    operation_id = value.get("operation_id")
    _require(
        isinstance(operation_id, str)
        and re.fullmatch(r"[0-9a-f]+-[0-9a-f]{16}-(?:install|update)", operation_id)
        is not None
        and operation_id.endswith(f"-{operation}"),
        "model lifecycle journal operation_id is invalid",
    )
    _require(
        value.get("target") == SUPPORTED_TARGET,
        "model lifecycle journal target is not the pinned NCM target",
    )
    _require(
        value.get("revision") == MODEL_REVISION,
        "model lifecycle journal revision is not the pinned model revision",
    )
    for digest_name in ("before_digest", "after_digest"):
        digest = value.get(digest_name)
        if digest is not None:
            _require(
                isinstance(digest, str)
                and len(digest) == 64
                and all(character in "0123456789abcdef" for character in digest),
                f"model lifecycle journal {digest_name} is not a lowercase SHA-256",
            )
    return value


def recover_model(
    root: Path,
    manifest: dict[str, Any] | None = None,
    *,
    revision_receipt: Path | None = None,
) -> dict[str, Any]:
    """Recover one interrupted publication using the journal's digest guards."""
    _ensure_private_directory(root)
    journal = _read_journal(root)
    if journal is None:
        return {"outcome": "no_effect", "root": str(root)}
    models = root / "models"
    staging = root / journal["staging_name"] if journal.get("staging_name") else None
    backup = root / journal["backup_name"] if journal.get("backup_name") else None
    phase = journal["phase"]
    # Before the live directory is moved, it is still the caller's original
    # tree.  Recovery at this point only removes private staging state; an
    # interrupted download must never turn into a model deletion.
    if phase in {"prepared", "staged"}:
        if backup is not None and backup.exists():
            _require(not backup.is_symlink(), "model rollback backup must not be a symlink")
            _require(
                journal.get("before_digest") is not None,
                "model lifecycle journal has an unexpected rollback backup",
            )
            if not models.exists():
                _require(
                    _tree_digest(backup) == journal["before_digest"],
                    "model rollback backup differs from the original model tree",
                )
                backup.rename(models)
            else:
                _require(_tree_digest(models) == journal.get("before_digest"), "live model tree changed while staging")
                shutil.rmtree(backup)
        if staging is not None and staging.exists():
            _require(not staging.is_symlink(), "model staging directory must not be a symlink")
            shutil.rmtree(staging)
        _journal_path(root).unlink(missing_ok=True)
        _sync_directory(root)
        return {"outcome": "rolled_back", "root": str(root), "recovered": phase}
    published_digest: str | None = None
    if models.exists() or models.is_symlink():
        _require(not models.is_symlink(), "live model directory must not be a symlink during recovery")
        published_digest = _tree_digest(models)
    if phase == "published":
        # Publication is not committed until the release acquisition receipt is
        # durable. A process crash between the directory rename and receipt
        # write must leave the journal in place and fail closed; deleting that
        # journal would turn an unrecorded model tree into accepted state.
        _require(
            published_digest is not None
            and published_digest == journal.get("after_digest"),
            "published model tree differs from the transaction candidate",
        )
        _require(
            manifest is not None,
            "published model lifecycle recovery requires the pinned acquisition manifest",
        )
        _require(
            revision_receipt is not None,
            "published model lifecycle recovery requires the canonical model revision receipt",
        )
        verified = verify_model_tree(
            root,
            manifest,
            revision_receipt=revision_receipt,
        )
        _validate_installed_receipt(
            root,
            manifest,
            verified,
            expected_operation_id=journal["operation_id"],
            revision_receipt=revision_receipt,
        )
        if backup is not None and backup.exists():
            _require(not backup.is_symlink(), "model rollback backup must not be a symlink")
            shutil.rmtree(backup)
        if staging is not None and staging.exists():
            _require(not staging.is_symlink(), "model staging directory must not be a symlink")
            shutil.rmtree(staging)
        _journal_path(root).unlink(missing_ok=True)
        _sync_directory(root)
        return {"outcome": "committed", "root": str(root), "recovered": "published"}
    if published_digest is not None:
        _require(
            published_digest == journal.get("after_digest"),
            "live model tree changed outside the model transaction",
        )
        # A live candidate left behind by a failed publication can be removed
        # only after its digest is known to be this transaction's candidate.
        shutil.rmtree(models)
    if backup is not None and backup.exists():
        _require(not backup.is_symlink(), "model rollback backup must not be a symlink")
        _require(
            journal.get("before_digest") is not None,
            "model lifecycle journal has an unexpected rollback backup",
        )
        _require(
            _tree_digest(backup) == journal["before_digest"],
            "model rollback backup differs from the original model tree",
        )
        backup.rename(models)
    elif journal.get("before_digest") is not None:
        raise VerificationFailure("model rollback backup is missing")
    if staging is not None and staging.exists():
        _require(not staging.is_symlink(), "model staging directory must not be a symlink")
        shutil.rmtree(staging)
    _journal_path(root).unlink(missing_ok=True)
    _sync_directory(root)
    return {"outcome": "rolled_back", "root": str(root), "recovered": phase}


def _write_receipt(root: Path, manifest: dict[str, Any], receipt: dict[str, Any], receipt_path: Path | None) -> Path:
    expected = (root / manifest["receipt"]["relative_path"]).absolute()
    configured = receipt_path or expected
    _require(configured.is_absolute(), "model receipt path must be absolute after resolution")
    _require(
        configured.absolute() == expected,
        "model receipt must remain under the transaction state root",
    )
    receipt["receipt_path"] = str(configured)
    _write_atomic(configured, json.dumps(receipt, indent=2, ensure_ascii=False).encode("utf-8") + b"\n")
    return configured


def acquire_model(
    root: Path,
    manifest: dict[str, Any],
    *,
    operation: str = "install",
    source_dir: Path | None = None,
    embedding_manifest_path: Path | None = None,
    revision_receipt_path: Path | None = None,
    receipt_path: Path | None = None,
    failure_after: str | None = None,
) -> dict[str, Any]:
    """Acquire, verify, and atomically publish the pinned model tree."""
    _require(operation in {"install", "update"}, "model acquisition operation must be install or update")
    # ``resolve()`` would silently follow a caller-provided symlink.  Keep the
    # lexical absolute path so the no-follow checks below can reject it.
    root = root.absolute()
    _ensure_private_directory(root, create=True)
    trusted_runtime_bytes: bytes | None = None
    trusted_embedding: dict[str, Any] | None = None
    if embedding_manifest_path is not None:
        trusted_embedding, trusted_runtime_bytes = _load_json(embedding_manifest_path, "trusted embedding manifest")
        # The release descriptor compares the source manifest's raw identity,
        # while the runtime stores the same object under its state root.
    files = validate_acquisition_manifest(
        manifest,
        embedding_manifest=embedding_manifest_path,
        revision_receipt=revision_receipt_path,
    )
    pending = _read_journal(root)
    _require(pending is None, f"model lifecycle has a pending journal at {_journal_path(root)}; recover first")
    models = root / "models"
    if operation == "install" and models.exists():
        try:
            existing = verify_model_tree(
                root,
                manifest,
                manifest_bytes=trusted_runtime_bytes,
                revision_receipt=revision_receipt_path,
            )
        except VerificationFailure as error:
            raise VerificationFailure(f"existing model tree is present but not the exact pin: {error}") from error
        receipt = _receipt(manifest, operation, "already_present", root, existing, _operation_id(operation))
        receipt_file = _write_receipt(root, manifest, receipt, receipt_path)
        receipt["receipt_path"] = str(receipt_file)
        return receipt

    operation_id = _operation_id(operation)
    staging_name = f"{STAGING_PREFIX}{operation_id}"
    backup_name = f"{BACKUP_PREFIX}{operation_id}"
    staging = root / staging_name
    candidate = staging / "candidate"
    before_digest = _tree_digest(models) if models.exists() else None
    journal = {
        "schema_version": 1,
        "operation_id": operation_id,
        "operation": operation,
        "phase": "prepared",
        "target": manifest["target"],
        "revision": manifest["revision"],
        "staging_name": staging_name,
        "backup_name": backup_name,
        "before_digest": before_digest,
        "after_digest": None,
    }
    try:
        staging.mkdir(mode=0o700)
        _ensure_private_directory(staging)
        _write_atomic(_journal_path(root), json.dumps(journal, indent=2).encode("utf-8") + b"\n")
        snapshot = candidate / MODEL_CACHE_REPOSITORY / "snapshots" / MODEL_REVISION
        (candidate / MODEL_CACHE_REPOSITORY / "refs").mkdir(parents=True)
        (snapshot / "onnx").mkdir(parents=True)
        _ensure_private_directory(candidate)
        _write_new_file(candidate / MODEL_CACHE_REPOSITORY / "refs" / "main", MODEL_REVISION.encode("utf-8"))
        for relative, entry in files.items():
            destination = snapshot / relative
            size, digest = _download(entry["url"], destination, entry["bytes"], source_dir=source_dir, relative_path=relative)
            _require(size == entry["bytes"] and digest == entry["sha256"], f"downloaded model {relative} differs from its pinned digest")
            if failure_after == relative:
                raise VerificationFailure(f"injected acquisition failure after {relative}")
        runtime_bytes = trusted_runtime_bytes or _manifest_runtime_bytes(manifest)
        _write_new_file(candidate / EMBEDDING_MANIFEST_NAME, runtime_bytes)
        candidate_digest = _tree_digest(candidate)
        journal["phase"] = "staged"
        journal["after_digest"] = candidate_digest
        _write_atomic(_journal_path(root), json.dumps(journal, indent=2).encode("utf-8") + b"\n")
        if failure_after == "staged":
            raise VerificationFailure("injected acquisition failure after staging")
        if models.exists():
            _lstat_directory(models, "existing model directory")
            models.rename(root / backup_name)
        journal["phase"] = "backed_up"
        _write_atomic(_journal_path(root), json.dumps(journal, indent=2).encode("utf-8") + b"\n")
        if failure_after == "backed_up":
            raise VerificationFailure("injected acquisition failure after backup")
        candidate.rename(models)
        journal["phase"] = "published"
        _write_atomic(_journal_path(root), json.dumps(journal, indent=2).encode("utf-8") + b"\n")
        verified = verify_model_tree(
            root,
            manifest,
            manifest_bytes=runtime_bytes,
            revision_receipt=revision_receipt_path,
        )
        _require(verified["tree_sha256"] == candidate_digest, "published model tree changed during verification")
        receipt = _receipt(manifest, operation, "committed", root, verified, operation_id)
        try:
            _write_receipt(root, manifest, receipt, receipt_path)
        except Exception:
            # A published model is not committed until its receipt is durable.
            # Mark the transaction as backed up so recovery removes the new
            # candidate and restores the prior exact tree.
            journal["phase"] = "backed_up"
            _write_atomic(_journal_path(root), json.dumps(journal, indent=2).encode("utf-8") + b"\n")
            raise
        if (root / backup_name).exists():
            shutil.rmtree(root / backup_name)
        shutil.rmtree(staging, ignore_errors=True)
        _journal_path(root).unlink(missing_ok=True)
        _sync_directory(root)
        return receipt
    except Exception as error:
        # Keep a journal long enough for an external recover, but perform the
        # same rollback immediately so an ordinary failed command is safe.
        try:
            recover_model(
                root,
                manifest,
                revision_receipt=revision_receipt_path,
            )
        except Exception as recovery_error:
            raise VerificationFailure(f"model acquisition failed ({error}); recovery failed: {recovery_error}") from recovery_error
        if isinstance(error, VerificationFailure):
            raise
        raise VerificationFailure(f"model acquisition failed: {error}") from error


def _receipt(manifest: dict[str, Any], operation: str, outcome: str, root: Path, verified: dict[str, Any], operation_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "operation_id": operation_id,
        "operation": operation,
        "outcome": outcome,
        "target": manifest["target"],
        "release_name": manifest["release_name"],
        "model": manifest["model"],
        "repository": manifest["repository"],
        "revision": manifest["revision"],
        "manifest_sha256": manifest["embedding_manifest_sha256"],
        "revision_provenance_sha256": manifest["revision_provenance_sha256"],
        "acquisition_manifest_sha256": sha256_bytes(canonical_json(manifest)),
        "root": str(root),
        "tree_sha256": verified["tree_sha256"],
        "files": verified["files"],
        "created_at_unix": int(time.time()),
    }


def _validate_installed_receipt(
    root: Path,
    manifest: dict[str, Any],
    verified: dict[str, Any],
    *,
    expected_operation_id: str | None = None,
    revision_receipt: Path | None = None,
) -> None:
    """Require the installed state receipt to identify the canonical model pin."""
    if revision_receipt is not None:
        validate_acquisition_manifest(manifest, revision_receipt=revision_receipt)
    receipt_path = root / manifest["receipt"]["relative_path"]
    receipt, _ = _load_json(receipt_path, "installed model acquisition receipt", MAX_RECEIPT_BYTES)
    required = set(manifest["receipt"]["required_fields"])
    _require(
        required <= set(receipt),
        "installed model acquisition receipt omits required identity fields",
    )
    _require(receipt.get("schema_version") == 1, "installed model acquisition receipt schema_version is invalid")
    operation_id = receipt.get("operation_id")
    _require(
        isinstance(operation_id, str) and bool(operation_id),
        "installed model acquisition receipt operation_id is invalid",
    )
    if expected_operation_id is not None:
        _require(
            operation_id == expected_operation_id,
            "installed model acquisition receipt operation identity differs from the pending transaction",
        )
    _require(receipt.get("operation") in {"install", "update"}, "installed model acquisition receipt operation is invalid")
    _require(receipt.get("outcome") in {"committed", "already_present"}, "installed model acquisition receipt outcome is invalid")
    for key in ("target", "model", "repository", "revision"):
        _require(receipt.get(key) == manifest[key], f"installed model acquisition receipt {key} differs from the release pin")
    _require(
        receipt.get("manifest_sha256") == manifest["embedding_manifest_sha256"],
        "installed model acquisition receipt manifest digest differs from the canonical model manifest",
    )
    _require(
        receipt.get("revision_provenance_sha256") == manifest["revision_provenance_sha256"],
        "installed model acquisition receipt revision receipt digest differs from the canonical receipt",
    )
    _require(
        receipt.get("acquisition_manifest_sha256") == sha256_bytes(canonical_json(manifest)),
        "installed model acquisition receipt descriptor digest differs from the release pin",
    )
    receipt_files = receipt.get("files")
    _require(isinstance(receipt_files, list), "installed model acquisition receipt files must be a list")
    expected_files = {
        entry["path"]: {"path": entry["path"], "bytes": entry["bytes"], "sha256": entry["sha256"]}
        for entry in manifest["files"]
    }
    seen_files: set[str] = set()
    validated_files: list[tuple[str, dict[str, Any]]] = []
    for index, entry in enumerate(receipt_files):
        _require(
            isinstance(entry, dict),
            f"installed model acquisition receipt files[{index}] must be an object",
        )
        relative = _safe_relative_path(
            entry.get("path"),
            f"installed model acquisition receipt files[{index}].path",
        )
        _require(
            relative not in seen_files,
            f"installed model acquisition receipt repeats {relative}",
        )
        _require_positive_int(
            entry.get("bytes"),
            f"installed model acquisition receipt files[{index}].bytes",
        )
        _require_digest(
            entry.get("sha256"),
            f"installed model acquisition receipt files[{index}].sha256",
        )
        seen_files.add(relative)
        validated_files.append((relative, entry))
    _require(
        seen_files == set(expected_files),
        "installed model acquisition receipt file set differs from the canonical model manifest",
    )
    for relative, entry in validated_files:
        _require(
            entry == expected_files[relative],
            "installed model acquisition receipt files differ from the canonical model manifest",
        )
    _require(
        receipt.get("tree_sha256") == verified["tree_sha256"],
        "installed model acquisition receipt tree digest differs from the installed model",
    )


def verify_release_contract(repo: Path) -> dict[str, Any]:
    """Verify checked-in release metadata without contacting the network."""
    manifest_path = repo / "product/ncm/release/model-acquisition-manifest.json"
    embedding_path = repo / "product/ncm/reference/embedding-manifest.json"
    worker_path = repo / "product/ncm/reference/worker-manifest.json"
    release_targets_path = repo / ".github/release-targets.json"
    worker_platforms_path = repo / "product/ncm/reference/worker-platforms.json"
    revision_receipt_path = repo / MODEL_REVISION_RECEIPT_PATH
    manifest, raw = _load_json(manifest_path, "release model acquisition manifest")
    validate_acquisition_manifest(
        manifest,
        embedding_manifest=embedding_path,
        revision_receipt=revision_receipt_path,
    )
    worker, worker_raw = _load_json(worker_path, "trusted worker manifest")
    _worker_manifest(worker, target=SUPPORTED_TARGET)
    checker_path = repo / "scripts/check-release-artifacts.py"
    _lstat_regular(checker_path, "release artifact checker")
    spec = importlib.util.spec_from_file_location(
        "ncm_release_artifact_checker", checker_path
    )
    _require(spec is not None and spec.loader is not None, "release artifact checker cannot be loaded")
    checker = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(checker)
        targets = checker.target_matrix(release_targets_path, worker_platforms_path)
    except (SystemExit, OSError, ValueError) as error:
        raise VerificationFailure(f"release target matrix is invalid: {error}") from error
    _require(
        any(
            target.get("name") == SUPPORTED_RELEASE_NAME
            and target.get("target") == SUPPORTED_TARGET
            and target.get("ncm") == "supported"
            for target in targets
        ),
        "release target matrix does not support the pinned NCM worker target",
    )
    _require(raw, "release model acquisition manifest is empty")
    return {
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_bytes(raw),
        "embedding_manifest_sha256": sha256_bytes(_read_regular(embedding_path, "trusted embedding manifest")),
        "revision_provenance_sha256": sha256_bytes(
            _read_regular(revision_receipt_path, "trusted model revision receipt")
        ),
        "worker_manifest_sha256": sha256_bytes(worker_raw),
        "target": SUPPORTED_TARGET,
        "release_targets": [target["name"] for target in targets],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    repo_default = Path(__file__).resolve().parents[3]
    parser.add_argument("--repo", type=Path, default=repo_default)
    parser.add_argument("--manifest", type=Path, default=repo_default / "product/ncm/release/model-acquisition-manifest.json")
    parser.add_argument("--embedding-manifest", type=Path, default=repo_default / "product/ncm/reference/embedding-manifest.json")
    parser.add_argument("--worker-manifest", type=Path, default=repo_default / "product/ncm/reference/worker-manifest.json")
    parser.add_argument("--revision-receipt", type=Path, default=repo_default / MODEL_REVISION_RECEIPT_PATH)
    parser.add_argument("--target", default=SUPPORTED_TARGET)
    parser.add_argument("--release-name", default=SUPPORTED_RELEASE_NAME)
    parser.add_argument("--profile", choices=("stable", "beta"), default="stable")
    parser.add_argument("--binary-archive", type=Path)
    parser.add_argument("--worker-archive", type=Path)
    parser.add_argument("--binary", type=Path, help="already installed CLI binary to smoke")
    parser.add_argument("--expected-version", help="trusted release SemVer stamped in --version")
    parser.add_argument("--expected-source-sha", help="trusted release source commit SHA")
    parser.add_argument("--expected-archive-sha256", help="trusted CLI archive SHA-256")
    parser.add_argument("--expected-binary-sha256", help="trusted installed CLI binary SHA-256")
    parser.add_argument("--model-root", "--state-root", dest="model_root", type=Path)
    parser.add_argument("--source-dir", type=Path, help="offline fixture source tree; never used implicitly")
    parser.add_argument("--receipt", type=Path, help="combined verification receipt path")
    parser.add_argument("--operation", choices=("install", "update", "recover", "verify"), default="verify")
    parser.add_argument(
        "--installed-e2e",
        action="store_true",
        help="exercise installed CLI NCM install, daemon restart, and production worker handshake",
    )
    parser.add_argument("--failure-after", choices=(*MODEL_REQUIRED_FILES, "staged", "backed_up"))
    arguments = parser.parse_args(argv)

    try:
        _require(
            arguments.operation == "verify" or arguments.model_root is not None,
            "--model-root is required for model lifecycle operations",
        )
        _require(
            arguments.source_dir is None or arguments.model_root is not None,
            "--source-dir requires --model-root",
        )
        if arguments.installed_e2e:
            _require(arguments.binary is not None, "--installed-e2e requires --binary")
            _require(arguments.worker_archive is not None, "--installed-e2e requires --worker-archive")
            _require(arguments.model_root is not None, "--installed-e2e requires --model-root")
            _require(
                arguments.operation in {"install", "verify"},
                "--installed-e2e requires --operation install or verify",
            )
        if arguments.binary_archive is None and arguments.worker_archive is None and arguments.binary is None and arguments.model_root is None and arguments.operation == "verify":
            result = verify_release_contract(arguments.repo.resolve())
            print(json.dumps(result, indent=2))
            return 0
        needs_model_contract = arguments.worker_archive is not None or arguments.model_root is not None
        model_manifest: dict[str, Any] | None = None
        model_raw: bytes | None = None
        if needs_model_contract:
            model_manifest, model_raw = _load_json(arguments.manifest, "model acquisition manifest")
            validate_acquisition_manifest(
                model_manifest,
                target=arguments.target,
                release_name=arguments.release_name,
                embedding_manifest=arguments.embedding_manifest,
                revision_receipt=arguments.revision_receipt,
            )
        result: dict[str, Any] = {"schema_version": 1, "target": arguments.target}
        if model_raw is not None:
            result["manifest_sha256"] = sha256_bytes(model_raw)
        installed_binary_result: dict[str, Any] | None = None
        if arguments.binary is not None:
            installed_binary_result = verify_installed_binary(
                arguments.binary,
                target=arguments.target,
                expected_version=arguments.expected_version,
                expected_source_sha=arguments.expected_source_sha,
                expected_binary_sha256=arguments.expected_binary_sha256,
            )
            result["installed_binary"] = installed_binary_result
        if arguments.binary_archive is not None:
            result["binary_archive"] = verify_binary_archive(
                arguments.binary_archive,
                target=arguments.target,
                profile=arguments.profile,
                expected_version=arguments.expected_version,
                expected_source_sha=arguments.expected_source_sha,
                expected_archive_sha256=arguments.expected_archive_sha256,
                expected_binary_sha256=(
                    installed_binary_result["sha256"]
                    if installed_binary_result is not None
                    else None
                ),
            )
        if arguments.worker_archive is not None:
            checksum = arguments.worker_archive.with_name(arguments.worker_archive.name + ".sha256")
            result["worker_archive"] = verify_worker_archive(
                arguments.worker_archive,
                target=arguments.target,
                worker_manifest_path=arguments.worker_manifest,
                model_manifest_path=arguments.manifest,
                revision_receipt_path=arguments.revision_receipt,
                checksum_path=checksum,
            )
        if arguments.model_root is not None:
            root = arguments.model_root.absolute()
            if arguments.operation == "recover":
                result["model"] = recover_model(
                    root,
                    model_manifest,
                    revision_receipt=arguments.revision_receipt,
                )
            elif arguments.operation in {"install", "update"}:
                _require(model_manifest is not None, "model acquisition manifest is required")
                result["model"] = acquire_model(
                    root,
                    model_manifest,
                    operation=arguments.operation,
                    source_dir=arguments.source_dir,
                    embedding_manifest_path=arguments.embedding_manifest,
                    revision_receipt_path=arguments.revision_receipt,
                    receipt_path=None,
                    failure_after=arguments.failure_after,
                )
            else:
                _require(model_manifest is not None, "model acquisition manifest is required")
                result["model"] = verify_model_tree(
                    root,
                    model_manifest,
                    manifest_bytes=_read_regular(
                        arguments.embedding_manifest,
                        "trusted embedding manifest",
                    ),
                    receipt_required=True,
                    revision_receipt=arguments.revision_receipt,
                )
        if arguments.installed_e2e:
            _require(model_manifest is not None and model_raw is not None, "--installed-e2e requires the model acquisition manifest")
            _require(arguments.binary is not None, "--installed-e2e requires --binary")
            _require(arguments.worker_archive is not None, "--installed-e2e requires --worker-archive")
            _require(arguments.model_root is not None, "--installed-e2e requires --model-root")
            result["installed_e2e"] = verify_installed_e2e(
                arguments.binary,
                arguments.worker_archive,
                arguments.model_root,
                model_manifest,
                acquisition_manifest_bytes=model_raw,
                embedding_manifest_path=arguments.embedding_manifest,
                worker_manifest_path=arguments.worker_manifest,
                revision_receipt=arguments.revision_receipt,
                target=arguments.target,
                release_name=arguments.release_name,
                profile=arguments.profile,
                expected_version=arguments.expected_version,
                expected_source_sha=arguments.expected_source_sha,
                expected_binary_sha256=arguments.expected_binary_sha256,
            )
        if arguments.receipt is not None:
            _write_atomic(arguments.receipt.resolve(), json.dumps(result, indent=2, ensure_ascii=False).encode("utf-8") + b"\n")
        print(json.dumps(result, indent=2))
        return 0
    except (VerificationFailure, OSError, subprocess.SubprocessError) as error:
        print(f"NCM installed release verification failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
