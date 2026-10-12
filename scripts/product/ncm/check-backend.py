#!/usr/bin/env python3
"""Validate and receipt the standalone Biomem-based Rust NCM backend.

Build the declared Bazel worker bundle, verify its worker and model pins, and
run the maintained adapter journey with the real encoder in isolated state.
The receipt records actual runtime results; host integration is verified separately.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from typing import Any

MODEL_CACHE_REPOSITORY = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2"
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REPOSITORY = "Xenova/paraphrase-multilingual-MiniLM-L12-v2"
MODEL_REVISION = "2c4055b12046f11709e9df2c122e59ffbdc2f900"
MODEL_REVISION_PROVENANCE = (
    "product/ncm/receipts/backend/2fc72f1d81f543224d8e7d8ef19195b026ba855f.json"
    "#/identities/model/revision"
)
MODEL_MAX_LENGTH = 128
MODEL_POOLING = "mean"
MODEL_NORMALIZE = True
MODEL_REQUIRED_FILES = (
    "onnx/model.onnx",
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
)
TEST_DOUBLE_MARKER = b"test-double/hash"
WORKER_MANIFEST_SCHEMA_VERSION = 1
WORKER_NAME = "tracedecay-ncm-worker"
# Mirrors `WORKER_EXECUTABLE_NAME` in worker_artifact.rs.
WORKER_EXECUTABLE_NAME = WORKER_NAME + (".exe" if os.name == "nt" else "")
# Mirrors the ncm-runtime build script: an absolute path here replaces the
# checked-in trust root, which pins no worker.
WORKER_MANIFEST_ENV = "TRACEDECAY_NCM_WORKER_MANIFEST"
WORKER_PROTOCOL_VERSION = 1
WORKER_PROTOCOL_IDENTITY = "tracedecay.ncm.worker.v1"


class GateFailure(RuntimeError):
    """One backend acceptance assertion failed."""


class VerifiedWorkerArtifact(dict[str, Any]):
    """Own a private staged worker and its sibling manifest until launch ends."""

    def __init__(
        self,
        receipt: dict[str, Any],
        *,
        source_path: Path,
        launch_path: Path,
        staging_dir: Path,
    ) -> None:
        super().__init__(receipt)
        self.source_path = source_path
        self.launch_path = launch_path
        self.staging_dir = staging_dir
        self._closed = False

    def close(self) -> None:
        """Remove the private staging directory after all child launches."""
        if self._closed:
            return
        self._closed = True
        shutil.rmtree(self.staging_dir, ignore_errors=True)

    def __enter__(self) -> VerifiedWorkerArtifact:
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:
        # The gate closes artifacts explicitly; this is only the failure-path
        # backstop when an unexpected exception aborts the gate.
        try:
            self.close()
        except Exception:
            pass


def canonical_json(value: Any) -> bytes:
    """Return stable compact UTF-8 JSON bytes."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    """Return a lowercase SHA-256 digest."""
    return hashlib.sha256(value).hexdigest()


def require(condition: bool, message: str) -> None:
    """Raise a typed gate failure when an assertion is false."""
    if not condition:
        raise GateFailure(message)


def run(
    command: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    check: bool = True,
    timeout: int = 3600,
    separate_stderr: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded command and retain output for the receipt."""
    print("+ " + " ".join(command), flush=True)
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if separate_stderr else subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if completed.stdout:
        lines = completed.stdout.splitlines()
        for line in lines[-40:]:
            print(line)
    if completed.stderr:
        for line in completed.stderr.splitlines()[-40:]:
            print(line, file=sys.stderr)
    if check and completed.returncode != 0:
        raise GateFailure(f"command exited {completed.returncode}: {' '.join(command)}")
    return completed


def git(repo: Path, *arguments: str) -> str:
    """Run a read-only git query."""
    completed = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise GateFailure(completed.stderr.strip() or f"git {' '.join(arguments)} failed")
    return completed.stdout.rstrip()


def bazel_output(repo: Path, target: str, environment: dict[str, str]) -> Path:
    """Resolve one declared Bazel output without guessing configuration paths."""
    completed = run(
        ["bazel", "cquery", target, "--output=files"],
        cwd=repo,
        environment=environment,
        separate_stderr=True,
    )
    outputs = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    require(len(outputs) == 1, f"{target} must produce exactly one file: {outputs}")
    path = Path(outputs[0])
    return path if path.is_absolute() else repo / path


def current_worker_target() -> tuple[str, str, str, str]:
    """Return the target tuple used by the Rust worker admission contract."""
    machine = platform.machine().lower()
    if sys.platform == "darwin":
        if machine in {"arm64", "aarch64"}:
            return "aarch64-apple-darwin", "macos", "aarch64", "unix"
        if machine in {"x86_64", "amd64"}:
            return "x86_64-apple-darwin", "macos", "x86_64", "unix"
    if sys.platform.startswith("linux"):
        if machine in {"arm64", "aarch64"}:
            arch = "aarch64"
        elif machine in {"x86_64", "amd64"}:
            arch = "x86_64"
        else:
            raise GateFailure(f"worker target is unsupported on this machine: {machine}")
        libc = platform.libc_ver()[0].lower()
        family = "musl" if "musl" in libc else "gnu"
        return f"{arch}-unknown-linux-{family}", "linux", arch, "unix"
    if sys.platform == "win32":
        if machine in {"arm64", "aarch64"}:
            arch = "aarch64"
        elif machine in {"x86_64", "amd64"}:
            arch = "x86_64"
        else:
            raise GateFailure(f"worker target is unsupported on this machine: {machine}")
        return f"{arch}-pc-windows-msvc", "windows", arch, "windows"
    raise GateFailure(f"worker target is unsupported on this platform: {sys.platform}")


def trusted_worker_manifest_path(repo: Path, environment: dict[str, str]) -> Path:
    """Return the trust root the ncm-runtime build script embeds."""
    override = environment.get(WORKER_MANIFEST_ENV)
    if override is None:
        return repo / "product" / "ncm" / "reference" / "worker-manifest.json"
    path = Path(override)
    require(path.is_absolute(), f"{WORKER_MANIFEST_ENV} must be an absolute path: {override}")
    return path


def worker_manifest_path(path: Path) -> Path:
    """Resolve the manifest explicitly bound beside a worker.

    The current working directory and binary ancestors are deliberately absent.
    The canonical bundle writer and private installer preserve this sibling
    binding for built and installed workers.
    """
    sibling = path.parent / "worker-manifest.json"
    try:
        metadata = sibling.lstat()
    except FileNotFoundError as error:
        raise GateFailure(f"worker manifest must be beside the worker: {sibling}") from error
    except OSError as error:
        raise GateFailure(f"inspect worker manifest {sibling}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise GateFailure(f"worker manifest must not be a symlink: {sibling}")
    if stat.S_ISREG(metadata.st_mode):
        return sibling
    raise GateFailure(f"worker manifest must be a regular file: {sibling}")


def _open_regular_no_follow(path: Path, *, label: str) -> int:
    """Open one regular file without following symlinks where supported."""
    try:
        path_metadata = path.lstat()
    except FileNotFoundError as error:
        raise GateFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise GateFailure(f"inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(path_metadata.st_mode):
        raise GateFailure(f"{label} must not be a symlink: {path}")
    if not stat.S_ISREG(path_metadata.st_mode):
        raise GateFailure(f"{label} must be a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if error.errno == errno.ELOOP or path.is_symlink():
            raise GateFailure(f"{label} must not be a symlink: {path}") from error
        raise GateFailure(f"read {label} {path}: {error}") from error
    try:
        metadata = os.fstat(descriptor)
    except OSError as error:
        os.close(descriptor)
        raise GateFailure(f"inspect {label} {path}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise GateFailure(f"{label} must be a regular file: {path}")
    return descriptor


def _read_regular_no_follow(
    path: Path, *, label: str, maximum_bytes: int | None = None
) -> bytes:
    """Read one exact regular-file handle, rejecting symlink substitution."""
    descriptor = _open_regular_no_follow(path, label=label)
    try:
        chunks: list[bytes] = []
        bytes_read = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)
            if maximum_bytes is not None and bytes_read > maximum_bytes:
                raise GateFailure(f"{label} exceeds the admitted byte bound: {path}")
            chunks.append(chunk)
        return b"".join(chunks)
    except OSError as error:
        raise GateFailure(f"read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)


def _regular_file_digest_no_follow(path: Path, *, label: str) -> tuple[int, str]:
    """Hash one exact regular-file descriptor and return its size and digest."""
    descriptor = _open_regular_no_follow(path, label=label)
    digest = hashlib.sha256()
    bytes_read = 0
    try:
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            bytes_read += len(chunk)
            digest.update(chunk)
    except OSError as error:
        raise GateFailure(f"read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    return bytes_read, digest.hexdigest()


def _directory_no_follow(path: Path, *, label: str) -> Path:
    """Require one existing directory entry without accepting symlinks."""
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise GateFailure(f"{label} is missing: {path}") from error
    except OSError as error:
        raise GateFailure(f"inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise GateFailure(f"{label} must not be a symlink: {path}")
    if not stat.S_ISDIR(metadata.st_mode):
        raise GateFailure(f"{label} must be a directory: {path}")
    return path


def _read_worker_manifest(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    """Read a worker manifest and retain the exact bytes for staging."""
    data = _read_regular_no_follow(path, label=f"{label} worker manifest")
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateFailure(f"read {label} worker manifest {path}: {error}") from error
    require(isinstance(value, dict), f"{label} worker manifest is not an object")
    return value, data


def _private_staging_directory(staging_root: Path | None = None) -> Path:
    """Create and validate one owner-private staging directory."""
    directory = Path(tempfile.mkdtemp(prefix="tracedecay-ncm-worker-", dir=staging_root))
    try:
        if os.name != "nt":
            os.chmod(directory, 0o700)
            metadata = directory.lstat()
            require(
                stat.S_IMODE(metadata.st_mode) == 0o700,
                f"worker staging directory is not private: {directory}",
            )
            if hasattr(os, "geteuid"):
                require(
                    metadata.st_uid == os.geteuid(),
                    f"worker staging directory is not owned by the current user: {directory}",
                )
        return directory
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise


def _write_private_file(directory: Path, name: str, payload: bytes, *, mode: int) -> Path:
    """Create one private regular file with exact bytes and permissions."""
    path = directory / name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise GateFailure(f"worker staging file must not be a symlink: {path}") from error
        raise GateFailure(f"create worker staging file {path}: {error}") from error
    try:
        offset = 0
        while offset < len(payload):
            try:
                written = os.write(descriptor, payload[offset:])
            except OSError as error:
                raise GateFailure(f"write worker staging file {path}: {error}") from error
            require(written > 0, f"write worker staging file made no progress: {path}")
            offset += written
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, mode)
        else:
            os.chmod(path, mode)
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        require(stat.S_ISREG(metadata.st_mode), f"worker staging file is not regular: {path}")
        if os.name != "nt":
            require(
                stat.S_IMODE(metadata.st_mode) == mode,
                f"worker staging file has unsafe permissions: {path}",
            )
    except OSError as error:
        raise GateFailure(f"seal worker staging file {path}: {error}") from error
    finally:
        os.close(descriptor)
    return path


def _stage_worker_bytes(
    source: Path,
    target_pin: dict[str, Any],
    manifest_bytes: bytes,
    manifest_path: Path,
    manifest_digest: str,
    staging_root: Path | None = None,
) -> VerifiedWorkerArtifact:
    """Hash source bytes once while copying them into a private launch root."""
    source_descriptor = _open_regular_no_follow(source, label="worker artifact")
    try:
        staging_dir = _private_staging_directory(staging_root)
    except Exception:
        os.close(source_descriptor)
        raise
    staged_path = staging_dir / WORKER_EXECUTABLE_NAME
    try:
        try:
            source_metadata = os.fstat(source_descriptor)
            if os.name != "nt":
                require(
                    source_metadata.st_mode & 0o111,
                    f"worker artifact is not executable: {source}",
                )
            require(
                source_metadata.st_size == target_pin["bytes"],
                f"worker artifact size mismatch: expected {target_pin['bytes']} bytes, got {source_metadata.st_size}",
            )

            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                staged_descriptor = os.open(staged_path, flags, 0o600)
            except OSError as error:
                if error.errno == errno.ELOOP:
                    raise GateFailure(
                        f"worker staging file must not be a symlink: {staged_path}"
                    ) from error
                raise GateFailure(f"create worker staging file {staged_path}: {error}") from error

            digest = hashlib.sha256()
            bytes_read = 0
            try:
                while True:
                    chunk = os.read(source_descriptor, 1024 * 1024)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    digest.update(chunk)
                    offset = 0
                    while offset < len(chunk):
                        written = os.write(staged_descriptor, chunk[offset:])
                        require(
                            written > 0,
                            f"write worker staging file made no progress: {staged_path}",
                        )
                        offset += written
            except OSError as error:
                raise GateFailure(f"copy worker artifact {source}: {error}") from error
            finally:
                try:
                    if hasattr(os, "fchmod"):
                        os.fchmod(staged_descriptor, 0o500)
                    else:
                        os.chmod(staged_path, 0o500)
                    os.fsync(staged_descriptor)
                finally:
                    os.close(staged_descriptor)

            require(
                bytes_read == target_pin["bytes"],
                f"worker artifact size mismatch: expected {target_pin['bytes']} bytes, got {bytes_read}",
            )
            source_final = os.fstat(source_descriptor)
            require(
                source_final.st_size == target_pin["bytes"],
                f"worker artifact size changed while reading: expected {target_pin['bytes']} bytes, got {source_final.st_size}",
            )
            digest_hex = digest.hexdigest()
            require(
                digest_hex == target_pin["sha256"],
                f"worker artifact digest mismatch: expected {target_pin['sha256']}, got {digest_hex}",
            )
            # chmod after the copy makes the staged launch pathname read/execute
            # only; the directory remains owner-private and create-new prevents
            # a replacement at that pathname.
            staged_metadata = staged_path.lstat()
            require(
                stat.S_ISREG(staged_metadata.st_mode),
                f"worker staging file is not regular: {staged_path}",
            )
            if os.name != "nt":
                require(
                    stat.S_IMODE(staged_metadata.st_mode) == 0o500,
                    f"worker staging file has unsafe permissions: {staged_path}",
                )
            _write_private_file(staging_dir, "worker-manifest.json", manifest_bytes, mode=0o600)
        finally:
            os.close(source_descriptor)
        receipt = {
            "path": str(source),
            "staged_path": str(staged_path),
            "staged_manifest_path": str(staging_dir / "worker-manifest.json"),
            "bytes": bytes_read,
            "sha256": digest_hex,
            "manifest_path": str(manifest_path),
            "manifest_sha256": manifest_digest,
            "target": target_pin["triple"],
        }
        return VerifiedWorkerArtifact(
            receipt,
            source_path=source,
            launch_path=staged_path,
            staging_dir=staging_dir,
        )
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


def validate_worker_manifest(manifest: dict[str, Any], *, target: tuple[str, str, str, str]) -> dict[str, Any]:
    """Validate the fields shared with ``worker_artifact.rs``."""
    require(
        set(manifest) == {"schema_version", "worker", "protocol_version", "protocol_identity", "targets"},
        "worker manifest has unexpected or missing fields",
    )
    require(
        isinstance(manifest["schema_version"], int)
        and not isinstance(manifest["schema_version"], bool)
        and manifest["schema_version"] == WORKER_MANIFEST_SCHEMA_VERSION,
        f"worker manifest schema version is unsupported: {manifest['schema_version']}",
    )
    require(
        manifest["worker"] == WORKER_NAME,
        f"worker manifest names {manifest['worker']}, expected {WORKER_NAME}",
    )
    require(
        isinstance(manifest["protocol_version"], int)
        and not isinstance(manifest["protocol_version"], bool)
        and manifest["protocol_version"] == WORKER_PROTOCOL_VERSION,
        f"worker manifest protocol version is unsupported: {manifest['protocol_version']}",
    )
    require(
        manifest["protocol_identity"] == WORKER_PROTOCOL_IDENTITY,
        f"worker manifest protocol identity is unsupported: {manifest['protocol_identity']}",
    )
    targets = manifest["targets"]
    # An empty list is the unpinned source trust root: every target is then
    # reported unsupported below, exactly as worker_artifact.rs does.
    require(isinstance(targets, list), "worker manifest targets must be a list")
    seen: set[str] = set()
    for item in targets:
        require(isinstance(item, dict), "worker manifest target is not an object")
        require(
            set(item) == {"triple", "os", "arch", "family", "bytes", "sha256"},
            "worker manifest target has unexpected or missing fields",
        )
        triple = item["triple"]
        require(
            isinstance(triple, str)
            and isinstance(item["os"], str)
            and isinstance(item["arch"], str)
            and isinstance(item["family"], str)
            and triple not in seen,
            f"worker manifest repeats target {triple}",
        )
        seen.add(triple)
        require(
            isinstance(item["bytes"], int)
            and not isinstance(item["bytes"], bool)
            and 0 < item["bytes"] <= 2**64 - 1,
            f"worker manifest target {triple} has an invalid byte count",
        )
        digest = item["sha256"]
        require(
            isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
            f"worker manifest target {triple} has an invalid sha256",
        )
    triple, os_name, arch, family = target
    selected = next((item for item in targets if item["triple"] == triple), None)
    require(selected is not None, f"worker target is not supported: {triple}")
    require((selected["os"], selected["arch"], selected["family"]) == (os_name, arch, family),
            f"worker manifest target metadata does not match {triple}")
    return selected


def read_worker_manifest(path: Path, *, label: str) -> dict[str, Any]:
    """Read one strict worker manifest through a no-follow file handle."""
    return _read_worker_manifest(path, label=label)[0]


def verify_worker_artifact(
    path: Path, *, repo: Path, environment: dict[str, str] | None = None,
    staging_root: Path | None = None,
) -> VerifiedWorkerArtifact:
    """Admit the exact worker bytes against the host's generated trust root.

    The pin proves artifact identity. Real inference is proven by the maintained
    adapter journey using the verified model, rather than strings in the binary.
    """
    trusted_manifest_path = trusted_worker_manifest_path(
        repo, dict(os.environ) if environment is None else environment
    )
    try:
        trusted_manifest_metadata = trusted_manifest_path.lstat()
    except FileNotFoundError as error:
        raise GateFailure(f"trusted worker manifest missing: {trusted_manifest_path}") from error
    except OSError as error:
        raise GateFailure(f"inspect trusted worker manifest {trusted_manifest_path}: {error}") from error
    require(
        not stat.S_ISLNK(trusted_manifest_metadata.st_mode),
        f"trusted worker manifest must not be a symlink: {trusted_manifest_path}",
    )
    require(
        stat.S_ISREG(trusted_manifest_metadata.st_mode),
        f"trusted worker manifest must be a regular file: {trusted_manifest_path}",
    )
    target = current_worker_target()
    trusted_manifest, _trusted_manifest_bytes = _read_worker_manifest(
        trusted_manifest_path, label="trusted"
    )
    selected_manifest_path = worker_manifest_path(path)
    selected_manifest, selected_manifest_bytes = _read_worker_manifest(
        selected_manifest_path, label="selected"
    )
    trusted_digest = sha256_bytes(canonical_json(trusted_manifest))
    selected_digest = sha256_bytes(canonical_json(selected_manifest))
    require(
        selected_digest == trusted_digest,
        f"worker manifest is stale or not sourced from the trusted build root: expected {trusted_digest}, got {selected_digest}",
    )
    target_pin = validate_worker_manifest(trusted_manifest, target=target)
    validate_worker_manifest(selected_manifest, target=target)
    return _stage_worker_bytes(
        path,
        target_pin,
        selected_manifest_bytes,
        selected_manifest_path,
        selected_digest,
        staging_root,
    )


def model_snapshot(model_root: Path, manifest: dict[str, Any]) -> tuple[Path, str]:
    """Resolve the installed snapshot using Rust's exact pinned no-follow rules."""
    require(isinstance(manifest, dict), "embedding manifest is not an object")
    require(manifest.get("model") == MODEL_NAME, "embedding manifest model is not pinned")
    require(
        manifest.get("repository") == MODEL_REPOSITORY,
        "embedding manifest repository is not pinned",
    )
    require(
        manifest.get("revision") == MODEL_REVISION,
        "embedding manifest revision is not the pinned Xenova revision",
    )
    require(
        manifest.get("revision_provenance") == MODEL_REVISION_PROVENANCE,
        "embedding manifest revision provenance is not pinned",
    )
    require(
        manifest.get("max_length") == MODEL_MAX_LENGTH,
        "embedding manifest max length is not pinned",
    )
    require(manifest.get("pooling") == MODEL_POOLING, "embedding manifest pooling is not pinned")
    require(
        manifest.get("normalize") is MODEL_NORMALIZE,
        "embedding manifest normalization is not pinned",
    )
    files = manifest.get("files")
    require(isinstance(files, list), "embedding manifest files are not a list")
    require(len(files) == len(MODEL_REQUIRED_FILES), "embedding manifest has the wrong file count")
    seen: set[str] = set()
    for item in files:
        require(isinstance(item, dict), "embedding manifest file is not an object")
        require(
            set(item) == {"path", "sha256", "bytes"},
            "embedding manifest file has unexpected or missing fields",
        )
        relative = item["path"]
        require(
            isinstance(relative, str) and relative in MODEL_REQUIRED_FILES,
            f"embedding manifest has an unsafe or unexpected path: {relative}",
        )
        require(relative not in seen, f"embedding manifest repeats {relative}")
        seen.add(relative)
        digest = item["sha256"]
        require(
            isinstance(digest, str)
            and len(digest) == 64
            and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
            f"embedding manifest has an invalid digest for {relative}",
        )
        require(
            isinstance(item["bytes"], int)
            and not isinstance(item["bytes"], bool)
            and item["bytes"] > 0,
            f"embedding manifest has an invalid size for {relative}",
        )
    require(set(seen) == set(MODEL_REQUIRED_FILES), "embedding manifest does not pin all model files")

    models = _directory_no_follow(model_root / "models", label="model cache directory")
    repository = _directory_no_follow(
        models / MODEL_CACHE_REPOSITORY,
        label="model cache repository",
    )
    refs = _directory_no_follow(repository / "refs", label="model cache refs directory")
    revision_file = refs / "main"
    revision_bytes = _read_regular_no_follow(
        revision_file,
        label="pinned model ref",
        maximum_bytes=256,
    )
    try:
        revision = revision_bytes.decode("utf-8").strip()
    except UnicodeDecodeError as error:
        raise GateFailure(f"pinned model ref is not UTF-8: {revision_file}") from error
    require(
        revision == MODEL_REVISION,
        f"pinned model ref is {revision}, expected {MODEL_REVISION}",
    )
    snapshots = _directory_no_follow(
        repository / "snapshots",
        label="model cache snapshots directory",
    )
    snapshot = _directory_no_follow(
        snapshots / MODEL_REVISION,
        label="pinned model snapshot",
    )
    return snapshot, revision


def verify_model(model_root: Path, manifest_path: Path) -> dict[str, Any]:
    """Verify every pinned model byte count, digest, and local manifest."""
    manifest_bytes = _read_regular_no_follow(
        manifest_path,
        label="checked-in embedding manifest",
        maximum_bytes=16 * 1024 * 1024,
    )
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateFailure(f"read checked-in embedding manifest {manifest_path}: {error}") from error
    snapshot, revision = model_snapshot(model_root, manifest)
    checked = []
    for item in manifest["files"]:
        relative = item["path"]
        parent = snapshot
        if relative == "onnx/model.onnx":
            parent = _directory_no_follow(snapshot / "onnx", label="pinned model ONNX directory")
            path = parent / "model.onnx"
        else:
            path = snapshot / relative
        actual_bytes, actual_sha256 = _regular_file_digest_no_follow(
            path,
            label=f"pinned model file {relative}",
        )
        require(actual_bytes == item["bytes"], f"model size mismatch: {item['path']}")
        require(actual_sha256 == item["sha256"], f"model digest mismatch: {item['path']}")
        checked.append({"path": item["path"], "bytes": actual_bytes, "sha256": actual_sha256})
    local_manifest = model_root / "models" / "ncm-encoder-manifest.json"
    local_manifest_bytes = _read_regular_no_follow(
        local_manifest,
        label="runtime model manifest",
        maximum_bytes=16 * 1024 * 1024,
    )
    try:
        local_manifest_value = json.loads(local_manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateFailure(f"read runtime model manifest {local_manifest}: {error}") from error
    require(
        local_manifest_value == manifest,
        "runtime model manifest differs from checked-in pin",
    )
    onnx_manifest_file = next(
        item for item in manifest["files"] if item["path"] == "onnx/model.onnx"
    )
    return {
        "root": str(model_root),
        "model": manifest["model"],
        "artifact_sha256": onnx_manifest_file["sha256"],
        "revision": revision,
        "manifest_sha256": sha256_bytes(manifest_bytes),
        "files": checked,
    }


def install_model(
    repo: Path, binary: Path, project_root: Path, model_root: Path,
    environment: dict[str, str],
) -> list[str]:
    """Use the shipped CLI's project/profile admission and sole model installer."""
    command = [
        str(binary), "--yes", "ncm", "install-model", "--path", str(project_root),
        "--state-root", str(model_root), "--json",
    ]
    run(command, cwd=repo, environment=environment, timeout=7200)
    return command


class WorkerWire:
    """Minimal stdlib client for artifact and identity assertions."""

    def __init__(self, binary: Path, state_root: Path, *, test_double: bool, path: str | None = None):
        arguments = [str(binary), "--state-root", str(state_root)]
        if test_double:
            arguments.append("--test-double")
        environment = os.environ.copy()
        if path is not None:
            environment["PATH"] = path
        self.process = subprocess.Popen(
            arguments,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        self.next_id = 1

    def call(self, operation: str, namespace: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Send one framed worker request."""
        require(self.process.stdin is not None and self.process.stdout is not None, "worker pipes unavailable")
        request = canonical_json(
            {
                "protocol_version": 1,
                "id": self.next_id,
                "deadline_ms": 30_000,
                "op": operation,
                "namespace": namespace,
                "payload": payload or {},
            }
        )
        self.next_id += 1
        self.process.stdin.write(struct.pack(">I", len(request)) + request)
        self.process.stdin.flush()
        header = self.process.stdout.read(4)
        if len(header) != 4:
            raise GateFailure(self._closed_message("worker closed before reply"))
        length = struct.unpack(">I", header)[0]
        body = self.process.stdout.read(length)
        require(len(body) == length, "worker reply was truncated")
        return json.loads(body)

    def _closed_message(self, prefix: str) -> str:
        error = b""
        if self.process.stderr is not None:
            error = self.process.stderr.read()
        return f"{prefix}: {error.decode(errors='replace')[-2000:]}"

    def close(self) -> None:
        """Close stdin and require a clean worker exit."""
        if self.process.stdin is not None:
            self.process.stdin.close()
        try:
            code = self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            code = self.process.wait(timeout=5)
        require(code == 0, self._closed_message(f"worker exited {code}"))


def worker_identity(binary: Path, state_root: Path, empty_path: Path, *, test_double: bool) -> dict[str, Any]:
    """Read the worker handshake identity with Python absent from PATH."""
    empty_path.mkdir(parents=True, exist_ok=True)
    worker = WorkerWire(binary, state_root, test_double=test_double, path=str(empty_path))
    try:
        reply = worker.call(
            "handshake",
            "0" * 64,
            {
                "protocol_version": WORKER_PROTOCOL_VERSION,
                "protocol_identity": WORKER_PROTOCOL_IDENTITY,
                "algorithm_profile": "ncm-biomem-rs.v1",
            },
        )
    finally:
        worker.close()
    require(str(reply.get("outcome", "")).lower() == "success", f"worker handshake failed: {reply}")
    payload = reply.get("payload") or {}
    encoder = payload.get("encoder") or {}
    if test_double:
        require(encoder.get("model") == TEST_DOUBLE_MARKER.decode(), "--test-double did not expose hash identity")
    else:
        require(encoder.get("model") != TEST_DOUBLE_MARKER.decode(), "production launch exposed hash identity")
        require(
            encoder.get("model") == "paraphrase-multilingual-MiniLM-L12-v2",
            f"unexpected production model identity: {encoder.get('model')}",
        )
    return {
        "state_generation": reply.get("state_generation"),
        "algorithm": payload.get("algorithm"),
        "projection_sha256": payload.get("projection_sha256"),
        "encoder": encoder,
        "epoch": payload.get("epoch"),
        "state_schema": payload.get("state_schema"),
    }


def prepare_isolated_root(model_root: Path, journey_root: Path) -> None:
    """Create a fresh namespace root containing an owned model cache copy."""
    require(journey_root.is_absolute(), "journey state root must be absolute")
    if journey_root.exists():
        require(not any(journey_root.iterdir()), f"journey state root is not fresh: {journey_root}")
    else:
        journey_root.mkdir(parents=True)
    source_models = _directory_no_follow(model_root / "models", label="verified model directory")
    destination = journey_root / "models"
    shutil.copytree(source_models, destination, symlinks=False)


def parse_test_counts(output: str) -> dict[str, int]:
    """Sum libtest result lines and reject vacuous success at the caller."""
    counts = {"passed": 0, "failed": 0, "ignored": 0}
    for match in re.finditer(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored", output):
        counts["passed"] += int(match.group(1))
        counts["failed"] += int(match.group(2))
        counts["ignored"] += int(match.group(3))
    return counts


def execute_population(
    name: str, command: list[str], *, repo: Path, environment: dict[str, str],
    require_zero_ignored: bool = True,
) -> dict[str, Any]:
    """Run a maintained test population and require executed assertions."""
    completed = run(command, cwd=repo, environment=environment, check=False)
    counts = parse_test_counts(completed.stdout)
    require(completed.returncode == 0, f"{name} tests failed")
    require(counts["passed"] > 0, f"{name} executed zero passing tests")
    require(counts["failed"] == 0, f"{name} reported failing tests")
    if require_zero_ignored:
        require(counts["ignored"] == 0, f"{name} skipped {counts['ignored']} mandatory tests")
    return {"name": name, "command": command, "counts": counts, "status": "pass"}


def main() -> int:
    """Verify the production backend using declared Bazel artifacts."""
    parser = argparse.ArgumentParser(description=__doc__)
    workspace = Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", Path(__file__).resolve().parents[3]))
    parser.add_argument("--repo", type=Path, default=workspace)
    parser.add_argument("--model-root", type=Path)
    parser.add_argument("--state-root", type=Path, required=True, help="absolute scratch parent; temporary state is removed")
    parser.add_argument(
        "--project-root", type=Path, required=True,
        help="registered project selector for canonical model admission in isolated state",
    )
    parser.add_argument("--install-model", action="store_true")
    parser.add_argument("--receipt-dir", type=Path)
    arguments = parser.parse_args()
    repo = (workspace / arguments.repo).resolve()
    environment = os.environ.copy()
    model_root = arguments.model_root
    if model_root is None and environment.get("TRACEDECAY_NCM_REAL_MODEL_ROOT"):
        model_root = Path(environment["TRACEDECAY_NCM_REAL_MODEL_ROOT"])
    if model_root is None or not model_root.is_absolute():
        parser.error("--model-root or TRACEDECAY_NCM_REAL_MODEL_ROOT must be an absolute state root")
    if not arguments.state_root.is_absolute():
        parser.error("--state-root must be absolute; configure large scratch data on the external volume")
    scratch_root = arguments.state_root
    scratch_root.mkdir(parents=True, exist_ok=True)
    _directory_no_follow(scratch_root, label="scratch parent")
    receipt_dir = arguments.receipt_dir or scratch_root / "receipts"
    require(receipt_dir.is_absolute(), "receipt directory must be absolute")
    receipt_dir.mkdir(parents=True, exist_ok=True)
    _directory_no_follow(receipt_dir, label="receipt directory")
    source_commit = git(repo, "rev-parse", "HEAD")
    blockers: list[str] = []
    commands: list[list[str]] = []
    populations: list[dict[str, Any]] = []
    artifact: VerifiedWorkerArtifact | None = None
    model: dict[str, Any] | None = None
    identities: dict[str, Any] = {}
    journey: dict[str, Any] | None = None
    real_target = "//crates/tracedecay-memory-provider-ncm:rust_backend_conformance__binary"
    build = [
        "bazel", "build", "//product/ncm:worker_bundle", real_target,
        "//crates/tracedecay-cli:tracedecay",
    ]
    commands.append(build)
    try:
        run(build, cwd=repo, environment=environment)
        worker = bazel_output(repo, "//product/ncm:worker", environment)
        trusted_manifest = bazel_output(repo, "//product/ncm:worker_manifest", environment)
        cli = bazel_output(repo, "//crates/tracedecay-cli:tracedecay", environment)
        trust_environment = environment.copy()
        trust_environment[WORKER_MANIFEST_ENV] = str(trusted_manifest)
        artifact = verify_worker_artifact(
            worker, repo=repo, environment=trust_environment, staging_root=scratch_root,
        )
        model_manifest = repo / "product/ncm/reference/embedding-manifest.json"
        try:
            model = verify_model(model_root, model_manifest)
        except GateFailure:
            if not arguments.install_model:
                raise
            commands.append(install_model(repo, cli, arguments.project_root.resolve(), model_root, environment))
            model = verify_model(model_root, model_manifest)

        # Standard wrappers supply their explicit test worker and isolate profile
        # state. They never substitute that worker into the production journey.
        for name, targets in (
            # The core library has no inline tests; its maintained population
            # lives in these integration targets.
            ("core", [
                "//crates/tracedecay-memory-ncm-core:center_read",
                "//crates/tracedecay-memory-ncm-core:center_write",
                "//crates/tracedecay-memory-ncm-core:consolidation",
                "//crates/tracedecay-memory-ncm-core:dynamics",
                "//crates/tracedecay-memory-ncm-core:kernel",
                "//crates/tracedecay-memory-ncm-core:numeric",
                "//crates/tracedecay-memory-ncm-core:projections",
                "//crates/tracedecay-memory-ncm-core:record_recall",
                "//crates/tracedecay-memory-ncm-core:signals",
                "//crates/tracedecay-memory-ncm-core:terrain",
            ]),
            ("runtime", ["//crates/tracedecay-memory-ncm-runtime:unit_test"]),
            ("adapter", ["//crates/tracedecay-memory-provider-ncm:rust_backend"]),
        ):
            command = ["bazel", "test", *targets, "--test_output=all"]
            commands.append(command)
            try:
                populations.append(execute_population(
                    name, command, repo=repo, environment=environment,
                    require_zero_ignored=name != "adapter",
                ))
            except (GateFailure, subprocess.TimeoutExpired) as error:
                blockers.append(f"{name}: {error}")
                populations.append({"name": name, "command": command, "status": "blocked", "error": str(error)})

        binary = bazel_output(repo, real_target, environment)
        with tempfile.TemporaryDirectory(prefix="tracedecay-ncm-journey-", dir=scratch_root) as temporary:
            owned_root = Path(temporary)
            journey_root = owned_root / "state"
            prepare_isolated_root(model_root, journey_root)
            # Verified bytes still need acquisition authority bound to this
            # destination; another root's receipt cannot be copied or reused.
            commands.append(install_model(
                repo, cli, arguments.project_root.resolve(), journey_root, environment,
            ))
            empty_path = owned_root / "empty-path"
            empty_path.mkdir()
            identities["production"] = worker_identity(artifact.launch_path, journey_root, empty_path, test_double=False)
            double_root = owned_root / "double"
            double_root.mkdir()
            identities["test_double_control"] = worker_identity(artifact.launch_path, double_root, empty_path, test_double=True)
            real_environment = environment.copy()
            real_environment.update({
                "PATH": str(empty_path),
                "HOME": str(owned_root / "home"),
                "TRACEDECAY_DATA_DIR": str(owned_root / "profile"),
                "CARGO_TARGET_DIR": str(owned_root / "test-state"),
                "TEST_TMPDIR": str(owned_root / "tmp"),
                "TMPDIR": str(owned_root / "tmp"),
                "TRACEDECAY_NCM_WORKER": str(artifact.launch_path),
                "TRACEDECAY_NCM_REAL_MODEL_ROOT": str(journey_root),
            })
            for key in ("HOME", "TRACEDECAY_DATA_DIR", "CARGO_TARGET_DIR", "TEST_TMPDIR"):
                Path(real_environment[key]).mkdir()
            command = [str(binary), "enabled::real_encoder_process_population", "--exact", "--ignored", "--test-threads=1"]
            commands.append(command)
            journey = execute_population("real_adapter_journey", command, repo=repo, environment=real_environment)
            journey.update({"path_is_empty": True, "state_is_temporary": True})
    except (GateFailure, OSError, subprocess.TimeoutExpired) as error:
        blockers.append(str(error))
    finally:
        if artifact is not None:
            artifact.close()

    receipt = {
        "schema_version": 1,
        "status": "pass" if not blockers else "blocked",
        "source_commit": source_commit,
        "source_tree": git(repo, "rev-parse", "HEAD^{tree}"),
        "working_tree_dirty": bool(git(repo, "status", "--porcelain")),
        "branch": git(repo, "branch", "--show-current"),
        "worker_artifact": artifact,
        "model": model,
        "identities": identities,
        "test_populations": populations,
        "journey": journey,
        "exact_commands": commands,
        "limitations": [
            "The adapter has no public STM-mask seam; the real journey asserts consolidation, LTM activity, restart recall, deletion and stale snapshot restore.",
            "This receipt verifies the standalone backend; shipped host behavior and release packaging require their own production journeys.",
        ],
        "blockers": blockers,
    }
    receipt_path = receipt_dir / f"backend-{source_commit}-{time.time_ns()}.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"backend receipt: {receipt_path}")
    for blocker in blockers:
        print(f"BLOCKER: {blocker}", file=sys.stderr)
    return 2 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
