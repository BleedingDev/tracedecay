"""Safe offline installation and verification for code-semantic model bytes.

The model source is an explicit local directory.  This module intentionally
does not contain a network client or a model-cache lookup.  A separate,
operator-controlled process may obtain bytes from the pinned upstream
revision; this contract starts after those bytes have arrived locally.

The on-disk protocol is small and explicit:

* ``fixture.json`` declares the complete five-member inventory;
* every member has a safe relative path, exact length, and SHA-256 digest;
* source, staging, and published trees contain only regular files with one
  link each;
* a sidecar journal records the target and identity before publication;
* the completed staging directory is atomically renamed into place; and
* a sidecar acquisition receipt binds target, model, revision, manifest, and
  package digest.

The package digest matches the historical Rust semantic catalog algorithm from
commits ``1cbad2dda`` and ``dca4de9de``.  In particular, the pinned Jina
fixture evaluates to ``70be8116...3c15af``.  This is a digest-only identity
contract; it does not claim a signature or signed attestation.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import secrets
import stat
import uuid
from contextlib import contextmanager
from types import MappingProxyType
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Mapping, Sequence


MANIFEST_SCHEMA = "tracedecay.distribution.fastembed-fixture.v1"
RECEIPT_SCHEMA = "tracedecay.product.semantic-acquisition-receipt.v1"
JOURNAL_SCHEMA = "tracedecay.product.semantic-install-journal.v1"
UNINSTALL_JOURNAL_SCHEMA = "tracedecay.product.semantic-uninstall-journal.v1"
EXPECTED_MODEL = "JinaEmbeddingsV2BaseCode"
EXPECTED_REVISION = "516f4baf13dec4ddddda8631e019b5737c8bc250"
EXPECTED_UPSTREAM = "https://huggingface.co/jinaai/jina-embeddings-v2-base-code"
EXPECTED_LICENSE = "Apache-2.0"
EXPECTED_LICENSE_URL = "https://www.apache.org/licenses/LICENSE-2.0"
EXPECTED_DIMENSIONS = 768
EXPECTED_MAX_LENGTH = 8192
EXPECTED_ARTIFACT_DIGEST = (
    "70be81163e9740d742b7857e132713b323b5042d661485354d781cb8313c15af"
)
REQUIRED_ROLES = (
    "model",
    "tokenizer",
    "config",
    "special_tokens_map",
    "tokenizer_config",
)
EXPECTED_MEMBER_PATHS = {
    "model": ("model.onnx", "onnx/model.onnx"),
    "tokenizer": ("tokenizer.json", "tokenizer.json"),
    "config": ("config.json", "config.json"),
    "special_tokens_map": ("special_tokens_map.json", "special_tokens_map.json"),
    "tokenizer_config": ("tokenizer_config.json", "tokenizer_config.json"),
}

_SHA256_RE = frozenset("0123456789abcdef")
_HEX_LENGTH = 64
_REVISION_LENGTH = 40
_MAX_MEMBER_BYTES = 2 * 1024 * 1024 * 1024
_MAX_TOTAL_BYTES = 4 * 1024 * 1024 * 1024
_CHUNK_SIZE = 1024 * 1024
_MANIFEST_NAME = "fixture.json"

# macOS exposes a few writable system roots through compatibility aliases.
# Resolve only these exact, OS-owned links before checking a caller-supplied
# path.  A user-created link anywhere below the resulting physical root stays
# visible to ``lstat`` and is rejected by the normal path checks.
_TRUSTED_OS_ANCESTOR_ALIASES = {
    Path("/etc"): Path("/private/etc"),
    Path("/home"): Path("/System/Volumes/Data/home"),
    Path("/tmp"): Path("/private/tmp"),
    Path("/var"): Path("/private/var"),
}


class SemanticProvisioningError(RuntimeError):
    """Base class for errors that leave the existing publication untouched."""


class ManifestValidationError(SemanticProvisioningError, ValueError):
    """The manifest is malformed or does not describe the pinned model."""


class UnsafePathError(SemanticProvisioningError, ValueError):
    """A path, link, or filesystem entry violates the package boundary."""


class VerificationError(SemanticProvisioningError, ValueError):
    """A package member, inventory, or published tree is not exact."""


class ReceiptValidationError(SemanticProvisioningError, ValueError):
    """An acquisition receipt is missing, malformed, or identity-mismatched."""


class JournalValidationError(SemanticProvisioningError, ValueError):
    """A recovery journal is malformed or does not bind to this operation."""


class SimulatedInterruption(SemanticProvisioningError):
    """Test/operator fault injection that intentionally leaves recovery state."""


@dataclass(frozen=True, slots=True)
class ModelManifest:
    """Validated immutable metadata for one model package."""

    document: Mapping[str, Any]

    @property
    def model(self) -> str:
        return self.document["model"]

    @property
    def revision(self) -> str:
        source = self.document["source"]
        return source["revision"]

    @property
    def members(self) -> Mapping[str, Mapping[str, Any]]:
        return self.document["members"]

    @property
    def roles(self) -> tuple[str, ...]:
        return tuple(self.members)

    @property
    def dimensions(self) -> int:
        return self.document["expected_dimensions"]

    @property
    def max_length(self) -> int:
        return self.document["max_length"]


@dataclass(frozen=True)
class DirectoryVerification:
    """Evidence returned after a complete exact directory scan."""

    root: Path
    manifest_sha256: str
    artifact_digest: str
    member_count: int
    total_bytes: int


@dataclass(frozen=True)
class InstallResult:
    """The durable result of an offline publication."""

    target: Path
    receipt: Path
    artifact_digest: str
    status: str


def canonical_json(value: Mapping[str, Any]) -> bytes:
    """Encode a JSON object deterministically for identity and receipts."""

    try:
        encoded = json.dumps(
            _thaw_json(value),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ManifestValidationError(f"cannot canonicalize JSON: {error}") from error
    return encoded


def _freeze_json(value: Any) -> Any:
    """Recursively freeze JSON values stored in a validated manifest."""

    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    """Return ordinary JSON containers for the encoder and receipt checks."""

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_lower_hex(value: Any, length: int = _HEX_LENGTH) -> bool:
    return isinstance(value, str) and len(value) == length and set(value) <= _SHA256_RE


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{field} must be a non-empty string")
    if "\x00" in value:
        raise ManifestValidationError(f"{field} must not contain NUL")
    return value


def _safe_relative_path(value: Any, field: str) -> str:
    """Validate a portable POSIX relative path without normalizing it."""

    if not isinstance(value, str) or not value:
        raise ManifestValidationError(f"{field} must be a non-empty relative path")
    if "\x00" in value or "\\" in value or value.startswith("/"):
        raise ManifestValidationError(f"{field} must be a portable relative path")
    if value.endswith("/") or "//" in value:
        raise ManifestValidationError(f"{field} must not contain empty path segments")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ManifestValidationError(f"{field} must not contain traversal segments")
    # A colon is rejected everywhere so a Windows drive or alternate data
    # stream cannot become a second interpretation of the same declaration.
    for part in parts:
        if ":" in part or not all(
            character.isascii()
            and (character.isalnum() or character in "._-")
            for character in part
        ):
            raise ManifestValidationError(f"{field} contains an unsafe path segment")
    # PurePosixPath is an assertion that the accepted syntax remains relative;
    # it does not perform the security decision for us.
    if PurePosixPath(value).is_absolute():
        raise ManifestValidationError(f"{field} must be relative")
    return value


def _expect_exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    missing = expected - actual
    unknown = actual - expected
    if missing or unknown:
        detail = []
        if missing:
            detail.append("missing " + ", ".join(sorted(missing)))
        if unknown:
            detail.append("unknown " + ", ".join(sorted(unknown)))
        raise ManifestValidationError(f"{field} has invalid fields ({'; '.join(detail)})")


def _positive_int(value: Any, field: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ManifestValidationError(f"{field} must be an integer in 1..={maximum}")
    return value


def parse_manifest(document: Mapping[str, Any], *, require_pinned: bool = True) -> ModelManifest:
    """Parse and validate a manifest without touching the filesystem.

    ``require_pinned=False`` is provided for focused tests of the protocol with
    tiny local payloads.  The checked-in product and distribution manifests,
    and all command-line paths, use the default pinned mode.
    """

    if not isinstance(document, Mapping):
        raise ManifestValidationError("manifest must be a JSON object")
    copied = json.loads(json.dumps(_thaw_json(document), ensure_ascii=True, allow_nan=False))
    _expect_exact_keys(
        copied,
        {"schema", "model", "source", "expected_dimensions", "max_length", "members", "artifact_digest"},
        "manifest",
    )
    if copied["schema"] != MANIFEST_SCHEMA:
        raise ManifestValidationError(f"manifest schema must be {MANIFEST_SCHEMA!r}")
    model = _require_string(copied["model"], "model")

    source = copied["source"]
    if not isinstance(source, Mapping):
        raise ManifestValidationError("source must be an object")
    _expect_exact_keys(
        source,
        {"upstream", "revision", "license", "license_url", "provenance"},
        "source",
    )
    upstream = _require_string(source["upstream"], "source.upstream")
    revision = _require_string(source["revision"], "source.revision")
    license_name = _require_string(source["license"], "source.license")
    license_url = _require_string(source["license_url"], "source.license_url")
    provenance = _require_string(source["provenance"], "source.provenance")
    if len(revision) != _REVISION_LENGTH or set(revision) - _SHA256_RE:
        raise ManifestValidationError("source.revision must be a full lowercase Git commit")
    if revision not in provenance:
        raise ManifestValidationError("source.provenance must identify source.revision")

    dimensions = _positive_int(copied["expected_dimensions"], "expected_dimensions", 65_536)
    max_length = _positive_int(copied["max_length"], "max_length", 8_192)
    members = copied["members"]
    if not isinstance(members, Mapping):
        raise ManifestValidationError("members must be an object")
    if set(members) != set(REQUIRED_ROLES):
        raise ManifestValidationError(
            "members must contain exactly: " + ", ".join(REQUIRED_ROLES)
        )

    local_paths: set[str] = {_MANIFEST_NAME}
    upstream_paths: set[str] = set()
    total_length = 0
    for role in REQUIRED_ROLES:
        member = members[role]
        if not isinstance(member, Mapping):
            raise ManifestValidationError(f"members.{role} must be an object")
        _expect_exact_keys(member, {"path", "upstream_path", "length", "sha256"}, f"members.{role}")
        path = _safe_relative_path(member["path"], f"members.{role}.path")
        upstream_path = _safe_relative_path(
            member["upstream_path"], f"members.{role}.upstream_path"
        )
        if path in local_paths:
            raise ManifestValidationError(f"duplicate local member path {path!r}")
        if upstream_path in upstream_paths:
            raise ManifestValidationError(f"duplicate upstream member path {upstream_path!r}")
        local_paths.add(path)
        upstream_paths.add(upstream_path)
        length = _positive_int(member["length"], f"members.{role}.length", _MAX_MEMBER_BYTES)
        digest = member["sha256"]
        if not _is_lower_hex(digest):
            raise ManifestValidationError(
                f"members.{role}.sha256 must be 64 lowercase hexadecimal characters"
            )
        total_length += length
        if total_length > _MAX_TOTAL_BYTES:
            raise ManifestValidationError(f"declared package exceeds {_MAX_TOTAL_BYTES} bytes")

    if require_pinned:
        if model != EXPECTED_MODEL:
            raise ManifestValidationError(f"model must be {EXPECTED_MODEL!r}")
        if (
            upstream != EXPECTED_UPSTREAM
            or revision != EXPECTED_REVISION
            or license_name != EXPECTED_LICENSE
            or license_url != EXPECTED_LICENSE_URL
        ):
            raise ManifestValidationError("manifest source is not the pinned Jina revision")
        if dimensions != EXPECTED_DIMENSIONS or max_length != EXPECTED_MAX_LENGTH:
            raise ManifestValidationError("manifest dimensions or maximum length are not pinned")
        for role, (path, upstream_path) in EXPECTED_MEMBER_PATHS.items():
            member = members[role]
            if member["path"] != path or member["upstream_path"] != upstream_path:
                raise ManifestValidationError(f"members.{role} path is not the pinned Jina path")

    computed = _artifact_digest_from_document(copied)
    declared_digest = copied["artifact_digest"]
    if not _is_lower_hex(declared_digest):
        raise ManifestValidationError("artifact_digest must be 64 lowercase hexadecimal characters")
    if declared_digest != computed:
        raise ManifestValidationError(
            f"artifact_digest mismatch: expected computed {computed}, got {declared_digest}"
        )
    if require_pinned and computed != EXPECTED_ARTIFACT_DIGEST:
        raise ManifestValidationError("pinned Jina artifact digest does not match historical catalog")
    return ModelManifest(_freeze_json(copied))


def _artifact_digest_from_document(document: Mapping[str, Any]) -> str:
    """Match Rust ``catalog_package_digest`` (BTreeMap role ordering)."""

    model = document["model"]
    revision = document["source"]["revision"]
    digest = hashlib.sha256()
    digest.update(b"tracedecay.fastembed.catalog-package.v1\0")
    digest.update(model.encode("utf-8"))
    digest.update(b"\0")
    digest.update(revision.encode("utf-8"))
    digest.update(b"\0")
    for role in sorted(document["members"]):
        member = document["members"][role]
        digest.update(role.encode("utf-8"))
        digest.update(b"\0")
        digest.update(member["upstream_path"].encode("utf-8"))
        digest.update(b"\0")
        digest.update(int(member["length"]).to_bytes(8, "little", signed=False))
        digest.update(member["sha256"].encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def artifact_digest(manifest: ModelManifest | Mapping[str, Any]) -> str:
    """Return the content/catalog identity for a validated manifest."""

    document = manifest.document if isinstance(manifest, ModelManifest) else manifest
    return _artifact_digest_from_document(document)


def manifest_digest(manifest: ModelManifest | Mapping[str, Any]) -> str:
    """Hash the canonical manifest bytes used by the acquisition receipt."""

    document = manifest.document if isinstance(manifest, ModelManifest) else manifest
    return _sha256_bytes(canonical_json(document))


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestValidationError(f"duplicate JSON field {key!r}")
        result[key] = value
    return result


def _reparse_pinned_manifest(manifest: ModelManifest) -> ModelManifest:
    """Reparse a manifest object at a trust boundary.

    ``parse_manifest(..., require_pinned=False)`` remains useful for protocol
    unit tests with tiny local fixtures, but an object supplied to a public
    install/recovery/verification operation must prove the checked-in pin on
    every call.  Canonical round-tripping also prevents a caller from handing
    us a mutable or otherwise exotic Mapping implementation.
    """

    if not isinstance(manifest, ModelManifest):
        raise ManifestValidationError("manifest must be a validated ModelManifest")
    try:
        canonical = canonical_json(manifest.document)
        document = json.loads(
            canonical.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys
        )
    except SemanticProvisioningError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise ManifestValidationError(f"cannot revalidate manifest: {error}") from error
    return parse_manifest(document, require_pinned=True)


def _read_json(path: Path, *, error_type: type[Exception]) -> dict[str, Any]:
    safe_path = _absolute_path(path)
    _check_no_symlink_components(safe_path.parent)
    try:
        data = _read_regular_file(safe_path)
        value = json.loads(
            data.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys
        )
    except ManifestValidationError as error:
        # The shared JSON hook reports duplicate keys as a manifest error, but
        # the caller's document type must remain observable to its validator.
        raise error_type(str(error)) from error
    except VerificationError as error:
        raise error_type(str(error)) from error
    except SemanticProvisioningError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise error_type(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise error_type(f"JSON document {path} must be an object")
    return value


def load_manifest(path: os.PathLike[str] | str, *, require_pinned: bool = True) -> ModelManifest:
    """Read and validate a local manifest without any network access."""

    value = _read_json(Path(path), error_type=ManifestValidationError)
    return parse_manifest(value, require_pinned=require_pinned)


def _trusted_os_alias_target(path: Path) -> Path | None:
    """Return the canonical target of one verified OS compatibility alias."""

    expected_target = _TRUSTED_OS_ANCESTOR_ALIASES.get(path)
    if expected_target is None:
        return None
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise UnsafePathError(f"cannot inspect trusted OS alias {path}: {error}") from error
    if not stat.S_ISLNK(metadata.st_mode):
        return None
    # ``realpath`` is used exactly once for the OS-owned component.  It is
    # accepted only when the link still points at the expected system target;
    # a replacement of the alias is treated as an ordinary unsafe symlink.
    try:
        resolved_target = Path(os.path.realpath(path))
    except OSError as error:
        raise UnsafePathError(f"cannot resolve trusted OS alias {path}: {error}") from error
    return expected_target if resolved_target == expected_target else None


def _canonicalize_trusted_os_aliases(absolute: Path) -> Path:
    """Resolve known OS aliases while preserving all other symlink components."""

    components = absolute.parts
    if not components:
        return absolute
    lexical = Path(components[0])
    canonical = Path(components[0])
    for component in components[1:]:
        lexical /= component
        trusted_target = _trusted_os_alias_target(lexical)
        if trusted_target is not None:
            canonical = trusted_target
        else:
            canonical /= component
    return canonical


def _absolute_path(path: os.PathLike[str] | str) -> Path:
    value = os.fspath(path)
    if not isinstance(value, (str, bytes)):
        raise UnsafePathError("path must be text")
    if isinstance(value, bytes):
        value = os.fsdecode(value)
    if "\x00" in value:
        raise UnsafePathError("path must not contain NUL")
    lexical = Path(value)
    if ".." in lexical.parts:
        raise UnsafePathError("path must not contain traversal segments")
    if not lexical.is_absolute():
        lexical = Path.cwd() / lexical
    return _canonicalize_trusted_os_aliases(lexical)


def _check_no_symlink_components(
    path: Path, *, include_leaf: bool = True
) -> tuple[tuple[Path, int, int], ...]:
    """Reject links and return stable identities for existing directories.

    The returned ``(path, device, inode)`` chain is rechecked at operation
    boundaries.  Files are opened through a no-follow directory descriptor
    chain below, so a caller cannot swap an admitted ancestor for a symlink
    between the lexical check and the actual read/write.
    """

    absolute = _absolute_path(path)
    components = absolute.parts
    current = Path(components[0])
    identities: list[tuple[Path, int, int]] = []
    for component in components[1:]:
        current /= component
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as error:
            raise UnsafePathError(f"cannot inspect path component {current}: {error}") from error
        if stat.S_ISLNK(metadata.st_mode):
            if include_leaf or current != absolute:
                raise UnsafePathError(f"symlink path component is not allowed: {current}")
            continue
        if current != absolute and not stat.S_ISDIR(metadata.st_mode):
            raise UnsafePathError(f"non-directory path component is not allowed: {current}")
        if stat.S_ISDIR(metadata.st_mode):
            identities.append((current, metadata.st_dev, metadata.st_ino))
    return tuple(identities)


def _assert_directory_identities(
    identities: Sequence[tuple[Path, int, int]], *, label: str = "path"
) -> None:
    """Fail closed if an admitted ancestor changed identity after checking."""

    for path, expected_device, expected_inode in identities:
        try:
            metadata = os.lstat(path)
        except OSError as error:
            raise UnsafePathError(f"cannot recheck {label} component {path}: {error}") from error
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_dev != expected_device
            or metadata.st_ino != expected_inode
        ):
            raise UnsafePathError(f"{label} component changed identity: {path}")


def _admitted_directory_identity(
    identities: Sequence[tuple[Path, int, int]], path: Path
) -> tuple[int, int] | None:
    """Return the admitted device/inode pair for one directory, if present."""

    for admitted_path, device, inode in identities:
        if admitted_path == path:
            return device, inode
    return None


def _assert_directory_fd_identity(
    fd: int,
    path: Path,
    identities: Sequence[tuple[Path, int, int]],
    *,
    label: str,
) -> None:
    """Prove an opened directory is the directory admitted by the path check."""

    try:
        metadata = os.fstat(fd)
    except OSError as error:
        raise UnsafePathError(f"cannot inspect admitted {label} directory {path}: {error}") from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise UnsafePathError(f"admitted {label} path is not a directory: {path}")
    expected = _admitted_directory_identity(identities, path)
    if expected is not None and (metadata.st_dev, metadata.st_ino) != expected:
        raise UnsafePathError(f"admitted {label} directory changed identity: {path}")


@contextmanager
def _admitted_directory_fd(
    path: Path,
    identities: Sequence[tuple[Path, int, int]],
    *,
    label: str,
) -> Iterator[int]:
    """Keep an admitted directory open across a sensitive cutover."""

    fd = _open_directory_no_follow(path)
    try:
        _assert_directory_identities(identities, label=label)
        _assert_directory_fd_identity(fd, path, identities, label=label)
        yield fd
    finally:
        os.close(fd)


def _open_directory_no_follow(path: Path) -> int:
    """Open every existing component through directory FDs with no-follow."""

    absolute = _absolute_path(path)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    supports_dir_fd = os.open in getattr(os, "supports_dir_fd", set())
    if not supports_dir_fd:
        try:
            return os.open(absolute, flags)
        except OSError as error:
            raise UnsafePathError(f"cannot open directory {absolute} safely: {error}") from error

    current_fd: int | None = None
    try:
        current_fd = os.open(os.sep, flags)
        for component in absolute.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        return current_fd
    except OSError as error:
        if current_fd is not None:
            os.close(current_fd)
        raise UnsafePathError(f"cannot open directory {absolute} without following links: {error}") from error


def _open_leaf_no_follow(path: Path, flags: int, mode: int = 0o600) -> tuple[int, int]:
    """Open a leaf relative to a no-follow descriptor for its parent."""

    parent_fd = _open_directory_no_follow(path.parent)
    supports_dir_fd = os.open in getattr(os, "supports_dir_fd", set())
    try:
        if supports_dir_fd:
            leaf_fd = os.open(path.name, flags, mode, dir_fd=parent_fd)
        else:
            leaf_fd = os.open(path, flags, mode)
    except OSError:
        os.close(parent_fd)
        raise
    return leaf_fd, parent_fd


def _atomic_replace(
    source: Path,
    destination: Path,
    *,
    source_parent_fd: int | None = None,
    destination_parent_fd: int | None = None,
    expected_source_identity: tuple[int, int] | None = None,
    expected_source_directory: bool | None = None,
    expected_source_parent_identities: Sequence[tuple[Path, int, int]] | None = None,
    expected_destination_parent_identities: Sequence[tuple[Path, int, int]] | None = None,
) -> None:
    """Atomically rename through admitted parent descriptors.

    The caller may retain descriptors from the path admission check.  Before
    the rename we recheck both the lexical ancestor identities and the entry
    being moved relative to those descriptors.  This closes the deterministic
    parent replacement and staged symlink swaps that can otherwise occur after
    a path was verified but before ``rename`` was called.
    """

    source = _absolute_path(source)
    destination = _absolute_path(destination)
    owns_source_parent_fd = source_parent_fd is None
    owns_destination_parent_fd = destination_parent_fd is None
    if source_parent_fd is None:
        source_parent_fd = _open_directory_no_follow(source.parent)
    try:
        if expected_source_parent_identities is not None:
            _assert_directory_identities(expected_source_parent_identities, label="source")
        if expected_destination_parent_identities is not None:
            _assert_directory_identities(expected_destination_parent_identities, label="destination")
        if source_parent_fd is not None:
            _assert_directory_fd_identity(
                source_parent_fd,
                source.parent,
                expected_source_parent_identities or (),
                label="source",
            )

        if destination_parent_fd is None:
            if source.parent == destination.parent:
                destination_parent_fd = source_parent_fd
                owns_destination_parent_fd = False
            else:
                destination_parent_fd = _open_directory_no_follow(destination.parent)
        _assert_directory_fd_identity(
            destination_parent_fd,
            destination.parent,
            expected_destination_parent_identities or (),
            label="destination",
        )

        supports_dir_fd = os.rename in getattr(os, "supports_dir_fd", set())
        if supports_dir_fd:
            source_metadata = os.stat(
                source.name, dir_fd=source_parent_fd, follow_symlinks=False
            )
            if stat.S_ISLNK(source_metadata.st_mode):
                raise UnsafePathError(f"cannot atomically move symlink: {source}")
            if expected_source_identity is not None and (
                source_metadata.st_dev,
                source_metadata.st_ino,
            ) != expected_source_identity:
                raise UnsafePathError(f"source changed identity before atomic publish: {source}")
            if expected_source_directory is not None and (
                stat.S_ISDIR(source_metadata.st_mode) != expected_source_directory
            ):
                raise UnsafePathError(f"source type changed before atomic publish: {source}")
            os.rename(
                source.name,
                destination.name,
                src_dir_fd=source_parent_fd,
                dst_dir_fd=destination_parent_fd,
            )
            if expected_source_identity is not None:
                try:
                    destination_metadata = os.stat(
                        destination.name,
                        dir_fd=destination_parent_fd,
                        follow_symlinks=False,
                    )
                except OSError as error:
                    raise UnsafePathError(
                        f"published entry cannot be revalidated: {destination}"
                    ) from error
                if stat.S_ISLNK(destination_metadata.st_mode):
                    try:
                        os.unlink(destination.name, dir_fd=destination_parent_fd)
                    except OSError:
                        pass
                    raise UnsafePathError(
                        f"published staging entry became a symlink: {destination}"
                    )
                if (
                    destination_metadata.st_dev,
                    destination_metadata.st_ino,
                ) != expected_source_identity or (
                    expected_source_directory is not None
                    and stat.S_ISDIR(destination_metadata.st_mode)
                    != expected_source_directory
                ):
                    raise UnsafePathError(
                        f"published entry changed identity: {destination}"
                    )
        else:
            if source.is_symlink():
                raise UnsafePathError(f"cannot atomically move symlink: {source}")
            os.replace(source, destination)
    except (OSError, ValueError) as error:
        if isinstance(error, (UnsafePathError, SemanticProvisioningError)):
            raise
        raise SemanticProvisioningError(
            f"cannot atomically rename {source} to {destination}: {error}"
        ) from error
    finally:
        if owns_source_parent_fd and source_parent_fd is not None:
            os.close(source_parent_fd)
        if (
            owns_destination_parent_fd
            and destination_parent_fd is not None
            and destination_parent_fd != source_parent_fd
        ):
            os.close(destination_parent_fd)


def _ensure_parent_directory(path: Path) -> None:
    parent = _absolute_path(path)
    missing: list[Path] = []
    current = parent
    while True:
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            missing.append(current)
            next_current = current.parent
            if next_current == current:
                raise UnsafePathError(f"cannot find parent of {current}")
            current = next_current
            continue
        except OSError as error:
            raise UnsafePathError(f"cannot inspect parent {current}: {error}") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise UnsafePathError(f"parent component is not a real directory: {current}")
        break
    for directory in reversed(missing):
        try:
            if os.mkdir in getattr(os, "supports_dir_fd", set()):
                parent_fd = _open_directory_no_follow(directory.parent)
                try:
                    os.mkdir(directory.name, 0o755, dir_fd=parent_fd)
                finally:
                    os.close(parent_fd)
            else:
                os.mkdir(directory, 0o755)
        except FileExistsError:
            pass
        try:
            metadata = os.lstat(directory)
        except OSError as error:
            raise UnsafePathError(f"cannot inspect created directory {directory}: {error}") from error
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise UnsafePathError(f"created parent is not a real directory: {directory}")


def _regular_metadata(path: Path, *, label: str) -> os.stat_result:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError as error:
        raise VerificationError(f"missing {label}: {path}") from error
    except OSError as error:
        raise VerificationError(f"cannot inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode):
        raise UnsafePathError(f"symlink {label} is not allowed: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise VerificationError(f"{label} is not a regular file: {path}")
    if metadata.st_nlink != 1:
        raise UnsafePathError(f"hard-linked {label} is not allowed: {path}")
    return metadata


def _read_regular_file(path: Path) -> bytes:
    metadata = _regular_metadata(path, label="file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd, parent_fd = _open_leaf_no_follow(path, flags)
    except OSError as error:
        raise UnsafePathError(f"cannot open regular file without following links {path}: {error}") from error
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_dev != metadata.st_dev
            or opened.st_ino != metadata.st_ino
        ):
            raise UnsafePathError(f"file changed identity while opening: {path}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, _CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)
        os.close(parent_fd)


def _hash_regular_file(path: Path, *, expected_length: int, expected_digest: str, label: str) -> None:
    metadata = _regular_metadata(path, label=label)
    if metadata.st_size != expected_length:
        raise VerificationError(
            f"{label} length mismatch for {path}: expected {expected_length}, got {metadata.st_size}"
        )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd, parent_fd = _open_leaf_no_follow(path, flags)
    except OSError as error:
        raise UnsafePathError(f"cannot open {label} without following links {path}: {error}") from error
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_dev != metadata.st_dev
            or opened.st_ino != metadata.st_ino
        ):
            raise UnsafePathError(f"{label} changed identity while hashing: {path}")
        digest = hashlib.sha256()
        length = 0
        while True:
            chunk = os.read(fd, _CHUNK_SIZE)
            if not chunk:
                break
            length += len(chunk)
            digest.update(chunk)
        if length != expected_length or digest.hexdigest() != expected_digest:
            raise VerificationError(
                f"{label} digest mismatch for {path}: expected {expected_digest}, got {digest.hexdigest()}"
            )
        after = os.fstat(fd)
        if after.st_size != expected_length or after.st_nlink != 1:
            raise UnsafePathError(f"{label} changed while hashing: {path}")
    finally:
        os.close(fd)
        os.close(parent_fd)


def _scan_tree(root: Path) -> tuple[set[str], set[str]]:
    """Return regular-file and directory inventories, rejecting links."""

    root = _absolute_path(root)
    _check_no_symlink_components(root)
    try:
        root_metadata = os.lstat(root)
    except OSError as error:
        raise VerificationError(f"cannot inspect package root {root}: {error}") from error
    if stat.S_ISLNK(root_metadata.st_mode):
        raise UnsafePathError(f"package root is a symlink: {root}")
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise VerificationError(f"package root is not a directory: {root}")

    files: set[str] = set()
    directories: set[str] = set()
    supports_dir_fd = os.open in getattr(os, "supports_dir_fd", set())
    if not supports_dir_fd:
        stack: list[tuple[Path, str]] = [(root, "")]
        while stack:
            directory, prefix = stack.pop()
            try:
                entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
            except OSError as error:
                raise VerificationError(f"cannot scan package directory {directory}: {error}") from error
            for entry in entries:
                relative = f"{prefix}/{entry.name}" if prefix else entry.name
                try:
                    _safe_relative_path(relative, "package entry")
                except ManifestValidationError as error:
                    raise UnsafePathError(str(error)) from error
                path = Path(entry.path)
                try:
                    metadata = os.lstat(path)
                except OSError as error:
                    raise VerificationError(f"cannot inspect package entry {path}: {error}") from error
                if stat.S_ISLNK(metadata.st_mode):
                    raise UnsafePathError(f"symlink package entry is not allowed: {path}")
                if stat.S_ISDIR(metadata.st_mode):
                    directories.add(relative)
                    stack.append((path, relative))
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise UnsafePathError(f"special package entry is not allowed: {path}")
                if metadata.st_nlink != 1:
                    raise UnsafePathError(f"hard-linked package entry is not allowed: {path}")
                files.add(relative)
        return files, directories

    root_fd = _open_directory_no_follow(root)
    try:
        opened_root = os.fstat(root_fd)
    except OSError as error:
        os.close(root_fd)
        raise UnsafePathError(f"cannot inspect opened package root {root}: {error}") from error
    if (
        opened_root.st_dev != root_metadata.st_dev
        or opened_root.st_ino != root_metadata.st_ino
        or not stat.S_ISDIR(opened_root.st_mode)
    ):
        os.close(root_fd)
        raise UnsafePathError(f"package root changed identity while opening: {root}")
    stack_fd: list[tuple[int, Path, str]] = [(root_fd, root, "")]
    try:
        while stack_fd:
            directory_fd, directory, prefix = stack_fd.pop()
            try:
                with os.scandir(directory_fd) as iterator:
                    entries = sorted(iterator, key=lambda entry: entry.name)
                for entry in entries:
                    relative = f"{prefix}/{entry.name}" if prefix else entry.name
                    try:
                        _safe_relative_path(relative, "package entry")
                    except ManifestValidationError as error:
                        raise UnsafePathError(str(error)) from error
                    path = directory / entry.name
                    try:
                        metadata = entry.stat(follow_symlinks=False)
                    except OSError as error:
                        raise VerificationError(f"cannot inspect package entry {path}: {error}") from error
                    if stat.S_ISLNK(metadata.st_mode):
                        raise UnsafePathError(f"symlink package entry is not allowed: {path}")
                    if stat.S_ISDIR(metadata.st_mode):
                        child_fd: int | None = None
                        try:
                            child_fd = os.open(
                                entry.name,
                                os.O_RDONLY
                                | getattr(os, "O_DIRECTORY", 0)
                                | getattr(os, "O_NOFOLLOW", 0),
                                dir_fd=directory_fd,
                            )
                            child_metadata = os.fstat(child_fd)
                        except OSError as error:
                            if child_fd is not None:
                                os.close(child_fd)
                            raise UnsafePathError(
                                f"cannot open package directory without following links {path}: {error}"
                            ) from error
                        if (
                            not stat.S_ISDIR(child_metadata.st_mode)
                            or child_metadata.st_dev != metadata.st_dev
                            or child_metadata.st_ino != metadata.st_ino
                        ):
                            os.close(child_fd)
                            raise UnsafePathError(f"package directory changed identity: {path}")
                        directories.add(relative)
                        stack_fd.append((child_fd, path, relative))
                        continue
                    if not stat.S_ISREG(metadata.st_mode):
                        raise UnsafePathError(f"special package entry is not allowed: {path}")
                    if metadata.st_nlink != 1:
                        raise UnsafePathError(f"hard-linked package entry is not allowed: {path}")
                    files.add(relative)
            except OSError as error:
                raise VerificationError(f"cannot scan package directory {directory}: {error}") from error
            finally:
                os.close(directory_fd)
    finally:
        # A failure while opening/scanning can leave child descriptors in the
        # stack; close them before propagating the rejection.
        for directory_fd, _, _ in stack_fd:
            try:
                os.close(directory_fd)
            except OSError:
                pass
    return files, directories


def _expected_directories(files: Sequence[str]) -> set[str]:
    directories: set[str] = set()
    for value in files:
        parent = PurePosixPath(value).parent
        while str(parent) != ".":
            directories.add(str(parent))
            parent = parent.parent
    return directories


def _verify_directory_core(
    root: os.PathLike[str] | str, manifest: ModelManifest
) -> DirectoryVerification:
    """Verify exact inventory, link policy, lengths, and SHA-256 values."""

    root_path = _absolute_path(root)
    expected_files = {_MANIFEST_NAME} | {member["path"] for member in manifest.members.values()}
    observed_files, observed_directories = _scan_tree(root_path)
    if observed_files != expected_files:
        missing = sorted(expected_files - observed_files)
        extra = sorted(observed_files - expected_files)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if extra:
            detail.append("undeclared " + ", ".join(extra))
        raise VerificationError("package member inventory mismatch: " + "; ".join(detail))
    expected_dirs = _expected_directories(tuple(expected_files))
    if observed_directories != expected_dirs:
        missing = sorted(expected_dirs - observed_directories)
        extra = sorted(observed_directories - expected_dirs)
        detail = []
        if missing:
            detail.append("missing directories " + ", ".join(missing))
        if extra:
            detail.append("undeclared directories " + ", ".join(extra))
        raise VerificationError("package directory inventory mismatch: " + "; ".join(detail))

    manifest_path = root_path / _MANIFEST_NAME
    actual_manifest = load_manifest(manifest_path, require_pinned=False)
    if actual_manifest.document != manifest.document:
        raise VerificationError("published fixture.json does not match the declared manifest")
    for role, member in manifest.members.items():
        _hash_regular_file(
            root_path / member["path"],
            expected_length=member["length"],
            expected_digest=member["sha256"],
            label=f"members.{role}",
        )
    return DirectoryVerification(
        root=root_path,
        manifest_sha256=manifest_digest(manifest),
        artifact_digest=artifact_digest(manifest),
        member_count=len(manifest.members),
        total_bytes=sum(member["length"] for member in manifest.members.values()),
    )


def verify_directory(
    root: os.PathLike[str] | str, manifest: ModelManifest
) -> DirectoryVerification:
    """Verify a directory after reparsing the exact pinned manifest identity."""

    return _verify_directory_core(root, _reparse_pinned_manifest(manifest))


def _copy_regular_file(source: Path, destination: Path, *, expected_length: int, expected_digest: str, label: str) -> None:
    source_metadata = _regular_metadata(source, label=f"source {label}")
    if source_metadata.st_size != expected_length:
        raise VerificationError(
            f"source {label} length mismatch: expected {expected_length}, got {source_metadata.st_size}"
        )
    destination_parent = destination.parent
    _ensure_parent_directory(destination_parent)
    if destination.exists() or destination.is_symlink():
        _regular_metadata(destination, label=f"staged {label}")
        raise VerificationError(f"refusing to replace existing staged file: {destination}")
    source_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        source_fd, source_parent_fd = _open_leaf_no_follow(source, source_flags)
    except OSError as error:
        raise UnsafePathError(f"cannot open source {source}: {error}") from error
    destination_fd: int | None = None
    destination_parent_fd: int | None = None
    try:
        opened_source = os.fstat(source_fd)
        if (
            not stat.S_ISREG(opened_source.st_mode)
            or opened_source.st_nlink != 1
            or opened_source.st_dev != source_metadata.st_dev
            or opened_source.st_ino != source_metadata.st_ino
        ):
            raise UnsafePathError(f"source {label} changed identity while copying")
        try:
            destination_fd, destination_parent_fd = _open_leaf_no_follow(
                destination, destination_flags, 0o600
            )
        except OSError as error:
            raise UnsafePathError(f"cannot create staged {label}: {error}") from error
        digest = hashlib.sha256()
        copied = 0
        while True:
            chunk = os.read(source_fd, _CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
            copied += len(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(destination_fd, view)
                if written <= 0:
                    raise OSError(errno.EIO, "short write while staging model member")
                view = view[written:]
        if copied != expected_length or digest.hexdigest() != expected_digest:
            raise VerificationError(
                f"source {label} digest mismatch: expected {expected_digest}, got {digest.hexdigest()}"
            )
        os.fsync(destination_fd)
        staged_metadata = os.fstat(destination_fd)
        if staged_metadata.st_nlink != 1 or staged_metadata.st_size != expected_length:
            raise UnsafePathError(f"staged {label} has unsafe metadata")
    except Exception:
        if destination_fd is not None:
            try:
                os.close(destination_fd)
            except OSError:
                pass
        if destination_parent_fd is not None:
            try:
                os.close(destination_parent_fd)
            except OSError:
                pass
        try:
            _remove_regular_file(destination, label=f"partial staged {label}")
        except SemanticProvisioningError:
            pass
        raise
    finally:
        os.close(source_fd)
        os.close(source_parent_fd)
        if destination_fd is not None:
            try:
                os.close(destination_fd)
            except OSError:
                pass
        if destination_parent_fd is not None:
            try:
                os.close(destination_parent_fd)
            except OSError:
                pass


def _remove_regular_file(path: Path, *, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as error:
        raise UnsafePathError(f"cannot inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise UnsafePathError(f"cannot remove non-regular {label}: {path}")
    if metadata.st_nlink != 1:
        raise UnsafePathError(f"cannot remove hard-linked {label}: {path}")
    try:
        parent_fd = _open_directory_no_follow(path.parent)
        try:
            supports_dir_fd = os.stat in getattr(os, "supports_dir_fd", set())
            if supports_dir_fd:
                current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
                if (
                    current.st_dev != metadata.st_dev
                    or current.st_ino != metadata.st_ino
                    or current.st_nlink != 1
                    or not stat.S_ISREG(current.st_mode)
                ):
                    raise UnsafePathError(f"{label} changed identity before removal: {path}")
                os.unlink(path.name, dir_fd=parent_fd)
            else:
                os.unlink(path)
        finally:
            os.close(parent_fd)
    except OSError as error:
        raise SemanticProvisioningError(f"cannot remove {label} {path}: {error}") from error


def _remove_tree(root: Path, *, label: str) -> None:
    files, directories = _scan_tree(root)
    for relative in sorted(files, reverse=True):
        _remove_regular_file(root / relative, label=f"{label} member")
    for relative in sorted(directories, key=lambda value: value.count("/"), reverse=True):
        _remove_directory(root / relative, label=f"{label} directory")
    _remove_directory(root, label=f"{label} root")


def _remove_directory(path: Path, *, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as error:
        raise SemanticProvisioningError(f"cannot inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise UnsafePathError(f"cannot remove non-directory {label}: {path}")
    parent_fd = _open_directory_no_follow(path.parent)
    try:
        if os.stat in getattr(os, "supports_dir_fd", set()):
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if (
                current.st_dev != metadata.st_dev
                or current.st_ino != metadata.st_ino
                or not stat.S_ISDIR(current.st_mode)
            ):
                raise UnsafePathError(f"{label} changed identity before removal: {path}")
            os.rmdir(path.name, dir_fd=parent_fd)
        else:
            os.rmdir(path)
    except OSError as error:
        raise SemanticProvisioningError(f"cannot remove {label} {path}: {error}") from error
    finally:
        os.close(parent_fd)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path, *, required: bool = False) -> None:
    try:
        fd = _open_directory_no_follow(path)
    except (OSError, SemanticProvisioningError) as error:
        if required:
            raise SemanticProvisioningError(f"cannot open directory for durable sync {path}: {error}") from error
        return
    try:
        os.fsync(fd)
    except OSError as error:
        if required:
            raise SemanticProvisioningError(f"cannot durably sync directory {path}: {error}") from error
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, document: Mapping[str, Any]) -> None:
    _ensure_parent_directory(path.parent)
    if path.exists() or path.is_symlink():
        metadata = os.lstat(path)
        if stat.S_ISLNK(metadata.st_mode):
            raise UnsafePathError(f"refusing to replace symlinked JSON path: {path}")
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise UnsafePathError(f"JSON path is not a single regular file: {path}")
    payload = canonical_json(document) + b"\n"
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    temporary = path.with_name(temporary_name)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    parent_fd = _open_directory_no_follow(path.parent)
    supports_dir_fd = os.open in getattr(os, "supports_dir_fd", set())
    try:
        if supports_dir_fd:
            fd = os.open(temporary_name, flags, 0o600, dir_fd=parent_fd)
        else:
            fd = os.open(temporary, flags, 0o600)
        try:
            view = memoryview(payload)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError(errno.EIO, "short JSON write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        if os.rename in getattr(os, "supports_dir_fd", set()):
            os.rename(
                temporary_name,
                path.name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
            )
        else:
            os.replace(temporary, path)
        try:
            os.fsync(parent_fd)
        except OSError as error:
            raise SemanticProvisioningError(
                f"cannot durably sync JSON parent directory {path.parent}: {error}"
            ) from error
    except Exception:
        try:
            if os.unlink in getattr(os, "supports_dir_fd", set()):
                os.unlink(temporary_name, dir_fd=parent_fd)
            else:
                _remove_regular_file(temporary, label="temporary JSON")
        except FileNotFoundError:
            pass
        except SemanticProvisioningError:
            pass
        raise
    finally:
        os.close(parent_fd)


def _safe_unlink(path: Path, *, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as error:
        raise SemanticProvisioningError(f"cannot inspect {label} {path}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise UnsafePathError(f"cannot remove non-regular {label}: {path}")
    if metadata.st_nlink != 1:
        raise UnsafePathError(f"cannot remove hard-linked {label}: {path}")
    parent_fd = _open_directory_no_follow(path.parent)
    try:
        supports_dir_fd = os.stat in getattr(os, "supports_dir_fd", set())
        if supports_dir_fd:
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if (
                current.st_dev != metadata.st_dev
                or current.st_ino != metadata.st_ino
                or current.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
            ):
                raise UnsafePathError(f"{label} changed identity before removal: {path}")
            os.unlink(path.name, dir_fd=parent_fd)
        else:
            os.unlink(path)
    finally:
        os.close(parent_fd)
    _fsync_directory(path.parent)


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """Serialize operations for one target without creating a lock symlink."""

    _ensure_parent_directory(path.parent)
    if path.exists() or path.is_symlink():
        metadata = os.lstat(path)
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise UnsafePathError(f"install lock is not a regular file: {path}")
        if metadata.st_nlink != 1:
            raise UnsafePathError(f"install lock is hard-linked: {path}")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as error:
        raise SemanticProvisioningError(f"cannot open install lock {path}: {error}") from error
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise UnsafePathError(f"install lock changed to an unsafe file: {path}")
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
        except ImportError:
            # Windows has no fcntl; the target contract still refuses links
            # and all operations remain atomic.  The native runtime supplies
            # its own cross-process lifecycle lock on that platform.
            pass
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
        except (ImportError, OSError):
            pass
        os.close(fd)


def staging_path_for(target: os.PathLike[str] | str) -> Path:
    target_path = _absolute_path(target)
    return target_path.with_name(target_path.name + ".staging")


def journal_path_for(target: os.PathLike[str] | str) -> Path:
    target_path = _absolute_path(target)
    return target_path.with_name(target_path.name + ".install-journal.json")


def receipt_path_for(target: os.PathLike[str] | str) -> Path:
    target_path = _absolute_path(target)
    return target_path.with_name(target_path.name + ".acquisition-receipt.json")


def uninstall_journal_path_for(target: os.PathLike[str] | str) -> Path:
    """Return the durable journal that records removal and rollback state."""

    target_path = _absolute_path(target)
    return target_path.with_name(target_path.name + ".uninstall-journal.json")


def removal_journal_path_for(target: os.PathLike[str] | str) -> Path:
    """Compatibility name for callers that call uninstall a removal."""

    return uninstall_journal_path_for(target)


def rollback_path_for(target: os.PathLike[str] | str) -> Path:
    """Return the private target snapshot retained for an explicit rollback."""

    target_path = _absolute_path(target)
    return target_path.with_name(target_path.name + ".rollback")


def rollback_receipt_path_for(
    target: os.PathLike[str] | str,
    receipt_path: os.PathLike[str] | str | None = None,
) -> Path:
    """Return the receipt snapshot paired with ``rollback_path_for``."""

    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target)
    return receipt.with_name(receipt.name + ".rollback")


def _validate_journal(document: Mapping[str, Any], target: Path, receipt: Path) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise JournalValidationError("journal must be an object")
    expected = {
        "schema",
        "operation_id",
        "phase",
        "target",
        "stage",
        "receipt",
        "model",
        "revision",
        "artifact_digest",
        "manifest_sha256",
        "copied_members",
    }
    actual = set(document)
    if actual != expected:
        raise JournalValidationError("journal fields do not match the v1 schema")
    if document["schema"] != JOURNAL_SCHEMA:
        raise JournalValidationError("journal schema is unsupported")
    if not _is_lower_hex(document["operation_id"], length=32):
        raise JournalValidationError("journal operation_id is invalid")
    phase = document["phase"]
    if not isinstance(phase, str) or phase not in {
        "created",
        "staging",
        "staged",
        "publishing",
        "published",
        "receipt_written",
    }:
        raise JournalValidationError("journal phase is invalid")
    if document["target"] != str(target) or document["receipt"] != str(receipt):
        raise JournalValidationError("journal target or receipt does not match this operation")
    stage = document["stage"]
    expected_stage = str(staging_path_for(target))
    if stage != expected_stage:
        raise JournalValidationError("journal stage does not match the target")
    if not isinstance(document["model"], str) or not isinstance(document["revision"], str):
        raise JournalValidationError("journal model identity is invalid")
    if not _is_lower_hex(document["artifact_digest"]) or not _is_lower_hex(document["manifest_sha256"]):
        raise JournalValidationError("journal digests are invalid")
    copied = document["copied_members"]
    if not isinstance(copied, list) or any(
        not isinstance(role, str) or role not in REQUIRED_ROLES for role in copied
    ):
        raise JournalValidationError("journal copied_members is invalid")
    if len(copied) != len(set(copied)):
        raise JournalValidationError("journal copied_members contains duplicates")
    copied_set = set(copied)
    if phase == "created" and copied_set:
        raise JournalValidationError("created journal cannot contain copied members")
    if phase in {"staged", "publishing", "published", "receipt_written"} and copied_set != set(REQUIRED_ROLES):
        raise JournalValidationError("completed journal phase has incomplete copied_members")
    return dict(document)


def _new_journal(target: Path, receipt: Path, manifest: ModelManifest) -> dict[str, Any]:
    return {
        "schema": JOURNAL_SCHEMA,
        "operation_id": uuid.uuid4().hex,
        "phase": "created",
        "target": str(target),
        "stage": str(staging_path_for(target)),
        "receipt": str(receipt),
        "model": manifest.model,
        "revision": manifest.revision,
        "artifact_digest": artifact_digest(manifest),
        "manifest_sha256": manifest_digest(manifest),
        "copied_members": [],
    }


def _write_journal(path: Path, journal: Mapping[str, Any]) -> None:
    _atomic_write_json(path, journal)


def _validate_uninstall_journal(
    document: Mapping[str, Any], target: Path, receipt: Path
) -> dict[str, Any]:
    """Validate the durable remove/rollback state machine before acting on it."""

    if not isinstance(document, Mapping):
        raise JournalValidationError("uninstall journal must be an object")
    expected = {
        "schema",
        "operation_id",
        "phase",
        "target",
        "receipt",
        "rollback_target",
        "rollback_receipt",
        "model",
        "revision",
        "artifact_digest",
        "manifest_sha256",
    }
    if set(document) != expected:
        raise JournalValidationError("uninstall journal fields do not match the v1 schema")
    if document["schema"] != UNINSTALL_JOURNAL_SCHEMA:
        raise JournalValidationError("uninstall journal schema is unsupported")
    if not _is_lower_hex(document["operation_id"], length=32):
        raise JournalValidationError("uninstall journal operation_id is invalid")
    phases = {
        "created",
        "target_moved",
        "receipt_moved",
        "uninstalled",
        "rollback_started",
        "target_restored",
        "receipt_restored",
        "rolled_back",
    }
    phase = document["phase"]
    if not isinstance(phase, str) or phase not in phases:
        raise JournalValidationError("uninstall journal phase is invalid")
    if document["target"] != str(target) or document["receipt"] != str(receipt):
        raise JournalValidationError("uninstall journal target or receipt does not match this operation")
    expected_rollback_target = str(rollback_path_for(target))
    expected_rollback_receipt = str(rollback_receipt_path_for(target, receipt))
    if (
        document["rollback_target"] != expected_rollback_target
        or document["rollback_receipt"] != expected_rollback_receipt
    ):
        raise JournalValidationError("uninstall journal rollback paths do not match the target")
    if not isinstance(document["model"], str) or not isinstance(document["revision"], str):
        raise JournalValidationError("uninstall journal model identity is invalid")
    if not _is_lower_hex(document["artifact_digest"]) or not _is_lower_hex(document["manifest_sha256"]):
        raise JournalValidationError("uninstall journal digests are invalid")
    return dict(document)


def _new_uninstall_journal(target: Path, receipt: Path, manifest: ModelManifest) -> dict[str, Any]:
    return {
        "schema": UNINSTALL_JOURNAL_SCHEMA,
        "operation_id": uuid.uuid4().hex,
        "phase": "created",
        "target": str(target),
        "receipt": str(receipt),
        "rollback_target": str(rollback_path_for(target)),
        "rollback_receipt": str(rollback_receipt_path_for(target, receipt)),
        "model": manifest.model,
        "revision": manifest.revision,
        "artifact_digest": artifact_digest(manifest),
        "manifest_sha256": manifest_digest(manifest),
    }


def load_receipt(path: os.PathLike[str] | str) -> dict[str, Any]:
    return _read_json(Path(path), error_type=ReceiptValidationError)


def _receipt_body(target: Path, manifest: ModelManifest) -> dict[str, Any]:
    inventory = {
        role: {
            "path": manifest.members[role]["path"],
            "upstream_path": manifest.members[role]["upstream_path"],
            "length": manifest.members[role]["length"],
            "sha256": manifest.members[role]["sha256"],
        }
        for role in sorted(manifest.members)
    }
    return {
        "schema": RECEIPT_SCHEMA,
        "operation": "offline_install",
        "status": "installed",
        "target": str(target),
        "model": manifest.model,
        "revision": manifest.revision,
        "artifact_digest": artifact_digest(manifest),
        "manifest_sha256": manifest_digest(manifest),
        "members": inventory,
    }


def _receipt_document(target: Path, manifest: ModelManifest) -> dict[str, Any]:
    body = _receipt_body(target, manifest)
    body["receipt_id"] = _sha256_bytes(canonical_json(body))
    # Keep receipt_id first in human-readable output through canonical sorting;
    # identity is over the complete body without that self-derived field.
    return body


def _validate_receipt_core(
    document: Mapping[str, Any],
    target: os.PathLike[str] | str,
    manifest: ModelManifest,
) -> None:
    """Validate all receipt fields and their target/model/package binding."""

    target_path = _absolute_path(target)
    if not isinstance(document, Mapping):
        raise ReceiptValidationError("receipt must be an object")
    expected_keys = {
        "schema",
        "receipt_id",
        "operation",
        "status",
        "target",
        "model",
        "revision",
        "artifact_digest",
        "manifest_sha256",
        "members",
    }
    if set(document) != expected_keys:
        raise ReceiptValidationError("receipt fields do not match the v1 schema")
    if document["schema"] != RECEIPT_SCHEMA or document["operation"] != "offline_install":
        raise ReceiptValidationError("receipt schema or operation is invalid")
    if document["status"] != "installed":
        raise ReceiptValidationError("receipt status is not installed")
    if document["target"] != str(target_path):
        raise ReceiptValidationError("receipt target does not match the requested target")
    if document["model"] != manifest.model or document["revision"] != manifest.revision:
        raise ReceiptValidationError("receipt model identity does not match the manifest")
    if document["artifact_digest"] != artifact_digest(manifest):
        raise ReceiptValidationError("receipt artifact digest does not match the manifest")
    if document["manifest_sha256"] != manifest_digest(manifest):
        raise ReceiptValidationError("receipt manifest digest does not match the manifest")
    expected_members = _receipt_body(target_path, manifest)["members"]
    if document["members"] != expected_members:
        raise ReceiptValidationError("receipt member inventory does not match the manifest")
    receipt_id = document["receipt_id"]
    if not _is_lower_hex(receipt_id):
        raise ReceiptValidationError("receipt_id must be a lowercase SHA-256 digest")
    body = dict(document)
    del body["receipt_id"]
    if _sha256_bytes(canonical_json(body)) != receipt_id:
        raise ReceiptValidationError("receipt_id does not cover the receipt body")


def validate_receipt(
    document: Mapping[str, Any],
    target: os.PathLike[str] | str,
    manifest: ModelManifest,
) -> None:
    """Validate a receipt after reparsing the exact pinned manifest identity."""

    _validate_receipt_core(document, target, _reparse_pinned_manifest(manifest))


def _validate_source(source: Path, manifest: ModelManifest) -> DirectoryVerification:
    """Verify the explicit source tree before copying a single member."""

    _check_no_symlink_components(source)
    result = _verify_directory_core(source, manifest)
    return result


def _create_directory(path: Path, *, label: str) -> None:
    if path.exists() or path.is_symlink():
        metadata = os.lstat(path)
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise UnsafePathError(f"{label} is not a real directory: {path}")
        return
    _ensure_parent_directory(path.parent)
    try:
        if os.mkdir in getattr(os, "supports_dir_fd", set()):
            parent_fd = _open_directory_no_follow(path.parent)
            try:
                os.mkdir(path.name, 0o700, dir_fd=parent_fd)
            finally:
                os.close(parent_fd)
        else:
            os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise UnsafePathError(f"created {label} is not a real directory: {path}")


def _ensure_stage_shape(stage: Path, manifest: ModelManifest) -> None:
    """Check an existing stage without deleting suspicious entries."""

    if stage.exists() or stage.is_symlink():
        files, directories = _scan_tree(stage)
        expected = {_MANIFEST_NAME} | {member["path"] for member in manifest.members.values()}
        if files - expected:
            raise VerificationError("staging contains undeclared files: " + ", ".join(sorted(files - expected)))
        if directories - _expected_directories(tuple(expected)):
            raise VerificationError("staging contains undeclared directories")
        return
    _create_directory(stage, label="staging directory")


def _copy_member_if_needed(source: Path, stage: Path, member: Mapping[str, Any], role: str) -> bool:
    destination = stage / member["path"]
    if destination.exists() or destination.is_symlink():
        metadata = _regular_metadata(destination, label=f"staged members.{role}")
        if metadata.st_nlink == 1 and metadata.st_size == member["length"]:
            try:
                _hash_regular_file(
                    destination,
                    expected_length=member["length"],
                    expected_digest=member["sha256"],
                    label=f"staged members.{role}",
                )
                return False
            except VerificationError:
                _remove_regular_file(destination, label=f"invalid staged members.{role}")
        else:
            _remove_regular_file(destination, label=f"invalid staged members.{role}")
    _copy_regular_file(
        source / member["path"],
        destination,
        expected_length=member["length"],
        expected_digest=member["sha256"],
        label=f"members.{role}",
    )
    return True


def _populate_stage(
    source: Path,
    stage: Path,
    manifest: ModelManifest,
    journal_path: Path,
    journal: dict[str, Any],
    interrupt_after: str | None,
) -> None:
    _ensure_stage_shape(stage, manifest)
    for index, role in enumerate(REQUIRED_ROLES):
        member = manifest.members[role]
        copied = _copy_member_if_needed(source, stage, member, role)
        if copied and role not in journal["copied_members"]:
            journal["copied_members"].append(role)
        journal["phase"] = "staging"
        _write_journal(journal_path, journal)
        if interrupt_after in {"member", "after-member"} and index == 0:
            raise SimulatedInterruption("interrupted after the first staged member")

    source_manifest_bytes = _read_regular_file(source / _MANIFEST_NAME)
    destination_manifest = stage / _MANIFEST_NAME
    if destination_manifest.exists() or destination_manifest.is_symlink():
        _regular_metadata(destination_manifest, label="staged fixture.json")
        _remove_regular_file(destination_manifest, label="old staged fixture.json")
    _ensure_parent_directory(destination_manifest.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    manifest_parent_fd: int | None = None
    try:
        fd, manifest_parent_fd = _open_leaf_no_follow(destination_manifest, flags, 0o600)
        try:
            view = memoryview(source_manifest_bytes)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise OSError(errno.EIO, "short staged fixture.json write")
                view = view[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception:
        try:
            _remove_regular_file(destination_manifest, label="partial staged fixture.json")
        except SemanticProvisioningError:
            pass
        if manifest_parent_fd is not None:
            try:
                os.close(manifest_parent_fd)
            except OSError:
                pass
        raise
    finally:
        if manifest_parent_fd is not None:
            try:
                os.close(manifest_parent_fd)
            except OSError:
                pass
    # The journal's staged phase is the durable promise that a restart can
    # publish this directory without the source.  Flush the directory entry
    # set before recording that promise.
    _fsync_directory(stage, required=True)
    journal["phase"] = "staged"
    _write_journal(journal_path, journal)
    if interrupt_after in {"staged", "after-staged", "before-publish"}:
        raise SimulatedInterruption("interrupted after complete staging")


def _recover_locked(
    target: Path,
    receipt: Path,
    manifest: ModelManifest,
    *,
    source: Path | None,
    interrupt_after: str | None,
    recovery_mode: bool = False,
    target_parent_fd: int | None = None,
    target_parent_identities: Sequence[tuple[Path, int, int]] | None = None,
) -> InstallResult | None:
    """Recover one journal, returning a completed/idempotent result if any."""

    journal_path = journal_path_for(target)
    stage = staging_path_for(target)
    journal_exists = journal_path.exists() or journal_path.is_symlink()
    target_exists = target.exists() or target.is_symlink()
    stage_exists = stage.exists() or stage.is_symlink()

    if not journal_exists:
        if stage_exists:
            raise JournalValidationError(
                f"orphaned staging directory has no journal; refusing adoption: {stage}"
            )
        if not target_exists:
            return None
        result = _verify_directory_core(target, manifest)
        if not receipt.exists() and not receipt.is_symlink():
            raise ReceiptValidationError(f"published target has no acquisition receipt: {receipt}")
        document = load_receipt(receipt)
        _validate_receipt_core(document, target, manifest)
        return InstallResult(target, receipt, result.artifact_digest, "already_installed")

    journal_document = _read_json(journal_path, error_type=JournalValidationError)
    journal = _validate_journal(journal_document, target, receipt)
    if journal["model"] != manifest.model or journal["revision"] != manifest.revision:
        raise JournalValidationError("journal model identity does not match the requested manifest")
    if journal["artifact_digest"] != artifact_digest(manifest) or journal["manifest_sha256"] != manifest_digest(manifest):
        raise JournalValidationError("journal digest does not match the requested manifest")
    if journal["stage"] != str(stage):
        raise JournalValidationError("journal stage does not match the target")

    if target_exists:
        result = _verify_directory_core(target, manifest)
        if receipt.exists() or receipt.is_symlink():
            document = load_receipt(receipt)
            _validate_receipt_core(document, target, manifest)
        else:
            _atomic_write_json(receipt, _receipt_document(target, manifest))
        if stage_exists:
            # A stage left after a successful rename is safe to remove only
            # after its complete identity has been checked.
            _verify_directory_core(stage, manifest)
            _remove_tree(stage, label="recovered staging")
        _safe_unlink(journal_path, label="install journal")
        return InstallResult(target, receipt, result.artifact_digest, "recovered")

    if stage_exists:
        _ensure_stage_shape(stage, manifest)
    else:
        _create_directory(stage, label="staging directory")
    if source is None:
        try:
            staged = _verify_directory_core(stage, manifest)
        except SemanticProvisioningError as error:
            raise VerificationError(
                "interrupted staging is incomplete; provide the explicit local source to resume"
            ) from error
    else:
        _validate_source(source, manifest)
        staged = None
        if journal["phase"] not in {"staged", "publishing"} or not stage_exists:
            _populate_stage(source, stage, manifest, journal_path, journal, interrupt_after)
        staged = _verify_directory_core(stage, manifest)
    if staged is None:
        staged = _verify_directory_core(stage, manifest)

    journal["phase"] = "publishing"
    _write_journal(journal_path, journal)
    if interrupt_after in {"publishing", "before-rename"}:
        raise SimulatedInterruption("interrupted immediately before atomic publish")
    publication_parent_fd = target_parent_fd
    owns_publication_parent_fd = publication_parent_fd is None
    if publication_parent_fd is None:
        publication_parent_fd = _open_directory_no_follow(target.parent)
    try:
        if target_parent_identities is not None:
            _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_fd_identity(
            publication_parent_fd,
            target.parent,
            target_parent_identities or (),
            label="target",
        )
        supports_dir_fd = os.stat in getattr(os, "supports_dir_fd", set())
        if supports_dir_fd:
            try:
                target_metadata = os.stat(
                    target.name,
                    dir_fd=publication_parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                target_metadata = None
            if target_metadata is not None:
                raise VerificationError(f"target appeared before atomic publish: {target}")
            try:
                stage_metadata = os.stat(
                    stage.name,
                    dir_fd=publication_parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError as error:
                raise UnsafePathError(f"staging directory disappeared before atomic publish: {stage}") from error
        else:
            if target.exists() or target.is_symlink():
                raise VerificationError(f"target appeared before atomic publish: {target}")
            stage_metadata = os.lstat(stage)
        if stat.S_ISLNK(stage_metadata.st_mode) or not stat.S_ISDIR(stage_metadata.st_mode):
            raise UnsafePathError(f"staging directory is not a real directory: {stage}")
        stage_identity = (stage_metadata.st_dev, stage_metadata.st_ino)
        _atomic_replace(
            stage,
            target,
            source_parent_fd=publication_parent_fd,
            destination_parent_fd=publication_parent_fd,
            expected_source_identity=stage_identity,
            expected_source_directory=True,
            expected_source_parent_identities=target_parent_identities,
            expected_destination_parent_identities=target_parent_identities,
        )
    finally:
        if owns_publication_parent_fd:
            os.close(publication_parent_fd)
    if target_parent_identities is not None:
        _assert_directory_identities(target_parent_identities, label="target")
    _fsync_directory(target.parent)
    journal["phase"] = "published"
    _write_journal(journal_path, journal)
    if interrupt_after in {"published", "after-publish"}:
        raise SimulatedInterruption("interrupted after atomic publish")
    _atomic_write_json(receipt, _receipt_document(target, manifest))
    journal["phase"] = "receipt_written"
    _write_journal(journal_path, journal)
    if interrupt_after in {"receipt", "after-receipt"}:
        raise SimulatedInterruption("interrupted after receipt publication")
    _safe_unlink(journal_path, label="install journal")
    status = "recovered" if recovery_mode else "installed"
    return InstallResult(target, receipt, staged.artifact_digest, status)


def _install_offline_core(
    source: os.PathLike[str] | str,
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    """Install an already validated package through a journaled cutover.

    This internal entry point is deliberately separate from ``install_offline``
    so protocol tests can exercise tiny fixtures without weakening the public
    pinned-manifest boundary.
    """
    source_path = _absolute_path(source)
    target_path = _absolute_path(target)
    source_identities = _check_no_symlink_components(source_path)
    _check_no_symlink_components(target_path.parent)
    _ensure_parent_directory(target_path.parent)
    target_parent_identities = _check_no_symlink_components(target_path.parent)
    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target_path)
    _check_no_symlink_components(receipt.parent)
    if receipt.parent != target_path.parent:
        # A receipt elsewhere is still allowed, but it must be an explicit
        # sibling-safe path; binding it to a different tree is easy to audit.
        _ensure_parent_directory(receipt.parent)
    receipt_parent_identities = _check_no_symlink_components(receipt.parent)
    lock = target_path.with_name(target_path.name + ".install.lock")
    with _exclusive_lock(lock):
        _assert_directory_identities(source_identities, label="source")
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        with _admitted_directory_fd(
            target_path.parent, target_parent_identities, label="target"
        ) as target_parent_fd:
            recovered = _recover_locked(
                target_path,
                receipt,
                manifest,
                source=source_path,
                interrupt_after=interrupt_after,
                recovery_mode=False,
                target_parent_fd=target_parent_fd,
                target_parent_identities=target_parent_identities,
            )
        if recovered is not None:
            return recovered
        if interrupt_after in {"journal", "after-journal"}:
            journal = _new_journal(target_path, receipt, manifest)
            _write_journal(journal_path_for(target_path), journal)
            raise SimulatedInterruption("interrupted after journal creation")
        journal_path = journal_path_for(target_path)
        journal = _new_journal(target_path, receipt, manifest)
        _write_journal(journal_path, journal)
        stage = staging_path_for(target_path)
        _create_directory(stage, label="staging directory")
        journal["phase"] = "staging"
        _write_journal(journal_path, journal)
        _validate_source(source_path, manifest)
        _assert_directory_identities(source_identities, label="source")
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        _populate_stage(source_path, stage, manifest, journal_path, journal, interrupt_after)
        # _recover_locked performs publication and all post-publication cleanup;
        # using it here keeps initial and restart paths identical.
        _assert_directory_identities(source_identities, label="source")
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        with _admitted_directory_fd(
            target_path.parent, target_parent_identities, label="target"
        ) as target_parent_fd:
            result = _recover_locked(
                target_path,
                receipt,
                manifest,
                source=None,
                interrupt_after=interrupt_after,
                recovery_mode=False,
                target_parent_fd=target_parent_fd,
                target_parent_identities=target_parent_identities,
            )
        if result is None:
            raise SemanticProvisioningError("installation journal vanished before publication")
        return result


def install_offline(
    source: os.PathLike[str] | str,
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest | None = None,
    manifest_path: os.PathLike[str] | str | None = None,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    """Install explicit local bytes after proving the pinned manifest.

    ``manifest`` is reparsed from canonical bytes at this boundary.  A caller
    cannot install a manifest parsed with ``require_pinned=False`` or a mutable
    hand-constructed ``ModelManifest``.
    """

    if manifest is None:
        if manifest_path is None:
            raise ManifestValidationError("manifest_path is required for command-line installation")
        manifest = load_manifest(manifest_path, require_pinned=True)
    else:
        manifest = _reparse_pinned_manifest(manifest)
    return _install_offline_core(
        source,
        target,
        manifest=manifest,
        receipt_path=receipt_path,
        interrupt_after=interrupt_after,
    )


def _recover_install_core(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest,
    receipt_path: os.PathLike[str] | str | None = None,
) -> InstallResult:
    """Recover a journal using only durable staged bytes; never fetches data."""
    target_path = _absolute_path(target)
    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target_path)
    _check_no_symlink_components(target_path.parent)
    _ensure_parent_directory(target_path.parent)
    target_parent_identities = _check_no_symlink_components(target_path.parent)
    _check_no_symlink_components(receipt.parent)
    _ensure_parent_directory(receipt.parent)
    receipt_parent_identities = _check_no_symlink_components(receipt.parent)
    with _exclusive_lock(target_path.with_name(target_path.name + ".install.lock")):
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        with _admitted_directory_fd(
            target_path.parent, target_parent_identities, label="target"
        ) as target_parent_fd:
            result = _recover_locked(
                target_path,
                receipt,
                manifest,
                source=None,
                interrupt_after=None,
                recovery_mode=True,
                target_parent_fd=target_parent_fd,
                target_parent_identities=target_parent_identities,
            )
        if result is None:
            raise JournalValidationError(f"no recoverable installation journal for {target_path}")
        return result


def recover_install(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest | None = None,
    manifest_path: os.PathLike[str] | str | None = None,
    receipt_path: os.PathLike[str] | str | None = None,
) -> InstallResult:
    """Recover a journal after proving the pinned manifest."""

    if manifest is None:
        if manifest_path is None:
            raise ManifestValidationError("manifest_path is required for recovery")
        manifest = load_manifest(manifest_path, require_pinned=True)
    else:
        manifest = _reparse_pinned_manifest(manifest)
    return _recover_install_core(target, manifest=manifest, receipt_path=receipt_path)


def _verify_installation_core(
    target: os.PathLike[str] | str,
    manifest: ModelManifest,
    *,
    receipt_path: os.PathLike[str] | str | None = None,
) -> DirectoryVerification:
    """Verify the published package and its schema-valid acquisition receipt."""

    target_path = _absolute_path(target)
    target_parent_identities = _check_no_symlink_components(target_path.parent)
    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target_path)
    receipt_parent_identities = _check_no_symlink_components(receipt.parent)
    _assert_directory_identities(target_parent_identities, label="target")
    result = _verify_directory_core(target_path, manifest)
    _assert_directory_identities(target_parent_identities, label="target")
    _assert_directory_identities(receipt_parent_identities, label="receipt")
    document = load_receipt(receipt)
    _validate_receipt_core(document, target_path, manifest)
    return result


def verify_installation(
    target: os.PathLike[str] | str,
    manifest: ModelManifest,
    *,
    receipt_path: os.PathLike[str] | str | None = None,
) -> DirectoryVerification:
    """Verify a publication after proving the pinned manifest."""

    return _verify_installation_core(
        target,
        _reparse_pinned_manifest(manifest),
        receipt_path=receipt_path,
    )


def _assert_uninstall_manifest_identity(
    journal: Mapping[str, Any], manifest: ModelManifest
) -> None:
    if journal["model"] != manifest.model or journal["revision"] != manifest.revision:
        raise JournalValidationError("uninstall journal model identity does not match the requested manifest")
    if (
        journal["artifact_digest"] != artifact_digest(manifest)
        or journal["manifest_sha256"] != manifest_digest(manifest)
    ):
        raise JournalValidationError("uninstall journal digest does not match the requested manifest")


def _uninstall_journal_for(
    target: Path, receipt: Path
) -> tuple[Path, dict[str, Any] | None]:
    path = uninstall_journal_path_for(target)
    if not path.exists() and not path.is_symlink():
        return path, None
    document = _read_json(path, error_type=JournalValidationError)
    return path, _validate_uninstall_journal(document, target, receipt)


def _validate_rollback_snapshot(
    target: Path,
    receipt: Path,
    manifest: ModelManifest,
    journal: Mapping[str, Any],
) -> None:
    rollback_target = Path(journal["rollback_target"])
    rollback_receipt = Path(journal["rollback_receipt"])
    if rollback_target.is_symlink() or rollback_receipt.is_symlink():
        raise UnsafePathError("rollback snapshot contains a symlink")
    if not rollback_target.is_dir():
        raise VerificationError(f"rollback snapshot is missing: {rollback_target}")
    if not rollback_receipt.is_file():
        raise ReceiptValidationError(f"rollback receipt is missing: {rollback_receipt}")
    _verify_directory_core(rollback_target, manifest)
    document = load_receipt(rollback_receipt)
    _validate_receipt_core(document, target, manifest)


def _finish_uninstall_locked(
    target: Path,
    receipt: Path,
    manifest: ModelManifest,
    journal_path: Path,
    journal: dict[str, Any],
    interrupt_after: str | None,
) -> InstallResult:
    """Finish an interrupted uninstall and retain its rollback snapshot."""

    rollback_target = Path(journal["rollback_target"])
    rollback_receipt = Path(journal["rollback_receipt"])
    phase = journal["phase"]
    if phase == "created":
        target_present = target.is_dir() and not target.is_symlink()
        rollback_present = rollback_target.is_dir() and not rollback_target.is_symlink()
        receipt_present = receipt.is_file() and not receipt.is_symlink()
        rollback_receipt_present = rollback_receipt.is_file() and not rollback_receipt.is_symlink()
        if not target_present and rollback_present and receipt_present and not rollback_receipt_present:
            # A target rename may complete before its new phase reaches disk.
            phase = "target_moved"
        else:
            if not target_present or rollback_present or rollback_receipt_present:
                raise JournalValidationError("created uninstall journal has an ambiguous target state")
            if not receipt_present:
                raise ReceiptValidationError(f"published target has no acquisition receipt: {receipt}")
            _verify_installation_core(target, manifest, receipt_path=receipt)
            _atomic_replace(target, rollback_target)
            _fsync_directory(target.parent)
            journal["phase"] = "target_moved"
            _write_journal(journal_path, journal)
            phase = journal["phase"]
            if interrupt_after in {"uninstall-target", "uninstall-after-target", "remove-target"}:
                raise SimulatedInterruption("interrupted after moving the target to its rollback snapshot")

    if phase == "target_moved":
        target_present = target.is_dir() and not target.is_symlink()
        rollback_present = rollback_target.is_dir() and not rollback_target.is_symlink()
        receipt_present = receipt.is_file() and not receipt.is_symlink()
        rollback_receipt_present = rollback_receipt.is_file() and not rollback_receipt.is_symlink()
        if not target_present and rollback_present and not receipt_present and rollback_receipt_present:
            # A receipt rename may complete before its new phase reaches disk.
            phase = "receipt_moved"
        else:
            if target_present or not rollback_present or not receipt_present or rollback_receipt_present:
                raise JournalValidationError("target_moved uninstall journal has an ambiguous target state")
            _verify_directory_core(rollback_target, manifest)
            document = load_receipt(receipt)
            _validate_receipt_core(document, target, manifest)
            _atomic_replace(receipt, rollback_receipt)
            _fsync_directory(receipt.parent)
            journal["phase"] = "receipt_moved"
            _write_journal(journal_path, journal)
            phase = journal["phase"]
            if interrupt_after in {"uninstall-receipt", "uninstall-after-receipt", "remove-receipt"}:
                raise SimulatedInterruption("interrupted after moving the acquisition receipt")

    if phase == "receipt_moved":
        if target.exists() or target.is_symlink() or receipt.exists() or receipt.is_symlink():
            raise JournalValidationError("receipt_moved uninstall journal has an ambiguous publication state")
        _validate_rollback_snapshot(target, receipt, manifest, journal)
        journal["phase"] = "uninstalled"
        _write_journal(journal_path, journal)
        phase = journal["phase"]

    if phase != "uninstalled":
        raise JournalValidationError(f"cannot finish uninstall from phase {phase!r}")
    _validate_rollback_snapshot(target, receipt, manifest, journal)
    return InstallResult(target, receipt, artifact_digest(manifest), "uninstalled")


def _finish_rollback_locked(
    target: Path,
    receipt: Path,
    manifest: ModelManifest,
    journal_path: Path,
    journal: dict[str, Any],
    interrupt_after: str | None,
) -> InstallResult:
    """Restore the retained uninstall snapshot through journaled phases."""

    phase = journal["phase"]
    if phase in {"created", "target_moved", "receipt_moved"}:
        _finish_uninstall_locked(target, receipt, manifest, journal_path, journal, None)
        phase = journal["phase"]
    if phase == "uninstalled":
        _validate_rollback_snapshot(target, receipt, manifest, journal)
        journal["phase"] = "rollback_started"
        _write_journal(journal_path, journal)
        phase = journal["phase"]
        if interrupt_after in {"rollback-journal", "rollback-after-journal"}:
            raise SimulatedInterruption("interrupted after rollback journal creation")

    rollback_target = Path(journal["rollback_target"])
    rollback_receipt = Path(journal["rollback_receipt"])
    if phase == "rollback_started":
        target_present = target.is_dir() and not target.is_symlink()
        rollback_present = rollback_target.is_dir() and not rollback_target.is_symlink()
        receipt_present = receipt.is_file() and not receipt.is_symlink()
        rollback_receipt_present = rollback_receipt.is_file() and not rollback_receipt.is_symlink()
        if target_present and not rollback_present and not receipt_present and rollback_receipt_present:
            # The target rename completed before its target_restored phase was
            # persisted.  Continue from the observed safe state.
            phase = "target_restored"
        elif target_present and not rollback_present and receipt_present and not rollback_receipt_present:
            # Both renames completed before the receipt_restored phase was
            # persisted.
            phase = "receipt_restored"
    if phase == "rollback_started":
        if (
            target.exists()
            or target.is_symlink()
            or rollback_target.is_symlink()
            or not rollback_target.is_dir()
        ):
            raise JournalValidationError("rollback_started journal has an ambiguous target state")
        if (
            receipt.exists()
            or receipt.is_symlink()
            or rollback_receipt.is_symlink()
            or not rollback_receipt.is_file()
        ):
            raise JournalValidationError("rollback_started journal has an ambiguous receipt state")
        _atomic_replace(rollback_target, target)
        _fsync_directory(target.parent)
        journal["phase"] = "target_restored"
        _write_journal(journal_path, journal)
        phase = journal["phase"]
        if interrupt_after in {"rollback-target", "rollback-after-target"}:
            raise SimulatedInterruption("interrupted after restoring the target")

    if phase == "target_restored":
        target_present = target.is_dir() and not target.is_symlink()
        rollback_present = rollback_target.is_dir() and not rollback_target.is_symlink()
        receipt_present = receipt.is_file() and not receipt.is_symlink()
        rollback_receipt_present = rollback_receipt.is_file() and not rollback_receipt.is_symlink()
        if target_present and not rollback_present and receipt_present and not rollback_receipt_present:
            phase = "receipt_restored"
    if phase == "target_restored":
        if not target.is_dir() or target.is_symlink() or rollback_target.exists() or rollback_target.is_symlink():
            raise JournalValidationError("target_restored journal has an ambiguous target state")
        if (
            receipt.exists()
            or receipt.is_symlink()
            or rollback_receipt.is_symlink()
            or not rollback_receipt.is_file()
        ):
            raise JournalValidationError("target_restored journal has an ambiguous receipt state")
        _verify_directory_core(target, manifest)
        _atomic_replace(rollback_receipt, receipt)
        _fsync_directory(receipt.parent)
        journal["phase"] = "receipt_restored"
        _write_journal(journal_path, journal)
        phase = journal["phase"]
        if interrupt_after in {"rollback-receipt", "rollback-after-receipt"}:
            raise SimulatedInterruption("interrupted after restoring the acquisition receipt")

    if phase == "receipt_restored":
        _verify_installation_core(target, manifest, receipt_path=receipt)
        journal["phase"] = "rolled_back"
        _write_journal(journal_path, journal)
        phase = journal["phase"]

    if phase != "rolled_back":
        raise JournalValidationError(f"cannot finish rollback from phase {phase!r}")
    if target.exists() and receipt.exists() and not rollback_target.exists() and not rollback_receipt.exists():
        _safe_unlink(journal_path, label="uninstall journal")
        return InstallResult(target, receipt, artifact_digest(manifest), "rolled_back")
    raise JournalValidationError("rolled-back journal has an unexpected snapshot state")


def _uninstall_offline_core(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    target_path = _absolute_path(target)
    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target_path)
    _check_no_symlink_components(target_path.parent)
    _ensure_parent_directory(target_path.parent)
    target_parent_identities = _check_no_symlink_components(target_path.parent)
    _check_no_symlink_components(receipt.parent)
    _ensure_parent_directory(receipt.parent)
    receipt_parent_identities = _check_no_symlink_components(receipt.parent)
    rollback_target = rollback_path_for(target_path)
    rollback_receipt = rollback_receipt_path_for(target_path, receipt)
    _check_no_symlink_components(rollback_target)
    _check_no_symlink_components(rollback_receipt)
    with _exclusive_lock(target_path.with_name(target_path.name + ".install.lock")):
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        journal_path, journal = _uninstall_journal_for(target_path, receipt)
        if journal is not None:
            _assert_uninstall_manifest_identity(journal, manifest)
            if journal["phase"] in {
                "rollback_started",
                "target_restored",
                "receipt_restored",
                "rolled_back",
            }:
                _finish_rollback_locked(target_path, receipt, manifest, journal_path, journal, None)
                journal = None
            else:
                result = _finish_uninstall_locked(
                    target_path, receipt, manifest, journal_path, journal, interrupt_after
                )
                return InstallResult(
                    result.target, result.receipt, result.artifact_digest, "already_uninstalled"
                )
        if journal is None:
            if rollback_target.exists() or rollback_receipt.exists():
                raise JournalValidationError("rollback snapshot exists without an uninstall journal")
            if not target_path.is_dir() or target_path.is_symlink():
                raise VerificationError(f"cannot uninstall missing target: {target_path}")
            _verify_installation_core(target_path, manifest, receipt_path=receipt)
            journal = _new_uninstall_journal(target_path, receipt, manifest)
            _write_journal(journal_path, journal)
            if interrupt_after in {"uninstall-journal", "uninstall-after-journal", "remove-journal"}:
                raise SimulatedInterruption("interrupted after uninstall journal creation")
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        return _finish_uninstall_locked(
            target_path, receipt, manifest, journal_path, journal, interrupt_after
        )


def uninstall_offline(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest | None = None,
    manifest_path: os.PathLike[str] | str | None = None,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    """Durably remove a publication while retaining an explicit rollback snapshot."""

    if manifest is None:
        if manifest_path is None:
            raise ManifestValidationError("manifest_path is required for uninstall")
        manifest = load_manifest(manifest_path, require_pinned=True)
    else:
        manifest = _reparse_pinned_manifest(manifest)
    return _uninstall_offline_core(
        target,
        manifest=manifest,
        receipt_path=receipt_path,
        interrupt_after=interrupt_after,
    )


def recover_uninstall(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest | None = None,
    manifest_path: os.PathLike[str] | str | None = None,
    receipt_path: os.PathLike[str] | str | None = None,
) -> InstallResult:
    """Finish an interrupted uninstall without reading source bytes."""

    if manifest is None:
        if manifest_path is None:
            raise ManifestValidationError("manifest_path is required for uninstall recovery")
        manifest = load_manifest(manifest_path, require_pinned=True)
    else:
        manifest = _reparse_pinned_manifest(manifest)
    result = _uninstall_offline_core(
        target,
        manifest=manifest,
        receipt_path=receipt_path,
        interrupt_after=None,
    )
    if result.status == "already_uninstalled":
        return InstallResult(result.target, result.receipt, result.artifact_digest, "recovered")
    return result


def _rollback_install_core(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    target_path = _absolute_path(target)
    receipt = _absolute_path(receipt_path) if receipt_path is not None else receipt_path_for(target_path)
    _check_no_symlink_components(target_path.parent)
    _ensure_parent_directory(target_path.parent)
    target_parent_identities = _check_no_symlink_components(target_path.parent)
    _check_no_symlink_components(receipt.parent)
    _ensure_parent_directory(receipt.parent)
    receipt_parent_identities = _check_no_symlink_components(receipt.parent)
    with _exclusive_lock(target_path.with_name(target_path.name + ".install.lock")):
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        journal_path, journal = _uninstall_journal_for(target_path, receipt)
        if journal is None:
            raise JournalValidationError(f"no rollback snapshot for {target_path}")
        _assert_uninstall_manifest_identity(journal, manifest)
        result = _finish_rollback_locked(
            target_path, receipt, manifest, journal_path, journal, interrupt_after
        )
        _assert_directory_identities(target_parent_identities, label="target")
        _assert_directory_identities(receipt_parent_identities, label="receipt")
        return result


def rollback_install(
    target: os.PathLike[str] | str,
    *,
    manifest: ModelManifest | None = None,
    manifest_path: os.PathLike[str] | str | None = None,
    receipt_path: os.PathLike[str] | str | None = None,
    interrupt_after: str | None = None,
) -> InstallResult:
    """Restore the latest durable uninstall snapshot through journaled phases."""

    if manifest is None:
        if manifest_path is None:
            raise ManifestValidationError("manifest_path is required for rollback")
        manifest = load_manifest(manifest_path, require_pinned=True)
    else:
        manifest = _reparse_pinned_manifest(manifest)
    return _rollback_install_core(
        target,
        manifest=manifest,
        receipt_path=receipt_path,
        interrupt_after=interrupt_after,
    )


def uninstall(*args: Any, **kwargs: Any) -> InstallResult:
    """Alias for ``uninstall_offline`` used by command integrations."""

    return uninstall_offline(*args, **kwargs)


def rollback(*args: Any, **kwargs: Any) -> InstallResult:
    """Alias for ``rollback_install`` used by command integrations."""

    return rollback_install(*args, **kwargs)
