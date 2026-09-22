#!/usr/bin/env python3
"""Plan release work without rebuilding already-published immutable artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import stat
import tarfile
from typing import Any
import zipfile


WORKER_POLICY_NAME = "tracedecay-ncm-worker"
WORKER_MANIFEST_NAME = "worker-manifest.json"
MODEL_ACQUISITION_MANIFEST_NAME = "model-acquisition-manifest.json"
MAX_EXECUTABLE_PREFIX = 4096


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
            policy_model_manifest = policy.get("model_acquisition_manifest")
            if (
                isinstance(policy_model_manifest, str)
                and Path(policy_model_manifest).name != sidecar.get("model_manifest")
            ):
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
    if sidecar["archive"] != "tar.gz" or sidecar["checksum"] != "sha256":
        raise SystemExit(f"release target {target['name']} has invalid NCM sidecar format")
    if sidecar["worker"] != WORKER_POLICY_NAME:
        raise SystemExit(f"release target {target['name']} has an unsupported NCM worker")
    if sidecar["manifest"] != WORKER_MANIFEST_NAME:
        raise SystemExit(f"release target {target['name']} has an invalid NCM worker manifest")
    if sidecar["model_manifest"] != MODEL_ACQUISITION_MANIFEST_NAME:
        raise SystemExit(
            f"release target {target['name']} has an invalid NCM model acquisition manifest"
        )
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


def _verify_executable_prefix(prefix: bytes, *, target: str, label: str) -> None:
    """Require an executable payload to use the release target's format/arch."""
    architecture = target.split("-", 1)[0]
    if target.endswith("-apple-darwin"):
        if len(prefix) < 8 or prefix[:4] not in {
            b"\xcf\xfa\xed\xfe",
            b"\xfe\xed\xfa\xcf",
        }:
            raise SystemExit(f"{label} is not a 64-bit Mach-O binary")
        expected_cpu = {
            "aarch64": 0x0100000C,
            "x86_64": 0x01000007,
        }.get(architecture)
        if expected_cpu is None:
            raise SystemExit(f"{label} target architecture is unsupported: {target}")
        byteorder = "little" if prefix[:4] == b"\xcf\xfa\xed\xfe" else "big"
        cpu_type = int.from_bytes(prefix[4:8], byteorder, signed=False)
        if cpu_type != expected_cpu:
            raise SystemExit(
                f"{label} Mach-O CPU type {cpu_type:#x} is not pinned for {target}"
            )
        return

    if target.endswith("-windows-msvc"):
        if len(prefix) < 0x40 or prefix[:2] != b"MZ":
            raise SystemExit(f"{label} is not a PE binary")
        pe_offset = int.from_bytes(prefix[0x3C:0x40], "little", signed=False)
        if pe_offset + 6 > len(prefix) or prefix[pe_offset : pe_offset + 4] != b"PE\0\0":
            raise SystemExit(f"{label} has no valid PE header")
        expected_machine = {
            "x86_64": 0x8664,
            "aarch64": 0xAA64,
            "i686": 0x014C,
        }.get(architecture)
        if expected_machine is None:
            raise SystemExit(f"{label} target architecture is unsupported: {target}")
        machine = int.from_bytes(prefix[pe_offset + 4 : pe_offset + 6], "little", signed=False)
        if machine != expected_machine:
            raise SystemExit(
                f"{label} PE COFF machine {machine:#x} is not pinned for {target}"
            )
        return

    if len(prefix) < 20 or prefix[:4] != b"\x7fELF":
        raise SystemExit(f"{label} is not an ELF binary")
    expected = {
        "x86_64": (2, 0x003E),
        "aarch64": (2, 0x00B7),
        "i686": (1, 0x0003),
        "armv7": (1, 0x0028),
    }.get(architecture)
    if expected is None:
        raise SystemExit(f"{label} target architecture is unsupported: {target}")
    elf_class, expected_machine = expected
    if prefix[4] != elf_class:
        raise SystemExit(
            f"{label} ELF class {prefix[4]} is not pinned for {target}"
        )
    if prefix[5] != 1:
        raise SystemExit(f"{label} ELF header has unsupported byte order")
    machine = int.from_bytes(prefix[18:20], "little", signed=False)
    if machine != expected_machine:
        raise SystemExit(
            f"{label} ELF machine {machine:#x} is not pinned for {target}"
        )


def _verify_member_prefix(member_file: Any, *, target: str, label: str) -> None:
    prefix = member_file.read(MAX_EXECUTABLE_PREFIX)
    _verify_executable_prefix(prefix, target=target, label=label)


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


def _regular_retained_file(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SystemExit(f"retained release asset is missing: {path.name}") from error
    except OSError as error:
        raise SystemExit(f"cannot inspect retained release asset {path.name}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"retained {label} must be a regular file: {path.name}")


def _validate_retained_binary_archive(
    path: Path, *, target: dict[str, Any], archive_name: str
) -> None:
    """Reject a retained CLI archive whose executable targets another platform."""
    _regular_retained_file(path, "CLI archive")
    target_triple = target["target"]
    expected_name = (
        "tracedecay.exe" if target_triple.endswith("-windows-msvc") else "tracedecay"
    )
    try:
        if target["archive"] == "tar.gz":
            with tarfile.open(path, mode="r:gz") as archive:
                members = archive.getmembers()
                names = [member.name for member in members]
                if names != [expected_name]:
                    raise SystemExit(
                        f"retained CLI archive {archive_name} must contain exactly "
                        f"{expected_name}; got {', '.join(names)}"
                    )
                member = members[0]
                if (
                    not member.isreg()
                    or member.linkname
                    or stat.S_IMODE(member.mode) != 0o755
                ):
                    raise SystemExit(
                        f"retained CLI archive {archive_name} contains an unsafe executable entry"
                    )
                executable = archive.extractfile(member)
                if executable is None:
                    raise SystemExit(
                        f"retained CLI archive {archive_name} has an unreadable executable"
                    )
                _verify_member_prefix(
                    executable,
                    target=target_triple,
                    label=f"retained CLI archive {archive_name} executable",
                )
        elif target["archive"] == "zip":
            with zipfile.ZipFile(path) as archive:
                entries = archive.infolist()
                names = [entry.filename for entry in entries]
                if names != [expected_name]:
                    raise SystemExit(
                        f"retained CLI archive {archive_name} must contain exactly "
                        f"{expected_name}; got {', '.join(names)}"
                    )
                entry = entries[0]
                mode = (entry.external_attr >> 16) & 0o170000
                if mode in {stat.S_IFLNK, stat.S_IFDIR} or entry.filename.endswith("/"):
                    raise SystemExit(
                        f"retained CLI archive {archive_name} contains an unsafe executable entry"
                    )
                with archive.open(entry) as executable:
                    _verify_member_prefix(
                        executable,
                        target=target_triple,
                        label=f"retained CLI archive {archive_name} executable",
                    )
        else:
            raise SystemExit(f"retained CLI archive format is unsupported: {archive_name}")
    except (OSError, RuntimeError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise SystemExit(f"invalid retained CLI archive {archive_name}: {error}") from error


def _validate_retained_mcpb(
    path: Path, *, target: dict[str, Any], bundle_name: str
) -> None:
    """Reject a retained MCPB whose embedded server targets another platform."""
    _regular_retained_file(path, "MCPB")
    binary_name = (
        "tracedecay.exe" if target["target"].endswith("-windows-msvc") else "tracedecay"
    )
    expected_names = ["manifest.json", f"server/{binary_name}"]
    try:
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            if len(names) != len(expected_names) or set(names) != set(expected_names):
                raise SystemExit(
                    f"retained MCPB {bundle_name} must contain exactly "
                    f"{', '.join(expected_names)}; got {', '.join(names)}"
                )
            executable_entry = next(
                entry for entry in entries if entry.filename == expected_names[1]
            )
            mode = (executable_entry.external_attr >> 16) & 0o170000
            if mode in {stat.S_IFLNK, stat.S_IFDIR} or executable_entry.filename.endswith("/"):
                raise SystemExit(
                    f"retained MCPB {bundle_name} contains an unsafe executable entry"
                )
            with archive.open(executable_entry) as executable:
                _verify_member_prefix(
                    executable,
                    target=target["target"],
                    label=f"retained MCPB {bundle_name} executable",
                )
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        raise SystemExit(f"invalid retained MCPB {bundle_name}: {error}") from error


def _validate_retained_sidecar_archive(
    path: Path, *, target: dict[str, Any], archive_name: str
) -> None:
    """Reject legacy sidecars before their bytes enter a recovery build."""
    expected_names = [
        WORKER_POLICY_NAME,
        WORKER_MANIFEST_NAME,
        MODEL_ACQUISITION_MANIFEST_NAME,
    ]
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise SystemExit(f"retained NCM sidecar is missing: {archive_name}") from error
    except OSError as error:
        raise SystemExit(f"cannot inspect retained NCM sidecar {archive_name}: {error}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"retained NCM sidecar must be a regular file: {archive_name}")
    try:
        with tarfile.open(path, mode="r:gz") as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            if names != expected_names:
                raise SystemExit(
                    f"retained NCM sidecar {archive_name} must contain exactly "
                    f"{', '.join(expected_names)}; got {', '.join(names)}"
                )
            by_name = {member.name: member for member in members}
            for name in expected_names:
                member = by_name[name]
                if not member.isreg() or member.linkname:
                    raise SystemExit(
                        f"retained NCM sidecar {archive_name} contains an unsafe entry: {name}"
                    )
            worker_member = by_name[WORKER_POLICY_NAME]
            if stat.S_IMODE(worker_member.mode) != 0o755:
                raise SystemExit(
                    f"retained NCM sidecar {archive_name} worker is not executable"
                )
            worker_file = archive.extractfile(worker_member)
            if worker_file is None:
                raise SystemExit(
                    f"retained NCM sidecar {archive_name} has an unreadable worker"
                )
            _verify_member_prefix(
                worker_file,
                target=target["target"],
                label=f"retained NCM sidecar {archive_name} worker",
            )
            model_file = archive.extractfile(by_name[MODEL_ACQUISITION_MANIFEST_NAME])
            if model_file is None:
                raise SystemExit(
                    f"retained NCM sidecar {archive_name} has no readable model acquisition manifest"
                )
            try:
                model_manifest = json.loads(model_file.read().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise SystemExit(
                    f"retained NCM sidecar {archive_name} model acquisition manifest is invalid: {error}"
                ) from error
    except (OSError, tarfile.TarError) as error:
        raise SystemExit(f"invalid retained NCM sidecar {archive_name}: {error}") from error
    if not isinstance(model_manifest, dict):
        raise SystemExit(
            f"retained NCM sidecar {archive_name} model acquisition manifest must be an object"
        )
    expected = {
        "schema_version": 1,
        "manifest_type": "ncm-model-acquisition",
        "provider_id": "ncm",
        "worker": WORKER_POLICY_NAME,
        "target": target["target"],
        "release_name": target["name"],
        "transaction_journal": "ncm-model-lifecycle-v1.json",
    }
    for key, value in expected.items():
        observed = (
            model_manifest.get("transaction", {}).get("journal")
            if key == "transaction_journal"
            else model_manifest.get(key)
        )
        if observed != value:
            raise SystemExit(
                f"retained NCM sidecar {archive_name} model acquisition manifest "
                f"{key} differs from the release pin"
            )
    revision_digest = model_manifest.get("revision_provenance_sha256")
    if not isinstance(revision_digest, str) or re.fullmatch(r"[0-9a-f]{64}", revision_digest) is None:
        raise SystemExit(
            f"retained NCM sidecar {archive_name} model acquisition manifest lacks "
            "the canonical revision receipt digest"
        )
    receipt = model_manifest.get("receipt")
    required_fields = receipt.get("required_fields") if isinstance(receipt, dict) else None
    if (
        not isinstance(receipt, dict)
        or receipt.get("relative_path") != "receipts/ncm-model-acquisition-v1.json"
        or not isinstance(required_fields, list)
        or "revision_provenance_sha256" not in required_fields
    ):
        raise SystemExit(
            f"retained NCM sidecar {archive_name} model acquisition manifest "
            "does not require the canonical acquisition receipt"
        )


def validate_asset_directory(
    targets: list[dict[str, Any]], tag: str, profile: str, directory: Path
) -> None:
    """Validate downloaded retained assets before recovery copies them."""
    try:
        metadata = directory.lstat()
    except FileNotFoundError as error:
        raise SystemExit(f"retained release asset directory is missing: {directory}") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise SystemExit(f"retained release asset directory is invalid: {directory}")
    actual: set[str] = set()
    for item in directory.iterdir():
        item_metadata = item.lstat()
        if stat.S_ISLNK(item_metadata.st_mode) or not stat.S_ISREG(item_metadata.st_mode):
            raise SystemExit(f"retained release asset must be a regular file: {item.name}")
        actual.add(item.name)
    expected = {
        asset
        for target in targets
        for asset in target_assets(target, tag, profile)
    }
    unexpected = sorted(actual - expected)
    if unexpected:
        raise SystemExit(
            "unexpected retained release assets: " + ", ".join(unexpected)
        )
    by_archive = {
        sidecar_assets(target, tag, profile)[0]: target
        for target in targets
        if target.get("sidecar") is not None
    }
    for archive_name, target in by_archive.items():
        if archive_name in actual:
            _validate_retained_sidecar_archive(
                directory / archive_name,
                target=target,
                archive_name=archive_name,
            )
    by_binary = {
        (
            f"tracedecay-beta-{tag}-{target['name']}.{target['archive']}"
            if profile == "beta"
            else f"tracedecay-{tag}-{target['name']}.{target['archive']}"
        ): target
        for target in targets
    }
    for archive_name, target in by_binary.items():
        if archive_name in actual:
            _validate_retained_binary_archive(
                directory / archive_name,
                target=target,
                archive_name=archive_name,
            )
    by_mcpb = {
        (
            f"tracedecay-beta-{tag}-{target['name']}.mcpb"
            if profile == "beta"
            else f"tracedecay-{tag}-{target['name']}.mcpb"
        ): target
        for target in targets
    }
    for bundle_name, target in by_mcpb.items():
        if bundle_name in actual:
            _validate_retained_mcpb(
                directory / bundle_name,
                target=target,
                bundle_name=bundle_name,
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--worker-platforms", type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--profile", choices=("stable", "beta"), default="stable")
    parser.add_argument("--asset-names", type=Path)
    parser.add_argument("--retained-output", type=Path)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument(
        "--asset-directory",
        type=Path,
        help="validate downloaded retained archive contents before recovery",
    )
    arguments = parser.parse_args()

    targets = load_targets(arguments.manifest, arguments.worker_platforms)
    if arguments.asset_directory is not None:
        validate_asset_directory(
            targets, arguments.tag, arguments.profile, arguments.asset_directory
        )
        if arguments.asset_names is None:
            return
    if (
        arguments.asset_names is None
        or arguments.retained_output is None
        or arguments.github_output is None
    ):
        raise SystemExit(
            "--asset-names, --retained-output, and --github-output are required "
            "when planning release recovery"
        )
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
