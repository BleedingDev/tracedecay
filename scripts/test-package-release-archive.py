#!/usr/bin/env python3
"""Behavioral tests for deterministic release archives."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PACKAGER = ROOT / "scripts" / "package-release-archive.py"
EPOCH = 1_700_000_001
PAYLOAD = b"tracedecay release binary\n"
WORKER_PAYLOAD = b"tracedecay ncm worker\n"
WORKER_MANIFEST = b'''{
  "schema_version": 1,
  "worker": "tracedecay-ncm-worker",
  "protocol_version": 1,
  "protocol_identity": "tracedecay.ncm.worker.v1",
  "targets": [
    {
      "triple": "aarch64-apple-darwin",
      "os": "macos",
      "arch": "aarch64",
      "family": "unix",
      "bytes": 22,
      "sha256": "d87c191e0c21953fd0c2e6f568942eb6b69c2166ec844dabce22a15976087753"
    }
  ]
}
'''


def package(
    binary: Path,
    output: Path,
    archive_format: str,
    entry_name: str,
    companion: tuple[Path, str] | None = None,
) -> None:
    command = [
        "python3",
        str(PACKAGER),
        "--binary",
        str(binary),
        "--output",
        str(output),
        "--format",
        archive_format,
        "--entry-name",
        entry_name,
        "--epoch",
        str(EPOCH),
    ]
    if companion is not None:
        command.extend(["--companion", f"{companion[0]}={companion[1]}"])
    subprocess.run(command, check=True)


def test_tar_gz(temp: Path, binary: Path) -> None:
    first = temp / "first.tar.gz"
    second = temp / "second.tar.gz"
    package(binary, first, "tar.gz", "tracedecay")
    os.utime(binary, (EPOCH + 100, EPOCH + 100))
    package(binary, second, "tar.gz", "tracedecay")
    assert first.read_bytes() == second.read_bytes()

    with tarfile.open(first, "r:gz") as archive:
        entries = archive.getmembers()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.name == "tracedecay"
        assert entry.mode == 0o755
        assert entry.uid == 0 and entry.gid == 0
        assert entry.uname == "" and entry.gname == ""
        assert entry.mtime == EPOCH
        extracted = archive.extractfile(entry)
        assert extracted is not None and extracted.read() == PAYLOAD


def test_zip(temp: Path, binary: Path) -> None:
    first = temp / "first.zip"
    second = temp / "second.zip"
    package(binary, first, "zip", "tracedecay.exe")
    os.utime(binary, (EPOCH + 200, EPOCH + 200))
    package(binary, second, "zip", "tracedecay.exe")
    assert first.read_bytes() == second.read_bytes()

    with zipfile.ZipFile(first) as archive:
        entries = archive.infolist()
        assert len(entries) == 1
        entry = entries[0]
        expected_time = list(time.gmtime(EPOCH)[:6])
        expected_time[5] -= expected_time[5] % 2
        assert entry.filename == "tracedecay.exe"
        assert entry.date_time == tuple(expected_time)
        assert entry.compress_type == zipfile.ZIP_STORED
        assert stat.S_IMODE(entry.external_attr >> 16) == 0o755
        assert archive.read(entry) == PAYLOAD


def test_ncm_worker_sidecar_archive(temp: Path) -> None:
    worker = temp / "tracedecay-ncm-worker"
    manifest = temp / "worker-manifest.json"
    worker.write_bytes(WORKER_PAYLOAD)
    worker.chmod(0o755)
    manifest.write_bytes(WORKER_MANIFEST)
    first = temp / "ncm-worker-first.tar.gz"
    second = temp / "ncm-worker-second.tar.gz"

    package(
        worker,
        first,
        "tar.gz",
        "tracedecay-ncm-worker",
        (manifest, "worker-manifest.json"),
    )
    os.utime(worker, (EPOCH + 300, EPOCH + 300))
    os.utime(manifest, (EPOCH + 301, EPOCH + 301))
    package(
        worker,
        second,
        "tar.gz",
        "tracedecay-ncm-worker",
        (manifest, "worker-manifest.json"),
    )
    assert first.read_bytes() == second.read_bytes()
    assert hashlib.sha256(first.read_bytes()).hexdigest() == (
        "689ad1ef88de475b1394761ad318ab669e33ab47e32f7964004ac80314aefad6"
    )

    with tarfile.open(first, "r:gz") as archive:
        entries = archive.getmembers()
        assert [entry.name for entry in entries] == [
            "tracedecay-ncm-worker",
            "worker-manifest.json",
        ]
        worker_entry, manifest_entry = entries
        assert worker_entry.mode == 0o755
        assert manifest_entry.mode == 0o644
        for entry in entries:
            assert entry.uid == 0 and entry.gid == 0
            assert entry.uname == "" and entry.gname == ""
            assert entry.mtime == EPOCH
        worker_file = archive.extractfile(worker_entry)
        manifest_file = archive.extractfile(manifest_entry)
        assert worker_file is not None and worker_file.read() == WORKER_PAYLOAD
        assert manifest_file is not None and manifest_file.read() == WORKER_MANIFEST

    manifest = json.loads(WORKER_MANIFEST)
    target = manifest["targets"][0]
    assert target["triple"] == "aarch64-apple-darwin"
    assert target["bytes"] == len(WORKER_PAYLOAD)
    assert target["sha256"] == hashlib.sha256(WORKER_PAYLOAD).hexdigest()


def test_ncm_worker_manifest_symlink_is_rejected(temp: Path) -> None:
    root = temp / "manifest-symlink"
    root.mkdir()
    worker = root / "tracedecay-ncm-worker"
    manifest = root / "worker-manifest.json"
    manifest_target = root / "worker-manifest-target.json"
    output = root / "ncm-worker-symlink.tar.gz"
    worker.write_bytes(WORKER_PAYLOAD)
    worker.chmod(0o755)
    manifest_target.write_bytes(WORKER_MANIFEST)
    manifest.symlink_to(manifest_target)
    command = [
        "python3",
        str(PACKAGER),
        "--binary",
        str(worker),
        "--output",
        str(output),
        "--format",
        "tar.gz",
        "--entry-name",
        "tracedecay-ncm-worker",
        "--epoch",
        str(EPOCH),
        "--companion",
        f"{manifest}=worker-manifest.json",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    assert completed.returncode != 0
    assert "must not be a symlink" in completed.stderr


def main() -> None:
    with tempfile.TemporaryDirectory() as temp_name:
        temp = Path(temp_name)
        binary = temp / "binary"
        binary.write_bytes(PAYLOAD)
        binary.chmod(0o755)
        test_tar_gz(temp, binary)
        test_zip(temp, binary)
        test_ncm_worker_sidecar_archive(temp)
        test_ncm_worker_manifest_symlink_is_rejected(temp)
    print("release archive packaging tests passed")


if __name__ == "__main__":
    main()
