#!/usr/bin/env python3
"""CLI for complete offline semantic model and receipt verification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from .provisioning import (
        SemanticProvisioningError,
        load_manifest,
        verify_installation,
    )
except ImportError:  # direct ``python path/to/verify_model.py`` execution
    from provisioning import SemanticProvisioningError, load_manifest, verify_installation  # type: ignore[no-redef]


def _default_manifest() -> Path:
    return Path(__file__).resolve().parents[3] / "product/semantic/model-manifest.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify a complete local semantic model package and its acquisition receipt."
    )
    parser.add_argument("target", nargs="?", type=Path)
    parser.add_argument("--target", dest="target_option", type=Path)
    parser.add_argument("--manifest", type=Path, default=_default_manifest())
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--json", action="store_true", help="emit machine-readable verification evidence")
    arguments = parser.parse_args(argv)
    target = arguments.target_option or arguments.target
    if target is None:
        parser.error("a target directory is required")
    try:
        manifest = load_manifest(arguments.manifest, require_pinned=True)
        result = verify_installation(target, manifest, receipt_path=arguments.receipt)
        payload = {
            "status": "verified",
            "target": str(result.root),
            "members": result.member_count,
            "bytes": result.total_bytes,
            "manifest_sha256": result.manifest_sha256,
            "artifact_digest": result.artifact_digest,
        }
        print(json.dumps(payload, sort_keys=True) if arguments.json else f"verified {result.root} {result.artifact_digest}")
        return 0
    except SemanticProvisioningError as error:
        print(f"semantic model verification: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
