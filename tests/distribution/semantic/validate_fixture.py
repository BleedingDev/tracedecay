#!/usr/bin/env python3
"""Validate a semantic fixture directory entirely offline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.product.semantic.provisioning import (  # noqa: E402
    SemanticProvisioningError,
    load_manifest,
    verify_directory,
    verify_installation,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate a complete local semantic fixture.")
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--require-receipt",
        action="store_true",
        help="also verify the target-bound acquisition receipt",
    )
    arguments = parser.parse_args(argv)
    try:
        manifest = load_manifest(arguments.fixture / "fixture.json", require_pinned=True)
        if arguments.require_receipt or arguments.receipt is not None:
            result = verify_installation(arguments.fixture, manifest, receipt_path=arguments.receipt)
        else:
            result = verify_directory(arguments.fixture, manifest)
        payload = {
            "status": "verified",
            "target": str(result.root),
            "members": result.member_count,
            "bytes": result.total_bytes,
            "manifest_sha256": result.manifest_sha256,
            "artifact_digest": result.artifact_digest,
        }
        print(json.dumps(payload, sort_keys=True) if arguments.json else f"{manifest.dimensions}\t{manifest.max_length}")
        return 0
    except SemanticProvisioningError as error:
        print(f"semantic fixture validation: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
