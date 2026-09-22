#!/usr/bin/env python3
"""Offline compatibility entry point for semantic fixture preparation."""

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
    install_offline,
    load_manifest,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prepare a semantic fixture from explicit local bytes; never contacts a network."
    )
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", nargs="?", type=Path)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        manifest_path = arguments.source / "fixture.json"
        manifest = load_manifest(manifest_path, require_pinned=True)
        if arguments.check:
            if arguments.destination is not None:
                parser.error("--check does not accept a destination")
            print(f"{manifest.dimensions}\t{manifest.max_length}")
            return 0
        if arguments.destination is None:
            parser.error("destination is required unless --check is used")
        result = install_offline(
            arguments.source,
            arguments.destination,
            manifest=manifest,
        )
        print(
            json.dumps(
                {
                    "status": result.status,
                    "target": str(result.target),
                    "receipt": str(result.receipt),
                    "artifact_digest": result.artifact_digest,
                },
                sort_keys=True,
            )
        )
        return 0
    except SemanticProvisioningError as error:
        print(f"semantic fixture preparation: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
