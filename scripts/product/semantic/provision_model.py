#!/usr/bin/env python3
"""CLI for journaled offline code-semantic model installation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from .provisioning import (
        EXPECTED_ARTIFACT_DIGEST,
        ManifestValidationError,
        SemanticProvisioningError,
        SimulatedInterruption,
        install_offline,
        load_manifest,
        rollback_install,
        uninstall_offline,
    )
except ImportError:  # direct ``python path/to/provision_model.py`` execution
    from provisioning import (  # type: ignore[no-redef]
        EXPECTED_ARTIFACT_DIGEST,
        ManifestValidationError,
        SemanticProvisioningError,
        SimulatedInterruption,
        install_offline,
        load_manifest,
        rollback_install,
        uninstall_offline,
    )


def _default_manifest() -> Path:
    return Path(__file__).resolve().parents[3] / "product/semantic/model-manifest.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Install a pinned semantic model from explicit local bytes; never contacts a network."
    )
    parser.add_argument("--manifest", type=Path, default=_default_manifest())
    parser.add_argument("--source", type=Path, help="local directory containing fixture.json and all members")
    parser.add_argument("--target", type=Path, help="destination directory to publish atomically")
    parser.add_argument("--receipt", type=Path, help="optional acquisition receipt sidecar")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument(
        "--uninstall",
        action="store_true",
        help="remove the published target and retain a durable rollback snapshot",
    )
    actions.add_argument(
        "--rollback",
        action="store_true",
        help="restore the latest durable uninstall snapshot",
    )
    parser.add_argument(
        "--interrupt-after",
        choices=(
            "journal",
            "member",
            "staged",
            "publishing",
            "published",
            "receipt",
            "uninstall-journal",
            "uninstall-target",
            "uninstall-receipt",
            "rollback-journal",
            "rollback-target",
            "rollback-receipt",
        ),
        help="fault-inject an interruption for recovery testing",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate manifest pins only; do not inspect or install bytes",
    )
    arguments = parser.parse_args(argv)
    try:
        manifest = load_manifest(arguments.manifest, require_pinned=True)
        if arguments.check:
            if arguments.uninstall or arguments.rollback:
                parser.error("--check cannot be combined with --uninstall or --rollback")
            print(
                json.dumps(
                    {
                        "model": manifest.model,
                        "revision": manifest.revision,
                        "dimensions": manifest.dimensions,
                        "max_length": manifest.max_length,
                        "artifact_digest": EXPECTED_ARTIFACT_DIGEST,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if arguments.target is None:
            parser.error("--target is required unless --check is used")
        if arguments.uninstall:
            if arguments.source is not None:
                parser.error("--source cannot be combined with --uninstall")
            result = uninstall_offline(
                arguments.target,
                manifest=manifest,
                receipt_path=arguments.receipt,
                interrupt_after=arguments.interrupt_after,
            )
        elif arguments.rollback:
            if arguments.source is not None:
                parser.error("--source cannot be combined with --rollback")
            result = rollback_install(
                arguments.target,
                manifest=manifest,
                receipt_path=arguments.receipt,
                interrupt_after=arguments.interrupt_after,
            )
        else:
            if arguments.source is None:
                parser.error("--source is required for installation")
            result = install_offline(
                arguments.source,
                arguments.target,
                manifest=manifest,
                receipt_path=arguments.receipt,
                interrupt_after=arguments.interrupt_after,
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
    except SimulatedInterruption as error:
        print(f"semantic model provisioning interrupted: {error}", file=sys.stderr)
        return 75
    except (SemanticProvisioningError, ManifestValidationError) as error:
        print(f"semantic model provisioning: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
