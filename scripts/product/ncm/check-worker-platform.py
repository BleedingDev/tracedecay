#!/usr/bin/env python3
"""Check the NCM worker platform policy against pinned and release manifests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from worker_platform_policy import (
    WorkerPlatformPolicyError,
    validate_worker_platform_policy,
    worker_platform_capability,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("product/ncm/reference/worker-platforms.json"),
    )
    parser.add_argument("--worker-manifest", type=Path)
    parser.add_argument("--release-targets", type=Path)
    parser.add_argument("--target", help="also print the capability for one target triple")
    parser.add_argument(
        "--require-supported",
        action="store_true",
        help="fail when --target is not an NCM-supported target",
    )
    arguments = parser.parse_args()
    try:
        policy = validate_worker_platform_policy(
            arguments.policy,
            worker_manifest_path=arguments.worker_manifest,
            release_target_manifest_path=arguments.release_targets,
        )
    except WorkerPlatformPolicyError as error:
        parser.error(str(error))
    if arguments.target:
        capability = worker_platform_capability(policy, arguments.target)
        print(json.dumps(capability, sort_keys=True))
        if arguments.require_supported and capability["status"] != "supported":
            return 2
    else:
        print("NCM worker platform policy is consistent with pinned and release manifests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
