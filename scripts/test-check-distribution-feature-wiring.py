#!/usr/bin/env python3
"""Regression tests for distribution feature ownership validation."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import tempfile


VALIDATOR = Path(__file__).with_name("check-distribution-feature-wiring.py")

ROOT_MANIFEST = """[package]
name = "tracedecay"
version = "0.1.0-beta.34"

[dependencies]
tracedecay-code-index = { version = "0.1.0" }
tracedecay-application = { version = "0.1.0" }

[features]
lite = ["tracedecay-code-index/lite", "tracedecay-code-index-runtime/lite"]
medium = ["tracedecay-code-index/medium"]
full = ["tracedecay-code-index/full", "tracedecay-code-index-runtime/full"]
hotpath = []
hotpath-alloc = ["hotpath"]
hotpath-cpu = ["hotpath"]
hotpath-mcp = ["hotpath"]
lang-dart = ["tracedecay-code-index/lang-dart"]
lang-markdown = ["tracedecay-code-index/lang-markdown"]
token-counting = []
test-transport = []
"""

CODE_INDEX_MANIFEST = """[package]
name = "tracedecay-code-index"
version = "0.1.0"

[dependencies]
tracedecay-code-extraction = { version = "0.1.0" }

[features]
lite = ["tracedecay-code-extraction/lite", "lang-markdown"]
medium = ["tracedecay-code-extraction/medium"]
full = ["tracedecay-code-extraction/full", "lang-markdown"]
lang-dart = ["tracedecay-code-extraction/lang-dart"]
lang-markdown = ["tracedecay-code-extraction/lang-markdown"]
"""

EXTRACTION_MANIFEST = """[package]
name = "tracedecay-code-extraction"
version = "0.1.0"

[features]
lite = ["lang-markdown"]
medium = ["lang-dart"]
full = ["medium", "lang-markdown"]
lang-dart = []
lang-markdown = []
"""

EXTRACTION_BUILD_MANIFEST = """[package]
name = "tracedecay-code-extraction"
version = "0.1.0"
edition = "2024"

[features]
default = []
lang-dart = ["dep:dart-grammar"]
lang-markdown = ["dep:markdown-grammar"]

[dependencies]
dart-grammar = { path = "dart-grammar", optional = true }
markdown-grammar = { path = "markdown-grammar", optional = true }
"""

EXTRACTION_BUILD_LIB = """#[cfg(feature = "lang-dart")]
pub fn dart() -> dart_grammar::Language {
    dart_grammar::language()
}

#[cfg(feature = "lang-markdown")]
pub fn markdown() -> markdown_grammar::Language {
    markdown_grammar::language()
}
"""

GRAMMAR_MANIFEST = """[package]
name = "{name}"
version = "0.1.0"
edition = "2024"

[lib]
path = "lib.rs"
"""

GRAMMAR_LIB = """pub struct Language;

pub fn language() -> Language {
    Language
}
"""

CLI_MANIFEST = """[package]
name = "tracedecay-cli"
version = "0.1.0"

[dependencies]
hotpath = { version = "0.24", optional = true }
regex = { version = "1", optional = true }
tracedecay = { version = "0.1.0" }

[features]
default = ["production"]
production = ["tracedecay/production"]
hotpath = [
    "dep:regex",
    "tracedecay/hotpath",
    "hotpath/hotpath",
    "hotpath/tokio",
    "hotpath/ureq-3",
]
hotpath-alloc = [
    "hotpath",
    "tracedecay/hotpath-alloc",
    "hotpath/hotpath-alloc",
]
hotpath-cpu = [
    "hotpath",
    "tracedecay/hotpath-cpu",
    "hotpath/hotpath-cpu",
]
hotpath-mcp = ["hotpath", "hotpath/hotpath-mcp"]
"""

NCM_ROOT_MANIFEST = ROOT_MANIFEST.replace(
    "[dependencies]\n",
    "[dependencies]\n"
    'tracedecay-memory-provider-ncm = { version = "0.1.0", optional = true, '
    'features = ["rust-backend"] }\n',
).replace(
    "[features]\n",
    '[features]\nmemory-provider-host = ["dep:tracedecay-memory-provider-ncm"]\n',
)

NCM_PROVIDER_MANIFEST = """[package]
name = "tracedecay-memory-provider-ncm"
version = "0.1.0"

[features]
default = []
rust-backend = [
    "dep:tracedecay-memory-ncm-runtime",
    "tracedecay-memory-ncm-runtime/real-encoder",
]

[dependencies]
tracedecay-memory-ncm-runtime = { path = "../tracedecay-memory-ncm-runtime", optional = true, default-features = false }
"""

NCM_RUNTIME_MANIFEST = """[package]
name = "tracedecay-memory-ncm-runtime"
version = "0.1.0"

[features]
default = []
real-encoder = ["dep:fastembed"]

[dependencies]
fastembed = { version = "1", optional = true, default-features = false }
"""

NCM_POLICY = {
    "schema_version": 1,
    "provider_id": "ncm",
    "worker": "tracedecay-ncm-worker",
    "policy": "pinned-artifact-only",
    "worker_manifest": "product/ncm/reference/worker-manifest.json",
    "release_target_manifest": ".github/release-targets.json",
    "fallback": "native-only",
    "runtime_policy": {
        "provider_package": "tracedecay-memory-provider-ncm",
        "provider_feature": "rust-backend",
        "runtime_package": "tracedecay-memory-ncm-runtime",
        "runtime_feature": "real-encoder",
        "worker_distribution": "separate-sidecar",
        "worker_manifest": "product/ncm/reference/worker-manifest.json",
        "model_manifest": "product/ncm/reference/embedding-manifest.json",
    },
    "packaging": {
        "worker_distribution": "separate-sidecar",
        "standard_cli_archive_includes_worker": False,
        "manifest_sidecar_required": True,
    },
    "supported_targets": [
        {
            "target": "aarch64-apple-darwin",
            "release_name": "aarch64-macos",
            "status": "supported",
        }
    ],
    "unsupported_targets": [
        {"target": "x86_64-unknown-linux-gnu", "reason": "no-pinned-worker-artifact"},
        {"target": "aarch64-unknown-linux-gnu", "reason": "no-pinned-worker-artifact"},
        {"target": "x86_64-pc-windows-msvc", "reason": "no-pinned-worker-artifact"},
    ],
    "release_targets": [
        {
            "name": "aarch64-macos",
            "target": "aarch64-apple-darwin",
            "ncm": "supported",
        },
        {
            "name": "x86_64-linux",
            "target": "x86_64-unknown-linux-gnu",
            "ncm": "native-only",
        },
        {
            "name": "aarch64-linux",
            "target": "aarch64-unknown-linux-gnu",
            "ncm": "native-only",
        },
        {
            "name": "x86_64-windows",
            "target": "x86_64-pc-windows-msvc",
            "ncm": "native-only",
        },
    ],
}

NCM_WORKER_MANIFEST = {
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
            "bytes": 1,
            "sha256": "a" * 64,
        }
    ],
}

NCM_MODEL_MANIFEST = {
    "model": "paraphrase-multilingual-MiniLM-L12-v2",
    "repository": "Xenova/paraphrase-multilingual-MiniLM-L12-v2",
    "revision": "2c4055b12046f11709e9df2c122e59ffbdc2f900",
    "revision_provenance": "product/ncm/receipts/test.json#/identities/model/revision",
    "files": [
        {"path": path, "sha256": "b" * 64, "bytes": 1}
        for path in (
            "onnx/model.onnx",
            "tokenizer.json",
            "config.json",
            "special_tokens_map.json",
            "tokenizer_config.json",
        )
    ],
    "max_length": 128,
    "pooling": "mean",
    "normalize": True,
}

NCM_RELEASE_TARGETS = {
    "include": [
        {
            "name": "aarch64-macos",
            "runner": "macos-14",
            "target": "aarch64-apple-darwin",
            "archive": "tar.gz",
        },
        {
            "name": "x86_64-linux",
            "runner": "ubuntu-22.04",
            "target": "x86_64-unknown-linux-gnu",
            "archive": "tar.gz",
        },
        {
            "name": "aarch64-linux",
            "runner": "ubuntu-22.04-arm",
            "target": "aarch64-unknown-linux-gnu",
            "archive": "tar.gz",
        },
        {
            "name": "x86_64-windows",
            "runner": "windows-latest",
            "target": "x86_64-pc-windows-msvc",
            "archive": "zip",
        },
    ]
}


def ncm_fixture() -> dict[str, object]:
    return {
        "policy.json": json.loads(json.dumps(NCM_POLICY)),
        "worker.json": json.loads(json.dumps(NCM_WORKER_MANIFEST)),
        "model.json": json.loads(json.dumps(NCM_MODEL_MANIFEST)),
        "release-targets.json": json.loads(json.dumps(NCM_RELEASE_TARGETS)),
        "provider.toml": NCM_PROVIDER_MANIFEST,
        "runtime.toml": NCM_RUNTIME_MANIFEST,
    }


@dataclass(frozen=True)
class FixtureResult:
    returncode: int
    stdout: str
    stderr: str


def run_fixture(
    root_source: str = ROOT_MANIFEST,
    root_packaged: str = ROOT_MANIFEST,
    code_index_source: str = CODE_INDEX_MANIFEST,
    code_index_packaged: str = CODE_INDEX_MANIFEST,
    extraction_source: str = EXTRACTION_MANIFEST,
    extraction_packaged: str = EXTRACTION_MANIFEST,
    cli_source: str = CLI_MANIFEST,
    cli_packaged: str = CLI_MANIFEST,
    extraction_build_manifest: str | None = None,
    ncm_files: dict[str, object] | None = None,
    ncm_workflows: list[Path] | None = None,
) -> FixtureResult:
    with tempfile.TemporaryDirectory() as temporary_directory:
        root = Path(temporary_directory)
        manifests = {
            "root-source.toml": root_source,
            "root-packaged.toml": root_packaged,
            "code-index-source.toml": code_index_source,
            "code-index-packaged.toml": code_index_packaged,
            "extraction-source.toml": extraction_source,
            "extraction-packaged.toml": extraction_packaged,
            "cli-source.toml": cli_source,
            "cli-packaged.toml": cli_packaged,
        }
        for name, contents in manifests.items():
            root.joinpath(name).write_text(contents, encoding="utf-8")
        command = [
            sys.executable,
            str(VALIDATOR),
            "--root-source",
            str(root / "root-source.toml"),
            "--root-packaged",
            str(root / "root-packaged.toml"),
            "--code-index-source",
            str(root / "code-index-source.toml"),
            "--code-index-packaged",
            str(root / "code-index-packaged.toml"),
            "--extraction-source",
            str(root / "extraction-source.toml"),
            "--extraction-packaged",
            str(root / "extraction-packaged.toml"),
            "--cli-source",
            str(root / "cli-source.toml"),
            "--cli-packaged",
            str(root / "cli-packaged.toml"),
        ]
        if ncm_files is not None:
            for name, contents in ncm_files.items():
                path = root / name
                if isinstance(contents, str):
                    text = contents
                else:
                    text = json.dumps(contents)
                path.write_text(text, encoding="utf-8")
            command.extend(
                [
                    "--ncm-platform-policy",
                    str(root / "policy.json"),
                    "--ncm-worker-manifest",
                    str(root / "worker.json"),
                    "--ncm-model-manifest",
                    str(root / "model.json"),
                    "--ncm-release-targets",
                    str(root / "release-targets.json"),
                    "--ncm-provider-manifest",
                    str(root / "provider.toml"),
                    "--ncm-runtime-manifest",
                    str(root / "runtime.toml"),
                ]
            )
            for workflow in ncm_workflows or []:
                command.extend(["--ncm-release-workflow", str(workflow)])
        if extraction_build_manifest is not None:
            extraction = root / "extraction-build"
            extraction.joinpath("src").mkdir(parents=True)
            extraction.joinpath("Cargo.toml").write_text(
                extraction_build_manifest, encoding="utf-8"
            )
            extraction.joinpath("src/lib.rs").write_text(
                EXTRACTION_BUILD_LIB, encoding="utf-8"
            )
            for grammar in ("dart-grammar", "markdown-grammar"):
                dependency = extraction / grammar
                dependency.mkdir()
                dependency.joinpath("Cargo.toml").write_text(
                    GRAMMAR_MANIFEST.format(name=grammar), encoding="utf-8"
                )
                dependency.joinpath("lib.rs").write_text(
                    GRAMMAR_LIB, encoding="utf-8"
                )
            command.extend(
                ["--check-extraction-manifest", str(extraction / "Cargo.toml")]
            )
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        return FixtureResult(completed.returncode, completed.stdout, completed.stderr)


def main() -> int:
    extracted_owner = run_fixture()
    if extracted_owner.returncode != 0:
        raise SystemExit(extracted_owner.stderr)

    cli_without_cpu = CLI_MANIFEST.replace(
        'hotpath-cpu = [\n    "hotpath",\n    "tracedecay/hotpath-cpu",\n    "hotpath/hotpath-cpu",\n]\n',
        "",
    )
    missing_cli_cpu = run_fixture(
        cli_source=cli_without_cpu,
        cli_packaged=cli_without_cpu,
    )
    if missing_cli_cpu.returncode == 0:
        raise SystemExit("CLI without the Hotpath CPU release feature was accepted")
    if "tracedecay-cli is missing required features" not in missing_cli_cpu.stderr:
        raise SystemExit("missing CLI CPU feature failed for an unexpected reason")

    cli_with_miswired_mcp = CLI_MANIFEST.replace(
        'hotpath-mcp = ["hotpath", "hotpath/hotpath-mcp"]',
        'hotpath-mcp = ["hotpath"]',
    )
    miswired_cli_mcp = run_fixture(
        cli_source=cli_with_miswired_mcp,
        cli_packaged=cli_with_miswired_mcp,
    )
    if miswired_cli_mcp.returncode == 0:
        raise SystemExit("CLI with a mountless Hotpath MCP feature was accepted")
    if "tracedecay-cli hotpath-mcp must enable" not in miswired_cli_mcp.stderr:
        raise SystemExit("miswired CLI MCP feature failed for an unexpected reason")

    # Internal forwarding topology is not a contract: rerouting a tier through
    # a different intermediate edge, forwarding a language alias through the
    # runtime crate, and adding a local tier member are all accepted without
    # touching this validator.
    rewired_root = ROOT_MANIFEST.replace(
        'lite = ["tracedecay-code-index/lite", "tracedecay-code-index-runtime/lite"]',
        'lite = ["tracedecay-code-index-runtime/lite"]',
    ).replace(
        'lang-dart = ["tracedecay-code-index/lang-dart"]',
        'lang-dart = ["tracedecay-code-index-runtime/lang-dart"]',
    ).replace(
        'full = ["tracedecay-code-index/full", "tracedecay-code-index-runtime/full"]',
        'full = ["tracedecay-code-index/full", "tracedecay-code-index-runtime/full", "lang-markdown"]',
    )
    rewired_code_index = CODE_INDEX_MANIFEST.replace(
        'lang-dart = ["tracedecay-code-extraction/lang-dart"]\n', ""
    )
    rewired = run_fixture(
        root_source=rewired_root,
        root_packaged=rewired_root,
        code_index_source=rewired_code_index,
        code_index_packaged=rewired_code_index,
    )
    if rewired.returncode != 0:
        raise SystemExit(
            "behavior-preserving forwarding change was rejected: " + rewired.stderr
        )

    packaged_root_drift = run_fixture(
        root_packaged=ROOT_MANIFEST.replace('lang-dart = ["tracedecay-code-index/lang-dart"]\n', "")
    )
    if packaged_root_drift.returncode == 0:
        raise SystemExit("packaged root manifest dropping a feature was accepted")
    if "packaged root feature wiring differs" not in packaged_root_drift.stderr:
        raise SystemExit("packaged root drift failed for an unexpected reason")

    extraction_with_new_language = EXTRACTION_MANIFEST.replace(
        "lang-markdown = []\n", "lang-markdown = []\nlang-rustdoc = []\n"
    )
    unforwarded_language = run_fixture(
        extraction_source=extraction_with_new_language,
        extraction_packaged=extraction_with_new_language,
    )
    if unforwarded_language.returncode == 0:
        raise SystemExit("new extraction language without public aliases was accepted")
    if "root language features differ" not in unforwarded_language.stderr:
        raise SystemExit("unforwarded language failed for an unexpected reason")

    isolated_languages = run_fixture(
        extraction_build_manifest=EXTRACTION_BUILD_MANIFEST
    )
    if isolated_languages.returncode != 0:
        raise SystemExit(isolated_languages.stderr)

    extraction_with_miswired_language = EXTRACTION_BUILD_MANIFEST.replace(
        'lang-dart = ["dep:dart-grammar"]',
        'lang-dart = ["dep:markdown-grammar"]',
    )
    miswired_language = run_fixture(
        extraction_build_manifest=extraction_with_miswired_language
    )
    if miswired_language.returncode == 0:
        raise SystemExit("miswired isolated extraction language was accepted")
    if "lang-dart does not compile in isolation" not in miswired_language.stderr:
        raise SystemExit("miswired extraction language failed for an unexpected reason")

    ncm_valid = run_fixture(
        root_source=NCM_ROOT_MANIFEST,
        root_packaged=NCM_ROOT_MANIFEST,
        ncm_files=ncm_fixture(),
        ncm_workflows=[
            VALIDATOR.parent.parent / ".github/workflows/release.yml",
            VALIDATOR.parent.parent / ".github/workflows/release-beta.yml",
        ],
    )
    if ncm_valid.returncode != 0:
        raise SystemExit("valid NCM distribution matrix was rejected: " + ncm_valid.stderr)

    ncm_without_runtime_feature = ncm_fixture()
    ncm_without_runtime_feature["provider.toml"] = NCM_PROVIDER_MANIFEST.replace(
        '    "tracedecay-memory-ncm-runtime/real-encoder",\n', ""
    )
    missing_runtime_feature = run_fixture(
        root_source=NCM_ROOT_MANIFEST,
        root_packaged=NCM_ROOT_MANIFEST,
        ncm_files=ncm_without_runtime_feature,
    )
    if missing_runtime_feature.returncode == 0:
        raise SystemExit("NCM host without the real encoder feature was accepted")
    if (
        "does not enable tracedecay-memory-ncm-runtime/real-encoder"
        not in missing_runtime_feature.stderr
    ):
        raise SystemExit(
            "missing NCM runtime feature failed for an unexpected reason: "
            + missing_runtime_feature.stderr
        )

    ncm_with_extra_supported_target = ncm_fixture()
    extra_supported_policy = ncm_with_extra_supported_target["policy.json"]
    assert isinstance(extra_supported_policy, dict)
    extra_supported_policy["release_targets"][1]["ncm"] = "supported"
    extra_supported_target = run_fixture(
        root_source=NCM_ROOT_MANIFEST,
        root_packaged=NCM_ROOT_MANIFEST,
        ncm_files=ncm_with_extra_supported_target,
    )
    if extra_supported_target.returncode == 0:
        raise SystemExit("non-macOS release target claiming NCM was accepted")
    if "without a pinned worker target" not in extra_supported_target.stderr:
        raise SystemExit(
            "extra NCM target failed for an unexpected reason: "
            + extra_supported_target.stderr
        )

    ncm_without_artifact_policy = ncm_fixture()
    artifact_policy = ncm_without_artifact_policy["policy.json"]
    assert isinstance(artifact_policy, dict)
    del artifact_policy["runtime_policy"]
    missing_artifact_policy = run_fixture(
        root_source=NCM_ROOT_MANIFEST,
        root_packaged=NCM_ROOT_MANIFEST,
        ncm_files=ncm_without_artifact_policy,
    )
    if missing_artifact_policy.returncode == 0:
        raise SystemExit("NCM policy without runtime/artifact policy was accepted")
    if "has no runtime_policy" not in missing_artifact_policy.stderr:
        raise SystemExit(
            "missing NCM runtime/artifact policy failed for an unexpected reason: "
            + missing_artifact_policy.stderr
        )

    ncm_model_drift = ncm_fixture()
    model_drift = ncm_model_drift["model.json"]
    assert isinstance(model_drift, dict)
    model_drift["revision"] = "0" * 40
    drifted_model = run_fixture(
        root_source=NCM_ROOT_MANIFEST,
        root_packaged=NCM_ROOT_MANIFEST,
        ncm_files=ncm_model_drift,
    )
    if drifted_model.returncode == 0:
        raise SystemExit("drifted NCM model revision was accepted")
    if "pinned model contract field revision" not in drifted_model.stderr:
        raise SystemExit(
            "drifted NCM model failed for an unexpected reason: " + drifted_model.stderr
        )

    with tempfile.TemporaryDirectory() as workflow_directory:
        broken_workflow = Path(workflow_directory) / "release.yml"
        broken_workflow.write_text(
            (VALIDATOR.parent.parent / ".github/workflows/release.yml")
            .read_text(encoding="utf-8")
            .replace('--companion "$manifest=worker-manifest.json"', ""),
            encoding="utf-8",
        )
        missing_archive_companion = run_fixture(
            root_source=NCM_ROOT_MANIFEST,
            root_packaged=NCM_ROOT_MANIFEST,
            ncm_files=ncm_fixture(),
            ncm_workflows=[broken_workflow],
        )
    if missing_archive_companion.returncode == 0:
        raise SystemExit("NCM sidecar workflow without the manifest companion was accepted")
    if "sidecar/archive/checksum contract" not in missing_archive_companion.stderr:
        raise SystemExit(
            "missing NCM archive companion failed for an unexpected reason: "
            + missing_archive_companion.stderr
        )

    print("distribution feature wiring fixtures passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
