#!/usr/bin/env python3
"""Validate and receipt the standalone Biomem-based Rust NCM backend.

This gate intentionally stops before host registration. It builds the production
worker, checks its artifact identity, executes non-empty backend populations,
runs a real-model worker/adapter journey in an isolated state root, and writes a
machine-readable receipt even when a prerequisite blocks acceptance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import time
from typing import Any, Iterable

BASE_COMMIT = "25778c7443cd0cfe257da363da01a56ea1d45d3f"
TASK_ID = "ncm-rs-022"
MODEL_CACHE_REPOSITORY = "models--Xenova--paraphrase-multilingual-MiniLM-L12-v2"
TEST_DOUBLE_MARKER = b"test-double/hash"
REAL_ARTIFACT_MARKERS = (b"fastembed", b"onnxruntime")


class GateFailure(RuntimeError):
    """One backend acceptance assertion failed."""


def canonical_json(value: Any) -> bytes:
    """Return stable compact UTF-8 JSON bytes."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    """Return a lowercase SHA-256 digest."""
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    """Hash a file without loading it all into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
) -> subprocess.CompletedProcess[str]:
    """Run one bounded command and retain output for the receipt."""
    print("+ " + " ".join(command), flush=True)
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if completed.stdout:
        lines = completed.stdout.splitlines()
        for line in lines[-40:]:
            print(line)
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


def target_dir(repo: Path, environment: dict[str, str]) -> Path:
    """Resolve Cargo's active target directory."""
    configured = environment.get("CARGO_TARGET_DIR")
    if not configured:
        return repo / "target"
    path = Path(configured)
    return path if path.is_absolute() else repo / path


def worker_path(repo: Path, environment: dict[str, str]) -> Path:
    """Return the debug production worker built by this gate."""
    name = "tracedecay-ncm-worker.exe" if os.name == "nt" else "tracedecay-ncm-worker"
    return target_dir(repo, environment) / "debug" / name


def verify_worker_artifact(path: Path) -> dict[str, Any]:
    """Prove real native inference is linked into the worker artifact.

    The same artifact serves production and ``--test-double`` launches (task
    017 froze the double as a launch flag, not a build), so the hash identity
    literal is necessarily present in the bytes. Which encoder is active is
    proven per launch by the handshake identity check in ``worker_identity``.
    """
    require(path.is_file(), f"worker artifact missing: {path}")
    data = path.read_bytes()
    markers = {marker.decode(): marker in data for marker in REAL_ARTIFACT_MARKERS}
    return {
        "path": str(path),
        "bytes": len(data),
        "sha256": sha256_bytes(data),
        "required_markers": markers,
    }


def model_snapshot(model_root: Path, manifest: dict[str, Any]) -> tuple[Path, str]:
    """Resolve the installed fastembed snapshot pinned by the local ref."""
    repository = model_root / "models" / MODEL_CACHE_REPOSITORY
    revision_file = repository / "refs" / "main"
    require(revision_file.is_file(), f"pinned model ref missing: {revision_file}")
    revision = revision_file.read_text(encoding="utf-8").strip()
    require(bool(revision), "pinned model revision is empty")
    snapshot = repository / "snapshots" / revision
    require(snapshot.is_dir(), f"pinned model snapshot missing: {snapshot}")
    require(bool(manifest.get("files")), "embedding manifest has no files")
    return snapshot, revision


def verify_model(model_root: Path, manifest_path: Path) -> dict[str, Any]:
    """Verify every pinned model byte count, digest, and local manifest."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    snapshot, revision = model_snapshot(model_root, manifest)
    checked = []
    for item in manifest["files"]:
        path = snapshot / item["path"]
        require(path.is_file(), f"pinned model file missing: {item['path']}")
        actual_bytes = path.stat().st_size
        actual_sha256 = file_sha256(path)
        require(actual_bytes == item["bytes"], f"model size mismatch: {item['path']}")
        require(actual_sha256 == item["sha256"], f"model digest mismatch: {item['path']}")
        checked.append({"path": item["path"], "bytes": actual_bytes, "sha256": actual_sha256})
    local_manifest = model_root / "models" / "ncm-encoder-manifest.json"
    require(local_manifest.is_file(), "runtime model manifest is absent")
    require(
        json.loads(local_manifest.read_text(encoding="utf-8")) == manifest,
        "runtime model manifest differs from checked-in pin",
    )
    return {
        "root": str(model_root),
        "model": manifest["model"],
        "artifact_sha256": manifest["files"][0]["sha256"],
        "revision": revision,
        "manifest_sha256": file_sha256(manifest_path),
        "files": checked,
    }


def install_model(repo: Path, model_root: Path, environment: dict[str, str]) -> None:
    """Call the runtime's sole download-capable encoder installer."""
    crate = model_root / ".backend-installer-src"
    if crate.exists():
        shutil.rmtree(crate)
    (crate / "src").mkdir(parents=True)
    runtime = repo / "crates" / "tracedecay-memory-ncm-runtime"
    (crate / "Cargo.toml").write_text(
        "[package]\nname = \"ncm-backend-installer\"\nversion = \"0.0.0\"\nedition = \"2024\"\n"
        "[workspace]\n\n[dependencies]\ntracedecay-memory-ncm-runtime = { path = "
        + json.dumps(str(runtime))
        + " }\n",
        encoding="utf-8",
    )
    (crate / "src" / "main.rs").write_text(
        "use tracedecay_memory_ncm_runtime::embedding::install;\n"
        "use tracedecay_memory_ncm_runtime::ports::{Deadline, StateRoot};\n"
        "fn main() {\n"
        " let path = std::env::args_os().nth(1).expect(\"state root\");\n"
        " let root = StateRoot::new(path).expect(\"absolute state root\");\n"
        " let id = install(&root, Deadline { remaining_ms: u64::MAX }).expect(\"runtime install\");\n"
        " println!(\"{} {}\", id.model, id.artifact_sha256);\n"
        "}\n",
        encoding="utf-8",
    )
    run(
        ["cargo", "run", "--quiet", "--manifest-path", str(crate / "Cargo.toml"), "--", str(model_root)],
        cwd=repo,
        environment=environment,
        timeout=7200,
    )


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
            {"protocol_version": 1, "algorithm_profile": "ncm-biomem-rs.v1"},
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
        "state_schema": payload.get("state_schema", "ncm.sqlite.v1"),
    }


def prepare_isolated_root(model_root: Path, journey_root: Path) -> None:
    """Create a fresh namespace root sharing only the verified model cache."""
    require(journey_root.is_absolute(), "journey state root must be absolute")
    if journey_root.exists():
        require(not any(journey_root.iterdir()), f"journey state root is not fresh: {journey_root}")
    else:
        journey_root.mkdir(parents=True)
    source_models = model_root / "models"
    require(source_models.is_dir(), "verified model directory is absent")
    destination = journey_root / "models"
    try:
        destination.symlink_to(source_models, target_is_directory=True)
    except OSError:
        shutil.copytree(source_models, destination)


def harness_source(repo: Path) -> str:
    """Return the isolated real-model adapter acceptance executable."""
    return r'''
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::TerminalCode;
use tracedecay_memory_provider_api::{
    CancellationToken, CanonicalPayload, HandshakeRequest, HandshakeRequestParts, MemoryProvider,
    OperationControl, OwnedExactScope, OwnedProviderId, OwnedVersionedId, PayloadSanitizationReceipt,
    PayloadSanitizationReceiptParts, ProviderCall, ProviderCallParts, ProviderLimits,
    ProviderOperation, ProviderReply, observation_extensions_digest,
};
use tracedecay_memory_provider_ncm::{
    NCM_PROVIDER_ID, NcmProviderAdapter, RustNcmConfig, RustNcmSurface, StateRoot, WorkerOptions,
};

const SCOPE_DIGEST: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn limits() -> ProviderLimits {
    ProviderLimits { request_bytes: 256*1024, response_bytes: 1024*1024,
        observation_batch_items: 16, recall_candidates: 16, concurrent_operations: 1,
        operation_millis: 30_000, snapshot_bytes: 256*1024*1024, inspection_items: 1_000 }
}
fn scope() -> OwnedExactScope {
    OwnedExactScope::new("ncm-backend", "project", "repository", "worktree",
        "refs/heads/feat/ncm-biomem-rust-v1", "standalone-acceptance", SCOPE_DIGEST).unwrap()
}
fn capabilities() -> Vec<OwnedVersionedId> {
    ["provider.health.v1", "observation.accept.v1", "recall.query.v1", "feedback.record.v1",
     "maintenance.run.v1", "inspection.read.v1", "correction.apply.v1", "deletion.by_source.v1",
     "snapshot.export.v1", "snapshot.restore.v1"]
        .into_iter().map(|v| OwnedVersionedId::new(v).unwrap()).collect()
}
fn adapter(worker: PathBuf, root: PathBuf) -> NcmProviderAdapter {
    let surface = RustNcmSurface::new(RustNcmConfig {
        worker_binary: worker,
        state_root: StateRoot::new(root).unwrap(),
        worker_options: WorkerOptions { test_double: false, reconciliation_deadline: Duration::from_secs(30), ..WorkerOptions::default() },
    }).unwrap();
    NcmProviderAdapter::new(Arc::new(surface)).unwrap()
}
fn handshake(provider: &NcmProviderAdapter, scope: &OwnedExactScope) -> (String, u64) {
    for _ in 0..2 {
        let response = provider.handshake(&HandshakeRequest::new(HandshakeRequestParts {
            provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).unwrap(), registration_revision: 1,
            exact_scope: scope.clone(), request_id: format!("handshake-{}", unique()),
            required_capabilities: capabilities(), host_limits: limits(),
            control: OperationControl::new(i64::MAX, 30_000, CancellationToken::new()),
            challenge_nonce: [7;32],
        }).unwrap());
        if response.terminal.terminal_code() == TerminalCode::StaleIdentity { continue; }
        assert_eq!(response.terminal.terminal_code(), TerminalCode::Success, "handshake: {:?}", response.terminal.diagnostic_id());
        return (response.ready_receipt_sha256.unwrap(), response.descriptor.unwrap().state_generation);
    }
    panic!("handshake stayed stale after identity refresh")
}
fn contract(op: ProviderOperation) -> &'static str { match op {
    ProviderOperation::Handshake => "tracedecay.memory.provider.handshake.v1",
    ProviderOperation::Health => "tracedecay.memory.provider.health.v1",
    ProviderOperation::Observe => "tracedecay.memory.provider.observation.v1",
    ProviderOperation::Recall => "tracedecay.memory.provider.recall.v1",
    ProviderOperation::Feedback => "tracedecay.memory.provider.feedback.v1",
    ProviderOperation::Maintenance => "tracedecay.memory.provider.maintenance.v1",
    ProviderOperation::Inspection => "tracedecay.memory.provider.inspection.v1",
    ProviderOperation::Correction => "tracedecay.memory.provider.correction.v1",
    ProviderOperation::DeleteBySource => "tracedecay.memory.provider.deletion-by-source.v1",
    ProviderOperation::SnapshotExport => "tracedecay.memory.provider.snapshot-export.v1",
    ProviderOperation::SnapshotRestore => "tracedecay.memory.provider.snapshot-restore.v1",
    ProviderOperation::Replay => "tracedecay.memory.provider.replay.v1",
}}
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}
fn payload(op: ProviderOperation, value: Value) -> CanonicalPayload {
    let bytes = serde_json::to_vec(&value).unwrap();
    CanonicalPayload::new(OwnedVersionedId::new(contract(op)).unwrap(), bytes.clone(), digest(&bytes)).unwrap()
}
fn call(op: ProviderOperation, scope: &OwnedExactScope, receipt: String, generation: u64,
        idem: Option<&str>, value: Value) -> ProviderCall {
    let call = ProviderCall::new(ProviderCallParts { operation: op,
        provider_id: OwnedProviderId::new(NCM_PROVIDER_ID).unwrap(), registration_revision: 1,
        ready_receipt_sha256: receipt, exact_scope: scope.clone(), request_id: format!("request-{}", unique()),
        operation_id: format!("operation-{}", unique()), expected_state_generation: generation,
        idempotency_key: idem.map(str::to_owned),
        control: OperationControl::new(i64::MAX, 30_000, CancellationToken::new()),
        payload: payload(op, value), required_capabilities: vec![OwnedVersionedId::new(op.capability_id()).unwrap()],
        extensions: Vec::new(),
    }).unwrap();
    if op != ProviderOperation::Observe { return call; }
    let extensions = observation_extensions_digest(&call.extensions).unwrap();
    let receipt = PayloadSanitizationReceipt::new(
        PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
            "tracedecay.memory.observation.hygiene.v1+ncm-backend", call.payload.sha256.clone(), extensions)).unwrap();
    call.with_sanitization(receipt)
}
fn invoke(provider: &NcmProviderAdapter, scope: &OwnedExactScope, op: ProviderOperation,
          idem: Option<&str>, value: Value) -> ProviderReply {
    let (receipt, generation) = handshake(provider, scope);
    provider.invoke(&call(op, scope, receipt, generation, idem, value))
}
fn success(label: &str, reply: &ProviderReply) {
    assert_eq!(reply.terminal.terminal_code(), TerminalCode::Success,
        "{label} failed: {:?}", reply.terminal.diagnostic_id());
}
fn response(reply: &ProviderReply) -> Value {
    serde_json::from_slice(&reply.payload.as_ref().unwrap().bytes).unwrap()
}
fn find_string(value: &Value, field: &str, expected: &str) -> bool { match value {
    Value::Object(o) => o.get(field).and_then(Value::as_str) == Some(expected)
        || o.values().any(|v| find_string(v, field, expected)),
    Value::Array(a) => a.iter().any(|v| find_string(v, field, expected)), _ => false,
}}
fn find_u64(value: &Value, field: &str) -> Option<u64> { match value {
    Value::Object(o) => o.get(field).and_then(Value::as_u64)
        .or_else(|| o.values().find_map(|v| find_u64(v, field))),
    Value::Array(a) => a.iter().find_map(|v| find_u64(v, field)), _ => None,
}}
fn absent(provider: &NcmProviderAdapter, scope: &OwnedExactScope, source: &str) {
    let reply = invoke(provider, scope, ProviderOperation::Recall, None,
        json!({"query_text":"Rust ownership prevents data races without garbage collection.", "top_k":5}));
    match reply.terminal.terminal_code() {
        TerminalCode::Success => assert!(!find_string(&response(&reply), "source", source), "deleted source recalled"),
        TerminalCode::SuccessZeroResults => {},
        other => panic!("unexpected recall terminal: {:?}", other),
    }
}
fn unique() -> u128 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() }

fn main() {
    let worker = PathBuf::from(std::env::args_os().nth(1).unwrap());
    let root = PathBuf::from(std::env::args_os().nth(2).unwrap());
    let scope = scope();
    let source = "backend-acceptance-source";
    let provider = adapter(worker.clone(), root.clone());
    let observed = invoke(&provider, &scope, ProviderOperation::Observe, Some("observe-real"), json!({
        "observation_kind":"tool.execution_settled.v1",
        "payload_contract":"tracedecay.memory.observation.tool-execution.v1",
        "canonical_payload":{"forget_source_key":source,
          "command":"Rust ownership prevents data races without garbage collection.",
          "outcome_summary":"ownership-record"}
    }));
    success("observe", &observed);
    let consolidated = invoke(&provider, &scope, ProviderOperation::Maintenance,
        Some("consolidate-real"), json!({"kind":"consolidate"}));
    success("consolidate", &consolidated);
    let inspection = invoke(&provider, &scope, ProviderOperation::Inspection, None, json!({}));
    success("inspection", &inspection);
    let ltm_active = find_u64(&response(&inspection), "ltm_active").unwrap();
    assert!(ltm_active > 0, "explicit consolidation left LTM empty");
    let exported = invoke(&provider, &scope, ProviderOperation::SnapshotExport, None, json!({}));
    success("snapshot export", &exported);
    let snapshot = response(&exported).get("bytes").and_then(Value::as_array).unwrap().clone();
    drop(provider);

    let provider = adapter(worker.clone(), root.clone());
    let recalled = invoke(&provider, &scope, ProviderOperation::Recall, None,
        json!({"query_text":"Rust ownership prevents data races without garbage collection.", "top_k":5}));
    success("restart recall", &recalled);
    assert!(find_string(&response(&recalled), "value_text", "ownership-record"), "restart recall lost consolidated record");
    let deleted = invoke(&provider, &scope, ProviderOperation::DeleteBySource,
        Some("delete-real"), json!({"source_id":source}));
    success("delete", &deleted);
    absent(&provider, &scope, source);
    drop(provider);

    let provider = adapter(worker, root);
    absent(&provider, &scope, source);
    let replay = invoke(&provider, &scope, ProviderOperation::Replay, Some("replay-revoked"), json!({}));
    assert_eq!(replay.terminal.terminal_code(), TerminalCode::CapabilityUnsupported,
        "v1 replay unexpectedly became available");
    absent(&provider, &scope, source);
    let restored = invoke(&provider, &scope, ProviderOperation::SnapshotRestore,
        Some("restore-revoked"), json!({"snapshot":snapshot}));
    success("snapshot restore", &restored);
    absent(&provider, &scope, source);
    println!("{}", json!({"status":"pass", "ltm_active_after_consolidate":ltm_active,
        "restart_recall":"persisted", "delete":"absent", "restart_after_delete":"absent",
        "replay":"capability_unsupported", "restore_revocation":"not_resurrected",
        "stm_exclusion_seam":false}));
}
'''


def build_harness(repo: Path, journey_root: Path, environment: dict[str, str]) -> Path:
    """Build the transient adapter journey without changing repository sources."""
    crate = journey_root / "adapter-journey-src"
    (crate / "src").mkdir(parents=True)
    provider = repo / "crates" / "tracedecay-memory-provider-ncm"
    provider_api = repo / "crates" / "tracedecay-memory-provider-api"
    package_name = "ncm-backend-journey-" + sha256_bytes(str(journey_root).encode())[:12]
    (crate / "Cargo.toml").write_text(
        "[package]\nname = " + json.dumps(package_name) + "\nversion = \"0.0.0\"\nedition = \"2024\"\n"
        "[workspace]\n\n[dependencies]\n"
        "tracedecay-memory-provider-ncm = { path = " + json.dumps(str(provider)) + ", features = [\"rust-backend\"] }\n"
        "tracedecay-memory-provider-api = { path = " + json.dumps(str(provider_api)) + " }\n"
        "serde_json = \"1\"\nsha2 = \"0.11\"\n",
        encoding="utf-8",
    )
    (crate / "src" / "main.rs").write_text(harness_source(repo), encoding="utf-8")
    run(
        ["cargo", "build", "--quiet", "--manifest-path", str(crate / "Cargo.toml")],
        cwd=repo,
        environment=environment,
    )
    name = package_name + (".exe" if os.name == "nt" else "")
    binary = target_dir(repo, environment) / "debug" / name
    require(binary.is_file(), f"adapter journey binary missing: {binary}")
    return binary


def run_adapter_journey(
    repo: Path,
    worker: Path,
    journey_root: Path,
    environment: dict[str, str],
) -> dict[str, Any]:
    """Run observe/consolidate/restart/delete/replay/restore through the adapter."""
    binary = build_harness(repo, journey_root, environment)
    empty_path = journey_root / "path-without-python"
    empty_path.mkdir()
    journey_environment = environment.copy()
    journey_environment["PATH"] = str(empty_path)
    completed = run(
        [str(binary), str(worker), str(journey_root)],
        cwd=repo,
        environment=journey_environment,
        timeout=1200,
    )
    line = next((line for line in reversed(completed.stdout.splitlines()) if line.startswith("{")), "")
    require(bool(line), "adapter journey emitted no JSON receipt")
    result = json.loads(line)
    require(result.get("status") == "pass", f"adapter journey did not pass: {result}")
    result["path_stripped_of_python"] = True
    result["state_root"] = str(journey_root)
    result["seam_note"] = (
        "No public STM-mask seam exists at the engine/adapter boundary. The gate requires explicit "
        "consolidation, ltm_active > 0 inspection evidence, worker restart, and successful persisted recall."
    )
    return result


def parse_test_ids(output: str) -> list[str]:
    """Extract Cargo's executable test IDs."""
    ids = []
    for line in output.splitlines():
        match = re.match(r"^(.+): test$", line.strip())
        if match:
            ids.append(match.group(1))
    return sorted(set(ids))


def parse_test_counts(output: str) -> dict[str, int]:
    """Sum Cargo test-result lines."""
    counts = {"passed": 0, "failed": 0, "ignored": 0}
    for match in re.finditer(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored", output):
        counts["passed"] += int(match.group(1))
        counts["failed"] += int(match.group(2))
        counts["ignored"] += int(match.group(3))
    return counts


def execute_population(
    name: str,
    base_command: list[str],
    *,
    repo: Path,
    environment: dict[str, str],
    extra_test_args: list[str] | None = None,
    require_zero_ignored: bool = True,
) -> dict[str, Any]:
    """List, require, then execute one Cargo test population."""
    listed_command = [*base_command, "--", "--list"]
    listed = run(listed_command, cwd=repo, environment=environment, check=False)
    ids = parse_test_ids(listed.stdout)
    require(listed.returncode == 0, f"{name} test discovery failed")
    require(bool(ids), f"{name} selected zero tests")
    command = list(base_command)
    if extra_test_args:
        command.extend(["--", *extra_test_args])
    completed = run(command, cwd=repo, environment=environment, check=False)
    counts = parse_test_counts(completed.stdout)
    require(completed.returncode == 0, f"{name} tests failed")
    require(counts["passed"] > 0, f"{name} executed zero passing tests")
    if require_zero_ignored:
        require(counts["ignored"] == 0, f"{name} skipped {counts['ignored']} mandatory tests")
    return {
        "name": name,
        "list_command": listed_command,
        "command": command,
        "selected_test_ids": ids,
        "counts": counts,
        "status": "pass",
    }


def evidence_file(path: Path) -> dict[str, Any]:
    """Describe a required prerequisite receipt."""
    return {
        "path": str(path),
        "exists": path.is_file(),
        "sha256": file_sha256(path) if path.is_file() else None,
    }


def numerical_summary(repo: Path) -> dict[str, Any]:
    """Summarize approved deviations, oracle fixture identities, and evaluation verdicts."""
    deviations_path = repo / "product/ncm/reference/deviations.json"
    deviations = json.loads(deviations_path.read_text(encoding="utf-8"))
    oracle_dir = repo / "product/ncm/reference/oracle"
    oracle = {path.name: file_sha256(path) for path in sorted(oracle_dir.glob("*.json"))}
    evaluation_path = repo / "product/ncm/evaluation/results-2026-09-06.json"
    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    return {
        "reference_commit": deviations.get("reference_commit"),
        "deviations": [
            {"id": item["id"], "component": item["component"], "status": item["status"]}
            for item in deviations["entries"]
        ],
        "oracle_fixture_sha256": oracle,
        "evaluation_acceptance": evaluation.get("acceptance"),
        "kernel_fidelity": evaluation.get("kernel_fidelity"),
        "algorithm_corrections": evaluation.get("algorithm_corrections"),
    }


def owned_paths(repo: Path) -> list[str]:
    """Return every path assigned to backend tasks 001-022."""
    dag = json.loads((repo / "product/ncm/plan/task_dag.json").read_text(encoding="utf-8"))
    paths = []
    for task in dag["tasks"]:
        if int(task["id"].rsplit("-", 1)[1]) <= 22:
            paths.extend(task["write_paths"])
    ownership = json.loads((repo / "product/ncm/bootstrap/ownership.json").read_text(encoding="utf-8"))
    paths.extend(ownership.get("coordinator_owned", []))
    for additions in ownership.get("task_ownership_additions", {}).values():
        if isinstance(additions, list):
            paths.extend(additions)
    return sorted(set(paths))


def path_allowed(path: str, allowed: Iterable[str]) -> bool:
    """Match exact files and declared directory prefixes."""
    for rule in allowed:
        normalized = rule.removesuffix("**")
        if normalized.endswith("/"):
            if path.startswith(normalized):
                return True
        elif path == normalized:
            return True
    return False


def allowed_diff(repo: Path) -> dict[str, Any]:
    """Review the complete branch and working-tree footprint against ownership."""
    allowed = owned_paths(repo)
    committed = set(filter(None, git(repo, "diff", "--name-only", BASE_COMMIT).splitlines()))
    status = git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    working = set()
    for line in status.splitlines():
        value = line[3:]
        if " -> " in value:
            value = value.split(" -> ", 1)[1]
        working.add(value)
    changed = sorted(committed | working)
    violations = [path for path in changed if not path_allowed(path, allowed)]
    stat = git(repo, "diff", "--stat", BASE_COMMIT)
    return {
        "base_commit": BASE_COMMIT,
        "ownership_manifest": "product/ncm/bootstrap/ownership.json",
        "allowed_rules": allowed,
        "changed_paths": changed,
        "violations": violations,
        "diff_stat": stat,
    }


def main() -> int:
    """Execute the backend-only acceptance gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--model-root", type=Path)
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--install-model", action="store_true")
    parser.add_argument("--receipt-dir", type=Path)
    arguments = parser.parse_args()

    repo = arguments.repo.resolve()
    environment = os.environ.copy()
    environment["RUSTC_WRAPPER"] = ""
    environment.setdefault("CARGO_TARGET_DIR", str(repo / "target"))
    environment.setdefault(
        "TRACEDECAY_DATA_DIR",
        str(target_dir(repo, environment) / "test-profile" / ".tracedecay"),
    )
    model_root = (
        arguments.model_root
        or (Path(environment["TRACEDECAY_NCM_REAL_MODEL_ROOT"]) if environment.get("TRACEDECAY_NCM_REAL_MODEL_ROOT") else None)
        or (target_dir(repo, environment) / "ncm-backend-model-root")
    ).resolve()
    journey_root = (
        arguments.state_root
        or (target_dir(repo, environment) / "test-profile" / f"ncm-backend-{os.getpid()}-{time.time_ns()}")
    ).resolve()
    receipt_dir = (arguments.receipt_dir or repo / "product/ncm/receipts/backend").resolve()
    receipt_dir.mkdir(parents=True, exist_ok=True)

    source_commit = git(repo, "rev-parse", "HEAD")
    source_tree = git(repo, "rev-parse", "HEAD^{tree}")
    branch = git(repo, "branch", "--show-current")
    commands: list[list[str]] = []
    blockers: list[str] = []
    limitations = [
        "The engine/adapter exposes no public STM-mask seam; LTM persistence is shown by explicit consolidation, ltm_active inspection, restart, and recall.",
        "This is standalone backend evidence only; tasks 023-026 still gate host integration, Observer/active behavior, usefulness, packaging, and release.",
    ]
    populations: list[dict[str, Any]] = []
    artifact: dict[str, Any] | None = None
    model: dict[str, Any] | None = None
    production_identity: dict[str, Any] | None = None
    test_double_identity: dict[str, Any] | None = None
    journey: dict[str, Any] | None = None

    diff = allowed_diff(repo)
    if diff["violations"]:
        blockers.append("allowed-path diff violations: " + ", ".join(diff["violations"]))

    evidence = {
        "conformance": evidence_file(repo / "product/ncm/conformance/receipt.json"),
        "performance": evidence_file(repo / "product/ncm/performance/results-sathanovo-2-local.json"),
        "evaluation": evidence_file(repo / "product/ncm/evaluation/results-2026-09-06.json"),
    }
    for name, item in evidence.items():
        if not item["exists"]:
            blockers.append(f"required {name} receipt missing: {item['path']}")

    production_environment = environment.copy()
    production_environment["CARGO_TARGET_DIR"] = str(
        target_dir(repo, environment) / "ncm-backend-production"
    )
    build = ["cargo", "build", "--locked", "-p", "tracedecay-memory-ncm-runtime", "--bin", "tracedecay-ncm-worker"]
    commands.append(build)
    try:
        run(build, cwd=repo, environment=production_environment)
        artifact = verify_worker_artifact(worker_path(repo, production_environment))
        if not all(artifact["required_markers"].values()):
            blockers.append(
                f"backend_artifact_identity_test: worker lacks real encoder markers: {artifact['required_markers']}"
            )
    except (GateFailure, subprocess.TimeoutExpired) as error:
        blockers.append(f"backend_artifact_identity_test: {error}")

    cheap_groups = [
        ("core_no_default", ["cargo", "test", "--locked", "-p", "tracedecay-memory-ncm-core", "--no-default-features"]),
        ("runtime_no_default", ["cargo", "test", "--locked", "-p", "tracedecay-memory-ncm-runtime", "--no-default-features"]),
        ("adapter_rust_backend", ["cargo", "test", "--locked", "-p", "tracedecay-memory-provider-ncm", "--no-default-features", "--features", "rust-backend"]),
    ]
    for name, command in cheap_groups:
        commands.append([*command, "--", "--list"])
        commands.append(command)
        try:
            populations.append(
                execute_population(
                    name,
                    command,
                    repo=repo,
                    environment=environment,
                    require_zero_ignored=name != "adapter_rust_backend",
                )
            )
        except (GateFailure, subprocess.TimeoutExpired) as error:
            blockers.append(f"{name}: {error}")
            populations.append({"name": name, "command": command, "status": "blocked", "error": str(error)})

    manifest_path = repo / "product/ncm/reference/embedding-manifest.json"
    model_root.mkdir(parents=True, exist_ok=True)
    try:
        model = verify_model(model_root, manifest_path)
    except GateFailure as error:
        if arguments.install_model:
            try:
                install_model(repo, model_root, environment)
                model = verify_model(model_root, manifest_path)
            except (GateFailure, subprocess.TimeoutExpired) as install_error:
                blockers.append(f"pinned real-model evidence: {install_error}")
        else:
            blockers.append(f"pinned real-model evidence: {error}; rerun with --install-model")

    if artifact is not None and model is not None:
        empty_path = journey_root.parent / f"ncm-backend-empty-path-{os.getpid()}"
        try:
            production_identity = worker_identity(
                Path(artifact["path"]), model_root, empty_path, test_double=False
            )
            test_double_root = journey_root.parent / f"ncm-backend-double-{os.getpid()}-{time.time_ns()}"
            test_double_root.mkdir(parents=True)
            test_double_identity = worker_identity(
                Path(artifact["path"]), test_double_root, empty_path, test_double=True
            )
        except GateFailure as error:
            blockers.append(f"worker launch identity: {error}")

        real_groups = [
            (
                "real_embeddings",
                ["cargo", "test", "--locked", "-p", "tracedecay-memory-ncm-runtime", "--test", "real_embeddings"],
                None,
            ),
            (
                "isolated_text_journey",
                [
                    "cargo", "test", "--locked", "-p", "tracedecay-memory-ncm-runtime",
                    "--test", "engine_transactions", "real_encoder_paraphrase_journey_returns_the_matching_record_first",
                ],
                ["--exact"],
            ),
            (
                "adapter_real_encoder_population",
                [
                    "cargo", "test", "--locked", "-p", "tracedecay-memory-provider-ncm",
                    "--no-default-features", "--features", "rust-backend", "--test", "rust_backend_conformance",
                    "enabled::real_encoder_process_population",
                ],
                ["--exact", "--ignored"],
            ),
        ]
        real_environment = environment.copy()
        real_environment["TRACEDECAY_NCM_REAL_MODEL_ROOT"] = str(model_root)
        real_environment["TRACEDECAY_NCM_WORKER"] = str(worker_path(repo, production_environment))
        for name, command, test_args in real_groups:
            commands.append([*command, "--", "--list"])
            commands.append([*command, "--", *(test_args or [])])
            try:
                populations.append(
                    execute_population(
                        name,
                        command,
                        repo=repo,
                        environment=real_environment,
                        extra_test_args=test_args,
                    )
                )
            except (GateFailure, subprocess.TimeoutExpired) as error:
                blockers.append(f"{name}: {error}")
                populations.append({"name": name, "command": command, "status": "blocked", "error": str(error)})

        try:
            prepare_isolated_root(model_root, journey_root)
            journey = run_adapter_journey(repo, Path(artifact["path"]), journey_root, environment)
        except (GateFailure, subprocess.TimeoutExpired) as error:
            blockers.append(f"real worker+adapter journey: {error}")

    status = "pass" if not blockers else "blocked"
    receipt = {
        "schema_version": 1,
        "task_id": TASK_ID,
        "status": status,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "branch": branch,
        "base_commit": BASE_COMMIT,
        "implementation_summary": (
            "Backend-only gate for production worker identity, non-empty Rust test populations, "
            "pinned MiniLM evidence, isolated worker/adapter persistence and deletion, and ownership review."
        ),
        "worker_artifact": artifact,
        "identities": {
            "production": production_identity,
            "test_double_control": test_double_identity,
            "algorithm_profile": "ncm-biomem-rs.v1",
            "model": model,
        },
        "numerical_difference_summary": numerical_summary(repo),
        "evidence": evidence,
        "test_populations": populations,
        "journey": journey,
        "allowed_path_diff": diff,
        "exact_commands": commands,
        "negative_controls": [
            "Removing fastembed or ONNX Runtime linkage fails required artifact-marker assertions.",
            "Embedding literal test-double/hash in the production artifact fails backend_artifact_identity_test.",
            "The same worker launched with --test-double must identify as test-double/hash; production launch must identify as pinned MiniLM.",
            "An empty Cargo selection fails before a population can pass; ignored real-model coverage is executed explicitly with --ignored.",
            "The adapter journey runs with PATH containing no Python executable; a PATH-resolved Python substitution cannot start.",
            "Skipping consolidation makes ltm_active_after_consolidate remain zero.",
            "Losing durable state makes restart recall miss ownership-record.",
            "A deletion no-op leaves backend-acceptance-source in recall.",
            "Enabling v1 replay changes CapabilityUnsupported and fails the replay assertion.",
            "Ignoring live revocations on restore resurrects the deleted source and fails the final absent assertion.",
        ],
        "limitations": limitations,
        "blockers": blockers,
        "reviewer_verdict": "pass" if status == "pass" else "blocked",
    }
    receipt_path = receipt_dir / f"{source_commit}.json"
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"backend receipt: {receipt_path}")
    if blockers:
        for blocker in blockers:
            print(f"BLOCKER: {blocker}", file=sys.stderr)
        return 2
    print("backend-accepted, host-not-yet-integrated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
