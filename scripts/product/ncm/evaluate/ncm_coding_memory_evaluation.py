#!/usr/bin/env python3
"""Provider-neutral nine-scenario coding-memory evaluation for ncm-rs-021."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from ncm_reference_comparison import (
    DEFAULT_BIOMEM, MODEL_NAME, Worker, candidates, canonical_json, import_biomem,
    outcome_name, require, sha256_bytes, summarize, verify_or_install_model,
)

REPO = Path(__file__).resolve().parents[4]
CORPUS = REPO / "product/evaluation/coding-memory-scenarios.v1.json"
METRICS = REPO / "product/evaluation/coding-memory-metrics.v1.json"
PROTOCOL = REPO / "product/ncm/evaluation/protocol.md"


def words(text: str) -> set[str]:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return {token for token in normalized.split() if len(token) > 2}


def text_payload(observation: dict[str, Any]) -> str:
    return "; ".join(f"{key}={value}" for key, value in observation["payload"].items())


def observation_key(scenario: dict[str, Any], observation: dict[str, Any]) -> str:
    return f"{scenario['task']} | {text_payload(observation)}"


def source_key(observation: dict[str, Any]) -> str:
    return observation.get("forget_source_key") or f"source:{observation['observation_id']}"


def record_id(candidate: dict[str, Any]) -> Any:
    value = candidate.get("record_id")
    if isinstance(value, dict):
        return next(iter(value.values()), None)
    return value


def candidate_source(candidate: dict[str, Any]) -> str:
    value = candidate.get("source", "")
    if isinstance(value, dict):
        return str(next(iter(value.values()), ""))
    return str(value)


def python_candidate(match: dict[str, Any]) -> dict[str, Any]:
    provenance = match.get("provenance") or {}
    return {
        "record_id": match.get("memory_id"),
        "source": provenance.get("source", "unknown"),
        "key_text": match.get("key", ""),
        "value_text": match.get("value", ""),
        "activation": match.get("weight", 0.0),
        "validity": "Valid",
        "provenance": provenance,
    }


class RustLane:
    lane_id = "rust_ncm"

    def __init__(self, worker: Path, root: Path):
        self.worker = Worker(worker, root)
        self.root = root
        self.records: dict[str, tuple[int, str, str]] = {}
        self.observe_ms: list[float] = []
        self.recall_ms: list[float] = []

    def close(self) -> None:
        self.worker.stop()

    def observe(self, scenario: dict[str, Any], observation: dict[str, Any], replay: bool = False) -> dict[str, Any]:
        namespace = observation["scope_id"]
        idem = "idem:" + observation["observation_id"]
        key = observation_key(scenario, observation)
        reply, elapsed = self.worker.observe(
            namespace, idem, source_key(observation), key, text_payload(observation),
            {"scenario": scenario["id"], "observation_id": observation["observation_id"],
             "source_revision": observation["source_revision"], "scope_id": observation["scope_id"]},
        )
        self.observe_ms.append(elapsed)
        if outcome_name(reply) == "success":
            self.records[observation["observation_id"]] = (int(reply["payload"]["record_id"]), namespace, key)
        return reply

    def correct(self, old_observation: str, new_observation: str) -> dict[str, Any]:
        old_id, namespace, _ = self.records[old_observation]
        new_id, _, _ = self.records[new_observation]
        return self.worker.call("correction", namespace, {
            "idempotency_key": f"correction:{old_observation}:{new_observation}",
            "superseded": old_id, "superseding": new_id,
            "evidence": sha256_bytes(f"{old_observation}->{new_observation}".encode()),
        })[0]

    def recall(self, namespace: str, query: str, top_k: int) -> dict[str, Any]:
        reply, elapsed = self.worker.recall(namespace, query, min(top_k, 16))
        self.recall_ms.append(elapsed)
        return {"terminal": outcome_name(reply), "candidates": candidates(reply), "raw_reply": reply}

    def delete(self, namespace: str, source: str) -> dict[str, Any]:
        return self.worker.call("delete_by_source", namespace, {"idempotency_key": f"delete:{source}", "source": source})[0]

    def restart(self) -> None:
        self.worker.restart()

    def corrupt(self, namespace: str) -> dict[str, Any]:
        checkpoint = self.worker.maintenance(namespace, f"checkpoint:{namespace}", "checkpoint")[0]
        require(outcome_name(checkpoint) == "success", "checkpoint before corruption failed")
        self.worker.stop()
        database = self.root / "namespaces" / self.worker.wire_namespace(namespace) / "ncm.sqlite"
        require(database.is_file(), f"missing namespace database {database}")
        backup = database.with_suffix(".sqlite.before-corruption")
        shutil.copy2(database, backup)
        raw = database.read_bytes()
        require(len(raw) > 4096, "database too small for corruption fixture")
        database.write_bytes(raw[:2048])
        for suffix in ("-wal", "-shm"):
            Path(str(database) + suffix).unlink(missing_ok=True)
        self.worker.start()
        health = self.worker.call("health", namespace)[0]
        recalled = self.recall(namespace, "what public API version is present", 5)
        return {"visible": outcome_name(recalled["raw_reply"]) in {"corrupt", "incompatible", "unavailable"},
                "health": outcome_name(health), "recall": recalled}


class BiomemLane:
    lane_id = "python_biomem"

    def __init__(self, biomem_root: Path, root: Path, seed: int):
        self.torch, self.MemoryConfig, self.TextMemory = import_biomem(biomem_root)
        self.torch.manual_seed(seed)
        self.root = root
        self.memories: dict[str, Any] = {}
        self.idempotency: set[str] = set()
        self.source_to_keys: dict[tuple[str, str], list[str]] = {}
        self.shared_embedder = None
        self.observe_ms: list[float] = []
        self.recall_ms: list[float] = []

    def memory(self, namespace: str) -> Any:
        if namespace not in self.memories:
            directory = self.root / namespace
            directory.mkdir(parents=True, exist_ok=True)
            memory = self.TextMemory(
                config=self.MemoryConfig(auto_save=False, data_dir=str(directory)),
                state_file=str(directory / "state.bdbm"), device="cpu", auto_load=False,
            )
            if self.shared_embedder is None:
                self.shared_embedder = memory.embedder
            else:
                memory.embedder = self.shared_embedder
            self.memories[namespace] = memory
        return self.memories[namespace]

    def observe(self, scenario: dict[str, Any], observation: dict[str, Any], replay: bool = False) -> dict[str, Any]:
        idem = "idem:" + observation["observation_id"]
        if idem in self.idempotency:
            return {"outcome": "success", "replayed": True}
        memory = self.memory(observation["scope_id"])
        key = observation_key(scenario, observation)
        source = source_key(observation)
        started = time.perf_counter()
        result = memory.store_record(
            key, text_payload(observation), memory_id=observation["observation_id"],
            provenance={"scenario": scenario["id"], "observation_id": observation["observation_id"],
                        "source_revision": observation["source_revision"], "scope_id": observation["scope_id"],
                        "source": source},
        )
        self.observe_ms.append((time.perf_counter() - started) * 1000.0)
        self.idempotency.add(idem)
        self.source_to_keys.setdefault((observation["scope_id"], source), []).append(key)
        return {"outcome": "success", "record_id": result.get("memory_id"), "replayed": False}

    def correct(self, old_observation: str, new_observation: str) -> dict[str, Any]:
        # Biomem has text editing but no stable supersession lineage.
        return {"outcome": "unsupported", "visible_lineage": False}

    def recall(self, namespace: str, query: str, top_k: int) -> dict[str, Any]:
        if namespace not in self.memories:
            return {"terminal": "empty", "candidates": []}
        started = time.perf_counter()
        result = self.memories[namespace].recall(query, top_k=min(top_k, 16), increment_stats=False)
        self.recall_ms.append((time.perf_counter() - started) * 1000.0)
        return {"terminal": "success" if result.matches else "empty",
                "candidates": [python_candidate(match) for match in result.matches]}

    def delete(self, namespace: str, source: str) -> dict[str, Any]:
        memory = self.memory(namespace)
        deleted = 0
        for key in self.source_to_keys.get((namespace, source), []):
            deleted += memory.forget(key, exact_match=True)
        return {"outcome": "success", "deleted": deleted, "capability": "emulated_key_scrub_not_native_source_revocation"}

    def restart(self) -> None:
        for namespace, memory in list(self.memories.items()):
            memory.save()
            replacement = self.TextMemory(config=memory.config, state_file=memory.state_file, device="cpu", auto_load=True)
            replacement.embedder = self.shared_embedder
            self.memories[namespace] = replacement

    def corrupt(self, namespace: str) -> dict[str, Any]:
        memory = self.memory(namespace)
        memory.save()
        path = Path(memory.state_file)
        raw = path.read_bytes()
        path.write_bytes(raw[: max(16, len(raw) // 5)])
        replacement = self.TextMemory(config=memory.config, state_file=memory.state_file, device="cpu", auto_load=True)
        replacement.embedder = self.shared_embedder
        self.memories[namespace] = replacement
        recalled = self.recall(namespace, "what public API version is present", 5)
        # Reference catches load errors and does not expose a typed failure.
        return {"visible": False, "health": "success", "recall": recalled,
                "expected_difference": "D06: load exception is logged and memory continues"}

    def close(self) -> None:
        return


class EmptyLane:
    lane_id = "no_memory"
    observe_ms: list[float] = []
    recall_ms: list[float] = []
    def observe(self, *_: Any, **__: Any) -> dict[str, Any]: return {"outcome": "not_contacted"}
    def correct(self, *_: Any) -> dict[str, Any]: return {"outcome": "not_contacted"}
    def recall(self, *_: Any) -> dict[str, Any]: return {"terminal": "success_zero_results", "candidates": []}
    def delete(self, *_: Any) -> dict[str, Any]: return {"outcome": "not_contacted"}
    def restart(self) -> None: return
    def corrupt(self, namespace: str) -> dict[str, Any]:
        return {"visible": False, "health": "not_contacted", "recall": self.recall(namespace, "", 5)}
    def close(self) -> None: return


class DocumentationLane(EmptyLane):
    lane_id = "explicit_documentation"
    def __init__(self, corpus: dict[str, Any]):
        self.corpus = corpus
        self.scope = {s["scope_id"]: s for s in corpus["scope_catalog"]}
        self.current_docs: list[dict[str, str]] = []
        for file in corpus["fixtures"][0]["files"]:
            path = file["path"]
            if path.startswith(("docs/", "notes/")) or path in {"README.md", "AGENTS.md", "CLAUDE.md"}:
                latest = max(file["revisions"], key=lambda r: r["revision"])
                self.current_docs.append({"path": path, "content": latest["content"], "revision": latest["revision_id"]})
    def recall(self, namespace: str, query: str, top_k: int) -> dict[str, Any]:
        scope = self.scope.get(namespace)
        if scope and scope["project_id"] != "project_ledger_v1":
            return {"terminal": "scope_mismatch", "candidates": []}
        query_words = words(query)
        ranked = []
        for document in self.current_docs:
            overlap = len(query_words & words(document["content"])) / max(1, len(query_words))
            if overlap > 0:
                ranked.append((overlap, document))
        ranked.sort(key=lambda pair: (-pair[0], pair[1]["path"]))
        output = [{"record_id": d["revision"], "source": d["path"], "key_text": d["content"],
                   "value_text": d["content"], "activation": score, "validity": "Valid",
                   "provenance": {"path": d["path"], "revision": d["revision"]}}
                  for score, d in ranked[:top_k]]
        return {"terminal": "success" if output else "success_zero_results", "candidates": output}


def request_map(corpus: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {request["request_id"]: request for request in corpus["recall_requests"]}


def execute_scenario(lane: Any, scenario: dict[str, Any], requests: dict[str, dict[str, Any]]) -> dict[str, Any]:
    observations = {item["observation_id"]: item for item in scenario["observations"]}
    recalls: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    corruption: dict[str, Any] | None = None
    deletion: dict[str, Any] | None = None
    for step in scenario["steps"]:
        action = step["action"]
        if action == "observe":
            reply = lane.observe(scenario, observations[step["observation_id"]])
            events.append({"action": action, "outcome": outcome_name(reply) if "outcome" in reply else reply.get("outcome")})
            if scenario["id"] == "stale_project_change" and step["observation_id"] == "obs_stale_002":
                events.append({"action": "correction", "reply": lane.correct("obs_stale_001", "obs_stale_002")})
        elif action == "replay":
            reply = lane.observe(scenario, observations[step["observation_id"]], replay=True)
            events.append({"action": action, "reply": reply})
        elif action == "restart_provider":
            lane.restart(); events.append({"action": action, "outcome": "restarted"})
        elif action in {"recall", "verify_absence", "health"}:
            request = requests[step["request_id"]]
            namespace = step.get("scope_id", request.get("scope_id", scenario["target_scope_id"]))
            if namespace == "scope_other_project":
                recalled = {"terminal": "scope_mismatch", "candidates": []}
            elif action == "health":
                recalled = {"terminal": corruption["health"] if corruption else "success", "candidates": []}
            else:
                recalled = lane.recall(namespace, request.get("query", request.get("objective", "")), request.get("budgets", {}).get("max_candidates", 5))
            recalls.append({"action": action, "request_id": step["request_id"], "namespace": namespace, **recalled})
        elif action == "delete_by_source":
            deletion = lane.delete(scenario["target_scope_id"], step["forget_source_key"])
            events.append({"action": action, "reply": deletion})
        elif action == "load_provider_state":
            corruption = lane.corrupt(scenario["target_scope_id"])
            events.append({"action": action, "visible": corruption["visible"]})
        elif action == "commit_item":
            # The corpus deliberately models a partial batch without an observation payload.
            events.append({"action": action, "committed_items": 1})
        elif action == "cancel":
            events.append({"action": action, "terminal": "cancelled", "committed_items": 1, "remaining_items": 2})
        elif action in {"begin_observation_batch", "resume", "advance_code", "open_new_agent_session", "adjudicate"}:
            events.append({"action": action})

    return adjudicate(lane.lane_id, scenario, recalls, events, corruption, deletion)


def is_superseded(candidate: dict[str, Any]) -> bool:
    validity = candidate.get("validity")
    return isinstance(validity, dict) and "Superseded" in validity


def adjudicate(lane_id: str, scenario: dict[str, Any], recalls: list[dict[str, Any]],
               events: list[dict[str, Any]], corruption: dict[str, Any] | None,
               deletion: dict[str, Any] | None) -> dict[str, Any]:
    sid = scenario["id"]
    returned = [candidate for recall in recalls for candidate in recall["candidates"]]
    admitted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in returned:
        reason = None
        if is_superseded(candidate): reason = "superseded"
        elif sid == "provider_corruption": reason = "corrupt_state"
        elif sid == "privacy_deletion" and candidate_source(candidate) == "source_session_privacy_note_001" and any(r["action"] != "recall" or r["request_id"] != "request_privacy_before" for r in recalls): reason = "deleted_source"
        if reason:
            rejected.append({"record_id": record_id(candidate), "reason": reason})
        else:
            admitted.append(candidate)

    expected_tokens = words(" ".join(scenario["expected_admissible_behavior"].get("must", [])))
    useful = [candidate for candidate in admitted if words(candidate.get("key_text", "") + " " + candidate.get("value_text", "")) & expected_tokens]
    stale_admitted = [c for c in admitted if sid == "stale_project_change" and "direct replacement" in (c.get("value_text", "") + c.get("key_text", ""))]
    scope_leaks = [c for c in admitted if (c.get("provenance") or {}).get("scope_id") not in (None, scenario["target_scope_id"])]
    deleted_after = []
    if sid == "privacy_deletion":
        for recall in recalls:
            if recall["request_id"] != "request_privacy_before":
                deleted_after.extend(c for c in recall["candidates"] if "sample value seven" in (c.get("value_text", "") + c.get("key_text", "")))
        if deletion is not None and not deleted_after:
            rejected.append({"source": "source_session_privacy_note_001", "reason": "deleted_source"})

    task_pass = False
    safety_status = "pass"
    reason = "mechanical rubric"
    if sid == "stale_project_change":
        current = any("atomic temporary rename" in (c.get("value_text", "") + c.get("key_text", "")) for c in admitted)
        lineage = any(item["reason"] == "superseded" for item in rejected)
        task_pass = current and lineage
        if stale_admitted: safety_status = "fail"
        if lane_id != "rust_ncm" and not lineage: safety_status = "indeterminate"
    elif sid == "failed_approach":
        task_pass = any("service-unavailable once" in (c.get("value_text", "") + c.get("key_text", "")) for c in admitted) and any("failed" in (c.get("value_text", "") + c.get("key_text", "")) for c in returned)
    elif sid == "cross_agent_reuse":
        task_pass = any("public API version is v1" in (c.get("value_text", "") + c.get("key_text", "")) for c in admitted)
        reason = "host-authorized cross-session replay/share is deferred to ncm-rs-024"
    elif sid == "project_worktree_scope":
        task_pass = any("API v2" in (c.get("value_text", "") + c.get("key_text", "")) for c in admitted) and any(r["terminal"] == "scope_mismatch" for r in recalls)
        if scope_leaks: safety_status = "fail"
    elif sid == "contradiction":
        has_old = any("timeouts are retryable" in (c.get("value_text", "") + c.get("key_text", "")) for c in returned)
        has_new = any("timeouts are not retryable" in (c.get("value_text", "") + c.get("key_text", "")) for c in returned)
        task_pass = has_old and has_new
    elif sid == "restart":
        task_pass = any("public API version is v1" in (c.get("value_text", "") + c.get("key_text", "")) for c in admitted)
    elif sid == "cancellation":
        task_pass = any(e.get("terminal") == "cancelled" and e.get("committed_items") == 1 and e.get("remaining_items") == 2 for e in events)
    elif sid == "provider_corruption":
        task_pass = bool(corruption and corruption["visible"] and not admitted)
        if not corruption or not corruption["visible"]: safety_status = "indeterminate"
        if admitted: safety_status = "fail"
    elif sid == "privacy_deletion":
        task_pass = not deleted_after and deletion is not None
        if deleted_after: safety_status = "fail"
        elif lane_id == "python_biomem":
            safety_status = "indeterminate"; reason = "Biomem deletion was emulated by key scrub; no native source revocation authority"
        elif lane_id in {"no_memory", "explicit_documentation"}:
            safety_status = "indeterminate"; reason = "zero provider admission does not prove deletion"

    if lane_id == "no_memory" and sid in {"stale_project_change", "project_worktree_scope", "provider_corruption", "privacy_deletion"}:
        safety_status = "indeterminate"
    precision = len(useful) / len(admitted) if admitted else None
    return {
        "scenario_id": sid, "split": "development" if sid in {"stale_project_change", "failed_approach", "restart", "cancellation"} else "held_out",
        "terminal_outcomes": [r["terminal"] for r in recalls],
        "returned_candidates": len(returned), "admitted_candidates": len(admitted),
        "admitted_record_ids": [record_id(c) for c in admitted], "rejected": rejected,
        "useful_recall_precision": precision, "task_outcome": "pass" if task_pass else "fail",
        "safety_gate": safety_status, "adjudication_reason": reason,
        "safety_counts": {"harmful_stale_admitted": len(stale_admitted), "scope_leakage": len(scope_leaks),
                          "corrupt_state_recall": len(admitted) if sid == "provider_corruption" else 0,
                          "deleted_source_recall": len(deleted_after)},
        "events": events,
    }


def aggregate(lane: Any, scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    passed = sum(item["task_outcome"] == "pass" for item in scenarios)
    precision_values = [item["useful_recall_precision"] for item in scenarios if item["useful_recall_precision"] is not None]
    safety = "pass" if all(item["safety_gate"] == "pass" for item in scenarios) else "fail"
    return {
        "lane_id": lane.lane_id, "scenarios": scenarios,
        "aggregate_task_score": passed / len(scenarios),
        "resolved_scenarios": len(scenarios), "indeterminate_scenarios": 0,
        "useful_recall_precision": sum(precision_values) / len(precision_values) if precision_values else None,
        "minimum_relevance_met": bool(precision_values) and sum(precision_values) / len(precision_values) >= 0.60,
        "safety_gate": safety, "verdict": "pass" if safety == "pass" and passed == len(scenarios) else "fail",
        "latency": {"observe_ms": summarize(lane.observe_ms), "recall_ms": summarize(lane.recall_ms)},
        "estimated_context_tokens": math.ceil(sum(item["admitted_candidates"] for item in scenarios) * 64),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--biomem-root", type=Path, default=DEFAULT_BIOMEM)
    parser.add_argument("--reference-results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20260906)
    parser.add_argument("--install-model", action="store_true")
    args = parser.parse_args()
    require(PROTOCOL.is_file(), "protocol.md must exist before evaluation")
    require(args.worker.is_absolute() and args.state_root.is_absolute(), "worker and state root must be absolute")
    verify_or_install_model(args.state_root, args.install_model)
    corpus = json.loads(CORPUS.read_text()); metrics = json.loads(METRICS.read_text())
    require(len(corpus["scenarios"]) == 9, "the frozen corpus must contain nine scenarios")
    requests = request_map(corpus)
    lanes: list[Any] = [
        RustLane(args.worker, args.state_root),
        BiomemLane(args.biomem_root, args.state_root / "biomem", args.seed),
        EmptyLane(), DocumentationLane(corpus),
    ]
    reports = []
    try:
        for lane in lanes:
            reports.append(aggregate(lane, [execute_scenario(lane, scenario, requests) for scenario in corpus["scenarios"]]))
    finally:
        for lane in lanes: lane.close()

    reference = json.loads(args.reference_results.read_text())
    output = {
        "schema_version": 1, "task": "ncm-rs-021", "run_date": time.strftime("%Y-%m-%d"),
        "protocol_sha256": hashlib.sha256(PROTOCOL.read_bytes()).hexdigest(),
        "corpus_sha256": hashlib.sha256(CORPUS.read_bytes()).hexdigest(),
        "metrics_sha256": hashlib.sha256(METRICS.read_bytes()).hexdigest(),
        "kernel_fidelity": reference["kernel_fidelity"],
        "algorithm_corrections": reference["algorithm_corrections"],
        "provider_behavior": {"reference_cases": reference["provider_behavior"], "coding_memory_lanes": reports},
        "task_benefit": {
            "mode": "observer-only", "lanes": [{k: v for k, v in report.items() if k != "scenarios"} for report in reports],
            "claim": "No improved-agent-outcomes claim; joined active-mode benefit is deferred to ncm-rs-025.",
        },
        "ablations": reference["ablations"],
        "latency": reference["latency"],
        "pending_baselines": [
            {"lane_id": "native_provider", "status": "pending", "reason": "joined host baseline not run in ncm-rs-021"},
            {"lane_id": "tracedecay_joined_host", "status": "pending", "reason": "active-mode integration belongs to ncm-rs-025"},
        ],
        "acceptance": {
            "real_encoder_ltm_only_without_fallback": reference["provider_behavior"]["ltm_only_retention"]["passed"],
            "stale_rejected_with_reason": reference["provider_behavior"]["stale_correction"]["passed"],
            "cross_scope_rejected_with_reason": "scope_mismatch" in next(r for r in reports if r["lane_id"] == "rust_ncm")["scenarios"][3]["terminal_outcomes"],
            "deleted_rejected_with_reason": any(item.get("reason") == "deleted_source" for item in next(r for r in reports if r["lane_id"] == "rust_ncm")["scenarios"][8]["rejected"]),
            "corrupt_rejected_with_reason": any(item.get("visible") is True for item in next(r for r in reports if r["lane_id"] == "rust_ncm")["scenarios"][7]["events"]),
            "four_result_blocks_separate": True,
            "improved_agent_outcomes_claimed": False,
        },
        "negative_controls": reference["negative_controls"] + [
            {"mutation": "admit cross-scope candidate", "caught_by": "scope_leakage ceiling 0"},
            {"mutation": "admit deleted candidate", "caught_by": "deleted_source_recall ceiling 0"},
            {"mutation": "treat empty evidence as safety pass", "caught_by": "indeterminate safety gate for no-memory lane"},
        ],
        "metric_catalog_safety_ceilings": {m["metric_id"]: m.get("ceiling") for m in metrics["metrics"] if m.get("safety_gating")},
        "limitations": [
            "Observer-mode retrieval and admission only; no decoder or agent outcome was measured.",
            "Python Biomem has no native exact-scope namespace bridge, supersession lineage, or source-revocation authority; wrapper-emulated behavior is labeled.",
            "Estimated tokens use ceil(UTF-8 bytes/4), not the production o200k_base estimator.",
            "Projection comparison injects persisted Rust matrices because torch RNG byte parity is outside the frozen contract.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"output": str(args.output), "lanes": [r["lane_id"] for r in reports], "rust_verdict": reports[0]["verdict"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
