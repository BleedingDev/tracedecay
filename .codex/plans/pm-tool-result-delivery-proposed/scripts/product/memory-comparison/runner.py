"""Prepare/replay the frozen host comparison and join actual delivery evidence.

No built-in provider, host authority, tokenizer, or task model. The downstream
production fixture implements HostFactory; controlled fixtures are test-only.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib
import json
from pathlib import Path
from typing import Protocol

from capture import AttemptTiming, ProcessIdentity, ProcessTreeCapture, capture_directories
from analysis_stats import summarize_attempts
from measurements import populations, summarize_measurements
from performance_fixture import fixed_fixtures
from event_log import JsonlEventSink, event, read_captures
from delivery_evidence import canonical_sections, is_tool_result, validate_tool_result

ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = ROOT / "product/evaluation/host-comparison"
LANES = ("provider:tracedecay.native", "provider:ncm", "no_memory", "explicit_documentation")
HOSTS = ("claude", "codex")
ORDER = ((0, 1, 2, 3), (1, 3, 0, 2), (3, 2, 1, 0), (2, 0, 3, 1))
TOKENIZER = {"identity": "tiktoken.o200k_base", "revision": "tiktoken-rs-0.12"}
LABELS = {"useful", "harmful", "stale", "irrelevant", "unverifiable", "indeterminate", "missing"}
BUDGETS = {"requested_candidates": 8, "effective_candidates": 5,
           "advisory_tokens": 1024, "total_context_tokens": 128000,
           "provider_deadline_ms": 5000, "advisory_slice_ms": 2000}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def text_digest(value):
    return hashlib.sha256(value.encode("utf-8", errors="strict")).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def load_cases():
    """Validate frozen bytes and full recall coverage before any driver import."""
    frozen = json.loads((ARTIFACTS / "input-freeze.json").read_text())
    cases = []
    for name, expected in frozen["heldout_sha256"].items():
        data = (ARTIFACTS / "heldout" / name).read_bytes()
        require(hashlib.sha256(data).hexdigest() == expected, f"frozen input drift: {name}")
        cases.extend(json.loads(data)["cases"])
    require(len(cases) == 18 and sum(len(c["queries"]) for c in cases) == 56,
            "frozen case/query denominator mismatch")
    require(len({c["id"] for c in cases}) == 18, "duplicate case")
    for case in cases:
        queries = [q["id"] for q in case["queries"]]
        calls = [s["query_id"] for s in case["steps"] if s["action"] == "recall"]
        require(sorted(queries) == sorted(calls) and len(set(calls)) == len(calls),
                f"each query must execute exactly once: {case['id']}")
    return cases, frozen


def prepare_plan():
    cases, frozen = load_cases()
    require(digest(fixed_fixtures()) == frozen["performance_fixture_sha256"], "performance fixture changed after freeze")
    schedule = []
    for host in HOSTS:
        for trial in range(3):
            for index, case in enumerate(cases):
                for position in ORDER[(index + trial) % 4]:
                    lane = LANES[position]
                    schedule.append({"case_id": case["id"], "host": host, "trial": trial,
                                     "lane": lane, "case_trial_id": f"{host}/{trial}/{case['id']}/{lane}",
                                     "case_input_sha256": digest(case)})
    return {"format": "tracedecay.host-comparison.plan.v1", "experiment": "host_retrieval_assessment",
            "task_benefit": {"status": "unmeasured", "reason": "no authorized downstream task runner"},
            "freeze": frozen, "cases": cases, "schedule": schedule,
            "planned": {"complete_replays": 24, "case_trials": 432, "query_attempts": 1344},
            "performance_populations": populations(),
            "performance_fixtures": fixed_fixtures(),
            "tokenizer": TOKENIZER, "analysis_seed": 6010317,
            "budgets": copy.deepcopy(BUDGETS)}


def actions_for(case):
    """Stable logical actions, unchanged between hosts/lanes. No authority minted."""
    sources, queries = ({s["id"]: s for s in case["sources"]}, {q["id"]: q for q in case["queries"]})
    return [{"action_id": f"{case['id']}/{i}", "step": copy.deepcopy(step),
             "source": copy.deepcopy(sources.get(step.get("source_id"))),
             "query": copy.deepcopy(queries.get(step.get("query_id")))}
            for i, step in enumerate(case["steps"])]


class HostFixture(Protocol):
    def replay(self, invocation: dict, capture: "Capture") -> dict:
        """Execute all ordered actions through production host controls.

        Handle in-flight cancel within this method and return one outcome per
        logical action. Return actual consumed input, selected build, canonical
        state and final delivery. Never construct grant/checkpoint authority here.
        """
        ...

    def close(self) -> dict:
        """Bounded join of this fixture's owned children; return exit evidence."""
        ...


class HostFactory(Protocol):
    def create(self, invocation: dict) -> HostFixture:
        """Create an independent namespace/process group; no operator state."""
        ...


class Capture:
    """Reviewed capture primitives for direct boundaries inside the real fixture.

    Deliberately does not time replay()/create() as host recall or cold startup.
    Owned PIDs/paths, producer clocks and cleanup evidence come from the fixture.
    """
    timing = staticmethod(AttemptTiming)
    process_identity = staticmethod(ProcessIdentity)
    process_tree = staticmethod(ProcessTreeCapture)
    directories = staticmethod(capture_directories)

    def __init__(self, sink=None, case_trial_id=None):
        self.action_results = []
        self.metadata = None
        self.sink = sink
        self.case_trial_id = case_trial_id

    def record_metadata(self, metadata):
        """Record actual fixture identity before actions; requested pins are not evidence."""
        require(self.metadata is None and not self.action_results, "fixture metadata must precede actions and cannot change")
        self.metadata = copy.deepcopy(metadata)
        if self.sink:
            self.sink(event("case_metadata", self.case_trial_id, metadata=self.metadata))

    def record_action(self, result):
        """Durably append a result before returning control to the next action."""
        require(not any(a["input"]["action_id"] == result["input"]["action_id"] for a in self.action_results),
                "action already recorded; retries need separate diagnostic runs")
        snapshot = copy.deepcopy(result)
        if self.sink:
            self.sink(event("action", self.case_trial_id, result=snapshot))
        self.action_results.append(snapshot)


def replay(plan, metadata, factory, emit):
    validate_metadata(metadata)
    require(plan == prepare_plan(), "plan differs from frozen schedule")
    cases = {case["id"]: case for case in plan["cases"]}
    stop_reason = None
    for scheduled in plan["schedule"]:
        if stop_reason:
            emit(event("case_finish", scheduled["case_trial_id"], result={"case_trial_id": scheduled["case_trial_id"], "status": "unexecuted", "reason": stop_reason}))
            continue
        case = cases[scheduled["case_id"]]
        invocation = {**scheduled, "case": copy.deepcopy(case), "actions": actions_for(case),
                      "budgets": copy.deepcopy(plan["budgets"]), "tokenizer": TOKENIZER,
                      "pins": copy.deepcopy(metadata["pins"])}
        fixture = None
        capture = Capture(emit, scheduled["case_trial_id"])
        result = {"case_trial_id": scheduled["case_trial_id"], "status": "unexecuted"}
        emit(event("case_begin", scheduled["case_trial_id"], invocation=invocation))
        try:
            fixture = factory.create(copy.deepcopy(invocation))
            result = fixture.replay(copy.deepcopy(invocation), capture)
            require(result["case_trial_id"] == scheduled["case_trial_id"], "driver changed trial")
            for row in result.get("actions", []):
                if row.get("status") in ("completed", "censored"):
                    require(row in capture.action_results, "fixture returned an action without a durable event")
        except Exception as error:  # preserve every subsequent planned row, no retry
            result = {**(capture.metadata or {}), "case_trial_id": scheduled["case_trial_id"], "status": "unexecuted",
                      "reason": f"fixture failure: {type(error).__name__}: {error}",
                      "actions": capture.action_results}
            stop_reason = result["reason"]
        finally:
            if fixture is not None:
                try:
                    result["cleanup"] = fixture.close()
                except Exception as error:
                    result["cleanup"] = {"status": "unmeasured", "reason": str(error)}
                if result["cleanup"].get("status") != "completed" or result["cleanup"].get("remaining_owned_children") != 0:
                    stop_reason = "owned fixture cleanup did not prove child completion"
        stop_reason = result.get("stop_remaining_reason") or stop_reason
        emit(event("case_finish", scheduled["case_trial_id"], result=result))


def validate_metadata(metadata):
    for field in ("commands", "source_revision", "dirty_diff_sha256", "build", "hardware", "pins", "mode"):
        require(bool(metadata.get(field)), f"missing execution metadata: {field}")
    require(metadata["mode"] in ("controlled_preparation", "production_host"), "unknown evidence mode")
    for field in ("machine", "ram_bytes", "os", "power_mode", "cpu_load"):
        require(field in metadata["hardware"], f"missing hardware field: {field}")
    for field in ("features", "profile", "rust_version", "runtime_versions"):
        require(field in metadata["build"], f"missing build field: {field}")
    for lane in LANES[:2]:
        pin = metadata["pins"].get(lane, {})
        for field in ("provider_id", "build_sha256", "state_schema", "effective_limits"):
            require(bool(pin.get(field)), f"missing {lane} pin: {field}")
        require(pin["provider_id"] == lane.removeprefix("provider:"), "provider substitution in pin")
    ncm = metadata["pins"]["provider:ncm"]
    require(ncm.get("encoder") == "minilm", "real-model lane requires MiniLM; HashEncoder is not admissible")
    require(bool(ncm.get("model_artifacts_sha256")), "unpinned MiniLM artifacts")
    require(metadata.get("tokenizer") == TOKENIZER, "tokenizer substitution")
    if metadata["mode"] == "production_host":
        def is_sha256(value):
            return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
        require(is_sha256(metadata["dirty_diff_sha256"]), "production diff digest is not SHA-256")
        require(all(is_sha256(pin["build_sha256"]) for pin in metadata["pins"].values()), "production build digest is not SHA-256")
        require(isinstance(ncm["model_artifacts_sha256"], dict) and
                all(is_sha256(value) for value in ncm["model_artifacts_sha256"].values()), "production model digest is not SHA-256")


def missing_row(scheduled, action, reason):
    return {**scheduled, "action_id": action["action_id"], "action": action["step"]["action"],
            "request_id": action["step"].get("query_id"), "phase": "host_request",
            "population": "heldout_retrieval", "warmup": False, "status": "unexecuted",
            "reason": reason, "terminal": None, "provider_contacted": None,
            "successful_completion": False, "expectation": "indeterminate",
            "timing": {"status": "unmeasured", "reason": reason}, "delivery": None}


def validate_delivery(delivery, lane):
    """Reconcile each actual delivered byte with final sections and stage ledger."""
    if is_tool_result(delivery):
        return validate_tool_result(delivery, lane)
    require(delivery.get("representation") in (None, "rendered_text_v1"), "unknown delivery representation")
    require(delivery["tokenizer"] == TOKENIZER, "tokenizer mismatch")
    final = delivery["final_text"]
    require(text_digest(final) == delivery["final_sha256"], "final delivered bytes digest mismatch")
    require("".join(s["text"] for s in delivery["sections"]) == final, "sections do not reconstruct delivery")
    for field in ("final_tokens", "canonical_tokens", "advisory_tokens", "candidate_body_tokens"):
        require(isinstance(delivery.get(field), int) and not isinstance(delivery[field], bool)
                and delivery[field] >= 0, f"missing exact tokenizer count: {field}")
    require(delivery["final_tokens"] <= 128000 and delivery["advisory_tokens"] <= 1024,
            "delivered context exceeds frozen quota")
    ledger = delivery["candidates"]
    require(len({c["candidate_ref"] for c in ledger}) == len(ledger), "duplicate candidate ledger entry")
    stages = ("returned", "admitted", "selected", "packed", "delivered")
    delivered = {c["candidate_ref"]: c for c in ledger if c["delivered"]}
    section_refs = [s["candidate_ref"] for s in delivery["sections"] if s["kind"] == "advisory"]
    require(len(section_refs) == len(set(section_refs)) and set(section_refs) == set(delivered),
            "delivered candidate ledger/sections mismatch")
    for c in ledger:
        require(all(isinstance(c.get(s), bool) for s in stages), "missing candidate stage")
        require(all(not c[b] or c[a] for a, b in zip(stages, stages[1:])), "candidate skipped host stage")
        require(c["delivered"] or bool(c.get("withholding_reason")), "missing withholding reason")
        if c["delivered"]:
            require(text_digest(c["content"]) == c["content_sha256"], "candidate content digest mismatch")
            section = next(s for s in delivery["sections"] if s.get("candidate_ref") == c["candidate_ref"])
            require(c["content"] == section["text"], "advisory section contains bytes outside the annotated candidate body")
            require(bool(c.get("sources")), "missing delivered source attribution")
    canonical = [s for s in delivery["sections"] if s["kind"] == "canonical"]
    require(all(s.get("source_refs") is not None for s in canonical), "missing canonical source attribution")
    require(all(s["kind"] in ("canonical", "advisory", "framing") for s in delivery["sections"]),
            "unknown context section attribution")
    require(all(all(char in " \t\r\n\f" for char in s["text"]) for s in delivery["sections"] if s["kind"] == "framing"),
            "unannotated substantive framing is not permitted")
    if lane == "no_memory":
        require(not delivered, "no-memory lane delivered advisory content")
    if lane == "explicit_documentation":
        for c in delivered.values():
            for source in c["sources"]:
                path = source.get("path", "")
                require(path in ("AGENTS.md", "README.md", "CLAUDE.md") or
                        (path.startswith(("docs/", "notes/")) and ".." not in Path(path).parts),
                        "documentation lane used a non-document source")
                require(source.get("current_revision") is True, "documentation revision is not current")
    return canonical


def validate_case_capture(scheduled, case, captured, metadata):
    require(captured["consumed_case"] == case, "mismatched consumed case input")
    require(captured["budgets"] == BUDGETS, "mismatched requested/effective budgets")
    require(bool(captured.get("projection_sha256")) and bool(captured.get("canonical_start_sha256")),
            "missing canonical projection/starting state")
    require(bool(captured.get("namespace_identity")) and bool(captured.get("physical_paths")),
            "missing isolated namespace/path mapping")
    process = captured.get("process_group", {})
    require(isinstance(process.get("pid"), int) and process["pid"] > 0 and bool(process.get("start_identity")),
            "missing owned process start identity")
    require(captured.get("mode") == metadata["mode"], "fixture mode substitution")
    lane = scheduled["lane"]
    expected = metadata["pins"].get(lane)
    require(captured.get("selected_provider") == expected, "selected provider/build/model substitution")
    require(captured.get("observer_enabled") is False, "primary lane observer interference")
    expected_actions = actions_for(case)
    require(len(captured["actions"]) == len(expected_actions), "missing action rows")
    rows = []
    selected = expected
    for action, result in zip(expected_actions, captured["actions"]):
        if action["step"]["action"] == "switch_provider" and lane.startswith("provider:"):
            role = action["step"]["details"]["target_role"]
            switched_lane = lane if role == "initial_provider" else next(l for l in LANES[:2] if l != lane)
            selected = metadata["pins"][switched_lane]
        require(result["input"] == action, "mismatched or reordered action input")
        row = {**missing_row(scheduled, action, "not captured"), **result,
               **scheduled, "action_id": action["action_id"]}
        require(row["status"] in ("completed", "censored", "unexecuted"), "invalid attempt status")
        if row["status"] != "unexecuted":
            require(row.get("selected_provider") == selected, "per-action selected provider substitution")
        require((row["terminal"] is not None) == (row["status"] == "completed"), "terminal/status mismatch")
        require(isinstance(row.get("provider_contacted"), bool) or row["status"] == "unexecuted",
                "missing provider contact evidence")
        if lane in LANES[2:]:
            require(not row["provider_contacted"], "control lane contacted an advisory provider")
        timing = row["timing"]
        if timing.get("status") == "measured":
            start, end = timing["start_monotonic_ns"], timing["end_monotonic_ns"]
            require(0 <= start <= end and timing["elapsed_ns"] == end - start, "timing boundary mismatch")
            require(timing.get("phase") == row["phase"], "timing phase misattribution")
        if row["status"] == "censored":
            require(timing.get("elapsed_at_cutoff_ns") is not None, "missing censor cutoff")
        if row.get("resources"):
            require(row["resources"]["phase"] == row["phase"], "resource phase misattribution")
            require(row["resources"]["root"] == row["process_group"], "resource process misattribution")
        if row["delivery"] is not None:
            require(action["query"] is not None, "non-recall action delivered context")
            validate_delivery(row["delivery"], lane)
        row["comparison_valid"] = True
        rows.append(row)
    return rows


def retain_invalid_attempts(scheduled, case, captured, reason):
    """Retain measurable failure denominators without awarding invalid evidence."""
    raw = {a.get("input", {}).get("action_id"): a for a in (captured or {}).get("actions", [])}
    rows = []
    for action in actions_for(case):
        row = missing_row(scheduled, action, reason)
        observed = raw.get(action["action_id"], {})
        if observed.get("status") in ("completed", "censored"):
            for field in ("status", "terminal", "provider_contacted", "timing", "phase", "resources", "process_group", "delivery"):
                if field in observed:
                    row[field] = copy.deepcopy(observed[field])
            row["input"] = action
            row["successful_completion"] = observed.get("successful_completion") is True
        row["comparison_valid"] = False
        rows.append(row)
    return rows


def assemble(plan, captures, metadata):
    """Invalid comparisons retain raw captures and all scheduled denominators."""
    require(plan == prepare_plan(), "plan differs from frozen schedule")
    validate_metadata(metadata)
    require(len({c["case_trial_id"] for c in captures}) == len(captures), "duplicate case capture/retry")
    by_id = {c["case_trial_id"]: c for c in captures}
    require(set(by_id) <= {s["case_trial_id"] for s in plan["schedule"]}, "unscheduled capture")
    cases = {case["id"]: case for case in plan["cases"]}
    rows, blocks, invalid, namespaces, processes = [], {}, [], set(), set()
    for scheduled in plan["schedule"]:
        case = cases[scheduled["case_id"]]
        captured = by_id.get(scheduled["case_trial_id"])
        reason = captured.get("reason", "fixture did not execute") if captured else "no fixture capture supplied"
        if captured and captured.get("status") != "unexecuted":
            try:
                trial_rows = validate_case_capture(scheduled, case, captured, metadata)
                namespace = captured["namespace_identity"]
                process = (captured["process_group"]["pid"], captured["process_group"]["start_identity"])
                require(namespace not in namespaces and process not in processes, "mutable namespace/process group reused between case trials")
                namespaces.add(namespace); processes.add(process)
                key = (scheduled["host"], scheduled["trial"], scheduled["case_id"])
                blocks.setdefault(key, []).append((scheduled, captured, trial_rows))
            except (ValueError, KeyError, TypeError) as error:
                reason = f"invalid comparison evidence: {error}"
                invalid.append({"case_trial_id": scheduled["case_trial_id"], "reason": reason})
                trial_rows = retain_invalid_attempts(scheduled, case, captured, reason)
        else:
            trial_rows = retain_invalid_attempts(scheduled, case, captured, reason)
        rows.extend(trial_rows)
    for key, group in blocks.items():
        signatures, canonical_by_request = [], {}
        for _, captured, trial_rows in group:
            signatures.append(digest({"projection": captured["projection_sha256"],
                                      "canonical_start": captured["canonical_start_sha256"]}))
            for row in trial_rows:
                if row["action"] == "recall" and row["delivery"]:
                    canonical_by_request.setdefault(row["request_id"], set()).add(digest(
                        canonical_sections(row["delivery"])))
        if len(set(signatures)) != 1 or any(len(values) > 1 for values in canonical_by_request.values()):
            for scheduled, _, trial_rows in group:
                invalid.append({"case_trial_id": scheduled["case_trial_id"], "reason": "canonical input/output drift"})
                for row in trial_rows:
                    row["comparison_valid"] = False
    queries = [r for r in rows if r["action"] == "recall"]
    strata = []
    for host in HOSTS:
        for lane in LANES:
            selected = [r for r in queries if r["host"] == host and r["lane"] == lane]
            strata.append({"host": host, "lane": lane, "planned_case_trials": 54,
                           "query_attempts": summarize_attempts(selected),
                           "query_expectations": {"status": "unmeasured", "scheduled_denominator": len(selected),
                                                  "reason": "independent frozen-source adjudication required"}})
    return {"format": "tracedecay.host-comparison.capture.v1", "mode": metadata["mode"],
            "experiment": "host_retrieval_assessment", "task_benefit": plan["task_benefit"],
            "metadata": metadata, "plan": plan, "raw_case_captures": captures, "measurements": rows,
            "invalid_comparisons": invalid, "strata": strata,
            "status": "invalid" if invalid else "captured_pending_adjudication" if len(captures) == 432
            and all(r["status"] == "completed" for r in queries) else "incomplete",
            "metric_report": {"status": "unmeasured", "reason": "run the existing Rust evaluator after source-grounded adjudication"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "assemble", "replay", "measurements"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--captures", type=Path)
    parser.add_argument("--driver", help="downstream module:factory callable; no built-in host driver")
    args = parser.parse_args()
    plan = prepare_plan()
    if args.command == "prepare":
        result = plan
    else:
        require(args.metadata is not None, "--metadata required")
        metadata = json.loads(args.metadata.read_text())
        validate_metadata(metadata)
        if args.command == "replay":
            require(bool(args.driver), "--driver required; production host connection is downstream")
            module, symbol = args.driver.split(":", 1)
            validate_metadata(metadata)
            factory = getattr(importlib.import_module(module), symbol)(metadata)
            with args.output.open("x") as stream:
                replay(plan, metadata, factory, JsonlEventSink(stream))
            return
        require(args.captures is not None, "--captures required")
        captures = read_captures(args.captures)
        result = ({"metadata": metadata, "measurements": summarize_measurements(captures)}
                  if args.command == "measurements" else assemble(plan, captures, metadata))
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2, ensure_ascii=False, allow_nan=False)
        stream.write("\n")


if __name__ == "__main__":
    main()
