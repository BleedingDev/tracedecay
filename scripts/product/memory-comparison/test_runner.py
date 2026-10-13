"""Discriminating preparation-only controls. Never execute held-out providers."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import adjudicate
import join_reports
import runner
from analysis_stats import paired_block_difference, paired_ratio_difference, summarize_attempts
from measurements import planned_attempts, populations, summarize_measurements
from performance_fixture import fixed_fixtures, record
from event_log import JsonlEventSink, event, read_captures


def metadata():
    return {"commands": [["controlled-fixture"]], "source_revision": "controlled",
            "dirty_diff_sha256": "controlled", "mode": "controlled_preparation",
            "build": {"features": [], "profile": "test", "rust_version": "unmeasured", "runtime_versions": {}},
            "hardware": {"machine": "controlled", "ram_bytes": None, "os": "controlled", "power_mode": None, "cpu_load": None},
            "tokenizer": runner.TOKENIZER,
            "pins": {lane: {"provider_id": lane.removeprefix("provider:"), "build_sha256": "controlled",
                              "state_schema": "controlled", "effective_limits": {"maximum_candidates": 5},
                              **({"encoder": "minilm", "model_artifacts_sha256": {"weights": "controlled"}}
                                 if lane == "provider:ncm" else {})} for lane in runner.LANES[:2]}}


def controlled_plan():
    """Independent development truth: no simulated held-out provider answers."""
    plan = runner.prepare_plan()
    replacement = {}
    cases = []
    for index, original in enumerate(plan["cases"]):
        case_id = f"development-{index}"
        source = {"id": f"{case_id}-source", "revision": "dev-r1", "lineage_id": f"{case_id}-lineage",
                  "origin": {"project": "development", "repository": "development-repo", "worktree": "main", "branch": "main", "session": "session-a"},
                  "content": "Development transport uses a bounded local queue with seven slots."}
        queries = [{"id": f"{case_id}-q-{q}", "text": "What is the development queue capacity?",
                    "expected_terminal": "success", "expected_useful_source_ids": [source["id"]],
                    "forbidden_source_ids": [], "required_facts": ["The development queue has seven slots."],
                    "prohibited_claims": ["The development queue is unbounded."]}
                   for q in range(len(original["queries"]))]
        case = {"id": case_id, "family": "development_control", "sources": [source], "queries": queries,
                "steps": [{"action": "observe", "source_id": source["id"]}] +
                         [{"action": "recall", "query_id": q["id"]} for q in queries]}
        cases.append(case)
        replacement[original["id"]] = case
    for scheduled in plan["schedule"]:
        case = replacement[scheduled["case_id"]]
        scheduled["case_trial_id"] = scheduled["case_trial_id"].replace(scheduled["case_id"], case["id"])
        scheduled["case_id"] = case["id"]
        scheduled["case_input_sha256"] = runner.digest(case)
    plan["cases"] = cases
    return plan


def candidate(case, query, *, delivered=True):
    source = next(s for s in case["sources"] if s["id"] == query["expected_useful_source_ids"][0])
    return {"candidate_ref": "candidate/1", "content": source["content"],
            "content_sha256": runner.text_digest(source["content"]), "returned": True, "admitted": delivered,
            "selected": delivered, "packed": delivered, "delivered": delivered,
            "withholding_reason": None if delivered else "host_scope_rejection", "scope_match": True,
            "provenance": "available", "sources": [{"source_id": source["id"],
               **{k: source[k] for k in ("revision", "lineage_id", "origin")},
               "content_sha256": runner.text_digest(source["content"])}]}


def delivery(candidates=()):
    sections = [{"kind": "canonical", "text": "canonical stable\n", "source_refs": ["canonical/source"]}]
    sections.extend({"kind": "advisory", "text": c["content"], "candidate_ref": c["candidate_ref"]}
                    for c in candidates if c["delivered"])
    text = "".join(s["text"] for s in sections)
    # Synthetic counts are explicitly controlled; Rust tests exercise real BPE.
    return {"tokenizer": runner.TOKENIZER, "final_text": text, "final_sha256": runner.text_digest(text),
            "sections": sections, "candidates": list(candidates), "final_tokens": 50,
            "canonical_tokens": 4, "advisory_tokens": 46 if any(c["delivered"] for c in candidates) else 0,
            "candidate_body_tokens": 46 if any(c["delivered"] for c in candidates) else 0}


def captured_case(plan, schedule, *, admitted=True):
    case = next(c for c in plan["cases"] if c["id"] == schedule["case_id"])
    actions = []
    for action in runner.actions_for(case):
        query = action["query"]
        d = delivery([candidate(case, query, delivered=admitted)]) if query else None
        actions.append({"input": action, "action": action["step"]["action"], "request_id": query["id"] if query else None,
                        "selected_provider": copy.deepcopy(metadata()["pins"][schedule["lane"]]),
                        "status": "completed", "terminal": "success", "provider_contacted": True,
                        "successful_completion": True, "phase": "host_request", "expectation": "pass",
                        "timing": {"phase": "host_request", "status": "measured", "start_monotonic_ns": 10,
                                   "end_monotonic_ns": 1010, "elapsed_ns": 1000}, "delivery": d})
    return {"case_trial_id": schedule["case_trial_id"], "status": "completed", "mode": "controlled_preparation",
            "namespace_identity": schedule["case_trial_id"], "physical_paths": {"fixture": "controlled-only"},
            "process_group": {"pid": 1, "start_identity": schedule["case_trial_id"]},
            "consumed_case": copy.deepcopy(case), "budgets": copy.deepcopy(plan["budgets"]),
            "selected_provider": copy.deepcopy(metadata()["pins"][schedule["lane"]]),
            "observer_enabled": False, "projection_sha256": "same-logical-projection",
            "canonical_start_sha256": "same-start-state", "actions": actions}


def annotations_for(captured, case, label="useful"):
    annotations = []
    for query in case["queries"]:
        source = next(s for s in case["sources"] if s["id"] == query["expected_useful_source_ids"][0])
        annotations.append({"case_trial_id": captured["case_trial_id"], "request_id": query["id"], "candidate_ref": "candidate/1",
                            "label": label, "reason": "independent development truth", "reviewer_id": "blinded-reviewer",
                            "provider_blinded": True, "prohibited_claims_absent": True,
                            "fact_evidence": [{"fact_index": 0, "source_id": source["id"],
                                               "source_quote": source["content"], "delivered_quote": source["content"]}]})
    return annotations


class RunnerIntegrityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frozen_plan = runner.prepare_plan()
        cls.plan = controlled_plan()

    def setUp(self):
        patched = patch("runner.prepare_plan", return_value=copy.deepcopy(self.plan))
        patched.start()
        self.addCleanup(patched.stop)

    def test_frozen_schedule_keeps_all_queries_and_rotation(self):
        plan = self.frozen_plan
        self.assertEqual(432, len(plan["schedule"]))
        count = sum(len(next(c for c in plan["cases"] if c["id"] == s["case_id"])["queries"]) for s in plan["schedule"])
        self.assertEqual(1344, count)
        for host in runner.HOSTS:
            for lane in runner.LANES:
                rows = [s for s in plan["schedule"] if s["host"] == host and s["lane"] == lane]
                self.assertEqual(54, len(rows))
        self.assertEqual("provider:ncm", plan["schedule"][4]["lane"])

    def test_source_input_drift_is_invalid_and_retained(self):
        scheduled = self.plan["schedule"][0]
        captured = captured_case(self.plan, scheduled)
        captured["consumed_case"]["queries"][0]["text"] = "easier query"
        report = runner.assemble(self.plan, [captured], metadata())
        self.assertEqual("invalid", report["status"])
        self.assertIn("mismatched consumed", report["invalid_comparisons"][0]["reason"])
        self.assertEqual([captured], report["raw_case_captures"])

    def test_real_encoder_substitution_rejected(self):
        meta = metadata()
        meta["pins"]["provider:ncm"]["encoder"] = "hash"
        with self.assertRaisesRegex(ValueError, "MiniLM"):
            runner.validate_metadata(meta)
        scheduled = self.plan["schedule"][0]
        captured = captured_case(self.plan, scheduled)
        captured["selected_provider"]["provider_id"] = "ncm"
        with self.assertRaisesRegex(ValueError, "substitution"):
            runner.validate_case_capture(scheduled, self.plan["cases"][0], captured, metadata())

    def test_successful_backend_rejected_by_host_cannot_receive_annotation(self):
        scheduled = self.plan["schedule"][0]
        captured = captured_case(self.plan, scheduled, admitted=False)
        report = runner.assemble(self.plan, [captured], metadata())
        q = self.plan["cases"][0]["queries"][0]
        annotations = [{"case_trial_id": scheduled["case_trial_id"], "request_id": q["id"],
                        "candidate_ref": "candidate/1", "label": "useful", "reason": "backend answered"}]
        with self.assertRaisesRegex(ValueError, "did not deliver"):
            adjudicate.metric_inputs(report, annotations)

    def test_missing_cases_are_unmeasured_and_never_disappear(self):
        report = runner.assemble(self.plan, [], metadata())
        self.assertEqual("incomplete", report["status"])
        self.assertEqual(1344, sum(s["query_attempts"]["counts"]["unexecuted"] for s in report["strata"]))
        bundles = adjudicate.metric_inputs(report, [])
        self.assertEqual(24, len(bundles))
        self.assertEqual(1344, sum(len(b["input"]["recalls"]) for b in bundles))
        self.assertTrue(all(s["context_tokens"]["kind"] == "unmeasured" for b in bundles for s in b["input"]["record"]["scenarios"]))

    def test_zero_admission_has_no_nonvacuous_safety_proof(self):
        report = runner.assemble(self.plan, [captured_case(self.plan, self.plan["schedule"][0], admitted=False)], metadata())
        bundle = adjudicate.metric_inputs(report, [])[0]
        scenario = bundle["input"]["record"]["scenarios"][0]
        self.assertEqual([], scenario["candidates"])
        self.assertIn({"check_id": "nonvacuous_safety", "outcome": "indeterminate"}, scenario["rubric_checks"])
        self.assertEqual("fail", scenario["task_outcome"]["kind"])

    def test_canonical_output_drift_blocks_provider_delta(self):
        first = captured_case(self.plan, self.plan["schedule"][0])
        second = captured_case(self.plan, self.plan["schedule"][1])
        second["canonical_start_sha256"] = "different-start"
        report = runner.assemble(self.plan, [first, second], metadata())
        self.assertTrue(any(i["reason"] == "canonical input/output drift" for i in report["invalid_comparisons"]))

    def test_exact_bytes_and_stage_transitions(self):
        case = self.plan["cases"][0]
        c = candidate(case, case["queries"][0])
        d = delivery([c])
        d["candidates"][0]["packed"] = False
        with self.assertRaisesRegex(ValueError, "stage"):
            runner.validate_delivery(d, "provider:ncm")
        d = delivery([candidate(case, case["queries"][0])])
        d["final_text"] += "undisclosed content"
        with self.assertRaisesRegex(ValueError, "digest"):
            runner.validate_delivery(d, "provider:ncm")

    def test_advisory_suffix_and_substantive_framing_cannot_escape_annotation(self):
        case = self.plan["cases"][0]
        for as_framing in (False, True):
            d = delivery([candidate(case, case["queries"][0])])
            harmful = " The development queue is unbounded."
            if as_framing:
                d["sections"].append({"kind": "framing", "text": harmful})
            else:
                d["sections"][-1]["text"] += harmful
            d["final_text"] = "".join(s["text"] for s in d["sections"])
            d["final_sha256"] = runner.text_digest(d["final_text"])
            with self.assertRaisesRegex(ValueError, "annotated|framing"):
                runner.validate_delivery(d, "provider:ncm")
        d = delivery([candidate(case, case["queries"][0])])
        d["sections"].append({"kind": "framing", "text": "\n\t "})
        d["final_text"] += "\n\t "
        d["final_sha256"] = runner.text_digest(d["final_text"])
        runner.validate_delivery(d, "provider:ncm")

    def test_stale_metrics_are_rejected_after_annotation_or_delivery_changes(self):
        captured = captured_case(self.plan, self.plan["schedule"][0])
        annotations = annotations_for(captured, self.plan["cases"][0])
        report = runner.assemble(self.plan, [captured], metadata())
        original = adjudicate.metric_inputs(report, annotations)
        self.assertEqual(original, adjudicate.metric_inputs(report, annotations))
        stale = {"host": original[0]["host"], "lane": original[0]["lane"], "trial": original[0]["trial"],
                 "report": {"assessment": "host_retrieval_assessment", "task_benefit": "unmeasured",
                            "metrics": {"provider": original[0]["input"]["record"]["provider"],
                                        "scenarios": [{"scenario_id": s["scenario_id"]} for s in original[0]["input"]["record"]["scenarios"]]}}}
        changed_annotations = copy.deepcopy(annotations)
        changed_annotations[0]["reviewer_id"] = "different-blinded-reviewer"
        revised = adjudicate.metric_inputs(report, changed_annotations)
        self.assertNotEqual(original[0]["input"]["record"]["provider"], revised[0]["input"]["record"]["provider"])
        with self.assertRaisesRegex(ValueError, "identity substitution"):
            join_reports.join(report, revised, [stale])
        changed_capture = copy.deepcopy(captured)
        d = next(a["delivery"] for a in changed_capture["actions"] if a["delivery"])
        d["candidates"][0]["content"] += "\n"
        d["candidates"][0]["content_sha256"] = runner.text_digest(d["candidates"][0]["content"])
        d["sections"][-1]["text"] = d["candidates"][0]["content"]
        d["final_text"] = "".join(s["text"] for s in d["sections"])
        d["final_sha256"] = runner.text_digest(d["final_text"])
        changed_report = runner.assemble(self.plan, [changed_capture], metadata())
        revised = adjudicate.metric_inputs(changed_report, annotations)
        self.assertNotEqual(original[0]["input"]["record"]["provider"], revised[0]["input"]["record"]["provider"])
        with self.assertRaisesRegex(ValueError, "identity substitution"):
            join_reports.join(changed_report, revised, [stale])

    def test_source_digest_changed_cannot_earn_useful_credit(self):
        case = self.plan["cases"][0]
        query = case["queries"][0]
        c = candidate(case, query)
        c["sources"][0]["content_sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "source-grounded"):
            adjudicate.evidence_for(c, query, case, {"label": "useful", "reason": "test", "reviewer_id": "blind-1", "provider_blinded": True})

    def test_positive_source_grounded_control_discriminates_missing_labels(self):
        captured = captured_case(self.plan, self.plan["schedule"][0])
        report = runner.assemble(self.plan, [captured], metadata())
        good = adjudicate.metric_inputs(report, annotations_for(captured, self.plan["cases"][0]))[0]
        self.assertEqual("pass", good["input"]["record"]["scenarios"][0]["task_outcome"]["kind"])
        unresolved = adjudicate.metric_inputs(report, [])[0]
        self.assertEqual("indeterminate", unresolved["input"]["record"]["scenarios"][0]["task_outcome"]["kind"])

    def test_only_useful_eligible_facts_cover_required_query_facts(self):
        captured = captured_case(self.plan, self.plan["schedule"][0])
        report = runner.assemble(self.plan, [captured], metadata())
        for label in ("irrelevant", "unverifiable"):
            annotations = annotations_for(captured, self.plan["cases"][0], label)
            bundle = adjudicate.metric_inputs(report, annotations)[0]
            self.assertTrue(all(q["outcome"] != "pass" for q in bundle["query_assessments"] if q["case_id"] == self.plan["cases"][0]["id"]))

    def test_challenge_witnesses_use_their_own_eligible_useful_adjudication(self):
        plan = copy.deepcopy(self.plan)
        case = plan["cases"][0]
        target = case["sources"][0]
        unaffected = {**copy.deepcopy(target), "id": "development-unaffected", "lineage_id": "unaffected-lineage"}
        case["sources"].append(unaffected)
        for query in case["queries"][1:]:
            query["expected_useful_source_ids"] = [unaffected["id"]]
        case["steps"] = [{"action": "observe", "source_id": target["id"]},
                         {"action": "observe", "source_id": unaffected["id"]},
                         {"action": "recall", "query_id": case["queries"][0]["id"]},
                         {"action": "delete_source", "source_id": target["id"], "details": {"selector": {"lineage_id": target["lineage_id"]}}},
                         *[{"action": "recall", "query_id": q["id"]} for q in case["queries"][1:]]]
        with patch("runner.prepare_plan", return_value=plan):
            for witness_query, label, scope in ((0, "useful", True), (0, "irrelevant", True),
                                                (1, "irrelevant", True), (1, "unverifiable", True), (1, "useful", False)):
                captured = captured_case(plan, plan["schedule"][0])
                captured["actions"][3]["control_evidence"] = {
                    "exercised": True, "verified": True, "evidence_refs": ["controlled-physical-delete"],
                    "target_source_ids": [target["id"]], "positive_action_id": f"{case['id']}/2",
                    "unaffected_action_id": f"{case['id']}/4", "deletion_fence_ref": "controlled-fence", "derived_state_erased": True}
                annotations = annotations_for(captured, case)
                annotations[witness_query]["label"] = label
                witness = next(a for a in captured["actions"] if a["request_id"] == case["queries"][witness_query]["id"])
                witness["delivery"]["candidates"][0]["scope_match"] = scope
                report = runner.assemble(plan, [captured], metadata())
                bundle = adjudicate.metric_inputs(report, annotations)[0]
                checks = bundle["input"]["record"]["scenarios"][0]["rubric_checks"]
                actual = next(c["outcome"] for c in checks if c["check_id"] == "challenge_evidence")
                self.assertEqual("pass" if label == "useful" and scope else "fail", actual)

    def test_durable_action_events_survive_owned_process_crash_during_action_or_cleanup(self):
        script = r'''
import json, os, signal, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import runner
from event_log import JsonlEventSink
from test_runner import captured_case, metadata
plan=json.loads(Path(sys.argv[2]).read_text())
runner.prepare_plan=lambda: plan
result=captured_case(plan, plan["schedule"][0])
class Fixture:
    def replay(self, invocation, capture):
        capture.record_metadata({k:v for k,v in result.items() if k not in ("actions","status","case_trial_id")})
        for index, row in enumerate(result["actions"]):
            if sys.argv[4]=="later_action" and index==2:
                sink.stream.write('{"event":')
                sink.stream.flush()
                os.fsync(sink.stream.fileno())
                os._exit(23)
            capture.record_action(row)
        return result
    def close(self):
        os.kill(os.getpid(), signal.SIGKILL)
class Factory:
    def create(self, invocation): return Fixture()
with Path(sys.argv[3]).open("x") as stream:
    sink=JsonlEventSink(stream)
    runner.replay(plan, metadata(), Factory(), sink)
'''
        with tempfile.TemporaryDirectory(prefix="comparison-crash-") as folder:
            root = Path(folder)
            plan_path = root / "plan.json"
            plan_path.write_text(json.dumps(self.plan))
            for trigger, expected_queries in (("later_action", 1), ("cleanup", len(self.plan["cases"][0]["queries"]))):
                output = root / f"{trigger}.jsonl"
                child = subprocess.run([sys.executable, "-S", "-c", script, str(Path(__file__).parent.resolve()),
                                        str(plan_path), str(output), trigger], capture_output=True, timeout=10)
                self.assertEqual(23 if trigger == "later_action" else -9, child.returncode, child.stderr.decode())
                captures = read_captures(output)
                self.assertEqual(1, len(captures))
                self.assertEqual(self.plan["schedule"][0]["case_trial_id"], captures[0]["namespace_identity"])
                self.assertEqual("unmeasured", captures[0]["cleanup"]["status"])
                if trigger == "later_action":
                    self.assertGreater(captures[0]["stream_recovery"]["unterminated_final_line_bytes"], 0)
                report = runner.assemble(self.plan, captures, metadata())
                counts = report["strata"][0]["query_attempts"]["counts"]
                self.assertEqual(expected_queries, counts["attempted"])
                self.assertEqual(expected_queries, counts["completed_success"])
                self.assertEqual(168 - expected_queries, counts["unexecuted"])
                self.assertEqual(expected_queries, report["strata"][0]["query_attempts"]["completed_terminal_latency_ns"]["n"])

    def test_event_recovery_rejects_complete_corruption_and_unbounded_tail(self):
        with tempfile.TemporaryDirectory(prefix="comparison-tail-") as folder:
            path = Path(folder) / "events.jsonl"
            path.write_bytes(b"not-json\n")
            with self.assertRaises(json.JSONDecodeError):
                read_captures(path)
            path.write_bytes(b"x" * 129)
            with patch("event_log.MAX_EVENT_LINE_BYTES", 128):
                with self.assertRaisesRegex(ValueError, "bounded"):
                    read_captures(path)

    def test_wrong_scope_and_stale_delivery_fail_query_rubric(self):
        for label, scope in (("stale", True), ("useful", False)):
            captured = captured_case(self.plan, self.plan["schedule"][0])
            for row in captured["actions"]:
                if row["delivery"]:
                    row["delivery"]["candidates"][0]["scope_match"] = scope
            report = runner.assemble(self.plan, [captured], metadata())
            bundle = adjudicate.metric_inputs(report, annotations_for(captured, self.plan["cases"][0], label))[0]
            self.assertEqual("fail", bundle["input"]["record"]["scenarios"][0]["task_outcome"]["kind"])

    def test_deleted_lineage_resurrected_after_replay_is_counted(self):
        plan = copy.deepcopy(self.plan)
        case = plan["cases"][0]
        source = case["sources"][0]
        case["steps"].insert(2, {"action": "delete_source", "source_id": source["id"],
                                  "details": {"selector": {"lineage_id": source["lineage_id"]}}})
        case["steps"].insert(3, {"action": "replay"})
        with patch("runner.prepare_plan", return_value=plan):
            captured = captured_case(plan, plan["schedule"][0])
            report = runner.assemble(plan, [captured], metadata())
            bundle = adjudicate.metric_inputs(report, annotations_for(captured, case))[0]
        candidates = bundle["input"]["record"]["scenarios"][0]["candidates"]
        self.assertFalse(candidates[0]["contains_forgotten_source"])
        self.assertTrue(candidates[-1]["contains_forgotten_source"])
        self.assertEqual("fail", bundle["input"]["record"]["scenarios"][0]["task_outcome"]["kind"])

    def test_ignored_cancellation_never_proves_cleanup(self):
        rows = [{"action": "cancel", "status": "completed", "control_evidence": {
            "evidence_refs": ["physical-trigger"], "exercised": True, "verified": True,
            "cancellation_completed": False, "released_permits": True, "remaining_cancelled_children": 0}}]
        self.assertEqual("fail", adjudicate.challenge_verdict({}, rows))

    def test_invalid_input_still_retains_attempted_failure_population(self):
        captured = captured_case(self.plan, self.plan["schedule"][0])
        captured["consumed_case"]["sources"][0]["content"] = "different input"
        report = runner.assemble(self.plan, [captured], metadata())
        counts = report["strata"][0]["query_attempts"]["counts"]
        self.assertEqual(len(self.plan["cases"][0]["queries"]), counts["attempted"])
        self.assertTrue(report["invalid_comparisons"])

    def test_resource_and_timing_phase_attribution(self):
        scheduled = self.plan["schedule"][0]
        captured = captured_case(self.plan, scheduled)
        captured["actions"][0]["timing"]["phase"] = "kernel"
        with self.assertRaisesRegex(ValueError, "phase"):
            runner.validate_case_capture(scheduled, self.plan["cases"][0], captured, metadata())
        captured = captured_case(self.plan, scheduled)
        captured["actions"][0]["resources"] = {"phase": "cold_start"}
        with self.assertRaisesRegex(ValueError, "resource phase"):
            runner.validate_case_capture(scheduled, self.plan["cases"][0], captured, metadata())


class EventProtocolIntegrityTests(unittest.TestCase):
    def test_durable_metadata_and_invocation_roundtrip_and_missing_fields_are_preserved(self):
        recorded = {"canonical_start_sha256": "canonical-A", "projection_sha256": "projection-A",
                    "namespace_identity": "namespace-A", "physical_paths": {"store": "owned-A"},
                    "process_group": {"pid": 101, "start_identity": "start-A"},
                    "build": {"revision": "build-A"}, "mode": "controlled_preparation",
                    "selected_provider": {"provider_id": "native", "build_sha256": "build-A"}}
        invocation = {"case_trial_id": "development", "case": {"id": "case-A"}, "lane": "provider:native"}
        for include_final_fields in (False, True):
            final = {"case_trial_id": "development", "status": "completed", "actions": [],
                     "cleanup": {"status": "completed", "remaining_owned_children": 0}}
            if include_final_fields:
                final.update(copy.deepcopy(recorded))
                final["requested_invocation"] = copy.deepcopy(invocation)
            with tempfile.TemporaryDirectory(prefix="comparison-metadata-") as folder:
                path = Path(folder) / "events.jsonl"
                with path.open("x") as stream:
                    sink = JsonlEventSink(stream)
                    sink(event("case_begin", "development", invocation=invocation))
                    sink(event("case_metadata", "development", metadata=recorded))
                    sink(event("case_finish", "development", result=final))
                actual = read_captures(path)[0]
            for field, value in recorded.items():
                self.assertEqual(value, actual[field])
            self.assertEqual(invocation, actual["requested_invocation"])
            self.assertEqual(final["cleanup"], actual["cleanup"])

    def test_final_metadata_cannot_rewrite_any_durably_recorded_field(self):
        recorded = {"canonical_start_sha256": "canonical-A", "projection_sha256": "projection-A",
                    "namespace_identity": "namespace-A", "physical_paths": {"store": "owned-A"},
                    "process_group": {"pid": 101, "start_identity": "start-A"},
                    "build": {"revision": "build-A"}, "mode": "controlled_preparation",
                    "selected_provider": {"provider_id": "native", "build_sha256": "build-A"}}
        for field in recorded:
            final = {**copy.deepcopy(recorded), "case_trial_id": "development", "status": "completed", "actions": []}
            final[field] = {"changed": "B"} if isinstance(recorded[field], dict) else "B"
            with tempfile.TemporaryDirectory(prefix="comparison-rewrite-") as folder:
                path = Path(folder) / "events.jsonl"
                with path.open("x") as stream:
                    sink = JsonlEventSink(stream)
                    sink(event("case_metadata", "development", metadata=recorded))
                    sink(event("case_finish", "development", result=final))
                with self.assertRaisesRegex(ValueError, f"durable metadata field: {field}"):
                    read_captures(path)

    def test_final_requested_invocation_cannot_rewrite_case_begin(self):
        original = {"case_trial_id": "development", "case": {"id": "case-A"}, "lane": "provider:native"}
        final = {"case_trial_id": "development", "status": "completed", "actions": [],
                 "requested_invocation": {**original, "case": {"id": "case-B"}}}
        with tempfile.TemporaryDirectory(prefix="comparison-invocation-") as folder:
            path = Path(folder) / "events.jsonl"
            with path.open("x") as stream:
                sink = JsonlEventSink(stream)
                sink(event("case_begin", "development", invocation=original))
                sink(event("case_finish", "development", result=final))
            with self.assertRaisesRegex(ValueError, "durable requested invocation"):
                read_captures(path)


class FrozenChallengeTargetTests(unittest.TestCase):
    def control(self, action, source_id=None, selector=None):
        case = {"sources": [{"id": "A-r1", "lineage_id": "A", "revision": "r1"},
                            {"id": "A-r2", "lineage_id": "A", "revision": "r2"},
                            {"id": "B", "lineage_id": "B", "revision": "r1"},
                            {"id": "C", "lineage_id": "C", "revision": "r1"}]}
        step = {"action": action}
        if source_id:
            step["source_id"] = source_id
        if selector is not None:
            step["details"] = {"selector": selector}
        receipt = {"exercised": True, "verified": True, "evidence_refs": ["development-control"],
                   "target_source_ids": [source_id] if source_id else ["B"], "positive_action_id": "before",
                   "unaffected_action_id": "after", "deletion_fence_ref": "development-fence", "derived_state_erased": True,
                   "before_actual_sha256": "original", "after_actual_sha256": "corrupt",
                   "before_claimed_sha256": "original", "after_claimed_sha256": "original"}
        rows = [{"action_id": "before", "action": "recall", "status": "completed", "comparison_valid": True},
                {"action_id": "challenge", "action": action, "status": "completed", "comparison_valid": True,
                 "input": {"step": step}, "control_evidence": receipt},
                {"action_id": "after", "action": "recall", "status": "completed", "comparison_valid": True}]
        return case, rows

    def test_delete_a_cannot_be_proved_by_receipt_and_positive_for_b(self):
        case, rows = self.control("delete_source", "A-r1")
        rows[1]["control_evidence"]["target_source_ids"] = ["B"]
        self.assertEqual("fail", adjudicate.challenge_verdict(case, rows, {"before": {"B"}, "after": {"C"}}))
        rows[1]["control_evidence"]["target_source_ids"] = ["A-r1"]
        self.assertEqual("pass", adjudicate.challenge_verdict(case, rows, {"before": {"A-r1"}, "after": {"C"}}))

    def test_lineage_receipt_must_cover_exact_frozen_source_revisions(self):
        case, rows = self.control("delete_source", "A-r2", {"lineage_id": "A", "all_revisions": True})
        witnesses = {"before": {"A-r2"}, "after": {"C"}}
        self.assertEqual("fail", adjudicate.challenge_verdict(case, rows, witnesses))
        rows[1]["control_evidence"]["target_source_ids"] = ["A-r1", "A-r2"]
        self.assertEqual("pass", adjudicate.challenge_verdict(case, rows, witnesses))
        rows[1]["control_evidence"]["target_source_ids"].append("B")
        self.assertEqual("fail", adjudicate.challenge_verdict(case, rows, witnesses))

    def test_corruption_cannot_take_its_target_from_the_receipt(self):
        case, rows = self.control("corrupt_state")
        witnesses = {"before": {"B"}, "after": {"C"}}
        self.assertEqual("indeterminate", adjudicate.challenge_verdict(case, rows, witnesses))
        rows[1]["input"]["step"]["source_id"] = "B"
        self.assertEqual("pass", adjudicate.challenge_verdict(case, rows, witnesses))
        rows[1]["input"]["step"]["source_id"] = "A-r1"
        self.assertEqual("fail", adjudicate.challenge_verdict(case, rows, witnesses))


class MeasurementTests(unittest.TestCase):
    def test_healthy_zero_result_empty_recall_is_a_success(self):
        row = {"status": "completed", "terminal": "success_zero_results", "successful_completion": True,
               "timing": {"elapsed_ns": 5}, "requires_positive_recall": False}
        summary = summarize_attempts([row], target_ns=10)
        self.assertEqual(1, summary["counts"]["completed_success"])
        self.assertEqual(0, summary["counts"]["completed_failure"])
        self.assertEqual("pass", summary["success_target"]["status"])
        row.update(requires_positive_recall=True, positive_evidence_verified=False)
        self.assertEqual(0, summarize_attempts([row])["counts"]["completed_success"])

    def test_fast_refusals_do_not_pass_success_latency(self):
        rows = [{"status": "completed", "terminal": "busy", "successful_completion": False,
                 "provider_contacted": True, "timing": {"elapsed_ns": 1}} for _ in range(100)]
        summary = summarize_attempts(rows, target_ns=250000000)
        self.assertEqual("fail", summary["success_target"]["status"])
        self.assertEqual(0, summary["successful_completion_latency_ns"]["n"])
        self.assertEqual(100, summary["completed_terminal_latency_ns"]["n"])

    def test_censoring_keeps_bounds_and_terminal_subclasses(self):
        rows = [{"status": "censored", "terminal": None, "timing": {"elapsed_at_cutoff_ns": 50}},
                {"status": "completed", "terminal": "cancelled", "timing": {"elapsed_ns": 1}},
                {"status": "unexecuted", "terminal": None, "timing": {}}]
        summary = summarize_attempts(rows, target_ns=100)
        self.assertEqual(3, summary["counts"]["planned"])
        self.assertEqual(2, summary["counts"]["attempted"])
        self.assertEqual(1, summary["counts"]["completed_failure"])
        self.assertEqual(1, summary["counts"]["cancelled"])
        one = summarize_attempts(rows[:1], target_ns=100)
        self.assertEqual("indeterminate", one["success_target"]["status"])
        self.assertEqual("fail", summarize_attempts(rows[:1], target_ns=40)["success_target"]["status"])

    def test_paired_resource_deltas_preserve_negative_and_missing(self):
        result = paired_block_difference({"a": [2, 4], "b": [3]}, {"a": [4, 6], "b": [5]}, seed=7)
        self.assertEqual(-2, result["difference"])
        self.assertEqual([-2, -2], result["interval_95"])
        self.assertEqual("incomplete", paired_block_difference({"a": [None]}, {"a": [5]}, seed=7)["status"])

    def test_refusal_marked_success_and_empty_positive_recall_cannot_win(self):
        row = {"status": "completed", "terminal": "busy", "successful_completion": True,
               "timing": {"elapsed_ns": 1}}
        self.assertEqual(0, summarize_attempts([row])["counts"]["completed_success"])
        row.update(terminal="success", requires_positive_recall=True, positive_evidence_verified=False)
        self.assertEqual("fail", summarize_attempts([row], target_ns=100)["success_target"]["status"])

    def test_finite_measurement_populations_and_fixed_payload_bytes(self):
        specs = populations()
        spec = next(s for s in specs if s["host"] == "claude" and s["lane"] == "provider:ncm" and s["population"] == "queue_saturation")
        self.assertEqual(410, len(planned_attempts(spec)))
        warm = next(s for s in specs if s["host"] == "claude" and s["lane"] == "provider:ncm" and s["population"] == "warm.current_session")
        rows = planned_attempts(warm)
        self.assertEqual(100, sum(not r["warmup"] for r in rows))
        self.assertEqual(50, sum(r["warmup"] for r in rows))
        fixtures = fixed_fixtures()
        self.assertEqual(128, len(fixtures["populated"]))
        self.assertEqual(64, sum(s["origin"]["session"] == "session-a" for s in fixtures["populated"]))
        self.assertTrue(all(len(s["content"].encode()) == 4096 for s in fixtures["populated"]))
        self.assertTrue(all(len(s["content"].encode()) == 1024 for s in fixtures["snapshot"]))
        self.assertEqual(64, len(record(0, 64, prefix="observe")["content"].encode()))

    def test_measurement_input_is_attributed_and_missing_rows_stay_planned(self):
        spec = next(s for s in populations() if s["applicability"] == "applicable")
        row = {**planned_attempts(spec)[0], "status": "completed", "terminal": "success", "successful_completion": True,
               "timing": {"status": "measured", "phase": "wrong", "start_monotonic_ns": 1, "end_monotonic_ns": 2, "elapsed_ns": 1}}
        with self.assertRaisesRegex(ValueError, "timing"):
            summarize_measurements([row])

    def test_ratio_bootstrap_pools_denominators_instead_of_case_ratios(self):
        left = {"a": (1, 1), "b": (0, 99)}
        right = {"a": (0, 1), "b": (0, 99)}
        result = paired_ratio_difference(left, right, seed=7)
        self.assertAlmostEqual(.01, result["difference"])
        self.assertEqual("incomplete", paired_ratio_difference({"a": (0, 0)}, {"a": (0, 0)}, seed=7)["status"])


if __name__ == "__main__":
    unittest.main()
