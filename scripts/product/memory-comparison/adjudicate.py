"""Join blinded, source-grounded retrieval assessments to the existing Rust metrics.

This module verifies annotation evidence references; it does not assign relevance
from word overlap. A human reviewer supplies the label and frozen-fact mapping.
"""

import argparse
import json
from pathlib import Path

from runner import LABELS, ROOT, TOKENIZER, digest, require, text_digest
from delivery_evidence import PRESENTATION_REVIEW, finally_emitted, is_tool_result

CHECKS = ("source_fidelity", "query_expectations", "nonvacuous_safety", "challenge_evidence")


def catalog_for(cases):
    """Rebind scenarios/checks while retaining all existing metric definitions."""
    catalog = json.loads((ROOT / "product/evaluation/coding-memory-metrics.v1.json").read_text())
    catalog["corpus_binding"] = {"corpus_id": "tracedecay.host-comparison.heldout.v1",
                                 "schema_version": 1, "bead_id": "pm-comparison-harness"}
    catalog["safety_critical_checks"] = [{"scenario_id": c["id"], "check_ids": list(CHECKS)} for c in cases]
    for metric in catalog["metrics"]:
        metric["rubric_check_bindings"] = []
        if metric["applicable_scenarios"] is not None:
            action = "correct" if metric["metric_id"] == "correction_latency" else "corrupt_state"
            metric["applicable_scenarios"] = [c["id"] for c in cases if any(s["action"] == action for s in c["steps"])]
    return catalog


def unmeasured(reason):
    return {"kind": "unmeasured", "reason": reason}


def evidence_for(candidate, query, case, annotation, delivery=None):
    """Verify annotation refers to frozen truth and an actually delivered body."""
    label = annotation.get("label", "missing") if annotation else "missing"
    reason = annotation.get("reason") if annotation else "no blinded source-grounded annotation supplied"
    require(label in LABELS and bool(reason), "missing label vocabulary/reason")
    sources = {s["id"]: s for s in case["sources"]}
    known, faithful = [], True
    for captured in candidate["sources"]:
        source = sources.get(captured["source_id"])
        if source is None:
            faithful = False
            continue
        known.append(source["id"])
        faithful &= all(captured.get(k) == source[k] for k in ("revision", "lineage_id", "origin"))
        faithful &= captured.get("content_sha256") == text_digest(source["content"])
    facts = []
    if annotation:
        if delivery and is_tool_result(delivery):
            require(annotation.get("presentation_sha256") == candidate["presentation_sha256"] and
                    annotation.get("advisory_review_sha256") == delivery["advisory_review_sha256"],
                    "annotation does not bind the full candidate presentation and advisory notices")
            require(annotation.get("presentation_review") == PRESENTATION_REVIEW and
                    type(annotation.get("prohibited_claims_absent")) is bool,
                    "annotation did not assess full candidate metadata, shared text, source attribution, and prohibited claims")
            require(candidate["final_join"]["status"] == "bound" or label in
                    ("missing", "indeterminate", "unverifiable"), "unresolved final-output join cannot receive a determinate label")
        require(annotation.get("reviewer_id") and annotation.get("provider_blinded") is True,
                "retrieval label lacks blinded reviewer identity")
        for proof in annotation.get("fact_evidence", []):
            index = proof["fact_index"]
            source_id = proof["source_id"]
            require(isinstance(index, int) and 0 <= index < len(query["required_facts"]), "unfrozen fact reference")
            require(source_id in known and source_id in query["expected_useful_source_ids"],
                    "useful evidence does not reference an expected delivered source")
            require(bool(proof["source_quote"]) and proof["source_quote"] in sources[source_id]["content"],
                    "evidence quote is absent from frozen source")
            require(bool(proof["delivered_quote"]) and proof["delivered_quote"] in candidate["content"],
                    "evidence quote was not finally delivered")
            facts.append(f"{query['id']}/required_facts/{index}")
    if label == "useful":
        require(faithful and facts, "useful label lacks source-grounded required-fact evidence")
        require(annotation.get("prohibited_claims_absent") is True, "useful annotation did not check prohibited claims")
    return label, reason, known, facts, faithful


def challenge_targets(case, step):
    """Resolve only the frozen action's source/lineage selector, never a receipt."""
    sources = {source["id"]: source for source in case.get("sources", [])}
    selector = step.get("details", {}).get("selector", {})
    if not isinstance(selector, dict):
        return None
    source_id = step.get("source_id")
    selected_source_id = selector.get("source_id")
    if selected_source_id:
        if source_id and selected_source_id != source_id:
            return None
        source_id = selected_source_id
    if source_id and source_id not in sources:
        return None
    revision = step.get("source_revision")
    if revision is not None and (not source_id or sources[source_id].get("revision") != revision):
        return None
    lineage = selector.get("lineage_id")
    if lineage:
        if source_id and sources[source_id].get("lineage_id") != lineage:
            return None
        targets = {source["id"] for source in sources.values() if source.get("lineage_id") == lineage}
        if not targets:
            return None
        if selector.get("all_revisions") is False:
            return {source_id} if source_id else None
        return targets
    return {source_id} if source_id else None


def challenge_verdict(case, rows, eligible_sources_by_action=None):
    """Non-vacuous challenge receipts must bind actual actions and source controls.

    The trusted fixture supplies physical evidence refs (canonical commits,
    corrupt artifact before/after digests, cancellation boundary/child join,
    deletion fanout/source fence); this function neither fabricates nor admits it.
    """
    eligible_sources_by_action = eligible_sources_by_action or {}
    challenges = [r for r in rows if r["action"] in ("cancel", "corrupt_state", "delete_source", "restart", "replay", "switch_provider")]
    if not challenges:
        return "pass"
    outcomes = []
    for row in challenges:
        receipt = row.get("control_evidence", {})
        if row["status"] != "completed" or not receipt.get("evidence_refs") or receipt.get("exercised") is not True:
            outcomes.append("indeterminate"); continue
        if receipt.get("verified") is not True:
            outcomes.append("fail"); continue
        if row["action"] in ("restart", "switch_provider"):
            if receipt.get("remaining_owned_children") != 0 or receipt.get("old_process_exited") is not True:
                outcomes.append("fail"); continue
        if row["action"] == "cancel":
            if (receipt.get("cancellation_completed") is not True or receipt.get("released_permits") is not True
                    or receipt.get("remaining_cancelled_children") != 0):
                outcomes.append("fail"); continue
        if row["action"] in ("corrupt_state", "delete_source"):
            target = set(receipt.get("target_source_ids", []))
            expected_target = challenge_targets(case, row.get("input", {}).get("step", {}))
            if expected_target is None or not target:
                outcomes.append("indeterminate"); continue
            if target != expected_target:
                outcomes.append("fail"); continue
            positive = next((r for r in rows if r["action_id"] == receipt.get("positive_action_id")), None)
            unaffected = next((r for r in rows if r["action_id"] == receipt.get("unaffected_action_id")), None)
            if not target or not positive or not unaffected or rows.index(positive) >= rows.index(row) or rows.index(unaffected) <= rows.index(row):
                outcomes.append("indeterminate"); continue
            def witness_sources(item):
                if item["status"] != "completed" or item.get("comparison_valid") is not True:
                    return set()
                return eligible_sources_by_action.get(item["action_id"], set())
            if not target & witness_sources(positive) or not witness_sources(unaffected) - target:
                outcomes.append("fail"); continue
            if row["action"] == "corrupt_state":
                before, after = receipt.get("before_actual_sha256"), receipt.get("after_actual_sha256")
                if not before or not after or before == after or receipt.get("before_claimed_sha256") != receipt.get("after_claimed_sha256"):
                    outcomes.append("fail"); continue
            elif not receipt.get("deletion_fence_ref") or receipt.get("derived_state_erased") is not True:
                outcomes.append("indeterminate"); continue
        outcomes.append("pass")
    return "fail" if "fail" in outcomes else "indeterminate" if "indeterminate" in outcomes else "pass"


def metric_inputs(report, annotations):
    """Return 24 existing-evaluator inputs; missing cases/queries stay unresolved."""
    require(report["format"] == "tracedecay.host-comparison.capture.v1", "not a host capture report")
    by_key = {}
    for a in annotations:
        key = (a["case_trial_id"], a["request_id"], a["candidate_ref"])
        require(key not in by_key, "duplicate candidate annotation")
        by_key[key] = a
    used, bundles = set(), []
    cases = report["plan"]["cases"]
    for host in ("claude", "codex"):
        for lane in ("provider:tracedecay.native", "provider:ncm", "no_memory", "explicit_documentation"):
            for trial in range(3):
                scenarios, recalls, assessments = [], [], []
                for case in cases:
                    rows = [r for r in report["measurements"] if r["host"] == host and r["lane"] == lane
                            and r["trial"] == trial and r["case_id"] == case["id"]]
                    expected_queries = {q["id"]: q for q in case["queries"]}
                    require(len([r for r in rows if r["action"] == "recall"]) == len(expected_queries), "scheduled query rows missing")
                    candidates, latencies, terminals, query_outcomes = [], [], [], []
                    token_sum, fidelity, positive = 0, "pass", False
                    eligible_sources_by_action = {}
                    deleted_lineages = set()
                    corruption_exercised, corrupt_count, corrupt_unknown = False, 0, False
                    correction_measurements = []
                    for row in rows:
                        if row["action"] == "corrupt_state":
                            corruption_exercised = True
                        if row["action"] == "correct":
                            span = row.get("correction_effectiveness")
                            if span and span.get("status") == "measured" and span.get("phase") == "correction_submission_to_verified_recall":
                                require(span.get("verified_recall_action_id") in {r["action_id"] for r in rows}, "correction span has no verifying recall")
                                require(span["elapsed_ns"] == span["end_monotonic_ns"] - span["start_monotonic_ns"] >= 0,
                                        "invalid correction timing boundaries")
                                correction_measurements.append(span["elapsed_ns"] // 1000)
                        if row["action"] == "delete_source" and row["status"] == "completed":
                            step = row["input"]["step"]
                            deleted_lineages.add(step.get("details", {}).get("selector", {}).get("lineage_id"))
                        if row["action"] != "recall":
                            continue
                        query = expected_queries[row["request_id"]]
                        delivery = row["delivery"]
                        metric_delivery = None
                        covered, forbidden, unknown, query_useful_sources = set(), False, False, set()
                        if delivery:
                            tool_result = is_tool_result(delivery)
                            metric_delivery = {k: delivery[k] for k in ("final_tokens", "canonical_tokens", "advisory_tokens", "candidate_body_tokens")}
                            metric_delivery.update({"tokenizer_identity": TOKENIZER["identity"], "tokenizer_revision": TOKENIZER["revision"]})
                            if tool_result:
                                # Preserve original artifact bytes, lexical spans, complete trace
                                # and block boundaries in the evaluator input, without rendering.
                                metric_delivery.update({"representation": "tool_result_v1", "tool_result": {
                                    k: v for k, v in delivery.items() if k != "candidates"}, "candidates": []})
                                metric_delivery["tool_result"]["candidates"] = []
                            else:
                                metric_delivery.update({k: delivery[k] for k in ("final_text", "final_sha256")})
                                metric_delivery.update({"sections": [{"kind": s["kind"], "text": s["text"], "candidate_ref": s.get("candidate_ref")}
                                                                     for s in delivery["sections"]], "candidates": []})
                            for captured in delivery["candidates"]:
                                key = (row["case_trial_id"], row["request_id"], captured["candidate_ref"])
                                emitted = finally_emitted(captured, delivery)
                                annotation = by_key.get(key) if emitted else None
                                if annotation:
                                    used.add(key)
                                label, reason, source_ids, facts, faithful = evidence_for(captured, query, case, annotation, delivery)
                                if emitted:
                                    forbidden |= bool(set(source_ids) & set(query["forbidden_source_ids"]))
                                    unknown |= label in ("missing", "indeterminate", "unverifiable")
                                    fidelity = "fail" if not faithful else fidelity
                                forgotten = any(s.get("lineage_id") in deleted_lineages for s in captured["sources"])
                                candidate = {"request_id": row["request_id"], "candidate_ref": captured["candidate_ref"],
                                             "label": label, "provenance": captured.get("provenance", "missing"),
                                             "scope_match": captured.get("scope_match") is True,
                                             "contains_forgotten_source": forgotten}
                                annotated = {"candidate": candidate, "content": captured.get("content", ""),
                                    "content_sha256": captured.get("content_sha256", ""), "source_ids": source_ids,
                                    "required_fact_refs": facts, "annotation_reason": reason}
                                if tool_result:
                                    annotated.update({k: v for k, v in captured.items() if k not in annotated})
                                    annotated["annotation_presentation_sha256"] = annotation.get("presentation_sha256") if annotation else None
                                    annotated["annotation_advisory_review_sha256"] = annotation.get("advisory_review_sha256") if annotation else None
                                    annotated["presentation_review"] = annotation.get("presentation_review") if annotation else None
                                    annotated["prohibited_claims_absent"] = annotation.get("prohibited_claims_absent") if annotation else None
                                    forbidden |= bool(annotation) and annotation.get("prohibited_claims_absent") is False
                                    metric_delivery["tool_result"]["candidates"].append(annotated)
                                    unknown |= captured["final_join"]["status"] != "bound"
                                else:
                                    annotated.update({"stages": [captured[s] for s in ("returned", "admitted", "selected", "packed", "delivered")],
                                                      "withholding_reason": captured.get("withholding_reason")})
                                    metric_delivery["candidates"].append(annotated)
                                if emitted:
                                    candidates.append(candidate)
                                    forbidden |= forgotten or label in ("harmful", "stale") or (
                                        not candidate["scope_match"] and
                                        (not tool_result or captured["final_join"]["status"] == "bound"))
                                    if (label == "useful" and faithful and not forgotten and candidate["scope_match"]
                                            and (not tool_result or captured["final_join"]["status"] == "bound")
                                            and candidate["provenance"] in ("available", "redacted")
                                            and not set(source_ids) & set(query["forbidden_source_ids"])):
                                        covered.update(facts)
                                        query_useful_sources.update(proof["source_id"] for proof in annotation.get("fact_evidence", []))
                                    if corruption_exercised:
                                        if not isinstance(captured.get("from_corrupt_state"), bool):
                                            corrupt_unknown = True
                                        else:
                                            corrupt_count += captured["from_corrupt_state"]
                            if tool_result and not delivery["candidates"]:
                                unknown |= any(b["text"] for b in delivery["advisory_blocks"])
                            token_sum = token_sum + delivery["final_tokens"] if token_sum is not None else None
                        else:
                            token_sum = None
                        if row.get("terminal"):
                            terminals.append(row["terminal"])
                        outcome = "indeterminate"
                        if row["status"] == "completed":
                            outcome = "fail" if row["terminal"] != query["expected_terminal"] or forbidden else "pass"
                            if query["expected_useful_source_ids"] and (not delivery or len(covered) < len(query["required_facts"])):
                                outcome = "indeterminate" if unknown or not delivery else "fail"
                        if delivery and is_tool_result(delivery) and unknown and outcome == "pass":
                            outcome = "indeterminate"
                        if row.get("comparison_valid") is False:
                            outcome, fidelity = "indeterminate", "indeterminate"
                        if row["status"] == "completed" and row.get("comparison_valid") is True and outcome == "pass":
                            eligible_sources_by_action[row["action_id"]] = query_useful_sources
                            positive |= bool(query_useful_sources)
                        query_outcomes.append(outcome)
                        assessments.append({"request_id": query["id"], "case_id": case["id"], "outcome": outcome})
                        elapsed = row["timing"].get("elapsed_ns")
                        measured = row["phase"] == "host_request" and elapsed is not None and row["status"] == "completed"
                        if measured:
                            latencies.append(elapsed // 1000)
                        recalls.append({"scenario_id": case["id"], "request_id": query["id"], "delivery": metric_delivery,
                                        "host_latency_micros": {"kind": "value", "value": elapsed // 1000} if measured
                                        else unmeasured(row["timing"].get("reason") or "host timing unmeasured")})
                    rubric = "fail" if "fail" in query_outcomes else "indeterminate" if "indeterminate" in query_outcomes else "pass"
                    nonvacuous = "pass" if positive else "indeterminate"
                    checks = dict(zip(CHECKS, (fidelity, rubric, nonvacuous, challenge_verdict(case, rows, eligible_sources_by_action))))
                    scenarios.append({"scenario_id": case["id"],
                        "terminal_gate": {"passed": all(r["status"] == "completed" for r in rows),
                                          "observed_terminal_codes": sorted(set(terminals)),
                                          "violations": [r["action_id"] for r in rows if r["status"] != "completed"]},
                        "task_outcome": {"kind": rubric, **({"reason": "retrieval evidence incomplete"} if rubric == "indeterminate" else {})},
                        "rubric_checks": [{"check_id": k, "outcome": v} for k, v in checks.items()],
                        "candidates": candidates, "recall_latency_micros": latencies,
                        "context_tokens": {"kind": "value", "value": token_sum} if token_sum is not None else unmeasured("missing final bytes"),
                        "curation_seconds": unmeasured("human curation effort unmeasured"),
                        "correction": ({"kind": "measured", "latency_micros": max(correction_measurements)}
                                       if correction_measurements and len(correction_measurements) == sum(s["action"] == "correct" for s in case["steps"])
                                       else unmeasured("direct submission-to-verified-read spans required for every correction"))
                        if any(s["action"] == "correct" for s in case["steps"]) else {"kind": "not_applicable", "reason": "no correction in case"},
                        "discovery": {"kind": "not_enumerated", "reason": "downstream task runner unmeasured"},
                        "corrupt_state": ({"kind": "enumerated", "admitted_from_corrupt_state": corrupt_count}
                                          if checks["challenge_evidence"] == "pass" and not corrupt_unknown
                                          else {"kind": "not_enumerable", "reason": "requires derived-state/source admission evidence"})
                        if any(s["action"] == "corrupt_state" for s in case["steps"]) else {"kind": "not_exercised"}})
                metric_input = {"record": {"provider": {
                    "lane_id": lane, "provider_id": lane.removeprefix("provider:") if lane.startswith("provider:") else None,
                    "run_identity_sha256": None}, "scenarios": scenarios}, "recalls": recalls}
                case_trial_ids = {s["case_trial_id"] for s in report["plan"]["schedule"]
                                  if s["host"] == host and s["lane"] == lane and s["trial"] == trial}
                metric_input["record"]["provider"]["run_identity_sha256"] = digest({
                    "host": host, "lane": lane, "trial": trial, "freeze": report["plan"]["freeze"],
                    "metadata": report["metadata"], "metric_input_without_identity": metric_input,
                    "consumed_captures": [c for c in report["raw_case_captures"] if c["case_trial_id"] in case_trial_ids],
                    "consumed_annotations": [a for a in annotations if a["case_trial_id"] in case_trial_ids]})
                bundles.append({"host": host, "lane": lane, "trial": trial, "query_assessments": assessments,
                                "input": metric_input})
    require(used == set(by_key), "annotation targets a candidate the host did not deliver")
    return bundles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("annotations", type=Path)
    parser.add_argument("--output-directory", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.capture.read_text())
    bundles = metric_inputs(report, json.loads(args.annotations.read_text()))
    args.output_directory.mkdir(parents=True, exist_ok=False)
    (args.output_directory / "catalog.json").write_text(json.dumps(catalog_for(report["plan"]["cases"]), indent=2) + "\n")
    for bundle in bundles:
        name = f"{bundle['host']}-{bundle['lane'].replace(':', '_')}-{bundle['trial']}"
        (args.output_directory / f"{name}.json").write_text(json.dumps(bundle["input"], indent=2) + "\n")
    (args.output_directory / "query-assessments.json").write_text(json.dumps([
        {k: v for k, v in b.items() if k != "input"} for b in bundles], indent=2) + "\n")


if __name__ == "__main__":
    main()
