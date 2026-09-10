"""Assemble existing Rust metric reports without redefining their metrics."""

import argparse
import json
from pathlib import Path

from adjudicate import metric_inputs
from analysis_stats import paired_block_difference, paired_ratio_difference, wilson
from runner import HOSTS, LANES, require


def join(capture, bundles, evaluated):
    expected = {(b["host"], b["lane"], b["trial"]): b for b in bundles}
    require(len(expected) == 24, "all 24 paired host/lane/trial inputs are required")
    reports = {}
    for result in evaluated:
        key = (result["host"], result["lane"], result["trial"])
        require(key in expected and key not in reports, "unscheduled or duplicate metric report")
        report = result["report"]
        require(report["assessment"] == "host_retrieval_assessment" and report["task_benefit"] == "unmeasured",
                "downstream task claims do not belong in retrieval report")
        require(report["metrics"]["provider"] == expected[key]["input"]["record"]["provider"], "metric report identity substitution")
        require([s["scenario_id"] for s in report["metrics"]["scenarios"]] ==
                [s["scenario_id"] for s in expected[key]["input"]["record"]["scenarios"]], "metric scenario drift")
        reports[key] = report
    strata, case_blocks, precision_blocks = [], {}, {}
    for host in HOSTS:
        for lane in LANES:
            selected = [expected[(host, lane, trial)] for trial in range(3)]
            actual = [reports[(host, lane, trial)]["metrics"] for trial in range(3) if (host, lane, trial) in reports]
            numerator = denominator = labeled = missing = indeterminate = 0
            for metrics in actual:
                precision = metrics["provider_metrics"]["useful_recall_precision"]
                if precision["value"]["kind"] == "ratio":
                    numerator += precision["value"]["numerator"]
                    denominator += precision["value"]["denominator"]
                counts = precision["label_counts"] or {}
                labeled += counts.get("labeled", 0); missing += counts.get("unlabeled", 0)
                indeterminate += counts.get("indeterminate", 0)
            cases = [s for b in selected for s in b["input"]["record"]["scenarios"]]
            queries = [q for b in selected for q in b["query_assessments"]]
            unresolved = sum(s["task_outcome"]["kind"] == "indeterminate" for s in cases)
            admissions = [c for s in cases for c in s["candidates"]]
            precision_value = numerator / denominator if denominator else None
            quality = "incomplete" if len(actual) != 3 or missing or indeterminate or unresolved or precision_value is None else "pass" if precision_value >= 0.60 else "fail"
            blocks = {}
            for case_id in {s["scenario_id"] for s in cases}:
                outcomes = [s["task_outcome"]["kind"] for s in cases if s["scenario_id"] == case_id]
                blocks[case_id] = [1 if value == "pass" else 0 if value == "fail" else None for value in outcomes]
            case_blocks[(host, lane)] = blocks
            ratios = {}
            for case_id in blocks:
                values = [s["metrics"]["useful_recall_precision"] for m in actual for s in m["scenarios"] if s["scenario_id"] == case_id]
                if len(values) != 3 or any(v["value"]["kind"] not in ("ratio", "not_applicable")
                                          or (v["label_counts"] or {}).get("unlabeled", 0)
                                          or (v["label_counts"] or {}).get("indeterminate", 0) for v in values):
                    ratios[case_id] = None
                else:
                    ratios[case_id] = (sum(v["value"].get("numerator", 0) for v in values),
                                       sum(v["value"].get("denominator", 0) for v in values))
            precision_blocks[(host, lane)] = ratios
            strata.append({"host": host, "lane": lane,
                "case_rubric": {**wilson(sum(s["task_outcome"]["kind"] == "pass" for s in cases), 54),
                                "failed": sum(s["task_outcome"]["kind"] == "fail" for s in cases), "unresolved": unresolved},
                "query_expectations": {**wilson(sum(q["outcome"] == "pass" for q in queries), 168),
                                       "failed": sum(q["outcome"] == "fail" for q in queries),
                                       "unresolved": sum(q["outcome"] == "indeterminate" for q in queries)},
                "useful_recall_precision": {**wilson(numerator, denominator), "value": precision_value,
                    "source": "pooled numerator/denominator from existing MetricReport", "minimum": 0.60,
                    "labeled": labeled, "missing": missing, "indeterminate": indeterminate,
                    "unique_candidates": len({(c["request_id"], c["candidate_ref"]) for c in admissions}),
                    "admission_events": len(admissions), "quality_gate": quality,
                    "interval_interpretation": "descriptive candidate-level Wilson interval; admissions share cases"},
                "safety_gate": "incomplete" if len(actual) != 3 else "pass" if all(m["safety_gate"]["passed"] for m in actual) else "fail"})
    deltas = []
    for host in HOSTS:
        for left, right in ((LANES[0], LANES[1]), (LANES[0], LANES[2]), (LANES[1], LANES[2]), (LANES[0], LANES[3]), (LANES[1], LANES[3])):
            deltas.append({"host": host, "left": left, "right": right, "measure": "case_rubric_pass_fraction",
                           **paired_block_difference(case_blocks[(host, left)], case_blocks[(host, right)], seed=6010317)})
            deltas.append({"host": host, "left": left, "right": right, "measure": "pooled_useful_recall_precision",
                           **paired_ratio_difference(precision_blocks[(host, left)], precision_blocks[(host, right)], seed=6010317)})
    return {"format": "tracedecay.host-comparison.report.v1", "mode": capture["mode"],
            "experiment": "host_retrieval_assessment", "task_benefit": capture["task_benefit"],
            "task_outcome_semantics": "source-grounded retrieval rubric; agent task benefit unmeasured",
            "metadata": capture["metadata"], "planned": capture["plan"]["planned"], "strata": strata,
            "paired_case_deltas": deltas, "raw_metric_reports": evaluated,
            "invalid_comparisons": capture["invalid_comparisons"],
            "comparative_conclusion": "invalid" if capture["invalid_comparisons"] else "incomplete" if len(reports) != 24 else "review_paired_intervals",
            "provider_conformance": {"status": "unmeasured", "reason": "separate common conformance report required"},
            "performance": {"status": "unmeasured", "reason": "attach separately scheduled direct phase/resource report"}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("annotations", type=Path)
    parser.add_argument("metric_reports", type=Path, help="JSON array of {host,lane,trial,report}")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    capture = json.loads(args.capture.read_text())
    bundles = metric_inputs(capture, json.loads(args.annotations.read_text()))
    result = join(capture, bundles, json.loads(args.metric_reports.read_text()))
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write("\n")


if __name__ == "__main__":
    main()
