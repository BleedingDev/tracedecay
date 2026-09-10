"""Frozen finite performance populations and phase/resource reporting.

This is an execution declaration, not a benchmark launcher. The host fixture
supplies one capture row per attempt using runner.Capture's reviewed collectors.
"""

from collections import defaultdict

from analysis_stats import distribution, paired_block_difference, summarize_attempts

HOSTS = ("claude", "codex")
LANES = ("provider:tracedecay.native", "provider:ncm", "no_memory", "explicit_documentation")


def populations():
    specs = []
    for state in ("empty", "populated_reopened"):
        for phase in ("cold_start", "first_recall", "spawn_to_first_delivered_context"):
            specs.append((f"{state}.{phase}", phase, 20, 1, 0, False, None))
    for query in ("current_session", "authorized_later_session", "empty"):
        specs.append((f"warm.{query}", "host_request", 10, 10, 5, False, None))
        # Direct provider timing is a distinct span of these same attempts.
        specs.append((f"warm.{query}.provider", "provider_recall", 10, 10, 5, True, 250_000_000))
    for size in (64, 4096, 16384):
        specs.append((f"observe.{size}", "durable_observe", 10, 10, 5, True, 500_000_000))
    for operation in ("feedback", "correction", "deletion"):
        specs.append((operation, f"durable_{operation}", 10, 10, 5, True, None))
        specs.append((f"{operation}.verification", "response_to_verifying_read", 10, 10, 5, True, None))
    for operation in ("replay", "shutdown", "snapshot_export", "snapshot_restore"):
        specs.append((operation, operation, 20, 1, 0, True, None))
    specs.append(("snapshot_preparation", "snapshot_preparation", 1, 1, 0, True, None))
    specs.append(("snapshot_oversize", "snapshot_oversize_rejection", 1, 1, 0, True, None))
    specs.append(("queue_saturation", "queue_burst", 10, 41, 0, True, None))
    for trigger in ("before_dispatch", "after_durable_batch_item", "during_recall", "during_replay"):
        specs.append((f"cancellation.{trigger}", "cancel_to_reaped", 20, 1, 0, True, None))
    for mode in ("off", "on"):
        specs.append((f"observer.{mode}", "host_request", 10, 10, 0, True, None))
    return [{"population": population, "phase": phase, "blocks": blocks, "attempts_per_block": attempts,
             "warmup_per_block": warmup, "provider_only": provider_only, "target_ns": target,
             "host": host, "lane": lane,
             "applicability": "not_applicable" if provider_only and lane in LANES[2:] else "applicable",
             "reason": "control performs no provider operation" if provider_only and lane in LANES[2:] else None}
            for population, phase, blocks, attempts, warmup, provider_only, target in specs
            for host in HOSTS for lane in LANES]


def planned_attempts(spec):
    if spec["applicability"] == "not_applicable":
        return []
    return [{"attempt_id": f"{spec['host']}/{spec['lane']}/{spec['population']}/{block}/{index}",
             "host": spec["host"], "lane": spec["lane"], "population": spec["population"],
             "phase": spec["phase"], "block": block, "index": index,
             "warmup": index < spec["warmup_per_block"]}
            for block in range(spec["blocks"])
            for index in range(spec["warmup_per_block"] + spec["attempts_per_block"])]


def summarize_measurements(captures):
    """Keep warm-ups, successful/failed terminals and matched costs separate."""
    if len({row["attempt_id"] for row in captures}) != len(captures):
        raise ValueError("duplicate attempt/retry")
    captured = {row["attempt_id"]: row for row in captures}
    consumed, results, paired = set(), [], defaultdict(dict)
    for spec in populations():
        rows = []
        for planned in planned_attempts(spec):
            row = captured.get(planned["attempt_id"])
            if row:
                if any(row.get(k) != v for k, v in planned.items()):
                    raise ValueError("phase/population/block attribution drift")
                consumed.add(planned["attempt_id"])
                if row.get("resources"):
                    if row["resources"].get("phase") != row["phase"] or row["resources"].get("root") != row.get("process_group"):
                        raise ValueError("resource process/phase attribution drift")
                timing = row["timing"]
                if timing.get("status") == "measured":
                    if (timing.get("phase") != row["phase"] or timing["end_monotonic_ns"] < timing["start_monotonic_ns"]
                            or timing["elapsed_ns"] != timing["end_monotonic_ns"] - timing["start_monotonic_ns"]):
                        raise ValueError("direct timing boundary mismatch")
                if row["status"] == "censored" and timing.get("elapsed_at_cutoff_ns") is None:
                    raise ValueError("missing measurement cutoff")
            else:
                row = {**planned, "status": "unexecuted", "terminal": None, "provider_contacted": None,
                       "reason": "planned measurement not supplied", "timing": {"status": "unmeasured"}}
            row = dict(row)
            row["requires_positive_recall"] = (spec["population"].startswith(("warm.current_session", "warm.authorized_later_session"))
                                               and spec["lane"].startswith("provider:"))
            rows.append(row)
        measured = [r for r in rows if not r["warmup"]]
        warmup = [r for r in rows if r["warmup"]]
        resource_fields = ("start_tree_rss_bytes", "end_tree_rss_bytes", "sampled_peak_tree_rss_bytes",
                           "peak_child_count", "end_child_count", "maximum_gap_ns", "instrumentation_thread_cpu_seconds")
        resources = {field: distribution([r["resources"][field] for r in measured
                                          if (r.get("resources") or {}).get(field) is not None]) for field in resource_fields}
        results.append({**spec, "attempts": summarize_attempts(measured, target_ns=spec["target_ns"],
                                                              required_completed=100 if spec["attempts_per_block"] == 10 else 0),
                        "warmup": summarize_attempts(warmup), "resource_distributions": resources,
                        "raw_attempts": rows})
        for field in ("elapsed_ns", *resource_fields):
            blocks = defaultdict(list)
            for row in measured:
                source = row["timing"] if field == "elapsed_ns" else row.get("resources", {})
                blocks[str(row["block"])].append(source.get(field))
            paired[(spec["host"], spec["population"], spec["phase"], field)][spec["lane"]] = dict(blocks)
    if consumed != set(captured):
        raise ValueError("unscheduled measurement")
    deltas = []
    for (host, population, phase, field), lanes in paired.items():
        for left, right in ((LANES[0], LANES[1]), (LANES[0], LANES[2]), (LANES[1], LANES[2]),
                            (LANES[0], LANES[3]), (LANES[1], LANES[3])):
            deltas.append({"host": host, "population": population, "phase": phase, "field": field,
                           "left": left, "right": right,
                           **paired_block_difference(lanes.get(left, {}), lanes.get(right, {}), seed=6010317)})
    return {"populations": results, "paired_deltas": deltas,
            "disk_attribution": "retain raw categories at before_startup/after_observation/after_maintenance/after_snapshot/after_shutdown",
            "queue_telemetry": "raw producer counts/bytes/wait/backpressure; absent is unmeasured",
            "limitations": ["sampled process-tree RSS is a lower bound", "shared pages can occur in multiple RSS values",
                            "20 cold trials give a p99 equal to the maximum", "phase spans may share an attempt; they are not extra workload executions"]}
