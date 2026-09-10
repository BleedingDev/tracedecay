"""Supplemental measurement summaries; quality metrics stay in the Rust evaluator.

Rows are planned attempts, not just successful samples. Never pool different
hosts, phases, populations, or warm-up states before calling these functions.
"""

import math
import random
import statistics

from capture import paired_delta


def nearest_rank(values, percentile):
    """Same ceil(p*n/100) convention as memory-evaluation::nearest_rank_percentile."""
    if not values:
        return None
    return sorted(values)[max(1, math.ceil(percentile * len(values) / 100)) - 1]


def distribution(values):
    if not values:
        return {"n": 0, "min": None, "max": None, "p50": None, "p95": None,
                "p99": None, "mad": None, "iqr": None}
    median = statistics.median(values)
    return {"n": len(values), "min": min(values), "max": max(values),
            **{f"p{p}": nearest_rank(values, p) for p in (50, 95, 99)},
            "mad": statistics.median(abs(v - median) for v in values),
            "iqr": nearest_rank(values, 75) - nearest_rank(values, 25)}


def summarize_attempts(rows, *, target_ns=None, required_completed=0):
    """Terminal subclasses overlap completion counts; publish the cross-tab too.

    Success-target ranks cover ALL scheduled attempts: known failures have
    infinite time-to-success; censored rows have [cutoff, infinity] bounds;
    unexecuted/missing-clock rows have [0, infinity]. Fast failures cannot win.
    """
    counts = dict.fromkeys(("planned", "attempted", "provider_contacted", "completed_success",
                           "completed_failure", "deadline", "cancelled", "censored",
                           "unexecuted", "warmup", "timing_unmeasured"), 0)
    cross, success, terminal_times, lower, upper = {}, [], [], [], []
    for row in rows:
        counts["planned"] += 1
        counts["warmup"] += bool(row.get("warmup"))
        attempted = row["status"] != "unexecuted"
        counts["attempted"] += attempted
        counts["unexecuted"] += not attempted
        counts["provider_contacted"] += row.get("provider_contacted") is True
        terminal = row.get("terminal")
        counts["censored"] += row["status"] == "censored"
        counts["deadline"] += terminal == "deadline_exceeded"
        counts["cancelled"] += terminal == "cancelled"
        elapsed = row.get("timing", {}).get("elapsed_ns")
        passed = row.get("successful_completion") is True and terminal in ("success", "partial", "success_zero_results")
        if row.get("requires_positive_recall"):
            passed &= row.get("positive_evidence_verified") is True
        counts["completed_success"] += passed
        counts["completed_failure"] += terminal is not None and not passed
        counts["timing_unmeasured"] += terminal is not None and elapsed is None
        key = f"{row['status']}:{terminal or 'no_terminal'}"
        cross[key] = cross.get(key, 0) + 1
        if elapsed is not None and terminal is not None:
            terminal_times.append(elapsed)
            if passed:
                success.append(elapsed)
        if passed and elapsed is not None:
            lower.append(elapsed); upper.append(elapsed)
        elif terminal is not None and not passed:
            lower.append(math.inf); upper.append(math.inf)
        else:
            lower.append(row.get("timing", {}).get("elapsed_at_cutoff_ns") or 0)
            upper.append(math.inf)
    target = {"status": "unmeasured", "ceiling_ns": target_ns}
    if target_ns is not None and rows:
        low, high = nearest_rank(lower, 95), nearest_rank(upper, 95)
        target = {"status": "fail" if low > target_ns else "pass" if high <= target_ns else "indeterminate",
                  "ceiling_ns": target_ns, "p95_lower_ns": low if math.isfinite(low) else None,
                  "p95_upper_ns": high if math.isfinite(high) else None,
                  "lower_infinite": not math.isfinite(low), "upper_infinite": not math.isfinite(high),
                  "successful_within_target": sum(v <= target_ns for v in success),
                  "scheduled_denominator": len(rows)}
    return {"counts": counts, "terminal_cross_tab": cross,
            "successful_completion_latency_ns": distribution(success),
            "completed_terminal_latency_ns": distribution(terminal_times),
            "steady_sample_support": "not_applicable" if not required_completed else "complete" if len(success) >= required_completed else "incomplete",
            "success_target": target}


def wilson(passes, total):
    if not 0 <= passes <= total:
        raise ValueError("invalid proportion")
    if not total:
        return {"numerator": passes, "denominator": total, "interval_95": None}
    z, p = 1.959963984540054, passes / total
    scale = 1 + z * z / total
    center = (p + z * z / (2 * total)) / scale
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / scale
    return {"numerator": passes, "denominator": total, "interval_95": [center - radius, center + radius]}


def paired_block_difference(left, right, *, seed, resamples=2000):
    """Resample complete paired blocks, retaining every within-block value.

    Caller supplies one host and one phase/population, keyed by whole case or
    process block. Missing pairs remain explicit and disable a comparative CI.
    Values may be negative (e.g. a provider's RSS below its control).
    """
    keys = sorted(set(left) | set(right))
    missing = [key for key in keys if not left.get(key) or not right.get(key)
               or len(left[key]) != len(right[key])
               or any(v is None for v in left[key] + right[key])]
    if not keys or missing:
        return {"status": "incomplete", "missing_blocks": missing, "planned_blocks": len(keys)}
    deltas = [[paired_delta(a, b) for a, b in zip(left[k], right[k])] for k in keys]
    rng = random.Random(seed)
    samples = [statistics.mean(v for block in rng.choices(deltas, k=len(deltas)) for v in block)
               for _ in range(resamples)]
    return {"status": "measured", "blocks": len(keys), "seed": seed, "resamples": resamples,
            "difference": statistics.mean(v for block in deltas for v in block),
            "interval_95": [nearest_rank(samples, 2.5), nearest_rank(samples, 97.5)]}


def paired_ratio_difference(left, right, *, seed, resamples=2000):
    """Bootstrap pooled existing-metric (numerator, denominator) case blocks.

    Zero-denominator resamples remain explicitly unresolved. Never average
    per-case ratios to replace the evaluator's pooled precision definition.
    """
    keys = sorted(set(left) | set(right))
    missing = [k for k in keys if left.get(k) is None or right.get(k) is None]
    if not keys or missing:
        return {"status": "incomplete", "missing_blocks": missing, "planned_blocks": len(keys)}
    def difference(selected):
        a = [left[k] for k in selected]; b = [right[k] for k in selected]
        da, db = sum(v[1] for v in a), sum(v[1] for v in b)
        return sum(v[0] for v in a) / da - sum(v[0] for v in b) / db if da and db else None
    rng = random.Random(seed)
    values = [difference(rng.choices(keys, k=len(keys))) for _ in range(resamples)]
    measured = [v for v in values if v is not None]
    return {"status": "measured" if len(measured) == resamples else "incomplete",
            "blocks": len(keys), "seed": seed, "resamples": resamples,
            "unresolved_resamples": resamples - len(measured), "difference": difference(keys),
            "interval_95": [nearest_rank(measured, 2.5), nearest_rank(measured, 97.5)]
            if len(measured) == resamples else None}
