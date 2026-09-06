#!/usr/bin/env python3
"""Print the compact budget view from an ncm-rs-020 raw result."""

import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text())
print(f"status={result['status']} commit={result['commit']} host={result['hardware']['hostname']}")
for run in result["runs"]:
    payload = run["result"]
    population = payload["population"]
    peak = run.get("peak_rss_bytes")
    print(f"{population}: wall={run['wall_seconds']:.3f}s peak_rss={peak}")
    data = payload.get("data", {})
    measurements = data.get("measurements", [])
    if "measurement" in data:
        measurements = [data["measurement"]]
    for measurement in measurements:
        if isinstance(measurement, dict) and "distribution" in measurement:
            dist = measurement["distribution"]
            print(f"  {measurement['name']}: p50={dist['p50_us']:.3f}us p95={dist['p95_us']:.3f}us p99={dist['p99_us']:.3f}us")
    for item in data.get("populations", []):
        for measurement in item.get("measurements", []):
            dist = measurement["distribution"]
            print(f"  {item['population']}/{measurement['name']}: p95={dist['p95_us']:.3f}us p99={dist['p99_us']:.3f}us")
