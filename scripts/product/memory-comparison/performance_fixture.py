"""Deterministic provider-neutral performance payloads, fixed before execution."""

import hashlib


def record(index, payload_bytes, *, prefix):
    header = f"{prefix} record {index:03d}: preserve the settled checkpoint. "
    if len(header.encode()) > payload_bytes:
        raise ValueError("performance fixture header exceeds declared payload")
    filler = "Stable canonical evidence for the bounded recall workload. "
    content = (header + filler * (payload_bytes // len(filler) + 1))[:payload_bytes]
    return {"source_id": f"{prefix}/{index}", "revision": "performance-r1", "sequence": index + 1,
            "origin": {"project": "comparison-performance", "repository": "comparison-performance-repo",
                       "worktree": "main-checkout", "branch": "main", "session": "session-a" if index % 2 == 0 else "session-b"},
            "observed_at": "2025-01-01T00:00:01Z", "valid_from": "2025-01-01T00:00:01Z", "valid_until": None,
            "content": content, "payload_bytes": payload_bytes,
            "content_sha256": hashlib.sha256(content.encode()).hexdigest()}


def fixed_fixtures():
    return {"format": "tracedecay.host-comparison.performance-fixtures.v1",
            "populated": [record(i, 4096, prefix="populated") for i in range(128)],
            "snapshot": [record(i, 1024, prefix="snapshot") for i in range(4)],
            "empty": [], "observe_payload_sizes": [64, 4096, 16384],
            "evaluation_time": "2025-01-01T01:00:00Z",
            "query": "Recall the settled checkpoint evidence for this session.",
            "source_policy": "balanced original sessions A/B; host authorizes history without relabeling origins",
            "snapshot_policy": "freeze measured preparation bytes before timed trials; never shrink on oversize",
            "cache_policy": "process cold only; do not purge filesystem caches or download models"}
