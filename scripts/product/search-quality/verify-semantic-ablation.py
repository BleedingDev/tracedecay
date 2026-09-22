#!/usr/bin/env python3
"""Independently verify production semantic ablation evidence.

The verifier deliberately has no ranking oracle. It validates that a report's
rankings came from the pinned production authority receipt, that candidate
metadata is bound to the supplied corpus bytes, and that the fixed lifecycle
matrix and quality gates are complete. Labels are loaded only by this scoring
boundary; they are never an input to production execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


TOP_K = 10
MIN_CANDIDATES = 10
MIN_DECOYS = 10
REPETITIONS = 10
RECALL_THRESHOLD_PPM = 800_000
PRECISION_THRESHOLD_PPM = 500_000
SCHEMA_VERSION = 1
MODEL_MANIFEST_RELATIVE = "product/semantic/model-manifest.json"
MODEL_MANIFEST_DIGEST = "sha256:002759bbdc40f06fe8f67401fff11e5fe0235a6466c8bcf395749131f9721499"
AUTHORITY_ID = "tracedecay.application.production-semantic-runtime.v1"
INDEX_KIND = "exact_flat"
CORPUS_DOMAIN = "tracedecay.search-eval.semantic-ablation.corpus.v1"
WORKLOAD_DOMAIN = "tracedecay.search-eval.semantic-ablation.workload.v1"
LABEL_DOMAIN = "tracedecay.search-eval.semantic-ablation.labels.v1"
SOURCE_DOMAIN = "tracedecay.search-eval.semantic-ablation.source.v1"
PROJECTION_DOMAIN = "tracedecay.search-eval.semantic-ablation.projection-material.v1"
VECTOR_DOMAIN = "tracedecay.search-eval.semantic-ablation.vector-material.v1"
ARTIFACT_DOMAIN = "tracedecay.search-eval.semantic-ablation.artifact.v1"
RANKING_DOMAIN = "tracedecay.search-eval.semantic-ablation.ranking.v1"
AUTHORITY_DOMAIN = "tracedecay.search-eval.semantic-ablation.authority.v1"
LIFECYCLE_DOMAIN = "tracedecay.search-eval.semantic-ablation.lifecycle.v1"
RECEIPT_DOMAIN = "tracedecay.search-eval.semantic-ablation.production-receipt.v1"
OUTPUT_DOMAIN = "tracedecay.search-eval.semantic-ablation.output.v1"
MATRIX_DOMAIN = "tracedecay.search-eval.semantic-ablation.matrix.v1"
CANDIDATE_RECORD_DOMAIN = "tracedecay.search-eval.semantic-ablation.candidate-record.v1"
PINNED_DIGESTS = {
    "workload_digest": "sha256:080be3856041722c34fd6ee40b76006d3513af2d34e40e13b15f6eb8db163627",
    "corpus_digest": "sha256:caf2666f212326415d77d657faf5acd38ce8fcb90a140b73ed0480bd8eaa89ed",
    "labels_digest": "sha256:71976cceef507d6ab8dc1c66a207ce34400ef145534a7f2f5af5444b356532fc",
    "model_digest": "sha256:70be81163e9740d742b7857e132713b323b5042d661485354d781cb8313c15af",
    "projection_digest": "sha256:02993e5b6c6cfab94f0383c7e2365e72fda07b8c7217d5f0bf54ac7ae4902eae",
    "vector_generation_digest": "sha256:8448e2a0d726d1b82be899155f3e139d9f80e5f2d32dca54cbe063471ed18cfa",
    "index_digest": "sha256:2d5377d189e2d2f5231710a27aaa18dc12774ccaa996ad40e95e74277814779f",
    "source_digest": "sha256:5a4dada128ff30e646e513bc0047f35a9a4b22631ee5271140caa8233ba5a14a",
    "artifact_manifest_digest": "sha256:c18d3e5eda3994bd4192c604dbf682fdab26aa9d86b3b340ac54cca1a5e4c825",
}
MODES = ("lexical_baseline", "semantic_only", "hybrid")
KINDS = ("cold", "warm", "restart")


def canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest(domain: str, value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical([domain, value])).hexdigest()


def tagged_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"read/parse {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def exact_keys(value: Any, keys: set[str], label: str) -> None:
    require(isinstance(value, dict), f"{label} must be an object")
    require(set(value) == keys, f"{label} has unexpected or missing fields")


def is_digest(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def split_subtokens(token: str) -> list[str]:
    result: list[str] = []
    for segment in re.split(r"[:./-]", token):
        current = ""
        previous: str | None = None
        for character in segment:
            boundary = (
                (previous == "_")
                or (character == "_")
                or (
                    previous is not None
                    and previous.islower()
                    and character.isupper()
                )
                or (
                    previous is not None
                    and previous.isascii()
                    and character.isascii()
                    and previous.isdigit() != character.isdigit()
                )
            )
            if boundary and current:
                result.append(current.lower())
                current = ""
            if character != "_":
                current += character
            previous = character
        if current:
            result.append(current.lower())
    return result


def production_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for chunk in re.findall(r"[A-Za-z0-9_]+", value):
        tokens.update(token for token in split_subtokens(chunk) if token)
    return tokens


def ratio(numerator: int, denominator: int) -> dict[str, int]:
    ppm = 0 if denominator == 0 else min(1_000_000, numerator * 1_000_000 // denominator)
    return {"numerator": numerator, "denominator": denominator, "ppm": ppm}


def at_least(metric: dict[str, int], threshold: int) -> bool:
    return metric["numerator"] * 1_000_000 >= metric["denominator"] * threshold


def expected_provenance(identity: dict[str, Any]) -> dict[str, Any]:
    return {
        field: identity[field]
        for field in (
            "model_id",
            "model_revision",
            "model_artifact_digest",
            "projection_id",
            "projection_revision",
            "projection_artifact_digest",
            "vector_generation_id",
            "vector_generation_revision",
            "vector_artifact_digest",
            "index_id",
            "index_revision",
            "index_artifact_digest",
        )
    }


def validate_model_manifest(
    artifact: dict[str, Any], manifest_bytes: bytes
) -> None:
    require(
        artifact["model_manifest_path"] == MODEL_MANIFEST_RELATIVE,
        "artifact does not name the product model manifest",
    )
    require(
        artifact["model_manifest_digest"] == MODEL_MANIFEST_DIGEST
        and tagged_digest(manifest_bytes) == artifact["model_manifest_digest"],
        "product model manifest bytes do not match the pinned digest",
    )
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"parse product model manifest: {error}") from error
    require(isinstance(manifest, dict), "product model manifest is not an object")
    identity = artifact["identity"]
    require(manifest.get("schema") == "tracedecay.distribution.fastembed-fixture.v1", "model manifest schema mismatch")
    require(manifest.get("model") == identity["model_id"], "model manifest model mismatch")
    require(
        isinstance(manifest.get("source"), dict)
        and manifest["source"].get("revision") == identity["model_revision"],
        "model manifest revision mismatch",
    )
    require(
        "sha256:" + str(manifest.get("artifact_digest")) == identity["model_artifact_digest"],
        "model manifest artifact mismatch",
    )
    require(manifest.get("expected_dimensions") == 768, "model manifest dimensions mismatch")
    require(manifest.get("max_length") == 8192, "model manifest max length mismatch")


def candidate_record_digest(
    candidate: dict[str, Any], document: dict[str, Any]
) -> str:
    return digest(
        CANDIDATE_RECORD_DOMAIN,
        [
            candidate["candidate_id"],
            candidate["document_id"],
            document["path"],
            document["content_digest"],
            candidate["scope"],
            candidate["source_path"],
            candidate["symbol"],
            candidate["aliases"],
            candidate["terms"],
            candidate["subtokens"],
        ],
    )


def validate_inputs(
    workload: dict[str, Any],
    labels: dict[str, Any],
    artifact: dict[str, Any],
    corpus: bytes,
    manifest_bytes: bytes,
) -> dict[str, str]:
    exact_keys(
        workload,
        {
            "schema_version",
            "workload_id",
            "source_repository_commit",
            "source_repository_tree",
            "corpus",
            "candidates",
            "queries",
            "repetition_contract",
            "quality_contract",
            "expected_artifact_identity",
        },
        "workload",
    )
    require(workload.get("schema_version") == SCHEMA_VERSION, "unsupported workload schema")
    require(isinstance(workload.get("workload_id"), str) and workload["workload_id"], "empty workload id")
    for field in ("source_repository_commit", "source_repository_tree"):
        require(
            isinstance(workload.get(field), str)
            and re.fullmatch(r"[0-9a-f]{40}", workload[field]) is not None,
            f"invalid source repository {field}",
        )
    queries = workload.get("queries")
    candidates = workload.get("candidates")
    documents = workload.get("corpus")
    require(isinstance(queries, list) and queries, "workload has no queries")
    require(isinstance(candidates, list) and candidates, "workload has no candidates")
    require(isinstance(documents, list) and documents, "workload has no corpus")

    quality = workload.get("quality_contract")
    exact_keys(
        quality,
        {
            "top_k",
            "minimum_candidates_per_query",
            "minimum_relevant_candidates_per_query",
            "recall_at_10_threshold_ppm",
            "precision_at_10_threshold_ppm",
            "fixed_precision_denominator",
        },
        "quality contract",
    )
    require(
        quality["top_k"] == TOP_K
        and isinstance(quality["minimum_candidates_per_query"], int)
        and quality["minimum_candidates_per_query"] >= MIN_CANDIDATES
        and isinstance(quality["minimum_relevant_candidates_per_query"], int)
        and quality["minimum_relevant_candidates_per_query"] > 0
        and quality["recall_at_10_threshold_ppm"] == RECALL_THRESHOLD_PPM
        and quality["precision_at_10_threshold_ppm"] == PRECISION_THRESHOLD_PPM
        and quality["fixed_precision_denominator"] is True,
        "quality contract is not the fixed gate",
    )
    repetitions = workload.get("repetition_contract")
    exact_keys(repetitions, set(KINDS), "repetition contract")
    require(
        all(repetitions.get(kind) == REPETITIONS for kind in KINDS),
        "cold, warm, and restart repetitions must each be exactly ten",
    )

    document_ids = [document.get("document_id") for document in documents]
    require(len(set(document_ids)) == len(document_ids), "duplicate corpus document id")
    document_by_id: dict[str, dict[str, Any]] = {}
    corpus_paths: set[str] = set()
    corpus_bindings: list[Any] = []
    actual_corpus_digest = tagged_digest(corpus)
    for document in documents:
        exact_keys(document, {"document_id", "path", "scope", "language", "eligibility", "content_digest"}, "corpus document")
        for field in ("document_id", "path", "scope", "language", "eligibility"):
            require(isinstance(document.get(field), str) and document[field].strip(), f"corpus document has invalid {field}")
        require(is_digest(document.get("content_digest")), f"corpus document {document['document_id']} has invalid content digest")
        require(document["content_digest"] == actual_corpus_digest, f"corpus document {document['document_id']} bytes mismatch")
        require(document["path"] not in corpus_paths, f"duplicate corpus path {document['path']}")
        corpus_paths.add(document["path"])
        document_by_id[document["document_id"]] = document
        corpus_bindings.append([document, actual_corpus_digest])

    candidate_ids = [candidate.get("candidate_id") for candidate in candidates]
    require(all(isinstance(candidate_id, str) and candidate_id.strip() for candidate_id in candidate_ids), "invalid candidate id")
    require(len(set(candidate_ids)) == len(candidate_ids), "duplicate candidate id")
    for candidate in candidates:
        exact_keys(
            candidate,
            {
                "candidate_id",
                "document_id",
                "scope",
                "source_path",
                "symbol",
                "aliases",
                "terms",
                "subtokens",
                "record_digest",
            },
            "workload candidate",
        )
        document = document_by_id.get(candidate.get("document_id"))
        require(document is not None, f"candidate {candidate.get('candidate_id')} cites unknown corpus document")
        require(candidate["scope"] == document["scope"], f"candidate {candidate['candidate_id']} scope is not bound to its document")
        for field in ("scope", "source_path", "symbol"):
            require(isinstance(candidate.get(field), str) and candidate[field].strip(), f"candidate {candidate.get('candidate_id')} has invalid {field}")
        for field in ("aliases", "terms", "subtokens"):
            require(
                isinstance(candidate.get(field), list)
                and candidate[field]
                and all(isinstance(value, str) and value.strip() for value in candidate[field]),
                f"candidate {candidate.get('candidate_id')} has invalid {field}",
            )
        require(is_digest(candidate.get("record_digest")), f"candidate {candidate['candidate_id']} has invalid record digest")
        require(candidate["record_digest"] == candidate_record_digest(candidate, document), f"candidate {candidate['candidate_id']} record digest is forged")
        for field in ("source_path", "symbol"):
            require(candidate[field].encode("utf-8") in corpus, f"candidate {candidate['candidate_id']} {field} is not bound to corpus bytes")

    query_ids = [query.get("query_id") for query in queries]
    require(all(isinstance(query_id, str) and query_id.strip() for query_id in query_ids), "invalid query id")
    require(len(set(query_ids)) == len(query_ids), "duplicate query id")
    for query in queries:
        exact_keys(query, {"query_id", "partition", "stratum", "query", "allowed_scopes"}, "workload query")
        require(query["partition"] in ("train", "validation"), f"unknown partition for {query['query_id']}")
        require(isinstance(query["stratum"], str) and query["stratum"].strip(), f"query {query['query_id']} has no stratum")
        require(isinstance(query["query"], str) and query["query"].strip(), f"query {query['query_id']} has no text")
        require(isinstance(query["allowed_scopes"], list) and query["allowed_scopes"] and all(isinstance(scope, str) and scope.strip() for scope in query["allowed_scopes"]), f"query {query['query_id']} has no allowed scopes")
        eligible = sum(candidate["scope"] in query["allowed_scopes"] for candidate in candidates)
        require(eligible >= quality["minimum_candidates_per_query"], f"query {query['query_id']} has too few eligible candidates")

    # Run the same tokenizer as production over every query/candidate metadata
    # field. This check remains independent of labels and ranking output.
    for query in queries:
        query_tokens = production_tokens(query["query"])
        for candidate in candidates:
            fields = {
                "candidate_id": candidate["candidate_id"],
                "terms": " ".join(candidate["terms"]),
                "subtokens": " ".join(candidate["subtokens"]),
                "path": candidate["source_path"],
                "symbol": candidate["symbol"],
                "aliases": " ".join(candidate["aliases"]),
            }
            for field, value in fields.items():
                overlap = sorted(query_tokens & production_tokens(value))
                require(not overlap, f"{query['query_id']} overlaps {candidate['candidate_id']} {field}: {overlap}")

    exact_keys(labels, {"schema_version", "workload_id", "label_set_id", "labels"}, "labels")
    require(labels.get("schema_version") == SCHEMA_VERSION, "unsupported labels schema")
    require(labels.get("workload_id") == workload["workload_id"], "labels do not bind workload")
    require(isinstance(labels.get("label_set_id"), str) and labels["label_set_id"].strip(), "labels have no label-set identity")
    label_rows = labels.get("labels")
    require(isinstance(label_rows, list) and len(label_rows) == len(queries), "labels do not cover every query")
    require(len({row.get("query_id") for row in label_rows}) == len(label_rows), "duplicate label query")
    query_set = set(query_ids)
    candidate_set = set(candidate_ids)
    labels_by_query: dict[str, dict[str, Any]] = {}
    for row in label_rows:
        exact_keys(row, {"query_id", "target_candidate_ids", "forbidden_candidate_ids"}, "label row")
        query_id = row["query_id"]
        targets = row["target_candidate_ids"]
        forbidden = row["forbidden_candidate_ids"]
        require(query_id in query_set, f"label cites unknown query {query_id}")
        require(isinstance(targets, list) and len(targets) >= quality["minimum_relevant_candidates_per_query"], f"{query_id} has too few targets")
        require(isinstance(forbidden, list), f"{query_id} has invalid forbidden labels")
        require(all(isinstance(candidate_id, str) and candidate_id for candidate_id in targets + forbidden), f"{query_id} has invalid candidate labels")
        require(len(set(targets)) == len(targets) and len(set(forbidden)) == len(forbidden), f"duplicate labels for {query_id}")
        require(set(targets) <= candidate_set and set(forbidden) <= candidate_set, f"{query_id} labels unknown candidate")
        require(set(targets).isdisjoint(forbidden), f"{query_id} target is forbidden")
        query = next(query for query in queries if query["query_id"] == query_id)
        eligible = sum(candidate["scope"] in query["allowed_scopes"] for candidate in candidates)
        require(eligible >= len(targets) + MIN_DECOYS, f"{query_id} has fewer than {MIN_DECOYS} eligible decoys")
        require(
            all(
                next(candidate for candidate in candidates if candidate["candidate_id"] == target)["scope"]
                in query["allowed_scopes"]
                for target in targets
            ),
            f"{query_id} labels an out-of-scope target",
        )
        labels_by_query[query_id] = row
    require(set(labels_by_query) == query_set, "labels do not cover exactly the workload")

    exact_keys(
        artifact,
        {
            "schema_version",
            "workload_id",
            "workload_digest",
            "model_manifest_path",
            "model_manifest_digest",
            "authority_id",
            "search_index_kind",
            "index_manifest_digest",
            "identity",
        },
        "artifact",
    )
    exact_keys(
        artifact.get("identity"),
        {
            "model_id",
            "model_revision",
            "model_artifact_digest",
            "projection_id",
            "projection_revision",
            "projection_artifact_digest",
            "vector_generation_id",
            "vector_generation_revision",
            "vector_artifact_digest",
            "index_id",
            "index_revision",
            "index_artifact_digest",
            "manifest_digest",
        },
        "artifact identity",
    )
    require(artifact["schema_version"] == SCHEMA_VERSION, "unsupported artifact schema")
    require(artifact["workload_id"] == workload["workload_id"], "artifact does not bind workload")
    require(artifact["authority_id"] == AUTHORITY_ID, "artifact authority is not production semantic runtime")
    require(artifact["search_index_kind"] == INDEX_KIND, "artifact index kind is not exact flat")
    require(is_digest(artifact["model_manifest_digest"]), "invalid model manifest digest")
    require(is_digest(artifact["index_manifest_digest"]), "invalid index manifest digest")
    identity = artifact["identity"]
    require(identity == workload["expected_artifact_identity"], "artifact identity does not match workload pin")
    require(artifact["index_manifest_digest"] == identity["index_artifact_digest"], "index manifest is not bound to immutable index identity")
    for field in ("model_id", "model_revision", "projection_id", "projection_revision", "vector_generation_id", "vector_generation_revision", "index_id", "index_revision"):
        require(isinstance(identity[field], str) and identity[field].strip(), f"empty artifact identity {field}")
    for field in ("model_artifact_digest", "projection_artifact_digest", "vector_artifact_digest", "index_artifact_digest", "manifest_digest"):
        require(is_digest(identity[field]), f"invalid artifact digest {field}")
    validate_model_manifest(artifact, manifest_bytes)

    corpus_digest = digest(CORPUS_DOMAIN, corpus_bindings)
    model_digest = identity["model_artifact_digest"]
    projection_digest = digest(PROJECTION_DOMAIN, [identity["projection_id"], identity["projection_revision"], model_digest, artifact["model_manifest_digest"]])
    vector_digest = digest(
        VECTOR_DOMAIN,
        [
            identity["vector_generation_id"],
            identity["vector_generation_revision"],
            projection_digest,
            [[document["document_id"], document["path"], document["content_digest"]] for document in documents],
        ],
    )
    # The index manifest digest is the immutable production index identity. A
    # second digest derived from it would be self-referential and would not
    # identify the bytes the vector reader opened.
    index_digest = identity["index_artifact_digest"]
    manifest_digest = digest(
        ARTIFACT_DOMAIN,
        {
            "workload_id": workload["workload_id"],
            "model_manifest_path": artifact["model_manifest_path"],
            "model_manifest_digest": artifact["model_manifest_digest"],
            "authority_id": artifact["authority_id"],
            "search_index_kind": artifact["search_index_kind"],
            "index_manifest_digest": artifact["index_manifest_digest"],
            "model_id": identity["model_id"],
            "model_revision": identity["model_revision"],
            "model_artifact_digest": model_digest,
            "projection_id": identity["projection_id"],
            "projection_revision": identity["projection_revision"],
            "projection_artifact_digest": projection_digest,
            "vector_generation_id": identity["vector_generation_id"],
            "vector_generation_revision": identity["vector_generation_revision"],
            "vector_artifact_digest": vector_digest,
            "index_id": identity["index_id"],
            "index_revision": identity["index_revision"],
            "index_artifact_digest": index_digest,
        },
    )
    require(identity["model_artifact_digest"] == model_digest, "model artifact digest mismatch")
    require(identity["projection_artifact_digest"] == projection_digest, "projection artifact digest mismatch")
    require(identity["vector_artifact_digest"] == vector_digest, "vector artifact digest mismatch")
    require(identity["index_artifact_digest"] == index_digest, "index artifact digest mismatch")
    require(identity["manifest_digest"] == manifest_digest, "artifact manifest digest mismatch")

    workload_digest = digest(WORKLOAD_DOMAIN, workload)
    require(workload_digest == PINNED_DIGESTS["workload_digest"], "workload digest differs from evaluator freeze")
    require(artifact["workload_digest"] == workload_digest, "artifact workload digest mismatch")
    labels_digest = digest(LABEL_DOMAIN, labels)
    source_digest = digest(
        SOURCE_DOMAIN,
        [
            workload["source_repository_commit"],
            workload["source_repository_tree"],
            workload_digest,
            corpus_digest,
        ],
    )
    result = {
        "workload_digest": workload_digest,
        "corpus_digest": corpus_digest,
        "labels_digest": labels_digest,
        "model_digest": model_digest,
        "projection_digest": projection_digest,
        "vector_generation_digest": vector_digest,
        "index_digest": index_digest,
        "source_digest": source_digest,
        "artifact_manifest_digest": manifest_digest,
    }
    require(result == PINNED_DIGESTS, "semantic input digest differs from evaluator freeze")
    return result


def run_key(run: dict[str, Any]) -> tuple[Any, ...]:
    return (
        MODES.index(run.get("mode")),
        run.get("partition", ""),
        run.get("query_id", ""),
        run.get("stratum", ""),
        KINDS.index(run.get("repetition_kind")),
        run.get("repetition_index", -1),
    )


def validate_lifecycle_matrix(runs: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for run in runs:
        groups.setdefault((run["mode"], run["query_id"]), {}).setdefault(run["repetition_kind"], []).append(run["lifecycle"]["runtime_identity"])
    for (mode, query_id), by_kind in groups.items():
        cold = by_kind.get(KINDS[0])
        warm = by_kind.get(KINDS[1])
        restart = by_kind.get(KINDS[2])
        require(cold is not None, f"{mode}/{query_id} has no cold lifecycle evidence")
        require(warm is not None, f"{mode}/{query_id} has no warm lifecycle evidence")
        require(restart is not None, f"{mode}/{query_id} has no restart lifecycle evidence")
        cold_ids = set(cold)
        warm_ids = set(warm)
        restart_ids = set(restart)
        require(len(cold_ids) == 1 and len(warm_ids) == 1 and cold_ids == warm_ids, f"{mode}/{query_id} does not prove warm reuse")
        require(len(restart_ids) == len(restart), f"{mode}/{query_id} restart repetitions reused a runtime identity")
        require(restart_ids.isdisjoint(cold_ids) and restart_ids.isdisjoint(warm_ids), f"{mode}/{query_id} restart identity was reused")


def verify_report(
    report: dict[str, Any],
    workload: dict[str, Any],
    labels: dict[str, Any],
    artifact: dict[str, Any],
    digests: dict[str, str],
) -> None:
    exact_keys(
        report,
        {
            "schema_version",
            "command",
            "status",
            "workload_digest",
            "corpus_digest",
            "labels_digest",
            "source_digest",
            "model_digest",
            "projection_digest",
            "vector_generation_digest",
            "index_digest",
            "artifact_manifest_digest",
            "artifact_identity",
            "output_digest",
            "matrix_digest",
            "runs",
            "modes",
        },
        "report",
    )
    require(report["schema_version"] == SCHEMA_VERSION and report["command"] == "semantic_ablation", "report schema or command mismatch")
    for field, value in digests.items():
        require(report.get(field) == value, f"report {field} mismatch")
    require(report["artifact_identity"] == artifact["identity"], "report artifact identity mismatch")
    runs = report["runs"]
    require(isinstance(runs, list) and runs, "report has no runs")
    require(runs == sorted(runs, key=run_key), "report runs are not deterministically sorted")
    queries = {query["query_id"]: query for query in workload["queries"]}
    labels_by_query = {row["query_id"]: row for row in labels["labels"]}
    candidates = {candidate["candidate_id"]: candidate for candidate in workload["candidates"]}
    expected_authority_digest = digest(AUTHORITY_DOMAIN, artifact["identity"])
    seen: set[tuple[Any, ...]] = set()
    stable: dict[tuple[str, str], str] = {}

    for run in runs:
        run_id = run.get("run_id", "<missing>")
        run_keys = {
            "schema_version", "run_id", "query_id", "partition", "stratum", "mode",
            "repetition_kind", "repetition_index", "workload_digest", "corpus_digest",
            "labels_digest", "model_digest", "projection_digest", "vector_generation_digest",
            "index_digest", "source_digest", "artifact_manifest_digest", "artifact_status",
            "artifact_mismatch", "fallback_used", "disabled_lanes", "invocation_counters",
            "authority_identity_digest", "lifecycle", "latency_micros",
            "production_receipt_digest", "ranking_digest", "ranked_candidates", "quality", "status",
        }
        require(set(run) in (run_keys, run_keys | {"failure"}), f"run {run_id} has unexpected or missing fields")
        require(run["schema_version"] == SCHEMA_VERSION, f"run {run_id} schema mismatch")
        query = queries.get(run.get("query_id"))
        require(query is not None, f"unknown query {run.get('query_id')}")
        require(run["partition"] == query["partition"] and run["stratum"] == query["stratum"], f"run {run_id} query binding mismatch")
        require(run["mode"] in MODES and run["repetition_kind"] in KINDS, f"run {run_id} mode/lifecycle mismatch")
        require(isinstance(run["repetition_index"], int) and 0 <= run["repetition_index"] < REPETITIONS, f"run {run_id} repetition index mismatch")
        expected_run_id = f"{run['mode']}/{run['partition']}/{run['query_id']}/{run['repetition_kind']}/{run['repetition_index']:02d}"
        require(run["run_id"] == expected_run_id, f"run {run_id} repetition identity mismatch")
        key = (run["mode"], run["query_id"], run["repetition_kind"], run["repetition_index"])
        require(key not in seen, f"duplicate repetition {key}")
        seen.add(key)
        for field, value in digests.items():
            require(run.get(field) == value, f"run {run_id} {field} mismatch")
        require(run["artifact_status"] == "verified" and run["artifact_mismatch"] is False and run["fallback_used"] is False, f"run {run_id} used fallback or mismatched artifact")
        require("failure" not in run or run["failure"] is None, f"run {run_id} recorded an execution failure")
        require(isinstance(run["latency_micros"], int) and run["latency_micros"] > 0, f"run {run_id} has no measured latency")
        require(run["authority_identity_digest"] == expected_authority_digest, f"run {run_id} foreign authority identity")

        lifecycle = run["lifecycle"]
        exact_keys(lifecycle, {"phase", "runtime_identity", "cache_reset", "runtime_restart", "lifecycle_receipt_digest"}, f"run {run_id} lifecycle")
        require(lifecycle["phase"] == run["repetition_kind"] and is_digest(lifecycle["runtime_identity"]), f"run {run_id} malformed lifecycle identity")
        require(lifecycle["lifecycle_receipt_digest"] == digest(LIFECYCLE_DOMAIN, [lifecycle["phase"], lifecycle["runtime_identity"], lifecycle["cache_reset"], lifecycle["runtime_restart"]]), f"run {run_id} lifecycle receipt mismatch")
        if run["repetition_kind"] == "cold":
            require(lifecycle["cache_reset"] is True and lifecycle["runtime_restart"] is False, f"run {run_id} is not cold")
        elif run["repetition_kind"] == "warm":
            require(lifecycle["cache_reset"] is False and lifecycle["runtime_restart"] is False, f"run {run_id} is not warm")
        else:
            require(lifecycle["cache_reset"] is True and lifecycle["runtime_restart"] is True, f"run {run_id} is not restart")

        counters = run["invocation_counters"]
        exact_keys(counters, {"exact", "lexical", "graph", "semantic"}, f"run {run_id} invocation counters")
        require(all(isinstance(counters[field], int) and counters[field] >= 0 for field in counters), f"run {run_id} invalid invocation counter")
        if run["mode"] == "lexical_baseline":
            require(counters["exact"] > 0 and counters["lexical"] > 0 and counters["graph"] > 0 and counters["semantic"] == 0, f"run {run_id} lexical baseline lane controls mismatch")
            expected_disabled, channel = ["semantic"], "lexical"
        elif run["mode"] == "semantic_only":
            require(counters["exact"] == 0 and counters["lexical"] == 0 and counters["graph"] == 0 and counters["semantic"] > 0, f"run {run_id} semantic-only invoked a disabled lane")
            expected_disabled, channel = ["exact", "lexical", "graph"], "semantic"
        else:
            require(all(counters[field] > 0 for field in ("exact", "lexical", "graph", "semantic")), f"run {run_id} hybrid lane controls mismatch")
            expected_disabled, channel = [], "hybrid"
        require(run["disabled_lanes"] == expected_disabled, f"run {run_id} disabled lanes mismatch")

        ranked = run["ranked_candidates"]
        require(isinstance(ranked, list) and len(ranked) >= MIN_CANDIDATES, f"run {run_id} has too few retained candidates")
        expected_prov = expected_provenance(artifact["identity"])
        seen_candidates: set[str] = set()
        previous_score = 1_000_001
        for index, row in enumerate(ranked):
            base_keys = {"candidate_id", "document_id", "source_path", "symbol", "record_digest", "rank", "score_ppm", "channel"}
            optional_keys = {"semantic_distance_micros", "provenance"}
            expected_keys = base_keys if run["mode"] == "lexical_baseline" else base_keys | optional_keys
            exact_keys(row, expected_keys, f"run {run_id} candidate {index + 1}")
            candidate = candidates.get(row.get("candidate_id"))
            require(candidate is not None, f"run {run_id} cites unknown candidate")
            require(row["candidate_id"] not in seen_candidates, f"run {run_id} duplicates candidate")
            seen_candidates.add(row["candidate_id"])
            require(row["rank"] == index + 1 and isinstance(row["score_ppm"], int) and 0 <= row["score_ppm"] <= 1_000_000 and row["score_ppm"] <= previous_score, f"run {run_id} ranking order is malformed")
            previous_score = row["score_ppm"]
            require(row["document_id"] == candidate["document_id"] and row["source_path"] == candidate["source_path"] and row["symbol"] == candidate["symbol"] and row["record_digest"] == candidate["record_digest"], f"run {run_id} candidate metadata is forged")
            require(candidate["scope"] in query["allowed_scopes"], f"run {run_id} ranking escapes query scope")
            require(row["channel"] == channel, f"run {run_id} candidate channel mismatch")
            if run["mode"] == "lexical_baseline":
                require("provenance" not in row and "semantic_distance_micros" not in row, f"run {run_id} lexical baseline carries semantic evidence")
            else:
                require(isinstance(row.get("semantic_distance_micros"), int) and row["semantic_distance_micros"] >= 0, f"run {run_id} semantic distance is missing")
                exact_keys(row["provenance"], set(expected_prov), f"run {run_id} provenance")
                require(row["provenance"] == expected_prov, f"run {run_id} semantic provenance mismatch")

        ranking_digest = digest(RANKING_DOMAIN, ranked)
        require(run["ranking_digest"] == ranking_digest, f"run {run_id} ranking digest mismatch")
        stable_key = (run["mode"], run["query_id"])
        if stable_key in stable:
            require(stable[stable_key] == ranking_digest, f"run {run_id} ranking is not byte-stable")
        stable[stable_key] = ranking_digest

        receipt_result = {
            "artifact_status": run["artifact_status"],
            "artifact_mismatch": run["artifact_mismatch"],
            "fallback_used": run["fallback_used"],
            "disabled_lanes": run["disabled_lanes"],
            "invocation_counters": run["invocation_counters"],
            "ranked_candidates": ranked,
        }
        expected_receipt = digest(
            RECEIPT_DOMAIN,
            {
                "identity": artifact["identity"],
                "workload_digest": run["workload_digest"],
                "corpus_digest": run["corpus_digest"],
                "query_id": query["query_id"],
                "partition": query["partition"],
                "stratum": query["stratum"],
                "query": query["query"],
                "allowed_scopes": query["allowed_scopes"],
                "mode": run["mode"],
                "repetition_kind": run["repetition_kind"],
                "repetition_index": run["repetition_index"],
                "lifecycle": lifecycle,
                "artifact_status": receipt_result["artifact_status"],
                "artifact_mismatch": receipt_result["artifact_mismatch"],
                "fallback_used": receipt_result["fallback_used"],
                "disabled_lanes": receipt_result["disabled_lanes"],
                "invocation_counters": receipt_result["invocation_counters"],
                "ranked_candidates": receipt_result["ranked_candidates"],
                "ranking_digest": ranking_digest,
            },
        )
        require(run["production_receipt_digest"] == expected_receipt, f"run {run_id} production receipt mismatch")

        label = labels_by_query[run["query_id"]]
        top = [row["candidate_id"] for row in ranked[:TOP_K]]
        hits = len(set(top) & set(label["target_candidate_ids"]))
        require(not set(top) & set(label["forbidden_candidate_ids"]), f"run {run_id} returned forbidden candidate")
        quality = run["quality"]
        exact_keys(quality, {"recall_at_10", "precision_at_10", "target_count", "returned_candidates"}, f"run {run_id} quality")
        exact_keys(quality["recall_at_10"], {"numerator", "denominator", "ppm"}, f"run {run_id} recall")
        exact_keys(quality["precision_at_10"], {"numerator", "denominator", "ppm"}, f"run {run_id} precision")
        expected_quality = {
            "recall_at_10": ratio(hits, len(label["target_candidate_ids"])),
            "precision_at_10": ratio(hits, TOP_K),
            "target_count": len(label["target_candidate_ids"]),
            "returned_candidates": len(ranked),
        }
        require(quality == expected_quality, f"run {run_id} quality mismatch")
        if run["mode"] == "lexical_baseline":
            expected_status = "control_pass" if hits == 0 else "fail"
        else:
            expected_status = "pass" if at_least(quality["recall_at_10"], RECALL_THRESHOLD_PPM) and at_least(quality["precision_at_10"], PRECISION_THRESHOLD_PPM) else "fail"
        require(run["status"] == expected_status and expected_status in ("pass", "control_pass"), f"run {run_id} quality gate failed")

    expected_total = len(MODES) * len(queries) * len(KINDS) * REPETITIONS
    require(len(runs) == expected_total, f"report has {len(runs)} runs; expected exactly {expected_total}")
    for mode in MODES:
        for query_id in queries:
            for kind in KINDS:
                indices = sorted(run["repetition_index"] for run in runs if run["mode"] == mode and run["query_id"] == query_id and run["repetition_kind"] == kind)
                require(indices == list(range(REPETITIONS)), f"{mode}/{query_id}/{kind} repetition matrix is incomplete or has extras")
    validate_lifecycle_matrix(runs)

    expected_modes: list[dict[str, Any]] = []
    for mode in MODES:
        mode_runs = [run for run in runs if run["mode"] == mode]
        require(mode_runs, f"{mode} has no runs")
        groups = sorted({(run["partition"], run["stratum"]) for run in mode_runs})
        strata: list[dict[str, Any]] = []
        for partition, stratum in groups:
            group = [run for run in mode_runs if run["partition"] == partition and run["stratum"] == stratum]
            recall_num = sum(run["quality"]["recall_at_10"]["numerator"] for run in group)
            recall_den = sum(run["quality"]["recall_at_10"]["denominator"] for run in group)
            precision_num = sum(run["quality"]["precision_at_10"]["numerator"] for run in group)
            precision_den = sum(run["quality"]["precision_at_10"]["denominator"] for run in group)
            recall = ratio(recall_num, recall_den)
            precision = ratio(precision_num, precision_den)
            if mode == "lexical_baseline":
                status = "control_pass" if recall_num == 0 else "fail"
            else:
                status = "pass" if at_least(recall, RECALL_THRESHOLD_PPM) and at_least(precision, PRECISION_THRESHOLD_PPM) else "fail"
            strata.append({
                "mode": mode,
                "partition": partition,
                "stratum": stratum,
                "query_count": len({run["query_id"] for run in group}),
                "recall_at_10": recall,
                "precision_at_10": precision,
                "status": status,
            })
        mode_status = ("control_pass" if all(item["status"] == "control_pass" for item in strata) else "pass" if all(item["status"] == "pass" for item in strata) else "fail")
        expected_modes.append({
            "mode": mode,
            "query_count": len({run["query_id"] for run in mode_runs}),
            "run_count": len(mode_runs),
            "status": mode_status,
            "strata": strata,
        })
    require(report["modes"] == expected_modes, "mode/stratum aggregates mismatch")
    require(report["status"] == "pass", "report status is not pass")
    require(report["output_digest"] == digest(OUTPUT_DOMAIN, runs), "report output digest mismatch")
    matrix_material = [
        [
            run["mode"], run["query_id"], run["partition"], run["stratum"],
            run["repetition_kind"], run["repetition_index"], run["ranking_digest"],
            run["production_receipt_digest"], run["lifecycle"],
        ]
        for run in runs
    ]
    require(report["matrix_digest"] == digest(MATRIX_DOMAIN, matrix_material), "report matrix digest mismatch")


def load_inputs(paths: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bytes, bytes]:
    try:
        corpus = paths.corpus.read_bytes()
        manifest = paths.manifest.read_bytes()
    except OSError as error:
        raise ValueError(f"read semantic bytes: {error}") from error
    return read_json(paths.workload), read_json(paths.labels), read_json(paths.artifact), corpus, manifest


def default_paths(root: Path) -> dict[str, Path]:
    fixture = root / "crates/tracedecay-query/assets/runtime-root/tests/fixtures/search_quality/semantic-ablation"
    return {
        "workload": fixture / "query-semantic-ablation-workload-v1.json",
        "labels": fixture / "labels-v1.json",
        "artifact": fixture / "artifact-v1.json",
        "corpus": fixture / "corpus/semantic_catalog.rs",
        "manifest": root / MODEL_MANIFEST_RELATIVE,
    }


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parents[3]
    defaults = default_paths(root)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    for name, path in defaults.items():
        parser.add_argument(f"--{name}", type=Path, default=path)
    args = parser.parse_args(argv)
    try:
        workload, labels, artifact, corpus, manifest = load_inputs(args)
        digests = validate_inputs(workload, labels, artifact, corpus, manifest)
        verify_report(read_json(args.report), workload, labels, artifact, digests)
    except ValueError as error:
        print(f"semantic-ablation verification failed: {error}", file=sys.stderr)
        return 1
    print("semantic-ablation verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
