#!/usr/bin/env python3
"""Focused, dependency-free checks for the direct-original runner."""

from __future__ import annotations

import importlib.util
import json
import hashlib
import hmac
import os
import stat
import subprocess
import sys
import tempfile
import time
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace


RUNNER_PATH = Path(__file__).with_name("runner.py")
SPEC = importlib.util.spec_from_file_location("native_original_runner", RUNNER_PATH)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def expect_raises(error_type, callback) -> None:
    try:
        callback()
    except error_type:
        return
    raise AssertionError(f"expected {error_type.__name__}")


def write_build_attestation(
    root: Path,
    binary: Path,
    source_root: Path,
    *,
    side: str = "product",
    features: str | None = None,
) -> Path:
    if features is None:
        features = (
            "test-transport"
            if side == "original"
            else "production,memory-provider-host,semantic-fastembed"
        )
    revision = subprocess.check_output(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"],
        text=True,
    ).strip().lower()
    binary_ref = runner.verify_binary(binary, side)
    path = root / f"{side}-build-attestation.json"
    path.write_text(
        json.dumps(
            {
                "format": runner.BUILD_ATTESTATION_FORMAT,
                "side": side,
                "features": features,
                "source_root": str(source_root.resolve()),
                "source_revision": revision,
                "binary_path": str(binary.resolve()),
                "binary_sha256": binary_ref["sha256"],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def owned_observations(route: str, *, store_value: object = "store", digest: str = "a" * 64):
    challenge = "c" * 64
    challenge_response = "d" * 64
    binding = {
        "authority_digest": digest,
        "endpoint": {"kind": "unix", "address": f"/tmp/{route}.sock"},
        "profile_root": f"/tmp/{route}/profile",
        "process_root": f"/tmp/{route}/process",
        "binary_root": f"/tmp/{route}/bin",
        "store_root": f"/tmp/{route}/profile",
    }
    evidence = {
        "protocol_observed": True,
        "source": "runner_owned_daemon_challenge",
        "challenge": challenge,
        "challenge_response": challenge_response,
        "challenge_response_scheme": "sha256_exact_daemon_response_bytes",
        "request_sha256": "e" * 64,
        "response_sha256": challenge_response,
        "response_bytes": 1,
        "request_id": f"native-original-authority/{route}/{challenge}",
        "response_jsonrpc": "2.0",
        **binding,
    }
    independent_authority_observation = {
        "observed": True,
        "source": "runner_owned_authority_record",
        "authority_digest": digest,
        "before": {"authority_digest": digest},
        "after": {"authority_digest": digest},
    }
    reachability = {
        "observed": True,
        "route": route,
        "authority_correlated": True,
        "authority_source": "/authority_evidence",
        "authority_binding": binding,
        "authority_protocol_evidence": evidence,
        "authority_digest": digest,
        "challenge_required": True,
        "challenge": challenge,
        "challenge_response": challenge_response,
        "independent_authority_observation": independent_authority_observation,
        "independent_authority_observed": True,
    }
    store = {
        "observed": True,
        "authority_correlated": True,
        "value": store_value,
        "authority_digest": digest,
        "authority_source": "/authority_evidence",
        "protocol_evidence": evidence,
    }
    return reachability, store, evidence


def owned_route_identity(route: str, *, operation_kind: str | None = None) -> dict[str, object]:
    value = {
        "route": route,
        "entrypoint": "cli_tool",
        "tool": None,
        "operation_kind": operation_kind,
        "classification": None,
        "availability": "unknown",
    }
    return {
        "observed": True,
        "runner_owned": True,
        **value,
        "published_tool": None,
        "contract_digest": runner.json_digest(value),
    }


def complete_ledger_row(value: dict[str, object], *, case_id: str | None = None) -> dict[str, object]:
    """Build the complete nullable ledger shape used by artifact tests."""

    row: dict[str, object] = {field: None for field in runner.LEDGER_REQUIRED_FIELDS}
    row.update(value)
    resolved_case_id = case_id or str(row.get("row_id") or "case")
    row["case_id"] = resolved_case_id
    row["row_id"] = resolved_case_id
    if not isinstance(row.get("outcome"), str):
        row["outcome"] = "blocked"
    attempt_id = str(row.get("attempt_id") or f"{resolved_case_id}/0")
    attempt_index = row.get("attempt_index")
    if not isinstance(attempt_index, int) or isinstance(attempt_index, bool):
        attempt_index = 0
    row["attempt_id"] = attempt_id
    row["attempt_index"] = attempt_index
    row["runner_attempt_binding"] = {
        "run_id": attempt_id.rsplit("/", 1)[0] if "/" in attempt_id else attempt_id,
        "row_id": resolved_case_id,
        "case_id": resolved_case_id,
        "attempt_id": attempt_id,
        "attempt_index": attempt_index,
    }
    return row


def test_semantic_mismatch_preserves_observables() -> None:
    original = {
        "status": "completed",
        "response": {
            "score": 900,
            "order": ["fact-a", "fact-b"],
            "receipt": {"id": "receipt-a"},
            "omissions": ["cursor"],
            "nonce": "original",
        },
    }
    product = {
        "status": "completed",
        "response": {
            "score": 899,
            "order": ["fact-a", "fact-b"],
            "receipt": {"id": "receipt-a"},
            "omissions": ["cursor"],
            "nonce": "product",
        },
    }
    result = runner.compare_results(original, product, {"ignore_json_pointers": ["/nonce"]})
    assert result["status"] == "fail"
    assert "original_semantic" in result and "product_semantic" in result
    assert result["original_semantic"]["score"] == 900


def test_missing_required_observable_is_unknown() -> None:
    side = {"status": "completed", "response": {"items": []}}
    result = runner.compare_results(
        side,
        side,
        {
            "semantic_json_pointers": ["/items", "/receipt"],
            "required_json_pointers": ["/receipt"],
        },
    )
    assert result["status"] == "unknown"
    assert result["missing_original"] == ["/receipt"]
    assert result["missing_product"] == ["/receipt"]


def test_status_priority_materializes_iterable() -> None:
    assert runner._status_priority(iter(["pass", "pass"])) == "pass"
    assert runner._status_priority(iter(["pass", "unknown"])) == "unknown"
    assert runner._status_priority(iter(["pass", "fail"])) == "fail"


def test_missing_original_binary() -> None:
    expect_raises(
        runner.BinaryError,
        lambda: runner.verify_binary(Path("/definitely/missing/native-original"), "original"),
    )


def test_binary_symlink_is_rejected_before_resolution() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-binary-symlink-") as temporary:
        root = Path(temporary)
        target = root / "target"
        target.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        target.chmod(0o755)
        link = root / "link"
        link.symlink_to(target)
        expect_raises(runner.BinaryError, lambda: runner.verify_binary(link, "product"))


def test_shared_binary_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-binaries-") as temporary:
        binary = Path(temporary) / "tracedecay"
        binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        binary.chmod(0o755)
        expect_raises(
            runner.BinaryError,
            lambda: runner.verify_distinct_binaries(binary, binary),
        )


def test_shared_binary_root_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-binary-root-") as temporary:
        root = Path(temporary)
        original = root / "original"
        product = root / "product"
        original.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        product.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        original.chmod(0o755)
        product.chmod(0o755)
        expect_raises(
            runner.BinaryError,
            lambda: runner.verify_distinct_binaries(original, product),
        )


def test_modified_reference_checkout_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-reference-") as temporary:
        checkout = Path(temporary)
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.email", "runner@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.name", "runner"], check=True)
        (checkout / "source.txt").write_text("pristine\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(checkout), "add", "source.txt"], check=True)
        subprocess.run(["git", "-C", str(checkout), "commit", "-q", "-m", "reference"], check=True)
        revision = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
        pinned_revision = runner.REFERENCE_REVISION
        runner.REFERENCE_REVISION = revision
        try:
            verified = runner.verify_reference_checkout(checkout)
            assert verified["clean"] is True
            (checkout / "source.txt").write_text("modified\n", encoding="utf-8")
            expect_raises(runner.ReferenceError, lambda: runner.verify_reference_checkout(checkout))
        finally:
            runner.REFERENCE_REVISION = pinned_revision


def test_reference_revision_override_is_refused() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-reference-override-") as temporary:
        checkout = Path(temporary)
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.email", "runner@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(checkout), "config", "user.name", "runner"], check=True)
        (checkout / "source.txt").write_text("alternate\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(checkout), "add", "source.txt"], check=True)
        subprocess.run(["git", "-C", str(checkout), "commit", "-q", "-m", "alternate"], check=True)
        revision = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
        expect_raises(
            runner.ReferenceError,
            lambda: runner.verify_reference_checkout(checkout, expected_revision=revision),
        )


def test_zero_relevant_cases_and_missing_operation_coverage() -> None:
    expect_raises(runner.RunnerError, lambda: runner.select_relevant_cases([], ["fact_store_search"]))
    cases = [{"id": "seed", "actions": [{"route": "fact_store_add"}]}]
    expect_raises(
        runner.RunnerError,
        lambda: runner.select_relevant_cases(cases, ["fact_store_add", "fact_store_search"]),
    )


def test_readiness_rejects_filtered_or_incomplete_suites() -> None:
    expect_raises(
        runner.RunnerError,
        lambda: runner.validate_readiness_selection(
            [{"id": "focused", "actions": [{"route": "fact_store_search"}]}],
            ["fact_store_search"],
            readiness_mode="readiness",
        ),
    )
    expect_raises(
        runner.RunnerError,
        lambda: runner.validate_readiness_selection(
            [{"id": "focused", "actions": [{"route": "fact_store_search"}]}],
            readiness_mode="readiness",
        ),
    )


def test_frozen_readiness_matrix_rows_and_counts_are_authoritative() -> None:
    matrix = runner.load_readiness_matrix()
    validated = runner.validate_frozen_readiness_matrix(matrix)
    assert validated["matrix_id"] == runner.READINESS_MATRIX_ID
    assert validated["row_count"] == 201
    assert validated["planned_attempts"] == 1381
    assert len(validated["row_ids"]) == 201
    tampered = deepcopy(matrix)
    tampered["rows"] = list(tampered["rows"])
    tampered["rows"][0] = dict(tampered["rows"][0])
    tampered["rows"][0]["oracle_kind"] = "wrong-oracle"
    expect_raises(runner.RunnerError, lambda: runner.validate_frozen_readiness_matrix(tampered))
    expect_raises(
        runner.RunnerError,
        lambda: runner.validate_readiness_selection([], readiness_mode="readiness"),
    )


def test_frozen_readiness_matrix_semantic_bindings_are_authoritative() -> None:
    matrix = runner.load_readiness_matrix()
    for field, nested_field in (
        ("operation", None),
        ("oracle", "case"),
        ("runs", "profile"),
        ("identity_evidence", None),
        ("effect_receipt_state", None),
    ):
        tampered = deepcopy(matrix)
        tampered["rows"] = list(tampered["rows"])
        tampered["rows"][0] = dict(tampered["rows"][0])
        if nested_field is None:
            tampered["rows"][0][field] = "tampered"
        else:
            tampered["rows"][0][field] = dict(tampered["rows"][0][field])
            tampered["rows"][0][field][nested_field] = "tampered"
        expect_raises(runner.RunnerError, lambda: runner.validate_frozen_readiness_matrix(tampered))


def test_case_result_digest_authenticates_final_serialized_payload() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-result-digest-") as temporary:
        path = Path(temporary) / "case-result.json"
        result = {
            "case_id": "digest-case",
            "status": "blocked",
            "matrix_row": {
                "id": "row-a",
                "operation": "FactStore::commit_fact",
                "oracle_case": "fact_add",
            },
            "attempt_ledger": [
                complete_ledger_row({
                    "attempt_id": "digest-case/0",
                    "attempt_index": 0,
                    "result_digest": None,
                }, case_id="digest-case")
            ],
            "attempt_ledger_receipt": {
                "path": "attempts.jsonl",
                "offset": 0,
                "bytes": 123,
                "sha256": "a" * 64,
            },
            "readiness_population": {
                "status": "blocked",
                "required_attempts": 3,
                "observed_attempts": 1,
            },
        }
        ledger_path = Path(temporary) / "attempts.jsonl"
        ledger_row_bytes = runner._json_bytes(result["attempt_ledger"][0]) + b"\n"
        ledger_path.write_bytes(ledger_row_bytes)
        result["attempt_ledger_receipt"] = {
            "path": str(ledger_path),
            "offset": 0,
            "bytes": len(ledger_row_bytes),
            "sha256": runner.bytes_digest(ledger_row_bytes),
        }
        runner._finalize_case_result_digest(result)
        expected_payload = deepcopy(result)
        expected_payload["result_digest"] = None
        expected_payload["attempt_ledger"][0]["result_digest"] = None
        expected_bytes = runner._json_bytes(expected_payload) + b"\n"
        assert result["result_digest"] == runner.bytes_digest(expected_bytes)
        runner._write_json(path, result)
        assert runner._load_json(path) == result

        tampered = deepcopy(result)
        tampered["readiness_population"]["observed_attempts"] = 2
        path.write_bytes(runner._json_bytes(tampered) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        tampered = deepcopy(result)
        tampered["matrix_row"]["operation"] = "tampered"
        path.write_bytes(runner._json_bytes(tampered) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        tampered = deepcopy(result)
        tampered["attempt_ledger_receipt"]["offset"] = 1
        path.write_bytes(runner._json_bytes(tampered) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        tampered = deepcopy(result)
        tampered["attempt_ledger"][0]["result_digest"] = "b" * 64
        path.write_bytes(runner._json_bytes(tampered) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        # Re-finalizing the outer result isolates the nested ledger shape
        # check: a missing/nullability-changing nested digest must still fail
        # even when the outer result_digest is internally consistent.
        missing_nested_digest = deepcopy(result)
        del missing_nested_digest["attempt_ledger"][0]["result_digest"]
        missing_nested_digest["result_digest"] = runner.case_result_digest(missing_nested_digest)
        path.write_bytes(runner._json_bytes(missing_nested_digest) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        missing_nested_rows = deepcopy(result)
        missing_nested_rows["attempt_ledger"] = []
        runner._finalize_case_result_digest(missing_nested_rows)
        path.write_bytes(runner._json_bytes(missing_nested_rows) + b"\n")
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))

        path.write_bytes(runner._json_bytes(result))
        expect_raises(runner.RunnerError, lambda: runner._load_json(path))


def test_case_result_digest_matches_independent_canonical_expectation() -> None:
    """Recompute the digest without calling the runner digest helper."""

    result = {
        "case_id": "independent-digest",
        "status": "blocked",
        "matrix_row": {"id": "row-a", "track": "native", "identity": "native_exact"},
        "attempt_ledger": [
            {"attempt_id": "independent-digest/0", "attempt_index": 0, "result_digest": None}
        ],
        "readiness_population": {
            "status": "blocked",
            "required_attempts": 3,
            "observed_attempts": 1,
        },
    }
    runner._finalize_case_result_digest(result)
    canonical = deepcopy(result)
    canonical["result_digest"] = None
    canonical["attempt_ledger"][0]["result_digest"] = None
    independent_bytes = (
        json.dumps(
            canonical,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    expected = hashlib.sha256(independent_bytes).hexdigest()
    assert result["result_digest"] == expected
    assert expected == "1d0194e1e2a3b7b1e7a354a13e90310b2faf27028004aa4a120188335ce1a8f3"


def test_case_schema_requires_nonempty_actions_at_runtime() -> None:
    contract = runner.load_contract()
    case = deepcopy(runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0])
    case["actions"] = []
    # The independent Draft 2020-12 schema and the dependency-free runtime
    # validator enforce the same minItems boundary.
    expect_raises(runner.ContractError, lambda: runner.validate_case(case, contract))

    tampered_contract = deepcopy(contract)
    tampered_contract["case_schema"]["properties"]["actions"]["minItems"] = 0
    expect_raises(runner.ContractError, lambda: runner.validate_contract(tampered_contract))


def test_ledger_receipt_requires_one_complete_canonical_jsonl_row() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-ledger-receipt-boundary-") as temporary:
        path = Path(temporary) / "attempts.jsonl"
        first = complete_ledger_row({"attempt_id": "case/0", "attempt_index": 0, "result_digest": None}, case_id="case")
        second = complete_ledger_row({"attempt_id": "case/1", "attempt_index": 1, "result_digest": None}, case_id="case")
        first_bytes = runner._json_bytes(first) + b"\n"
        second_bytes = runner._json_bytes(second) + b"\n"
        path.write_bytes(first_bytes + second_bytes)
        receipt = {
            "path": str(path),
            "offset": 0,
            "bytes": len(first_bytes),
            "sha256": runner.bytes_digest(first_bytes),
        }
        assert runner._valid_ledger_receipt(receipt)
        assert runner._read_ledger_receipt_row(receipt) == first

        # A hash of a parseable suffix is still not a JSONL row: receipts must
        # start at a line boundary and contain exactly one physical record.
        middle = {
            **receipt,
            "offset": 1,
            "bytes": len(first_bytes) - 1,
            "sha256": runner.bytes_digest(first_bytes[1:]),
        }
        assert not runner._valid_ledger_receipt(middle)
        spanning = {
            **receipt,
            "bytes": len(first_bytes) + len(second_bytes),
            "sha256": runner.bytes_digest(first_bytes + second_bytes),
        }
        assert not runner._valid_ledger_receipt(spanning)

        # Reordered/noncanonical JSON is not an authoritative append, even
        # when the parsed object has the required nullable digest field.
        noncanonical = b'{"result_digest":null,"attempt_index":0,"attempt_id":"case/0"}\n'
        path.write_bytes(noncanonical)
        noncanonical_receipt = {
            "path": str(path),
            "offset": 0,
            "bytes": len(noncanonical),
            "sha256": runner.bytes_digest(noncanonical),
        }
        assert not runner._valid_ledger_receipt(noncanonical_receipt)


def test_embedded_case_schema_is_valid_and_bound_to_case_artifacts() -> None:
    """Compile both schemas with Ajv2020 and reject vacuous/weak cases."""

    node = __import__("shutil").which("node")
    assert node, "Node.js is required for the independent Ajv2020 contract check"
    root = RUNNER_PATH.resolve().parents[3]
    script = r'''
const fs = require("fs");
const Ajv2020 = require("ajv/dist/2020");
const contract = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
const example = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const matrix = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
// The contract document is a metadata envelope with intentionally namespaced
// annotation keys; its validation is run with strict=false.  The embedded
// case schema is a standalone strict Draft 2020-12 schema.
const ajv = new Ajv2020({allErrors: true, strict: false});
const topValidate = ajv.compile(contract);
if (topValidate({})) throw new Error("top-level contract schema accepts an empty object");
if (!topValidate(contract)) throw new Error(ajv.errorsText(topValidate.errors));
const caseAjv = new Ajv2020({allErrors: true, strict: true});
const caseValidate = caseAjv.compile(contract.case_schema);
if (!caseValidate(example)) throw new Error(ajv.errorsText(caseValidate.errors));
if (caseValidate({})) throw new Error("embedded case schema accepts an empty object");
const validBinding = {
  matrix_row_id: "row-a",
  track: "native",
  profile: "native_parity_v1",
  operation: "FactStore::commit_fact",
  oracle_kind: "independent_570_candidate_production",
  oracle_case: "fact_add",
  identity: "native_exact",
  identity_evidence: {
    candidate_binary_sha256: {state: "required_per_attempt", value: null},
    process_identity: {state: "required_per_attempt", value: null},
    reference_binary_sha256: {state: "required_per_attempt", value: null},
    route_identity: {state: "required_per_attempt", value: null},
    store_identity: {state: "required_per_attempt", value: null}
  },
  effect_receipt_state: {
    effect_digest: {state: "required_per_attempt", value: null},
    expected_effect: "typed effect",
    receipt_digest: {state: "required_per_attempt", value: null},
    receipt_policy: "required_for_mutations",
    reopen_or_no_effect: {state: "required_per_attempt", value: null},
    state_digest: {state: "required_per_attempt", value: null}
  },
  required_identity_fields: ["candidate_binary_sha256"],
  required_evidence_fields: ["effect_digest"],
  identity_requirements: matrix.identity_requirements,
  track_identity_requirements: matrix.identity_requirements.native
};
const withBinding = {...example, matrix_binding: validBinding};
if (!caseValidate(withBinding)) throw new Error(ajv.errorsText(caseValidate.errors));
const badIdentity = JSON.parse(JSON.stringify(withBinding));
badIdentity.matrix_binding.identity_evidence.process_identity = {state: "required_per_attempt", value: null, forged: true};
if (caseValidate(badIdentity)) throw new Error("identity evidence accepts an extra forged field");
const badEffect = JSON.parse(JSON.stringify(withBinding));
badEffect.matrix_binding.effect_receipt_state.effect_digest = "forged";
if (caseValidate(badEffect)) throw new Error("effect receipt state accepts a scalar");
for (const [label, mutate] of [
  ["fragment-id", value => { value.case_schema.$id += "#fragment"; }],
  ["empty-actions", value => { value.case_schema.properties.actions.minItems = 0; }],
  ["weak-identity-definition", value => { value.case_schema.$defs.identityEvidence.properties.store_identity = {}; }],
  ["weak-effect-definition", value => { value.case_schema.$defs.effectReceiptState.properties.state_digest = {}; }],
  ["unbound-case-schema", value => { value.case_schema.properties.matrix_binding.properties.identity_evidence = {}; }],
]) {
  const broken = JSON.parse(JSON.stringify(contract));
  mutate(broken);
  if (topValidate(broken)) throw new Error(`top-level contract accepted ${label}`);
}
'''
    completed = subprocess.run(
        [
            node,
            "-e",
            script,
            str(RUNNER_PATH.with_name("case-contract.json")),
            str(RUNNER_PATH.with_name("example-case.json")),
            str(root / ".codex" / "plans" / "native-original" / "execution-results" / "readiness-matrix.json"),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_normative_wrapper_check_is_part_of_the_bounded_suite() -> None:
    wrapper = RUNNER_PATH.with_name("test_native_original_runner_wrapper.py")
    completed = subprocess.run(
        [sys.executable, "-S", str(wrapper)],
        cwd=RUNNER_PATH.resolve().parents[3],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_readiness_attempt_population_blocks_short_runs() -> None:
    matrix = {
        "rows": [
            {"id": "row-a", "runs": {"planned": 3}},
            {"id": "row-b", "runs": {"planned": 1}},
        ]
    }
    results = [
        {
            "attempt_ledger": [
                {"matrix_row": {"id": "row-a"}, "row_id": "row-a"},
                {"matrix_row": {"id": "row-b"}, "row_id": "row-b"},
            ]
        }
    ]
    population = runner._readiness_attempt_population(matrix, results)
    assert population["status"] == "blocked"
    assert population["complete"] is False
    assert population["required_attempts"] == 4
    assert population["observed_attempts"] == 2
    assert population["missing_attempts"] == 2
    assert population["missing_runs"] == {"row-a": 2}
    assert population["rows"][0]["status"] == "blocked"
    complete = runner._readiness_attempt_population(
        matrix,
        [
            {
                "attempt_ledger": [
                    {"matrix_row": {"id": "row-a"}},
                    {"matrix_row": {"id": "row-a"}},
                    {"matrix_row": {"id": "row-a"}},
                    {"matrix_row": {"id": "row-b"}},
                ]
            }
        ],
    )
    assert complete["status"] == "complete"
    assert complete["complete"] is True


def test_run_suite_readiness_blocks_unpopulated_matrix() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-readiness-population-") as temporary:
        root = Path(temporary)
        original_dir = root / "original"
        product_dir = root / "product"
        original_dir.mkdir()
        product_dir.mkdir()
        original = original_dir / "tracedecay"
        product = product_dir / "tracedecay"
        original.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        product.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        original.chmod(0o755)
        product.chmod(0o755)
        matrix_path = RUNNER_PATH.resolve().parents[3] / ".codex" / "plans" / "native-original" / "execution-results" / "readiness-matrix.json"
        matrix = runner.load_readiness_matrix(matrix_path)
        validated_matrix = runner.validate_frozen_readiness_matrix(matrix)
        template = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
        cases = []
        for row in validated_matrix["rows"]:
            case = deepcopy(template)
            case["id"] = row["id"]
            case["matrix_binding"] = runner._matrix_row_binding(row)
            route = runner._matrix_row_contract_route(row) or runner.MATRIX_UNAVAILABLE_ROUTE
            action = {
                "id": "matrix-operation",
                "route": route,
                "matrix_route": route,
                "matrix_row_id": row["id"],
                "matrix_operation": row["operation"],
                "matrix_oracle_case": row["oracle"]["case"],
                "request": {},
            }
            if runner._matrix_row_contract_route(row) in (None, runner.MATRIX_UNAVAILABLE_ROUTE):
                action["matrix_candidate_route"] = row["candidate_route"]
                action["matrix_reference_route"] = row["reference_route"]
            case["actions"] = [action]
            cases.append(case)
        reference_checkout = (
            RUNNER_PATH.resolve().parents[5]
            / ".worktrees"
            / runner.REFERENCE_CHECKOUT_NAME
        )
        original_run_case = runner.run_case
        original_attestation = write_build_attestation(
            root, original, reference_checkout, side="original"
        )
        attestation = write_build_attestation(root, product, RUNNER_PATH.resolve().parents[3])

        def fake_run_case(case, **_kwargs):
            return {
                "case_id": case["id"],
                "status": "pass",
                "artifact_directory": str(root / "fake-artifacts"),
            }

        runner.run_case = fake_run_case
        try:
            report = runner.run_suite(
                contract=runner.load_contract(),
                cases=cases,
                reference_checkout=reference_checkout,
                original_binary=original,
                product_binary=product,
                artifact_root=root / "artifacts",
                product_source_root=RUNNER_PATH.resolve().parents[3],
                original_build_attestation=original_attestation,
                product_build_attestation=attestation,
                readiness_mode="readiness",
                readiness_matrix_path=matrix_path,
                run_id="readiness-short",
            )
        finally:
            runner.run_case = original_run_case
        assert report["status"] == "blocked"
        assert report["readiness_population"]["complete"] is False
        assert report["readiness_population"]["required_attempts"] == 1381
        # The compatibility fake still materializes one durable ledger row per
        # selected matrix row; planned repeat cells remain unpopulated.
        assert report["readiness_population"]["observed_attempts"] == 201
        assert report["readiness_population"]["missing_attempts"] == 1180
        assert report["readiness"]["population"]["status"] == "blocked"
        manifest = json.loads((root / "artifacts" / "readiness-short" / "run-manifest.json").read_text(encoding="utf-8"))
        assert manifest["status"] == "blocked"
        assert manifest["observed_attempts"] == 201
        assert manifest["readiness"]["population"]["missing_attempts"] == 1180
        case_manifest = runner._load_json(
            root / "artifacts" / "readiness-short" / "000-native.fact.commit" / "case-result.json"
        )
        assert case_manifest["readiness_population"]["status"] == "blocked"
        assert isinstance(case_manifest["result_digest"], str)


def test_operation_effect_policy_is_typed() -> None:
    route = {"id": "write", "operation_kind": "mutation", "effect_policy": "required"}
    comparison = {
        "effect_mode": "required",
        "effect_json_pointers": ["/effect"],
        "receipt_json_pointers": ["/receipt"],
        "state_json_pointers": ["/state"],
    }
    original_reachability, original_store, _ = owned_observations("write", store_value="store-a")
    product_reachability, product_store, _ = owned_observations("write", store_value="store-b")
    def durable(response: dict[str, object]) -> dict[str, object]:
        receipt_digest = "a" * 64
        after_values = {
            "effect": {"/effect": response["effect"]},
            "receipt": {"/receipt": response["receipt"]},
        }
        reopen_values = {"state": {"/state": response["state"]}}
        return {
            "observed": True,
            "source": "runner_owned_daemon_action_receipt",
            "authority_correlated": True,
            "action_receipt_sha256": receipt_digest,
            "after": {"observed": True, "values": after_values, "sha256": runner.json_digest(after_values)},
            "reopen": {"observed": True, "values": reopen_values, "sha256": runner.json_digest(reopen_values)},
        }
    original = {
        "status": "completed",
        "response": {
            "effect": {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})},
            "receipt": {"id": "r", "receipt_digest": runner.json_digest({"id": "r"})},
            "state": {"generation": 1, "state_digest": runner.json_digest({"generation": 1})},
        },
        "operation_reachability": original_reachability,
        "authority_evidence": original_store["protocol_evidence"],
        "store_identity": original_store,
        "route_identity": owned_route_identity("write", operation_kind="mutation"),
        "action_receipt": {"receipt_sha256": "a" * 64},
        "runner_observed_durable": durable({
            "effect": {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})},
            "receipt": {"id": "r", "receipt_digest": runner.json_digest({"id": "r"})},
            "state": {"generation": 1, "state_digest": runner.json_digest({"generation": 1})},
        }),
    }
    product = {
        "status": "completed",
        "response": {
            "effect": {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})},
            "receipt": {"id": "r", "receipt_digest": runner.json_digest({"id": "r"})},
            "state": {"generation": 1, "state_digest": runner.json_digest({"generation": 1})},
        },
        "operation_reachability": product_reachability,
        "authority_evidence": product_store["protocol_evidence"],
        "store_identity": product_store,
        "route_identity": owned_route_identity("write", operation_kind="mutation"),
        "action_receipt": {"receipt_sha256": "a" * 64},
        "runner_observed_durable": durable({
            "effect": {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})},
            "receipt": {"id": "r", "receipt_digest": runner.json_digest({"id": "r"})},
            "state": {"generation": 1, "state_digest": runner.json_digest({"generation": 1})},
        }),
    }
    result = runner._pair_comparison(
        original,
        product,
        comparison=comparison,
        route=route,
        case_classification="original_native",
    )
    assert result["status"] == "pass", result
    missing = runner._pair_comparison(
        original,
        product,
        comparison={"semantic_json_pointers": []},
        route=route,
        case_classification="original_native",
    )
    assert missing["status"] == "effect_unknown", missing


def test_production_durable_observations_are_emitted_from_after_and_reopen() -> None:
    """The production attachment path must retain both independent phases."""

    comparison = {
        "effect_mode": "required",
        "effect_json_pointers": ["/effect"],
        "receipt_json_pointers": ["/receipt"],
        "state_json_pointers": ["/state"],
    }
    with tempfile.TemporaryDirectory(prefix="native-original-durable-emit-") as temporary:
        root = Path(temporary)
        side_root = root / "side"
        side_root.mkdir()
        response = {
            "effect": {"kind": "commit"},
            "receipt": {"id": "receipt"},
            "state": {"generation": 2},
        }

        def side_result(action_id: str) -> dict[str, object]:
            receipt_sha = "8" * 64
            response_digest = runner._daemon_result_digest(response)
            return {
                "status": "completed",
                "response": deepcopy(response),
                "side": "original",
                "route": "write",
                "entrypoint": "cli_tool",
                "action_receipt": {
                    "receipt_sha256": receipt_sha,
                    "authority_digest": "a" * 64,
                    "route": "write",
                    "entrypoint": "cli_tool",
                    "receipt": {
                        "receipt_sha256": receipt_sha,
                        "result_digest": response_digest,
                    },
                },
                "operation_reachability": {
                    "authority_correlated": True,
                    "authority_digest": "a" * 64,
                },
                "route_identity": {
                    "route": "write",
                    "entrypoint": "cli_tool",
                    "observed": True,
                    "runner_owned": True,
                },
                "roots": {"artifact_root": str(side_root / action_id)},
            }

        for action_id in ("main", "after", "reopen"):
            (side_root / action_id).mkdir()
            (side_root / action_id / "result.json").write_bytes(b"{}\n")
        main = side_result("main")
        action_result = {
            "route": "write",
            "input": {"id": "main", "route": "write", "comparison": comparison},
            "original": main,
            "product": deepcopy(main),
            "checkpoints": {
                "after": [{"id": "after", "original": side_result("after"), "product": side_result("after")}],
                "reopened": [{"id": "reopen", "original": side_result("reopen"), "product": side_result("reopen")}],
            },
        }
        original_authenticated = runner._authenticated_result_observation
        original_verified = runner._verified_receipt_proof
        original_wrapper = runner._action_receipt_wrapper_valid
        runner._authenticated_result_observation = lambda _value: True
        runner._verified_receipt_proof = lambda _value, **_kwargs: True
        runner._action_receipt_wrapper_valid = lambda _value, **_kwargs: True
        try:
            runner._attach_runner_observed_durable(
                action_result,
                action_result["input"],
            )
            for side in runner.SIDE_NAMES:
                durable = action_result[side]["runner_observed_durable"]
                assert durable["observed"] is True, durable
                assert durable["authority_correlated"] is True, durable
                assert durable["after"]["source"] == "runner_observed_after_checkpoint"
                assert durable["reopen"]["source"] == "runner_observed_reopen_checkpoint"
                assert runner._runner_durable_value(
                    action_result[side], field="effect_json_pointers", pointer="/effect", phase="after"
                ) == response["effect"]
        finally:
            runner._authenticated_result_observation = original_authenticated
            runner._verified_receipt_proof = original_verified
            runner._action_receipt_wrapper_valid = original_wrapper
        assert json.loads((side_root / "main" / "result.json").read_text(encoding="utf-8"))["runner_observed_durable"]["observed"] is True


def test_unavailable_route_identity_has_a_typed_boundary() -> None:
    route = next(
        route
        for route in runner.load_contract()["routes"]
        if route.get("id") == runner.MATRIX_UNAVAILABLE_ROUTE
    )
    identity = runner._route_identity(route, {"route": route["id"]}, None, "original")
    assert identity["entrypoint"] == "unavailable"
    assert runner._usable_identity_field_value("route_identity", identity) is True


def test_operation_reachability_and_store_identity_are_required() -> None:
    route = {"id": "read", "operation_kind": "read", "effect_policy": "none"}
    comparison = {
        "effect_mode": "none",
        "no_effect_json_pointers": ["/state"],
        "semantic_json_pointers": ["/state"],
    }
    side = {"status": "completed", "response": {"state": "same"}}
    result = runner._pair_comparison(
        side,
        side,
        comparison=comparison,
        route=route,
        case_classification="original_native",
    )
    assert result["status"] == "unknown", result
    assert any(item.endswith("store_identity") for item in result["missing_observations"])
    read_reachability, read_store, read_evidence = owned_observations("read", store_value="store-original")
    observed = {
        **side,
        "operation_reachability": read_reachability,
        "authority_evidence": read_evidence,
        "store_identity": read_store,
        "route_identity": owned_route_identity("read", operation_kind="read"),
    }
    _, product_store, _ = owned_observations("read", store_value="store-product")
    observed_product = {**observed, "store_identity": product_store}
    result = runner._pair_comparison(
        observed,
        observed_product,
        comparison=comparison,
        route=route,
        case_classification="original_native",
    )
    assert result["status"] == "pass", result


def test_reopen_specs_are_side_specific() -> None:
    mismatch = runner._validate_reopen_pair(
        {"route": "memory_status", "comparison": {}},
        {"route": "fact_store_get", "comparison": {}},
    )
    assert mismatch and mismatch["status"] == "invalid", mismatch
    mismatch = runner._validate_reopen_pair(
        {"route": "memory_status", "comparison": {"required_json_pointers": ["/a"]}},
        {"route": "memory_status", "comparison": {"required_json_pointers": ["/b"]}},
    )
    assert mismatch and mismatch["status"] == "invalid", mismatch
    contract = runner.load_contract()
    case = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    case["lifecycle"]["reopen"]["product"]["action"]["side"] = "original"
    expect_raises(runner.ContractError, lambda: runner.validate_case(case, contract))


def test_ledger_includes_checkpoint_reopen_and_no_effect_evidence() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-ledger-evidence-") as temporary:
        root = Path(temporary)
        original_dir = root / "original"
        product_dir = root / "product"
        original_dir.mkdir()
        product_dir.mkdir()
        original = original_dir / "tracedecay"
        product = product_dir / "tracedecay"
        original.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        product.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        original.chmod(0o755)
        product.chmod(0o755)
        ledger_reachability, ledger_store, ledger_evidence = owned_observations("memory_status", store_value="store-ledger")
        side_result = {
            "status": "completed",
            "response": {"state": "stable", "no_effect": True},
            "operation_reachability": ledger_reachability,
            "authority_evidence": ledger_evidence,
            "store_identity": ledger_store,
            "route_identity": owned_route_identity("memory_status", operation_kind="read"),
        }
        checkpoint = {
            "action_id": "before-state",
            "route": "memory_status",
            "input": {
                "comparison": {
                    "state_json_pointers": ["/state"],
                    "no_effect_json_pointers": ["/no_effect"],
                }
            },
            "original": side_result,
            "product": side_result,
        }
        reopen_action = {
            "id": "reopen-status",
            "route": "memory_status",
            "comparison": {"state_json_pointers": ["/state"]},
        }
        result = {
            "case_id": "ledger-evidence",
            "status": "pass",
            "artifact_directory": str(root / "artifacts"),
            "roots": {
                "original": {"profile_root": str(root / "profile-original")},
                "product": {"profile_root": str(root / "profile-product")},
            },
            "daemon_runtime": {"original": {"pid": 1}, "product": {"pid": 2}},
            "actions": [
                {
                    "action_id": "main",
                    "input": checkpoint["input"],
                    "original": side_result,
                    "product": side_result,
                    "checkpoints": {"before": [checkpoint], "after": [], "reopened": []},
                }
            ],
            "lifecycle_spec": {
                "reopen": {
                    "original": {"action": reopen_action},
                    "product": {"action": reopen_action},
                }
            },
            "lifecycle": {
                "reopen_comparison": {"status": "pass"},
                "reopen": {
                    "original": {"status": "pass", "result": side_result, "previous_daemon": {"pid": 1}, "reopened_daemon": {"pid": 3}},
                    "product": {"status": "pass", "result": side_result, "previous_daemon": {"pid": 2}, "reopened_daemon": {"pid": 4}},
                },
            },
        }
        row = runner._build_attempt_ledger(
            result=result,
            case={"id": "ledger-evidence", "fixtures": {}},
            contract=runner.load_contract(),
            original_binary=original,
            product_binary=product,
            source_roots={"original": root / "reference", "product": root / "candidate"},
            attempt_id="ledger-evidence/0",
            attempt_index=0,
        )
        assert row["checkpoint_state"]["original"]["observed"] is True
        assert row["operation_reachability"]["original"]["observed"] is True
        assert row["reopen_state"]["original"]["observed"] is True
        assert row["no_effect_evidence"]["original"]["observed"] is True
        assert row["reopen_or_no_effect"]["no_effect_digest"]["original"] is not None


def test_full_materialized_ledger_passes_independent_revalidation() -> None:
    """Exercise a complete runner-owned ledger row through its final gates."""

    native_original = next(
        (Path(candidate) for candidate in ("/bin/ls", "/usr/bin/ls") if Path(candidate).is_file()),
        None,
    )
    native_product = next(
        (Path(candidate) for candidate in ("/bin/cat", "/usr/bin/cat") if Path(candidate).is_file()),
        None,
    )
    if native_original is None or native_product is None:
        return
    with tempfile.TemporaryDirectory(prefix="native-original-ledger-pass-") as temporary:
        root = Path(temporary)
        project_root = RUNNER_PATH.resolve().parents[3]
        reference_root = (
            project_root.parent / "native-original-reference-57006f60"
        ).resolve()
        if not reference_root.is_dir():
            reference_root = project_root.parent / ".worktrees" / runner.REFERENCE_CHECKOUT_NAME
        reference_root = reference_root.resolve()
        product_source = project_root.resolve()
        if not reference_root.is_dir():
            return

        original_source = runner.verify_source_root(reference_root, side="original")
        product_source_evidence = runner.verify_source_root(product_source, side="product")
        original_input = runner.verify_binary(native_original, "original")
        product_input = runner.verify_binary(native_product, "product")
        original_attestation_path = write_build_attestation(
            root, native_original, reference_root, side="original"
        )
        product_attestation_path = write_build_attestation(
            root, native_product, product_source, side="product"
        )
        original_attestation = runner.verify_build_attestation(
            original_attestation_path,
            binary=original_input,
            source_root=original_source,
            expected_revision=runner.REFERENCE_REVISION,
            expected_features="test-transport",
            side="original",
        )
        product_revision = runner.resolve_product_revision(product_source, None)
        product_attestation = runner.verify_build_attestation(
            product_attestation_path,
            binary=product_input,
            source_root=product_source_evidence,
            expected_revision=product_revision,
            expected_features="production,memory-provider-host,semantic-fastembed",
            side="product",
        )
        original_copy = runner._copy_immutable_binary(
            original_input,
            root=root / "original-binary-root",
            side="original",
            expected_sha256=original_input["sha256"],
        )
        product_copy = runner._copy_immutable_binary(
            product_input,
            root=root / "product-binary-root",
            side="product",
            expected_sha256=product_input["sha256"],
        )

        contract = runner.load_contract()
        route = runner._route_map(contract)["memory_status"]
        action_input = {
            "comparison": {
                "effect_mode": "none",
                "no_effect_json_pointers": ["/no_effect"],
            }
        }
        request_refs = {}
        for side in runner.SIDE_NAMES:
            request_dir = root / f"request-{side}"
            request_dir.mkdir()
            request_bytes = runner._json_bytes({"probe": "memory-status"}) + b"\n"
            request_path = request_dir / "request.json"
            request_path.write_bytes(request_bytes)
            request_refs[side] = {
                "path": str(request_path),
                "bytes": len(request_bytes),
                "sha256": runner.bytes_digest(request_bytes),
            }

        side_results = {}
        for side, binary_copy, store_value, authority_digest in (
            ("original", original_copy, "original-store", "a" * 64),
            ("product", product_copy, "product-store", "b" * 64),
        ):
            reachability, store_identity, authority_evidence = owned_observations(
                "memory_status",
                store_value=store_value,
                digest=authority_digest,
            )
            action = {
                "id": "status",
                "route": "memory_status",
                "entrypoint": "cli_tool",
            }
            route_identity = runner._route_identity(route, action, "cli_tool", side)
            identity_binding = runner._runner_identity_binding(
                side=side,
                binary=Path(binary_copy["path"]),
                route_identity=route_identity,
                reachability=reachability,
                authority_evidence=authority_evidence,
            )
            side_results[side] = {
                "status": "completed",
                "response": {"no_effect": {"status": "unchanged"}},
                "operation_reachability": reachability,
                "authority_evidence": authority_evidence,
                "store_identity": store_identity,
                "route_identity": route_identity,
                "runner_identity_binding": identity_binding,
                "route": "memory_status",
                "entrypoint": "cli_tool",
                "request": {"probe": "memory-status"},
                "request_sha256": request_refs[side]["sha256"],
                "request_artifact": request_refs[side],
                "process": {"status": "completed"},
            }

        side_roots = {}
        for side, source, binary_copy in (
            ("original", reference_root, original_copy),
            ("product", product_source, product_copy),
        ):
            side_dir = root / side
            side_roots[side] = {
                "source_root": str(source),
                "binary_root": str(Path(binary_copy["path"]).parent.parent),
                "project_root": str(side_dir / "project"),
                "profile_root": str(side_dir / "profile"),
                "state_root": str(side_dir / "state"),
                "store_root": str(side_dir / "profile"),
                "process_root": str(side_dir / "process"),
                "socket_root": str(side_dir / "socket"),
                "artifact_root": str(side_dir / "artifact"),
            }
        result = {
            "case_id": "full-ledger-pass",
            "status": "pass",
            "artifact_directory": str(root / "artifacts"),
            "roots": side_roots,
            "composition_reached": {"original": True, "product": True},
            "actions": [
                {
                    "action_id": "status",
                    "route": "memory_status",
                    "input": action_input,
                    "original": side_results["original"],
                    "product": side_results["product"],
                }
            ],
            "build_attestations": {
                "original": original_attestation,
                "product": product_attestation,
            },
            "binary_provenance": {
                "original": original_copy,
                "product": product_copy,
            },
            "source_bindings": {
                "original": original_source,
                "product": product_source_evidence,
            },
            "product_source_revision": product_revision,
        }
        case = {
            "id": "full-ledger-pass",
            "classification": "original_native",
            "fixtures": {},
        }
        row = runner._build_attempt_ledger(
            result=result,
            case=case,
            contract=contract,
            original_binary=Path(original_copy["path"]),
            product_binary=Path(product_copy["path"]),
            source_roots={"original": reference_root, "product": product_source},
            attempt_id="full-ledger-pass/0",
            attempt_index=0,
        )
        assert row["outcome"] == "pass", row.get("binding_validation")
        assert row["binding_validation"]["status"] == "pass"
        assert row["authority_correlated"] == {"original": True, "product": True}
        assert row["reference_store_identity"]["value"] == "original-store"
        assert row["candidate_store_identity"]["value"] == "product-store"
        runner._validate_ledger_bindings(row)


def test_child_process_is_bounded_and_group_is_cleaned() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-process-") as temporary:
        root = Path(temporary)
        marker = root / "child.pid"
        script = root / "spawn_child.py"
        script.write_text(
            "import os, signal, sys, time\n"
            "marker = sys.argv[1]\n"
            "pid = os.fork()\n"
            "if pid == 0:\n"
            "    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\n"
            "    open(marker, 'w', encoding='utf-8').write(str(os.getpid()))\n"
            "    while True: time.sleep(1)\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        artifact_dir = root / "artifacts"
        result = runner.run_process(
            [sys.executable, str(script), str(marker)],
            cwd=root,
            environment={**os.environ, "PYTHONUNBUFFERED": "1"},
            artifact_dir=artifact_dir,
            timeout=runner.ProcessTimeouts(0.1, 0.5, 0.5),
        )
        assert result["status"] == "censored"
        assert result["process"]["process_group_id"] is not None
        assert result["cleanup"]["status"] == "completed", result
        if marker.exists():
            child_pid = int(marker.read_text(encoding="utf-8"))
            for _ in range(20):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
            else:
                raise AssertionError(f"owned child survived cleanup: {child_pid}")


def test_spawn_failure_returns_typed_evidence() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-spawn-failure-") as temporary:
        root = Path(temporary)
        result = runner.run_process(
            [str(root / "missing-binary")],
            cwd=root,
            environment=dict(os.environ),
            artifact_dir=root / "artifacts",
            timeout=runner.ProcessTimeouts(0.1, 0.01, 0.01),
        )
        assert result["status"] == "unknown", result
        assert result["process"]["pid"] is None
        assert result["process"]["failure_class"] == "spawn_failure"
        assert result["cleanup"]["status"] == "not_started"
        assert result["cleanup"]["failure_class"] == "spawn_failure"
        assert result["process"]["remaining_owned_children"] == 0


def test_cleanup_failure_cannot_complete_an_action() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-cleanup-") as temporary:
        root = Path(temporary)
        script = root / "complete.py"
        script.write_text("raise SystemExit(0)\n", encoding="utf-8")
        original_group_members = runner._group_members
        original_signal_group = runner._signal_group
        runner._group_members = lambda _group_id: [999999]
        runner._signal_group = lambda *_args: False
        try:
            result = runner.run_process(
                [sys.executable, str(script)],
                cwd=root,
                environment=dict(os.environ),
                artifact_dir=root / "artifacts",
                timeout=runner.ProcessTimeouts(0.1, 0.01, 0.01),
            )
        finally:
            runner._group_members = original_group_members
            runner._signal_group = original_signal_group
        assert result["cleanup"]["status"] == "failed", result
        assert result["status"] == "unknown", result


def test_run_suite_appends_ledger_when_case_raises() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-suite-raise-") as temporary:
        root = Path(temporary)
        original_dir = root / "original"
        product_dir = root / "product"
        original_dir.mkdir()
        product_dir.mkdir()
        original = original_dir / "tracedecay"
        product = product_dir / "tracedecay"
        original.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        product.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        original.chmod(0o755)
        product.chmod(0o755)
        contract = runner.load_contract()
        cases = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))
        reference_checkout = (
            RUNNER_PATH.resolve().parents[5]
            / ".worktrees"
            / runner.REFERENCE_CHECKOUT_NAME
        )
        product_source_root = RUNNER_PATH.resolve().parents[3]
        original_attestation = write_build_attestation(
            root, original, reference_checkout, side="original"
        )
        attestation = write_build_attestation(root, product, product_source_root)
        original_run_case = runner.run_case

        def raise_case(*_args, **_kwargs):
            raise RuntimeError("injected case failure")

        runner.run_case = raise_case
        try:
            expect_raises(
                RuntimeError,
                lambda: runner.run_suite(
                    contract=contract,
                    cases=cases,
                    reference_checkout=reference_checkout,
                    original_binary=original,
                    product_binary=product,
                    artifact_root=root / "artifacts",
                    product_source_root=product_source_root,
                    original_build_attestation=original_attestation,
                    product_build_attestation=attestation,
                    run_id="raise-case",
                ),
            )
        finally:
            runner.run_case = original_run_case
        ledger_path = root / "artifacts" / "raise-case" / "attempts.jsonl"
        assert ledger_path.is_file()
        rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 1
        assert rows[0]["outcome"] == "unknown"
        assert set(runner.LEDGER_REQUIRED_FIELDS).issubset(rows[0])
        assert (root / "artifacts" / "raise-case" / "000-fact-search-after-independent-seed" / "attempt.json").is_file()
        manifest = json.loads((root / "artifacts" / "raise-case" / "run-manifest.json").read_text(encoding="utf-8"))
        assert manifest["status"] == "unknown"
        assert manifest["error"]["type"] == "RuntimeError"


def test_matrix_case_ledger_cli_compatibility_records_blocked_attempt() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-matrix-cli-") as temporary:
        ledger = Path(temporary) / "attempts.jsonl"
        command = [
            sys.executable,
            "-S",
            str(RUNNER_PATH),
            "--case",
            "native.fact.commit",
            "--ledger",
            str(ledger),
        ]
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        assert completed.returncode == 2, completed
        assert "unrecognized arguments" not in completed.stderr
        row = json.loads(ledger.read_text(encoding="utf-8").splitlines()[0])
        assert row["row_id"] == "native.fact.commit"
        assert row["outcome"] == "blocked"
        assert row["oracle_kind"] == "independent_570_candidate_production"
        frozen = runner.validate_frozen_readiness_matrix(runner.load_readiness_matrix())["rows"][0]
        assert row["matrix_binding"] == runner._matrix_row_binding(frozen)
        assert row["matrix_row"]["identity_evidence"] == frozen["identity_evidence"]
        assert row["matrix_row"]["effect_receipt_state"] == frozen["effect_receipt_state"]


def test_parseable_terminal_outcomes_are_typed() -> None:
    expected = {
        "cancelled": "cancelled",
        "partial": "partial",
        "unavailable": "unsupported",
        "effect_unknown": "effect_unknown",
        "blocked": "blocked",
    }
    for source, mapped in expected.items():
        response = {"outcome": {"status": source}}
        assert runner._parsed_terminal_status(response)[0] == mapped
        status = runner._response_status(
            {"status": "completed"},
            response,
            None,
        )
        assert status == mapped
        comparison = runner.compare_results(
            {"status": "completed", "response": response},
            {"status": "completed", "response": response},
        )
        assert comparison["status"] == mapped


def test_owned_daemon_publishes_and_cleans_identity() -> None:
    if os.name == "nt":
        return
    with tempfile.TemporaryDirectory(prefix="native-original-daemon-") as temporary:
        root = Path(temporary)
        binary = root / "fake-tracedecay.py"
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, signal, socket, sys, time\n"
            "if sys.argv[1:3] != ['daemon', 'run']:\n"
            "    raise SystemExit(2)\n"
            "socket_path = sys.argv[sys.argv.index('--socket') + 1]\n"
            "profile = sys.argv[sys.argv.index('--profile-root') + 1]\n"
            "os.makedirs(profile, exist_ok=True)\n"
            "listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n"
            "listener.bind(socket_path)\n"
            "listener.listen(4)\n"
            "listener.settimeout(0.05)\n"
            "record = {'pid': os.getpid(), 'process_run_id': 'fake-run', 'epoch': 1,\n"
            " 'version': 'fake', 'endpoint': {'kind': 'unix', 'address': socket_path},\n"
            " 'auth_token': 'a' * 64, 'profile_root': os.path.realpath(profile)}\n"
            "with open(os.path.join(profile, 'daemon-authority.json'), 'w') as stream:\n"
            "    json.dump(record, stream)\n"
            "running = True\n"
            "def stop(*_):\n"
            "    global running\n"
            "    running = False\n"
            "signal.signal(signal.SIGTERM, stop)\n"
            "while running:\n"
            "    try:\n"
            "        connection, _ = listener.accept()\n"
            "        connection.close()\n"
            "    except socket.timeout:\n"
            "        pass\n"
            "listener.close()\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        side_dir = root / "side"
        side_dir.mkdir()
        daemon = runner._OwnedDaemon.start(
            side="original",
            binary=binary,
            side_dir=side_dir,
            profile_root=side_dir / "profile",
            artifact_dir=root / "artifacts",
            timeouts=runner.ProcessTimeouts(2.0, 0.5, 0.5),
        )
        assert daemon.identity is not None
        assert daemon.identity["pid"] == daemon.process.pid
        assert daemon.current_identity() == daemon.identity
        cleanup = daemon.stop(runner.ProcessTimeouts(2.0, 0.5, 0.5), reason="test")
        assert cleanup["status"] == "completed", cleanup
        assert not daemon.socket_path.exists()
        assert (root / "artifacts" / "process.json").is_file()
        assert (root / "artifacts" / "cleanup.json").is_file()


def test_readiness_cli_requires_cases() -> None:
    expect_raises(
        runner.RunnerError,
        lambda: runner.main(["validate", "--readiness-mode", "readiness"]),
    )


def test_readiness_binding_is_exact_and_raw_matrix_is_authoritative() -> None:
    matrix = runner.load_readiness_matrix()
    row = runner.validate_frozen_readiness_matrix(matrix)["rows"][0]
    template = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    case = deepcopy(template)
    case["id"] = row["id"]
    case["matrix_binding"] = runner._matrix_row_binding(row)
    expect_raises(
        runner.RunnerError,
        lambda: runner.validate_readiness_selection([case], readiness_mode="readiness", readiness_matrix=matrix),
    )
    tampered = deepcopy(matrix)
    tampered["rows"] = list(tampered["rows"])
    tampered["rows"][0] = dict(tampered["rows"][0])
    tampered["rows"][0]["operation"] = "relabelled"
    expect_raises(
        runner.RunnerError,
        lambda: runner.validate_readiness_selection([case], readiness_mode="readiness", readiness_matrix=tampered),
    )
    case["matrix_binding"] = dict(case["matrix_binding"], track="semantic")
    expect_raises(
        runner.RunnerError,
        lambda: runner._validate_readiness_case_binding(case, row),
    )


def test_readiness_action_route_cannot_relabel_every_row_as_search() -> None:
    matrix = runner.load_readiness_matrix()
    row = runner.validate_frozen_readiness_matrix(matrix)["rows"][0]
    template = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    case = deepcopy(template)
    case["id"] = row["id"]
    case["matrix_binding"] = runner._matrix_row_binding(row)
    action = {
        "id": "wrong-route",
        "route": "fact_store_search",
        "matrix_route": "fact_store_search",
        "matrix_row_id": row["id"],
        "matrix_operation": row["operation"],
        "matrix_oracle_case": row["oracle"]["case"],
        "request": {},
    }
    case["actions"] = [action]
    expect_raises(runner.RunnerError, lambda: runner._validate_readiness_case_binding(case, row))


def test_readiness_no_callable_rows_require_explicit_unavailable_route() -> None:
    matrix = runner.load_readiness_matrix()
    rows = runner.validate_frozen_readiness_matrix(matrix)["rows"]
    row = next(row for row in rows if runner._matrix_row_contract_route(row) is None)
    template = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    case = deepcopy(template)
    case["id"] = row["id"]
    case["matrix_binding"] = runner._matrix_row_binding(row)
    action = {
        "id": "blocked-unavailable-route",
        "route": runner.MATRIX_UNAVAILABLE_ROUTE,
        "matrix_route": runner.MATRIX_UNAVAILABLE_ROUTE,
        "matrix_row_id": row["id"],
        "matrix_operation": row["operation"],
        "matrix_oracle_case": row["oracle"]["case"],
        "matrix_candidate_route": row["candidate_route"],
        "matrix_reference_route": row["reference_route"],
        "request": {},
    }
    case["actions"] = [action]
    runner._validate_readiness_case_binding(case, row)
    forged = deepcopy(case)
    forged["actions"][0]["route"] = "fact_store_search"
    forged["actions"][0]["matrix_route"] = "fact_store_search"
    expect_raises(runner.RunnerError, lambda: runner._validate_readiness_case_binding(forged, row))


def test_false_callable_native_rows_are_explicitly_unavailable() -> None:
    matrix = runner.validate_frozen_readiness_matrix(runner.load_readiness_matrix())
    expected = {
        "native.fact.query_as_of",
        "native.fact.query_as_of_response",
        "native.fact.query_lineage",
        "native.fact.query_lineage_response",
        "native.fact.retrieval_anchor",
        "native.fact.purge_superseded",
        "native.fact.history",
        "native.session.task_lookup",
        "native.host.safety_extensions",
    }
    actual = {
        row["id"]
        for row in matrix["rows"]
        if runner._matrix_row_contract_route(row) == runner.MATRIX_UNAVAILABLE_ROUTE
    }
    assert actual == expected
    for row in matrix["rows"]:
        if row["id"] in expected:
            assert row["candidate_route"] and row["reference_route"]


def test_setup_schema_is_closed_and_pairwise_identical() -> None:
    contract = runner.load_contract()
    case = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    bad = deepcopy(case)
    bad["side_setup"]["unexpected-side"] = []
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad, contract))
    bad = deepcopy(case)
    bad["side_setup"]["original"][0]["unexpected"] = True
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad, contract))
    bad = deepcopy(case)
    bad["side_setup"]["product"] = []
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad, contract))
    bad = deepcopy(case)
    bad["side_setup"]["product"][0]["request"]["category"] = "other"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad, contract))


def test_retrieval_oracle_cannot_use_an_empty_store() -> None:
    contract = runner.load_contract()
    case = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]
    case["side_setup"]["original"] = []
    case["side_setup"]["product"] = []
    expect_raises(runner.ContractError, lambda: runner.validate_case(case, contract))


def test_setup_mutation_effects_are_materialized_without_unbound_routes() -> None:
    contract = runner.load_contract()
    reach_original, store_original, evidence_original = owned_observations("fact_store_add", store_value="setup-o")
    reach_product, store_product, evidence_product = owned_observations("fact_store_add", store_value="setup-p")
    setup_comparison = {
        "effect_mode": "required",
        "effect_json_pointers": ["/effect"],
        "receipt_json_pointers": ["/receipt"],
        "state_json_pointers": ["/state"],
    }
    def side(reach, store, evidence):
        return {
            "status": "completed",
            "response": {
                "effect": {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})},
                "receipt": {"id": "setup-receipt", "receipt_digest": runner.json_digest({"id": "setup-receipt"})},
                "state": {"generation": 1, "state_digest": runner.json_digest({"generation": 1})},
            },
            "operation_reachability": reach,
            "authority_evidence": evidence,
            "store_identity": store,
            "route_identity": owned_route_identity("fact_store_add", operation_kind="mutation"),
            "route": "fact_store_add",
            "input": {"comparison": setup_comparison},
        }
    result = {
        "actions": [],
        "setup": {
            "original": [{"id": "seed", "result": side(reach_original, store_original, evidence_original)}],
            "product": [{"id": "seed", "result": side(reach_product, store_product, evidence_product)}],
        },
    }
    evidence = runner._effect_evidence(
        result,
        {"classification": "original_native"},
        contract,
        {"original": {}, "product": {}},
        {"original": {"observed": True}, "product": {"observed": True}},
        {"original": {}, "product": {}},
    )
    assert evidence["per_action"] and evidence["per_action"][0]["route"] == "fact_store_add"


def test_population_rejects_unexpected_and_overcounted_rows() -> None:
    matrix = {"rows": [{"id": "row-a", "runs": {"planned": 1}}]}
    population = runner._readiness_attempt_population(
        matrix,
        [{"attempt_ledger": [{"row_id": "row-a"}, {"row_id": "row-a"}, {"row_id": "row-extra"}]}],
    )
    assert population["complete"] is False
    assert population["over_counted_runs"] == {"row-a": 1}
    assert population["unexpected_rows"] == {"row-extra": 1}
    assert population["rows"][0]["status"] == "over_counted"


def test_frozen_population_rejects_duplicate_or_relabelled_attempts() -> None:
    row = {
        "id": "row-a",
        "track": "native",
        "operation": "FactStore::commit_fact",
        "oracle_kind": "independent_570_candidate_production",
        "oracle": {"case": "fact_add"},
        "identity": "native_v1",
        "identity_evidence": {"provider_id": {"state": "required_per_attempt"}},
        "effect_receipt_state": {"effect": {"state": "required_per_attempt"}},
        "runs": {"planned": 1, "profile": "native_parity_v1"},
    }
    matrix_row = {
        "id": row["id"],
        "track": row["track"],
        "operation": row["operation"],
        "oracle_kind": row["oracle_kind"],
        "oracle_case": row["oracle"]["case"],
        "identity": row["identity"],
        "identity_evidence": row["identity_evidence"],
        "effect_receipt_state": row["effect_receipt_state"],
        "profile": row["runs"]["profile"],
        "planned_runs": row["runs"]["planned"],
    }
    attempt = complete_ledger_row({
        "attempt_id": "run/0",
        "attempt_index": 0,
        "outcome": "blocked",
        "result_digest": None,
        "matrix_row": matrix_row,
    }, case_id="row-a")
    with tempfile.TemporaryDirectory(prefix="native-original-population-ledger-") as temporary:
        receipt_path = Path(temporary) / "attempts.jsonl"

        def receipt_for(value):
            receipt_bytes = runner._json_bytes(value) + b"\n"
            receipt_path.write_bytes(receipt_bytes)
            return {
                "path": str(receipt_path),
                "offset": 0,
                "bytes": len(receipt_bytes),
                "sha256": runner.bytes_digest(receipt_bytes),
            }

        receipt = receipt_for(attempt)
        complete = runner._readiness_attempt_population(
            {"rows": [row]}, [{"case_id": "row-a", "attempt_ledger": [attempt], "attempt_ledger_receipt": receipt}]
        )
        assert complete["complete"] is True
        unpersisted = runner._readiness_attempt_population(
            {"rows": [row]}, [{"attempt_ledger": [attempt]}]
        )
        assert unpersisted["complete"] is False
        assert unpersisted["unexpected_rows"]["<unpersisted-attempt>"] == 1
        duplicate = runner._readiness_attempt_population(
            {"rows": [row]}, [{"case_id": "row-a", "attempt_ledger": [attempt, {**attempt, "attempt_id": "run/1"}], "attempt_ledger_receipt": receipt}]
        )
        assert duplicate["complete"] is False
        assert duplicate["unexpected_rows"]["<receipt-row-cardinality>"] == 1
        relabelled_attempt = {**attempt, "matrix_row": {**matrix_row, "operation": "wrong"}}
        relabelled = runner._readiness_attempt_population(
            {"rows": [row]},
            [{"case_id": "row-a", "attempt_ledger": [relabelled_attempt], "attempt_ledger_receipt": receipt_for(relabelled_attempt)}],
        )
        assert relabelled["complete"] is False
        assert "row-a:matrix_operation" in relabelled["unexpected_rows"]
        for field in ("oracle_case", "profile", "planned_runs"):
            altered_attempt = {**attempt, "matrix_row": {**matrix_row, field: "wrong" if field != "planned_runs" else 99}}
            altered = runner._readiness_attempt_population(
                {"rows": [row]},
                [{"case_id": "row-a", "attempt_ledger": [altered_attempt], "attempt_ledger_receipt": receipt_for(altered_attempt)}],
            )
            assert altered["complete"] is False
            assert f"row-a:matrix_{field}" in altered["unexpected_rows"]


def test_readiness_receipt_re_reads_the_named_ledger_span() -> None:
    row = {
        "id": "row-a",
        "track": "native",
        "operation": "FactStore::commit_fact",
        "oracle_kind": "independent_570_candidate_production",
        "oracle": {"case": "fact_add"},
        "identity": "native_v1",
        "identity_evidence": {"provider_id": {"state": "required_per_attempt"}},
        "effect_receipt_state": {"effect": {"state": "required_per_attempt"}},
        "runs": {"planned": 1, "profile": "native_parity_v1"},
    }
    matrix_row = {
        "id": row["id"],
        "track": row["track"],
        "operation": row["operation"],
        "oracle_kind": row["oracle_kind"],
        "oracle_case": row["oracle"]["case"],
        "identity": row["identity"],
        "identity_evidence": row["identity_evidence"],
        "effect_receipt_state": row["effect_receipt_state"],
        "profile": row["runs"]["profile"],
        "planned_runs": row["runs"]["planned"],
    }
    with tempfile.TemporaryDirectory(prefix="native-original-receipt-") as temporary:
        path = Path(temporary) / "attempts.jsonl"
        attempt = complete_ledger_row(
            {"attempt_id": "run/0", "attempt_index": 0, "outcome": "blocked", "result_digest": None, "matrix_row": matrix_row},
            case_id="row-a",
        )
        payload = runner._json_bytes(attempt) + b"\n"
        path.write_bytes(payload)
        receipt = {"path": str(path), "offset": 0, "bytes": len(payload), "sha256": runner.bytes_digest(payload)}
        complete = runner._readiness_attempt_population(
            {"rows": [row]}, [{"case_id": "row-a", "attempt_ledger": [attempt], "attempt_ledger_receipt": receipt}]
        )
        assert complete["complete"] is True
        path.write_bytes(b'{"attempt_id":"forged"}\n')
        tampered = runner._readiness_attempt_population(
            {"rows": [row]}, [{"case_id": "row-a", "attempt_ledger": [attempt], "attempt_ledger_receipt": receipt}]
        )
        assert tampered["complete"] is False
        assert tampered["unexpected_rows"]["<unpersisted-attempt>"] == 1


def test_store_identity_aggregate_carries_an_observed_value() -> None:
    reach, store, evidence = owned_observations("memory_status", store_value="physical-store")
    side = {
        "status": "completed",
        "response": {"store_identity": "physical-store"},
        "operation_reachability": reach,
        "authority_evidence": evidence,
        "store_identity": store,
    }
    aggregate = runner._observed_store_identity({"actions": [{"action_id": "a", "route": "memory_status", "original": side}]}, "original")
    assert aggregate["observed"] is True
    assert aggregate["value"] == "physical-store"
    assert aggregate["values"] == ["physical-store"]


def test_typed_effect_rejects_generic_status_and_marker_maps() -> None:
    for field in ("effect_json_pointers", "receipt_json_pointers", "state_json_pointers"):
        assert runner._typed_effect_observable(field, {"status": "ok"}, strict=True) is False
        marker = field.removesuffix("_json_pointers").replace("_", "")
        assert runner._typed_effect_observable(field, {marker: "arbitrary"}, strict=True) is False
    assert runner._typed_effect_observable("effect_json_pointers", {"kind": "commit"}, strict=True) is False
    assert runner._typed_effect_observable("receipt_json_pointers", {"id": "receipt"}, strict=True) is False
    assert runner._typed_effect_observable("state_json_pointers", {"generation": 1}, strict=True) is False
    assert runner._typed_effect_observable(
        "effect_json_pointers", {"kind": "commit", "effect_digest": runner.json_digest({"kind": "commit"})}, strict=True
    ) is True


def test_setup_failure_without_process_evidence_is_causal_setup() -> None:
    for status in ("unsupported", "invalid", "cancelled", "partial", "effect_unknown", "blocked"):
        assert runner._first_causal_stage(
            {"status": status, "actions": [], "setup": {"original": [{"result": {"status": status}}]}}
        ) == "setup"


def test_ncm_worker_requires_attested_native_process() -> None:
    assert runner._forbidden_ncm_worker_path("/bin/echo") is True
    assert runner._forbidden_ncm_worker_path("/bin/sleep") is True
    assert runner._forbidden_ncm_worker_path(sys.executable) is True
    with tempfile.TemporaryDirectory(prefix="native-original-worker-") as temporary:
        script = Path(temporary) / "worker.sh"
        script.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        script.chmod(0o755)
        assert runner._forbidden_ncm_worker_path(script) is True


def test_ncm_worker_manifest_is_runner_input_and_response_claims_fail_closed() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-ncm-manifest-") as temporary:
        root = Path(temporary)
        manifest_path = root / "worker.json"
        source_root = RUNNER_PATH.resolve().parents[3]
        manifest = {
            "format": runner.NCM_WORKER_ATTESTATION_FORMAT,
            "kind": "ncm_encoder_worker",
            "worker_id": runner.NCM_WORKER_ID,
            "binary_path": "/opt/tracedecay/bin/ncm-worker",
            "binary_sha256": "6" * 64,
            "source_root": str(source_root),
            "source_revision": subprocess.check_output(
                ["git", "-C", str(source_root), "rev-parse", "HEAD"], text=True
            ).strip().lower(),
            "protocol_version": "1.0",
            "implementation_sha256": "1" * 64,
            "model_artifact_sha256": "2" * 64,
            "tokenizer_sha256": "3" * 64,
            "vector_fixture_digest": "4" * 64,
        }
        manifest["pin_sha256"] = runner.json_digest(
            {field: manifest[field] for field in runner.NCM_WORKER_PIN_FIELDS}
        )
        manifest_path.write_bytes(runner._json_bytes(manifest) + b"\n")
        trusted = runner._load_ncm_worker_attestation(manifest_path)
        assert trusted["_runner_input"] is True
        assert trusted["_runner_input_sha256"] == runner.bytes_digest(manifest_path.read_bytes())
        side_result = {
            "status": "completed",
            "response": {
                "worker_attestation": manifest,
                "worker_sha256": "6" * 64,
            },
        }
        missing, reason = runner._ncm_worker_identity(
            result={"artifact_directory": str(root), "daemon_runtime": {}},
            side="product",
            side_results=[side_result],
            expected_source=None,
        )
        assert missing is None
        assert reason and "explicit runner-owned" in reason

        manifest_path.write_bytes(runner._json_bytes({**manifest, "protocol_version": "tampered"}) + b"\n")
        missing, reason = runner._ncm_worker_identity(
            result={"artifact_directory": str(root), "daemon_runtime": {}},
            side="product",
            side_results=[side_result],
            expected_source=None,
            trusted_attestation=trusted,
        )
        assert missing is None
        assert reason and "authenticated" in reason


def test_independent_ncm_conformance_does_not_require_a_570_worker() -> None:
    binary = runner.verify_binary(Path(sys.executable).resolve(), "product")
    route = owned_route_identity("ncm_handshake", operation_kind="read")
    def side(digest: str, side_name: str) -> dict[str, object]:
        reachability, store, evidence = owned_observations(
            "ncm_handshake", store_value=f"{side_name}-store", digest=digest
        )
        binding = runner._runner_identity_binding(
            side=side_name,
            binary=Path(binary["path"]),
            route_identity=route,
            reachability=reachability,
            authority_evidence=evidence,
        )
        return {
            "status": "completed",
            "response": {
                "provider_id": "ncm",
                "implementation_sha256": "1" * 64,
                "protocol_version": "1.0",
                "state_schema_version": "schema-v1",
                "state_generation": 1,
                "model_artifact_sha256": "2" * 64,
                "tokenizer_sha256": "3" * 64,
                "vector_fixture_digest": "4" * 64,
                "worker_sha256": "5" * 64,
            },
            "operation_reachability": reachability,
            "authority_evidence": evidence,
            "store_identity": store,
            "route_identity": route,
            "runner_identity_binding": binding,
        }
    result = {
        "actions": [{
            "action_id": "handshake",
            "route": "ncm_handshake",
            "original": side("a" * 64, "original"),
            "product": side("b" * 64, "product"),
        }],
        "source_bindings": {"original": {}, "product": {}},
        "daemon_runtime": {
            "product": {
                "pid": 100,
                "process_run_id": "run",
                "epoch": 1,
                "binary_sha256": "f" * 64,
            }
        },
    }
    with tempfile.TemporaryDirectory(prefix="native-original-ncm-attestation-") as temporary:
        attestation_path = Path(temporary) / "trusted-ncm-worker.json"
        attestation = {
            "format": runner.NCM_WORKER_ATTESTATION_FORMAT,
            "kind": "ncm_encoder_worker",
            "worker_id": runner.NCM_WORKER_ID,
            "binary_path": "/opt/tracedecay/bin/ncm-worker",
            "binary_sha256": "6" * 64,
            "source_root": str(RUNNER_PATH.resolve().parents[3]),
            "source_revision": subprocess.check_output(
                ["git", "-C", str(RUNNER_PATH.resolve().parents[3]), "rev-parse", "HEAD"],
                text=True,
            ).strip().lower(),
            "protocol_version": "1.0",
            "implementation_sha256": "1" * 64,
            "model_artifact_sha256": "2" * 64,
            "tokenizer_sha256": "3" * 64,
            "vector_fixture_digest": "4" * 64,
        }
        attestation["pin_sha256"] = runner.json_digest(
            {field: attestation[field] for field in runner.NCM_WORKER_PIN_FIELDS}
        )
        attestation_path.write_bytes(runner._json_bytes(attestation) + b"\n")
        trusted = runner._load_ncm_worker_attestation(attestation_path)
    original_worker = runner._ncm_worker_identity
    runner._ncm_worker_identity = lambda **_kwargs: (
        {
            "worker_sha256": "5" * 64,
            "worker_attestation": deepcopy(attestation),
            "worker_attestation_input": {
                "path": trusted["_runner_input_path"],
                "sha256": trusted["_runner_input_sha256"],
                "bytes": trusted["_runner_input_bytes"],
            },
            "provider_id": "ncm",
            "implementation_sha256": "1" * 64,
            "protocol_version": "1.0",
            "state_schema_version": "schema-v1",
            "state_generation": 1,
            "model_artifact_sha256": "2" * 64,
            "tokenizer_sha256": "3" * 64,
            "vector_fixture_digest": "4" * 64,
            "worker_process": {
                "observed": True,
                "pid": 101,
                "parent_pid": 100,
                "parent_sha256": "f" * 64,
            },
            "daemon_identity": deepcopy(result["daemon_runtime"]["product"]),
            "receipt_authority_digest": runner._authority_identity_digest(
                result["daemon_runtime"]["product"]
            ),
            "immutable_copy": {"immutable": True, "sha256": "5" * 64},
        }, None
    )
    try:
        identity = runner._track_identity_evidence(
            result,
            {
                "matrix_binding": {
                    "track": "ncm",
                    "oracle_kind": "independent_real_worker_conformance",
                }
            },
            binary,
            binary,
            {},
            trusted,
        )
    finally:
        runner._ncm_worker_identity = original_worker
    assert identity["complete"] is True, identity
    assert identity["missing_fields"] == {}, identity
    assert identity["observed"]["worker_conformance"]["original"]["status"] == "unavailable"
    assert identity["observed"]["worker_sha256"]["product"] == "5" * 64
    result["daemon_runtime"]["product"]["pid"] = 102
    mismatched = runner._track_identity_evidence(
        result,
        {
            "matrix_binding": {
                "track": "ncm",
                "oracle_kind": "independent_real_worker_conformance",
            }
        },
        binary,
        binary,
        {},
        trusted,
    )
    assert mismatched["complete"] is False
    assert "worker_sha256" in mismatched["invalid_fields"]


def test_native_and_semantic_identity_requires_receipt_authenticated_response() -> None:
    binary = runner.verify_binary(Path(sys.executable).resolve(), "product")
    reachability, store, evidence = owned_observations("memory_status", store_value="store")
    route = owned_route_identity("memory_status", operation_kind="read")
    response = {
        "provider_id": "tracedecay.native",
        "registration_revision": 1,
        "provider_instance": "instance",
        "exact_scope": {"profile_id": "p"},
        "request_digest": "a" * 64,
        "contribution_digest": "b" * 64,
        "marker": {"provider_id": "tracedecay.native"},
        "model_artifact_sha256": "c" * 64,
        "tokenizer_sha256": "d" * 64,
        "projection_key": "projection",
        "search_index_key": "index",
        "source_generation": 1,
        "vector_generation": 1,
        "capability_manifest_digest": "e" * 64,
        "calibration_profile_id": "calibration",
        "calibration_digest": "f" * 64,
        "rerank_policy_pins": {"policy": "pinned"},
    }
    for track in ("native", "semantic"):
        identity = runner._track_identity_evidence(
            {
                "actions": [{
                    "action_id": "identity",
                    "route": "memory_status",
                    "original": {
                        "side": "original",
                        "status": "completed",
                        "response": response,
                        "operation_reachability": reachability,
                        "authority_evidence": evidence,
                        "store_identity": store,
                        "route_identity": route,
                    },
                    "product": {
                        "side": "product",
                        "status": "completed",
                        "response": response,
                        "operation_reachability": reachability,
                        "authority_evidence": evidence,
                        "store_identity": store,
                        "route_identity": route,
                    },
                }]
            },
            {"id": f"{track}-identity", "matrix_binding": {"track": track}},
            binary,
            binary,
            {},
        )
        assert identity["complete"] is False
        assert identity["missing_fields"], identity


def test_immutable_copy_root_must_be_private_and_source_is_rechecked() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-copy-") as temporary:
        root = Path(temporary)
        source = root / "source"
        source.write_bytes(b"#!/bin/sh\nexit 0\n")
        source.chmod(0o755)
        copy_root = root / "copy"
        copy_root.mkdir()
        copy_root.chmod(0o777)
        ref = runner.verify_binary(source, "original")
        expect_raises(
            runner.BinaryError,
            lambda: runner._copy_immutable_binary(ref, root=copy_root, side="original", expected_sha256=ref["sha256"]),
        )


def test_immutable_copy_modes_are_owner_only_and_sealed_consistently() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-copy-mode-") as temporary:
        root = Path(temporary)
        source = root / "source"
        source.write_bytes(b"#!/bin/sh\nexit 0\n")
        source.chmod(0o755)
        ref = runner.verify_binary(source, "original")
        copy_root = root / "copy"
        copied = runner._copy_immutable_binary(
            ref,
            root=copy_root,
            side="original",
            expected_sha256=ref["sha256"],
        )
        destination = Path(copied["path"])
        assert stat.S_IMODE(destination.stat().st_mode) == runner.IMMUTABLE_COPY_FILE_MODE
        assert stat.S_IMODE(destination.parent.stat().st_mode) == runner.IMMUTABLE_COPY_MODE
        # The root remains writable only until all side copies are installed;
        # the explicit seal uses the same owner-only mode and validates it.
        assert stat.S_IMODE(copy_root.stat().st_mode) == 0o700
        runner._seal_immutable_copy_root(copy_root, label="copy")
        assert stat.S_IMODE(copy_root.stat().st_mode) == runner.IMMUTABLE_COPY_MODE


def test_render_context_never_exposes_authority_challenge_response() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-render-secret-") as temporary:
        root = Path(temporary)
        side_dir = root / "side"
        side_dir.mkdir()
        binary = root / "binary"
        binary.write_bytes(b"#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
        context = runner._route_context(
            side="original",
            binary=binary,
            case={"id": "secret-case"},
            action={"id": "secret-action"},
            side_dir=side_dir,
            artifact_dir=root / "artifact",
            request_file=root / "request.json",
            bindings={
                "previous": {
                    "challenge_response": "d" * 64,
                    "safe": "retained",
                }
            },
            authority_identity={
                "pid": 123,
                "challenge_response": "d" * 64,
                "safe": "retained",
            },
            authority_challenge="c" * 64,
        )
        assert "challenge_response" not in context["bindings"]["previous"]
        assert runner._render("${bindings.previous.safe}", context) == "retained"
        expect_raises(
            runner.ContractError,
            lambda: runner._render("${bindings.previous.challenge_response}", context),
        )
        assert context["authority_identity"] == {"pid": 123, "safe": "retained"}


def test_partial_authority_identity_cannot_correlate_reachability() -> None:
    digest = runner.json_digest({"pid": 7, "process_run_id": "run"})
    result = runner._operation_reachability(
        side="original",
        route_id="memory_status",
        entrypoint="cli_tool",
        process={"status": "completed"},
        response={"authority_digest": digest},
        status="completed",
        authority_identity={"pid": 7, "process_run_id": "run"},
    )
    assert result["observed"] is False
    assert result["authority_correlated"] is False


def test_unknown_effect_policy_cannot_pass() -> None:
    unknown_reachability, unknown_store, unknown_evidence = owned_observations("unknown", store_value="store")
    side = {
        "status": "completed",
        "response": {"ok": True},
        "operation_reachability": unknown_reachability,
        "authority_evidence": unknown_evidence,
        "store_identity": unknown_store,
        "route_identity": owned_route_identity("unknown", operation_kind="coverage_gap"),
    }
    product = {**side, "store_identity": {**side["store_identity"], "value": "store-product"}}
    result = runner._pair_comparison(
        side,
        product,
        comparison={"semantic_json_pointers": ["/ok"]},
        route={"id": "unknown", "operation_kind": "coverage_gap", "effect_policy": "unknown"},
        case_classification="original_native",
    )
    assert result["status"] == "effect_unknown"


def test_track_identity_missing_blocks_matrix_pass() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-track-identity-") as temporary:
        root = Path(temporary)
        original = root / "original" / "tracedecay"
        product = root / "product" / "tracedecay"
        original.parent.mkdir()
        product.parent.mkdir()
        original.write_text("#!/bin/sh\n", encoding="utf-8")
        product.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        original.chmod(0o755)
        product.chmod(0o755)
        row = runner.validate_frozen_readiness_matrix(runner.load_readiness_matrix())["rows"][0]
        case = {"id": row["id"], "matrix_binding": runner._matrix_row_binding(row), "fixtures": {}}
        result = {
            "case_id": row["id"],
            "status": "pass",
            "composition_reached": {"original": True, "product": True},
            "actions": [],
            "artifact_directory": str(root),
            "roots": {"original": {}, "product": {}},
        }
        ledger = runner._build_attempt_ledger(
            result=result,
            case={**case, "_matrix_row": row},
            contract=runner.load_contract(),
            original_binary=original,
            product_binary=product,
            source_roots={"original": root, "product": root / "candidate"},
            attempt_id="track/0",
            attempt_index=0,
        )
        assert ledger["outcome"] == "blocked"
        assert ledger["track_identity"]["complete"] is False
        assert result["status"] == "blocked"


def test_mcp_tool_must_match_published_route() -> None:
    contract = runner.load_contract()
    case = {
        "id": "mcp-tool-mismatch",
        "classification": "host_extension",
        "actions": [{"id": "call", "route": "fact_store_search", "entrypoint": "mcp_stdio", "tool": "wrong_tool", "request": {}}],
        "composition_proof": {"product": {"route": "fact_store_search", "request": {}, "required_json_pointers": ["/ok"], "equals": {"/ok": True}}},
    }
    expect_raises(runner.ContractError, lambda: runner.validate_case(case, contract))


def test_fact_store_tool_variants_match_daemon_route_mapping() -> None:
    contract = runner.load_contract()
    routes = runner._route_map(contract)
    for route_id, expected_tool in runner.FACT_STORE_MCP_ROUTE_TOOLS.items():
        route = routes[route_id]
        endpoint = route["entrypoints"]["mcp_stdio"]
        assert endpoint["tool"] == expected_tool
        assert runner._validate_mcp_tool(route, {"tool": expected_tool}, route_id) == expected_tool
        expect_raises(
            runner.RunnerError,
            lambda route=route, route_id=route_id: runner._validate_mcp_tool(
                route,
                {"tool": "tracedecay_fact_store"},
                route_id,
            ),
        )

    tampered = deepcopy(contract)
    tampered_route = next(route for route in tampered["routes"] if route["id"] == "fact_store_search")
    tampered_route["entrypoints"]["mcp_stdio"]["tool"] = "tracedecay_fact_store"
    expect_raises(runner.ContractError, lambda: runner.validate_contract(tampered))


def test_id_and_route_alias_conflicts_are_rejected_at_each_action_boundary() -> None:
    contract = runner.load_contract()
    template = runner.load_cases(RUNNER_PATH.with_name("example-case.json"))[0]

    with tempfile.TemporaryDirectory(prefix="native-original-case-alias-") as temporary:
        conflicting = Path(temporary) / "cases.json"
        conflicting.write_bytes(
            runner._json_bytes(
                [{"id": "case-a", "case_id": "case-b", "actions": []}]
            )
            + b"\n"
        )
        expect_raises(runner.ContractError, lambda: runner.load_cases(conflicting))

    bad_case_id = deepcopy(template)
    bad_case_id["case_id"] = "different-case"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_case_id, contract))

    bad_main_id = deepcopy(template)
    bad_main_id["actions"][0]["action_id"] = "different-action"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_main_id, contract))

    bad_main_route = deepcopy(template)
    bad_main_route["actions"][0]["operation"] = "fact_store_get"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_main_route, contract))

    bad_aux_id = deepcopy(template)
    bad_aux_id["side_setup"]["original"][0]["action_id"] = "different-seed"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_aux_id, contract))

    bad_aux_route = deepcopy(template)
    bad_aux_route["side_setup"]["original"][0]["operation"] = "fact_store_search"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_aux_route, contract))

    bad_checkpoint_id = deepcopy(template)
    bad_checkpoint_id["actions"][0]["checkpoints"]["before"]["action_id"] = "different-checkpoint"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_checkpoint_id, contract))

    bad_checkpoint_route = deepcopy(template)
    bad_checkpoint_route["actions"][0]["checkpoints"]["before"]["operation"] = "fact_store_get"
    expect_raises(runner.ContractError, lambda: runner.validate_case(bad_checkpoint_route, contract))


def test_nonzero_parseable_process_requires_typed_error() -> None:
    assert runner._response_status({"status": "error"}, {"status": "failed"}, None) == "unknown"
    assert runner._response_status({"status": "error"}, {"status": "error", "error": {"code": "E"}}, None) == "error"
    assert runner._parsed_terminal_status({"status": "complete", "outcome": {"status": "failed"}})[0] == "unknown"


def test_authority_correlation_rejects_response_self_assertion() -> None:
    reachability = runner._operation_reachability(
        side="product",
        route_id="memory_status",
        entrypoint="cli_tool",
        process={"status": "completed"},
        response={"store_identity": "self-asserted"},
        status="completed",
        authority_identity={"pid": 42, "process_run_id": "run"},
    )
    assert reachability["observed"] is False
    assert reachability["authority_correlated"] is False


def test_composition_proof_requires_runner_authority_evidence() -> None:
    result = {
        "status": "completed",
        "response": {"route": "memory_status", "ok": True},
    }
    assessment = runner._proof_result(
        result,
        {"required_json_pointers": ["/ok"], "equals": {"/ok": True}},
        "product",
    )
    assert assessment["status"] == "unknown"
    assert assessment["composition_reached"] is False


def test_pair_comparison_rejects_response_self_asserted_authority() -> None:
    reachability, store, _ = owned_observations("memory_status", store_value="store")
    side = {
        "status": "completed",
        "response": {"state": "stable"},
        "operation_reachability": reachability,
        "store_identity": store,
        "route_identity": owned_route_identity("memory_status", operation_kind="read"),
    }
    forged = {**side, "authority_evidence": {"authority_digest": reachability["authority_digest"]}}
    comparison = {
        "effect_mode": "none",
        "no_effect_json_pointers": ["/state"],
        "semantic_json_pointers": ["/state"],
    }
    route = {"id": "memory_status", "operation_kind": "read", "effect_policy": "none"}
    result = runner._pair_comparison(
        forged,
        forged,
        comparison=comparison,
        route=route,
        case_classification="original_native",
    )
    assert result["status"] == "unknown"
    # The missing observation is side-qualified so a report can identify
    # which leg failed authority correlation.  Check the semantic suffix
    # rather than requiring a lossy unqualified label.
    assert any(
        item.endswith(".authority_evidence")
        for item in result["missing_observations"]
    )


def test_reopen_pair_requires_exact_request_entrypoint_tool_and_bindings() -> None:
    contract = runner.load_contract()
    original = {"route": "memory_status", "entrypoint": "mcp_stdio", "tool": "tracedecay_memory_status", "request": {"a": 1}, "output_bindings": {"x": "/x"}, "comparison": {}}
    product = deepcopy(original)
    product["request"] = {"a": 2}
    mismatch = runner._validate_reopen_pair(original, product, contract=contract)
    assert mismatch and mismatch["status"] == "invalid"
    product = deepcopy(original)
    product["tool"] = "wrong"
    expect_raises(runner.ContractError, lambda: runner._validate_reopen_pair(original, product, contract=contract))


def test_retained_terminal_statuses_are_enumerated_without_pass_collapse() -> None:
    for status in runner.RETAINED_OUTCOME_STATUS_V1:
        assert status in runner._PARSED_TERMINAL_STATUS
        mapped, _ = runner._parsed_terminal_status({"status": status})
        assert mapped in (*runner.OUTCOME_STATUSES, "completed", "error")
        if status not in ("complete", "complete_zero", "ok", "recorded"):
            assert mapped != "completed"


def test_request_digest_uses_exact_captured_request_hashes() -> None:
    original_request = {"side": "original", "nested": [True, 1]}
    product_request = {"side": "product", "nested": [True, 1]}
    result = {
        "actions": [
            {
                "action_id": "a",
                "route": "memory_status",
                "original": {
                    "request_sha256": runner.bytes_digest(runner._json_bytes(original_request) + b"\n"),
                    "request": original_request,
                },
                "product": {
                    "request_sha256": runner.bytes_digest(runner._json_bytes(product_request) + b"\n"),
                    "request": product_request,
                },
            }
        ]
    }
    evidence = runner._request_digest_evidence(result)
    assert evidence["original"][0]["sha256"] == runner.bytes_digest(runner._json_bytes(original_request) + b"\n")
    assert evidence["product"][0]["sha256"] == runner.bytes_digest(runner._json_bytes(product_request) + b"\n")
    assert evidence["combined_sha256"] != runner.json_digest({})


def test_mcp_request_digest_requires_exact_stdin_exchange() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-mcp-digest-") as temporary:
        root = Path(temporary)
        request = {"query": "exact"}
        request_bytes = runner._json_bytes(request) + b"\n"
        request_path = root / "request.json"
        request_path.write_bytes(request_bytes)
        request_ref = {
            "path": str(request_path),
            "bytes": len(request_bytes),
            "sha256": runner.bytes_digest(request_bytes),
        }
        def side_result(side: str) -> dict[str, object]:
            request_id = f"native-original/{side}/mcp-digest/mcp-call"
            stdin_bytes = b"\n".join(
                runner._json_bytes(item)
                for item in (
                    {
                        "jsonrpc": "2.0",
                        "id": f"{request_id}/initialize",
                        "method": "initialize",
                        "params": {},
                    },
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": {"name": "tracedecay_memory_status", "arguments": request},
                    },
                )
            ) + b"\n"
            stdin_path = root / f"stdin-{side}.bin"
            stdin_path.write_bytes(stdin_bytes)
            stdin_ref = {
                "path": str(stdin_path),
                "bytes": len(stdin_bytes),
                "sha256": runner.bytes_digest(stdin_bytes),
            }
            return {
                "entrypoint": "mcp_stdio",
                "request": request,
                "request_sha256": request_ref["sha256"],
                "request_artifact": request_ref,
                "route_identity": {
                    "published_tool": "tracedecay_memory_status",
                },
                "process": {
                    "stdin": stdin_ref,
                    "stdin_exchange_sha256": stdin_ref["sha256"],
                },
            }
        result = {
            "case_id": "mcp-digest",
            "actions": [
                {
                    "action_id": "mcp-call",
                    "route": "memory_status",
                    "original": side_result("original"),
                    "product": side_result("product"),
                }
            ]
        }
        evidence = runner._request_digest_evidence(result)
        assert evidence["complete"] is True
        assert evidence["original"][0]["mcp_stdin"]["tool"] == "tracedecay_memory_status"
        stdin_path = root / "stdin-original.bin"
        stdin_path.write_bytes(stdin_path.read_bytes() + b"tampered")
        evidence = runner._request_digest_evidence(result)
        assert evidence["complete"] is False
        # Preserve independently verified product evidence while rejecting the
        # tampered original exchange; a partial pair must remain incomplete.
        assert evidence["original"] == [] and len(evidence["product"]) == 1


def test_normative_action_receipt_requires_prepare_key_and_exact_result() -> None:
    """Exercise the daemon's signed prepare/call/receipt contract independently."""

    with tempfile.TemporaryDirectory(prefix="native-original-action-receipt-") as temporary:
        root = Path(temporary)
        identity = {
            "side": "original",
            "pid": 123,
            "process_group_id": 123,
            "process_run_id": "daemon-run",
            "epoch": 7,
            "version": "test",
            "endpoint": {"kind": "unix", "address": str(root / "daemon.sock")},
            "profile_root": str(root / "profile"),
            "store_root": str(root / "store"),
            "process_root": str(root / "process"),
            "binary_path": str(root / "tracedecay"),
            "binary_sha256": "a" * 64,
            "authority_path": str(root / "authority.json"),
        }
        store = {
            "project_id": None,
            "project_root": str(root / "project"),
            "data_root": str(root / "data"),
            "graph_db_path": str(root / "graph.db"),
            "serving_branch": None,
        }
        arguments = {"query": "receipt-bound", "limit": 1}
        action_digest = runner._daemon_action_digest(
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            entrypoint="mcp_stdio",
            arguments=arguments,
        )
        response = {
            "jsonrpc": "2.0",
            "id": "native-original/original/case/action",
            "result": {"content": [{"type": "text", "text": "ok"}]},
        }
        candidate = {
            "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
            "revision": 1,
            "action_id": "action",
            "route": "memory_status",
            "nonce": "b" * 64,
            "action_digest": action_digest,
            "scope": store["project_root"],
            "tool": "tracedecay_memory_status",
            "entrypoint": "mcp_stdio",
            "daemon_generation": {"epoch": 7, "process_run_id": "daemon-run"},
            "store_identity": store,
            "result_digest": runner._daemon_result_digest(response),
            "expires_at": 1_700_000_000_000_001,
            "issued_at": 100,
            "receipt_mac": "",
            "receipt_sha256": "",
        }
        key = b"k" * 32
        signed = {**candidate, "_runner_proof_key": key}
        candidate["receipt_mac"] = runner._daemon_receipt_mac(signed)
        candidate["receipt_sha256"] = runner._daemon_receipt_sha256(candidate)
        validated = runner._validate_daemon_action_receipt(
            candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="action",
            daemon_identity=identity,
            action_digest=action_digest,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            expires_at=1_700_000_000_000_001,
            store_identity=store,
            result=response,
            proof_key=key,
        )
        assert validated == candidate
        assert runner._validate_daemon_action_receipt(
            candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="action",
            daemon_identity=identity,
            action_digest=action_digest,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            result={**response, "result": {"content": [{"type": "text", "text": "tampered"}]}},
            proof_key=key,
        ) is None
        assert runner._validate_daemon_action_receipt(
            candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="action",
            daemon_identity=identity,
            action_digest=action_digest,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            result=response,
            proof_key=b"x" * 32,
        ) is None


def test_action_prepare_wire_has_protocol_fields_and_accepts_direct_ack() -> None:
    """The daemon adapter returns ActionPrepareResponse as the direct result."""

    import socket

    with tempfile.TemporaryDirectory(prefix="native-original-action-prepare-") as temporary:
        root = Path(temporary)
        identity = {
            "side": "original",
            "pid": 123,
            "process_group_id": 123,
            "process_run_id": "daemon-run",
            "epoch": 7,
            "version": "test",
            "endpoint": {"kind": "loopback", "address": "127.0.0.1:32123"},
            "profile_root": str(root / "profile"),
            "store_root": str(root / "store"),
            "process_root": str(root / "process"),
            "binary_path": str(root / "tracedecay"),
            "binary_sha256": "a" * 64,
            "authority_path": str(root / "authority.json"),
        }
        captured: dict[str, bytes] = {}

        class FakeStream:
            def __init__(self) -> None:
                self.response = b""
                self.sent = False

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def settimeout(self, _timeout):
                return None

            def sendall(self, value: bytes) -> None:
                captured["request"] = bytes(value)
                request = json.loads(value.splitlines()[-1].decode("utf-8"))
                prepare = request["params"]["_meta"]["nativeOriginalActionPrepare"]
                self.response = (
                    runner._json_bytes(
                        {
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "result": {
                                "format": prepare["format"],
                                "revision": prepare["revision"],
                                "action_id": prepare["action_id"],
                                "route": prepare["route"],
                                "nonce": prepare["nonce"],
                                "action_digest": prepare["action_digest"],
                                "scope": prepare["scope"],
                                "tool": prepare["tool"],
                                "entrypoint": prepare["entrypoint"],
                                "daemon_generation": {
                                    "epoch": identity["epoch"],
                                    "process_run_id": identity["process_run_id"],
                                },
                                "store_identity": {
                                    "project_id": None,
                                    "project_root": prepare["scope"],
                                    "data_root": prepare["scope"] + "/data",
                                    "graph_db_path": prepare["scope"] + "/data/graph.db",
                                    "serving_branch": None,
                                },
                                "expires_at": prepare["expires_at"],
                            },
                        }
                    )
                    + b"\n"
                )

            def shutdown(self, _how):
                return None

            def recv(self, _size: int) -> bytes:
                if self.sent:
                    return b""
                self.sent = True
                return self.response

        stream = FakeStream()
        original_create_connection = socket.create_connection
        socket.create_connection = lambda *_args, **_kwargs: stream
        daemon = SimpleNamespace(
            side="original",
            profile_root=root / "profile",
            authority={"auth_token": "c" * 64},
            current_identity=lambda: identity,
        )
        try:
            prepared = runner._OwnedDaemon.prepare_action(
                daemon,
                route_id="memory_status",
                entrypoint="mcp_stdio",
                action_id="action",
                scope=str(root / "project"),
                tool="tracedecay_memory_status",
                arguments={"query": "wire"},
                timeout_seconds=1.0,
            )
        finally:
            socket.create_connection = original_create_connection
        lines = [json.loads(line.decode("utf-8")) for line in captured["request"].splitlines()]
        wire_prepare = lines[-1]["params"]["_meta"]["nativeOriginalActionPrepare"]
        assert lines[1]["project_path"] == wire_prepare["scope"]
        assert wire_prepare["format"] == runner.DAEMON_ACTION_RECEIPT_FORMAT
        assert wire_prepare["revision"] == 1
        assert prepared["action_digest"] == wire_prepare["action_digest"]
        child_input = runner._mcp_input(
            {
                "entrypoint": "mcp_stdio",
                "tool": "tracedecay_memory_status",
            },
            {"side": "original", "case_id": "case", "action_id": "action"},
            {"query": "wire"},
            {"entrypoints": {"mcp_stdio": {"tool": "tracedecay_memory_status"}}},
            action_prepare=prepared,
        )
        assert prepared["_runner_proof_key"].hex().encode("ascii") not in child_input
        child_call = json.loads(child_input.splitlines()[-1].decode("utf-8"))
        assert prepared["action_digest"] == runner._daemon_action_digest(
            scope=prepared["scope"],
            tool=child_call["params"]["name"],
            entrypoint=prepared["entrypoint"],
            arguments=child_call["params"]["arguments"],
        )


def test_action_prepare_rejects_ack_expiry_mismatch() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-action-expiry-") as temporary:
        root = Path(temporary)
        identity = {
            "side": "original",
            "pid": 123,
            "process_run_id": "daemon-run",
            "epoch": 7,
            "version": "test",
            "endpoint": {"kind": "unix", "address": str(root / "daemon.sock")},
            "profile_root": str(root / "profile"),
            "store_root": str(root / "store"),
            "process_root": str(root / "process"),
            "binary_path": str(root / "tracedecay"),
            "binary_sha256": "a" * 64,
            "authority_path": str(root / "authority.json"),
        }

        def fake_exchange(daemon, request, *, timeout_seconds, label):
            del daemon, timeout_seconds, label
            prepare = request["params"]["_meta"]["nativeOriginalActionPrepare"]
            acknowledgement = {
                "format": prepare["format"],
                "revision": prepare["revision"],
                "action_id": prepare["action_id"],
                "route": prepare["route"],
                "nonce": prepare["nonce"],
                "action_digest": prepare["action_digest"],
                "scope": prepare["scope"],
                "tool": prepare["tool"],
                "entrypoint": prepare["entrypoint"],
                "daemon_generation": {
                    "epoch": identity["epoch"],
                    "process_run_id": identity["process_run_id"],
                },
                "expires_at": prepare["expires_at"] + 1,
            }
            response = {"jsonrpc": "2.0", "id": request["id"], "result": acknowledgement}
            wire = runner._json_bytes(response) + b"\n"
            return response, b"request\n", wire, deepcopy(identity)

        original_exchange = runner._owned_daemon_rpc_exchange
        runner._owned_daemon_rpc_exchange = fake_exchange
        daemon = SimpleNamespace(
            side="original",
            profile_root=root / "profile",
            authority={"auth_token": "c" * 64},
            current_identity=lambda: identity,
        )
        try:
            expect_raises(
                runner.RunnerError,
                lambda: runner._OwnedDaemon.prepare_action(
                    daemon,
                    route_id="memory_status",
                    entrypoint="mcp_stdio",
                    action_id="action",
                    scope=str(root / "project"),
                    tool="tracedecay_memory_status",
                    arguments={"query": "wire"},
                    timeout_seconds=1.0,
                ),
            )
        finally:
            runner._owned_daemon_rpc_exchange = original_exchange


def test_owned_daemon_action_receipt_round_trip_is_runner_authenticated() -> None:
    """Run a child through MCP and accept only its daemon-signed receipt."""

    with tempfile.TemporaryDirectory(prefix="native-original-action-e2e-") as temporary:
        root = Path(temporary)
        side_dir = root / "side"
        side_dir.mkdir()
        binary = root / "fake-mcp.py"
        binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        binary.chmod(0o755)
        case = {"id": "e2e-case"}
        action = {
            "id": "e2e-action",
            "route": "memory_status",
            "entrypoint": "mcp_stdio",
            "tool": "tracedecay_memory_status",
            "request": {"query": "e2e"},
        }
        scope = (side_dir / "project").resolve()
        scope.mkdir()
        request_id = "native-original/original/e2e-case/e2e-action"
        store = {
            "project_id": "e2e-project",
            "project_root": str(scope),
            "data_root": str(side_dir / "data"),
            "graph_db_path": str(side_dir / "data" / "graph.db"),
            "serving_branch": "main",
        }
        key = b"r" * 32
        expires_at = time.time_ns() // 1_000 + 5_000_000
        action_digest = runner._daemon_action_digest(
            scope=str(scope),
            tool="tracedecay_memory_status",
            entrypoint="mcp_stdio",
            arguments=action["request"],
        )
        identity = {
            "side": "original",
            "pid": 777,
            "process_group_id": 777,
            "process_run_id": "e2e-run",
            "epoch": 11,
            "version": "e2e",
            "endpoint": {"kind": "unix", "address": str(side_dir / "daemon.sock")},
            "profile_root": str(side_dir / "profile"),
            "store_root": str(side_dir / "profile"),
            "process_root": str(side_dir / "process"),
            "binary_path": str(binary),
            "binary_sha256": runner.verify_binary(binary, "original")["sha256"],
            "authority_path": str(side_dir / "authority.json"),
        }
        initialize = {
            "jsonrpc": "2.0",
            "id": f"{request_id}/initialize",
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "serverInfo": {"name": "e2e-daemon", "version": "1"},
            },
        }
        response = {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"content": [{"type": "text", "text": "ok"}]},
        }
        receipt = {
            "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
            "revision": 1,
            "action_id": action["id"],
            "route": action["route"],
            "nonce": "55" * 32,
            "action_digest": action_digest,
            "scope": str(scope),
            "tool": action["tool"],
            "entrypoint": action["entrypoint"],
            "daemon_generation": {
                "epoch": identity["epoch"],
                "process_run_id": identity["process_run_id"],
            },
            "store_identity": store,
            "result_digest": runner._daemon_result_digest(response),
            "expires_at": expires_at,
            "issued_at": time.time_ns() // 1_000,
            "receipt_mac": "",
            "receipt_sha256": "",
        }
        receipt["receipt_mac"] = runner._daemon_receipt_mac(
            {**receipt, "_runner_proof_key": key}
        )
        receipt["receipt_sha256"] = runner._daemon_receipt_sha256(receipt)
        response["result"]["_meta"] = {
            "nativeOriginalActionReceipt": receipt,
        }
        # Embed only the public terminal response.  The proof key remains in
        # this test's runner-side closure and is never available to the child.
        binary.write_text(
            f"#!{sys.executable}\n"
            "import json, sys\n"
            "for _line in sys.stdin:\n"
            "    pass\n"
            f"INITIALIZE = {json.dumps(initialize, sort_keys=True)}\n"
            f"RESPONSE = {json.dumps(response, sort_keys=True)}\n"
            "print(json.dumps(INITIALIZE, sort_keys=True, separators=(',', ':')))\n"
            "print(json.dumps(RESPONSE, sort_keys=True, separators=(',', ':')))\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        identity["binary_sha256"] = runner.verify_binary(binary, "original")["sha256"]

        def prepare_action(**kwargs):
            assert kwargs["scope"] == str(scope)
            assert kwargs["arguments"] == action["request"]
            return {
                "observed": True,
                "source": "runner_owned_daemon_action_prepare",
                "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
                "revision": 1,
                "side": "original",
                "route": action["route"],
                "entrypoint": action["entrypoint"],
                "action_id": action["id"],
                "scope": str(scope),
                "tool": action["tool"],
                "nonce": receipt["nonce"],
                "action_digest": action_digest,
                "expires_at": expires_at,
                "store_identity": deepcopy(store),
                "authority_digest": runner._authority_identity_digest(identity),
                "daemon_identity": deepcopy(identity),
                "daemon_generation": deepcopy(receipt["daemon_generation"]),
                "wire_request_sha256": "a" * 64,
                "wire_response_sha256": "b" * 64,
                "_runner_proof_key": key,
            }

        daemon = SimpleNamespace(
            current_identity=lambda: deepcopy(identity),
            prepare_action=prepare_action,
        )
        contract = {
            "routes": [
                {
                    "id": "memory_status",
                    "availability": {"original": "supported"},
                    "operation_kind": "read",
                    "effect_policy": "none",
                    "entrypoints": {
                        "mcp_stdio": {
                            "tool": "tracedecay_memory_status",
                            "argv_template": [str(binary)],
                        }
                    },
                }
            ]
        }
        original_connectable = runner._endpoint_connectable
        runner._endpoint_connectable = lambda _endpoint: True
        try:
            result = runner._execute_side_action(
                side="original",
                binary=binary,
                contract=contract,
                case=case,
                action=action,
                side_dir=side_dir,
                artifact_dir=root / "artifacts",
                timeouts=runner.ProcessTimeouts(2.0, 0.5, 0.5),
                owned_daemon=daemon,
            )
        finally:
            runner._endpoint_connectable = original_connectable
        assert result["status"] == "completed", result
        assert result["composition_reached"] is True
        assert result["action_receipt"]["receipt_sha256"] == receipt["receipt_sha256"]
        stdin = (root / "artifacts" / "process" / "stdin.bin").read_bytes()
        assert key.hex().encode("ascii") not in stdin


def test_action_receipt_error_digest_excludes_attached_metadata() -> None:
    receipt = {
        "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
        "revision": 1,
        "receipt_mac": "a" * 64,
    }
    response = {
        "jsonrpc": "2.0",
        "id": "native-original/original/case/action",
        "error": {
            "code": -32000,
            "message": "typed failure",
            "data": {"nativeOriginalActionReceipt": receipt},
        },
    }
    expected = {"code": -32000, "message": "typed failure", "data": None}
    assert runner._daemon_result_payload(response) == expected
    assert runner._daemon_result_digest(response) == runner._length_prefixed_digest(
        (runner._json_bytes(expected),)
    )
    request_id = response["id"]
    initialize = {
        "jsonrpc": "2.0",
        "id": f"{request_id}/initialize",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": {"name": "daemon", "version": "1"},
        },
    }
    stdout = b"".join(
        runner._json_bytes(item) + b"\n" for item in (initialize, response)
    )
    parsed, parse_error = runner._mcp_response(
        stdout, request_id, allow_terminal_error=True
    )
    assert parsed == response and parse_error is None
    parsed, parse_error = runner._mcp_response(stdout, request_id)
    assert parsed is None and parse_error is not None
    # A tool-level MCP refusal is a successful JSON-RPC result with
    # ``isError:true``.  The runner accepts it only when the daemon attached
    # the signed receipt, and maps it to a deliberate terminal error.
    tool_error = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [{"type": "text", "text": "typed failure"}],
            "isError": True,
            "_meta": {"nativeOriginalActionReceipt": deepcopy(receipt)},
        },
    }
    tool_stdout = b"".join(
        runner._json_bytes(item) + b"\n" for item in (initialize, tool_error)
    )
    parsed, parse_error = runner._mcp_response(
        tool_stdout, request_id, allow_terminal_error=True
    )
    assert parsed == tool_error and parse_error is None
    assert runner._parsed_terminal_status(parsed)[0] == "error"
    assert runner._response_status({"status": "completed"}, parsed, None) == "error"
    parsed, parse_error = runner._mcp_response(tool_stdout, request_id)
    assert parsed is None and parse_error is not None
    with tempfile.TemporaryDirectory(prefix="native-original-error-receipt-") as temporary:
        root = Path(temporary)
        store = {
            "project_id": "error-project",
            "project_root": str(root / "project"),
            "data_root": str(root / "data"),
            "graph_db_path": str(root / "data" / "graph.db"),
            "serving_branch": "main",
        }
        identity = {
            "side": "original",
            "pid": 17,
            "process_run_id": "error-run",
            "epoch": 3,
            "version": "test",
            "endpoint": {"kind": "unix", "address": str(root / "daemon.sock")},
            "profile_root": str(root / "profile"),
            "store_root": str(root / "profile"),
            "process_root": str(root / "process"),
            "binary_path": str(root / "tracedecay"),
            "binary_sha256": "a" * 64,
            "authority_path": str(root / "authority.json"),
        }
        action_digest = "12" * 32
        candidate = {
            "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
            "revision": 1,
            "action_id": "error-action",
            "route": "memory_status",
            "nonce": "13" * 32,
            "action_digest": action_digest,
            "scope": store["project_root"],
            "tool": "tracedecay_memory_status",
            "entrypoint": "mcp_stdio",
            "daemon_generation": {"epoch": 3, "process_run_id": "error-run"},
            "store_identity": store,
            "result_digest": runner._daemon_result_digest(response),
            "expires_at": 1_700_000_000_000_001,
            "issued_at": 1_700_000_000_000_000,
            "receipt_mac": "",
            "receipt_sha256": "",
        }
        key = b"e" * 32
        candidate["receipt_mac"] = runner._daemon_receipt_mac(
            {**candidate, "_runner_proof_key": key}
        )
        candidate["receipt_sha256"] = runner._daemon_receipt_sha256(candidate)
        assert runner._validate_daemon_action_receipt(
            candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
            daemon_identity=identity,
            action_digest=action_digest,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            result=response,
            proof_key=key,
        ) == candidate
        tool_candidate = deepcopy(candidate)
        tool_candidate.update(
            {
                "action_id": "tool-error-action",
                "nonce": "14" * 32,
                "action_digest": "15" * 32,
                "result_digest": runner._daemon_result_digest(tool_error),
            }
        )
        tool_candidate["receipt_mac"] = runner._daemon_receipt_mac(
            {**tool_candidate, "_runner_proof_key": key}
        )
        tool_candidate["receipt_sha256"] = runner._daemon_receipt_sha256(tool_candidate)
        assert runner._validate_daemon_action_receipt(
            tool_candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="tool-error-action",
            daemon_identity=identity,
            action_digest="15" * 32,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            result=tool_error,
            proof_key=key,
        ) == tool_candidate
        # Removing the receipt from error.data must change the terminal
        # payload only when the response is otherwise changed; the validator
        # still authenticates the attached error-data receipt itself.
        tampered = deepcopy(response)
        tampered["error"]["message"] = "different"
        assert runner._validate_daemon_action_receipt(
            candidate,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
            daemon_identity=identity,
            action_digest=action_digest,
            scope=store["project_root"],
            tool="tracedecay_memory_status",
            result=tampered,
            proof_key=key,
        ) is None
        authority_digest = runner._authority_identity_digest(identity)
        store_identity = {
            "observed": True,
            "source": "runner_owned_daemon_action_receipt",
            "value": deepcopy(store),
            "digest": runner.json_digest(store),
            "authority_correlated": True,
            "authority_digest": authority_digest,
            "authority_source": "runner_owned_daemon_action_receipt",
        }
        wrapper = {
            "observed": True,
            "protocol_observed": True,
            "source": "runner_owned_daemon_action_receipt",
            "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
            "revision": 1,
            "side": "original",
            "route": "memory_status",
            "entrypoint": "mcp_stdio",
            "action_id": "error-action",
            "scope": store["project_root"],
            "tool": "tracedecay_memory_status",
            "nonce": candidate["nonce"],
            "action_digest": action_digest,
            "daemon_observed_route": "memory_status",
            "daemon_observed_entrypoint": "mcp_stdio",
            "expires_at": 1_700_000_000_000_001,
            "authority_digest": authority_digest,
            "receipt_sha256": candidate["receipt_sha256"],
            "receipt": deepcopy(candidate),
            "prepared_store_identity": deepcopy(store),
            "store_identity": store_identity,
            "live_store_identity": deepcopy(store),
            "daemon_identity": deepcopy(identity),
            "endpoint": deepcopy(identity["endpoint"]),
            "profile_root": identity["profile_root"],
            "process_root": identity["process_root"],
            "binary_root": str(root / "bin"),
            "store_root": identity["store_root"],
            "route_identity": {
                "route": "memory_status",
                "entrypoint": "mcp_stdio",
                "observed": True,
                "runner_owned": True,
            },
        }
        assert runner._action_receipt_wrapper_valid(
            wrapper,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
        )
        forged_mac = deepcopy(wrapper)
        forged_mac["receipt"]["receipt_mac"] = "f" * 64
        assert not runner._action_receipt_wrapper_valid(
            forged_mac,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
        )
        broken_wrapper = deepcopy(wrapper)
        broken_wrapper["live_store_identity"]["data_root"] = str(root / "other-data")
        assert not runner._action_receipt_wrapper_valid(
            broken_wrapper,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
        )
        broken_prepare = deepcopy(wrapper)
        broken_prepare["prepared_store_identity"]["data_root"] = str(root / "other-data")
        assert not runner._action_receipt_wrapper_valid(
            broken_prepare,
            side="original",
            route_id="memory_status",
            entrypoint="mcp_stdio",
            action_id="error-action",
        )


def test_action_receipt_sha256_matches_cross_language_golden_vector() -> None:
    """Pin action/result/HMAC/receipt framing independently of Python helpers."""

    def lp(parts: tuple[bytes, ...]) -> bytes:
        return b"".join(
            len(value).to_bytes(8, "big", signed=False) + value for value in parts
        )

    format_bytes = runner.DAEMON_ACTION_RECEIPT_FORMAT.encode("utf-8")
    action_args = '{"limit":5,"query":"typed dispatch"}'
    action_parts = (
        format_bytes,
        b"1",
        b"/workspace/golden",
        b"tracedecay_search",
        b"mcp_stdio",
        action_args.encode("utf-8"),
    )
    action_digest = hashlib.sha256(lp(action_parts)).hexdigest()
    assert action_digest == "ba976a9affb57255509c3074812528652eba127bb43824083d42b512886b18dc"
    assert runner._daemon_action_digest(
        scope="/workspace/golden",
        tool="tracedecay_search",
        entrypoint="mcp_stdio",
        arguments={"limit": 5, "query": "typed dispatch"},
    ) == action_digest

    # The nested vector proves recursive key ordering and the one-byte boolean
    # representation, rather than only testing flat action fields.
    nested_json = b'{"a":{"a":true,"z":2},"z":1}'
    nested_parts = (
        format_bytes,
        b"1",
        b"/scope",
        b"tool",
        b"mcp_stdio",
        nested_json,
    )
    nested_digest = hashlib.sha256(lp(nested_parts)).hexdigest()
    assert nested_digest == "845e97727c85776f2eb6586b00fa21b97feb290257eeec38f7e5a2957caa501c"
    assert runner._daemon_action_digest(
        scope="/scope",
        tool="tool",
        entrypoint="mcp_stdio",
        arguments={"z": 1, "a": {"z": 2, "a": True}},
    ) == nested_digest

    result_json = b'{"content":[{"text":"ok","type":"text"}]}'
    result_digest = hashlib.sha256(lp((result_json,))).hexdigest()
    assert result_digest == "bddd60f823dbda9f775dde4bf2bd2a2431141f1e6752c43059a92029bf3193fa"
    result = {
        "jsonrpc": "2.0",
        "id": "native-original/original/golden-action",
        "result": {"content": [{"type": "text", "text": "ok"}]},
    }
    assert runner._daemon_result_digest(result) == result_digest

    values = (
        b"golden-action",
        b"fact_store_search",
        b"11" * 32,
        b"22" * 32,
        b"/workspace/golden",
        b"tracedecay_search",
        b"mcp_stdio",
        b"9",
        b"golden-run",
        b"project-golden",
        b"/workspace/golden",
        b"/var/tmp/golden-data",
        b"/var/tmp/golden-data/db.sqlite",
        b"main",
        b"33" * 32,
        b"1700000000000100",
        b"1700000000000000",
    )
    mac_material = lp((format_bytes, b"1", *values))
    key = b"k" * 32
    receipt_mac = hmac.new(key, mac_material, hashlib.sha256).hexdigest()
    assert receipt_mac == "3dfd3d15b4dc2428aaec6a8acaab13e570e92504dc1d7542731419c824667ad2"
    receipt_sha = hashlib.sha256(
        lp((format_bytes, b"1", *values, receipt_mac.encode("ascii")))
    ).hexdigest()
    assert receipt_sha == "7a0a9eaffd0a3834cf2468752818b5cb17ca5f249c32b54d64cd810602d343c3"

    candidate = {
        "format": runner.DAEMON_ACTION_RECEIPT_FORMAT,
        "revision": 1,
        "action_id": "golden-action",
        "route": "fact_store_search",
        "nonce": "11" * 32,
        "action_digest": "22" * 32,
        "scope": "/workspace/golden",
        "tool": "tracedecay_search",
        "entrypoint": "mcp_stdio",
        "daemon_generation": {"epoch": 9, "process_run_id": "golden-run"},
        "store_identity": {
            "project_id": "project-golden",
            "project_root": "/workspace/golden",
            "data_root": "/var/tmp/golden-data",
            "graph_db_path": "/var/tmp/golden-data/db.sqlite",
            "serving_branch": "main",
        },
        "result_digest": "33" * 32,
        "expires_at": 1_700_000_000_000_100,
        "issued_at": 1_700_000_000_000_000,
        "receipt_mac": receipt_mac,
    }
    assert runner._daemon_receipt_mac(
        {**candidate, "_runner_proof_key": key}
    ) == receipt_mac
    assert runner._daemon_receipt_sha256(candidate) == receipt_sha


def test_mcp_response_rejects_unexpected_parseable_trailing_stdout() -> None:
    request_id = "native-original/original/case/action"
    target = {"jsonrpc": "2.0", "id": request_id, "result": {"content": []}}
    initialize = {
        "jsonrpc": "2.0",
        "id": f"{request_id}/initialize",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": {"name": "test", "version": "1"},
        },
    }
    response, error = runner._mcp_response(
        runner._json_bytes(initialize)
        + b"\n"
        + runner._json_bytes(target)
        + b"\n"
        + runner._json_bytes({"jsonrpc": "2.0", "id": "unexpected", "result": {}})
        + b"\n",
        request_id,
    )
    assert response is None and error
    response, error = runner._mcp_response(
        runner._json_bytes(initialize) + b"\n" + runner._json_bytes(target) + b"\n",
        request_id,
    )
    assert response == target and error is None


def test_mcp_response_requires_boolean_iserror_and_string_text_blocks() -> None:
    request_id = "native-original/original/case/action"
    initialize = {
        "jsonrpc": "2.0",
        "id": f"{request_id}/initialize",
        "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": {"name": "test", "version": "1"},
        },
    }

    def parse(target_result: dict[str, object]) -> tuple[object | None, str | None]:
        target = {"jsonrpc": "2.0", "id": request_id, "result": target_result}
        return runner._mcp_response(
            runner._json_bytes(initialize) + b"\n" + runner._json_bytes(target) + b"\n",
            request_id,
        )

    for invalid in (None, 1, "true", [], {}):
        response, error = parse(
            {"content": [{"type": "text", "text": "ok"}], "isError": invalid}
        )
        assert response is None and error
    for invalid in (None, 1, True, {}, []):
        response, error = parse({"content": [{"type": "text", "text": invalid}]})
        assert response is None and error
    response, error = parse({"content": [{"type": "text", "text": "ok"}], "isError": False})
    assert isinstance(response, dict) and error is None


def test_strict_json_boundary_rejects_trailing_duplicates_and_nonfinite_values() -> None:
    for payload in (b'{"a":1}{"b":2}', b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e400}'):
        value, error = runner._parse_json_bytes(payload)
        assert value is None
        assert error
    value, error = runner._parse_json_bytes(b'{"a":true}')
    assert value == {"a": True} and error is None
    assert runner._json_semantic_equal({"a": True}, {"a": 1}) is False


def test_receipt_digest_inputs_reject_floats_and_scope_selectors() -> None:
    # Finite floats are valid Python JSON, but there is no agreed Rust/Python
    # digest-number representation in this protocol.  Reject them at both
    # action and terminal-result digest boundaries.
    expect_raises(
        runner.RunnerError,
        lambda: runner._canonical_wire_arguments({"limit": 1.5}),
    )
    expect_raises(
        runner.RunnerError,
        lambda: runner._daemon_action_digest(
            scope="/project",
            tool="tracedecay_memory_status",
            entrypoint="mcp_stdio",
            arguments={"limit": 1.5},
        ),
    )
    response = {
        "jsonrpc": "2.0",
        "id": "native-original/original/case/action",
        "result": {"content": [{"type": "text", "value": 1.5}]},
    }
    assert runner._daemon_result_digest(response) is None

    # Reject canonical and compatibility spellings recursively, including a
    # null selector: scope must come from the daemon before prepare.
    for key in (
        "session_id",
        "sessionId",
        "thread_id",
        "threadId",
        "conversation_id",
        "provider_session_id",
        "agentSessionId",
        "task-session-id",
    ):
        expect_raises(
            runner.ContractError,
            lambda key=key: runner._reject_receipt_scoped_selectors({"nested": {key: None}}),
        )


def test_mutation_effect_receipt_state_reject_arbitrary_scalars() -> None:
    route = {"id": "write", "operation_kind": "mutation", "effect_policy": "required"}
    comparison = {
        "effect_mode": "required",
        "effect_json_pointers": ["/effect"],
        "receipt_json_pointers": ["/receipt"],
        "state_json_pointers": ["/state"],
    }
    original_reachability, original_store, evidence = owned_observations("write", store_value="store-a")
    product_reachability, product_store, _ = owned_observations("write", store_value="store-b")
    original = {
        "status": "completed",
        "response": {"effect": "arbitrary", "receipt": "arbitrary", "state": "arbitrary"},
        "operation_reachability": original_reachability,
        "authority_evidence": evidence,
        "store_identity": original_store,
        "route_identity": owned_route_identity("write", operation_kind="mutation"),
    }
    product = {
        **original,
        "operation_reachability": product_reachability,
        "store_identity": product_store,
    }
    result = runner._pair_comparison(
        original,
        product,
        comparison=comparison,
        route=route,
        case_classification="original_native",
    )
    assert result["status"] == "effect_unknown", result


def test_first_causal_stage_preserves_cleanup_failure() -> None:
    result = {
        "status": "unknown",
        "actions": [
            {
                "original": {
                    "status": "unknown",
                    "cleanup": {"status": "failed", "failure_class": "process_failure"},
                },
                "product": {"status": "completed"},
            }
        ],
    }
    assert runner._first_causal_stage(result) == "cleanup"


def test_first_causal_stage_prefers_transport_before_cleanup() -> None:
    result = {
        "status": "unknown",
        "actions": [
            {
                "original": {
                    "status": "unknown",
                    "process": {"process": {"failure_class": "process_failure"}},
                    "cleanup": {"status": "failed"},
                },
                "product": {"status": "completed"},
            }
        ],
    }
    assert runner._first_causal_stage(result) == "transport"


def test_first_causal_stage_reports_composition_after_setup_transport_checks() -> None:
    result = {
        "status": "unknown",
        "composition_evidence": {
            "original": {
                "status": "unknown",
                "composition_reached": False,
                "checks": [
                    {
                        "result": {
                            "status": "completed",
                            "process": {"status": "completed"},
                        }
                    }
                ],
            },
            "product": {"status": "pass", "composition_reached": True},
        },
        "actions": [],
    }
    assert runner._first_causal_stage(result) == "composition"


def test_first_causal_stage_reports_spawn_inside_composition_check() -> None:
    result = {
        "status": "unknown",
        "composition_evidence": {
            "original": {
                "status": "unknown",
                "composition_reached": False,
                "checks": [
                    {
                        "result": {
                            "status": "unknown",
                            "process": {
                                "process": {"failure_class": "spawn_failure"}
                            },
                        }
                    }
                ],
            }
        },
        "actions": [],
    }
    assert runner._first_causal_stage(result) == "spawn"


def test_fuzz_malformed_scalar_deep_and_pointer_inputs_are_typed() -> None:
    callbacks = (
        lambda: runner.load_contract(None),
        lambda: runner.load_cases(None),
        lambda: runner.verify_source_root(None, side="product"),
        lambda: runner._daemon_socket_path(None),
        lambda: runner._prepare_private_socket_parent(None),
        lambda: runner._proof_result(
            {"status": "completed", "response": {}},
            {"required_json_pointers": 1, "equals": {}},
            "original",
        ),
        lambda: runner._effect_evidence(
            {"actions": 1}, {"id": "malformed"}, {}, {}, {}, {}
        ),
        lambda: runner._first_causal_stage({"actions": 1}),
        lambda: runner.json_pointer([], "/" + ("9" * 5000)),
    )
    for callback in callbacks:
        expect_raises(runner.RunnerError, callback)


def test_store_identity_observed_empty_same_physical_is_unknown() -> None:
    route = {"id": "read", "operation_kind": "read", "effect_policy": "none"}
    comparison = {
        "effect_mode": "none",
        "no_effect_json_pointers": ["/state"],
        "semantic_json_pointers": ["/state"],
    }
    def side(value, observed=True):
        reachability, store, evidence = owned_observations("read", store_value=value)
        store["observed"] = observed
        return {
            "status": "completed",
            "response": {"state": "stable"},
            "operation_reachability": reachability,
            "authority_evidence": evidence,
            "route_identity": owned_route_identity("read", operation_kind="read"),
            "store_identity": store,
        }
    empty = runner._pair_comparison(side(""), side("store-b"), comparison=comparison, route=route, case_classification="original_native")
    assert empty["status"] == "unknown"
    missing = runner._pair_comparison(side(None), side("store-b"), comparison=comparison, route=route, case_classification="original_native")
    assert missing["status"] == "unknown"
    unobserved = runner._pair_comparison(side("store-a", observed=False), side("store-b"), comparison=comparison, route=route, case_classification="original_native")
    assert unobserved["status"] == "unknown"
    same = runner._pair_comparison(side("/tmp/shared-store"), side("/tmp/shared-store"), comparison=comparison, route=route, case_classification="original_native")
    assert same["status"] == "unknown"


def test_request_digest_rejects_unbacked_hash() -> None:
    evidence = runner._request_digest_evidence(
        {
            "actions": [
                {
                    "action_id": "a",
                    "route": "memory_status",
                    "original": {"request_sha256": "a" * 64},
                    "product": {"request_sha256": "b" * 64},
                }
            ]
        }
    )
    assert evidence["complete"] is False
    assert evidence["original"] == [] and evidence["product"] == []


def test_existing_attempt_ledger_rejects_duplicate_positions() -> None:
    with tempfile.TemporaryDirectory(prefix="native-original-ledger-duplicate-") as temporary:
        path = Path(temporary) / "attempts.jsonl"
        row = {field: None for field in runner.LEDGER_REQUIRED_FIELDS}
        row.update(
            {
                "attempt_id": "run/0",
                "attempt_index": 0,
                "row_id": "row-a",
                "outcome": "blocked",
            }
        )
        path.write_text(json.dumps(row) + "\n" + json.dumps({**row, "attempt_id": "run/1"}) + "\n", encoding="utf-8")
        expect_raises(runner.RunnerError, lambda: runner._validate_existing_attempt_ledger(path))


def test_timeout_kill_race_is_typed_unknown_evidence() -> None:
    class RaceProcess:
        pid = 123456
        returncode = None

        def communicate(self, input=None, timeout=None):
            raise subprocess.TimeoutExpired(["race"], timeout)

        def poll(self):
            return None

        def kill(self):
            raise OSError("process exited during kill")

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(["race"], timeout)

    original_spawn = runner._spawn
    runner._spawn = lambda *_args, **_kwargs: RaceProcess()
    try:
        with tempfile.TemporaryDirectory(prefix="native-original-kill-race-") as temporary:
            root = Path(temporary)
            result = runner.run_process(
                ["race"],
                cwd=root,
                environment=dict(os.environ),
                artifact_dir=root / "artifacts",
                timeout=runner.ProcessTimeouts(0.01, 0.01, 0.01),
            )
    finally:
        runner._spawn = original_spawn
    assert result["status"] == "unknown", result
    assert result["process"]["failure_class"] == "process_failure", result


def test_authority_shape_and_scalar_response_cannot_pass() -> None:
    authority = runner._response_authority_correlation(
        {"authority": {"digest": ["not-a-digest"]}}, "a" * 64
    )
    assert authority["observed"] is False
    assert runner._response_status({"status": "completed"}, "scalar", None) == "unknown"
    assert runner.compare_results(
        {"status": "completed", "response": "scalar"},
        {"status": "completed", "response": "scalar"},
    )["status"] == "unknown"


def main() -> int:
    tests = [
        test_semantic_mismatch_preserves_observables,
        test_missing_required_observable_is_unknown,
        test_status_priority_materializes_iterable,
        test_missing_original_binary,
        test_binary_symlink_is_rejected_before_resolution,
        test_shared_binary_is_refused,
        test_shared_binary_root_is_refused,
        test_modified_reference_checkout_is_refused,
        test_reference_revision_override_is_refused,
        test_zero_relevant_cases_and_missing_operation_coverage,
        test_readiness_rejects_filtered_or_incomplete_suites,
        test_frozen_readiness_matrix_rows_and_counts_are_authoritative,
        test_frozen_readiness_matrix_semantic_bindings_are_authoritative,
        test_case_result_digest_authenticates_final_serialized_payload,
        test_case_result_digest_matches_independent_canonical_expectation,
        test_case_schema_requires_nonempty_actions_at_runtime,
        test_ledger_receipt_requires_one_complete_canonical_jsonl_row,
        test_embedded_case_schema_is_valid_and_bound_to_case_artifacts,
        test_normative_wrapper_check_is_part_of_the_bounded_suite,
        test_readiness_attempt_population_blocks_short_runs,
        test_run_suite_readiness_blocks_unpopulated_matrix,
        test_operation_effect_policy_is_typed,
        test_production_durable_observations_are_emitted_from_after_and_reopen,
        test_unavailable_route_identity_has_a_typed_boundary,
        test_operation_reachability_and_store_identity_are_required,
        test_reopen_specs_are_side_specific,
        test_ledger_includes_checkpoint_reopen_and_no_effect_evidence,
        test_full_materialized_ledger_passes_independent_revalidation,
        test_child_process_is_bounded_and_group_is_cleaned,
        test_spawn_failure_returns_typed_evidence,
        test_cleanup_failure_cannot_complete_an_action,
        test_run_suite_appends_ledger_when_case_raises,
        test_matrix_case_ledger_cli_compatibility_records_blocked_attempt,
        test_parseable_terminal_outcomes_are_typed,
        test_owned_daemon_publishes_and_cleans_identity,
        test_readiness_cli_requires_cases,
        test_readiness_binding_is_exact_and_raw_matrix_is_authoritative,
        test_readiness_action_route_cannot_relabel_every_row_as_search,
        test_readiness_no_callable_rows_require_explicit_unavailable_route,
        test_false_callable_native_rows_are_explicitly_unavailable,
        test_setup_schema_is_closed_and_pairwise_identical,
        test_retrieval_oracle_cannot_use_an_empty_store,
        test_setup_mutation_effects_are_materialized_without_unbound_routes,
        test_population_rejects_unexpected_and_overcounted_rows,
        test_frozen_population_rejects_duplicate_or_relabelled_attempts,
        test_readiness_receipt_re_reads_the_named_ledger_span,
        test_store_identity_aggregate_carries_an_observed_value,
        test_typed_effect_rejects_generic_status_and_marker_maps,
        test_setup_failure_without_process_evidence_is_causal_setup,
        test_ncm_worker_requires_attested_native_process,
        test_ncm_worker_manifest_is_runner_input_and_response_claims_fail_closed,
        test_independent_ncm_conformance_does_not_require_a_570_worker,
        test_native_and_semantic_identity_requires_receipt_authenticated_response,
        test_immutable_copy_root_must_be_private_and_source_is_rechecked,
        test_immutable_copy_modes_are_owner_only_and_sealed_consistently,
        test_render_context_never_exposes_authority_challenge_response,
        test_partial_authority_identity_cannot_correlate_reachability,
        test_unknown_effect_policy_cannot_pass,
        test_track_identity_missing_blocks_matrix_pass,
        test_mcp_tool_must_match_published_route,
        test_fact_store_tool_variants_match_daemon_route_mapping,
        test_id_and_route_alias_conflicts_are_rejected_at_each_action_boundary,
        test_nonzero_parseable_process_requires_typed_error,
        test_authority_correlation_rejects_response_self_assertion,
        test_composition_proof_requires_runner_authority_evidence,
        test_pair_comparison_rejects_response_self_asserted_authority,
        test_reopen_pair_requires_exact_request_entrypoint_tool_and_bindings,
        test_retained_terminal_statuses_are_enumerated_without_pass_collapse,
        test_request_digest_uses_exact_captured_request_hashes,
        test_mcp_request_digest_requires_exact_stdin_exchange,
        test_normative_action_receipt_requires_prepare_key_and_exact_result,
        test_action_prepare_wire_has_protocol_fields_and_accepts_direct_ack,
        test_action_prepare_rejects_ack_expiry_mismatch,
        test_owned_daemon_action_receipt_round_trip_is_runner_authenticated,
        test_action_receipt_error_digest_excludes_attached_metadata,
        test_action_receipt_sha256_matches_cross_language_golden_vector,
        test_mcp_response_rejects_unexpected_parseable_trailing_stdout,
        test_mcp_response_requires_boolean_iserror_and_string_text_blocks,
        test_strict_json_boundary_rejects_trailing_duplicates_and_nonfinite_values,
        test_receipt_digest_inputs_reject_floats_and_scope_selectors,
        test_mutation_effect_receipt_state_reject_arbitrary_scalars,
        test_first_causal_stage_preserves_cleanup_failure,
        test_first_causal_stage_prefers_transport_before_cleanup,
        test_first_causal_stage_reports_composition_after_setup_transport_checks,
        test_first_causal_stage_reports_spawn_inside_composition_check,
        test_fuzz_malformed_scalar_deep_and_pointer_inputs_are_typed,
        test_store_identity_observed_empty_same_physical_is_unknown,
        test_request_digest_rejects_unbacked_hash,
        test_existing_attempt_ledger_rejects_duplicate_positions,
        test_timeout_kill_race_is_typed_unknown_evidence,
        test_authority_shape_and_scalar_response_cannot_pass,
    ]
    for test in tests:
        test()
        print(f"ok {test.__name__}")
    print(f"{len(tests)} native-original runner checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
