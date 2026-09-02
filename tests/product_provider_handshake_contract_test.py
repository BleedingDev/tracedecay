#!/usr/bin/env python3
"""Contract tests for provider handshake compatibility and request control."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CONTRACT = REPO / "product/contracts/memory-provider-v1/provider-handshake-contract.json"
SCHEMA = REPO / "product/contracts/memory-provider-v1/provider-handshake-contract.schema.json"
DOC = REPO / "product/contracts/memory-provider-v1/provider-handshake-contract.md"
ISSUES = REPO / ".beads/issues.jsonl"
CHECKER = REPO / "scripts/product/check-provider-handshake-contract.py"


class ProviderHandshakeContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    def run_checker(
        self,
        contract: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        if contract is None and schema is None:
            contract_path = CONTRACT
            schema_path = SCHEMA
            temporary = None
        else:
            temporary = tempfile.TemporaryDirectory()
            root = Path(temporary.name)
            contract_path = root / "contract.json"
            schema_path = root / "schema.json"
            contract_path.write_text(
                json.dumps(contract or self.contract, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            schema_path.write_text(
                json.dumps(schema or self.schema, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        try:
            return subprocess.run(
                [
                    "python3",
                    str(CHECKER),
                    "--repo",
                    str(REPO),
                    "--contract",
                    str(contract_path),
                    "--schema",
                    str(schema_path),
                    "--doc",
                    str(DOC),
                    "--issues",
                    str(ISSUES),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        finally:
            if temporary is not None:
                temporary.cleanup()

    def assert_rejected(
        self,
        contract: dict[str, Any],
        marker: str,
        schema: dict[str, Any] | None = None,
    ) -> None:
        result = self.run_checker(contract, schema)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertFalse(receipt["ok"])
        self.assertIn(marker, "\n".join(receipt["errors"]))

    def limit(self, contract: dict[str, Any], limit_id: str) -> dict[str, Any]:
        return next(row for row in contract["limit_catalog"] if row["id"] == limit_id)

    def test_real_repository_contract_is_valid(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(
            receipt["contract_id"], "tracedecay.memory.provider.handshake.v1"
        )
        self.assertEqual(receipt["bead_id"], "tdmem-0202")
        self.assertEqual(receipt["protocol"], "1.0")
        self.assertEqual(receipt["limit_count"], 8)
        self.assertEqual(receipt["readiness_state_count"], 22)
        self.assertTrue(receipt["handshake_read_only"])
        self.assertFalse(receipt["silent_fallback"])

    def test_cross_major_implicit_downgrade_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["protocol_identity"]["implicit_downgrade"] = True
        self.assert_rejected(contract, "implicit protocol downgrade must be false")

    def test_empty_protocol_intersection_must_fail(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["protocol_identity"]["empty_intersection_policy"] = "select_host_default"
        self.assert_rejected(
            contract,
            "empty protocol intersection must reject as incompatible",
        )

    def test_handshake_request_must_carry_deadline_and_cancellation(self) -> None:
        contract = copy.deepcopy(self.contract)
        fields = contract["handshake_request"]["required_fields"]
        fields.remove("deadline")
        fields.remove("cancellation")
        self.assert_rejected(
            contract,
            "handshake request required fields must remain canonical and ordered",
        )

    def test_provider_id_must_come_from_registry_selection(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["handshake_request"]["provider_id_source"] = "provider_response"
        self.assert_rejected(
            contract,
            "provider ID must come from accepted registry selection",
        )

    def test_scope_must_come_from_tracedecay(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["handshake_request"]["exact_scope_source"] = "provider_path"
        self.assert_rejected(
            contract,
            "exact scope must come from TraceDecay scope authority",
        )

    def test_provider_instance_id_cannot_be_provider_identity(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["handshake_response"]["provider_instance_id_semantics"] = "stable_provider_id"
        self.assert_rejected(
            contract,
            "provider instance ID must remain opaque runtime identity",
        )

    def test_runtime_location_cannot_be_identity(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["implementation_identity"]["socket_path_is_identity"] = True
        self.assert_rejected(
            contract,
            "implementation_identity.socket_path_is_identity must be false",
        )

    def test_state_path_cannot_be_authority(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["state_identity"]["path_is_authority"] = True
        self.assert_rejected(contract, "provider state path must never be authority")

    def test_scope_wildcards_are_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["wildcards_allowed"] = True
        self.assert_rejected(
            contract,
            "exact_scope_identity.wildcards_allowed must be false",
        )

    def test_scope_cannot_be_inferred_from_cwd(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["cwd_inference_allowed"] = True
        self.assert_rejected(
            contract,
            "exact_scope_identity.cwd_inference_allowed must be false",
        )

    def test_exact_scope_digest_golden_bytes_are_fixed(self) -> None:
        digest = self.contract["exact_scope_identity"]["digest"]
        fields = [
            "profile_id",
            "project_id",
            "repository_identity",
            "worktree_identity",
            "branch_identity",
            "agent_session_id",
            "resolved_scope_digest",
        ]
        self.assertEqual(digest["string_field_order"], fields)
        self.assertEqual(
            digest["string_field_encoding"],
            "u64_big_endian_byte_length_then_utf8_bytes",
        )
        self.assertNotIn("scope_revision_encoding", digest)

        golden = digest["golden_vector"]
        canonical = bytearray(digest["domain_ascii"].encode("ascii"))
        canonical.extend(bytes.fromhex(digest["domain_suffix_byte_hex"]))
        for field in fields:
            encoded = golden[field].encode("utf-8")
            canonical.extend(len(encoded).to_bytes(8, "big"))
            canonical.extend(encoded)

        offset = len(digest["domain_ascii"].encode("ascii")) + 1
        for field in fields:
            byte_length = int.from_bytes(canonical[offset : offset + 8], "big")
            offset += 8
            encoded = golden[field].encode("utf-8")
            self.assertEqual(byte_length, len(encoded))
            self.assertEqual(canonical[offset : offset + byte_length], encoded)
            offset += byte_length
        self.assertEqual(offset, len(canonical))
        self.assertEqual(
            hashlib.sha256(canonical).hexdigest(),
            "2f525c8c3d59bfa3d9729405c4f3f1307fade77494b6ddf251c89abc490f0a52",
        )
        self.assertEqual(
            golden["digest"],
            "2f525c8c3d59bfa3d9729405c4f3f1307fade77494b6ddf251c89abc490f0a52",
        )
        self.assertTrue(golden["resolved_scope_digest"].startswith("sha256:"))

    def test_exact_scope_digest_field_order_is_canonical(self) -> None:
        contract = copy.deepcopy(self.contract)
        order = contract["exact_scope_identity"]["digest"]["string_field_order"]
        order[0], order[1] = order[1], order[0]
        self.assert_rejected(contract, "exact scope digest string field order drifted")

    def test_exact_scope_digest_boundaries_are_length_prefixed(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["digest"][
            "string_field_encoding"
        ] = "concatenated_utf8_bytes"
        self.assert_rejected(
            contract,
            "exact scope digest string boundary encoding drifted",
        )

    def test_exact_scope_digest_rejects_reintroduced_scope_revision_field(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["digest"][
            "scope_revision_encoding"
        ] = "u64_big_endian"
        self.assert_rejected(contract, "exact_scope_identity.digest fields drifted")

    def test_exact_scope_digest_uses_resolved_scope_digest_not_numeric_revision(
        self,
    ) -> None:
        digest = self.contract["exact_scope_identity"]["digest"]
        self.assertIn("resolved_scope_digest", digest["string_field_order"])
        self.assertNotIn("scope_revision", digest["string_field_order"])
        contract = copy.deepcopy(self.contract)
        order = contract["exact_scope_identity"]["digest"]["string_field_order"]
        order[order.index("resolved_scope_digest")] = "scope_revision"
        self.assert_rejected(contract, "exact scope digest string field order drifted")

    def test_exact_scope_digest_domain_requires_trailing_nul(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["digest"]["domain_suffix_byte_hex"] = ""
        self.assert_rejected(
            contract,
            "exact scope digest domain must end with one NUL byte",
        )

    def test_exact_scope_digest_golden_output_is_checked(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["digest"]["golden_vector"][
            "digest"
        ] = "0" * 64
        self.assert_rejected(contract, "exact scope digest fixed golden vector drifted")

    def test_exact_scope_digest_output_is_strict_lowercase_hex(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["exact_scope_identity"]["digest"][
            "output_encoding"
        ] = "uppercase_hex_64"
        self.assert_rejected(
            contract,
            "exact scope digest output must be lowercase SHA-256 hex",
        )

    def test_missing_limit_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["limit_catalog"] = [
            row for row in contract["limit_catalog"] if row["id"] != "response_bytes"
        ]
        self.assert_rejected(
            contract,
            "limit catalog must exactly contain the eight V1 bounded limits",
        )

    def test_unbounded_limit_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["limit_negotiation"]["unbounded_value_allowed"] = True
        self.assert_rejected(
            contract,
            "limit_negotiation.unbounded_value_allowed must be false",
        )

    def test_provider_cannot_exceed_effective_limit(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["limit_negotiation"]["provider_may_exceed_effective_limit"] = True
        self.assert_rejected(
            contract,
            "limit_negotiation.provider_may_exceed_effective_limit must be false",
        )

    def test_effective_limit_must_use_lower_ceiling(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["limit_negotiation"]["algorithm"] = "provider_ceiling_wins"
        self.assert_rejected(
            contract,
            "effective limit algorithm must take the host/provider minimum",
        )

    def test_expired_deadline_cannot_call_provider(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control"]["deadline"][
            "expired_before_dispatch_policy"
        ] = "call_and_cancel"
        self.assert_rejected(
            contract,
            "expired deadline must terminate without provider call",
        )

    def test_deadline_cannot_be_extended(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control"]["deadline"]["deadline_extension_allowed"] = True
        self.assert_rejected(contract, "deadline extension must be false")

    def test_already_cancelled_request_cannot_call_provider(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control"]["cancellation"][
            "already_cancelled_policy"
        ] = "call_provider"
        self.assert_rejected(
            contract,
            "already-cancelled request must terminate without provider call",
        )

    def test_cancellation_cannot_be_success(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["request_control"]["cancellation"][
            "cancellation_as_success_allowed"
        ] = True
        self.assert_rejected(contract, "cancellation as success must be false")

    def test_process_existence_cannot_prove_ready(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["side_effect_contract"]["ready_from_process_existence_allowed"] = True
        self.assert_rejected(
            contract,
            "side_effect_contract.ready_from_process_existence_allowed must be false",
        )

    def test_handshake_cannot_mutate_provider_state(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["side_effect_contract"]["provider_state_mutation_allowed"] = True
        self.assert_rejected(
            contract,
            "side_effect_contract.provider_state_mutation_allowed must be false",
        )

    def test_handshake_cannot_inject_context(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["side_effect_contract"]["context_injection_allowed"] = True
        self.assert_rejected(
            contract,
            "side_effect_contract.context_injection_allowed must be false",
        )

    def test_readiness_states_cannot_hide_scope_mismatch(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["readiness_states"].remove("scope_mismatch")
        self.assert_rejected(
            contract,
            "readiness states must exactly cover the V1 typed outcomes",
        )

    def test_ready_receipt_cannot_survive_provider_restart(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["ready_receipt"]["portable_across_provider_restart"] = True
        self.assert_rejected(
            contract,
            "ready_receipt.portable_across_provider_restart must be false",
        )

    def test_unknown_verification_bead_is_rejected(self) -> None:
        contract = copy.deepcopy(self.contract)
        contract["verification_beads"].append("tdmem-9999")
        self.assert_rejected(
            contract,
            "verification_beads references unknown Beads issue tdmem-9999",
        )

    def test_schema_root_cannot_allow_unknown_fields(self) -> None:
        schema = copy.deepcopy(self.schema)
        schema["additionalProperties"] = True
        self.assert_rejected(
            copy.deepcopy(self.contract),
            "handshake schema root must be a strict object",
            schema,
        )


if __name__ == "__main__":
    unittest.main()
