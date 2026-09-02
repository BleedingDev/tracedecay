#!/usr/bin/env python3
"""Contract tests for the tdmem-0507 observation hygiene policy.

Standard library only, mirroring
``tests/product_host_event_observation_policy_test.py``: the document is walked
by hand so the structural shape the schema demands is enforced together with
the cross-artifact invariants a pure schema check cannot express — that the
Rust table, the provider-boundary receipt vocabulary, and the product document
name the same classes, actions, prefixes, and digest domains.
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import unittest
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
POLICY = REPO / "product/observations/observation-hygiene-policy-v1.json"
SCHEMA = REPO / "product/observations/observation-hygiene-policy-v1.schema.json"
EVENT_POLICY = REPO / "product/observations/host-event-observation-policy.json"
OBSERVATION_CONTRACT = (
    REPO / "product/contracts/memory-provider-v1/provider-observation-contract.json"
)
HYGIENE_CRATE_ROOT = REPO / "crates/tracedecay-memory-hygiene"
HYGIENE_CRATE = HYGIENE_CRATE_ROOT / "src"
EMBEDDED_POLICY = (
    HYGIENE_CRATE_ROOT / "policy/observation-hygiene-policy-v1.json"
)
PROVIDER_API_HYGIENE = (
    REPO / "crates/tracedecay-memory-provider-api/src/hygiene.rs"
)

SEVERITY_LADDER = ["accept", "annotate", "redact", "quarantine", "reject"]
WITHHELD_REASONS = ["secret_rejected", "quarantined", "unclassifiable_payload"]


def squeeze(source: str) -> str:
    """Collapses whitespace runs so an assertion survives a rustfmt reflow.

    These gates are about what the Rust source declares, not about where
    rustfmt chose to break the line.
    """
    return " ".join(source.split())


def framed_sha256(domain: bytes, parts: list[bytes]) -> str:
    """Reproduces ``tracedecay_domain::canonical_text::canonical_framed_sha256``.

    Every part, the domain separator included, is prefixed with its big-endian
    ``u64`` length. Restating the framing here rather than shelling out to Rust
    is the point: a change to either side's framing has to be made twice, and
    the golden digest below catches it if it is only made once.
    """
    digest = hashlib.sha256()
    digest.update(struct.pack(">Q", len(domain)))
    digest.update(domain)
    for part in parts:
        digest.update(struct.pack(">Q", len(part)))
        digest.update(part)
    return digest.hexdigest()


class ObservationHygienePolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy: dict[str, Any] = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.schema: dict[str, Any] = json.loads(SCHEMA.read_text(encoding="utf-8"))
        cls.policy_rs: str = (HYGIENE_CRATE / "policy.rs").read_text(encoding="utf-8")
        cls.lib_rs: str = (HYGIENE_CRATE / "lib.rs").read_text(encoding="utf-8")
        cls.findings_rs: str = (HYGIENE_CRATE / "findings.rs").read_text(
            encoding="utf-8"
        )
        cls.credentials_rs: str = (HYGIENE_CRATE / "credentials.rs").read_text(
            encoding="utf-8"
        )
        cls.api_rs: str = PROVIDER_API_HYGIENE.read_text(encoding="utf-8")

    # -- document shape ------------------------------------------------------

    def test_documents_parse_as_json_objects(self) -> None:
        self.assertIsInstance(self.policy, dict)
        self.assertIsInstance(self.schema, dict)

    def test_policy_carries_every_required_top_level_key(self) -> None:
        for key in self.schema["required"]:
            self.assertIn(key, self.policy, f"policy is missing required key {key!r}")

    def test_policy_declares_no_key_the_schema_does_not_allow(self) -> None:
        self.assertFalse(self.schema["additionalProperties"])
        allowed = set(self.schema["properties"])
        self.assertEqual(
            set(self.policy) - allowed,
            set(),
            "policy declares keys the schema forbids",
        )

    def test_policy_identity_matches_schema_constants(self) -> None:
        self.assertEqual(self.policy["schema_version"], 1)
        self.assertEqual(
            self.policy["contract_id"], "tracedecay.observation-hygiene-policy.v1"
        )
        self.assertEqual(self.policy["bead_id"], "tdmem-0507")
        self.assertEqual(self.policy["status"], "accepted")
        self.assertEqual(
            self.policy["sanitizer_id"], "tracedecay.memory.observation.hygiene.v1"
        )

    # -- severity ladder and actions -----------------------------------------

    def test_severity_ladder_is_the_canonical_total_order(self) -> None:
        self.assertEqual(self.policy["severity_ladder"], SEVERITY_LADDER)

    def test_every_class_uses_a_ladder_action_and_a_unique_identity(self) -> None:
        classes = self.policy["classes"]
        self.assertGreaterEqual(len(classes), 12)
        class_ids = [row["class_id"] for row in classes]
        self.assertEqual(len(class_ids), len(set(class_ids)), "duplicate class_id")
        for row in classes:
            self.assertRegex(row["class_id"], r"^[a-z][a-z0-9_]*$")
            self.assertIn(row["action"], SEVERITY_LADDER)
            self.assertTrue(row["rationale"].strip())
            self.assertTrue(row["detector_reason"].strip())

    def test_action_and_withheld_reason_agree_for_every_class(self) -> None:
        for row in self.policy["classes"]:
            action = row["action"]
            reason = row["withheld_reason"]
            if action == "reject":
                self.assertEqual(reason, "secret_rejected", row["class_id"])
            elif action == "quarantine":
                self.assertEqual(reason, "quarantined", row["class_id"])
            else:
                self.assertIsNone(
                    reason,
                    f"{row['class_id']} is delivered but declares a withheld reason",
                )

    def test_only_redact_mutates_the_provider_bound_payload(self) -> None:
        for row in self.policy["classes"]:
            self.assertEqual(
                row["mutates_payload"],
                row["action"] == "redact",
                f"{row['class_id']} disagrees about rewriting bytes",
            )

    def test_reject_floor_is_a_subset_of_the_rejecting_classes(self) -> None:
        rejecting = {
            row["class_id"] for row in self.policy["classes"] if row["action"] == "reject"
        }
        floor = self.policy["reject_floor_classes"]
        self.assertEqual(len(floor), len(set(floor)), "duplicate reject-floor class")
        self.assertTrue(floor)
        self.assertEqual(
            set(floor),
            rejecting,
            "every rejecting class must sit on the reject floor and vice versa",
        )

    def test_no_transient_class_may_withhold(self) -> None:
        transient = [
            row for row in self.policy["classes"] if row["class_id"].startswith("transient_")
        ]
        self.assertGreaterEqual(len(transient), 4)
        for row in transient:
            self.assertIn(
                row["action"],
                ("accept", "annotate", "redact"),
                f"{row['class_id']} would withhold an observation over transient noise",
            )
            self.assertIsNone(row["withheld_reason"], row["class_id"])

    # -- receipt vocabulary ---------------------------------------------------

    def test_dispositions_and_withheld_reasons_are_closed_sets(self) -> None:
        self.assertEqual(self.policy["dispositions"]["delivered"], ["accepted", "redacted"])
        self.assertEqual(self.policy["dispositions"]["withheld"], WITHHELD_REASONS)
        self.assertIs(
            self.policy["dispositions"]["accepted_requires_identical_bytes"], True
        )
        self.assertIs(
            self.policy["dispositions"]["redacted_requires_different_bytes"], True
        )

    def test_receipt_declares_framing_field_order_and_carries_no_matched_bytes(
        self,
    ) -> None:
        receipt = self.policy["receipt"]
        self.assertEqual(receipt["framing"], "length_prefixed_sha256")
        self.assertIs(receipt["carries_matched_bytes"], False)
        self.assertEqual(
            receipt["field_order"],
            [
                "sanitizer_revision",
                "source_payload_sha256",
                "sanitized_payload_sha256",
                "extensions_digest",
                "disposition",
                "finding_count",
                "findings_digest",
            ],
        )
        self.assertEqual(
            receipt["withheld_field_order"],
            [
                "sanitizer_revision",
                "source_payload_sha256",
                "extensions_digest",
                "reason",
                "finding_count",
                "findings_digest",
            ],
        )
        self.assertNotEqual(receipt["id_prefix"], receipt["withheld_id_prefix"])

    def test_receipt_prefixes_and_digest_domains_match_the_rust_sources(self) -> None:
        receipt = self.policy["receipt"]
        self.assertIn(
            f'pub const OBSERVATION_HYGIENE_RECEIPT_ID_PREFIX: &str = "{receipt["id_prefix"]}"',
            self.api_rs,
        )
        self.assertIn(
            "pub const OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX: &str = "
            f'"{receipt["withheld_id_prefix"]}"',
            self.api_rs,
        )
        self.assertIn(
            f'RECEIPT_DIGEST_DOMAIN: &[u8] = b"{receipt["digest_domain"]}"',
            self.api_rs,
        )
        self.assertIn(receipt["findings_digest_domain"], self.api_rs)

    def test_the_withheld_identity_has_its_own_digest_domain(self) -> None:
        # A delivered receipt and a withheld audit row derive over different
        # field sets; sharing a domain separator would let one derivation's
        # framed input be replayed as the other's.
        receipt = self.policy["receipt"]
        self.assertNotEqual(
            receipt["digest_domain"],
            receipt["withheld_digest_domain"],
            "the withheld identity must not reuse the receipt digest domain",
        )
        squeezed_api_rs = squeeze(self.api_rs)
        self.assertIn(
            f'WITHHELD_DIGEST_DOMAIN: &[u8] = b"{receipt["withheld_digest_domain"]}"',
            squeezed_api_rs,
        )
        self.assertNotIn(
            f'WITHHELD_DIGEST_DOMAIN: &[u8] = b"{receipt["digest_domain"]}"',
            squeezed_api_rs,
        )
        self.assertTrue(receipt["domain_separation_reason"].strip())

    def test_findings_digest_domain_and_golden_digest_are_gated(self) -> None:
        # The findings digest is part of every receipt identifier, so both the
        # domain separator and the framing are contract. `findings.rs` must
        # declare the document's domain, and the digest of the empty finding set
        # recomputed from that domain must equal the golden value the document
        # publishes — which `policy_contract.rs` asserts `findings_digest(&[])`
        # also equals.
        receipt = self.policy["receipt"]
        domain = receipt["findings_digest_domain"]
        self.assertIn(
            f'FINDINGS_DIGEST_DOMAIN: &[u8] = b"{domain}"',
            squeeze(self.findings_rs),
            "findings.rs no longer frames the findings digest under the "
            "domain the product document declares",
        )
        self.assertEqual(
            framed_sha256(domain.encode("utf-8"), []),
            receipt["empty_findings_digest"],
            "the golden empty-findings digest drifted from the declared domain "
            "or from the length-prefixed framing",
        )

    def test_the_embedded_crate_copy_is_byte_identical_to_the_product_document(
        self,
    ) -> None:
        # The crate embeds its own copy so it compiles and packages inside its
        # ownership area (`crates/tracedecay-memory-hygiene/**`) instead of
        # reaching across it with an `include_str!`. This gate is what keeps the
        # two copies one document.
        self.assertTrue(
            EMBEDDED_POLICY.exists(),
            f"{EMBEDDED_POLICY} is missing; the crate cannot embed its policy",
        )
        self.assertEqual(
            EMBEDDED_POLICY.read_bytes(),
            POLICY.read_bytes(),
            "the crate-local policy copy drifted from the canonical product "
            "document; copy product/observations/"
            "observation-hygiene-policy-v1.json over it",
        )
        self.assertIn(
            'include_str!("../policy/observation-hygiene-policy-v1.json")',
            self.policy_rs,
            "the crate must embed its own copy, not reach outside its area",
        )
        squeezed_policy_rs = squeeze(self.policy_rs)
        self.assertIn(
            'OBSERVATION_HYGIENE_POLICY_V1_CANONICAL_PATH: &str = '
            '"product/observations/observation-hygiene-policy-v1.json"',
            squeezed_policy_rs,
        )
        self.assertIn(
            'OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH: &str = '
            '"crates/tracedecay-memory-hygiene/policy/'
            'observation-hygiene-policy-v1.json"',
            squeezed_policy_rs,
        )

    def test_reject_floor_signals_only_harden_and_are_implemented(self) -> None:
        signals = self.policy["reject_floor_signals"]
        floor = set(self.policy["reject_floor_classes"])
        declared = signals["direct_signal_classes"] + signals["probe_signal_classes"]
        self.assertTrue(declared)
        for class_id in declared:
            self.assertIn(
                class_id,
                floor,
                f"{class_id} is a multi-signal class but is not on the reject "
                "floor, so the supplementary pass could change a "
                "classification instead of hardening it",
            )
        self.assertIs(
            signals["known_credential_prefixes_are_exhaustive"],
            False,
            "the vendored catalogue has one owner; this list is a floor, not a copy",
        )
        self.assertTrue(signals["reason"].strip())
        self.assertGreaterEqual(signals["minimum_credential_run_length"], 8)
        self.assertGreaterEqual(signals["entropy_candidate_minimum_length"], 16)
        self.assertGreaterEqual(signals["maximum_detector_probes_per_payload"], 1)
        for separator in signals["candidate_separators"]:
            self.assertEqual(len(separator), 1)
        for prefix in signals["known_credential_prefixes"]:
            self.assertTrue(prefix.isascii() and len(prefix) >= 2, prefix)
        self.assertEqual(
            len(signals["known_credential_prefixes"]),
            len(set(signals["known_credential_prefixes"])),
        )
        # The pass exists in the crate and fails closed on an exhausted budget.
        self.assertIn("fn credential_classes(", self.credentials_rs)
        self.assertIn("HygieneClass::DetectorUnavailable", self.credentials_rs)

    def test_a_credential_bearing_key_never_forms_a_path_segment(self) -> None:
        # The finding on the key was already anchored to a placeholder, but the
        # key also formed a path segment, so every descendant finding carried it
        # verbatim into the receipt's findings digest.
        self.assertIn("CredentialKey(String)", self.findings_rs)
        self.assertIn("pub fn credential_bearing_key_marker(", self.findings_rs)
        self.assertIn("PathSegment::CredentialKey(marker)", self.lib_rs)

    def test_an_unexplained_byte_change_is_an_error_not_a_fabricated_finding(
        self,
    ) -> None:
        self.assertIn("pub fn attribute_sanitizer_output(", self.lib_rs)
        self.assertIn("HygieneError::UnattributedRedaction", self.lib_rs)
        self.assertIn(
            "_ => Err(HygieneError::UnattributedRedaction),",
            squeeze(self.lib_rs),
            "the diff-attribution fallback must refuse to attribute a change it "
            "cannot explain, rather than assert a class",
        )
        self.assertNotIn(
            "_ => found.push(HygieneFindingV1::new( HygieneClass::CredentialAssignment,",
            squeeze(self.lib_rs),
            "the fallback must not fabricate a class for an unexplained change",
        )

    def test_receipt_field_order_matches_the_rust_derivation_order(self) -> None:
        # Extracts the exact ordered list of fields `derive_receipt_id` frames,
        # so that both a field the document forgets to declare and a field the
        # document declares but the Rust derivation no longer frames are
        # caught — a plain subsequence/ordering check misses the first kind.
        match = re.search(
            r"fn derive_receipt_id\(.*?framed_digest\(\s*RECEIPT_DIGEST_DOMAIN,\s*&\[(.*?)\],\s*\);",
            self.api_rs,
            re.S,
        )
        self.assertIsNotNone(
            match, "derive_receipt_id no longer frames a receipt digest array"
        )
        args_block = match.group(1)
        identifiers: list[str] = []
        for raw_line in args_block.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            bytes_local = re.fullmatch(r"&(?P<name>[a-z_][a-z0-9_]*)_bytes", line)
            direct_call = re.fullmatch(
                r"(?P<name>[a-z_][a-z0-9_]*)(?:\.as_str\(\))?\.as_bytes\(\)", line
            )
            if bytes_local:
                identifiers.append(bytes_local.group("name"))
            elif direct_call:
                identifiers.append(direct_call.group("name"))
            else:
                self.fail(f"unrecognized derive_receipt_id argument: {line!r}")
        self.assertEqual(
            identifiers,
            self.policy["receipt"]["field_order"],
            "the document's declared field order drifted from the Rust "
            "derivation order — either a field is missing from the document "
            "or the document declares a field the derivation no longer frames",
        )

    def test_withheld_field_order_matches_the_rust_derivation_order(self) -> None:
        # Withheld identities are durable replay-cursor audit evidence. Their
        # framing needs the same exhaustive drift gate as delivered receipts.
        match = re.search(
            r"pub fn derive_withheld_receipt_id\(.*?framed_digest\(\s*WITHHELD_DIGEST_DOMAIN,\s*&\[(.*?)\],\s*\);",
            self.api_rs,
            re.S,
        )
        self.assertIsNotNone(
            match, "derive_withheld_receipt_id no longer frames a digest array"
        )
        args_block = match.group(1)
        identifiers: list[str] = []
        for raw_line in args_block.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            bytes_local = re.fullmatch(r"&(?P<name>[a-z_][a-z0-9_]*)_bytes", line)
            direct_call = re.fullmatch(
                r"(?P<name>[a-z_][a-z0-9_]*)(?:\.as_str\(\))?\.as_bytes\(\)", line
            )
            if bytes_local:
                identifiers.append(bytes_local.group("name"))
            elif direct_call:
                identifiers.append(direct_call.group("name"))
            else:
                self.fail(
                    f"unrecognized derive_withheld_receipt_id argument: {line!r}"
                )
        self.assertEqual(
            identifiers,
            self.policy["receipt"]["withheld_field_order"],
            "the withheld identity's declared field order drifted from the "
            "Rust derivation order",
        )

    # -- cross-contract grounding ---------------------------------------------

    def test_admission_boundary_binds_the_ordering_both_lanes_depend_on(self) -> None:
        boundary = self.policy["admission_boundary"]
        self.assertEqual(boundary["runs_at"], "admission")
        self.assertEqual(
            boundary["runs_before"], "digest_and_idempotency_key_derivation"
        )
        self.assertIs(boundary["single_admitted_pipeline"], True)
        self.assertIs(boundary["mutates_canonical_evidence"], False)
        self.assertIs(boundary["raw_secret_material_allowed"], False)
        self.assertIs(boundary["delivered_bytes_equal_journal_bytes"], True)
        self.assertIs(boundary["withheld_events_advance_replay_cursor"], True)

    def test_referenced_contracts_exist_on_disk(self) -> None:
        boundary = self.policy["admission_boundary"]
        for key in ("observation_contract", "event_policy"):
            resolved = REPO / boundary[key]
            self.assertTrue(
                resolved.exists(),
                f"admission_boundary.{key} references {boundary[key]!r}, "
                "which does not exist relative to the repository root",
            )
        self.assertEqual(
            REPO / boundary["observation_contract"], OBSERVATION_CONTRACT
        )
        self.assertEqual(REPO / boundary["event_policy"], EVENT_POLICY)

    def test_secret_exclusion_agrees_with_the_host_event_policy(self) -> None:
        event_policy = json.loads(EVENT_POLICY.read_text(encoding="utf-8"))
        self.assertIs(
            event_policy["policy_boundary"]["uncommitted_secrets_never_admitted"], True
        )
        self.assertIs(
            event_policy["policy_boundary"]["transient_and_noise_default_excluded"], True
        )
        self.assertEqual(
            event_policy["dimensions"]["sensitivity"]["secret_value"], "secret"
        )

    def test_detector_authority_names_the_single_shared_corpus(self) -> None:
        authority = self.policy["detector_authority"]
        self.assertEqual(
            authority["credential_corpus"],
            "tracedecay_runtime_core::memory::hygiene::detect_secret_like",
        )
        self.assertEqual(
            authority["canonical_redaction"],
            "tracedecay_runtime_core::privacy::sanitize_memory_fact_payload",
        )
        crate_sources = self.lib_rs + self.credentials_rs
        self.assertIn("detect_secret_like", crate_sources)
        self.assertIn("sanitize_memory_fact_payload", crate_sources)
        # The multi-signal pass reaches for the shared detector rather than a
        # second catalogue: no rule table may be declared in this crate beyond
        # the two direct signals the document names.
        self.assertIn(
            "use tracedecay_runtime_core::memory::hygiene::detect_secret_like",
            self.credentials_rs,
        )

    def test_every_class_identity_is_implemented_in_the_rust_table(self) -> None:
        for row in self.policy["classes"]:
            self.assertIn(
                f'=> "{row["class_id"]}"',
                self.policy_rs,
                f'{row["class_id"]} has no wire spelling in the Rust table',
            )

    def test_payload_ceiling_is_bounded_and_fails_closed(self) -> None:
        limits = self.policy["payload_limits"]
        self.assertGreaterEqual(limits["max_canonical_bytes"], 1024)
        self.assertEqual(limits["over_limit_outcome"], "error")

    def test_invariants_are_present_and_distinct(self) -> None:
        invariants = self.policy["invariants"]
        self.assertGreaterEqual(len(invariants), 8)
        self.assertEqual(len(invariants), len(set(invariants)))
        joined = " ".join(invariants).lower()
        for phrase in (
            "before any digest",
            "never mutates",
            "replay cursor",
            "byte-identical",
        ):
            self.assertIn(phrase, joined, f"invariants no longer state {phrase!r}")


if __name__ == "__main__":
    unittest.main()
