"""Development-only JSON evidence regressions; no frozen corpus or real host."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

HERE = Path(__file__).resolve().parent
LIVE = next(p for p in HERE.parents if (p / 'scripts/product/memory-comparison/event_log.py').is_file()) / 'scripts/product/memory-comparison'
# Draft imports use its modified files and the unchanged live utility modules.
sys.path.insert(0, str(LIVE))
sys.path.insert(0, str(HERE))
from delivery_evidence import LexicalJson, PRESENTATION_REVIEW, TOKENIZER, sha, validate_tool_result
import adjudicate
import runner


def artifact(text, name):
    return {"artifact_path": f"development-only/{name}", "utf8": text,
            "byte_length": len(text.encode()), "sha256": sha(text)}


def fixture(bound=True, notices=True):
    trace_ref = 'recall-trace-v1:' + 'b' * 64 + ':' + 'a' * 64
    control = {"trace_ref": trace_ref, "item_ref": "recall-item-v1:0"}
    source = {"source_id": "source", "revision": "r1", "lineage_id": "lineage",
              "origin": {"kind": "development"}, "content_sha256": sha("Use café mode.")}
    candidate = {"candidate_id": "host.alias.survivor", "content": "Use café mode.",
                 "provenance": "Canonical observer source", "explanation": "Use café mode. Metadata must also be reviewed.",
                 "provenance_evidence": {"sources": [source], "recall": control}}
    advisory = {"provider_id": "development.provider", "registration_revision": 1,
                "recall_trace": {"trace_ref": trace_ref, "request_id": "actual-host-request"},
                "canonical_history_replay": {"delivery_scope": {"project": "development", "session": "s"}},
                "lane_notice": "Provider words remain inside the charged presentation.", "candidates": [candidate]}
    payload = '{ "canonical" : {"note":"café\\u0020safe"},\n "advisory_provider_memory" : ' + json.dumps(advisory, ensure_ascii=False) + ' }'
    parsed = LexicalJson(payload)
    texts = ['warning'] + [payload, 'metrics'] if notices else [payload]
    content = [{"type": "text", "text": t} for t in texts]
    content.append({"type": "image", "data": "development-only", "mimeType": "image/png"})
    carrier = {"content": content, "isError": False}
    index = 1 if notices else 0
    # Python checks structure/arithmetic only. These synthetic observations are
    # replaced by the pinned Rust tokenizer in the Rust verifier regressions.
    fake_count = lambda text: len(text.encode()) // 16 + bool(text)
    blocks = [{"content_index": i, "text": t, "sha256": sha(t), "utf8_bytes": len(t.encode()), "tokens": fake_count(t)} for i, t in enumerate(texts)]
    canonical = {"content_index": index, **parsed.span('/canonical', member=True), "source_refs": ["canonical-source"],
                 "authority": "canonical", "section": "canonical_facts"}
    canonical['text'] = parsed.verify(canonical, member=True)
    spans = [{"content_index": index, "attribution": "observed_advisory_member", **parsed.span('/advisory_provider_memory', member=True)}]
    for b in blocks:
        if b['content_index'] != index:
            spans.append({"content_index": b['content_index'], "attribution": "unclassified_text", "pointer": None, "start": 0, "end": b['utf8_bytes'], "sha256": b['sha256'], "value": b['text']})
    spans.sort(key=lambda s: (s['content_index'], s['start']))
    advisory_blocks = []
    for b in blocks:
        raw = b['text'].encode()
        t = b''.join(raw[s['start']:s['end']] for s in spans if s['content_index'] == b['content_index']).decode()
        advisory_blocks.append({"content_index": b['content_index'], "text": t, "sha256": sha(t), "tokens": fake_count(t)})
    review = b''.join(len(b['text'].encode()).to_bytes(8, 'big') + b['text'].encode() for b in advisory_blocks)
    trace = {"trace_id": 'a' * 64, "request_id": "actual-host-request", "provider_id": "development.provider",
             "registration_revision": 1, "requested_count": 1, "degraded": False,
             "items": [{"candidate_id": "original-provider-id", "provider_rank": 0, "stage": "injected",
                        "host_reason_code": "injected", "host_decision": {"decision": "injected"}, "tokens": 5}], "token_summary": None}
    presentation = parsed.span('/advisory_provider_memory/candidates/0')
    row = {"candidate_ref": "original-provider-id", "content": candidate['content'], "content_sha256": sha(candidate['content']),
           "sources": [source], "scope_match": True, "provenance": "available", "presentation_span": presentation,
           "presentation_sha256": presentation['sha256'], "final_join": {
               "status": "bound", **control, "provider_rank": 0, "retained_candidate_id": "original-provider-id", "compiled_stage": "injected"}
           if bound else {"status": "unresolved", "reason": "development missing retained binding"}}
    syntax, cursor = [], 0
    top = [parsed.members["/" + k] for k in parsed.value]
    for start, end in [*top, (len(parsed.data), len(parsed.data))]:
        if start > cursor:
            text = parsed.data[cursor:start].decode()
            syntax.append({"content_index": index, "start": cursor, "end": start, "text": text, "sha256": sha(text)})
        cursor = end
    return {"representation": "tool_result_v1", "tokenizer": TOKENIZER, "payload_syntax_spans": syntax,
            "raw_carrier": artifact(json.dumps(carrier, ensure_ascii=True, indent=1) + '\n', 'stdout'),
            "projection": {"result": carrier, "text_blocks": [{"content_index": b['content_index'], "text": b['text']} for b in blocks],
                           "payload_content_index": index, "host_evidence": [{"kind": "json_member", "pointer": '/canonical', "value": parsed.value['canonical'],
                                                                              "authority": 'canonical', "section": 'canonical_facts'}],
                           "advisory": {"kind": "json_member", "pointer": '/advisory_provider_memory', "value": advisory}},
            "text_blocks": blocks, "canonical_spans": [canonical], "canonical_sha256": sha(canonical['text']),
            "advisory_spans": spans, "advisory_blocks": advisory_blocks, "advisory_review_sha256": hashlib.sha256(review).hexdigest(),
            "final_tokens": sum(b['tokens'] for b in blocks), "canonical_tokens": fake_count(canonical['text']),
            "advisory_tokens": sum(b['tokens'] for b in advisory_blocks), "candidate_body_tokens": fake_count(candidate['content']),
            "candidates": [row], "retained_trace": {"status": "observed", "artifact": artifact(json.dumps(trace), 'trace'),
                "correlation": advisory['recall_trace'], "provider_id": advisory['provider_id'], "registration_revision": 1,
                "delivery_scope": advisory['canonical_history_replay']['delivery_scope'], "store_evidence": {"id": "observed"},
                "daemon_identity": {"pid": 101, "process_run_id": "development"}},
            "model_input_tokens": {"status": "unmeasured", "reason": "No complete model API boundary observed."}}


def annotation(delivery, label='useful'):
    return {"label": label, "reason": "development source-grounded review", "reviewer_id": "development", "provider_blinded": True,
            "presentation_sha256": delivery['candidates'][0]['presentation_sha256'], "advisory_review_sha256": delivery['advisory_review_sha256'],
            "fact_evidence": [{"fact_index": 0, "source_id": "source", "source_quote": "Use café mode.", "delivered_quote": "Use café mode."}],
            "prohibited_claims_absent": True, "presentation_review": PRESENTATION_REVIEW}


def truth():
    return ({"id": "q", "required_facts": ["Use café mode."], "expected_useful_source_ids": ["source"]},
            {"sources": [{"id": "source", "revision": "r1", "lineage_id": "lineage", "origin": {"kind": "development"}, "content": "Use café mode."}]})


class OriginalJsonTests(unittest.TestCase):
    def test_unicode_escapes_key_escaping_nested_arrays_and_original_member_bytes(self):
        raw = '{ "é/~" : ["café", "\\uD83D\\uDE00", {"a": 1e2}], "last" : true }'
        parser = LexicalJson(raw)
        span = parser.span('/é~1~0/1')
        self.assertEqual('"\\uD83D\\uDE00"', parser.verify(span))
        self.assertEqual('😀', span['value'])
        member = parser.span('/last', member=True)
        self.assertEqual('"last" : true', parser.verify(member, member=True))
        self.assertEqual(raw.encode().index(b'"last"'), member['start'])

    def test_duplicate_decoded_keys_nonfinite_bad_unicode_and_trailing_values_rejected(self):
        for raw in ('{"a":1,"\\u0061":2}', '{"a":NaN}', '[Infinity]', '1e9999', '"\\ud800"', '"x\ny"', '01', 'true false', '{"a":1,}', '[1,]'):
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeError)):
                LexicalJson(raw)

    def test_exact_integer_domain_boundaries_and_bare_negative_zero(self):
        raw = '[-9223372036854775808,9223372036854775807,9223372036854775808,18446744073709551615,-0,0]'
        parser = LexicalJson(raw)
        self.assertEqual([-(2**63), 2**63-1, 2**63, 2**64-1, 0, 0], parser.value)
        self.assertTrue(all(type(value) is int for value in parser.value))
        span = parser.span('/4')
        self.assertEqual('-0', parser.verify(span))
        self.assertEqual(sha('-0'), span['sha256'])
        span['value'] = -0.0
        with self.assertRaisesRegex(ValueError, 'span'): parser.verify(span)
        maximum = parser.span('/3'); maximum['value'] -= 1
        with self.assertRaisesRegex(ValueError, 'span'): parser.verify(maximum)

    def test_overflow_integer_neighbors_are_rejected_before_semantic_projection(self):
        for integer in ('-9223372036854775809', '18446744073709551616', '18446744073709551617'):
            with self.subTest(integer=integer), self.assertRaisesRegex(ValueError, 'shared exact domain'):
                LexicalJson('{"number":' + integer + '}')

    def test_fraction_exponent_domain_is_finite_binary64_with_original_lexemes(self):
        parser = LexicalJson('[1.0,1e0,-0.0,-0e0,18446744073709551616.0,1.7976931348623157e308,5e-324,1e-9999]')
        self.assertTrue(all(type(value) is float for value in parser.value))
        self.assertEqual(float(2**64), parser.value[4])
        self.assertEqual('1e0', parser.verify(parser.span('/1')))
        self.assertEqual('-0e0', parser.verify(parser.span('/3')))
        self.assertEqual(5e-324, parser.value[6])
        self.assertEqual(0.0, parser.value[7])
        for number in ('1e309','-1e309'):
            with self.assertRaisesRegex(ValueError, 'nonfinite'): LexicalJson(number)

    def test_repeated_identical_values_do_not_authorize_a_different_pointer_or_offset(self):
        parser = LexicalJson('{"a":"same","b":"same"}')
        wrong = parser.span('/b')
        wrong['pointer'] = '/a'
        with self.assertRaisesRegex(ValueError, 'span'):
            parser.verify(wrong)

    def test_byte_offsets_are_not_character_offsets_and_bool_is_not_integer(self):
        parser = LexicalJson('{"é":"é", "b":2}')
        span = parser.span('/b')
        span['start'] -= 2
        with self.assertRaises(ValueError): parser.verify(span)
        span = parser.span('/b'); span['end'] = True
        with self.assertRaises(ValueError): parser.verify(span)


class DeliveryTests(unittest.TestCase):
    def test_complete_wrapper_metadata_and_unknown_nontext_blocks_are_retained(self):
        d = fixture()
        self.assertEqual(1, len(runner.validate_delivery(d, 'provider:development.provider')))
        self.assertEqual('image', d['projection']['result']['content'][-1]['type'])
        self.assertNotEqual(d['candidates'][0]['candidate_ref'], d['candidates'][0]['presentation_span']['value']['candidate_id'])

    def test_carrier_or_semantic_projection_substitution_fails(self):
        for field in ('carrier', 'projection', 'span'):
            d = fixture()
            if field == 'carrier': d['raw_carrier']['utf8'] += ' '
            elif field == 'projection': d['projection']['result']['isError'] = True
            else: d['canonical_spans'][0]['value'] = {'note': 'different'}
            with self.subTest(field=field), self.assertRaises(ValueError): validate_tool_result(d, 'provider:development.provider')

    def test_omitted_reordered_and_changed_warning_blocks_fail(self):
        for change in ('omit', 'reorder', 'rewrite'):
            d = fixture()
            if change == 'omit': d['text_blocks'].pop()
            elif change == 'reorder': d['text_blocks'].reverse()
            else: d['text_blocks'][0]['text'] = 'rewritten'
            with self.subTest(change=change), self.assertRaises(ValueError): validate_tool_result(d, 'provider:development.provider')

    def test_partial_advisory_subtree_and_unreviewed_explanation_span_fail(self):
        d = fixture()
        d['advisory_spans'] = d['advisory_spans'][:1]
        with self.assertRaisesRegex(ValueError, 'coverage'): validate_tool_result(d, 'provider:development.provider')
        d = fixture()
        parser = LexicalJson(d['text_blocks'][1]['text'])
        d['candidates'][0]['presentation_span'] = parser.span('/advisory_provider_memory/candidates/0/content')
        with self.assertRaisesRegex(ValueError, 'presentation'): validate_tool_result(d, 'provider:development.provider')

    def test_canonical_original_bytes_and_source_projection_must_agree(self):
        for field in ('text', 'authority', 'source_refs'):
            d = fixture(); d['canonical_spans'][0][field] = None
            with self.subTest(field=field), self.assertRaises((ValueError, TypeError)):
                validate_tool_result(d, 'provider:development.provider')

    def test_unknown_final_join_stays_unresolved_and_compiled_stage_is_not_delivery(self):
        d = fixture(bound=False)
        validate_tool_result(d, 'provider:development.provider')
        d['candidates'][0]['final_join'] = {'status': 'unresolved'}
        with self.assertRaisesRegex(ValueError, 'reason'): validate_tool_result(d, 'provider:development.provider')
        d = fixture(); d['candidates'][0]['final_join']['compiled_stage'] = 'selected'
        with self.assertRaisesRegex(ValueError, 'join mismatch'): validate_tool_result(d, 'provider:development.provider')

    def test_trace_rank_provider_and_scope_substitution_fail(self):
        for mutate in (lambda d: d['candidates'][0]['final_join'].update(provider_rank=1),
                       lambda d: d['retained_trace'].update(provider_id='other'),
                       lambda d: d['retained_trace'].update(delivery_scope={'project': 'other'}),
                       lambda d: d['retained_trace']['artifact'].update(sha256='wrong')):
            d = fixture(); mutate(d)
            with self.assertRaises(ValueError): validate_tool_result(d, 'provider:development.provider')

    def test_full_block_and_advisory_budgets_have_fixed_ceilings(self):
        for token_field, block_field, limit in (('final_tokens','text_blocks',128000), ('advisory_tokens','advisory_blocks',1024)):
            d = fixture(); delta = limit + 1 - d[token_field]
            d[token_field] += delta; d[block_field][0]['tokens'] += delta
            with self.subTest(field=token_field), self.assertRaisesRegex(ValueError, 'quota'):
                validate_tool_result(d, 'provider:development.provider')

    def test_no_memory_and_docs_cannot_claim_provider_candidate_delivery(self):
        for lane in ('no_memory', 'explicit_documentation'):
            with self.subTest(lane=lane), self.assertRaises(ValueError): validate_tool_result(fixture(), lane)

    def test_unrecognized_tag_and_mixed_legacy_fields_fail(self):
        d = fixture(); d['representation'] = 'tool_result_v2'
        with self.assertRaisesRegex(ValueError, 'unknown'): runner.validate_delivery(d, 'provider:development.provider')
        d = fixture(); d['final_text'] = ''
        with self.assertRaisesRegex(ValueError, 'mixed'): runner.validate_delivery(d, 'provider:development.provider')

    def test_candidate_body_label_must_bind_full_presentation_and_all_notices(self):
        d = fixture(); q, c = truth()
        self.assertEqual('useful', adjudicate.evidence_for(d['candidates'][0], q, c, annotation(d), d)[0])
        for field in ('presentation_sha256', 'advisory_review_sha256'):
            a = annotation(d); a[field] = d['candidates'][0]['content_sha256']
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'full candidate presentation'):
                adjudicate.evidence_for(d['candidates'][0], q, c, a, d)

    def test_hash_match_without_explicit_presentation_assessment_does_not_admit(self):
        d = fixture(); q, c = truth(); a = annotation(d); a.pop('presentation_review')
        with self.assertRaisesRegex(ValueError, 'did not assess'):
            adjudicate.evidence_for(d['candidates'][0], q, c, a, d)

    def test_payload_delimiters_cannot_be_omitted_from_original_partition(self):
        d = fixture(); d['payload_syntax_spans'].pop()
        with self.assertRaisesRegex(ValueError, 'delimiter partition'):
            validate_tool_result(d, 'provider:development.provider')

    def test_unresolved_join_cannot_gain_useful_label_from_body_match(self):
        d = fixture(bound=False); q,c = truth()
        with self.assertRaisesRegex(ValueError, 'unresolved'):
            adjudicate.evidence_for(d['candidates'][0], q, c, annotation(d), d)
        self.assertEqual('indeterminate', adjudicate.evidence_for(d['candidates'][0], q, c, annotation(d, 'indeterminate'), d)[0])

    def test_legacy_substantive_framing_is_still_rejected(self):
        text = 'body'
        d = {'final_text': text + ' warning', 'final_sha256': sha(text + ' warning'), 'tokenizer': TOKENIZER,
             'final_tokens': 2, 'canonical_tokens': 0, 'advisory_tokens': 1, 'candidate_body_tokens': 1,
             'sections': [{'kind':'advisory','candidate_ref':'c','text':text},{'kind':'framing','text':' warning'}],
             'candidates': [{'candidate_ref':'c','content':text,'content_sha256':sha(text),'sources':[{'source_id':'s'}],
                             **{s: True for s in ('returned','admitted','selected','packed','delivered')}}]}
        with self.assertRaisesRegex(ValueError, 'substantive framing'): runner.validate_delivery(d, 'provider:development.provider')


def report_fixture(delivery, expected_positive=True):
    query, case = truth()
    query.update(forbidden_source_ids=[], expected_terminal="success")
    if not expected_positive:
        query.update(required_facts=[], expected_useful_source_ids=[])
    case.update(id="development", queries=[query], steps=[{"action":"recall", "query_id":"q"}])
    schedule, rows = [], []
    for host in runner.HOSTS:
        for lane in runner.LANES:
            for trial in range(3):
                key = f"{host}/{trial}/development/{lane}"
                scheduled = {"case_trial_id":key,"host":host,"lane":lane,"trial":trial,"case_id":"development"}
                schedule.append(scheduled)
                first = host == "claude" and lane == runner.LANES[0] and trial == 0
                rows.append({**scheduled,"request_id":"q","action_id":"development/0","action":"recall",
                             "status":"completed" if first else "unexecuted","terminal":"success" if first else None,
                             "delivery":delivery if first else None,"phase":"host_request","timing":{"status":"unmeasured","reason":"no host"},
                             "comparison_valid":True})
    return {"format":"tracedecay.host-comparison.capture.v1", "metadata":{}, "raw_case_captures":[],
            "plan":{"cases":[case],"schedule":schedule,"freeze":{}},"measurements":rows}


class ExporterTests(unittest.TestCase):
    def test_export_keeps_all_raw_bytes_and_separate_final_binding(self):
        d = fixture(); report = report_fixture(d); a = annotation(d)
        a.update(case_trial_id=report['plan']['schedule'][0]['case_trial_id'], request_id='q', candidate_ref='original-provider-id')
        bundles = adjudicate.metric_inputs(report, [a])
        self.assertEqual(24, len(bundles))
        metric = bundles[0]['input']['recalls'][0]['delivery']
        self.assertEqual('tool_result_v1', metric['representation'])
        self.assertNotIn('final_text', metric)
        self.assertEqual([], metric['candidates'])
        self.assertEqual(d['raw_carrier'], metric['tool_result']['raw_carrier'])
        self.assertEqual(d['retained_trace'], metric['tool_result']['retained_trace'])
        self.assertEqual(a['presentation_sha256'], metric['tool_result']['candidates'][0]['annotation_presentation_sha256'])
        self.assertEqual('pass', bundles[0]['query_assessments'][0]['outcome'])
        self.assertEqual(d['final_tokens'], bundles[0]['input']['record']['scenarios'][0]['context_tokens']['value'])

    def test_unjoined_scope_is_unresolved_and_cannot_pass_empty_expected_query(self):
        d = fixture(bound=False); d['candidates'][0]['scope_match'] = False
        report = report_fixture(d, expected_positive=False)
        bundles = adjudicate.metric_inputs(report, [])
        self.assertEqual('indeterminate', bundles[0]['query_assessments'][0]['outcome'])
        candidate = bundles[0]['input']['record']['scenarios'][0]['candidates'][0]
        self.assertEqual('missing', candidate['label'])
        self.assertFalse(candidate['scope_match'])

    def test_zero_candidates_do_not_silently_exempt_unreviewed_lane_notices(self):
        d = fixture(); d['candidates'] = []
        # Structural validation would reject this changed coverage; this unit
        # isolates the exporter's treatment of a zero-candidate notice surface.
        report = report_fixture(d, expected_positive=False)
        self.assertEqual('indeterminate', adjudicate.metric_inputs(report, [])[0]['query_assessments'][0]['outcome'])

    def test_run_identity_binds_original_presentation_and_annotation(self):
        d = fixture(); report = report_fixture(d); a = annotation(d)
        a.update(case_trial_id=report['plan']['schedule'][0]['case_trial_id'],request_id='q',candidate_ref='original-provider-id')
        first = adjudicate.metric_inputs(report,[a])[0]['input']['record']['provider']['run_identity_sha256']
        a['reason'] += ' second review'
        second = adjudicate.metric_inputs(report,[a])[0]['input']['record']['provider']['run_identity_sha256']
        self.assertNotEqual(first,second)


if __name__ == '__main__':
    unittest.main()
