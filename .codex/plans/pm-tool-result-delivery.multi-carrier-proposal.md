# Follow-up proposal only: ordered actual carriers for one recall action

The frozen `tool_result_v1` draft has one raw CLI carrier and one selected JSON context payload. It cannot yet describe the documentation baseline's actual canonical-context result followed by explicit body-required document-read results. Do not represent those responses by inventing merged JSON.

Add a distinct tag, provisionally `tool_result_sequence_v1`, whose `carriers` array contains the actual results in command completion/delivery order. Each entry retains its command reference, zero-based ordinal, raw UTF-8 artifact, complete parsed result and ordered decoded text blocks. Preserve every command response, including errors, warnings, metrics and cache stubs. Duplicate or missing ordinals, reordered command references and omitted responses invalidate the evidence.

Reuse the original-span parser and independent block-token counter. Each carrier has a typed projection: the existing canonical-context projection, or a separately reviewed public documentation-read projection that binds the actual path/revision/body-present state and original body/member spans. The latter must follow the actual read-result schema and observed source mapping; it must not manufacture `advisory_provider_memory` JSON or a new provider lane. A cache stub or missing body cannot prove delivered documentation.

All full-block token counts sum across the entire action and remain at most 128000. All advisory/documentation presentation spans concatenate only within their original block, then their independent counts sum across carriers and remain at most 1024. Provider/documentation metadata and unclassified prose remain charged and explicitly reviewed. Exceeding a cumulative quota is a captured failure; it never authorizes deleting a response, silently rereading, or trying another document selection. Retain the original frozen selection and every actual response.

Candidate presentation references gain a carrier ordinal plus the original content index and JSON pointer/span. Bind annotations to those exact presentations and the ordered shared presentation across the action. Candidate identity, labels, source checks and current recall/admission denominators remain unchanged; an extra carrier is not an extra scheduled query or synthetic candidate.

The single action's successful-delivery span starts immediately before the first canonical-context request and ends when the final required public read response is fully captured. Preserve component spans as diagnostics; summing them must not substitute for the complete action elapsed span. Keep one logical action result/event and one scheduled recall/query row. Process cleanup and task/model-input status remain governed by the existing protocol.

This document proposes the additive shape only. No implementation, schema widening, test execution or live apply for multiple carriers is included in the frozen patch.
