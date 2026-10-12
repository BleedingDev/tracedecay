import type {
  OperationApplicationProviderFeedbackResult,
  ProviderControlScopeV1,
  RequestFor,
} from "./operations";
import type { EffectTermination, HttpSuccessEnvelope, OperationTermination } from "./types";

type ProviderResult = OperationApplicationProviderFeedbackResult;
type ProviderOperation = ProviderResult["result"]["operation"];
type ProviderRequest = RequestFor<`application_provider_${ProviderOperation}`>;

const UTF8 = new TextEncoder();

function reference(value: string, maximumBytes = 1_024): boolean {
  return value.length > 0 && value.trim() === value &&
    UTF8.encode(value).byteLength <= maximumBytes && !/\p{Cc}/u.test(value);
}

function digest(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

async function framedSha256(domainText: string, values: readonly string[]): Promise<string> {
  const domain = UTF8.encode(domainText);
  const fields = values.map((value) => UTF8.encode(value));
  const encoded = new Uint8Array(
    domain.byteLength + fields.reduce((length, field) => length + 8 + field.byteLength, 0),
  );
  encoded.set(domain);
  const view = new DataView(encoded.buffer);
  let offset = domain.byteLength;
  for (const field of fields) {
    view.setBigUint64(offset, BigInt(field.byteLength));
    offset += 8;
    encoded.set(field, offset);
    offset += field.byteLength;
  }
  const hash = await crypto.subtle.digest("SHA-256", encoded);
  return Array.from(new Uint8Array(hash), (byte) =>
    byte.toString(16).padStart(2, "0")).join("");
}

function scopeDigest(scope: ProviderControlScopeV1): Promise<string> {
  return framedSha256("tracedecay.memory-provider.exact-scope.v1\0", [
    scope.profile_id,
    scope.project_id,
    scope.repository_identity,
    scope.worktree_identity,
    scope.branch_identity,
    scope.agent_session_id,
    scope.resolved_scope_digest,
  ]);
}

async function agentSessionId(
  scope: ProviderControlScopeV1,
  canonicalSessionId: string,
): Promise<string> {
  const digest = await framedSha256(
    "tracedecay.memory-provider.agent-session-binding.v1\0",
    [scope.profile_id, scope.project_id, scope.repository_identity,
     scope.worktree_identity, scope.branch_identity, scope.resolved_scope_digest,
     canonicalSessionId],
  );
  return `tdmem-agent-session.v1.${digest}`;
}

function sameValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (typeof left !== "object" || left == null ||
      typeof right !== "object" || right == null) return false;
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  return leftEntries.length === rightEntries.length && leftEntries.every(
    ([key, value]) => Object.hasOwn(right, key) &&
      sameValue(value, (right as Record<string, unknown>)[key]),
  );
}

function effectMatches(effect: ProviderResult["effect"]): boolean {
  const items = [...effect.committed_item_refs, ...effect.uncommitted_item_refs];
  if (items.length > 4_096 || new Set(items).size !== items.length ||
      !items.every((item) => reference(item, 256))) return false;
  if ([effect.provider_receipt_digest, effect.verification_digest,
       effect.duplicate_of_idempotency_key].some(
    (value) => value != null && !digest(value),
  )) return false;
  if ((effect.committed_boundary != null && !reference(effect.committed_boundary, 256)) ||
      (effect.reconciliation_action != null && !reference(effect.reconciliation_action, 512)) ||
      (effect.duplicate_of_operation_id != null && !reference(effect.duplicate_of_operation_id, 256))) return false;
  if (effect.state !== "duplicate" &&
      (effect.duplicate_of_idempotency_key != null || effect.duplicate_of_operation_id != null)) return false;
  const sameGeneration = (effect.state_generation_before ?? null) === (effect.state_generation_after ?? null);
  const knownGenerations = effect.state_generation_before != null &&
    effect.state_generation_after != null &&
    effect.state_generation_after >= effect.state_generation_before;
  const noItems = items.length === 0;
  switch (effect.state) {
    case "none":
      return sameGeneration && effect.committed_boundary == null && noItems &&
        effect.provider_receipt_digest == null && effect.reconciliation_action == null &&
        effect.verification_digest == null;
    case "committed":
      return knownGenerations && effect.committed_boundary == null &&
        effect.uncommitted_item_refs.length === 0 && effect.provider_receipt_digest != null &&
        effect.reconciliation_action == null && effect.verification_digest != null;
    case "duplicate":
      return knownGenerations && sameGeneration && effect.committed_boundary == null &&
        noItems && effect.provider_receipt_digest != null && effect.reconciliation_action == null &&
        effect.verification_digest == null && effect.duplicate_of_idempotency_key != null &&
        effect.duplicate_of_operation_id != null;
    case "partial":
      return knownGenerations && effect.committed_boundary != null &&
        effect.committed_item_refs.length > 0 && effect.uncommitted_item_refs.length > 0 &&
        effect.provider_receipt_digest != null && effect.reconciliation_action != null &&
        effect.verification_digest != null;
    case "unknown":
      return effect.state_generation_before == null && effect.state_generation_after == null &&
        effect.committed_boundary == null && noItems && effect.reconciliation_action != null &&
        effect.verification_digest == null;
  }
}

function terminalAllowsEffect(result: ProviderResult): boolean {
  const effect = result.effect.state;
  switch (result.terminal) {
    case "success":
    case "partial":
      return effect === "none" || effect === "committed" || effect === "duplicate";
    case "deadline_exceeded":
    case "cancelled":
    case "contract_violation":
    case "internal_failure":
      return effect === "none" || effect === "partial" || effect === "unknown";
    case "provider_unavailable":
      return effect === "none" || effect === "unknown";
    case "partial_effect":
      return effect === "partial";
    case "effect_unknown":
      return effect === "unknown";
    default:
      return effect === "none";
  }
}

// The host's read_termination and effect_termination in provider_control/outcome.rs
// select the outer lifecycle; deletion's receipt proves the host fence separately.
function readTermination(terminal: ProviderResult["terminal"]): OperationTermination {
  switch (terminal) {
    case "success":
    case "success_zero_results": return "completed";
    case "partial":
    case "partial_effect": return "partial";
    case "cancelled": return "cancelled";
    case "deadline_exceeded": return "timed_out";
    case "provider_unavailable":
    case "scope_unavailable": return "unavailable";
    case "effect_unknown": return "effect_unknown";
    default: return "failed";
  }
}

function effectTermination(result: ProviderResult): EffectTermination | undefined {
  switch (result.effect.state) {
    case "unknown": return "effect_unknown";
    case "partial": return "partial";
    case "committed":
    case "duplicate": return result.terminal === "partial" ? "partial" : "completed";
    case "none": {
      if (result.terminal === "cancelled") return "cancelled";
      if (result.terminal === "deadline_exceeded") return "timed_out";
      if (result.terminal !== "success" && result.terminal !== "success_zero_results") {
        return "failed";
      }
      const output = result.result;
      if (output.data == null || !("receipt" in output.data) ||
          output.data.receipt.state_generation_before !== output.data.receipt.state_generation_after ||
          result.effect.committed_item_refs.length !== 0 || result.effect.committed_boundary != null) {
        return undefined;
      }
      switch (output.operation) {
        case "feedback": return output.data.signal === "ignored" ? "no_change" : undefined;
        case "correction": return output.data.affected_provider_effects === 0 ? "no_change" : undefined;
        case "maintenance":
          return (output.data.dry_run || (output.data.changed_items === 0 && output.data.removed_items === 0)) &&
            output.data.state_changed !== true && !output.data.partial ? "no_change" : undefined;
        case "snapshot_restore": return "no_change";
        case "replay":
          return output.data.applied_observations === 0 && output.data.rejected_observations === 0 &&
            output.data.effect_unknown_observations === 0 && !output.data.partial ? "no_change" : undefined;
        default: return undefined;
      }
    }
  }
}

function httpOutcomeMatches(
  operationId: string,
  result: ProviderResult,
  envelope: HttpSuccessEnvelope<unknown>,
): boolean {
  const outcome = envelope.outcome;
  const read = ["health", "inspection", "snapshot_export"].includes(result.result.operation);
  if (read) {
    return outcome.outcome === "evidence" &&
      outcome.value.execution.termination === readTermination(result.terminal);
  }
  if (outcome.outcome !== "effect") return false;
  const effect = outcome.value;
  const receipt = effect.receipt;
  const deletion = result.result.operation === "delete_by_source" ? result.result.data : null;
  // The retained service requires exact inner/outer host intent receipt equality.
  const termination = deletion == null ? effectTermination(result) : "completed";
  return termination !== undefined &&
    (deletion == null || sameValue(receipt, deletion.intent.host_receipt)) &&
    effect.effect_id === `effect.provider-control.${result.operation_id}` &&
    effect.effect_class === "administrative" && receipt.effect_class === effect.effect_class &&
    effect.idempotency_key === result.idempotency_key && receipt.idempotency_key === effect.idempotency_key &&
    receipt.operation === operationId.replace("operation.application.", "use-case.application.retained.").replaceAll("_", "-") &&
    receipt.request_id === envelope.request_id && sameValue(receipt.scope, envelope.scope) &&
    receipt.expected_state === effect.expected_state && receipt.outcome === termination &&
    effect.execution.termination === (termination === "no_change" ? "completed" : termination) &&
    effect.reconciliation === (["partial", "effect_unknown"].includes(termination) ? "pending" : "reconciled");
}

async function requestMatches(request: ProviderRequest, result: ProviderResult): Promise<boolean> {
  if ("state" in request && request.state.kind === "canonical_session" &&
      (request.state.provider_id !== result.provider_id ||
       request.state.registration_revision !== result.registration_revision ||
       await agentSessionId(result.scope, request.state.session_id) !==
         result.scope.agent_session_id)) return false;
  const output = result.result;
  if (output.data == null) return true;
  switch (output.operation) {
    case "feedback": {
      const input = request as RequestFor<"application_provider_feedback">;
      return sameValue(input.source, output.data.source) && input.signal === output.data.signal;
    }
    case "correction": {
      const input = request as RequestFor<"application_provider_correction">;
      return sameValue(input.source, output.data.source) &&
        input.correction.kind === output.data.correction_kind &&
        input.expected_source_revision === output.data.target.source.source_revision;
    }
    case "delete_by_source": {
      const input = request as RequestFor<"application_provider_delete_by_source">;
      return sameValue(input.source, output.data.source) && input.mode === output.data.mode &&
        input.include_snapshots === output.data.include_snapshots &&
        input.expected_fence_revision === output.data.intent.fence_revision_before;
    }
    case "inspection": {
      const input = request as RequestFor<"application_provider_inspection">;
      return sameValue(input.selection, output.data.selection) &&
        (output.data.evidence_origin.kind !== "host_snapshot_artifact" || input.cursor == null) &&
        output.data.items.items.length <= input.maximum_items &&
        UTF8.encode(JSON.stringify(output.data.items)).byteLength <= input.maximum_bytes;
    }
    case "maintenance": {
      const input = request as RequestFor<"application_provider_maintenance">;
      return input.task === output.data.task && input.dry_run === output.data.dry_run &&
        output.data.scanned_items <= input.maximum_items;
    }
    case "snapshot_export":
      return output.data.identity.byte_length <=
        (request as RequestFor<"application_provider_snapshot_export">).maximum_bytes;
    case "snapshot_restore": {
      const input = request as RequestFor<"application_provider_snapshot_restore">;
      return input.snapshot_ref === output.data.snapshot_ref &&
        input.expected_state_generation === output.data.receipt.state_generation_before;
    }
    case "replay": {
      const input = request as RequestFor<"application_provider_replay">;
      return input.first_source_sequence === output.data.first_source_sequence &&
        input.last_source_sequence === output.data.last_source_sequence &&
        input.expected_state_generation === output.data.receipt.state_generation_before &&
        output.data.acknowledged_sequence >= input.expected_previous_acknowledged_sequence;
    }
    case "health":
      return true;
  }
}

/** Request and effect checks from ProviderControlResultV1, after schema decoding. */
export async function providerControlTerminalMatches(
  operationId: string,
  request: unknown,
  payload: unknown,
  envelope?: HttpSuccessEnvelope<unknown>,
): Promise<boolean> {
  if (payload == null || typeof payload !== "object") return false;
  const result = payload as ProviderResult;
  if (operationId !== `operation.application.provider_${result.result.operation}` ||
      !reference(result.provider_id) || result.registration_revision < 1 ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(result.operation_id) ||
      !effectMatches(result.effect) || !terminalAllowsEffect(result)) return false;
  const scope = result.scope;
  if (![scope.profile_id, scope.project_id, scope.repository_identity,
        scope.worktree_identity, scope.branch_identity, scope.agent_session_id].every(
    (value) => reference(value) && value !== "*",
  ) || !/^sha256:[0-9a-f]{64}$/.test(scope.resolved_scope_digest)) return false;
  const mutation = !["health", "inspection", "snapshot_export"].includes(result.result.operation);
  if (mutation ? result.idempotency_key == null || !digest(result.idempotency_key) :
      result.idempotency_key != null || result.effect.state !== "none") return false;
  if (result.effect.state === "duplicate" &&
      result.effect.duplicate_of_idempotency_key !== result.idempotency_key) return false;
  if ((result.diagnostic_id != null && !reference(result.diagnostic_id, 128)) ||
      (result.domain_detail != null &&
       (!reference(result.domain_detail.code, 128) || !reference(result.domain_detail.message, 8_192))) ||
      !result.warnings.every((warning) => reference(warning, 8_192))) return false;
  const output = result.result;
  if (["success", "success_zero_results", "partial"].includes(result.terminal) &&
      output.data == null) return false;
  if (result.effect.state === "committed" || result.effect.state === "partial") {
    if (result.effect.state_generation_before == null ||
        result.effect.state_generation_after == null ||
        result.effect.state_generation_after <= result.effect.state_generation_before) return false;
  }
  if (output.data != null && "receipt" in output.data) {
    const receipt = output.data.receipt;
    if (receipt.state_generation_after < receipt.state_generation_before ||
        !digest(receipt.provider_receipt_digest) ||
        (result.effect.state === "none" &&
         receipt.state_generation_before !== receipt.state_generation_after) ||
        (result.effect.provider_receipt_digest != null &&
         result.effect.provider_receipt_digest !== receipt.provider_receipt_digest)) return false;
    if ((result.effect.state === "committed" || result.effect.state === "partial") &&
        (result.effect.state_generation_before !== receipt.state_generation_before ||
         result.effect.state_generation_after !== receipt.state_generation_after)) return false;
  }
  if (output.data != null && "target" in output.data) {
    const target = output.data.target;
    if (output.data.source.observation_id !== target.source.observation_id ||
        !reference(target.stable_memory_ref) ||
        ![target.source.canonical_provider_id, target.source.canonical_session_id,
          target.source.source_key, target.source.observation_id].every((value) => reference(value)) ||
        [target.source.stable_record_id, target.source.source_revision].some(
          (value) => value != null && !reference(value),
        ) || !digest(target.source.content_sha256)) return false;
    if ("target_digest" in output.data && !digest(output.data.target_digest)) return false;
  }
  if (output.operation === "health" && output.data != null) {
    if (!reference(output.data.provider_instance_id) ||
        ![output.data.implementation_identity_digest, output.data.state_identity_digest,
          output.data.effective_limits_digest].every(digest) ||
        output.data.scope_digest !== await scopeDigest(scope)) return false;
  }
  if (mutation && ["success", "success_zero_results"].includes(result.terminal) &&
      result.effect.state === "none") {
    if (output.operation === "delete_by_source") {
      if (output.data == null ||
          !["verified", "retained_under_lock"].includes(output.data.erasure.state)) return false;
    } else if (effectTermination(result) !== "no_change") return false;
  }
  return (envelope === undefined || httpOutcomeMatches(operationId, result, envelope)) &&
    await requestMatches(request as ProviderRequest, result) &&
    UTF8.encode(JSON.stringify(result)).byteLength <= 33_554_432;
}
