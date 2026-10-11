import {
  APPLICATION_HTTP_BINDINGS,
  type ApplicationHttpOperationId,
} from '../contracts/application.ts';
import type {
  ApplicationProblemKind,
  ApplicationProblemRecord,
  ResolvedScope,
} from '../contracts/generated.ts';

export const FIXTURE_SCOPE: ResolvedScope = {
  project_id: 'project.work',
  repository_id: 'repository.work',
  worktree_id: 'worktree.work',
  reference: null,
  scope_digest: 'sha256:scope',
};

const EXECUTION = {
  started_at: 1,
  ended_at: 2,
  effective_deadline: null,
  cancellation: null,
  budget: { units_consumed: 0, bytes_consumed: 0, elapsed_micros: 0 },
  termination: 'completed',
};

export function applicationSuccess(
  operation: ApplicationHttpOperationId,
  payload: unknown,
  options: { scope?: unknown; requestId?: string; bindingId?: string } = {},
): { kind: 'success'; value: Record<string, unknown> } {
  const binding = APPLICATION_HTTP_BINDINGS[operation];
  const outcome =
    binding.receipt === 'durable_effect'
      ? {
          outcome: 'effect',
          value: {
            effect_id: 'effect.fixture',
            effect_class: 'administrative',
            idempotency_key: 'idempotency.fixture',
            authority: {},
            expected_state: 'sha256:expected',
            execution: EXECUTION,
            reconciliation: 'reconciled',
            receipt: {},
            payload,
          },
        }
      : {
          outcome: 'evidence',
          value: {
            temporal: {},
            authority: {},
            evidence_authorities: [],
            coverage: {},
            omissions: [],
            scores: [],
            contributions: [],
            page: {
              sort_contract_id: 'sort.fixture',
              sort_revision: 1,
              total: null,
              returned: 0,
              cursor: null,
              expires_at: null,
            },
            execution: EXECUTION,
            payload,
          },
        };
  return {
    kind: 'success',
    value: {
      binding_id: options.bindingId ?? binding.bindingIds[0],
      contract: {
        schema_id: binding.resultSchema.schemaId,
        schema_revision: binding.resultSchema.revision,
      },
      request_id: options.requestId ?? 'request.fixture',
      scope: options.scope ?? FIXTURE_SCOPE,
      outcome,
    },
  };
}

export function applicationProblem(
  kind: ApplicationProblemKind,
  options: {
    binding?: ApplicationHttpOperationId;
    problem?: Partial<ApplicationProblemRecord>;
  } = {},
): { kind: 'problem'; value: Record<string, unknown> } {
  const revalidate = kind === 'conflict' || kind === 'stale';
  const problem: ApplicationProblemRecord = {
    revision: 1,
    kind,
    code: `fixture.${kind}`,
    message: `fixture ${kind} problem`,
    diagnostic: null,
    detail: null,
    committed_receipt: null,
    owning_layer: 'application',
    terminality: 'pre_admission',
    retryable: revalidate,
    retry: revalidate ? 'after_revalidate' : 'never',
    retry_scope: revalidate ? 'fresh_request' : null,
    retry_after_millis: null,
    cancellation_stage: null,
    unavailable_classification: null,
    execution_failure_classification: null,
    request_id: 'request.fixture',
    trace_id: 'trace.fixture',
    details: [],
    legal_actions: revalidate ? ['refresh'] : [],
    coverage: null,
    ...options.problem,
  };
  return {
    kind: 'problem',
    value: {
      ...(options.binding === undefined
        ? {}
        : { binding_id: APPLICATION_HTTP_BINDINGS[options.binding].bindingIds[0] }),
      contract: { schema_id: 'schema.application.problem', schema_revision: 1 },
      request_id: problem.request_id,
      problem,
    },
  };
}
