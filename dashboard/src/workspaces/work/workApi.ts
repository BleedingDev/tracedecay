import type { WireSchema } from '../../data/query/wireSchema.ts';
import {
  ApplicationProblemEnvelopeSchema,
  ResolvedScopeSchema,
  type ApplicationProblemKind,
  type ApplicationProblemRecord,
  type ResolvedScope,
} from '../../contracts/generated.ts';
import {
  APPLICATION_HTTP_BINDINGS,
  decodeApplicationHttpSuccess,
  type ApplicationHttpOperationId,
} from '../../contracts/application.ts';
import { decodeJsonBody, UNDECODABLE } from '../../data/query/responseBody.ts';
import { readOnlyScopeRefusal } from '../../data/scope/store.ts';
import type { DomainStateKind } from '../../ui/StateChip.tsx';

/**
 * The dashboard's adapter onto the daemon's application routes.
 *
 * Work, Workflow, handoff and feedback routes answer with the application's
 * `HttpJsonEnvelope<T>` (`crates/tracedecay-api/src/lib.rs`), not the
 * dashboard's own `DashboardEnvelopeV1<T>`. Both halves of that envelope are
 * decoded by generated contracts: a success by the SDK generator's canonical
 * decoder (`contracts/application.ts`), checked against the operation's own
 * binding, result contract, outcome family and receipt; a problem by the
 * schemars `ApplicationProblemEnvelope`. What this module owns is only the
 * dashboard side of the wire: the scoped gateway URL, the gateway's own
 * refusals (auth, read-only scope), and the projection into `WorkResult`.
 */

/** A route the daemon actually mounts, with the contracts on either side of it.
 *
 * `operation` is the canonical operation id, which keys the binding contract
 * the response is checked against; `path` is the dashboard gateway path, which
 * the SDK's `/application/...` route cannot stand in for. */
export interface WorkRoute<Request, Response> {
  readonly operation: ApplicationHttpOperationId;
  readonly path: string;
  readonly request: WireSchema<Request>;
  readonly response: WireSchema<Response>;
}

/** What a call produced: the contract, or a reason there is no contract.
 *
 * There is no third case. A refusal always carries a domain state and a
 * sentence, so no caller can render an absence as an empty success. When the
 * daemon itself refused, `problem` is its canonical record: which of stale and
 * conflict it was, the retry directive, the legal actions, and any receipt for
 * an effect that committed before the failure. */
export type WorkResult<T> =
  | { readonly outcome: 'value'; readonly value: T; readonly scope?: ResolvedScope }
  | {
      readonly outcome: 'refused';
      readonly state: DomainStateKind;
      readonly detail: string;
      readonly problem?: ApplicationProblemRecord;
    };

const PROBLEM_STATE: Readonly<Record<ApplicationProblemKind, DomainStateKind>> = {
  invalid_request: 'error',
  // The daemon conflates "no such task" with "not yours" so that probing cannot
  // reveal whether a binding exists; `denied` keeps that conflation.
  not_found_or_not_authorized: 'denied',
  conflict: 'conflicting',
  stale: 'stale',
  partial_effect: 'partial',
  unsupported: 'unsupported',
  unavailable: 'unavailable',
  reset_required: 'unavailable',
  execution_failed: 'error',
  saturated: 'unavailable',
  cancelled: 'cancelled',
  timed_out: 'timed_out',
};

function refused(state: DomainStateKind, detail: string): WorkResult<never> {
  return { outcome: 'refused', state, detail };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Whether a command's answer means cached reads no longer describe the daemon.
 *
 * True when the command committed, when it failed after committing part of its
 * effect, and when the daemon's problem tells the caller to revalidate (a stale
 * or conflicting precondition). A plain refusal leaves every read true.
 */
export function workResultInvalidatesReads(result: WorkResult<unknown>): boolean {
  if (result.outcome === 'value') return true;
  const problem = result.problem;
  return (
    problem !== undefined &&
    (problem.committed_receipt !== null || problem.retry === 'after_revalidate')
  );
}

function decodeSuccess<Response>(
  route: WorkRoute<unknown, Response>,
  body: unknown,
): WorkResult<Response> {
  if (!isRecord(body) || body.kind !== 'success') {
    return refused('unsupported_schema', 'the response envelope is not the shape this build reads');
  }
  let payload: unknown;
  let envelopeScope: unknown;
  try {
    const envelope = decodeApplicationHttpSuccess(route.operation, body.value, (value) => value);
    payload = envelope.outcome.value.payload;
    envelopeScope = envelope.scope;
  } catch {
    return refused('unsupported_schema', `the response envelope does not answer ${route.operation}`);
  }
  if (payload === null) {
    return refused('unavailable', 'the daemon answered without a value');
  }
  const parsed = route.response.safeParse(payload);
  if (!parsed.success) {
    return refused('unsupported_schema', `the payload does not satisfy ${route.operation}`);
  }
  const scope = ResolvedScopeSchema.safeParse(envelopeScope);
  if (!scope.success) {
    return refused('unsupported_schema', 'the response envelope carries no valid resolved scope');
  }
  return { outcome: 'value', value: parsed.data, scope: scope.data };
}

/**
 * Decode a canonical problem, or `undefined` when the body is not one.
 *
 * A problem that names a binding must name this route's; a concealed denial
 * must name none, or its binding would answer the question the concealment
 * exists to refuse. A problem raised before binding admission (the gateway's
 * project resolution) legitimately names none.
 */
function decodeProblem(route: WorkRoute<unknown, unknown>, body: unknown): WorkResult<never> | undefined {
  if (!isRecord(body) || body.kind !== 'problem' || !isRecord(body.value)) return undefined;
  const { binding_id: bindingId, ...envelope } = body.value;
  const parsed = ApplicationProblemEnvelopeSchema.safeParse(envelope);
  if (!parsed.success) {
    return refused('unsupported_schema', 'the daemon returned a problem this build cannot read');
  }
  const problem = parsed.data.problem;
  const bindingMatches =
    bindingId === undefined ||
    (problem.kind !== 'not_found_or_not_authorized' &&
      APPLICATION_HTTP_BINDINGS[route.operation].bindingIds.some((id) => id === bindingId));
  if (!bindingMatches || problem.request_id !== parsed.data.request_id) {
    return refused('unsupported_schema', `the problem does not answer ${route.operation}`);
  }
  return { outcome: 'refused', state: PROBLEM_STATE[problem.kind], detail: problem.message, problem };
}

/** A non-2xx the gateway answered itself, outside the application contract. */
function gatewayRefusal(status: number, body: unknown): WorkResult<never> {
  const detail = isRecord(body) && typeof body.detail === 'string' ? body.detail : `HTTP ${status}`;
  switch (status) {
    case 404:
      return refused('denied', detail);
    case 503:
      return refused('unavailable', detail);
    default:
      return refused('error', detail);
  }
}

/**
 * Call one application route.
 *
 * The request is validated against its generated schema before it is sent, so a
 * malformed command is a local failure rather than a 400 the user has to
 * interpret. The response payload is validated against its generated schema
 * before it is returned, so nothing downstream can receive a value this build's
 * contracts do not describe.
 *
 * `signal` is the caller's abort: a fetch or body read it cancels rejects
 * rather than resolving as `offline` or `unsupported_schema`, so React Query
 * discards the abandoned read instead of caching a failure nobody observed.
 */
export async function callWork<Request, Response>(
  route: WorkRoute<Request, Response>,
  request: Request,
  url: string,
  signal?: AbortSignal,
): Promise<WorkResult<Response>> {
  const encoded = route.request.safeParse(request);
  if (!encoded.success) {
    return refused('error', `the request does not satisfy ${route.operation}`);
  }

  let response: Response_;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { accept: 'application/json', 'content-type': 'application/json' },
      body: JSON.stringify(encoded.data),
      signal,
    });
  } catch (err) {
    if (signal?.aborted === true) throw err;
    return refused('offline', 'the daemon could not be reached');
  }

  // The SDK withholds 401/403 bodies too: they are the gateway's credential
  // and origin checks, not application problems.
  if (response.status === 401) {
    return refused('unauthorized', 'the dashboard session is not authorized');
  }
  if (response.status === 403) {
    return refused('denied', 'the dashboard origin was refused');
  }

  const body = await decodeJsonBody(response, signal);
  if (response.ok) {
    if (body === UNDECODABLE) {
      return refused('unsupported_schema', 'the daemon returned a body that is not JSON');
    }
    return decodeSuccess(route, body);
  }
  const locked = response.status === 405 ? readOnlyScopeRefusal(body) : null;
  if (locked !== null) return refused('locked', locked.detail);
  return decodeProblem(route, body) ?? gatewayRefusal(response.status, body);
}

/** Aliased so the `Response` type is not shadowed by the generic parameter. */
type Response_ = globalThis.Response;
