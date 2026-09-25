# Retained provider-control uncertainty: final paired draft

Both review patches are UNAPPLIED. No Cargo, tests, or generators ran.

Apply only this final pair after root review:
1. pm-retained-host-unknown.integration.patch
2. pm-provider-control-uncertainty.integration.patch

The outcome patch supersedes pm-outcome-receipt-followup.integration.patch and
pm-outcome-receipt-followup-v2.integration.patch; do not apply either older
outcome patch alongside this final one.

## Root representation decision implemented

The retained host-facing ProviderControlEffectV1 Unknown shape may omit a
provider receipt when no provider reply or durable host-intent outcome was
witnessed. A present receipt digest is still validated as a canonical SHA-256.
Unknown still forbids generations, boundaries, item partitions, verification,
and duplicate identities, and requires a bounded nonempty reconciliation action.
All terminal/operation/scope/registration/idempotency contracts remain intact.
Committed, Partial, and Duplicate still require provider receipts.

The core MemoryProvider API CommittedEffectEvidence and raw provider reply
validators are untouched. Its validate_unknown_effect continues to require
provider_receipt_sha256. No host metadata is hashed into a provider proof.
The contract documentation change is on the validator method, so it does not
alter the derived schema description or introduce a new wire field.

## Producer behavior

- Malformed success retains the actual mutation receipt digest before clearing
  operation data. Missing evidence remains missing; a malformed present digest
  still causes strict retained validation to reject the result.
- Projection/artifact failures can retain the core-validated received terminal's
  provider receipt, diagnostic, and reconciliation action independently from
  rejected operation data. The call must match the retained provider,
  registration, exact scope, operation identity, and idempotency key; the terminal
  must also match its call's provider, operation, operation identity, exact scope,
  and duplicate binding. A mismatched terminal contributes no provider proof.
- No-reply host uncertainty retains the full typed payload, accepted identity,
  warning, no provider receipt, outer EffectUnknown, and Pending reconciliation.
- The committed fixture supplies its verification digest and no partial boundary.
- Only a successful initial Complete inspection page with no request cursor,
  next cursor, or redactions gets total=returned. Partial/resumed totals stay
  unknown; existing paging assertions remain and a resumed Complete case is added.

## Drafted regression coverage

- Retained host Unknown without a receipt is valid, actual receipt remains valid,
  malformed present receipt is invalid, and commit/verification/duplicate fields
  cannot be smuggled into host uncertainty.
- Unknown still enforces bounded reconciliation, terminal, key, operation ID,
  registration, and scope checks.
- Otherwise-valid Committed/Partial/Duplicate effects reject receipt removal.
- Host uncertainty preserves accepted provider/control identity and warning.
- An actual provider terminal's receipt/diagnostic/reconciliation are available
  only for the operation it answers; a different operation is rejected.
- Malformed-success fallback retains the actual receipt and diagnostic.
- Complete first-page, partial page, resumed final page, and item-limit behavior.

The old no-provider-reply representation blocker is resolved by the root's
retained-contract decision; payload=None is not used.
