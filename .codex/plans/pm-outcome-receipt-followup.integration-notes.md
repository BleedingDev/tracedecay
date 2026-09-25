# Outcome receipt follow-up: frozen partial draft

This one-file delta is UNAPPLIED. No Cargo, tests, or generators ran.

Included:
- Preserve the actual provider mutation-receipt digest before clearing malformed success data, retaining original identity and diagnostic. No digest is fabricated.
- Correct committed fixture: no partial boundary; distinct fixture verification digest is present.
- Report a total only for successful initial Complete inspection with no request cursor, next cursor, or redactions. Partial and resumed-page totals remain unknown.
- Preserve existing full-page/cursor assertions and add a resumed Complete page asserting unknown total and unchanged payload.

Explicit remaining blocker:
- HostIntentUnknown/DispatchWorker and other no-provider-reply uncertainty have no actual provider receipt. The existing Unknown provider effect validator correctly requires one; it must not be weakened or satisfied by a host-derived fake digest.
- EffectResult and EffectReceipt have no diagnostic, original provider operation ID, provider registration/scope, or reconciliation-action metadata slots. Payload=None can retain EffectUnknown, Pending, key, and derived effect ID but drops required diagnostic and explicit provider identity. ApplicationEnvelope has no extra slot either.
- Therefore the no-provider-reply branch is unchanged pending the root's explicit representation decision. The existing first distinction test still exercises that unresolved branch; its assertions were not removed or replaced with an easier provider-reply fixture.
- A malformed successful reply without any actual mutation receipt remains subject to the same representation gap; the preservation change does not invent missing receipt evidence.

No shared contract/validator edits are included. The production source was read only while drafting.
