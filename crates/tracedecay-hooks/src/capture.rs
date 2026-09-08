//! Minimal native hook capture into the bounded replay spool.
//!
//! This path reads only a daemon-published binding and writes only the
//! content-free transport spool. It has no daemon, database, query, model,
//! session, memory, sync, or indexing authority.

#[cfg(test)]
#[path = "capture_tests.rs"]
mod tests;

use std::path::Path;
use std::time::Instant;

use tracedecay_domain::UtcMicros;

use crate::{
    HookConfigurationFileReaderV1, HookConfigurationReadOutcomeV1, HookConfigurationSubscriberV1,
    HookDeliveryReceiptSpoolV1, HookEventEnvelopeV2, HookHostV1, HookScopeBindingV1,
    HookSpoolConfigV1, HookSpoolError, HookSpoolV1, NativeEnvelopeMaterialV1,
    NativeHookDecodeError, OpenCodePluginSurfaceV1, decode_native_hook_event,
    decode_opencode_plugin_event, hook_configuration_path, hook_delivery_receipt_spool_root,
};

/// The real host surface that supplied native hook bytes.
///
/// OpenCode's direct tool callback has a distinct checked-in wire shape even
/// though it produces the same host-neutral envelope as its event-bus route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeHookCaptureSourceV1 {
    Host(HookHostV1),
    OpenCodeToolExecuteAfter,
}

impl NativeHookCaptureSourceV1 {
    pub const fn host(self) -> HookHostV1 {
        match self {
            Self::Host(host) => host,
            Self::OpenCodeToolExecuteAfter => HookHostV1::OpenCode,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeHookCaptureOutcomeV1 {
    Captured,
    Unsupported,
    Unbound,
    Rejected,
    Full,
    ResetRequired,
    Unavailable,
    AdmissionTimedOut,
}

fn record_capture_outcome(outcome: NativeHookCaptureOutcomeV1) {
    let _ = outcome;
    #[cfg(feature = "hotpath")]
    {
        hotpath::gauge!(match outcome {
            NativeHookCaptureOutcomeV1::AdmissionTimedOut =>
                "hooks.capture.outcome.admission_timed_out",
            NativeHookCaptureOutcomeV1::Captured => "hooks.capture.outcome.captured",
            NativeHookCaptureOutcomeV1::Unsupported => "hooks.capture.outcome.unsupported",
            NativeHookCaptureOutcomeV1::Unbound => "hooks.capture.outcome.unbound",
            NativeHookCaptureOutcomeV1::Rejected => "hooks.capture.outcome.rejected",
            NativeHookCaptureOutcomeV1::Full => "hooks.capture.outcome.full",
            NativeHookCaptureOutcomeV1::ResetRequired => "hooks.capture.outcome.reset_required",
            NativeHookCaptureOutcomeV1::Unavailable => "hooks.capture.outcome.unavailable",
        })
        .inc(1);
    }
}

/// Captures only after admitting the receipt writer under the same invocation
/// deadline. Success transfers that lease to the caller, which must retain it
/// through stdout flush and receipt append; no post-publication admission occurs.
#[hotpath::measure(label = "hooks.capture.native_event")]
pub fn capture_native_event_with_delivery_writer(
    data_root: &Path,
    source: NativeHookCaptureSourceV1,
    payload: &[u8],
    material: NativeEnvelopeMaterialV1,
    now: UtcMicros,
    deadline: Instant,
) -> Result<HookDeliveryReceiptSpoolV1, NativeHookCaptureOutcomeV1> {
    let result =
        capture_native_event_for_replay_inner(data_root, source, payload, material, now, deadline);
    record_capture_outcome(
        result
            .as_ref()
            .map_or_else(|outcome| *outcome, |_| NativeHookCaptureOutcomeV1::Captured),
    );
    result
}

fn capture_native_event_for_replay_inner(
    data_root: &Path,
    source: NativeHookCaptureSourceV1,
    payload: &[u8],
    material: NativeEnvelopeMaterialV1,
    now: UtcMicros,
    deadline: Instant,
) -> Result<HookDeliveryReceiptSpoolV1, NativeHookCaptureOutcomeV1> {
    let host = source.host();
    let decoded_result = match source {
        NativeHookCaptureSourceV1::Host(host) => decode_native_hook_event(host, payload),
        NativeHookCaptureSourceV1::OpenCodeToolExecuteAfter => {
            decode_opencode_plugin_event(OpenCodePluginSurfaceV1::ToolExecuteAfter, payload)
        }
    };
    let decoded = match decoded_result {
        Ok(decoded) => decoded,
        Err(
            NativeHookDecodeError::UnsupportedNativeEvent
            | NativeHookDecodeError::UnsupportedNativeFamily,
        ) => return Err(NativeHookCaptureOutcomeV1::Unsupported),
        Err(_) => return Err(NativeHookCaptureOutcomeV1::Rejected),
    };
    let subscriber = HookConfigurationSubscriberV1::new(HookConfigurationFileReaderV1::new(
        hook_configuration_path(data_root, host),
    ));
    let HookConfigurationReadOutcomeV1::Bound(snapshot) = subscriber.load_current(host, now) else {
        return Err(NativeHookCaptureOutcomeV1::Unbound);
    };
    let envelope = match decoded.into_envelope(&snapshot.binding, material) {
        Ok(envelope) => envelope,
        Err(_) => return Err(NativeHookCaptureOutcomeV1::Rejected),
    };
    // Decode and binding validation are complete before any receipt writes.
    let delivery_writer = HookDeliveryReceiptSpoolV1::open_until(
        hook_delivery_receipt_spool_root(data_root, host),
        deadline,
    )
    .map_err(delivery_admission_outcome)?;
    // This candidate supplies only the stable identity for capacity admission.
    // Nothing is persisted until the caller has actually flushed host output.
    let candidate = native_hook_delivery_settlement(source, material, now)
        .ok_or(NativeHookCaptureOutcomeV1::Rejected)?;
    let candidate =
        crate::HookDeliverySourceReceiptV1::new(candidate).map_err(delivery_admission_outcome)?;
    delivery_writer
        .admit_capacity(&candidate)
        .map_err(delivery_admission_outcome)?;
    let spool_root = data_root.join("hook-v2-spool").join(host.hook_key());
    let mut spool =
        match HookSpoolV1::open_until(spool_root, HookSpoolConfigV1::stock(host), now, deadline) {
            Ok((spool, _)) => spool,
            Err(HookSpoolError::AdmissionTimedOut) => {
                return Err(NativeHookCaptureOutcomeV1::AdmissionTimedOut);
            }
            Err(HookSpoolError::SpoolFull) => return Err(NativeHookCaptureOutcomeV1::Full),
            Err(HookSpoolError::ResetRequired { .. }) => {
                return Err(NativeHookCaptureOutcomeV1::ResetRequired);
            }
            Err(_) => return Err(NativeHookCaptureOutcomeV1::Unavailable),
        };
    let envelope = redelivered_envelope(&mut spool, &snapshot.binding, envelope);
    match spool.append(envelope, &snapshot.binding, now) {
        Ok(_) => Ok(delivery_writer),
        Err(HookSpoolError::SpoolFull) => Err(NativeHookCaptureOutcomeV1::Full),
        Err(HookSpoolError::ResetRequired { .. }) => Err(NativeHookCaptureOutcomeV1::ResetRequired),
        Err(_) => Err(NativeHookCaptureOutcomeV1::Unavailable),
    }
}

/// Reuses the queued envelope when this callback is a redelivery of an event
/// the spool already holds.
///
/// The event ID is derived from the host's own identity fields, so one host
/// event delivered twice — a retried callback, or sibling hooks firing
/// concurrently for the same edit — produces two envelopes that differ only in
/// the instant each process observed it. `HookSpoolV1::append` is idempotent
/// for an identical envelope but reports `EventIdConflict` for that timestamp
/// difference, which the capture lane surfaced as a failed hook exit even
/// though the event was already durable. Normalise the observation instant the
/// way the response path's `replay_envelope_if_pending` does; anything else
/// that differs stays a genuine conflict.
fn redelivered_envelope(
    spool: &mut HookSpoolV1,
    binding: &HookScopeBindingV1,
    envelope: HookEventEnvelopeV2,
) -> HookEventEnvelopeV2 {
    let Ok(Some(queued)) = spool.pending_envelope(envelope.event_id) else {
        return envelope;
    };
    if queued.validate(binding).is_err() {
        return envelope;
    }
    let mut candidate = envelope.clone();
    candidate.observed_at = queued.observed_at;
    if queued == candidate {
        queued
    } else {
        envelope
    }
}

fn delivery_admission_outcome(
    error: crate::delivery_spool::HookDeliverySpoolError,
) -> NativeHookCaptureOutcomeV1 {
    use crate::delivery_spool::HookDeliverySpoolError;
    match error {
        HookDeliverySpoolError::AdmissionTimedOut => NativeHookCaptureOutcomeV1::AdmissionTimedOut,
        HookDeliverySpoolError::Full => NativeHookCaptureOutcomeV1::Full,
        _ => NativeHookCaptureOutcomeV1::Unavailable,
    }
}

/// Derives native output settlement identity; callers persist only after flush.
pub fn native_hook_delivery_settlement(
    source: NativeHookCaptureSourceV1,
    material: NativeEnvelopeMaterialV1,
    delivered_at: UtcMicros,
) -> Option<tracedecay_domain::DeliverySettlementV1> {
    let host = source.host();
    let owner = tracedecay_domain::canonical_sha256(&(
        "tracedecay.native-hook-output-delivery.v1",
        host.hook_key(),
        material.event_id,
    ))
    .ok()?;
    let channel = tracedecay_domain::canonical_sha256(&(
        "tracedecay.native-hook-output-channel.v1",
        host.hook_key(),
        material.protected_session_id,
    ))
    .ok()?;
    let attempted_at = std::cmp::max(material.observed_at, delivered_at);
    Some(tracedecay_domain::DeliverySettlementV1 {
        attempt: tracedecay_domain::DeliverySettlementAttemptV1 {
            owner_event_id: format!(
                "hook:native:{}",
                owner.as_str().trim_start_matches("sha256:")
            ),
            event_class: tracedecay_domain::DeliveryEventClassV1::Activity,
            channel: tracedecay_domain::DeliveryChannelIdentityV1 {
                surface: tracedecay_domain::DeliverySurfaceFamilyV1::Hook,
                channel_ref: format!(
                    "hook:{}:{}",
                    host.hook_key(),
                    channel.as_str().trim_start_matches("sha256:")
                ),
            },
            work_attempt: None,
            eligible: 1,
            valid_at: material.observed_at,
            attempted_at,
        },
        outcome: tracedecay_domain::DeliverySettlementOutcomeV1::Delivered,
        settled_at: attempted_at,
        drop_reason: None,
    })
}
