use super::*;
use crate::delivery_spool::HookDeliverySpoolError;
use crate::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracedecay_domain::{
    DeliveryChannelIdentityV1, DeliveryEventClassV1, DeliverySettlementAttemptV1,
    DeliverySettlementOutcomeV1, DeliverySettlementV1, DeliverySurfaceFamilyV1,
};

struct TestDir(PathBuf);
impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        Self(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/test-profile/capture-leases")
                .join(format!(
                    "{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                )),
        )
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
const HOST: HookHostV1 = HookHostV1::ClaudeCode;
const PAYLOAD: &[u8] = include_bytes!("../fixtures/host_events/claude/stop.json");
fn bind(root: &Path) {
    std::fs::create_dir_all(root).unwrap();
    HookConfigurationPublisherV1::new(HookConfigurationFileWriterV1::new(hook_configuration_path(
        root, HOST,
    )))
    .publish(HookConfigurationSnapshotV1 {
        schema_version: HOOK_CONFIGURATION_SCHEMA_VERSION,
        revision: 1,
        published_at: UtcMicros(1),
        expires_at: UtcMicros(1000),
        binding: HookScopeBindingV1 {
            host: HOST,
            project_id: [1; 16],
            repository_id: [2; 16],
            worktree_id: [3; 16],
            worktree_epoch: 1,
            binding_token: [4; 32],
            capabilities: vec![HookCapabilityV1 {
                family: HookEventFamily::SessionBoundary,
                support: HookEventSupportV1::Native,
            }],
        },
    })
    .unwrap();
}
fn capture(
    root: &Path,
    payload: &[u8],
    deadline: Instant,
) -> Result<HookDeliveryReceiptSpoolV1, NativeHookCaptureOutcomeV1> {
    capture_native_event_with_delivery_writer(
        root,
        NativeHookCaptureSourceV1::Host(HOST),
        payload,
        material(10),
        UtcMicros(10),
        deadline,
    )
}
#[test]
fn held_receipt_writer_refuses_before_capture_publication() {
    let root = TestDir::new();
    bind(&root.0);
    let owner =
        HookDeliveryReceiptSpoolV1::open(hook_delivery_receipt_spool_root(&root.0, HOST)).unwrap();
    assert_eq!(
        capture(&root.0, PAYLOAD, Instant::now() + Duration::from_millis(20)).unwrap_err(),
        NativeHookCaptureOutcomeV1::AdmissionTimedOut
    );
    assert!(!root.0.join("hook-v2-spool").exists());
    assert!(owner.pending(64).unwrap().is_empty());
}
#[test]
fn capture_retains_receipt_writer_through_commit() {
    let root = TestDir::new();
    bind(&root.0);
    let writer = capture(
        &root.0,
        PAYLOAD,
        Instant::now()
            + Duration::from_micros(HookSynchronousDeadlineV1::start().remaining_micros()),
    )
    .unwrap();
    let receipt_root = hook_delivery_receipt_spool_root(&root.0, HOST);
    assert_eq!(
        HookDeliveryReceiptSpoolV1::open(&receipt_root).unwrap_err(),
        HookDeliverySpoolError::Busy
    );
    let receipt = HookDeliverySourceReceiptV1::new(DeliverySettlementV1 {
        attempt: DeliverySettlementAttemptV1 {
            owner_event_id: "hook:native:fixture".to_owned(),
            event_class: DeliveryEventClassV1::Activity,
            channel: DeliveryChannelIdentityV1 {
                surface: DeliverySurfaceFamilyV1::Hook,
                channel_ref: "hook:claude:session-fixture".to_owned(),
            },
            work_attempt: None,
            eligible: 1,
            valid_at: UtcMicros(10),
            attempted_at: UtcMicros(11),
        },
        outcome: DeliverySettlementOutcomeV1::Delivered,
        settled_at: UtcMicros(11),
        drop_reason: None,
    })
    .unwrap();
    assert!(writer.append(&receipt).unwrap());
    assert_eq!(
        HookDeliveryReceiptSpoolV1::open(&receipt_root).unwrap_err(),
        HookDeliverySpoolError::Busy
    );
    drop(writer);
    assert_eq!(
        HookDeliveryReceiptSpoolV1::open(&receipt_root)
            .unwrap()
            .pending(64)
            .unwrap(),
        vec![receipt]
    );
}
#[test]
fn unbound_unsupported_and_invalid_capture_do_not_create_writers() {
    for (payload, outcome) in [
        (PAYLOAD, NativeHookCaptureOutcomeV1::Unbound),
        (b"not json".as_slice(), NativeHookCaptureOutcomeV1::Rejected),
        (
            br#"{"hook_event_name":"Unknown"}"#.as_slice(),
            NativeHookCaptureOutcomeV1::Unsupported,
        ),
    ] {
        let root = TestDir::new();
        assert_eq!(
            capture(&root.0, payload, Instant::now()).unwrap_err(),
            outcome
        );
        assert!(!root.0.exists());
    }
    let root = TestDir::new();
    bind(&root.0);
    assert_eq!(
        capture(&root.0, b"not json", Instant::now()).unwrap_err(),
        NativeHookCaptureOutcomeV1::Rejected
    );
    assert!(!hook_delivery_receipt_spool_root(&root.0, HOST).exists());
    assert!(!root.0.join("hook-v2-spool").exists());
}

fn material(observed_at: i64) -> NativeEnvelopeMaterialV1 {
    NativeEnvelopeMaterialV1 {
        event_id: [5; 16],
        protected_session_id: [6; 32],
        observed_at: UtcMicros(observed_at),
        tool_id: None,
        effect_receipt_id: None,
        file_id: None,
        changed_range_count: 0,
    }
}

fn native_receipt(observed_at: i64) -> HookDeliverySourceReceiptV1 {
    HookDeliverySourceReceiptV1::new(
        native_hook_delivery_settlement(
            NativeHookCaptureSourceV1::Host(HOST),
            material(observed_at),
            UtcMicros(observed_at + 1),
        )
        .unwrap(),
    )
    .unwrap()
}

fn fill_receipt_queue(root: &Path, include_retry: bool) -> HookDeliverySourceReceiptV1 {
    let receipt_root = hook_delivery_receipt_spool_root(root, HOST);
    let writer = HookDeliveryReceiptSpoolV1::open(&receipt_root).unwrap();
    let original = native_receipt(10);
    for index in 0..crate::delivery_spool::MAX_PENDING_RECEIPTS {
        let mut settlement = original.settlement.clone();
        if index != 0 || !include_retry {
            settlement.attempt.owner_event_id = format!("hook:native:other-{index}");
        }
        let receipt = HookDeliverySourceReceiptV1::new(settlement).unwrap();
        let name = format!(
            "{}.delivery.v1.json",
            tracedecay_domain::canonical_text::encode_lowercase_hex(&receipt.receipt_id)
        );
        std::fs::write(
            receipt_root.join(name),
            tracedecay_domain::canonical_json_bytes(&receipt).unwrap(),
        )
        .unwrap();
    }
    drop(writer);
    original
}

#[test]
fn full_receipt_queue_refuses_new_event_before_capture() {
    let root = TestDir::new();
    bind(&root.0);
    fill_receipt_queue(&root.0, false);
    assert_eq!(
        capture(
            &root.0,
            PAYLOAD,
            Instant::now()
                + Duration::from_micros(HookSynchronousDeadlineV1::start().remaining_micros())
        )
        .unwrap_err(),
        NativeHookCaptureOutcomeV1::Full
    );
    assert!(!root.0.join("hook-v2-spool").exists());
    let writer =
        HookDeliveryReceiptSpoolV1::open(hook_delivery_receipt_spool_root(&root.0, HOST)).unwrap();
    assert_eq!(
        writer.pending(usize::MAX).unwrap().len(),
        crate::delivery_spool::MAX_PENDING_RECEIPTS
    );
}

#[test]
fn full_receipt_queue_admits_same_identity_retry_without_replacing_evidence() {
    let root = TestDir::new();
    bind(&root.0);
    let original = fill_receipt_queue(&root.0, true);
    let retry = native_receipt(20);
    assert_eq!(original.receipt_id, retry.receipt_id);
    let writer = capture_native_event_with_delivery_writer(
        &root.0,
        NativeHookCaptureSourceV1::Host(HOST),
        PAYLOAD,
        material(20),
        UtcMicros(20),
        Instant::now()
            + Duration::from_micros(HookSynchronousDeadlineV1::start().remaining_micros()),
    )
    .unwrap();
    assert!(root.0.join("hook-v2-spool").exists());
    assert!(!writer.append(&retry).unwrap());
    let pending = writer.pending(usize::MAX).unwrap();
    assert_eq!(pending.len(), crate::delivery_spool::MAX_PENDING_RECEIPTS);
    assert!(pending.contains(&original));
    assert!(!pending.contains(&retry));
}
