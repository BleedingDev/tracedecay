use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tracedecay_domain::{
    DeliveryChannelIdentityV1, DeliveryEventClassV1, DeliverySettlementAttemptV1,
    DeliverySettlementOutcomeV1, DeliverySettlementV1, DeliverySurfaceFamilyV1, UtcMicros,
};
use tracedecay_hooks::{
    HOOK_SYNCHRONOUS_BUDGET, HookDeliveryReceiptSpoolV1, HookDeliveryReceiptWriterV1,
    HookDeliveryRetentionV1, HookDeliverySourceReceiptV1,
};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "tracedecay-delivery-receipt-concurrency-{}-{sequence}",
            std::process::id(),
        ));
        tracedecay_private_fs::create_private_directory(&root)
            .expect("create owned receipt fixture");
        Self(root)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn concurrent_writers_on_a_slow_disk_publish_every_receipt_within_budget() {
    const WRITERS: usize = 8;
    let root = TestDir::new();
    drop(HookDeliveryReceiptSpoolV1::open(&root.0, Duration::ZERO).unwrap());
    let _slow_disk =
        tracedecay_private_fs::framed_log::sync_latency::inject(&root.0, Duration::from_millis(20));
    let receipts = (0..WRITERS).map(indexed_receipt).collect::<Vec<_>>();
    let retained = std::thread::scope(|scope| {
        let writers = receipts
            .iter()
            .map(|receipt| {
                let root = &root.0;
                scope.spawn(move || {
                    HookDeliveryReceiptWriterV1::open_within(root, HOOK_SYNCHRONOUS_BUDGET)
                        .and_then(|writer| writer.retain(receipt))
                })
            })
            .collect::<Vec<_>>();
        writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        retained,
        (0..WRITERS)
            .map(|_| Ok(HookDeliveryRetentionV1::Published))
            .collect::<Vec<_>>()
    );
    let mut pending = HookDeliveryReceiptSpoolV1::open(&root.0, Duration::ZERO)
        .unwrap()
        .pending(64)
        .unwrap();
    let mut expected = receipts;
    pending.sort_by_key(|receipt| receipt.receipt_id);
    expected.sort_by_key(|receipt| receipt.receipt_id);
    assert_eq!(pending, expected);
}

fn indexed_receipt(index: usize) -> HookDeliverySourceReceiptV1 {
    HookDeliverySourceReceiptV1::new(DeliverySettlementV1 {
        attempt: DeliverySettlementAttemptV1 {
            owner_event_id: format!("hook:native:fixture-{index}"),
            event_class: DeliveryEventClassV1::Activity,
            channel: DeliveryChannelIdentityV1 {
                surface: DeliverySurfaceFamilyV1::Hook,
                channel_ref: "hook:claude:session-fixture".to_owned(),
            },
            work_attempt: None,
            eligible: 1,
            valid_at: UtcMicros(100),
            attempted_at: UtcMicros(110),
        },
        outcome: DeliverySettlementOutcomeV1::Delivered,
        settled_at: UtcMicros(110),
        drop_reason: None,
    })
    .expect("receipt")
}
