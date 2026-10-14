//! Live-compaction outcome → operator receipt.

use crate::retention::live_compaction::LiveStoreCompactionOutcomeV1;
use tracedecay_runtime_core::logging::log_daemon_event;

/// Record one live-compaction outcome and return whether the store is healthy.
#[must_use]
pub fn record_live_compaction_outcome(
    store_name: &'static str,
    outcome: LiveStoreCompactionOutcomeV1,
) -> bool {
    match outcome {
        LiveStoreCompactionOutcomeV1::NotScheduled => true,
        LiveStoreCompactionOutcomeV1::Compacted {
            page_count_before,
            page_count_after,
            freelist_before,
            freelist_after,
        } => {
            log_daemon_event(
                "retention_compaction",
                &[
                    ("store", store_name.to_owned()),
                    (
                        "observed_page_count_reduction",
                        page_count_before
                            .saturating_sub(page_count_after)
                            .to_string(),
                    ),
                    ("freelist_before", freelist_before.to_string()),
                    ("freelist_after", freelist_after.to_string()),
                ],
            );
            true
        }
        LiveStoreCompactionOutcomeV1::NoProgress {
            page_count_before,
            page_count_after,
            freelist_before,
            freelist_after,
        } => {
            log_daemon_event(
                "retention_compaction_no_progress",
                &[
                    ("store", store_name.to_owned()),
                    ("page_count_before", page_count_before.to_string()),
                    ("page_count_after", page_count_after.to_string()),
                    ("freelist_before", freelist_before.to_string()),
                    ("freelist_after", freelist_after.to_string()),
                ],
            );
            // A successful no-op needs the ordinary maintenance cadence, not
            // a short retry loop that cannot change the store's vacuum mode.
            true
        }
        LiveStoreCompactionOutcomeV1::Failed(failure) => {
            log_daemon_event(
                "retention_degraded",
                &[
                    ("pass", "compaction".to_owned()),
                    ("failure", failure.as_str().to_owned()),
                ],
            );
            false
        }
    }
}
