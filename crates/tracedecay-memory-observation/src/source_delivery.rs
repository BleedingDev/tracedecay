//! Exact source delivery evidence, separate from enqueue progress.

use crate::{
    AdmittedObservationV1, DeliveryStateV1, ObservationDeliveryReceiptV1, SourceSequenceV1,
};

/// One canonical source whose durable delivery a caller needs to establish.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedSourceDeliveryV1 {
    /// Position in the caller's exact source stream.
    pub source_sequence: SourceSequenceV1,
    /// Canonical event identity expected at that position.
    pub source_event_id: String,
}

/// Evidence for one requested source, returned in request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceDeliveryEvidenceV1 {
    /// No journal admission exists at the exact source key.
    Missing,
    /// Retained, strictly validated admission and its current delivery evidence.
    Retained {
        /// Sanitized admission, including the original source binding.
        admitted: Box<AdmittedObservationV1>,
        /// Actual durable state; enqueue progress never substitutes for it.
        state: DeliveryStateV1,
        /// Validated latest receipt, absent before an attempt has settled.
        receipt: Option<ObservationDeliveryReceiptV1>,
    },
    /// Content was purged; retained delivery metadata cannot authorize its use.
    Purged {
        /// State still retained by the delivery audit.
        state: DeliveryStateV1,
    },
}
