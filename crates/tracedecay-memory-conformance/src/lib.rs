#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(warnings)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::print_stderr)]
#![deny(clippy::print_stdout)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::unwrap_used)]
//! Provider-neutral conformance fixtures and differential reports.
//!
//! The crate runs any [`tracedecay_memory_provider_api::MemoryProvider`]
//! through the same pinned mandatory handshake, health, observation, and
//! recall scenarios. Product reports retain canonical provider replies for
//! evaluation; observer reports expose only typed terminal receipts and
//! therefore cannot carry provider payloads or extensions into product output.

mod fixture;
mod harness;
mod report;

pub use fixture::{
    ConformanceError, FixtureIdentity, MANDATORY_FIXTURE_BUILD_SHA256,
    MANDATORY_FIXTURE_ID, MandatoryFixture, MandatoryScenario,
};
pub use harness::MandatoryConformanceHarness;
pub use report::{
    DifferentialField, DifferentialFinding, DifferentialReport, ObserverConformanceReport,
    ObserverScenarioReceipt, ObserverTerminalReceipt, ProductConformanceReport, ScenarioReport,
};
