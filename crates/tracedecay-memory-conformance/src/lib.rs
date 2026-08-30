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
//! Provider-neutral conformance and differential evaluation for cognitive-memory providers.
//!
//! Typed fixtures bind an exact contract set, provider implementation, build,
//! and coding scope. [`ProviderHarness`] materializes calls from the provider's
//! real handshake receipt and evaluates behavior without a dashboard or any
//! TraceDecay storage or code-index dependency.
//!
//! Product and observer execution deliberately return different report types.
//! [`ProductRunReport`] may retain typed provider outputs for active-path tests;
//! [`ObserverRunReport`] retains complete validated terminal consequences but
//! cannot carry provider-returned operation payload bytes into product output.
//! Both reports bind the immutable fixture-controlled inputs through
//! [`ScenarioIdentity`].

mod error;
mod fixture;
mod report;
mod runner;

pub use error::EvaluationError;
pub use fixture::{
    ContractIdentity, EffectGenerationExpectation, ExpectedCommittedEffect, FixtureIdentity,
    GenerationExpectation, HandshakeExpectation, HandshakeFixture, ItemRefsExpectation,
    OperationExpectation, OperationFixture, OptionalTextExpectation, PayloadExpectation,
    ProviderBuildIdentity, RequestControlFixture, ScenarioFixture, ScenarioIdentity,
    TerminalExpectation, mandatory_conformance_fixture,
};
pub use report::{
    ConformanceStatus, ConformanceViolation, DifferentialReport, DifferentialStep,
    ObservedStepSummary, ObserverRunReport, ObserverStepEvaluation, ObserverStepResult,
    ProductRunReport, ProductStepOutput, ProductStepResult, RunSummary, StepEvaluation, StepKind,
};
pub use runner::ProviderHarness;
