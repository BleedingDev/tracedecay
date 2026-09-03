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

pub mod adversarial;
mod baseline;
mod canonical;
mod error;
mod fixture;
mod report;
mod runner;
mod scenario_corpus;

pub use adversarial::{
    AdversarialLedgerV1, AdversarialPayloadSourceV1, AdversarialProviderInputsV1,
    AdversarialProviderV1, AdversarialScriptV1, ExhibitedV1, HandshakeMisbehaviourV1,
    MisbehaviourV1, NoPayloadSourceV1, ReleaseLatchV1,
};
pub use baseline::{
    AdjudicationRecord, BaselineComparison, BaselineError, BaselineLane, BaselineReport,
    BaselineRunConfig, BaselineRunIdentity, BaselineRunOutput, BaselineRunner, BaselineStepRecord,
    BaselineTimings, BatchBoundary, CallTiming, CheckRecord, CheckVerdict, ComparisonCell,
    ComparisonRow, ContextAdmission, ContextEntry, CountRecord, HostConfigIdentity, LaneIdentity,
    LaneKind, LimitsRecord, O200K_BASE_ESTIMATOR_ID, O200K_BASE_ESTIMATOR_REVISION,
    O200kBaseTokenEstimator, ProviderCallRecord, ProviderIdentityRecord, ProviderLane,
    ScenarioBaselineResult, ScenarioCostSummary, SharedInputs, StepOutcome, TerminalGate,
    TokenEstimateError, TokenEstimator, TokenEstimatorIdentity, TokenRecord,
};
pub use canonical::{CanonicalJsonError, canonical_json, canonical_json_sha256};
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
pub use scenario_corpus::{
    AdjudicationPolicy, AdjudicationRubric, CodeEvidenceRevision, CorpusError, DigestStatus,
    EvidenceDefinition, ExpectedAdmissibleBehavior, FileRevision, FixtureDefinition, FixtureFile,
    FixturePolicy, ObservationDefinition, ProviderSelectionPolicy, RUBRIC_WEIGHT_BASIS_POINTS,
    RecallBudgets, RecallExclusions, RecallRequestDefinition, RecallRequestOperation,
    RecallTemporalQuery, RubricCheck, ScenarioCorpus, ScenarioDefinition, ScenarioStep, ScopeEntry,
    basis_points,
};
