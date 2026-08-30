use tracedecay_memory_provider_api::contract::{CommittedEffectState, TerminalCode};
use tracedecay_memory_provider_api::{
    HandshakeResponse, ProviderDescriptor, ProviderOperation, ProviderReply,
};

use crate::FixtureIdentity;

/// Full product-visible result for one mandatory provider operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioReport {
    /// Mandatory operation that was executed.
    pub operation: ProviderOperation,
    /// Complete canonical provider reply retained for product evaluation.
    pub reply: ProviderReply,
}

/// Full conformance report for explicit product evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductConformanceReport {
    /// Exact contract, fixture, provider, and implementation identities.
    pub identity: FixtureIdentity,
    /// Provider descriptor observed before the run.
    pub descriptor: ProviderDescriptor,
    /// Complete validated handshake response.
    pub handshake: HandshakeResponse,
    /// Complete validated mandatory operation replies.
    pub scenarios: Vec<ScenarioReport>,
}

/// Payload-free terminal summary used by observer-only evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObserverTerminalReceipt {
    /// Closed terminal classification.
    pub terminal_code: TerminalCode,
    /// Truthful provider-local committed-effect state.
    pub committed_effect: CommittedEffectState,
    /// Provider-local generation visible after the operation.
    pub state_generation: u64,
}

/// Payload-free observer receipt for one mandatory operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObserverScenarioReceipt {
    /// Mandatory operation that was observed.
    pub operation: ProviderOperation,
    /// Terminal-only observer receipt.
    pub terminal: ObserverTerminalReceipt,
}

/// Structurally isolated observer result with no payload or extension field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObserverConformanceReport {
    /// Exact contract, fixture, provider, and implementation identities.
    pub identity: FixtureIdentity,
    /// Payload-free handshake terminal.
    pub handshake: ObserverTerminalReceipt,
    /// Payload-free mandatory operation terminals.
    pub scenarios: Vec<ObserverScenarioReceipt>,
}

/// Stable field classification for one differential finding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DifferentialField {
    /// Providers returned different handshake terminals.
    HandshakeTerminal,
    /// Providers returned a different number of mandatory scenarios.
    ScenarioCount,
    /// Providers returned operations in different positions.
    ScenarioOperation,
    /// Providers returned different operation terminals.
    OperationTerminal,
    /// Providers reported different committed-effect states.
    CommittedEffect,
    /// Providers reported different post-operation generations.
    StateGeneration,
    /// Providers returned different canonical payload digests.
    PayloadSha256,
}

/// One typed difference between two product conformance reports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialFinding {
    /// Mandatory operation when the finding is operation-specific.
    pub operation: Option<ProviderOperation>,
    /// Stable compared field.
    pub field: DifferentialField,
    /// Left report value rendered for diagnostics.
    pub left: String,
    /// Right report value rendered for diagnostics.
    pub right: String,
}

/// Provider-neutral differential report over two conformance runs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialReport {
    /// Pinned fixture identity used by the left report.
    pub fixture_id: String,
    /// Left logical provider identity.
    pub left_provider_id: String,
    /// Right logical provider identity.
    pub right_provider_id: String,
    /// Deterministically ordered typed findings.
    pub findings: Vec<DifferentialFinding>,
}

impl ProductConformanceReport {
    /// Compares terminal semantics and canonical payload digests with another run.
    #[must_use]
    pub fn compare(&self, other: &Self) -> DifferentialReport {
        let mut findings = Vec::new();
        if self.handshake.terminal.terminal_code != other.handshake.terminal.terminal_code {
            findings.push(DifferentialFinding {
                operation: None,
                field: DifferentialField::HandshakeTerminal,
                left: self.handshake.terminal.terminal_code.as_wire().to_owned(),
                right: other.handshake.terminal.terminal_code.as_wire().to_owned(),
            });
        }
        if self.scenarios.len() != other.scenarios.len() {
            findings.push(DifferentialFinding {
                operation: None,
                field: DifferentialField::ScenarioCount,
                left: self.scenarios.len().to_string(),
                right: other.scenarios.len().to_string(),
            });
        }
        for (left, right) in self.scenarios.iter().zip(&other.scenarios) {
            if left.operation != right.operation {
                findings.push(DifferentialFinding {
                    operation: Some(left.operation),
                    field: DifferentialField::ScenarioOperation,
                    left: left.operation.capability_id().to_owned(),
                    right: right.operation.capability_id().to_owned(),
                });
            }
            if left.reply.terminal.terminal_code != right.reply.terminal.terminal_code {
                findings.push(DifferentialFinding {
                    operation: Some(left.operation),
                    field: DifferentialField::OperationTerminal,
                    left: left.reply.terminal.terminal_code.as_wire().to_owned(),
                    right: right.reply.terminal.terminal_code.as_wire().to_owned(),
                });
            }
            if left.reply.terminal.committed_effect != right.reply.terminal.committed_effect {
                findings.push(DifferentialFinding {
                    operation: Some(left.operation),
                    field: DifferentialField::CommittedEffect,
                    left: format!("{:?}", left.reply.terminal.committed_effect),
                    right: format!("{:?}", right.reply.terminal.committed_effect),
                });
            }
            if left.reply.state_generation != right.reply.state_generation {
                findings.push(DifferentialFinding {
                    operation: Some(left.operation),
                    field: DifferentialField::StateGeneration,
                    left: left.reply.state_generation.to_string(),
                    right: right.reply.state_generation.to_string(),
                });
            }
            let left_payload = left
                .reply
                .payload
                .as_ref()
                .map(|payload| payload.sha256.as_str());
            let right_payload = right
                .reply
                .payload
                .as_ref()
                .map(|payload| payload.sha256.as_str());
            if left_payload != right_payload {
                findings.push(DifferentialFinding {
                    operation: Some(left.operation),
                    field: DifferentialField::PayloadSha256,
                    left: format!("{left_payload:?}"),
                    right: format!("{right_payload:?}"),
                });
            }
        }
        DifferentialReport {
            fixture_id: self.identity.fixture_id.clone(),
            left_provider_id: self.identity.provider_id.clone(),
            right_provider_id: other.identity.provider_id.clone(),
            findings,
        }
    }
}
