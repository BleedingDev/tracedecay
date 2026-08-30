use tracedecay_memory_provider_api::contract::{
    CommittedEffectState, FallbackEligibility, TerminalCode,
};
use tracedecay_memory_provider_api::{
    HandshakeResponse, MemoryProvider, ProviderDescriptor, ProviderReply,
};

use crate::fixture::{ConformanceError, MandatoryFixture, MandatoryScenario, require_sha256};
use crate::report::{
    ObserverConformanceReport, ObserverScenarioReceipt, ObserverTerminalReceipt,
    ProductConformanceReport, ScenarioReport,
};

/// Provider-neutral runner for the pinned mandatory conformance fixture.
pub struct MandatoryConformanceHarness<'a> {
    provider: &'a dyn MemoryProvider,
}

impl<'a> MandatoryConformanceHarness<'a> {
    /// Borrows one provider implementation without selecting its topology.
    #[must_use]
    pub const fn new(provider: &'a dyn MemoryProvider) -> Self {
        Self { provider }
    }

    /// Runs the fixture and retains complete canonical replies for evaluation.
    pub fn run_product(
        &self,
        fixture: &MandatoryFixture,
    ) -> Result<ProductConformanceReport, ConformanceError> {
        let descriptor = self.provider.descriptor();
        validate_descriptor(fixture, &descriptor)?;
        let handshake = self.provider.handshake(&fixture.handshake);
        validate_handshake(fixture, &descriptor, &handshake)?;
        let ready_receipt = handshake
            .ready_receipt_sha256
            .as_deref()
            .ok_or(ConformanceError::HandshakeContractViolation(
                "successful handshake omitted ready receipt",
            ))?;
        let mut scenarios = Vec::with_capacity(fixture.scenarios.len());
        for scenario in &fixture.scenarios {
            if scenario.call.ready_receipt_sha256 != ready_receipt {
                return Err(ConformanceError::OperationContractViolation {
                    operation: scenario.operation(),
                    reason: "fixture ready receipt differs from handshake",
                });
            }
            let reply = self.provider.invoke(&scenario.call);
            validate_reply(fixture, scenario, &reply)?;
            scenarios.push(ScenarioReport {
                operation: scenario.operation(),
                reply,
            });
        }
        Ok(ProductConformanceReport {
            identity: fixture.identity.clone(),
            descriptor,
            handshake,
            scenarios,
        })
    }

    /// Runs the same fixture and returns terminal-only observer receipts.
    ///
    /// The return type has no provider payload, warning, extension, receipt,
    /// namespace, or accepted-scope field, so observer output cannot be reused
    /// as product context without an explicit new conversion boundary.
    pub fn run_observer(
        &self,
        fixture: &MandatoryFixture,
    ) -> Result<ObserverConformanceReport, ConformanceError> {
        let product = self.run_product(fixture)?;
        let handshake_generation = product
            .handshake
            .descriptor
            .as_ref()
            .map_or(product.descriptor.state_generation, |descriptor| {
                descriptor.state_generation
            });
        let handshake = ObserverTerminalReceipt {
            terminal_code: product.handshake.terminal.terminal_code,
            committed_effect: product.handshake.terminal.committed_effect,
            state_generation: handshake_generation,
        };
        let scenarios = product
            .scenarios
            .iter()
            .map(|scenario| ObserverScenarioReceipt {
                operation: scenario.operation,
                terminal: ObserverTerminalReceipt {
                    terminal_code: scenario.reply.terminal.terminal_code,
                    committed_effect: scenario.reply.terminal.committed_effect,
                    state_generation: scenario.reply.state_generation,
                },
            })
            .collect();
        Ok(ObserverConformanceReport {
            identity: product.identity.clone(),
            handshake,
            scenarios,
        })
    }
}

fn validate_descriptor(
    fixture: &MandatoryFixture,
    descriptor: &ProviderDescriptor,
) -> Result<(), ConformanceError> {
    if descriptor.provider_id.as_str() != fixture.identity.provider_id {
        return Err(ConformanceError::ProviderIdentityMismatch {
            expected: fixture.identity.provider_id.clone(),
            actual: descriptor.provider_id.as_str().to_owned(),
        });
    }
    if descriptor.implementation_identity_sha256
        != fixture.identity.provider_implementation_sha256
    {
        return Err(ConformanceError::ProviderImplementationMismatch {
            expected: fixture.identity.provider_implementation_sha256.clone(),
            actual: descriptor.implementation_identity_sha256.clone(),
        });
    }
    for capability in [
        "provider.health.v1",
        "observation.accept.v1",
        "recall.query.v1",
    ] {
        if !descriptor.supports(capability) {
            return Err(ConformanceError::MissingMandatoryCapability(capability));
        }
    }
    Ok(())
}

fn validate_handshake(
    fixture: &MandatoryFixture,
    descriptor: &ProviderDescriptor,
    response: &HandshakeResponse,
) -> Result<(), ConformanceError> {
    if response.terminal.terminal_code != TerminalCode::Success {
        return Err(ConformanceError::HandshakeRejected(
            response.terminal.terminal_code,
        ));
    }
    if response.terminal.operation_id != fixture.handshake.request_id {
        return Err(ConformanceError::HandshakeContractViolation(
            "terminal operation identity differs from request",
        ));
    }
    if response.terminal.exact_scope_sha256 != fixture.exact_scope_sha256 {
        return Err(ConformanceError::HandshakeContractViolation(
            "terminal exact-scope digest differs from fixture",
        ));
    }
    if response.terminal.fallback != FallbackEligibility::Forbidden {
        return Err(ConformanceError::HandshakeContractViolation(
            "fallback is not explicitly forbidden",
        ));
    }
    if response.terminal.committed_effect != CommittedEffectState::None {
        return Err(ConformanceError::HandshakeContractViolation(
            "read-only handshake reported a committed effect",
        ));
    }
    let response_descriptor = response
        .descriptor
        .as_ref()
        .ok_or(ConformanceError::HandshakeContractViolation(
            "successful handshake omitted descriptor",
        ))?;
    if response_descriptor != descriptor {
        return Err(ConformanceError::HandshakeContractViolation(
            "handshake descriptor differs from preflight descriptor",
        ));
    }
    if response.accepted_scope.as_ref() != Some(&fixture.handshake.exact_scope) {
        return Err(ConformanceError::HandshakeContractViolation(
            "accepted exact scope differs from request",
        ));
    }
    if response.effective_limits.is_none() {
        return Err(ConformanceError::HandshakeContractViolation(
            "successful handshake omitted effective limits",
        ));
    }
    if response
        .provider_instance_id
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(ConformanceError::HandshakeContractViolation(
            "successful handshake omitted provider instance identity",
        ));
    }
    if response.state_namespace.as_deref().is_none_or(str::is_empty) {
        return Err(ConformanceError::HandshakeContractViolation(
            "successful handshake omitted state namespace",
        ));
    }
    let ready_receipt = response
        .ready_receipt_sha256
        .as_deref()
        .ok_or(ConformanceError::HandshakeContractViolation(
            "successful handshake omitted ready receipt",
        ))?;
    require_sha256(ready_receipt, "ready_receipt_sha256")
        .map_err(|_| ConformanceError::HandshakeContractViolation("ready receipt is malformed"))?;
    Ok(())
}

fn validate_reply(
    fixture: &MandatoryFixture,
    scenario: &MandatoryScenario,
    reply: &ProviderReply,
) -> Result<(), ConformanceError> {
    let operation = scenario.operation();
    if reply.terminal.terminal_code != TerminalCode::Success {
        return Err(ConformanceError::OperationRejected {
            operation,
            terminal_code: reply.terminal.terminal_code,
        });
    }
    if reply.terminal.operation_id != scenario.call.operation_id {
        return Err(ConformanceError::OperationContractViolation {
            operation,
            reason: "terminal operation identity differs from request",
        });
    }
    if reply.terminal.exact_scope_sha256 != fixture.exact_scope_sha256 {
        return Err(ConformanceError::OperationContractViolation {
            operation,
            reason: "terminal exact-scope digest differs from fixture",
        });
    }
    if reply.terminal.fallback != FallbackEligibility::Forbidden {
        return Err(ConformanceError::OperationContractViolation {
            operation,
            reason: "fallback is not explicitly forbidden",
        });
    }
    if reply.terminal.committed_effect != scenario.expected_committed_effect {
        return Err(ConformanceError::OperationContractViolation {
            operation,
            reason: "committed-effect classification differs from fixture",
        });
    }
    if scenario.payload_required && reply.payload.is_none() {
        return Err(ConformanceError::OperationContractViolation {
            operation,
            reason: "successful mandatory result omitted payload",
        });
    }
    if let Some(payload) = &reply.payload {
        require_sha256(&payload.sha256, "payload_sha256").map_err(|_| {
            ConformanceError::OperationContractViolation {
                operation,
                reason: "result payload digest is malformed",
            }
        })?;
    }
    if scenario.expected_committed_effect == CommittedEffectState::Committed {
        let receipt = reply
            .terminal
            .provider_receipt_sha256
            .as_deref()
            .ok_or(ConformanceError::OperationContractViolation {
                operation,
                reason: "committed mutation omitted provider receipt",
            })?;
        require_sha256(receipt, "provider_receipt_sha256").map_err(|_| {
            ConformanceError::OperationContractViolation {
                operation,
                reason: "provider receipt is malformed",
            }
        })?;
    }
    Ok(())
}
