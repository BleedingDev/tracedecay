#![allow(dead_code)]
//! Shared fixtures. Every builder produces envelopes that pass `validate()`,
//! so a test that wants an invalid one has to break it on purpose.

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use sha2::{Digest, Sha256};
use tracedecay_memory_observation::{
    AdmittedObservationV1, BackpressureGateV1, BackpressurePolicyV1, CanonicalSettlementReceiptV1,
    DeliveryReceiptIdV1, ForgetSourceKeyV1, IngressControlV1, LeaseRequestV1, LeasedObservationV1,
    ObservationCommittedEffectV1, ObservationDeliveryReceiptV1, ObservationIdV1,
    ObservationIdempotencyKeyV1, ObservationLaneKeyV1, ObservationOutcomeV1, ObservationPrivacyV1,
    PrivacyClassificationV1, ProvenanceOriginV1, ProviderEffectSummaryV1, ProviderTargetV1,
    RetentionClassV1, RetentionPolicyV1, SanitizationBindingV1, SourceAuthorityV1,
    SourceSequenceV1, SourceStreamIdV1, SourceStreamKeyV1, SqliteObservationJournal,
    WithheldAdmissionV1, extensions_digest,
};
use tracedecay_memory_provider_api::{
    CanonicalPayload, OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId,
    PayloadSanitizationReceipt, PayloadSanitizationReceiptParts, SanitizationDisposition,
    WithheldReason, derive_withheld_receipt_id, empty_findings_digest,
};

pub const SECOND: i64 = 1_000_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;
pub const T0: i64 = 1_780_000_000_000_000;
pub const LEASE: i64 = 60 * SECOND;
pub const PROVIDER: &str = "tracedecay.native";
pub const INSTANCE: &str = "instance-1";
pub const OWNER: &str = "dispatcher-1";
pub const STREAM: &str = "session-1";
pub const READY_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const PROVENANCE_DIGEST: &str =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const SETTLEMENT_DIGEST: &str =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
pub const PROVIDER_RECEIPT_DIGEST: &str =
    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
pub const RESOLVED_SCOPE_DIGEST: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

pub type TestResult = Result<(), Box<dyn Error>>;

pub fn digest_hex(bytes: &[u8]) -> String {
    let value = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

pub fn policy() -> RetentionPolicyV1 {
    RetentionPolicyV1 {
        ephemeral_max_age_micros: HOUR,
        session_max_age_micros: DAY,
        project_max_age_micros: 30 * DAY,
        profile_max_age_micros: 365 * DAY,
        receipt_retention_micros: 7 * DAY,
        max_queue_items: 64,
        max_queue_bytes: 1_048_576,
        max_attempts: 3,
        backoff_base_micros: 10 * SECOND,
        backoff_max_micros: 5 * MINUTE,
        sweep_batch_rows: 256,
    }
}

pub fn scope() -> Result<OwnedExactScope, Box<dyn Error>> {
    Ok(OwnedExactScope::new(
        "profile-1",
        "project-1",
        "repo-1",
        "worktree-1",
        "refs/heads/main",
        "session-1",
        RESOLVED_SCOPE_DIGEST,
    )?)
}

pub fn payload(body: &str) -> Result<CanonicalPayload, Box<dyn Error>> {
    let bytes = body.as_bytes().to_vec();
    let sha256 = digest_hex(&bytes);
    Ok(CanonicalPayload::new(
        OwnedVersionedId::new("tracedecay.memory.observation.session-message.v1")?,
        bytes,
        sha256,
    )?)
}

pub fn extension(id: &str, body: &str) -> Result<OwnedOpaqueExtension, Box<dyn Error>> {
    let bytes = body.as_bytes().to_vec();
    let sha256 = digest_hex(&bytes);
    Ok(OwnedOpaqueExtension::new(
        OwnedVersionedId::new(id)?,
        1,
        false,
        sha256,
        bytes,
    )?)
}

pub fn target() -> Result<ProviderTargetV1, Box<dyn Error>> {
    Ok(ProviderTargetV1 {
        provider_id: OwnedProviderId::new(PROVIDER)?,
        provider_instance_id: INSTANCE.to_owned(),
        registration_revision: 4,
        ready_receipt_digest: READY_DIGEST.to_owned(),
    })
}

pub const SANITIZER_REVISION: &str = "observation-hygiene-policy.v1.3";

/// Mints the hygiene receipt an admission pipeline would produce for `body`,
/// and wraps it in the binding the journal stores.
///
/// The receipt is a real [`PayloadSanitizationReceipt`], not a stand-in: the
/// journal reparses and revalidates it, so a fixture that forged one would fail
/// admission exactly like a tampered one.
pub fn binding_for(body: &str) -> Result<SanitizationBindingV1, Box<dyn Error>> {
    binding_for_extensions(body, &[])
}

pub fn binding_for_extensions(
    body: &str,
    extensions: &[OwnedOpaqueExtension],
) -> Result<SanitizationBindingV1, Box<dyn Error>> {
    let sanitized = digest_hex(body.as_bytes());
    let source = digest_hex(format!("raw-source-of:{body}").as_bytes());
    let receipt = PayloadSanitizationReceipt::new(PayloadSanitizationReceiptParts {
        sanitizer_revision: SANITIZER_REVISION.to_owned(),
        source_payload_sha256: source.clone(),
        sanitized_payload_sha256: sanitized,
        extensions_digest: extensions_digest(extensions)?,
        disposition: SanitizationDisposition::Redacted,
        finding_count: 1,
        findings_digest: digest_hex(b"finding-set:secret-span-redacted"),
    })?;
    Ok(SanitizationBindingV1 {
        receipt_id: receipt.receipt_id().to_owned(),
        sanitizer_revision: receipt.sanitizer_revision().to_owned(),
        source_payload_sha256: source,
        receipt_json: receipt.to_json(),
    })
}

/// A binding for a payload the pipeline read and left byte-identical.
pub fn accepted_binding_for(body: &str) -> Result<SanitizationBindingV1, Box<dyn Error>> {
    accepted_binding_for_extensions(body, &[])
}

pub fn accepted_binding_for_extensions(
    body: &str,
    extensions: &[OwnedOpaqueExtension],
) -> Result<SanitizationBindingV1, Box<dyn Error>> {
    let sanitized = digest_hex(body.as_bytes());
    let receipt = PayloadSanitizationReceipt::new(
        PayloadSanitizationReceiptParts::accepted_unmodified_with_extensions(
            SANITIZER_REVISION,
            sanitized.clone(),
            extensions_digest(extensions)?,
        ),
    )?;
    Ok(SanitizationBindingV1 {
        receipt_id: receipt.receipt_id().to_owned(),
        sanitizer_revision: receipt.sanitizer_revision().to_owned(),
        source_payload_sha256: sanitized,
        receipt_json: receipt.to_json(),
    })
}

/// Rebuilds a receipt's JSON after a test mutates one of its parts, so a
/// perturbation test can produce a *self-consistent* receipt that is still
/// bound to the wrong thing.
pub fn receipt_json(parts: PayloadSanitizationReceiptParts) -> Result<String, Box<dyn Error>> {
    Ok(PayloadSanitizationReceipt::new(parts)?.to_json())
}

pub fn empty_findings() -> String {
    empty_findings_digest()
}

pub fn withheld_at(sequence: u64, forget_key: &str) -> Result<WithheldAdmissionV1, Box<dyn Error>> {
    let source_payload_sha256 = digest_hex(format!("refused-{sequence}").as_bytes());
    let findings_digest = digest_hex(format!("findings-{sequence}").as_bytes());
    let sanitizer_revision = SANITIZER_REVISION.to_owned();
    let extensions_digest = extensions_digest(&[])?;
    let receipt_id = derive_withheld_receipt_id(
        &sanitizer_revision,
        &source_payload_sha256,
        &extensions_digest,
        WithheldReason::SecretRejected,
        1,
        &findings_digest,
    );
    Ok(WithheldAdmissionV1 {
        source_authority: "host_session".to_owned(),
        exact_scope_sha256: scope()?.exact_scope_sha256(),
        source_stream: STREAM.to_owned(),
        source_sequence: sequence,
        source_event_id: format!("event-{sequence}"),
        source_event_revision: "0".to_owned(),
        receipt_id,
        reason: WithheldReason::SecretRejected.as_str().to_owned(),
        source_payload_sha256,
        extensions_digest,
        sanitizer_revision,
        finding_count: 1,
        findings_digest,
        forget_source_key: ForgetSourceKeyV1::new(forget_key)?,
    })
}

/// Builds one admitted observation. Mutate the returned value and re-seal it
/// with [`seal`] when a test needs a variant.
pub struct Builder {
    pub source_sequence: u64,
    pub source_event_id: String,
    pub source_event_revision: u64,
    pub source_stream: String,
    pub source_authority: SourceAuthorityV1,
    pub observation_kind: String,
    pub body: String,
    pub extensions: Vec<OwnedOpaqueExtension>,
    pub registration_revision: u64,
    pub provider_id: String,
    pub provider_instance_id: String,
    pub retention_class: RetentionClassV1,
    pub classification: PrivacyClassificationV1,
    pub forget_source_key: String,
    pub admitted_at: i64,
    pub expires_at: i64,
    pub deadline: i64,
    /// Overrides the binding the builder would otherwise mint for `body`. Only
    /// a test that wants a *broken* binding needs this.
    pub sanitization: Option<SanitizationBindingV1>,
    pub entropy: [u8; 10],
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            source_sequence: 1,
            source_event_id: "event-1".to_owned(),
            source_event_revision: 0,
            source_stream: STREAM.to_owned(),
            source_authority: SourceAuthorityV1::HostSession,
            observation_kind: "session.message_committed.v1".to_owned(),
            body: "{\"message\":\"hello\"}".to_owned(),
            extensions: Vec::new(),
            registration_revision: 4,
            provider_id: PROVIDER.to_owned(),
            provider_instance_id: INSTANCE.to_owned(),
            retention_class: RetentionClassV1::Project,
            classification: PrivacyClassificationV1::Internal,
            forget_source_key: "forget:session-1".to_owned(),
            admitted_at: T0,
            expires_at: T0 + 30 * DAY,
            deadline: T0 + HOUR,
            sanitization: None,
            entropy: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    }
}

impl Builder {
    pub fn at_sequence(sequence: u64) -> Self {
        Self {
            source_sequence: sequence,
            source_event_id: format!("event-{sequence}"),
            entropy: [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                u8::try_from(sequence >> 8).unwrap_or(0),
                u8::try_from(sequence & 0xff).unwrap_or(0),
            ],
            body: format!("{{\"message\":\"hello-{sequence}\"}}"),
            ..Self::default()
        }
    }

    /// Stands in for the random tail of a freshly minted uuid v7.
    ///
    /// A real admitter mints one observation id per row, so two rows never
    /// share one. Folding the row's identity into the seed reproduces that
    /// without giving up determinism, and keeps `entropy` a working override
    /// for tests that need two ids for one logical observation.
    fn row_entropy(&self) -> [u8; 10] {
        let seed = digest_hex(
            format!(
                "{:?}|{}|{}|{}|{}|{}|{}|{}|{}",
                self.entropy,
                self.provider_id,
                self.provider_instance_id,
                self.registration_revision,
                self.source_authority.as_wire(),
                self.source_stream,
                self.source_sequence,
                self.source_event_id,
                self.source_event_revision,
            )
            .as_bytes(),
        );
        let bytes = seed.as_bytes();
        let mut tail = [0_u8; 10];
        for (slot, byte) in tail.iter_mut().zip(bytes) {
            *slot = *byte;
        }
        tail
    }

    pub fn build(&self) -> Result<AdmittedObservationV1, Box<dyn Error>> {
        let payload = payload(&self.body)?;
        let extensions_digest = extensions_digest(&self.extensions)?;
        let source = CanonicalSettlementReceiptV1 {
            source_authority: self.source_authority,
            commit_point_id: "session_observation_store.commit".to_owned(),
            source_event_id: self.source_event_id.clone(),
            source_event_revision: self.source_event_revision,
            source_event_sha256: digest_hex(self.source_event_id.as_bytes()),
            source_stream: SourceStreamIdV1::new(self.source_stream.clone())?,
            source_sequence: SourceSequenceV1(self.source_sequence),
            settled_at_unix_micros: self.admitted_at - SECOND,
            settlement_proof_sha256: SETTLEMENT_DIGEST.to_owned(),
        };
        let target = ProviderTargetV1 {
            provider_id: OwnedProviderId::new(self.provider_id.clone())?,
            provider_instance_id: self.provider_instance_id.clone(),
            registration_revision: self.registration_revision,
            ready_receipt_digest: READY_DIGEST.to_owned(),
        };
        let privacy = ObservationPrivacyV1 {
            classification: self.classification,
            retention_class: self.retention_class,
            redaction_revision: 3,
            content_policy_revision: 2,
            forget_source_key: ForgetSourceKeyV1::new(self.forget_source_key.clone())?,
            expires_at_unix_micros: self.expires_at,
        };
        let mut admitted = AdmittedObservationV1 {
            observation_id: ObservationIdV1::from_v7_parts(
                u64::try_from(self.admitted_at / 1000)? & 0x0000_ffff_ffff_ffff,
                self.row_entropy(),
            )?,
            idempotency_key: ObservationIdempotencyKeyV1::parse(READY_DIGEST)?,
            target,
            exact_scope: scope()?,
            source,
            observation_kind: OwnedVersionedId::new(self.observation_kind.clone())?,
            payload,
            extensions: self.extensions.clone(),
            extensions_digest,
            provenance_origin: ProvenanceOriginV1::Agent,
            provenance_sha256: PROVENANCE_DIGEST.to_owned(),
            privacy,
            sanitization: match self.sanitization.clone() {
                Some(binding) => binding,
                None => binding_for_extensions(&self.body, &self.extensions)?,
            },
            occurred_at_unix_micros: self.admitted_at - 2 * SECOND,
            admitted_at_unix_micros: self.admitted_at,
            deadline_unix_micros: self.deadline,
            request_id: format!("request-{}", self.source_sequence),
            envelope_sha256: READY_DIGEST.to_owned(),
        };
        seal(&mut admitted);
        admitted.validate()?;
        Ok(admitted)
    }
}

/// Recomputes the derived key and envelope digest after a test mutates fields.
pub fn seal(admitted: &mut AdmittedObservationV1) {
    admitted.idempotency_key = admitted.derive_idempotency_key();
    admitted.envelope_sha256 = admitted.expected_envelope_sha256();
}

pub fn stream_key(sequence_stream: &str) -> Result<SourceStreamKeyV1, Box<dyn Error>> {
    Ok(SourceStreamKeyV1 {
        source_authority: SourceAuthorityV1::HostSession,
        exact_scope_sha256: scope()?.exact_scope_sha256(),
        source_stream: SourceStreamIdV1::new(sequence_stream.to_owned())?,
    })
}

pub fn journal(path: &std::path::Path) -> Result<SqliteObservationJournal, Box<dyn Error>> {
    Ok(SqliteObservationJournal::open(path, policy())?)
}

pub fn lease_request(now: i64, max_items: u32) -> LeaseRequestV1 {
    lease_request_for(INSTANCE, now, max_items)
}

/// A lease request from a named provider instance of the same registration.
pub fn lease_request_for(instance: &str, now: i64, max_items: u32) -> LeaseRequestV1 {
    LeaseRequestV1 {
        provider_id: PROVIDER.to_owned(),
        registration_revision: 4,
        provider_instance_id: instance.to_owned(),
        exact_scope_sha256: None,
        lease_owner: OWNER.to_owned(),
        now_unix_micros: now,
        lease_duration_micros: LEASE,
        max_items,
        max_bytes: 1_048_576,
    }
}

/// Builds the receipt a dispatcher would record for one leased attempt.
pub fn receipt_for(
    leased: &LeasedObservationV1,
    outcome: ObservationOutcomeV1,
    committed_effect: ObservationCommittedEffectV1,
    at: i64,
) -> ObservationDeliveryReceiptV1 {
    ObservationDeliveryReceiptV1 {
        receipt_id: DeliveryReceiptIdV1::derive(&leased.observation_id, leased.attempt_number),
        observation_id: leased.observation_id.clone(),
        idempotency_key: leased.idempotency_key.clone(),
        payload_sha256: leased.payload.sha256.clone(),
        extensions_digest: leased.extensions_digest.clone(),
        provider_id: leased.target.provider_id.clone(),
        provider_instance_id: Some(leased.target.provider_instance_id.clone()),
        registration_revision: leased.target.registration_revision,
        state_generation_before: Some(1),
        state_generation_after: Some(2),
        attempt_number: leased.attempt_number,
        outcome,
        committed_effect,
        provider_effect_summary: ProviderEffectSummaryV1 {
            effect_count: u32::from(committed_effect != ObservationCommittedEffectV1::None),
            stable_memory_refs: Vec::new(),
            provider_trace_refs: Vec::new(),
            no_effect_reason: None,
        },
        provider_receipt_digest: outcome
            .requires_provider_receipt()
            .then(|| PROVIDER_RECEIPT_DIGEST.to_owned()),
        started_at_unix_micros: at,
        finished_at_unix_micros: at + 1_000,
        warnings: Vec::new(),
    }
}

pub fn applied_receipt(leased: &LeasedObservationV1, at: i64) -> ObservationDeliveryReceiptV1 {
    receipt_for(
        leased,
        ObservationOutcomeV1::Applied,
        ObservationCommittedEffectV1::Applied,
        at,
    )
}

pub fn unavailable_receipt(leased: &LeasedObservationV1, at: i64) -> ObservationDeliveryReceiptV1 {
    receipt_for(
        leased,
        ObservationOutcomeV1::ProviderUnavailable,
        ObservationCommittedEffectV1::None,
        at,
    )
}

/// Backpressure thresholds that never shed on their own, so a test that wants
/// a shed has to create real pressure rather than inherit it from the fixture.
///
/// The queue ceiling itself is still enforced by the gate — that bound belongs
/// to the journal's retention policy, not to these thresholds.
pub fn backpressure_policy() -> BackpressurePolicyV1 {
    BackpressurePolicyV1 {
        shed_optional_at_ppm: 900_000,
        refuse_at_ppm: 1_000_000,
        max_backlog_age_micros: 365 * DAY,
        foreground_budget_micros: HOUR,
        foreground_breach_streak: 3,
    }
}

pub fn gate() -> Result<BackpressureGateV1, Box<dyn Error>> {
    Ok(BackpressureGateV1::new(backpressure_policy())?)
}

/// A gate under caller-chosen thresholds.
pub fn gate_with(policy: BackpressurePolicyV1) -> Result<BackpressureGateV1, Box<dyn Error>> {
    Ok(BackpressureGateV1::new(policy)?)
}

/// The provider lane the fixture target addresses.
pub fn lane() -> Result<ObservationLaneKeyV1, Box<dyn Error>> {
    Ok(ObservationLaneKeyV1::of(&target()?))
}

/// A caller-owned ingest bound with an explicit clock, deadline, and
/// cancellation flag.
///
/// Nothing here defaults: a test says what instant the lane is measured on,
/// how much budget the call has, and whether the caller has given up. All
/// three are interior-mutable so a test can move the clock or cancel *while*
/// an ingest is in flight, which is the only way to prove the bound reaches
/// inside a record rather than only between them.
#[derive(Debug)]
pub struct TestIngestControl {
    now: AtomicI64,
    deadline: AtomicI64,
    cancelled: AtomicBool,
}

impl TestIngestControl {
    /// A control whose clock reads `now` and whose deadline is `budget` later.
    #[must_use]
    pub fn at(now: i64, budget: i64) -> Self {
        Self {
            now: AtomicI64::new(now),
            deadline: AtomicI64::new(now.saturating_add(budget)),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Moves the caller's clock.
    pub fn set_now(&self, now: i64) {
        self.now.store(now, Ordering::Relaxed);
    }

    /// Fires the caller's cancellation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

impl IngressControlV1 for TestIngestControl {
    fn now_unix_micros(&self) -> i64 {
        self.now.load(Ordering::Relaxed)
    }

    fn deadline_unix_micros(&self) -> i64 {
        self.deadline.load(Ordering::Relaxed)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

/// The bound most tests run under: the fixture instant with a day of budget,
/// so nothing stops on a deadline the test did not ask for.
#[must_use]
pub fn ingest_control() -> TestIngestControl {
    TestIngestControl::at(T0, DAY)
}
