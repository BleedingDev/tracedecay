//! Host admission authority for provider recall replies.
//!
//! A provider answers a recall call with a canonical
//! `tracedecay.memory.provider.recall.v1` outcome. Nothing in that outcome is
//! trusted on its own: every candidate re-asserts the exact coding scope and a
//! validity record, and this module is the single place where those provider
//! claims are compared against the scope and temporal query the host itself
//! admitted when it built the [`ProviderCall`].
//!
//! Admission is **rank-final**: it runs after the provider's own ordering and
//! before any normalization, deduplication, or context packing. Later stages
//! consume only [`RecallAdmission::admitted`]; a denied candidate survives only
//! as a [`DeniedRecallCandidate`] ledger row that carries identity, reason, and
//! the provider's claimed scope digest — never content — so it is structurally
//! unable to re-enter a prompt while remaining fully audit-visible.
//!
//! Provider assertions cannot widen scope or validity. Every candidate names
//! the [`ScopeBinding`] it attests (`exact_coding_scope`, `project_facts`, or
//! `profile_facts`, mirroring the authority-matrix namespaces); the host
//! admits a binding only when the registry recorded it as authorized for the
//! provider at registration ([`RecallScopeBindingsV1`], carried with the
//! admitted call and never read from a reply), and then applies that
//! binding's required / optional / forbidden identity-field rules against the
//! admitted scope byte-for-byte. A `temporal_state` the provider claims must
//! agree with the state the host computes from the validity timestamps, and
//! unknown validity is governed by the host's admitted
//! [`UnknownValidityPolicy`], never by the provider.

use std::collections::BTreeSet;

use chrono::{DateTime, SecondsFormat};
use serde::de::{Deserializer, Error as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracedecay_memory_provider_api::contract::{TemporalMode, TerminalCode};

use crate::recall_normalization::{
    NativeScoreDefect, NativeScoreV1, ValidatedNativeScoreV1, validate_native_score,
};
use tracedecay_memory_provider_api::{
    ApiError, CanonicalPayload, OwnedExactScope, OwnedProviderId, OwnedVersionedId, ProviderCall,
    ProviderReply,
};

/// Canonical payload contract identity of recall requests and outcomes.
pub const RECALL_PAYLOAD_CONTRACT_ID: &str = "tracedecay.memory.provider.recall.v1";

/// Capability every recall call requires.
pub const RECALL_QUERY_CAPABILITY_ID: &str = "recall.query.v1";

/// Maximum bytes of a decode diagnostic retained in a typed error.
const MAX_DECODE_DETAIL_BYTES: usize = 512;

/// Host policy for candidates whose validity the provider could not establish.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownValidityPolicy {
    /// Deny the candidate and record it in the ledger.
    Exclude,
    /// Admit the candidate but mark the whole recall lane degraded.
    Degrade,
    /// Admit the candidate with an explicit per-candidate warning.
    AllowWithWarning,
}

impl UnknownValidityPolicy {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Exclude => "exclude",
            Self::Degrade => "degrade",
            Self::AllowWithWarning => "allow_with_warning",
        }
    }
}

/// Closed set of temporal states a validity record may be in.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalState {
    /// Valid at the evaluation instant.
    Current,
    /// `valid_from` lies after the evaluation instant.
    Future,
    /// `valid_until` lies at or before the evaluation instant.
    Expired,
    /// A later record supersedes this one.
    Superseded,
    /// The record was revoked.
    Revoked,
    /// The provider could not establish validity.
    Unknown,
}

impl TemporalState {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Future => "future",
            Self::Expired => "expired",
            Self::Superseded => "superseded",
            Self::Revoked => "revoked",
            Self::Unknown => "unknown",
        }
    }

    fn from_wire(value: &str) -> Option<Self> {
        match value {
            "current" => Some(Self::Current),
            "future" => Some(Self::Future),
            "expired" => Some(Self::Expired),
            "superseded" => Some(Self::Superseded),
            "revoked" => Some(Self::Revoked),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

/// One exact-scope identity field, named for denial ledgers.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeField {
    /// `profile_id`.
    ProfileId,
    /// `project_id`.
    ProjectId,
    /// `repository_identity`.
    RepositoryIdentity,
    /// `worktree_identity`.
    WorktreeIdentity,
    /// `branch_identity`.
    BranchIdentity,
    /// `agent_session_id`.
    AgentSessionId,
    /// `resolved_scope_digest`.
    ResolvedScopeDigest,
}

/// Identity namespace one candidate attests, mirroring the namespace variants
/// of the accepted coding-memory authority matrix. Wire values are those of
/// `tracedecay.memory.provider.recall.v1` `candidate_scope_binding.bindings`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeBinding {
    /// All seven identity fields bind byte-for-byte to the admitted scope.
    ExactCodingScope,
    /// A project-owned fact: profile and project bind; repository, worktree,
    /// and branch are optional; session and resolved-scope digest are
    /// forbidden.
    ProjectFacts,
    /// A profile-owned fact: only the profile binds; every other field is
    /// forbidden.
    ProfileFacts,
}

impl ScopeBinding {
    /// Returns the canonical wire value.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::ExactCodingScope => "exact_coding_scope",
            Self::ProjectFacts => "project_facts",
            Self::ProfileFacts => "profile_facts",
        }
    }

    /// Decodes one canonical wire value.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "exact_coding_scope" => Some(Self::ExactCodingScope),
            "project_facts" => Some(Self::ProjectFacts),
            "profile_facts" => Some(Self::ProfileFacts),
            _ => None,
        }
    }

    /// The rule this binding applies to one identity field, per the
    /// contract's `candidate_scope_binding.binding_rules`.
    const fn field_rule(self, field: ScopeField) -> ScopeFieldRule {
        match self {
            Self::ExactCodingScope => ScopeFieldRule::RequiredEqual,
            Self::ProjectFacts => match field {
                ScopeField::ProfileId | ScopeField::ProjectId => ScopeFieldRule::RequiredEqual,
                ScopeField::RepositoryIdentity
                | ScopeField::WorktreeIdentity
                | ScopeField::BranchIdentity => ScopeFieldRule::OptionalEmptyOrEqual,
                ScopeField::AgentSessionId | ScopeField::ResolvedScopeDigest => {
                    ScopeFieldRule::Forbidden
                }
            },
            Self::ProfileFacts => match field {
                ScopeField::ProfileId => ScopeFieldRule::RequiredEqual,
                _ => ScopeFieldRule::Forbidden,
            },
        }
    }
}

/// How one binding treats one identity field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScopeFieldRule {
    /// Must be non-empty and byte-equal to the admitted scope.
    RequiredEqual,
    /// Either empty or byte-equal to the admitted scope.
    OptionalEmptyOrEqual,
    /// Must be empty.
    Forbidden,
}

/// Scope bindings the registry authorized one provider to attest, recorded
/// at registration and handed to admission with the admitted call.
///
/// This is host-owned data: a provider reply can neither declare nor widen
/// it. An empty set authorizes nothing, so every candidate is denied.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RecallScopeBindingsV1(BTreeSet<ScopeBinding>);

impl RecallScopeBindingsV1 {
    /// Builds the authorized set from typed bindings.
    pub fn new(bindings: impl IntoIterator<Item = ScopeBinding>) -> Self {
        Self(bindings.into_iter().collect())
    }

    /// Builds the authorized set from contract wire values, refusing any
    /// value outside the closed contract vocabulary.
    pub fn from_wire<'a>(
        values: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, RecallAdmissionError> {
        let mut bindings = BTreeSet::new();
        for value in values {
            let binding = ScopeBinding::from_wire(value).ok_or_else(|| {
                RecallAdmissionError::ScopeBindingUnknown {
                    value: bounded_detail(value),
                }
            })?;
            bindings.insert(binding);
        }
        Ok(Self(bindings))
    }

    /// Returns whether `binding` is authorized.
    #[must_use]
    pub fn authorizes(&self, binding: ScopeBinding) -> bool {
        self.0.contains(&binding)
    }

    /// Iterates the authorized bindings in canonical order.
    pub fn iter(&self) -> impl Iterator<Item = ScopeBinding> + '_ {
        self.0.iter().copied()
    }

    /// Returns whether nothing is authorized.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Typed reason one candidate was refused admission.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecallDenialReason {
    /// The candidate attests a scope binding the registry did not authorize
    /// for its provider.
    ScopeBindingUnauthorized {
        /// The binding the provider claimed.
        binding: ScopeBinding,
    },
    /// An identity field differs from the admitted scope.
    ScopeMismatch {
        /// First differing field in contract order.
        field: ScopeField,
    },
    /// Every identity field matches but the resolved scope digest does not:
    /// the candidate belongs to an earlier resolution of this checkout.
    StaleIdentity,
    /// The candidate carries an identity the host cannot resolve at all.
    UnknownIdentity {
        /// Empty or malformed field.
        field: ScopeField,
    },
    /// The candidate carries an identity its scope binding forbids.
    ForbiddenIdentity {
        /// First non-empty forbidden field in contract order.
        field: ScopeField,
    },
    /// `valid_from` lies after the evaluation window.
    NotYetValid,
    /// `valid_until` lies at or before the evaluation window.
    Expired,
    /// The record is revoked and the query did not include revoked records.
    Revoked,
    /// The record is superseded and the query did not include superseded
    /// records.
    Superseded,
    /// Validity is unknown and the admitted policy excludes such records.
    UnknownValidity,
    /// The validity record is internally inconsistent or contradicts the
    /// provider's own claimed temporal state.
    InvalidValidityRecord {
        /// Bounded, content-free description of the inconsistency.
        detail: String,
    },
    /// Inline content does not hash to the declared `content_sha256`.
    ContentDigestMismatch,
    /// The candidate carries neither or both of `content` / `content_ref`.
    ContentSelectionInvalid,
    /// The provider-native score is absent, malformed, non-finite, or
    /// contradicts the range the provider itself declared, so no honest
    /// relevance can be established for the candidate.
    NativeScoreMalformed {
        /// The first defect found in contract field order.
        defect: NativeScoreDefect,
    },
}

impl RecallDenialReason {
    /// Stable snake_case label for metrics and log fields.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::ScopeBindingUnauthorized { .. } => "scope_binding_unauthorized",
            Self::ScopeMismatch { .. } => "scope_mismatch",
            Self::StaleIdentity => "stale_identity",
            Self::UnknownIdentity { .. } => "unknown_identity",
            Self::ForbiddenIdentity { .. } => "forbidden_identity",
            Self::NotYetValid => "not_yet_valid",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::Superseded => "superseded",
            Self::UnknownValidity => "unknown_validity",
            Self::InvalidValidityRecord { .. } => "invalid_validity_record",
            Self::ContentDigestMismatch => "content_digest_mismatch",
            Self::ContentSelectionInvalid => "content_selection_invalid",
            Self::NativeScoreMalformed { .. } => "native_score_malformed",
        }
    }
}

/// Failure that prevents admission from producing any decision at all.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RecallAdmissionError {
    /// The reply terminal is not a success, zero-result, or partial terminal.
    #[error("recall terminal {} carries no admissible outcome", .terminal_code.as_wire())]
    TerminalNotSuccessful {
        /// The reply terminal code.
        terminal_code: TerminalCode,
    },
    /// A successful terminal arrived without the mandatory outcome payload.
    #[error("recall reply terminal succeeded without an outcome payload")]
    MissingPayload,
    /// The payload names a contract other than the recall outcome contract.
    #[error("recall payload contract {contract_id} is not {RECALL_PAYLOAD_CONTRACT_ID}")]
    PayloadContractMismatch {
        /// The declared contract identity.
        contract_id: String,
    },
    /// The payload bytes are not a canonical recall outcome.
    #[error("recall outcome payload could not be decoded: {detail}")]
    PayloadDecode {
        /// Bounded decoder diagnostic.
        detail: String,
    },
    /// The outcome envelope names a different call than the one dispatched.
    #[error("recall outcome {field} does not match the dispatched call")]
    OutcomeBinding {
        /// The mismatching envelope field.
        field: &'static str,
    },
    /// The provider returned more candidates than the admitted budget.
    #[error("recall outcome returned {returned} candidates over the admitted budget {maximum}")]
    CandidateBudgetExceeded {
        /// Candidates returned.
        returned: usize,
        /// Admitted maximum.
        maximum: usize,
    },
    /// Two candidates share one request-scoped identity.
    #[error("recall outcome repeats candidate id {0}")]
    DuplicateCandidateId(String),
    /// The admitted temporal query is malformed.
    #[error("recall temporal query {field} is invalid: {detail}")]
    InvalidTemporalQuery {
        /// The offending field.
        field: &'static str,
        /// Bounded diagnostic.
        detail: &'static str,
    },
    /// A recall request part failed API validation.
    #[error("recall request part invalid: {0}")]
    Api(#[from] ApiError),
    /// A registry-declared scope binding is outside the contract vocabulary.
    #[error("recall scope binding {value} is not a contract value")]
    ScopeBindingUnknown {
        /// The offending wire value, bounded.
        value: String,
    },
    /// The request could not be encoded.
    #[error("recall request could not be encoded: {detail}")]
    RequestEncode {
        /// Bounded encoder diagnostic.
        detail: String,
    },
}

/// Parses one `utc_rfc3339_nanos` timestamp into UTC nanoseconds since epoch.
///
/// This is the only instant parser the admission authority uses; hosts that
/// project their own instants onto the wire compare against it rather than
/// against a second parser.
#[must_use]
pub fn parse_rfc3339_nanos(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .ok()?
        .timestamp_nanos_opt()
}

/// Formats UTC microseconds since epoch as an RFC 3339 UTC timestamp with
/// microsecond precision, the representation recall requests carry.
#[must_use]
pub fn rfc3339_utc_micros(micros: i64) -> Option<String> {
    DateTime::from_timestamp_micros(micros)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// Temporal query the host admitted for one recall.
///
/// Every instant is retained both as the canonical wire string (so the
/// request payload carries exactly what was admitted) and as parsed UTC
/// nanoseconds (so admission arithmetic never re-parses provider input).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedTemporalQuery {
    mode: TemporalMode,
    evaluation_time: String,
    evaluation_nanos: i64,
    as_of: Option<(String, i64)>,
    interval: Option<((String, i64), (String, i64))>,
    include_superseded: bool,
    include_revoked: bool,
    unknown_validity_policy: UnknownValidityPolicy,
}

impl AdmittedTemporalQuery {
    /// Builds a `current` query evaluated at `evaluation_time`.
    pub fn current(evaluation_time: &str) -> Result<Self, RecallAdmissionError> {
        let evaluation_nanos = parse_instant(evaluation_time, "evaluation_time")?;
        Ok(Self {
            mode: TemporalMode::Current,
            evaluation_time: evaluation_time.to_owned(),
            evaluation_nanos,
            as_of: None,
            interval: None,
            include_superseded: false,
            include_revoked: false,
            unknown_validity_policy: UnknownValidityPolicy::Exclude,
        })
    }

    /// Builds an `as_of` query evaluated at `as_of`.
    pub fn as_of(evaluation_time: &str, as_of: &str) -> Result<Self, RecallAdmissionError> {
        let mut query = Self::current(evaluation_time)?;
        let as_of_nanos = parse_instant(as_of, "as_of")?;
        if as_of_nanos > query.evaluation_nanos {
            return Err(RecallAdmissionError::InvalidTemporalQuery {
                field: "as_of",
                detail: "as_of lies after evaluation_time",
            });
        }
        query.mode = TemporalMode::AsOf;
        query.as_of = Some((as_of.to_owned(), as_of_nanos));
        Ok(query)
    }

    /// Builds an `interval` query over `[interval_start, interval_end)`.
    pub fn interval(
        evaluation_time: &str,
        interval_start: &str,
        interval_end: &str,
    ) -> Result<Self, RecallAdmissionError> {
        let mut query = Self::current(evaluation_time)?;
        let start = parse_instant(interval_start, "interval_start")?;
        let end = parse_instant(interval_end, "interval_end")?;
        if start >= end {
            return Err(RecallAdmissionError::InvalidTemporalQuery {
                field: "interval_end",
                detail: "interval_end must lie after interval_start",
            });
        }
        query.mode = TemporalMode::Interval;
        query.interval = Some((
            (interval_start.to_owned(), start),
            (interval_end.to_owned(), end),
        ));
        Ok(query)
    }

    /// Builds a `history` query that retains historical records with their
    /// validity metadata.
    pub fn history(evaluation_time: &str) -> Result<Self, RecallAdmissionError> {
        let mut query = Self::current(evaluation_time)?;
        query.mode = TemporalMode::History;
        Ok(query)
    }

    /// Admits superseded records instead of denying them.
    #[must_use]
    pub const fn with_include_superseded(mut self, include: bool) -> Self {
        self.include_superseded = include;
        self
    }

    /// Admits revoked records instead of denying them.
    #[must_use]
    pub const fn with_include_revoked(mut self, include: bool) -> Self {
        self.include_revoked = include;
        self
    }

    /// Sets the policy for records with unknown validity.
    #[must_use]
    pub const fn with_unknown_validity_policy(mut self, policy: UnknownValidityPolicy) -> Self {
        self.unknown_validity_policy = policy;
        self
    }

    /// Returns the temporal mode.
    #[must_use]
    pub const fn mode(&self) -> TemporalMode {
        self.mode
    }

    /// Returns the admitted evaluation instant as the canonical wire string.
    #[must_use]
    pub fn evaluation_time(&self) -> &str {
        &self.evaluation_time
    }

    /// Returns the admitted evaluation instant in UTC nanoseconds.
    #[must_use]
    pub const fn evaluation_nanos(&self) -> i64 {
        self.evaluation_nanos
    }

    /// Returns the unknown-validity policy.
    #[must_use]
    pub const fn unknown_validity_policy(&self) -> UnknownValidityPolicy {
        self.unknown_validity_policy
    }

    /// Returns whether the query admits revoked records.
    #[must_use]
    pub const fn include_revoked(&self) -> bool {
        self.include_revoked
    }

    /// Returns whether the query admits superseded records.
    #[must_use]
    pub const fn include_superseded(&self) -> bool {
        self.include_superseded
    }

    /// Returns the canonical `temporal_query` wire object.
    #[must_use]
    pub fn to_wire_value(&self) -> Value {
        let (interval_start, interval_end) = self
            .interval
            .as_ref()
            .map_or((Value::Null, Value::Null), |((start, _), (end, _))| {
                (Value::String(start.clone()), Value::String(end.clone()))
            });
        serde_json::json!({
            "mode": self.mode.as_wire(),
            "evaluation_time": self.evaluation_time,
            "as_of": self.as_of.as_ref().map_or(Value::Null, |(value, _)| Value::String(value.clone())),
            "interval_start": interval_start,
            "interval_end": interval_end,
            "include_superseded": self.include_superseded,
            "include_revoked": self.include_revoked,
            "unknown_validity_policy": self.unknown_validity_policy.as_wire(),
        })
    }

    /// The single instant a point query evaluates validity at.
    fn point_instant(&self) -> Option<i64> {
        match self.mode {
            TemporalMode::Current => Some(self.evaluation_nanos),
            TemporalMode::AsOf => self.as_of.as_ref().map(|(_, nanos)| *nanos),
            TemporalMode::Interval | TemporalMode::History => None,
        }
    }
}

fn parse_instant(value: &str, field: &'static str) -> Result<i64, RecallAdmissionError> {
    parse_rfc3339_nanos(value).ok_or(RecallAdmissionError::InvalidTemporalQuery {
        field,
        detail: "not a utc_rfc3339_nanos timestamp",
    })
}

/// Positive, finite recall budgets the host admits for one request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallBudgetsV1 {
    /// Maximum candidates the provider may return.
    pub maximum_candidates: u64,
    /// Maximum inline content bytes per candidate.
    pub maximum_candidate_content_bytes: u64,
    /// Maximum inline content bytes across all candidates.
    pub maximum_total_content_bytes: u64,
    /// Maximum source references per candidate.
    pub maximum_source_refs_per_candidate: u64,
    /// Maximum trace references per candidate.
    pub maximum_trace_refs_per_candidate: u64,
    /// Maximum warnings in the outcome.
    pub maximum_warnings: u64,
    /// Maximum opaque extensions per candidate.
    pub maximum_extensions_per_candidate: u64,
}

impl RecallBudgetsV1 {
    /// Rejects zero budgets, which the contract forbids.
    pub fn validate(&self) -> Result<(), RecallAdmissionError> {
        for (field, value) in [
            ("maximum_candidates", self.maximum_candidates),
            (
                "maximum_candidate_content_bytes",
                self.maximum_candidate_content_bytes,
            ),
            (
                "maximum_total_content_bytes",
                self.maximum_total_content_bytes,
            ),
            (
                "maximum_source_refs_per_candidate",
                self.maximum_source_refs_per_candidate,
            ),
            (
                "maximum_trace_refs_per_candidate",
                self.maximum_trace_refs_per_candidate,
            ),
            ("maximum_warnings", self.maximum_warnings),
            (
                "maximum_extensions_per_candidate",
                self.maximum_extensions_per_candidate,
            ),
        ] {
            if value == 0 {
                return Err(RecallAdmissionError::InvalidTemporalQuery {
                    field,
                    detail: "recall budgets must be positive",
                });
            }
        }
        Ok(())
    }
}

/// Everything the host binds into one canonical recall request payload.
#[derive(Clone, Debug)]
pub struct RecallRequestParts {
    /// Target provider identity.
    pub provider_id: OwnedProviderId,
    /// Accepted registration revision.
    pub registration_revision: u64,
    /// Fabric-validated ready-receipt digest.
    pub ready_receipt_sha256: String,
    /// Exact coding scope the host admitted.
    pub exact_scope: OwnedExactScope,
    /// Stable request identity.
    pub request_id: String,
    /// Bounded objective text.
    pub objective: String,
    /// Bounded query text.
    pub query: String,
    /// Admitted temporal query.
    pub temporal: AdmittedTemporalQuery,
    /// Admitted budgets.
    pub budgets: RecallBudgetsV1,
    /// Pinned policy revision.
    pub policy_revision: u64,
    /// Absolute UTC deadline in microseconds.
    pub deadline_utc_micros: i64,
    /// Remaining budget in milliseconds at dispatch.
    pub remaining_millis: u64,
}

/// Builds the canonical recall request payload bound to the admitted parts.
pub fn build_recall_request_payload(
    parts: &RecallRequestParts,
) -> Result<CanonicalPayload, RecallAdmissionError> {
    parts.exact_scope.validate()?;
    parts.budgets.validate()?;
    if parts.policy_revision == 0 {
        return Err(RecallAdmissionError::InvalidTemporalQuery {
            field: "policy_revision",
            detail: "policy revision must be positive",
        });
    }
    let value = serde_json::json!({
        "provider_id": parts.provider_id.as_str(),
        "registration_revision": parts.registration_revision,
        "ready_receipt_digest": parts.ready_receipt_sha256,
        "exact_scope_identity": scope_wire_value(&parts.exact_scope),
        "request_identity": parts.request_id,
        "objective": parts.objective,
        "query": parts.query,
        "temporal_query": parts.temporal.to_wire_value(),
        "budgets": parts.budgets,
        "exclusions": {
            "stable_memory_refs": [],
            "candidate_ids": [],
            "source_refs": [],
            "trace_refs": [],
            "observation_ids": [],
            "content_sha256": [],
        },
        "required_capabilities": [RECALL_QUERY_CAPABILITY_ID],
        "policy_revision": parts.policy_revision,
        "extensions": [],
        "deadline": {
            "deadline_utc_micros": parts.deadline_utc_micros,
            "remaining_millis": parts.remaining_millis,
        },
        "cancellation": "live",
    });
    let bytes =
        serde_json::to_vec(&value).map_err(|error| RecallAdmissionError::RequestEncode {
            detail: bounded_detail(&error.to_string()),
        })?;
    let sha256 = hex::encode(Sha256::digest(&bytes));
    Ok(CanonicalPayload::new(
        OwnedVersionedId::new(RECALL_PAYLOAD_CONTRACT_ID)?,
        bytes,
        sha256,
    )?)
}

fn scope_wire_value(scope: &OwnedExactScope) -> Value {
    serde_json::json!({
        "profile_id": scope.profile_id,
        "project_id": scope.project_id,
        "repository_identity": scope.repository_identity,
        "worktree_identity": scope.worktree_identity,
        "branch_identity": scope.branch_identity,
        "agent_session_id": scope.agent_session_id,
        "resolved_scope_digest": scope.resolved_scope_digest,
    })
}

/// Requires a nullable field to be present on the wire; `null` decodes to
/// `None`, a missing key is a contract violation.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Exact-scope identity as a provider asserts it on one candidate, together
/// with the explicit binding that says which fields the provider attests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallScopeIdentityV1 {
    /// Which identity namespace the provider attests for this candidate.
    pub scope_binding: ScopeBinding,
    /// Claimed profile identity.
    pub profile_id: String,
    /// Claimed project identity.
    pub project_id: String,
    /// Claimed repository identity.
    pub repository_identity: String,
    /// Claimed worktree identity.
    pub worktree_identity: String,
    /// Claimed branch identity.
    pub branch_identity: String,
    /// Claimed agent session identity.
    pub agent_session_id: String,
    /// Claimed resolved scope digest.
    pub resolved_scope_digest: String,
}

impl RecallScopeIdentityV1 {
    /// Digest of the claimed scope, when the claim is well-formed enough to
    /// digest. A malformed claim yields `None` rather than a fabricated digest.
    #[must_use]
    pub fn claimed_scope_sha256(&self) -> Option<String> {
        OwnedExactScope::new(
            self.profile_id.as_str(),
            self.project_id.as_str(),
            self.repository_identity.as_str(),
            self.worktree_identity.as_str(),
            self.branch_identity.as_str(),
            self.agent_session_id.as_str(),
            self.resolved_scope_digest.as_str(),
        )
        .ok()
        .map(|scope| scope.exact_scope_sha256())
    }

    fn fields(&self) -> [(ScopeField, &str); 7] {
        [
            (ScopeField::ProfileId, self.profile_id.as_str()),
            (ScopeField::ProjectId, self.project_id.as_str()),
            (
                ScopeField::RepositoryIdentity,
                self.repository_identity.as_str(),
            ),
            (
                ScopeField::WorktreeIdentity,
                self.worktree_identity.as_str(),
            ),
            (ScopeField::BranchIdentity, self.branch_identity.as_str()),
            (ScopeField::AgentSessionId, self.agent_session_id.as_str()),
            (
                ScopeField::ResolvedScopeDigest,
                self.resolved_scope_digest.as_str(),
            ),
        ]
    }
}

/// Scope the provider searched, re-asserted on the outcome envelope. This is
/// a binding to the request the provider answered, never an attestation
/// about any candidate, so it carries no `scope_binding`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallOutcomeScopeV1 {
    /// Searched profile identity.
    pub profile_id: String,
    /// Searched project identity.
    pub project_id: String,
    /// Searched repository identity.
    pub repository_identity: String,
    /// Searched worktree identity.
    pub worktree_identity: String,
    /// Searched branch identity.
    pub branch_identity: String,
    /// Searched agent session identity.
    pub agent_session_id: String,
    /// Searched resolved scope digest.
    pub resolved_scope_digest: String,
}

impl RecallOutcomeScopeV1 {
    fn fields(&self) -> [(ScopeField, &str); 7] {
        [
            (ScopeField::ProfileId, self.profile_id.as_str()),
            (ScopeField::ProjectId, self.project_id.as_str()),
            (
                ScopeField::RepositoryIdentity,
                self.repository_identity.as_str(),
            ),
            (
                ScopeField::WorktreeIdentity,
                self.worktree_identity.as_str(),
            ),
            (ScopeField::BranchIdentity, self.branch_identity.as_str()),
            (ScopeField::AgentSessionId, self.agent_session_id.as_str()),
            (
                ScopeField::ResolvedScopeDigest,
                self.resolved_scope_digest.as_str(),
            ),
        ]
    }
}

fn admitted_scope_fields(scope: &OwnedExactScope) -> [(ScopeField, &str); 7] {
    [
        (ScopeField::ProfileId, scope.profile_id.as_str()),
        (ScopeField::ProjectId, scope.project_id.as_str()),
        (
            ScopeField::RepositoryIdentity,
            scope.repository_identity.as_str(),
        ),
        (
            ScopeField::WorktreeIdentity,
            scope.worktree_identity.as_str(),
        ),
        (ScopeField::BranchIdentity, scope.branch_identity.as_str()),
        (ScopeField::AgentSessionId, scope.agent_session_id.as_str()),
        (
            ScopeField::ResolvedScopeDigest,
            scope.resolved_scope_digest.as_str(),
        ),
    ]
}

/// Validity record as a provider asserts it on one candidate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallValidityV1 {
    /// When the provider observed the record.
    #[serde(deserialize_with = "required_nullable")]
    pub observed_at: Option<String>,
    /// Start of validity, inclusive.
    #[serde(deserialize_with = "required_nullable")]
    pub valid_from: Option<String>,
    /// End of validity, exclusive; `null` is open-ended.
    #[serde(deserialize_with = "required_nullable")]
    pub valid_until: Option<String>,
    /// When a later record superseded this one.
    #[serde(deserialize_with = "required_nullable")]
    pub superseded_at: Option<String>,
    /// Stable reference of the superseding record.
    #[serde(deserialize_with = "required_nullable")]
    pub superseded_by: Option<String>,
    /// When the record was revoked.
    #[serde(deserialize_with = "required_nullable")]
    pub revoked_at: Option<String>,
    /// Provider-local source revision.
    #[serde(deserialize_with = "required_nullable")]
    pub source_revision: Option<String>,
    /// Temporal state the provider claims.
    pub temporal_state: String,
}

/// One candidate exactly as the provider returned it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallCandidateV1 {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Optional stable provider memory reference.
    #[serde(deserialize_with = "required_nullable")]
    pub stable_memory_ref: Option<String>,
    /// Inline content, exclusive with `content_ref`.
    #[serde(deserialize_with = "required_nullable")]
    pub content: Option<String>,
    /// Content reference, exclusive with `content`.
    #[serde(deserialize_with = "required_nullable")]
    pub content_ref: Option<Value>,
    /// Canonical content digest.
    pub content_sha256: String,
    /// Provider-native score, retained opaque for host normalization.
    pub native_score: Value,
    /// Claimed exact scope.
    pub exact_scope_identity: RecallScopeIdentityV1,
    /// Claimed validity.
    pub validity: RecallValidityV1,
    /// Provenance record.
    pub provenance: Value,
    /// Explanation record.
    pub explanation: Value,
    /// Source references.
    pub source_refs: Vec<String>,
    /// Trace references.
    pub trace_refs: Vec<String>,
    /// Sensitivity label.
    pub sensitivity: String,
    /// Memory class.
    pub memory_class: Value,
    /// Provider warnings.
    pub warnings: Vec<String>,
    /// Opaque extensions.
    pub extensions: Vec<Value>,
}

/// Recall outcome envelope exactly as the provider returned it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallOutcomeV1 {
    /// Provider identity.
    pub provider_id: String,
    /// Provider runtime instance.
    pub provider_instance_id: String,
    /// Registration revision.
    pub registration_revision: u64,
    /// Ready-receipt digest.
    pub ready_receipt_digest: String,
    /// Request identity.
    pub request_identity: String,
    /// Scope the provider searched.
    pub exact_scope_identity: RecallOutcomeScopeV1,
    /// Provider state generation.
    pub provider_state_generation: u64,
    /// Candidates in provider order.
    pub candidates: Vec<RecallCandidateV1>,
    /// Coverage record.
    pub coverage: Value,
    /// Ordering record.
    pub ordering: Value,
    /// Terminal record.
    pub terminal: Value,
    /// Outcome warnings.
    pub warnings: Vec<String>,
}

/// Content carried by one admitted candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecallCandidateContent<'candidate> {
    /// Inline content whose digest was verified.
    Inline(&'candidate str),
    /// A content reference that still needs scope-revalidated hydration.
    Reference(&'candidate Value),
}

/// One candidate that passed admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AdmittedRecallCandidate {
    candidate: RecallCandidateV1,
    host_temporal_state: TemporalState,
    warnings: Vec<String>,
    native_score: ValidatedNativeScoreV1,
}

impl AdmittedRecallCandidate {
    /// Returns the candidate as the provider returned it.
    #[must_use]
    pub const fn candidate(&self) -> &RecallCandidateV1 {
        &self.candidate
    }

    /// Returns the temporal state the host computed for the candidate.
    #[must_use]
    pub const fn host_temporal_state(&self) -> TemporalState {
        self.host_temporal_state
    }

    /// Returns the scope binding the candidate was admitted under.
    #[must_use]
    pub const fn scope_binding(&self) -> ScopeBinding {
        self.candidate.exact_scope_identity.scope_binding
    }

    /// Returns host warnings attached at admission.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Returns the provider-native score exactly as declared. Admission
    /// established it is well formed; nothing here rewrites it.
    #[must_use]
    pub const fn native_score(&self) -> &NativeScoreV1 {
        self.native_score.score()
    }

    /// Returns the framed digest of the declared native score, which host
    /// normalization records as the input its value was derived from.
    #[must_use]
    pub fn native_score_sha256(&self) -> &str {
        self.native_score.native_score_sha256()
    }

    /// Returns the verified content selection.
    #[must_use]
    pub fn content(&self) -> RecallCandidateContent<'_> {
        match (&self.candidate.content, &self.candidate.content_ref) {
            (Some(content), _) => RecallCandidateContent::Inline(content),
            (None, Some(reference)) => RecallCandidateContent::Reference(reference),
            // Unreachable after admission; admission denies every candidate
            // that carries neither field. Reporting an empty reference keeps
            // the accessor total without inventing content.
            (None, None) => RecallCandidateContent::Reference(&Value::Null),
        }
    }

    /// Consumes the admitted wrapper.
    #[must_use]
    pub fn into_candidate(self) -> RecallCandidateV1 {
        self.candidate
    }
}

/// Audit row for one denied candidate. Carries no content by construction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeniedRecallCandidate {
    /// Request-scoped candidate identity.
    pub candidate_id: String,
    /// Optional stable provider memory reference.
    pub stable_memory_ref: Option<String>,
    /// Typed denial reason.
    pub reason: RecallDenialReason,
    /// Scope binding the provider claimed.
    pub provider_claimed_scope_binding: ScopeBinding,
    /// Digest of the scope the provider claimed, when digestible.
    pub provider_claimed_scope_sha256: Option<String>,
    /// Temporal state the provider claimed.
    pub provider_claimed_temporal_state: String,
}

/// Serialisable admission report for explain traces and audit logs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallAdmissionReport {
    /// Request identity the admission ran under.
    pub request_id: String,
    /// Digest of the admitted exact scope.
    pub exact_scope_sha256: String,
    /// Admitted temporal mode.
    pub temporal_mode: String,
    /// Admitted evaluation instant.
    pub evaluation_time: String,
    /// Admitted unknown-validity policy.
    pub unknown_validity_policy: UnknownValidityPolicy,
    /// Scope bindings the registry authorized the provider to attest.
    pub authorized_scope_bindings: RecallScopeBindingsV1,
    /// Candidates the provider returned.
    pub received_count: usize,
    /// Request-scoped identity of every candidate the provider returned, in
    /// the provider's own order.
    ///
    /// The denial ledger and the admitted slice are each in provider order,
    /// but neither alone recovers the interleaving of the two. This is the
    /// one place a later stage — an explain trace, an audit query — can learn
    /// the provider rank of *every* received candidate, so a per-candidate
    /// reconciliation can be a complete, ordered partition rather than a
    /// concatenation of per-stage groups. It carries identities only: the
    /// report stays content-free.
    pub received_candidate_ids: Vec<String>,
    /// Candidates admitted, in provider order.
    pub admitted_count: usize,
    /// Denied candidates, in provider order.
    pub denied: Vec<DeniedRecallCandidate>,
    /// Whether the lane is degraded because unknown-validity candidates were
    /// admitted under [`UnknownValidityPolicy::Degrade`].
    pub degraded: bool,
    /// Lane-level host warnings.
    pub warnings: Vec<String>,
}

impl RecallAdmissionReport {
    /// Counts denials per reason label in label order.
    #[must_use]
    pub fn denial_counts(&self) -> Vec<(&'static str, usize)> {
        let mut counts: Vec<(&'static str, usize)> = Vec::new();
        for denied in &self.denied {
            let label = denied.reason.label();
            match counts.iter_mut().find(|(existing, _)| *existing == label) {
                Some((_, count)) => *count += 1,
                None => counts.push((label, 1)),
            }
        }
        counts.sort_by(|left, right| left.0.cmp(right.0));
        counts
    }
}

/// Rank-final admission result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecallAdmission {
    /// Admitted candidates in provider order.
    pub admitted: Vec<AdmittedRecallCandidate>,
    /// Audit report including every denial.
    pub report: RecallAdmissionReport,
}

/// Admits the candidates of one validated recall reply against the exact
/// scope and temporal query the host dispatched in `call`.
///
/// The reply must already have passed fabric validation. The outcome envelope
/// is bound to the call (provider, revision, receipt, request identity, and
/// scope); a mismatch is an error, not a denial, because the whole outcome is
/// then unattributable. `authorized` is the registry's record of the scope
/// bindings this provider may attest; it comes from registration, never from
/// the reply.
pub fn admit_recall_reply(
    call: &ProviderCall,
    temporal: &AdmittedTemporalQuery,
    maximum_candidates: usize,
    authorized: &RecallScopeBindingsV1,
    reply: &ProviderReply,
) -> Result<RecallAdmission, RecallAdmissionError> {
    let terminal_code = reply.terminal.terminal_code();
    if !matches!(
        terminal_code,
        TerminalCode::Success | TerminalCode::SuccessZeroResults | TerminalCode::Partial
    ) {
        return Err(RecallAdmissionError::TerminalNotSuccessful { terminal_code });
    }
    let payload = reply
        .payload
        .as_ref()
        .ok_or(RecallAdmissionError::MissingPayload)?;
    let outcome = decode_recall_outcome(payload)?;
    bind_outcome_to_call(&outcome, call)?;
    if outcome.candidates.len() > maximum_candidates {
        return Err(RecallAdmissionError::CandidateBudgetExceeded {
            returned: outcome.candidates.len(),
            maximum: maximum_candidates,
        });
    }
    admit_recall_candidates(
        &call.exact_scope,
        &call.request_id,
        temporal,
        authorized,
        outcome.candidates,
    )
}

/// Decodes one canonical recall outcome payload without admitting anything.
pub fn decode_recall_outcome(
    payload: &CanonicalPayload,
) -> Result<RecallOutcomeV1, RecallAdmissionError> {
    if payload.contract_id.as_str() != RECALL_PAYLOAD_CONTRACT_ID {
        return Err(RecallAdmissionError::PayloadContractMismatch {
            contract_id: payload.contract_id.as_str().to_owned(),
        });
    }
    payload.validate()?;
    serde_json::from_slice::<RecallOutcomeV1>(&payload.bytes).map_err(|error| {
        RecallAdmissionError::PayloadDecode {
            detail: bounded_detail(&error.to_string()),
        }
    })
}

fn bind_outcome_to_call(
    outcome: &RecallOutcomeV1,
    call: &ProviderCall,
) -> Result<(), RecallAdmissionError> {
    if outcome.provider_id != call.provider_id.as_str() {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "provider_id",
        });
    }
    if outcome.registration_revision != call.registration_revision {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "registration_revision",
        });
    }
    if outcome.ready_receipt_digest != call.ready_receipt_sha256 {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "ready_receipt_digest",
        });
    }
    if outcome.request_identity != call.request_id {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "request_identity",
        });
    }
    if outcome.provider_instance_id.trim().is_empty() {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "provider_instance_id",
        });
    }
    let claimed = outcome.exact_scope_identity.fields();
    let admitted = admitted_scope_fields(&call.exact_scope);
    if claimed
        .iter()
        .zip(admitted.iter())
        .any(|((_, left), (_, right))| left != right)
    {
        return Err(RecallAdmissionError::OutcomeBinding {
            field: "exact_scope_identity",
        });
    }
    Ok(())
}

/// Pure, clock-free, deterministic admission of already-decoded candidates.
///
/// Provider order is preserved in both the admitted list and the denial
/// ledger. The same inputs always yield the same output.
pub fn admit_recall_candidates(
    admitted_scope: &OwnedExactScope,
    request_id: &str,
    temporal: &AdmittedTemporalQuery,
    authorized: &RecallScopeBindingsV1,
    candidates: Vec<RecallCandidateV1>,
) -> Result<RecallAdmission, RecallAdmissionError> {
    admitted_scope.validate()?;
    let mut seen = BTreeSet::new();
    for candidate in &candidates {
        if !seen.insert(candidate.candidate_id.as_str()) {
            return Err(RecallAdmissionError::DuplicateCandidateId(
                candidate.candidate_id.clone(),
            ));
        }
    }
    let received_count = candidates.len();
    let received_candidate_ids: Vec<String> = candidates
        .iter()
        .map(|candidate| candidate.candidate_id.clone())
        .collect();
    let mut admitted = Vec::new();
    let mut denied = Vec::new();
    let mut degraded = false;
    for candidate in candidates {
        match admit_one(admitted_scope, temporal, authorized, &candidate) {
            Ok((decision, native_score)) => {
                degraded |= decision.degrades_lane;
                admitted.push(AdmittedRecallCandidate {
                    host_temporal_state: decision.host_temporal_state,
                    warnings: decision.warnings,
                    native_score,
                    candidate,
                });
            }
            Err(reason) => denied.push(DeniedRecallCandidate {
                provider_claimed_scope_binding: candidate.exact_scope_identity.scope_binding,
                provider_claimed_scope_sha256: candidate
                    .exact_scope_identity
                    .claimed_scope_sha256(),
                provider_claimed_temporal_state: candidate.validity.temporal_state.clone(),
                candidate_id: candidate.candidate_id,
                stable_memory_ref: candidate.stable_memory_ref,
                reason,
            }),
        }
    }
    let mut warnings = Vec::new();
    if degraded {
        warnings.push(
            "unknown-validity candidates admitted under the degrade policy; lane is degraded"
                .to_owned(),
        );
    }
    Ok(RecallAdmission {
        report: RecallAdmissionReport {
            request_id: request_id.to_owned(),
            exact_scope_sha256: admitted_scope.exact_scope_sha256(),
            temporal_mode: temporal.mode.as_wire().to_owned(),
            evaluation_time: temporal.evaluation_time.clone(),
            unknown_validity_policy: temporal.unknown_validity_policy,
            authorized_scope_bindings: authorized.clone(),
            received_count,
            received_candidate_ids,
            admitted_count: admitted.len(),
            denied,
            degraded,
            warnings,
        },
        admitted,
    })
}

struct AdmitDecision {
    host_temporal_state: TemporalState,
    warnings: Vec<String>,
    degrades_lane: bool,
}

fn admit_one(
    admitted_scope: &OwnedExactScope,
    temporal: &AdmittedTemporalQuery,
    authorized: &RecallScopeBindingsV1,
    candidate: &RecallCandidateV1,
) -> Result<(AdmitDecision, ValidatedNativeScoreV1), RecallDenialReason> {
    check_scope(admitted_scope, authorized, &candidate.exact_scope_identity)?;
    check_content(candidate)?;
    let decision = check_validity(temporal, &candidate.validity)?;
    // Relevance inputs are admitted, never repaired: a score the host cannot
    // project honestly denies the candidate here rather than reaching
    // normalization as a neutral value.
    let native_score = validate_native_score(&candidate.native_score)
        .map_err(|defect| RecallDenialReason::NativeScoreMalformed { defect })?;
    Ok((decision, native_score))
}

/// Applies the claimed scope binding's field rules byte-for-byte in contract
/// order.
///
/// The binding must first be one the registry authorized for the provider.
/// Then, for every field: a malformed value (surrounding whitespace or control
/// characters) is an identity the host cannot resolve; a required field that
/// is empty is likewise unknown; a required or optional field that differs
/// from the admitted scope is a scope mismatch, except a differing
/// `resolved_scope_digest` under `exact_coding_scope`, which is a stale
/// resolution of this checkout; and a non-empty forbidden field is an
/// identity the binding does not allow the provider to claim.
fn check_scope(
    admitted_scope: &OwnedExactScope,
    authorized: &RecallScopeBindingsV1,
    claimed: &RecallScopeIdentityV1,
) -> Result<(), RecallDenialReason> {
    let binding = claimed.scope_binding;
    if !authorized.authorizes(binding) {
        return Err(RecallDenialReason::ScopeBindingUnauthorized { binding });
    }
    let claimed_fields = claimed.fields();
    for (field, value) in &claimed_fields {
        let malformed = value.trim() != *value || value.chars().any(char::is_control);
        let missing_required =
            value.is_empty() && binding.field_rule(*field) == ScopeFieldRule::RequiredEqual;
        if malformed || missing_required {
            return Err(RecallDenialReason::UnknownIdentity { field: *field });
        }
    }
    let admitted_fields = admitted_scope_fields(admitted_scope);
    for ((field, claimed_value), (_, admitted_value)) in
        claimed_fields.iter().zip(admitted_fields.iter())
    {
        match binding.field_rule(*field) {
            ScopeFieldRule::RequiredEqual => {
                if claimed_value == admitted_value {
                    continue;
                }
                return Err(match field {
                    ScopeField::ResolvedScopeDigest => RecallDenialReason::StaleIdentity,
                    other => RecallDenialReason::ScopeMismatch { field: *other },
                });
            }
            ScopeFieldRule::OptionalEmptyOrEqual => {
                if claimed_value.is_empty() || claimed_value == admitted_value {
                    continue;
                }
                return Err(RecallDenialReason::ScopeMismatch { field: *field });
            }
            ScopeFieldRule::Forbidden => {
                if !claimed_value.is_empty() {
                    return Err(RecallDenialReason::ForbiddenIdentity { field: *field });
                }
            }
        }
    }
    Ok(())
}

fn check_content(candidate: &RecallCandidateV1) -> Result<(), RecallDenialReason> {
    match (&candidate.content, &candidate.content_ref) {
        (Some(content), None) => {
            let actual = hex::encode(Sha256::digest(content.as_bytes()));
            if actual != candidate.content_sha256 {
                return Err(RecallDenialReason::ContentDigestMismatch);
            }
            Ok(())
        }
        (None, Some(reference)) if !reference.is_null() => Ok(()),
        _ => Err(RecallDenialReason::ContentSelectionInvalid),
    }
}

fn invalid(detail: &'static str) -> RecallDenialReason {
    RecallDenialReason::InvalidValidityRecord {
        detail: detail.to_owned(),
    }
}

fn parse_optional_instant(
    value: Option<&String>,
    field: &'static str,
) -> Result<Option<i64>, RecallDenialReason> {
    match value {
        None => Ok(None),
        Some(text) => parse_rfc3339_nanos(text).map(Some).ok_or_else(|| {
            RecallDenialReason::InvalidValidityRecord {
                detail: format!("{field} is not a utc_rfc3339_nanos timestamp"),
            }
        }),
    }
}

/// Computes the host's own temporal state, compares it with the provider's
/// claim, and applies the admitted temporal query.
fn check_validity(
    temporal: &AdmittedTemporalQuery,
    validity: &RecallValidityV1,
) -> Result<AdmitDecision, RecallDenialReason> {
    let claimed_state = TemporalState::from_wire(&validity.temporal_state)
        .ok_or_else(|| invalid("temporal_state is not a contract value"))?;
    if validity
        .source_revision
        .as_deref()
        .is_none_or(|revision| revision.trim().is_empty())
    {
        return Err(invalid("source_revision is required"));
    }
    parse_optional_instant(validity.observed_at.as_ref(), "observed_at")?;
    let valid_from = parse_optional_instant(validity.valid_from.as_ref(), "valid_from")?;
    let valid_until = parse_optional_instant(validity.valid_until.as_ref(), "valid_until")?;
    let superseded_at = parse_optional_instant(validity.superseded_at.as_ref(), "superseded_at")?;
    let revoked_at = parse_optional_instant(validity.revoked_at.as_ref(), "revoked_at")?;
    if let (Some(from), Some(until)) = (valid_from, valid_until)
        && from >= until
    {
        return Err(invalid("valid_from must lie before valid_until"));
    }
    if validity.superseded_by.is_some() && superseded_at.is_none() {
        return Err(invalid("superseded_by requires superseded_at"));
    }

    if claimed_state == TemporalState::Unknown {
        // The provider disclaims validity. It may not also assert revocation,
        // supersession, or a window it says it does not know.
        if revoked_at.is_some() || superseded_at.is_some() {
            return Err(invalid(
                "unknown temporal_state cannot carry revocation or supersession",
            ));
        }
        return match temporal.unknown_validity_policy {
            UnknownValidityPolicy::Exclude => Err(RecallDenialReason::UnknownValidity),
            UnknownValidityPolicy::Degrade => Ok(AdmitDecision {
                host_temporal_state: TemporalState::Unknown,
                warnings: vec!["validity unknown; admitted under degrade policy".to_owned()],
                degrades_lane: true,
            }),
            UnknownValidityPolicy::AllowWithWarning => Ok(AdmitDecision {
                host_temporal_state: TemporalState::Unknown,
                warnings: vec![
                    "validity unknown; admitted under allow_with_warning policy".to_owned(),
                ],
                degrades_lane: false,
            }),
        };
    }

    // A known state must be backed by a validity start.
    let Some(from) = valid_from else {
        return Err(invalid("valid_from is required for a known temporal_state"));
    };

    // Revocation and supersession dominate any window arithmetic.
    let host_state = if revoked_at.is_some() {
        TemporalState::Revoked
    } else if superseded_at.is_some() {
        TemporalState::Superseded
    } else {
        match temporal.point_instant() {
            Some(instant) => window_state(from, valid_until, instant),
            None => match temporal.interval {
                Some(((_, start), (_, end))) => {
                    if from >= end {
                        TemporalState::Future
                    } else if valid_until.is_some_and(|until| until <= start) {
                        TemporalState::Expired
                    } else {
                        TemporalState::Current
                    }
                }
                // History: the record is retained with its metadata; the
                // provider's own window classification stands only if it is
                // consistent with the evaluation instant.
                None => window_state(from, valid_until, temporal.evaluation_nanos),
            },
        }
    };

    // Provider claims cannot expand what the host computed. Revocation and
    // supersession are compared strictly; window states are compared strictly
    // for point queries, while interval/history queries evaluate the window
    // against the query and only reject claims that contradict the record's
    // own timestamps.
    let claim_consistent = match host_state {
        TemporalState::Revoked | TemporalState::Superseded => claimed_state == host_state,
        _ => {
            if claimed_state == TemporalState::Revoked || claimed_state == TemporalState::Superseded
            {
                false
            } else if temporal.point_instant().is_some() {
                claimed_state == host_state
            } else {
                // Interval/history: the provider classifies against its own
                // evaluation instant; only an impossible claim is rejected.
                let against_evaluation = window_state(from, valid_until, temporal.evaluation_nanos);
                claimed_state == against_evaluation || claimed_state == host_state
            }
        }
    };
    if !claim_consistent {
        return Err(invalid(
            "provider temporal_state contradicts validity timestamps",
        ));
    }

    match host_state {
        TemporalState::Revoked if !temporal.include_revoked => Err(RecallDenialReason::Revoked),
        TemporalState::Superseded if !temporal.include_superseded => {
            Err(RecallDenialReason::Superseded)
        }
        TemporalState::Future if temporal.mode != TemporalMode::History => {
            Err(RecallDenialReason::NotYetValid)
        }
        TemporalState::Expired if temporal.mode != TemporalMode::History => {
            Err(RecallDenialReason::Expired)
        }
        state => Ok(AdmitDecision {
            host_temporal_state: state,
            warnings: Vec::new(),
            degrades_lane: false,
        }),
    }
}

fn window_state(from: i64, until: Option<i64>, instant: i64) -> TemporalState {
    if from > instant {
        TemporalState::Future
    } else if until.is_some_and(|until| until <= instant) {
        TemporalState::Expired
    } else {
        TemporalState::Current
    }
}

fn bounded_detail(detail: &str) -> String {
    let mut end = detail.len().min(MAX_DECODE_DETAIL_BYTES);
    while end > 0 && !detail.is_char_boundary(end) {
        end -= 1;
    }
    detail[..end].to_owned()
}

impl<'de> Deserialize<'de> for AdmittedTemporalQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            mode: String,
            evaluation_time: String,
            #[serde(deserialize_with = "required_nullable")]
            as_of: Option<String>,
            #[serde(deserialize_with = "required_nullable")]
            interval_start: Option<String>,
            #[serde(deserialize_with = "required_nullable")]
            interval_end: Option<String>,
            include_superseded: bool,
            include_revoked: bool,
            unknown_validity_policy: UnknownValidityPolicy,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mode = TemporalMode::from_wire(&wire.mode)
            .ok_or_else(|| D::Error::custom("temporal mode is not a contract value"))?;
        let query = match (mode, wire.as_of, wire.interval_start, wire.interval_end) {
            (TemporalMode::Current, None, None, None) => Self::current(&wire.evaluation_time),
            (TemporalMode::AsOf, Some(as_of), None, None) => {
                Self::as_of(&wire.evaluation_time, &as_of)
            }
            (TemporalMode::Interval, None, Some(start), Some(end)) => {
                Self::interval(&wire.evaluation_time, &start, &end)
            }
            (TemporalMode::History, None, None, None) => Self::history(&wire.evaluation_time),
            _ => Err(RecallAdmissionError::InvalidTemporalQuery {
                field: "mode",
                detail: "temporal bounds do not match the mode",
            }),
        }
        .map_err(D::Error::custom)?;
        Ok(query
            .with_include_superseded(wire.include_superseded)
            .with_include_revoked(wire.include_revoked)
            .with_unknown_validity_policy(wire.unknown_validity_policy))
    }
}

impl Serialize for AdmittedTemporalQuery {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_wire_value().serialize(serializer)
    }
}
