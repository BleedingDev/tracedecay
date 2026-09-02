//! The class-to-action table, parsed from the canonical product document.
//!
//! The table is data, not a chain of `if` statements: a class is admitted,
//! annotated, redacted, quarantined, or rejected because
//! `product/observations/observation-hygiene-policy-v1.json` says so, and the
//! same document is asserted from Python by
//! `tests/product_observation_hygiene_policy_test.py`. Drift in either
//! direction fails a test rather than silently changing what crosses the
//! provider boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::Deserialize;
use tracedecay_domain::canonical_text::sha256_hex;

/// The canonical policy document, byte-pinned at compile time.
///
/// PACKAGING SEAM, resolved: the canonical copy lives under `product/` because
/// it is a product contract with a Python gate, and the ownership area for this
/// crate is `crates/tracedecay-memory-hygiene/**`. Embedding the product path
/// directly would reach across that boundary and would leave `cargo package`
/// with an unresolvable `include_str!`. The crate therefore carries its own
/// byte-identical copy under `policy/`, embeds that, and
/// `tests/product_observation_hygiene_policy_test.py` asserts the two files are
/// byte-for-byte equal — so the crate packages and compiles on its own while
/// drift between the copies fails a gate rather than passing silently.
pub const OBSERVATION_HYGIENE_POLICY_V1_JSON: &str =
    include_str!("../policy/observation-hygiene-policy-v1.json");

/// Repository-relative path of the canonical product copy the embedded document
/// must equal, named here so the Python gate and this crate cannot disagree
/// about which file is canonical.
pub const OBSERVATION_HYGIENE_POLICY_V1_CANONICAL_PATH: &str =
    "product/observations/observation-hygiene-policy-v1.json";

/// Repository-relative path of the crate-local copy this crate embeds.
pub const OBSERVATION_HYGIENE_POLICY_V1_EMBEDDED_PATH: &str =
    "crates/tracedecay-memory-hygiene/policy/observation-hygiene-policy-v1.json";

/// Stable sanitizer identity carried by every receipt this crate mints.
pub const OBSERVATION_HYGIENE_SANITIZER_ID: &str = "tracedecay.memory.observation.hygiene.v1";

/// Number of policy-document digest hex characters folded into the revision.
const REVISION_DIGEST_PREFIX_LEN: usize = 16;

/// Why a hygiene policy could not be assembled.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PolicyError {
    /// The embedded policy document did not parse.
    #[error("observation hygiene policy document did not parse")]
    MalformedDocument,
    /// The document declared an identity this build does not implement.
    #[error("observation hygiene policy document declares unexpected {field}")]
    UnexpectedIdentity {
        /// Document field whose value this build does not implement.
        field: &'static str,
    },
    /// The document named a class this build does not implement.
    #[error("observation hygiene policy document declares unknown class {class}")]
    UnknownClass {
        /// Class identity as spelled in the document.
        class: String,
    },
    /// The document listed one class twice.
    #[error("observation hygiene policy document declares class {class} twice")]
    DuplicateClass {
        /// Class identity as spelled in the document.
        class: String,
    },
    /// This build implements a class the document does not cover.
    #[error("observation hygiene policy document does not cover class {class}")]
    MissingClass {
        /// Class identity this build implements.
        class: &'static str,
    },
    /// An override tried to lower a class below its policy severity.
    #[error("override lowers class {class} below its policy severity")]
    SeverityDowngrade {
        /// Class the override tried to weaken.
        class: &'static str,
    },
    /// The document declared a class whose action and withheld reason disagree.
    #[error("class {class} pairs action {action} with an incompatible withheld reason")]
    InconsistentWithheldReason {
        /// Class identity as spelled in the document.
        class: String,
        /// Action as spelled in the document.
        action: String,
    },
    /// The document named a multi-signal class that does not sit on the reject
    /// floor, so the supplementary pass could weaken rather than harden.
    #[error("reject-floor signal class {class} does not sit on the reject floor")]
    SignalClassOffFloor {
        /// Class identity as spelled in the document.
        class: String,
    },
}

/// What the pipeline does with one detected class.
///
/// The variants are declared in ascending severity, so the derived `Ord` is
/// the severity ladder the policy document publishes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HygieneAction {
    /// Deliver the bytes unchanged and record nothing.
    Accept,
    /// Deliver the bytes unchanged but record a typed finding on the receipt.
    Annotate,
    /// Rewrite the exact detected span in the provider-bound copy.
    Redact,
    /// Withhold the observation because no exact span can be proven.
    Quarantine,
    /// Withhold the observation because the material must never be delivered.
    Reject,
}

impl HygieneAction {
    /// Returns the stable wire spelling.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Annotate => "annotate",
            Self::Redact => "redact",
            Self::Quarantine => "quarantine",
            Self::Reject => "reject",
        }
    }

    /// Parses a stable wire spelling.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "accept" => Some(Self::Accept),
            "annotate" => Some(Self::Annotate),
            "redact" => Some(Self::Redact),
            "quarantine" => Some(Self::Quarantine),
            "reject" => Some(Self::Reject),
            _ => None,
        }
    }

    /// Returns whether this action rewrites bytes in the provider-bound copy.
    #[must_use]
    pub const fn rewrites_payload(&self) -> bool {
        matches!(self, Self::Redact)
    }

    /// Returns whether this action stops the observation from being delivered.
    #[must_use]
    pub const fn withholds(&self) -> bool {
        matches!(self, Self::Quarantine | Self::Reject)
    }
}

/// One detectable class of material the pipeline classifies.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HygieneClass {
    /// A PEM private-key block.
    PrivateKey,
    /// A presented bearer token.
    BearerToken,
    /// A known issuer credential prefix from the vendored catalogue.
    KnownCredentialPrefix,
    /// A high-entropy token whose byte span the public detector does not expose.
    HighEntropyToken,
    /// The credential detector itself failed to compile.
    DetectorUnavailable,
    /// A credential-like `key=value` assignment inside one string.
    CredentialAssignment,
    /// A value whose object key proves it is a credential.
    SensitiveField,
    /// Credential material inside an object key.
    CredentialBearingKey,
    /// A process identifier.
    TransientProcessId,
    /// An instance-shaped temporary path.
    TransientTempPath,
    /// An ephemeral local bind address.
    TransientEphemeralPort,
    /// A run-log line.
    TransientRunLog,
}

impl HygieneClass {
    /// Every class this build implements, in document order.
    pub const ALL: [Self; 12] = [
        Self::PrivateKey,
        Self::BearerToken,
        Self::KnownCredentialPrefix,
        Self::HighEntropyToken,
        Self::DetectorUnavailable,
        Self::CredentialAssignment,
        Self::SensitiveField,
        Self::CredentialBearingKey,
        Self::TransientProcessId,
        Self::TransientTempPath,
        Self::TransientEphemeralPort,
        Self::TransientRunLog,
    ];

    /// Returns the stable wire spelling.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PrivateKey => "private_key",
            Self::BearerToken => "bearer_token",
            Self::KnownCredentialPrefix => "known_credential_prefix",
            Self::HighEntropyToken => "high_entropy_token",
            Self::DetectorUnavailable => "detector_unavailable",
            Self::CredentialAssignment => "credential_assignment",
            Self::SensitiveField => "sensitive_field",
            Self::CredentialBearingKey => "credential_bearing_key",
            Self::TransientProcessId => "transient_process_id",
            Self::TransientTempPath => "transient_temp_path",
            Self::TransientEphemeralPort => "transient_ephemeral_port",
            Self::TransientRunLog => "transient_run_log",
        }
    }

    /// Parses a stable wire spelling.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|class| class.as_str() == value)
    }

    /// Maps a reason string from the canonical credential detector onto a class.
    ///
    /// The reasons are the exact strings
    /// `tracedecay_runtime_core::memory::hygiene::detect_secret_like` returns;
    /// an unrecognised reason means the shared corpus grew a class this policy
    /// has not yet ruled on, which is treated as detector unavailability so the
    /// pipeline fails closed rather than silently admitting it.
    #[must_use]
    pub fn for_detector_reason(reason: &str) -> Self {
        match reason {
            "PEM private-key block" => Self::PrivateKey,
            "bearer token" => Self::BearerToken,
            "known credential prefix" => Self::KnownCredentialPrefix,
            "credential-like key=value assignment" => Self::CredentialAssignment,
            "high-entropy token" => Self::HighEntropyToken,
            _ => Self::DetectorUnavailable,
        }
    }
}

/// The declared inputs to the supplementary multi-signal reject-floor pass.
///
/// `detect_secret_like` answers with the *first* pattern that matched a string,
/// so a string carrying two classes reports only one of them. These signals let
/// the pipeline prove the classes that first answer hid. They may only add
/// reject-floor classes: nothing here can lower a class the shared corpus
/// already proved, and the prefix list is explicitly not a second copy of the
/// vendored catalogue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RejectFloorSignals {
    known_credential_prefixes: Vec<String>,
    candidate_separators: Vec<char>,
    minimum_credential_run_length: usize,
    entropy_candidate_minimum_length: usize,
    maximum_detector_probes_per_payload: usize,
}

impl RejectFloorSignals {
    /// Returns the declared issuer prefixes the direct scan recognises.
    #[must_use]
    pub fn known_credential_prefixes(&self) -> &[String] {
        &self.known_credential_prefixes
    }

    /// Returns the characters a whitespace token is additionally split on when
    /// deriving probe candidates.
    #[must_use]
    pub fn candidate_separators(&self) -> &[char] {
        &self.candidate_separators
    }

    /// Returns how many credential characters must follow a declared prefix
    /// before the run is treated as issuer credential material.
    #[must_use]
    pub fn minimum_credential_run_length(&self) -> usize {
        self.minimum_credential_run_length
    }

    /// Returns the shortest candidate the entropy probe will spend a detector
    /// call on.
    #[must_use]
    pub fn entropy_candidate_minimum_length(&self) -> usize {
        self.entropy_candidate_minimum_length
    }

    /// Returns the bounded number of supplementary detector probes one payload
    /// may spend before the pass fails closed.
    #[must_use]
    pub fn maximum_detector_probes_per_payload(&self) -> usize {
        self.maximum_detector_probes_per_payload
    }
}

/// The class-to-action table plus the identity that binds a receipt to it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationHygienePolicyV1 {
    actions: BTreeMap<HygieneClass, HygieneAction>,
    reject_floor: BTreeSet<HygieneClass>,
    signals: RejectFloorSignals,
    revision: String,
    document_digest: String,
    max_canonical_bytes: usize,
}

impl ObservationHygienePolicyV1 {
    /// Returns the table exactly as the canonical product document declares it.
    pub fn canonical() -> Result<Self, PolicyError> {
        static CANONICAL: OnceLock<Result<ObservationHygienePolicyV1, PolicyError>> =
            OnceLock::new();
        CANONICAL.get_or_init(Self::parse_canonical).clone()
    }

    /// Returns the canonical table with selected classes raised in severity.
    ///
    /// An override may only move a class up the ladder. Weakening a class — and
    /// in particular anything on the reject floor — is refused, so a deployment
    /// can harden the policy but cannot configure the secret gate off.
    pub fn with_overrides(
        overrides: &[(HygieneClass, HygieneAction)],
    ) -> Result<Self, PolicyError> {
        let mut policy = Self::canonical()?;
        for (class, action) in overrides {
            let current = policy.action(*class);
            if *action < current {
                return Err(PolicyError::SeverityDowngrade {
                    class: class.as_str(),
                });
            }
            policy.actions.insert(*class, *action);
        }
        policy.revision = derive_revision(&policy.document_digest, &policy.actions);
        Ok(policy)
    }

    /// Returns the action this policy takes for one class.
    #[must_use]
    pub fn action(&self, class: HygieneClass) -> HygieneAction {
        self.actions
            .get(&class)
            .copied()
            .unwrap_or(HygieneAction::Reject)
    }

    /// Returns whether a class sits on the reject floor.
    #[must_use]
    pub fn is_reject_floor(&self, class: HygieneClass) -> bool {
        self.reject_floor.contains(&class)
    }

    /// Returns the declared inputs to the supplementary multi-signal pass.
    #[must_use]
    pub fn signals(&self) -> &RejectFloorSignals {
        &self.signals
    }

    /// Returns the sanitizer revision receipts record.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Returns the lowercase SHA-256 of the canonical policy document bytes.
    #[must_use]
    pub fn document_digest(&self) -> &str {
        &self.document_digest
    }

    /// Returns the bounded canonical byte ceiling scanned payloads must respect.
    #[must_use]
    pub fn max_canonical_bytes(&self) -> usize {
        self.max_canonical_bytes
    }

    fn parse_canonical() -> Result<Self, PolicyError> {
        let document: PolicyDocument = serde_json::from_str(OBSERVATION_HYGIENE_POLICY_V1_JSON)
            .map_err(|_| PolicyError::MalformedDocument)?;
        if document.contract_id != "tracedecay.observation-hygiene-policy.v1" {
            return Err(PolicyError::UnexpectedIdentity {
                field: "contract_id",
            });
        }
        if document.sanitizer_id != OBSERVATION_HYGIENE_SANITIZER_ID {
            return Err(PolicyError::UnexpectedIdentity {
                field: "sanitizer_id",
            });
        }
        let declared_ladder: Vec<HygieneAction> = document
            .severity_ladder
            .iter()
            .filter_map(|action| HygieneAction::from_wire(action))
            .collect();
        let expected_ladder = [
            HygieneAction::Accept,
            HygieneAction::Annotate,
            HygieneAction::Redact,
            HygieneAction::Quarantine,
            HygieneAction::Reject,
        ];
        if declared_ladder != expected_ladder {
            return Err(PolicyError::UnexpectedIdentity {
                field: "severity_ladder",
            });
        }

        let mut actions = BTreeMap::new();
        for row in &document.classes {
            let class = HygieneClass::from_wire(&row.class_id).ok_or_else(|| {
                PolicyError::UnknownClass {
                    class: row.class_id.clone(),
                }
            })?;
            let action =
                HygieneAction::from_wire(&row.action).ok_or(PolicyError::UnexpectedIdentity {
                    field: "classes[].action",
                })?;
            let reason_agrees = match (action.withholds(), row.withheld_reason.as_deref()) {
                (true, Some("secret_rejected")) => action == HygieneAction::Reject,
                (true, Some("quarantined")) => action == HygieneAction::Quarantine,
                (false, None) => true,
                _ => false,
            };
            if !reason_agrees {
                return Err(PolicyError::InconsistentWithheldReason {
                    class: row.class_id.clone(),
                    action: row.action.clone(),
                });
            }
            if action.rewrites_payload() != row.mutates_payload {
                return Err(PolicyError::InconsistentWithheldReason {
                    class: row.class_id.clone(),
                    action: row.action.clone(),
                });
            }
            if actions.insert(class, action).is_some() {
                return Err(PolicyError::DuplicateClass {
                    class: row.class_id.clone(),
                });
            }
        }
        for class in HygieneClass::ALL {
            if !actions.contains_key(&class) {
                return Err(PolicyError::MissingClass {
                    class: class.as_str(),
                });
            }
        }

        let mut reject_floor = BTreeSet::new();
        for class_id in &document.reject_floor_classes {
            let class =
                HygieneClass::from_wire(class_id).ok_or_else(|| PolicyError::UnknownClass {
                    class: class_id.clone(),
                })?;
            if actions.get(&class).copied() != Some(HygieneAction::Reject) {
                return Err(PolicyError::InconsistentWithheldReason {
                    class: class_id.clone(),
                    action: HygieneAction::Reject.as_str().to_owned(),
                });
            }
            reject_floor.insert(class);
        }

        let signals = parse_reject_floor_signals(&document.reject_floor_signals, &reject_floor)?;

        let document_digest = sha256_hex(OBSERVATION_HYGIENE_POLICY_V1_JSON.as_bytes());
        let revision = derive_revision(&document_digest, &actions);
        Ok(Self {
            actions,
            reject_floor,
            signals,
            revision,
            document_digest,
            max_canonical_bytes: document.payload_limits.max_canonical_bytes,
        })
    }
}

fn parse_reject_floor_signals(
    document: &PolicyRejectFloorSignals,
    reject_floor: &BTreeSet<HygieneClass>,
) -> Result<RejectFloorSignals, PolicyError> {
    // A signal class that is not on the reject floor would let the
    // supplementary pass *change* a classification rather than harden it, which
    // is the one thing this pass must never be able to do.
    for class_id in document
        .direct_signal_classes
        .iter()
        .chain(&document.probe_signal_classes)
    {
        let class = HygieneClass::from_wire(class_id).ok_or_else(|| PolicyError::UnknownClass {
            class: class_id.clone(),
        })?;
        if !reject_floor.contains(&class) {
            return Err(PolicyError::SignalClassOffFloor {
                class: class_id.clone(),
            });
        }
    }
    if document.known_credential_prefixes_are_exhaustive {
        // The vendored catalogue has exactly one owner; a list here claiming to
        // be exhaustive would be a second corpus by another name.
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.known_credential_prefixes_are_exhaustive",
        });
    }
    if document.known_credential_prefixes.is_empty()
        || document
            .known_credential_prefixes
            .iter()
            .any(|prefix| prefix.len() < 2 || !prefix.is_ascii())
    {
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.known_credential_prefixes",
        });
    }
    let mut candidate_separators = Vec::with_capacity(document.candidate_separators.len());
    for separator in &document.candidate_separators {
        let mut characters = separator.chars();
        match (characters.next(), characters.next()) {
            (Some(character), None) => candidate_separators.push(character),
            _ => {
                return Err(PolicyError::UnexpectedIdentity {
                    field: "reject_floor_signals.candidate_separators",
                });
            }
        }
    }
    if candidate_separators.is_empty() {
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.candidate_separators",
        });
    }
    if document.minimum_credential_run_length < 8 {
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.minimum_credential_run_length",
        });
    }
    if document.entropy_candidate_minimum_length < 16 {
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.entropy_candidate_minimum_length",
        });
    }
    if document.maximum_detector_probes_per_payload == 0 {
        return Err(PolicyError::UnexpectedIdentity {
            field: "reject_floor_signals.maximum_detector_probes_per_payload",
        });
    }
    Ok(RejectFloorSignals {
        known_credential_prefixes: document.known_credential_prefixes.clone(),
        candidate_separators,
        minimum_credential_run_length: document.minimum_credential_run_length,
        entropy_candidate_minimum_length: document.entropy_candidate_minimum_length,
        maximum_detector_probes_per_payload: document.maximum_detector_probes_per_payload,
    })
}

/// Derives the sanitizer revision from the document digest and the effective
/// table, so an override is visible to anyone reading a receipt.
fn derive_revision(
    document_digest: &str,
    actions: &BTreeMap<HygieneClass, HygieneAction>,
) -> String {
    let mut table = String::new();
    for (class, action) in actions {
        table.push_str(class.as_str());
        table.push('=');
        table.push_str(action.as_str());
        table.push(';');
    }
    let table_digest = sha256_hex(table.as_bytes());
    let document_prefix = document_digest
        .get(..REVISION_DIGEST_PREFIX_LEN)
        .unwrap_or(document_digest);
    let table_prefix = table_digest
        .get(..REVISION_DIGEST_PREFIX_LEN)
        .unwrap_or(table_digest.as_str());
    format!("{OBSERVATION_HYGIENE_SANITIZER_ID}+{document_prefix}+{table_prefix}")
}

#[derive(Deserialize)]
struct PolicyDocument {
    contract_id: String,
    sanitizer_id: String,
    severity_ladder: Vec<String>,
    reject_floor_classes: Vec<String>,
    reject_floor_signals: PolicyRejectFloorSignals,
    payload_limits: PolicyPayloadLimits,
    classes: Vec<PolicyClass>,
}

#[derive(Deserialize)]
struct PolicyRejectFloorSignals {
    candidate_separators: Vec<String>,
    minimum_credential_run_length: usize,
    entropy_candidate_minimum_length: usize,
    maximum_detector_probes_per_payload: usize,
    direct_signal_classes: Vec<String>,
    probe_signal_classes: Vec<String>,
    known_credential_prefixes: Vec<String>,
    known_credential_prefixes_are_exhaustive: bool,
}

#[derive(Deserialize)]
struct PolicyPayloadLimits {
    max_canonical_bytes: usize,
}

#[derive(Deserialize)]
struct PolicyClass {
    class_id: String,
    action: String,
    withheld_reason: Option<String>,
    mutates_payload: bool,
}
