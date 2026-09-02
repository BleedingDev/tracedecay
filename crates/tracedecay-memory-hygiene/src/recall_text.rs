//! Untrusted-text hardening for provider recall candidates.
//!
//! # Why this exists
//!
//! An observation travels *host → provider* and is sanitized so a credential
//! never leaves the host. A recall candidate travels the other way — *provider
//! → agent* — and carries the opposite risk: the provider's words are placed
//! inside a context pack an agent reads as part of its instructions. Untreated,
//! a stored "memory" is an untrusted command channel. It can end a line and
//! open its own section heading, it can carry chat-control or tool-call markup
//! a host might parse, it can hide direction-override characters that make the
//! rendered line read differently from the bytes, and it can echo back a secret
//! the host never wanted delivered anywhere.
//!
//! This module is the single seam that classifies recall text as **untrusted
//! advisory data** before it reaches context assembly. It is deliberately not a
//! semantic filter: it does not try to decide whether a sentence *means*
//! something dangerous. It enforces four structural properties instead:
//!
//! 1. **Containment.** Hardened text is a single line with no control
//!    characters, so an advisory item cannot terminate its own rendered line
//!    and start a new section, list, or heading in the pack.
//! 2. **No control markup.** Chat special tokens (`<|…|>`) and tool-call tags
//!    are replaced by one opaque marker, so advisory text cannot present itself
//!    to a markup-parsing host as a tool invocation.
//! 3. **No secret echo.** The text is run through the *same* admitted secret
//!    pipeline as an outbound observation ([`ObservationSanitizer`]). Reject-
//!    floor material withholds the whole item; a redactable span is redacted in
//!    place and the redaction is recorded.
//! 4. **Labelled trust.** Every admitted item is prefixed with a boundary label
//!    naming it as untrusted memory, and the provenance-derived trust tier
//!    gates admission by policy: structurally suspicious text from an
//!    unattributed candidate is withheld rather than repaired.
//!
//! # Ordering is load-bearing
//!
//! The secret scan runs **before** structural folding. A PEM private key is a
//! multi-line block, and folding its newlines away first would hide it from the
//! line-oriented credential detectors. Truncation is never used to enforce a
//! size ceiling: half a memory is a different claim from the whole one, so an
//! oversized candidate is withheld with a typed reason instead.
//!
//! Every outcome here is typed. Nothing is silently dropped, and no caller has
//! to parse prose to learn what happened.

use serde_json::Value;
use tracedecay_domain::canonical_text::canonical_framed_sha256;

use crate::{HygieneError, ObservationAdmission, ObservationSanitizer, SanitizationDisposition};

/// Identity of the untrusted advisory-recall hardening policy implemented
/// here.
pub const ADVISORY_RECALL_HARDENING_POLICY_ID: &str = "tracedecay.memory.recall.untrusted-text.v1";

/// Revision of [`ADVISORY_RECALL_HARDENING_POLICY_ID`]. Any change to the
/// neutralization vocabulary, the boundary label, or the trust gates must
/// increment this.
pub const ADVISORY_RECALL_HARDENING_POLICY_REVISION: u32 = 1;

/// Domain separator for the digests that bind a hardened item to its source.
const ADVISORY_TEXT_DIGEST_DOMAIN: &[u8] = b"tracedecay.memory.recall.untrusted-text.v1";

/// The opaque stand-in written over neutralized control markup.
///
/// It contains no `<`, `|`, or newline, so re-hardening already hardened text
/// is a fixpoint and the marker cannot itself be mistaken for markup.
pub const NEUTRALIZED_CONTROL_MARKUP: &str = "[untrusted-marker-removed]";

/// The instruction boundary every admitted advisory item carries.
///
/// The label is written by the host, not by the provider, and any occurrence
/// of it inside provider text is neutralized first, so a candidate cannot forge
/// a boundary and present its own words as host framing.
pub const UNTRUSTED_BOUNDARY_LABEL: &str = "[untrusted-memory]";

/// Ceiling the configurable content bound may not exceed.
pub const ADVISORY_CONTENT_CHARS_CEILING: usize = 8_192;

/// Content ceiling of the pinned production policy.
///
/// It sits at or above the host's own admitted per-candidate content budget
/// (`RecallBudgetsV1::maximum_candidate_content_bytes`, 4 KiB in the mounted
/// project route), because a candidate the host already admitted must not then
/// be refused here for a length the host itself allowed. This ceiling exists to
/// bound hostile input, not to second-guess admission.
pub const PINNED_ADVISORY_CONTENT_CHARS: usize = 4_096;

/// Explanation ceiling of the pinned production policy. An explanation is
/// metadata beside a claim, so it is bounded more tightly than the claim.
pub const PINNED_ADVISORY_EXPLANATION_CHARS: usize = 1_024;

/// Metadata ceiling of the pinned production policy.
///
/// A candidate identity, a claimed provenance source, and a redaction reason
/// are *labels*, not claims: they name something. A label longer than this is
/// not a label, it is smuggled content wearing a label's clothes, so it is
/// refused rather than rendered beside the hardened claim.
pub const PINNED_ADVISORY_METADATA_CHARS: usize = 256;

/// Chat-control and tool-call tag names neutralized wherever they appear as
/// markup.
///
/// The list is deliberately narrow and tool-specific. A generic tag such as
/// `<user>` is left alone because ordinary code facts contain generic angle
/// brackets, and over-neutralizing them would corrupt legitimate memories.
const CONTROL_MARKUP_TAGS: &[&str] = &[
    "antml:function_calls",
    "antml:invoke",
    "antml:parameter",
    "function_call",
    "function_calls",
    "invoke",
    "parameter",
    "tool_call",
    "tool_calls",
    "tool_result",
    "tool_use",
    "tools",
];

/// Longest markup run that may be collapsed into one marker. A `<` with no
/// closing delimiter inside this window is ordinary text, not markup.
const CONTROL_MARKUP_WINDOW_CHARS: usize = 512;

/// How much a candidate's provenance lets the host trust its text.
///
/// Declaration order *is* the trust order: an unattributed candidate is the
/// weakest, a candidate that names its source is the strongest.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdvisoryTrustTierV1 {
    /// No provenance was established for the candidate, or the claim it made
    /// could not be confirmed by any host authority.
    Unattributed,
    /// The provider named a source or gave a redaction reason. It is the
    /// provider's own claim, and no host authority has confirmed it.
    ProviderAttested,
    /// A host authority confirmed the claimed source names one of the host's
    /// own exact evidence shapes.
    HostConfirmed,
}

impl AdvisoryTrustTierV1 {
    /// Stable wire label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unattributed => "unattributed",
            Self::ProviderAttested => "provider_attested",
            Self::HostConfirmed => "host_confirmed",
        }
    }
}

/// Why an [`AdvisoryRecallPolicyV1`] could not be constructed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdvisoryRecallPolicyError {
    /// The requested content ceiling is zero or above the hard ceiling.
    #[error("advisory content ceiling {requested} must be in 1..={ceiling}")]
    ContentCeilingOutOfRange {
        /// Ceiling the caller asked for.
        requested: usize,
        /// Hard ceiling this build enforces.
        ceiling: usize,
    },
    /// The requested explanation ceiling is zero or above the hard ceiling.
    #[error("advisory explanation ceiling {requested} must be in 1..={ceiling}")]
    ExplanationCeilingOutOfRange {
        /// Ceiling the caller asked for.
        requested: usize,
        /// Hard ceiling this build enforces.
        ceiling: usize,
    },
    /// The requested metadata ceiling is zero or above the hard ceiling.
    #[error("advisory metadata ceiling {requested} must be in 1..={ceiling}")]
    MetadataCeilingOutOfRange {
        /// Ceiling the caller asked for.
        requested: usize,
        /// Hard ceiling this build enforces.
        ceiling: usize,
    },
}

/// Pinned configuration of the untrusted-text gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdvisoryRecallPolicyV1 {
    minimum_trust_tier: AdvisoryTrustTierV1,
    suspicious_structure_trust_floor: AdvisoryTrustTierV1,
    max_content_chars: usize,
    max_explanation_chars: usize,
    max_metadata_chars: usize,
}

impl AdvisoryRecallPolicyV1 {
    /// The production policy.
    ///
    /// Provenance alone never blocks an ordinary memory: an unattributed
    /// candidate is labelled, not discarded, because most legitimate recall
    /// carries weak provenance and blocking it would gut the lane. What
    /// provenance *does* gate is tolerance for suspicious structure — text that
    /// only becomes renderable after control markup or hidden characters were
    /// neutralized must at least carry a provider-attested provenance state, or
    /// it is withheld rather than repaired.
    #[must_use]
    pub const fn pinned() -> Self {
        Self {
            minimum_trust_tier: AdvisoryTrustTierV1::Unattributed,
            suspicious_structure_trust_floor: AdvisoryTrustTierV1::ProviderAttested,
            max_content_chars: PINNED_ADVISORY_CONTENT_CHARS,
            max_explanation_chars: PINNED_ADVISORY_EXPLANATION_CHARS,
            max_metadata_chars: PINNED_ADVISORY_METADATA_CHARS,
        }
    }

    /// An explicit policy.
    ///
    /// # Errors
    ///
    /// Returns [`AdvisoryRecallPolicyError`] when a ceiling is zero or above
    /// [`ADVISORY_CONTENT_CHARS_CEILING`].
    pub const fn new(
        minimum_trust_tier: AdvisoryTrustTierV1,
        suspicious_structure_trust_floor: AdvisoryTrustTierV1,
        max_content_chars: usize,
        max_explanation_chars: usize,
        max_metadata_chars: usize,
    ) -> Result<Self, AdvisoryRecallPolicyError> {
        if max_content_chars == 0 || max_content_chars > ADVISORY_CONTENT_CHARS_CEILING {
            return Err(AdvisoryRecallPolicyError::ContentCeilingOutOfRange {
                requested: max_content_chars,
                ceiling: ADVISORY_CONTENT_CHARS_CEILING,
            });
        }
        if max_explanation_chars == 0 || max_explanation_chars > ADVISORY_CONTENT_CHARS_CEILING {
            return Err(AdvisoryRecallPolicyError::ExplanationCeilingOutOfRange {
                requested: max_explanation_chars,
                ceiling: ADVISORY_CONTENT_CHARS_CEILING,
            });
        }
        if max_metadata_chars == 0 || max_metadata_chars > ADVISORY_CONTENT_CHARS_CEILING {
            return Err(AdvisoryRecallPolicyError::MetadataCeilingOutOfRange {
                requested: max_metadata_chars,
                ceiling: ADVISORY_CONTENT_CHARS_CEILING,
            });
        }
        Ok(Self {
            minimum_trust_tier,
            suspicious_structure_trust_floor,
            max_content_chars,
            max_explanation_chars,
            max_metadata_chars,
        })
    }

    /// Trust tier below which no candidate is admitted at all.
    #[must_use]
    pub const fn minimum_trust_tier(&self) -> AdvisoryTrustTierV1 {
        self.minimum_trust_tier
    }

    /// Trust tier a candidate must reach before neutralized control markup or
    /// hidden characters are tolerated instead of withheld.
    #[must_use]
    pub const fn suspicious_structure_trust_floor(&self) -> AdvisoryTrustTierV1 {
        self.suspicious_structure_trust_floor
    }

    /// Longest admitted content, in characters.
    #[must_use]
    pub const fn max_content_chars(&self) -> usize {
        self.max_content_chars
    }

    /// Longest retained explanation, in characters.
    #[must_use]
    pub const fn max_explanation_chars(&self) -> usize {
        self.max_explanation_chars
    }

    /// Longest admitted provider-controlled metadata label, in characters.
    #[must_use]
    pub const fn max_metadata_chars(&self) -> usize {
        self.max_metadata_chars
    }
}

/// One typed neutralization the hardener performed.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdvisoryTextFindingV1 {
    /// A line break was folded into a space so the item stays on one line.
    LineBreakFolded,
    /// A control character was removed.
    ControlCharacterRemoved,
    /// A zero-width, joiner, or bidirectional-override character was removed.
    HiddenCharacterRemoved,
    /// Chat-control or tool-call markup was replaced by
    /// [`NEUTRALIZED_CONTROL_MARKUP`].
    ControlMarkupNeutralized,
    /// A forged copy of [`UNTRUSTED_BOUNDARY_LABEL`] was neutralized.
    ForgedBoundaryLabelNeutralized,
    /// The admitted secret pipeline redacted a span of the text.
    SensitiveSpanRedacted,
    /// The candidate's explanation was dropped by the same gate that admitted
    /// its content.
    ExplanationWithheld,
}

impl AdvisoryTextFindingV1 {
    /// Stable wire label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LineBreakFolded => "line_break_folded",
            Self::ControlCharacterRemoved => "control_character_removed",
            Self::HiddenCharacterRemoved => "hidden_character_removed",
            Self::ControlMarkupNeutralized => "control_markup_neutralized",
            Self::ForgedBoundaryLabelNeutralized => "forged_boundary_label_neutralized",
            Self::SensitiveSpanRedacted => "sensitive_span_redacted",
            Self::ExplanationWithheld => "explanation_withheld",
        }
    }
}

/// One finding and how many times it fired.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AdvisoryTextFindingCountV1 {
    /// The neutralization that fired.
    pub finding: AdvisoryTextFindingV1,
    /// How many times it fired.
    pub occurrences: u32,
}

/// Why one candidate's text was withheld instead of admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvisoryTextWithheldReasonV1 {
    /// The admitted secret pipeline refused the text.
    SecretMaterial,
    /// The candidate's trust tier is below the policy's hard floor.
    TrustBelowFloor,
    /// The text only became renderable after control markup or hidden
    /// characters were neutralized, and the candidate is not trusted enough for
    /// that repair to be tolerated.
    SuspiciousStructureBelowTrustFloor,
    /// The text is longer than the policy admits. It is never truncated,
    /// because half a memory is a different claim from the whole one.
    OversizedContent,
    /// Nothing renderable remained after hardening.
    EmptyAfterHardening,
    /// The text could not be classified at all, so it is refused rather than
    /// delivered unclassified.
    Unclassifiable,
}

impl AdvisoryTextWithheldReasonV1 {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::SecretMaterial => "advisory_text_secret_material",
            Self::TrustBelowFloor => "advisory_text_trust_below_floor",
            Self::SuspiciousStructureBelowTrustFloor => {
                "advisory_text_suspicious_structure_below_trust_floor"
            }
            Self::OversizedContent => "advisory_text_oversized_content",
            Self::EmptyAfterHardening => "advisory_text_empty_after_hardening",
            Self::Unclassifiable => "advisory_text_unclassifiable",
        }
    }
}

/// One hardened advisory item, ready for context assembly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HardenedAdvisoryTextV1 {
    content: String,
    explanation: Option<String>,
    trust_tier: AdvisoryTrustTierV1,
    findings: Vec<AdvisoryTextFindingCountV1>,
    source_content_sha256: String,
    hardened_content_sha256: String,
}

impl HardenedAdvisoryTextV1 {
    /// The agent-visible content, boundary label included.
    #[must_use]
    pub fn content(&self) -> &str {
        &self.content
    }

    /// The retained explanation, if the gate admitted one.
    #[must_use]
    pub fn explanation(&self) -> Option<&str> {
        self.explanation.as_deref()
    }

    /// Trust tier the candidate's provenance established.
    #[must_use]
    pub const fn trust_tier(&self) -> AdvisoryTrustTierV1 {
        self.trust_tier
    }

    /// Typed neutralizations performed, in canonical order.
    #[must_use]
    pub fn findings(&self) -> &[AdvisoryTextFindingCountV1] {
        &self.findings
    }

    /// Whether one named finding fired.
    #[must_use]
    pub fn recorded(&self, finding: AdvisoryTextFindingV1) -> bool {
        self.findings.iter().any(|entry| entry.finding == finding)
    }

    /// Digest of the provider's original content.
    #[must_use]
    pub fn source_content_sha256(&self) -> &str {
        &self.source_content_sha256
    }

    /// Digest of the delivered content.
    #[must_use]
    pub fn hardened_content_sha256(&self) -> &str {
        &self.hardened_content_sha256
    }

    /// Identity of the policy that produced this value.
    #[must_use]
    pub const fn policy_id(&self) -> &'static str {
        ADVISORY_RECALL_HARDENING_POLICY_ID
    }

    /// Revision of the policy that produced this value.
    #[must_use]
    pub const fn policy_revision(&self) -> u32 {
        ADVISORY_RECALL_HARDENING_POLICY_REVISION
    }
}

/// The outcome of hardening one candidate's text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvisoryTextAdmissionV1 {
    /// The text may be compiled into a context pack.
    Admitted(HardenedAdvisoryTextV1),
    /// The text must not be delivered. The reason is typed and the source
    /// digest still points at what was refused, so a withholding is auditable
    /// without keeping a copy of the refused bytes.
    Withheld {
        /// Which rule fired.
        reason: AdvisoryTextWithheldReasonV1,
        /// Trust tier the candidate carried.
        trust_tier: AdvisoryTrustTierV1,
        /// Digest of the provider's original content.
        source_content_sha256: String,
    },
}

impl AdvisoryTextAdmissionV1 {
    /// The hardened item, when the text was admitted.
    #[must_use]
    pub const fn admitted(&self) -> Option<&HardenedAdvisoryTextV1> {
        match self {
            Self::Admitted(hardened) => Some(hardened),
            Self::Withheld { .. } => None,
        }
    }

    /// The typed withholding reason, when the text was refused.
    #[must_use]
    pub const fn withheld_reason(&self) -> Option<AdvisoryTextWithheldReasonV1> {
        match self {
            Self::Admitted(_) => None,
            Self::Withheld { reason, .. } => Some(*reason),
        }
    }
}

/// Which provider-controlled label one metadata hardening decided about.
///
/// Metadata is agent-visible exactly like content is: a candidate identity, a
/// claimed source, and a redaction reason are all interpolated into the line
/// the agent reads. They travel the same untrusted path and are named here so
/// a refusal says *which* label was refused.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AdvisoryMetadataFieldV1 {
    /// The provider's candidate identity.
    CandidateId,
    /// A claimed provenance source.
    ProvenanceSource,
    /// A provider-authored provenance reason.
    ProvenanceReason,
    /// A provider-authored explanation of why a candidate is relevant.
    ///
    /// It is rendered beside host evidence in a context pack and retained in
    /// an operator-visible explain trace, so it is untrusted for exactly the
    /// same reasons a claimed source is and passes exactly the same gate.
    ProviderExplanation,
}

impl AdvisoryMetadataFieldV1 {
    /// Stable wire label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidateId => "candidate_id",
            Self::ProvenanceSource => "provenance_source",
            Self::ProvenanceReason => "provenance_reason",
            Self::ProviderExplanation => "provider_explanation",
        }
    }
}

/// The outcome of hardening one provider-controlled metadata label.
///
/// A refused label is never repaired into something that looks like a real
/// label: the caller substitutes a host-minted stand-in, so the rendered line
/// never carries provider bytes the gate refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdvisoryMetadataAdmissionV1 {
    /// The label may be rendered. It is one line, control-free, carries no
    /// chat or tool markup, and no forged host boundary label.
    Admitted {
        /// Which label this is.
        field: AdvisoryMetadataFieldV1,
        /// The rendered form.
        value: String,
        /// Digest of the provider's original label.
        source_sha256: String,
    },
    /// The label must not be rendered.
    Withheld {
        /// Which label this is.
        field: AdvisoryMetadataFieldV1,
        /// Which rule fired.
        reason: AdvisoryTextWithheldReasonV1,
        /// Digest of the provider's original label.
        source_sha256: String,
    },
}

impl AdvisoryMetadataAdmissionV1 {
    /// The rendered label, when the gate admitted it.
    #[must_use]
    pub fn admitted(&self) -> Option<&str> {
        match self {
            Self::Admitted { value, .. } => Some(value),
            Self::Withheld { .. } => None,
        }
    }

    /// The typed refusal, when the gate refused the label.
    #[must_use]
    pub const fn withheld_reason(&self) -> Option<AdvisoryTextWithheldReasonV1> {
        match self {
            Self::Admitted { .. } => None,
            Self::Withheld { reason, .. } => Some(*reason),
        }
    }

    /// Which label this outcome is about.
    #[must_use]
    pub const fn field(&self) -> AdvisoryMetadataFieldV1 {
        match self {
            Self::Admitted { field, .. } | Self::Withheld { field, .. } => *field,
        }
    }

    /// Digest of the provider's original label, admitted or not.
    #[must_use]
    pub fn source_sha256(&self) -> &str {
        match self {
            Self::Admitted { source_sha256, .. } | Self::Withheld { source_sha256, .. } => {
                source_sha256
            }
        }
    }
}

/// Whether one already-rendered string is contained: exactly one line, no
/// control or hidden characters, and no forged host boundary label.
///
/// This is the property a renderer may rely on. It is deliberately cheap and
/// total so a downstream boundary that cannot depend on this crate can
/// re-derive the same predicate and refuse anything that fails it.
#[must_use]
pub fn is_contained_advisory_label(text: &str) -> bool {
    !text.is_empty()
        && !text.contains(UNTRUSTED_BOUNDARY_LABEL)
        && !text.chars().any(|character| {
            is_line_break(character) || is_hidden(character) || character.is_control()
        })
}

/// The untrusted-text gate every provider recall candidate passes through
/// before context assembly.
#[derive(Clone, Debug)]
pub struct AdvisoryTextHardener {
    sanitizer: ObservationSanitizer,
    policy: AdvisoryRecallPolicyV1,
}

impl AdvisoryTextHardener {
    /// Builds the gate from the canonical product hygiene policy and the
    /// pinned advisory policy.
    ///
    /// # Errors
    ///
    /// Returns [`HygieneError`] when the canonical class-to-action table cannot
    /// be assembled. The gate fails closed: a caller that cannot build it must
    /// not deliver provider text.
    pub fn new() -> Result<Self, HygieneError> {
        Ok(Self {
            sanitizer: ObservationSanitizer::new()?,
            policy: AdvisoryRecallPolicyV1::pinned(),
        })
    }

    /// Builds the gate from explicit parts.
    #[must_use]
    pub const fn with_parts(
        sanitizer: ObservationSanitizer,
        policy: AdvisoryRecallPolicyV1,
    ) -> Self {
        Self { sanitizer, policy }
    }

    /// The advisory policy in force.
    #[must_use]
    pub const fn policy(&self) -> &AdvisoryRecallPolicyV1 {
        &self.policy
    }

    /// Hardens one candidate's content and optional explanation.
    ///
    /// # Errors
    ///
    /// Returns [`HygieneError`] when the admitted secret pipeline itself fails
    /// — a detector fault or an encoding fault. A payload the pipeline refuses
    /// to classify is *not* an error: it is a typed
    /// [`AdvisoryTextWithheldReasonV1::Unclassifiable`] withholding, because
    /// unclassified provider text must never be delivered.
    pub fn harden(
        &self,
        content: &str,
        explanation: Option<&str>,
        trust_tier: AdvisoryTrustTierV1,
    ) -> Result<AdvisoryTextAdmissionV1, HygieneError> {
        let source_content_sha256 = text_digest("content", content);
        let withhold = |reason: AdvisoryTextWithheldReasonV1| {
            Ok(AdvisoryTextAdmissionV1::Withheld {
                reason,
                trust_tier,
                source_content_sha256: source_content_sha256.clone(),
            })
        };

        if trust_tier < self.policy.minimum_trust_tier {
            return withhold(AdvisoryTextWithheldReasonV1::TrustBelowFloor);
        }
        if content.chars().count() > self.policy.max_content_chars {
            return withhold(AdvisoryTextWithheldReasonV1::OversizedContent);
        }

        // The secret pass runs on the provider's original bytes. Folding line
        // breaks first would hide a multi-line PEM block from the line-oriented
        // credential detectors.
        let scanned = match self.scan_secrets(content)? {
            SecretScan::Withheld => {
                return withhold(AdvisoryTextWithheldReasonV1::SecretMaterial);
            }
            SecretScan::Unclassifiable => {
                return withhold(AdvisoryTextWithheldReasonV1::Unclassifiable);
            }
            SecretScan::Text { text, redacted } => (text, redacted),
        };
        let (scanned_text, content_redacted) = scanned;

        let mut counts = FindingCounts::default();
        if content_redacted {
            counts.record(AdvisoryTextFindingV1::SensitiveSpanRedacted, 1);
        }
        let neutralized = neutralize(&scanned_text, &mut counts);
        if neutralized.trim().is_empty() {
            return withhold(AdvisoryTextWithheldReasonV1::EmptyAfterHardening);
        }
        if counts.structurally_suspicious()
            && trust_tier < self.policy.suspicious_structure_trust_floor
        {
            return withhold(AdvisoryTextWithheldReasonV1::SuspiciousStructureBelowTrustFloor);
        }

        let explanation = match explanation {
            None => None,
            Some(explanation) => match self.harden_explanation(explanation, &mut counts)? {
                Some(explanation) => Some(explanation),
                None => {
                    counts.record(AdvisoryTextFindingV1::ExplanationWithheld, 1);
                    None
                }
            },
        };

        let content = format!("{UNTRUSTED_BOUNDARY_LABEL} {neutralized}");
        let hardened_content_sha256 = text_digest("content", &content);
        Ok(AdvisoryTextAdmissionV1::Admitted(HardenedAdvisoryTextV1 {
            content,
            explanation,
            trust_tier,
            findings: counts.into_sorted(),
            source_content_sha256,
            hardened_content_sha256,
        }))
    }

    /// Hardens one provider-controlled metadata label.
    ///
    /// A candidate identity and a provenance source or reason are rendered
    /// into the same agent-visible line as the claim itself, so they are
    /// untrusted for exactly the same reasons and pass the same gate: the
    /// admitted secret pipeline first, then containment to one line with no
    /// control markup, hidden characters, or forged host boundary label.
    ///
    /// # Errors
    ///
    /// Returns [`HygieneError`] when the admitted secret pipeline itself
    /// faults. A label the pipeline refuses to classify is not an error: it is
    /// a typed [`AdvisoryTextWithheldReasonV1::Unclassifiable`] withholding.
    pub fn harden_metadata(
        &self,
        field: AdvisoryMetadataFieldV1,
        value: &str,
    ) -> Result<AdvisoryMetadataAdmissionV1, HygieneError> {
        let source_sha256 = text_digest(field.as_str(), value);
        let withhold = |reason: AdvisoryTextWithheldReasonV1| {
            Ok(AdvisoryMetadataAdmissionV1::Withheld {
                field,
                reason,
                source_sha256: source_sha256.clone(),
            })
        };
        if value.chars().count() > self.policy.max_metadata_chars {
            return withhold(AdvisoryTextWithheldReasonV1::OversizedContent);
        }
        let scanned = match self.scan_secrets(value)? {
            SecretScan::Withheld => {
                return withhold(AdvisoryTextWithheldReasonV1::SecretMaterial);
            }
            SecretScan::Unclassifiable => {
                return withhold(AdvisoryTextWithheldReasonV1::Unclassifiable);
            }
            SecretScan::Text { text, redacted: _ } => text,
        };
        let mut counts = FindingCounts::default();
        let rendered = neutralize(&scanned, &mut counts);
        if rendered.trim().is_empty() {
            return withhold(AdvisoryTextWithheldReasonV1::EmptyAfterHardening);
        }
        // Belt and braces: the renderer's own precondition is re-checked here
        // rather than assumed from the neutralizer's implementation, so a
        // future change to the neutralizer cannot quietly widen what a label
        // may contain.
        if !is_contained_advisory_label(&rendered) {
            return withhold(AdvisoryTextWithheldReasonV1::EmptyAfterHardening);
        }
        Ok(AdvisoryMetadataAdmissionV1::Admitted {
            field,
            value: rendered,
            source_sha256,
        })
    }

    /// Hardens one explanation. An explanation is optional metadata: when the
    /// gate refuses it, the item keeps its content and loses the explanation
    /// rather than disappearing.
    fn harden_explanation(
        &self,
        explanation: &str,
        counts: &mut FindingCounts,
    ) -> Result<Option<String>, HygieneError> {
        if explanation.chars().count() > self.policy.max_explanation_chars {
            return Ok(None);
        }
        let (text, redacted) = match self.scan_secrets(explanation)? {
            SecretScan::Withheld | SecretScan::Unclassifiable => return Ok(None),
            SecretScan::Text { text, redacted } => (text, redacted),
        };
        if redacted {
            counts.record(AdvisoryTextFindingV1::SensitiveSpanRedacted, 1);
        }
        let neutralized = neutralize(&text, counts);
        if neutralized.trim().is_empty() {
            return Ok(None);
        }
        Ok(Some(neutralized))
    }

    /// Runs one string through the admitted secret pipeline.
    fn scan_secrets(&self, text: &str) -> Result<SecretScan, HygieneError> {
        let payload = Value::String(text.to_owned());
        let admission = match self.sanitizer.admit(&payload) {
            Ok(admission) => admission,
            Err(error) => {
                if matches!(
                    error,
                    HygieneError::PayloadTooLarge { .. } | HygieneError::PayloadTooDeep { .. }
                ) {
                    return Ok(SecretScan::Unclassifiable);
                }
                return Err(error);
            }
        };
        match admission {
            ObservationAdmission::Withheld { .. } => Ok(SecretScan::Withheld),
            ObservationAdmission::Admitted { sanitized, receipt } => match sanitized {
                Value::String(text) => Ok(SecretScan::Text {
                    text,
                    redacted: receipt.disposition() == SanitizationDisposition::Redacted,
                }),
                // A string payload that came back as another JSON shape is an
                // unexplained rewrite; refuse it rather than deliver it.
                _ => Ok(SecretScan::Unclassifiable),
            },
        }
    }
}

/// What the admitted secret pipeline said about one string.
enum SecretScan {
    /// The pipeline refused the text outright.
    Withheld,
    /// The pipeline could not classify the text at all.
    Unclassifiable,
    /// The text may be delivered, possibly with redacted spans.
    Text {
        /// Delivered text.
        text: String,
        /// Whether a span was rewritten.
        redacted: bool,
    },
}

/// Bounded tally of the neutralizations one hardening performed.
#[derive(Default)]
struct FindingCounts {
    line_breaks: u32,
    control_characters: u32,
    hidden_characters: u32,
    control_markup: u32,
    forged_labels: u32,
    redactions: u32,
    explanation_withheld: u32,
}

impl FindingCounts {
    fn record(&mut self, finding: AdvisoryTextFindingV1, occurrences: u32) {
        let slot = match finding {
            AdvisoryTextFindingV1::LineBreakFolded => &mut self.line_breaks,
            AdvisoryTextFindingV1::ControlCharacterRemoved => &mut self.control_characters,
            AdvisoryTextFindingV1::HiddenCharacterRemoved => &mut self.hidden_characters,
            AdvisoryTextFindingV1::ControlMarkupNeutralized => &mut self.control_markup,
            AdvisoryTextFindingV1::ForgedBoundaryLabelNeutralized => &mut self.forged_labels,
            AdvisoryTextFindingV1::SensitiveSpanRedacted => &mut self.redactions,
            AdvisoryTextFindingV1::ExplanationWithheld => &mut self.explanation_withheld,
        };
        *slot = slot.saturating_add(occurrences);
    }

    /// Whether the text only became renderable through repairs that a
    /// legitimate memory has no reason to need.
    ///
    /// Folded line breaks are excluded: ordinary facts are multi-line. Hidden
    /// characters, control characters, control markup, and a forged host
    /// boundary label are not ordinary.
    const fn structurally_suspicious(&self) -> bool {
        self.control_characters > 0
            || self.hidden_characters > 0
            || self.control_markup > 0
            || self.forged_labels > 0
    }

    fn into_sorted(self) -> Vec<AdvisoryTextFindingCountV1> {
        let mut findings = Vec::new();
        let mut push = |finding: AdvisoryTextFindingV1, occurrences: u32| {
            if occurrences > 0 {
                findings.push(AdvisoryTextFindingCountV1 {
                    finding,
                    occurrences,
                });
            }
        };
        push(AdvisoryTextFindingV1::LineBreakFolded, self.line_breaks);
        push(
            AdvisoryTextFindingV1::ControlCharacterRemoved,
            self.control_characters,
        );
        push(
            AdvisoryTextFindingV1::HiddenCharacterRemoved,
            self.hidden_characters,
        );
        push(
            AdvisoryTextFindingV1::ControlMarkupNeutralized,
            self.control_markup,
        );
        push(
            AdvisoryTextFindingV1::ForgedBoundaryLabelNeutralized,
            self.forged_labels,
        );
        push(
            AdvisoryTextFindingV1::SensitiveSpanRedacted,
            self.redactions,
        );
        push(
            AdvisoryTextFindingV1::ExplanationWithheld,
            self.explanation_withheld,
        );
        findings.sort();
        findings
    }
}

/// Domain-separated digest binding one hardened item to its source text.
fn text_digest(field: &str, text: &str) -> String {
    canonical_framed_sha256(
        ADVISORY_TEXT_DIGEST_DOMAIN,
        &[field.as_bytes(), text.as_bytes()],
    )
}

/// Makes one untrusted string safe to render inside a single advisory line.
fn neutralize(text: &str, counts: &mut FindingCounts) -> String {
    let without_markup = neutralize_control_markup(text, counts);
    let without_forged_label = neutralize_forged_label(&without_markup, counts);
    let mut rendered = String::with_capacity(without_forged_label.len());
    for character in without_forged_label.chars() {
        if is_line_break(character) {
            counts.record(AdvisoryTextFindingV1::LineBreakFolded, 1);
            rendered.push(' ');
        } else if is_hidden(character) {
            counts.record(AdvisoryTextFindingV1::HiddenCharacterRemoved, 1);
        } else if character.is_control() {
            counts.record(AdvisoryTextFindingV1::ControlCharacterRemoved, 1);
        } else {
            rendered.push(character);
        }
    }
    rendered.trim().to_owned()
}

/// Whether one character would end the advisory item's rendered line.
const fn is_line_break(character: char) -> bool {
    matches!(
        character,
        '\n' | '\r' | '\u{0b}' | '\u{0c}' | '\u{85}' | '\u{2028}' | '\u{2029}'
    )
}

/// Whether one character is invisible or rewrites reading direction, so the
/// rendered line would not say what the bytes say.
const fn is_hidden(character: char) -> bool {
    matches!(character,
        '\u{ad}'
        | '\u{200b}'..='\u{200f}'
        | '\u{202a}'..='\u{202e}'
        | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{2069}'
        | '\u{feff}')
}

/// Replaces chat-control tokens and tool-call markup with one opaque marker.
fn neutralize_control_markup(text: &str, counts: &mut FindingCounts) -> String {
    let mut rendered = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let Some(offset) = rest.find('<') else {
            rendered.push_str(rest);
            return rendered;
        };
        let (head, tail) = rest.split_at(offset);
        rendered.push_str(head);
        if let Some(consumed) = control_markup_len(tail) {
            counts.record(AdvisoryTextFindingV1::ControlMarkupNeutralized, 1);
            rendered.push_str(NEUTRALIZED_CONTROL_MARKUP);
            rest = tail.get(consumed..).unwrap_or_default();
        } else {
            rendered.push('<');
            rest = tail.get(1..).unwrap_or_default();
        }
    }
}

/// Byte length of the control-markup run that starts at `tail`, if any.
///
/// `tail` always starts with `<`.
fn control_markup_len(tail: &str) -> Option<usize> {
    let body = tail.get(1..)?;
    if let Some(special) = body.strip_prefix('|') {
        let end = bounded_find(special, "|>")?;
        return Some(1 + 1 + end + 2);
    }
    let name_body = body.strip_prefix('/').unwrap_or(body);
    let slash = usize::from(body.len() != name_body.len());
    let name_len = name_body
        .char_indices()
        .find(|(_, character)| {
            !(character.is_ascii_alphanumeric() || matches!(character, ':' | '_' | '-'))
        })
        .map_or(name_body.len(), |(index, _)| index);
    let name = name_body.get(..name_len)?.to_ascii_lowercase();
    if !CONTROL_MARKUP_TAGS.contains(&name.as_str()) {
        return None;
    }
    let after_name = name_body.get(name_len..)?;
    let end = bounded_find(after_name, ">")?;
    Some(1 + slash + name_len + end + 1)
}

/// Finds `needle` inside the first [`CONTROL_MARKUP_WINDOW_CHARS`] characters
/// of `haystack`, returning its byte offset.
fn bounded_find(haystack: &str, needle: &str) -> Option<usize> {
    let window_end = haystack
        .char_indices()
        .nth(CONTROL_MARKUP_WINDOW_CHARS)
        .map_or(haystack.len(), |(index, _)| index);
    haystack.get(..window_end)?.find(needle)
}

/// Neutralizes a provider-supplied copy of the host's boundary label so an
/// item cannot present its own words as host framing.
fn neutralize_forged_label(text: &str, counts: &mut FindingCounts) -> String {
    if !text.contains(UNTRUSTED_BOUNDARY_LABEL) {
        return text.to_owned();
    }
    let occurrences =
        u32::try_from(text.matches(UNTRUSTED_BOUNDARY_LABEL).count()).unwrap_or(u32::MAX);
    counts.record(
        AdvisoryTextFindingV1::ForgedBoundaryLabelNeutralized,
        occurrences,
    );
    text.replace(UNTRUSTED_BOUNDARY_LABEL, NEUTRALIZED_CONTROL_MARKUP)
}
