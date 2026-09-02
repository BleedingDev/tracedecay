//! Sanitization receipts that bind one dispatched observation payload to the
//! admitted hygiene pipeline that produced it.
//!
//! The receipt is the only evidence the provider boundary accepts that an
//! observation passed secret and transient-data hygiene. It carries digests
//! and counts exclusively: no matched byte, no redacted value, and no source
//! text ever enters a receipt, a receipt identifier, or an error rendered from
//! this module.
//!
//! Two invariants make the receipt useful rather than decorative:
//!
//! * The identifier is a length-framed SHA-256 over every field, so
//!   [`PayloadSanitizationReceipt::validate`] rejects a receipt whose fields
//!   were edited after minting.
//! * [`ProviderCall::validate`](super::ProviderCall::validate) fails closed for
//!   [`ProviderOperation::Observe`](super::ProviderOperation::Observe) unless a
//!   self-consistent receipt is attached whose sanitized digest equals the
//!   digest of the canonical payload about to be dispatched.
//!
//! The receipt is deliberately serialized and parsed by hand. This crate is the
//! inward dependency root of the provider boundary and its only external crate
//! is `sha2`; a durable journal still has to persist the receipt as opaque text
//! and reconstruct it after a restart, so [`PayloadSanitizationReceipt::to_json`]
//! and [`PayloadSanitizationReceipt::from_json`] provide that round trip
//! without adding a serialization dependency. `from_json` is strict — unknown
//! keys, duplicate keys, missing keys, escapes, and trailing content are all
//! rejected — and always revalidates the derived identifier before returning.

use sha2::{Digest, Sha256};

use super::{ApiError, empty_opaque_extensions_digest, lowercase_sha256_hex, require_sha256};

/// Domain separator for the length-framed sanitization-receipt digest.
const RECEIPT_DIGEST_DOMAIN: &[u8] = b"tracedecay.memory.observation.hygiene.receipt.v1";

/// Stable prefix carried by every derived sanitization-receipt identifier.
pub const OBSERVATION_HYGIENE_RECEIPT_ID_PREFIX: &str = "obs-hygiene-receipt.v1.";

/// Stable prefix carried by every withheld-admission identity.
pub const OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX: &str = "obs-hygiene-withheld.v1.";

/// Domain separator for a withheld-admission identity.
const WITHHELD_DIGEST_DOMAIN: &[u8] = b"tracedecay.memory.observation.hygiene.withheld.v1";

/// Maximum UTF-8 bytes accepted in a sanitizer revision label.
pub const SANITIZER_REVISION_MAX_BYTES: usize = 256;

/// Lowercase SHA-256 of the empty finding set, used by callers that admit a
/// payload no detector flagged.
const EMPTY_FINDINGS_DOMAIN: &[u8] = b"tracedecay.memory.observation.hygiene.findings.v1";

/// Disposition of a payload the hygiene pipeline admitted for delivery.
///
/// Withheld payloads never reach a provider and therefore never carry a
/// disposition; they are reported through [`WithheldReason`] instead.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SanitizationDisposition {
    /// The pipeline changed no byte; delivered bytes equal source bytes.
    Accepted,
    /// The pipeline rewrote at least one span; delivered bytes differ.
    Redacted,
}

impl SanitizationDisposition {
    /// Returns the stable wire spelling of this disposition.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Redacted => "redacted",
        }
    }

    /// Parses a stable wire spelling back into a disposition.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "accepted" => Some(Self::Accepted),
            "redacted" => Some(Self::Redacted),
            _ => None,
        }
    }
}

/// Reason one observation was withheld from every provider.
///
/// A withheld observation is never dispatched, so it never acquires a terminal
/// code. The canonical evidence that produced it is retained untouched; only
/// the provider-bound copy is discarded.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum WithheldReason {
    /// A self-identifying credential class was present and cannot be redacted
    /// into a payload that is still safe to hand a provider.
    SecretRejected,
    /// The payload could not be redacted in place without guessing a span, so
    /// it is held back rather than partially sanitized.
    Quarantined,
    /// The payload's shape — its nesting or its canonical byte length — lies
    /// beyond what the hygiene pipeline will walk, so nothing about its
    /// content was classified and it must not be delivered.
    ///
    /// This is the typed terminal a mounted journey records for a settled
    /// record the pipeline refused as a structural admission error: the
    /// canonical evidence stays untouched, the replay cursor advances, and no
    /// provider work exists. The row carries only digests, never the payload.
    UnclassifiablePayload,
}

impl WithheldReason {
    /// Returns the stable wire spelling of this reason.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SecretRejected => "secret_rejected",
            Self::Quarantined => "quarantined",
            Self::UnclassifiablePayload => "unclassifiable_payload",
        }
    }

    /// Parses a stable wire spelling back into a reason.
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "secret_rejected" => Some(Self::SecretRejected),
            "quarantined" => Some(Self::Quarantined),
            "unclassifiable_payload" => Some(Self::UnclassifiablePayload),
            _ => None,
        }
    }
}

/// Derives the stable identity of one withheld hygiene decision.
///
/// The identity covers every audit field required to independently rederive the
/// decision after restart. It carries only digests, counts, and typed labels;
/// no matched or redacted content enters the result.
#[must_use]
pub fn derive_withheld_receipt_id(
    sanitizer_revision: &str,
    source_payload_sha256: &str,
    extensions_digest: &str,
    reason: WithheldReason,
    finding_count: u32,
    findings_digest: &str,
) -> String {
    let finding_count_bytes = finding_count.to_be_bytes();
    let digest = framed_digest(
        WITHHELD_DIGEST_DOMAIN,
        &[
            sanitizer_revision.as_bytes(),
            source_payload_sha256.as_bytes(),
            extensions_digest.as_bytes(),
            reason.as_str().as_bytes(),
            &finding_count_bytes,
            findings_digest.as_bytes(),
        ],
    );
    format!(
        "{OBSERVATION_HYGIENE_WITHHELD_ID_PREFIX}{}",
        lowercase_sha256_hex(digest)
    )
}

/// Constructor inputs for one sanitization receipt.
///
/// Every digest is lowercase SHA-256 hex. `source_payload_sha256` names the
/// bytes the pipeline read; `sanitized_payload_sha256` names the bytes it is
/// willing to see delivered, which for [`SanitizationDisposition::Accepted`]
/// are the same bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadSanitizationReceiptParts {
    /// Stable identity of the sanitizer and policy revision that ran.
    pub sanitizer_revision: String,
    /// Lowercase SHA-256 of the canonical bytes the pipeline read.
    pub source_payload_sha256: String,
    /// Lowercase SHA-256 of the canonical bytes cleared for delivery.
    pub sanitized_payload_sha256: String,
    /// Lowercase SHA-256 of the exact canonical extension set cleared for delivery.
    pub extensions_digest: String,
    /// Whether the pipeline rewrote any span.
    pub disposition: SanitizationDisposition,
    /// Number of distinct findings the pipeline recorded.
    pub finding_count: u32,
    /// Lowercase SHA-256 over the sorted, deduplicated finding descriptors.
    pub findings_digest: String,
}

impl PayloadSanitizationReceiptParts {
    /// Parts for a payload the pipeline read and left byte-identical.
    ///
    /// This is the shape every call site that dispatches an already-clean
    /// canonical payload needs, and it is the only way to build receipt parts
    /// without restating the "accepted means unmodified" invariant by hand.
    #[must_use]
    pub fn accepted_unmodified(
        sanitizer_revision: impl Into<String>,
        payload_sha256: impl Into<String>,
    ) -> Self {
        let payload_sha256 = payload_sha256.into();
        Self {
            sanitizer_revision: sanitizer_revision.into(),
            source_payload_sha256: payload_sha256.clone(),
            sanitized_payload_sha256: payload_sha256,
            extensions_digest: empty_opaque_extensions_digest(),
            disposition: SanitizationDisposition::Accepted,
            finding_count: 0,
            findings_digest: empty_findings_digest(),
        }
    }

    /// Parts for payload and extensions the pipeline left byte-identical.
    #[must_use]
    pub fn accepted_unmodified_with_extensions(
        sanitizer_revision: impl Into<String>,
        payload_sha256: impl Into<String>,
        extensions_digest: impl Into<String>,
    ) -> Self {
        let payload_sha256 = payload_sha256.into();
        Self {
            sanitizer_revision: sanitizer_revision.into(),
            source_payload_sha256: payload_sha256.clone(),
            sanitized_payload_sha256: payload_sha256,
            extensions_digest: extensions_digest.into(),
            disposition: SanitizationDisposition::Accepted,
            finding_count: 0,
            findings_digest: empty_findings_digest(),
        }
    }
}

/// Lowercase SHA-256 naming the empty finding set.
#[must_use]
pub fn empty_findings_digest() -> String {
    lowercase_sha256_hex(framed_digest(EMPTY_FINDINGS_DOMAIN, &[]))
}

/// Proof that one payload passed the admitted observation-hygiene pipeline.
///
/// Fields are private and the identifier is derived from all of them, so a
/// receipt cannot be re-pointed at other bytes, re-labelled with another
/// disposition, or backdated to a different sanitizer revision without
/// [`PayloadSanitizationReceipt::validate`] noticing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadSanitizationReceipt {
    receipt_id: String,
    sanitizer_revision: String,
    source_payload_sha256: String,
    sanitized_payload_sha256: String,
    extensions_digest: String,
    disposition: SanitizationDisposition,
    finding_count: u32,
    findings_digest: String,
}

impl PayloadSanitizationReceipt {
    /// Mints a receipt and derives its identifier from every field.
    pub fn new(parts: PayloadSanitizationReceiptParts) -> Result<Self, ApiError> {
        let PayloadSanitizationReceiptParts {
            sanitizer_revision,
            source_payload_sha256,
            sanitized_payload_sha256,
            extensions_digest,
            disposition,
            finding_count,
            findings_digest,
        } = parts;
        require_sanitizer_revision(&sanitizer_revision)?;
        require_sha256(&source_payload_sha256, "source_payload_sha256")?;
        require_sha256(&sanitized_payload_sha256, "sanitized_payload_sha256")?;
        require_sha256(&extensions_digest, "sanitization_extensions_digest")?;
        require_sha256(&findings_digest, "sanitization_findings_digest")?;
        require_disposition_matches_digests(
            disposition,
            &source_payload_sha256,
            &sanitized_payload_sha256,
        )?;
        let receipt_id = derive_receipt_id(
            &sanitizer_revision,
            &source_payload_sha256,
            &sanitized_payload_sha256,
            &extensions_digest,
            disposition,
            finding_count,
            &findings_digest,
        );
        Ok(Self {
            receipt_id,
            sanitizer_revision,
            source_payload_sha256,
            sanitized_payload_sha256,
            extensions_digest,
            disposition,
            finding_count,
            findings_digest,
        })
    }

    /// Returns the derived receipt identifier.
    #[must_use]
    pub fn receipt_id(&self) -> &str {
        &self.receipt_id
    }

    /// Returns the sanitizer and policy revision that produced this receipt.
    #[must_use]
    pub fn sanitizer_revision(&self) -> &str {
        &self.sanitizer_revision
    }

    /// Returns the digest of the bytes the pipeline read.
    #[must_use]
    pub fn source_payload_sha256(&self) -> &str {
        &self.source_payload_sha256
    }

    /// Returns the digest of the bytes cleared for delivery.
    #[must_use]
    pub fn sanitized_payload_sha256(&self) -> &str {
        &self.sanitized_payload_sha256
    }

    /// Returns the digest of the exact extension set cleared for delivery.
    #[must_use]
    pub fn extensions_digest(&self) -> &str {
        &self.extensions_digest
    }

    /// Returns whether the pipeline rewrote any span.
    #[must_use]
    pub fn disposition(&self) -> SanitizationDisposition {
        self.disposition
    }

    /// Returns the number of distinct findings recorded.
    #[must_use]
    pub fn finding_count(&self) -> u32 {
        self.finding_count
    }

    /// Returns the digest over the recorded findings.
    #[must_use]
    pub fn findings_digest(&self) -> &str {
        &self.findings_digest
    }

    /// Recomputes every invariant and the derived identifier.
    ///
    /// Callers that reconstruct a receipt from durable storage must run this
    /// before trusting it; [`PayloadSanitizationReceipt::from_json`] already
    /// does.
    pub fn validate(&self) -> Result<(), ApiError> {
        require_sanitizer_revision(&self.sanitizer_revision)?;
        require_sha256(&self.source_payload_sha256, "source_payload_sha256")?;
        require_sha256(&self.sanitized_payload_sha256, "sanitized_payload_sha256")?;
        require_sha256(&self.extensions_digest, "sanitization_extensions_digest")?;
        require_sha256(&self.findings_digest, "sanitization_findings_digest")?;
        require_disposition_matches_digests(
            self.disposition,
            &self.source_payload_sha256,
            &self.sanitized_payload_sha256,
        )?;
        let expected = derive_receipt_id(
            &self.sanitizer_revision,
            &self.source_payload_sha256,
            &self.sanitized_payload_sha256,
            &self.extensions_digest,
            self.disposition,
            self.finding_count,
            &self.findings_digest,
        );
        if expected != self.receipt_id {
            return Err(ApiError::SanitizationReceiptTampered);
        }
        Ok(())
    }

    /// Proves this receipt describes exactly the bytes about to be dispatched.
    pub fn verify_binding(
        &self,
        payload_sha256: &str,
        extensions_digest: &str,
    ) -> Result<(), ApiError> {
        self.validate()?;
        if self.sanitized_payload_sha256 != payload_sha256
            || self.extensions_digest != extensions_digest
        {
            return Err(ApiError::SanitizationReceiptUnbound);
        }
        Ok(())
    }

    /// Serializes the receipt to the canonical flat JSON object a durable
    /// journal stores verbatim.
    ///
    /// Every value is a constrained string or an unsigned integer, so the
    /// encoding never needs an escape sequence.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut output = String::with_capacity(512);
        output.push('{');
        push_json_string_field(&mut output, "receipt_id", &self.receipt_id, true);
        push_json_string_field(
            &mut output,
            "sanitizer_revision",
            &self.sanitizer_revision,
            false,
        );
        push_json_string_field(
            &mut output,
            "source_payload_sha256",
            &self.source_payload_sha256,
            false,
        );
        push_json_string_field(
            &mut output,
            "sanitized_payload_sha256",
            &self.sanitized_payload_sha256,
            false,
        );
        push_json_string_field(
            &mut output,
            "extensions_digest",
            &self.extensions_digest,
            false,
        );
        push_json_string_field(&mut output, "disposition", self.disposition.as_str(), false);
        output.push_str(",\"finding_count\":");
        output.push_str(&self.finding_count.to_string());
        push_json_string_field(&mut output, "findings_digest", &self.findings_digest, false);
        output.push('}');
        output
    }

    /// Parses a receipt previously produced by
    /// [`PayloadSanitizationReceipt::to_json`] and revalidates it.
    ///
    /// Unknown keys, duplicate keys, missing keys, string escapes, and trailing
    /// content are rejected: a restarted dispatcher must reconstruct exactly the
    /// receipt that was minted, never an approximation of it.
    pub fn from_json(source: &str) -> Result<Self, ApiError> {
        let receipt = parse_receipt_json(source)?;
        receipt.validate()?;
        Ok(receipt)
    }
}

fn require_disposition_matches_digests(
    disposition: SanitizationDisposition,
    source_payload_sha256: &str,
    sanitized_payload_sha256: &str,
) -> Result<(), ApiError> {
    match disposition {
        SanitizationDisposition::Accepted if sanitized_payload_sha256 != source_payload_sha256 => {
            Err(ApiError::SanitizationAcceptedPayloadModified)
        }
        SanitizationDisposition::Redacted if sanitized_payload_sha256 == source_payload_sha256 => {
            Err(ApiError::SanitizationRedactedPayloadUnmodified)
        }
        _ => Ok(()),
    }
}

fn require_sanitizer_revision(value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > SANITIZER_REVISION_MAX_BYTES {
        return Err(ApiError::InvalidSanitizerRevision);
    }
    let printable_ascii_without_json_escapes = value
        .bytes()
        .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b'"' && byte != b'\\');
    if printable_ascii_without_json_escapes {
        Ok(())
    } else {
        Err(ApiError::InvalidSanitizerRevision)
    }
}

fn derive_receipt_id(
    sanitizer_revision: &str,
    source_payload_sha256: &str,
    sanitized_payload_sha256: &str,
    extensions_digest: &str,
    disposition: SanitizationDisposition,
    finding_count: u32,
    findings_digest: &str,
) -> String {
    let finding_count_bytes = finding_count.to_be_bytes();
    let digest = framed_digest(
        RECEIPT_DIGEST_DOMAIN,
        &[
            sanitizer_revision.as_bytes(),
            source_payload_sha256.as_bytes(),
            sanitized_payload_sha256.as_bytes(),
            extensions_digest.as_bytes(),
            disposition.as_str().as_bytes(),
            &finding_count_bytes,
            findings_digest.as_bytes(),
        ],
    );
    let mut receipt_id = String::with_capacity(OBSERVATION_HYGIENE_RECEIPT_ID_PREFIX.len() + 64);
    receipt_id.push_str(OBSERVATION_HYGIENE_RECEIPT_ID_PREFIX);
    receipt_id.push_str(&lowercase_sha256_hex(digest));
    receipt_id
}

/// Length-framed SHA-256 over a domain separator and an ordered part list.
///
/// Every frame, the domain tag included, is preceded by its big-endian `u64`
/// length, so no two different splits of the same concatenated bytes collide.
fn framed_digest(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn push_json_string_field(output: &mut String, key: &str, value: &str, first: bool) {
    if !first {
        output.push(',');
    }
    output.push('"');
    output.push_str(key);
    output.push_str("\":\"");
    output.push_str(value);
    output.push('"');
}

const RECEIPT_JSON_KEYS: [&str; 8] = [
    "receipt_id",
    "sanitizer_revision",
    "source_payload_sha256",
    "sanitized_payload_sha256",
    "extensions_digest",
    "disposition",
    "finding_count",
    "findings_digest",
];

fn parse_receipt_json(source: &str) -> Result<PayloadSanitizationReceipt, ApiError> {
    let bytes = source.as_bytes();
    let mut cursor = skip_whitespace(bytes, 0);
    if bytes.get(cursor) != Some(&b'{') {
        return Err(ApiError::MalformedSanitizationReceiptJson("object"));
    }
    cursor = skip_whitespace(bytes, cursor.saturating_add(1));

    let mut receipt_id: Option<String> = None;
    let mut sanitizer_revision: Option<String> = None;
    let mut source_payload_sha256: Option<String> = None;
    let mut sanitized_payload_sha256: Option<String> = None;
    let mut extensions_digest: Option<String> = None;
    let mut disposition: Option<SanitizationDisposition> = None;
    let mut finding_count: Option<u32> = None;
    let mut findings_digest: Option<String> = None;

    if bytes.get(cursor) == Some(&b'}') {
        return Err(ApiError::MalformedSanitizationReceiptJson("missing_field"));
    }

    loop {
        let (key, next) = parse_json_string(bytes, cursor)?;
        if !RECEIPT_JSON_KEYS.contains(&key.as_str()) {
            return Err(ApiError::MalformedSanitizationReceiptJson("unknown_field"));
        }
        cursor = skip_whitespace(bytes, next);
        if bytes.get(cursor) != Some(&b':') {
            return Err(ApiError::MalformedSanitizationReceiptJson("separator"));
        }
        cursor = skip_whitespace(bytes, cursor.saturating_add(1));

        if key == "finding_count" {
            let (value, next) = parse_json_u32(bytes, cursor)?;
            if finding_count.replace(value).is_some() {
                return Err(ApiError::MalformedSanitizationReceiptJson(
                    "duplicate_field",
                ));
            }
            cursor = skip_whitespace(bytes, next);
        } else {
            let (value, next) = parse_json_string(bytes, cursor)?;
            let duplicated = match key.as_str() {
                "receipt_id" => receipt_id.replace(value).is_some(),
                "sanitizer_revision" => sanitizer_revision.replace(value).is_some(),
                "source_payload_sha256" => source_payload_sha256.replace(value).is_some(),
                "sanitized_payload_sha256" => sanitized_payload_sha256.replace(value).is_some(),
                "extensions_digest" => extensions_digest.replace(value).is_some(),
                "findings_digest" => findings_digest.replace(value).is_some(),
                _ => {
                    let parsed = SanitizationDisposition::from_wire(&value)
                        .ok_or(ApiError::MalformedSanitizationReceiptJson("disposition"))?;
                    disposition.replace(parsed).is_some()
                }
            };
            if duplicated {
                return Err(ApiError::MalformedSanitizationReceiptJson(
                    "duplicate_field",
                ));
            }
            cursor = skip_whitespace(bytes, next);
        }

        match bytes.get(cursor) {
            Some(&b',') => cursor = skip_whitespace(bytes, cursor.saturating_add(1)),
            Some(&b'}') => {
                cursor = skip_whitespace(bytes, cursor.saturating_add(1));
                break;
            }
            _ => return Err(ApiError::MalformedSanitizationReceiptJson("object")),
        }
    }

    if cursor != bytes.len() {
        return Err(ApiError::MalformedSanitizationReceiptJson(
            "trailing_content",
        ));
    }

    let missing = ApiError::MalformedSanitizationReceiptJson("missing_field");
    Ok(PayloadSanitizationReceipt {
        receipt_id: receipt_id.ok_or(missing.clone())?,
        sanitizer_revision: sanitizer_revision.ok_or(missing.clone())?,
        source_payload_sha256: source_payload_sha256.ok_or(missing.clone())?,
        sanitized_payload_sha256: sanitized_payload_sha256.ok_or(missing.clone())?,
        extensions_digest: extensions_digest.ok_or(missing.clone())?,
        disposition: disposition.ok_or(missing.clone())?,
        finding_count: finding_count.ok_or(missing.clone())?,
        findings_digest: findings_digest.ok_or(missing)?,
    })
}

fn skip_whitespace(bytes: &[u8], from: usize) -> usize {
    let mut cursor = from;
    while matches!(bytes.get(cursor), Some(&b' ' | &b'\t' | &b'\n' | &b'\r')) {
        cursor = cursor.saturating_add(1);
    }
    cursor
}

fn parse_json_string(bytes: &[u8], from: usize) -> Result<(String, usize), ApiError> {
    if bytes.get(from) != Some(&b'"') {
        return Err(ApiError::MalformedSanitizationReceiptJson("string"));
    }
    let start = from.saturating_add(1);
    let mut cursor = start;
    loop {
        match bytes.get(cursor) {
            Some(&b'"') => break,
            // The encoder only ever emits constrained ASCII, so an escape or a
            // control byte means this text was not produced by `to_json`.
            Some(&b'\\') | None => {
                return Err(ApiError::MalformedSanitizationReceiptJson("string"));
            }
            Some(byte) if *byte < 0x20 => {
                return Err(ApiError::MalformedSanitizationReceiptJson("string"));
            }
            Some(_) => cursor = cursor.saturating_add(1),
        }
    }
    let text = core::str::from_utf8(bytes.get(start..cursor).unwrap_or_default())
        .map_err(|_| ApiError::MalformedSanitizationReceiptJson("string"))?;
    Ok((text.to_owned(), cursor.saturating_add(1)))
}

fn parse_json_u32(bytes: &[u8], from: usize) -> Result<(u32, usize), ApiError> {
    let mut cursor = from;
    let mut value: u32 = 0;
    let mut digits = 0_usize;
    while let Some(byte) = bytes.get(cursor) {
        if !byte.is_ascii_digit() {
            break;
        }
        // A leading zero followed by another digit is not canonical JSON as
        // this module emits it.
        if digits == 1 && value == 0 {
            return Err(ApiError::MalformedSanitizationReceiptJson("number"));
        }
        value = value
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(u32::from(byte - b'0')))
            .ok_or(ApiError::MalformedSanitizationReceiptJson("number"))?;
        digits = digits.saturating_add(1);
        cursor = cursor.saturating_add(1);
    }
    if digits == 0 {
        return Err(ApiError::MalformedSanitizationReceiptJson("number"));
    }
    Ok((value, cursor))
}
