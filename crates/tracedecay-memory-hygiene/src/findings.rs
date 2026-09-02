//! Typed findings and the digest that binds them to a receipt.
//!
//! A finding names *what* was detected and *where* in the document structure.
//! It never carries the matched text, a redacted value, or any source byte —
//! that invariant is what lets a finding be logged, digested, and returned in
//! an error without becoming a second copy of the secret.

use tracedecay_domain::canonical_text::canonical_framed_sha256;

use crate::policy::{HygieneAction, HygieneClass};

/// Domain separator for the length-framed findings digest.
pub(crate) const FINDINGS_DIGEST_DOMAIN: &[u8] =
    b"tracedecay.memory.observation.hygiene.findings.v1";

/// Opening delimiter of the opaque marker that stands in for an object key
/// whose own text is credential material.
///
/// The marker replaces such a key everywhere a structural path is rendered —
/// in the finding on the key itself *and* in the location of every finding on
/// a value nested beneath it. A path segment is otherwise the raw key, so
/// leaving it in place would copy the credential into the receipt digest of any
/// descendant finding.
pub const CREDENTIAL_BEARING_KEY_MARKER_PREFIX: &str = "<credential-bearing-key:";

/// Domain separator for the digest inside a credential-bearing key marker.
const CREDENTIAL_BEARING_KEY_DIGEST_DOMAIN: &[u8] = b"tracedecay.memory.observation.hygiene.key.v1";

/// Hex characters of the key digest folded into the marker.
///
/// Long enough that two distinct credential keys in one object stay distinct
/// findings rather than collapsing into one during canonicalization, and short
/// enough that the marker stays readable in an audit row.
const CREDENTIAL_BEARING_KEY_DIGEST_LEN: usize = 16;

/// Renders the opaque marker naming one credential-bearing key slot.
///
/// The key is reduced to a domain-separated one-way digest, so the marker is
/// stable across runs and distinguishes sibling credential keys without any
/// substring of the key surviving into a location.
#[must_use]
pub fn credential_bearing_key_marker(key: &str) -> String {
    let digest = canonical_framed_sha256(CREDENTIAL_BEARING_KEY_DIGEST_DOMAIN, &[key.as_bytes()]);
    let short = digest
        .get(..CREDENTIAL_BEARING_KEY_DIGEST_LEN)
        .unwrap_or(digest.as_str());
    format!("{CREDENTIAL_BEARING_KEY_MARKER_PREFIX}{short}>")
}

/// One typed detection, anchored to a structural location.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HygieneFindingV1 {
    class: HygieneClass,
    action: HygieneAction,
    location: String,
}

impl HygieneFindingV1 {
    /// Records one finding at a structural location.
    ///
    /// `location` is a JSON structural path such as `$.metadata.notes[0]`. It
    /// must never be built from detected content; a key holding credential
    /// material is rendered as [`credential_bearing_key_marker`] instead of the
    /// key text, in this finding and in every descendant's.
    #[must_use]
    pub fn new(class: HygieneClass, action: HygieneAction, location: impl Into<String>) -> Self {
        Self {
            class,
            action,
            location: location.into(),
        }
    }

    /// Returns the detected class.
    #[must_use]
    pub fn class(&self) -> HygieneClass {
        self.class
    }

    /// Returns the action the policy took for this finding.
    #[must_use]
    pub fn action(&self) -> HygieneAction {
        self.action
    }

    /// Returns the structural location.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }
}

/// Sorts and deduplicates findings in place, returning the canonical ordering
/// a receipt digest is taken over.
pub fn canonicalize(findings: &mut Vec<HygieneFindingV1>) {
    findings.sort();
    findings.dedup();
}

/// Lowercase SHA-256 over the canonical finding ordering.
///
/// Callers must canonicalize first; the digest is only stable over the sorted,
/// deduplicated sequence.
#[must_use]
pub fn findings_digest(findings: &[HygieneFindingV1]) -> String {
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(findings.len().saturating_mul(3));
    for finding in findings {
        parts.push(finding.class.as_str().as_bytes().to_vec());
        parts.push(finding.action.as_str().as_bytes().to_vec());
        parts.push(finding.location.as_bytes().to_vec());
    }
    let borrowed: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    canonical_framed_sha256(FINDINGS_DIGEST_DOMAIN, &borrowed)
}

/// One step of a structural JSON path.
///
/// The `CredentialKey` variant is what keeps a credential-bearing key out of
/// every descendant location: the marker is substituted for the key *as the
/// walk descends*, so no code path further down can reach the key text at all.
#[derive(Clone, Debug)]
pub(crate) enum PathSegment<'a> {
    /// An object member.
    Key(&'a str),
    /// An array element.
    Index(usize),
    /// An object member whose own text is credential material, already reduced
    /// to its opaque marker by [`credential_bearing_key_marker`].
    CredentialKey(String),
}

/// Renders a structural path as `$`, `$.key`, `$[0]`, and combinations.
pub(crate) fn render_path(segments: &[PathSegment<'_>]) -> String {
    let mut path = String::from("$");
    for segment in segments {
        match segment {
            PathSegment::Key(key) => {
                path.push('.');
                path.push_str(key);
            }
            PathSegment::CredentialKey(marker) => {
                path.push('.');
                path.push_str(marker);
            }
            PathSegment::Index(index) => {
                path.push('[');
                path.push_str(&index.to_string());
                path.push(']');
            }
        }
    }
    path
}

/// Renders the location of a finding anchored to an object key whose own text
/// is the detected material, so the key never enters the receipt.
pub(crate) fn render_key_location(parent: &[PathSegment<'_>], marker: &str) -> String {
    let mut path = render_path(parent);
    path.push('.');
    path.push_str(marker);
    path
}
