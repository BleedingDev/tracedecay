//! Every credential class one string carries, not just the first one.
//!
//! `tracedecay_runtime_core::memory::hygiene::detect_secret_like` is the single
//! owner of the credential corpus, and this crate keeps it that way. Its public
//! answer, however, is `Option<String>`: the reason of the **first** pattern
//! that matched. A string such as an assignment whose value is a live issuer
//! token matches two patterns, and which reason comes back is decided by
//! catalogue ordering. Treating that one reason as the classification of the
//! whole string is how a payload that must be withheld ends up merely redacted,
//! because `credential_assignment` is a `redact` class while
//! `known_credential_prefix` is on the reject floor.
//!
//! There is no public surface that enumerates the matched spans, so this module
//! runs a supplementary multi-signal pass declared by
//! `reject_floor_signals` in the policy document:
//!
//! * the shared detector's own first answer is always taken, unchanged;
//! * three direct signals — a PEM armour header, a presented bearer token, and
//!   a declared issuer prefix followed by a long enough credential run — are
//!   checked against the whole string, because those three shapes span the
//!   whitespace or prefix boundaries a probe cannot re-derive;
//! * the shared detector is then re-probed over bounded candidate substrings,
//!   which is how a high-entropy token hidden behind an earlier pattern match
//!   is still proven by the corpus that owns the entropy threshold rather than
//!   by a local copy of it.
//!
//! The pass can only *add* reject-floor classes. Every signal class is checked
//! against the reject floor when the policy is parsed, an exhausted probe
//! budget classifies as [`HygieneClass::DetectorUnavailable`], and a detector
//! reason this build does not recognise already maps to the same class — so
//! every uncertain outcome fails closed. Nothing here retains a matched byte:
//! the only value that leaves this module is a set of class identities.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use regex::Regex;
use tracedecay_runtime_core::memory::hygiene::detect_secret_like;

use crate::policy::{HygieneClass, ObservationHygienePolicyV1, RejectFloorSignals};

/// Minimum length of the token after `bearer` before the direct signal fires.
///
/// This mirrors `tracedecay-bearer-token-memory` in the shared rule supplement
/// exactly, so the direct signal is the upstream memory-profile rule re-asked
/// on its own rather than a second, differently tuned rule.
const BEARER_MINIMUM_TOKEN_LENGTH: usize = 20;

/// PEM armour header, mirroring `tracedecay-private-key-header` upstream.
const PRIVATE_KEY_HEADER_PATTERN: &str = r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY( BLOCK)?-----";

struct DirectPatterns {
    private_key: Regex,
    bearer: Regex,
}

fn direct_patterns() -> Option<&'static DirectPatterns> {
    static PATTERNS: OnceLock<Option<DirectPatterns>> = OnceLock::new();
    PATTERNS
        .get_or_init(|| {
            Some(DirectPatterns {
                private_key: Regex::new(PRIVATE_KEY_HEADER_PATTERN).ok()?,
                bearer: Regex::new(&format!(
                    r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{{{BEARER_MINIMUM_TOKEN_LENGTH},}}"
                ))
                .ok()?,
            })
        })
        .as_ref()
}

/// Bounded supplementary-probe allowance for one payload.
///
/// Probing is deliberately cheap on ordinary knowledge — a candidate has to
/// clear the entropy detector's own preconditions before it costs a detector
/// call, and prose has no such tokens — but a payload engineered to be nothing
/// but long mixed-case tokens could otherwise spend unbounded scan work. The
/// budget is spent per payload, and exhausting it is classified rather than
/// ignored, so the answer to "we stopped looking" is a reject-floor class.
pub(crate) struct ProbeBudget {
    remaining: usize,
}

impl ProbeBudget {
    /// Opens a budget of `maximum` supplementary detector probes.
    pub(crate) fn new(maximum: usize) -> Self {
        Self { remaining: maximum }
    }

    fn spend(&mut self) -> bool {
        match self.remaining.checked_sub(1) {
            Some(remaining) => {
                self.remaining = remaining;
                true
            }
            None => false,
        }
    }
}

/// Returns every credential class this build can prove `text` carries.
///
/// The result is sorted and deduplicated. An empty result means no signal
/// fired; it never means "the detectors were unavailable", which is itself a
/// class.
pub(crate) fn credential_classes(
    text: &str,
    policy: &ObservationHygienePolicyV1,
    budget: &mut ProbeBudget,
) -> Vec<HygieneClass> {
    let mut classes: BTreeSet<HygieneClass> = BTreeSet::new();

    // The shared corpus' own answer, taken exactly as it is given.
    if let Some(reason) = detect_secret_like(text) {
        classes.insert(HygieneClass::for_detector_reason(&reason));
    }

    let Some(direct) = direct_patterns() else {
        // A supplementary detector that failed to compile has proven nothing
        // about the classes the first answer may have hidden.
        classes.insert(HygieneClass::DetectorUnavailable);
        return classes.into_iter().collect();
    };
    let signals = policy.signals();
    if direct.private_key.is_match(text) {
        classes.insert(HygieneClass::PrivateKey);
    }
    if direct.bearer.is_match(text) {
        classes.insert(HygieneClass::BearerToken);
    }
    match carries_known_credential_prefix(text, signals, budget) {
        Ok(true) => {
            classes.insert(HygieneClass::KnownCredentialPrefix);
        }
        Ok(false) => {}
        Err(()) => {
            classes.insert(HygieneClass::DetectorUnavailable);
        }
    }
    if classes.iter().any(|class| policy.is_reject_floor(*class)) {
        // The payload is already withheld; probing further would only spend
        // budget to reach the same answer.
        return classes.into_iter().collect();
    }

    for candidate in probe_candidates(text, signals) {
        if !budget.spend() {
            classes.insert(HygieneClass::DetectorUnavailable);
            break;
        }
        if let Some(reason) = detect_secret_like(candidate) {
            classes.insert(HygieneClass::for_detector_reason(&reason));
            if classes.iter().any(|class| policy.is_reject_floor(*class)) {
                break;
            }
        }
    }
    classes.into_iter().collect()
}

/// Returns whether the shared detector corroborates a declared issuer prefix
/// at a token boundary.
///
/// Prefix spelling and run length only select bounded candidates. They are not
/// themselves proof: ordinary identifiers such as `npm_config_registry` and
/// `sk-learn-preprocessing` share issuer-looking prefixes and can be long. The
/// corpus owner must classify the exact token as a known credential before this
/// supplementary pass adds the reject-floor class.
fn carries_known_credential_prefix(
    text: &str,
    signals: &RejectFloorSignals,
    budget: &mut ProbeBudget,
) -> Result<bool, ()> {
    for prefix in signals.known_credential_prefixes() {
        let mut searched = 0usize;
        while let Some(offset) = text
            .get(searched..)
            .and_then(|rest| rest.find(prefix.as_str()))
        {
            let start = searched.saturating_add(offset);
            let boundary = text
                .get(..start)
                .and_then(|before| before.chars().next_back())
                .is_none_or(|character| !is_credential_character(character));
            let run_length = text
                .get(start..)
                .map(credential_run_length)
                .unwrap_or_default();
            if boundary && run_length >= signals.minimum_credential_run_length() {
                if !budget.spend() {
                    return Err(());
                }
                let candidate = text
                    .get(start..start.saturating_add(run_length))
                    .ok_or(())?;
                if detect_secret_like(candidate)
                    .map(|reason| HygieneClass::for_detector_reason(&reason))
                    == Some(HygieneClass::KnownCredentialPrefix)
                {
                    return Ok(true);
                }
            }
            searched = start.saturating_add(prefix.len());
        }
    }
    Ok(false)
}

fn is_credential_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_' || character == '-'
}

fn credential_run_length(text: &str) -> usize {
    text.chars()
        .take_while(|character| is_credential_character(*character))
        .map(char::len_utf8)
        .sum()
}

/// Derives the bounded candidate substrings the shared detector is re-probed
/// over.
///
/// Only candidates that could clear the shared entropy detector's own
/// preconditions are returned — at least the declared minimum length, mixed
/// letters and digits, and not pure hex — so a git SHA, a UUID, a long
/// identifier, and ordinary prose cost nothing. Each whitespace token is
/// considered whole and, when it contains one, split on the declared
/// separators: `api_key=<token>` is one whitespace token whose value only
/// becomes visible to the entropy detector once the name in front of it is no
/// longer diluting the score.
fn probe_candidates<'a>(text: &'a str, signals: &RejectFloorSignals) -> Vec<&'a str> {
    let separators = signals.candidate_separators();
    let mut candidates: BTreeSet<&'a str> = BTreeSet::new();
    for token in text.split_whitespace() {
        if is_probe_candidate(token, signals) {
            candidates.insert(token);
        }
        for part in token.split(|character| separators.contains(&character)) {
            if part.len() != token.len() && is_probe_candidate(part, signals) {
                candidates.insert(part);
            }
        }
    }
    candidates.into_iter().collect()
}

/// Returns whether a candidate is worth one shared-detector call.
///
/// These are the shared entropy detector's own preconditions, deliberately
/// stated a little more loosely than upstream states them: the filter must
/// never be the reason a token is not looked at, only the reason an obviously
/// uninteresting one is skipped.
fn is_probe_candidate(candidate: &str, signals: &RejectFloorSignals) -> bool {
    let trimmed = candidate.trim_matches(|character: char| !character.is_ascii_alphanumeric());
    trimmed.len() >= signals.entropy_candidate_minimum_length()
        && trimmed.bytes().any(|byte| byte.is_ascii_alphabetic())
        && trimmed.bytes().any(|byte| byte.is_ascii_digit())
        && !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
}
