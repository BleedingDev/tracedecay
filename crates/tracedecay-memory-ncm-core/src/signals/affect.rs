//! Four-channel affect construction and the optional keyword heuristic.
//!
//! The keyword policy deliberately preserves the reference's substring
//! matching. It does not tokenize, understand negation, or infer human emotion:
//! for example, `content` matches inside `contentment`, and `not great` still
//! boosts dopamine. Callers must retain [`SignalSource`] as policy identity.

use super::SignalSource;
use crate::types::{AffectVector, CoreError};
use std::borrow::Borrow;

const DOPAMINE_POSITIVE: &[&str] = &[
    "radost",
    "úspěch",
    "skvělé",
    "výborně",
    "super",
    "hurá",
    "joy",
    "success",
    "great",
    "excellent",
    "amazing",
    "win",
];
const SEROTONIN_POSITIVE: &[&str] = &[
    "klid",
    "pohoda",
    "spokojenost",
    "mír",
    "harmonie",
    "calm",
    "peace",
    "satisfied",
    "content",
    "balanced",
];
const SEROTONIN_NEGATIVE: &[&str] = &["strach", "úzkost", "panika", "fear", "anxiety", "panic"];
const CORTISOL_POSITIVE: &[&str] = &[
    "chyba",
    "problém",
    "strach",
    "nemoc",
    "špatně",
    "nebezpečí",
    "error",
    "problem",
    "fear",
    "illness",
    "bad",
    "danger",
];
const OXYTOCIN_POSITIVE: &[&str] = &[
    "vztah",
    "člověk",
    "pomoc",
    "děkuji",
    "přátelství",
    "láska",
    "relation",
    "help",
    "thank",
    "friendship",
    "love",
    "together",
];

/// Returns explicit neutral affect `[1, 1, 1, 1]`.
#[must_use]
pub const fn neutral() -> AffectVector {
    AffectVector::neutral()
}

/// Constructs an affect vector while rejecting non-finite channels.
pub fn validated(values: [f32; 4]) -> Result<AffectVector, CoreError> {
    AffectVector::validated(values)
}

/// Creates a reference-compatible preset by exact, case-sensitive name.
///
/// Unknown names are neutral, matching `EmotionExtractor.from_name`. Direct
/// channel names (`dopamin`, `serotonin`, `kortizol`, `oxytocin`) are aliases
/// accepted by the executed reference.
pub fn from_name(name: &str) -> Result<AffectVector, CoreError> {
    let values = match name {
        "positive" | "dopamin" => [1.3, 1.0, 1.0, 1.0],
        "curious" | "serotonin" => [1.0, 1.2, 1.0, 1.0],
        "negative" | "stressed" | "kortizol" => [1.0, 1.0, 1.3, 1.0],
        "social" | "oxytocin" => [1.0, 1.0, 1.0, 1.25],
        _ => [1.0; 4],
    };
    AffectVector::validated(values)
}

/// Creates an affect vector from the reference's exact dictionary spellings.
///
/// The accepted keys are `dopamin`, `serotonin`, `kortizol`, and `oxytocin`.
/// Despite the Python docstring, `dopamine` and `cortisol` are not aliases in
/// executed behavior. Missing or unknown finite keys leave channels neutral.
pub fn from_dict<I, K, V>(dictionary: I) -> Result<AffectVector, CoreError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Borrow<f32>,
{
    let mut affect = [1.0; 4];
    for (key, value) in dictionary {
        let value = *value.borrow();
        if !value.is_finite() {
            return Err(CoreError::NonFinite("affect dictionary"));
        }
        match key.as_ref() {
            "dopamin" => affect[0] = value,
            "serotonin" => affect[1] = value,
            "kortizol" => affect[2] = value,
            "oxytocin" => affect[3] = value,
            _ => {}
        }
    }
    AffectVector::validated(affect)
}

/// Applies the pinned Czech/English keyword substring heuristic.
///
/// Each listed word contributes at most once, regardless of repetition. Values
/// are clamped per channel to `[0.5, 2.0]`. Substrings and negated phrases are
/// intentionally not corrected because doing so would change the versioned
/// policy.
#[must_use]
pub fn extract_keyword_affect(text: &str) -> (AffectVector, SignalSource) {
    let lowercase = text.to_lowercase();
    let dopamine = channel_value(&lowercase, DOPAMINE_POSITIVE, &[], 0.3, 0.0);
    let serotonin = channel_value(
        &lowercase,
        SEROTONIN_POSITIVE,
        SEROTONIN_NEGATIVE,
        0.2,
        -0.15,
    );
    let cortisol = channel_value(&lowercase, CORTISOL_POSITIVE, &[], 0.3, 0.0);
    let oxytocin = channel_value(&lowercase, OXYTOCIN_POSITIVE, &[], 0.25, 0.0);
    (
        AffectVector([dopamine, serotonin, cortisol, oxytocin]),
        SignalSource::KeywordHeuristicV1,
    )
}

fn channel_value(
    text: &str,
    positive: &[&str],
    negative: &[&str],
    boost: f32,
    penalty: f32,
) -> f32 {
    let mut value = 1.0_f32;
    for word in positive {
        if text.contains(word) {
            value += boost;
        }
    }
    for word in negative {
        if text.contains(word) {
            value += penalty;
        }
    }
    value.clamp(0.5, 2.0)
}
