//! Checked scalar and vector kernels used by the NCM core.
//!
//! The kernels use `f32` arithmetic deliberately. Inputs are validated before
//! any arithmetic, and an arithmetic result that becomes non-finite is reported
//! as a typed [`CoreError`] rather than being allowed into persisted state.

use crate::types::CoreError;

/// Denominator floor used by `torch.nn.functional.normalize`.
pub const NORMALIZE_EPSILON: f32 = 1e-12;
/// Additive epsilon used for both terms of Biomem's intensity softmax.
pub const WEIGHT_EPSILON: f32 = 1e-8;

/// Rejects NaN and infinity in a slice.
pub fn validate_finite(values: &[f32], what: &'static str) -> Result<(), CoreError> {
    if values.iter().all(|value| value.is_finite()) {
        Ok(())
    } else {
        Err(CoreError::NonFinite(what))
    }
}

/// Checks a slice length before it is used by a numerical operation.
pub fn validate_dimension(
    values: &[f32],
    expected: usize,
    what: &'static str,
) -> Result<(), CoreError> {
    if values.len() == expected {
        Ok(())
    } else {
        Err(CoreError::DimensionMismatch {
            what,
            expected,
            actual: values.len(),
        })
    }
}

fn checked(value: f32, what: &'static str) -> Result<f32, CoreError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(CoreError::NonFinite(what))
    }
}

fn validate_pair(left: &[f32], right: &[f32]) -> Result<(), CoreError> {
    validate_dimension(right, left.len(), "vector pair")?;
    validate_finite(left, "left vector")?;
    validate_finite(right, "right vector")
}

/// Computes a dot product with `f32` accumulation.
pub fn dot(left: &[f32], right: &[f32]) -> Result<f32, CoreError> {
    validate_pair(left, right)?;
    let product_sum: f32 = left.iter().zip(right).map(|(a, b)| a * b).sum();
    checked(product_sum, "dot product")
}

/// Computes the Euclidean L2 norm with `f32` square, sum, and square-root steps.
pub fn l2_norm(values: &[f32]) -> Result<f32, CoreError> {
    checked(dot(values, values)?.sqrt(), "L2 norm")
}

/// Normalizes as PyTorch does: `x / max(||x||, 1e-12)`.
pub fn normalize(values: &[f32]) -> Result<Vec<f32>, CoreError> {
    validate_finite(values, "normalize input")?;
    let denominator = l2_norm(values)?.max(NORMALIZE_EPSILON);
    let normalized: Vec<f32> = values.iter().map(|value| *value / denominator).collect();
    validate_finite(&normalized, "normalize output")?;
    Ok(normalized)
}

/// Computes a clamped dot-product cosine similarity.
pub fn cosine(left: &[f32], right: &[f32]) -> Result<f32, CoreError> {
    checked(dot(left, right)?.clamp(-1.0, 1.0), "cosine")
}

/// Computes Biomem's normalized-key squared distance, `2 - 2*cosine`.
pub fn squared_distance(left: &[f32], right: &[f32]) -> Result<f32, CoreError> {
    checked(2.0 - 2.0 * cosine(left, right)?, "squared distance")
}

/// Computes the Gaussian RBF weight `exp(-d² / (2σ²))`.
pub fn rbf_weight(distance_squared: f32, sigma: f32) -> Result<f32, CoreError> {
    validate_finite(&[distance_squared, sigma], "RBF inputs")?;
    if distance_squared < 0.0 || sigma <= 0.0 {
        return Err(CoreError::InvalidState(
            "RBF requires d2 >= 0 and sigma > 0".to_owned(),
        ));
    }
    let sigma_squared = checked(sigma * sigma, "RBF sigma squared")?;
    let denominator = checked(2.0 * sigma_squared, "RBF denominator")?;
    if denominator == 0.0 {
        return Err(CoreError::InvalidState(
            "RBF sigma squared underflows".to_owned(),
        ));
    }
    checked((-distance_squared / denominator).exp(), "RBF weight")
}

/// Alias for [`rbf_weight`] when the surrounding code calls the kernel `rbf`.
pub fn rbf(distance_squared: f32, sigma: f32) -> Result<f32, CoreError> {
    rbf_weight(distance_squared, sigma)
}

/// Computes a numerically stable softmax from finite log weights.
pub fn softmax(log_weights: &[f32]) -> Result<Vec<f32>, CoreError> {
    validate_finite(log_weights, "softmax logits")?;
    if log_weights.is_empty() {
        return Ok(Vec::new());
    }
    let maximum = log_weights
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);
    let mut exponentials = Vec::with_capacity(log_weights.len());
    for value in log_weights {
        let shifted = *value - maximum;
        exponentials.push(checked(shifted.exp(), "softmax exponential")?);
    }
    let total: f32 = exponentials.iter().sum();
    let total = checked(total, "softmax sum")?;
    if total <= 0.0 {
        return Err(CoreError::InvalidState(
            "softmax sum must be positive".to_owned(),
        ));
    }
    for value in &mut exponentials {
        *value /= total;
    }
    validate_finite(&exponentials, "softmax output")?;
    Ok(exponentials)
}

/// Alias for [`softmax`] emphasizing its stable implementation.
pub fn stable_softmax(log_weights: &[f32]) -> Result<Vec<f32>, CoreError> {
    softmax(log_weights)
}

/// Computes Biomem's normalized intensity weights:
/// `softmax(log(rbf + 1e-8) + log(h + 1e-8))`.
pub fn log_space_softmax(weights: &[f32], intensity: &[f32]) -> Result<Vec<f32>, CoreError> {
    validate_pair(weights, intensity)?;
    if weights.iter().any(|value| *value < 0.0) {
        return Err(CoreError::InvalidState(
            "negative softmax kernel weight".to_owned(),
        ));
    }
    if intensity.iter().any(|value| *value < 0.0) {
        return Err(CoreError::InvalidState(
            "negative softmax intensity".to_owned(),
        ));
    }
    let logits: Vec<f32> = weights
        .iter()
        .zip(intensity)
        .map(|(weight, h)| (*weight + WEIGHT_EPSILON).ln() + (*h + WEIGHT_EPSILON).ln())
        .collect();
    validate_finite(&logits, "softmax logits")?;
    softmax(&logits)
}

/// Computes a Minkowski dissimilarity for an arbitrary positive `p`.
///
/// The NCM production profile calls this with `p = 0.5`; for that value the
/// expression is `(sum(sqrt(abs(a-b))))²`. This function is a dissimilarity,
/// not a metric, so callers must not assume the triangle inequality.
pub fn minkowski(left: &[f32], right: &[f32], p: f32) -> Result<f32, CoreError> {
    validate_pair(left, right)?;
    checked(p, "Minkowski p")?;
    if p <= 0.0 {
        return Err(CoreError::InvalidState(
            "Minkowski p must be positive".to_owned(),
        ));
    }
    let mut sum = 0.0_f32;
    for (a, b) in left.iter().zip(right) {
        let difference = checked(*a - *b, "Minkowski difference")?;
        let term = if p == 0.5 {
            difference.abs().sqrt()
        } else {
            difference.abs().powf(p)
        };
        sum += term;
        checked(sum, "Minkowski sum")?;
    }
    let result = if p == 0.5 {
        sum.powi(2)
    } else {
        sum.powf(1.0 / p)
    };
    checked(result, "Minkowski dissimilarity")
}

/// Computes the production p=0.5 Minkowski dissimilarity.
pub fn minkowski_p_half(left: &[f32], right: &[f32]) -> Result<f32, CoreError> {
    minkowski(left, right, 0.5)
}

/// Selects up to `k` values and their original indices with deterministic ties.
///
/// Values are ordered largest-first when `largest` is true and smallest-first
/// otherwise. Equal values, including signed zero, are ordered by ascending
/// original index.
pub fn top_k(values: &[f32], k: usize, largest: bool) -> Result<Vec<(f32, usize)>, CoreError> {
    validate_finite(values, "top-k values")?;
    let mut selected: Vec<(f32, usize)> = values
        .iter()
        .copied()
        .enumerate()
        .map(|(index, value)| (value, index))
        .collect();
    selected.sort_by(|(left_value, left_index), (right_value, right_index)| {
        if left_value == right_value {
            left_index.cmp(right_index)
        } else if largest {
            right_value.total_cmp(left_value)
        } else {
            left_value.total_cmp(right_value)
        }
    });
    selected.truncate(k);
    Ok(selected)
}

/// Selects up to `k` largest values with ascending-index tie order.
pub fn top_k_largest(values: &[f32], k: usize) -> Result<Vec<(f32, usize)>, CoreError> {
    top_k(values, k, true)
}

/// Selects up to `k` smallest values with ascending-index tie order.
pub fn top_k_smallest(values: &[f32], k: usize) -> Result<Vec<(f32, usize)>, CoreError> {
    top_k(values, k, false)
}

/// Computes the logistic sigmoid with an overflow-safe negative branch.
pub fn sigmoid(value: f32) -> Result<f32, CoreError> {
    checked(value, "sigmoid input")?;
    let result = if value >= 0.0 {
        1.0 / (1.0 + (-value).exp())
    } else {
        let exponential = value.exp();
        exponential / (1.0 + exponential)
    };
    checked(result, "sigmoid output")
}

/// Computes `ln(1+x)` for finite `x > -1`.
pub fn log1p(value: f32) -> Result<f32, CoreError> {
    checked(value, "log1p input")?;
    if value <= -1.0 {
        return Err(CoreError::InvalidState("log1p requires x > -1".to_owned()));
    }
    checked(value.ln_1p(), "log1p output")
}

/// Computes the hyperbolic tangent of a finite scalar.
pub fn tanh(value: f32) -> Result<f32, CoreError> {
    checked(value, "tanh input")?;
    checked(value.tanh(), "tanh output")
}

/// Advances SplitMix64 once.
#[must_use]
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Small reproducible xoshiro256** generator seeded with SplitMix64.
///
/// This generator is deterministic but not cryptographic. Gaussian values use
/// the f32 Box-Muller transform and cache the second value of each pair.
#[derive(Clone, Debug)]
pub struct DeterministicRng {
    state: [u64; 4],
    spare_gaussian: Option<f32>,
}

impl DeterministicRng {
    /// Seeds the generator, including the valid seed value zero.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut splitmix_state = seed;
        let mut state = [0_u64; 4];
        for word in &mut state {
            *word = splitmix64(&mut splitmix_state);
        }
        Self {
            state,
            spare_gaussian: None,
        }
    }

    /// Alias for [`Self::new`].
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        Self::new(seed)
    }

    /// Advances xoshiro256** and returns one 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let shifted = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= shifted;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    /// Returns a uniform f32 in `[0, 1)` using 24 random mantissa bits.
    pub fn uniform(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 * (1.0 / 16_777_216.0)
    }

    /// Returns a standard-normal f32 using a cached Box-Muller pair.
    pub fn gaussian(&mut self) -> f32 {
        if let Some(value) = self.spare_gaussian.take() {
            return value;
        }
        let first = 1.0 - self.uniform();
        let radius = (-2.0 * first.ln()).sqrt();
        let angle = std::f32::consts::TAU * self.uniform();
        let (sine, cosine) = angle.sin_cos();
        self.spare_gaussian = Some(radius * sine);
        radius * cosine
    }
}

/// Streaming FNV-1a 128-bit digest state.
///
/// This is a stable dependency-free checksum, not a cryptographic digest. The
/// runtime owns SHA-256 identities used for security-sensitive contract data.
#[derive(Clone, Debug)]
pub struct Digest128(u128);

impl Default for Digest128 {
    fn default() -> Self {
        Self(0x6c62_272e_07bb_0142_62b8_2175_6295_c58d)
    }
}

impl Digest128 {
    /// Adds bytes in their supplied order.
    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u128::from(*byte);
            self.0 = self
                .0
                .wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b);
        }
    }

    /// Returns a lowercase, zero-padded 32-character hexadecimal digest.
    #[must_use]
    pub fn hex(&self) -> String {
        format!("{:032x}", self.0)
    }
}

/// Computes a dependency-free FNV-1a 128-bit hexadecimal checksum.
#[must_use]
pub fn digest128(bytes: &[u8]) -> String {
    let mut digest = Digest128::default();
    digest.update(bytes);
    digest.hex()
}
