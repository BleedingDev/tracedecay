//! Persisted, validated f32 projection matrices matching Biomem's layer families.
//!
//! Generation is explicit, never a deserialization fallback. Loaded matrices need
//! only match the frozen shapes and be finite: trained or oracle matrices need
//! not have the initialization distribution. Forward calls never mutate a bundle.

use crate::numeric::{
    DeterministicRng, Digest128, dot, normalize, validate_dimension, validate_finite,
};
use crate::types::{
    CONTEXT_DIM, CoreError, EMBEDDING_DIM, LTM_KEY_DIM, STM_KEY_DIM, TERRAIN_DIM, VALUE_DIM,
};
use serde::{Deserialize, Serialize};

/// Explicit linear transform in torch layout `[output, input]`, row-major.
/// This is a transport structure; validation occurs when constructing a bundle.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionMatrix {
    /// Output dimensionality (matrix rows).
    pub rows: usize,
    /// Input dimensionality (matrix columns).
    pub cols: usize,
    /// Flattened row-major matrix weights.
    pub weights: Vec<f32>,
    /// Output bias; `None` for the two orthogonal projection families.
    pub bias: Option<Vec<f32>>,
}

impl ProjectionMatrix {
    fn validate(&self, rows: usize, cols: usize, bias: bool) -> Result<(), CoreError> {
        for (what, expected, actual) in [
            ("matrix rows", rows, self.rows),
            ("matrix columns", cols, self.cols),
        ] {
            if actual != expected {
                return Err(CoreError::DimensionMismatch {
                    what,
                    expected,
                    actual,
                });
            }
        }
        validate_dimension(&self.weights, rows * cols, "matrix weights")?;
        validate_finite(&self.weights, "matrix weights")?;
        match (&self.bias, bias) {
            (Some(values), true) => {
                validate_dimension(values, rows, "matrix bias")?;
                validate_finite(values, "matrix bias")
            }
            (None, false) => Ok(()),
            _ => Err(CoreError::InvalidState(
                "incorrect projection bias presence".into(),
            )),
        }
    }

    fn forward(&self, input: &[f32]) -> Result<Vec<f32>, CoreError> {
        validate_dimension(input, self.cols, "projection input")?;
        validate_finite(input, "projection input")?;
        let mut output = Vec::with_capacity(self.rows);
        for (index, row) in self.weights.chunks_exact(self.cols).enumerate() {
            let bias = self.bias.as_ref().map_or(0.0, |bias| bias[index]);
            output.push(dot(row, input)? + bias);
        }
        validate_finite(&output, "projection output")?;
        Ok(output)
    }

    fn xavier(rows: usize, cols: usize, gain: f32, rng: &mut DeterministicRng) -> Self {
        let bound = gain * (6.0 / (rows + cols) as f32).sqrt();
        let weights = (0..rows * cols)
            .map(|_| (2.0 * rng.uniform() - 1.0) * bound)
            .collect();
        Self {
            rows,
            cols,
            weights,
            bias: Some(vec![0.0; rows]),
        }
    }

    // Modified Gram–Schmidt with reorthogonalization on a Gaussian matrix.
    // Wide matrices have orthonormal rows; tall matrices orthonormal columns,
    // matching torch orthogonal_'s transpose-before-QR convention.
    fn orthogonal(rows: usize, cols: usize, rng: &mut DeterministicRng) -> Self {
        let count = rows.min(cols);
        let width = rows.max(cols);
        let mut basis: Vec<Vec<f32>> = Vec::with_capacity(count);
        for _ in 0..count {
            loop {
                let mut vector: Vec<f32> = (0..width).map(|_| rng.gaussian()).collect();
                for _ in 0..2 {
                    for previous in &basis {
                        let projection: f32 = vector.iter().zip(previous).map(|(a, b)| a * b).sum();
                        for (value, component) in vector.iter_mut().zip(previous) {
                            *value -= projection * component;
                        }
                    }
                }
                let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
                // Resample a numerically degenerate column rather than inventing
                // a zero basis vector or returning a partially generated bundle.
                if norm > 1e-6 && norm.is_finite() {
                    for value in &mut vector {
                        *value /= norm;
                    }
                    basis.push(vector);
                    break;
                }
            }
        }
        let mut weights = vec![0.0; rows * cols];
        for row in 0..rows {
            for col in 0..cols {
                weights[row * cols + col] = if rows <= cols {
                    basis[row][col]
                } else {
                    basis[col][row]
                };
            }
        }
        Self {
            rows,
            cols,
            weights,
            bias: None,
        }
    }
}

/// Complete transport representation for all seven persisted transforms.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionMatrices {
    /// Embedding → normalized LTM key, 64×384 with bias.
    pub to_ltm_key: ProjectionMatrix,
    /// Embedding → normalized STM key, 16×384 with bias.
    pub to_stm_key: ProjectionMatrix,
    /// Embedding → unnormalized value, 128×384 with bias.
    pub to_value: ProjectionMatrix,
    /// Embedding → normalized context, 16×384 without bias.
    pub to_context: ProjectionMatrix,
    /// LTM key → tanh terrain coordinate, 3×64 with bias.
    pub ltm_to_terrain: ProjectionMatrix,
    /// STM key → tanh terrain coordinate, 3×16 with bias.
    pub stm_to_terrain: ProjectionMatrix,
    /// Reference-only STM → LTM key, 64×16 without bias (D08).
    pub stm_to_ltm: ProjectionMatrix,
}

impl ProjectionMatrices {
    fn ordered(&self) -> [&ProjectionMatrix; 7] {
        [
            &self.to_ltm_key,
            &self.to_stm_key,
            &self.to_value,
            &self.to_context,
            &self.ltm_to_terrain,
            &self.stm_to_terrain,
            &self.stm_to_ltm,
        ]
    }
}

/// Immutable validated bundle. Serde loading routes through `from_matrices`, so
/// malformed persisted state cannot bypass shape/finiteness validation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ProjectionMatrices", into = "ProjectionMatrices")]
pub struct ProjectionBundle {
    matrices: ProjectionMatrices,
}

impl TryFrom<ProjectionMatrices> for ProjectionBundle {
    type Error = CoreError;

    fn try_from(matrices: ProjectionMatrices) -> Result<Self, Self::Error> {
        Self::from_matrices(matrices)
    }
}

impl From<ProjectionBundle> for ProjectionMatrices {
    fn from(bundle: ProjectionBundle) -> Self {
        bundle.matrices
    }
}

/// Numerical portion of an observed record. The direct LTM key is retained at
/// observation time, never reconstructed from the unrelated STM projection (D08).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectedRecord {
    /// Canonical direct LTM-basis key, 64 elements.
    pub ltm_key: Vec<f32>,
    /// STM key, 16 elements.
    pub stm_key: Vec<f32>,
    /// Unnormalized projection of the value embedding, 128 elements.
    pub value: Vec<f32>,
    /// Normalized key context fingerprint, 16 elements.
    pub context: Vec<f32>,
    /// Terrain coordinate derived from the canonical LTM key.
    pub ltm_terrain: Vec<f32>,
    /// Terrain coordinate derived from the STM key.
    pub stm_terrain: Vec<f32>,
}

impl ProjectionBundle {
    /// Explicit one-time generation. Same seed produces identical matrices on
    /// the same numerical platform; torch RNG bit parity is intentionally not required.
    #[must_use]
    pub fn generate(seed: u64) -> Self {
        let mut rng = DeterministicRng::new(seed);
        Self {
            matrices: ProjectionMatrices {
                to_ltm_key: ProjectionMatrix::xavier(LTM_KEY_DIM, EMBEDDING_DIM, 1.0, &mut rng),
                to_stm_key: ProjectionMatrix::xavier(STM_KEY_DIM, EMBEDDING_DIM, 1.0, &mut rng),
                to_value: ProjectionMatrix::xavier(VALUE_DIM, EMBEDDING_DIM, 1.0, &mut rng),
                to_context: ProjectionMatrix::orthogonal(CONTEXT_DIM, EMBEDDING_DIM, &mut rng),
                ltm_to_terrain: ProjectionMatrix::xavier(TERRAIN_DIM, LTM_KEY_DIM, 0.5, &mut rng),
                stm_to_terrain: ProjectionMatrix::xavier(TERRAIN_DIM, STM_KEY_DIM, 0.5, &mut rng),
                stm_to_ltm: ProjectionMatrix::orthogonal(LTM_KEY_DIM, STM_KEY_DIM, &mut rng),
            },
        }
    }

    /// Validates all shapes, bias presence and finite elements before constructing
    /// a bundle. Arbitrary valid persisted/oracle matrices are not reinitialized.
    pub fn from_matrices(matrices: ProjectionMatrices) -> Result<Self, CoreError> {
        let shapes = [
            (LTM_KEY_DIM, EMBEDDING_DIM, true),
            (STM_KEY_DIM, EMBEDDING_DIM, true),
            (VALUE_DIM, EMBEDDING_DIM, true),
            (CONTEXT_DIM, EMBEDDING_DIM, false),
            (TERRAIN_DIM, LTM_KEY_DIM, true),
            (TERRAIN_DIM, STM_KEY_DIM, true),
            (LTM_KEY_DIM, STM_KEY_DIM, false),
        ];
        for (matrix, (rows, cols, bias)) in matrices.ordered().into_iter().zip(shapes) {
            matrix.validate(rows, cols, bias)?;
        }
        Ok(Self { matrices })
    }

    /// Immutable explicit matrices for persistence and audit; no mutation bypass.
    #[must_use]
    pub fn matrices(&self) -> &ProjectionMatrices {
        &self.matrices
    }

    /// Stable 128-bit FNV-1a identity of matrix and bias f32 little-endian bytes,
    /// in declaration order. This task-local checksum is not cryptographic SHA-256;
    /// the runtime owns the frozen contract's SHA-256 persistence/receipt digest.
    #[must_use]
    pub fn identity(&self) -> String {
        let mut digest = Digest128::default();
        for matrix in self.matrices.ordered() {
            for value in matrix.weights.iter().chain(matrix.bias.iter().flatten()) {
                digest.update(&value.to_le_bytes());
            }
        }
        digest.hex()
    }

    /// Embedding → LTM key, normalized with the reference 1e-12 denominator floor.
    pub fn project_to_ltm(&self, input: &[f32]) -> Result<Vec<f32>, CoreError> {
        normalize(&self.matrices.to_ltm_key.forward(input)?)
    }

    /// Embedding → STM key, normalized.
    pub fn project_to_stm(&self, input: &[f32]) -> Result<Vec<f32>, CoreError> {
        normalize(&self.matrices.to_stm_key.forward(input)?)
    }

    /// Embedding → value, deliberately NOT normalized.
    pub fn project_to_value(&self, input: &[f32]) -> Result<Vec<f32>, CoreError> {
        self.matrices.to_value.forward(input)
    }

    /// Embedding → normalized context fingerprint (no bias).
    pub fn project_to_context(&self, input: &[f32]) -> Result<Vec<f32>, CoreError> {
        normalize(&self.matrices.to_context.forward(input)?)
    }

    /// LTM key → tanh terrain coordinate in `[-1,1]^3`.
    pub fn ltm_to_3d(&self, key: &[f32]) -> Result<Vec<f32>, CoreError> {
        Ok(self
            .matrices
            .ltm_to_terrain
            .forward(key)?
            .into_iter()
            .map(f32::tanh)
            .collect())
    }

    /// STM key → tanh terrain coordinate in `[-1,1]^3`.
    pub fn stm_to_3d(&self, key: &[f32]) -> Result<Vec<f32>, CoreError> {
        Ok(self
            .matrices
            .stm_to_terrain
            .forward(key)?
            .into_iter()
            .map(f32::tanh)
            .collect())
    }

    /// Reference `normalize(U*k_stm)` for fixtures only. Production consolidation
    /// must use the canonical LTM key retained by `project_record` (D08).
    pub fn consolidate_key(&self, key: &[f32]) -> Result<Vec<f32>, CoreError> {
        normalize(&self.matrices.stm_to_ltm.forward(key)?)
    }

    /// Projects a key/value embedding pair atomically, preserving its direct LTM key.
    pub fn project_record(&self, key: &[f32], value: &[f32]) -> Result<ProjectedRecord, CoreError> {
        for input in [key, value] {
            validate_dimension(input, EMBEDDING_DIM, "record embedding")?;
            validate_finite(input, "record embedding")?;
        }
        let ltm_key = self.project_to_ltm(key)?;
        let stm_key = self.project_to_stm(key)?;
        Ok(ProjectedRecord {
            ltm_terrain: self.ltm_to_3d(&ltm_key)?,
            stm_terrain: self.stm_to_3d(&stm_key)?,
            ltm_key,
            stm_key,
            value: self.project_to_value(value)?,
            context: self.project_to_context(key)?,
        })
    }
}
